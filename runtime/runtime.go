package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/Chao-superw/durarun-go/observe"
	"github.com/Chao-superw/durarun-go/protect"
	"github.com/Chao-superw/durarun-go/wal"
)

// Runtime integrates WAL, observability and protection into a single runner.
type Runtime struct {
	cfg                 Config
	walWriter           *wal.Writer
	tracer              *observe.Tracer
	metrics             *observe.Metrics
	completedSteps      map[string]*wal.CompletedStep
	cancelFunc          context.CancelFunc
	stateMu             sync.Mutex
	nextCheckpoint      time.Time
	checkpointScheduled bool
	gateMu              sync.Mutex
	gateChanged         chan struct{}
	mutations           int
	paused              bool
	closed              bool
	closeOnce           sync.Once
	closeErr            error
}

// New creates a Runtime.  In RecoveryMode it reads the existing WAL to
// discover which steps have already completed.
func New(cfg Config) (*Runtime, error) {
	for _, name := range []string{"DURARUN_AUTHORIZATION_VERSION", "DURARUN_SDK_BOOTSTRAP"} {
		if _, present := os.LookupEnv(name); present {
			return nil, fmt.Errorf("runtime: generation configuration is unsupported by the local SDK")
		}
	}
	if cfg.Durable || cfg.CheckpointSocket != "" || os.Getenv("DURARUN_DURABLE") == "true" {
		if cfg.WorkDir == "" {
			cfg.WorkDir = os.Getenv("DURARUN_WORK_DIR")
		}
		if expected := os.Getenv("DURARUN_WORK_DIR"); expected != "" {
			actual, err := filepath.Abs(cfg.WorkDir)
			if err != nil || actual != filepath.Clean(expected) {
				return nil, fmt.Errorf("runtime: work directory differs from durable execution workspace")
			}
		}
		if cfg.CheckpointSocket == "" {
			cfg.CheckpointSocket = os.Getenv("DURARUN_CHECKPOINT_SOCKET")
		}
		if cfg.ApplicationVersion == "" {
			cfg.ApplicationVersion = os.Getenv("DURARUN_APPLICATION_VERSION")
		}
		if cfg.CheckpointInterval == 0 {
			if value := os.Getenv("DURARUN_CHECKPOINT_INTERVAL_SECONDS"); value != "" {
				seconds, err := strconv.ParseInt(value, 10, 32)
				if err != nil || seconds < 0 {
					return nil, fmt.Errorf("runtime: invalid checkpoint interval")
				}
				cfg.CheckpointInterval = time.Duration(seconds) * time.Second
			}
		}
		if os.Getenv("DURARUN_RECOVERY") == "true" {
			cfg.RecoveryMode = true
		}
	}
	return newRuntime(cfg)
}

// newRuntime opens the WAL after configuration has been selected.
func newRuntime(cfg Config) (_ *Runtime, retErr error) {
	w, err := wal.NewWriter(cfg.WorkDir)
	if err != nil {
		return nil, fmt.Errorf("runtime: create WAL writer: %w", err)
	}

	defer func() {
		if retErr != nil {
			_ = w.Close()
		}
	}()
	rt := &Runtime{
		gateChanged:    make(chan struct{}),
		nextCheckpoint: time.Now().Add(cfg.CheckpointInterval),
		cfg:            cfg,
		walWriter:      w,
		completedSteps: make(map[string]*wal.CompletedStep),
	}

	if cfg.RecoveryMode {
		reader, err := wal.NewReader(cfg.WorkDir)
		if err != nil {
			return nil, fmt.Errorf("runtime: open WAL reader: %w", err)
		}
		manifest, err := reader.BuildManifest()
		if err != nil {
			return nil, fmt.Errorf("runtime: build manifest: %w", err)
		}
		for i := range manifest.CompletedSteps {
			cs := &manifest.CompletedSteps[i]
			rt.completedSteps[cs.StepID] = cs
		}
	}

	obsCfg := observe.Config{
		TraceExporter:   cfg.Observe.TraceExporter,
		MetricsExporter: cfg.Observe.MetricsExporter,
		ServiceName:     "agent-fabric",
	}
	rt.tracer, err = observe.NewTracer(obsCfg)
	if err != nil {
		return nil, fmt.Errorf("runtime: create tracer: %w", err)
	}
	rt.metrics, err = observe.NewMetrics(obsCfg)
	if err != nil {
		return nil, fmt.Errorf("runtime: create metrics: %w", err)
	}

	return rt, nil
}

// Run executes fn inside a job-level span, honouring JobTimeout and
// recording a WAL job_cancelled record when the context is cancelled.
func (rt *Runtime) Run(ctx context.Context, fn func(*Context) error) error {
	ctx, jobCancel := protect.ApplyJobTimeout(ctx, rt.cfg.JobTimeout)
	defer jobCancel()

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	rt.stateMu.Lock()
	rt.cancelFunc = cancel
	rt.stateMu.Unlock()
	defer cancel()

	ctx, span := rt.tracer.StartSpan(ctx, "af.job.run",
		attribute.Bool(observe.AttrRecoveryMode, rt.cfg.RecoveryMode),
	)

	afCtx := &Context{ctx: ctx, runtime: rt}
	err := fn(afCtx)

	// Capture context state before defers fire.
	ctxErr := ctx.Err()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()

	if ctxErr != nil {
		if err == nil {
			err = ctxErr
		}
		if gateErr := rt.enterMutation(context.Background()); gateErr != nil {
			err = errors.Join(err, gateErr)
		} else {
			_, walErr := rt.walWriter.JobCancelled(ctxErr.Error())
			rt.leaveMutation()
			if walErr != nil {
				err = errors.Join(err, fmt.Errorf("runtime: record cancellation: %w", walErr))
			}
		}
	}

	return err
}

// Cancel signals the running job to stop.
func (rt *Runtime) Cancel() {
	rt.stateMu.Lock()
	cancel := rt.cancelFunc
	rt.stateMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Close shuts down observability providers and closes the WAL. No gate/state
// lock is held during WAL fsync or provider shutdown.
func (rt *Runtime) Close() error {
	rt.closeOnce.Do(func() {
		rt.Cancel()
		rt.gateMu.Lock()
		rt.closed = true
		rt.notifyGate()
		rt.gateMu.Unlock()
		ctx := context.Background()
		rt.closeErr = errors.Join(rt.tracer.Shutdown(ctx), rt.metrics.Shutdown(ctx), rt.walWriter.Sync(), rt.walWriter.Close())
	})
	return rt.closeErr
}
