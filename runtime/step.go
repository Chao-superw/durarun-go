package runtime

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/Chao-superw/durarun-go/observe"
	"github.com/Chao-superw/durarun-go/protect"
)

// StepOption configures a single Step invocation.
type StepOption func(*stepConfig)

type stepConfig struct {
	timeout time.Duration
	retry   protect.RetryConfig
}

// WithTimeout overrides the default step timeout for this step.
func WithTimeout(d time.Duration) StepOption {
	return func(sc *stepConfig) { sc.timeout = d }
}

// WithRetry sets a retry policy for this step.
func WithRetry(cfg protect.RetryConfig) StepOption {
	return func(sc *stepConfig) { sc.retry = cfg }
}

// Step executes fn as a named step with WAL journaling, OTEL tracing,
// metrics recording, optional timeout and retry.
func (c *Context) Step(id string, fn func(ctx context.Context) error, opts ...StepOption) (retErr error) {
	rt := c.runtime
	if err := rt.enterMutation(c.ctx); err != nil {
		return err
	}
	completed := false
	defer func() {
		rt.leaveMutation()
		if completed && retErr == nil {
			retErr = rt.checkpointIfDue(c.ctx)
		}
	}()

	// Merge options.
	sc := stepConfig{
		timeout: rt.cfg.StepTimeout,
		retry:   protect.DefaultRetryConfig(),
	}
	for _, o := range opts {
		o(&sc)
	}

	// Recovery: skip already-completed steps.
	if rt.cfg.RecoveryMode {
		if _, ok := rt.completedSteps[id]; ok {
			_, span := rt.tracer.StartSpan(c.ctx, "af.step."+id,
				attribute.String(observe.AttrStepID, id),
				attribute.Bool(observe.AttrStepSkipped, true),
			)
			span.End()
			completed = true
			return nil
		}
	}

	// Normal execution path.
	ctx, span := rt.tracer.StartSpan(c.ctx, "af.step."+id,
		attribute.String(observe.AttrStepID, id),
	)
	start := time.Now()

	if _, err := rt.walWriter.StepBegin(id); err != nil {
		err = fmt.Errorf("runtime: record step %s begin: %w", id, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		rt.metrics.RecordStep(ctx, id, time.Since(start), err)
		span.End()
		return err
	}

	// Apply step-level timeout.
	stepCtx, stepCancel := protect.ApplyStepTimeout(ctx, sc.timeout)
	stepCtx = context.WithValue(stepCtx, mutationContextKey{}, true)
	defer stepCancel()

	// Execute with retry.
	onRetry := func(attempt int) {
		// Retry notifications cannot return errors; report the lost diagnostic record
		// without changing the user's retry policy.
		if _, err := rt.walWriter.StepRetry(id, attempt); err != nil {
			log.Printf("runtime: record step %s retry %d: %v", id, attempt, err)
		}
	}
	_, err := protect.Execute(stepCtx, sc.retry, fn, onRetry)

	duration := time.Since(start)

	if err == nil {
		if _, walErr := rt.walWriter.StepEnd(id, 0, ""); walErr != nil {
			err = fmt.Errorf("runtime: record step %s end: %w", id, walErr)
		} else {
			rt.metrics.RecordWALFsync(ctx, rt.walWriter.LastFsyncLatency())
			span.SetAttributes(attribute.Int(observe.AttrStepExitCode, 0))
		}
	} else if protect.IsTimeout(err) {
		if _, walErr := rt.walWriter.StepTimeout(id); walErr != nil {
			err = errors.Join(err, fmt.Errorf("runtime: record step %s timeout: %w", id, walErr))
		}
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	rt.metrics.RecordStep(ctx, id, duration, err)
	span.End()

	// Propagate job-level cancellation even when the step itself succeeded.
	if c.ctx.Err() != nil && err == nil {
		return c.ctx.Err()
	}
	completed = true
	return err
}
