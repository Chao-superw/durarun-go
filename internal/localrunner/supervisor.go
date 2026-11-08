package localrunner

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/Chao-superw/durarun-go/internal/protocol"
)

const DefaultShutdownGracePeriod = 10 * time.Second

// Supervisor directly executes Argv in a separate process group. Nil streams
// and Env inherit the runner's standard streams and environment. A Supervisor
// owns its process group, including descendants left behind by the main child.
// Run must not be used concurrently by multiple supervisors in a PID 1 process.
type Supervisor struct {
	Argv                []string
	WorkDir             string
	Env                 []string
	Stdin               io.Reader
	Stdout              io.Writer
	Stderr              io.Writer
	ShutdownGracePeriod time.Duration
	// ReapChildren enables Linux subreaper adoption outside PID 1. Use only
	// in a dedicated runner process, since adoption is process-wide.
	ReapChildren bool
}

// Run separates user outcomes (including nonzero exits and signals) from
// runner failures. A user failure has a non-nil result and a nil error; launch
// and supervision failures have a non-nil error and an explanatory result.
// Cancellation requests a group-wide SIGTERM, never CommandContext's
// immediate kill of just the direct child.
func (s *Supervisor) Run(ctx context.Context) (*protocol.ProcessResult, error) {
	if len(s.Argv) == 0 || s.Argv[0] == "" {
		return runnerFailure(ExitUsage, fmt.Errorf("supervisor: argv must not be empty"))
	}
	if s.ShutdownGracePeriod < 0 {
		return runnerFailure(ExitUsage, fmt.Errorf("supervisor: shutdown grace period must not be negative"))
	}
	if err := ctx.Err(); err != nil {
		return runnerFailure(ExitInternal, fmt.Errorf("supervisor: before start: %w", err))
	}
	if s.ReapChildren {
		if err := prepareChildReaping(); err != nil {
			return runnerFailure(ExitInternal, fmt.Errorf("prepare child reaping: %w", err))
		}
	}
	grace := s.ShutdownGracePeriod
	if grace == 0 {
		grace = DefaultShutdownGracePeriod
	}

	// Register before starting the child unless the complete execution lifecycle
	// already owns signals; that context preserves the original INT/TERM cause.
	var signals chan os.Signal
	if !executionOwnsSignals(ctx) {
		signals = make(chan os.Signal, 8)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
		defer signal.Stop(signals)
	}

	cmd := exec.Command(s.Argv[0], s.Argv[1:]...)
	cmd.Dir, cmd.Env = s.WorkDir, s.Env
	cmd.Stdin, cmd.Stdout, cmd.Stderr = s.Stdin, s.Stdout, s.Stderr
	if cmd.Stdin == nil {
		cmd.Stdin = os.Stdin
	}
	if cmd.Stdout == nil {
		cmd.Stdout = os.Stdout
	}
	if cmd.Stderr == nil {
		cmd.Stderr = os.Stderr
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Bound waiting on redirected IO inherited by a surviving descendant.
	cmd.WaitDelay = grace
	if err := cmd.Start(); err != nil {
		return runnerFailure(launchExitCode(err), fmt.Errorf("exec start: %w", err))
	}
	pid := cmd.Process.Pid
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()

	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	var deadline *time.Timer
	var deadlineC <-chan time.Time
	defer func() {
		if deadline != nil {
			deadline.Stop()
		}
	}()
	cancelled := ctx.Done()
	var result *protocol.ProcessResult
	var runErr error
	shuttingDown := false
	killed := false
	var killTime time.Time

	shutdown := func(sig syscall.Signal) {
		// pid is the child's new group ID, never the runner/test parent's group.
		if err := signalProcessGroup(pid, sig); err != nil && runErr == nil {
			runErr = fmt.Errorf("signal process group: %w", err)
		}
		if !shuttingDown {
			shuttingDown = true
			deadline = time.NewTimer(grace)
			deadlineC = deadline.C
		}
	}

	for {
		// PID 1 / the CLI subreaper reaps adopted children, never cmd.Wait's child.
		reapOrphans(pid)
		if result != nil && !processGroupExists(pid) {
			if runErr != nil {
				result.Error = runErr.Error()
				return result, &RunError{ExitCode: ExitInternal, Err: runErr}
			}
			return result, nil
		}
		// A zombie cannot be killed. Do not hang forever waiting for an external
		// init to reap it when the runner is not PID 1 (e.g. local CLI tests).
		if result != nil && killed && time.Since(killTime) >= time.Second {
			if runErr != nil {
				result.Error = runErr.Error()
				return result, &RunError{ExitCode: ExitInternal, Err: runErr}
			}
			return result, nil
		}
		select {
		case sig := <-signals:
			shutdown(sig.(syscall.Signal))
		case <-cancelled:
			cancelled = nil
			shutdown(executionCancellationSignal(ctx))
		case err := <-wait:
			wait = nil
			result = ExitToResult(err)
			if result.Error != "" {
				runErr = err
			}
			// Normal main-process exit must not leave background children alive.
			if processGroupExists(pid) && !shuttingDown {
				shutdown(syscall.SIGTERM)
			}
		case <-deadlineC:
			deadlineC = nil
			if err := signalProcessGroup(pid, syscall.SIGKILL); err != nil && runErr == nil {
				runErr = fmt.Errorf("kill process group: %w", err)
			}
			killed, killTime = true, time.Now()
		case <-poll.C:
		}
	}
}

func runnerFailure(code int, err error) (*protocol.ProcessResult, error) {
	return &protocol.ProcessResult{ExitCode: code, Error: err.Error()}, &RunError{ExitCode: code, Err: err}
}
