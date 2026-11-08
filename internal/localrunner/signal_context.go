package localrunner

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
)

type executionSignalKey struct{}

type executionSignal struct{ signal syscall.Signal }

func (s *executionSignal) Error() string { return "execution interrupted by " + s.signal.String() }
func (s *executionSignal) Unwrap() error { return context.Canceled }

// SignalContext owns SIGINT/SIGTERM for an entire execution lifecycle, including
// bootstrap and publication. Supervisors using this context (or a descendant)
// forward its original signal rather than registering a second signal handler.
// Ordinary context cancellation and deadline/revocation still request SIGTERM.
// The caller must invoke stop to unregister and join the signal listener.
func SignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(context.WithValue(parent, executionSignalKey{}, true))
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case sig := <-signals:
			cancel(&executionSignal{signal: sig.(syscall.Signal)})
		case <-ctx.Done():
		}
	}()
	return ctx, func() {
		signal.Stop(signals)
		cancel(context.Canceled)
		<-done
	}
}

func executionOwnsSignals(ctx context.Context) bool {
	owned, _ := ctx.Value(executionSignalKey{}).(bool)
	return owned
}

func executionCancellationSignal(ctx context.Context) syscall.Signal {
	var sig *executionSignal
	if executionOwnsSignals(ctx) && errors.As(context.Cause(ctx), &sig) {
		return sig.signal
	}
	return syscall.SIGTERM
}
