package localrunner

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSignalContextPropagatesParentCancellationAndJoins(t *testing.T) {
	parent, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	ctx, stop := SignalContext(parent)
	defer stop()
	child, childCancel := context.WithTimeout(ctx, time.Second)
	defer childCancel()
	require.True(t, executionOwnsSignals(child))
	cause := errors.New("execution authority revoked")
	cancel(cause)
	<-child.Done()
	require.ErrorIs(t, context.Cause(child), cause)
	require.Equal(t, syscall.SIGTERM, executionCancellationSignal(child))
	stop()
	stop() // Stop is safe after parent cancellation and on repeated cleanup.
	require.ErrorIs(t, context.Cause(ctx), cause)
}

func TestSignalContextPreservesSignalThroughDerivedContexts(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			ctx, stop := SignalContext(context.Background())
			defer stop()
			child, cancel := context.WithCancelCause(ctx)
			cancel(fmt.Errorf("wrapped cancellation: %w", &executionSignal{signal: sig}))
			require.ErrorIs(t, context.Cause(child), context.Canceled)
			require.Equal(t, sig, executionCancellationSignal(child))
		})
	}
}

func TestSignalContextOrdinaryStopAndDeadlineRemainTERM(t *testing.T) {
	ctx, stop := SignalContext(context.Background())
	child, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	require.ErrorIs(t, context.Cause(child), context.DeadlineExceeded)
	require.Equal(t, syscall.SIGTERM, executionCancellationSignal(child))
	stop()
	require.ErrorIs(t, context.Cause(ctx), context.Canceled)
	require.Equal(t, syscall.SIGTERM, executionCancellationSignal(ctx))
	require.False(t, executionOwnsSignals(context.Background()))
}
