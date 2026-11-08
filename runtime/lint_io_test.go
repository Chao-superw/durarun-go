package runtime

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Chao-superw/durarun-go/protect"
)

func newRuntimeForWALFailureTest(t *testing.T) *Runtime {
	t.Helper()
	rt, err := New(Config{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// These tests deliberately close the WAL before Runtime.Close. The
		// best-effort teardown must still shut down observability providers;
		// the injected Close error is asserted separately below.
		_ = rt.Close()
	})
	return rt
}

func TestStep_WALBeginFailureDoesNotExecuteWork(t *testing.T) {
	rt := newRuntimeForWALFailureTest(t)
	if err := rt.walWriter.Close(); err != nil {
		t.Fatal(err)
	}
	called := false
	err := rt.Run(context.Background(), func(c *Context) error {
		return c.Step("unrecorded", func(context.Context) error {
			called = true
			return nil
		})
	})
	if called {
		t.Fatal("work executed even though StepBegin could not be recorded")
	}
	if !errors.Is(err, os.ErrClosed) || !strings.Contains(err.Error(), "begin") {
		t.Fatalf("expected wrapped StepBegin closed-WAL error, got %v", err)
	}
}

func TestStep_WALEndFailureIsNotSuccess(t *testing.T) {
	rt := newRuntimeForWALFailureTest(t)
	calls := 0
	err := rt.Run(context.Background(), func(c *Context) error {
		return c.Step("uncommitted", func(context.Context) error {
			calls++
			// Fault injection happens after StepBegin, before StepEnd.
			return rt.walWriter.Close()
		})
	})
	if calls != 1 {
		t.Fatalf("expected one successful work invocation, got %d", calls)
	}
	if !errors.Is(err, os.ErrClosed) || !strings.Contains(err.Error(), "end") {
		t.Fatalf("expected wrapped StepEnd closed-WAL error, got %v", err)
	}
}

func TestStep_WALTimeoutFailurePreservesTimeoutClassification(t *testing.T) {
	rt := newRuntimeForWALFailureTest(t)
	err := rt.Run(context.Background(), func(c *Context) error {
		return c.Step("timeout", func(ctx context.Context) error {
			<-ctx.Done()
			if err := rt.walWriter.Close(); err != nil {
				return err
			}
			return ctx.Err()
		}, WithTimeout(time.Millisecond))
	})
	if !protect.IsTimeout(err) || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected a recognizable timeout and WAL failure, got %v", err)
	}
}

func TestRun_WALCancellationFailurePreservesCanceled(t *testing.T) {
	rt := newRuntimeForWALFailureTest(t)
	if err := rt.walWriter.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := rt.Run(ctx, func(*Context) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation was lost when its WAL record failed: %v", err)
	}
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("WAL write failure was not retained alongside cancellation: %v", err)
	}
}

func TestClose_RetainsWALSyncAndCloseErrors(t *testing.T) {
	rt := newRuntimeForWALFailureTest(t)
	if err := rt.walWriter.Close(); err != nil {
		t.Fatal(err)
	}
	// Sync may wrap a platform errno while Close uses os.File.Sync. Compare
	// both independently observed errors rather than hard-coding one platform.
	syncErr := rt.walWriter.Sync()
	closeErr := rt.walWriter.Close()
	if syncErr == nil || closeErr == nil {
		t.Fatalf("fault injection must fail both WAL operations: sync=%v close=%v", syncErr, closeErr)
	}
	err := rt.Close()
	if err == nil {
		t.Fatal("Runtime.Close silently discarded the WAL failures")
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("expected aggregated shutdown errors, got %T: %v", err, err)
	}
	causes := joined.Unwrap()
	if len(causes) != 2 || causes[0].Error() != syncErr.Error() || causes[1].Error() != closeErr.Error() {
		t.Fatalf("Close must retain both sync and close failures, got %v", causes)
	}
}
