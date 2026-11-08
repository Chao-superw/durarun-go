package wal

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type writerEventCase struct {
	name    string
	durable bool
	write   func(*Writer) (int64, error)
	fields  map[string]any
}

func writerEventCases() []writerEventCase {
	return []writerEventCase{
		{name: "begin", write: func(w *Writer) (int64, error) { return w.StepBegin("begin-id") },
			fields: map[string]any{"type": TypeStepBegin, "step_id": "begin-id"}},
		{name: "end", durable: true, write: func(w *Writer) (int64, error) { return w.StepEnd("end-id", 23, "out/result") },
			fields: map[string]any{"type": TypeStepEnd, "step_id": "end-id", "exit": float64(23), "output_ref": "out/result"}},
		{name: "retry", durable: true, write: func(w *Writer) (int64, error) { return w.StepRetry("retry-id", 2) },
			fields: map[string]any{"type": TypeStepRetry, "step_id": "retry-id", "attempt": float64(2)}},
		{name: "timeout", durable: true, write: func(w *Writer) (int64, error) { return w.StepTimeout("timeout-id") },
			fields: map[string]any{"type": TypeStepTimeout, "step_id": "timeout-id", "reason": "timeout"}},
		{name: "cancel", durable: true, write: func(w *Writer) (int64, error) { return w.JobCancelled("cancel-reason") },
			fields: map[string]any{"type": TypeJobCancelled, "reason": "cancel-reason"}},
	}
}

func writerEventAssertJSON(t *testing.T, data []byte, fields map[string]any, seq int64, before, after int64) {
	t.Helper()
	require.True(t, strings.HasSuffix(string(data), "\n"), "each event remains one complete JSONL record")
	require.Equal(t, 1, strings.Count(string(data), "\n"))
	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	require.EqualValues(t, seq, got["seq"])
	timestamp, ok := got["ts"].(float64)
	require.True(t, ok)
	require.GreaterOrEqual(t, int64(timestamp), before)
	require.LessOrEqual(t, int64(timestamp), after)
	delete(got, "seq")
	delete(got, "ts")
	require.Equal(t, fields, got, "no event-specific field may appear, disappear or change type")
}

func TestWriterEventsRegressionFieldsAndSyncPolicy(t *testing.T) {
	events := append(writerEventCases(), writerEventCase{name: "end-zero-exit", durable: true,
		write:  func(w *Writer) (int64, error) { return w.StepEnd("zero-id", 0, "") },
		fields: map[string]any{"type": TypeStepEnd, "step_id": "zero-id", "exit": float64(0)}})
	for _, event := range events {
		t.Run(event.name, func(t *testing.T) {
			// A local pipe accepts the small event write but cannot be fsynced.
			// It distinguishes write-only Begin from all four durable event kinds
			// without a production sync callback or other injection point.
			read, write, err := os.Pipe()
			require.NoError(t, err)
			t.Cleanup(func() { _ = read.Close(); _ = write.Close() })
			require.Error(t, platformSync(write), "fixture must reject fsync")
			w := &Writer{file: write, seq: 40, fsyncLatency: -time.Second}
			before := time.Now().UnixMilli()
			seq, err := event.write(w)
			after := time.Now().UnixMilli()
			if event.durable {
				require.Error(t, err, "durable events must propagate their post-write sync failure")
				require.Zero(t, seq)
			} else {
				require.NoError(t, err, "Begin must not add fsync")
				require.EqualValues(t, 41, seq)
			}
			require.EqualValues(t, 41, w.seq, "sync failure still consumes the assigned sequence")
			require.Equal(t, -time.Second, w.LastFsyncLatency(), "only successful fsync may update latency")
			require.NoError(t, write.Close())
			data, err := io.ReadAll(read)
			require.NoError(t, err)
			writerEventAssertJSON(t, data, event.fields, 41, before, after)
		})
	}
}

func TestWriterEventsRegressionAppendFailureConsumesSequenceWithoutSync(t *testing.T) {
	for _, event := range writerEventCases() {
		t.Run(event.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "readonly-wal")
			require.NoError(t, os.WriteFile(path, []byte("unchanged"), 0600))
			file, err := os.Open(path)
			require.NoError(t, err)
			defer func() { require.NoError(t, file.Close()) }()
			// This fd rejects Write but supports Sync. If an append error wrongly
			// reaches fsync, the sentinel latency would be overwritten.
			require.NoError(t, platformSync(file), "fixture must allow sync on its read-only fd")
			w := &Writer{file: file, seq: 40, fsyncLatency: -time.Second}
			for expected := int64(41); expected <= 42; expected++ {
				seq, err := event.write(w)
				require.Error(t, err)
				require.Zero(t, seq)
				var pathErr *os.PathError
				require.ErrorAs(t, err, &pathErr)
				require.Equal(t, "write", pathErr.Op, "append error must remain the returned cause")
				require.Equal(t, expected, w.seq, "append failure consumes, never rolls back, sequence")
				require.Equal(t, -time.Second, w.LastFsyncLatency(), "append failure cannot sync")
			}
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, "unchanged", string(data))
		})
	}
}

func TestWriterEventsRegressionConcurrentSequenceAndFields(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	events := writerEventCases()
	const workers, perWorker = 8, 10
	type outcome struct {
		seq  int64
		kind string
		err  error
	}
	results := make(chan outcome, workers*perWorker)
	start := make(chan struct{})
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			<-start
			for i := range perWorker {
				event := events[i%len(events)]
				seq, err := event.write(w)
				results <- outcome{seq, event.fields["type"].(string), err}
			}
		})
	}
	close(start)
	group.Wait()
	close(results)
	assigned := make(map[int64]string)
	for result := range results {
		require.NoError(t, result.err)
		require.NotContains(t, assigned, result.seq)
		assigned[result.seq] = result.kind
	}
	position, err := w.SyncPosition()
	require.NoError(t, err)
	require.EqualValues(t, workers*perWorker, position)
	r, err := NewReader(dir)
	require.NoError(t, err)
	records, err := r.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, workers*perWorker)
	for i, record := range records {
		require.EqualValues(t, i+1, record.Seq)
		require.Equal(t, assigned[record.Seq], record.Type)
		require.Positive(t, record.Timestamp)
	}
}

func writerEventWaitingOnMutex() bool {
	buffer := make([]byte, 1<<20)
	n := runtime.Stack(buffer, true)
	for _, stack := range strings.Split(string(buffer[:n]), "\n\n") {
		if strings.Contains(stack, "wal.(*Writer).") &&
			(strings.Contains(stack, "sync.runtime_SemacquireMutex") || strings.Contains(stack, "sync.(*Mutex).Lock(")) {
			return true
		}
	}
	return false
}

func TestWriterEventsRegressionAssignsIdentityUnderLock(t *testing.T) {
	for _, event := range writerEventCases() {
		t.Run(event.name, func(t *testing.T) {
			dir := t.TempDir()
			w, err := NewWriter(dir)
			require.NoError(t, err)
			defer func() { require.NoError(t, w.Close()) }()
			w.mu.Lock()
			locked := true
			done := make(chan error, 1)
			completed := make(chan struct{})
			defer func() {
				if locked {
					w.mu.Unlock()
				}
				select {
				case <-completed:
				case <-time.After(3 * time.Second):
					t.Error("event goroutine did not join")
				}
			}()
			go func() { defer close(completed); _, err := event.write(w); done <- err }()
			// Establish the actual mutex wait, not a guessed sleep. Advancing the
			// wall clock then makes a timestamp captured before locking observable.
			require.Eventually(t, writerEventWaitingOnMutex, time.Second, time.Millisecond)
			require.Zero(t, w.seq, "sequence assignment must wait for the mutex")
			blockedAt := time.Now().UnixMilli()
			require.Eventually(t, func() bool { return time.Now().UnixMilli() > blockedAt }, time.Second, time.Millisecond)
			unlockedAt := time.Now().UnixMilli()
			locked = false
			w.mu.Unlock()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(3 * time.Second):
				t.Fatal("event did not finish after unlocking")
			}
			data, err := os.ReadFile(filepath.Join(walDirPath(dir), walFileName(1)))
			require.NoError(t, err)
			writerEventAssertJSON(t, data, event.fields, 1, unlockedAt, time.Now().UnixMilli())
		})
	}
}
