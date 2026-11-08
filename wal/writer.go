package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Writer appends records to WAL files with automatic rotation at maxFileSize.
// An fsync is issued after every StepEnd record and after the new event types
// StepRetry, StepTimeout and JobCancelled.
type Writer struct {
	mu           sync.Mutex
	dir          string
	seq          int64
	file         *os.File
	fileNum      int
	fileSize     int64
	fsyncLatency time.Duration
}

// NewWriter opens (or resumes) a WAL in the given directory.
func NewWriter(dir string) (*Writer, error) {
	wdir := walDirPath(dir)
	if err := os.MkdirAll(wdir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}

	files, err := listWALFiles(wdir)
	if err != nil {
		return nil, err
	}

	w := &Writer{dir: dir}

	if len(files) == 0 {
		w.fileNum = 1
	} else {
		lastFile := files[len(files)-1]
		w.fileNum = parseFileNum(lastFile)

		seq, err := lastSeqInDir(wdir, files)
		if err != nil {
			return nil, err
		}
		w.seq = seq

		info, err := os.Stat(filepath.Join(wdir, lastFile))
		if err != nil {
			return nil, err
		}
		w.fileSize = info.Size()
		if w.fileSize >= maxFileSize {
			w.fileNum++
			w.fileSize = 0
		}
	}

	f, err := os.OpenFile(
		filepath.Join(wdir, walFileName(w.fileNum)),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644,
	)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	w.file = f
	return w, nil
}

// append writes a record and rotates the file when necessary.
func (w *Writer) append(r Record) (int64, error) {
	if w.file == nil {
		return 0, fmt.Errorf("wal: writer closed")
	}
	data, err := json.Marshal(r)
	if err != nil {
		return 0, err
	}
	data = append(data, '\n')

	if w.fileSize+int64(len(data)) > maxFileSize {
		if err := w.file.Sync(); err != nil {
			return 0, err
		}
		if err := w.file.Close(); err != nil {
			return 0, err
		}
		w.fileNum++
		w.fileSize = 0
		f, err := os.OpenFile(
			filepath.Join(walDirPath(w.dir), walFileName(w.fileNum)),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644,
		)
		if err != nil {
			return 0, err
		}
		w.file = f
	}

	n, err := w.file.Write(data)
	if err != nil {
		return 0, err
	}
	w.fileSize += int64(n)
	return r.Seq, nil
}

// fsync issues a platform-optimized sync and records the latency.
func (w *Writer) fsync() error {
	start := time.Now()
	if err := platformSync(w.file); err != nil {
		return err
	}
	w.fsyncLatency = time.Since(start)
	return nil
}

// appendRecord keeps identity, write/rotation and optional sync under one lock.
// Errors consume allocated sequence numbers, as successful writes do.
func (w *Writer) appendRecord(r Record, durable bool) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++
	r.Seq = w.seq
	r.Timestamp = time.Now().UnixMilli()
	seq, err := w.append(r)
	if err != nil {
		return 0, err
	}
	if durable {
		if err := w.fsync(); err != nil {
			return 0, err
		}
	}
	return seq, nil
}

// StepBegin records the start of a step.
func (w *Writer) StepBegin(stepID string) (int64, error) {
	return w.appendRecord(Record{
		Type:   TypeStepBegin,
		StepID: stepID,
	}, false)
}

// StepEnd records the end of a step, issues fsync.
func (w *Writer) StepEnd(stepID string, exitCode int, outputRef string) (int64, error) {
	exit := exitCode
	return w.appendRecord(Record{
		Type:      TypeStepEnd,
		StepID:    stepID,
		Exit:      &exit,
		OutputRef: outputRef,
	}, true)
}

// StepRetry records a retry attempt for a step, issues fsync.
func (w *Writer) StepRetry(stepID string, attempt int) (int64, error) {
	return w.appendRecord(Record{
		Type:    TypeStepRetry,
		StepID:  stepID,
		Attempt: attempt,
	}, true)
}

// StepTimeout records that a step timed out, issues fsync.
func (w *Writer) StepTimeout(stepID string) (int64, error) {
	return w.appendRecord(Record{
		Type:   TypeStepTimeout,
		StepID: stepID,
		Reason: "timeout",
	}, true)
}

// JobCancelled records that the entire job was cancelled, issues fsync.
func (w *Writer) JobCancelled(reason string) (int64, error) {
	return w.appendRecord(Record{
		Type:   TypeJobCancelled,
		Reason: reason,
	}, true)
}

// Sync forces a flush of the current WAL file.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	return w.fsync()
}

// Close syncs and closes the WAL file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

// SyncPosition returns the durable high watermark under the same lock as fsync.
func (w *Writer) SyncPosition() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return 0, fmt.Errorf("wal: writer closed")
	}
	if err := w.fsync(); err != nil {
		return 0, err
	}
	return w.seq, nil
}

// LastFsyncLatency returns the duration of the most recent fsync.
func (w *Writer) LastFsyncLatency() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fsyncLatency
}
