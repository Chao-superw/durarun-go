package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

// BuildManifestStrict is an opt-in reader for generation checkpoint preflight.
// It rejects symlinks and special entries, pins directory descriptors, and checks
// each actually opened descriptor before reading it. Both budgets are mandatory;
// callers choose their effective checkpoint limits. All directory entries count
// toward the budgets, but only the legacy WAL filename set contributes records.
//
// No reader goroutine is abandoned on cancellation. O_NONBLOCK prevents a FIFO
// open from waiting for a writer; it cannot interrupt stalled regular filesystem
// I/O. Context checks bound work between synchronous filesystem operations.
func BuildManifestStrict(ctx context.Context, dir string, maxBytes int64, maxFiles int32) (*Manifest, error) {
	if ctx == nil {
		return nil, fmt.Errorf("wal: strict reader context required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxBytes <= 0 || maxBytes == math.MaxInt64 || maxFiles <= 0 || maxFiles == math.MaxInt32 {
		return nil, fmt.Errorf("wal: strict reader requires positive bounded byte and file limits")
	}
	work, err := openStrictWALAt(ctx, unix.AT_FDCWD, dir, unix.O_DIRECTORY)
	if err != nil {
		return nil, err
	}
	defer func() { _ = work.Close() }()
	wdir, err := openStrictWALAt(ctx, int(work.Fd()), walDir, unix.O_DIRECTORY)
	if err != nil {
		// As with NewReader, a missing .wal is an error, not a cold empty WAL.
		return nil, err
	}
	defer func() { _ = wdir.Close() }()
	entries, err := wdir.ReadDir(int(maxFiles) + 1)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(entries) > int(maxFiles) {
		return nil, fmt.Errorf("wal: strict file limit exceeded")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var records []Record
	var totalBytes int64
	walFiles := 0
	for _, entry := range entries {
		name := entry.Name()
		isWAL := strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log")
		recs, size, err := readStrictWALAt(ctx, int(wdir.Fd()), name, isWAL, maxBytes-totalBytes)
		if err != nil {
			return nil, err
		}
		totalBytes += size
		if isWAL {
			walFiles++
			records = append(records, recs...)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateRecordSequence(records); err != nil {
		return nil, err
	}
	manifest := buildManifest(records, walFiles)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return manifest, nil
}

func openStrictWALAt(ctx context.Context, parent int, name string, extraFlags int) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_NOFOLLOW|extraFlags, 0)
	if err != nil {
		return nil, &os.PathError{Op: "strict WAL open", Path: name, Err: err}
	}
	file := os.NewFile(uintptr(fd), name)
	if err := ctx.Err(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func readStrictWALAt(ctx context.Context, parent int, name string, parse bool, remaining int64) ([]Record, int64, error) {
	file, err := openStrictWALAt(ctx, parent, name, 0)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = file.Close() }()
	// This is the same fd that supplies the bytes, not a path-based Lstat check
	// followed by an unchecked open. Non-WAL names are checked too, never followed.
	info, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	if !info.Mode().IsRegular() {
		return nil, 0, fmt.Errorf("wal: strict input %q is not a regular file", name)
	}
	if info.Size() < 0 || info.Size() > remaining {
		return nil, 0, fmt.Errorf("wal: strict byte limit exceeded")
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if !parse {
		return nil, info.Size(), nil
	}
	// Reject concurrent growth or truncation rather than reading forever. The
	// shared JSONL parser sees at most the budgeted size plus one detection byte.
	bounded := &io.LimitedReader{R: &strictWALContextReader{ctx: ctx, input: file}, N: info.Size() + 1}
	records, err := readWALRecords(bounded)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, 0, ctxErr
	}
	if err != nil {
		return nil, 0, err
	}
	if bounded.N != 1 {
		return nil, 0, fmt.Errorf("wal: strict input size changed while reading")
	}
	return records, info.Size(), nil
}

type strictWALContextReader struct {
	ctx   context.Context
	input io.Reader
}

func (r *strictWALContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.input.Read(p)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	return n, err
}
