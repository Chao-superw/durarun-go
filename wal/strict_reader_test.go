package wal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestBuildManifestStrictMatchesLegacyAggregationAndTotalLimits(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(dir)
	require.NoError(t, err)
	for i, step := range []string{"success-one", "failed", "success-two"} {
		_, err = writer.StepBegin(step)
		require.NoError(t, err)
		exit := 0
		if i == 1 {
			exit = 23
		}
		_, err = writer.StepEnd(step, exit, "output/"+step)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	first := filepath.Join(walDirPath(dir), walFileName(1))
	data, err := os.ReadFile(first)
	require.NoError(t, err)
	lines := bytes.SplitAfter(data, []byte("\n"))
	// Split the real writer output across two files; exact sequence continuity
	// and completed-step aggregation must remain the legacy implementation's.
	require.NoError(t, os.WriteFile(first, bytes.Join(lines[:3], nil), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(walDirPath(dir), walFileName(2)), bytes.Join(lines[3:], nil), 0600))
	ignored := []byte(`{"seq":999999,"not":"WAL"}`)
	require.NoError(t, os.WriteFile(filepath.Join(walDirPath(dir), "manifest.json"), ignored, 0600))
	budget := int64(len(data) + len(ignored))
	legacy, err := NewReader(dir)
	require.NoError(t, err)
	want, err := legacy.BuildManifest()
	require.NoError(t, err)
	got, err := BuildManifestStrict(context.Background(), dir, budget, 3)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.EqualValues(t, 6, got.WALTotalRecords)
	require.Equal(t, 2, got.WALFileCount, "ordinary non-WAL files cannot count as WAL aliases")
	require.Len(t, got.CompletedSteps, 2)
	require.Equal(t, "success-two", got.LastCompleted)
	_, err = BuildManifestStrict(context.Background(), dir, budget-1, 3)
	require.ErrorContains(t, err, "byte limit")
	_, err = BuildManifestStrict(context.Background(), dir, budget, 2)
	require.ErrorContains(t, err, "file limit")
}

func TestBuildManifestStrictEmptyMissingAndCorrupt(t *testing.T) {
	for _, kind := range []string{"empty", "missing", "gap", "json", "overlong-line"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			if kind != "missing" {
				require.NoError(t, os.Mkdir(walDirPath(dir), 0700))
			}
			var data []byte
			switch kind {
			case "gap":
				var err error
				data, err = json.Marshal(Record{Seq: 2, Type: TypeStepBegin, StepID: "gap"})
				require.NoError(t, err)
			case "json":
				data = []byte("not-json\n")
			case "overlong-line":
				data = bytes.Repeat([]byte("x"), 1<<20+1)
			}
			if data != nil {
				require.NoError(t, os.WriteFile(filepath.Join(walDirPath(dir), walFileName(1)), data, 0600))
			}
			got, err := BuildManifestStrict(context.Background(), dir, 2<<20, 10)
			switch kind {
			case "empty":
				require.NoError(t, err)
				require.Zero(t, got.WALTotalRecords)
				require.Zero(t, got.WALFileCount)
			case "missing":
				require.True(t, os.IsNotExist(err), "%v", err)
			case "gap", "json":
				require.ErrorIs(t, err, ErrCorrupt)
			default:
				require.Error(t, err)
			}
		})
	}
}

func TestBuildManifestStrictRejectsFilesystemShapes(t *testing.T) {
	for _, kind := range []string{"work-link", "wal-link", "wal-fifo", "entry-link", "non-wal-link", "entry-directory", "non-wal-directory"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			wdir := walDirPath(dir)
			switch kind {
			case "work-link":
				link := filepath.Join(t.TempDir(), "workspace")
				require.NoError(t, os.Symlink(dir, link))
				dir = link
			case "wal-link":
				require.NoError(t, os.Symlink(t.TempDir(), wdir))
			case "wal-fifo":
				require.NoError(t, unix.Mkfifo(wdir, 0600))
			default:
				require.NoError(t, os.Mkdir(wdir, 0700))
				name := walFileName(1)
				if kind == "non-wal-link" || kind == "non-wal-directory" {
					name = "manifest.json"
				}
				path := filepath.Join(wdir, name)
				if kind == "entry-directory" || kind == "non-wal-directory" {
					require.NoError(t, os.Mkdir(path, 0700))
				} else {
					target := filepath.Join(t.TempDir(), "regular")
					require.NoError(t, os.WriteFile(target, nil, 0600))
					require.NoError(t, os.Symlink(target, path))
				}
			}
			_, err := BuildManifestStrict(context.Background(), dir, 1024, 10)
			require.Error(t, err)
		})
	}
}

func TestBuildManifestStrictUsesPinnedDirectoryAndOpenedType(t *testing.T) {
	dir := t.TempDir()
	wdir := walDirPath(dir)
	require.NoError(t, os.Mkdir(wdir, 0700))
	name := walFileName(1)
	record, err := json.Marshal(Record{Seq: 1, Type: TypeStepBegin, StepID: "pinned"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(wdir, name), record, 0600))
	root, err := openStrictWALAt(context.Background(), unix.AT_FDCWD, wdir, unix.O_DIRECTORY)
	require.NoError(t, err)
	defer func() { require.NoError(t, root.Close()) }()
	moved := filepath.Join(dir, "moved")
	require.NoError(t, os.Rename(wdir, moved))
	require.NoError(t, os.Symlink(t.TempDir(), wdir))
	got, size, err := readStrictWALAt(context.Background(), int(root.Fd()), name, true, 1024)
	require.NoError(t, err)
	require.EqualValues(t, len(record), size)
	require.Equal(t, "pinned", got[0].StepID, "directory path substitution cannot redirect the pinned descriptor")
	// A previously observed regular entry is replaced with a FIFO before open.
	// The actual descriptor, not this stale path observation, must be checked.
	path := filepath.Join(moved, name)
	info, err := os.Lstat(path)
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
	require.NoError(t, os.Remove(path))
	require.NoError(t, unix.Mkfifo(path, 0600))
	done := make(chan error, 1)
	go func() {
		_, _, err := readStrictWALAt(context.Background(), int(root.Fd()), name, true, 1024)
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "not a regular file")
	case <-time.After(300 * time.Millisecond):
		// Recover an accidentally reintroduced blocking open without leaking it.
		fd, err := unix.Open(path, unix.O_WRONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err == nil {
			_ = unix.Close(fd)
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("test-owned FIFO recovery failed")
		}
		t.Fatal("strict open blocked on a FIFO replacement")
	}
}

type strictWALReadFunc func([]byte) (int, error)

func (f strictWALReadFunc) Read(p []byte) (int, error) { return f(p) }

func TestBuildManifestStrictCancellationAndInvalidLimits(t *testing.T) {
	dir := t.TempDir()
	_, err := BuildManifestStrict(nil, dir, 1, 1) //nolint:staticcheck // Exercise the invalid nil-context boundary before filesystem access.
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = BuildManifestStrict(ctx, dir, 1, 1)
	require.ErrorIs(t, err, context.Canceled)
	for _, limits := range [][2]int64{{0, 1}, {-1, 1}, {1, 0}, {1, -1}} {
		_, err = BuildManifestStrict(context.Background(), dir, limits[0], int32(limits[1]))
		require.ErrorContains(t, err, "limits")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	reader := &strictWALContextReader{ctx: ctx, input: strictWALReadFunc(func(p []byte) (int, error) {
		cancel()
		return copy(p, []byte("data")), io.EOF
	})}
	_, err = reader.Read(make([]byte, 10))
	require.ErrorIs(t, err, context.Canceled, "tail cancellation cannot become a successful read")
	reader.input = strictWALReadFunc(func([]byte) (int, error) { t.Error("cancelled reader reached its input"); return 0, nil })
	_, err = reader.Read(make([]byte, 10))
	require.ErrorIs(t, err, context.Canceled)
}
