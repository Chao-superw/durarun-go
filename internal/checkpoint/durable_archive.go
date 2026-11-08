package checkpoint

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const DefaultMaxBytes int64 = 1 << 30
const DefaultMaxFiles int32 = 100000

// StagedArchive is immutable snapshot data, outside the live working generation.
// Its owner removes Path after upload; all digesting is streaming.
type StagedArchive struct {
	Path   string
	Size   int64
	SHA256 string
}

// StageWorkspace requires a cooperative application safe point. It syncs each
// regular file and copies the workspace while mutations remain paused. It never
// treats SIGSTOP, a timer, or a filesystem walk as application consistency.
func StageWorkspace(workDir, stagingDir string, maxBytes int64, maxFiles int32) (_ *StagedArchive, retErr error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	if maxFiles <= 0 {
		maxFiles = DefaultMaxFiles
	}
	workDir, err := filepath.Abs(workDir)
	if err != nil {
		return nil, err
	}
	stagingDir, err = filepath.Abs(stagingDir)
	if err != nil {
		return nil, err
	}
	if stagingDir == workDir || strings.HasPrefix(stagingDir, workDir+string(filepath.Separator)) {
		return nil, fmt.Errorf("checkpoint staging must be outside workspace")
	}
	f, err := os.CreateTemp(stagingDir, "checkpoint-*.tar.gz")
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = errors.Join(retErr, f.Close())
		if retErr != nil {
			_ = os.Remove(f.Name())
		}
	}()
	h := sha256.New()
	bw := &boundedWriter{w: io.MultiWriter(f, h), remaining: maxBytes}
	gz := gzip.NewWriter(bw)
	tw := tar.NewWriter(gz)
	var total int64
	var count int32
	err = filepath.Walk(workDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == workDir {
			return nil
		}
		count++
		if count > maxFiles {
			return fmt.Errorf("checkpoint entry limit exceeded")
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("checkpoint rejects link or special file %s", path)
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		if total > maxBytes {
			return fmt.Errorf("checkpoint expanded size limit exceeded")
		}
		rel, e := filepath.Rel(workDir, path)
		if e != nil {
			return e
		}
		header, e := tar.FileInfoHeader(info, "")
		if e != nil {
			return e
		}
		header.Name = filepath.ToSlash(rel)
		if e = tw.WriteHeader(header); e != nil {
			return e
		}
		if info.IsDir() {
			return nil
		}
		in, e := os.Open(path)
		if e != nil {
			return e
		}
		if e = in.Sync(); e != nil {
			_ = in.Close()
			return e
		}
		n, e := io.CopyN(tw, in, info.Size())
		closeErr := in.Close()
		if e != nil {
			return e
		}
		if n != info.Size() {
			return io.ErrUnexpectedEOF
		}
		return closeErr
	})
	err = errors.Join(err, tw.Close(), gz.Close())
	if err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	if err = f.Chmod(0400); err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &StagedArchive{Path: f.Name(), Size: info.Size(), SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

type boundedWriter struct {
	w         io.Writer
	remaining int64
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, fmt.Errorf("checkpoint archive size limit exceeded")
	}
	n, e := w.w.Write(p)
	w.remaining -= int64(n)
	return n, e
}

// RestoreVerified creates a new work generation under parent, never renaming a
// mounted workspace. No partial generation is returned on any integrity error.
func RestoreVerified(parent string, r io.Reader, size int64, digest string, maxBytes int64, maxFiles int32) (_ string, retErr error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	if maxFiles <= 0 {
		maxFiles = DefaultMaxFiles
	}
	if size < 0 || size > maxBytes || len(digest) != 64 {
		return "", fmt.Errorf("invalid archive size or digest")
	}
	f, err := os.CreateTemp(parent, ".restore-*.tar.gz")
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(r, size+1))
	if err != nil {
		return "", err
	}
	if n != size || hex.EncodeToString(h.Sum(nil)) != digest {
		return "", fmt.Errorf("checkpoint archive size or SHA256 mismatch")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	generation, err := os.MkdirTemp(parent, "work-")
	if err != nil {
		return "", err
	}
	defer func() {
		if retErr != nil {
			_ = os.RemoveAll(generation)
		}
	}()
	if err = extractStrict(generation, f, maxBytes, maxFiles); err != nil {
		return "", err
	}
	return generation, nil
}

func extractStrict(dest string, r io.Reader, maxBytes int64, maxFiles int32) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()
	// Include bounded tar overhead as well as expanded payload. This also bounds
	// padding and metadata discarded internally by archive/tar.
	limit := &io.LimitedReader{R: gz, N: maxBytes + int64(maxFiles)*2048 + (1 << 20)}
	tr := tar.NewReader(limit)
	seen := make(map[string]bool)
	// Header count bounds parsing; materialized count additionally includes every
	// implicit MkdirAll parent. Producers count directories too, so one deep file
	// cannot multiply filesystem objects outside maxFiles. Consumed metadata also
	// uses one unit even though it is not installed into the workspace.
	materialized := make(map[string]bool) // true for directories, false for files
	var objects int32
	reservePaths := func(name string, directory bool) error {
		var pending []string
		for end := 0; end <= len(name); end++ {
			if end < len(name) && name[end] != '/' {
				continue
			}
			prefix := name[:end]
			wantDirectory := end < len(name) || directory
			if previous, exists := materialized[prefix]; exists {
				if previous != wantDirectory {
					return fmt.Errorf("checkpoint path type conflict")
				}
				continue
			}
			if len(pending) >= int(maxFiles-objects) {
				return fmt.Errorf("checkpoint materialized entry limit exceeded")
			}
			pending = append(pending, prefix)
		}
		// Reserve the full set before mkdir: a rejected entry creates no prefixes.
		for _, prefix := range pending {
			materialized[prefix] = prefix != name || directory
		}
		objects += int32(len(pending))
		return nil
	}
	var total int64
	var count int32
	for {
		hdr, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		count++
		if count > maxFiles {
			return fmt.Errorf("checkpoint entry limit exceeded")
		}
		name := strings.TrimSuffix(hdr.Name, "/")
		if !filepath.IsLocal(name) || strings.Contains(name, "\\") || strings.ContainsRune(name, 0) {
			return fmt.Errorf("unsafe checkpoint path %q", hdr.Name)
		}
		for _, part := range strings.Split(name, "/") {
			if part == ".." || part == "." || part == "" {
				return fmt.Errorf("unsafe checkpoint path %q", hdr.Name)
			}
		}
		if seen[name] {
			return fmt.Errorf("duplicate checkpoint path %q", name)
		}
		seen[name] = true
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeDir {
			return fmt.Errorf("checkpoint rejects link or special entry %q", name)
		}
		if hdr.Size < 0 || hdr.Size > maxBytes-total {
			return fmt.Errorf("checkpoint expanded size limit exceeded")
		}
		total += hdr.Size
		if err := reservePaths(name, hdr.Typeflag == tar.TypeDir); err != nil {
			return err
		}
		target := filepath.Join(dest, filepath.FromSlash(name))
		if hdr.Typeflag == tar.TypeDir {
			if hdr.Size != 0 {
				return fmt.Errorf("directory has payload")
			}
			if e = os.MkdirAll(target, 0700); e != nil {
				return e
			}
			continue
		}
		if e = os.MkdirAll(filepath.Dir(target), 0700); e != nil {
			return e
		}
		// Fresh generation + rejected links + O_EXCL prevent traversal/overwrite.
		f, e := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600|os.FileMode(hdr.Mode)&0100)
		if e != nil {
			return e
		}
		_, copyErr := io.CopyN(f, tr, hdr.Size)
		syncErr := f.Sync()
		closeErr := f.Close()
		if e = errors.Join(copyErr, syncErr, closeErr); e != nil {
			return e
		}
	}
	// Consume the gzip trailer to verify its checksum. Only zero tar padding is
	// allowed after the end marker; no second archive can hide after it.
	buf := make([]byte, 32768)
	for {
		n, e := limit.Read(buf)
		for _, b := range buf[:n] {
			if b != 0 {
				return fmt.Errorf("unexpected bytes after tar end")
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
	}
	if limit.N == 0 {
		return fmt.Errorf("checkpoint expanded archive limit exceeded")
	}
	return nil
}
