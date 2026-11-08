package checkpoint

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func archiveForTest(t *testing.T, headers []*tar.Header, payloads []string) []byte {
	t.Helper()
	var out bytes.Buffer
	gz := gzip.NewWriter(&out)
	tw := tar.NewWriter(gz)
	for i, h := range headers {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if payloads[i] != "" {
			if _, err := tw.Write([]byte(payloads[i])); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func restoreTestArchive(t *testing.T, data []byte, maxBytes int64, maxFiles int32) (string, error) {
	t.Helper()
	h := sha256.Sum256(data)
	return RestoreVerified(t.TempDir(), bytes.NewReader(data), int64(len(data)), hex.EncodeToString(h[:]), maxBytes, maxFiles)
}

func TestDurableArchiveRejectsUnsafeEntries(t *testing.T) {
	cases := []struct {
		name   string
		header tar.Header
	}{
		{"absolute", tar.Header{Name: "/escape", Typeflag: tar.TypeReg}},
		{"traversal", tar.Header{Name: "a/../../escape", Typeflag: tar.TypeReg}},
		{"dot", tar.Header{Name: "a/./escape", Typeflag: tar.TypeReg}},
		{"windows", tar.Header{Name: "a\\escape", Typeflag: tar.TypeReg}},
		{"symlink", tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "inside"}},
		{"hardlink", tar.Header{Name: "link", Typeflag: tar.TypeLink, Linkname: "inside"}},
		{"fifo", tar.Header{Name: "fifo", Typeflag: tar.TypeFifo}},
		{"device", tar.Header{Name: "device", Typeflag: tar.TypeChar}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := archiveForTest(t, []*tar.Header{&tc.header}, []string{""})
			if _, err := restoreTestArchive(t, data, 1<<20, 100); err == nil {
				t.Fatal("unsafe entry accepted")
			}
		})
	}
	duplicate := archiveForTest(t, []*tar.Header{{Name: "same", Typeflag: tar.TypeReg}, {Name: "same", Typeflag: tar.TypeReg}}, []string{"", ""})
	if _, err := restoreTestArchive(t, duplicate, 1<<20, 100); err == nil {
		t.Fatal("duplicate path accepted")
	}
}
func TestDurableArchiveIntegrityAndLimits(t *testing.T) {
	data := archiveForTest(t, []*tar.Header{{Name: "ok", Typeflag: tar.TypeReg, Size: 4096}}, []string{string(make([]byte, 4096))})
	if _, err := restoreTestArchive(t, data, 1024, 100); err == nil {
		t.Fatal("expanded size bomb accepted")
	}
	pair := archiveForTest(t, []*tar.Header{{Name: "a", Typeflag: tar.TypeReg}, {Name: "b", Typeflag: tar.TypeReg}}, []string{"", ""})
	if _, err := restoreTestArchive(t, pair, 1<<20, 1); err == nil {
		t.Fatal("file count limit ignored")
	}
	h := sha256.Sum256(data)
	digest := hex.EncodeToString(h[:])
	if _, err := RestoreVerified(t.TempDir(), bytes.NewReader(data), int64(len(data))+1, digest, 1<<20, 100); err == nil {
		t.Fatal("short payload accepted")
	}
	changed := bytes.Clone(data)
	changed[len(changed)/2] ^= 1
	if _, err := RestoreVerified(t.TempDir(), bytes.NewReader(changed), int64(len(data)), digest, 1<<20, 100); err == nil {
		t.Fatal("hash mismatch accepted")
	}
	// Correct outer SHA does not make a corrupt gzip trailer valid.
	changed = bytes.Clone(data)
	changed[len(changed)-8] ^= 1
	if _, err := restoreTestArchive(t, changed, 1<<20, 100); err == nil {
		t.Fatal("gzip checksum ignored")
	}
}
func TestStageWorkspaceImmutableAndRejectsLinks(t *testing.T) {
	work, stage := t.TempDir(), t.TempDir()
	path := filepath.Join(work, "value")
	if err := os.WriteFile(path, []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := StageWorkspace(work, stage, 1<<20, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("after"), 0600); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(snapshot.Path)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreVerified(t.TempDir(), bytes.NewReader(data), snapshot.Size, snapshot.SHA256, 1<<20, 100)
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(restored, "value"))
	if err != nil || string(original) != "before" {
		t.Fatalf("staged snapshot mutated: %q %v", original, err)
	}
	if err = os.Symlink("value", filepath.Join(work, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err = StageWorkspace(work, stage, 1<<20, 100); err == nil {
		t.Fatal("snapshot link accepted")
	}
	if _, err = StageWorkspace(work, work, 1<<20, 100); err == nil {
		t.Fatal("recursive staging accepted")
	}
}
