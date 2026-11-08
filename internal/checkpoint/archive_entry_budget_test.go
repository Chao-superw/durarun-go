package checkpoint

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRestoreArchiveCountsImplicitDirectories(t *testing.T) {
	data := archiveForTest(t, []*tar.Header{{Name: "a/b/payload", Typeflag: tar.TypeReg, Size: 1}}, []string{"x"})
	destination := t.TempDir()
	err := extractStrict(destination, bytes.NewReader(data), 1<<20, 2)
	require.Error(t, err, "one tar header must not create three filesystem entries under a two-entry limit")
	entries, readErr := os.ReadDir(destination)
	require.NoError(t, readErr)
	require.Empty(t, entries, "over-budget implicit parents must be rejected before mkdir")
	restored, err := restoreTestArchive(t, data, 1<<20, 3)
	require.NoError(t, err, "exact materialized budget remains accepted")
	payload, err := os.ReadFile(filepath.Join(restored, "a/b/payload"))
	require.NoError(t, err)
	require.Equal(t, "x", string(payload))
}

func TestRestoreArchiveSharedAndLateParentsCountOnce(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		headers := []*tar.Header{{Name: "a/b/first", Typeflag: tar.TypeReg}, {Name: "a/b/second", Typeflag: tar.TypeReg}}
		payloads := []string{"", ""}
		if explicit {
			headers = append(headers, &tar.Header{Name: "a", Typeflag: tar.TypeDir}, &tar.Header{Name: "a/b", Typeflag: tar.TypeDir})
			payloads = append(payloads, "", "")
		}
		data := archiveForTest(t, headers, payloads)
		restored, err := restoreTestArchive(t, data, 1<<20, 4)
		require.NoError(t, err, "shared and later explicit parents cannot be charged twice")
		for _, name := range []string{"a/b/first", "a/b/second"} {
			_, err = os.Stat(filepath.Join(restored, name))
			require.NoError(t, err)
		}
		if !explicit {
			_, err = restoreTestArchive(t, data, 1<<20, 3)
			require.Error(t, err, "two headers still materialize four entries")
		}
	}
}
