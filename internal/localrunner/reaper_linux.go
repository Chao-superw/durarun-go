//go:build linux

package localrunner

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

var reapingEnabled atomic.Bool

func prepareChildReaping() error {
	if os.Getpid() != 1 {
		if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
			return err
		}
	}
	reapingEnabled.Store(true)
	return nil
}

// A container's PID 1 adopts orphaned descendants. Enumerate every thread's
// children because exec and adoption can occur on different Go runtime threads.
// Wait4(-1) would race exec.Cmd.Wait and lose the primary child's exit status.
func reapOrphans(primary int) {
	if os.Getpid() == 1 || reapingEnabled.Load() {
		reapChildren(primary)
	}
}

func reapChildren(primary int) {
	paths, _ := filepath.Glob("/proc/self/task/*/children")
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for _, field := range strings.Fields(string(data)) {
			pid, err := strconv.Atoi(field)
			if err != nil || pid <= 0 || pid == primary {
				continue
			}
			var status syscall.WaitStatus
			_, _ = syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
		}
	}
}
