//go:build linux

package localrunner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Run the subreaper test in its own process; never adopt or reap children
// belonging to the go test process or another test.
func TestLinuxReaperAdoptsOrphansWithoutStealingPrimary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReaperProcess$")
	cmd.Env = append(os.Environ(), "RUNNER_REAPER_PROCESS=supervisor", "RUNNER_REAPER_DIR="+t.TempDir(), "GORACE=atexit_sleep_ms=0")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("isolated reaper: %v\n%s", err, output)
	}
}

func TestReaperProcess(t *testing.T) {
	mode := os.Getenv("RUNNER_REAPER_PROCESS")
	if mode == "" {
		return
	}
	dir := os.Getenv("RUNNER_REAPER_DIR")
	pidPath := filepath.Join(dir, "orphan")
	start := func(mode string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestReaperProcess$")
		cmd.Env = append(os.Environ(), "RUNNER_REAPER_PROCESS="+mode)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd
	}
	switch mode {
	case "orphan":
		if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		os.Exit(0)
	case "primary":
		start("orphan")
		os.Exit(7)
	case "supervisor":
		if err := prepareChildReaping(); err != nil {
			t.Fatal(err)
		}
		primary := start("primary")
		waited := false
		defer func() {
			if waited {
				return
			}
			if err := primary.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("kill primary during cleanup: %v", err)
			}
			var exitErr *exec.ExitError
			if err := primary.Wait(); err != nil && !errors.As(err, &exitErr) {
				t.Errorf("wait primary during cleanup: %v", err)
			}
		}()
		deadline := time.Now().Add(5 * time.Second)
		orphan := 0
		for time.Now().Before(deadline) {
			reapOrphans(primary.Process.Pid)
			if orphan == 0 {
				data, _ := os.ReadFile(pidPath)
				orphan, _ = strconv.Atoi(string(data))
			}
			status, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", primary.Process.Pid))
			if orphan > 0 && syscall.Kill(orphan, 0) == syscall.ESRCH && strings.Contains(string(status), "State:\tZ") {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if orphan == 0 || syscall.Kill(orphan, 0) != syscall.ESRCH {
			t.Fatalf("orphan %d was not reaped", orphan)
		}
		result := ExitToResult(primary.Wait())
		waited = true
		if result.ExitCode != 7 || result.Error != "" {
			t.Fatalf("reaper stole primary exit status: %+v", result)
		}
	}
}
