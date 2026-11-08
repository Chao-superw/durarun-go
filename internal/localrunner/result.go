package localrunner

import (
	"errors"
	"os"
	"os/exec"
	"syscall"

	"github.com/Chao-superw/durarun-go/internal/protocol"
)

const (
	ExitUsage    = 2
	ExitInternal = 125
	ExitNoExec   = 126
	ExitNotFound = 127
)

// RunError identifies a runner failure separately from the user's result.
type RunError struct {
	ExitCode int
	Err      error
}

func (e *RunError) Error() string { return e.Err.Error() }
func (e *RunError) Unwrap() error { return e.Err }

// ExitToResult converts Wait's outcome without classifying a user's nonzero
// exit as a runner error. Signals use the shell-compatible 128+signal code.
func ExitToResult(err error) *protocol.ProcessResult {
	if err == nil {
		return &protocol.ProcessResult{ExitCode: 0}
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result := &protocol.ProcessResult{ExitCode: exitErr.ExitCode()}
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			result.Signal = int(status.Signal())
			result.ExitCode = 128 + result.Signal
		}
		return result
	}
	return &protocol.ProcessResult{ExitCode: ExitInternal, Error: err.Error()}
}

func launchExitCode(err error) int {
	if errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
		return ExitNotFound
	}
	return ExitNoExec
}
