//go:build unix

package localrunner

import (
	"syscall"
)

// signalProcessGroup never targets the caller's own process group.
func signalProcessGroup(pid int, sig syscall.Signal) error {
	if pid <= 1 || pid == syscall.Getpgrp() {
		return syscall.EINVAL
	}
	err := syscall.Kill(-pid, sig)
	if err == syscall.ESRCH {
		return nil
	}
	return err
}

func processGroupExists(pid int) bool {
	err := syscall.Kill(-pid, 0)
	return err == nil || err == syscall.EPERM
}
