//go:build !linux

package localrunner

// Outside Linux, orphan adoption and reaping remain the host init's job.
func reapOrphans(primary int) {}

func prepareChildReaping() error { return nil }
