package protect

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestIsTimeoutWrappedAndJoined(t *testing.T) {
	ioErr := errors.New("WAL unavailable")
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain", context.DeadlineExceeded, true},
		{"wrapped", fmt.Errorf("step: %w", context.DeadlineExceeded), true},
		{"joined", errors.Join(ioErr, context.DeadlineExceeded), true},
		{"unrelated", ioErr, false},
		{"canceled", errors.Join(ioErr, context.Canceled), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsTimeout(tc.err); got != tc.want {
				t.Fatalf("IsTimeout(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
