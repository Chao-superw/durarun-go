package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/Chao-superw/durarun-go/internal/checkpoint"
	"github.com/Chao-superw/durarun-go/internal/protocol"
)

// CheckpointReport is the verified, committed checkpoint identity and watermark.
type CheckpointReport = protocol.DurableCheckpointManifest

type mutationContextKey struct{}

// Mutate participates in the cooperative safe-point barrier for application
// writes outside Step. All writes must be inside Step/Mutate; background writers
// not participating in this API make a checkpoint application-inconsistent.
// Neither Mutate nor Checkpoint may be nested inside another Step/Mutate.
func (rt *Runtime) Mutate(ctx context.Context, fn func(context.Context) error) error {
	if ctx.Value(mutationContextKey{}) != nil {
		return fmt.Errorf("runtime: nested mutation")
	}
	if err := rt.enterMutation(ctx); err != nil {
		return err
	}
	defer rt.leaveMutation()
	return fn(context.WithValue(ctx, mutationContextKey{}, true))
}

// Checkpoint pauses new mutations, waits for existing Steps/Mutate calls, syncs
// the WAL and holds the barrier until the Runner has staged an immutable copy.
// The runner commits the manifest after the barrier is released. Call between steps, not from
// inside a step; this is an explicit application safe point, never a signal.
func (rt *Runtime) Checkpoint(ctx context.Context) (*CheckpointReport, error) {
	if ctx.Value(mutationContextKey{}) != nil {
		return nil, fmt.Errorf("runtime: checkpoint must be between mutations")
	}
	if rt.cfg.CheckpointSocket == "" {
		return nil, fmt.Errorf("runtime: cooperative checkpoint socket not configured")
	}
	timeout := rt.cfg.CheckpointTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	release, err := rt.pauseMutations(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	position, err := rt.walWriter.SyncPosition()
	if err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", rt.cfg.CheckpointSocket)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if err = conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	request := checkpoint.SafePointRequest{Version: checkpoint.CooperativeVersion, WALPosition: position, ApplicationVersion: rt.cfg.ApplicationVersion}
	if err = json.NewEncoder(conn).Encode(request); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(conn)
	var response checkpoint.SafePointResponse
	if err = dec.Decode(&response); err != nil {
		return nil, err
	}
	if response.Error != "" {
		return nil, fmt.Errorf("runtime: stage checkpoint: %s", response.Error)
	}
	if !response.Staged {
		return nil, fmt.Errorf("runtime: invalid checkpoint handshake")
	}
	// An explicit acknowledgement prevents a timed-out SDK from resuming
	// mutations while the server still stages and subsequently commits that copy.
	if err = json.NewEncoder(conn).Encode(checkpoint.SafePointAcknowledgement{Version: checkpoint.CooperativeVersion, Staged: true}); err != nil {
		return nil, err
	}
	release()
	response = checkpoint.SafePointResponse{}
	if err = dec.Decode(&response); err != nil {
		return nil, err
	}
	if response.Error != "" {
		return nil, fmt.Errorf("runtime: commit checkpoint: %s", response.Error)
	}
	if response.Manifest == nil {
		return nil, fmt.Errorf("runtime: missing committed checkpoint")
	}
	if err = response.Manifest.Validate(); err != nil {
		return nil, err
	}
	if response.Manifest.WALPosition != position || response.Manifest.ApplicationVersion != request.ApplicationVersion {
		return nil, fmt.Errorf("runtime: checkpoint watermark/version mismatch")
	}
	rt.stateMu.Lock()
	rt.nextCheckpoint = time.Now().Add(rt.cfg.CheckpointInterval)
	rt.stateMu.Unlock()
	return response.Manifest, nil
}

// checkpointIfDue is called only after a Step releases its mutation barrier.
// There is deliberately no background snapshot timer: an application without a
// safe boundary cannot be checkpointed consistently just because time elapsed.
func (rt *Runtime) checkpointIfDue(ctx context.Context) error {
	if rt.cfg.CheckpointInterval <= 0 || rt.cfg.CheckpointSocket == "" {
		return nil
	}
	// Only the last active Step may initiate automatic quiescence. An inner
	// Step cannot wait for its still-running outer Step (which is awaiting it).
	rt.gateMu.Lock()
	busy := rt.mutations != 0 || rt.paused || rt.closed
	rt.gateMu.Unlock()
	if busy {
		return nil
	}
	rt.stateMu.Lock()
	if rt.checkpointScheduled || time.Now().Before(rt.nextCheckpoint) {
		rt.stateMu.Unlock()
		return nil
	}
	rt.checkpointScheduled = true
	rt.stateMu.Unlock()
	_, err := rt.Checkpoint(ctx)
	rt.stateMu.Lock()
	rt.checkpointScheduled = false
	rt.stateMu.Unlock()
	return err
}

// SafePoint is an alias emphasizing the application's quiescence contract.
func (rt *Runtime) SafePoint(ctx context.Context) (*CheckpointReport, error) {
	return rt.Checkpoint(ctx)
}
func (c *Context) Checkpoint() (*CheckpointReport, error) { return c.runtime.Checkpoint(c.ctx) }
func (c *Context) SafePoint() (*CheckpointReport, error)  { return c.Checkpoint() }

func (rt *Runtime) notifyGate() { close(rt.gateChanged); rt.gateChanged = make(chan struct{}) }
func (rt *Runtime) enterMutation(ctx context.Context) error {
	for {
		rt.gateMu.Lock()
		if rt.closed {
			rt.gateMu.Unlock()
			return fmt.Errorf("runtime: closed")
		}
		if err := ctx.Err(); err != nil {
			rt.gateMu.Unlock()
			return err
		}
		if !rt.paused {
			rt.mutations++
			rt.gateMu.Unlock()
			return nil
		}
		changed := rt.gateChanged
		rt.gateMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (rt *Runtime) leaveMutation() {
	rt.gateMu.Lock()
	rt.mutations--
	rt.notifyGate()
	rt.gateMu.Unlock()
}
func (rt *Runtime) pauseMutations(ctx context.Context) (func(), error) {
	for {
		rt.gateMu.Lock()
		if rt.closed {
			rt.gateMu.Unlock()
			return nil, fmt.Errorf("runtime: closed")
		}
		if !rt.paused {
			rt.paused = true
			rt.gateMu.Unlock()
			break
		}
		changed := rt.gateChanged
		rt.gateMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// release is only called by this checkpoint goroutine, including its defer.
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		rt.gateMu.Lock()
		rt.paused = false
		rt.notifyGate()
		rt.gateMu.Unlock()
	}
	for {
		rt.gateMu.Lock()
		n, closed, changed := rt.mutations, rt.closed, rt.gateChanged
		rt.gateMu.Unlock()
		if closed {
			release()
			return nil, fmt.Errorf("runtime: closed")
		}
		if n == 0 {
			return release, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			release()
			return nil, ctx.Err()
		}
	}
}
