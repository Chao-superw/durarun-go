package runtime

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Chao-superw/durarun-go/internal/checkpoint"
	"github.com/Chao-superw/durarun-go/internal/protocol"
)

func cooperativeTestListener(t *testing.T) (net.Listener, string) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "dr-sdk-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "cp.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln, path
}

func TestCooperativeBarrierReleasesAfterStagingBeforeCommit(t *testing.T) {
	ln, path := cooperativeTestListener(t)
	rt, err := New(Config{WorkDir: t.TempDir(), CheckpointSocket: path, ApplicationVersion: "v1", CheckpointTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Close() }()
	firstEntered, firstRelease := make(chan struct{}), make(chan struct{})
	mutationDone := make(chan error, 1)
	go func() {
		mutationDone <- rt.Mutate(context.Background(), func(context.Context) error { close(firstEntered); <-firstRelease; return nil })
	}()
	<-firstEntered
	requestSeen, sendStaged, ackSeen, sendCommit := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		conn, e := ln.Accept()
		if e != nil {
			serverDone <- e
			return
		}
		defer func() { _ = conn.Close() }()
		decoder := json.NewDecoder(conn)
		var request checkpoint.SafePointRequest
		if e = decoder.Decode(&request); e != nil {
			serverDone <- e
			return
		}
		close(requestSeen)
		<-sendStaged
		encoder := json.NewEncoder(conn)
		if e = encoder.Encode(checkpoint.SafePointResponse{Staged: true}); e != nil {
			serverDone <- e
			return
		}
		var ack checkpoint.SafePointAcknowledgement
		if e = decoder.Decode(&ack); e != nil {
			serverDone <- e
			return
		}
		close(ackSeen)
		<-sendCommit
		id := protocol.ExecutionIdentity{Namespace: "test", JobUID: "job", AttemptUID: "attempt", PodUID: "pod", Ordinal: 1, ExecutionEpoch: 1, SpecHash: strings.Repeat("a", 64)}
		digest := strings.Repeat("b", 64)
		report := protocol.DurableCheckpointManifest{Identity: id, Sequence: 1, WALPosition: request.WALPosition, ApplicationVersion: request.ApplicationVersion, Archive: protocol.BlobRef{Name: "work", Key: protocol.BlobKey(id, digest), SHA256: digest}, CreatedAt: time.Now()}
		if e = report.Seal(); e != nil {
			serverDone <- e
			return
		}
		serverDone <- encoder.Encode(checkpoint.SafePointResponse{Manifest: &report})
	}()
	checkpointDone := make(chan error, 1)
	go func() { _, e := rt.Checkpoint(context.Background()); checkpointDone <- e }()
	select {
	case <-requestSeen:
		t.Fatal("snapshot started during application mutation")
	case <-time.After(30 * time.Millisecond):
	}
	close(firstRelease)
	if err = <-mutationDone; err != nil {
		t.Fatal(err)
	}
	<-requestSeen
	secondEntered := make(chan struct{})
	go func() {
		mutationDone <- rt.Mutate(context.Background(), func(context.Context) error { close(secondEntered); return nil })
	}()
	select {
	case <-secondEntered:
		t.Fatal("application mutation ran before staging")
	case <-time.After(30 * time.Millisecond):
	}
	close(sendStaged)
	<-ackSeen
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("application stayed blocked during remote commit")
	}
	if err = <-mutationDone; err != nil {
		t.Fatal(err)
	}
	close(sendCommit)
	if err = <-checkpointDone; err != nil {
		t.Fatal(err)
	}
	if err = <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestCooperativeCloseDoesNotDeadlockPendingSnapshot(t *testing.T) {
	ln, path := cooperativeTestListener(t)
	rt, err := New(Config{WorkDir: t.TempDir(), CheckpointSocket: path, ApplicationVersion: "v1", CheckpointTimeout: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan struct{})
	stopServer := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, e := ln.Accept()
		if e != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		var request checkpoint.SafePointRequest
		_ = json.NewDecoder(conn).Decode(&request)
		close(accepted)
		<-stopServer
	}()
	checkpointDone := make(chan error, 1)
	go func() { _, e := rt.Checkpoint(context.Background()); checkpointDone <- e }()
	<-accepted
	closeDone := make(chan error, 1)
	go func() { closeDone <- rt.Close() }()
	select {
	case err = <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Runtime.Close deadlocked")
	}
	select {
	case err = <-checkpointDone:
		if err == nil {
			t.Fatal("uncommitted checkpoint succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("checkpoint ignored cancellation deadline")
	}
	close(stopServer)
	<-serverDone
}

func TestIntervalCheckpointRunsOnlyAfterCompletedStep(t *testing.T) {
	ln, path := cooperativeTestListener(t)
	rt, err := New(Config{WorkDir: t.TempDir(), CheckpointSocket: path, ApplicationVersion: "v1", CheckpointInterval: time.Second, CheckpointTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Close() }()
	rt.nextCheckpoint = time.Now().Add(-time.Second)
	requested := make(chan checkpoint.SafePointRequest, 1)
	go func() {
		conn, e := ln.Accept()
		if e != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		var req checkpoint.SafePointRequest
		if e = json.NewDecoder(conn).Decode(&req); e != nil {
			return
		}
		requested <- req
		// A checkpoint failure must propagate rather than silently ignoring policy.
		_ = json.NewEncoder(conn).Encode(checkpoint.SafePointResponse{Error: "injected stage failure"})
	}()
	err = rt.Run(context.Background(), func(c *Context) error {
		return c.Step("scheduled", func(context.Context) error {
			select {
			case <-requested:
				t.Error("automatic checkpoint ran inside step mutation")
			default:
			}
			return nil
		})
	})
	if err == nil || !strings.Contains(err.Error(), "injected stage failure") {
		t.Fatalf("scheduled checkpoint error was hidden: %v", err)
	}
	select {
	case req := <-requested:
		if req.WALPosition != 2 {
			t.Fatalf("automatic checkpoint lacks completed step: %+v", req)
		}
	case <-time.After(time.Second):
		t.Fatal("due interval did not request a safe-boundary checkpoint")
	}
}

func TestIntervalNestedStepDoesNotWaitForOuterMutation(t *testing.T) {
	rt, err := New(Config{WorkDir: t.TempDir(), CheckpointSocket: "/nonexistent-checkpoint-socket", ApplicationVersion: "v1", CheckpointInterval: time.Second, CheckpointTimeout: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Close() }()
	rt.nextCheckpoint = time.Now().Add(-time.Second)
	innerReturned := false
	err = rt.Run(context.Background(), func(c *Context) error {
		return c.Step("outer", func(context.Context) error {
			if e := c.Step("inner", func(context.Context) error { return nil }); e != nil {
				return e
			}
			innerReturned = true
			return nil
		})
	})
	if !innerReturned {
		t.Fatalf("inner step tried to checkpoint while its outer mutation was active: %v", err)
	}
	if err == nil {
		t.Fatal("outer boundary should report the nonexistent checkpoint socket")
	}
}

func TestCooperativeNestedMutationCheckpointRejected(t *testing.T) {
	rt, err := New(Config{WorkDir: t.TempDir(), CheckpointSocket: "/not-used"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Close() }()
	err = rt.Mutate(context.Background(), func(ctx context.Context) error { _, e := rt.Checkpoint(ctx); return e })
	if err == nil {
		t.Fatal("nested checkpoint was not rejected")
	}
}
