package localrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Chao-superw/durarun-go/internal/checkpoint"
	"github.com/Chao-superw/durarun-go/internal/protocol"
	"github.com/Chao-superw/durarun-go/wal"
)

type checkpointConfig struct {
	identity protocol.ExecutionIdentity
	restore  *protocol.DurableCheckpointManifest
	version  string
}

type checkpointPublisher func(context.Context, *checkpoint.StagedArchive, protocol.DurableCheckpointManifest) (*protocol.DurableCheckpointManifest, error)

func startCooperativeCheckpoints(ctx context.Context, cfg checkpointConfig, workDir, staging string, publish checkpointPublisher) (string, func(), error) {
	// Short /tmp path stays under Unix socket sockaddr limits on Darwin/Linux.
	socketDir, err := os.MkdirTemp("/tmp", "dr-")
	if err != nil {
		return "", nil, err
	}
	socket := filepath.Join(socketDir, "cp.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		_ = os.RemoveAll(socketDir)
		return "", nil, err
	}
	if err = os.Chmod(socket, 0600); err != nil {
		_ = listener.Close()
		_ = os.RemoveAll(socketDir)
		return "", nil, err
	}
	serverCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); _ = listener.Close(); wg.Wait(); _ = os.RemoveAll(socketDir) }) }
	sequence := int64(0)
	lastWAL := int64(0)
	if cfg.restore != nil {
		sequence = cfg.restore.Sequence
		lastWAL = cfg.restore.WALPosition
	}
	go func() {
		defer wg.Done()
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			// Serialize checkpoint sequences without holding a runtime/WAL mutex during
			// network traffic. The application's barrier is released after staging.
			handleDurableCheckpoint(serverCtx, conn, cfg, workDir, staging, &sequence, &lastWAL, publish)
			_ = conn.Close()
		}
	}()
	return socket, stop, nil
}

func handleDurableCheckpoint(ctx context.Context, conn net.Conn, cfg checkpointConfig, workDir, staging string, sequence, lastWAL *int64, publish checkpointPublisher) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	encoder := json.NewEncoder(conn)
	fail := func(err error) { _ = encoder.Encode(checkpoint.SafePointResponse{Error: err.Error()}) }
	var request checkpoint.SafePointRequest
	// The connection carries one request, followed by the two server responses.
	decoder := json.NewDecoder(&limitedSocketReader{conn: conn, remaining: 4096})
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		fail(err)
		return
	}
	if request.Version != checkpoint.CooperativeVersion || request.ApplicationVersion != cfg.version || request.WALPosition < *lastWAL {
		fail(fmt.Errorf("invalid cooperative checkpoint request"))
		return
	}
	reader, err := wal.NewReader(workDir)
	if err != nil {
		fail(err)
		return
	}
	walManifest, err := reader.BuildManifest()
	if err != nil {
		fail(err)
		return
	}
	if walManifest.WALTotalRecords != request.WALPosition {
		fail(fmt.Errorf("safe-point WAL watermark differs from synced WAL"))
		return
	}
	snapshot, err := checkpoint.StageWorkspace(workDir, staging, 0, 0)
	if err != nil {
		fail(err)
		return
	}
	defer func() { _ = os.Remove(snapshot.Path) }()
	*sequence++
	manifest := protocol.DurableCheckpointManifest{Identity: cfg.identity, Sequence: *sequence, WALPosition: request.WALPosition, Archive: protocol.BlobRef{Name: "workspace.tar.gz", Key: protocol.BlobKey(cfg.identity, snapshot.SHA256), Size: snapshot.Size, SHA256: snapshot.SHA256}, ApplicationVersion: request.ApplicationVersion, CreatedAt: time.Now().UTC()}
	if err = manifest.Seal(); err != nil {
		fail(err)
		return
	}
	// Snapshot is immutable and fsynced. Only now may the SDK resume mutations.
	if err = encoder.Encode(checkpoint.SafePointResponse{Staged: true}); err != nil {
		return
	}
	var ack checkpoint.SafePointAcknowledgement
	if err = decoder.Decode(&ack); err != nil || ack.Version != checkpoint.CooperativeVersion || !ack.Staged {
		return // SDK may have timed out and resumed before staging completed.
	}
	committed, err := publish(ctx, snapshot, manifest)
	if err != nil {
		fail(err)
		return
	}
	*lastWAL = request.WALPosition
	_ = encoder.Encode(checkpoint.SafePointResponse{Manifest: committed})
}

type limitedSocketReader struct {
	conn      net.Conn
	remaining int
}

func (r *limitedSocketReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, fmt.Errorf("checkpoint request too large")
	}
	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	n, e := r.conn.Read(p)
	r.remaining -= n
	return n, e
}
