//go:build linux || darwin

package localrunner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Chao-superw/durarun-go/internal/checkpoint"
	"github.com/Chao-superw/durarun-go/internal/protocol"
	"golang.org/x/sys/unix"
)

type localJob struct {
	Argv               []string `json:"argv"`
	ApplicationVersion string   `json:"applicationVersion"`
}

// RunLocal supervises one SDK workload. Each resume extracts the latest
// committed checkpoint into a new workspace under the private run directory.
func RunLocal(ctx context.Context, state string, argv []string, resume bool) (*protocol.ProcessResult, string, error) {
	state, err := filepath.Abs(state)
	if err != nil {
		return nil, "", err
	}
	var job localJob
	if !resume {
		if len(argv) == 0 {
			return nil, "", fmt.Errorf("local run requires a program")
		}
		path, err := exec.LookPath(argv[0])
		if err != nil {
			return nil, "", err
		}
		path, err = filepath.Abs(path)
		if err != nil {
			return nil, "", err
		}
		job = localJob{Argv: append([]string{path}, argv[1:]...), ApplicationVersion: "local/v1"}
		if err := os.Mkdir(state, 0700); err != nil {
			return nil, "", err
		}
	}
	dir, err := os.Open(state)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = dir.Close() }()
	if err := unix.Flock(int(dir.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, "", fmt.Errorf("lock local run: %w", err)
	}
	jobPath := filepath.Join(state, "job.json")
	if resume {
		raw, err := os.ReadFile(jobPath)
		if err != nil {
			return nil, "", err
		}
		if err := json.Unmarshal(raw, &job); err != nil {
			return nil, "", err
		}
		if len(job.Argv) == 0 || !filepath.IsAbs(job.Argv[0]) || job.ApplicationVersion != "local/v1" {
			return nil, "", fmt.Errorf("invalid local job record")
		}
	} else if err := writeLocalRecord(state, jobPath, job); err != nil {
		return nil, "", err
	}

	raw, err := json.Marshal(job)
	if err != nil {
		return nil, "", err
	}
	specHash := fmt.Sprintf("%x", sha256.Sum256(raw))
	jobUID := fmt.Sprintf("%x", sha256.Sum256([]byte(state)))
	ordinal := int32(1)
	manifestPath := filepath.Join(state, "checkpoint.json")
	var selected *protocol.DurableCheckpointManifest
	var work string
	if resume {
		raw, err := os.ReadFile(manifestPath)
		if err != nil {
			return nil, "", fmt.Errorf("read committed checkpoint: %w", err)
		}
		selected = new(protocol.DurableCheckpointManifest)
		if err := json.Unmarshal(raw, selected); err != nil {
			return nil, "", err
		}
		if err := selected.Validate(); err != nil {
			return nil, "", err
		}
		if selected.Identity.Namespace != "local" || selected.Identity.JobUID != jobUID || selected.Identity.SpecHash != specHash ||
			selected.ApplicationVersion != job.ApplicationVersion || selected.Identity.Ordinal == 1<<31-1 {
			return nil, "", fmt.Errorf("checkpoint belongs to a different local job or version")
		}
		ordinal = selected.Identity.Ordinal + 1
		file, err := os.Open(filepath.Join(state, selected.Archive.SHA256+".tar.gz"))
		if err != nil {
			return nil, "", err
		}
		defer func() { _ = file.Close() }()
		work, err = checkpoint.RestoreVerified(state, file, selected.Archive.Size, selected.Archive.SHA256, 0, 0)
		if err != nil {
			return nil, "", fmt.Errorf("restore checkpoint: %w", err)
		}
	} else {
		work, err = os.MkdirTemp(state, "work-")
		if err != nil {
			return nil, "", err
		}
	}
	attempt := filepath.Base(work)
	cfg := checkpointConfig{
		identity: protocol.ExecutionIdentity{Namespace: "local", JobUID: jobUID, AttemptUID: attempt, PodUID: attempt, Ordinal: ordinal, ExecutionEpoch: int64(ordinal), SpecHash: specHash},
		restore:  selected, version: job.ApplicationVersion,
	}
	socket, stop, err := startCooperativeCheckpoints(ctx, cfg, work, state, func(ctx context.Context, snapshot *checkpoint.StagedArchive, manifest protocol.DurableCheckpointManifest) (*protocol.DurableCheckpointManifest, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := os.Rename(snapshot.Path, filepath.Join(state, snapshot.SHA256+".tar.gz")); err != nil {
			return nil, err
		}
		if err := dir.Sync(); err != nil {
			return nil, err
		}
		if err := writeLocalRecord(state, manifestPath, manifest); err != nil {
			return nil, err
		}
		return &manifest, nil
	})
	if err != nil {
		return nil, "", err
	}
	defer stop()
	supervisor := Supervisor{Argv: job.Argv, WorkDir: work, ReapChildren: true,
		Env:                 executionEnvironment(os.Environ(), work, socket, job.ApplicationVersion, resume, 0, ordinal),
		ShutdownGracePeriod: 3 * time.Second}
	result, err := supervisor.Run(ctx)
	return result, work, err
}

func writeLocalRecord(dir, path string, value any) error {
	file, err := os.CreateTemp(dir, ".record-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(file.Name()) }()
	writeErr := json.NewEncoder(file).Encode(value)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	parent, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = parent.Close() }()
	return parent.Sync()
}

func executionEnvironment(env []string, workDir, socket, version string, recovery bool, interval, ordinal int32) []string {
	out := make([]string, 0, len(env)+4)
	for _, v := range env {
		key, _, _ := strings.Cut(v, "=")
		switch key {
		case "DURARUN_CHECKPOINT_INTERVAL_SECONDS", "DURARUN_ATTEMPT_ORDINAL", "DURARUN_DURABLE", "DURARUN_WORK_DIR", "DURARUN_CHECKPOINT_SOCKET", "DURARUN_APPLICATION_VERSION", "DURARUN_RECOVERY":
			continue
		}
		out = append(out, v)
	}
	return append(out, fmt.Sprintf("DURARUN_ATTEMPT_ORDINAL=%d", ordinal), fmt.Sprintf("DURARUN_CHECKPOINT_INTERVAL_SECONDS=%d", interval), "DURARUN_DURABLE=true", "DURARUN_WORK_DIR="+workDir, "DURARUN_CHECKPOINT_SOCKET="+socket, "DURARUN_APPLICATION_VERSION="+version, fmt.Sprintf("DURARUN_RECOVERY=%t", recovery))
}
