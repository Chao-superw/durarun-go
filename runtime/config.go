package runtime

import "time"

// Config controls the Runtime behaviour.
type Config struct {
	WorkDir      string        // WAL and working data directory (required)
	RecoveryMode bool          // whether to resume from a previous WAL
	StepTimeout  time.Duration // default per-step timeout (0 = no timeout)
	JobTimeout   time.Duration // job-level timeout (0 = no timeout)
	Observe      ObserveConfig // observability settings
	// Durable opts into runner-provided environment paths and cooperative checkpoints.
	// An explicit CheckpointSocket or DURARUN_DURABLE=true also opts in.
	// Plain runtimes ignore the remaining DURARUN_* environment variables.
	Durable            bool
	CheckpointSocket   string
	ApplicationVersion string
	CheckpointTimeout  time.Duration
	// CheckpointInterval requests checkpoints only at completed Step boundaries.
	// Zero disables automatic scheduling unless the durable Runner supplies it.
	CheckpointInterval time.Duration
}

// ObserveConfig selects trace and metric exporters.
type ObserveConfig struct {
	TraceExporter   string // "stdout", "noop", ""
	MetricsExporter string // "stdout", "noop", ""
}
