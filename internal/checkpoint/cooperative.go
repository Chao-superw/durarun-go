package checkpoint

import "github.com/Chao-superw/durarun-go/internal/protocol"

// CooperativeVersion identifies the SDK-to-supervisor safe-point handshake.
// This is not a signal-based snapshot: the caller must quiesce all application
// mutations until Staged is received. The remote commit can finish afterwards.
const CooperativeVersion = "cooperative/v1"

type SafePointRequest struct {
	Version            string `json:"version"`
	WALPosition        int64  `json:"walPosition"`
	ApplicationVersion string `json:"applicationVersion"`
}

type SafePointAcknowledgement struct {
	Version string `json:"version"`
	Staged  bool   `json:"staged"`
}

type SafePointResponse struct {
	Staged   bool                                `json:"staged,omitempty"`
	Manifest *protocol.DurableCheckpointManifest `json:"manifest,omitempty"`
	Error    string                              `json:"error,omitempty"`
}
