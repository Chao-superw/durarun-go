package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const DurableVersion = "durable/v1"
const MaxBlobBytes int64 = 1 << 30

var componentRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)
var hashRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

type ExecutionIdentity struct {
	Namespace      string `json:"namespace"`
	JobUID         string `json:"jobUID"`
	AttemptUID     string `json:"attemptUID"`
	PodUID         string `json:"podUID"`
	Ordinal        int32  `json:"ordinal"`
	ExecutionEpoch int64  `json:"executionEpoch"`
	SpecHash       string `json:"specHash"`
}

func (i ExecutionIdentity) Validate() error {
	for _, c := range []string{i.Namespace, string(i.JobUID), string(i.AttemptUID), string(i.PodUID)} {
		if !componentRE.MatchString(c) {
			return fmt.Errorf("invalid identity component")
		}
	}
	if i.Ordinal < 1 || i.ExecutionEpoch < 1 || !hashRE.MatchString(i.SpecHash) {
		return fmt.Errorf("invalid identity ordinal, epoch or spec hash")
	}
	return nil
}

// Prefix includes every fencing field. No name-only or label-only scope exists.
func (i ExecutionIdentity) Prefix() string {
	return "durable/v1/" + i.Namespace + "/" + string(i.JobUID) + "/" + string(i.AttemptUID) + "/" + string(i.PodUID) + "/" + strconv.FormatInt(int64(i.Ordinal), 10) + "/" + strconv.FormatInt(i.ExecutionEpoch, 10) + "/" + i.SpecHash + "/"
}
func BlobKey(i ExecutionIdentity, digest string) string { return i.Prefix() + "blobs/" + digest }
func ValidateKey(key string) error {
	if len(key) == 0 || len(key) > 1024 || strings.HasPrefix(key, "/") {
		return fmt.Errorf("invalid object key")
	}
	for _, c := range strings.Split(key, "/") {
		if !componentRE.MatchString(c) || c == "." || c == ".." {
			return fmt.Errorf("invalid object key component")
		}
	}
	return nil
}

type BlobRef struct {
	Name   string `json:"name"`
	Key    string `json:"key"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func (r BlobRef) Validate() error {
	if !componentRE.MatchString(r.Name) || r.Name == "." || r.Name == ".." || r.Size < 0 || r.Size > MaxBlobBytes || !hashRE.MatchString(r.SHA256) {
		return fmt.Errorf("invalid blob reference")
	}
	return ValidateKey(r.Key)
}
func (r BlobRef) ValidateFor(i ExecutionIdentity) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Key != BlobKey(i, r.SHA256) {
		return fmt.Errorf("blob outside execution scope")
	}
	return nil
}

type DurableCheckpointManifest struct {
	SchemaVersion      string            `json:"schemaVersion"`
	ContractVersion    string            `json:"contractVersion"`
	Identity           ExecutionIdentity `json:"identity"`
	Sequence           int64             `json:"sequence"`
	Archive            BlobRef           `json:"archive"`
	WALPosition        int64             `json:"walPosition"`
	ApplicationVersion string            `json:"applicationVersion"`
	CreatedAt          time.Time         `json:"createdAt"`
	Digest             string            `json:"digest,omitempty"`
}

// CanonicalDigest uses Go's deterministic struct-field JSON representation (not
// arbitrary JSON text). Callers clear self-digest before passing a typed value.
func CanonicalDigest(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
func (m *DurableCheckpointManifest) Seal() error {
	m.SchemaVersion = DurableVersion
	m.ContractVersion = DurableVersion
	m.Digest = ""
	m.CreatedAt = m.CreatedAt.UTC()
	var err error
	m.Digest, err = CanonicalDigest(*m)
	if err != nil {
		return err
	}
	return m.Validate()
}
func (m DurableCheckpointManifest) Validate() error {
	if m.SchemaVersion != DurableVersion || m.ContractVersion != DurableVersion {
		return fmt.Errorf("unsupported durable contract")
	}
	if err := m.Identity.Validate(); err != nil {
		return err
	}
	if m.Sequence < 1 || m.WALPosition < 0 || m.CreatedAt.IsZero() || m.ApplicationVersion == "" || len(m.ApplicationVersion) > 256 {
		return fmt.Errorf("invalid checkpoint metadata")
	}
	if err := m.Archive.ValidateFor(m.Identity); err != nil {
		return err
	}
	want := m.Digest
	m.Digest = ""
	got, err := CanonicalDigest(m)
	if err != nil {
		return err
	}
	if !hashRE.MatchString(want) || got != want {
		return fmt.Errorf("manifest digest mismatch")
	}
	return nil
}
