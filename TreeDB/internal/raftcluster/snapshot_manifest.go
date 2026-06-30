package raftcluster

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	SnapshotManifestFormatV1 = "treedb.raftcluster.snapshot-manifest"
	SnapshotManifestVersion1 = uint16(1)
)

var ErrInvalidSnapshotManifest = errors.New("raftcluster: invalid snapshot manifest")

// SnapshotScopeIdentityV1 is the logical state scope covered by a snapshot
// manifest. It must match the receiver's expected scope before any later
// snapshot install/export integration may trust the metadata.
type SnapshotScopeIdentityV1 struct {
	ScopeRule     string `json:"scope_rule"`
	DatabaseScope string `json:"database_scope"`
	CatalogScope  string `json:"catalog_scope"`
}

func (s SnapshotScopeIdentityV1) isZero() bool {
	return strings.TrimSpace(s.ScopeRule) == "" &&
		strings.TrimSpace(s.DatabaseScope) == "" &&
		strings.TrimSpace(s.CatalogScope) == ""
}

func (s SnapshotScopeIdentityV1) validate() error {
	if strings.TrimSpace(s.ScopeRule) == "" {
		return fmt.Errorf("%w: missing scope rule", ErrInvalidSnapshotManifest)
	}
	if strings.TrimSpace(s.DatabaseScope) == "" {
		return fmt.Errorf("%w: missing database scope", ErrInvalidSnapshotManifest)
	}
	if strings.TrimSpace(s.CatalogScope) == "" {
		return fmt.Errorf("%w: missing catalog scope", ErrInvalidSnapshotManifest)
	}
	return nil
}

// SnapshotManifestV1 is a metadata-only export contract. It describes the
// durable local FSM point and logical digest only; it does not copy snapshot
// files, install state into another DB, replay tails, truncate Raft logs,
// rejoin nodes, or claim that the exported metadata can serve reads.
type SnapshotManifestV1 struct {
	Format            string                  `json:"format"`
	Version           uint16                  `json:"version"`
	GroupID           GroupID                 `json:"group_id"`
	NodeID            NodeID                  `json:"node_id"`
	LastIncludedTerm  uint64                  `json:"last_included_term"`
	LastIncludedIndex uint64                  `json:"last_included_index"`
	AppliedCommandLSN uint64                  `json:"applied_command_lsn"`
	LogicalDigestV1   string                  `json:"logical_digest_v1"`
	Scope             SnapshotScopeIdentityV1 `json:"scope"`
	CreatedAt         time.Time               `json:"created_at"`
}

func (m SnapshotManifestV1) Validate(expectedScope SnapshotScopeIdentityV1) error {
	if m.Format != SnapshotManifestFormatV1 {
		return fmt.Errorf("%w: unsupported format %q", ErrInvalidSnapshotManifest, m.Format)
	}
	if m.Version != SnapshotManifestVersion1 {
		return fmt.Errorf("%w: unsupported version %d", ErrInvalidSnapshotManifest, m.Version)
	}
	if err := validateID("group id", string(m.GroupID)); err != nil {
		return errors.Join(ErrInvalidSnapshotManifest, ErrMissingGroupID, err)
	}
	if err := validateID("node id", string(m.NodeID)); err != nil {
		return errors.Join(ErrInvalidSnapshotManifest, ErrMissingNodeID, err)
	}
	if m.LastIncludedIndex == 0 {
		return fmt.Errorf("%w: missing last included index", ErrInvalidSnapshotManifest)
	}
	if m.LastIncludedTerm == 0 {
		return fmt.Errorf("%w: missing last included term", ErrInvalidSnapshotManifest)
	}
	if m.AppliedCommandLSN == 0 {
		return fmt.Errorf("%w: missing applied command LSN", ErrInvalidSnapshotManifest)
	}
	if err := validateLogicalDigestV1Hex(m.LogicalDigestV1); err != nil {
		return err
	}
	if err := m.Scope.validate(); err != nil {
		return err
	}
	if !expectedScope.isZero() && m.Scope != expectedScope {
		return fmt.Errorf("%w: mismatched scope identity", ErrInvalidSnapshotManifest)
	}
	if m.CreatedAt.IsZero() {
		return fmt.Errorf("%w: missing creation time", ErrInvalidSnapshotManifest)
	}
	return nil
}

func EncodeSnapshotManifestV1(manifest SnapshotManifestV1) ([]byte, error) {
	if err := manifest.Validate(SnapshotScopeIdentityV1{}); err != nil {
		return nil, err
	}
	return json.Marshal(manifest)
}

func DecodeSnapshotManifestV1(src []byte, expectedScope SnapshotScopeIdentityV1) (SnapshotManifestV1, error) {
	dec := json.NewDecoder(bytes.NewReader(src))
	dec.DisallowUnknownFields()
	var manifest SnapshotManifestV1
	if err := dec.Decode(&manifest); err != nil {
		return SnapshotManifestV1{}, fmt.Errorf("%w: decode JSON: %v", ErrInvalidSnapshotManifest, err)
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return SnapshotManifestV1{}, fmt.Errorf("%w: trailing JSON content", ErrInvalidSnapshotManifest)
	}
	if err := manifest.Validate(expectedScope); err != nil {
		return SnapshotManifestV1{}, err
	}
	return manifest, nil
}

func validateLogicalDigestV1Hex(digest string) error {
	if strings.TrimSpace(digest) == "" {
		return fmt.Errorf("%w: missing logical digest", ErrInvalidSnapshotManifest)
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil {
		return fmt.Errorf("%w: invalid logical digest: %v", ErrInvalidSnapshotManifest, err)
	}
	if len(decoded) != 32 {
		return fmt.Errorf("%w: logical digest has %d bytes, want 32", ErrInvalidSnapshotManifest, len(decoded))
	}
	return nil
}
