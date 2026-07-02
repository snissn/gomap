package raftcluster

import (
	"archive/tar"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const (
	SnapshotManifestFormatV1 = "treedb.raftcluster.snapshot-manifest"
	SnapshotManifestVersion1 = uint16(1)

	RaftSnapshotArchiveFormatV1       = "treedb.raftcluster.raft-snapshot-archive"
	RaftSnapshotArchiveVersion1       = uint16(1)
	RaftSnapshotArchiveManifestPathV1 = "manifest.json"
	RaftSnapshotArchiveHeaderMaxBytes = 1 << 20
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

func (s SnapshotScopeIdentityV1) normalizeExpected() SnapshotScopeIdentityV1 {
	scopeRule := s.ScopeRule
	if scopeRule == "" {
		scopeRule = string(raftentry.ScopeRuleSingleGroupV1)
	}
	databaseScope := s.DatabaseScope
	if databaseScope == "" {
		databaseScope = raftentry.DatabaseScopeDefaultV1
	}
	catalogScope := s.CatalogScope
	if catalogScope == "" {
		catalogScope = raftentry.CatalogScopeDefaultV1
	}
	return SnapshotScopeIdentityV1{
		ScopeRule:     scopeRule,
		DatabaseScope: databaseScope,
		CatalogScope:  catalogScope,
	}
}

func (s SnapshotScopeIdentityV1) validate() error {
	if err := validateScopeIdentityField("scope rule", s.ScopeRule); err != nil {
		return err
	}
	if err := validateScopeIdentityField("database scope", s.DatabaseScope); err != nil {
		return err
	}
	if err := validateScopeIdentityField("catalog scope", s.CatalogScope); err != nil {
		return err
	}
	return nil
}

func validateScopeIdentityField(name, value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		switch name {
		case "scope rule":
			return fmt.Errorf("%w: missing scope rule", ErrInvalidSnapshotManifest)
		case "database scope":
			return fmt.Errorf("%w: missing database scope", ErrInvalidSnapshotManifest)
		case "catalog scope":
			return fmt.Errorf("%w: missing catalog scope", ErrInvalidSnapshotManifest)
		default:
			return fmt.Errorf("%w: missing %s", ErrInvalidSnapshotManifest, name)
		}
	}
	if trimmed != value {
		return fmt.Errorf("%w: non-canonical %s", ErrInvalidSnapshotManifest, name)
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
	if m.Scope != expectedScope.normalizeExpected() {
		return fmt.Errorf("%w: mismatched scope identity", ErrInvalidSnapshotManifest)
	}
	if m.CreatedAt.IsZero() {
		return fmt.Errorf("%w: missing creation time", ErrInvalidSnapshotManifest)
	}
	return nil
}

func EncodeSnapshotManifestV1(manifest SnapshotManifestV1) ([]byte, error) {
	if err := manifest.Validate(manifest.Scope); err != nil {
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
	if hex.EncodeToString(decoded) != digest {
		return fmt.Errorf("%w: non-canonical logical digest", ErrInvalidSnapshotManifest)
	}
	return nil
}

// RaftSnapshotV1 is the production snapshot payload handed to the Raft
// adapter. The manifest identifies the durable apply boundary and logical
// digest. Production snapshots should use ArchivePath so persistence can stream
// bytes from a stable staged source; Payload is retained for small byte-backed
// tests and fixtures.
type RaftSnapshotV1 struct {
	Manifest    SnapshotManifestV1
	Payload     []byte
	ArchivePath string
}

func (s RaftSnapshotV1) Clone() RaftSnapshotV1 {
	s.Payload = bytes.Clone(s.Payload)
	return s
}

func (s RaftSnapshotV1) Validate() error {
	if err := s.Manifest.Validate(s.Manifest.Scope); err != nil {
		return err
	}
	if len(s.Payload) != 0 && s.ArchivePath != "" {
		return fmt.Errorf("%w: snapshot has both payload and archive path", ErrInvalidSnapshotManifest)
	}
	if len(s.Payload) == 0 && s.ArchivePath == "" {
		return fmt.Errorf("%w: empty snapshot archive source", ErrInvalidSnapshotManifest)
	}
	src, err := s.OpenArchive()
	if err != nil {
		return err
	}
	payloadManifest, decodeErr := DecodeSnapshotManifestV1FromArchiveReader(src)
	closeErr := src.Close()
	if decodeErr != nil {
		return decodeErr
	}
	if closeErr != nil {
		return fmt.Errorf("%w: close snapshot archive source: %v", ErrInvalidSnapshotManifest, closeErr)
	}
	if !snapshotManifestV1Equal(payloadManifest, s.Manifest) {
		return fmt.Errorf("%w: snapshot payload manifest does not match outer manifest", ErrInvalidSnapshotManifest)
	}
	return nil
}

func (s RaftSnapshotV1) OpenArchive() (io.ReadCloser, error) {
	if len(s.Payload) != 0 && s.ArchivePath != "" {
		return nil, fmt.Errorf("%w: snapshot has both payload and archive path", ErrInvalidSnapshotManifest)
	}
	if len(s.Payload) != 0 {
		return io.NopCloser(bytes.NewReader(s.Payload)), nil
	}
	if s.ArchivePath == "" {
		return nil, fmt.Errorf("%w: empty snapshot archive source", ErrInvalidSnapshotManifest)
	}
	file, err := os.Open(s.ArchivePath)
	if err != nil {
		return nil, fmt.Errorf("%w: open snapshot archive source: %v", ErrInvalidSnapshotManifest, err)
	}
	return file, nil
}

func (s RaftSnapshotV1) Release() error {
	if s.ArchivePath == "" {
		return nil
	}
	if err := os.Remove(s.ArchivePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func snapshotManifestV1Equal(a, b SnapshotManifestV1) bool {
	return a.Format == b.Format &&
		a.Version == b.Version &&
		a.GroupID == b.GroupID &&
		a.NodeID == b.NodeID &&
		a.LastIncludedTerm == b.LastIncludedTerm &&
		a.LastIncludedIndex == b.LastIncludedIndex &&
		a.AppliedCommandLSN == b.AppliedCommandLSN &&
		a.LogicalDigestV1 == b.LogicalDigestV1 &&
		a.Scope == b.Scope &&
		a.CreatedAt.Equal(b.CreatedAt)
}

// RaftSnapshotExporterV1 is implemented by FSMs that can export a production
// snapshot payload for the HashiCorp Raft adapter.
type RaftSnapshotExporterV1 interface {
	ExportRaftSnapshotV1() (RaftSnapshotV1, error)
}

// RaftSnapshotInstallerV1 is implemented by FSMs that can discard local state
// and install a production snapshot payload from the HashiCorp Raft adapter.
type RaftSnapshotInstallerV1 interface {
	InstallRaftSnapshotV1(io.Reader) error
}

// RaftSnapshotArchiveHeaderV1 is stored as manifest.json at the root of the
// production snapshot archive. The remaining archive layout is producer-owned.
type RaftSnapshotArchiveHeaderV1 struct {
	Format   string             `json:"format"`
	Version  uint16             `json:"version"`
	Manifest SnapshotManifestV1 `json:"manifest"`
}

func NewRaftSnapshotArchiveHeaderV1(manifest SnapshotManifestV1) RaftSnapshotArchiveHeaderV1 {
	return RaftSnapshotArchiveHeaderV1{
		Format:   RaftSnapshotArchiveFormatV1,
		Version:  RaftSnapshotArchiveVersion1,
		Manifest: manifest,
	}
}

func (h RaftSnapshotArchiveHeaderV1) Validate(expectedScope SnapshotScopeIdentityV1) error {
	if h.Format != RaftSnapshotArchiveFormatV1 {
		return fmt.Errorf("%w: unsupported snapshot archive format %q", ErrInvalidSnapshotManifest, h.Format)
	}
	if h.Version != RaftSnapshotArchiveVersion1 {
		return fmt.Errorf("%w: unsupported snapshot archive version %d", ErrInvalidSnapshotManifest, h.Version)
	}
	return h.Manifest.Validate(expectedScope)
}

func EncodeRaftSnapshotArchiveHeaderV1(header RaftSnapshotArchiveHeaderV1) ([]byte, error) {
	if err := header.Validate(header.Manifest.Scope); err != nil {
		return nil, err
	}
	return json.Marshal(header)
}

func DecodeRaftSnapshotArchiveHeaderV1(src []byte, expectedScope SnapshotScopeIdentityV1) (RaftSnapshotArchiveHeaderV1, error) {
	dec := json.NewDecoder(bytes.NewReader(src))
	dec.DisallowUnknownFields()
	var header RaftSnapshotArchiveHeaderV1
	if err := dec.Decode(&header); err != nil {
		return RaftSnapshotArchiveHeaderV1{}, fmt.Errorf("%w: decode snapshot archive header JSON: %v", ErrInvalidSnapshotManifest, err)
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return RaftSnapshotArchiveHeaderV1{}, fmt.Errorf("%w: trailing snapshot archive header JSON content", ErrInvalidSnapshotManifest)
	}
	if expectedScope == (SnapshotScopeIdentityV1{}) {
		expectedScope = header.Manifest.Scope
	}
	if err := header.Validate(expectedScope); err != nil {
		return RaftSnapshotArchiveHeaderV1{}, err
	}
	return header, nil
}

func DecodeSnapshotManifestV1FromArchive(payload []byte) (SnapshotManifestV1, error) {
	return DecodeSnapshotManifestV1FromArchiveReader(bytes.NewReader(payload))
}

func DecodeSnapshotManifestV1FromArchiveReader(reader io.Reader) (SnapshotManifestV1, error) {
	if reader == nil {
		return SnapshotManifestV1{}, fmt.Errorf("%w: nil snapshot archive reader", ErrInvalidSnapshotManifest)
	}
	tr := tar.NewReader(reader)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return SnapshotManifestV1{}, fmt.Errorf("%w: read snapshot archive: %v", ErrInvalidSnapshotManifest, err)
		}
		if header == nil || header.Name != RaftSnapshotArchiveManifestPathV1 {
			continue
		}
		raw, err := readRaftSnapshotArchiveHeaderBytesV1(tr, header.Size)
		if err != nil {
			return SnapshotManifestV1{}, err
		}
		decoded, err := DecodeRaftSnapshotArchiveHeaderV1(raw, SnapshotScopeIdentityV1{})
		if err != nil {
			return SnapshotManifestV1{}, err
		}
		return decoded.Manifest, nil
	}
	return SnapshotManifestV1{}, fmt.Errorf("%w: missing snapshot archive header", ErrInvalidSnapshotManifest)
}

func readRaftSnapshotArchiveHeaderBytesV1(reader io.Reader, size int64) ([]byte, error) {
	if size > RaftSnapshotArchiveHeaderMaxBytes {
		return nil, fmt.Errorf("%w: snapshot archive header is %d bytes, max %d", ErrInvalidSnapshotManifest, size, RaftSnapshotArchiveHeaderMaxBytes)
	}
	raw, err := io.ReadAll(io.LimitReader(reader, RaftSnapshotArchiveHeaderMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read snapshot archive header: %v", ErrInvalidSnapshotManifest, err)
	}
	if len(raw) > RaftSnapshotArchiveHeaderMaxBytes {
		return nil, fmt.Errorf("%w: snapshot archive header exceeds %d bytes", ErrInvalidSnapshotManifest, RaftSnapshotArchiveHeaderMaxBytes)
	}
	return raw, nil
}
