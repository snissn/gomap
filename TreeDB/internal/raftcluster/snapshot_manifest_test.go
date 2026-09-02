package raftcluster

import (
	"archive/tar"
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSnapshotManifestV1RoundTripDeterministicJSON(t *testing.T) {
	manifest := validSnapshotManifestV1()
	expectedScope := manifest.Scope

	encoded, err := EncodeSnapshotManifestV1(manifest)
	if err != nil {
		t.Fatalf("EncodeSnapshotManifestV1: %v", err)
	}
	encodedAgain, err := EncodeSnapshotManifestV1(manifest)
	if err != nil {
		t.Fatalf("EncodeSnapshotManifestV1 again: %v", err)
	}
	if !bytes.Equal(encoded, encodedAgain) {
		t.Fatalf("EncodeSnapshotManifestV1 not deterministic:\nfirst:  %s\nsecond: %s", encoded, encodedAgain)
	}
	decoded, err := DecodeSnapshotManifestV1(encoded, expectedScope)
	if err != nil {
		t.Fatalf("DecodeSnapshotManifestV1: %v", err)
	}
	if decoded != manifest {
		t.Fatalf("decoded manifest=%+v, want %+v", decoded, manifest)
	}
	if !strings.Contains(string(encoded), `"logical_digest_v1":"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"`) {
		t.Fatalf("encoded manifest missing logical digest hex: %s", encoded)
	}
}

func TestSnapshotManifestV1ValidationFailsClosed(t *testing.T) {
	expectedScope := validSnapshotManifestV1().Scope
	tests := []struct {
		name string
		mut  func(*SnapshotManifestV1)
	}{
		{
			name: "missing group",
			mut:  func(m *SnapshotManifestV1) { m.GroupID = "" },
		},
		{
			name: "missing node",
			mut:  func(m *SnapshotManifestV1) { m.NodeID = "" },
		},
		{
			name: "missing last included index",
			mut:  func(m *SnapshotManifestV1) { m.LastIncludedIndex = 0 },
		},
		{
			name: "missing last included term",
			mut:  func(m *SnapshotManifestV1) { m.LastIncludedTerm = 0 },
		},
		{
			name: "missing applied command lsn",
			mut:  func(m *SnapshotManifestV1) { m.AppliedCommandLSN = 0 },
		},
		{
			name: "missing logical digest",
			mut:  func(m *SnapshotManifestV1) { m.LogicalDigestV1 = "" },
		},
		{
			name: "short logical digest",
			mut:  func(m *SnapshotManifestV1) { m.LogicalDigestV1 = "abcd" },
		},
		{
			name: "missing scope rule",
			mut:  func(m *SnapshotManifestV1) { m.Scope.ScopeRule = "" },
		},
		{
			name: "mismatched scope",
			mut:  func(m *SnapshotManifestV1) { m.Scope.DatabaseScope = "database/other" },
		},
		{
			name: "missing creation time",
			mut:  func(m *SnapshotManifestV1) { m.CreatedAt = time.Time{} },
		},
		{
			name: "wrong format",
			mut:  func(m *SnapshotManifestV1) { m.Format = "other" },
		},
		{
			name: "wrong version",
			mut:  func(m *SnapshotManifestV1) { m.Version = SnapshotManifestVersion1 + 1 },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			manifest := validSnapshotManifestV1()
			tc.mut(&manifest)
			if err := manifest.Validate(expectedScope); !errors.Is(err, ErrInvalidSnapshotManifest) {
				t.Fatalf("Validate error=%v, want ErrInvalidSnapshotManifest", err)
			}
		})
	}
}

func TestSnapshotManifestV1DecodeRejectsUnknownFields(t *testing.T) {
	manifest := validSnapshotManifestV1()
	encoded, err := EncodeSnapshotManifestV1(manifest)
	if err != nil {
		t.Fatalf("EncodeSnapshotManifestV1: %v", err)
	}
	withUnknown := bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"snapshot_path":"/tmp/snap"`), 1)
	if _, err := DecodeSnapshotManifestV1(withUnknown, manifest.Scope); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("DecodeSnapshotManifestV1 unknown field error=%v, want ErrInvalidSnapshotManifest", err)
	}
}

func TestSnapshotManifestV1DecodeRejectsMissingAppliedCommandLSN(t *testing.T) {
	manifest := validSnapshotManifestV1()
	encoded, err := EncodeSnapshotManifestV1(manifest)
	if err != nil {
		t.Fatalf("EncodeSnapshotManifestV1: %v", err)
	}
	withoutLSN := bytes.Replace(encoded, []byte(`"applied_command_lsn":12,`), nil, 1)
	if bytes.Equal(withoutLSN, encoded) {
		t.Fatalf("test fixture did not remove applied_command_lsn: %s", encoded)
	}
	if _, err := DecodeSnapshotManifestV1(withoutLSN, manifest.Scope); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("DecodeSnapshotManifestV1 missing applied_command_lsn error=%v, want ErrInvalidSnapshotManifest", err)
	}
}

func TestSnapshotManifestV1DecodeRejectsNonCanonicalLogicalDigest(t *testing.T) {
	manifest := validSnapshotManifestV1()
	encoded, err := EncodeSnapshotManifestV1(manifest)
	if err != nil {
		t.Fatalf("EncodeSnapshotManifestV1: %v", err)
	}
	upperDigest := strings.ToUpper(manifest.LogicalDigestV1)
	nonCanonical := bytes.Replace(encoded, []byte(`"logical_digest_v1":"`+manifest.LogicalDigestV1+`"`), []byte(`"logical_digest_v1":"`+upperDigest+`"`), 1)
	if bytes.Equal(nonCanonical, encoded) {
		t.Fatalf("test fixture did not replace logical_digest_v1: %s", encoded)
	}
	if _, err := DecodeSnapshotManifestV1(nonCanonical, manifest.Scope); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("DecodeSnapshotManifestV1 non-canonical digest error=%v, want ErrInvalidSnapshotManifest", err)
	}
}

func TestSnapshotManifestV1DecodeNormalizesZeroExpectedScopeToDefault(t *testing.T) {
	manifest := validSnapshotManifestV1()
	encoded, err := EncodeSnapshotManifestV1(manifest)
	if err != nil {
		t.Fatalf("EncodeSnapshotManifestV1: %v", err)
	}
	if _, err := DecodeSnapshotManifestV1(encoded, SnapshotScopeIdentityV1{}); err != nil {
		t.Fatalf("DecodeSnapshotManifestV1 default scope: %v", err)
	}

	other := manifest
	other.Scope.DatabaseScope = "database/other"
	encodedOther, err := EncodeSnapshotManifestV1(other)
	if err != nil {
		t.Fatalf("EncodeSnapshotManifestV1 other scope: %v", err)
	}
	if _, err := DecodeSnapshotManifestV1(encodedOther, SnapshotScopeIdentityV1{}); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("DecodeSnapshotManifestV1 other scope error=%v, want ErrInvalidSnapshotManifest", err)
	}
}

func TestSnapshotManifestV1DecodeRejectsPaddedManifestScope(t *testing.T) {
	manifest := validSnapshotManifestV1()
	encoded, err := EncodeSnapshotManifestV1(manifest)
	if err != nil {
		t.Fatalf("EncodeSnapshotManifestV1: %v", err)
	}
	padded := bytes.Replace(encoded, []byte(`"database_scope":"database/default"`), []byte(`"database_scope":"database/default "`), 1)
	if bytes.Equal(padded, encoded) {
		t.Fatalf("test fixture did not pad database_scope: %s", encoded)
	}
	if _, err := DecodeSnapshotManifestV1(padded, SnapshotScopeIdentityV1{}); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("DecodeSnapshotManifestV1 padded scope error=%v, want ErrInvalidSnapshotManifest", err)
	}
}

func TestSnapshotManifestV1DecodeRejectsPaddedExpectedScope(t *testing.T) {
	manifest := validSnapshotManifestV1()
	encoded, err := EncodeSnapshotManifestV1(manifest)
	if err != nil {
		t.Fatalf("EncodeSnapshotManifestV1: %v", err)
	}
	expectedScope := manifest.Scope
	expectedScope.DatabaseScope += " "
	if _, err := DecodeSnapshotManifestV1(encoded, expectedScope); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("DecodeSnapshotManifestV1 padded expected scope error=%v, want ErrInvalidSnapshotManifest", err)
	}
}

func TestRaftSnapshotV1ValidateRejectsMismatchedPayloadManifest(t *testing.T) {
	outer := validSnapshotManifestV1()
	embedded := outer
	embedded.LastIncludedIndex++
	snapshot := RaftSnapshotV1{
		Manifest: outer,
		Payload:  validRaftSnapshotArchivePayloadV1(t, embedded),
	}
	if err := snapshot.Validate(); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("Validate mismatched payload manifest error=%v, want ErrInvalidSnapshotManifest", err)
	}
}

func TestRaftSnapshotV1ValidateAcceptsMatchingPayloadManifest(t *testing.T) {
	manifest := validSnapshotManifestV1()
	snapshot := RaftSnapshotV1{
		Manifest: manifest,
		Payload:  validRaftSnapshotArchivePayloadV1(t, manifest),
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("Validate matching payload manifest: %v", err)
	}
}

func TestRaftSnapshotV1ValidateAcceptsMatchingPayloadManifestWithMonotonicCreatedAt(t *testing.T) {
	manifest := validSnapshotManifestV1()
	manifest.CreatedAt = time.Now()
	snapshot := RaftSnapshotV1{
		Manifest: manifest,
		Payload:  validRaftSnapshotArchivePayloadV1(t, manifest),
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("Validate matching payload manifest with monotonic CreatedAt: %v", err)
	}
}

func TestRaftSnapshotV1ValidateAcceptsMatchingArchivePathManifest(t *testing.T) {
	manifest := validSnapshotManifestV1()
	payload := validRaftSnapshotArchivePayloadV1(t, manifest)
	file, err := os.CreateTemp(t.TempDir(), "snapshot-*.tar")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		t.Fatalf("Write payload: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close payload: %v", err)
	}
	snapshot := RaftSnapshotV1{
		Manifest:    manifest,
		ArchivePath: file.Name(),
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("Validate matching archive path manifest: %v", err)
	}
}

func TestRaftSnapshotV1ValidateRejectsMissingArchivePath(t *testing.T) {
	manifest := validSnapshotManifestV1()
	archivePath := writeRaftSnapshotArchiveFileForTest(t, validRaftSnapshotArchivePayloadV1(t, manifest))
	if err := os.Remove(archivePath); err != nil {
		t.Fatalf("Remove archive path: %v", err)
	}
	snapshot := RaftSnapshotV1{
		Manifest:    manifest,
		ArchivePath: archivePath,
	}
	if err := snapshot.Validate(); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("Validate missing archive path error=%v, want ErrInvalidSnapshotManifest", err)
	}
	reader, err := snapshot.OpenArchive()
	if reader != nil {
		_ = reader.Close()
	}
	if !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("OpenArchive missing archive path error=%v, want ErrInvalidSnapshotManifest", err)
	}
}

func TestRaftSnapshotV1ValidateRejectsReplacedArchivePathManifest(t *testing.T) {
	outer := validSnapshotManifestV1()
	replacement := outer
	replacement.LastIncludedIndex++
	archivePath := writeRaftSnapshotArchiveFileForTest(t, validRaftSnapshotArchivePayloadV1(t, outer))
	if err := os.WriteFile(archivePath, validRaftSnapshotArchivePayloadV1(t, replacement), 0o600); err != nil {
		t.Fatalf("Replace archive path: %v", err)
	}
	snapshot := RaftSnapshotV1{
		Manifest:    outer,
		ArchivePath: archivePath,
	}
	if err := snapshot.Validate(); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("Validate replaced archive path manifest error=%v, want ErrInvalidSnapshotManifest", err)
	}
}

func TestRaftSnapshotV1ValidateRejectsAmbiguousArchiveSources(t *testing.T) {
	manifest := validSnapshotManifestV1()
	snapshot := RaftSnapshotV1{
		Manifest:    manifest,
		Payload:     validRaftSnapshotArchivePayloadV1(t, manifest),
		ArchivePath: "snapshot.tar",
	}
	if err := snapshot.Validate(); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("Validate ambiguous archive source error=%v, want ErrInvalidSnapshotManifest", err)
	}
}

func TestDecodeSnapshotManifestV1FromArchiveReaderRejectsOversizedHeader(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	headerSize := int64(RaftSnapshotArchiveHeaderMaxBytes + 1)
	if err := tw.WriteHeader(&tar.Header{
		Name: RaftSnapshotArchiveManifestPathV1,
		Mode: 0o600,
		Size: headerSize,
	}); err != nil {
		t.Fatalf("WriteHeader oversized manifest: %v", err)
	}
	if _, err := tw.Write(bytes.Repeat([]byte("x"), int(headerSize))); err != nil {
		t.Fatalf("Write oversized manifest: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close archive: %v", err)
	}

	if _, err := DecodeSnapshotManifestV1FromArchiveReader(bytes.NewReader(buf.Bytes())); !errors.Is(err, ErrInvalidSnapshotManifest) {
		t.Fatalf("DecodeSnapshotManifestV1FromArchiveReader err=%v want ErrInvalidSnapshotManifest", err)
	}
}

func validSnapshotManifestV1() SnapshotManifestV1 {
	return SnapshotManifestV1{
		Format:            SnapshotManifestFormatV1,
		Version:           SnapshotManifestVersion1,
		GroupID:           "group-a",
		NodeID:            "node-a",
		LastIncludedTerm:  4,
		LastIncludedIndex: 9,
		AppliedCommandLSN: 12,
		LogicalDigestV1:   "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		Scope: SnapshotScopeIdentityV1{
			ScopeRule:     "single-group-v1",
			DatabaseScope: "database/default",
			CatalogScope:  "catalog/default",
		},
		CreatedAt: time.Unix(1700000000, 123).UTC(),
	}
}

func validRaftSnapshotArchivePayloadV1(t *testing.T, manifest SnapshotManifestV1) []byte {
	t.Helper()
	headerBytes, err := EncodeRaftSnapshotArchiveHeaderV1(NewRaftSnapshotArchiveHeaderV1(manifest))
	if err != nil {
		t.Fatalf("EncodeRaftSnapshotArchiveHeaderV1: %v", err)
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{
		Name: RaftSnapshotArchiveManifestPathV1,
		Mode: 0o600,
		Size: int64(len(headerBytes)),
	}); err != nil {
		t.Fatalf("WriteHeader manifest: %v", err)
	}
	if _, err := tw.Write(headerBytes); err != nil {
		t.Fatalf("Write manifest: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close archive: %v", err)
	}
	return buf.Bytes()
}
