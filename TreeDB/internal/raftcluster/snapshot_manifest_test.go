package raftcluster

import (
	"bytes"
	"errors"
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
