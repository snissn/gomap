package raftfsm

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestVerifyInstalledSnapshotManifestV1MatchesLocalFSM(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "fsm:snapshot-install:users")
	if result, err := fsm.ApplyCommittedEntryV1(committedCommand(3, 1, raw)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	manifest, err := fsm.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712345000, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	if err := fsm.VerifyInstalledSnapshotManifestV1(manifest); err != nil {
		t.Fatalf("VerifyInstalledSnapshotManifestV1: %v", err)
	}
}

func TestVerifyInstalledSnapshotManifestV1RejectsMismatches(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "fsm:snapshot-install:mismatch")
	if result, err := fsm.ApplyCommittedEntryV1(committedCommand(4, 1, raw)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	manifest, err := fsm.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712345001, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	tests := []struct {
		name string
		mut  func(*raftcluster.SnapshotManifestV1)
		want string
	}{
		{
			name: "group",
			mut:  func(m *raftcluster.SnapshotManifestV1) { m.GroupID = "other-group" },
			want: "does not match FSM group",
		},
		{
			name: "index",
			mut:  func(m *raftcluster.SnapshotManifestV1) { m.LastIncludedIndex++ },
			want: "does not match durable progress",
		},
		{
			name: "applied-lsn",
			mut:  func(m *raftcluster.SnapshotManifestV1) { m.AppliedCommandLSN++ },
			want: "does not match durable progress",
		},
		{
			name: "digest",
			mut: func(m *raftcluster.SnapshotManifestV1) {
				m.LogicalDigestV1 = strings.Repeat("0", 64)
			},
			want: "does not match FSM digest",
		},
		{
			name: "scope",
			mut:  func(m *raftcluster.SnapshotManifestV1) { m.Scope.CatalogScope = "catalog/other" },
			want: "not valid for FSM scope",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mutated := manifest
			tc.mut(&mutated)
			err := fsm.VerifyInstalledSnapshotManifestV1(mutated)
			if codeOfFSMErr(err) != raftentry.ErrorRejectedConflictV1 || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("VerifyInstalledSnapshotManifestV1 err=%v code=%s, want rejected conflict containing %q", err, codeOfFSMErr(err), tc.want)
			}
		})
	}
}
