package raftfsm

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestSnapshotManifestExportBuildsFromDurableProgressAndDigest(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "fsm:snapshot-manifest:create-users")
	result, err := fsm.ApplyCommittedEntryV1(committedCommand(7, 1, raw))
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	progress, ok := fsm.progress.LastAppliedRecord()
	if !ok {
		t.Fatal("LastAppliedRecord missing after apply")
	}
	createdAt := time.Unix(1712345678, 900).UTC()
	manifest, err := fsm.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{CreatedAt: createdAt})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	digest, err := fsm.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1: %v", err)
	}
	if manifest.Format != raftcluster.SnapshotManifestFormatV1 ||
		manifest.Version != raftcluster.SnapshotManifestVersion1 ||
		manifest.GroupID != "default" ||
		manifest.NodeID != "node-a" ||
		manifest.LastIncludedTerm != progress.EntryID.Term ||
		manifest.LastIncludedIndex != progress.EntryID.Index ||
		manifest.AppliedCommandLSN != progress.AppliedCommandLSN ||
		manifest.LogicalDigestV1 != digest.Hex() ||
		manifest.CreatedAt != createdAt {
		t.Fatalf("manifest=%+v progress=%+v digest=%s", manifest, progress, digest.Hex())
	}
	wantScope := raftcluster.SnapshotScopeIdentityV1{
		ScopeRule:     string(raftentry.ScopeRuleSingleGroupV1),
		DatabaseScope: raftentry.DatabaseScopeDefaultV1,
		CatalogScope:  raftentry.CatalogScopeDefaultV1,
	}
	if manifest.Scope != wantScope {
		t.Fatalf("manifest scope=%+v, want %+v", manifest.Scope, wantScope)
	}
	encoded, err := raftcluster.EncodeSnapshotManifestV1(manifest)
	if err != nil {
		t.Fatalf("EncodeSnapshotManifestV1: %v", err)
	}
	decoded, err := raftcluster.DecodeSnapshotManifestV1(encoded, wantScope)
	if err != nil {
		t.Fatalf("DecodeSnapshotManifestV1: %v", err)
	}
	if decoded != manifest {
		t.Fatalf("decoded manifest=%+v, want %+v", decoded, manifest)
	}
}

func TestSnapshotManifestExportRefusesNilClosedAndMissingProgress(t *testing.T) {
	var nilFSM *FSM
	if _, err := nilFSM.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{}); codeOfFSMErr(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("nil ExportSnapshotManifestV1 error=%v, want unsafe durability", err)
	}

	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	if _, err := fsm.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{}); codeOfFSMErr(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("missing-progress ExportSnapshotManifestV1 error=%v, want unsafe durability", err)
	}
	if err := fsm.Close(); err != nil {
		t.Fatalf("Close FSM: %v", err)
	}
	if _, err := fsm.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{}); codeOfFSMErr(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("closed ExportSnapshotManifestV1 error=%v, want unsafe durability", err)
	}
}

func TestSnapshotManifestExportRejectsProgressBehindLocalCoverage(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	fsm := openFSMForTest(t, db, dbDir)

	users := deterministicCreateCollectionEntry(t, "users", "fsm:snapshot-manifest:users")
	if result, err := fsm.ApplyCommittedEntryV1(committedCommand(1, 1, users)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1 users: %v result=%+v", err, result)
	}
	firstProgress, ok := fsm.progress.LastAppliedRecord()
	if !ok {
		t.Fatal("LastAppliedRecord missing after first apply")
	}
	orders := deterministicCreateCollectionEntry(t, "orders", "fsm:snapshot-manifest:orders")
	if result, err := fsm.ApplyCommittedEntryV1(committedCommand(1, 2, orders)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1 orders: %v result=%+v", err, result)
	}
	coveredLSN := db.State().AppliedCommandLSN
	if coveredLSN <= firstProgress.AppliedCommandLSN {
		t.Fatalf("AppliedCommandLSN after second apply=%d, want greater than first progress %d", coveredLSN, firstProgress.AppliedCommandLSN)
	}
	progressPath := raftapply.DurableApplyProgressStorePath(fsm.metadataDir)
	if err := fsm.Close(); err != nil {
		t.Fatalf("Close FSM: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close DB: %v", err)
	}
	if err := os.Remove(progressPath); err != nil {
		t.Fatalf("Remove progress metadata: %v", err)
	}
	progress, err := raftapply.OpenDurableApplyProgressStoreFile(progressPath, raftapply.DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("Open rolled-back progress store: %v", err)
	}
	if err := progress.RecordApplied(firstProgress); err != nil {
		t.Fatalf("Record rolled-back progress: %v", err)
	}
	if err := progress.Close(); err != nil {
		t.Fatalf("Close rolled-back progress store: %v", err)
	}

	reopenedDB := openFSMTestDB(t, dbDir)
	defer func() { _ = reopenedDB.Close() }()
	if got := reopenedDB.State().AppliedCommandLSN; got != coveredLSN {
		t.Fatalf("reopened AppliedCommandLSN=%d, want %d", got, coveredLSN)
	}
	reopenedFSM, openErr := Open(Options{
		DB:      reopenedDB,
		Cluster: validFSMClusterConfig(dbDir),
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if openErr != nil {
		t.Fatalf("Open with progress behind local coverage and stored result: %v", openErr)
	}
	defer func() { _ = reopenedFSM.Close() }()
	if _, err := reopenedFSM.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{}); codeOfFSMErr(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("ExportSnapshotManifestV1 lagging progress error=%v, want unsafe durability", err)
	}
}

func codeOfFSMErr(err error) raftentry.DeterministicErrorCodeV1 {
	code, _ := ErrorCodeOf(err)
	return code
}
