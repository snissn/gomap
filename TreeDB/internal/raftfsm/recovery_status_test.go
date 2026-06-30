package raftfsm

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestRecoveryStatusV1BeforeSnapshotInstallIsUnsafe(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	status, err := fsm.RecoveryStatusV1(context.Background(), RecoveryStatusOptionsV1{
		TailTargetIndex:   1,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1: %v", err)
	}
	if status.Readiness != raftcluster.RecoveryReadinessUnsafeNoSnapshotV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateNoneV1 ||
		status.TailState != raftcluster.RecoveryTailStateNoSnapshotV1 ||
		status.ReadSafetyState != raftcluster.RecoveryReadSafetyAppliedIndexLaggingV1 ||
		status.SafeToServeReads {
		t.Fatalf("status=%+v, want unsafe no-snapshot with lagging applied-index read safety", status)
	}
}

func TestRecoveryStatusV1TracksSnapshotTailAndAppliedIndexReadiness(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	users := deterministicCreateCollectionEntry(t, "users", "fsm:recovery-status:users")
	if result, err := fsm.ApplyCommittedEntryV1(committedCommand(9, 1, users)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1 users: %v result=%+v", err, result)
	}
	manifest, err := fsm.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712347000, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}

	status, err := fsm.RecoveryStatusV1(context.Background(), RecoveryStatusOptionsV1{
		SnapshotManifest:  &manifest,
		TailTargetIndex:   2,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 pending tail: %v", err)
	}
	if status.Readiness != raftcluster.RecoveryReadinessTailPendingV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateManifestVerifiedV1 ||
		status.TailState != raftcluster.RecoveryTailStatePendingV1 ||
		status.TailLagEntries != 1 ||
		status.ReadSafetyState != raftcluster.RecoveryReadSafetyAppliedIndexLaggingV1 ||
		status.RequiredAppliedIndex != 2 ||
		status.SafeToServeReads {
		t.Fatalf("pending status=%+v", status)
	}

	orders := deterministicCreateCollectionEntry(t, "orders", "fsm:recovery-status:orders")
	if result, err := fsm.ApplyCommittedEntryV1(committedCommand(9, 2, orders)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1 orders: %v result=%+v", err, result)
	}
	status, err = fsm.RecoveryStatusV1(context.Background(), RecoveryStatusOptionsV1{
		SnapshotManifest:  &manifest,
		TailTargetIndex:   2,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 tail complete: %v", err)
	}
	if status.Readiness != raftcluster.RecoveryReadinessReadyAppliedIndexV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateManifestVerifiedV1 ||
		status.TailState != raftcluster.RecoveryTailStateCompleteV1 ||
		status.TailLagEntries != 0 ||
		status.ReadSafetyState != raftcluster.RecoveryReadSafetyAppliedIndexSatisfiedV1 ||
		!status.SafeToServeReads {
		t.Fatalf("tail-complete status=%+v", status)
	}
	if status.SnapshotManifest == nil || status.SnapshotManifest.LogicalDigestV1 != manifest.LogicalDigestV1 {
		t.Fatalf("status manifest=%+v, want copied manifest digest %s", status.SnapshotManifest, manifest.LogicalDigestV1)
	}
}

func TestRecoveryStatusV1VerifiesDuplicateSnapshotBoundaryLogicalDigest(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	users := deterministicCreateCollectionEntry(t, "users", "fsm:recovery-status:duplicate-users")
	if result, err := fsm.ApplyCommittedEntryV1(committedCommand(9, 1, users)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1 users: %v result=%+v", err, result)
	}
	orders := deterministicCreateCollectionEntry(t, "orders", "fsm:recovery-status:orders-before-duplicate")
	if result, err := fsm.ApplyCommittedEntryV1(committedCommand(9, 2, orders)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1 orders: %v result=%+v", err, result)
	}
	duplicate, err := fsm.ApplyCommittedEntryV1(committedCommand(9, 3, users))
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 duplicate users: %v result=%+v", err, duplicate)
	}
	manifest, err := fsm.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712347002, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	if duplicate.ResultDigest.Hex() == manifest.LogicalDigestV1 {
		t.Fatalf("duplicate result digest unexpectedly equals manifest digest %s", manifest.LogicalDigestV1)
	}

	audits := deterministicCreateCollectionEntry(t, "audits", "fsm:recovery-status:tail-after-duplicate")
	if result, err := fsm.ApplyCommittedEntryV1(committedCommand(9, 4, audits)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1 audits: %v result=%+v", err, result)
	}
	status, err := fsm.RecoveryStatusV1(context.Background(), RecoveryStatusOptionsV1{
		SnapshotManifest:  &manifest,
		TailTargetIndex:   4,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 duplicate boundary: %v", err)
	}
	if status.Readiness != raftcluster.RecoveryReadinessReadyAppliedIndexV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateManifestVerifiedV1 ||
		status.TailState != raftcluster.RecoveryTailStateCompleteV1 ||
		status.ReadSafetyState != raftcluster.RecoveryReadSafetyAppliedIndexSatisfiedV1 ||
		!status.SafeToServeReads {
		t.Fatalf("status=%+v, want verified duplicate snapshot boundary with complete tail and safe reads", status)
	}
}

func TestRecoveryStatusV1RejectsLocalCoverageBeyondDurableProgress(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	users := deterministicCreateCollectionEntry(t, "users", "fsm:recovery-status:coverage-users")
	if result, err := fsm.ApplyCommittedEntryV1(committedCommand(9, 1, users)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1 users: %v result=%+v", err, result)
	}
	manifest, err := fsm.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712347003, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	progressBefore, ok, err := fsm.lastAppliedProgressRecord()
	if err != nil {
		t.Fatalf("lastAppliedProgressRecord before uncovered apply: %v", err)
	}
	if !ok {
		t.Fatal("missing progress after first apply")
	}

	ordersEntry := committedCommand(9, 2, deterministicCreateCollectionEntry(t, "orders", "fsm:recovery-status:coverage-orders"))
	ordersID := raftentry.ApplyEntryID{Term: ordersEntry.Term, Index: ordersEntry.Index}
	meta, err := fsm.applyMetadata(ordersEntry, ordersID)
	if err != nil {
		t.Fatalf("applyMetadata orders: %v", err)
	}
	if result, err := raftapply.ApplyCommittedEntryV1(db, ordersEntry.Bytes, meta, raftapply.Options{DecodeLimits: fsm.decodeLimits}); err != nil {
		t.Fatalf("lower ApplyCommittedEntryV1 orders: %v result=%+v", err, result)
	}
	if got := db.State().AppliedCommandLSN; got <= progressBefore.AppliedCommandLSN {
		t.Fatalf("AppliedCommandLSN after uncovered apply=%d, want greater than durable progress %d", got, progressBefore.AppliedCommandLSN)
	}
	progressAfter, ok, err := fsm.lastAppliedProgressRecord()
	if err != nil {
		t.Fatalf("lastAppliedProgressRecord after uncovered apply: %v", err)
	}
	if !ok || progressAfter != progressBefore {
		t.Fatalf("progress after uncovered apply=%+v ok=%v, want unchanged %+v", progressAfter, ok, progressBefore)
	}

	status, err := fsm.RecoveryStatusV1(context.Background(), RecoveryStatusOptionsV1{
		SnapshotManifest:  &manifest,
		TailTargetIndex:   1,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 uncovered coverage: %v", err)
	}
	if status.Readiness != raftcluster.RecoveryReadinessUnsafeManifestUnverifiedV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateManifestRejectedV1 ||
		status.SafeToServeReads ||
		len(status.Errors) == 0 {
		t.Fatalf("status=%+v, want unsafe rejected manifest after uncovered local coverage", status)
	}
	if !strings.Contains(strings.Join(status.Errors, "\n"), "does not match durable progress coverage") {
		t.Fatalf("status errors=%v, want durable progress coverage mismatch", status.Errors)
	}
}

func TestRecoveryStatusV1MismatchedManifestRemainsUnsafe(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "fsm:recovery-status:mismatch")
	if result, err := fsm.ApplyCommittedEntryV1(committedCommand(10, 1, raw)); err != nil {
		t.Fatalf("ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	manifest, err := fsm.ExportSnapshotManifestV1(SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712347001, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	manifest.LogicalDigestV1 = strings.Repeat("0", 64)

	status, err := fsm.RecoveryStatusV1(context.Background(), RecoveryStatusOptionsV1{
		SnapshotManifest:  &manifest,
		TailTargetIndex:   1,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 mismatched manifest: %v", err)
	}
	if status.Readiness != raftcluster.RecoveryReadinessUnsafeManifestUnverifiedV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateManifestRejectedV1 ||
		status.SafeToServeReads ||
		len(status.Errors) == 0 {
		t.Fatalf("status=%+v, want unsafe rejected manifest with error detail", status)
	}
}
