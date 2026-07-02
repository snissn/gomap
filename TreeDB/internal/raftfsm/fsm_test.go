package raftfsm

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	testCatalogVersionStart        = 7
	deterministicCollectionRefName = 1
)

func TestSingleGroupApplyLoopCloseReopenDurableProgressResult(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")

	db := openFSMTestDB(t, dbDir)
	fsm := openFSMForTest(t, db, dbDir)
	raw := deterministicCreateCollectionEntry(t, "users", "fsm:create:users")

	results, err := fsm.ApplyCommittedEntriesV1([]CommittedEntryV1{committedCommand(2, 1, raw)})
	if err != nil {
		t.Fatalf("ApplyCommittedEntriesV1: %v results=%+v", err, results)
	}
	if len(results) != 1 {
		t.Fatalf("results=%d, want 1", len(results))
	}
	assertApplied(t, results[0], raftentry.ApplyStatusApplied, 1)
	assertLastApplied(t, fsm, raftentry.ApplyEntryID{Term: 2, Index: 1})
	if _, err := collections.NewCollectionManager(db).OpenCollection("users"); err != nil {
		t.Fatalf("OpenCollection users: %v", err)
	}
	record, ok, err := fsm.results.LookupApplyResult(raftentry.ApplyEntryID{Term: 2, Index: 1})
	if err != nil || !ok {
		t.Fatalf("LookupApplyResult before reopen=(%+v,%t,%v), want durable record", record, ok, err)
	}
	if record.Result != results[0] || record.AppliedCommandLSN == 0 {
		t.Fatalf("record before reopen=%+v lsn=%d, want result %+v with coverage", record.Result, record.AppliedCommandLSN, results[0])
	}
	if err := fsm.Close(); err != nil {
		t.Fatalf("Close FSM: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close DB: %v", err)
	}

	reopenedDB := openFSMTestDB(t, dbDir)
	defer func() { _ = reopenedDB.Close() }()
	reopenedFSM := openFSMForTest(t, reopenedDB, dbDir)
	defer func() { _ = reopenedFSM.Close() }()
	assertLastApplied(t, reopenedFSM, raftentry.ApplyEntryID{Term: 2, Index: 1})
	reopenedRecord, ok, err := reopenedFSM.results.LookupApplyResult(record.EntryID)
	if err != nil || !ok {
		t.Fatalf("LookupApplyResult after reopen=(%+v,%t,%v), want durable record", reopenedRecord, ok, err)
	}
	if reopenedRecord.Result != results[0] || reopenedRecord.AppliedCommandLSN != record.AppliedCommandLSN {
		t.Fatalf("reopened record=%+v, want result %+v lsn %d", reopenedRecord, results[0], record.AppliedCommandLSN)
	}

	replayed, err := reopenedFSM.ApplyCommittedEntryV1(committedCommand(2, 1, raw))
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 replay after reopen: %v result=%+v", err, replayed)
	}
	if replayed != results[0] {
		t.Fatalf("replayed result=%+v, want stored %+v", replayed, results[0])
	}

	duplicate, err := reopenedFSM.ApplyCommittedEntryV1(committedCommand(2, 2, raw))
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 idempotency replay: %v result=%+v", err, duplicate)
	}
	if duplicate.Status != raftentry.ApplyStatusAlreadyApplied || duplicate.ResultDigest != results[0].ResultDigest {
		t.Fatalf("duplicate result=%+v, want already-applied with original digest %s", duplicate, results[0].ResultDigest.Hex())
	}
	assertLastApplied(t, reopenedFSM, raftentry.ApplyEntryID{Term: 2, Index: 2})
}

func TestSingleGroupFSMLogicalDigestRejectsClosedFSM(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")

	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	if err := fsm.Close(); err != nil {
		t.Fatalf("Close FSM: %v", err)
	}

	if _, err := fsm.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{}); err == nil {
		t.Fatal("LogicalDigestV1 on closed FSM succeeded, want unsafe durability error")
	} else if code, ok := ErrorCodeOf(err); !ok || code != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("ErrorCodeOf(LogicalDigestV1 closed)=(%s,%t), want %s err=%v", code, ok, raftentry.ErrorUnsafeDurabilityModeV1, err)
	}
}

func TestSingleGroupApplyLoopRejectsProgressAheadOfLocalCoverage(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")

	db := openFSMTestDB(t, dbDir)
	fsm := openFSMForTest(t, db, dbDir)
	beforeLSN := db.State().AppliedCommandLSN
	staleID := raftentry.ApplyEntryID{Term: 1, Index: 1}
	if err := fsm.progress.RecordApplied(raftapply.ApplyProgressRecordV1{
		EntryID:           staleID,
		AppliedCommandLSN: beforeLSN + 1,
	}); err != nil {
		t.Fatalf("RecordApplied stale progress: %v", err)
	}

	if got, ok := fsm.LastApplied(); ok {
		t.Fatalf("LastApplied with uncovered progress=(%+v,%t), want unset", got, ok)
	}

	raw := deterministicCreateCollectionEntry(t, "users", "fsm:create:users:uncovered-progress")
	result, err := fsm.ApplyCommittedEntryV1(committedCommand(1, 2, raw))
	assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorUnsafeDurabilityModeV1)
	if got := db.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("AppliedCommandLSN after uncovered progress=%d, want unchanged %d", got, beforeLSN)
	}
	if _, openErr := collections.NewCollectionManager(db).OpenCollection("users"); !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after uncovered progress err=%v, want ErrCollectionNotFound", openErr)
	}
	if err := fsm.Close(); err != nil {
		t.Fatalf("Close FSM: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close DB: %v", err)
	}

	reopenedDB := openFSMTestDB(t, dbDir)
	defer func() { _ = reopenedDB.Close() }()
	reopenedFSM, openErr := Open(Options{
		DB:      reopenedDB,
		Cluster: validFSMClusterConfig(dbDir),
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if openErr == nil {
		_ = reopenedFSM.Close()
		t.Fatal("Open with uncovered progress succeeded, want unsafe durability error")
	}
	if code, ok := ErrorCodeOf(openErr); !ok || code != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("ErrorCodeOf(Open uncovered progress)=(%s,%t), want %s err=%v", code, ok, raftentry.ErrorUnsafeDurabilityModeV1, openErr)
	}
}

func TestSingleGroupApplyLoopRepairsMissingProgressWithStoredResult(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")

	db := openFSMTestDB(t, dbDir)
	fsm := openFSMForTest(t, db, dbDir)
	raw := deterministicCreateCollectionEntry(t, "users", "fsm:create:users:missing-progress")
	result, err := fsm.ApplyCommittedEntryV1(committedCommand(1, 1, raw))
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	coveredLSN := db.State().AppliedCommandLSN
	if coveredLSN == 0 {
		t.Fatal("AppliedCommandLSN after apply is zero, want local coverage")
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
	if openErr == nil {
		defer func() { _ = reopenedFSM.Close() }()
	} else {
		t.Fatalf("Open with missing progress and stored result: %v", openErr)
	}
	if got, ok := reopenedFSM.LastApplied(); ok {
		t.Fatalf("LastApplied with missing progress=(%+v,%t), want unset before replay repair", got, ok)
	}
	replayed, err := reopenedFSM.ApplyCommittedEntryV1(committedCommand(1, 1, raw))
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 result-backed replay repair: %v result=%+v", err, replayed)
	}
	if replayed != result {
		t.Fatalf("replayed result=%+v, want stored %+v", replayed, result)
	}
	assertLastApplied(t, reopenedFSM, raftentry.ApplyEntryID{Term: 1, Index: 1})
}

func TestSingleGroupApplyLoopRepairsProgressBehindLocalCoverageWithStoredResult(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")

	db := openFSMTestDB(t, dbDir)
	fsm := openFSMForTest(t, db, dbDir)
	users := deterministicCreateCollectionEntry(t, "users", "fsm:create:users:progress-behind")
	first, err := fsm.ApplyCommittedEntryV1(committedCommand(1, 1, users))
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 first: %v result=%+v", err, first)
	}
	assertApplied(t, first, raftentry.ApplyStatusApplied, 1)
	firstProgress, ok := fsm.progress.LastAppliedRecord()
	if !ok {
		t.Fatal("LastAppliedRecord after first apply missing")
	}

	orders := deterministicCreateCollectionEntry(t, "orders", "fsm:create:orders:progress-behind")
	second, err := fsm.ApplyCommittedEntryV1(committedCommand(1, 2, orders))
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 second: %v result=%+v", err, second)
	}
	assertApplied(t, second, raftentry.ApplyStatusApplied, 1)
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
	if openErr == nil {
		defer func() { _ = reopenedFSM.Close() }()
	} else {
		t.Fatalf("Open with progress behind local coverage and stored result: %v", openErr)
	}
	assertLastApplied(t, reopenedFSM, firstProgress.EntryID)
	replayed, err := reopenedFSM.ApplyCommittedEntryV1(committedCommand(1, 2, orders))
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 result-backed progress repair: %v result=%+v", err, replayed)
	}
	if replayed != second {
		t.Fatalf("replayed second result=%+v, want stored %+v", replayed, second)
	}
	assertLastApplied(t, reopenedFSM, raftentry.ApplyEntryID{Term: 1, Index: 2})
}

func TestSingleGroupApplyLoopRejectsProgressBehindLocalCoverageWithoutStoredResult(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")

	db := openFSMTestDB(t, dbDir)
	fsm := openFSMForTest(t, db, dbDir)
	users := deterministicCreateCollectionEntry(t, "users", "fsm:create:users:progress-behind-missing-result")
	first, err := fsm.ApplyCommittedEntryV1(committedCommand(1, 1, users))
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 first: %v result=%+v", err, first)
	}
	assertApplied(t, first, raftentry.ApplyStatusApplied, 1)
	firstProgress, ok := fsm.progress.LastAppliedRecord()
	if !ok {
		t.Fatal("LastAppliedRecord after first apply missing")
	}
	firstResult, ok, err := fsm.results.LookupApplyResult(firstProgress.EntryID)
	if err != nil || !ok {
		t.Fatalf("LookupApplyResult first=(%+v,%t,%v), want stored result", firstResult, ok, err)
	}

	orders := deterministicCreateCollectionEntry(t, "orders", "fsm:create:orders:progress-behind-missing-result")
	second, err := fsm.ApplyCommittedEntryV1(committedCommand(1, 2, orders))
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 second: %v result=%+v", err, second)
	}
	assertApplied(t, second, raftentry.ApplyStatusApplied, 1)
	coveredLSN := db.State().AppliedCommandLSN
	if coveredLSN <= firstProgress.AppliedCommandLSN {
		t.Fatalf("AppliedCommandLSN after second apply=%d, want greater than first progress %d", coveredLSN, firstProgress.AppliedCommandLSN)
	}
	progressPath := raftapply.DurableApplyProgressStorePath(fsm.metadataDir)
	resultPath := raftapply.DurableApplyResultStorePath(fsm.metadataDir)
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
	if err := os.Remove(resultPath); err != nil {
		t.Fatalf("Remove result metadata: %v", err)
	}
	results, err := raftapply.OpenDurableApplyResultStoreFile(resultPath, raftapply.DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("Open rolled-back result store: %v", err)
	}
	if err := results.RecordApplyResult(firstResult); err != nil {
		t.Fatalf("Record rolled-back result: %v", err)
	}
	if err := results.Close(); err != nil {
		t.Fatalf("Close rolled-back result store: %v", err)
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
	if openErr == nil {
		_ = reopenedFSM.Close()
		t.Fatal("Open with progress behind local coverage and missing result succeeded, want unsafe durability error")
	}
	if code, ok := ErrorCodeOf(openErr); !ok || code != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("ErrorCodeOf(Open progress behind missing result)=(%s,%t), want %s err=%v", code, ok, raftentry.ErrorUnsafeDurabilityModeV1, openErr)
	}
}

func TestSingleGroupApplyLoopFailsClosedOnOrderingAndUnsupportedEntries(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	users := deterministicCreateCollectionEntry(t, "users", "fsm:create:users:ordering")
	seed, err := fsm.ApplyCommittedEntryV1(committedCommand(3, 1, users))
	if err != nil {
		t.Fatalf("seed apply: %v result=%+v", err, seed)
	}
	assertApplied(t, seed, raftentry.ApplyStatusApplied, 1)

	orders := deterministicCreateCollectionEntry(t, "orders", "fsm:create:orders:gap")
	gap, err := fsm.ApplyCommittedEntryV1(committedCommand(3, 3, orders))
	if err != nil {
		t.Fatalf("apply raft log index gap: %v result=%+v", err, gap)
	}
	assertApplied(t, gap, raftentry.ApplyStatusApplied, 1)
	gapLSN := db.State().AppliedCommandLSN
	assertLastApplied(t, fsm, raftentry.ApplyEntryID{Term: 3, Index: 3})

	profiles := deterministicCreateCollectionEntry(t, "profiles", "fsm:create:profiles:lower-term")
	lowerTerm, err := fsm.ApplyCommittedEntryV1(committedCommand(2, 4, profiles))
	assertRejected(t, lowerTerm, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	assertNoApplyProgress(t, db, gapLSN, fsm, raftentry.ApplyEntryID{Term: 3, Index: 3}, "profiles")

	differentSameIndex := deterministicCreateCollectionEntry(t, "admins", "fsm:create:admins:duplicate-index")
	duplicateConflict, err := fsm.ApplyCommittedEntryV1(committedCommand(3, 3, differentSameIndex))
	assertRejected(t, duplicateConflict, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	assertNoApplyProgress(t, db, gapLSN, fsm, raftentry.ApplyEntryID{Term: 3, Index: 3}, "admins")

	audits := deterministicCreateCollectionEntry(t, "audits", "fsm:create:audits:duplicate-term")
	sameIndexDifferentTerm, err := fsm.ApplyCommittedEntryV1(committedCommand(4, 3, audits))
	assertRejected(t, sameIndexDifferentTerm, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	assertNoApplyProgress(t, db, gapLSN, fsm, raftentry.ApplyEntryID{Term: 3, Index: 3}, "audits")

	profiles = deterministicCreateCollectionEntry(t, "profiles", "fsm:create:profiles:advance")
	advanced, err := fsm.ApplyCommittedEntryV1(committedCommand(3, 4, profiles))
	if err != nil {
		t.Fatalf("advance apply: %v result=%+v", err, advanced)
	}
	assertApplied(t, advanced, raftentry.ApplyStatusApplied, 1)
	advancedLSN := db.State().AppliedCommandLSN

	customers := deterministicCreateCollectionEntry(t, "customers", "fsm:create:customers:lower-index")
	lowerIndex, err := fsm.ApplyCommittedEntryV1(committedCommand(3, 3, customers))
	assertRejected(t, lowerIndex, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	assertNoApplyProgress(t, db, advancedLSN, fsm, raftentry.ApplyEntryID{Term: 3, Index: 4}, "customers")

	snapshots := deterministicCreateCollectionEntry(t, "snapshots", "fsm:create:snapshots:unsupported")
	unsupported, err := fsm.ApplyCommittedEntryV1(CommittedEntryV1{Type: EntryTypeV1("snapshot-v1"), Term: 3, Index: 4, Bytes: snapshots})
	assertRejected(t, unsupported, err, raftentry.ApplyStatusRejectedUnsupported, raftentry.ErrorUnsupportedVersionV1)
	assertNoApplyProgress(t, db, advancedLSN, fsm, raftentry.ApplyEntryID{Term: 3, Index: 4}, "snapshots")
}

func TestValidateAppliedPrefixAllowsInitialIndexGap(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm, err := Open(Options{
		DB:      db,
		Cluster: validFSMClusterConfig(dbDir),
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync:          true,
			AllowInitialIndexGap: true,
		},
	})
	if err != nil {
		t.Fatalf("Open FSM: %v", err)
	}
	defer func() { _ = fsm.Close() }()

	first := committedCommand(6, 2, deterministicCreateCollectionEntry(t, "users", "fsm:create:users:initial-gap"))
	firstResult, err := fsm.ApplyCommittedEntryV1(first)
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 first gap entry: %v result=%+v", err, firstResult)
	}
	assertApplied(t, firstResult, raftentry.ApplyStatusApplied, 1)
	if result, err := fsm.ValidateAppliedPrefixV1([]CommittedEntryV1{first}); err != nil {
		t.Fatalf("ValidateAppliedPrefixV1 initial gap: %v result=%+v", err, result)
	}

	second := committedCommand(6, 3, deterministicCreateCollectionEntry(t, "orders", "fsm:create:orders:initial-gap"))
	secondResult, err := fsm.ApplyCommittedEntryV1(second)
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 second gap entry: %v result=%+v", err, secondResult)
	}
	assertApplied(t, secondResult, raftentry.ApplyStatusApplied, 1)
	if result, err := fsm.ValidateAppliedPrefixV1([]CommittedEntryV1{second}); err == nil {
		t.Fatalf("ValidateAppliedPrefixV1 suffix-only result=%+v, want conflict", result)
	} else if code, ok := ErrorCodeOf(err); !ok || code != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("ValidateAppliedPrefixV1 suffix-only code=(%s,%t), want %s err=%v", code, ok, raftentry.ErrorRejectedConflictV1, err)
	}
	if result, err := fsm.ValidateAppliedPrefixV1([]CommittedEntryV1{first, second}); err != nil {
		t.Fatalf("ValidateAppliedPrefixV1 full initial-gap prefix: %v result=%+v", err, result)
	}
}

func TestValidateAppliedPrefixAllowsRaftLogIndexGaps(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	first := committedCommand(6, 1, deterministicCreateCollectionEntry(t, "users", "fsm:create:users:raft-gap"))
	firstResult, err := fsm.ApplyCommittedEntryV1(first)
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 first: %v result=%+v", err, firstResult)
	}
	assertApplied(t, firstResult, raftentry.ApplyStatusApplied, 1)

	second := committedCommand(7, 3, deterministicCreateCollectionEntry(t, "orders", "fsm:create:orders:raft-gap"))
	secondResult, err := fsm.ApplyCommittedEntryV1(second)
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 second with raft gap: %v result=%+v", err, secondResult)
	}
	assertApplied(t, secondResult, raftentry.ApplyStatusApplied, 1)
	assertLastApplied(t, fsm, raftentry.ApplyEntryID{Term: 7, Index: 3})

	if result, err := fsm.ValidateAppliedPrefixV1([]CommittedEntryV1{first, second}); err != nil {
		t.Fatalf("ValidateAppliedPrefixV1 raft gap prefix: %v result=%+v", err, result)
	}
	divergentGap := committedCommand(7, 2, deterministicCreateCollectionEntry(t, "admins", "fsm:create:admins:raft-gap"))
	if result, err := fsm.ValidateAppliedPrefixV1([]CommittedEntryV1{first, divergentGap}); err == nil {
		t.Fatalf("ValidateAppliedPrefixV1 missing gap progress result=%+v, want conflict", result)
	} else if code, ok := ErrorCodeOf(err); !ok || code != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("ValidateAppliedPrefixV1 missing gap progress code=(%s,%t), want %s err=%v", code, ok, raftentry.ErrorRejectedConflictV1, err)
	}
}

func TestInitialIndexGapReplaysResultAheadBeforeLaterEntry(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForInitialGapTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	first := committedCommand(6, 2, deterministicCreateCollectionEntry(t, "users", "fsm:create:users:gap-result-ahead"))
	firstResult, err := applyCommittedEntryWithFaultForTest(t, fsm, first, raftapply.FaultAfterResultRecordBeforeProgressV1)
	assertRejected(t, firstResult, err, raftentry.ApplyStatusRecoveryRequired, raftentry.ErrorUnsafeDurabilityModeV1)
	if got := fsm.progress.Len(); got != 0 {
		t.Fatalf("progress records after result-ahead fault=%d, want 0", got)
	}
	if got := fsm.results.Len(); got != 1 {
		t.Fatalf("result records after result-ahead fault=%d, want 1", got)
	}
	if got := db.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after result-ahead fault=%d, want 1", got)
	}
	if result, err := fsm.ValidateAppliedPrefixV1(nil); err == nil {
		t.Fatalf("ValidateAppliedPrefixV1 empty result-ahead prefix result=%+v, want conflict", result)
	} else if code, ok := ErrorCodeOf(err); !ok || code != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("ValidateAppliedPrefixV1 empty result-ahead prefix code=(%s,%t), want %s err=%v", code, ok, raftentry.ErrorRejectedConflictV1, err)
	}

	second := committedCommand(6, 4, deterministicCreateCollectionEntry(t, "orders", "fsm:create:orders:gap-result-ahead"))
	skipped, err := fsm.ApplyCommittedEntryV1(second)
	assertRejected(t, skipped, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorUnsafeDurabilityModeV1)
	if got := fsm.progress.Len(); got != 0 {
		t.Fatalf("progress records after skipped later entry=%d, want 0", got)
	}

	replayedFirst, err := fsm.ApplyCommittedEntryV1(first)
	if err != nil {
		t.Fatalf("replay first result-ahead entry: %v result=%+v", err, replayedFirst)
	}
	assertApplied(t, replayedFirst, raftentry.ApplyStatusApplied, 1)
	assertLastApplied(t, fsm, raftentry.ApplyEntryID{Term: 6, Index: 2})

	appliedSecond, err := fsm.ApplyCommittedEntryV1(second)
	if err != nil {
		t.Fatalf("apply second after replaying first: %v result=%+v", err, appliedSecond)
	}
	assertApplied(t, appliedSecond, raftentry.ApplyStatusApplied, 1)
	assertLastApplied(t, fsm, raftentry.ApplyEntryID{Term: 6, Index: 4})
}

func TestValidateAppliedPrefixAllowsResultAheadRecoveryState(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForInitialGapTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	first := committedCommand(6, 2, deterministicCreateCollectionEntry(t, "users", "fsm:create:users:prefix-result-ahead"))
	firstResult, err := fsm.ApplyCommittedEntryV1(first)
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 first: %v result=%+v", err, firstResult)
	}
	assertApplied(t, firstResult, raftentry.ApplyStatusApplied, 1)

	second := committedCommand(6, 3, deterministicCreateCollectionEntry(t, "orders", "fsm:create:orders:prefix-result-ahead"))
	secondResult, err := applyCommittedEntryWithFaultForTest(t, fsm, second, raftapply.FaultAfterResultRecordBeforeProgressV1)
	assertRejected(t, secondResult, err, raftentry.ApplyStatusRecoveryRequired, raftentry.ErrorUnsafeDurabilityModeV1)
	if got := fsm.progress.Len(); got != 1 {
		t.Fatalf("progress records after second result-ahead fault=%d, want 1", got)
	}
	if got := fsm.results.Len(); got != 2 {
		t.Fatalf("result records after second result-ahead fault=%d, want 2", got)
	}

	if result, err := fsm.ValidateAppliedPrefixV1([]CommittedEntryV1{first}); err != nil {
		t.Fatalf("ValidateAppliedPrefixV1 progress prefix with result-ahead state: %v result=%+v", err, result)
	}
	if result, err := fsm.ValidateAppliedPrefixV1([]CommittedEntryV1{first, second}); err == nil {
		t.Fatalf("ValidateAppliedPrefixV1 result-ahead full prefix result=%+v, want conflict before replay", result)
	} else if code, ok := ErrorCodeOf(err); !ok || code != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("ValidateAppliedPrefixV1 result-ahead full prefix code=(%s,%t), want %s err=%v", code, ok, raftentry.ErrorRejectedConflictV1, err)
	}

	replayedSecond, err := fsm.ApplyCommittedEntryV1(second)
	if err != nil {
		t.Fatalf("replay second result-ahead entry: %v result=%+v", err, replayedSecond)
	}
	assertApplied(t, replayedSecond, raftentry.ApplyStatusApplied, 1)
	if result, err := fsm.ValidateAppliedPrefixV1([]CommittedEntryV1{first, second}); err != nil {
		t.Fatalf("ValidateAppliedPrefixV1 full prefix after replay: %v result=%+v", err, result)
	}
}

func TestSingleGroupApplyLoopLogicalDigestConvergesAcrossDBs(t *testing.T) {
	raw := deterministicCreateCollectionEntry(t, "users", "fsm:create:users:converge")
	entries := []CommittedEntryV1{committedCommand(5, 1, raw)}

	root := t.TempDir()
	dbADir := filepath.Join(root, "a", "db")
	dbA := openFSMTestDB(t, dbADir)
	defer func() { _ = dbA.Close() }()
	fsmA := openFSMForTest(t, dbA, dbADir)
	defer func() { _ = fsmA.Close() }()
	if results, err := fsmA.ApplyCommittedEntriesV1(entries); err != nil {
		t.Fatalf("ApplyCommittedEntriesV1 A: %v results=%+v", err, results)
	}
	digestA, err := fsmA.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1 A: %v", err)
	}

	dbBDir := filepath.Join(root, "b", "db")
	dbB := openFSMTestDB(t, dbBDir)
	defer func() { _ = dbB.Close() }()
	fsmB := openFSMForTest(t, dbB, dbBDir)
	defer func() { _ = fsmB.Close() }()
	if results, err := fsmB.ApplyCommittedEntriesV1(entries); err != nil {
		t.Fatalf("ApplyCommittedEntriesV1 B: %v results=%+v", err, results)
	}
	digestB, err := fsmB.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1 B: %v", err)
	}
	if digestA != digestB {
		t.Fatalf("logical digest mismatch A=%s B=%s", digestA.Hex(), digestB.Hex())
	}
}

func TestOpenDerivesApplyStoresFromValidatedRaftClusterLayout(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()

	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	resolved, err := raftcluster.Validate(validFSMClusterConfig(dbDir))
	if err != nil {
		t.Fatalf("Validate cluster config: %v", err)
	}
	if fsm.metadataDir != resolved.Layout.ApplyDir {
		t.Fatalf("metadataDir=%q want cluster apply dir %q", fsm.metadataDir, resolved.Layout.ApplyDir)
	}

	overlapping := validFSMClusterConfig(dbDir)
	overlapping.ClusterDir = raftcluster.CommandWALDir(dbDir)
	_, err = Open(Options{
		DB:      db,
		Cluster: overlapping,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if err == nil {
		t.Fatal("Open with cluster dir overlapping command WAL returned nil error")
	}
}

func TestCommittedEntryMetadataPreservedIntoApplyMetadata(t *testing.T) {
	trace := []byte("trace-a")
	target := raftentry.TargetIdentityV1{
		ScopeRule:     raftentry.ScopeRuleSingleGroupV1,
		DatabaseScope: "database/default",
		CatalogScope:  "catalog/default",
		CommandID:     nativewire.CommandCreateCollection,
	}
	entry := CommittedEntryV1{
		Type: EntryTypeCommandEntryV1,
		RequestMetadata: raftentry.RequestMetadataV1{
			RequestID:     42,
			AckPolicy:     nativewire.AckVisible,
			TraceContext:  trace,
			Compression:   "none",
			OmitResultIDs: true,
		},
		ExpectedTarget: &target,
	}
	fsm := &FSM{}
	meta, err := fsm.applyMetadata(entry, raftentry.ApplyEntryID{Term: 1, Index: 1})
	if err != nil {
		t.Fatalf("applyMetadata: %v", err)
	}
	if meta.RequestMetadata.RequestID != entry.RequestMetadata.RequestID ||
		meta.RequestMetadata.AckPolicy != entry.RequestMetadata.AckPolicy ||
		meta.RequestMetadata.Compression != entry.RequestMetadata.Compression ||
		!meta.RequestMetadata.OmitResultIDs ||
		string(meta.RequestMetadata.TraceContext) != string(trace) {
		t.Fatalf("RequestMetadata=%+v, want preserved %+v", meta.RequestMetadata, entry.RequestMetadata)
	}
	trace[0] = 'X'
	if string(meta.RequestMetadata.TraceContext) != "trace-a" {
		t.Fatalf("RequestMetadata trace was not cloned: %q", meta.RequestMetadata.TraceContext)
	}
	if meta.ExpectedTarget == nil || !meta.ExpectedTarget.Equal(target) {
		t.Fatalf("ExpectedTarget=%+v, want %+v", meta.ExpectedTarget, target)
	}
	target.CommandID = nativewire.CommandInsertBatch
	if meta.ExpectedTarget.CommandID != nativewire.CommandCreateCollection {
		t.Fatalf("ExpectedTarget was not cloned: %+v", meta.ExpectedTarget)
	}
}

func TestSingleGroupApplyLoopRejectsExpectedTargetMismatch(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()
	beforeLSN := db.State().AppliedCommandLSN

	raw := deterministicCreateCollectionEntry(t, "users", "fsm:create:users:target-mismatch")
	expected := decodedTargetForTest(t, raw)
	expected.CollectionMeta = append([]byte(nil), expected.CollectionMeta...)
	expected.CollectionMeta = append(expected.CollectionMeta, 0)

	result, err := fsm.ApplyCommittedEntryV1(CommittedEntryV1{
		Type:                     EntryTypeCommandEntryV1,
		Term:                     1,
		Index:                    1,
		Bytes:                    raw,
		CurrentCatalogVersion:    testCatalogVersionStart,
		HasCurrentCatalogVersion: true,
		ExpectedTarget:           &expected,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorTargetMismatchV1)
	if got := db.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("AppliedCommandLSN after target mismatch=%d, want unchanged %d", got, beforeLSN)
	}
	if got, ok := fsm.LastApplied(); ok {
		t.Fatalf("LastApplied after target mismatch=(%+v,%t), want unset", got, ok)
	}
	if record, ok, lookupErr := fsm.results.LookupApplyResult(raftentry.ApplyEntryID{Term: 1, Index: 1}); lookupErr != nil || ok {
		t.Fatalf("LookupApplyResult after target mismatch=(%+v,%t,%v), want miss", record, ok, lookupErr)
	}
	if _, openErr := collections.NewCollectionManager(db).OpenCollection("users"); !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after target mismatch err=%v, want ErrCollectionNotFound", openErr)
	}
}

func TestSingleGroupApplyLoopZeroValueFSMFailsClosed(t *testing.T) {
	var fsm FSM
	if err := fsm.Close(); err != nil {
		t.Fatalf("zero-value Close: %v", err)
	}
	result, err := fsm.ApplyCommittedEntryV1(CommittedEntryV1{
		Type:  EntryTypeCommandEntryV1,
		Term:  1,
		Index: 1,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorUnsafeDurabilityModeV1)
}

func TestSingleGroupApplyLoopMixedMutationEntriesContiguous(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	create := deterministicCreateCollectionEntryWithOptions(t, "users", "fsm:create:users:mixed", testCreateCollectionMetaOptions{
		documentFormat: uint64(nativewire.DocumentFormatBSON),
	})
	docU1 := testBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}})
	docU2 := testBSONDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "sea"}})
	insert := deterministicInsertBatchEntry(t, "users", "fsm:insert:users:mixed", nativewire.DocumentFormatBSON, [][]byte{[]byte("u1"), []byte("u2")}, [][]byte{docU1, docU2})
	docU1Replace := testBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "sfo"}})
	replace := deterministicReplaceBatchEntry(t, "users", "fsm:replace:users:mixed", nativewire.DocumentFormatBSON, [][]byte{[]byte("u1")}, [][]byte{docU1Replace})
	deleteEntry := deterministicDeleteBatchEntry(t, "users", "fsm:delete:users:mixed", [][]byte{[]byte("u2")})

	results, err := fsm.ApplyCommittedEntriesV1([]CommittedEntryV1{
		committedCommand(4, 1, create),
		committedCommand(4, 2, insert),
		committedCommand(4, 3, replace),
		committedCommand(4, 4, deleteEntry),
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntriesV1 mixed: %v results=%+v", err, results)
	}
	if len(results) != 4 {
		t.Fatalf("mixed results=%d, want 4", len(results))
	}
	for i, wantAffected := range []int64{1, 2, 1, 1} {
		assertApplied(t, results[i], raftentry.ApplyStatusApplied, wantAffected)
	}
	assertLastApplied(t, fsm, raftentry.ApplyEntryID{Term: 4, Index: 4})

	opened, err := collections.NewCollectionManager(db).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users: %v", err)
	}
	gotU1, err := opened.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if got := bson.Raw(gotU1).Lookup("city").StringValue(); got != "sfo" {
		t.Fatalf("u1 city=%q, want sfo", got)
	}
	gotU2, err := opened.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("Get u2: %v", err)
	}
	if gotU2 != nil {
		t.Fatalf("u2 after delete=%x, want nil", gotU2)
	}
}

func openFSMTestDB(t testing.TB, dir string) *backenddb.DB {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          dir,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
	})
	if err != nil {
		t.Fatalf("Open DB: %v", err)
	}
	return db
}

func openFSMForTest(t testing.TB, db *backenddb.DB, dbDir string) *FSM {
	t.Helper()
	fsm, err := Open(Options{
		DB:      db,
		Cluster: validFSMClusterConfig(dbDir),
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if err != nil {
		t.Fatalf("Open FSM: %v", err)
	}
	return fsm
}

func openFSMForInitialGapTest(t testing.TB, db *backenddb.DB, dbDir string) *FSM {
	t.Helper()
	fsm, err := Open(Options{
		DB:      db,
		Cluster: validFSMClusterConfig(dbDir),
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync:          true,
			AllowInitialIndexGap: true,
		},
	})
	if err != nil {
		t.Fatalf("Open FSM: %v", err)
	}
	return fsm
}

func applyCommittedEntryWithFaultForTest(t *testing.T, fsm *FSM, entry CommittedEntryV1, point raftapply.FaultPointV1) (raftentry.ApplyResultV1, error) {
	t.Helper()
	id := raftentry.ApplyEntryID{Term: entry.Term, Index: entry.Index}
	meta, err := fsm.applyMetadata(entry, id)
	if err != nil {
		t.Fatalf("applyMetadata: %v", err)
	}
	return raftapply.ApplyCommittedEntryV1(fsm.db, entry.Bytes, meta, raftapply.Options{
		DecodeLimits:  fsm.decodeLimits,
		ProgressStore: fsm.progress,
		ResultStore:   fsm.results,
		FaultInjector: fsmSinglePointFaultInjector{point: point},
	})
}

type fsmSinglePointFaultInjector struct {
	point raftapply.FaultPointV1
}

func (f fsmSinglePointFaultInjector) InjectApplyFault(point raftapply.FaultPointV1, _ raftapply.ApplyFaultContextV1) error {
	if point != f.point {
		return nil
	}
	return errors.New("injected apply fault")
}

func validFSMClusterConfig(dbDir string) raftcluster.Config {
	return raftcluster.Config{
		Dir:     dbDir,
		NodeID:  "node-a",
		GroupID: "default",
		Peers: []raftcluster.Peer{
			{ID: "node-a", Address: "127.0.0.1:9201"},
			{ID: "node-b", Address: "127.0.0.1:9202"},
			{ID: "node-c", Address: "127.0.0.1:9203"},
		},
	}
}

func committedCommand(term, index uint64, raw []byte) CommittedEntryV1 {
	return CommittedEntryV1{
		Type:                     EntryTypeCommandEntryV1,
		Term:                     term,
		Index:                    index,
		Bytes:                    raw,
		CurrentCatalogVersion:    testCatalogVersionStart,
		HasCurrentCatalogVersion: true,
	}
}

func deterministicCreateCollectionEntry(t testing.TB, collection, idempotency string) []byte {
	t.Helper()
	return deterministicCreateCollectionEntryWithOptions(t, collection, idempotency, testCreateCollectionMetaOptions{})
}

type testCreateCollectionMetaOptions struct {
	documentFormat uint64
}

func deterministicCreateCollectionEntryWithOptions(t testing.TB, collection, idempotency string, opts testCreateCollectionMetaOptions) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandCreateCollection, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionMeta, Bytes: testCreateCollectionMetaPayload(collection, opts)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func testCreateCollectionMetaPayload(collection string, opts testCreateCollectionMetaOptions) []byte {
	dst := binary.AppendUvarint(nil, 1) // collection_meta version
	dst = appendTestString(dst, collection)
	dst = binary.AppendUvarint(dst, opts.documentFormat) // document_format
	dst = binary.AppendUvarint(dst, 0)                   // data_root_storage_policy
	dst = binary.AppendUvarint(dst, 0)                   // index_state_storage_policy
	dst = append(dst, 0)                                 // allow_array_values_in_index
	dst = append(dst, 0)                                 // disable_indexed_write_memtables
	dst = append(dst, 0)                                 // buffered_indexed_writes
	dst = binary.AppendVarint(dst, 0)                    // buffered_indexed_write_max_documents
	dst = binary.AppendVarint(dst, 0)                    // buffered_indexed_write_max_bytes
	dst = binary.AppendVarint(dst, 0)                    // buffered_indexed_write_max_root_runs
	dst = append(dst, 0)                                 // buffered_indexed_async_flush
	dst = append(dst, 0)                                 // buffered_indexed_overlay_roots
	dst = binary.AppendVarint(dst, 0)                    // buffered_indexed_async_flush_max_queued_units
	dst = binary.AppendUvarint(dst, 0)                   // index_count
	return dst
}

func deterministicInsertBatchEntry(t testing.TB, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte) []byte {
	t.Helper()
	return deterministicMutationEntry(t, nativewire.CommandInsertBatch, collection, idempotency, format, ids, documents, nil)
}

func deterministicReplaceBatchEntry(t testing.TB, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte) []byte {
	t.Helper()
	extra := []nativewire.Section{{ID: nativewire.SectionReplacementMode, Bytes: binary.AppendUvarint(nil, 1)}}
	return deterministicMutationEntry(t, nativewire.CommandReplaceBatch, collection, idempotency, format, ids, documents, extra)
}

func deterministicDeleteBatchEntry(t testing.TB, collection, idempotency string, ids [][]byte) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandDeleteBatch, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionRef, Bytes: deterministicTestCollectionNameRef(collection)},
		{ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, ids...)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func deterministicMutationEntry(t testing.TB, command nativewire.CommandID, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte, extra []nativewire.Section) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: command, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionRef, Bytes: deterministicTestCollectionNameRef(collection)},
		{ID: nativewire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(format))},
		{ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, ids...)},
		{ID: nativewire.SectionDocuments, Bytes: nativewire.AppendByteVector(nil, documents...)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	sections = append(sections, extra...)
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func deterministicTestCollectionNameRef(collection string) []byte {
	return append([]byte{deterministicCollectionRefName}, collection...)
}

func decodedTargetForTest(t *testing.T, raw []byte) raftentry.TargetIdentityV1 {
	t.Helper()
	entry, err := raftentry.DecodeCommandEntryV1(raw, raftentry.DecodeOptions{})
	if err != nil {
		t.Fatalf("DecodeCommandEntryV1: %v", err)
	}
	return entry.Target
}

func testBSONDocument(t *testing.T, document bson.D) []byte {
	t.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		t.Fatalf("Marshal BSON: %v", err)
	}
	return encoded
}

func appendTestString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func assertApplied(t *testing.T, result raftentry.ApplyResultV1, wantStatus raftentry.ApplyStatusV1, wantAffected int64) {
	t.Helper()
	if result.Status != wantStatus ||
		result.DeterministicErrorCode != raftentry.ErrorNoneV1 ||
		result.AffectedCount != wantAffected ||
		result.CommandDigest == (raftentry.CommandDigestV1{}) ||
		result.ResultDigest == (raftentry.CommandDigestV1{}) {
		t.Fatalf("applied result=%+v, want status=%s affected=%d non-zero digests", result, wantStatus, wantAffected)
	}
}

func assertRejected(t *testing.T, result raftentry.ApplyResultV1, err error, wantStatus raftentry.ApplyStatusV1, wantCode raftentry.DeterministicErrorCodeV1) {
	t.Helper()
	if err == nil {
		t.Fatalf("got nil error for rejected result %+v", result)
	}
	if result.Status != wantStatus || result.DeterministicErrorCode != wantCode {
		t.Fatalf("rejected result=%+v err=%v, want status=%s code=%s", result, err, wantStatus, wantCode)
	}
	if code, ok := ErrorCodeOf(err); !ok || code != wantCode {
		t.Fatalf("ErrorCodeOf(%v)=(%s,%t), want %s", err, code, ok, wantCode)
	}
}

func assertLastApplied(t testing.TB, fsm *FSM, want raftentry.ApplyEntryID) {
	t.Helper()
	got, ok := fsm.LastApplied()
	if !ok || got != want {
		t.Fatalf("LastApplied=(%+v,%t), want %+v", got, ok, want)
	}
}

func assertNoApplyProgress(t *testing.T, db *backenddb.DB, wantLSN uint64, fsm *FSM, wantLast raftentry.ApplyEntryID, absentCollection string) {
	t.Helper()
	if got := db.State().AppliedCommandLSN; got != wantLSN {
		t.Fatalf("AppliedCommandLSN=%d, want unchanged %d", got, wantLSN)
	}
	assertLastApplied(t, fsm, wantLast)
	if _, err := collections.NewCollectionManager(db).OpenCollection(absentCollection); err == nil {
		t.Fatalf("%s collection exists after rejected apply", absentCollection)
	} else if !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection %s: %v", absentCollection, err)
	}
}
