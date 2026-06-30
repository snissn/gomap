package raftfsm

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestClusterAdapterAppliesProviderCommittedEntryDurably(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")

	db := openFSMTestDB(t, dbDir)
	fsm := openFSMForTest(t, db, dbDir)
	raw := deterministicCreateCollectionEntry(t, "users", "fsm:cluster-adapter:create:users")
	entry := clusterCommittedCommand(2, 1, raw)

	result, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), entry)
	if err != nil {
		t.Fatalf("ApplyCommittedCommandEntryV1: %v result=%+v", err, result)
	}
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	if _, err := collections.NewCollectionManager(db).OpenCollection("users"); err != nil {
		t.Fatalf("OpenCollection users: %v", err)
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
	if _, err := collections.NewCollectionManager(reopenedDB).OpenCollection("users"); err != nil {
		t.Fatalf("OpenCollection users after reopen: %v", err)
	}

	replayed, err := reopenedFSM.ApplyCommittedCommandEntryV1(context.Background(), entry)
	if err != nil {
		t.Fatalf("ApplyCommittedCommandEntryV1 replay: %v result=%+v", err, replayed)
	}
	if replayed != result {
		t.Fatalf("replayed result=%+v want stored %+v", replayed, result)
	}
	duplicate, err := reopenedFSM.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(2, 2, raw))
	if err != nil {
		t.Fatalf("ApplyCommittedCommandEntryV1 duplicate idempotency replay: %v result=%+v", err, duplicate)
	}
	if duplicate.Status != raftentry.ApplyStatusAlreadyApplied || duplicate.ResultDigest != result.ResultDigest {
		t.Fatalf("duplicate result=%+v want already-applied with original digest %s", duplicate, result.ResultDigest.Hex())
	}
}

func TestClusterAdapterPreflightRejectsDecodedRawMismatch(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")

	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "fsm:cluster-adapter:preflight-raw")
	decodedRaw := deterministicCreateCollectionEntry(t, "orders", "fsm:cluster-adapter:preflight-decoded")
	decoded, err := raftentry.DecodeCommandEntryV1(decodedRaw, raftentry.DecodeOptions{})
	if err != nil {
		t.Fatalf("DecodeCommandEntryV1 decodedRaw: %v", err)
	}

	_, err = fsm.PreflightCommandEntryV1(context.Background(), raftcluster.CommandEntryPreflightRequestV1{
		GroupID:                  fsm.cluster.GroupID,
		NodeID:                   fsm.cluster.NodeID,
		EntryBytes:               raw,
		DecodedEntry:             decoded,
		CurrentCatalogVersion:    testCatalogVersionStart,
		HasCurrentCatalogVersion: true,
		SyncLocalCommandWAL:      true,
	})
	if code, ok := ErrorCodeOf(err); !ok || code != raftentry.ErrorMalformedEntryV1 {
		t.Fatalf("PreflightCommandEntryV1 mismatch code=(%s,%t) err=%v, want malformed", code, ok, err)
	}
}

func TestClusterAdapterPreflightAllowsKnownIdempotencyReplay(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")

	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	create := deterministicCreateCollectionEntryWithOptions(t, "users", "fsm:cluster-adapter:preflight-replay:create", testCreateCollectionMetaOptions{
		documentFormat: uint64(nativewire.DocumentFormatBSON),
	})
	if result, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(2, 1, create)); err != nil {
		t.Fatalf("ApplyCommittedCommandEntryV1 create: %v result=%+v", err, result)
	}
	doc := testBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}})
	insert := deterministicInsertBatchEntry(t, "users", "fsm:cluster-adapter:preflight-replay:insert", nativewire.DocumentFormatBSON, [][]byte{[]byte("u1")}, [][]byte{doc})
	insertResult, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(2, 2, insert))
	if err != nil {
		t.Fatalf("ApplyCommittedCommandEntryV1 insert: %v result=%+v", err, insertResult)
	}
	assertApplied(t, insertResult, raftentry.ApplyStatusApplied, 1)
	decoded, err := raftentry.DecodeCommandEntryV1(insert, raftentry.DecodeOptions{})
	if err != nil {
		t.Fatalf("DecodeCommandEntryV1 insert: %v", err)
	}

	for _, currentCatalogVersion := range []uint64{testCatalogVersionStart, testCatalogVersionStart + 1} {
		result, err := fsm.PreflightCommandEntryV1(context.Background(), raftcluster.CommandEntryPreflightRequestV1{
			GroupID:                  fsm.cluster.GroupID,
			NodeID:                   fsm.cluster.NodeID,
			EntryBytes:               insert,
			DecodedEntry:             decoded,
			CurrentCatalogVersion:    currentCatalogVersion,
			HasCurrentCatalogVersion: true,
			SyncLocalCommandWAL:      true,
		})
		if err != nil {
			t.Fatalf("PreflightCommandEntryV1 known retry catalog %d: %v", currentCatalogVersion, err)
		}
		if !result.KnownIdempotencyReplay {
			t.Fatalf("PreflightCommandEntryV1 known retry catalog %d replay=%v want true", currentCatalogVersion, result.KnownIdempotencyReplay)
		}
	}
}

func TestClusterAdapterAllowsRaftLogIndexGapsAndPreservesOrderingRejections(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")

	db := openFSMTestDB(t, dbDir)
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, dbDir)
	defer func() { _ = fsm.Close() }()

	firstRaw := deterministicCreateCollectionEntry(t, "users", "fsm:cluster-adapter:ordering:users")
	first, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(2, 1, firstRaw))
	if err != nil {
		t.Fatalf("ApplyCommittedCommandEntryV1 first: %v result=%+v", err, first)
	}
	assertApplied(t, first, raftentry.ApplyStatusApplied, 1)

	gapRaw := deterministicCreateCollectionEntry(t, "orders", "fsm:cluster-adapter:ordering:gap")
	gap, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(2, 3, gapRaw))
	if err != nil {
		t.Fatalf("ApplyCommittedCommandEntryV1 raft log index gap: %v result=%+v", err, gap)
	}
	assertApplied(t, gap, raftentry.ApplyStatusApplied, 1)

	replayed, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(2, 1, firstRaw))
	if err != nil {
		t.Fatalf("ApplyCommittedCommandEntryV1 replay below last applied: %v result=%+v", err, replayed)
	}
	if replayed != first {
		t.Fatalf("replayed result=%+v want stored %+v", replayed, first)
	}
	assertLastApplied(t, fsm, raftentry.ApplyEntryID{Term: 2, Index: 3})

	conflictRaw := deterministicCreateCollectionEntry(t, "profiles", "fsm:cluster-adapter:ordering:conflict")
	conflict, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(2, 1, conflictRaw))
	assertRejected(t, conflict, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)

	regressionRaw := deterministicCreateCollectionEntry(t, "profiles", "fsm:cluster-adapter:ordering:term")
	regression, err := fsm.ApplyCommittedCommandEntryV1(context.Background(), clusterCommittedCommand(1, 4, regressionRaw))
	assertRejected(t, regression, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
}

func clusterCommittedCommand(term, index uint64, raw []byte) raftcluster.CommittedCommandEntryV1 {
	targetEntry := committedCommand(term, index, raw)
	return raftcluster.CommittedCommandEntryV1{
		Term:                     targetEntry.Term,
		Index:                    targetEntry.Index,
		Bytes:                    targetEntry.Bytes,
		CurrentCatalogVersion:    targetEntry.CurrentCatalogVersion,
		HasCurrentCatalogVersion: targetEntry.HasCurrentCatalogVersion,
		SyncLocalCommandWAL:      true,
		RequestMetadata:          targetEntry.RequestMetadata,
		ExpectedTarget:           targetEntry.ExpectedTarget,
	}
}
