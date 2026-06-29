package raftfsm

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const testCatalogVersionStart = 7

func TestSingleGroupApplyLoopCloseReopenDurableProgressResult(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	metadataDir := filepath.Join(root, "raftapply")

	db := openFSMTestDB(t, dbDir)
	fsm := openFSMForTest(t, db, metadataDir)
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
	reopenedFSM := openFSMForTest(t, reopenedDB, metadataDir)
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

func TestSingleGroupApplyLoopFailsClosedOnOrderingAndUnsupportedEntries(t *testing.T) {
	root := t.TempDir()
	db := openFSMTestDB(t, filepath.Join(root, "db"))
	defer func() { _ = db.Close() }()
	fsm := openFSMForTest(t, db, filepath.Join(root, "raftapply"))
	defer func() { _ = fsm.Close() }()

	users := deterministicCreateCollectionEntry(t, "users", "fsm:create:users:ordering")
	seed, err := fsm.ApplyCommittedEntryV1(committedCommand(3, 1, users))
	if err != nil {
		t.Fatalf("seed apply: %v result=%+v", err, seed)
	}
	assertApplied(t, seed, raftentry.ApplyStatusApplied, 1)
	beforeLSN := db.State().AppliedCommandLSN

	orders := deterministicCreateCollectionEntry(t, "orders", "fsm:create:orders:gap")
	gap, err := fsm.ApplyCommittedEntryV1(committedCommand(3, 3, orders))
	assertRejected(t, gap, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	assertNoApplyProgress(t, db, beforeLSN, fsm, raftentry.ApplyEntryID{Term: 3, Index: 1})

	lowerTerm, err := fsm.ApplyCommittedEntryV1(committedCommand(2, 2, orders))
	assertRejected(t, lowerTerm, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	assertNoApplyProgress(t, db, beforeLSN, fsm, raftentry.ApplyEntryID{Term: 3, Index: 1})

	differentSameIndex := deterministicCreateCollectionEntry(t, "admins", "fsm:create:admins:duplicate-index")
	duplicateConflict, err := fsm.ApplyCommittedEntryV1(committedCommand(3, 1, differentSameIndex))
	assertRejected(t, duplicateConflict, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	assertNoApplyProgress(t, db, beforeLSN, fsm, raftentry.ApplyEntryID{Term: 3, Index: 1})

	profiles := deterministicCreateCollectionEntry(t, "profiles", "fsm:create:profiles:advance")
	advanced, err := fsm.ApplyCommittedEntryV1(committedCommand(3, 2, profiles))
	if err != nil {
		t.Fatalf("advance apply: %v result=%+v", err, advanced)
	}
	assertApplied(t, advanced, raftentry.ApplyStatusApplied, 1)
	advancedLSN := db.State().AppliedCommandLSN

	lowerIndex, err := fsm.ApplyCommittedEntryV1(committedCommand(3, 1, orders))
	assertRejected(t, lowerIndex, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	assertNoApplyProgress(t, db, advancedLSN, fsm, raftentry.ApplyEntryID{Term: 3, Index: 2})

	unsupported, err := fsm.ApplyCommittedEntryV1(CommittedEntryV1{Type: EntryTypeV1("snapshot-v1"), Term: 3, Index: 2, Bytes: orders})
	assertRejected(t, unsupported, err, raftentry.ApplyStatusRejectedUnsupported, raftentry.ErrorUnsupportedVersionV1)
	assertNoApplyProgress(t, db, advancedLSN, fsm, raftentry.ApplyEntryID{Term: 3, Index: 2})
}

func TestSingleGroupApplyLoopLogicalDigestConvergesAcrossDBs(t *testing.T) {
	raw := deterministicCreateCollectionEntry(t, "users", "fsm:create:users:converge")
	entries := []CommittedEntryV1{committedCommand(5, 1, raw)}

	root := t.TempDir()
	dbA := openFSMTestDB(t, filepath.Join(root, "a", "db"))
	defer func() { _ = dbA.Close() }()
	fsmA := openFSMForTest(t, dbA, filepath.Join(root, "a", "raftapply"))
	defer func() { _ = fsmA.Close() }()
	if results, err := fsmA.ApplyCommittedEntriesV1(entries); err != nil {
		t.Fatalf("ApplyCommittedEntriesV1 A: %v results=%+v", err, results)
	}
	digestA, err := fsmA.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1 A: %v", err)
	}

	dbB := openFSMTestDB(t, filepath.Join(root, "b", "db"))
	defer func() { _ = dbB.Close() }()
	fsmB := openFSMForTest(t, dbB, filepath.Join(root, "b", "raftapply"))
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

func openFSMForTest(t testing.TB, db *backenddb.DB, metadataDir string) *FSM {
	t.Helper()
	fsm, err := Open(Options{
		DB:          db,
		MetadataDir: metadataDir,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync: true,
		},
	})
	if err != nil {
		t.Fatalf("Open FSM: %v", err)
	}
	return fsm
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

func deterministicCreateCollectionEntry(t *testing.T, collection, idempotency string) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandCreateCollection, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionMeta, Bytes: testCreateCollectionMetaPayload(collection)},
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

func testCreateCollectionMetaPayload(collection string) []byte {
	dst := binary.AppendUvarint(nil, 1) // collection_meta version
	dst = appendTestString(dst, collection)
	dst = binary.AppendUvarint(dst, 0) // document_format
	dst = binary.AppendUvarint(dst, 0) // data_root_storage_policy
	dst = binary.AppendUvarint(dst, 0) // index_state_storage_policy
	dst = append(dst, 0)               // allow_array_values_in_index
	dst = append(dst, 0)               // disable_indexed_write_memtables
	dst = append(dst, 0)               // buffered_indexed_writes
	dst = binary.AppendVarint(dst, 0)  // buffered_indexed_write_max_documents
	dst = binary.AppendVarint(dst, 0)  // buffered_indexed_write_max_bytes
	dst = binary.AppendVarint(dst, 0)  // buffered_indexed_write_max_root_runs
	dst = append(dst, 0)               // buffered_indexed_async_flush
	dst = append(dst, 0)               // buffered_indexed_overlay_roots
	dst = binary.AppendVarint(dst, 0)  // buffered_indexed_async_flush_max_queued_units
	dst = binary.AppendUvarint(dst, 0) // index_count
	return dst
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

func assertNoApplyProgress(t *testing.T, db *backenddb.DB, wantLSN uint64, fsm *FSM, wantLast raftentry.ApplyEntryID) {
	t.Helper()
	if got := db.State().AppliedCommandLSN; got != wantLSN {
		t.Fatalf("AppliedCommandLSN=%d, want unchanged %d", got, wantLSN)
	}
	assertLastApplied(t, fsm, wantLast)
	if _, err := collections.NewCollectionManager(db).OpenCollection("orders"); err == nil {
		t.Fatal("orders collection exists after rejected apply")
	} else if !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection orders: %v", err)
	}
}
