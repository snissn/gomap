package raftapply

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const testCatalogVersionStart = 7
const testCurrentCollectionMetaWireVersion = 5

func TestUnsupportedDeterministicEntryRejectsBeforeAppendAndStores(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/insert_batch_entry.hex")

	beforeLSN := db.State().AppliedCommandLSN
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedUnsupported, raftentry.ErrorUnsupportedCommandV1)
	if result.CommandDigest == (raftentry.CommandDigestV1{}) {
		t.Fatal("unsupported deterministic entry should still report a digest")
	}
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("command-WAL seam calls append=%d finalize=%d, want 0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 0 {
		t.Fatalf("command WAL frames=%d, want 0", got)
	}
	if got := db.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("AppliedCommandLSN=%d, want %d", got, beforeLSN)
	}
	if progress.Len() != 0 {
		t.Fatalf("progress records=%d, want 0", progress.Len())
	}
	if results.Len() != 0 {
		t.Fatalf("result records=%d, want 0", results.Len())
	}
}

func TestCreateCollectionApplyCreatesCatalogAndDeterministicResult(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	opened, err := collections.NewCollectionManager(db).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users: %v", err)
	}
	if got := opened.Meta().Name; got != "users" {
		t.Fatalf("collection name=%q, want users", got)
	}
	frames := readCommandWALFrames(t, dir)
	if len(frames) != 1 {
		t.Fatalf("command WAL frames=%d, want 1", len(frames))
	}
	assertCatalogCreateFrame(t, frames[0], "users")
	if got := db.State().AppliedCommandLSN; got != frames[0].LSN {
		t.Fatalf("AppliedCommandLSN=%d, want catalog frame LSN %d", got, frames[0].LSN)
	}
	if progress.Len() != 1 || results.Len() != 1 {
		t.Fatalf("store lengths progress=%d results=%d, want 1/1", progress.Len(), results.Len())
	}

	replayed, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("same ApplyEntryID replay: %v", err)
	}
	if replayed != result {
		t.Fatalf("same ApplyEntryID replay result=%+v, want stored %+v", replayed, result)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 1 {
		t.Fatalf("command WAL frames after result replay=%d, want 1", got)
	}

	duplicate, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 2), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, duplicate, raftentry.ApplyStatusAlreadyApplied, 0)
	if duplicate.ResultDigest != result.ResultDigest {
		t.Fatalf("duplicate result digest=%s, want %s", duplicate.ResultDigest.Hex(), result.ResultDigest.Hex())
	}
	frames = readCommandWALFrames(t, dir)
	if len(frames) != 2 {
		t.Fatalf("command WAL frames after same-schema duplicate=%d, want 2", len(frames))
	}
	assertCatalogCreateFrame(t, frames[1], "users")
	if got := db.State().AppliedCommandLSN; got != frames[1].LSN {
		t.Fatalf("AppliedCommandLSN=%d, want duplicate no-op frame LSN %d", got, frames[1].LSN)
	}
	if progress.Len() != 2 || results.Len() != 2 {
		t.Fatalf("store lengths after duplicate progress=%d results=%d, want 2/2", progress.Len(), results.Len())
	}
}

func TestCreateCollectionAcceptsCurrentCollectionMetaWireVersion(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:v5", testCreateCollectionMetaOptions{
		version: testCurrentCollectionMetaWireVersion,
	})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{})
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	opened, err := collections.NewCollectionManager(db).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users: %v", err)
	}
	if got := opened.Meta().Name; got != "users" {
		t.Fatalf("collection name=%q, want users", got)
	}
	frames := readCommandWALFrames(t, dir)
	if len(frames) != 1 {
		t.Fatalf("command WAL frames=%d, want 1", len(frames))
	}
	assertCatalogCreateFrame(t, frames[0], "users")
}

func TestCreateCollectionForcesNativewireStoragePolicies(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:storage", testCreateCollectionMetaOptions{
		version:            testCurrentCollectionMetaWireVersion,
		dataRootStorage:    2,
		indexStateStorage:  2,
		includeIndex:       true,
		indexStoragePolicy: 2,
	})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{})
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	opened, err := collections.NewCollectionManager(db).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users: %v", err)
	}
	meta := opened.Meta()
	assertNativewireFastStoragePolicies(t, meta)

	frames := readCommandWALFrames(t, dir)
	if len(frames) != 1 {
		t.Fatalf("command WAL frames=%d, want 1", len(frames))
	}
	assertCatalogCreateFrame(t, frames[0], "users")
	assertNativewireFastStoragePolicies(t, catalogCreateFrameMeta(t, frames[0]))
}

func TestStoredResultReplayRecordsMissingProgress(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	id := raftentry.ApplyEntryID{Term: 1, Index: 1}
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users", testCreateCollectionMetaOptions{})
	entry, err := raftentry.DecodeCommandEntryV1(raw, raftentry.DecodeOptions{ApplyEntryID: id})
	if err != nil {
		t.Fatalf("DecodeCommandEntryV1: %v", err)
	}
	result := raftentry.ApplyResultV1{
		Status:                 raftentry.ApplyStatusApplied,
		CommandDigest:          entry.Digest,
		DeterministicErrorCode: raftentry.ErrorNoneV1,
		AffectedCount:          1,
		ResultDigest:           entry.Digest,
	}
	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	if err := results.RecordApplyResult(ApplyResultRecordV1{
		EntryID:       id,
		CommandDigest: entry.Digest,
		Result:        result,
	}); err != nil {
		t.Fatalf("RecordApplyResult: %v", err)
	}

	seam := &countingCommandWALApplySeam{}
	replayed, err := ApplyCommittedEntryV1(db, raw, applyMeta(id.Term, id.Index), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 stored result replay: %v", err)
	}
	if replayed != result {
		t.Fatalf("stored result replay=%+v, want %+v", replayed, result)
	}
	if progress.Len() != 1 {
		t.Fatalf("progress records after stored result replay=%d, want 1", progress.Len())
	}
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("stored result replay reached command WAL seam append=%d finalize=%d, want 0/0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 0 {
		t.Fatalf("command WAL frames after stored result replay=%d, want 0", got)
	}
}

func TestCreateCollectionDuplicateIncompatibleFailsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	beforeFrames := readCommandWALFrames(t, dir)
	beforeLSN := db.State().AppliedCommandLSN

	incompatible := deterministicCreateCollectionEntry(t, "users", "client-a:create:users-bson", testCreateCollectionMetaOptions{
		documentFormat: uint64(nativewire.DocumentFormatBSON),
	})
	rejected, err := ApplyCommittedEntryV1(db, incompatible, applyMeta(1, 2), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertRejected(t, rejected, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
		t.Fatalf("command WAL frames after incompatible duplicate=%d, want %d", got, len(beforeFrames))
	}
	if got := db.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("AppliedCommandLSN after incompatible duplicate=%d, want %d", got, beforeLSN)
	}
	opened, err := collections.NewCollectionManager(db).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users: %v", err)
	}
	if got := opened.Meta().Options.DocumentFormat; got != collections.DocumentFormatDefault {
		t.Fatalf("document format after rejected duplicate=%q, want default", got)
	}
	if progress.Len() != 1 || results.Len() != 1 {
		t.Fatalf("store lengths after rejected duplicate progress=%d results=%d, want 1/1", progress.Len(), results.Len())
	}
}

func TestCreateCollectionStaleCatalogGuardFailsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	rawOrders := deterministicCreateCollectionEntry(t, "orders", "client-a:create:orders", testCreateCollectionMetaOptions{})
	applied, err := ApplyCommittedEntryV1(db, rawOrders, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, applied, raftentry.ApplyStatusApplied, 1)
	beforeFrames := readCommandWALFrames(t, dir)
	beforeLSN := db.State().AppliedCommandLSN

	seam := &countingCommandWALApplySeam{}
	rawUsersStale := deterministicCreateCollectionEntry(t, "users", "client-a:create:users-stale", testCreateCollectionMetaOptions{})
	rejected, err := ApplyCommittedEntryV1(db, rawUsersStale, applyMetaWithCatalogVersion(1, 2, testCatalogVersionStart+1), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRejected(t, rejected, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("stale catalog guard reached command-WAL seam append=%d finalize=%d, want 0/0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
		t.Fatalf("command WAL frames after stale guard=%d, want %d", got, len(beforeFrames))
	}
	if got := db.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("AppliedCommandLSN after stale guard=%d, want %d", got, beforeLSN)
	}
	if _, openErr := collections.NewCollectionManager(db).OpenCollection("users"); !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after stale guard error=%v, want ErrCollectionNotFound", openErr)
	}
}

func TestCreateCollectionReopenPreservesCatalogAndLogicalDigest(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: NewMemoryApplyProgressStore(8, 8),
		ResultStore:   NewMemoryApplyResultStore(8),
	})
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	beforeDigest, err := LogicalDigestV1ForDB(db, LogicalDigestOptionsV1{})
	if err != nil {
		_ = db.Close()
		t.Fatalf("LogicalDigestV1ForDB before reopen: %v", err)
	}
	beforeLSN := db.State().AppliedCommandLSN
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openApplyHarnessDB(t, dir)
	defer func() { _ = reopen.Close() }()
	opened, err := collections.NewCollectionManager(reopen).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users after reopen: %v", err)
	}
	if got := opened.Meta().Name; got != "users" {
		t.Fatalf("reopened collection name=%q, want users", got)
	}
	if got := reopen.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("reopen AppliedCommandLSN=%d, want %d", got, beforeLSN)
	}
	afterDigest, err := LogicalDigestV1ForDB(reopen, LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB after reopen: %v", err)
	}
	if afterDigest != beforeDigest {
		t.Fatalf("logical digest after reopen=%s, want %s", afterDigest.Hex(), beforeDigest.Hex())
	}
}

func TestPostApplyResultStoreFailureRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ResultStore: recordApplyResultStoreFailAfterPreflight{},
	})
	assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if _, openErr := collections.NewCollectionManager(db).OpenCollection("users"); openErr != nil {
		t.Fatalf("OpenCollection users after recovery-required result store failure: %v", openErr)
	}
	frames := readCommandWALFrames(t, dir)
	if len(frames) != 1 {
		t.Fatalf("command WAL frames=%d, want 1", len(frames))
	}
	if got := db.State().AppliedCommandLSN; got != frames[0].LSN {
		t.Fatalf("AppliedCommandLSN=%d, want visible command WAL LSN %d", got, frames[0].LSN)
	}
}

func TestPostApplyProgressStoreFailureRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: recordProgressStoreFailAfterPreflight{},
	})
	assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if _, openErr := collections.NewCollectionManager(db).OpenCollection("users"); openErr != nil {
		t.Fatalf("OpenCollection users after recovery-required progress store failure: %v", openErr)
	}
	frames := readCommandWALFrames(t, dir)
	if len(frames) != 1 {
		t.Fatalf("command WAL frames=%d, want 1", len(frames))
	}
	if got := db.State().AppliedCommandLSN; got != frames[0].LSN {
		t.Fatalf("AppliedCommandLSN=%d, want visible command WAL LSN %d", got, frames[0].LSN)
	}
}

func TestLogicalDigestConvergesAcrossFreshDBsAndIgnoresLocalLayout(t *testing.T) {
	rawUsers := deterministicCreateCollectionEntry(t, "users", "client-a:create:users", testCreateCollectionMetaOptions{})
	rawOrders := deterministicCreateCollectionEntryWithCatalogVersion(t, "orders", "client-a:create:orders", testCatalogVersionStart+1, testCreateCollectionMetaOptions{})

	dirA := t.TempDir()
	dbA := openApplyHarnessDB(t, dirA)
	defer func() { _ = dbA.Close() }()
	applyCreateSequence(t, dbA, rawUsers, rawOrders)
	if err := dbA.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint A: %v", err)
	}
	digestA, err := LogicalDigestV1ForDB(dbA, LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB A: %v", err)
	}

	dirB := t.TempDir()
	dbB := openApplyHarnessDBWithOptions(t, dirB, backenddb.Options{IndexOuterLeavesInValueLog: true})
	defer func() { _ = dbB.Close() }()
	if err := dbB.Set([]byte("layout-noise"), []byte("advances local raw roots and command WAL LSN")); err != nil {
		t.Fatalf("layout-noise Set: %v", err)
	}
	if err := dbB.RotateCommandWALActiveSegment(false); err != nil {
		t.Fatalf("RotateCommandWALActiveSegment: %v", err)
	}
	applyCreateSequence(t, dbB, rawUsers)
	if err := dbB.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint B mid-sequence: %v", err)
	}
	applyCreateSequenceFrom(t, dbB, 2, rawOrders)
	digestB, err := LogicalDigestV1ForDB(dbB, LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB B: %v", err)
	}
	if digestB != digestA {
		t.Fatalf("logical digests differ across local layout/checkpoint timing: A=%s B=%s", digestA.Hex(), digestB.Hex())
	}

	dirC := t.TempDir()
	dbC := openApplyHarnessDB(t, dirC)
	defer func() { _ = dbC.Close() }()
	rawAccounts := deterministicCreateCollectionEntry(t, "accounts", "client-a:create:accounts", testCreateCollectionMetaOptions{})
	applyCreateSequence(t, dbC, rawAccounts)
	digestC, err := LogicalDigestV1ForDB(dbC, LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB C: %v", err)
	}
	if digestC == digestA {
		t.Fatalf("different catalog produced same logical digest %s", digestA.Hex())
	}
	scopedDigest, err := LogicalDigestV1ForDB(dbA, LogicalDigestOptionsV1{CatalogScope: "catalog/other"})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB scoped: %v", err)
	}
	if scopedDigest == digestA {
		t.Fatalf("different catalog scope produced same logical digest %s", digestA.Hex())
	}
	databaseDigest, err := LogicalDigestV1ForDB(dbA, LogicalDigestOptionsV1{DatabaseScope: "database/other"})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB database scope: %v", err)
	}
	if databaseDigest == digestA {
		t.Fatalf("different database scope produced same logical digest %s", digestA.Hex())
	}
}

func TestMalformedBytesReturnDeterministicErrorWithoutAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	seam := &countingCommandWALApplySeam{}
	result, err := ApplyCommittedEntryV1(db, []byte("bad"), applyMeta(1, 1), Options{CommandWALApplySeam: seam})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedMalformed, raftentry.ErrorMalformedEntryV1)
	if result.CommandDigest != (raftentry.CommandDigestV1{}) {
		t.Fatalf("malformed digest=%s, want zero digest", result.CommandDigest.Hex())
	}
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("malformed input reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
}

func TestDecodeCreateCollectionMetaRejectsUnsupportedEnums(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
		want    string
	}{
		{
			name:    "document_format",
			payload: testCreateCollectionMetaPayload("users", testCreateCollectionMetaOptions{documentFormat: 99}),
			want:    "document_format",
		},
		{
			name:    "data_root_storage",
			payload: testCreateCollectionMetaPayload("users", testCreateCollectionMetaOptions{dataRootStorage: 99}),
			want:    "data_root_storage",
		},
		{
			name:    "index_state_storage",
			payload: testCreateCollectionMetaPayload("users", testCreateCollectionMetaOptions{indexStateStorage: 99}),
			want:    "index_state_storage",
		},
		{
			name:    "index_value_type",
			payload: testCreateCollectionMetaPayload("users", testCreateCollectionMetaOptions{includeIndex: true, indexValueType: 99}),
			want:    "index value type",
		},
		{
			name:    "index_storage_policy",
			payload: testCreateCollectionMetaPayload("users", testCreateCollectionMetaOptions{includeIndex: true, indexStoragePolicy: 99}),
			want:    "index storage policy",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeCreateCollectionMetaV1(tc.payload)
			if got := codeOf(err); got != raftentry.ErrorUnsupportedFeatureV1 {
				t.Fatalf("decodeCreateCollectionMetaV1 error=%v code=%s, want unsupported feature", err, got)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("decodeCreateCollectionMetaV1 error=%v, want mention %q", err, tc.want)
			}
		})
	}
}

func TestHarnessApplyAPIAcceptsDeterministicBytesOnly(t *testing.T) {
	method := reflect.TypeOf((*Harness).ApplyCommittedEntryV1)
	if method.NumIn() != 3 {
		t.Fatalf("Harness.ApplyCommittedEntryV1 inputs=%d, want receiver+bytes+metadata", method.NumIn())
	}
	bytesType := method.In(1)
	if bytesType.Kind() != reflect.Slice || bytesType.Elem().Kind() != reflect.Uint8 {
		t.Fatalf("ApplyCommittedEntryV1 input=%s, want []byte deterministic entry", bytesType)
	}
	if got := method.In(2); got != reflect.TypeOf(ApplyMetadataV1{}) {
		t.Fatalf("ApplyCommittedEntryV1 metadata input=%s, want ApplyMetadataV1", got)
	}
}

func TestProgressBoundOverflowFailsBeforeAppendOrMutation(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 2), Options{
		ProgressStore:       NewMemoryApplyProgressStore(8, 1),
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorResourceExhaustedV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("overflow reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	_, openErr := collections.NewCollectionManager(db).OpenCollection("users")
	if !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after overflow error=%v, want ErrCollectionNotFound", openErr)
	}
}

func TestProgressGapFailsBeforeAppendOrMutation(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 2), Options{
		ProgressStore:       NewMemoryApplyProgressStore(8, 8),
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("gap reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after gap=%d, want 0", got)
	}
	_, openErr := collections.NewCollectionManager(db).OpenCollection("users")
	if !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after gap error=%v, want ErrCollectionNotFound", openErr)
	}
}

func TestResultStoreCapacityFailsBeforeAppendOrMutation(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(0)
	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorResourceExhaustedV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("result-store overflow reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after result-store overflow=%d, want 0", got)
	}
	_, openErr := collections.NewCollectionManager(db).OpenCollection("users")
	if !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after result-store overflow error=%v, want ErrCollectionNotFound", openErr)
	}
	if progress.Len() != 0 || results.Len() != 0 {
		t.Fatalf("store lengths after result-store overflow progress=%d results=%d, want 0/0", progress.Len(), results.Len())
	}
}

func TestProgressRecordCapacityFailsBeforeAppendOrMutation(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(0, 8)
	results := NewMemoryApplyResultStore(8)
	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorResourceExhaustedV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("progress-store overflow reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after progress-store overflow=%d, want 0", got)
	}
	_, openErr := collections.NewCollectionManager(db).OpenCollection("users")
	if !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after progress-store overflow error=%v, want ErrCollectionNotFound", openErr)
	}
	if progress.Len() != 0 || results.Len() != 0 {
		t.Fatalf("store lengths after progress-store overflow progress=%d results=%d, want 0/0", progress.Len(), results.Len())
	}
}

func TestReadOnlyDBApplyFailsBeforeAppendOrMutation(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close writable DB: %v", err)
	}
	ro, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open read-only DB: %v", err)
	}
	defer func() { _ = ro.Close() }()

	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, applyErr := ApplyCommittedEntryV1(ro, raw, applyMeta(1, 1), Options{CommandWALApplySeam: seam})
	assertRejected(t, result, applyErr, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorReadOnlyV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("read-only apply reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	_, openErr := collections.NewCollectionManager(ro).OpenCollection("users")
	if !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after read-only apply error=%v, want ErrCollectionNotFound", openErr)
	}
}

func TestUnsafeDurabilityFailsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	meta := applyMeta(1, 1)
	meta.LocalDurabilityBoundary = LocalDurabilityBoundaryV1("local-visible-only-v1")
	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, err := ApplyCommittedEntryV1(db, raw, meta, Options{CommandWALApplySeam: seam})
	assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorUnsafeDurabilityModeV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("unsafe durability reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
}

func TestSameApplyEntryIDDifferentDigestConflictsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	id := raftentry.ApplyEntryID{Term: 1, Index: 1}
	results := NewMemoryApplyResultStore(8)
	var otherDigest raftentry.CommandDigestV1
	otherDigest[0] = 1
	if err := results.RecordApplyResult(ApplyResultRecordV1{
		EntryID:       id,
		CommandDigest: otherDigest,
		Result: raftentry.ApplyResultV1{
			Status:                 raftentry.ApplyStatusApplied,
			CommandDigest:          otherDigest,
			DeterministicErrorCode: raftentry.ErrorNoneV1,
		},
	}); err != nil {
		t.Fatalf("seed result record: %v", err)
	}

	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(id.Term, id.Index), Options{
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("conflict reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	if results.Len() != 1 {
		t.Fatalf("result records after conflict=%d, want seeded record only", results.Len())
	}
}

func TestMemoryStoresAreBounded(t *testing.T) {
	id1 := raftentry.ApplyEntryID{Term: 1, Index: 1}
	id2 := raftentry.ApplyEntryID{Term: 1, Index: 2}
	var digest raftentry.CommandDigestV1
	digest[0] = 7

	results := NewMemoryApplyResultStore(1)
	if err := results.RecordApplyResult(ApplyResultRecordV1{EntryID: id1, CommandDigest: digest}); err != nil {
		t.Fatalf("RecordApplyResult id1: %v", err)
	}
	if err := results.RecordApplyResult(ApplyResultRecordV1{EntryID: id2, CommandDigest: digest}); codeOf(err) != raftentry.ErrorResourceExhaustedV1 {
		t.Fatalf("RecordApplyResult id2 error=%v code=%s, want resource exhausted", err, codeOf(err))
	}

	progress := NewMemoryApplyProgressStore(1, 8)
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: id1, CommandDigest: digest}); err != nil {
		t.Fatalf("RecordApplied id1: %v", err)
	}
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: id2, CommandDigest: digest}); codeOf(err) != raftentry.ErrorResourceExhaustedV1 {
		t.Fatalf("RecordApplied id2 error=%v code=%s, want resource exhausted", err, codeOf(err))
	}

	progress = NewMemoryApplyProgressStore(8, 8)
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: id1, CommandDigest: digest}); err != nil {
		t.Fatalf("RecordApplied lower-index setup: %v", err)
	}
	if err := progress.CheckCanApply(id1); codeOf(err) != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("CheckCanApply duplicate/lower error=%v code=%s, want conflict", err, codeOf(err))
	}
	id3 := raftentry.ApplyEntryID{Term: 1, Index: 3}
	if err := progress.CheckCanApply(id3); codeOf(err) != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("CheckCanApply gap error=%v code=%s, want conflict", err, codeOf(err))
	}
}

func TestMemoryApplyResultStorePreservesFirstResultForSameDigest(t *testing.T) {
	id := raftentry.ApplyEntryID{Term: 1, Index: 1}
	var digest raftentry.CommandDigestV1
	digest[0] = 7
	first := raftentry.ApplyResultV1{
		Status:                 raftentry.ApplyStatusApplied,
		CommandDigest:          digest,
		DeterministicErrorCode: raftentry.ErrorNoneV1,
		AffectedCount:          1,
	}
	second := raftentry.ApplyResultV1{
		Status:                 raftentry.ApplyStatusAlreadyApplied,
		CommandDigest:          digest,
		DeterministicErrorCode: raftentry.ErrorNoneV1,
		AffectedCount:          0,
	}

	results := NewMemoryApplyResultStore(1)
	if err := results.RecordApplyResult(ApplyResultRecordV1{EntryID: id, CommandDigest: digest, Result: first}); err != nil {
		t.Fatalf("RecordApplyResult first: %v", err)
	}
	if err := results.RecordApplyResult(ApplyResultRecordV1{EntryID: id, CommandDigest: digest, Result: second}); err != nil {
		t.Fatalf("RecordApplyResult second: %v", err)
	}
	record, ok, err := results.LookupApplyResult(id)
	if err != nil || !ok {
		t.Fatalf("LookupApplyResult=(%+v,%t,%v), want first record", record, ok, err)
	}
	if record.Result != first {
		t.Fatalf("stored result=%+v, want first %+v", record.Result, first)
	}
}

func openApplyHarnessDB(t *testing.T, dir string) *backenddb.DB {
	t.Helper()
	return openApplyHarnessDBWithOptions(t, dir, backenddb.Options{})
}

func openApplyHarnessDBWithOptions(t *testing.T, dir string, opts backenddb.Options) *backenddb.DB {
	t.Helper()
	opts.Dir = dir
	opts.CommandWAL = true
	opts.DisableBackgroundPrune = true
	opts.CommandWALStatsScan = true
	opts.CommandWALSegmentTargetBytes = 1 << 20
	db, err := backenddb.Open(opts)
	if err != nil {
		t.Fatalf("Open DB: %v", err)
	}
	return db
}

func applyMeta(term, index uint64) ApplyMetadataV1 {
	return applyMetaWithCatalogVersion(term, index, testCatalogVersionStart)
}

func applyMetaWithCatalogVersion(term, index, catalogVersion uint64) ApplyMetadataV1 {
	return ApplyMetadataV1{
		EntryID:                  raftentry.ApplyEntryID{Term: term, Index: index},
		LocalDurabilityBoundary:  LocalDurabilityCommandWALV1,
		CurrentCatalogVersion:    catalogVersion,
		HasCurrentCatalogVersion: true,
	}
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
		t.Fatalf("ApplyCommittedEntryV1 returned nil error for rejected result %+v", result)
	}
	if result.Status != wantStatus || result.DeterministicErrorCode != wantCode {
		t.Fatalf("result status/code=%s/%s, want %s/%s (err=%v)", result.Status, result.DeterministicErrorCode, wantStatus, wantCode, err)
	}
	if got := codeOf(err); got != wantCode {
		t.Fatalf("error code=%s, want %s (err=%v)", got, wantCode, err)
	}
}

func assertRecoveryRequired(t *testing.T, result raftentry.ApplyResultV1, err error, wantCode raftentry.DeterministicErrorCodeV1) {
	t.Helper()
	if err == nil {
		t.Fatalf("ApplyCommittedEntryV1 returned nil error for recovery-required result %+v", result)
	}
	if result.Status != raftentry.ApplyStatusRecoveryRequired || result.DeterministicErrorCode != wantCode {
		t.Fatalf("result status/code=%s/%s, want recovery-required/%s (err=%v)", result.Status, result.DeterministicErrorCode, wantCode, err)
	}
	if got := codeOf(err); got != wantCode {
		t.Fatalf("error code=%s, want %s (err=%v)", got, wantCode, err)
	}
	if result.CommandDigest == (raftentry.CommandDigestV1{}) {
		t.Fatalf("recovery-required result has zero command digest: %+v", result)
	}
}

func assertCatalogCreateFrame(t *testing.T, env commitlog.CommandEnvelope, collection string) {
	t.Helper()
	if env.Kind != commitlog.CommandKindCatalogCreateCollection ||
		env.Scope != commitlog.CommandScopeCatalog ||
		env.PayloadFormat != commitlog.PayloadFormatCatalogCreateCollectionV1 {
		t.Fatalf("command WAL frame identity=%+v, want catalog create collection", env)
	}
	payload, err := commitlog.DecodeCatalogCreateCollectionPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeCatalogCreateCollectionPayload: %v", err)
	}
	if payload.Collection != collection {
		t.Fatalf("catalog create collection=%q, want %q", payload.Collection, collection)
	}
}

func catalogCreateFrameMeta(t *testing.T, env commitlog.CommandEnvelope) collections.CollectionMeta {
	t.Helper()
	payload, err := commitlog.DecodeCatalogCreateCollectionPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeCatalogCreateCollectionPayload: %v", err)
	}
	var disk struct {
		Name    string                        `json:"name"`
		Options collections.CollectionOptions `json:"options,omitempty"`
		Indexes []collections.IndexDefinition `json:"indexes,omitempty"`
	}
	if err := json.Unmarshal(payload.Metadata, &disk); err != nil {
		t.Fatalf("Unmarshal catalog collection metadata: %v", err)
	}
	return collections.CollectionMeta{
		Name:    disk.Name,
		Options: disk.Options,
		Indexes: disk.Indexes,
	}
}

func assertNativewireFastStoragePolicies(t *testing.T, meta collections.CollectionMeta) {
	t.Helper()
	if got := meta.Options.DataRootStoragePolicy; got != collections.RootStorageFast {
		t.Fatalf("data root storage policy=%q, want %q", got, collections.RootStorageFast)
	}
	if got := meta.Options.IndexStateStoragePolicy; got != collections.RootStorageFast {
		t.Fatalf("index state storage policy=%q, want %q", got, collections.RootStorageFast)
	}
	if len(meta.Indexes) != 1 {
		t.Fatalf("indexes=%d, want 1", len(meta.Indexes))
	}
	if got := meta.Indexes[0].StoragePolicy; got != collections.RootStorageFast {
		t.Fatalf("index storage policy=%q, want %q", got, collections.RootStorageFast)
	}
}

func codeOf(err error) raftentry.DeterministicErrorCodeV1 {
	code, _ := ErrorCodeOf(err)
	return code
}

type recordApplyResultStoreFailAfterPreflight struct{}

func (recordApplyResultStoreFailAfterPreflight) LookupApplyResult(raftentry.ApplyEntryID) (ApplyResultRecordV1, bool, error) {
	return ApplyResultRecordV1{}, false, nil
}

func (recordApplyResultStoreFailAfterPreflight) CheckCanRecordApplyResult(ApplyResultRecordV1) error {
	return nil
}

func (recordApplyResultStoreFailAfterPreflight) RecordApplyResult(ApplyResultRecordV1) error {
	return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "result store unavailable after apply")
}

type recordProgressStoreFailAfterPreflight struct{}

func (recordProgressStoreFailAfterPreflight) CheckCanApply(raftentry.ApplyEntryID) error {
	return nil
}

func (recordProgressStoreFailAfterPreflight) CheckCanRecordApplied(ApplyProgressRecordV1) error {
	return nil
}

func (recordProgressStoreFailAfterPreflight) RecordApplied(ApplyProgressRecordV1) error {
	return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "progress store unavailable after apply")
}

func (recordProgressStoreFailAfterPreflight) LastApplied() (raftentry.ApplyEntryID, bool) {
	return raftentry.ApplyEntryID{}, false
}

type testCreateCollectionMetaOptions struct {
	version            uint64
	documentFormat     uint64
	dataRootStorage    uint64
	indexStateStorage  uint64
	includeIndex       bool
	indexValueType     uint64
	indexStoragePolicy uint64
}

func deterministicCreateCollectionEntry(t *testing.T, collection, idempotency string, opts testCreateCollectionMetaOptions) []byte {
	t.Helper()
	return deterministicCreateCollectionEntryWithCatalogVersion(t, collection, idempotency, testCatalogVersionStart, opts)
}

func deterministicCreateCollectionEntryWithCatalogVersion(t *testing.T, collection, idempotency string, catalogVersion uint64, opts testCreateCollectionMetaOptions) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandCreateCollection, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionMeta, Bytes: testCreateCollectionMetaPayload(collection, opts)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
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
	version := opts.version
	if version == 0 {
		version = 1
	}
	dst := binary.AppendUvarint(nil, version) // collection_meta version
	dst = appendTestString(dst, collection)   // name
	dst = binary.AppendUvarint(dst, opts.documentFormat)
	dst = binary.AppendUvarint(dst, opts.dataRootStorage)   // data_root_storage_policy
	dst = binary.AppendUvarint(dst, opts.indexStateStorage) // index_state_storage_policy
	dst = append(dst, 0)                                    // allow_array_values_in_index
	dst = append(dst, 0)                                    // disable_indexed_write_memtables
	dst = append(dst, 0)                                    // buffered_indexed_writes
	dst = binary.AppendVarint(dst, 0)                       // buffered_indexed_write_max_documents
	dst = binary.AppendVarint(dst, 0)                       // buffered_indexed_write_max_bytes
	dst = binary.AppendVarint(dst, 0)                       // buffered_indexed_write_max_root_runs
	dst = append(dst, 0)                                    // buffered_indexed_async_flush
	dst = append(dst, 0)                                    // buffered_indexed_overlay_roots
	dst = binary.AppendVarint(dst, 0)                       // buffered_indexed_async_flush_max_queued_units
	if !opts.includeIndex {
		dst = binary.AppendUvarint(dst, 0) // index_count
	} else {
		valueType := opts.indexValueType
		if valueType == 0 {
			valueType = 1
		}
		dst = binary.AppendUvarint(dst, 1) // index_count
		dst = appendTestString(dst, "email")
		dst = appendTestString(dst, "email")
		dst = binary.AppendUvarint(dst, valueType)
		dst = append(dst, 0) // unique
		dst = append(dst, 0) // multi_key
		dst = binary.AppendUvarint(dst, opts.indexStoragePolicy)
	}
	if version >= 2 {
		dst = binary.AppendUvarint(dst, 0) // vector_index_count
	}
	return dst
}

func appendTestString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func applyCreateSequence(t *testing.T, db *backenddb.DB, entries ...[]byte) []raftentry.ApplyResultV1 {
	t.Helper()
	return applyCreateSequenceWithOptions(t, db, 1, true, entries...)
}

func applyCreateSequenceFrom(t *testing.T, db *backenddb.DB, startIndex uint64, entries ...[]byte) []raftentry.ApplyResultV1 {
	t.Helper()
	return applyCreateSequenceWithOptions(t, db, startIndex, startIndex == 1, entries...)
}

func applyCreateSequenceWithOptions(t *testing.T, db *backenddb.DB, startIndex uint64, enforceProgress bool, entries ...[]byte) []raftentry.ApplyResultV1 {
	t.Helper()
	results := NewMemoryApplyResultStore(len(entries) + 8)
	var progress ApplyProgressStore
	if enforceProgress {
		progress = NewMemoryApplyProgressStore(len(entries)+8, startIndex+uint64(len(entries))+8)
	}
	out := make([]raftentry.ApplyResultV1, 0, len(entries))
	for i, entry := range entries {
		index := startIndex + uint64(i)
		meta := applyMetaWithCatalogVersion(1, index, expectedCatalogVersionFromEntry(t, entry, index))
		result, err := ApplyCommittedEntryV1(db, entry, meta, Options{
			ProgressStore: progress,
			ResultStore:   results,
		})
		if err != nil {
			t.Fatalf("ApplyCommittedEntryV1 index %d: %v result=%+v", index, err, result)
		}
		if result.Status != raftentry.ApplyStatusApplied {
			t.Fatalf("ApplyCommittedEntryV1 index %d status=%s, want applied", index, result.Status)
		}
		out = append(out, result)
	}
	return out
}

func expectedCatalogVersionFromEntry(t *testing.T, entry []byte, index uint64) uint64 {
	t.Helper()
	decoded, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{
		ApplyEntryID: raftentry.ApplyEntryID{Term: 1, Index: index},
	})
	if err != nil {
		t.Fatalf("DecodeCommandEntryV1: %v", err)
	}
	version, err := decodeExpectedCatalogVersionV1(decoded.Target.ExpectedCatalogVersion)
	if err != nil {
		t.Fatalf("decodeExpectedCatalogVersionV1: %v", err)
	}
	return version
}

func readHexFixture(t *testing.T, rel string) []byte {
	t.Helper()
	raw, err := os.ReadFile(rel)
	if err != nil {
		t.Fatalf("read fixture %s: %v", rel, err)
	}
	hexText := strings.Join(strings.Fields(string(raw)), "")
	out, err := hex.DecodeString(hexText)
	if err != nil {
		t.Fatalf("decode fixture %s: %v", rel, err)
	}
	return out
}

func readCommandWALFrames(t *testing.T, dir string) []commitlog.CommandEnvelope {
	t.Helper()
	walDir := backenddb.WALDirPath(dir)
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("ReadDir %s: %v", walDir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && commitlog.IsCommandSegmentName(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	var frames []commitlog.CommandEnvelope
	for _, name := range names {
		path := filepath.Join(walDir, name)
		r, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatalf("NewReader %s: %v", name, err)
		}
		for {
			env, err := r.ReadCommandFrame()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				_ = r.Close()
				t.Fatalf("ReadCommandFrame %s: %v", name, err)
			}
			frames = append(frames, env)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("Close reader %s: %v", name, err)
		}
	}
	return frames
}

type countingCommandWALApplySeam struct {
	appendCalls   int
	finalizeCalls int
}

func (s *countingCommandWALApplySeam) Append(db *backenddb.DB, frame commandwalapply.LoweredFrame, meta commandwalapply.ApplyMetadata, opts commandwalapply.Options) (commandwalapply.Handle, commandwalapply.Result, error) {
	s.appendCalls++
	return commandwalapply.Handle{}, commandwalapply.Result{}, errors.New("unexpected append")
}

func (s *countingCommandWALApplySeam) Finalize(db *backenddb.DB, handle commandwalapply.Handle, meta commandwalapply.ApplyMetadata, opts commandwalapply.Options) (commandwalapply.Result, error) {
	s.finalizeCalls++
	return commandwalapply.Result{}, errors.New("unexpected finalize")
}

func (s *countingCommandWALApplySeam) Abort(db *backenddb.DB, handle commandwalapply.Handle) {}
