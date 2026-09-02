package raftapply

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	"go.mongodb.org/mongo-driver/v2/bson"
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
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_index_entry.hex")

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
	if len(frames) != 1 {
		t.Fatalf("command WAL frames after idempotency replay=%d, want 1", len(frames))
	}
	if got := db.State().AppliedCommandLSN; got != frames[0].LSN {
		t.Fatalf("AppliedCommandLSN=%d, want original catalog frame LSN %d", got, frames[0].LSN)
	}
	if progress.Len() != 2 || results.Len() != 2 {
		t.Fatalf("store lengths after duplicate progress=%d results=%d, want 2/2", progress.Len(), results.Len())
	}
}

func TestIdempotencyDuplicateProgressFailureRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:duplicate-progress-failure", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	beforeFrames := readCommandWALFrames(t, dir)
	if len(beforeFrames) != 1 {
		t.Fatalf("seed command WAL frames=%d, want 1", len(beforeFrames))
	}

	duplicate, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 2), Options{
		ProgressStore: recordProgressStoreFailAfterPreflight{},
		ResultStore:   results,
	})
	assertRecoveryRequired(t, duplicate, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if results.Len() != 2 {
		t.Fatalf("result records after duplicate progress failure=%d, want 2", results.Len())
	}
	if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
		t.Fatalf("command WAL frames after duplicate progress failure=%d, want %d", got, len(beforeFrames))
	}
}

func TestIdempotencyDuplicateResultStoreFailureRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:duplicate-result-failure", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	beforeFrames := readCommandWALFrames(t, dir)
	if len(beforeFrames) != 1 {
		t.Fatalf("seed command WAL frames=%d, want 1", len(beforeFrames))
	}

	duplicate, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 2), Options{
		ProgressStore: progress,
		ResultStore:   recordDuplicateApplyResultStoreFailAfterPreflight{base: results},
	})
	assertRecoveryRequired(t, duplicate, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if results.Len() != 1 {
		t.Fatalf("result records after duplicate result failure=%d, want 1", results.Len())
	}
	if progress.Len() != 1 {
		t.Fatalf("progress records after duplicate result failure=%d, want 1", progress.Len())
	}
	if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
		t.Fatalf("command WAL frames after duplicate result failure=%d, want %d", got, len(beforeFrames))
	}
}

func TestIdempotencyDuplicateRecordsCurrentAppliedCommandLSN(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	rawUsers := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:duplicate-current-lsn", testCreateCollectionMetaOptions{})
	first, err := ApplyCommittedEntryV1(db, rawUsers, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, first, raftentry.ApplyStatusApplied, 1)
	frames := readCommandWALFrames(t, dir)
	if len(frames) != 1 {
		t.Fatalf("seed command WAL frames=%d, want 1", len(frames))
	}
	originalLSN := frames[0].LSN

	rawOrders := deterministicCreateCollectionEntry(t, "orders", "client-a:create:orders:advance-lsn", testCreateCollectionMetaOptions{})
	advanced, err := ApplyCommittedEntryV1(db, rawOrders, applyMeta(1, 2), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, advanced, raftentry.ApplyStatusApplied, 1)
	currentLSN := db.State().AppliedCommandLSN
	if currentLSN <= originalLSN {
		t.Fatalf("AppliedCommandLSN after intervening command=%d, want greater than original %d", currentLSN, originalLSN)
	}

	duplicateID := raftentry.ApplyEntryID{Term: 1, Index: 3}
	duplicate, err := ApplyCommittedEntryV1(db, rawUsers, applyMeta(duplicateID.Term, duplicateID.Index), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, duplicate, raftentry.ApplyStatusAlreadyApplied, 0)
	if duplicate.ResultDigest != first.ResultDigest {
		t.Fatalf("duplicate result digest=%s, want %s", duplicate.ResultDigest.Hex(), first.ResultDigest.Hex())
	}
	record, ok, err := results.LookupApplyResult(duplicateID)
	if err != nil {
		t.Fatalf("LookupApplyResult duplicate: %v", err)
	}
	if !ok {
		t.Fatal("duplicate result record missing")
	}
	if record.AppliedCommandLSN != currentLSN {
		t.Fatalf("duplicate result AppliedCommandLSN=%d, want current %d", record.AppliedCommandLSN, currentLSN)
	}
	progress.mu.Lock()
	progressRecord, ok := progress.records[duplicateID]
	progress.mu.Unlock()
	if !ok {
		t.Fatal("duplicate progress record missing")
	}
	if progressRecord.AppliedCommandLSN != currentLSN {
		t.Fatalf("duplicate progress AppliedCommandLSN=%d, want current %d", progressRecord.AppliedCommandLSN, currentLSN)
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
	results := NewMemoryApplyResultStore(8)

	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(id.Term, id.Index), Options{
		ResultStore: results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 seed result: %v result=%+v", err, result)
	}
	laterID := raftentry.ApplyEntryID{Term: 1, Index: 2}
	laterRaw := deterministicCreateCollectionEntry(t, "orders", "client-a:create:orders", testCreateCollectionMetaOptions{})
	later, err := ApplyCommittedEntryV1(db, laterRaw, applyMeta(laterID.Term, laterID.Index), Options{
		ResultStore: results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 later result: %v result=%+v", err, later)
	}
	if later.ResultDigest == result.ResultDigest {
		t.Fatalf("later result digest=%s unexpectedly equals first boundary", later.ResultDigest.Hex())
	}
	beforeFrames := readCommandWALFrames(t, dir)
	if len(beforeFrames) != 2 {
		t.Fatalf("seed command WAL frames=%d, want 2", len(beforeFrames))
	}
	progress := NewMemoryApplyProgressStore(8, 8)

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
	progressRecord, ok, err := progress.LookupApplyProgress(id)
	if err != nil || !ok {
		t.Fatalf("LookupApplyProgress stored result=(%+v,%t,%v), want repaired progress", progressRecord, ok, err)
	}
	if progressRecord.LogicalDigestV1 != LogicalDigestV1(result.ResultDigest) {
		t.Fatalf("stored result progress digest=%s, want stored boundary %s", progressRecord.LogicalDigestV1.Hex(), result.ResultDigest.Hex())
	}
	if progressRecord.LogicalDigestV1 == LogicalDigestV1(later.ResultDigest) {
		t.Fatalf("stored result progress digest=%s used later DB boundary", progressRecord.LogicalDigestV1.Hex())
	}
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("stored result replay reached command WAL seam append=%d finalize=%d, want 0/0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
		t.Fatalf("command WAL frames after stored result replay=%d, want %d", got, len(beforeFrames))
	}
}

func TestStoredDuplicateResultReplayRecordsBoundaryProgressDigest(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	rawUsers := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:stored-duplicate-replay", testCreateCollectionMetaOptions{})
	first, err := ApplyCommittedEntryV1(db, rawUsers, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, first, raftentry.ApplyStatusApplied, 1)

	rawOrders := deterministicCreateCollectionEntry(t, "orders", "client-a:create:orders:stored-duplicate-replay", testCreateCollectionMetaOptions{})
	advanced, err := ApplyCommittedEntryV1(db, rawOrders, applyMeta(1, 2), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, advanced, raftentry.ApplyStatusApplied, 1)
	if advanced.ResultDigest == first.ResultDigest {
		t.Fatalf("advanced result digest=%s unexpectedly equals original boundary", advanced.ResultDigest.Hex())
	}
	beforeFrames := readCommandWALFrames(t, dir)
	if len(beforeFrames) != 2 {
		t.Fatalf("seed command WAL frames=%d, want 2", len(beforeFrames))
	}

	duplicateID := raftentry.ApplyEntryID{Term: 1, Index: 3}
	failed, err := ApplyCommittedEntryV1(db, rawUsers, applyMeta(duplicateID.Term, duplicateID.Index), Options{
		ProgressStore: recordProgressStoreFailAfterPreflight{},
		ResultStore:   results,
	})
	assertRecoveryRequired(t, failed, err, raftentry.ErrorUnsafeDurabilityModeV1)
	record, ok, err := results.LookupApplyResult(duplicateID)
	if err != nil || !ok {
		t.Fatalf("LookupApplyResult duplicate=(%+v,%t,%v), want stored duplicate result", record, ok, err)
	}
	if record.Result.ResultDigest != first.ResultDigest {
		t.Fatalf("stored duplicate result digest=%s, want original result boundary %s", record.Result.ResultDigest.Hex(), first.ResultDigest.Hex())
	}
	if record.ProgressLogicalDigestV1 != LogicalDigestV1(advanced.ResultDigest) {
		t.Fatalf("stored duplicate progress digest=%s, want duplicate boundary %s", record.ProgressLogicalDigestV1.Hex(), advanced.ResultDigest.Hex())
	}
	if progress.Len() != 2 {
		t.Fatalf("progress records after duplicate progress failure=%d, want original two records", progress.Len())
	}

	seam := &countingCommandWALApplySeam{}
	replayed, err := ApplyCommittedEntryV1(db, rawUsers, applyMeta(duplicateID.Term, duplicateID.Index), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 stored duplicate replay: %v result=%+v", err, replayed)
	}
	if replayed != record.Result {
		t.Fatalf("stored duplicate replay=%+v, want stored duplicate %+v", replayed, record.Result)
	}
	progressRecord, ok, err := progress.LookupApplyProgress(duplicateID)
	if err != nil || !ok {
		t.Fatalf("LookupApplyProgress duplicate=(%+v,%t,%v), want repaired progress", progressRecord, ok, err)
	}
	if progressRecord.LogicalDigestV1 != LogicalDigestV1(advanced.ResultDigest) {
		t.Fatalf("repaired duplicate progress digest=%s, want duplicate boundary %s", progressRecord.LogicalDigestV1.Hex(), advanced.ResultDigest.Hex())
	}
	if progressRecord.LogicalDigestV1 == LogicalDigestV1(first.ResultDigest) {
		t.Fatalf("repaired duplicate progress digest=%s used original result boundary", progressRecord.LogicalDigestV1.Hex())
	}
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("stored duplicate replay reached command WAL seam append=%d finalize=%d, want 0/0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
		t.Fatalf("command WAL frames after stored duplicate replay=%d, want %d", got, len(beforeFrames))
	}
}

func TestStoredResultReplayProgressFailureRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	id := raftentry.ApplyEntryID{Term: 1, Index: 1}
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:stored-progress-failure", testCreateCollectionMetaOptions{})
	results := NewMemoryApplyResultStore(8)
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(id.Term, id.Index), Options{
		ResultStore: results,
	})
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	beforeFrames := readCommandWALFrames(t, dir)
	if len(beforeFrames) != 1 {
		t.Fatalf("seed command WAL frames=%d, want 1", len(beforeFrames))
	}

	replayed, err := ApplyCommittedEntryV1(db, raw, applyMeta(id.Term, id.Index), Options{
		ProgressStore: recordProgressStoreFailAfterPreflight{},
		ResultStore:   results,
	})
	assertRecoveryRequired(t, replayed, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if results.Len() != 1 {
		t.Fatalf("result records after stored result progress failure=%d, want 1", results.Len())
	}
	if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
		t.Fatalf("command WAL frames after stored result progress failure=%d, want %d", got, len(beforeFrames))
	}
}

func TestFaultBeforeLocalWALAppendDoesNotAdvanceMetadata(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:fault-before-append", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
		FaultInjector: singlePointFaultInjector{point: FaultBeforeLocalWALAppendV1},
	})
	assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorUnsafeDurabilityModeV1)
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after pre-append fault=%d, want 0", got)
	}
	if len(readCommandWALFrames(t, dir)) != 0 || progress.Len() != 0 || results.Len() != 0 {
		t.Fatalf("pre-append fault frames/progress/results=%d/%d/%d, want 0/0/0", len(readCommandWALFrames(t, dir)), progress.Len(), results.Len())
	}
	if _, openErr := collections.NewCollectionManager(db).OpenCollection("users"); !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after pre-append fault err=%v, want ErrCollectionNotFound", openErr)
	}
}

func TestFaultAfterLocalWALAppendBeforeVisibilityDoesNotAdvanceMetadata(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:fault-after-append", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
		FaultInjector: singlePointFaultInjector{point: FaultAfterLocalWALAppendBeforeVisibleV1},
	})
	assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after post-append/pre-visible fault=%d, want 0", got)
	}
	if progress.Len() != 0 || results.Len() != 0 {
		t.Fatalf("post-append/pre-visible fault progress/results=%d/%d, want 0/0", progress.Len(), results.Len())
	}
	if _, openErr := collections.NewCollectionManager(db).OpenCollection("users"); !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after post-append/pre-visible fault err=%v, want ErrCollectionNotFound", openErr)
	}
}

func TestCollectionMutationFaultAfterLocalWALAppendBeforeVisibilityRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	create := deterministicCreateCollectionEntry(t, "docs", "client-a:create:docs", testCreateCollectionMetaOptions{})
	createResult, err := ApplyCommittedEntryV1(db, create, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 create: %v result=%+v", err, createResult)
	}
	assertApplied(t, createResult, raftentry.ApplyStatusApplied, 1)
	beforeFrames := readCommandWALFrames(t, dir)
	beforeLSN := db.State().AppliedCommandLSN

	doc := []byte(`{"name":"one"}`)
	insert := deterministicInsertBatchEntry(t, "docs", "client-a:insert:docs:fault-after-append", nativewire.DocumentFormatJSON, [][]byte{[]byte("d1")}, [][]byte{doc})
	result, err := ApplyCommittedEntryV1(db, insert, applyMeta(1, 2), Options{
		ProgressStore: progress,
		ResultStore:   results,
		FaultInjector: singlePointFaultInjector{point: FaultAfterLocalWALAppendBeforeVisibleV1},
	})
	assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if got := db.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("AppliedCommandLSN after mutation post-append/pre-visible fault=%d, want %d", got, beforeLSN)
	}
	if progress.Len() != 1 || results.Len() != 1 {
		t.Fatalf("store lengths after mutation post-append/pre-visible fault progress=%d results=%d, want 1/1", progress.Len(), results.Len())
	}
	frames := readCommandWALFrames(t, dir)
	if len(frames) != len(beforeFrames)+1 {
		t.Fatalf("command WAL frames after mutation post-append/pre-visible fault=%d, want %d", len(frames), len(beforeFrames)+1)
	}
	assertCollectionInsertFrame(t, frames[len(frames)-1], "docs", map[string][]byte{"d1": doc})
	if err := db.CheckStorageMaintenanceReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("CheckStorageMaintenanceReady after mutation post-append/pre-visible fault error=%v, want ErrRecoveryRequired", err)
	}
}

func TestFaultAfterVisibleBeforeResultRecordRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:fault-visible", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
		FaultInjector: singlePointFaultInjector{point: FaultAfterVisibleBeforeResultRecordV1},
	})
	assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if _, openErr := collections.NewCollectionManager(db).OpenCollection("users"); openErr != nil {
		t.Fatalf("OpenCollection users after visible fault: %v", openErr)
	}
	if len(readCommandWALFrames(t, dir)) != 1 || progress.Len() != 0 || results.Len() != 0 {
		t.Fatalf("visible-before-result fault frames/progress/results=%d/%d/%d, want 1/0/0", len(readCommandWALFrames(t, dir)), progress.Len(), results.Len())
	}
}

func TestFaultAfterResultRecordBeforeProgressReplaysWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:fault-result", testCreateCollectionMetaOptions{})
	first, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
		FaultInjector: singlePointFaultInjector{point: FaultAfterResultRecordBeforeProgressV1},
	})
	assertRecoveryRequired(t, first, err, raftentry.ErrorUnsafeDurabilityModeV1)
	beforeFrames := readCommandWALFrames(t, dir)
	if len(beforeFrames) != 1 || progress.Len() != 0 || results.Len() != 1 {
		t.Fatalf("result-before-progress fault frames/progress/results=%d/%d/%d, want 1/0/1", len(beforeFrames), progress.Len(), results.Len())
	}

	replayed, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("replay after result-before-progress fault: %v", err)
	}
	assertApplied(t, replayed, raftentry.ApplyStatusApplied, 1)
	if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
		t.Fatalf("frames after result-store replay=%d, want %d", got, len(beforeFrames))
	}
	if progress.Len() != 1 || results.Len() != 1 {
		t.Fatalf("store lengths after result-store replay progress/results=%d/%d, want 1/1", progress.Len(), results.Len())
	}
}

func TestStoredResultCoverageCannotOutrunAppliedLSN(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	id := raftentry.ApplyEntryID{Term: 1, Index: 1}
	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users:uncovered-result", testCreateCollectionMetaOptions{})
	entry, err := raftentry.DecodeCommandEntryV1(raw, raftentry.DecodeOptions{ApplyEntryID: id})
	if err != nil {
		t.Fatalf("DecodeCommandEntryV1: %v", err)
	}
	results := NewMemoryApplyResultStore(8)
	if err := results.RecordApplyResult(ApplyResultRecordV1{
		EntryID:           id,
		CommandDigest:     entry.Digest,
		IdempotencyKey:    entry.IdempotencyKey,
		AppliedCommandLSN: 1,
		Result: raftentry.ApplyResultV1{
			Status:                 raftentry.ApplyStatusApplied,
			CommandDigest:          entry.Digest,
			DeterministicErrorCode: raftentry.ErrorNoneV1,
			AffectedCount:          1,
			ResultDigest:           entry.Digest,
		},
	}); err != nil {
		t.Fatalf("RecordApplyResult: %v", err)
	}
	progress := NewMemoryApplyProgressStore(8, 8)
	seam := &countingCommandWALApplySeam{}
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(id.Term, id.Index), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if progress.Len() != 0 || seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != 0 {
		t.Fatalf("uncovered result replay progress/append/frames=%d/%d/%d, want 0/0/0", progress.Len(), seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
}

func TestIdempotencyDifferentDigestFailsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	first := deterministicCreateCollectionEntry(t, "users", "client-a:create:shared-idempotency", testCreateCollectionMetaOptions{})
	applied, err := ApplyCommittedEntryV1(db, first, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, applied, raftentry.ApplyStatusApplied, 1)
	beforeFrames := readCommandWALFrames(t, dir)

	seam := &countingCommandWALApplySeam{}
	conflict := deterministicCreateCollectionEntry(t, "orders", "client-a:create:shared-idempotency", testCreateCollectionMetaOptions{})
	rejected, err := ApplyCommittedEntryV1(db, conflict, applyMetaWithCatalogVersion(1, 2, testCatalogVersionStart+1), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRejected(t, rejected, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	if seam.appendCalls != 0 || len(readCommandWALFrames(t, dir)) != len(beforeFrames) {
		t.Fatalf("idempotency conflict append/frames=%d/%d, want 0/%d", seam.appendCalls, len(readCommandWALFrames(t, dir)), len(beforeFrames))
	}
	if _, openErr := collections.NewCollectionManager(db).OpenCollection("orders"); !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection orders after idempotency conflict err=%v, want ErrCollectionNotFound", openErr)
	}
}

func TestCollectionMutationApplyInsertReplaceDeleteFramesAndDigest(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(16, 16)
	results := NewMemoryApplyResultStore(16)
	apply := func(index uint64, raw []byte) raftentry.ApplyResultV1 {
		t.Helper()
		result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, index), Options{
			ProgressStore: progress,
			ResultStore:   results,
		})
		if err != nil {
			t.Fatalf("ApplyCommittedEntryV1 index %d: %v result=%+v", index, err, result)
		}
		return result
	}

	create := deterministicCreateCollectionEntry(t, "users", "client-a:create:users-bson", testCreateCollectionMetaOptions{
		documentFormat: uint64(nativewire.DocumentFormatBSON),
	})
	docU1 := testBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}})
	docU2 := testBSONDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "sea"}})
	insert := deterministicInsertBatchEntry(t, "users", "client-a:insert:users:1", nativewire.DocumentFormatBSON, [][]byte{[]byte("u2"), []byte("u1")}, [][]byte{docU2, docU1})
	docU1Replace := testBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "sfo"}})
	docMissing := testBSONDocument(t, bson.D{{Key: "_id", Value: "missing"}, {Key: "city", Value: "nyc"}})
	replace := deterministicReplaceBatchEntry(t, "users", "client-a:replace:users:1", nativewire.DocumentFormatBSON, [][]byte{[]byte("u1"), []byte("missing")}, [][]byte{docU1Replace, docMissing})
	deleteEntry := deterministicDeleteBatchEntry(t, "users", "client-a:delete:users:1", [][]byte{[]byte("u2")})

	assertApplied(t, apply(1, create), raftentry.ApplyStatusApplied, 1)
	insertResult := apply(2, insert)
	assertApplied(t, insertResult, raftentry.ApplyStatusApplied, 2)
	replaceResult := apply(3, replace)
	assertApplied(t, replaceResult, raftentry.ApplyStatusApplied, 1)
	if replaceResult.MatchedCount != 1 {
		t.Fatalf("replace matched=%d want 1", replaceResult.MatchedCount)
	}
	deleteResult := apply(4, deleteEntry)
	assertApplied(t, deleteResult, raftentry.ApplyStatusApplied, 1)
	if insertResult.ResultDigest == replaceResult.ResultDigest || replaceResult.ResultDigest == deleteResult.ResultDigest {
		t.Fatalf("mutation result digests did not change across document mutations insert=%s replace=%s delete=%s", insertResult.ResultDigest.Hex(), replaceResult.ResultDigest.Hex(), deleteResult.ResultDigest.Hex())
	}

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

	frames := readCommandWALFrames(t, dir)
	if len(frames) != 4 {
		t.Fatalf("command WAL frames=%d, want 4", len(frames))
	}
	assertCatalogCreateFrame(t, frames[0], "users")
	assertCollectionInsertFrame(t, frames[1], "users", map[string][]byte{
		"u1": docU1,
		"u2": docU2,
	})
	assertCollectionUpdateFrame(t, frames[2], "users", map[string][]byte{
		"u1": docU1Replace,
	})
	assertCollectionDeleteFrame(t, frames[3], "users", [][]byte{[]byte("u2")})
	if got := db.State().AppliedCommandLSN; got != frames[3].LSN {
		t.Fatalf("AppliedCommandLSN=%d, want delete frame LSN %d", got, frames[3].LSN)
	}
	if progress.Len() != 4 || results.Len() != 4 {
		t.Fatalf("store lengths progress=%d results=%d, want 4/4", progress.Len(), results.Len())
	}
}

func TestCollectionMutationCommitAmbiguousAfterPublishRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	create := deterministicCreateCollectionEntry(t, "docs", "client-a:create:docs", testCreateCollectionMetaOptions{
		version:            testCurrentCollectionMetaWireVersion,
		includeVectorIndex: true,
	})
	createResult, err := ApplyCommittedEntryV1(db, create, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 create: %v result=%+v", err, createResult)
	}
	assertApplied(t, createResult, raftentry.ApplyStatusApplied, 1)

	collection, err := collections.NewCollectionManager(db).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection docs: %v", err)
	}
	beforeLSN := db.State().AppliedCommandLSN

	badVectorDoc := []byte(`{"embedding":[1,0,0]}`)
	insert := deterministicInsertBatchEntry(t, "docs", "client-a:insert:docs:bad-vector", nativewire.DocumentFormatJSON, [][]byte{[]byte("bad")}, [][]byte{badVectorDoc})
	result, err := ApplyCommittedEntryV1(db, insert, applyMeta(1, 2), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if !errors.Is(err, collections.ErrCommitAmbiguous) {
		t.Fatalf("ApplyCommittedEntryV1 err=%v, want ErrCommitAmbiguous", err)
	}
	if progress.Len() != 1 || results.Len() != 1 {
		t.Fatalf("store lengths after recovery-required mutation progress=%d results=%d, want 1/1", progress.Len(), results.Len())
	}
	got, err := collection.Get([]byte("bad"))
	if err != nil {
		t.Fatalf("Get committed bad-vector document: %v", err)
	}
	if !bytes.Equal(got, badVectorDoc) {
		t.Fatalf("committed bad-vector document=%s, want %s", got, badVectorDoc)
	}
	frames := readCommandWALFrames(t, dir)
	if len(frames) == 0 {
		t.Fatal("command WAL frames=0, want inserted mutation frame")
	}
	last := frames[len(frames)-1]
	assertCollectionInsertFrame(t, last, "docs", map[string][]byte{"bad": badVectorDoc})
	if got := db.State().AppliedCommandLSN; got <= beforeLSN || got != last.LSN {
		t.Fatalf("AppliedCommandLSN=%d before=%d last_insert_lsn=%d", got, beforeLSN, last.LSN)
	}
}

func TestCollectionMutationMalformedJSONFailsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	create := deterministicCreateCollectionEntry(t, "docs", "client-a:create:docs-indexed-json", testCreateCollectionMetaOptions{
		includeIndex: true,
	})
	createResult, err := ApplyCommittedEntryV1(db, create, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 create: %v result=%+v", err, createResult)
	}
	assertApplied(t, createResult, raftentry.ApplyStatusApplied, 1)
	cases := []struct {
		name string
		doc  []byte
	}{
		{name: "invalid-syntax", doc: []byte(`{"email":`)},
		{name: "array", doc: []byte(`[]`)},
		{name: "scalar", doc: []byte(`1`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			beforeFrames := readCommandWALFrames(t, dir)
			beforeLSN := db.State().AppliedCommandLSN
			seam := &countingCommandWALApplySeam{}
			id := []byte("bad-" + tc.name)
			insert := deterministicInsertBatchEntry(t, "docs", "client-a:insert:docs:bad-json:"+tc.name, nativewire.DocumentFormatJSON, [][]byte{id}, [][]byte{tc.doc})
			result, err := ApplyCommittedEntryV1(db, insert, applyMeta(1, 2), Options{
				ProgressStore:       progress,
				ResultStore:         results,
				CommandWALApplySeam: seam,
			})
			assertRejected(t, result, err, raftentry.ApplyStatusRejectedMalformed, raftentry.ErrorMalformedEntryV1)
			if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
				t.Fatalf("malformed JSON reached command WAL seam append=%d finalize=%d, want 0/0", seam.appendCalls, seam.finalizeCalls)
			}
			if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
				t.Fatalf("command WAL frames after malformed JSON=%d, want %d", got, len(beforeFrames))
			}
			if got := db.State().AppliedCommandLSN; got != beforeLSN {
				t.Fatalf("AppliedCommandLSN after malformed JSON=%d, want %d", got, beforeLSN)
			}
			if progress.Len() != 1 || results.Len() != 1 {
				t.Fatalf("store lengths after malformed JSON progress=%d results=%d, want 1/1", progress.Len(), results.Len())
			}
			if got, getErr := collections.NewCollectionManager(db).OpenCollection("docs"); getErr != nil {
				t.Fatalf("OpenCollection docs after malformed JSON: %v", getErr)
			} else if doc, getErr := got.Get(id); getErr != nil || doc != nil {
				t.Fatalf("Get bad after malformed JSON doc=%q err=%v, want nil/<nil>", doc, getErr)
			}
		})
	}
}

func TestCollectionMutationUniqueConflictFailsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	create := deterministicCreateCollectionEntry(t, "docs", "client-a:create:docs-unique-json", testCreateCollectionMetaOptions{
		includeIndex: true,
		indexUnique:  true,
	})
	createResult, err := ApplyCommittedEntryV1(db, create, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 create: %v result=%+v", err, createResult)
	}
	assertApplied(t, createResult, raftentry.ApplyStatusApplied, 1)
	seed := deterministicInsertBatchEntry(t, "docs", "client-a:insert:docs:seed", nativewire.DocumentFormatJSON, [][]byte{[]byte("u1")}, [][]byte{[]byte(`{"email":"same@example.com","city":"hnl"}`)})
	seedResult, err := ApplyCommittedEntryV1(db, seed, applyMeta(1, 2), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 seed: %v result=%+v", err, seedResult)
	}
	assertApplied(t, seedResult, raftentry.ApplyStatusApplied, 1)
	beforeFrames := readCommandWALFrames(t, dir)
	beforeLSN := db.State().AppliedCommandLSN

	seam := &countingCommandWALApplySeam{}
	conflicting := deterministicInsertBatchEntry(t, "docs", "client-a:insert:docs:unique-conflict", nativewire.DocumentFormatJSON, [][]byte{[]byte("u2")}, [][]byte{[]byte(`{"email":"same@example.com","city":"sea"}`)})
	result, err := ApplyCommittedEntryV1(db, conflicting, applyMeta(1, 3), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	if !errors.Is(err, collections.ErrUniqueIndexConflict) {
		t.Fatalf("ApplyCommittedEntryV1 err=%v, want ErrUniqueIndexConflict", err)
	}
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("unique conflict reached command WAL seam append=%d finalize=%d, want 0/0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
		t.Fatalf("command WAL frames after unique conflict=%d, want %d", got, len(beforeFrames))
	}
	if got := db.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("AppliedCommandLSN after unique conflict=%d, want %d", got, beforeLSN)
	}
	opened, err := collections.NewCollectionManager(db).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection docs after unique conflict: %v", err)
	}
	got, err := opened.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("Get u2 after unique conflict: %v", err)
	}
	if got != nil {
		t.Fatalf("u2 after unique conflict=%s, want nil", got)
	}
	if progress.Len() != 2 || results.Len() != 2 {
		t.Fatalf("store lengths after unique conflict progress=%d results=%d, want 2/2", progress.Len(), results.Len())
	}
}

func TestCollectionMutationReplaceUniqueConflictFailsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	create := deterministicCreateCollectionEntry(t, "docs", "client-a:create:docs-replace-unique-json", testCreateCollectionMetaOptions{
		includeIndex: true,
		indexUnique:  true,
	})
	createResult, err := ApplyCommittedEntryV1(db, create, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 create: %v result=%+v", err, createResult)
	}
	assertApplied(t, createResult, raftentry.ApplyStatusApplied, 1)
	seed := deterministicInsertBatchEntry(t, "docs", "client-a:insert:docs:seed-replace", nativewire.DocumentFormatJSON, [][]byte{[]byte("u1"), []byte("u2")}, [][]byte{
		[]byte(`{"email":"one@example.com","city":"hnl"}`),
		[]byte(`{"email":"two@example.com","city":"sea"}`),
	})
	seedResult, err := ApplyCommittedEntryV1(db, seed, applyMeta(1, 2), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 seed: %v result=%+v", err, seedResult)
	}
	assertApplied(t, seedResult, raftentry.ApplyStatusApplied, 2)
	beforeFrames := readCommandWALFrames(t, dir)
	beforeLSN := db.State().AppliedCommandLSN

	seam := &countingCommandWALApplySeam{}
	conflicting := deterministicReplaceBatchEntry(t, "docs", "client-a:replace:docs:unique-conflict", nativewire.DocumentFormatJSON, [][]byte{[]byte("u2")}, [][]byte{
		[]byte(`{"email":"one@example.com","city":"sfo"}`),
	})
	result, err := ApplyCommittedEntryV1(db, conflicting, applyMeta(1, 3), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	if !errors.Is(err, collections.ErrUniqueIndexConflict) {
		t.Fatalf("ApplyCommittedEntryV1 err=%v, want ErrUniqueIndexConflict", err)
	}
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("replace unique conflict reached command WAL seam append=%d finalize=%d, want 0/0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
		t.Fatalf("command WAL frames after replace unique conflict=%d, want %d", got, len(beforeFrames))
	}
	if got := db.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("AppliedCommandLSN after replace unique conflict=%d, want %d", got, beforeLSN)
	}
	opened, err := collections.NewCollectionManager(db).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection docs after replace unique conflict: %v", err)
	}
	got, err := opened.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("Get u2 after replace unique conflict: %v", err)
	}
	if !bytes.Contains(got, []byte(`two@example.com`)) || bytes.Contains(got, []byte(`one@example.com`)) {
		t.Fatalf("u2 after replace unique conflict=%s, want original unique email", got)
	}
	if progress.Len() != 2 || results.Len() != 2 {
		t.Fatalf("store lengths after replace unique conflict progress=%d results=%d, want 2/2", progress.Len(), results.Len())
	}
}

func TestCollectionMutationCoveredCommandWALFailureRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	frame, err := commandwalapply.TestNoopFrame()
	if err != nil {
		t.Fatalf("TestNoopFrame: %v", err)
	}
	handle, _, err := commandwalapply.Append(db, frame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := commandwalapply.Finalize(db, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	entry := raftentry.CommandEntryV1{Digest: raftentry.CommandDigestV1{1, 2, 3}}
	result, err := NewHarness(db, Options{}).collectionMutationApplyError(entry, handle, fmt.Errorf("publish failed: %w", backenddb.ErrRecoveryRequired))
	assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
}

func TestCollectionMutationFinalizeFailureAfterAppendRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	create := deterministicCreateCollectionEntry(t, "docs", "client-a:create:docs-finalize-failure", testCreateCollectionMetaOptions{})
	createResult, err := ApplyCommittedEntryV1(db, create, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 create: %v result=%+v", err, createResult)
	}
	assertApplied(t, createResult, raftentry.ApplyStatusApplied, 1)
	beforeFrames := readCommandWALFrames(t, dir)
	beforeLSN := db.State().AppliedCommandLSN

	seam := &failingFinalizeCommandWALApplySeam{
		finalizeErr: fmt.Errorf("synthetic finalize failure: %w", backenddb.ErrCommandWALRejected),
	}
	emptyInsert := deterministicInsertBatchEntry(t, "docs", "client-a:insert:docs:empty-finalize-failure", nativewire.DocumentFormatJSON, nil, nil)
	result, err := ApplyCommittedEntryV1(db, emptyInsert, applyMeta(1, 2), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if seam.appendCalls != 1 || seam.finalizeCalls != 1 || seam.abortCalls != 1 {
		t.Fatalf("seam calls append/finalize/abort=%d/%d/%d, want 1/1/1", seam.appendCalls, seam.finalizeCalls, seam.abortCalls)
	}
	frames := readCommandWALFrames(t, dir)
	if len(frames) != len(beforeFrames)+1 {
		t.Fatalf("command WAL frames after finalize failure=%d, want %d", len(frames), len(beforeFrames)+1)
	}
	last := frames[len(frames)-1]
	assertCollectionInsertFrame(t, last, "docs", map[string][]byte{})
	if got := db.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("AppliedCommandLSN after finalize failure=%d, want %d", got, beforeLSN)
	}
	if progress.Len() != 1 || results.Len() != 1 {
		t.Fatalf("store lengths after finalize failure progress=%d results=%d, want 1/1", progress.Len(), results.Len())
	}
	if err := db.CheckStorageMaintenanceReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("CheckStorageMaintenanceReady after finalize failure error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCollectionMutationStaleCatalogGuardFailsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	create := deterministicCreateCollectionEntry(t, "users", "client-a:create:users-json", testCreateCollectionMetaOptions{})
	applied, err := ApplyCommittedEntryV1(db, create, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertApplied(t, applied, raftentry.ApplyStatusApplied, 1)
	beforeFrames := readCommandWALFrames(t, dir)
	beforeLSN := db.State().AppliedCommandLSN

	seam := &countingCommandWALApplySeam{}
	insert := deterministicInsertBatchEntry(t, "users", "client-a:insert:users-stale", nativewire.DocumentFormatJSON, [][]byte{[]byte("u1")}, [][]byte{[]byte(`{"city":"hnl"}`)})
	rejected, err := ApplyCommittedEntryV1(db, insert, applyMetaWithCatalogVersion(1, 2, testCatalogVersionStart+1), Options{
		ProgressStore:       progress,
		ResultStore:         results,
		CommandWALApplySeam: seam,
	})
	assertRejected(t, rejected, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("stale mutation guard reached command-WAL seam append=%d finalize=%d, want 0/0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != len(beforeFrames) {
		t.Fatalf("command WAL frames after stale mutation guard=%d, want %d", got, len(beforeFrames))
	}
	if got := db.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("AppliedCommandLSN after stale mutation guard=%d, want %d", got, beforeLSN)
	}
	opened, err := collections.NewCollectionManager(db).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users: %v", err)
	}
	got, err := opened.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1 after stale mutation guard: %v", err)
	}
	if got != nil {
		t.Fatalf("u1 after stale mutation guard=%s, want nil", got)
	}
	if progress.Len() != 1 || results.Len() != 1 {
		t.Fatalf("store lengths after stale mutation guard progress=%d results=%d, want 1/1", progress.Len(), results.Len())
	}
}

func TestUpdateBSONSetApplyReplayNoopAffectedCountAndLogicalDigest(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	id := []byte("u1")
	baseDoc := testBSONDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "city", Value: "hnl"},
		{Key: "visits", Value: int32(1)},
	})
	updatedDoc := testBSONDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "city", Value: "sfo"},
		{Key: "visits", Value: int32(1)},
	})
	create := deterministicCreateCollectionEntry(t, "users", "client-a:create:users-bson-set", testCreateCollectionMetaOptions{
		documentFormat: uint64(nativewire.DocumentFormatBSON),
	})
	insert := deterministicInsertBatchEntry(t, "users", "client-a:insert:users-bson-set", nativewire.DocumentFormatBSON, [][]byte{id}, [][]byte{baseDoc})
	applyCreateSequence(t, db, create, insert)
	if frames := readCommandWALFrames(t, dir); len(frames) != 2 {
		t.Fatalf("seed command WAL frames=%d, want 2", len(frames))
	}

	results := NewMemoryApplyResultStore(16)
	update := deterministicUpdateBSONSetEntry(t, "users", "client-a:update-bson-set:city", id, []collections.BSONSetField{
		{Key: "city", Value: testBSONSetRawValue(t, "sfo")},
	})
	applied, err := ApplyCommittedEntryV1(db, update, applyMeta(1, 3), Options{ResultStore: results})
	assertApplied(t, applied, raftentry.ApplyStatusApplied, 1)
	if applied.MatchedCount != 1 {
		t.Fatalf("update matched=%d want 1", applied.MatchedCount)
	}
	frames := readCommandWALFrames(t, dir)
	if len(frames) != 3 {
		t.Fatalf("command WAL frames after update=%d, want 3", len(frames))
	}
	assertCollectionUpdateFrame(t, frames[2], "users", map[string][]byte{"u1": updatedDoc})
	opened, err := collections.NewCollectionManager(db).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users: %v", err)
	}
	got, err := opened.Get(id)
	if err != nil {
		t.Fatalf("Get u1 after update: %v", err)
	}
	if gotCity := bson.Raw(got).Lookup("city").StringValue(); gotCity != "sfo" {
		t.Fatalf("city after update=%q, want sfo", gotCity)
	}

	replayed, err := ApplyCommittedEntryV1(db, update, applyMeta(1, 3), Options{ResultStore: results})
	if err != nil {
		t.Fatalf("same ApplyEntryID replay: %v", err)
	}
	if replayed != applied {
		t.Fatalf("same ApplyEntryID replay result=%+v, want %+v", replayed, applied)
	}
	duplicate, err := ApplyCommittedEntryV1(db, update, applyMeta(1, 4), Options{ResultStore: results})
	assertApplied(t, duplicate, raftentry.ApplyStatusAlreadyApplied, 0)
	if duplicate.MatchedCount != 0 {
		t.Fatalf("duplicate matched=%d want 0", duplicate.MatchedCount)
	}
	if duplicate.ResultDigest != applied.ResultDigest {
		t.Fatalf("duplicate digest=%s, want %s", duplicate.ResultDigest.Hex(), applied.ResultDigest.Hex())
	}
	if got := len(readCommandWALFrames(t, dir)); got != 3 {
		t.Fatalf("command WAL frames after replay/idempotency=%d, want 3", got)
	}

	noOp := deterministicUpdateBSONSetEntry(t, "users", "client-a:update-bson-set:no-op", id, []collections.BSONSetField{
		{Key: "city", Value: testBSONSetRawValue(t, "sfo")},
	})
	noOpResult, err := ApplyCommittedEntryV1(db, noOp, applyMeta(1, 5), Options{ResultStore: results})
	assertApplied(t, noOpResult, raftentry.ApplyStatusApplied, 0)
	if noOpResult.MatchedCount != 1 {
		t.Fatalf("no-op update matched=%d want 1", noOpResult.MatchedCount)
	}
	frames = readCommandWALFrames(t, dir)
	if len(frames) != 4 {
		t.Fatalf("command WAL frames after no-op update=%d, want 4", len(frames))
	}
	assertCollectionUpdateFrame(t, frames[3], "users", map[string][]byte{})

	missing := deterministicUpdateBSONSetEntry(t, "users", "client-a:update-bson-set:missing", []byte("missing"), []collections.BSONSetField{
		{Key: "city", Value: testBSONSetRawValue(t, "sea")},
	})
	missingResult, err := ApplyCommittedEntryV1(db, missing, applyMeta(1, 6), Options{ResultStore: results})
	assertApplied(t, missingResult, raftentry.ApplyStatusApplied, 0)
	if missingResult.MatchedCount != 0 {
		t.Fatalf("missing update matched=%d want 0", missingResult.MatchedCount)
	}
	frames = readCommandWALFrames(t, dir)
	if len(frames) != 5 {
		t.Fatalf("command WAL frames after missing update=%d, want 5", len(frames))
	}
	assertCollectionUpdateFrame(t, frames[4], "users", map[string][]byte{})

	dirB := t.TempDir()
	dbB := openApplyHarnessDB(t, dirB)
	defer func() { _ = dbB.Close() }()
	applyCreateSequence(t, dbB, create, insert)
	resultB, err := ApplyCommittedEntryV1(dbB, update, applyMeta(1, 3), Options{ResultStore: NewMemoryApplyResultStore(8)})
	assertApplied(t, resultB, raftentry.ApplyStatusApplied, 1)
	digestA, err := LogicalDigestV1ForDB(db, LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB A: %v", err)
	}
	digestB, err := LogicalDigestV1ForDB(dbB, LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB B: %v", err)
	}
	if digestA != digestB {
		t.Fatalf("logical digest mismatch A=%s B=%s", digestA.Hex(), digestB.Hex())
	}
}

func TestUpdateBSONSetInvalidBSONRejectsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	seam := &countingCommandWALApplySeam{}
	raw := deterministicUpdateBSONSetEntryRawValues(t, "users", "client-a:update-bson-set:invalid-bson", []byte("u1"), []string{"city"}, [][]byte{
		{byte(bson.TypeString), 0xff},
	})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{CommandWALApplySeam: seam})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedMalformed, raftentry.ErrorMalformedEntryV1)
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("invalid BSON reached command-WAL seam append=%d finalize=%d, want 0/0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 0 {
		t.Fatalf("command WAL frames after invalid BSON=%d, want 0", got)
	}
}

func TestUpdateBSONSetInvalidUTF8FieldRejectsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	seam := &countingCommandWALApplySeam{}
	raw := deterministicUpdateBSONSetEntryRawFieldNames(t, "users", "client-a:update-bson-set:invalid-field-name", []byte("u1"), [][]byte{{0xff}}, [][]byte{
		testBSONSetRawValueBytes(t, "sfo"),
	})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{CommandWALApplySeam: seam})
	assertRejected(t, result, err, raftentry.ApplyStatusRejectedMalformed, raftentry.ErrorMalformedEntryV1)
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("invalid UTF-8 field reached command-WAL seam append=%d finalize=%d, want 0/0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 0 {
		t.Fatalf("command WAL frames after invalid UTF-8 field=%d, want 0", got)
	}
}

func TestUpdateBSONSetNoIdempotencyRejectsBeforeAppend(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	seam := &countingCommandWALApplySeam{}
	raw := deterministicUpdateBSONSetEntry(t, "users", raftentry.NoIdempotencyTokenV1, []byte("u1"), []collections.BSONSetField{
		{Key: "city", Value: testBSONSetRawValue(t, "sfo")},
	})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{CommandWALApplySeam: seam})
	assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorNoIdempotencyV1)
	if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
		t.Fatalf("no-idempotency update reached command-WAL seam append=%d finalize=%d, want 0/0", seam.appendCalls, seam.finalizeCalls)
	}
	if got := len(readCommandWALFrames(t, dir)); got != 0 {
		t.Fatalf("command WAL frames after no-idempotency update=%d, want 0", got)
	}
}

func TestUpdateBSONSetPreflightRejectsJSONNoIndexBeforeCommit(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	create := deterministicCreateCollectionEntry(t, "users", "client-a:create:users-json", testCreateCollectionMetaOptions{})
	insert := deterministicInsertBatchEntry(t, "users", "client-a:insert:users-json", nativewire.DocumentFormatJSON,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"Ada","city":"hnl"}`)},
	)
	applyCreateSequence(t, db, create, insert)

	update := deterministicUpdateBSONSetEntry(t, "users", "client-a:update-bson-set:json-no-index", []byte("u1"), []collections.BSONSetField{
		{Key: "city", Value: testBSONSetRawValue(t, "sfo")},
	})
	_, err := PreflightCommandEntryV1(db, update, applyMeta(1, 3), Options{})
	if got := codeOf(err); got != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("PreflightCommandEntryV1 code=%s err=%v, want rejected-conflict", got, err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "bson") {
		t.Fatalf("PreflightCommandEntryV1 err=%v, want BSON format rejection", err)
	}
}

func TestLowerCollectionMutationRejectsInvalidIDs(t *testing.T) {
	tests := []struct {
		name      string
		command   nativewire.CommandID
		ids       [][]byte
		documents [][]byte
		wantCode  raftentry.DeterministicErrorCodeV1
	}{
		{
			name:      "duplicate insert ids",
			command:   nativewire.CommandInsertBatch,
			ids:       [][]byte{[]byte("u1"), []byte("u1")},
			documents: [][]byte{[]byte(`{"city":"hnl"}`), []byte(`{"city":"sfo"}`)},
			wantCode:  raftentry.ErrorRejectedConflictV1,
		},
		{
			name:      "duplicate replace ids",
			command:   nativewire.CommandReplaceBatch,
			ids:       [][]byte{[]byte("u1"), []byte("u1")},
			documents: [][]byte{[]byte(`{"city":"hnl"}`), []byte(`{"city":"sfo"}`)},
			wantCode:  raftentry.ErrorRejectedConflictV1,
		},
		{
			name:     "duplicate delete ids",
			command:  nativewire.CommandDeleteBatch,
			ids:      [][]byte{[]byte("u1"), []byte("u1")},
			wantCode: raftentry.ErrorRejectedConflictV1,
		},
		{
			name:      "empty insert id",
			command:   nativewire.CommandInsertBatch,
			ids:       [][]byte{[]byte("")},
			documents: [][]byte{[]byte(`{"city":"hnl"}`)},
			wantCode:  raftentry.ErrorMalformedEntryV1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := syntheticMutationCommandEntryV1(tt.command, tt.ids, tt.documents)
			_, err := lowerCollectionMutationV1(entry, nativewire.Limits{})
			if got := codeOf(err); got != tt.wantCode {
				t.Fatalf("lowerCollectionMutationV1 error code=%s, want %s (err=%v)", got, tt.wantCode, err)
			}
		})
	}
}

func TestCollectionMutationCommandWALReplayHandlers(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	create := deterministicCreateCollectionEntry(t, "users", "client-a:create:users-json", testCreateCollectionMetaOptions{})
	applyCreateSequence(t, db, create)

	insertPayload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("users", []commitlog.CollectionDocument{{
		ID:       []byte("u1"),
		Document: []byte(`{"city":"hnl"}`),
	}})
	if err != nil {
		_ = db.Close()
		t.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
	}
	insertFrame, err := commandwalapply.CollectionInsertBatchByIDFrame(insertPayload)
	if err != nil {
		_ = db.Close()
		t.Fatalf("CollectionInsertBatchByIDFrame: %v", err)
	}
	insertHandle, insertResult, err := commandwalapply.Append(db, insertFrame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
	if err != nil {
		_ = db.Close()
		t.Fatalf("Append insert replay frame: %v", err)
	}
	commandwalapply.Abort(db, insertHandle)
	if err := db.Close(); err != nil {
		t.Fatalf("Close before insert replay: %v", err)
	}

	db = openApplyHarnessDB(t, dir)
	opened, err := collections.NewCollectionManager(db).OpenCollection("users")
	if err != nil {
		_ = db.Close()
		t.Fatalf("OpenCollection users after insert replay: %v", err)
	}
	got, err := opened.Get([]byte("u1"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("Get u1 after insert replay: %v", err)
	}
	if string(got) != `{"city":"hnl"}` {
		_ = db.Close()
		t.Fatalf("u1 after insert replay=%s", got)
	}
	if got := db.State().AppliedCommandLSN; got != insertResult.LSN {
		_ = db.Close()
		t.Fatalf("AppliedCommandLSN after insert replay=%d, want %d", got, insertResult.LSN)
	}

	updatePayload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("users", []commitlog.CollectionDocument{{
		ID:       []byte("u1"),
		Document: []byte(`{"city":"sfo"}`),
	}})
	if err != nil {
		_ = db.Close()
		t.Fatalf("EncodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	updateFrame, err := commandwalapply.CollectionUpdateBatchByIDFrame(updatePayload)
	if err != nil {
		_ = db.Close()
		t.Fatalf("CollectionUpdateBatchByIDFrame: %v", err)
	}
	updateHandle, updateResult, err := commandwalapply.Append(db, updateFrame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
	if err != nil {
		_ = db.Close()
		t.Fatalf("Append update replay frame: %v", err)
	}
	commandwalapply.Abort(db, updateHandle)
	if err := db.Close(); err != nil {
		t.Fatalf("Close before update replay: %v", err)
	}

	db = openApplyHarnessDB(t, dir)
	opened, err = collections.NewCollectionManager(db).OpenCollection("users")
	if err != nil {
		_ = db.Close()
		t.Fatalf("OpenCollection users after update replay: %v", err)
	}
	got, err = opened.Get([]byte("u1"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("Get u1 after update replay: %v", err)
	}
	if string(got) != `{"city":"sfo"}` {
		_ = db.Close()
		t.Fatalf("u1 after update replay=%s", got)
	}
	if got := db.State().AppliedCommandLSN; got != updateResult.LSN {
		_ = db.Close()
		t.Fatalf("AppliedCommandLSN after update replay=%d, want %d", got, updateResult.LSN)
	}

	deletePayload, err := commitlog.EncodeCollectionDeleteBatchByIDPayload("users", [][]byte{[]byte("u1")})
	if err != nil {
		_ = db.Close()
		t.Fatalf("EncodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	deleteFrame, err := commandwalapply.CollectionDeleteBatchByIDFrame(deletePayload)
	if err != nil {
		_ = db.Close()
		t.Fatalf("CollectionDeleteBatchByIDFrame: %v", err)
	}
	deleteHandle, deleteResult, err := commandwalapply.Append(db, deleteFrame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
	if err != nil {
		_ = db.Close()
		t.Fatalf("Append delete replay frame: %v", err)
	}
	commandwalapply.Abort(db, deleteHandle)
	if err := db.Close(); err != nil {
		t.Fatalf("Close before delete replay: %v", err)
	}

	db = openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()
	opened, err = collections.NewCollectionManager(db).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users after delete replay: %v", err)
	}
	got, err = opened.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get u1 after delete replay: %v", err)
	}
	if got != nil {
		t.Fatalf("u1 after delete replay=%s, want nil", got)
	}
	if got := db.State().AppliedCommandLSN; got != deleteResult.LSN {
		t.Fatalf("AppliedCommandLSN after delete replay=%d, want %d", got, deleteResult.LSN)
	}
}

func TestCollectionMutationLargeValueLogPointerReopenAndConverges(t *testing.T) {
	create := deterministicCreateCollectionEntry(t, "events", "client-a:create:events", testCreateCollectionMetaOptions{})
	largeDoc := []byte(`{"id":"big","payload":"` + strings.Repeat("x", 8192) + `"}`)
	insert := deterministicInsertBatchEntry(t, "events", "client-a:insert:events:big", nativewire.DocumentFormatJSON, [][]byte{[]byte("big")}, [][]byte{largeDoc})
	opts := backenddb.Options{ValueLog: backenddb.ValueLogOptions{PointerThreshold: 1, ForcePointers: true}}

	dirA := t.TempDir()
	dbA := openApplyHarnessDBWithOptions(t, dirA, opts)
	applyCreateSequence(t, dbA, create, insert)
	digestA, err := LogicalDigestV1ForDB(dbA, LogicalDigestOptionsV1{})
	if err != nil {
		_ = dbA.Close()
		t.Fatalf("LogicalDigestV1ForDB A before reopen: %v", err)
	}
	beforeLSN := dbA.State().AppliedCommandLSN
	if err := dbA.Close(); err != nil {
		t.Fatalf("Close A: %v", err)
	}

	reopenA := openApplyHarnessDBWithOptions(t, dirA, opts)
	opened, err := collections.NewCollectionManager(reopenA).OpenCollection("events")
	if err != nil {
		_ = reopenA.Close()
		t.Fatalf("OpenCollection events after reopen: %v", err)
	}
	got, err := opened.Get([]byte("big"))
	if err != nil {
		_ = reopenA.Close()
		t.Fatalf("Get big after reopen: %v", err)
	}
	if !bytes.Equal(got, largeDoc) {
		_ = reopenA.Close()
		t.Fatalf("large document after reopen length=%d, want %d", len(got), len(largeDoc))
	}
	if got := reopenA.State().AppliedCommandLSN; got != beforeLSN {
		_ = reopenA.Close()
		t.Fatalf("reopen AppliedCommandLSN=%d, want %d", got, beforeLSN)
	}
	reopenDigest, err := LogicalDigestV1ForDB(reopenA, LogicalDigestOptionsV1{})
	if err != nil {
		_ = reopenA.Close()
		t.Fatalf("LogicalDigestV1ForDB A after reopen: %v", err)
	}
	if err := reopenA.Close(); err != nil {
		t.Fatalf("Close reopen A: %v", err)
	}
	if reopenDigest != digestA {
		t.Fatalf("logical digest after reopen=%s, want %s", reopenDigest.Hex(), digestA.Hex())
	}

	dirB := t.TempDir()
	dbB := openApplyHarnessDBWithOptions(t, dirB, opts)
	defer func() { _ = dbB.Close() }()
	if err := dbB.RotateCommandWALActiveSegment(false); err != nil {
		t.Fatalf("RotateCommandWALActiveSegment B: %v", err)
	}
	applyCreateSequence(t, dbB, create)
	if err := dbB.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint B mid-sequence: %v", err)
	}
	applyCreateSequenceFrom(t, dbB, 2, insert)
	digestB, err := LogicalDigestV1ForDB(dbB, LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB B: %v", err)
	}
	if digestB != digestA {
		t.Fatalf("fresh DB digest=%s, want %s", digestB.Hex(), digestA.Hex())
	}

	dirC := t.TempDir()
	dbC := openApplyHarnessDBWithOptions(t, dirC, opts)
	defer func() { _ = dbC.Close() }()
	changedDoc := []byte(`{"id":"big","payload":"` + strings.Repeat("y", 8192) + `"}`)
	changedInsert := deterministicInsertBatchEntry(t, "events", "client-a:insert:events:changed", nativewire.DocumentFormatJSON, [][]byte{[]byte("big")}, [][]byte{changedDoc})
	applyCreateSequence(t, dbC, create, changedInsert)
	digestC, err := LogicalDigestV1ForDB(dbC, LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB C: %v", err)
	}
	if digestC == digestA {
		t.Fatalf("intentional document difference produced same logical digest %s", digestA.Hex())
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

func TestPostPublishLogicalDigestFailureRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	raw := deterministicCreateCollectionEntry(t, "users", "client-a:create:users", testCreateCollectionMetaOptions{})
	h := NewHarness(db, Options{})
	h.logicalDigestV1Fn = func(LogicalDigestOptionsV1) (LogicalDigestV1, error) {
		return LogicalDigestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "digest unavailable after publish")
	}
	result, err := h.ApplyCommittedEntryV1(raw, applyMeta(1, 1))
	assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
	if _, openErr := collections.NewCollectionManager(db).OpenCollection("users"); openErr != nil {
		t.Fatalf("OpenCollection users after recovery-required digest failure: %v", openErr)
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

func TestProgressInitialGapFailsBeforeAppendOrMutation(t *testing.T) {
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
		t.Fatalf("initial gap reached append path append=%d frames=%d", seam.appendCalls, len(readCommandWALFrames(t, dir)))
	}
	if got := db.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("AppliedCommandLSN after initial gap=%d, want 0", got)
	}
	_, openErr := collections.NewCollectionManager(db).OpenCollection("users")
	if !errors.Is(openErr, collections.ErrCollectionNotFound) {
		t.Fatalf("OpenCollection users after initial gap error=%v, want ErrCollectionNotFound", openErr)
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
		EntryID:           id,
		CommandDigest:     otherDigest,
		IdempotencyKey:    []byte("seed-conflict"),
		AppliedCommandLSN: 1,
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
	if err := results.RecordApplyResult(ApplyResultRecordV1{EntryID: id1, CommandDigest: digest, IdempotencyKey: []byte("key-1"), AppliedCommandLSN: 1}); err != nil {
		t.Fatalf("RecordApplyResult id1: %v", err)
	}
	if err := results.RecordApplyResult(ApplyResultRecordV1{EntryID: id2, CommandDigest: digest, IdempotencyKey: []byte("key-2"), AppliedCommandLSN: 2}); codeOf(err) != raftentry.ErrorResourceExhaustedV1 {
		t.Fatalf("RecordApplyResult id2 error=%v code=%s, want resource exhausted", err, codeOf(err))
	}

	progress := NewMemoryApplyProgressStore(1, 8)
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: id1, CommandDigest: digest, AppliedCommandLSN: 1}); err != nil {
		t.Fatalf("RecordApplied id1: %v", err)
	}
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: id2, CommandDigest: digest, AppliedCommandLSN: 2}); codeOf(err) != raftentry.ErrorResourceExhaustedV1 {
		t.Fatalf("RecordApplied id2 error=%v code=%s, want resource exhausted", err, codeOf(err))
	}

	progress = NewMemoryApplyProgressStore(8, 8)
	highTermID1 := raftentry.ApplyEntryID{Term: 2, Index: 1}
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: highTermID1, CommandDigest: digest, AppliedCommandLSN: 1}); err != nil {
		t.Fatalf("RecordApplied lower-index setup: %v", err)
	}
	if err := progress.CheckCanApply(highTermID1); codeOf(err) != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("CheckCanApply duplicate/lower error=%v code=%s, want conflict", err, codeOf(err))
	}
	id3 := raftentry.ApplyEntryID{Term: 2, Index: 3}
	if err := progress.CheckCanApply(id3); err != nil {
		t.Fatalf("CheckCanApply gap: %v", err)
	}
	lowerTermAfterGap := raftentry.ApplyEntryID{Term: 1, Index: 3}
	if err := progress.CheckCanApply(lowerTermAfterGap); codeOf(err) != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("CheckCanApply lower-term gap error=%v code=%s, want conflict", err, codeOf(err))
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
	if err := results.RecordApplyResult(ApplyResultRecordV1{EntryID: id, CommandDigest: digest, IdempotencyKey: []byte("key-1"), AppliedCommandLSN: 1, Result: first}); err != nil {
		t.Fatalf("RecordApplyResult first: %v", err)
	}
	if err := results.RecordApplyResult(ApplyResultRecordV1{EntryID: id, CommandDigest: digest, IdempotencyKey: []byte("key-1"), AppliedCommandLSN: 1, Result: second}); err != nil {
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

func openApplyHarnessDB(t testing.TB, dir string) *backenddb.DB {
	t.Helper()
	return openApplyHarnessDBWithOptions(t, dir, backenddb.Options{})
}

func openApplyHarnessDBWithOptions(t testing.TB, dir string, opts backenddb.Options) *backenddb.DB {
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

func assertCollectionInsertFrame(t *testing.T, env commitlog.CommandEnvelope, collection string, docs map[string][]byte) {
	t.Helper()
	if env.Kind != commitlog.CommandKindCollectionInsertBatchByID ||
		env.Scope != commitlog.CommandScopeCollection ||
		env.PayloadFormat != commitlog.PayloadFormatCollectionInsertBatchByIDV1 {
		t.Fatalf("command WAL frame identity=%+v, want collection insert", env)
	}
	payload, err := commitlog.DecodeCollectionInsertBatchByIDPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionInsertBatchByIDPayload: %v", err)
	}
	assertCollectionDocumentPayload(t, payload.Collection, payload.Documents, collection, docs)
}

func assertCollectionUpdateFrame(t *testing.T, env commitlog.CommandEnvelope, collection string, docs map[string][]byte) {
	t.Helper()
	if env.Kind != commitlog.CommandKindCollectionUpdateBatchByID ||
		env.Scope != commitlog.CommandScopeCollection ||
		env.PayloadFormat != commitlog.PayloadFormatCollectionUpdateBatchByIDV1 {
		t.Fatalf("command WAL frame identity=%+v, want collection update", env)
	}
	payload, err := commitlog.DecodeCollectionUpdateBatchByIDPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	assertCollectionDocumentPayload(t, payload.Collection, payload.Documents, collection, docs)
}

func assertCollectionDeleteFrame(t *testing.T, env commitlog.CommandEnvelope, collection string, ids [][]byte) {
	t.Helper()
	if env.Kind != commitlog.CommandKindCollectionDeleteBatchByID ||
		env.Scope != commitlog.CommandScopeCollection ||
		env.PayloadFormat != commitlog.PayloadFormatCollectionDeleteBatchByIDV1 {
		t.Fatalf("command WAL frame identity=%+v, want collection delete", env)
	}
	payload, err := commitlog.DecodeCollectionDeleteBatchByIDPayload(env.Payload)
	if err != nil {
		t.Fatalf("DecodeCollectionDeleteBatchByIDPayload: %v", err)
	}
	if payload.Collection != collection {
		t.Fatalf("delete collection=%q, want %q", payload.Collection, collection)
	}
	if len(payload.IDs) != len(ids) {
		t.Fatalf("delete ids=%d, want %d", len(payload.IDs), len(ids))
	}
	want := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		want[string(id)] = struct{}{}
	}
	seen := make(map[string]struct{}, len(payload.IDs))
	for _, id := range payload.IDs {
		key := string(id)
		if _, ok := want[key]; !ok {
			t.Fatalf("unexpected delete id %q", string(id))
		}
		if _, ok := seen[key]; ok {
			t.Fatalf("duplicate delete id %q", string(id))
		}
		seen[key] = struct{}{}
	}
	for _, id := range ids {
		if _, ok := seen[string(id)]; !ok {
			t.Fatalf("missing delete id %q", string(id))
		}
	}
}

func assertCollectionDocumentPayload(t *testing.T, gotCollection string, got []commitlog.CollectionDocument, wantCollection string, want map[string][]byte) {
	t.Helper()
	if gotCollection != wantCollection {
		t.Fatalf("payload collection=%q, want %q", gotCollection, wantCollection)
	}
	if len(got) != len(want) {
		t.Fatalf("payload documents=%d, want %d", len(got), len(want))
	}
	seen := make(map[string]struct{}, len(got))
	for _, doc := range got {
		key := string(doc.ID)
		wantDoc, ok := want[key]
		if !ok {
			t.Fatalf("unexpected payload document id %q", string(doc.ID))
		}
		if _, ok := seen[key]; ok {
			t.Fatalf("duplicate payload document id %q", string(doc.ID))
		}
		seen[key] = struct{}{}
		if !bytes.Equal(doc.Document, wantDoc) {
			t.Fatalf("payload document %q=%x, want %x", string(doc.ID), doc.Document, wantDoc)
		}
	}
	for id := range want {
		if _, ok := seen[id]; !ok {
			t.Fatalf("missing payload document id %q", id)
		}
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

func TestCollectionMutationRequiresRecoveryCodedUnsafeDurability(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", codedError(raftentry.ErrorUnsafeDurabilityModeV1, "unsafe durability after publish"))
	if !collectionMutationRequiresRecovery(err) {
		t.Fatalf("coded unsafe durability error did not require recovery")
	}
	err = fmt.Errorf("wrapped: %w", codedError(raftentry.ErrorRejectedConflictV1, "ordinary conflict"))
	if collectionMutationRequiresRecovery(err) {
		t.Fatalf("coded rejected conflict unexpectedly required recovery")
	}
}

type recordApplyResultStoreFailAfterPreflight struct{}

func (recordApplyResultStoreFailAfterPreflight) LookupApplyResult(raftentry.ApplyEntryID) (ApplyResultRecordV1, bool, error) {
	return ApplyResultRecordV1{}, false, nil
}

func (recordApplyResultStoreFailAfterPreflight) LookupApplyResultByIdempotencyKey([]byte) (ApplyResultRecordV1, bool, error) {
	return ApplyResultRecordV1{}, false, nil
}

func (recordApplyResultStoreFailAfterPreflight) CheckCanRecordApplyResult(ApplyResultRecordV1) error {
	return nil
}

func (recordApplyResultStoreFailAfterPreflight) RecordApplyResult(ApplyResultRecordV1) error {
	return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "result store unavailable after apply")
}

type recordDuplicateApplyResultStoreFailAfterPreflight struct {
	base *MemoryApplyResultStore
}

func (s recordDuplicateApplyResultStoreFailAfterPreflight) LookupApplyResult(id raftentry.ApplyEntryID) (ApplyResultRecordV1, bool, error) {
	return s.base.LookupApplyResult(id)
}

func (s recordDuplicateApplyResultStoreFailAfterPreflight) LookupApplyResultByIdempotencyKey(key []byte) (ApplyResultRecordV1, bool, error) {
	return s.base.LookupApplyResultByIdempotencyKey(key)
}

func (s recordDuplicateApplyResultStoreFailAfterPreflight) CheckCanRecordApplyResult(record ApplyResultRecordV1) error {
	return s.base.CheckCanRecordApplyResult(record)
}

func (recordDuplicateApplyResultStoreFailAfterPreflight) RecordApplyResult(ApplyResultRecordV1) error {
	return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "result store unavailable during duplicate apply")
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

func (recordProgressStoreFailAfterPreflight) LookupApplyProgress(raftentry.ApplyEntryID) (ApplyProgressRecordV1, bool, error) {
	return ApplyProgressRecordV1{}, false, nil
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
	indexUnique        bool
	includeVectorIndex bool
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
		dst = append(dst, boolByte(opts.indexUnique)) // unique
		dst = append(dst, 0)                          // multi_key
		dst = binary.AppendUvarint(dst, opts.indexStoragePolicy)
	}
	if version >= 2 {
		if !opts.includeVectorIndex {
			dst = binary.AppendUvarint(dst, 0) // vector_index_count
			return dst
		}
		dst = binary.AppendUvarint(dst, 1) // vector_index_count
		dst = appendTestString(dst, "embedding")
		dst = appendTestString(dst, "embedding")
		dst = binary.AppendUvarint(dst, 1) // metric: cosine
		dst = binary.AppendVarint(dst, 2)  // dimensions
		dst = binary.AppendVarint(dst, 4)  // m
		dst = binary.AppendVarint(dst, 16) // ef_construction
		dst = binary.AppendVarint(dst, 8)  // ef_search
		dst = binary.AppendUvarint(dst, 1) // encoding: float32
		if version >= 3 {
			dst = binary.AppendUvarint(dst, 1) // strategy: native runtime
			dst = binary.AppendUvarint(dst, 0) // quantized_index_count
		}
	}
	return dst
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func appendTestString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func deterministicInsertBatchEntry(t *testing.T, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte) []byte {
	t.Helper()
	return deterministicMutationEntry(t, nativewire.CommandInsertBatch, collection, idempotency, format, ids, documents, nil)
}

func deterministicReplaceBatchEntry(t *testing.T, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte) []byte {
	t.Helper()
	extra := []nativewire.Section{{ID: nativewire.SectionReplacementMode, Bytes: binary.AppendUvarint(nil, 1)}}
	return deterministicMutationEntry(t, nativewire.CommandReplaceBatch, collection, idempotency, format, ids, documents, extra)
}

func syntheticMutationCommandEntryV1(command nativewire.CommandID, ids, documents [][]byte) raftentry.CommandEntryV1 {
	sections := []nativewire.Section{
		{ID: nativewire.SectionCollectionRef, Bytes: append([]byte{deterministicCollectionRefTagName}, []byte("users")...)},
		{ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, ids...)},
	}
	if command == nativewire.CommandInsertBatch || command == nativewire.CommandReplaceBatch {
		sections = append(sections,
			nativewire.Section{ID: nativewire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(nativewire.DocumentFormatJSON))},
			nativewire.Section{ID: nativewire.SectionDocuments, Bytes: nativewire.AppendByteVector(nil, documents...)},
		)
	}
	if command == nativewire.CommandReplaceBatch {
		sections = append(sections, nativewire.Section{ID: nativewire.SectionReplacementMode, Bytes: binary.AppendUvarint(nil, 1)})
	}
	return raftentry.CommandEntryV1{
		Decoded: nativewire.DeterministicEntry{Sections: sections},
		Target:  raftentry.TargetIdentityV1{CommandID: command},
	}
}

func deterministicDeleteBatchEntry(t *testing.T, collection, idempotency string, ids [][]byte) []byte {
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

func deterministicUpdateBSONSetEntry(t *testing.T, collection, idempotency string, id []byte, fields []collections.BSONSetField) []byte {
	t.Helper()
	names := make([]string, len(fields))
	values := make([][]byte, len(fields))
	for i, field := range fields {
		names[i] = field.Key
		value := make([]byte, 1+len(field.Value.Value))
		value[0] = byte(field.Value.Type)
		copy(value[1:], field.Value.Value)
		values[i] = value
	}
	return deterministicUpdateBSONSetEntryRawValues(t, collection, idempotency, id, names, values)
}

func deterministicUpdateBSONSetEntryRawValues(t *testing.T, collection, idempotency string, id []byte, names []string, values [][]byte) []byte {
	t.Helper()
	fieldNames := make([][]byte, len(names))
	for i := range names {
		fieldNames[i] = []byte(names[i])
	}
	return deterministicUpdateBSONSetEntryRawFieldNames(t, collection, idempotency, id, fieldNames, values)
}

func deterministicUpdateBSONSetEntryRawFieldNames(t *testing.T, collection, idempotency string, id []byte, fieldNames [][]byte, values [][]byte) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandUpdateBSONSet, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionRef, Bytes: deterministicTestCollectionNameRef(collection)},
		{ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, id)},
		{ID: nativewire.SectionUpdateFieldNames, Bytes: nativewire.AppendByteVector(nil, fieldNames...)},
		{ID: nativewire.SectionUpdateFieldValues, Bytes: nativewire.AppendByteVector(nil, values...)},
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

func deterministicMutationEntry(t *testing.T, command nativewire.CommandID, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte, extra []nativewire.Section) []byte {
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
	return append([]byte{deterministicCollectionRefTagName}, collection...)
}

func testBSONDocument(t *testing.T, document bson.D) []byte {
	t.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		t.Fatalf("Marshal BSON: %v", err)
	}
	return encoded
}

func testBSONSetRawValue(t *testing.T, value any) bson.RawValue {
	t.Helper()
	typ, raw, err := bson.MarshalValue(value)
	if err != nil {
		t.Fatalf("MarshalValue(%T): %v", value, err)
	}
	return bson.RawValue{Type: typ, Value: raw}
}

func testBSONSetRawValueBytes(t *testing.T, value any) []byte {
	t.Helper()
	raw := testBSONSetRawValue(t, value)
	out := make([]byte, 1+len(raw.Value))
	out[0] = byte(raw.Type)
	copy(out[1:], raw.Value)
	return out
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

type singlePointFaultInjector struct {
	point FaultPointV1
}

func (f singlePointFaultInjector) InjectApplyFault(point FaultPointV1, _ ApplyFaultContextV1) error {
	if point != f.point {
		return nil
	}
	return errors.New("injected apply fault")
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

type failingFinalizeCommandWALApplySeam struct {
	appendCalls   int
	finalizeCalls int
	abortCalls    int
	finalizeErr   error
}

func (s *failingFinalizeCommandWALApplySeam) Append(db *backenddb.DB, frame commandwalapply.LoweredFrame, meta commandwalapply.ApplyMetadata, opts commandwalapply.Options) (commandwalapply.Handle, commandwalapply.Result, error) {
	s.appendCalls++
	return commandwalapply.Append(db, frame, meta, opts)
}

func (s *failingFinalizeCommandWALApplySeam) Finalize(db *backenddb.DB, handle commandwalapply.Handle, meta commandwalapply.ApplyMetadata, opts commandwalapply.Options) (commandwalapply.Result, error) {
	s.finalizeCalls++
	if s.finalizeErr != nil {
		return commandwalapply.Result{}, s.finalizeErr
	}
	return commandwalapply.Finalize(db, handle, meta, opts)
}

func (s *failingFinalizeCommandWALApplySeam) Abort(db *backenddb.DB, handle commandwalapply.Handle) {
	s.abortCalls++
	commandwalapply.Abort(db, handle)
}

var benchmarkCollectionMutationSink collectionMutationV1
var benchmarkApplyResultSink raftentry.ApplyResultV1

func BenchmarkApplyCommittedEntryCloseout3043(b *testing.B) {
	b.Run("supported_create_collection", func(b *testing.B) {
		baseDir := b.TempDir()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dir := filepath.Join(baseDir, fmt.Sprintf("supported-%06d", i))
			db := openApplyHarnessDB(b, dir)
			raw := deterministicCreateCollectionEntryForBenchmark(b, "users", fmt.Sprintf("bench:create:%d", i))
			result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
				ProgressStore: NewMemoryApplyProgressStore(8, 8),
				ResultStore:   NewMemoryApplyResultStore(8),
			})
			if err != nil {
				_ = db.Close()
				b.Fatalf("ApplyCommittedEntryV1 supported: %v result=%+v", err, result)
			}
			benchmarkApplyResultSink = result
			if err := db.Close(); err != nil {
				b.Fatalf("Close DB: %v", err)
			}
		}
	})
	b.Run("rejected_unsupported_before_append", func(b *testing.B) {
		dir := b.TempDir()
		db := openApplyHarnessDB(b, dir)
		defer func() { _ = db.Close() }()
		raw := readHexFixtureForBenchmark(b, "../nativewire/testdata/v1/create_index_entry.hex")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, uint64(i)+1), Options{})
			if err == nil || result.Status != raftentry.ApplyStatusRejectedUnsupported {
				b.Fatalf("ApplyCommittedEntryV1 rejected result=%+v err=%v", result, err)
			}
			benchmarkApplyResultSink = result
		}
	})
	b.Run("duplicate_result_replay", func(b *testing.B) {
		dir := b.TempDir()
		db := openApplyHarnessDB(b, dir)
		defer func() { _ = db.Close() }()
		progress := NewMemoryApplyProgressStore(8, 8)
		results := NewMemoryApplyResultStore(8)
		raw := deterministicCreateCollectionEntryForBenchmark(b, "users", "bench:create:duplicate")
		seed, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
			ProgressStore: progress,
			ResultStore:   results,
		})
		if err != nil {
			b.Fatalf("seed duplicate replay: %v result=%+v", err, seed)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
				ProgressStore: progress,
				ResultStore:   results,
			})
			if err != nil {
				b.Fatalf("duplicate replay: %v result=%+v", err, result)
			}
			benchmarkApplyResultSink = result
		}
	})
	b.Run("close_reopen_replay_boundary", func(b *testing.B) {
		baseDir := b.TempDir()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dir := filepath.Join(baseDir, fmt.Sprintf("reopen-%06d", i))
			db := openApplyHarnessDB(b, dir)
			raw := deterministicCreateCollectionEntryForBenchmark(b, "users", fmt.Sprintf("bench:create:reopen:%d", i))
			result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
				ProgressStore: NewMemoryApplyProgressStore(8, 8),
				ResultStore:   NewMemoryApplyResultStore(8),
			})
			if err != nil {
				_ = db.Close()
				b.Fatalf("ApplyCommittedEntryV1 close/reopen: %v result=%+v", err, result)
			}
			if err := db.Close(); err != nil {
				b.Fatalf("Close DB: %v", err)
			}
			reopened := openApplyHarnessDB(b, dir)
			if _, err := collections.NewCollectionManager(reopened).OpenCollection("users"); err != nil {
				_ = reopened.Close()
				b.Fatalf("OpenCollection after reopen: %v", err)
			}
			benchmarkApplyResultSink = result
			if err := reopened.Close(); err != nil {
				b.Fatalf("Close reopened DB: %v", err)
			}
		}
	})
}

func BenchmarkApplyCommittedEntryUpdateBSONSet(b *testing.B) {
	dir := b.TempDir()
	db := openApplyHarnessDB(b, dir)
	defer func() { _ = db.Close() }()

	id := []byte("u1")
	create := deterministicCreateCollectionEntryForBenchmarkWithOptions(b, "users", "bench:create:update-bson-set", testCreateCollectionMetaOptions{
		documentFormat: uint64(nativewire.DocumentFormatBSON),
	})
	insertDoc := testBSONDocumentForBenchmark(b, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "city", Value: "hnl"},
		{Key: "visits", Value: int32(1)},
	})
	insert := deterministicMutationEntryForBenchmark(b, nativewire.CommandInsertBatch, "users", "bench:insert:update-bson-set", nativewire.DocumentFormatBSON, [][]byte{id}, [][]byte{insertDoc}, nil)
	for i, raw := range [][]byte{create, insert} {
		result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, uint64(i)+1), Options{})
		if err != nil {
			b.Fatalf("seed ApplyCommittedEntryV1 %d: %v result=%+v", i, err, result)
		}
	}

	updateA := deterministicUpdateBSONSetEntryForBenchmark(b, "users", "bench:update-bson-set:a", id, []collections.BSONSetField{
		{Key: "city", Value: testBSONSetRawValueForBenchmark(b, "sfo")},
	})
	updateB := deterministicUpdateBSONSetEntryForBenchmark(b, "users", "bench:update-bson-set:b", id, []collections.BSONSetField{
		{Key: "city", Value: testBSONSetRawValueForBenchmark(b, "sea")},
	})
	entries := [][]byte{updateA, updateB}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ApplyCommittedEntryV1(db, entries[i&1], applyMeta(1, uint64(i)+3), Options{})
		if err != nil {
			b.Fatalf("ApplyCommittedEntryV1 update_bson_set: %v result=%+v", err, result)
		}
		if result.Status != raftentry.ApplyStatusApplied || result.AffectedCount != 1 {
			b.Fatalf("update_bson_set result=%+v, want applied affected=1", result)
		}
		benchmarkApplyResultSink = result
	}
}

func BenchmarkDecodeLowerCollectionMutationV1(b *testing.B) {
	ids := make([][]byte, 16)
	documents := make([][]byte, 16)
	for i := range ids {
		ids[i] = []byte("doc-" + string(rune('a'+i)))
		documents[i] = []byte(`{"name":"` + string(rune('a'+i)) + `"}`)
	}
	raw := deterministicMutationEntryForBenchmark(b, nativewire.CommandInsertBatch, "bench", "bench:insert", nativewire.DocumentFormatJSON, ids, documents, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, err := raftentry.DecodeCommandEntryV1(raw, raftentry.DecodeOptions{})
		if err != nil {
			b.Fatalf("DecodeCommandEntryV1: %v", err)
		}
		mutation, err := lowerCollectionMutationV1(entry, nativewire.Limits{})
		if err != nil {
			b.Fatalf("lowerCollectionMutationV1: %v", err)
		}
		benchmarkCollectionMutationSink = mutation
	}
}

func deterministicMutationEntryForBenchmark(b *testing.B, command nativewire.CommandID, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte, extra []nativewire.Section) []byte {
	b.Helper()
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
		b.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		b.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func deterministicCreateCollectionEntryForBenchmark(b *testing.B, collection, idempotency string) []byte {
	b.Helper()
	return deterministicCreateCollectionEntryForBenchmarkWithOptions(b, collection, idempotency, testCreateCollectionMetaOptions{})
}

func deterministicCreateCollectionEntryForBenchmarkWithOptions(b *testing.B, collection, idempotency string, opts testCreateCollectionMetaOptions) []byte {
	b.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandCreateCollection, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionMeta, Bytes: testCreateCollectionMetaPayload(collection, opts)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		b.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		b.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func deterministicUpdateBSONSetEntryForBenchmark(b *testing.B, collection, idempotency string, id []byte, fields []collections.BSONSetField) []byte {
	b.Helper()
	names := make([][]byte, len(fields))
	values := make([][]byte, len(fields))
	for i, field := range fields {
		names[i] = []byte(field.Key)
		value := make([]byte, 1+len(field.Value.Value))
		value[0] = byte(field.Value.Type)
		copy(value[1:], field.Value.Value)
		values[i] = value
	}
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandUpdateBSONSet, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionRef, Bytes: deterministicTestCollectionNameRef(collection)},
		{ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, id)},
		{ID: nativewire.SectionUpdateFieldNames, Bytes: nativewire.AppendByteVector(nil, names...)},
		{ID: nativewire.SectionUpdateFieldValues, Bytes: nativewire.AppendByteVector(nil, values...)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		b.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		b.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func testBSONDocumentForBenchmark(b *testing.B, document bson.D) []byte {
	b.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		b.Fatalf("Marshal BSON: %v", err)
	}
	return encoded
}

func testBSONSetRawValueForBenchmark(b *testing.B, value any) bson.RawValue {
	b.Helper()
	typ, raw, err := bson.MarshalValue(value)
	if err != nil {
		b.Fatalf("MarshalValue(%T): %v", value, err)
	}
	return bson.RawValue{Type: typ, Value: raw}
}

func readHexFixtureForBenchmark(b *testing.B, rel string) []byte {
	b.Helper()
	raw, err := os.ReadFile(rel)
	if err != nil {
		b.Fatalf("read fixture %s: %v", rel, err)
	}
	hexText := strings.Join(strings.Fields(string(raw)), "")
	out, err := hex.DecodeString(hexText)
	if err != nil {
		b.Fatalf("decode fixture %s: %v", rel, err)
	}
	return out
}
