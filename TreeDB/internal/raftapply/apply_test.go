package raftapply

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
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
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestUnsupportedDeterministicEntryRejectsBeforeAppendAndStores(t *testing.T) {
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer func() { _ = db.Close() }()

	progress := NewMemoryApplyProgressStore(8, 8)
	results := NewMemoryApplyResultStore(8)
	seam := &countingCommandWALApplySeam{}
	raw := readHexFixture(t, "../nativewire/testdata/v1/update_bson_set_entry.hex")

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
	_, insertResult, err := commandwalapply.Append(db, insertFrame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{})
	if err != nil {
		_ = db.Close()
		t.Fatalf("Append insert replay frame: %v", err)
	}
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
	_, updateResult, err := commandwalapply.Append(db, updateFrame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{})
	if err != nil {
		_ = db.Close()
		t.Fatalf("Append update replay frame: %v", err)
	}
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
	_, deleteResult, err := commandwalapply.Append(db, deleteFrame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{})
	if err != nil {
		_ = db.Close()
		t.Fatalf("Append delete replay frame: %v", err)
	}
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

func TestLogicalDigestConvergesAcrossFreshDBsAndIgnoresLocalLayout(t *testing.T) {
	rawUsers := deterministicCreateCollectionEntry(t, "users", "client-a:create:users", testCreateCollectionMetaOptions{})
	rawOrders := deterministicCreateCollectionEntry(t, "orders", "client-a:create:orders", testCreateCollectionMetaOptions{})

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
	return ApplyMetadataV1{
		EntryID:                 raftentry.ApplyEntryID{Term: term, Index: index},
		LocalDurabilityBoundary: LocalDurabilityCommandWALV1,
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
	for _, id := range payload.IDs {
		if _, ok := want[string(id)]; !ok {
			t.Fatalf("unexpected delete id %q", string(id))
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
	for _, doc := range got {
		wantDoc, ok := want[string(doc.ID)]
		if !ok {
			t.Fatalf("unexpected payload document id %q", string(doc.ID))
		}
		if !bytes.Equal(doc.Document, wantDoc) {
			t.Fatalf("payload document %q=%x, want %x", string(doc.ID), doc.Document, wantDoc)
		}
	}
}

func codeOf(err error) raftentry.DeterministicErrorCodeV1 {
	code, _ := ErrorCodeOf(err)
	return code
}

type testCreateCollectionMetaOptions struct {
	documentFormat uint64
}

func deterministicCreateCollectionEntry(t *testing.T, collection, idempotency string, opts testCreateCollectionMetaOptions) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandCreateCollection, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionMeta, Bytes: testCreateCollectionMetaPayload(collection, opts)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 7)},
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

func deterministicInsertBatchEntry(t *testing.T, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte) []byte {
	t.Helper()
	return deterministicMutationEntry(t, nativewire.CommandInsertBatch, collection, idempotency, format, ids, documents, nil)
}

func deterministicReplaceBatchEntry(t *testing.T, collection, idempotency string, format nativewire.DocumentFormat, ids, documents [][]byte) []byte {
	t.Helper()
	return deterministicMutationEntry(t, nativewire.CommandReplaceBatch, collection, idempotency, format, ids, documents, []nativewire.Section{
		{ID: nativewire.SectionReplacementMode, Bytes: binary.AppendUvarint(nil, 1)},
	})
}

func deterministicDeleteBatchEntry(t *testing.T, collection, idempotency string, ids [][]byte) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandDeleteBatch, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionRef, Bytes: deterministicTestCollectionNameRef(collection)},
		{ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, ids...)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 7)},
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
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 7)},
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
	return append([]byte{1}, collection...)
}

func testBSONDocument(t *testing.T, document bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	return raw
}

func testCreateCollectionMetaPayload(collection string, opts testCreateCollectionMetaOptions) []byte {
	dst := binary.AppendUvarint(nil, 1)     // collection_meta version
	dst = appendTestString(dst, collection) // name
	dst = binary.AppendUvarint(dst, opts.documentFormat)
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
		result, err := ApplyCommittedEntryV1(db, entry, applyMeta(1, startIndex+uint64(i)), Options{
			ProgressStore: progress,
			ResultStore:   results,
		})
		if err != nil {
			t.Fatalf("ApplyCommittedEntryV1 index %d: %v result=%+v", startIndex+uint64(i), err, result)
		}
		if result.Status != raftentry.ApplyStatusApplied {
			t.Fatalf("ApplyCommittedEntryV1 index %d status=%s, want applied", startIndex+uint64(i), result.Status)
		}
		out = append(out, result)
	}
	return out
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

var benchmarkCollectionMutationSink collectionMutationV1

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
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 7)},
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
