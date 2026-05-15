package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCommandWALRawSetDeleteBatchReplaysThroughNormalExecutor(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	b := db.NewBatch()
	if err := b.Set([]byte("keep"), []byte("before")); err != nil {
		t.Fatalf("Set keep before: %v", err)
	}
	if err := b.Set([]byte("drop"), []byte("gone")); err != nil {
		t.Fatalf("Set drop before: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync before: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close before batch: %v", err)
	}

	db.testFailFinalizeCommit.Store(true)
	crashBatch := db.NewBatch()
	if err := crashBatch.Set([]byte("keep"), []byte("after")); err != nil {
		t.Fatalf("Set keep crash batch: %v", err)
	}
	if err := crashBatch.Delete([]byte("drop")); err != nil {
		t.Fatalf("Delete drop crash batch: %v", err)
	}
	err := crashBatch.WriteSync()
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("crash batch WriteSync error=%v, want failpoint", err)
	}
	_ = crashBatch.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close crashed db: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "keep", "after")
	if got, err := reopen.Get([]byte("drop")); err != nil || got != nil {
		t.Fatalf("Get(drop)=%q err=%v, want missing after command WAL delete replay", got, err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN=%d, want 2", got)
	}
}

func TestCommandWALCrashAfterFrameBeforeRootPublishRecovers(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	db.testFailFinalizeCommit.Store(true)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	err := b.WriteSync()
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("WriteSync error=%v, want failpoint", err)
	}
	_ = b.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "v")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALCrashDuringRootPublishSelectsOldTupleOrNewTuple(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	activeMetaPage := db.metaPageID
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	corruptIndexPageByte(t, dir, activeMetaPage)
	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "v")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALCrashAfterRootAppliedLSNBeforeCleanupSkipsFrame(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
		t.Fatalf("command WAL frame missing before cleanup-resume reopen: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "v")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALRecoveryCrashDuringReplayResumesFromAppliedLSN(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("a"), Value: []byte("1")}})
	writeCommandWALRawKVFrame(t, dir, 1, 2, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("b"), Value: []byte("2")}})

	testCommandWALRecoveryFailAfterLSN.Store(1)
	_, err := Open(Options{Dir: dir})
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("Open with recovery failpoint error=%v, want failpoint", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "a", "1")
	assertDBValue(t, reopen, "b", "2")
	if got := reopen.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN=%d, want 2", got)
	}
}

func TestCommandWALRIDFencePreservedForRawKVBatch(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeValueLogRID(t, dir, 7, []byte("large-value"))
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("k"), RID: 7}})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", "large-value")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALPointerBatchReplaysThroughRIDFence(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	largeValue := strings.Repeat("large-value-", 1024)
	ptr := writeValueLogRID(t, dir, 17, []byte(largeValue))

	db = openCommandWALDB(t, dir)
	db.testFailFinalizeCommit.Store(true)
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	err := b.WriteSync()
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("WriteSync error=%v, want failpoint", err)
	}
	_ = b.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close crashed db: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "k", largeValue)
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
}

func TestCommandWALMissingRIDFenceFailsRecovery(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte("k"), RID: 99}})

	_, err := Open(Options{Dir: dir})
	if err == nil || !strings.Contains(err.Error(), "missing value-log rid 99") {
		t.Fatalf("Open error=%v, want missing rid recovery failure", err)
	}
}

func TestCommandWALIdempotentSkipRequiresDigestProof(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 2, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("k"), Value: []byte("v")}})

	_, err := Open(Options{Dir: dir})
	if !errors.Is(err, ErrCommandWALAppliedLSNNonContig) {
		t.Fatalf("Open error=%v, want ErrCommandWALAppliedLSNNonContig instead of skipping missing LSN", err)
	}
}

func TestCommandWALExistingRawReplayTestsMappedToRawKVBatch(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSet, Key: []byte("a"), Value: []byte("1")},
		{Op: commitlog.RawKVOpSet, Key: []byte("b"), Value: []byte("2")},
		{Op: commitlog.RawKVOpDelete, Key: []byte("a")},
	})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	if got, err := reopen.Get([]byte("a")); err != nil || got != nil {
		t.Fatalf("Get(a)=%q err=%v, want missing after typed RawKVBatch delete replay", got, err)
	}
	assertDBValue(t, reopen, "b", "2")
}

func TestCommandWALExistingRIDFenceTestsMappedToExternalRefFence(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close bootstrap db: %v", err)
	}
	writeValueLogRID(t, dir, 1, []byte("one"))
	writeValueLogRID(t, dir, 2, []byte("two"))
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSetRID, Key: []byte("a"), RID: 1},
		{Op: commitlog.RawKVOpSetRID, Key: []byte("b"), RID: 2},
	})

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	assertDBValue(t, reopen, "a", "one")
	assertDBValue(t, reopen, "b", "two")
}

func enableCommandWALFormat(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
}

func openCommandWALDB(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open command WAL DB: %v", err)
	}
	return db
}

func assertDBValue(t *testing.T, db *DB, key string, want string) {
	t.Helper()
	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("Get(%q)=%q, want %q", key, got, want)
	}
}

func writeCommandWALRawKVFrame(t *testing.T, dir string, segmentSeq uint64, lsn uint64, ops []commitlog.RawKVOperation) {
	t.Helper()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	payload, err := commitlog.EncodeRawKVBatchPayload(ops)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, segmentSeq))
	w, err := commitlog.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.AppendCommand(commitlog.CommandEnvelope{
		LSN:           lsn,
		Kind:          commitlog.CommandKindRawKVBatch,
		Scope:         commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
		Payload:       payload,
	}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendCommand: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
}

func writeValueLogRID(t *testing.T, dir string, rid uint64, value []byte) page.ValuePtr {
	t.Helper()
	valueLogDir := resolveStorageLayout(dir).valueVLogDir
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("MkdirAll value_vlog: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(valueLogDir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("New value writer: %v", err)
	}
	ptr, err := w.Append(0, nil, rid, value)
	if err != nil {
		_ = w.Close()
		t.Fatalf("Append value log: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close value writer: %v", err)
	}
	return ptr
}
