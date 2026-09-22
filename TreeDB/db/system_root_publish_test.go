package db

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func mustFrozenSystemMemtable(tb testing.TB, kvs ...string) memtable.Table {
	tb.Helper()
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		tb.Fatalf("new memtable: %v", err)
	}
	for i := 0; i+1 < len(kvs); i += 2 {
		mt.Set([]byte(kvs[i]), []byte(kvs[i+1]))
	}
	mt.Freeze()
	return mt
}

func mustFrozenSystemPointerMemtable(tb testing.TB, key string, ptr page.ValuePtr) memtable.Table {
	tb.Helper()
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		tb.Fatalf("new memtable: %v", err)
	}
	mt.SetEntry([]byte(key), nil, ptr, node.FlagPointer)
	mt.Freeze()
	return mt
}

// advancePastRetainedDurableSlotForTest first checkpoints the unreachable
// state, then publishes and checkpoints one inline-only successor. The first
// checkpoint places the removal in one durable slot; the successor overwrites
// the other slot that could still retain the resource. Callers must only use
// this after the system-root contents they care about were removed, because the
// helper intentionally publishes a replacement system root.
func advancePastRetainedDurableSlotForTest(tb testing.TB, db *DB) {
	tb.Helper()
	before := db.currentCommitSeq()
	if err := db.Checkpoint(); err != nil {
		tb.Fatalf("checkpoint unreachable state before durable-slot advance: %v", err)
	}
	durableCommit, _ := durableSlotStateForTest(db)
	if durableCommit < before {
		tb.Fatalf("unreachable state durable sequence=%d want at least %d", durableCommit, before)
	}
	key := fmt.Sprintf("sys/test/durable-slot-advance/%020d", before+1)
	if _, err := db.PublishSystemRootIterator(mustFrozenSystemMemtable(tb, key, "advance").NewIterator(nil, nil)); err != nil {
		tb.Fatalf("advance past retained durable slot: %v", err)
	}
	if got, want := db.currentCommitSeq(), before+1; got != want {
		tb.Fatalf("durable-slot advance commit sequence=%d want %d", got, want)
	}
	if err := db.Checkpoint(); err != nil {
		tb.Fatalf("checkpoint durable-slot advance: %v", err)
	}
	durableCommit, slotCommits := durableSlotStateForTest(db)
	if durableCommit < before+1 {
		tb.Fatalf("durable-slot advance durable sequence=%d want at least %d", durableCommit, before+1)
	}
	for slot, commit := range slotCommits {
		if commit < before {
			tb.Fatalf("durable slot %d sequence=%d want at least unreachable sequence %d", slot, commit, before)
		}
	}
}

func durableSlotStateForTest(db *DB) (uint64, [2]uint64) {
	db.durablePublishMu.Lock()
	db.rootReuseMu.RLock()
	durableCommit := db.durableRoot.record.CommitSeq
	slotCommits := db.durableRoot.slotCommit
	db.rootReuseMu.RUnlock()
	db.durablePublishMu.Unlock()
	return durableCommit, slotCommits
}

func systemRangeKVs(count int, overrides map[int]string) []string {
	kvs := make([]string, 0, count*2)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("sys/%04d", i)
		value := fmt.Sprintf("value-%04d", i)
		if override, ok := overrides[i]; ok {
			value = override
		}
		kvs = append(kvs, key, value)
	}
	return kvs
}

func collectRootPageIDs(tb testing.TB, db *DB, rootID uint64) []uint64 {
	tb.Helper()
	snap := db.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("expected snapshot")
	}
	defer snap.Close()
	tr, err := snap.treeAtRoot(rootID)
	if err != nil {
		tb.Fatalf("treeAtRoot(%d): %v", rootID, err)
	}
	pageIDs, err := tr.CollectPageIDs()
	if err != nil {
		tb.Fatalf("CollectPageIDs(%d): %v", rootID, err)
	}
	return pageIDs
}

func TestPublishSystemRootIterator_PersistsAndPreservesUserRoot(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	if err := db.Set([]byte("user/a"), []byte("uv")); err != nil {
		t.Fatalf("set user key: %v", err)
	}

	before := db.State()
	if before == nil {
		t.Fatal("expected backend state")
	}

	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	mt.Set([]byte("sys/a"), []byte("sv"))
	mt.Freeze()

	newSystemRoot, err := db.PublishSystemRootIterator(mt.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish system root: %v", err)
	}

	after := db.State()
	if after == nil {
		t.Fatal("expected state after publish")
	}
	if after.RootPageID != before.RootPageID {
		t.Fatalf("user root changed: got %d want %d", after.RootPageID, before.RootPageID)
	}
	if after.SystemRootPageID != newSystemRoot {
		t.Fatalf("system root=%d want %d", after.SystemRootPageID, newSystemRoot)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	entry, err := snap.GetEntryAtRoot(after.SystemRootPageID, []byte("sys/a"))
	if err != nil {
		t.Fatalf("GetEntryAtRoot(system): %v", err)
	}
	if got := string(entry.Value); got != "sv" {
		t.Fatalf("system value=%q want %q", got, "sv")
	}
	if got, err := snap.Get([]byte("user/a")); err != nil || string(got) != "uv" {
		t.Fatalf("user get got=%q err=%v want uv", string(got), err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	closed = true

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	reopenSnap := reopened.AcquireSnapshot()
	if reopenSnap == nil {
		t.Fatal("expected reopen snapshot")
	}
	defer reopenSnap.Close()
	reopenState := reopened.State()
	if reopenState == nil {
		t.Fatal("expected reopen state")
	}
	entry, err = reopenSnap.GetEntryAtRoot(reopenState.SystemRootPageID, []byte("sys/a"))
	if err != nil {
		t.Fatalf("reopen GetEntryAtRoot(system): %v", err)
	}
	if got := string(entry.Value); got != "sv" {
		t.Fatalf("reopen system value=%q want %q", got, "sv")
	}
}

func TestPublishSystemRootIterator_CommittedValueLogPointerReadableViaSnapshotRoot(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	appender, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("new replay inline appender: %v", err)
	}
	defer func() { _ = appender.close() }()
	db.SetValueLogAppender(appender)
	defer db.SetValueLogAppender(nil)

	before := db.State()
	if before == nil {
		t.Fatal("expected initial state")
	}

	key := []byte("sys/collections/users/catalog-meta")
	want := bytes.Repeat([]byte(`{"collection":"users","meta_version":5}|`), 8)
	ptrs, err := db.AppendValueLogValues([][]byte{want})
	if err != nil {
		t.Fatalf("append system catalog value-log value: %v", err)
	}
	if len(ptrs) != 1 {
		t.Fatalf("AppendValueLogValues returned %d ptrs, want 1", len(ptrs))
	}
	if ptrs[0].FileID == 0 || !page.IsValueLogFileID(ptrs[0].FileID) {
		t.Fatalf("AppendValueLogValues returned non-value-log pointer: %+v", ptrs[0])
	}

	systemTable := mustFrozenSystemPointerMemtable(t, string(key), ptrs[0])
	systemRoot, err := db.PublishSystemRootIterator(systemTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish system root: %v", err)
	}
	if systemRoot == 0 {
		t.Fatal("publish system root returned root 0")
	}

	after := db.State()
	if after == nil {
		t.Fatal("expected state after publish")
	}
	if after.CommitSeq <= before.CommitSeq {
		t.Fatalf("commit seq did not advance: before=%d after=%d", before.CommitSeq, after.CommitSeq)
	}
	if after.SystemRootPageID != systemRoot {
		t.Fatalf("system root page=%d want %d", after.SystemRootPageID, systemRoot)
	}
	if after.ValueLogSet == nil {
		t.Fatal("state missing value-log set after pointer system-root publish")
	}
	if _, ok := after.ValueLogSet.Files[ptrs[0].FileID]; !ok {
		t.Fatalf("committed state missing value-log file %d", ptrs[0].FileID)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	entry, err := snap.GetEntryAtRoot(systemRoot, key)
	if err != nil {
		t.Fatalf("GetEntryAtRoot(system catalog pointer): %v", err)
	}
	if entry.Flags&node.FlagPointer == 0 {
		t.Fatalf("system catalog entry flags=%#x want value-log pointer", entry.Flags)
	}
	if entry.ValuePtr != ptrs[0] {
		t.Fatalf("system catalog pointer=%+v want %+v", entry.ValuePtr, ptrs[0])
	}

	got, err := snap.GetAtRoot(systemRoot, key)
	if err != nil {
		t.Fatalf("GetAtRoot(system catalog value): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("system catalog value mismatch: got %d bytes want %d", len(got), len(want))
	}

	rootReader, err := snap.ReaderAtRoot(systemRoot)
	if err != nil {
		t.Fatalf("ReaderAtRoot(system root): %v", err)
	}
	got, err = rootReader.GetAppend(key, nil)
	if err != nil {
		t.Fatalf("SnapshotRootReader.GetAppend(system catalog value): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("system catalog root reader value mismatch: got %d bytes want %d", len(got), len(want))
	}
}

func TestPublishSystemRootIterator_DoesNotCreateBatch(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	called := 0
	db.testBatchCreateHook = func() { called++ }

	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	mt.Set([]byte("sys/a"), []byte("sv"))
	mt.Freeze()

	if _, err := db.PublishSystemRootIterator(mt.NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish system root: %v", err)
	}
	if called != 0 {
		t.Fatalf("testBatchCreateHook called %d times; want 0", called)
	}
}

func TestPublishSystemRootIterator_ColdPublishDoesNotCountWarmFallback(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	mt := mustFrozenSystemMemtable(t, "sys/a", "sv")
	if _, err := db.PublishSystemRootIterator(mt.NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish system root: %v", err)
	}

	stats := db.systemRootPublishStatsSnapshot()
	if stats.warmAttempts != 0 {
		t.Fatalf("warmAttempts=%d want 0", stats.warmAttempts)
	}
	if stats.warmRebuildFallbacks != 0 {
		t.Fatalf("warmRebuildFallbacks=%d want 0", stats.warmRebuildFallbacks)
	}
}

func TestPublishSystemRootIterator_WarmPublishUsesFallbackCounter(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 1

	initial := mustFrozenSystemMemtable(t, "sys/a", "sv-a")
	if _, err := db.PublishSystemRootIterator(initial.NewIterator(nil, nil)); err != nil {
		t.Fatalf("initial publish system root: %v", err)
	}

	warm := mustFrozenSystemMemtable(t, "sys/a", "sv-a2", "sys/b", "sv-b")
	if _, err := db.PublishSystemRootIterator(warm.NewIterator(nil, nil)); err != nil {
		t.Fatalf("warm publish system root: %v", err)
	}

	stats := db.systemRootPublishStatsSnapshot()
	if stats.warmAttempts != 1 {
		t.Fatalf("warmAttempts=%d want 1", stats.warmAttempts)
	}
	if stats.warmRebuildFallbacks != 1 {
		t.Fatalf("warmRebuildFallbacks=%d want 1", stats.warmRebuildFallbacks)
	}
}

func TestPublishSystemRootIterator_PersistsAcrossReopen_AfterWarmFallback(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	db.testSystemRootWarmMaxDeltaOps = 1

	if err := db.Set([]byte("user/a"), []byte("uv")); err != nil {
		t.Fatalf("set user key: %v", err)
	}

	initial := mustFrozenSystemMemtable(t, "sys/a", "sv-a")
	if _, err := db.PublishSystemRootIterator(initial.NewIterator(nil, nil)); err != nil {
		t.Fatalf("initial publish system root: %v", err)
	}

	warm := mustFrozenSystemMemtable(t, "sys/a", "sv-a2", "sys/b", "sv-b")
	newSystemRoot, err := db.PublishSystemRootIterator(warm.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("warm publish system root: %v", err)
	}

	stats := db.systemRootPublishStatsSnapshot()
	if stats.warmAttempts != 1 {
		t.Fatalf("warmAttempts=%d want 1", stats.warmAttempts)
	}
	if stats.warmRebuildFallbacks != 1 {
		t.Fatalf("warmRebuildFallbacks=%d want 1", stats.warmRebuildFallbacks)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	closed = true

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	reopenState := reopened.State()
	if reopenState == nil {
		t.Fatal("expected reopen state")
	}
	if reopenState.SystemRootPageID != newSystemRoot {
		t.Fatalf("reopen system root=%d want %d", reopenState.SystemRootPageID, newSystemRoot)
	}

	reopenSnap := reopened.AcquireSnapshot()
	if reopenSnap == nil {
		t.Fatal("expected reopen snapshot")
	}
	defer reopenSnap.Close()

	entry, err := reopenSnap.GetEntryAtRoot(reopenState.SystemRootPageID, []byte("sys/a"))
	if err != nil {
		t.Fatalf("reopen GetEntryAtRoot(sys/a): %v", err)
	}
	if got := string(entry.Value); got != "sv-a2" {
		t.Fatalf("reopen sys/a=%q want %q", got, "sv-a2")
	}
	entry, err = reopenSnap.GetEntryAtRoot(reopenState.SystemRootPageID, []byte("sys/b"))
	if err != nil {
		t.Fatalf("reopen GetEntryAtRoot(sys/b): %v", err)
	}
	if got := string(entry.Value); got != "sv-b" {
		t.Fatalf("reopen sys/b=%q want %q", got, "sv-b")
	}
	if got, err := reopenSnap.Get([]byte("user/a")); err != nil || string(got) != "uv" {
		t.Fatalf("reopen user get got=%q err=%v want uv", string(got), err)
	}
}

func TestPublishSystemRootIterator_StatsExposeWarmFallbackCounters(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 1

	initial := mustFrozenSystemMemtable(t, "sys/a", "sv-a")
	if _, err := db.PublishSystemRootIterator(initial.NewIterator(nil, nil)); err != nil {
		t.Fatalf("initial publish system root: %v", err)
	}
	warm := mustFrozenSystemMemtable(t, "sys/a", "sv-a2", "sys/b", "sv-b")
	if _, err := db.PublishSystemRootIterator(warm.NewIterator(nil, nil)); err != nil {
		t.Fatalf("warm publish system root: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.publish.system_root.warm_attempts"]; got != "1" {
		t.Fatalf("warm_attempts=%q want 1", got)
	}
	if got := stats["treedb.publish.system_root.warm_rebuild_fallbacks"]; got != "1" {
		t.Fatalf("warm_rebuild_fallbacks=%q want 1", got)
	}
}

func TestPublishSystemRootIterator_WarmSparseDelta_PreservesSomePages(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	initial := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	initialRoot, err := db.PublishSystemRootIterator(initial.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("initial publish system root: %v", err)
	}
	oldPages := collectRootPageIDs(t, db, initialRoot)

	sparse := mustFrozenSystemMemtable(t, systemRangeKVs(2048, map[int]string{
		1024: "value-1024-updated",
	})...)
	newRoot, err := db.PublishSystemRootIterator(sparse.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("sparse warm publish system root: %v", err)
	}
	if newRoot == initialRoot {
		t.Fatalf("expected new system root id after sparse warm publish")
	}

	stats := db.systemRootPublishStatsSnapshot()
	if stats.warmAttempts != 1 {
		t.Fatalf("warmAttempts=%d want 1", stats.warmAttempts)
	}
	if stats.warmNativeApplyAttempts != 1 {
		t.Fatalf("warmNativeApplyAttempts=%d want 1", stats.warmNativeApplyAttempts)
	}
	if stats.warmRebuildFallbacks != 0 {
		t.Fatalf("warmRebuildFallbacks=%d want 0", stats.warmRebuildFallbacks)
	}
	if stats.warmPreservedPages == 0 {
		t.Fatalf("warmPreservedPages=%d want >0", stats.warmPreservedPages)
	}
	if stats.warmRewrittenPages >= uint64(len(oldPages)) {
		t.Fatalf("warmRewrittenPages=%d want <%d", stats.warmRewrittenPages, len(oldPages))
	}
}

func TestPublishSystemRootIterator_WarmSparseDelta_DoesNotFallbackBelowThreshold(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	initial := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	if _, err := db.PublishSystemRootIterator(initial.NewIterator(nil, nil)); err != nil {
		t.Fatalf("initial publish system root: %v", err)
	}
	sparse := mustFrozenSystemMemtable(t, systemRangeKVs(2048, map[int]string{
		17: "value-0017-updated",
	})...)
	if _, err := db.PublishSystemRootIterator(sparse.NewIterator(nil, nil)); err != nil {
		t.Fatalf("sparse warm publish system root: %v", err)
	}

	stats := db.systemRootPublishStatsSnapshot()
	if stats.warmAttempts != 1 {
		t.Fatalf("warmAttempts=%d want 1", stats.warmAttempts)
	}
	if stats.warmNativeApplyAttempts != 1 {
		t.Fatalf("warmNativeApplyAttempts=%d want 1", stats.warmNativeApplyAttempts)
	}
	if stats.warmRebuildFallbacks != 0 {
		t.Fatalf("warmRebuildFallbacks=%d want 0", stats.warmRebuildFallbacks)
	}
}

func TestPublishSystemRootIterator_WarmDenseDelta_UsesRebuildFallbackAboveThreshold(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	initial := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	if _, err := db.PublishSystemRootIterator(initial.NewIterator(nil, nil)); err != nil {
		t.Fatalf("initial publish system root: %v", err)
	}
	denseOverrides := make(map[int]string, 1024)
	for i := 0; i < 1024; i++ {
		denseOverrides[i] = fmt.Sprintf("value-%04d-updated", i)
	}
	dense := mustFrozenSystemMemtable(t, systemRangeKVs(2048, denseOverrides)...)
	if _, err := db.PublishSystemRootIterator(dense.NewIterator(nil, nil)); err != nil {
		t.Fatalf("dense warm publish system root: %v", err)
	}

	stats := db.systemRootPublishStatsSnapshot()
	if stats.warmAttempts != 1 {
		t.Fatalf("warmAttempts=%d want 1", stats.warmAttempts)
	}
	if stats.warmNativeApplyAttempts != 0 {
		t.Fatalf("warmNativeApplyAttempts=%d want 0", stats.warmNativeApplyAttempts)
	}
	if stats.warmRebuildFallbacks != 1 {
		t.Fatalf("warmRebuildFallbacks=%d want 1", stats.warmRebuildFallbacks)
	}
}

func TestPublishSystemRootIterator_WarmApplyHandlesInsertUpdateDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	db.testSystemRootWarmMaxDeltaOps = 8

	initial := mustFrozenSystemMemtable(t,
		"sys/a", "va",
		"sys/b", "vb",
		"sys/c", "vc",
	)
	if _, err := db.PublishSystemRootIterator(initial.NewIterator(nil, nil)); err != nil {
		t.Fatalf("initial publish system root: %v", err)
	}

	target := mustFrozenSystemMemtable(t,
		"sys/a", "va",
		"sys/b", "vb2",
		"sys/d", "vd",
	)
	newSystemRoot, err := db.PublishSystemRootIterator(target.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("warm publish system root: %v", err)
	}

	stats := db.systemRootPublishStatsSnapshot()
	if stats.warmNativeApplyAttempts != 1 {
		t.Fatalf("warmNativeApplyAttempts=%d want 1", stats.warmNativeApplyAttempts)
	}
	if stats.warmRebuildFallbacks != 0 {
		t.Fatalf("warmRebuildFallbacks=%d want 0", stats.warmRebuildFallbacks)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	if entry, err := snap.GetEntryAtRoot(newSystemRoot, []byte("sys/a")); err != nil || string(entry.Value) != "va" {
		t.Fatalf("sys/a got=%q err=%v want va", string(entry.Value), err)
	}
	if entry, err := snap.GetEntryAtRoot(newSystemRoot, []byte("sys/b")); err != nil || string(entry.Value) != "vb2" {
		t.Fatalf("sys/b got=%q err=%v want vb2", string(entry.Value), err)
	}
	if _, err := snap.GetEntryAtRoot(newSystemRoot, []byte("sys/c")); err == nil {
		t.Fatal("expected sys/c to be deleted")
	}
	if entry, err := snap.GetEntryAtRoot(newSystemRoot, []byte("sys/d")); err != nil || string(entry.Value) != "vd" {
		t.Fatalf("sys/d got=%q err=%v want vd", string(entry.Value), err)
	}
}

func TestPublishSystemRootIterator_UsesGenericOrderedRootPublishHelper(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	calls := 0
	db.testOrderedRootPublishHook = func(uint64) { calls++ }

	initial := mustFrozenSystemMemtable(t, "sys/a", "sv-a")
	if _, err := db.PublishSystemRootIterator(initial.NewIterator(nil, nil)); err != nil {
		t.Fatalf("initial publish system root: %v", err)
	}
	warm := mustFrozenSystemMemtable(t, "sys/a", "sv-b")
	if _, err := db.PublishSystemRootIterator(warm.NewIterator(nil, nil)); err != nil {
		t.Fatalf("warm publish system root: %v", err)
	}
	if calls != 2 {
		t.Fatalf("ordered root helper calls=%d want 2", calls)
	}
}

func TestPublishSystemRootIterator_WarmApplyUpdatesValueLogRefTrackerInline(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	oldPtr := appendPointersInNewSegment(t, dir, 0, 11, 10_000, 1, func(int) []byte {
		return []byte("old-system-pointer")
	})[0]
	newPtr := appendPointersInNewSegment(t, dir, 0, 12, 20_000, 1, func(int) []byte {
		return []byte("new-system-pointer")
	})[0]

	initial := mustFrozenSystemPointerMemtable(t, "sys/p", oldPtr)
	if _, err := db.PublishSystemRootIterator(initial.NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish initial system root: %v", err)
	}

	target := mustFrozenSystemPointerMemtable(t, "sys/p", newPtr)
	if _, err := db.PublishSystemRootIterator(target.NewIterator(nil, nil)); err != nil {
		t.Fatalf("publish warm system root: %v", err)
	}

	seq := db.currentCommitSeq()
	incRefs, ok := db.valueLogRefTracker.referencedSet(seq)
	if !ok {
		t.Fatalf("expected incremental ref set for seq=%d", seq)
	}
	if _, ok := incRefs[newPtr.FileID]; !ok {
		t.Fatalf("expected new pointer file %d in ref set", newPtr.FileID)
	}
	if _, ok := incRefs[oldPtr.FileID]; ok {
		t.Fatalf("expected old pointer file %d to be removed", oldPtr.FileID)
	}

	fullCounts, fullSeq, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if fullSeq != seq {
		t.Fatalf("scan seq mismatch: got=%d want=%d", fullSeq, seq)
	}
	fullRefs := valueLogRefSetFromCounts(fullCounts)
	if !reflect.DeepEqual(incRefs, fullRefs) {
		t.Fatalf("incremental/full-scan mismatch: incremental=%v full=%v", incRefs, fullRefs)
	}
}
