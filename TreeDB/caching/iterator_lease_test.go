package caching

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func mustIteratorLeaseStatInt64(t *testing.T, stats map[string]string, key string) int64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %q", key)
	}
	val, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return val
}

func mustIteratorLeaseStatFloat64(t *testing.T, stats map[string]string, key string) float64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %q", key)
	}
	val, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return val
}

func clearQueueLockedForIteratorLeaseTest(db *DB) {
	db.queue = nil
	db.queueShardIDs = nil
	db.queueLaneIDs = nil
	db.queueIDs = nil
	db.queueEnqueueNS = nil
	db.queueRanges = nil
	db.queueWALPaths = nil
	db.queueValueLogPaths = nil
	db.queueBacklogBytes.Store(0)
}

func TestIterator_QueuedViewLeaseHeldUntilClose(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}

	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	view := db.memtables.Load()
	if view == nil || len(view.queue) != 1 {
		t.Fatalf("expected one queued memtable view, got=%v", view)
	}
	queued, ok := view.queue[0].(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("queued memtable type=%T want *memtable.AppendOnly", view.queue[0])
	}
	if refs := view.refs.Load(); refs != 1 {
		t.Fatalf("view refs before iterator=%d want=1", refs)
	}
	baselineStats := db.Stats()
	retainBefore := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.memtable_view.retain_total")
	releaseBefore := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.memtable_view.release_total")
	leasesBefore := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.memtable_view.leases_inflight")
	deferredViewsTotalBefore := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.memtable_view.deferred_views_total")
	deferredMemtablesTotalBefore := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.memtable_view.deferred_memtables_total")
	deferredBytesTotalBefore := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.memtable_view.deferred_bytes_total")
	if deferredViewsCurrent := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.memtable_view.deferred_views_current"); deferredViewsCurrent != 0 {
		t.Fatalf("deferred views before iterator=%d want=0", deferredViewsCurrent)
	}
	if deferredMemtablesCurrent := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.memtable_view.deferred_memtables_current"); deferredMemtablesCurrent != 0 {
		t.Fatalf("deferred memtables before iterator=%d want=0", deferredMemtablesCurrent)
	}
	if deferredBytesCurrent := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.memtable_view.deferred_bytes_current"); deferredBytesCurrent != 0 {
		t.Fatalf("deferred bytes before iterator=%d want=0", deferredBytesCurrent)
	}
	if got := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.retired_memtable_hold.groups_current"); got != 0 {
		t.Fatalf("retired hold groups before iterator=%d want=0", got)
	}
	if got := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.retired_memtable_hold.memtables_current"); got != 0 {
		t.Fatalf("retired hold memtables before iterator=%d want=0", got)
	}
	if got := mustIteratorLeaseStatInt64(t, baselineStats, "treedb.cache.retired_memtable_hold.bytes_current"); got != 0 {
		t.Fatalf("retired hold bytes before iterator=%d want=0", got)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}

	if refs := view.refs.Load(); refs != 2 {
		t.Fatalf("view refs while iterator open=%d want=2", refs)
	}
	openStats := db.Stats()
	if got := mustIteratorLeaseStatInt64(t, openStats, "treedb.cache.memtable_view.retain_total"); got != retainBefore+1 {
		t.Fatalf("retain_total while iterator open=%d want=%d", got, retainBefore+1)
	}
	if got := mustIteratorLeaseStatInt64(t, openStats, "treedb.cache.memtable_view.release_total"); got != releaseBefore {
		t.Fatalf("release_total while iterator open=%d want=%d", got, releaseBefore)
	}
	if got := mustIteratorLeaseStatInt64(t, openStats, "treedb.cache.memtable_view.leases_inflight"); got != leasesBefore+1 {
		t.Fatalf("leases_inflight while iterator open=%d want=%d", got, leasesBefore+1)
	}
	if got := mustIteratorLeaseStatInt64(t, openStats, "treedb.cache.memtable_view.leases_inflight_max"); got < leasesBefore+1 {
		t.Fatalf("leases_inflight_max while iterator open=%d want >=%d", got, leasesBefore+1)
	}
	if got := mustIteratorLeaseStatInt64(t, openStats, "treedb.cache.memtable_view.deferred_views_current"); got != 0 {
		t.Fatalf("deferred_views_current while iterator open=%d want=0", got)
	}
	if got := mustIteratorLeaseStatInt64(t, openStats, "treedb.cache.memtable_view.deferred_memtables_current"); got != 0 {
		t.Fatalf("deferred_memtables_current while iterator open=%d want=0", got)
	}
	if got := mustIteratorLeaseStatInt64(t, openStats, "treedb.cache.memtable_view.deferred_bytes_current"); got != 0 {
		t.Fatalf("deferred_bytes_current while iterator open=%d want=0", got)
	}

	db.mu.Lock()
	db.queueRetiredMemtableLocked(queued)
	clearQueueLockedForIteratorLeaseTest(db)
	db.publishMemtablesLocked()
	db.mu.Unlock()

	if refs := view.refs.Load(); refs != 1 {
		t.Fatalf("view refs after publish with iterator open=%d want=1", refs)
	}
	if got := queued.Len(); got != 2 {
		t.Fatalf("queued memtable reset too early len=%d want=2", got)
	}
	deferredStats := db.Stats()
	db.retiredMemtablesMu.Lock()
	deferredRetired := len(db.deferredRetiredMemtables)
	db.retiredMemtablesMu.Unlock()
	if deferredRetired != 1 {
		t.Fatalf("global deferred retired memtables after publish=%d want=1", deferredRetired)
	}
	if got := mustIteratorLeaseStatInt64(t, deferredStats, "treedb.cache.memtable_view.deferred_views_current"); got != 0 {
		t.Fatalf("deferred_views_current after publish=%d want=0 with global retired hold", got)
	}
	if got := mustIteratorLeaseStatInt64(t, deferredStats, "treedb.cache.memtable_view.deferred_views_total"); got != deferredViewsTotalBefore {
		t.Fatalf("deferred_views_total after publish=%d want=%d", got, deferredViewsTotalBefore)
	}
	if got := mustIteratorLeaseStatInt64(t, deferredStats, "treedb.cache.memtable_view.deferred_memtables_current"); got != 0 {
		t.Fatalf("deferred_memtables_current after publish=%d want=0 with global retired hold", got)
	}
	if got := mustIteratorLeaseStatInt64(t, deferredStats, "treedb.cache.memtable_view.deferred_memtables_total"); got != deferredMemtablesTotalBefore {
		t.Fatalf("deferred_memtables_total after publish=%d want=%d", got, deferredMemtablesTotalBefore)
	}
	if got := mustIteratorLeaseStatInt64(t, deferredStats, "treedb.cache.memtable_view.deferred_bytes_current"); got != 0 {
		t.Fatalf("deferred_bytes_current after publish=%d want=0 with global retired hold", got)
	}
	if got := mustIteratorLeaseStatInt64(t, deferredStats, "treedb.cache.memtable_view.deferred_bytes_total"); got != deferredBytesTotalBefore {
		t.Fatalf("deferred_bytes_total after publish=%d want=%d", got, deferredBytesTotalBefore)
	}
	if got := mustIteratorLeaseStatFloat64(t, deferredStats, "treedb.cache.memtable_view.deferred_oldest_age_ms"); got != 0 {
		t.Fatalf("deferred_oldest_age_ms after publish=%f want=0 with global retired hold", got)
	}
	if got := mustIteratorLeaseStatInt64(t, deferredStats, "treedb.cache.retired_memtable_hold.groups_current"); got != 1 {
		t.Fatalf("retired hold groups after publish=%d want=1", got)
	}
	if got := mustIteratorLeaseStatInt64(t, deferredStats, "treedb.cache.retired_memtable_hold.memtables_current"); got != 1 {
		t.Fatalf("retired hold memtables after publish=%d want=1", got)
	}
	if got := mustIteratorLeaseStatInt64(t, deferredStats, "treedb.cache.retired_memtable_hold.bytes_current"); got <= 0 {
		t.Fatalf("retired hold bytes after publish=%d want >0", got)
	}
	if got := mustIteratorLeaseStatFloat64(t, deferredStats, "treedb.cache.retired_memtable_hold.oldest_age_ms"); got < 0 {
		t.Fatalf("retired hold oldest age after publish=%f want >=0", got)
	}

	seen := map[string]string{}
	for it.Valid() {
		seen[string(it.Key())] = string(it.Value())
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if len(seen) != 2 || seen["k1"] != "v1" || seen["k2"] != "v2" {
		t.Fatalf("iterator values=%v want map[k1:v1 k2:v2]", seen)
	}

	if err := it.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	if refs := view.refs.Load(); refs != 0 {
		t.Fatalf("view refs after first close=%d want=0", refs)
	}
	if got := queued.Len(); got != 0 {
		t.Fatalf("queued memtable len after first close=%d want=0", got)
	}
	db.retiredMemtablesMu.Lock()
	deferredRetired = len(db.deferredRetiredMemtables)
	db.retiredMemtablesMu.Unlock()
	if deferredRetired != 0 {
		t.Fatalf("global deferred retired memtables after iterator close=%d want=0", deferredRetired)
	}
	closedStats := db.Stats()
	if got := mustIteratorLeaseStatInt64(t, closedStats, "treedb.cache.memtable_view.release_total"); got != releaseBefore+1 {
		t.Fatalf("release_total after iterator close=%d want=%d", got, releaseBefore+1)
	}
	if got := mustIteratorLeaseStatInt64(t, closedStats, "treedb.cache.memtable_view.leases_inflight"); got != leasesBefore {
		t.Fatalf("leases_inflight after iterator close=%d want=%d", got, leasesBefore)
	}
	if got := mustIteratorLeaseStatInt64(t, closedStats, "treedb.cache.memtable_view.deferred_views_current"); got != 0 {
		t.Fatalf("deferred_views_current after iterator close=%d want=0", got)
	}
	if got := mustIteratorLeaseStatInt64(t, closedStats, "treedb.cache.memtable_view.deferred_memtables_current"); got != 0 {
		t.Fatalf("deferred_memtables_current after iterator close=%d want=0", got)
	}
	if got := mustIteratorLeaseStatInt64(t, closedStats, "treedb.cache.memtable_view.deferred_bytes_current"); got != 0 {
		t.Fatalf("deferred_bytes_current after iterator close=%d want=0", got)
	}
	if got := mustIteratorLeaseStatFloat64(t, closedStats, "treedb.cache.memtable_view.deferred_oldest_age_ms"); got != 0 {
		t.Fatalf("deferred_oldest_age_ms after iterator close=%f want=0", got)
	}
	if got := mustIteratorLeaseStatInt64(t, closedStats, "treedb.cache.retired_memtable_hold.groups_current"); got != 0 {
		t.Fatalf("retired hold groups after iterator close=%d want=0", got)
	}
	if got := mustIteratorLeaseStatInt64(t, closedStats, "treedb.cache.retired_memtable_hold.memtables_current"); got != 0 {
		t.Fatalf("retired hold memtables after iterator close=%d want=0", got)
	}
	if got := mustIteratorLeaseStatInt64(t, closedStats, "treedb.cache.retired_memtable_hold.bytes_current"); got != 0 {
		t.Fatalf("retired hold bytes after iterator close=%d want=0", got)
	}
	if got := mustIteratorLeaseStatFloat64(t, closedStats, "treedb.cache.retired_memtable_hold.oldest_age_ms"); got != 0 {
		t.Fatalf("retired hold oldest age after iterator close=%f want=0", got)
	}

	if err := it.Close(); err != nil {
		t.Fatalf("close second: %v", err)
	}
	if refs := view.refs.Load(); refs != 0 {
		t.Fatalf("view refs after second close=%d want=0", refs)
	}
	secondCloseStats := db.Stats()
	if got := mustIteratorLeaseStatInt64(t, secondCloseStats, "treedb.cache.memtable_view.release_total"); got != releaseBefore+1 {
		t.Fatalf("release_total after second close=%d want=%d", got, releaseBefore+1)
	}
}

func TestUntrackedMemtableViewRetainDoesNotCreateGlobalRetiredHold(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}

	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	view := db.memtables.Load()
	if view == nil || len(view.queue) != 1 {
		t.Fatalf("expected one queued memtable view, got=%v", view)
	}
	queued, ok := view.queue[0].(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("queued memtable type=%T want *memtable.AppendOnly", view.queue[0])
	}

	untracked := db.retainMemtableViewUntracked()
	if untracked != view {
		t.Fatalf("retainMemtableViewUntracked()=%p want current view %p", untracked, view)
	}
	if readers := db.memtableViewReaders.Load(); readers != 0 {
		t.Fatalf("untracked retain registered memtable readers=%d want=0", readers)
	}

	db.mu.Lock()
	db.queueRetiredMemtableLocked(queued)
	clearQueueLockedForIteratorLeaseTest(db)
	db.publishMemtablesLocked()
	db.mu.Unlock()

	db.retiredMemtablesMu.Lock()
	deferredRetired := len(db.deferredRetiredMemtables)
	db.retiredMemtablesMu.Unlock()
	if deferredRetired != 0 {
		t.Fatalf("global deferred retired memtables after untracked retain publish=%d want=0", deferredRetired)
	}
	if got := queued.Len(); got != 2 {
		t.Fatalf("queued memtable reset while untracked retain open len=%d want=2", got)
	}

	db.releaseUntrackedMemtableView(untracked)
	if got := queued.Len(); got != 0 {
		t.Fatalf("queued memtable len after untracked release=%d want=0", got)
	}
}

func TestRetiredMemtableHoldWaitsForUntrackedRetainAfterTrackedRelease(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}

	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	view := db.memtables.Load()
	if view == nil || len(view.queue) != 1 {
		t.Fatalf("expected one queued memtable view, got=%v", view)
	}
	queued, ok := view.queue[0].(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("queued memtable type=%T want *memtable.AppendOnly", view.queue[0])
	}

	tracked := db.retainMemtableView()
	if tracked != view {
		t.Fatalf("retainMemtableView()=%p want current view %p", tracked, view)
	}
	untracked := db.retainMemtableViewUntracked()
	if untracked != view {
		t.Fatalf("retainMemtableViewUntracked()=%p want current view %p", untracked, view)
	}

	db.mu.Lock()
	db.queueRetiredMemtableLocked(queued)
	clearQueueLockedForIteratorLeaseTest(db)
	db.publishMemtablesLocked()
	db.mu.Unlock()

	if refs := view.refs.Load(); refs != 2 {
		t.Fatalf("view refs after publish with tracked+untracked retains=%d want=2", refs)
	}
	if got := queued.Len(); got != 2 {
		t.Fatalf("queued memtable reset before releases len=%d want=2", got)
	}
	db.retiredMemtablesMu.Lock()
	deferredRetired := len(db.deferredRetiredMemtables)
	db.retiredMemtablesMu.Unlock()
	if deferredRetired != 1 {
		t.Fatalf("global deferred retired memtables after publish=%d want=1", deferredRetired)
	}

	db.releaseMemtableView(tracked)

	if refs := view.refs.Load(); refs != 1 {
		t.Fatalf("view refs after tracked release=%d want=1", refs)
	}
	if readers := db.memtableViewReaders.Load(); readers != 0 {
		t.Fatalf("external reader count after tracked release=%d want=0", readers)
	}
	if got := queued.Len(); got != 2 {
		t.Fatalf("queued memtable reset while untracked retain remains len=%d want=2", got)
	}
	db.retiredMemtablesMu.Lock()
	deferredRetired = len(db.deferredRetiredMemtables)
	db.retiredMemtablesMu.Unlock()
	if deferredRetired != 1 {
		t.Fatalf("global deferred retired memtables after tracked release=%d want=1", deferredRetired)
	}

	db.releaseUntrackedMemtableView(untracked)

	if refs := view.refs.Load(); refs != 0 {
		t.Fatalf("view refs after untracked release=%d want=0", refs)
	}
	if got := queued.Len(); got != 0 {
		t.Fatalf("queued memtable len after untracked release=%d want=0", got)
	}
	db.retiredMemtablesMu.Lock()
	deferredRetired = len(db.deferredRetiredMemtables)
	db.retiredMemtablesMu.Unlock()
	if deferredRetired != 0 {
		t.Fatalf("global deferred retired memtables after untracked release=%d want=0", deferredRetired)
	}
}

func TestUntrackedMemtableViewReleaseDoesNotUnregisterExternalReader(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	view := db.memtables.Load()
	if view == nil {
		t.Fatal("expected published memtable view")
	}

	db.registerMemtableViewReader(view)
	view.refs.Add(1)
	untracked := db.retainMemtableViewUntracked()
	if untracked != view {
		t.Fatalf("retainMemtableViewUntracked()=%p want current view %p", untracked, view)
	}

	db.releaseUntrackedMemtableView(untracked)

	if readers := db.memtableViewReaders.Load(); readers != 1 {
		t.Fatalf("external reader count after untracked release=%d want=1", readers)
	}
	db.retiredMemtablesMu.Lock()
	retainedRefs := db.retainedMemtableViews[view]
	db.retiredMemtablesMu.Unlock()
	if retainedRefs != 1 {
		t.Fatalf("retained view refs after untracked release=%d want=1", retainedRefs)
	}

	db.releaseMemtableViewRef(view, false, true)
	if readers := db.memtableViewReaders.Load(); readers != 0 {
		t.Fatalf("external reader count after external release=%d want=0", readers)
	}
}

func TestDeferRetiredMemtablesCopiesHeldSlice(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}

	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	view := db.memtables.Load()
	if view == nil || len(view.queue) != 1 {
		t.Fatalf("expected one queued memtable view, got=%v", view)
	}
	queued, ok := view.queue[0].(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("queued memtable type=%T want *memtable.AppendOnly", view.queue[0])
	}

	tracked := db.retainMemtableView()
	if tracked != view {
		t.Fatalf("retainMemtableView()=%p want current view %p", tracked, view)
	}
	retired := []memtable.Table{queued}
	if !db.deferRetiredMemtables(retired) {
		t.Fatal("deferRetiredMemtables did not hold retained view memtable")
	}
	retired[0] = nil

	db.retiredMemtablesMu.Lock()
	if len(db.deferredRetiredMemtables) != 1 {
		db.retiredMemtablesMu.Unlock()
		t.Fatalf("deferred retired holds=%d want=1", len(db.deferredRetiredMemtables))
	}
	held := db.deferredRetiredMemtables[0]
	db.retiredMemtablesMu.Unlock()
	if len(held.mems) != 1 || held.mems[0] != queued {
		t.Fatalf("held memtables mutated with caller slice: %+v", held.mems)
	}
	if held.memtables != 1 {
		t.Fatalf("held memtable count=%d want=1", held.memtables)
	}
	if held.bytes <= 0 {
		t.Fatalf("held bytes=%d want >0", held.bytes)
	}

	db.releasePublishedMemtableView(view)
	db.releaseMemtableView(tracked)
	if got := queued.Len(); got != 0 {
		t.Fatalf("queued memtable len after retained view release=%d want=0", got)
	}
}

type failingIteratorBackend struct {
	*MockBackend
	err error
}

func (b *failingIteratorBackend) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return nil, b.err
}

func TestIterator_BackendErrorClosesQueuedIterators(t *testing.T) {
	backend := &failingIteratorBackend{
		MockBackend: NewMockBackend(),
		err:         errors.New("backend iterator boom"),
	}
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}

	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.backendRangeKnown = true
	db.backendRange = keyRange{valid: true, min: []byte("a"), max: []byte("z")}
	db.mu.Unlock()
	db.backendRangeInit.Do(func() {})

	view := db.memtables.Load()
	if view == nil || len(view.queue) != 1 {
		t.Fatalf("expected one queued memtable view, got=%v", view)
	}
	queued, ok := view.queue[0].(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("queued memtable type=%T want *memtable.AppendOnly", view.queue[0])
	}

	if _, err := db.Iterator(nil, nil); err == nil {
		t.Fatalf("expected iterator error")
	}

	done := make(chan struct{})
	go func() {
		db.mu.Lock()
		db.queueRetiredMemtableLocked(queued)
		clearQueueLockedForIteratorLeaseTest(db)
		db.publishMemtablesLocked()
		db.mu.Unlock()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish blocked; queued iterator lease likely leaked on backend error")
	}

	if refs := view.refs.Load(); refs != 0 {
		t.Fatalf("view refs after publish=%d want=0", refs)
	}
	if got := queued.Len(); got != 0 {
		t.Fatalf("queued memtable len after publish=%d want=0", got)
	}
}
