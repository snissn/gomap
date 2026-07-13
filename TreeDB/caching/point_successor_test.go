package caching

import (
	"bytes"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func openPointSuccessorTestDB(t *testing.T, backend *MockBackend) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:     1 << 30,
		MemtableShards:     4,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func requireSeekGE(t *testing.T, db *DB, start, end []byte, wantKey, wantValue []byte) {
	t.Helper()
	key, value, found, err := db.SeekGE(start, end)
	if err != nil {
		t.Fatalf("SeekGE(%q,%q): %v", start, end, err)
	}
	if !found || !bytes.Equal(key, wantKey) || !bytes.Equal(value, wantValue) {
		t.Fatalf("SeekGE(%q,%q) = (%q,%q,%t), want (%q,%q,true)", start, end, key, value, found, wantKey, wantValue)
	}
}

func rotatePointSuccessorMemtables(t *testing.T, db *DB) {
	t.Helper()
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.rotateMemtableLocked(false); err != nil {
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
}

func TestSeekGE_SourcePrecedenceTombstonesAndBounds(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("b"), []byte("backend-b"))
	backend.Set([]byte("c"), []byte("backend-c"))
	backend.Set([]byte("f"), []byte("backend-f"))
	db := openPointSuccessorTestDB(t, backend)

	if err := db.Set([]byte("d"), []byte("queued-d")); err != nil {
		t.Fatalf("Set queued d: %v", err)
	}
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete queued b: %v", err)
	}
	rotatePointSuccessorMemtables(t, db)
	if err := db.Set([]byte("c"), []byte("mutable-c")); err != nil {
		t.Fatalf("Set mutable c: %v", err)
	}

	requireSeekGE(t, db, []byte("b"), []byte("e"), []byte("c"), []byte("mutable-c"))
	requireSeekGE(t, db, []byte("d"), []byte("e"), []byte("d"), []byte("queued-d"))
	if key, value, found, err := db.SeekGE([]byte("e"), []byte("f")); err != nil || found || key != nil || value != nil {
		t.Fatalf("bounded miss = (%q,%q,%t,%v), want nil,nil,false,nil", key, value, found, err)
	}
}

func TestSeekGE_SourceCountersSpanTombstoneRounds(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("c"), []byte("older-visible"))
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:     1 << 30,
		MemtableShards:     1,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete b: %v", err)
	}
	if err := db.Set([]byte("c"), []byte("visible")); err != nil {
		t.Fatalf("Set c: %v", err)
	}

	before := db.Stats()
	requireSeekGE(t, db, []byte("b"), []byte("d"), []byte("c"), []byte("visible"))
	after := db.Stats()
	parse := func(name string, stats map[string]string) uint64 {
		t.Helper()
		value, err := strconv.ParseUint(stats[name], 10, 64)
		if err != nil {
			t.Fatalf("parse %s=%q: %v", name, stats[name], err)
		}
		return value
	}

	const sourcesTotal = "treedb.cache.point_successor.sources_total"
	if got := parse(sourcesTotal, after) - parse(sourcesTotal, before); got != 4 {
		t.Fatalf("sources inspected = %d, want 4 across two sources and two rounds", got)
	}
	const sourcesMax = "treedb.cache.point_successor.sources_max"
	if got := parse(sourcesMax, after); got != 4 {
		t.Fatalf("sources max = %d, want 4 for the whole SeekGE call", got)
	}
	const backendProbes = "treedb.cache.point_successor.backend_probes_total"
	if got := parse(backendProbes, after) - parse(backendProbes, before); got != 2 {
		t.Fatalf("backend probes = %d, want 2 across both rounds", got)
	}
}

func TestSeekGE_SourceCountersSkipDisjointNilBackend(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("z"), []byte("disjoint"))
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:     1 << 30,
		MemtableShards:     1,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete b: %v", err)
	}
	if err := db.Set([]byte("c"), []byte("visible")); err != nil {
		t.Fatalf("Set c: %v", err)
	}

	// Remove the published-root shortcut from this isolated view so SeekGE uses
	// the known backend range and returns a nil disk cursor for [b,d).
	db.writeMu.Lock()
	view := db.memtables.Load()
	view.rootIterator.published = nil
	view.rootIterator.publishedRootID = 0
	db.writeMu.Unlock()
	db.mu.Lock()
	db.backendRangeKnown = true
	db.backendRange = keyRange{valid: true, min: []byte("z"), max: []byte("z")}
	db.mu.Unlock()
	db.backendRangeInit.Do(func() {})

	before := db.Stats()
	requireSeekGE(t, db, []byte("b"), []byte("d"), []byte("c"), []byte("visible"))
	after := db.Stats()
	parse := func(name string, stats map[string]string) uint64 {
		t.Helper()
		value, err := strconv.ParseUint(stats[name], 10, 64)
		if err != nil {
			t.Fatalf("parse %s=%q: %v", name, stats[name], err)
		}
		return value
	}

	const sourcesTotal = "treedb.cache.point_successor.sources_total"
	if got := parse(sourcesTotal, after) - parse(sourcesTotal, before); got != 2 {
		t.Fatalf("sources inspected = %d, want 2 mutable probes across both rounds", got)
	}
	const sourcesMax = "treedb.cache.point_successor.sources_max"
	if got := parse(sourcesMax, after); got != 2 {
		t.Fatalf("sources max = %d, want 2 for the whole SeekGE call", got)
	}
	const backendProbes = "treedb.cache.point_successor.backend_probes_total"
	if got := parse(backendProbes, after) - parse(backendProbes, before); got != 0 {
		t.Fatalf("backend probes = %d, want 0 for disjoint nil cursor", got)
	}
}

func TestSeekGE_NewerRangeSpanMasksOlderCandidate(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("b"), []byte("masked"))
	backend.Set([]byte("d"), []byte("visible"))
	db := openPointSuccessorTestDB(t, backend)

	if err := db.Set([]byte("z"), []byte("span carrier")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	rotatePointSuccessorMemtables(t, db)
	db.mu.Lock()
	db.queueRangeSpans[len(db.queueRangeSpans)-1] = []batch.DeleteRange{{Start: []byte("a"), End: []byte("c")}}
	db.publishMemtablesLocked()
	db.mu.Unlock()

	requireSeekGE(t, db, []byte("a"), []byte("e"), []byte("d"), []byte("visible"))
}

func TestSeekGE_DoesNotRotateOrCreateGeneralIterator(t *testing.T) {
	SetIteratorDebug(true)
	t.Cleanup(func() { SetIteratorDebug(false) })
	backend := NewMockBackend()
	db := openPointSuccessorTestDB(t, backend)
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	before := db.Stats()
	requireSeekGE(t, db, []byte("k"), []byte("l"), []byte("k"), []byte("v"))
	after := db.Stats()
	for _, name := range []string{
		"treedb.cache.iterator.calls_total",
		"treedb.cache.iterator.snapshot_rotations_total",
	} {
		if before[name] != after[name] {
			t.Fatalf("%s changed across SeekGE: %q -> %q", name, before[name], after[name])
		}
	}
	if before["treedb.cache.queue_len"] != after["treedb.cache.queue_len"] {
		t.Fatalf("queue length changed across SeekGE: %q -> %q", before["treedb.cache.queue_len"], after["treedb.cache.queue_len"])
	}
	if calls, err := strconv.ParseUint(after["treedb.cache.point_successor.calls_total"], 10, 64); err != nil || calls != 1 {
		t.Fatalf("point successor calls = %q (%v), want 1", after["treedb.cache.point_successor.calls_total"], err)
	}
	if merges := after["treedb.cache.point_successor.general_merge_iterators_total"]; merges != "0" {
		t.Fatalf("point successor general merge iterators = %q, want 0", merges)
	}
}

func TestSeekGE_ReturnsOwnedBytes(t *testing.T) {
	backend := NewMockBackend()
	db := openPointSuccessorTestDB(t, backend)
	if err := db.Set([]byte("k"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	key, value, found, err := db.SeekGE([]byte("k"), nil)
	if err != nil || !found {
		t.Fatalf("SeekGE: found=%t err=%v", found, err)
	}
	key[0], value[0] = 'x', 'X'
	requireSeekGE(t, db, []byte("k"), nil, []byte("k"), []byte("value"))
}

func TestSeekGE_CompetingVersionsAcrossMutableQueuedAndPublishedSources(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("k"), []byte("published"))
	backend.Set([]byte("z"), []byte("published-z"))
	db := openPointSuccessorTestDB(t, backend)

	for _, value := range []string{"queued-old", "queued-new"} {
		if err := db.Set([]byte("k"), []byte(value)); err != nil {
			t.Fatalf("Set %s: %v", value, err)
		}
		rotatePointSuccessorMemtables(t, db)
	}
	if err := db.Set([]byte("k"), []byte("mutable")); err != nil {
		t.Fatalf("Set mutable: %v", err)
	}
	requireSeekGE(t, db, []byte("k"), nil, []byte("k"), []byte("mutable"))

	if err := db.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete mutable: %v", err)
	}
	requireSeekGE(t, db, []byte("k"), nil, []byte("z"), []byte("published-z"))
}

func TestSeekGE_FlushPublicationPreservesAnswer(t *testing.T) {
	backend := NewMockBackend()
	db := openPointSuccessorTestDB(t, backend)
	db.flushMu.Lock()
	for _, item := range []struct{ key, value string }{{"b", "bee"}, {"d", "dee"}} {
		if err := db.Set([]byte(item.key), []byte(item.value)); err != nil {
			db.flushMu.Unlock()
			t.Fatalf("Set %s: %v", item.key, err)
		}
		rotatePointSuccessorMemtables(t, db)
	}
	requireSeekGE(t, db, []byte("a"), []byte("c"), []byte("b"), []byte("bee"))
	db.flushMu.Unlock()
	db.flushAll(false)
	requireSeekGE(t, db, []byte("a"), []byte("c"), []byte("b"), []byte("bee"))
}

func TestSeekGE_ConcurrentCommitSnapshotIsCoherent(t *testing.T) {
	backend := NewMockBackend()
	db := openPointSuccessorTestDB(t, backend)
	if err := db.Set([]byte("k"), []byte("0")); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 1; i <= 200; i++ {
			if err := db.Set([]byte("k"), []byte(strconv.Itoa(i))); err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			key, value, found, err := db.SeekGE([]byte("k"), []byte("l"))
			if err != nil || !found || !bytes.Equal(key, []byte("k")) {
				errCh <- errors.New("incoherent point-successor snapshot")
				return
			}
			if _, err := strconv.Atoi(string(value)); err != nil {
				errCh <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestSeekGE_AfterCloseFailsClosed(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		FlushThreshold:     1 << 30,
		MemtableShards:     4,
		MaxQueuedMemtables: -1,
		AllowUnsafe:        true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, found, err := db.SeekGE(nil, nil); !errors.Is(err, backenddb.ErrClosed) || found {
		t.Fatalf("SeekGE after close: found=%t err=%v, want false ErrClosed", found, err)
	}
}
