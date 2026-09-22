package caching

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

type countingLogWriter struct {
	appendCalls int
	batchCalls  int
	flushCalls  int
	syncCalls   int
}

func (w *countingLogWriter) Append(record commitlog.Record) error {
	w.appendCalls++
	return nil
}

func (w *countingLogWriter) AppendBatch(records []commitlog.Record) error {
	w.batchCalls++
	return errors.New("append batch not expected")
}

func (w *countingLogWriter) RotateTo(_ string) error { return nil }
func (w *countingLogWriter) RotateToWithSync(_ string, _ bool) error {
	return nil
}
func (w *countingLogWriter) Size() int64 { return 0 }
func (w *countingLogWriter) Flush() error {
	w.flushCalls++
	return nil
}
func (w *countingLogWriter) Sync() error {
	w.syncCalls++
	return nil
}
func (w *countingLogWriter) Close() error { return nil }

type errMemtable struct {
	memtable.Table
	deleteErr error
}

func (m *errMemtable) DeleteWithCallback(_ []byte, _ func(k, v []byte) error) error {
	return m.deleteErr
}

func collectIteratorKeysForDeleteRangeTest(t *testing.T, it interface {
	Valid() bool
	Next()
	Key() []byte
	Error() error
	Close() error
}) []string {
	t.Helper()
	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		t.Fatalf("iterator error: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}
	return keys
}

func TestCachingDB_CommandWALRangeSpanVisibilitySnapshotsAndCheckpoint(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	defer func() { _ = backend.Close() }()
	db, err := Open(t.TempDir(), backend, Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"z", "vz"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set(%s): %v", kv.k, err)
		}
	}
	snapBefore := db.AcquireSnapshot()
	if snapBefore == nil {
		t.Fatalf("AcquireSnapshot before range returned nil")
	}
	defer func() { _ = snapBefore.Close() }()

	appendCalls := 0
	if err := db.DeleteRangeAfterCommandWALAppend([]byte("a"), []byte("d"), func() error {
		appendCalls++
		return nil
	}); err != nil {
		t.Fatalf("DeleteRangeAfterCommandWALAppend: %v", err)
	}
	if appendCalls != 1 {
		t.Fatalf("append calls=%d want 1", appendCalls)
	}

	if val, err := snapBefore.Get([]byte("b")); err != nil || string(val) != "vb" {
		t.Fatalf("snapshot before range b=(%q,%v), want vb,nil", val, err)
	}
	if val, err := db.Get([]byte("b")); err != nil || val != nil {
		t.Fatalf("live b after range=(%q,%v), want missing", val, err)
	}
	if ok, err := db.Has([]byte("b")); err != nil || ok {
		t.Fatalf("live Has(b)=(%t,%v), want false,nil", ok, err)
	}
	if val, err := db.Get([]byte("z")); err != nil || string(val) != "vz" {
		t.Fatalf("live z after range=(%q,%v), want vz,nil", val, err)
	}
	if got, err := db.HasMany([][]byte{[]byte("a"), []byte("b"), []byte("z")}); err != nil || len(got) != 3 || got[0] || got[1] || !got[2] {
		t.Fatalf("HasMany after range=(%v,%v), want [false false true],nil", got, err)
	}
	if got, err := db.HasPrefixes([][]byte{[]byte("a"), []byte("z")}); err != nil || len(got) != 2 || got[0] || !got[1] {
		t.Fatalf("HasPrefixes after range=(%v,%v), want [false true],nil", got, err)
	}
	statsAfterRange := db.Stats()
	if got := deleteRangeStatUint64(t, statsAfterRange, "treedb.cache.range_span.active_layers"); got != 1 {
		t.Fatalf("active range layers=%d want 1", got)
	}
	if got := deleteRangeStatUint64(t, statsAfterRange, "treedb.cache.range_span.active_spans"); got != 1 {
		t.Fatalf("active range spans=%d want 1", got)
	}

	if err := db.Set([]byte("b"), []byte("new")); err != nil {
		t.Fatalf("Set(b after range): %v", err)
	}
	if val, err := db.Get([]byte("b")); err != nil || string(val) != "new" {
		t.Fatalf("live b after override=(%q,%v), want new,nil", val, err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	if got, want := collectIteratorKeysForDeleteRangeTest(t, it), []string{"b", "z"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("forward keys=%v want %v", got, want)
	}
	rit, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("ReverseIterator: %v", err)
	}
	if got, want := collectIteratorKeysForDeleteRangeTest(t, rit), []string{"z", "b"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("reverse keys=%v want %v", got, want)
	}

	snapAfter := db.AcquireSnapshot()
	if snapAfter == nil {
		t.Fatalf("AcquireSnapshot after range returned nil")
	}
	defer func() { _ = snapAfter.Close() }()
	if _, err := snapAfter.Get([]byte("a")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("snapshot after range Get(a) err=%v want ErrKeyNotFound", err)
	}
	if val, err := snapAfter.Get([]byte("b")); err != nil || string(val) != "new" {
		t.Fatalf("snapshot after range b=(%q,%v), want new,nil", val, err)
	}
	if out, err := snapAfter.GetAppend([]byte("b"), nil); err != nil || string(out) != "new" {
		t.Fatalf("snapshot after range GetAppend(b)=(%q,%v), want new,nil", out, err)
	}
	if _, err := snapAfter.GetAppend([]byte("a"), nil); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("snapshot after range GetAppend(a) err=%v, want ErrKeyNotFound", err)
	}

	stats := db.Stats()
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.materialized_keys_total"); got != 0 {
		t.Fatalf("materialized_keys_total=%d want 0", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.iterators_total"); got != 0 {
		t.Fatalf("delete_range iterators_total=%d want 0", got)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stats = db.Stats()
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.range_span.active_layers"); got != 0 {
		t.Fatalf("active range layers after checkpoint=%d want 0", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.range_span.spans_flushed_total"); got != 1 {
		t.Fatalf("spans_flushed_total=%d want 1", got)
	}
	if val, err := db.Get([]byte("a")); err != nil || val != nil {
		t.Fatalf("a after checkpoint=(%q,%v), want missing", val, err)
	}
	if val, err := db.Get([]byte("b")); err != nil || string(val) != "new" {
		t.Fatalf("b after checkpoint=(%q,%v), want new,nil", val, err)
	}
}

func TestCachingDB_CommandWALRangeSpanFiltersPublishedRoot(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	defer func() { _ = backend.Close() }()
	db, err := Open(t.TempDir(), backend, Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	published := newRootDomainTestTable(t,
		rootDomainTestOp{key: "b", value: "root-b"},
		rootDomainTestOp{key: "c", value: "root-c"},
		rootDomainTestOp{key: "z", value: "root-z"},
	)
	db.mu.Lock()
	if ok := db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{{
			lookup: published,
			rootID: 1,
		}},
	}); !ok {
		db.mu.Unlock()
		t.Fatalf("installPublishedRootSetLocked returned false")
	}
	db.mu.Unlock()

	if err := db.DeleteRangeAfterCommandWALAppend([]byte("b"), []byte("d"), func() error { return nil }); err != nil {
		t.Fatalf("DeleteRangeAfterCommandWALAppend: %v", err)
	}

	if val, err := db.Get([]byte("z")); err != nil || string(val) != "root-z" {
		t.Fatalf("active span outside-root z=(%q,%v), want root-z,nil", val, err)
	}
	if out, err := db.GetAppend([]byte("z"), []byte("prefix:")); err != nil || string(out) != "prefix:root-z" {
		t.Fatalf("active span outside-root GetAppend(z)=(%q,%v), want prefix:root-z,nil", out, err)
	}
	if val, err := db.Get([]byte("c")); err != nil || val != nil {
		t.Fatalf("active span inside-root c=(%q,%v), want missing", val, err)
	}
	if _, err := db.GetAppend([]byte("c"), nil); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("active span inside-root GetAppend(c) err=%v want ErrKeyNotFound", err)
	}
	if ok, err := db.Has([]byte("c")); err != nil || ok {
		t.Fatalf("active span Has(c)=(%t,%v), want false,nil", ok, err)
	}
	if err := db.Set([]byte("b"), []byte("new-b")); err != nil {
		t.Fatalf("Set override: %v", err)
	}
	if val, err := db.Get([]byte("b")); err != nil || string(val) != "new-b" {
		t.Fatalf("active span newer override b=(%q,%v), want new-b,nil", val, err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	if val, err := snap.Get([]byte("z")); err != nil || string(val) != "root-z" {
		t.Fatalf("snapshot active span outside-root z=(%q,%v), want root-z,nil", val, err)
	}
	if out, err := snap.GetAppend([]byte("z"), []byte("prefix:")); err != nil || string(out) != "prefix:root-z" {
		t.Fatalf("snapshot active span outside-root GetAppend(z)=(%q,%v), want prefix:root-z,nil", out, err)
	}
	if _, err := snap.Get([]byte("c")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("snapshot active span inside-root c err=%v want ErrKeyNotFound", err)
	}
	if val, err := snap.Get([]byte("b")); err != nil || string(val) != "new-b" {
		t.Fatalf("snapshot active span newer override b=(%q,%v), want new-b,nil", val, err)
	}
}

func TestCachingDB_CommandWALRangeSpanPreservesPublishedRootMiss(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	defer func() { _ = backend.Close() }()
	if err := backend.SetSync([]byte("root-a-only"), []byte("root-a")); err != nil {
		t.Fatalf("seed root A: %v", err)
	}
	rootA := uint64(0)
	if state := backend.State(); state != nil {
		rootA = state.RootPageID
	}
	if rootA == 0 {
		t.Fatalf("root A id is zero")
	}
	if err := backend.SetSync([]byte("m"), []byte("default-m")); err != nil {
		t.Fatalf("seed default root: %v", err)
	}

	db, err := Open(t.TempDir(), backend, Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.mu.Lock()
	if ok := db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{{
			rootID: rootA,
		}},
	}); !ok {
		db.mu.Unlock()
		t.Fatalf("installPublishedRootSetLocked returned false")
	}
	db.mu.Unlock()
	if err := db.DeleteRangeAfterCommandWALAppend([]byte("x"), []byte("z"), func() error { return nil }); err != nil {
		t.Fatalf("DeleteRangeAfterCommandWALAppend: %v", err)
	}

	if val, err := db.Get([]byte("root-a-only")); err != nil || string(val) != "root-a" {
		t.Fatalf("live root-id active span Get(root-a-only)=(%q,%v), want root-a,nil", val, err)
	}
	if out, err := db.GetAppend([]byte("root-a-only"), []byte("prefix:")); err != nil || string(out) != "prefix:root-a" {
		t.Fatalf("live root-id active span GetAppend(root-a-only)=(%q,%v), want prefix:root-a,nil", out, err)
	}
	if ok, err := db.Has([]byte("root-a-only")); err != nil || !ok {
		t.Fatalf("live root-id active span Has(root-a-only)=(%t,%v), want true,nil", ok, err)
	}
	if val, err := db.Get([]byte("m")); err != nil || val != nil {
		t.Fatalf("live root-bound miss Get(m)=(%q,%v), want missing", val, err)
	}
	if out, err := db.GetAppend([]byte("m"), []byte("prefix:")); !errors.Is(err, tree.ErrKeyNotFound) || string(out) != "prefix:" {
		t.Fatalf("live root-bound miss GetAppend(m)=(%q,%v), want prefix:,ErrKeyNotFound", out, err)
	}
	if ok, err := db.Has([]byte("m")); err != nil || ok {
		t.Fatalf("live root-bound miss Has(m)=(%t,%v), want false,nil", ok, err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	if _, err := snap.Get([]byte("m")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("snapshot root-bound miss Get(m) err=%v want ErrKeyNotFound", err)
	}
	if out, err := snap.GetAppend([]byte("m"), []byte("prefix:")); !errors.Is(err, tree.ErrKeyNotFound) || string(out) != "prefix:" {
		t.Fatalf("snapshot root-bound miss GetAppend(m)=(%q,%v), want prefix:,ErrKeyNotFound", out, err)
	}
}

func TestCachingDB_CommandWALRangeSpanIteratorPreservesPublishedRoot(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	defer func() { _ = backend.Close() }()
	if err := backend.SetSync([]byte("root-a-only"), []byte("root-a")); err != nil {
		t.Fatalf("seed root A: %v", err)
	}
	rootA := uint64(0)
	if state := backend.State(); state != nil {
		rootA = state.RootPageID
	}
	if rootA == 0 {
		t.Fatalf("root A id is zero")
	}
	if err := backend.SetSync([]byte("m"), []byte("default-m")); err != nil {
		t.Fatalf("seed default root: %v", err)
	}

	db, err := Open(t.TempDir(), backend, Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.mu.Lock()
	if ok := db.installPublishedRootSetLocked(&publishedRootSet{
		generation: 1,
		iterator: publishedRootRef{
			rootID: rootA,
		},
	}); !ok {
		db.mu.Unlock()
		t.Fatalf("installPublishedRootSetLocked returned false")
	}
	db.mu.Unlock()
	if err := db.DeleteRangeAfterCommandWALAppend([]byte("x"), []byte("z"), func() error { return nil }); err != nil {
		t.Fatalf("DeleteRangeAfterCommandWALAppend: %v", err)
	}

	liveIt, err := db.Iterator([]byte("m"), []byte("n"))
	if err != nil {
		t.Fatalf("live Iterator: %v", err)
	}
	if got := collectIteratorKeysForDeleteRangeTest(t, liveIt); len(got) != 0 {
		t.Fatalf("live root-bound iterator keys=%v want empty", got)
	}
	liveRIt, err := db.ReverseIterator([]byte("m"), []byte("n"))
	if err != nil {
		t.Fatalf("live ReverseIterator: %v", err)
	}
	if got := collectIteratorKeysForDeleteRangeTest(t, liveRIt); len(got) != 0 {
		t.Fatalf("live root-bound reverse iterator keys=%v want empty", got)
	}
	if got, err := db.HasPrefixes([][]byte{[]byte("m")}); err != nil || len(got) != 1 || got[0] {
		t.Fatalf("live HasPrefixes(m)=(%v,%v), want [false],nil", got, err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	snapIt, err := snap.Iterator([]byte("m"), []byte("n"))
	if err != nil {
		t.Fatalf("snapshot Iterator: %v", err)
	}
	if got := collectIteratorKeysForDeleteRangeTest(t, snapIt); len(got) != 0 {
		t.Fatalf("snapshot root-bound iterator keys=%v want empty", got)
	}
	snapRIt, err := snap.ReverseIterator([]byte("m"), []byte("n"))
	if err != nil {
		t.Fatalf("snapshot ReverseIterator: %v", err)
	}
	if got := collectIteratorKeysForDeleteRangeTest(t, snapRIt); len(got) != 0 {
		t.Fatalf("snapshot root-bound reverse iterator keys=%v want empty", got)
	}
	if got, err := snap.HasPrefixes([][]byte{[]byte("m")}); err != nil || len(got) != 1 || got[0] {
		t.Fatalf("snapshot HasPrefixes(m)=(%v,%v), want [false],nil", got, err)
	}
}

func TestCachingDB_CommandWALRangeSpanSurvivesDisjointOrdinaryDeleteRange(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("b"), []byte("old"))
	db, err := Open(t.TempDir(), backend, Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.DeleteRangeAfterCommandWALAppend([]byte("a"), []byte("c"), func() error { return nil }); err != nil {
		t.Fatalf("DeleteRangeAfterCommandWALAppend: %v", err)
	}
	if val, err := db.Get([]byte("b")); err != nil || val != nil {
		t.Fatalf("b after active span=(%q,%v), want missing", val, err)
	}

	if err := db.DeleteRange([]byte("x"), []byte("z")); err != nil {
		t.Fatalf("ordinary DeleteRange: %v", err)
	}
	if val, err := db.Get([]byte("b")); err != nil || val != nil {
		t.Fatalf("b after disjoint ordinary DeleteRange=(%q,%v), want missing", val, err)
	}
	stats := db.Stats()
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.range_span.active_spans"); got != 1 {
		t.Fatalf("active range spans after ordinary DeleteRange=%d want 1", got)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if val, err := db.Get([]byte("b")); err != nil || val != nil {
		t.Fatalf("b after checkpoint=(%q,%v), want missing", val, err)
	}
}

func TestCachingDB_CommandWALRangeSpanBlocksLaterWriteSyncBypass(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appendCalls := 0
	if err := db.DeleteRangeAfterCommandWALAppend([]byte("a"), []byte("c"), func() error {
		appendCalls++
		return nil
	}); err != nil {
		t.Fatalf("DeleteRangeAfterCommandWALAppend: %v", err)
	}
	if appendCalls != 1 {
		t.Fatalf("append calls=%d want 1", appendCalls)
	}

	b := db.NewBatch()
	if err := b.Set([]byte("b"), []byte("new")); err != nil {
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}

	if val, err := db.Get([]byte("b")); err != nil || string(val) != "new" {
		t.Fatalf("newer point after active span b=(%q,%v), want new,nil", val, err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if val, err := db.Get([]byte("b")); err != nil || string(val) != "new" {
		t.Fatalf("newer point after span checkpoint b=(%q,%v), want new,nil", val, err)
	}
}

func TestCachingBatch_CommandWALRangeOnlyUsesSpanLayer(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, key := range []string{"a", "b", "c", "z"} {
		if err := db.Set([]byte(key), []byte("v")); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	b := db.NewBatch()
	for _, bounds := range []struct{ start, end string }{{"a", "c"}, {"c", "d"}, {"b", "d"}} {
		if err := b.DeleteRange([]byte(bounds.start), []byte(bounds.end)); err != nil {
			t.Fatalf("DeleteRange(%s,%s): %v", bounds.start, bounds.end, err)
		}
	}
	appendCalls := 0
	if err := b.WriteAfterCommandWALAppend(false, func() error {
		appendCalls++
		return nil
	}); err != nil {
		t.Fatalf("WriteAfterCommandWALAppend: %v", err)
	}
	_ = b.Close()
	if appendCalls != 1 {
		t.Fatalf("append calls=%d want 1", appendCalls)
	}
	if val, err := db.Get([]byte("b")); err != nil || val != nil {
		t.Fatalf("b after batch range=(%q,%v), want missing", val, err)
	}
	stats := db.Stats()
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.batch_calls_total"); got != 3 {
		t.Fatalf("batch_calls_total=%d want 3", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.batch_writes_total"); got != 1 {
		t.Fatalf("batch_writes_total=%d want 1", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.input_ranges_total"); got != 3 {
		t.Fatalf("input_ranges_total=%d want 3", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.effective_ranges_total"); got != 1 {
		t.Fatalf("effective_ranges_total=%d want 1", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.coalesced_ranges_total"); got != 2 {
		t.Fatalf("coalesced_ranges_total=%d want 2", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.materialized_keys_total"); got != 0 {
		t.Fatalf("materialized_keys_total=%d want 0", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.range_span.layers_total"); got != 1 {
		t.Fatalf("range span layers_total=%d want 1", got)
	}
}

func rangeSpanTestKeyForLane(t *testing.T, db *DB, wantLane int) []byte {
	t.Helper()
	for i := byte(1); i < 250; i++ {
		key := []byte{i}
		if gotLane := db.laneForShardIndex(db.shardIndex(key)); gotLane == wantLane {
			return key
		}
	}
	t.Fatalf("no test key found for lane %d", wantLane)
	return nil
}

func rangeSpanTestEnd(key []byte) []byte {
	if len(key) == 0 || key[0] == 0xff {
		return nil
	}
	return []byte{key[0] + 1}
}

func TestCachingDB_CommandWALRangeSpanFlushCollectionRespectsLaneBarriers(t *testing.T) {
	newDB := func(t *testing.T) *DB {
		t.Helper()
		db, err := Open(t.TempDir(), NewMockBackend(), Options{
			ExternalCommandWAL: true,
			JournalLanes:       2,
			MemtableShards:     2,
			FlushThreshold:     1 << 20,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}

	t.Run("span waits for older point from another lane", func(t *testing.T) {
		db := newDB(t)
		defer func() { _ = db.Close() }()
		key := rangeSpanTestKeyForLane(t, db, 1)
		if err := db.Set(key, []byte("old")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := db.DeleteRangeAfterCommandWALAppend(key, rangeSpanTestEnd(key), func() error { return nil }); err != nil {
			t.Fatalf("DeleteRangeAfterCommandWALAppend: %v", err)
		}

		db.mu.Lock()
		if len(db.queue) < 2 {
			db.mu.Unlock()
			t.Fatalf("queue len=%d want point+span", len(db.queue))
		}
		spanUnits, _, _, _ := db.collectFlushUnitsLocked(0, flushCombineMaxMemtables, flushCombineTargetBytes)
		if len(spanUnits) != 0 {
			db.mu.Unlock()
			t.Fatalf("lane 0 collected %d units before older lane-1 point flushed; want barrier", len(spanUnits))
		}
		pointUnits, _, _, _ := db.collectFlushUnitsLocked(1, flushCombineMaxMemtables, flushCombineTargetBytes)
		if len(pointUnits) == 0 || len(pointUnits[0].spans) != 0 {
			db.mu.Unlock()
			t.Fatalf("lane 1 units=%+v, want older point unit before span", pointUnits)
		}
		db.mu.Unlock()

		if err := db.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		if got := deleteRangeStatUint64(t, db.Stats(), "treedb.cache.range_span.active_layers"); got != 0 {
			t.Fatalf("active range layers after checkpoint=%d want 0", got)
		}
		if val, err := db.Get(key); err != nil || val != nil {
			t.Fatalf("Get after checkpoint=(%q,%v), want missing", val, err)
		}
	})

	t.Run("newer point waits behind older span from another lane", func(t *testing.T) {
		db := newDB(t)
		defer func() { _ = db.Close() }()
		key := rangeSpanTestKeyForLane(t, db, 1)
		if err := db.DeleteRangeAfterCommandWALAppend(key, rangeSpanTestEnd(key), func() error { return nil }); err != nil {
			t.Fatalf("DeleteRangeAfterCommandWALAppend: %v", err)
		}
		if err := db.Set(key, []byte("new")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		db.mu.Lock()
		if err := db.rotateMutableShardsLocked(minMemtablePrealloc, false); err != nil {
			db.mu.Unlock()
			t.Fatalf("rotateMutableShardsLocked: %v", err)
		}
		defer db.mu.Unlock()
		if len(db.queue) < 2 {
			t.Fatalf("queue len=%d want span+point", len(db.queue))
		}
		pointUnits, _, _, _ := db.collectFlushUnitsLocked(1, flushCombineMaxMemtables, flushCombineTargetBytes)
		if len(pointUnits) != 0 {
			t.Fatalf("lane 1 collected %d point units before older span flushed; want barrier", len(pointUnits))
		}
		spanUnits, _, _, _ := db.collectFlushUnitsLocked(0, flushCombineMaxMemtables, flushCombineTargetBytes)
		if len(spanUnits) == 0 || len(spanUnits[0].spans) == 0 {
			t.Fatalf("lane 0 units=%+v, want span barrier at queue head", spanUnits)
		}
	})
}

func TestCachingDB_CommandWALRangeSpanCheckpointCombinesFlushUnits(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const ranges = 40
	for i := 0; i < ranges; i++ {
		start := []byte{byte(i + 1)}
		end := []byte{byte(i + 2)}
		if err := db.DeleteRangeAfterCommandWALAppend(start, end, func() error { return nil }); err != nil {
			t.Fatalf("DeleteRangeAfterCommandWALAppend(%d): %v", i, err)
		}
	}
	before := db.Stats()
	if got := deleteRangeStatUint64(t, before, "treedb.cache.range_span.active_layers"); got != ranges {
		t.Fatalf("active_layers before checkpoint=%d want %d", got, ranges)
	}
	beforeBatches := deleteRangeStatUint64(t, before, "treedb.cache.stats.backend_write_batches_total")
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	after := db.Stats()
	if got := deleteRangeStatUint64(t, after, "treedb.cache.range_span.active_layers"); got != 0 {
		t.Fatalf("active_layers after checkpoint=%d want 0", got)
	}
	if got := deleteRangeStatUint64(t, after, "treedb.cache.range_span.spans_flushed_total"); got != ranges {
		t.Fatalf("spans_flushed_total=%d want %d", got, ranges)
	}
	if got := deleteRangeStatUint64(t, after, "treedb.cache.range_span.range_only_flushed_total"); got != ranges {
		t.Fatalf("range_only_flushed_total=%d want %d", got, ranges)
	}
	if got := deleteRangeStatUint64(t, after, "treedb.cache.range_span.flush_batches_total"); got != 1 {
		t.Fatalf("range_span flush_batches_total=%d want 1", got)
	}
	if got := deleteRangeStatUint64(t, after, "treedb.cache.stats.backend_write_batches_total") - beforeBatches; got != 1 {
		t.Fatalf("backend write batches for range spans=%d want 1", got)
	}
}

func TestCachingDB_CommandWALRangeSpanBounds(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, kv := range []struct{ k, v []byte }{
		{[]byte{}, []byte("empty")},
		{[]byte("a"), []byte("va")},
		{[]byte("b"), []byte("vb")},
	} {
		if err := db.Set(kv.k, kv.v); err != nil {
			t.Fatalf("Set(%q): %v", kv.k, err)
		}
	}
	appendCalls := 0
	if err := db.DeleteRangeAfterCommandWALAppend([]byte("z"), []byte("a"), func() error {
		appendCalls++
		return nil
	}); err != nil {
		t.Fatalf("reversed DeleteRange: %v", err)
	}
	if err := db.DeleteRangeAfterCommandWALAppend(nil, []byte{}, func() error {
		appendCalls++
		return nil
	}); err != nil {
		t.Fatalf("nil-to-empty DeleteRange: %v", err)
	}
	if appendCalls != 0 {
		t.Fatalf("noop append calls=%d want 0", appendCalls)
	}
	if val, err := db.Get([]byte{}); err != nil || string(val) != "empty" {
		t.Fatalf("empty key after no-ops=(%q,%v), want empty,nil", val, err)
	}
	if err := db.DeleteRangeAfterCommandWALAppend([]byte{}, []byte("a"), func() error {
		appendCalls++
		return nil
	}); err != nil {
		t.Fatalf("empty-to-a DeleteRange: %v", err)
	}
	if val, err := db.Get([]byte{}); err != nil || val != nil {
		t.Fatalf("empty key after [empty,a)=(%q,%v), want missing", val, err)
	}
	if val, err := db.Get([]byte("a")); err != nil || string(val) != "va" {
		t.Fatalf("a after [empty,a)=(%q,%v), want va,nil", val, err)
	}
	if err := db.DeleteRangeAfterCommandWALAppend(nil, nil, func() error {
		appendCalls++
		return nil
	}); err != nil {
		t.Fatalf("full DeleteRange: %v", err)
	}
	for _, key := range [][]byte{[]byte("a"), []byte("b")} {
		if val, err := db.Get(key); err != nil || val != nil {
			t.Fatalf("%q after full range=(%q,%v), want missing", key, val, err)
		}
	}
	if appendCalls != 2 {
		t.Fatalf("append calls=%d want 2 for effective ranges", appendCalls)
	}
}

func TestCachingDB_DeleteRange_WALApplyFailurePoisonsWrites(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 30,
		JournalLanes:   1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := db.Set([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	if !it.Valid() {
		_ = it.Close()
		t.Fatalf("expected iterator to see keys before delete range")
	}
	_ = it.Close()

	stub := &countingLogWriter{}
	failErr := errors.New("apply delete failed")

	db.mu.Lock()
	l := &db.lanes[0]
	l.walMu.Lock()
	origWal := l.wal
	l.wal = stub
	l.walMu.Unlock()
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		shard.mem = &errMemtable{Table: shard.mem, deleteErr: failErr}
		shard.mu.Unlock()
	}
	db.mu.Unlock()

	err = db.DeleteRange([]byte("a"), []byte("z"))
	if err == nil || !strings.Contains(err.Error(), "cannot preserve entry revision") {
		t.Fatalf("expected delete range revision-preservation error, got %v", err)
	}

	db.mu.Lock()
	l.walMu.Lock()
	l.wal = origWal
	l.walMu.Unlock()
	db.mu.Unlock()
	if stub.appendCalls == 0 {
		t.Fatalf("expected Append to be called")
	}
	if stub.batchCalls != 0 {
		t.Fatalf("unexpected AppendBatch calls: %d", stub.batchCalls)
	}

	err = db.Set([]byte("c"), []byte("3"))
	if err == nil || !strings.Contains(err.Error(), "cannot preserve entry revision") {
		t.Fatalf("expected poisoned WAL revision-preservation error on Set, got %v", err)
	}
}

func TestCachingDB_DeleteRangeAfterCommandWALAppendFailureDoesNotMutate(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := db.Set([]byte("z"), []byte("vz")); err != nil {
		t.Fatalf("Set(z): %v", err)
	}

	appendErr := errors.New("append failed")
	appendCalls := 0
	if err := db.DeleteRangeAfterCommandWALAppend([]byte("a"), []byte("z"), func() error {
		appendCalls++
		return appendErr
	}); !errors.Is(err, appendErr) {
		t.Fatalf("DeleteRangeAfterCommandWALAppend error=%v, want appendErr", err)
	}
	if appendCalls != 1 {
		t.Fatalf("append calls=%d, want 1", appendCalls)
	}
	got, err := db.Get([]byte("a"))
	if err != nil || string(got) != "va" {
		t.Fatalf("Get(a)=(%q,%v), want va,nil after append failure", got, err)
	}
	got, err = db.Get([]byte("z"))
	if err != nil || string(got) != "vz" {
		t.Fatalf("Get(z)=(%q,%v), want vz,nil after append failure", got, err)
	}
}

func TestCachingDB_DeleteRangeAfterCommandWALAppendNoopDoesNotAppend(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appendCalls := 0
	if err := db.DeleteRangeAfterCommandWALAppend([]byte("z"), []byte("a"), func() error {
		appendCalls++
		return nil
	}); err != nil {
		t.Fatalf("reversed no-op DeleteRangeAfterCommandWALAppend: %v", err)
	}
	if err := db.DeleteRangeAfterCommandWALAppend(nil, []byte{}, func() error {
		appendCalls++
		return nil
	}); err != nil {
		t.Fatalf("unbounded-to-empty no-op DeleteRangeAfterCommandWALAppend: %v", err)
	}
	if appendCalls != 0 {
		t.Fatalf("append calls=%d, want 0 for no-op ranges", appendCalls)
	}
}

func TestCachingDB_DeleteRange_WALFlushesAfterDeletes(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 30,
		JournalLanes:   1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := db.Set([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}
	if err := db.Set([]byte("z"), []byte("9")); err != nil {
		t.Fatalf("Set(z): %v", err)
	}

	stub := &countingLogWriter{}
	db.mu.Lock()
	l := &db.lanes[0]
	l.walMu.Lock()
	origWal := l.wal
	l.wal = stub
	l.walMu.Unlock()
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		l.walMu.Lock()
		l.wal = origWal
		l.walMu.Unlock()
		db.mu.Unlock()
	}()

	if err := db.DeleteRange([]byte("a"), []byte("c")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	if stub.appendCalls == 0 {
		t.Fatalf("expected WAL Append calls for range deletes")
	}
	if stub.flushCalls != 1 {
		t.Fatalf("expected single WAL Flush after range delete, got %d", stub.flushCalls)
	}
	if stub.syncCalls != 0 {
		t.Fatalf("expected no WAL Sync for non-sync delete range, got %d", stub.syncCalls)
	}
}

func TestCachingDB_DeleteRange_WALSkipsFlushWhenNoDeletes(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 30,
		JournalLanes:   1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("m"), []byte("1")); err != nil {
		t.Fatalf("Set(m): %v", err)
	}

	stub := &countingLogWriter{}
	db.mu.Lock()
	l := &db.lanes[0]
	l.walMu.Lock()
	origWal := l.wal
	l.wal = stub
	l.walMu.Unlock()
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		l.walMu.Lock()
		l.wal = origWal
		l.walMu.Unlock()
		db.mu.Unlock()
	}()

	if err := db.DeleteRange([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	if stub.appendCalls != 0 {
		t.Fatalf("expected no WAL Append calls when range has no keys, got %d", stub.appendCalls)
	}
	if stub.flushCalls != 0 {
		t.Fatalf("expected no WAL Flush when range has no keys, got %d", stub.flushCalls)
	}
}

func TestCachingDB_DeleteRange_WALFlushesOnlySelectedLane(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 30,
		JournalLanes:   4,
		MemtableShards: 4,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for i := 0; i < 64; i++ {
		key := []byte{byte('a' + (i % 26)), byte('0' + byte(i%10)), byte('x'), byte('0' + byte((i/10)%10))}
		if err := db.Set(key, []byte("v")); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	stubs := make([]*countingLogWriter, len(db.lanes))
	db.mu.Lock()
	origWALs := make([]commitWriter, len(db.lanes))
	for i := range db.lanes {
		stub := &countingLogWriter{}
		stubs[i] = stub
		l := &db.lanes[i]
		l.walMu.Lock()
		origWALs[i] = l.wal
		l.wal = stub
		l.walMu.Unlock()
	}
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		for i := range db.lanes {
			l := &db.lanes[i]
			l.walMu.Lock()
			l.wal = origWALs[i]
			l.walMu.Unlock()
		}
		db.mu.Unlock()
	}()

	if err := db.DeleteRange([]byte("a"), []byte("z")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	totalAppends := 0
	totalFlushes := 0
	totalSyncs := 0
	activeLaneCount := 0
	for i, stub := range stubs {
		totalAppends += stub.appendCalls
		totalFlushes += stub.flushCalls
		totalSyncs += stub.syncCalls
		if stub.appendCalls > 0 || stub.flushCalls > 0 || stub.syncCalls > 0 {
			activeLaneCount++
		}
		if stub.flushCalls > 1 {
			t.Fatalf("lane %d flush calls=%d want <= 1", i, stub.flushCalls)
		}
	}
	if totalAppends == 0 {
		t.Fatalf("expected WAL appends for range deletes")
	}
	if totalFlushes != 1 {
		t.Fatalf("expected exactly one lane flush per DeleteRange call, got total flushes=%d", totalFlushes)
	}
	if activeLaneCount != 1 {
		t.Fatalf("expected DeleteRange to use exactly one WAL lane, active lanes=%d", activeLaneCount)
	}
	if totalSyncs != 0 {
		t.Fatalf("expected no WAL sync calls for non-sync DeleteRange, got %d", totalSyncs)
	}
}

func TestCachingBatchDeleteRangeSerializesScanAndPublish(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.Set([]byte("b"), []byte("old"))
	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20, JournalLanes: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	rangeBatch := db.NewBatch()
	if err := rangeBatch.DeleteRange([]byte("a"), []byte("d")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	rangeDone := make(chan error, 1)
	go func() {
		rangeDone <- rangeBatch.Write()
		_ = rangeBatch.Close()
	}()

	select {
	case <-backend.iteratorStartedCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("range batch did not start backend iterator")
	}

	writerDone := make(chan error, 1)
	go func() {
		b := db.NewBatch()
		defer b.Close()
		if err := b.Set([]byte("b"), []byte("updated")); err != nil {
			writerDone <- err
			return
		}
		if err := b.Set([]byte("c"), []byte("new")); err != nil {
			writerDone <- err
			return
		}
		writerDone <- b.Write()
	}()

	select {
	case err := <-writerDone:
		t.Fatalf("concurrent writer completed before blocked range scan published: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(backend.iteratorBlockCh)
	select {
	case err := <-rangeDone:
		if err != nil {
			t.Fatalf("range batch Write: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for range batch")
	}
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("concurrent writer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for concurrent writer")
	}

	if val, err := db.Get([]byte("b")); err != nil || string(val) != "updated" {
		t.Fatalf("b=(%q,%v), want updated", val, err)
	}
	if val, err := db.Get([]byte("c")); err != nil || string(val) != "new" {
		t.Fatalf("c=(%q,%v), want new", val, err)
	}
}

func TestCachingBatchDeleteRangeMaterializationCapFailsClosed(t *testing.T) {
	oldEntries := cachedBatchDeleteRangeMaterializeMaxEntries
	oldBytes := cachedBatchDeleteRangeMaterializeMaxKeyBytes
	cachedBatchDeleteRangeMaterializeMaxEntries = 2
	cachedBatchDeleteRangeMaterializeMaxKeyBytes = defaultCachedBatchDeleteRangeMaterializeMaxKeyBytes
	defer func() {
		cachedBatchDeleteRangeMaterializeMaxEntries = oldEntries
		cachedBatchDeleteRangeMaterializeMaxKeyBytes = oldBytes
	}()

	dir := t.TempDir()
	backend := NewMockBackend()
	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		backend.Set([]byte(kv.k), []byte(kv.v))
	}
	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20, JournalLanes: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	defer b.Close()
	if err := b.DeleteRange([]byte("a"), []byte("z")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := b.Write(); !errors.Is(err, ErrBatchDeleteRangeTooLarge) {
		t.Fatalf("Write err=%v, want ErrBatchDeleteRangeTooLarge", err)
	}
	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		val, err := db.Get([]byte(kv.k))
		if err != nil || string(val) != kv.v {
			t.Fatalf("%s=(%q,%v), want %q", kv.k, val, err, kv.v)
		}
	}
}

func TestCachingBatchDeleteRangeBackendBatchUsesSetOpsBridge(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.Set([]byte("a"), []byte("1"))
	backend.Set([]byte("b"), []byte("2"))
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	defer b.Close()
	b.backend = backend.NewBatch()
	if err := b.Set([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("backend Set: %v", err)
	}
	if err := b.DeleteRange([]byte("a"), []byte("c")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if b.hasDeleteRanges {
		t.Fatalf("backend-backed DeleteRange should not switch to cached materialization")
	}
	if len(b.entries) != 0 {
		t.Fatalf("backend-backed DeleteRange queued %d cached entries", len(b.entries))
	}

	backend.mu.RLock()
	lastOps := append([]batch.Entry(nil), backend.lastOps...)
	backend.mu.RUnlock()
	if len(lastOps) != 1 || lastOps[0].Type != batch.OpDeleteRange || string(lastOps[0].Key) != "a" || string(lastOps[0].Value) != "c" {
		t.Fatalf("last backend SetOps=%+v, want single DeleteRange [a,c)", lastOps)
	}

	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	backend.mu.RLock()
	_, hasA := backend.data["a"]
	_, hasB := backend.data["b"]
	gotC := string(backend.data["c"])
	backend.mu.RUnlock()
	if hasA || hasB || gotC != "3" {
		t.Fatalf("backend data after bridged range: hasA=%t hasB=%t c=%q, want only c=3", hasA, hasB, gotC)
	}
}

func TestCachingBatchDeleteRangeMixedOrder(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20, JournalLanes: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}

	b := db.NewBatch()
	if err := b.Set([]byte("b"), []byte("shadowed")); err != nil {
		t.Fatalf("batch Set shadowed: %v", err)
	}
	if err := b.DeleteRange([]byte("a"), []byte("d")); err != nil {
		t.Fatalf("batch DeleteRange: %v", err)
	}
	if err := b.Set([]byte("c"), []byte("after")); err != nil {
		t.Fatalf("batch Set after: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("batch Write: %v", err)
	}
	_ = b.Close()

	if val, err := db.Get([]byte("a")); err != nil || val != nil {
		t.Fatalf("a=(%q,%v), want missing", val, err)
	}
	if val, err := db.Get([]byte("b")); err != nil || val != nil {
		t.Fatalf("b=(%q,%v), want missing", val, err)
	}
	if val, err := db.Get([]byte("c")); err != nil || string(val) != "after" {
		t.Fatalf("c=(%q,%v), want after", val, err)
	}
	if val, err := db.Get([]byte("d")); err != nil || string(val) != "vd" {
		t.Fatalf("d=(%q,%v), want vd", val, err)
	}
}
