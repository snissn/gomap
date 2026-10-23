package caching

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

type countingIteratorBackend struct {
	*MockBackend
	iteratorCalls        atomic.Int64
	reverseIteratorCalls atomic.Int64
}

func (b *countingIteratorBackend) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	b.iteratorCalls.Add(1)
	return b.MockBackend.Iterator(start, end)
}

func (b *countingIteratorBackend) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	b.reverseIteratorCalls.Add(1)
	return b.MockBackend.ReverseIterator(start, end)
}

func TestIterator_EmptyForegroundLeaseBoundsDebugAndShutdown(t *testing.T) {
	previousDebug := iteratorDebugEnabled.Load()
	t.Cleanup(func() { SetIteratorDebug(previousDebug) })
	for _, reverse := range []bool{false, true} {
		for _, debug := range []bool{false, true} {
			for _, tc := range []struct {
				name                string
				start, end          []byte
				backendKeys, queued bool
			}{
				{name: "empty"},
				{name: "disjoint backend", start: []byte{}, end: []byte("a"), backendKeys: true},
				{name: "disjoint queue", start: []byte("z"), end: []byte("zz"), backendKeys: true, queued: true},
			} {
				name := tc.name + "/forward"
				if reverse {
					name = tc.name + "/reverse"
				}
				if debug {
					name += "/debug"
				}
				t.Run(name, func(t *testing.T) {
					SetIteratorDebug(debug)
					backend := &countingIteratorBackend{MockBackend: NewMockBackend()}
					if tc.backendKeys {
						backend.Set([]byte("k"), []byte("v"))
					}
					db, err := Open(t.TempDir(), backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableShards: 1})
					if err != nil {
						t.Fatal(err)
					}
					dbClosed := false
					defer func() {
						if !dbClosed {
							_ = db.Close()
						}
					}()
					if tc.queued {
						if err := db.Set([]byte("b"), []byte("cached")); err != nil {
							t.Fatal(err)
						}
						db.mu.Lock()
						err = db.rotateMemtableLocked(false)
						db.mu.Unlock()
						if err != nil {
							t.Fatal(err)
						}
					}
					if err := db.ensureBackendRange(); err != nil {
						t.Fatal(err)
					}
					view := db.memtables.Load()
					if view == nil || (tc.queued && len(view.queue) != 1) {
						t.Fatal("fixture did not publish the expected memtable view")
					}
					refs := view.refs.Load()
					retains := db.memtableViewTelemetry.retainTotal.Load()
					backendCalls := backend.iteratorCalls.Load() + backend.reverseIteratorCalls.Load()
					iteratorCalls := db.iteratorCallsTotal.Load()
					open := db.Iterator
					if reverse {
						open = db.ReverseIterator
					}
					it, err := open(tc.start, tc.end)
					if err != nil {
						t.Fatal(err)
					}
					defer it.Close()
					if it.Valid() || it.Error() != nil {
						t.Fatalf("empty iterator valid=%t error=%v", it.Valid(), it.Error())
					}
					it.Seek([]byte("seek"))
					start, end := it.Domain()
					if !bytes.Equal(start, tc.start) || !bytes.Equal(end, tc.end) || (start == nil) != (tc.start == nil) || (end == nil) != (tc.end == nil) {
						t.Fatalf("domain=(%v,%v) want=(%v,%v)", start, end, tc.start, tc.end)
					}
					tracked, ok := it.(*foregroundTrackedIterator)
					if !ok || tracked.db != db {
						t.Fatalf("missing foreground lease: %T", it)
					}
					if debug {
						decorated, ok := tracked.Iterator.(*debugIterator)
						if !ok {
							t.Fatalf("missing debug decoration: %T", tracked.Iterator)
						}
						queueLen, sources := decorated.DebugStats()
						wantQueue := 0
						if tc.queued {
							wantQueue = 1
						}
						if queueLen != wantQueue || sources != 0 {
							t.Fatalf("debug shape=(%d,%d) want=(%d,0)", queueLen, sources, wantQueue)
						}
					}
					wantCalls := iteratorCalls
					if debug && !reverse {
						wantCalls++
					}
					if db.iteratorCallsTotal.Load() != wantCalls || backend.iteratorCalls.Load()+backend.reverseIteratorCalls.Load() != backendCalls {
						t.Fatal("empty route changed debug calls or opened a backend iterator")
					}
					if view.refs.Load() != refs || db.activeForegroundIterators.Load() != 1 {
						t.Fatal("empty route retained a memtable view or released the foreground lease")
					}
					wantRetains := retains
					if tc.queued {
						// Selecting a queued root still takes the normal lease, even when
						// range filtering subsequently excludes every memtable source.
						wantRetains++
					}
					if got := db.memtableViewTelemetry.retainTotal.Load(); got != wantRetains {
						t.Fatalf("view retains=%d want=%d", got, wantRetains)
					}
					dbClosed = true
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					if db.activeForegroundIterators.Load() != 1 {
						t.Fatal("shutdown ended an unclosed iterator's foreground lease")
					}
					if err := it.Close(); err != nil {
						t.Fatal(err)
					}
					if err := it.Close(); err != nil || db.activeForegroundIterators.Load() != 0 {
						t.Fatal("repeated Close did not end exactly one foreground lease")
					}
				})
			}
		}
	}
}

func TestIterator_EmptyForegroundCloseGraceOnce(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableShards: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	db.foregroundMaintenanceGraceState.Store(3)
	db.activeForegroundIterators.publishGrace(3)
	db.foregroundMaintenanceGraceDeadlineUnixNano.Store(time.Now().Add(-time.Second).UnixNano())
	before := db.foregroundMaintenanceGraceActivity.Load()
	var closes sync.WaitGroup
	for i := 0; i < 8; i++ {
		closes.Add(1)
		go func() {
			defer closes.Done()
			if err := it.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	closes.Wait()
	if db.activeForegroundIterators.Load() != 0 || db.foregroundMaintenanceGraceActivity.Load() != before+1 {
		t.Fatal("concurrent Close ended the foreground grace more than once")
	}
}

func TestIterator_BackendOnlyFastPathUsesBackendIterator(t *testing.T) {
	backend := &countingIteratorBackend{MockBackend: NewMockBackend()}
	backend.Set([]byte("k1"), []byte("v1"))
	backend.Set([]byte("k2"), []byte("v2"))

	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.mu.Lock()
	db.backendRangeKnown = true
	db.backendRange = keyRange{valid: true, min: []byte("k1"), max: []byte("k2")}
	db.mu.Unlock()
	db.backendRangeInit.Do(func() {})

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()

	got := 0
	for it.Valid() {
		got++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if got != 2 {
		t.Fatalf("keys=%d want=2", got)
	}
	if calls := backend.iteratorCalls.Load(); calls != 1 {
		t.Fatalf("backend iterator calls=%d want=1", calls)
	}
}

func TestIterator_BackendOnlyFastPathSkipsBackendOnKnownDisjointRange(t *testing.T) {
	backend := &countingIteratorBackend{MockBackend: NewMockBackend()}
	backend.Set([]byte("k1"), []byte("v1"))
	backend.Set([]byte("k2"), []byte("v2"))

	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.mu.Lock()
	db.backendRangeKnown = true
	db.backendRange = keyRange{valid: true, min: []byte("k1"), max: []byte("k2")}
	db.mu.Unlock()
	db.backendRangeInit.Do(func() {})

	it, err := db.Iterator([]byte("z"), []byte("zz"))
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()

	if it.Valid() {
		t.Fatalf("expected empty iterator for disjoint range")
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if calls := backend.iteratorCalls.Load(); calls != 0 {
		t.Fatalf("backend iterator calls=%d want=0", calls)
	}
}

func TestIterator_BackendOnlyFastPathAvoidsViewLease(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, mode := range []string{"current", "published root", "nil view", "stale view"} {
			name := mode + "/forward"
			if reverse {
				name = mode + "/reverse"
			}
			t.Run(name, func(t *testing.T) {
				backend := &countingIteratorBackend{MockBackend: NewMockBackend()}
				backend.Set([]byte("k"), []byte("backend"))
				db, err := Open(t.TempDir(), backend, Options{
					DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableShards: 1,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if err := db.ensureBackendRange(); err != nil {
					t.Fatal(err)
				}

				// Exercise the published-root authority independently of the generic
				// backend, and preserve the existing nil/stale-view fallback behavior.
				wantValue := "backend"
				published := newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "published"})
				db.mu.Lock()
				original := db.memtables.Load()
				switch mode {
				case "published root":
					view := &memtableView{rootIterator: rootDomainSnapshot{
						published: published,
					}}
					view.refs.Store(1)
					db.memtables.Store(view)
					wantValue = "published"
				case "nil view":
					db.memtables.Store(nil)
				case "stale view":
					view := &memtableView{}
					view.refs.Store(1)
					db.memtables.Store(view)
				}
				db.mu.Unlock()
				defer func() {
					db.mu.Lock()
					db.memtables.Store(original)
					db.mu.Unlock()
				}()

				retains := db.memtableViewTelemetry.retainTotal.Load()
				releases := db.memtableViewTelemetry.releaseTotal.Load()
				open := db.Iterator
				if reverse {
					open = db.ReverseIterator
				}
				it, err := open(nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer it.Close()
				if !it.Valid() {
					t.Fatalf("iterator invalid: %v", it.Error())
				}
				if string(it.Key()) != "k" || string(it.Value()) != wantValue {
					t.Fatalf("iterator key=%q value=%q want k/%s", it.Key(), it.Value(), wantValue)
				}
				it.Next()
				if it.Valid() || it.Error() != nil {
					t.Fatalf("iterator end valid=%t error=%v", it.Valid(), it.Error())
				}
				if err := it.Close(); err != nil {
					t.Fatal(err)
				}
				if db.memtableViewTelemetry.retainTotal.Load() != retains || db.memtableViewTelemetry.releaseTotal.Load() != releases {
					t.Fatal("backend-only iterator took a transient memtable view lease")
				}
				if db.activeForegroundIterators.Load() != 0 {
					t.Fatal("Close did not release the foreground iterator lease")
				}
			})
		}
	}
}
