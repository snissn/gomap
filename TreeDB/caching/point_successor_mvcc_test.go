package caching

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Use an optional interface so the concurrency regression is red before the
// capability is implemented, without changing production code first.
type versionRangeSeeker interface {
	SeekGEVersionRange(start, end []byte) ([]byte, []byte, bool, error)
}

// Raw callers can recreate a physically deleted version. Shared multi-probe
// tombstone skipping would then produce a result that never won in any state.
// Store commits cannot recreate <=floor keys; nevertheless the shared seam
// restarts exclusively when it encounters a physical tombstone.
func TestSeekGE_RawPhysicalDeleteCounterexample(t *testing.T) {
	db := openPointSuccessorTestDB(t, NewMockBackend())
	a, _ := mvcckey.Encode([]byte("raw-key"), 80)
	b, _ := mvcckey.Encode([]byte("raw-key"), 40)
	upper, _ := mvcckey.AppendKeyVersionsUpper(nil, []byte("raw-key"))
	if err := db.Set(b, []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(a); err != nil {
		t.Fatal(err)
	}
	mt := db.mutableShards[db.shardIndex(a)].mem
	first := seekPointSuccessorTable(mt, a, upper, nil, 0, 0)
	if !first.found || first.flags&node.FlagTombstone == 0 {
		t.Fatal("expected first physical tombstone")
	}
	if err := db.Set(a, []byte("restored")); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(b, []byte("after")); err != nil {
		t.Fatal(err)
	}
	second := seekPointSuccessorTable(mt, pointSuccessorAfter(first.key), upper, nil, 0, 0)
	if !second.found || !bytes.Equal(second.key, b) || string(second.value) != "after" {
		t.Fatal("counterexample schedule did not exercise skipped restoration")
	}
	// Before the restoration b had "before"; afterwards a always wins.
	requireSeekGE(t, db, a, upper, a, []byte("restored"))
}

type successorPartialPublishBackend struct {
	BackendDB
	firstPublished chan struct{}
	finish         chan struct{}
	once           sync.Once
}

func (b *successorPartialPublishBackend) NewBatch() batch.Interface {
	return &successorPartialPublishBatch{Interface: b.BackendDB.NewBatch(), backend: b}
}

type successorPartialPublishBatch struct {
	batch.Interface
	backend *successorPartialPublishBackend
}

func (b *successorPartialPublishBatch) Write() error {
	if err := b.Interface.Write(); err != nil {
		return err
	}
	b.backend.once.Do(func() { close(b.backend.firstPublished); <-b.backend.finish })
	return nil
}

// The first mutable probe happens before AcquireSnapshot. Publish versions
// committed after that probe through real sorted, one-record flush chunks.
// A lower timestamp must never become visible before its newer predecessor.
func TestSeekGEVersionRange_RotationPartialPublication(t *testing.T) {
	db, barrier, lower, upper := openSuccessorAcquireBarrier(t)
	seeker, ok := any(db).(versionRangeSeeker)
	if !ok {
		t.Fatal("qualified MVCC successor capability missing")
	}
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	partial := &successorPartialPublishBackend{BackendDB: barrier.BackendDB, firstPublished: make(chan struct{}), finish: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-partial.finish:
		default:
			close(partial.finish)
		}
	})
	barrier.BackendDB = partial
	db.flushBackendMaxEntries = 1
	db.flushBackendMaxBatches = -1
	done := make(chan struct{})
	var key, value []byte
	var found bool
	var readErr error
	go func() { defer close(done); key, value, found, readErr = seeker.SeekGEVersionRange(lower, upper) }()
	awaitSuccessorSignal(t, barrier.entered)
	if !db.writeMu.TryRLock() {
		t.Fatal("qualified successor serializes point writes")
	}
	db.writeMu.RUnlock()
	newer, _ := mvcckey.Encode([]byte("one-key"), 80)
	older, _ := mvcckey.Encode([]byte("one-key"), 40)
	if err := db.Set(newer, []byte("newer")); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(older, []byte("older")); err != nil {
		t.Fatal(err)
	}
	rotatePointSuccessorMemtables(t, db)
	flushed := make(chan struct{})
	go func() { defer close(flushed); db.flushOneLocked(false) }()
	awaitSuccessorSignal(t, partial.firstPublished)
	close(barrier.resume)
	awaitSuccessorSignal(t, done)
	close(partial.finish)
	awaitSuccessorSignal(t, flushed)
	if readErr != nil || !found || !bytes.Equal(key, newer) || string(value) != "newer" {
		t.Fatalf("partial publication returned (%x,%q,%t,%v), want newest committed predecessor", key, value, found, readErr)
	}
}

func TestSeekGEVersionRange_SameTimestampRetainedBytes(t *testing.T) {
	for _, mode := range []string{"skiplist", "btree", "hash_sorted", "append_only"} {
		t.Run(mode, func(t *testing.T) {
			db, barrier, lower, upper := openSuccessorAcquireBarrier(t, mode)
			seeker, ok := any(db).(versionRangeSeeker)
			if !ok {
				t.Fatal("qualified MVCC successor capability missing")
			}
			physical, _ := mvcckey.Encode([]byte("one-key"), 50)
			if err := db.Set(physical, []byte("before")); err != nil {
				t.Fatal(err)
			}
			db.flushMu.Lock()
			defer db.flushMu.Unlock()
			done := make(chan struct{})
			var value []byte
			var readErr error
			go func() { defer close(done); _, value, _, readErr = seeker.SeekGEVersionRange(lower, upper) }()
			awaitSuccessorSignal(t, barrier.entered)
			if !db.writeMu.TryRLock() {
				t.Fatal("qualified successor serializes point writes")
			}
			db.writeMu.RUnlock()
			if err := db.Set(physical, []byte("after")); err != nil {
				t.Fatal(err)
			}
			rotatePointSuccessorMemtables(t, db)
			db.flushOneLocked(false)
			close(barrier.resume)
			awaitSuccessorSignal(t, done)
			if readErr != nil || string(value) != "before" {
				t.Fatalf("retained candidate = %q,%v", value, readErr)
			}
			// The returned bytes also survive lease release and later writes/recycling.
			if err := db.Set(physical, []byte("much-longer-replacement")); err != nil {
				t.Fatal(err)
			}
			if string(value) != "before" {
				t.Fatalf("owned bytes changed: %q", value)
			}
		})
	}
}

type successorAcquireBarrier struct {
	BackendDB
	snapper backendSnapshotProvider
	blocked atomic.Bool
	entered chan struct{}
	resume  chan struct{}
}

func (b *successorAcquireBarrier) AcquireSnapshot() *backenddb.Snapshot {
	if b.blocked.CompareAndSwap(false, true) {
		close(b.entered)
		<-b.resume
	}
	return b.snapper.AcquireSnapshot()
}

type successorProbeCallback struct {
	memtable.Table
	seeker memtable.SuccessorTable
	once   sync.Once
	after  func()
}

func (mt *successorProbeCallback) SeekGE(start, end []byte) ([]byte, []byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	k, v, p, f, r, found := mt.seeker.SeekGE(start, end)
	mt.once.Do(mt.after)
	return k, v, p, f, r, found
}

func TestSeekGEVersionRange_PhysicalDeleteRetriesFreshView(t *testing.T) {
	db := openPointSuccessorTestDB(t, NewMockBackend())
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	lower, _ := mvcckey.Encode([]byte("retry-key"), 80)
	upper, _ := mvcckey.AppendKeyVersionsUpper(nil, []byte("retry-key"))
	if err := db.Delete(lower); err != nil {
		t.Fatal(err)
	}
	idx := db.shardIndex(lower)
	original := db.mutableShards[idx].mem
	wrapped := &successorProbeCallback{Table: original, seeker: original.(memtable.SuccessorTable)}
	wrapped.after = func() {
		rotatePointSuccessorMemtables(t, db)
		if err := db.Set(lower, []byte("new-view")); err != nil {
			t.Fatal(err)
		}
	}
	db.mu.Lock()
	db.mutableShards[idx].mem = wrapped
	db.rootPointStates[idx].mutable = wrapped
	db.publishMemtablesLocked()
	db.mu.Unlock()
	k, v, found, err := db.SeekGEVersionRange(lower, upper)
	if err != nil || !found || !bytes.Equal(k, lower) || string(v) != "new-view" {
		t.Fatalf("fresh retry = (%x,%q,%t,%v)", k, v, found, err)
	}
	if got := db.pointSuccessorMVCCPhysicalDeleteFallbacksTotal.Load(); got != 1 {
		t.Fatalf("physical retries = %d", got)
	}
	if got := db.pointSuccessorCallsTotal.Load(); got != 1 {
		t.Fatalf("logical calls counted retry twice: %d", got)
	}
}

func TestSeekGEVersionRange_ConcurrentReaders(t *testing.T) {
	db, barrier, lower, upper := openSuccessorAcquireBarrier(t)
	first := make(chan struct{})
	go func() { defer close(first); _, _, _, _ = db.SeekGEVersionRange(lower, upper) }()
	awaitSuccessorSignal(t, barrier.entered)
	second := make(chan struct{})
	go func() { defer close(second); _, _, _, _ = db.SeekGEVersionRange(lower, upper) }()
	awaitSuccessorSignal(t, second)
	close(barrier.resume)
	awaitSuccessorSignal(t, first)
}

func TestSeekGEVersionRange_AdmittedWriterCannotInsertIntoRotatedGeneration(t *testing.T) {
	db, barrier, lower, upper := openSuccessorAcquireBarrier(t)
	db.externalCommandWAL = true
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	idx := db.shardIndex(lower)
	old := db.mutableShards[idx].mem
	readDone := make(chan struct{})
	var gotKey, gotValue []byte
	var readErr error
	go func() { defer close(readDone); gotKey, gotValue, _, readErr = db.SeekGEVersionRange(lower, upper) }()
	awaitSuccessorSignal(t, barrier.entered)
	late, _ := mvcckey.Encode([]byte("one-key"), 40)
	newer, _ := mvcckey.Encode([]byte("one-key"), 80)
	admitted, resume := make(chan struct{}), make(chan struct{})
	writeDone := make(chan struct{})
	var writeErr error
	go func() {
		defer close(writeDone)
		writeErr = db.SetAfterCommandWALAppendWithRevision(late, []byte("late"), func(page.EntryRevision) error { close(admitted); <-resume; return nil })
	}()
	awaitSuccessorSignal(t, admitted)
	rotatePointSuccessorMemtables(t, db)
	if err := db.Set(newer, []byte("newer")); err != nil {
		t.Fatal(err)
	}
	close(resume)
	awaitSuccessorSignal(t, writeDone)
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, _, _, found := old.GetEntry(late); found {
		t.Fatal("admitted writer inserted into frozen captured generation")
	}
	rotatePointSuccessorMemtables(t, db)
	db.flushOneLocked(false)
	close(barrier.resume)
	awaitSuccessorSignal(t, readDone)
	if readErr != nil || !bytes.Equal(gotKey, newer) || string(gotValue) != "newer" {
		t.Fatalf("late-writer read = (%x,%q,%v)", gotKey, gotValue, readErr)
	}
}

func TestSeekGEVersionRange_FallbackExclusive(t *testing.T) {
	for _, cause := range []string{"noncanonical", "range-span"} {
		t.Run(cause, func(t *testing.T) {
			db, barrier, lower, upper := openSuccessorAcquireBarrier(t)
			if cause == "noncanonical" {
				upper = nil
			} else {
				db.mu.Lock()
				db.queueRangeSpans = [][]batch.DeleteRange{{{Start: []byte("outside"), End: []byte("outside-end")}}}
				db.publishMemtablesLocked()
				db.mu.Unlock()
			}
			done := make(chan struct{})
			go func() { defer close(done); _, _, _, _ = db.SeekGEVersionRange(lower, upper) }()
			awaitSuccessorSignal(t, barrier.entered)
			shared := db.writeMu.TryRLock()
			if shared {
				db.writeMu.RUnlock()
			}
			close(barrier.resume)
			awaitSuccessorSignal(t, done)
			if shared {
				t.Fatal("fallback lost generic exclusive gate")
			}
		})
	}
}

func openSuccessorAcquireBarrier(t *testing.T, modes ...string) (*DB, *successorAcquireBarrier, []byte, []byte) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	lower, err := mvcckey.Encode([]byte("one-key"), 100)
	if err != nil {
		t.Fatal(err)
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, []byte("one-key"))
	if err != nil {
		t.Fatal(err)
	}
	key, _ := mvcckey.Encode([]byte("one-key"), 10)
	if err := backend.Set(key, []byte("old")); err != nil {
		t.Fatal(err)
	}
	mode := ""
	if len(modes) > 0 {
		mode = modes[0]
	}
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: mode, FlushThreshold: 1 << 30, MemtableShards: 4, MaxQueuedMemtables: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(); _ = backend.Close() })
	if err := db.ensureBackendRange(); err != nil {
		t.Fatal(err)
	}
	b := &successorAcquireBarrier{BackendDB: backend, snapper: backend, entered: make(chan struct{}), resume: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-b.resume:
		default:
			close(b.resume)
		}
	})
	db.backend = b
	return db, b, lower, upper
}

func awaitSuccessorSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("successor barrier timed out")
	}
}

func TestSeekGE_GenericKeepsExclusiveGate(t *testing.T) {
	db, barrier, lower, upper := openSuccessorAcquireBarrier(t)
	done := make(chan struct{})
	go func() { defer close(done); _, _, _, _ = db.SeekGE(lower, upper) }()
	awaitSuccessorSignal(t, barrier.entered)
	shared := db.writeMu.TryRLock()
	if shared {
		db.writeMu.RUnlock()
	}
	close(barrier.resume)
	awaitSuccessorSignal(t, done)
	if shared {
		t.Fatal("generic successor released its exclusive publication fence")
	}
}

func TestSeekGEVersionRange_AllowsPointWriterDuringBackendAcquire(t *testing.T) {
	db, barrier, lower, upper := openSuccessorAcquireBarrier(t)
	seeker, ok := any(db).(versionRangeSeeker)
	if !ok {
		t.Fatal("qualified MVCC successor capability missing")
	}
	done := make(chan struct{})
	var key, value []byte
	var found bool
	var readErr error
	go func() { defer close(done); key, value, found, readErr = seeker.SeekGEVersionRange(lower, upper) }()
	awaitSuccessorSignal(t, barrier.entered)
	shared := db.writeMu.TryRLock()
	if shared {
		db.writeMu.RUnlock()
	}
	exclusive := db.writeMu.TryLock()
	if exclusive {
		db.writeMu.Unlock()
	}
	close(barrier.resume)
	awaitSuccessorSignal(t, done)
	if !shared {
		t.Fatal("qualified successor still serializes point writers")
	}
	if exclusive {
		t.Fatal("qualified successor lost Close/checkpoint/maintenance exclusion")
	}
	want, _ := mvcckey.Encode([]byte("one-key"), 10)
	if readErr != nil || !found || !bytes.Equal(key, want) || string(value) != "old" {
		t.Fatalf("read = (%x,%q,%t,%v)", key, value, found, readErr)
	}
}

// A Store must exclude physical prune for the whole qualified seek. Sorted
// oldest-first deletion alone cannot protect a retained old table from a later
// backend snapshot whose winning logical tombstone anchor has been removed.
func TestSeekGEVersionRange_PruneRequiresStoreFence(t *testing.T) {
	db, barrier, lower, upper := openSuccessorAcquireBarrier(t)
	seed, _ := mvcckey.Encode([]byte("one-key"), 10)
	if err := barrier.BackendDB.(*backenddb.DB).Delete(seed); err != nil {
		t.Fatal(err)
	}
	old, _ := mvcckey.Encode([]byte("one-key"), 40)
	anchor, _ := mvcckey.Encode([]byte("one-key"), 80)
	if err := barrier.BackendDB.(*backenddb.DB).Set(anchor, []byte{2}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(old, []byte{1, 'o', 'l', 'd'}); err != nil {
		t.Fatal(err)
	}
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	rotatePointSuccessorMemtables(t, db)
	done := make(chan struct{})
	var key, value []byte
	var found bool
	var err error
	go func() { defer close(done); key, value, found, err = db.SeekGEVersionRange(lower, upper) }()
	awaitSuccessorSignal(t, barrier.entered)
	// Simulate already-active prune after it captured its snapshot. No new Store
	// admission is acquired here: this is the source-level counterexample.
	if e := db.Delete(old); e != nil {
		t.Fatal(e)
	}
	if e := db.Delete(anchor); e != nil {
		t.Fatal(e)
	}
	rotatePointSuccessorMemtables(t, db)
	for len(db.queue) > 0 {
		db.flushOneLocked(false)
	}
	it, e := barrier.BackendDB.Iterator(lower, upper)
	if e != nil {
		t.Fatal(e)
	}
	if it.Valid() {
		_ = it.Close()
		t.Fatal("backend range still contains a version after physical prune")
	}
	if e := it.Close(); e != nil {
		t.Fatal(e)
	}
	close(barrier.resume)
	awaitSuccessorSignal(t, done)
	if count := db.pointSuccessorMVCCPhysicalDeleteFallbacksTotal.Load(); count != 0 {
		t.Fatalf("counterexample observed physical tombstones: %d", count)
	}
	t.Logf("unfenced prune returned retained live40 after backend tomb80 deletion: key=%x value=%x", key, value)
	if err != nil || !found || !bytes.Equal(key, old) || !bytes.Equal(value, []byte{1, 'o', 'l', 'd'}) {
		t.Fatalf("prune counterexample not reproduced: %x %x %t %v", key, value, found, err)
	}
}
