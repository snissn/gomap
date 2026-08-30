package caching

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestDB_HasDoesNotBlockOnGlobalMu(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableMode: "skiplist", MemtableShards: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	db.mu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := db.Has([]byte("k"))
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Has: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Has blocked while db.mu was held")
	}
	db.mu.Unlock()
}

func TestDB_HasNoAllocsWithQueuedMemtables(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableMode: "skiplist", MemtableShards: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("set: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	view := db.memtables.Load()
	if view == nil || len(view.queue) == 0 {
		t.Fatalf("expected queued memtable in snapshot")
	}

	key := []byte("a")
	runtime.GC()
	allocs := testing.AllocsPerRun(1000, func() {
		ok, err := db.Has(key)
		if err != nil {
			panic(err)
		}
		if !ok {
			panic("expected key to exist")
		}
	})
	if allocs > 0.5 {
		t.Fatalf("expected Has to avoid allocations with queued memtables; got %.2f allocs/op", allocs)
	}
}

func TestSnapshot_HasAllocsIndependentOfQueuedMemtables(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableMode: "skiplist", MemtableShards: 8})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("00000005")

	emptySnap := db.AcquireSnapshot()
	if emptySnap == nil {
		t.Fatal("expected empty snapshot")
	}
	runtime.GC()
	allocsEmpty := testing.AllocsPerRun(1000, func() {
		ok, err := emptySnap.Has([]byte("missing"))
		if err != nil {
			panic(err)
		}
		if ok {
			panic("expected missing key")
		}
	})
	_ = emptySnap.Close()

	if err := db.Set(key, []byte("vb")); err != nil {
		t.Fatalf("set queued: %v", err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected queued snapshot")
	}
	defer snap.Close()

	runtime.GC()
	allocsQueued := testing.AllocsPerRun(1000, func() {
		ok, err := snap.Has(key)
		if err != nil {
			panic(err)
		}
		if !ok {
			panic("expected key to exist")
		}
	})
	if allocsQueued > allocsEmpty+0.5 {
		t.Fatalf("expected Snapshot.Has allocations to be independent of queued memtables; empty=%.2f allocs/op queued=%.2f allocs/op", allocsEmpty, allocsQueued)
	}
}

func TestDB_MemtableSnapshotUpdatesOnRotate(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableMode: "skiplist", MemtableShards: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("set: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	if err := db.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("set: %v", err)
	}
	ok, err := db.Has([]byte("b"))
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !ok {
		t.Fatalf("expected key in new mutable memtable")
	}
}

func TestDB_MemtableSnapshotUpdatesOnFlush(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableMode: "skiplist", MemtableShards: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("set: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	if !db.flushOne() {
		t.Fatalf("expected flush to run")
	}

	view := db.memtables.Load()
	if view == nil {
		t.Fatalf("expected snapshot")
	}
	if len(view.queue) != 0 {
		t.Fatalf("expected snapshot queue to be empty after flush, got %d", len(view.queue))
	}
}

func TestDB_MemtableSnapshotImmutableAcrossInPlaceQueueCompaction(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableMode: "skiplist", MemtableShards: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("set a: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	if err := db.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("set b: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	view := db.memtables.Load()
	if view == nil || len(view.queue) != 2 {
		t.Fatalf("expected snapshot queue len 2; got %v", view)
	}
	first := view.queue[0]
	second := view.queue[1]
	if len(view.queueRanges) != 2 {
		t.Fatalf("expected snapshot queueRanges len 2; got %d", len(view.queueRanges))
	}
	firstRange := view.queueRanges[0]
	secondRange := view.queueRanges[1]
	firstMin := append([]byte(nil), firstRange.min...)
	firstMax := append([]byte(nil), firstRange.max...)
	secondMin := append([]byte(nil), secondRange.min...)
	secondMax := append([]byte(nil), secondRange.max...)
	firstValid := firstRange.valid
	secondValid := secondRange.valid

	if err := db.DeleteRange([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	if len(view.queue) != 2 {
		t.Fatalf("snapshot mutated: expected queue len 2, got %d", len(view.queue))
	}
	if view.queue[0] != first || view.queue[1] != second {
		t.Fatalf("snapshot mutated: queue elements changed")
	}
	if len(view.queueRanges) != 2 {
		t.Fatalf("snapshot mutated: expected queueRanges len 2, got %d", len(view.queueRanges))
	}
	if view.queueRanges[0].valid != firstValid || !bytes.Equal(view.queueRanges[0].min, firstMin) || !bytes.Equal(view.queueRanges[0].max, firstMax) {
		t.Fatalf("snapshot mutated: first queueRange changed")
	}
	if view.queueRanges[1].valid != secondValid || !bytes.Equal(view.queueRanges[1].min, secondMin) || !bytes.Equal(view.queueRanges[1].max, secondMax) {
		t.Fatalf("snapshot mutated: second queueRange changed")
	}

	view2 := db.memtables.Load()
	if view2 == nil || len(view2.queue) != 1 {
		t.Fatalf("expected compacted snapshot queue len 1; got %v", view2)
	}
}

func TestDB_IteratorAllocsIndependentOfQueueLen(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableMode: "skiplist", MemtableShards: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.backendRangeInit.Do(func() {})
	db.mu.Lock()
	db.backendRangeKnown = true
	db.backendRange = keyRange{}
	db.mu.Unlock()

	start := []byte("z")
	end := []byte("zz")

	runtime.GC()
	allocsEmpty := testing.AllocsPerRun(1000, func() {
		it, err := db.Iterator(start, end)
		if err != nil {
			panic(err)
		}
		_ = it.Close()
	})

	if err := db.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("set a: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()
	if err := db.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("set b: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	view := db.memtables.Load()
	if view == nil || len(view.queue) != 2 {
		t.Fatalf("expected snapshot queue len 2; got %v", view)
	}
	runtime.GC()
	allocsQueued := testing.AllocsPerRun(1000, func() {
		it, err := db.Iterator(start, end)
		if err != nil {
			panic(err)
		}
		_ = it.Close()
	})

	if allocsQueued > allocsEmpty+0.5 {
		t.Fatalf("expected Iterator allocations to be independent of queued memtables; empty=%.2f allocs/op queued=%.2f allocs/op", allocsEmpty, allocsQueued)
	}
}

func TestSnapshot_HasAllocsIndependentOfQueuedShards(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	db, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "skiplist",
		MemtableShards: 8,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var key [8]byte
	for i := uint64(0); ; i++ {
		binary.BigEndian.PutUint64(key[:], i)
		if db.shardIndex(key[:]) == 5 {
			break
		}
	}
	targetKey := append([]byte(nil), key[:]...)

	if err := db.Set(targetKey, []byte("v")); err != nil {
		t.Fatalf("set target: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate target: %v", err)
	}
	db.mu.Unlock()

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()

	runtime.GC()
	allocs := testing.AllocsPerRun(1000, func() {
		ok, err := snap.Has(targetKey)
		if err != nil {
			panic(err)
		}
		if !ok {
			panic("expected queued snapshot hit")
		}
	})

	if allocs > 0.5 {
		t.Fatalf("expected Snapshot.Has allocations to stay at 0 for queued shard hits; got %.2f allocs/op", allocs)
	}
}

func TestPointReads_RawFallbackHasAllocsIndependentOfQueuedShards(t *testing.T) {
	const shards = 8
	const targetShard = 5

	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, shards),
		mutableShardMask: shards - 1,
	}

	var key [8]byte
	for i := uint64(0); ; i++ {
		binary.BigEndian.PutUint64(key[:], i)
		if db.shardIndex(key[:]) == targetShard {
			break
		}
	}
	targetKey := append([]byte(nil), key[:]...)

	queue := make([]memtable.Table, 0, shards)
	queueShardIDs := make([]uint16, 0, shards)
	for shard := 0; shard < shards; shard++ {
		mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
		if err != nil {
			t.Fatalf("new memtable: %v", err)
		}
		queue = append(queue, mt)
		queueShardIDs = append(queueShardIDs, uint16(shard))
	}
	queue[targetShard].SetEntry(targetKey, []byte("v"), page.ValuePtr{}, node.FlagInline)

	db.mu.Lock()
	db.queue = queue
	db.queueShardIDs = queueShardIDs
	db.mu.Unlock()

	runtime.GC()
	allocs := testing.AllocsPerRun(1000, func() {
		ok, err := db.Has(targetKey)
		if err != nil {
			panic(err)
		}
		if !ok {
			panic("expected raw fallback queued hit")
		}
	})

	if allocs > 0.5 {
		t.Fatalf("expected raw fallback Has allocations to stay at 0 for queued shard hits; got %.2f allocs/op", allocs)
	}
}

func TestDB_BatchWriteBypassAllocsIndependentOfQueueLen(t *testing.T) {
	backend := &noAllocBackend{}
	db, err := Open(t.TempDir(), backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableMode: "skiplist", MemtableShards: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("z")
	val := []byte("vz")

	b := db.NewBatchWithSize(1)
	entry := batch.Entry{Type: batch.OpPut, Key: key, Value: val}
	prepareBatch := func() {
		if cap(b.entries) == 0 {
			b.entries = make([]batch.Entry, 1)
		} else {
			b.entries = b.entries[:1]
		}
		b.entries[0] = entry
		b.size = len(key) + len(val)
	}

	prepareBatch()
	if err := b.writeBypass(false); err != nil {
		t.Fatalf("writeBypass warmup: %v", err)
	}

	runtime.GC()
	allocsEmpty := testing.AllocsPerRun(1000, func() {
		prepareBatch()
		if err := b.writeBypass(false); err != nil {
			panic(err)
		}
	})

	if err := db.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("set a: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()
	if err := db.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("set b: %v", err)
	}
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	view := db.memtables.Load()
	if view == nil || len(view.queue) != 2 {
		t.Fatalf("expected snapshot queue len 2; got %v", view)
	}

	runtime.GC()
	allocsQueued := testing.AllocsPerRun(1000, func() {
		prepareBatch()
		if err := b.writeBypass(false); err != nil {
			panic(err)
		}
	})

	if allocsQueued > allocsEmpty+0.5 {
		t.Fatalf("expected writeBypass allocations to be independent of queued memtables; empty=%.2f allocs/op queued=%.2f allocs/op", allocsEmpty, allocsQueued)
	}
}

func TestDB_BatchWriteBypassRetainsMemtableViewLease(t *testing.T) {
	backend := &noAllocBackend{}
	db, err := Open(t.TempDir(), backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableMode: "skiplist", MemtableShards: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("k")
	val := []byte("v")
	getStarted := make(chan struct{})
	getRelease := make(chan struct{})

	db.mu.Lock()
	if len(db.mutableShards) != 1 {
		db.mu.Unlock()
		t.Fatalf("expected one mutable shard; got %d", len(db.mutableShards))
	}
	base := db.mutableShards[0].mem
	db.mutableShards[0].mem = &blockingGetMemtable{
		Table:   base,
		started: getStarted,
		release: getRelease,
	}
	db.mutableShards[0].rng = keyRange{}
	db.mutableShards[0].rng.add(key)
	db.publishMemtablesLocked()
	view := db.memtables.Load()
	db.mu.Unlock()

	if view == nil {
		t.Fatal("expected published memtable view")
	}
	if got := view.refs.Load(); got != 1 {
		t.Fatalf("expected baseline refcount 1; got %d", got)
	}

	b := db.NewBatchWithSize(1)
	if err := b.Set(key, val); err != nil {
		t.Fatalf("batch set: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- b.writeBypass(false)
	}()

	select {
	case <-getStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("writeBypass did not reach memtable Get")
	}

	if got := view.refs.Load(); got < 2 {
		t.Fatalf("expected writeBypass to retain memtable view while reading; refs=%d", got)
	}

	close(getRelease)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("writeBypass: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writeBypass blocked after releasing memtable Get")
	}

	if got := view.refs.Load(); got != 1 {
		t.Fatalf("expected writeBypass to release memtable view lease; refs=%d", got)
	}
}

func TestDB_GetRecordsForegroundGraceCrossing(t *testing.T) {
	backend := &blockingPointReadBackend{
		noAllocBackend: &noAllocBackend{},
		started:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	cdb, err := Open(t.TempDir(), backend, Options{DisableWAL: true, AllowUnsafe: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer cdb.Close()

	cdb.foregroundMaintenanceGraceState.Store(2)
	cdb.activeForegroundIterators.publishGrace(2)
	done := make(chan error, 1)
	go func() {
		_, err := cdb.Get([]byte("k"))
		done <- err
	}()
	select {
	case <-backend.started:
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not reach backend")
	}
	if transitioned, active := cdb.activeForegroundIterators.transitionGrace(2); !transitioned || active != 1 {
		t.Fatalf("grace boundary: transitioned=%v active=%d want true,1", transitioned, active)
	}
	cdb.foregroundMaintenanceGraceState.Store(3)
	close(backend.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Get blocked after backend release")
	}
	if got := cdb.foregroundMaintenanceGraceActivity.Load(); got != 1 {
		t.Fatalf("grace-crossing activity=%d want=1", got)
	}
	if got := cdb.activeForegroundIterators.Load(); got != 0 {
		t.Fatalf("active foreground reads=%d want=0", got)
	}
}

type blockingPointReadBackend struct {
	*noAllocBackend
	started chan struct{}
	release chan struct{}
}

func (b *blockingPointReadBackend) Get([]byte) ([]byte, error) {
	close(b.started)
	<-b.release
	return nil, nil
}

type blockingGetMemtable struct {
	memtable.Table
	started chan struct{}
	release <-chan struct{}
}

func (m *blockingGetMemtable) Get(key []byte) ([]byte, bool, bool) {
	select {
	case <-m.started:
	default:
		close(m.started)
	}
	<-m.release
	return m.Table.Get(key)
}

type noAllocBackend struct {
	batch noAllocBatch
	iter  emptyUnsafeIterator
}

func (b *noAllocBackend) Get(key []byte) ([]byte, error) { return nil, nil }

func (b *noAllocBackend) GetUnsafe(key []byte) ([]byte, error) { return nil, nil }

func (b *noAllocBackend) GetAppend(key, dst []byte) ([]byte, error) { return dst, nil }

func (b *noAllocBackend) Has(key []byte) (bool, error) { return false, nil }

func (b *noAllocBackend) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return &b.iter, nil
}

func (b *noAllocBackend) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return &b.iter, nil
}

func (b *noAllocBackend) NewBatch() batch.Interface { return &b.batch }

func (b *noAllocBackend) Close() error { return nil }

func (b *noAllocBackend) Print() error { return nil }

func (b *noAllocBackend) Stats() map[string]string { return nil }

type noAllocBatch struct{}

func (b *noAllocBatch) Set(key, value []byte) error { return nil }

func (b *noAllocBatch) Delete(key []byte) error { return nil }

func (b *noAllocBatch) DeleteRange(start, end []byte) error { return nil }

func (b *noAllocBatch) SetOps(ops []batch.Entry) error { return nil }

func (b *noAllocBatch) Write() error { return nil }

func (b *noAllocBatch) WriteSync() error { return nil }

func (b *noAllocBatch) Close() error { return nil }

func (b *noAllocBatch) Replay(func(batch.Entry) error) error { return nil }

func (b *noAllocBatch) GetByteSize() (int, error) { return 0, nil }

type emptyUnsafeIterator struct{}

func (it *emptyUnsafeIterator) Valid() bool { return false }

func (it *emptyUnsafeIterator) Next() {}

func (it *emptyUnsafeIterator) Seek(key []byte) {}

func (it *emptyUnsafeIterator) UnsafeKey() []byte { return nil }

func (it *emptyUnsafeIterator) UnsafeValue() []byte { return nil }

func (it *emptyUnsafeIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return nil, page.ValuePtr{}, 0
}

func (it *emptyUnsafeIterator) Key() []byte { return nil }

func (it *emptyUnsafeIterator) Value() []byte { return nil }

func (it *emptyUnsafeIterator) KeyCopy(dst []byte) []byte { return dst }

func (it *emptyUnsafeIterator) ValueCopy(dst []byte) []byte { return dst }

func (it *emptyUnsafeIterator) IsDeleted() bool { return false }

func (it *emptyUnsafeIterator) Error() error { return nil }

func (it *emptyUnsafeIterator) Close() error { return nil }

func (it *emptyUnsafeIterator) Domain() ([]byte, []byte) { return nil, nil }
