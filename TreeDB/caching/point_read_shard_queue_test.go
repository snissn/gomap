package caching

import (
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type countingTable struct {
	inner         memtable.Table
	getCalls      int
	getEntryCalls int
}

func (t *countingTable) Set(key, value []byte) { t.inner.Set(key, value) }
func (t *countingTable) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	t.inner.SetEntry(key, value, ptr, flags)
}
func (t *countingTable) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	return t.inner.PutWithCallback(key, value, cb)
}
func (t *countingTable) Delete(key []byte) { t.inner.Delete(key) }
func (t *countingTable) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	return t.inner.DeleteWithCallback(key, cb)
}
func (t *countingTable) SetSteal(key, value []byte) { t.inner.SetSteal(key, value) }
func (t *countingTable) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	t.inner.SetEntrySteal(key, value, ptr, flags)
}
func (t *countingTable) DeleteSteal(key []byte) { t.inner.DeleteSteal(key) }
func (t *countingTable) Get(key []byte) ([]byte, bool, bool) {
	t.getCalls++
	return t.inner.Get(key)
}
func (t *countingTable) GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	t.getEntryCalls++
	return t.inner.GetEntry(key)
}
func (t *countingTable) Size() int64 { return t.inner.Size() }
func (t *countingTable) Len() int    { return t.inner.Len() }
func (t *countingTable) NewIterator(start, end []byte) iterator.UnsafeIterator {
	return t.inner.NewIterator(start, end)
}
func (t *countingTable) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	return t.inner.NewReverseIterator(start, end)
}
func (t *countingTable) Freeze() { t.inner.Freeze() }

var _ memtable.Table = (*countingTable)(nil)

type panicBackend struct{}

func (panicBackend) Get(_ []byte) ([]byte, error) { panic("backend Get should not be called") }
func (panicBackend) GetUnsafe(_ []byte) ([]byte, error) {
	panic("backend GetUnsafe should not be called")
}
func (panicBackend) GetAppend(_ []byte, _ []byte) ([]byte, error) {
	panic("backend GetAppend should not be called")
}
func (panicBackend) GetMany(_ [][]byte) ([][]byte, error) {
	panic("backend GetMany should not be called")
}
func (panicBackend) Has(_ []byte) (bool, error) { panic("backend Has should not be called") }
func (panicBackend) Iterator(_, _ []byte) (iterator.UnsafeIterator, error) {
	panic("backend Iterator should not be called")
}
func (panicBackend) ReverseIterator(_, _ []byte) (iterator.UnsafeIterator, error) {
	panic("backend ReverseIterator should not be called")
}
func (panicBackend) NewBatch() batch.Interface { panic("backend NewBatch should not be called") }
func (panicBackend) Close() error              { panic("backend Close should not be called") }
func (panicBackend) Print() error              { panic("backend Print should not be called") }
func (panicBackend) Stats() map[string]string  { return nil }

type countingBackend struct {
	panicBackend
	getCalls       int
	getAppendCalls int
	getManyCalls   int
}

func (b *countingBackend) Get(_ []byte) ([]byte, error) {
	b.getCalls++
	return []byte("backend"), nil
}

func (b *countingBackend) GetAppend(_ []byte, dst []byte) ([]byte, error) {
	b.getAppendCalls++
	return append(dst, []byte("backend")...), nil
}

func (b *countingBackend) GetMany(keys [][]byte) ([][]byte, error) {
	b.getManyCalls++
	out := make([][]byte, len(keys))
	for i := range keys {
		out[i] = []byte("backend")
	}
	return out, nil
}

func TestPointReads_ConsultOnlyShardImmutableQueue(t *testing.T) {
	const shards = 8
	const targetShard = 5

	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, shards),
		mutableShardMask: shards - 1,
	}

	// Ensure shardIndex() routes a deterministic key to targetShard.
	var key [8]byte
	for i := uint64(0); ; i++ {
		binary.BigEndian.PutUint64(key[:], i)
		if db.shardIndex(key[:]) == targetShard {
			break
		}
	}
	wantKey := append([]byte(nil), key[:]...)

	queue := make([]memtable.Table, 0, shards)
	queueShardIDs := make([]uint16, 0, shards)
	tables := make([]*countingTable, 0, shards)

	for shard := 0; shard < shards; shard++ {
		mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
		if err != nil {
			t.Fatalf("new memtable: %v", err)
		}
		ct := &countingTable{inner: mt}
		queue = append(queue, ct)
		queueShardIDs = append(queueShardIDs, uint16(shard))
		tables = append(tables, ct)
	}

	// Put the key only into the target shard's immutable memtable.
	queue[targetShard].SetEntry(wantKey, []byte("v"), page.ValuePtr{}, node.FlagInline)

	db.memtables.Store(&memtableView{
		mutables:      make([]memtable.Table, shards), // non-nil length enables shard filtering
		queue:         queue,
		queueShardIDs: queueShardIDs,
	})

	val, found, err := db.getMemtable(wantKey)
	if err != nil {
		t.Fatalf("getMemtable: %v", err)
	}
	if !found || string(val) != "v" {
		t.Fatalf("unexpected get: found=%v val=%q", found, string(val))
	}

	for shard := 0; shard < shards; shard++ {
		calls := tables[shard].getEntryCalls
		if shard == targetShard {
			if calls == 0 {
				t.Fatalf("expected target shard GetEntry calls > 0, got %d", calls)
			}
			continue
		}
		if calls != 0 {
			t.Fatalf("expected non-target shard %d GetEntry calls = 0, got %d", shard, calls)
		}
	}

	ok, err := db.Has(wantKey)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !ok {
		t.Fatalf("expected Has to be true")
	}
	for shard := 0; shard < shards; shard++ {
		calls := tables[shard].getCalls
		if shard == targetShard {
			if calls == 0 {
				t.Fatalf("expected target shard Get calls > 0, got %d", calls)
			}
			continue
		}
		if calls != 0 {
			t.Fatalf("expected non-target shard %d Get calls = 0, got %d", shard, calls)
		}
	}
}

func TestPointReads_EmptyMemtablesBypassToBackend(t *testing.T) {
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	ct := &countingTable{inner: mt}
	backend := &countingBackend{}

	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	db.memtables.Store(&memtableView{
		mutables: []memtable.Table{ct},
	})

	key := []byte("k")
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "backend" {
		t.Fatalf("unexpected Get value: %q", got)
	}
	if backend.getCalls != 1 {
		t.Fatalf("expected backend Get calls = 1, got %d", backend.getCalls)
	}
	if ct.getEntryCalls != 0 {
		t.Fatalf("expected memtable GetEntry calls = 0, got %d", ct.getEntryCalls)
	}

	gotAppend, err := db.GetAppend(key, []byte("p:"))
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if string(gotAppend) != "p:backend" {
		t.Fatalf("unexpected GetAppend value: %q", gotAppend)
	}
	if backend.getAppendCalls != 1 {
		t.Fatalf("expected backend GetAppend calls = 1, got %d", backend.getAppendCalls)
	}
	if ct.getEntryCalls != 0 {
		t.Fatalf("expected memtable GetEntry calls = 0 after GetAppend, got %d", ct.getEntryCalls)
	}

	gotMany, err := db.GetMany([][]byte{key, []byte("k2")})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(gotMany) != 2 || string(gotMany[0]) != "backend" || string(gotMany[1]) != "backend" {
		t.Fatalf("unexpected GetMany values: %#v", gotMany)
	}
	if backend.getManyCalls != 1 {
		t.Fatalf("expected backend GetMany calls = 1, got %d", backend.getManyCalls)
	}
	if ct.getEntryCalls != 0 {
		t.Fatalf("expected memtable GetEntry calls = 0 after GetMany, got %d", ct.getEntryCalls)
	}
}

func TestPointReads_EmptyMemtableBypassGuardChecksMutableLen(t *testing.T) {
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	ct := &countingTable{inner: mt}
	key := []byte("k")
	ct.SetEntry(key, []byte("v"), page.ValuePtr{}, node.FlagInline)

	// Simulate a stale mutableBytes==0 window while mutable view still has data.
	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	db.memtables.Store(&memtableView{
		mutables: []memtable.Table{ct},
	})

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("unexpected Get value: %q", got)
	}
	if ct.getEntryCalls == 0 {
		t.Fatalf("expected memtable GetEntry to be consulted")
	}
	callsAfterGet := ct.getEntryCalls

	gotMany, err := db.GetMany([][]byte{key})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(gotMany) != 1 || string(gotMany[0]) != "v" {
		t.Fatalf("unexpected GetMany value: %#v", gotMany)
	}
	if ct.getEntryCalls <= callsAfterGet {
		t.Fatalf("expected memtable GetEntry to be consulted by GetMany")
	}

	gotAppend, err := db.GetAppend(key, []byte("p:"))
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if string(gotAppend) != "p:v" {
		t.Fatalf("unexpected GetAppend value: %q", gotAppend)
	}
	if ct.getEntryCalls < 2 {
		t.Fatalf("expected memtable GetEntry to be consulted by GetAppend")
	}
}

func TestPointReads_CurrentMutableRouteUsesRangedShard(t *testing.T) {
	const shards = 8
	const routeShard = 3

	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, shards),
		mutableShardMask: shards - 1,
	}

	var key [8]byte
	for i := uint64(0); ; i++ {
		binary.BigEndian.PutUint64(key[:], i)
		if db.shardIndex(key[:]) != routeShard {
			break
		}
	}
	wantKey := append([]byte(nil), key[:]...)

	mutables := make([]memtable.Table, shards)
	tables := make([]*countingTable, shards)
	for shard := 0; shard < shards; shard++ {
		mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
		if err != nil {
			t.Fatalf("new memtable: %v", err)
		}
		ct := &countingTable{inner: mt}
		mutables[shard] = ct
		tables[shard] = ct
	}
	mutables[routeShard].SetEntry(wantKey, []byte("routed"), page.ValuePtr{}, node.FlagInline)

	rng := keyRange{}
	rng.add(wantKey)
	db.memtables.Store(&memtableView{
		mutables: mutables,
		mutableRoute: &mutableRouteState{
			shardID: routeShard,
			rng:     rng,
		},
	})
	db.mutableBytes.Store(1)

	val, found, err := db.getMemtable(wantKey)
	if err != nil {
		t.Fatalf("getMemtable: %v", err)
	}
	if !found || string(val) != "routed" {
		t.Fatalf("unexpected routed get: found=%v val=%q", found, string(val))
	}

	for shard := 0; shard < shards; shard++ {
		calls := tables[shard].getEntryCalls
		if shard == routeShard {
			if calls == 0 {
				t.Fatalf("expected routed shard GetEntry calls > 0, got %d", calls)
			}
			continue
		}
		if calls != 0 {
			t.Fatalf("expected non-routed shard %d GetEntry calls = 0, got %d", shard, calls)
		}
	}
}

func TestPointReads_CurrentMutableRouteSetProbesNewestMatchFirst(t *testing.T) {
	const shards = 8
	const oldShard = 1
	const newShard = 3

	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, shards),
		mutableShardMask: shards - 1,
	}

	var rawKey [8]byte
	binary.BigEndian.PutUint64(rawKey[:], 42)
	key := append([]byte(nil), rawKey[:]...)

	mutables := make([]memtable.Table, shards)
	for shard := 0; shard < shards; shard++ {
		mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
		if err != nil {
			t.Fatalf("new memtable: %v", err)
		}
		mutables[shard] = mt
	}
	mutables[oldShard].SetEntry(key, []byte("old"), page.ValuePtr{}, node.FlagInline)
	mutables[newShard].SetEntry(key, []byte("new"), page.ValuePtr{}, node.FlagInline)

	rng := keyRange{}
	rng.add(key)
	db.memtables.Store(&memtableView{
		mutables: mutables,
		mutableRoute: &mutableRouteState{
			shardID: newShard,
			rng:     cloneRange(rng),
			entries: []mutableRouteEntry{
				{shardID: oldShard, rng: cloneRange(rng)},
				{shardID: newShard, rng: cloneRange(rng)},
			},
		},
	})
	db.mutableBytes.Store(1)

	val, found, err := db.getMemtable(key)
	if err != nil {
		t.Fatalf("getMemtable: %v", err)
	}
	if !found || string(val) != "new" {
		t.Fatalf("unexpected route-set get: found=%v val=%q", found, string(val))
	}
}

func TestPointReads_RangedQueueUsesQueueRangeNotHashShard(t *testing.T) {
	const shards = 8
	const routeShard = 6

	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, shards),
		mutableShardMask: shards - 1,
	}

	var key [8]byte
	for i := uint64(0); ; i++ {
		binary.BigEndian.PutUint64(key[:], i)
		if db.shardIndex(key[:]) != routeShard {
			break
		}
	}
	wantKey := append([]byte(nil), key[:]...)

	queue := make([]memtable.Table, 0, shards)
	queueShardIDs := make([]uint16, 0, shards)
	queueRouteModes := make([]uint8, 0, shards)
	queueRanges := make([]keyRange, 0, shards)
	tables := make([]*countingTable, 0, shards)

	for shard := 0; shard < shards; shard++ {
		mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
		if err != nil {
			t.Fatalf("new memtable: %v", err)
		}
		ct := &countingTable{inner: mt}
		queue = append(queue, ct)
		queueShardIDs = append(queueShardIDs, uint16(shard))
		queueRouteModes = append(queueRouteModes, memtableRouteHashed)
		queueRanges = append(queueRanges, keyRange{})
		tables = append(tables, ct)
	}

	queue[routeShard].SetEntry(wantKey, []byte("queued-routed"), page.ValuePtr{}, node.FlagInline)
	queueRouteModes[routeShard] = memtableRouteRanged
	queueRanges[routeShard].add(wantKey)

	db.memtables.Store(&memtableView{
		mutables:        make([]memtable.Table, shards),
		queue:           queue,
		queueShardIDs:   queueShardIDs,
		queueRouteModes: queueRouteModes,
		queueRanges:     queueRanges,
	})

	val, found, err := db.getMemtable(wantKey)
	if err != nil {
		t.Fatalf("getMemtable: %v", err)
	}
	if !found || string(val) != "queued-routed" {
		t.Fatalf("unexpected queued routed get: found=%v val=%q", found, string(val))
	}

	for shard := 0; shard < shards; shard++ {
		calls := tables[shard].getEntryCalls
		if shard == routeShard {
			if calls == 0 {
				t.Fatalf("expected ranged queue shard GetEntry calls > 0, got %d", calls)
			}
			continue
		}
		if calls != 0 {
			t.Fatalf("expected non-ranged queue shard %d GetEntry calls = 0, got %d", shard, calls)
		}
	}
}
