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
