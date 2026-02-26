package caching

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
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
func (panicBackend) AcquireSnapshot() *db.Snapshot {
	panic("backend AcquireSnapshot should not be called")
}

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

type snapshotPointReadBackend struct {
	panicBackend
	acknowledgeSnapshots int
}

func (b *snapshotPointReadBackend) AcquireSnapshot() *db.Snapshot {
	b.acknowledgeSnapshots++
	return &db.Snapshot{}
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

func TestSnapshotPointReads_ConsultOnlyShardImmutableQueue(t *testing.T) {
	const shards = 8
	const targetShard = 4

	db := &DB{
		backend:          &snapshotPointReadBackend{},
		mutableShards:    make([]memShard, shards),
		mutableShardMask: shards - 1,
	}

	var key [8]byte
	var wantKey []byte
	for i := uint64(0); ; i++ {
		binary.BigEndian.PutUint64(key[:], i)
		if db.shardIndex(key[:]) == targetShard {
			wantKey = append(wantKey[:0], key[:]...)
			break
		}
	}
	if len(wantKey) == 0 {
		t.Fatalf("failed to locate key for shard %d", targetShard)
	}

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
	queue[targetShard].SetEntry(wantKey, []byte("visible"), page.ValuePtr{}, node.FlagInline)

	db.memtables.Store(&memtableView{
		mutables:      make([]memtable.Table, shards),
		queue:         queue,
		queueShardIDs: queueShardIDs,
	})

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	defer func() {
		_ = snap.Close()
	}()

	got, err := snap.Get(wantKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "visible" {
		t.Fatalf("Get: got=%q want=%q", got, "visible")
	}

	gotUnsafe, err := snap.GetUnsafe(wantKey)
	if err != nil {
		t.Fatalf("GetUnsafe: %v", err)
	}
	if string(gotUnsafe) != "visible" {
		t.Fatalf("GetUnsafe: got=%q want=%q", gotUnsafe, "visible")
	}

	gotAppend, err := snap.GetAppend(wantKey, []byte("p:"))
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if string(gotAppend) != "p:visible" {
		t.Fatalf("GetAppend: got=%q want=%q", gotAppend, "p:visible")
	}

	ok, err := snap.Has(wantKey)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !ok {
		t.Fatalf("Has: want true for %q", wantKey)
	}

	entry, err := snap.GetEntry(wantKey)
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if string(entry.Value) != "visible" {
		t.Fatalf("GetEntry: got=%q want=%q", entry.Value, "visible")
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
}

func TestSnapshotPointReads_TombstoneSkipsBackend(t *testing.T) {
	const shards = 4
	const targetShard = 2

	db := &DB{
		backend:          &snapshotPointReadBackend{},
		mutableShards:    make([]memShard, shards),
		mutableShardMask: shards - 1,
	}

	var key [8]byte
	var targetKey []byte
	for i := uint64(0); ; i++ {
		binary.BigEndian.PutUint64(key[:], i)
		if db.shardIndex(key[:]) == targetShard {
			targetKey = append(targetKey[:0], key[:]...)
			break
		}
	}
	if len(targetKey) == 0 {
		t.Fatalf("failed to locate key for shard %d", targetShard)
	}

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
	queue[targetShard].SetEntry(targetKey, nil, page.ValuePtr{}, node.FlagTombstone)

	db.memtables.Store(&memtableView{
		mutables:      make([]memtable.Table, shards),
		queue:         queue,
		queueShardIDs: queueShardIDs,
	})

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	defer func() {
		_ = snap.Close()
	}()

	if _, err := snap.Get(targetKey); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("Get: want ErrKeyNotFound, got %v", err)
	}
	if _, err := snap.GetUnsafe(targetKey); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetUnsafe: want ErrKeyNotFound, got %v", err)
	}
	if _, err := snap.GetAppend(targetKey, []byte("p:")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetAppend: want ErrKeyNotFound, got %v", err)
	}
	ok, err := snap.Has(targetKey)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if ok {
		t.Fatalf("Has: want false")
	}
	if _, err := snap.GetEntry(targetKey); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("GetEntry: want ErrKeyNotFound, got %v", err)
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
}

func TestSnapshotMemtableReader_SkipsNonTargetQueueShardsForMiss(t *testing.T) {
	const shards = 8
	const targetShard = 1

	db := &DB{
		mutableShards:    make([]memShard, shards),
		mutableShardMask: shards - 1,
	}

	var targetKey [8]byte
	targetFound := false
	for i := uint64(0); i < 1<<18; i++ {
		binary.BigEndian.PutUint64(targetKey[:], i)
		if db.shardIndex(targetKey[:]) == targetShard {
			targetFound = true
			break
		}
	}
	if !targetFound {
		t.Fatalf("failed to locate key for shard %d", targetShard)
	}

	var missKey [8]byte
	missFound := false
	missShard := -1
	for i := uint64(0); i < 1<<18; i++ {
		binary.BigEndian.PutUint64(missKey[:], i<<1|1)
		shard := db.shardIndex(missKey[:])
		if shard != targetShard {
			missFound = true
			missShard = shard
			break
		}
	}
	if !missFound {
		t.Fatalf("failed to locate miss key not in shard %d", targetShard)
	}

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
	queue[targetShard].SetEntry(targetKey[:], []byte("visible"), page.ValuePtr{}, node.FlagInline)

	view := &memtableView{
		mutables:      make([]memtable.Table, shards),
		queue:         queue,
		queueShardIDs: queueShardIDs,
	}
	reader := &snapshotMemtableReader{
		db:   db,
		view: view,
	}

	entry, found, err := reader.GetEntry(missKey[:])
	if err != nil {
		t.Fatalf("GetEntry miss: %v", err)
	}
	if found {
		t.Fatalf("expected miss for key %x", missKey)
	}
	if !bytes.Equal(entry.Value, nil) {
		t.Fatalf("expected nil entry value on miss")
	}

	for shard := 0; shard < shards; shard++ {
		if shard == missShard {
			if tables[shard].getEntryCalls == 0 {
				t.Fatalf("expected shard %d GetEntry calls > 0 on miss, got %d", shard, tables[shard].getEntryCalls)
			}
			continue
		}
		if tables[shard].getEntryCalls != 0 {
			t.Fatalf("expected shard %d GetEntry calls = 0 on miss, got %d", shard, tables[shard].getEntryCalls)
		}
	}
}
