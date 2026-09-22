package caching

import (
	"encoding/binary"
	"fmt"
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
	iterCalls     int
	reverseCalls  int
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
	t.iterCalls++
	return t.inner.NewIterator(start, end)
}
func (t *countingTable) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	t.reverseCalls++
	return t.inner.NewReverseIterator(start, end)
}
func (t *countingTable) Freeze() { t.inner.Freeze() }

var _ memtable.Table = (*countingTable)(nil)

type pointPreferredCountingTable struct {
	*countingTable
}

func (t *pointPreferredCountingTable) PreferSortedPointProbes(_, _ []byte, _ int) bool {
	return true
}

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
	lastGetMany    [][]byte
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
	b.lastGetMany = make([][]byte, len(keys))
	for i := range keys {
		b.lastGetMany[i] = append([]byte(nil), keys[i]...)
	}
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
		rootPointShards: func() []rootDomainSnapshot {
			shardsSnap := make([]rootDomainSnapshot, shards)
			for shard := 0; shard < shards; shard++ {
				shardsSnap[shard].immutables = []memtable.Table{queue[shard]}
			}
			return shardsSnap
		}(),
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
	beforeHasTargetGetEntryCalls := tables[targetShard].getEntryCalls

	ok, err := db.Has(wantKey)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !ok {
		t.Fatalf("expected Has to be true")
	}
	for shard := 0; shard < shards; shard++ {
		if shard == targetShard {
			if got := tables[shard].getEntryCalls; got <= beforeHasTargetGetEntryCalls {
				t.Fatalf("expected target shard GetEntry calls to increase after Has, got before=%d after=%d", beforeHasTargetGetEntryCalls, got)
			}
			if got := tables[shard].getCalls; got != 0 {
				t.Fatalf("expected target shard Get calls = 0, got %d", got)
			}
			continue
		}
		if got := tables[shard].getCalls; got != 0 {
			t.Fatalf("expected non-target shard %d Get calls = 0, got %d", shard, got)
		}
		if got := tables[shard].getEntryCalls; got != 0 {
			t.Fatalf("expected non-target shard %d GetEntry calls = 0, got %d", shard, got)
		}
	}
}

func TestSnapshotPointReads_ConsultOnlyShardImmutableQueue(t *testing.T) {
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

	queue[targetShard].SetEntry(wantKey, []byte("v"), page.ValuePtr{}, node.FlagInline)

	snap := &Snapshot{
		db: db,
		rootPointShards: func() []rootDomainSnapshot {
			shardsSnap := make([]rootDomainSnapshot, shards)
			for shard := 0; shard < shards; shard++ {
				shardsSnap[shard].immutables = []memtable.Table{queue[shard]}
			}
			return shardsSnap
		}(),
	}

	got, err := snap.Get(wantKey)
	if err != nil {
		t.Fatalf("Snapshot.Get: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("unexpected snapshot value: %q", got)
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
		rootPointShards: []rootDomainSnapshot{
			{mutable: ct},
		},
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
	if ct.getEntryCalls <= callsAfterGet && ct.iterCalls == 0 {
		t.Fatalf("expected GetMany to consult the mutable memtable")
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

func TestGetMany_ConsultsOnlyTouchedPublishedRootPointShards(t *testing.T) {
	const shards = 8
	const firstTarget = 1
	const secondTarget = 5

	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, shards),
		mutableShardMask: shards - 1,
	}

	var keyA, keyB [8]byte
	for i := uint64(0); ; i++ {
		binary.BigEndian.PutUint64(keyA[:], i)
		if db.shardIndex(keyA[:]) == firstTarget {
			break
		}
	}
	for i := uint64(0); ; i++ {
		binary.BigEndian.PutUint64(keyB[:], i)
		if db.shardIndex(keyB[:]) == secondTarget {
			break
		}
	}
	wantKeyA := append([]byte(nil), keyA[:]...)
	wantKeyB := append([]byte(nil), keyB[:]...)

	tables := make([]*countingTable, shards)
	shardsSnap := make([]rootDomainSnapshot, shards)
	for shard := 0; shard < shards; shard++ {
		mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
		if err != nil {
			t.Fatalf("new memtable: %v", err)
		}
		ct := &countingTable{inner: mt}
		tables[shard] = ct
		shardsSnap[shard].immutables = []memtable.Table{ct}
	}
	tables[firstTarget].SetEntry(wantKeyA, []byte("va"), page.ValuePtr{}, node.FlagInline)
	tables[secondTarget].SetEntry(wantKeyB, []byte("vb"), page.ValuePtr{}, node.FlagInline)

	db.memtables.Store(&memtableView{
		rootPointShards: shardsSnap,
	})

	got, err := db.GetMany([][]byte{wantKeyA, wantKeyB})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 2 || string(got[0]) != "va" || string(got[1]) != "vb" {
		t.Fatalf("unexpected GetMany values: %#v", got)
	}
	for shard, ct := range tables {
		switch shard {
		case firstTarget, secondTarget:
			if ct.iterCalls == 0 {
				t.Fatalf("expected shard %d iterator calls > 0", shard)
			}
			if ct.getEntryCalls != 0 {
				t.Fatalf("expected shard %d GetEntry calls = 0, got %d", shard, ct.getEntryCalls)
			}
		default:
			if ct.iterCalls != 0 {
				t.Fatalf("expected shard %d iterator calls = 0, got %d", shard, ct.iterCalls)
			}
			if ct.getEntryCalls != 0 {
				t.Fatalf("expected shard %d GetEntry calls = 0, got %d", shard, ct.getEntryCalls)
			}
		}
	}
}

func TestGetMany_DedupesRepeatedKeysWithinShard(t *testing.T) {
	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	ct := &countingTable{inner: mt}
	key := []byte("k")
	ct.SetEntry(key, []byte("v"), page.ValuePtr{}, node.FlagInline)
	db.memtables.Store(&memtableView{
		rootPointShards: []rootDomainSnapshot{
			{immutables: []memtable.Table{ct}},
		},
	})

	got, err := db.GetMany([][]byte{key, key, key})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 3 || string(got[0]) != "v" || string(got[1]) != "v" || string(got[2]) != "v" {
		t.Fatalf("unexpected GetMany values: %#v", got)
	}
	if ct.iterCalls != 1 {
		t.Fatalf("expected one iterator probe, got %d", ct.iterCalls)
	}
	if ct.getEntryCalls != 0 {
		t.Fatalf("expected no GetEntry probes, got %d", ct.getEntryCalls)
	}
}

func TestGetMany_UsesPublishedRootPointShardsAsAuthority(t *testing.T) {
	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	ct := &countingTable{inner: mt}
	key := []byte("k")
	ct.SetEntry(key, []byte("root"), page.ValuePtr{}, node.FlagInline)

	db.memtables.Store(&memtableView{
		mutables: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "wrong-mutable"}),
		},
		queue: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "wrong-queue"}),
		},
		queueShardIDs: []uint16{0},
		rootPointShards: []rootDomainSnapshot{
			{immutables: []memtable.Table{ct}},
		},
	})

	got, err := db.GetMany([][]byte{key})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 1 || string(got[0]) != "root" {
		t.Fatalf("unexpected GetMany value: %#v", got)
	}
	if ct.iterCalls == 0 {
		t.Fatal("expected published root-point shard iterator to be consulted")
	}
	if ct.getEntryCalls != 0 {
		t.Fatalf("expected no GetEntry probes, got %d", ct.getEntryCalls)
	}
}

func TestGetMany_DuplicateHitsReuseSingleProbeWithoutResultAliasing(t *testing.T) {
	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	ct := &countingTable{inner: mt}
	key := []byte("k")
	ct.SetEntry(key, []byte("value"), page.ValuePtr{}, node.FlagInline)
	db.memtables.Store(&memtableView{
		rootPointShards: []rootDomainSnapshot{
			{immutables: []memtable.Table{ct}},
		},
	})

	got, err := db.GetMany([][]byte{key, key, key})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 3 || string(got[0]) != "value" || string(got[1]) != "value" || string(got[2]) != "value" {
		t.Fatalf("unexpected GetMany values: %#v", got)
	}
	if ct.iterCalls != 1 {
		t.Fatalf("expected one iterator probe, got %d", ct.iterCalls)
	}
	got[0][0] = 'X'
	if string(got[1]) != "value" || string(got[2]) != "value" {
		t.Fatalf("duplicate outputs must not alias: %#v", got)
	}
}

func TestGetMany_UsesPointProbesWhenTablePrefersSparseSortedRefs(t *testing.T) {
	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	ct := &countingTable{inner: mt}
	pt := &pointPreferredCountingTable{countingTable: ct}
	for _, kv := range []struct{ key, value string }{
		{"a", "va"}, {"m", "vm"}, {"z", "vz"},
	} {
		pt.SetEntry([]byte(kv.key), []byte(kv.value), page.ValuePtr{}, node.FlagInline)
	}
	db.memtables.Store(&memtableView{
		rootPointShards: []rootDomainSnapshot{
			{immutables: []memtable.Table{pt}},
		},
	})

	got, err := db.GetMany([][]byte{[]byte("z"), []byte("a"), []byte("m")})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 3 || string(got[0]) != "vz" || string(got[1]) != "va" || string(got[2]) != "vm" {
		t.Fatalf("unexpected GetMany values: %#v", got)
	}
	if ct.iterCalls != 0 {
		t.Fatalf("expected no iterator probes, got %d", ct.iterCalls)
	}
	if ct.getEntryCalls != 3 {
		t.Fatalf("expected one point probe per unique key, got %d", ct.getEntryCalls)
	}
}

func TestGetMany_DuplicateMissesCollapseToSingleBackendKey(t *testing.T) {
	backend := &countingBackend{}
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}

	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	mt.Set([]byte("other"), []byte("v"))
	db.memtables.Store(&memtableView{
		rootPointShards: []rootDomainSnapshot{
			{immutables: []memtable.Table{mt}},
		},
	})

	key := []byte("missing")
	got, err := db.GetMany([][]byte{key, key, key})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 3 || string(got[0]) != "backend" || string(got[1]) != "backend" || string(got[2]) != "backend" {
		t.Fatalf("unexpected GetMany values: %#v", got)
	}
	if backend.getManyCalls != 1 {
		t.Fatalf("expected one backend GetMany call, got %d", backend.getManyCalls)
	}
	if len(backend.lastGetMany) != 1 || string(backend.lastGetMany[0]) != "missing" {
		t.Fatalf("expected one deduped backend key, got %#v", backend.lastGetMany)
	}
}

func BenchmarkGetMany_PublishedRootPointShards(b *testing.B) {
	cases := []struct {
		name string
		keys [][]byte
	}{
		{
			name: "distinct8",
			keys: [][]byte{
				[]byte("k0"), []byte("k1"), []byte("k2"), []byte("k3"),
				[]byte("k4"), []byte("k5"), []byte("k6"), []byte("k7"),
			},
		},
		{
			name: "duplicate64",
			keys: func() [][]byte {
				keys := make([][]byte, 64)
				for i := range keys {
					keys[i] = []byte("k0")
				}
				return keys
			}(),
		},
		{
			name: "duplicate256_hit",
			keys: func() [][]byte {
				keys := make([][]byte, 256)
				for i := range keys {
					keys[i] = []byte("k0")
				}
				return keys
			}(),
		},
		{
			name: "duplicate256_miss",
			keys: func() [][]byte {
				keys := make([][]byte, 256)
				for i := range keys {
					keys[i] = []byte("missing")
				}
				return keys
			}(),
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var backend BackendDB = &panicBackend{}
			if tc.name == "duplicate256_miss" {
				backend = &countingBackend{}
			}
			db := &DB{
				backend:          backend,
				mutableShards:    make([]memShard, 1),
				mutableShardMask: 0,
			}

			mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
			if err != nil {
				b.Fatalf("new memtable: %v", err)
			}
			ct := &countingTable{inner: mt}
			for i := 0; i < 8; i++ {
				key := []byte(fmt.Sprintf("k%d", i))
				ct.SetEntry(key, []byte("v"), page.ValuePtr{}, node.FlagInline)
			}
			db.memtables.Store(&memtableView{
				rootPointShards: []rootDomainSnapshot{
					{immutables: []memtable.Table{ct}},
				},
			})

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := db.GetMany(tc.keys)
				if err != nil {
					b.Fatalf("GetMany: %v", err)
				}
				if len(got) != len(tc.keys) {
					b.Fatalf("len(GetMany)=%d want %d", len(got), len(tc.keys))
				}
			}
		})
	}
}
