package caching

import (
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestPointReads_UsePublishedRootDomainShardSnapshotsAsAuthority(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("k"), []byte("backend"))

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

	view := &memtableView{
		rootPointShards: []rootDomainSnapshot{
			{
				immutables: []memtable.Table{
					newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "root"}),
				},
			},
		},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "root" {
		t.Fatalf("Get value=%q want %q", string(got), "root")
	}

	ok, err := db.Has([]byte("k"))
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !ok {
		t.Fatal("expected Has to consult published root-domain shard snapshot")
	}
}

func TestIterator_UsesPublishedRootDomainIteratorAsAuthority(t *testing.T) {
	backend := NewMockBackend()

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

	view := &memtableView{
		rootIterator: rootDomainSnapshot{
			immutables: []memtable.Table{
				newRootDomainTestTable(t,
					rootDomainTestOp{key: "a", value: "va"},
					rootDomainTestOp{key: "b", value: "vb"},
				),
			},
		},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	var got []string
	for it.Valid() {
		got = append(got, string(it.Key()))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("keys=%v want [a b]", got)
	}
}

func TestReverseIterator_UsesPublishedRootDomainIteratorAsAuthority(t *testing.T) {
	backend := NewMockBackend()

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

	view := &memtableView{
		rootIterator: rootDomainSnapshot{
			immutables: []memtable.Table{
				newRootDomainTestTable(t,
					rootDomainTestOp{key: "a", value: "va"},
					rootDomainTestOp{key: "b", value: "vb"},
				),
			},
		},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	it, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("ReverseIterator: %v", err)
	}
	defer it.Close()

	var got []string
	for it.Valid() {
		got = append(got, string(it.Key()))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Fatalf("keys=%v want [b a]", got)
	}
}

func TestIterator_RawFallbackWithoutPublishedViewStillReadsQueuedState(t *testing.T) {
	db := &DB{
		backend:          panicBackend{},
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	db.backendRangeInit.Do(func() {})

	mt, err := newRootDomainTable(
		rootDomainTestOp{key: "a", value: "va"},
		rootDomainTestOp{key: "b", value: "vb"},
	)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}

	db.mu.Lock()
	db.queue = []memtable.Table{mt}
	db.queueShardIDs = []uint16{0}
	db.queueRanges = []keyRange{{valid: true, min: []byte("a"), max: []byte("b")}}
	db.backendRangeKnown = true
	db.mu.Unlock()

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	var got []string
	for it.Valid() {
		got = append(got, string(it.Key()))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("keys=%v want [a b]", got)
	}
}

func TestPointReads_IgnoreSyntheticViewStateWithoutPublishedRootDomains(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("k"), []byte("backend"))

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

	view := &memtableView{
		mutables: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "wrong-mutable"}),
		},
		queue: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "k", value: "wrong-queue"}),
		},
		queueShardIDs: []uint16{0},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "backend" {
		t.Fatalf("Get value=%q want %q", string(got), "backend")
	}

	ok, err := db.Has([]byte("k"))
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !ok {
		t.Fatal("expected Has to consult backend once synthetic view state is ignored")
	}
}

func TestIterator_IgnoreSyntheticViewStateWithoutPublishedRootDomains(t *testing.T) {
	backend := NewMockBackend()
	backend.Set([]byte("a"), []byte("backend-a"))

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

	view := &memtableView{
		mutables: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "m", value: "wrong-mutable"}),
		},
		queue: []memtable.Table{
			newRootDomainTestTable(t, rootDomainTestOp{key: "b", value: "wrong-queue"}),
		},
		queueShardIDs: []uint16{0},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	var got []string
	for it.Valid() {
		got = append(got, string(it.Key()))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("keys=%v want [a]", got)
	}

	rit, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("ReverseIterator: %v", err)
	}
	defer rit.Close()
	got = got[:0]
	for rit.Valid() {
		got = append(got, string(rit.Key()))
		rit.Next()
	}
	if err := rit.Error(); err != nil {
		t.Fatalf("reverse iterator error: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("reverse keys=%v want [a]", got)
	}
}

func TestIterator_UsesPublishedRootDomainIteratorRangesAsAuthority(t *testing.T) {
	backend := NewMockBackend()

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

	view := &memtableView{
		rootIterator: rootDomainSnapshot{
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "a", value: "va"}),
			},
		},
		rootIteratorRanges: []keyRange{
			{valid: true, min: []byte("a"), max: []byte("a")},
		},
		queueRanges: []keyRange{
			{valid: true, min: []byte("z"), max: []byte("z")},
		},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	it, err := db.Iterator([]byte("a"), []byte("b"))
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	var got []string
	for it.Valid() {
		got = append(got, string(it.Key()))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("keys=%v want [a]", got)
	}
}

func TestReverseIterator_UsesPublishedRootDomainIteratorRangesAsAuthority(t *testing.T) {
	backend := NewMockBackend()

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

	view := &memtableView{
		rootIterator: rootDomainSnapshot{
			immutables: []memtable.Table{
				newRootDomainTestTable(t, rootDomainTestOp{key: "a", value: "va"}),
			},
		},
		rootIteratorRanges: []keyRange{
			{valid: true, min: []byte("a"), max: []byte("a")},
		},
		queueRanges: []keyRange{
			{valid: true, min: []byte("z"), max: []byte("z")},
		},
	}
	view.refs.Store(1)
	db.memtables.Store(view)

	it, err := db.ReverseIterator([]byte("a"), []byte("b"))
	if err != nil {
		t.Fatalf("ReverseIterator: %v", err)
	}
	defer it.Close()

	var got []string
	for it.Valid() {
		got = append(got, string(it.Key()))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("reverse iterator error: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("keys=%v want [a]", got)
	}
}
