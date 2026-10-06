package caching

import (
	"errors"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWReadAdapterCursorOwnsOldRoot(t *testing.T) {
	b, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	w, err := memtable.NewCOWWriter(b)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { r := w.Close(); r.Drain() }()
	p, err := w.Prepare([]memtable.COWMutation{
		{Key: []byte("a"), Value: []byte("old"), Revision: page.EntryRevision(7)},
		{Key: []byte("b"), Flags: node.FlagTombstone, Revision: page.EntryRevision(8)},
	}, memtable.COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root := p.Publish()
	table := &cowTable{root: root, size: 5}
	it := table.NewIterator([]byte("a"), []byte("c"))
	defer it.Close()
	key, val, _, _, rev, ok := table.SeekGE([]byte("a"), []byte("b"))
	if !ok || string(key) != "a" || string(val) != "old" || rev != 7 {
		t.Fatalf("successor key=%q val=%q revision=%d found=%v", key, val, rev, ok)
	}
	p, err = w.Prepare([]memtable.COWMutation{{Key: []byte("a"), Value: []byte("new"), Revision: 9}}, memtable.COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	newRoot := p.Publish()
	defer func() { r := newRoot.Release(); r.Drain() }()
	r := root.Release()
	r.Drain()
	// The iterator owns the old generation after its creating cut releases it.
	if !it.Valid() || string(it.Key()) != "a" || string(it.Value()) != "old" {
		t.Fatalf("old cursor lost: key=%q value=%q error=%v", it.Key(), it.Value(), it.Error())
	}
	copied := it.ValueCopy(nil)
	it.Next()
	_, _, flags, rev := iterator.UnsafeEntryWithRevision(it)
	if !it.Valid() || string(it.Key()) != "b" || !it.IsDeleted() || flags != node.FlagTombstone || rev != 8 {
		t.Fatalf("tombstone lost: key=%q flags=%d revision=%d", it.Key(), flags, rev)
	}
	it.Seek([]byte("a"))
	if string(it.Value()) != "old" {
		t.Fatalf("seek observed newer root: %q", it.Value())
	}
	if err := it.Close(); err != nil || it.Valid() || string(copied) != "old" {
		t.Fatalf("close/copy contract: valid=%v copy=%q err=%v", it.Valid(), copied, err)
	}
}

func TestCOWReadAdapterRefusesReverseAndCallback(t *testing.T) {
	table := &cowTable{}
	called := false
	if err := table.PutWithCallback(nil, nil, func(_, _ []byte) error { called = true; return nil }); !errors.Is(err, ErrCOWUnsupported) || called {
		t.Fatalf("callback refusal: called=%v err=%v", called, err)
	}
	it := table.NewReverseIterator(nil, nil)
	if it.Valid() || !errors.Is(it.Error(), ErrCOWUnsupported) {
		t.Fatalf("reverse refusal: valid=%v err=%v", it.Valid(), it.Error())
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCOWReadAdapterMovementCloseRace(t *testing.T) {
	b, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	w, err := memtable.NewCOWWriter(b)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { r := w.Close(); r.Drain() }()
	p, err := w.Prepare([]memtable.COWMutation{{Key: []byte("a"), Value: []byte("value")}}, memtable.COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root := p.Publish()
	defer func() { r := root.Release(); r.Drain() }()
	it := (&cowTable{root: root}).NewIterator(nil, nil)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				it.Seek([]byte("a"))
				it.Next()
				it.Valid()
				it.KeyCopy(nil)
				it.ValueCopy(nil)
				it.Error()
			}
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); _ = it.Close() }()
	wg.Wait()
	if it.Valid() {
		t.Fatal("closed iterator became valid")
	}
}
