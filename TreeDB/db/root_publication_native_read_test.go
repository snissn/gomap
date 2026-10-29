package db

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"testing"
)

// The external leaf is created AFTER the captured Snapshot. A source-only
// reader bound to that Snapshot's old file set cannot supply this positive.
func TestNativeConstructionRootReadCustody(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	writer := newRewriteWriter(ValueLogDirPath(database.dir), 0, 0, 4096)
	writer.ConfigureLeafLog(LeafLogDirPath(database.dir), rewriteLeafLogLaneID, 0)
	database.SetLeafPageLog(writer)
	database.SetCurrentValueLogReadBarrier(func(uint32) error { return writer.Flush() })
	defer database.SetCurrentValueLogReadBarrier(nil)
	defer writer.Close()
	basis := database.AcquireSnapshot()
	defer basis.Close()
	group, err := database.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	build := func(base uint64, value string) uint64 {
		delta := batch.New(nil, 64)
		defer delta.Close()
		if e := delta.Set([]byte("key"), []byte(value)); e != nil {
			t.Fatal(e)
		}
		root, e := group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: base, Delta: delta})
		if e != nil {
			t.Fatal(e)
		}
		return root
	}
	first := build(0, "first")
	segments, err := leafPageLogCreatedSegments(database.leafPageLog)
	if err != nil || len(segments) == 0 {
		t.Fatalf("new leaf segment: %+v %v", segments, err)
	}
	for _, seg := range segments {
		if _, exists := basis.state.ValueLogSet.Files[seg.FileID]; exists {
			t.Fatal("new segment was already in captured basis")
		}
	}
	sentinel := errors.New("callback refusal")
	err = group.WithNativeConstructionRoots([]uint64{first}, func(readers []iterator.UnsafeIterator) error {
		it := readers[0]
		if !it.Valid() || !bytes.Equal(it.UnsafeKey(), []byte("key")) || !bytes.Equal(it.UnsafeValue(), []byte("first")) {
			t.Fatal("private current root mismatch")
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("callback error: %v", err)
	}
	second := build(first, "second")
	called := false
	for _, root := range []uint64{first, ^uint64(0)} {
		if e := group.WithNativeConstructionRoots([]uint64{root}, func([]iterator.UnsafeIterator) error { called = true; return nil }); !errors.Is(e, ErrRootPublicationBasisMismatch) {
			t.Fatalf("foreign/superseded root: %v", e)
		}
	}
	if called {
		t.Fatal("unauthenticated root reached callback")
	}
	if e := group.WithNativeConstructionRoots([]uint64{second, 0}, func(it []iterator.UnsafeIterator) error {
		if it[1] != nil || !it[0].Valid() || string(it[0].UnsafeValue()) != "second" {
			t.Fatal("replacement/empty read")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	// Overflow evicts read custody only. Every original build still succeeds.
	var newest uint64
	for i := 0; i < 4; i++ {
		newest = build(0, "window")
	}
	if e := group.WithNativeConstructionRoots([]uint64{second}, func([]iterator.UnsafeIterator) error { t.Fatal("evicted root callback"); return nil }); !errors.Is(e, ErrRootPublicationBasisMismatch) {
		t.Fatalf("evicted root: %v", e)
	}
	if e := group.WithNativeConstructionRoots([]uint64{newest}, func(it []iterator.UnsafeIterator) error {
		if !it[0].Valid() || string(it[0].UnsafeValue()) != "window" {
			t.Fatal("newest current root")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if err = group.Close(); err != nil {
		t.Fatal(err)
	}
	if err = group.WithNativeConstructionRoots(nil, func([]iterator.UnsafeIterator) error { t.Fatal("closed group callback"); return nil }); !errors.Is(err, ErrNativeSemanticUnsupported) {
		t.Fatalf("closed group: %v", err)
	}
	if err = basis.Close(); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
}
