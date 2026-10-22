package caching

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWForwardSeekPointerCacheAndRetainedCut(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	a, b := mvccAdvanceKey(t, "a\x00", 9), mvccAdvanceKey(t, "b", 8)
	av, bv := bytes.Repeat([]byte("old-a"), 200), bytes.Repeat([]byte("old-b"), 200)
	cowOrderedLifecycleSet(t, db, owner, a, av)
	cowOrderedLifecycleSet(t, db, owner, b, bv)
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	public, err := snapshot.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	bound := public.(*snapshotBoundIterator)
	inner := bound.inner.(*cowMergeIterator)
	bound.Seek(a) // Establish common child alignment, rather than constructor position.
	if !bound.Valid() || !bytes.Equal(bound.Value(), av) || !inner.cached {
		t.Fatal("initial pointer did not materialize into iterator cache")
	}
	workspace := inner.workspace
	_, ap, af, ar := inner.Iterator.(iterator.RevisionUnsafeIterator).UnsafeEntryWithRevision()
	if af&node.FlagPointer == 0 {
		t.Fatal("fixture did not activate pointer materialization")
	}
	cowOrderedLifecycleSet(t, db, owner, b, []byte("new-b"))
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	bound.SeekForward(b)
	if inner.cached || inner.value != nil || !bound.Valid() || !bytes.Equal(bound.Key(), b) {
		t.Fatal("forward movement retained the previous value cache")
	}
	if got := bound.Value(); !bytes.Equal(got, bv) || inner.workspace != workspace {
		t.Fatal("retained cut lost old pointer value or replaced its admitted workspace")
	}
	bound.SeekForward(a) // Backward request must retain the full Seek contract.
	_, again, flags, revision := inner.Iterator.(iterator.RevisionUnsafeIterator).UnsafeEntryWithRevision()
	if inner.cached || again != ap || flags != af || revision != ar || !bytes.Equal(bound.Value(), av) {
		t.Fatal("fallback changed retained native identity or left stale cache")
	}
	bound.SeekForward([]byte{255})
	if bound.Valid() || inner.cached || inner.value != nil || bound.Error() != nil {
		t.Fatal("exhausted movement did not clear cache")
	}
	bound.SeekForward(b) // Exhaustion is a full Seek fallback and can reposition.
	if !bound.Valid() || !bytes.Equal(bound.Value(), bv) {
		t.Fatal("exhausted iterator did not recover through original Seek")
	}
}

func TestCOWForwardSeekMixedBasisRawMaskNativeParity(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	zero, a3, a2, b := mvccAdvanceKey(t, "0", 1), mvccAdvanceKey(t, "a\x00", 3), mvccAdvanceKey(t, "a\x00", 2), mvccAdvanceKey(t, "b", 1)
	for _, key := range [][]byte{zero, a3, a2, b} {
		cowOrderedLifecycleSet(t, db, owner, key, append([]byte("disk-"), key...))
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(a3); err != nil {
		t.Fatal(err)
	} // A physical mask over the durable version.
	cowOrderedLifecycleSet(t, db, owner, b, []byte("overlay-b"))
	if err := db.Set([]byte{255}, []byte("raw")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	fast, err := snapshot.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()
	full, err := snapshot.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	fast.Seek(zero)
	full.Seek(zero)
	for _, target := range [][]byte{a3, a2, b, {255}, {255, 0}, a3, {}} {
		fast.(*snapshotBoundIterator).SeekForward(target)
		full.Seek(target)
		if fast.Valid() != full.Valid() || fast.Error() != full.Error() {
			t.Fatal("mixed source state differs")
		}
		if !fast.Valid() {
			continue
		}
		if !bytes.Equal(fast.Key(), full.Key()) || !bytes.Equal(fast.Value(), full.Value()) {
			t.Fatalf("mixed source record differs at %x", target)
		}
		fi := fast.(*snapshotBoundIterator).inner.(*cowMergeIterator).Iterator.(iterator.RevisionUnsafeIterator)
		bi := full.(*snapshotBoundIterator).inner.(*cowMergeIterator).Iterator.(iterator.RevisionUnsafeIterator)
		fv, fp, ff, fr := fi.UnsafeEntryWithRevision()
		bv, bp, bf, br := bi.UnsafeEntryWithRevision()
		if !bytes.Equal(fv, bv) || fp != bp || ff != bf || fr != br {
			t.Fatal("mixed source native identity differs")
		}
		if bytes.Equal(fast.Key(), a3) {
			t.Fatal("physical mask resurrected durable version")
		}
	}
}

func TestCOWForwardSeekHeadClosedErrorAndWarmAllocation(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	for ts := uint64(12); ts > 0; ts-- {
		cowOrderedLifecycleSet(t, db, owner, mvccAdvanceKey(t, "same", ts), []byte{byte(ts)})
	}
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	key := mvccAdvanceKey(t, "same", 6)
	source := snapshot.cowCut.shards[db.shardIndex(key)].NewIterator(nil, nil).(*cowIterator)
	defer source.Close()
	head, valid, err := source.SeekForwardHead(key)
	if err != nil || !valid || !bytes.Equal(head, key) {
		t.Fatalf("lagging head=%x valid=%v err=%v", head, valid, err)
	}
	before := db.cow.budget.Stats()
	if n := testing.AllocsPerRun(100, func() {
		_, _, _ = source.SeekForwardHead(key)
	}); n != 0 {
		t.Fatalf("retained head allocations=%v", n)
	}
	if after := db.cow.budget.Stats(); after.TotalBytes != before.TotalBytes || after.Views != before.Views {
		t.Fatal("retained head acquired another owner or admission")
	}
	failure := errors.New("child sticky failure")
	source.mu.Lock()
	source.err = failure
	source.mu.Unlock()
	if _, valid, err := source.SeekForwardHead(key); valid || !errors.Is(err, failure) {
		t.Fatalf("child error hidden: valid=%v err=%v", valid, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if _, valid, _ := source.SeekForwardHead(key); valid {
		t.Fatal("closed child returned an unpinned key")
	}
}

type cowForwardPausedIterator struct {
	merging.Iterator
	entered, release chan struct{}
	owner            *Snapshot
	once             sync.Once
}

func (it *cowForwardPausedIterator) SeekForward(key []byte) {
	it.once.Do(func() { close(it.entered); <-it.release })
	if state := it.owner.readState.Load(); state != snapshotReadClosedBit|1 {
		panic(fmt.Sprintf("forward read state=%x", state))
	}
	seekForwardIterator(it.Iterator, key)
}

func TestCOWForwardSeekPinsConcurrentClose(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	a, b := mvccAdvanceKey(t, "a", 1), mvccAdvanceKey(t, "b", 1)
	cowOrderedLifecycleSet(t, db, owner, a, []byte("a"))
	cowOrderedLifecycleSet(t, db, owner, b, []byte("b"))
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	public, err := snapshot.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	bound := public.(*snapshotBoundIterator)
	bound.Seek(a)
	paused := &cowForwardPausedIterator{Iterator: bound.inner, owner: snapshot, entered: make(chan struct{}), release: make(chan struct{})}
	bound.inner = paused
	done := make(chan struct{})
	go func() { bound.SeekForward(b); close(done) }()
	var once sync.Once
	unpause := func() { once.Do(func() { close(paused.release) }) }
	defer unpause()
	<-paused.entered
	if snapshot.readState.Load() != 1 {
		t.Fatal("forward movement did not hold one original read")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot.finalized.Load() || snapshot.cowCut == nil {
		t.Fatal("Close reclaimed active forward source")
	}
	unpause()
	<-done
	if bound.Valid() || !errors.Is(bound.Error(), backenddb.ErrClosed) {
		t.Fatal("closed owner remained readable")
	}
	if err := bound.Close(); err != nil {
		t.Fatal(err)
	}
	if views := db.cow.budget.Stats().Views; views != 0 {
		t.Fatalf("last forward operation left %d views", views)
	}
}

type cowForwardAdvanceIterator struct {
	*mvccAdvanceIterator
	forwards int
}

func (it *cowForwardAdvanceIterator) SeekForward(key []byte) { it.forwards++; it.Seek(key) }

func TestCOWForwardSeekAdvanceFallbackKeepsOriginalCounts(t *testing.T) {
	for _, cow := range []bool{false, true} {
		raw := &cowForwardAdvanceIterator{mvccAdvanceIterator: new(mvccAdvanceIterator)}
		for ts := uint64(64); ts > 0; ts-- {
			raw.records = append(raw.records, mvccAdvanceRecord{key: mvccAdvanceKey(t, "a", ts)})
		}
		want := mvccAdvanceKey(t, "b", 1)
		raw.records = append(raw.records, mvccAdvanceRecord{key: want})
		bound := &snapshotBoundIterator{cow: cow, owner: &Snapshot{}, inner: raw}
		v, s, m, e := bound.AdvanceMVCCKey(mvccAdvanceUpper(t, "a"), 8)
		wantForward := 0
		if cow {
			wantForward = 1
		}
		if v != 8 || s != 8 || m != nil || e != nil || raw.nexts != 8 || raw.seeks != 1 || raw.forwards != wantForward || raw.values != 0 || !bytes.Equal(raw.Key(), want) {
			t.Fatalf("cow=%v counts=%d/%d next/seek/forward/value=%d/%d/%d/%d errors=%v/%v", cow, v, s, raw.nexts, raw.seeks, raw.forwards, raw.values, m, e)
		}
	}
}
