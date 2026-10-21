package caching

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type mvccAdvanceRecord struct {
	key, value []byte
	deleted    bool
}

type mvccAdvanceIterator struct {
	records                     []mvccAdvanceRecord
	index, nexts, seeks, values int
	nextErr, seekErr, err       error
	beforeNext                  func()
}

func (it *mvccAdvanceIterator) Valid() bool { return it.err == nil && it.index < len(it.records) }
func (it *mvccAdvanceIterator) Next() {
	if it.beforeNext != nil {
		it.beforeNext()
	}
	it.nexts++
	if it.nextErr != nil {
		it.err = it.nextErr
		return
	}
	it.index++
}
func (it *mvccAdvanceIterator) Seek(key []byte) {
	it.seeks++
	if it.seekErr != nil {
		it.err = it.seekErr
		return
	}
	it.index = sort.Search(len(it.records), func(i int) bool { return bytes.Compare(it.records[i].key, key) >= 0 })
}
func (it *mvccAdvanceIterator) Key() []byte                 { return it.records[it.index].key }
func (it *mvccAdvanceIterator) Value() []byte               { it.values++; return it.records[it.index].value }
func (it *mvccAdvanceIterator) KeyCopy(dst []byte) []byte   { return append(dst[:0], it.Key()...) }
func (it *mvccAdvanceIterator) ValueCopy(dst []byte) []byte { return append(dst[:0], it.Value()...) }
func (it *mvccAdvanceIterator) Error() error                { return it.err }
func (it *mvccAdvanceIterator) Close() error                { return nil }
func (it *mvccAdvanceIterator) Domain() ([]byte, []byte)    { return nil, nil }
func (it *mvccAdvanceIterator) UnsafeKey() []byte           { return it.Key() }
func (it *mvccAdvanceIterator) UnsafeValue() []byte         { return it.Value() }
func (it *mvccAdvanceIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	flags := byte(node.FlagInline)
	if it.IsDeleted() {
		flags = node.FlagTombstone
	}
	return it.records[it.index].value, page.ValuePtr{}, flags
}
func (it *mvccAdvanceIterator) IsDeleted() bool { return it.records[it.index].deleted }

func mvccAdvanceKey(t *testing.T, logical string, ts uint64) []byte {
	t.Helper()
	key, err := mvcckey.Encode([]byte(logical), ts)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func mvccAdvanceUpper(t *testing.T, logical string) []byte {
	t.Helper()
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, []byte(logical))
	if err != nil {
		t.Fatal(err)
	}
	return upper
}

func TestSnapshotIteratorAdvanceMVCCKeyBounded(t *testing.T) {
	for _, depth := range []int{1, 4, 8, 9, 40} {
		for _, limit := range []int{-1, 0, 1, 8, 100} {
			t.Run(fmt.Sprintf("depth%d_limit%d", depth, limit), func(t *testing.T) {
				raw := &mvccAdvanceIterator{}
				for ts := depth; ts > 0; ts-- {
					raw.records = append(raw.records, mvccAdvanceRecord{key: mvccAdvanceKey(t, "a", uint64(ts))})
				}
				want := mvccAdvanceKey(t, "b", 1)
				raw.records = append(raw.records, mvccAdvanceRecord{key: want})
				owner := &Snapshot{}
				bound := &snapshotBoundIterator{cow: true, owner: owner, inner: raw}
				// Every inner step remains inside the same single active read.
				raw.beforeNext = func() {
					if state := owner.readState.Load(); state != 1 {
						t.Errorf("inner Next active reads=%d want1", state)
					}
				}
				visited, skipped, keyErr, storageErr := bound.AdvanceMVCCKey(mvccAdvanceUpper(t, "a"), limit)
				steps := max(0, min(8, limit))
				wantNext := min(depth, steps)
				wantSkipped := min(depth-1, steps)
				wantSeek := 0
				if depth > steps {
					wantSeek = 1
				}
				if keyErr != nil || storageErr != nil || visited != uint64(wantSkipped) || skipped != visited || raw.nexts != wantNext || raw.seeks != wantSeek || raw.values != 0 || !bytes.Equal(raw.Key(), want) {
					t.Fatalf("counts=%d/%d next/seek/value=%d/%d/%d key=%x errors=%v/%v", visited, skipped, raw.nexts, raw.seeks, raw.values, raw.Key(), keyErr, storageErr)
				}
				if owner.readState.Load() != 0 {
					t.Fatal("active read not released")
				}
			})
		}
	}
}

func TestSnapshotIteratorAdvanceMVCCKeyPreciseErrorsAndClosed(t *testing.T) {
	valid := mvccAdvanceKey(t, "a", 9)
	for _, malformed := range [][]byte{
		append(append([]byte(nil), valid[:len(valid)-8]...), 0x80),
		append(append([]byte(nil), valid[:len(valid)-8]...), bytes.Repeat([]byte{0xff}, 8)...),
		append(append([]byte(nil), valid[:len(valid)-8]...), bytes.Repeat([]byte{0xff}, mvcckey.MaxEncodedKeySize)...),
	} {
		_, _, wantErr := mvcckey.DecodeAppend(nil, malformed)
		raw := &mvccAdvanceIterator{records: []mvccAdvanceRecord{{key: valid}, {key: malformed}}}
		bound := &snapshotBoundIterator{inner: raw}
		visited, skipped, keyErr, storageErr := bound.AdvanceMVCCKey(mvccAdvanceUpper(t, "a"), 8)
		if visited != 1 || skipped != 0 || !errors.Is(keyErr, wantErr) || storageErr != nil || raw.values != 0 || raw.seeks != 0 {
			t.Fatalf("malformed result=%d/%d %v/%v want %v", visited, skipped, keyErr, storageErr, wantErr)
		}
	}
	for _, failSeek := range []bool{false, true} {
		failure := errors.New("storage failed")
		raw := &mvccAdvanceIterator{records: []mvccAdvanceRecord{{key: valid}, {key: mvccAdvanceKey(t, "a", 8)}}}
		limit := 8
		if failSeek {
			raw.seekErr, limit = failure, 0
		} else {
			raw.nextErr = failure
		}
		bound := &snapshotBoundIterator{inner: raw}
		_, _, keyErr, storageErr := bound.AdvanceMVCCKey(mvccAdvanceUpper(t, "a"), limit)
		if keyErr != nil || !errors.Is(storageErr, failure) || raw.values != 0 {
			t.Fatalf("storage result %v/%v", keyErr, storageErr)
		}
	}
	for _, ownerClosed := range []bool{false, true} {
		raw := &mvccAdvanceIterator{records: []mvccAdvanceRecord{{key: valid}}}
		bound := &snapshotBoundIterator{cow: true, owner: &Snapshot{}, inner: raw}
		if ownerClosed {
			bound.owner.readState.Store(snapshotReadClosedBit)
		} else {
			bound.closed.Store(true)
		}
		visited, skipped, keyErr, storageErr := bound.AdvanceMVCCKey(mvccAdvanceUpper(t, "a"), 8)
		if visited != 0 || skipped != 0 || keyErr != nil || !errors.Is(storageErr, backenddb.ErrClosed) || raw.nexts != 0 || raw.seeks != 0 {
			t.Fatal("closed iterator moved")
		}
	}
}

func TestSnapshotIteratorAdvanceMVCCKeyDoesNotAllocate(t *testing.T) {
	raw := &mvccAdvanceIterator{records: []mvccAdvanceRecord{
		{key: mvccAdvanceKey(t, "a\x00escaped", 9)},
		{key: mvccAdvanceKey(t, "a\x00escaped", 8)},
		{key: mvccAdvanceKey(t, "b", 1)},
	}}
	bound := &snapshotBoundIterator{cow: true, owner: &Snapshot{}, inner: raw}
	upper := mvccAdvanceUpper(t, "a\x00escaped")
	if allocations := testing.AllocsPerRun(100, func() {
		raw.index = 0
		visited, skipped, malformed, storage := bound.AdvanceMVCCKey(upper, 8)
		if visited != 1 || skipped != 1 || malformed != nil || storage != nil {
			panic("unexpected group advance")
		}
	}); allocations != 0 {
		t.Fatalf("group advance allocated %g objects", allocations)
	}
}

func TestSnapshotIteratorAdvanceMVCCKeyMergedTombstones(t *testing.T) {
	makeInner := func() merging.Iterator {
		newer := &mvccAdvanceIterator{records: []mvccAdvanceRecord{
			{key: mvccAdvanceKey(t, "a", 9)},
			{key: mvccAdvanceKey(t, "a", 7), deleted: true},
			{key: mvccAdvanceKey(t, "b", 4), deleted: true},
			{key: mvccAdvanceKey(t, "c", 1), value: []byte("new-c")},
		}}
		older := &mvccAdvanceIterator{records: []mvccAdvanceRecord{
			{key: mvccAdvanceKey(t, "a", 9), value: []byte("shadow")},
			{key: mvccAdvanceKey(t, "a", 7), value: []byte("erased")},
			{key: mvccAdvanceKey(t, "a", 3)},
			{key: mvccAdvanceKey(t, "b", 4), value: []byte("erased-b")},
			{key: mvccAdvanceKey(t, "b", 2), value: []byte("old-b")},
		}}
		return merging.NewMergingIterator([]merging.IteratorSource{{Iter: newer, Priority: 0}, {Iter: older, Priority: 1}}, nil, nil)
	}
	coalesced, ordinary := makeInner(), makeInner()
	defer coalesced.Close()
	defer ordinary.Close()
	upper := mvccAdvanceUpper(t, "a")
	var expected uint64
	for ordinary.Next(); ordinary.Valid() && bytes.Compare(ordinary.Key(), upper) < 0; ordinary.Next() {
		expected++
	}
	bound := &snapshotBoundIterator{inner: coalesced}
	visited, skipped, keyErr, storageErr := bound.AdvanceMVCCKey(upper, 8)
	if keyErr != nil || storageErr != nil || visited != expected || skipped != expected || !bytes.Equal(coalesced.Key(), ordinary.Key()) || !bytes.Equal(coalesced.Value(), []byte("old-b")) {
		t.Fatalf("merged result=%d/%d key=%x errors=%v/%v", visited, skipped, coalesced.Key(), keyErr, storageErr)
	}
}

type mvccAdvancePausedIterator struct {
	merging.Iterator
	entered, release chan struct{}
	readState        *Snapshot
	once             sync.Once
}

func (it *mvccAdvancePausedIterator) Next() {
	it.once.Do(func() { close(it.entered); <-it.release })
	if state := it.readState.readState.Load(); state != snapshotReadClosedBit|1 {
		panic(fmt.Sprintf("paused read state=%x", state))
	}
	it.Iterator.Next()
}

func TestSnapshotIteratorAdvanceMVCCKeyPinsClose(t *testing.T) {
	db, authority := cowOrderedLifecycleFixture(t)
	cowOrderedLifecycleSet(t, db, authority, mvccAdvanceKey(t, "a", 9), []byte("a"))
	cowOrderedLifecycleSet(t, db, authority, mvccAdvanceKey(t, "a", 8), []byte("older-a"))
	cowOrderedLifecycleSet(t, db, authority, mvccAdvanceKey(t, "a", 7), []byte("oldest-a"))
	cowOrderedLifecycleSet(t, db, authority, mvccAdvanceKey(t, "b", 1), []byte("b"))
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
	paused := &mvccAdvancePausedIterator{Iterator: bound.inner, entered: make(chan struct{}), release: make(chan struct{}), readState: snapshot}
	bound.inner = paused
	type result struct {
		visited, skipped   uint64
		malformed, storage error
	}
	done := make(chan result, 1)
	upper := mvccAdvanceUpper(t, "a")
	go func() { v, s, m, e := bound.AdvanceMVCCKey(upper, 8); done <- result{v, s, m, e} }()
	var releaseOnce sync.Once
	unpause := func() { releaseOnce.Do(func() { close(paused.release) }) }
	defer unpause()
	<-paused.entered
	if snapshot.readState.Load() != 1 {
		t.Fatal("operation did not own exactly one active read")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot.finalized.Load() || snapshot.cowCut == nil {
		t.Fatal("Close reclaimed active source")
	}
	unpause()
	got := <-done
	if got.visited != 2 || got.skipped != 2 || got.malformed != nil || got.storage != nil {
		t.Fatalf("admitted result=%+v", got)
	}
	if bound.Valid() {
		t.Fatal("closed owner left iterator valid")
	}
	if err := bound.Close(); err != nil {
		t.Fatal(err)
	}
	// The consumed snapshot wrapper is cleared at finalization. Observe its
	// admitted owner ledger rather than inspecting that detached handle.
	if views := db.cow.budget.Stats().Views; views != 0 {
		t.Fatalf("last iterator left %d admitted views", views)
	}
}
