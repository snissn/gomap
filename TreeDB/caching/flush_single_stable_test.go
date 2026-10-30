package caching

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type oneStableRawRow struct {
	key, value []byte
	ptr        page.ValuePtr
	flags      byte
	revision   page.EntryRevision
}

// Unused methods deliberately retain a nil embedded interface: a new accessor
// would fail rather than silently escaping the trace. These views are stable.
type oneStableTraceIterator struct {
	iterator.UnsafeIterator
	rows              []oneStableRawRow
	trace             *[]string
	idx, closes       int
	iterErr, closeErr error
	actions           map[string]func()
}

func (it *oneStableTraceIterator) event(name string) {
	*it.trace = append(*it.trace, name)
	if action := it.actions[name]; action != nil {
		action()
	}
}
func (it *oneStableTraceIterator) Valid() bool       { it.event("Valid"); return it.idx < len(it.rows) }
func (it *oneStableTraceIterator) Next()             { it.event("Next"); it.idx++ }
func (it *oneStableTraceIterator) UnsafeKey() []byte { it.event("Key"); return it.rows[it.idx].key }
func (it *oneStableTraceIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	it.event("LegacyEntry")
	r := it.rows[it.idx]
	return r.value, r.ptr, r.flags
}
func (it *oneStableTraceIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	it.event("Entry")
	r := it.rows[it.idx]
	return r.value, r.ptr, r.flags, r.revision
}
func (it *oneStableTraceIterator) Error() error { it.event("Error"); return it.iterErr }
func (it *oneStableTraceIterator) Close() error { it.closes++; it.event("Close"); return it.closeErr }

// Embedding only UnsafeIterator masks the optional revision extension.
type oneStableLegacyIterator struct{ iterator.UnsafeIterator }

type oneStableTraceTable struct {
	memtable.Table
	it              *oneStableTraceIterator
	legacy, nilIter bool
	newAction       func()
}

func (tb *oneStableTraceTable) StableUnsafeIteratorSlices() bool { return true }
func (tb *oneStableTraceTable) NewIterator(_, _ []byte) iterator.UnsafeIterator {
	tb.it.event("New")
	if tb.newAction != nil {
		tb.newAction()
	}
	if tb.nilIter {
		return nil
	}
	tb.it.idx = 0 // Each count/merge traversal starts from a fresh iterator position.
	if tb.legacy {
		return &oneStableLegacyIterator{tb.it}
	}
	return tb.it
}

type oneStableMergeFunc func([]flushUnit, *canonicalFlushRun, func(batch.Entry) error) error

func oneStableSeedRun() canonicalFlushRun {
	return canonicalFlushRun{sourceMemtables: 11, sourcePointOps: 12,
		plannedPointOps: 13, shadowedPointOps: 14, deletePointOps: 15,
		rangeDeleteOps: 16, rangeBarriers: 17, laneBarriers: 18}
}

func TestSingleStableMergeRawShapesAndTrace(t *testing.T) {
	ptr := page.ValuePtr{FileID: 9, Offset: 42, Length: 7}
	rows := []oneStableRawRow{
		{key: []byte("a"), flags: node.FlagInline, revision: 1},
		{key: []byte("b"), value: []byte{}, flags: node.FlagInline, revision: 2},
		{key: []byte("dup"), value: []byte("first"), flags: 0x80, revision: 3},
		{key: []byte("dup"), value: []byte("ignored"), ptr: ptr, flags: node.FlagTombstone | node.FlagPointer | 0x80, revision: 4},
		{key: []byte("p"), value: []byte("ignored"), ptr: ptr, flags: node.FlagPointer | node.FlagInline | 0x80, revision: 5},
	}
	want := []batch.Entry{
		{Type: batch.OpPut, Key: rows[0].key, Revision: 1},
		{Type: batch.OpPut, Key: rows[1].key, Value: []byte{}, Revision: 2},
		{Type: batch.OpPut, Key: rows[2].key, Value: []byte("first"), Revision: 3},
		{Type: batch.OpDelete, Key: rows[3].key, Revision: 4},
		{Type: batch.OpPut, Key: rows[4].key, ValuePtr: ptr, IsPtr: true, Revision: 5},
	}
	for _, legacy := range []bool{false, true} {
		for _, count := range []int{0, 1, len(rows)} {
			for _, noEmit := range []bool{false, true} {
				name := fmt.Sprintf("legacy=%v/rows=%d/noEmit=%v", legacy, count, noEmit)
				t.Run(name, func(t *testing.T) {
					var results [2]canonicalFlushRun
					var traces [2][]string
					var outputs [2][]batch.Entry
					for n, merge := range []oneStableMergeFunc{oneStableLegacyReference, mergeCanonicalStableIteratorUnits} {
						it := &oneStableTraceIterator{rows: rows[:count], trace: &traces[n]}
						tb := &oneStableTraceTable{it: it, legacy: legacy}
						results[n] = oneStableSeedRun()
						var emit func(batch.Entry) error
						if !noEmit {
							emit = func(e batch.Entry) error { it.event("Emit"); outputs[n] = append(outputs[n], e); return nil }
						}
						if err := merge([]flushUnit{{mem: tb}}, &results[n], emit); err != nil {
							t.Fatal(err)
						}
						if it.closes != 1 {
							t.Fatalf("closes=%d", it.closes)
						}
					}
					if !reflect.DeepEqual(traces[0], traces[1]) || !reflect.DeepEqual(results[0], results[1]) || !reflect.DeepEqual(outputs[0], outputs[1]) {
						t.Fatalf("reference mismatch: traces=%v / %v stats=%+v / %+v outputs=%+v / %+v", traces[0], traces[1], results[0], results[1], outputs[0], outputs[1])
					}
					expected := append([]batch.Entry(nil), want[:count]...)
					if legacy {
						for i := range expected {
							expected[i].Revision = page.LegacyEntryRevision
						}
					}
					if !noEmit && !reflect.DeepEqual(outputs[1], expected) {
						t.Fatalf("entries=%+v want=%+v", outputs[1], expected)
					}
					seed := oneStableSeedRun()
					seed.plannedPointOps = count
					seed.deletePointOps = 0
					if count == len(rows) {
						seed.deletePointOps = 1
					}
					if !reflect.DeepEqual(results[1], seed) {
						t.Fatalf("stats=%+v want=%+v", results[1], seed)
					}
					entryEvent := "Entry"
					if legacy {
						entryEvent = "LegacyEntry"
					}
					trace := []string{"New", "Valid"}
					for i := 0; i < count; i++ {
						trace = append(trace, "Valid", "Key", "Valid", entryEvent, "Key")
						if !noEmit {
							trace = append(trace, "Emit")
						}
						trace = append(trace, "Next", "Valid")
					}
					trace = append(trace, "Error", "Close")
					if !reflect.DeepEqual(traces[1], trace) {
						t.Fatalf("trace=%v want=%v", traces[1], trace)
					}
				})
			}
		}
	}
}

func TestSingleStableMergeErrorPriorityAndAdmission(t *testing.T) {
	emitErr, iterErr, closeErr := errors.New("emit"), errors.New("iterator"), errors.New("close")
	for _, stop := range []int{-1, 0, 1} {
		for _, failIter := range []bool{false, true} {
			for _, failClose := range []bool{false, true} {
				t.Run(fmt.Sprintf("emit=%d/iter=%v/close=%v", stop, failIter, failClose), func(t *testing.T) {
					for _, merge := range []oneStableMergeFunc{oneStableLegacyReference, mergeCanonicalStableIteratorUnits} {
						var trace []string
						it := &oneStableTraceIterator{rows: []oneStableRawRow{{key: []byte("a")}, {key: []byte("b")}}, trace: &trace}
						if failIter {
							it.iterErr = iterErr
						}
						if failClose {
							it.closeErr = closeErr
						}
						run := oneStableSeedRun()
						calls := 0
						err := merge([]flushUnit{{mem: &oneStableTraceTable{it: it}}}, &run, func(batch.Entry) error {
							it.event("Emit")
							current := calls
							calls++
							if current == stop {
								return emitErr
							}
							return nil
						})
						want := error(nil)
						if stop >= 0 {
							want = emitErr
						} else if failIter {
							want = iterErr
						} else if failClose {
							want = closeErr
						}
						if err != want || it.closes != 1 {
							t.Fatalf("err=%v want=%v closes=%d", err, want, it.closes)
						}
						seed := oneStableSeedRun()
						if stop < 0 {
							seed.plannedPointOps = 2
							seed.deletePointOps = 0
						}
						if !reflect.DeepEqual(run, seed) {
							t.Fatalf("run=%+v want=%+v", run, seed)
						}
						wantNext := 2
						if stop >= 0 {
							wantNext = stop
						}
						if it.idx != wantNext || !reflect.DeepEqual(trace[len(trace)-2:], []string{"Error", "Close"}) {
							t.Fatalf("idx=%d trace=%v", it.idx, trace)
						}
					}
				})
			}
		}
	}
	for _, merge := range []oneStableMergeFunc{oneStableLegacyReference, mergeCanonicalStableIteratorUnits} {
		var trace []string
		it := &oneStableTraceIterator{trace: &trace}
		tb := &oneStableTraceTable{it: it, nilIter: true}
		if err := merge([]flushUnit{{mem: tb}}, nil, nil); err == nil || err.Error() != "cachingdb: missing canonical flush run" || len(trace) != 0 {
			t.Fatalf("nil out err=%v trace=%v", err, trace)
		}
		if err := merge([]flushUnit{{mem: tb}}, &canonicalFlushRun{}, nil); err == nil || err.Error() != "cachingdb: nil canonical stable iterator" || !reflect.DeepEqual(trace, []string{"New"}) {
			t.Fatalf("nil iterator err=%v trace=%v", err, trace)
		}
		trace = nil
		it.iterErr = iterErr
		it.closeErr = closeErr
		tb.nilIter = false
		run := oneStableSeedRun()
		if err := merge([]flushUnit{{mem: tb}}, &run, nil); err != iterErr || it.closes != 1 || run.plannedPointOps != 0 || run.deletePointOps != 0 {
			t.Fatalf("empty err=%v closes=%d run=%+v", err, it.closes, run)
		}
	}
}

// Each invocation owns a goroutine; Goexit never kills the test or flush worker.
func oneStableUnwind(merge oneStableMergeFunc, faultAt, faultKind, cleanupAt, cleanupKind string) (trace []string, run canonicalFlushRun, returned bool, recovered any, closes, index int) {
	it := &oneStableTraceIterator{rows: []oneStableRawRow{{key: []byte("a")}, {key: []byte("b")}}, trace: &trace, actions: make(map[string]func())}
	fault := func(kind, label string) func() {
		return func() {
			switch kind {
			case "panic":
				panic(label)
			case "nil":
				panic(nil)
			case "exit":
				runtime.Goexit()
			}
		}
	}
	if faultAt != "New" {
		it.actions[faultAt] = fault(faultKind, "fault/"+faultAt)
	}
	if cleanupAt != "" {
		it.actions[cleanupAt] = fault(cleanupKind, "cleanup/"+cleanupAt)
	}
	tb := &oneStableTraceTable{it: it}
	if faultAt == "New" {
		tb.newAction = fault(faultKind, "fault/"+faultAt)
	}
	run = oneStableSeedRun()
	done := make(chan struct{})
	go func() {
		defer func() { recovered = recover(); closes = it.closes; index = it.idx; close(done) }()
		_ = merge([]flushUnit{{mem: tb}}, &run, func(batch.Entry) error {
			it.event("Emit")
			if faultAt == "Emit" && faultKind == "return" {
				return errors.New("emit return")
			}
			return nil
		})
		returned = true
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		panic("single stable unwind did not finish")
	}
	return
}

func TestSingleStableMergeAbnormalExitTrace(t *testing.T) {
	for _, at := range []string{"New", "Valid", "Key", "Entry", "Emit", "Next", "Error", "Close"} {
		for _, kind := range []string{"panic", "nil", "exit"} {
			t.Run(at+"/"+kind, func(t *testing.T) {
				a, ar, an, ap, ac, ai := oneStableUnwind(oneStableLegacyReference, at, kind, "", "")
				b, br, bn, bp, bc, bi := oneStableUnwind(mergeCanonicalStableIteratorUnits, at, kind, "", "")
				if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(ar, br) || an || bn || ac != bc || ai != bi || (ap == nil) != (bp == nil) {
					t.Fatalf("unwind differs: %v/%+v/%v/%v/%d/%d vs %v/%+v/%v/%v/%d/%d", a, ar, an, ap, ac, ai, b, br, bn, bp, bc, bi)
				}
				if kind == "panic" && bp != "fault/"+at {
					t.Fatalf("panic lost: %v", bp)
				}
				wantClose := 1
				if at == "New" || at == "Error" {
					wantClose = 0
				}
				if bc != wantClose {
					t.Fatalf("closes=%d want=%d", bc, wantClose)
				}
				seed := oneStableSeedRun()
				if at == "Error" || at == "Close" {
					seed.plannedPointOps = 2
					seed.deletePointOps = 0
				}
				if !reflect.DeepEqual(br, seed) {
					t.Fatalf("run=%+v want=%+v", br, seed)
				}
			})
		}
	}
	// Cleanup faults may replace an in-flight callback fault. Error failing also
	// prevents Close; do not invent a "Close always" policy or recover/repanic.
	for _, at := range []string{"Error", "Close"} {
		for _, kind := range []string{"panic", "exit"} {
			for _, earlier := range []string{"panic", "nil", "exit", "return"} {
				a, ar, an, ap, ac, ai := oneStableUnwind(oneStableLegacyReference, "Emit", earlier, at, kind)
				b, br, bn, bp, bc, bi := oneStableUnwind(mergeCanonicalStableIteratorUnits, "Emit", earlier, at, kind)
				if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(ar, br) || an || bn || ac != bc || ai != bi || (ap == nil) != (bp == nil) {
					t.Fatalf("cleanup differs: %v/%v vs %v/%v", a, ap, b, bp)
				}
				if kind == "panic" && bp != "cleanup/"+at {
					t.Fatalf("cleanup panic lost: %v", bp)
				}
				if kind == "exit" && bp != nil {
					t.Fatalf("Goexit converted: %v", bp)
				}
			}
		}
	}
}

func TestSingleStableMergeFrozenAppendOnlyAndZeroUnits(t *testing.T) {
	mt := memtable.NewAppendOnlyWithEntryCapacity(8)
	mt.SetEntryWithRevision([]byte("z"), []byte("old"), page.ValuePtr{}, node.FlagInline, 1)
	mt.SetEntryWithRevision([]byte("a"), []byte("a-value"), page.ValuePtr{}, node.FlagInline, 2)
	mt.SetEntryWithRevision([]byte("z"), []byte("new"), page.ValuePtr{}, node.FlagInline, 3)
	mt.SetEntryWithRevision([]byte("gone"), nil, page.ValuePtr{}, node.FlagTombstone, 4)
	ptr := page.ValuePtr{FileID: 8, Offset: 16, Length: 5}
	mt.SetEntryWithRevision([]byte("ptr"), nil, ptr, node.FlagPointer, 5)
	mt.Freeze()
	var got []batch.Entry
	run := canonicalFlushRun{}
	if err := mergeCanonicalStableIteratorUnits([]flushUnit{{mem: mt}}, &run, func(e batch.Entry) error { got = append(got, e); return nil }); err != nil {
		t.Fatal(err)
	}
	want := []batch.Entry{{Type: batch.OpPut, Key: []byte("a"), Value: []byte("a-value"), Revision: 2}, {Type: batch.OpDelete, Key: []byte("gone"), Revision: 4}, {Type: batch.OpPut, Key: []byte("ptr"), ValuePtr: ptr, IsPtr: true, Revision: 5}, {Type: batch.OpPut, Key: []byte("z"), Value: []byte("new"), Revision: 3}}
	if !reflect.DeepEqual(got, want) || run.plannedPointOps != 4 || run.deletePointOps != 1 {
		t.Fatalf("got=%+v stats=%+v", got, run)
	}
	// Values above remain readable after the iterator's Close and Next calls.
	for _, merge := range []oneStableMergeFunc{oneStableLegacyReference, mergeCanonicalStableIteratorUnits} {
		zero := oneStableSeedRun()
		if err := merge(nil, &zero, nil); err != nil || zero.plannedPointOps != 0 || zero.deletePointOps != 0 || zero.shadowedPointOps != 14 {
			t.Fatalf("zero=%+v err=%v", zero, err)
		}
	}
}

// Mock chunks deliberately expose retained Set views rather than copying them.
// This is a failure-path custody test, not an atomic-publication/recovery model.
type oneStableChunkBackend struct {
	*MockBackend
	failAt, writes, closes int
	failure                error
	chunks                 [][]batch.Entry
	iteratorClosesAtWrite  []int
	it                     *oneStableTraceIterator
}

func (b *oneStableChunkBackend) NewBatch() batch.Interface {
	return &oneStableChunkBatch{MockBatch: &MockBatch{mb: b.MockBackend}, owner: b}
}
func (b *oneStableChunkBackend) NewBatchWithSize(int) batch.Interface { return b.NewBatch() }

type oneStableChunkBatch struct {
	*MockBatch
	owner   *oneStableChunkBackend
	entries []batch.Entry
}

func (b *oneStableChunkBatch) Set(k, v []byte) error {
	b.entries = append(b.entries, batch.Entry{Type: batch.OpPut, Key: k, Value: v})
	return nil
}
func (b *oneStableChunkBatch) SetView(k, v []byte) error { return b.Set(k, v) }
func (b *oneStableChunkBatch) Write() error {
	b.owner.writes++
	b.owner.chunks = append(b.owner.chunks, b.entries)
	b.owner.iteratorClosesAtWrite = append(b.owner.iteratorClosesAtWrite, b.owner.it.closes)
	if b.owner.writes == b.owner.failAt {
		return b.owner.failure
	}
	return nil
}
func (b *oneStableChunkBatch) WriteSync() error { return b.Write() }
func (b *oneStableChunkBatch) Close() error     { b.owner.closes++; return nil }

func TestSingleStableMergeMultiChunkFailureRetainsQueuedUnit(t *testing.T) {
	for _, failAt := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			var trace []string
			rows := make([]oneStableRawRow, 5)
			for i := range rows {
				rows[i] = oneStableRawRow{key: []byte(fmt.Sprintf("k%d", i)), value: []byte(fmt.Sprintf("v%d", i)), flags: node.FlagInline}
			}
			it := &oneStableTraceIterator{rows: rows, trace: &trace}
			tb := &oneStableTraceTable{it: it}
			backend := &oneStableChunkBackend{MockBackend: NewMockBackend(), failAt: failAt, failure: errors.New("chunk write failed"), it: it}
			var reported error
			db := &DB{backend: backend, flushBackendMaxEntries: 2, flushBackendMaxBatches: -1, queue: []memtable.Table{tb}, queueIDs: []uint64{91}, queueLaneIDs: []uint16{0}, notifyError: func(err error) { reported = err }}
			units := []flushUnit{{mem: tb, id: 91, memLen: 5}}
			if !db.canStreamCanonicalPointUnitsFromStableIterators(units, 0) {
				t.Fatal("stable stream not eligible")
			}
			if db.flushCanonicalPointUnitsStableIteratorStreamed(true, 0, nil, units, []uint64{91}, 0, 5, 0, flushCollectionMode(0)) {
				t.Fatal("failed chunk succeeded")
			}
			if !errors.Is(reported, backend.failure) || backend.writes != failAt || backend.closes != failAt {
				t.Fatalf("reported=%v writes/closes=%d/%d", reported, backend.writes, backend.closes)
			}
			if len(db.queue) != 1 || db.queue[0] != tb || !reflect.DeepEqual(db.queueIDs, []uint64{91}) || !reflect.DeepEqual(db.queueLaneIDs, []uint16{0}) {
				t.Fatalf("queue custody changed: %v %v", db.queueIDs, db.queueLaneIDs)
			}
			// Count pass closes once; merge must close exactly once more. Final
			// chunk emission occurs after merge Close, earlier chunks before it.
			if it.closes != 2 {
				t.Fatalf("iterator closes=%d want=2", it.closes)
			}
			for n, chunk := range backend.chunks {
				wantClose := 1
				if n == 2 {
					wantClose = 2
				}
				if backend.iteratorClosesAtWrite[n] != wantClose {
					t.Fatalf("chunk%d closes=%d want=%d", n, backend.iteratorClosesAtWrite[n], wantClose)
				}
				for j, e := range chunk {
					k := n*2 + j
					if string(e.Key) != fmt.Sprintf("k%d", k) || string(e.Value) != fmt.Sprintf("v%d", k) {
						t.Fatalf("unstable retained chunk%d: %+v", n, chunk)
					}
				}
			}
		})
	}
}
func oneStableLegacyReference(units []flushUnit, out *canonicalFlushRun, emit func(batch.Entry) error) (err error) {
	if out == nil {
		return errors.New("cachingdb: missing canonical flush run")
	}
	heap := make(stableOpMergeHeap, 0, len(units))

	iters := make([]iterator.UnsafeIterator, len(units))
	wrappers := make([]stableUnsafeOpIter, len(units))
	defer func() {
		if cerr := closeCanonicalStableIterators(iters); err == nil {
			err = cerr
		}
	}()

	for i := range units {
		iter := units[i].mem.NewIterator(nil, nil)
		if iter == nil {
			return errors.New("cachingdb: nil canonical stable iterator")
		}
		iters[i] = iter
		wrappers[i] = stableUnsafeOpIter{UnsafeIterator: iter}
		if iter.Valid() {
			priority := len(units) - 1 - i
			heap = append(heap, stableOpMergeItem{iter: &wrappers[i], priority: priority, key: wrappers[i].Key()})
		}
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		(&heap).down(i, len(heap))
	}

	plannedDeletes := 0
	plannedPointOps := 0
	for len(heap) > 0 {
		top := heap.pop()
		currentKey := top.key

		for len(heap) > 0 {
			next := heap.peek()
			if next != nil && bytes.Equal(next.key, currentKey) {
				shadowed := heap.pop()
				out.shadowedPointOps++
				shadowed.iter.Next()
				if shadowed.iter.Valid() {
					shadowed.key = shadowed.iter.Key()
					heap.push(shadowed)
				}
				continue
			}
			break
		}

		entry := top.iter.Entry()
		if emit != nil {
			if err := emit(entry); err != nil {
				return err
			}
		}
		plannedPointOps++
		if entry.Type == batch.OpDelete {
			plannedDeletes++
		}

		top.iter.Next()
		if top.iter.Valid() {
			top.key = top.iter.Key()
			heap.push(top)
		}
	}
	out.plannedPointOps = plannedPointOps
	out.deletePointOps = plannedDeletes
	return nil
}
