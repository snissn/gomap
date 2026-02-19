package caching

import (
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type pointerBatch struct {
	entries []batch.Entry
}

func (b *pointerBatch) Set(key, value []byte) error {
	b.entries = append(b.entries, batch.Entry{Type: batch.OpPut, Key: key, Value: value})
	return nil
}

func (b *pointerBatch) SetView(key, value []byte) error {
	return b.Set(key, value)
}

func (b *pointerBatch) Delete(key []byte) error {
	b.entries = append(b.entries, batch.Entry{Type: batch.OpDelete, Key: key})
	return nil
}

func (b *pointerBatch) DeleteView(key []byte) error {
	return b.Delete(key)
}

func (b *pointerBatch) SetPointer(key []byte, ptr page.ValuePtr) error {
	b.entries = append(b.entries, batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true})
	return nil
}

func (b *pointerBatch) SetPointerView(key []byte, ptr page.ValuePtr) error {
	return b.SetPointer(key, ptr)
}

func (b *pointerBatch) SetOps(ops []batch.Entry) error {
	b.entries = append(b.entries, ops...)
	return nil
}

func (b *pointerBatch) Write() error { return nil }

func (b *pointerBatch) WriteSync() error { return nil }

func (b *pointerBatch) Close() error { return nil }

func (b *pointerBatch) Replay(fn func(batch.Entry) error) error {
	for _, e := range b.entries {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

func (b *pointerBatch) GetByteSize() (int, error) { return len(b.entries), nil }

func TestPutEntryRunsClearsReferences(t *testing.T) {
	runs := make([][]batch.Entry, 2)
	runs[0] = []batch.Entry{{Type: batch.OpPut, Key: []byte("k0"), Value: []byte("v0")}}
	runs[1] = []batch.Entry{{Type: batch.OpDelete, Key: []byte("k1")}}

	putEntryRuns(runs)

	for i := range runs {
		if runs[i] != nil {
			t.Fatalf("run %d not cleared", i)
		}
	}
}

func TestPutUnitRunsClearsReferences(t *testing.T) {
	unitRuns := make([][][]batch.Entry, 2)
	unitRuns[0] = [][]batch.Entry{{{Type: batch.OpPut, Key: []byte("k0"), Value: []byte("v0")}}}
	unitRuns[1] = [][]batch.Entry{{{Type: batch.OpDelete, Key: []byte("k1")}}}

	putUnitRuns(unitRuns)

	for i := range unitRuns {
		if unitRuns[i] != nil {
			t.Fatalf("unit runs %d not cleared", i)
		}
	}
}

func TestPutOpMergeHeapClearsReferences(t *testing.T) {
	h := make(opMergeHeap, 1, 2)
	h[0] = opMergeItem{
		iter:     &opRunIter{valid: true},
		priority: 7,
		key:      []byte("k"),
	}

	putOpMergeHeap(h)

	if h[0].iter != nil || h[0].priority != 0 || h[0].key != nil {
		t.Fatalf("heap item not cleared: %+v", h[0])
	}
}

func TestBuildOpRunsChunking(t *testing.T) {
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("NewWithCapacityMode: %v", err)
	}
	var key [8]byte
	for i := 0; i < 5; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		mt.Set(key[:], []byte{byte(i + 1)})
	}
	binary.BigEndian.PutUint64(key[:], uint64(2))
	mt.Delete(key[:])
	mt.Freeze()

	runs, deleteOps, err := buildOpRuns(mt, 2)
	if err != nil {
		t.Fatalf("buildOpRuns: %v", err)
	}
	defer func() {
		for _, run := range runs {
			putEntrySlice(run)
		}
		putEntryRuns(runs)
	}()

	if deleteOps != 1 {
		t.Fatalf("deleteOps=%d want=1", deleteOps)
	}
	if len(runs) == 0 {
		t.Fatalf("expected at least one run")
	}
	for i, run := range runs {
		if len(run) == 0 || len(run) > 2 {
			t.Fatalf("run %d has unexpected size %d", i, len(run))
		}
	}
}

func TestFlushDeferredValueLogUnitsPointerOnly(t *testing.T) {
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("NewWithCapacityMode: %v", err)
	}
	entries := []struct {
		key string
		ptr page.ValuePtr
	}{
		{key: "a", ptr: page.ValuePtr{Offset: 10, Length: 1, FileID: 1}},
		{key: "b", ptr: page.ValuePtr{Offset: 20, Length: 2, FileID: 1}},
		{key: "c", ptr: page.ValuePtr{Offset: 30, Length: 3, FileID: 1}},
	}
	for _, e := range entries {
		mt.SetEntry([]byte(e.key), nil, e.ptr, node.FlagPointer)
	}
	mt.Freeze()

	db := &DB{
		flushBuildChunkCap: 2,
	}
	backendBatch := &pointerBatch{}

	pendingOps, err := db.flushDeferredValueLogUnits([]flushUnit{{mem: mt}}, backendBatch, false, 0)
	if err != nil {
		t.Fatalf("flushDeferredValueLogUnits: %v", err)
	}
	if pendingOps != len(entries) {
		t.Fatalf("pendingOps=%d want=%d", pendingOps, len(entries))
	}

	if len(backendBatch.entries) != len(entries) {
		t.Fatalf("backend batch entries=%d want=%d", len(backendBatch.entries), len(entries))
	}
	for i, e := range backendBatch.entries {
		if !e.IsPtr {
			t.Fatalf("entry %d expected pointer op", i)
		}
	}
}
