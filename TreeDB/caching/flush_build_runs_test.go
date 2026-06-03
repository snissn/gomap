package caching

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
)

func buildRunKey(v uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], v)
	return append([]byte(nil), key[:]...)
}

func collectRunEntriesForTest(t *testing.T, m memtable.Table, chunkCap int) ([]batch.Entry, int) {
	t.Helper()
	runs, deletes, err := buildOpRuns(m, chunkCap)
	if err != nil {
		t.Fatalf("buildOpRuns: %v", err)
	}
	defer func() {
		for _, run := range runs {
			putEntrySlice(run)
		}
		putEntryRuns(runs)
	}()
	var out []batch.Entry
	for _, run := range runs {
		out = append(out, run...)
	}
	return out, deletes
}

func TestBuildOpRunsAppendOnlySortedDuplicateTombstoneDeleteShapes(t *testing.T) {
	m := memtable.NewAppendOnlyWithEntryCapacity(16)
	m.Set(buildRunKey(1), []byte("old-1"))
	m.Set(buildRunKey(3), []byte("value-3"))
	m.Set(buildRunKey(2), []byte("value-2")) // order break: creates sorted-run state
	m.Set(buildRunKey(1), []byte("new-1"))   // duplicate latest wins
	m.Delete(buildRunKey(3))                 // tombstone latest wins
	m.Set(buildRunKey(4), []byte("value-4"))
	m.Freeze()

	entries, deletes := collectRunEntriesForTest(t, m, 2)
	if deletes != 1 {
		t.Fatalf("delete count=%d want 1", deletes)
	}
	if len(entries) != 4 {
		t.Fatalf("entries=%d want 4: %+v", len(entries), entries)
	}
	want := []struct {
		key    uint64
		op     batch.OpType
		value  string
		isTomb bool
	}{
		{key: 1, op: batch.OpPut, value: "new-1"},
		{key: 2, op: batch.OpPut, value: "value-2"},
		{key: 3, op: batch.OpDelete, isTomb: true},
		{key: 4, op: batch.OpPut, value: "value-4"},
	}
	for i := range want {
		gotKey := binary.BigEndian.Uint64(entries[i].Key)
		if gotKey != want[i].key || entries[i].Type != want[i].op {
			t.Fatalf("entry[%d] key/op=(%d,%d) want (%d,%d)", i, gotKey, entries[i].Type, want[i].key, want[i].op)
		}
		if !want[i].isTomb && string(entries[i].Value) != want[i].value {
			t.Fatalf("entry[%d] value=%q want %q", i, entries[i].Value, want[i].value)
		}
	}
}

func TestBuildOpRunsAppendOnlyRandomDeleteHeavyLatestWins(t *testing.T) {
	m := memtable.NewAppendOnlyWithEntryCapacity(128)
	for i := 0; i < 64; i++ {
		k := uint64((i*37 + 11) % 64)
		if i%3 == 0 {
			m.Delete(buildRunKey(k))
			continue
		}
		m.Set(buildRunKey(k), []byte(fmt.Sprintf("v-%02d", i)))
	}
	m.Freeze()

	entries, deletes := collectRunEntriesForTest(t, m, 7)
	if len(entries) == 0 {
		t.Fatalf("expected random/delete-heavy run output")
	}
	seenDeletes := 0
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
			t.Fatalf("entries not strictly sorted/latest at %d: %x >= %x", i, entries[i-1].Key, entries[i].Key)
		}
	}
	for _, entry := range entries {
		if entry.Type == batch.OpDelete {
			seenDeletes++
		}
	}
	if deletes != seenDeletes {
		t.Fatalf("delete count=%d want seen tombstones %d", deletes, seenDeletes)
	}
}

type unstableRunTable struct {
	entries []batch.Entry
}

func (t *unstableRunTable) Set(key, value []byte)                                     {}
func (t *unstableRunTable) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {}
func (t *unstableRunTable) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	return nil
}
func (t *unstableRunTable) Delete(key []byte) {}
func (t *unstableRunTable) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	return nil
}
func (t *unstableRunTable) SetSteal(key, value []byte)                                     {}
func (t *unstableRunTable) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {}
func (t *unstableRunTable) DeleteSteal(key []byte)                                         {}
func (t *unstableRunTable) Get(key []byte) ([]byte, bool, bool)                            { return nil, false, false }
func (t *unstableRunTable) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	return nil, page.ValuePtr{}, 0, false
}
func (t *unstableRunTable) Size() int64 { return 0 }
func (t *unstableRunTable) Len() int    { return len(t.entries) }
func (t *unstableRunTable) NewIterator(start, end []byte) iterator.UnsafeIterator {
	return &unstableRunIterator{entries: t.entries}
}
func (t *unstableRunTable) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	return &unstableRunIterator{entries: t.entries}
}
func (t *unstableRunTable) Freeze() {}

type unstableRunIterator struct {
	entries []batch.Entry
	idx     int
	keyBuf  []byte
	valBuf  []byte
}

func (it *unstableRunIterator) Valid() bool     { return it.idx < len(it.entries) }
func (it *unstableRunIterator) Next()           { it.idx++ }
func (it *unstableRunIterator) Seek(key []byte) {}
func (it *unstableRunIterator) Key() []byte     { return it.UnsafeKey() }
func (it *unstableRunIterator) Value() []byte   { return it.UnsafeValue() }
func (it *unstableRunIterator) KeyCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeKey()...)
}
func (it *unstableRunIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeValue()...)
}
func (it *unstableRunIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	it.keyBuf = append(it.keyBuf[:0], it.entries[it.idx].Key...)
	return it.keyBuf
}
func (it *unstableRunIterator) UnsafeValue() []byte {
	if !it.Valid() || it.entries[it.idx].Type == batch.OpDelete {
		return nil
	}
	it.valBuf = append(it.valBuf[:0], it.entries[it.idx].Value...)
	return it.valBuf
}
func (it *unstableRunIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, 0
	}
	entry := it.entries[it.idx]
	if entry.Type == batch.OpDelete {
		return nil, page.ValuePtr{}, 0x02
	}
	return it.UnsafeValue(), entry.ValuePtr, 0
}
func (it *unstableRunIterator) IsDeleted() bool {
	return it.Valid() && it.entries[it.idx].Type == batch.OpDelete
}
func (it *unstableRunIterator) Error() error             { return nil }
func (it *unstableRunIterator) Close() error             { return nil }
func (it *unstableRunIterator) Domain() ([]byte, []byte) { return nil, nil }

func TestBuildOpRunsCopiesUnstableIteratorScratch(t *testing.T) {
	tbl := &unstableRunTable{entries: []batch.Entry{
		{Type: batch.OpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Type: batch.OpPut, Key: []byte("k2"), Value: []byte("v2")},
	}}
	runs, _, err := buildOpRuns(tbl, 8)
	if err != nil {
		t.Fatalf("buildOpRuns: %v", err)
	}
	defer func() {
		for _, run := range runs {
			putEntrySlice(run)
		}
		putEntryRuns(runs)
	}()
	if len(runs) != 1 || len(runs[0]) != 2 {
		t.Fatalf("runs=%v", runs)
	}
	// Force the iterator scratch buffers to be reused and mutated; buildOpRuns
	// must not retain those transient views for non-stable iterator tables.
	it := tbl.NewIterator(nil, nil)
	_ = it.UnsafeKey()
	_ = it.UnsafeValue()
	copy(it.(*unstableRunIterator).keyBuf, []byte("XX"))
	copy(it.(*unstableRunIterator).valBuf, []byte("YY"))
	if string(runs[0][0].Key) != "k1" || string(runs[0][0].Value) != "v1" {
		t.Fatalf("run retained unstable iterator scratch: key=%q value=%q", runs[0][0].Key, runs[0][0].Value)
	}
}

func benchmarkAppendOnlyRunShape(b *testing.B, writes int, shape string) memtable.Table {
	b.Helper()
	m := memtable.NewAppendOnlyWithEntryCapacity(writes)
	for i := 0; i < writes; i++ {
		var k uint64
		switch shape {
		case "sorted":
			k = uint64(i)
		case "duplicate_tombstone":
			k = uint64(i % (writes / 4))
		case "random_delete_heavy":
			k = uint64((i*1103515245 + 12345) & (writes - 1))
		default:
			k = uint64(i)
		}
		key := buildRunKey(k)
		if (shape == "duplicate_tombstone" && i%11 == 0) || (shape == "random_delete_heavy" && i%3 == 0) {
			m.Delete(key)
			continue
		}
		m.Set(key, []byte("value-payload"))
	}
	m.Freeze()
	return m
}

func BenchmarkBuildOpRunsAppendOnlyShapes(b *testing.B) {
	const writes = 1 << 15
	for _, shape := range []string{"sorted", "duplicate_tombstone", "random_delete_heavy"} {
		b.Run(shape, func(b *testing.B) {
			m := benchmarkAppendOnlyRunShape(b, writes, shape)
			runs, _, err := buildOpRuns(m, 8192)
			if err != nil {
				b.Fatalf("warm buildOpRuns: %v", err)
			}
			for _, run := range runs {
				putEntrySlice(run)
			}
			putEntryRuns(runs)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runs, _, err := buildOpRuns(m, 8192)
				if err != nil {
					b.Fatalf("buildOpRuns: %v", err)
				}
				for _, run := range runs {
					putEntrySlice(run)
				}
				putEntryRuns(runs)
			}
		})
	}
}
