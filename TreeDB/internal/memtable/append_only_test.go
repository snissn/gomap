package memtable

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
	"unsafe"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestAppendOnlyNextCapacityGrowthPolicy(t *testing.T) {
	tests := []struct {
		name    string
		current int
		want    int
	}{
		{name: "below-min", current: appendOnlyMinInitialEntries - 1, want: appendOnlyMinInitialEntries},
		{name: "below-cutoff-quadruples", current: appendOnlyMinInitialEntries, want: appendOnlyMinInitialEntries * 4},
		{name: "just-below-cutoff-quadruples", current: appendOnlyAggressiveGrowCutoff - 1, want: (appendOnlyAggressiveGrowCutoff - 1) * 4},
		{name: "at-cutoff-doubles", current: appendOnlyAggressiveGrowCutoff, want: appendOnlyAggressiveGrowCutoff * 2},
		{name: "above-cutoff-doubles", current: appendOnlyAggressiveGrowCutoff + 1, want: (appendOnlyAggressiveGrowCutoff + 1) * 2},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := appendOnlyNextCapacity(tc.current); got != tc.want {
				t.Fatalf("appendOnlyNextCapacity(%d)=%d want=%d", tc.current, got, tc.want)
			}
		})
	}
}

func TestAppendOnlyEntryPayloadIndexAllowsLargeSideBuffers(t *testing.T) {
	var ent appendOnlyEntry
	const payloadIndex = uint32(1<<31 | 12345)
	const flags = node.FlagPointer | byte(0x40)

	appendOnlyEntrySetPayloadIndex(&ent, payloadIndex)
	copy(ent.inlineKey[:], []byte("long-key"))
	appendOnlyEntrySetKeyIndex(&ent, 1)
	appendOnlyEntrySetFlags(&ent, flags)

	if got := appendOnlyEntryPayloadIndex(&ent); got != payloadIndex {
		t.Fatalf("payload index=%d want=%d", got, payloadIndex)
	}
	if got := appendOnlyEntryFlags(&ent); got != flags {
		t.Fatalf("flags=%d want=%d", got, flags)
	}
}

func TestAppendOnlyInlineGrowthSkipsIntermediateDoublingBelowCutoff(t *testing.T) {
	mt := &AppendOnly{
		entries:        make([]appendOnlyEntry, appendOnlyMinInitialEntries),
		baseEntriesLen: appendOnlyMinInitialEntries,
		growEntriesLen: appendOnlyMinInitialEntries,
		ordered:        true,
		lastIdx:        -1,
	}
	if got := cap(mt.entries); got != appendOnlyMinInitialEntries {
		t.Fatalf("initial cap(entries)=%d want=%d", got, appendOnlyMinInitialEntries)
	}

	growthCaps := make([]int, 0, 2)
	lastCap := cap(mt.entries)
	value := []byte("v")
	for i := 0; i < appendOnlyMinInitialEntries*2+1; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		mt.Set(key[:], value)
		if got := cap(mt.entries); got != lastCap {
			growthCaps = append(growthCaps, got)
			lastCap = got
		}
	}

	if len(growthCaps) != 1 {
		t.Fatalf("growth caps=%v want exactly one growth step", growthCaps)
	}
	if growthCaps[0] < appendOnlyMinInitialEntries*4 {
		t.Fatalf("growth cap=%d want >= %d", growthCaps[0], appendOnlyMinInitialEntries*4)
	}
}

func TestAppendOnlyInitialEntriesForCapacity(t *testing.T) {
	t.Run("pointer-estimate", func(t *testing.T) {
		capacity := defaultMemtableCapacity
		got := appendOnlyInitialEntriesForCapacity(capacity, appendOnlyEstimatedBytesPerEntryPointer)
		want := capacity / appendOnlyEstimatedBytesPerEntryPointer
		if got != want {
			t.Fatalf("pointer estimate entries=%d want=%d", got, want)
		}
	})

	t.Run("custom-estimate", func(t *testing.T) {
		capacity := 96 * 200
		got := appendOnlyInitialEntriesForCapacity(capacity, 96)
		if got != 200 {
			t.Fatalf("custom estimate entries=%d want=%d", got, 200)
		}
	})

	t.Run("minimum-clamp", func(t *testing.T) {
		got := appendOnlyInitialEntriesForCapacity(1, 1<<20)
		if got != appendOnlyMinInitialEntries {
			t.Fatalf("min clamp entries=%d want=%d", got, appendOnlyMinInitialEntries)
		}
	})
}

func TestAppendOnlyEntryHintDoesNotRaiseInitialCapacityBeyondBudget(t *testing.T) {
	capacity := appendOnlyMinInitialEntries * appendOnlyEstimatedBytesPerEntryPointer
	hint := appendOnlyMaxInitialEntries

	m := NewAppendOnlyWithCapacityEstimatedEntryBytesAndHint(capacity, appendOnlyEstimatedBytesPerEntryPointer, hint)
	if got := len(m.entries); got != appendOnlyMinInitialEntries {
		t.Fatalf("initial len(entries)=%d want %d", got, appendOnlyMinInitialEntries)
	}
	if got := m.baseEntriesLen; got != appendOnlyMinInitialEntries {
		t.Fatalf("baseEntriesLen=%d want %d", got, appendOnlyMinInitialEntries)
	}
	if got := m.growEntriesLen; got != hint {
		t.Fatalf("growEntriesLen=%d want hint floor %d", got, hint)
	}
}

func TestAppendOnlyCRUD(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)

	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k1"), []byte("v1"))
	m.Set([]byte("k1"), []byte("v1b"))
	m.Delete([]byte("k2"))

	val, del, ok := m.Get([]byte("k1"))
	if !ok || del || string(val) != "v1b" {
		t.Fatalf("Get(k1) = (%q,%v,%v), want (v1b,false,true)", string(val), del, ok)
	}

	val, del, ok = m.Get([]byte("k2"))
	if !ok || !del || val != nil {
		t.Fatalf("Get(k2) = (%v,%v,%v), want (nil,true,true)", val, del, ok)
	}
}

func TestAppendOnlyApplyBorrowValueSortedBatch_CopiesKeysBorrowsValues(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key := []byte("k-short")
	value := []byte("borrowed-value")

	m.ApplyBorrowValueSortedBatch([]batchpkg.Entry{{
		Type:  batchpkg.OpPut,
		Key:   key,
		Value: value,
	}}, false, nil)

	if m.count != 1 {
		t.Fatalf("count=%d want=1", m.count)
	}
	ent := &m.entries[0]
	got := m.appendOnlyEntryValue(ent)
	if len(got) != len(value) {
		t.Fatalf("value len=%d want=%d", len(got), len(value))
	}
	if unsafe.SliceData(got) != unsafe.SliceData(value) {
		t.Fatal("expected borrowed value storage to alias caller slice")
	}
	if len(m.valueArena.chunks) != 0 {
		t.Fatalf("valueArena chunks=%d want=0 for inline borrowed value", len(m.valueArena.chunks))
	}

	key[0] = 'z'
	if gotKey := string(m.appendOnlyEntryKey(ent)); gotKey != "k-short" {
		t.Fatalf("stored key changed after caller mutation: got=%q want=%q", gotKey, "k-short")
	}
	gotValue, deleted, ok := m.Get([]byte("k-short"))
	if !ok || deleted || string(gotValue) != "borrowed-value" {
		t.Fatalf("Get(k-short)=(%q,%v,%v) want=(borrowed-value,false,true)", string(gotValue), deleted, ok)
	}
}

func TestAppendOnlyApplyBorrowValueSortedBatchTrusted_PreservesOrderedFastPath(t *testing.T) {
	m := NewAppendOnlyWithCapacityEstimatedEntryBytes(appendOnlyMinInitialEntries*appendOnlyEstimatedBytesPerEntryPointer, appendOnlyEstimatedBytesPerEntryPointer)
	entries := make([]batchpkg.Entry, appendOnlyMinInitialEntries*4)
	value := []byte("borrowed-value")
	callbacks := 0
	for i := range entries {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i+1))
		entries[i] = batchpkg.Entry{Type: batchpkg.OpPut, Key: append([]byte(nil), key[:]...), Value: value}
	}

	m.ApplyBorrowValueSortedBatchTrusted(entries, false, func(key []byte) {
		callbacks++
	})

	if callbacks != len(entries) {
		t.Fatalf("callbacks=%d want=%d", callbacks, len(entries))
	}
	if !m.ordered {
		t.Fatal("expected ordered append-only memtable after trusted sorted batch")
	}
	if !m.hasLast || m.lastIdx != len(entries)-1 {
		t.Fatalf("last state hasLast=%v lastIdx=%d want lastIdx=%d", m.hasLast, m.lastIdx, len(entries)-1)
	}
	if m.count != len(entries) {
		t.Fatalf("count=%d want=%d", m.count, len(entries))
	}
	if m.latest != nil || m.latest64 != nil {
		t.Fatalf("trusted ordered batch should not materialize latest maps: latest=%v latest64=%v", m.latest != nil, m.latest64 != nil)
	}

	lastKey := entries[len(entries)-1].Key
	got, deleted, ok := m.Get(lastKey)
	if !ok || deleted || string(got) != string(value) {
		t.Fatalf("Get(last)=(%q,%v,%v) want=(%q,false,true)", string(got), deleted, ok, string(value))
	}
}

func TestAppendOnlyApplyBorrowValueSortedBatchIndicesTrusted_PreservesOrderedFastPath(t *testing.T) {
	m := NewAppendOnlyWithCapacityEstimatedEntryBytes(appendOnlyMinInitialEntries*appendOnlyEstimatedBytesPerEntryPointer, appendOnlyEstimatedBytesPerEntryPointer)
	entries := make([]batchpkg.Entry, appendOnlyMinInitialEntries*4)
	value := []byte("borrowed-value")
	idxs := make([]int, 0, len(entries)/2)
	callbacks := 0
	lastSelected := -1
	for i := range entries {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i+1))
		entries[i] = batchpkg.Entry{Type: batchpkg.OpPut, Key: append([]byte(nil), key[:]...), Value: value}
		if i%2 == 0 {
			idxs = append(idxs, i)
			lastSelected = i
		}
	}

	m.ApplyBorrowValueSortedBatchIndicesTrusted(entries, idxs, false, func(key []byte) {
		callbacks++
	})

	if callbacks != len(idxs) {
		t.Fatalf("callbacks=%d want=%d", callbacks, len(idxs))
	}
	if !m.ordered {
		t.Fatal("expected ordered append-only memtable after trusted sorted batch indices")
	}
	if !m.hasLast || m.lastIdx != len(idxs)-1 {
		t.Fatalf("last state hasLast=%v lastIdx=%d want lastIdx=%d", m.hasLast, m.lastIdx, len(idxs)-1)
	}
	if m.count != len(idxs) {
		t.Fatalf("count=%d want=%d", m.count, len(idxs))
	}
	lastKey := entries[lastSelected].Key
	got, deleted, ok := m.Get(lastKey)
	if !ok || deleted || string(got) != string(value) {
		t.Fatalf("Get(lastSelected)=(%q,%v,%v) want=(%q,false,true)", string(got), deleted, ok, string(value))
	}
}

func TestAppendOnlyApplyBorrowValueSortedBatchTrusted_FallsBackWhenBatchStartsBeforeLastKey(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	var highKey [8]byte
	binary.BigEndian.PutUint64(highKey[:], 10)
	m.Set(highKey[:], []byte("ten"))

	var keyOne [8]byte
	var keyTwo [8]byte
	binary.BigEndian.PutUint64(keyOne[:], 1)
	binary.BigEndian.PutUint64(keyTwo[:], 2)
	m.ApplyBorrowValueSortedBatchTrusted([]batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: keyOne[:], Value: []byte("one")},
		{Type: batchpkg.OpPut, Key: keyTwo[:], Value: []byte("two")},
	}, false, nil)

	if m.ordered {
		t.Fatal("expected fallback trusted batch to mark memtable unordered")
	}
	if !m.latestDirty {
		t.Fatal("expected unordered fallback to defer latest index materialization")
	}
	if m.latest != nil || m.latest64 != nil {
		t.Fatal("expected unordered fallback to keep latest index unmaterialized before read")
	}

	if got, deleted, ok := m.Get(keyOne[:]); !ok || deleted || string(got) != "one" {
		t.Fatalf("Get(keyOne)=(%q,%v,%v) want=(one,false,true)", string(got), deleted, ok)
	}
	if m.latestDirty {
		t.Fatal("expected first point read to materialize latest index")
	}
	if got, deleted, ok := m.Get(highKey[:]); !ok || deleted || string(got) != "ten" {
		t.Fatalf("Get(highKey)=(%q,%v,%v) want=(ten,false,true)", string(got), deleted, ok)
	}
}

func TestAppendOnlyApplyBorrowValueSortedBatchIndicesTrusted_FallsBackWhenBatchStartsBeforeLastKey(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	var highKey [8]byte
	binary.BigEndian.PutUint64(highKey[:], 10)
	m.Set(highKey[:], []byte("ten"))

	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: func() []byte { var k [8]byte; binary.BigEndian.PutUint64(k[:], 1); return append([]byte(nil), k[:]...) }(), Value: []byte("one")},
		{Type: batchpkg.OpPut, Key: func() []byte { var k [8]byte; binary.BigEndian.PutUint64(k[:], 2); return append([]byte(nil), k[:]...) }(), Value: []byte("two")},
	}
	m.ApplyBorrowValueSortedBatchIndicesTrusted(entries, []int{0, 1}, false, nil)

	if m.ordered {
		t.Fatal("expected fallback trusted batch indices to mark memtable unordered")
	}
	if !m.latestDirty {
		t.Fatal("expected unordered fallback to defer latest index materialization")
	}
	if m.latest != nil || m.latest64 != nil {
		t.Fatal("expected unordered fallback to keep latest index unmaterialized before read")
	}

	if got, deleted, ok := m.Get(entries[0].Key); !ok || deleted || string(got) != "one" {
		t.Fatalf("Get(entries[0])=(%q,%v,%v) want=(one,false,true)", string(got), deleted, ok)
	}
	if m.latestDirty {
		t.Fatal("expected first point read to materialize latest index")
	}
	if got, deleted, ok := m.Get(highKey[:]); !ok || deleted || string(got) != "ten" {
		t.Fatalf("Get(highKey)=(%q,%v,%v) want=(ten,false,true)", string(got), deleted, ok)
	}
}

func TestAppendOnlyFirstPayloadUsesDenseEntryCapacityFloor(t *testing.T) {
	m := &AppendOnly{
		entries: make([]appendOnlyEntry, 1024),
		ordered: true,
		lastIdx: -1,
	}

	m.Set([]byte("k-short"), []byte("value"))

	if got := cap(m.values); got != cap(m.entries) {
		t.Fatalf("value cap=%d want entry cap=%d", got, cap(m.entries))
	}
	if got := len(m.values); got != 1 {
		t.Fatalf("value len=%d want=1", got)
	}
}

func TestAppendOnlyIteratorSortedLatest(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)

	// Out-of-order + duplicate key forces sort/dedup snapshot path.
	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k1"), []byte("v1"))
	m.Set([]byte("k1"), []byte("v1b"))
	m.Delete([]byte("k2"))

	it := m.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()

	if !it.Valid() || string(it.Key()) != "k1" {
		t.Fatalf("first key = %q, want k1", string(it.Key()))
	}
	if got := string(it.Value()); got != "v1b" {
		t.Fatalf("k1 value = %q, want v1b", got)
	}
	it.Next()

	if !it.Valid() || string(it.Key()) != "k2" {
		t.Fatalf("second key = %q, want k2", string(it.Key()))
	}
	_, _, flags := it.UnsafeEntry()
	if flags&node.FlagTombstone == 0 {
		t.Fatalf("k2 should be tombstone, flags=%d", flags)
	}
	it.Next()

	if it.Valid() {
		t.Fatalf("iterator should be exhausted")
	}
}

func TestAppendOnlySetEntryPreservesExtraFlagBits(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	ptr := page.ValuePtr{Offset: 7, Length: 11, FileID: 3}
	const extra = byte(0x40)

	m.SetEntry([]byte("b-ptr"), []byte("tail"), ptr, node.FlagPointer|extra)
	m.SetEntrySteal([]byte("a-del"), nil, page.ValuePtr{}, node.FlagTombstone|extra)

	_, gotPtr, flags, ok := m.GetEntry([]byte("b-ptr"))
	if !ok {
		t.Fatalf("GetEntry(ptr) missing")
	}
	if gotPtr != ptr {
		t.Fatalf("ptr=%+v want=%+v", gotPtr, ptr)
	}
	if flags != node.FlagPointer|extra {
		t.Fatalf("ptr flags=%#x want=%#x", flags, node.FlagPointer|extra)
	}

	_, _, flags, ok = m.GetEntry([]byte("a-del"))
	if !ok {
		t.Fatalf("GetEntry(del) missing")
	}
	if flags != node.FlagTombstone|extra {
		t.Fatalf("del flags=%#x want=%#x", flags, node.FlagTombstone|extra)
	}

	it := m.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	seen := 0
	for ; it.Valid(); it.Next() {
		key := string(it.UnsafeKey())
		_, gotPtr, gotFlags := it.UnsafeEntry()
		switch key {
		case "a-del":
			if gotFlags != node.FlagTombstone|extra {
				t.Fatalf("iterator del flags=%#x want=%#x", gotFlags, node.FlagTombstone|extra)
			}
			seen++
		case "b-ptr":
			if gotPtr != ptr {
				t.Fatalf("iterator ptr=%+v want=%+v", gotPtr, ptr)
			}
			if gotFlags != node.FlagPointer|extra {
				t.Fatalf("iterator ptr flags=%#x want=%#x", gotFlags, node.FlagPointer|extra)
			}
			seen++
		}
	}
	if seen != 2 {
		t.Fatalf("iterator saw %d flagged entries, want 2", seen)
	}
}

func TestAppendOnlyResetClearsLatestIndex(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("k1"), []byte("v1"))
	m.Set([]byte("k2"), []byte("v2"))
	m.Reset()

	if v, del, ok := m.Get([]byte("k1")); ok || del || v != nil {
		t.Fatalf("Get(k1) after reset = (%v,%v,%v), want (nil,false,false)", v, del, ok)
	}

	it := m.NewIterator(nil, nil)
	if it.Valid() {
		t.Fatalf("iterator should be empty after reset")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close after reset: %v", err)
	}

	m.Set([]byte("k1"), []byte("v1b"))
	v, del, ok := m.Get([]byte("k1"))
	if !ok || del || string(v) != "v1b" {
		t.Fatalf("Get(k1) after reset+set = (%q,%v,%v), want (v1b,false,true)", string(v), del, ok)
	}
}

func TestAppendOnlyGetOnly8ByteKeysNoPanic(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)

	var k2 [8]byte
	var k1 [8]byte
	binary.BigEndian.PutUint64(k2[:], 2)
	binary.BigEndian.PutUint64(k1[:], 1)

	m.Set(k2[:], []byte("v2"))
	m.Set(k1[:], []byte("v1"))
	m.Set(k1[:], []byte("v1b"))

	// Build latest index cache; with only 8-byte keys this populates latest64 but
	// may leave latest nil.
	it := m.NewIterator(nil, nil)
	_ = it.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic for 8-byte-key lookup fast path: %v", r)
		}
	}()

	if v, del, ok := m.Get(k1[:]); !ok || del || string(v) != "v1b" {
		t.Fatalf("Get(k1)=(%q,%v,%v), want (v1b,false,true)", string(v), del, ok)
	}
	if _, _, ok := m.Get([]byte("not-8-byte-key")); ok {
		t.Fatalf("expected non-8-byte key miss")
	}
	if _, _, _, ok := m.GetEntry([]byte("another-miss")); ok {
		t.Fatalf("expected GetEntry miss")
	}
}

func TestAppendOnlyOrderedGetAndGetEntryFastPath(t *testing.T) {
	keyKinds := []struct {
		name    string
		makeKey func(uint64) []byte
	}{
		{
			name: "non-8-byte",
			makeKey: func(v uint64) []byte {
				return []byte{byte('a' + v)}
			},
		},
		{
			name: "8-byte",
			makeKey: func(v uint64) []byte {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, v)
				return k
			},
		},
	}
	states := []struct {
		name   string
		frozen bool
	}{
		{name: "mutable", frozen: false},
		{name: "frozen", frozen: true},
	}

	for _, kk := range keyKinds {
		kk := kk
		for _, state := range states {
			state := state
			t.Run(kk.name+"/"+state.name, func(t *testing.T) {
				m := NewAppendOnlyWithCapacity(0)

				hitKey := kk.makeKey(1)
				tombstoneKey := kk.makeKey(2)
				emptyKey := kk.makeKey(3)
				pointerKey := kk.makeKey(4)
				missKey := kk.makeKey(5)

				m.Set(hitKey, []byte("value-hit"))
				m.Delete(tombstoneKey)
				m.Set(emptyKey, []byte{})
				ptrWant := page.ValuePtr{Offset: 123, Length: 456, FileID: 7}
				m.SetEntry(pointerKey, nil, ptrWant, node.FlagPointer)

				if !m.ordered {
					t.Fatalf("expected memtable to stay ordered for fast path")
				}
				if state.frozen {
					m.Freeze()
				}

				if got, del, ok := m.Get(hitKey); !ok || del || string(got) != "value-hit" {
					t.Fatalf("Get(hit) = (%q,%v,%v), want (value-hit,false,true)", string(got), del, ok)
				}
				if got, del, ok := m.Get(tombstoneKey); !ok || !del || got != nil {
					t.Fatalf("Get(tombstone) = (%v,%v,%v), want (nil,true,true)", got, del, ok)
				}
				if got, del, ok := m.Get(emptyKey); !ok || del || got != nil {
					t.Fatalf("Get(empty) = (%v,%v,%v), want (nil,false,true)", got, del, ok)
				}
				if got, del, ok := m.Get(pointerKey); !ok || del || got != nil {
					t.Fatalf("Get(pointer) = (%v,%v,%v), want (nil,false,true)", got, del, ok)
				}
				if got, del, ok := m.Get(missKey); ok || del || got != nil {
					t.Fatalf("Get(miss) = (%v,%v,%v), want (nil,false,false)", got, del, ok)
				}

				if got, ptr, flags, ok := m.GetEntry(hitKey); !ok || string(got) != "value-hit" || ptr != (page.ValuePtr{}) || flags != node.FlagInline {
					t.Fatalf("GetEntry(hit) = (%q,%+v,%d,%v), want (value-hit,zero,%d,true)", string(got), ptr, flags, ok, node.FlagInline)
				}
				if got, ptr, flags, ok := m.GetEntry(tombstoneKey); !ok || got != nil || ptr != (page.ValuePtr{}) || flags != node.FlagTombstone {
					t.Fatalf("GetEntry(tombstone) = (%v,%+v,%d,%v), want (nil,zero,%d,true)", got, ptr, flags, ok, node.FlagTombstone)
				}
				if got, ptr, flags, ok := m.GetEntry(emptyKey); !ok || got != nil || ptr != (page.ValuePtr{}) || flags != node.FlagInline {
					t.Fatalf("GetEntry(empty) = (%v,%+v,%d,%v), want (nil,zero,%d,true)", got, ptr, flags, ok, node.FlagInline)
				}
				if got, ptr, flags, ok := m.GetEntry(pointerKey); !ok || got != nil || ptr != ptrWant || flags != node.FlagPointer {
					t.Fatalf("GetEntry(pointer) = (%v,%+v,%d,%v), want (nil,%+v,%d,true)", got, ptr, flags, ok, ptrWant, node.FlagPointer)
				}
				if got, ptr, flags, ok := m.GetEntry(missKey); ok || got != nil || ptr != (page.ValuePtr{}) || flags != 0 {
					t.Fatalf("GetEntry(miss) = (%v,%+v,%d,%v), want (nil,zero,0,false)", got, ptr, flags, ok)
				}
			})
		}
	}
}

func TestAppendOnlyGetBuildsLatestIndexOnFirstPointRead(t *testing.T) {
	keyKinds := []struct {
		name       string
		makeKey    func(v uint64) []byte
		latestIdx  func(m *AppendOnly, key []byte) (idx int, ok bool)
		latestSize func(m *AppendOnly) int
	}{
		{
			name: "short-inline",
			makeKey: func(v uint64) []byte {
				return []byte{byte('a' + v)}
			},
			latestIdx: func(m *AppendOnly, key []byte) (int, bool) {
				inlineKey, ok := appendOnlyInlineMapKeyFromBytes(key)
				if !ok || m.latestInline == nil {
					return 0, false
				}
				idx, ok := m.latestInline[inlineKey]
				return idx, ok
			},
			latestSize: func(m *AppendOnly) int {
				if m.latestInline == nil {
					return 0
				}
				return len(m.latestInline)
			},
		},
		{
			name: "long-string",
			makeKey: func(v uint64) []byte {
				return []byte(fmt.Sprintf("long-key-%08d", v))
			},
			latestIdx: func(m *AppendOnly, key []byte) (int, bool) {
				if m.latest == nil {
					return 0, false
				}
				idx, ok := m.latest[appendOnlyLookupKeyString(key)]
				return idx, ok
			},
			latestSize: func(m *AppendOnly) int {
				if m.latest == nil {
					return 0
				}
				return len(m.latest)
			},
		},
		{
			name: "8-byte",
			makeKey: func(v uint64) []byte {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, v)
				return k
			},
			latestIdx: func(m *AppendOnly, key []byte) (int, bool) {
				k64, ok := appendOnlyKeyU64(key)
				if !ok || m.latest64 == nil {
					return 0, false
				}
				idx, ok := m.latest64[k64]
				return idx, ok
			},
			latestSize: func(m *AppendOnly) int {
				if m.latest64 == nil {
					return 0
				}
				return len(m.latest64)
			},
		},
	}

	for _, kk := range keyKinds {
		kk := kk
		t.Run(kk.name, func(t *testing.T) {
			m := NewAppendOnlyWithCapacity(0)

			hi := kk.makeKey(2)
			lo := kk.makeKey(1)

			m.Set(hi, []byte("hi"))
			m.Set(lo, []byte("lo")) // force unordered

			m.mu.RLock()
			ordered := m.ordered
			dirty := m.latestDirty
			m.mu.RUnlock()
			if ordered {
				t.Fatalf("expected memtable to become unordered")
			}
			if !dirty {
				t.Fatalf("expected latest index to be dirty after order break")
			}
			m.mu.RLock()
			latestSizeBeforeGet := kk.latestSize(m)
			_, okBeforeGet := kk.latestIdx(m, lo)
			countBeforeGet := m.count
			m.mu.RUnlock()
			if latestSizeBeforeGet != 0 {
				t.Fatalf("expected latest index to remain unmaterialized on order break, size=%d", latestSizeBeforeGet)
			}
			if okBeforeGet {
				t.Fatalf("expected latest index lookup to miss before first read")
			}
			if countBeforeGet != 2 {
				t.Fatalf("count before Get=%d want 2", countBeforeGet)
			}

			if got, del, ok := m.Get(lo); !ok || del || string(got) != "lo" {
				t.Fatalf("Get(lo) = (%q,%v,%v), want (lo,false,true)", string(got), del, ok)
			}

			m.mu.RLock()
			dirty = m.latestDirty
			latestSize := kk.latestSize(m)
			m.mu.RUnlock()
			if dirty {
				t.Fatalf("expected point read to keep latest index clean (latestDirty=false)")
			}
			if latestSize < 2 {
				t.Fatalf("expected latest index to be populated, size=%d", latestSize)
			}

			m.Set(lo, []byte("lo2"))

			m.mu.RLock()
			dirty = m.latestDirty
			count := m.count
			idx, ok := kk.latestIdx(m, lo)
			m.mu.RUnlock()
			if dirty {
				t.Fatalf("expected writes to keep latest index clean after rebuild")
			}
			if !ok {
				t.Fatalf("expected latest index lookup to succeed after rebuild")
			}
			if idx != count-1 {
				t.Fatalf("latest index points at %d, want %d", idx, count-1)
			}
			if got, del, ok := m.Get(lo); !ok || del || string(got) != "lo2" {
				t.Fatalf("Get(lo after overwrite) = (%q,%v,%v), want (lo2,false,true)", string(got), del, ok)
			}

			m.Delete(lo)

			m.mu.RLock()
			dirty = m.latestDirty
			count = m.count
			idx, ok = kk.latestIdx(m, lo)
			m.mu.RUnlock()
			if dirty {
				t.Fatalf("expected deletes to keep latest index clean after rebuild")
			}
			if !ok {
				t.Fatalf("expected latest index lookup to succeed after delete")
			}
			if idx != count-1 {
				t.Fatalf("latest index points at %d after delete, want %d", idx, count-1)
			}
			if got, del, ok := m.Get(lo); !ok || !del || got != nil {
				t.Fatalf("Get(lo after delete) = (%v,%v,%v), want (nil,true,true)", got, del, ok)
			}
		})
	}
}

func TestAppendOnlyGet_EmptyKey_IncrementalLatestIndex(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)

	// Force the memtable into the unordered path so it uses the latest-index maps.
	m.Set([]byte("b"), []byte("1"))
	m.Set([]byte("a"), []byte("2"))

	// Trigger a point read to build the latest index (latestDirty=false) before
	// adding the empty key.
	if got, del, ok := m.Get([]byte("a")); !ok || del || string(got) != "2" {
		t.Fatalf("precondition Get(a) = (%q,%v,%v), want (2,false,true)", string(got), del, ok)
	}

	m.Set([]byte{}, []byte("empty"))

	if got, del, ok := m.Get([]byte{}); !ok || del || string(got) != "empty" {
		t.Fatalf("Get(empty key) = (%q,%v,%v), want (empty,false,true)", string(got), del, ok)
	}
	if got, ptr, flags, ok := m.GetEntry([]byte{}); !ok || string(got) != "empty" || ptr != (page.ValuePtr{}) || flags != node.FlagInline {
		t.Fatalf("GetEntry(empty key) = (%q,%+v,%d,%v), want (empty,zero,%d,true)", string(got), ptr, flags, ok, node.FlagInline)
	}

	m.Delete([]byte{})
	if got, del, ok := m.Get([]byte{}); !ok || !del || got != nil {
		t.Fatalf("Get(empty key after delete) = (%v,%v,%v), want (nil,true,true)", got, del, ok)
	}
	if got, ptr, flags, ok := m.GetEntry([]byte{}); !ok || got != nil || ptr != (page.ValuePtr{}) || flags != node.FlagTombstone {
		t.Fatalf("GetEntry(empty key after delete) = (%v,%+v,%d,%v), want (nil,zero,%d,true)", got, ptr, flags, ok, node.FlagTombstone)
	}
	m.mu.RLock()
	ent := &m.entries[m.count-1]
	inlineLen := appendOnlyEntryInlineKeyLen(ent)
	keyIndex := appendOnlyEntryKeyIndex(ent)
	encodedKey := m.appendOnlyEntryKey(ent)
	m.mu.RUnlock()
	if inlineLen != 0 || keyIndex == 0 {
		t.Fatalf("empty key entry encoding inlineLen=%d keyIndex=%d, want side-key encoding", inlineLen, keyIndex)
	}
	if encodedKey == nil || len(encodedKey) != 0 {
		t.Fatalf("encoded empty key = %v len=%d, want non-nil empty key", encodedKey, len(encodedKey))
	}
}

func TestAppendOnlyApplyBorrowValueSortedBatchTrusted_EmptyKeyPointer(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	ptr := page.ValuePtr{Offset: 7, Length: 11, FileID: 3}

	m.ApplyBorrowValueSortedBatchTrusted([]batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte{}, Value: []byte("empty-ptr"), IsPtr: true, ValuePtr: ptr},
		{Type: batchpkg.OpPut, Key: []byte("a"), Value: []byte("a")},
	}, true, nil)

	if !m.ordered {
		t.Fatal("expected empty-key trusted batch to stay ordered")
	}
	got, gotPtr, flags, ok := m.GetEntry([]byte{})
	if !ok || string(got) != "empty-ptr" || gotPtr != ptr || flags != node.FlagPointer {
		t.Fatalf("GetEntry(empty key pointer) = (%q,%+v,%d,%v), want (empty-ptr,%+v,%d,true)", string(got), gotPtr, flags, ok, ptr, node.FlagPointer)
	}

	it := m.NewIterator(nil, nil)
	defer it.Close()
	if !it.Valid() {
		t.Fatal("expected iterator to start at empty key")
	}
	if gotKey := it.UnsafeKey(); gotKey == nil || len(gotKey) != 0 {
		t.Fatalf("iterator empty key = %v len=%d, want non-nil empty key", gotKey, len(gotKey))
	}
	got, gotPtr, flags = it.UnsafeEntry()
	if string(got) != "empty-ptr" || gotPtr != ptr || flags != node.FlagPointer {
		t.Fatalf("iterator empty entry = (%q,%+v,%d), want (empty-ptr,%+v,%d)", string(got), gotPtr, flags, ptr, node.FlagPointer)
	}
}

func TestAppendOnlyLatestIndexShortInlineKeysSurviveEntryGrowth(t *testing.T) {
	m := NewAppendOnlyWithCapacityEstimatedEntryBytes(
		appendOnlyMinInitialEntries*appendOnlyEstimatedBytesPerEntryPointer,
		appendOnlyEstimatedBytesPerEntryPointer,
	)

	m.Set([]byte("b"), []byte("first"))
	m.Set([]byte("a"), []byte("target"))
	if got, del, ok := m.Get([]byte("a")); !ok || del || string(got) != "target" {
		t.Fatalf("precondition Get(a) = (%q,%v,%v), want (target,false,true)", string(got), del, ok)
	}

	for i := 0; i < appendOnlyMinInitialEntries; i++ {
		key := []byte(fmt.Sprintf("long-growth-key-%08d", i))
		m.Set(key, []byte("growth"))
	}

	if got, del, ok := m.Get([]byte("a")); !ok || del || string(got) != "target" {
		t.Fatalf("Get(a) after growth = (%q,%v,%v), want (target,false,true)", string(got), del, ok)
	}
}

func TestAppendOnlyMutableIteratorShortInlineKeyStableAfterClose(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("b"), []byte("first"))
	m.Set([]byte("a"), []byte("target"))
	arenaChunks := len(m.valueArena.chunks)
	arenaPos := m.valueArena.curPos

	it := m.NewIterator(nil, nil)
	if len(m.valueArena.chunks) != arenaChunks || m.valueArena.curPos != arenaPos {
		t.Fatalf("mutable iterator copied inline keys into memtable arena")
	}
	if !it.Valid() {
		t.Fatal("iterator unexpectedly invalid")
	}
	key := it.UnsafeKey()
	if got := string(key); got != "a" {
		t.Fatalf("iterator key=%q want a", got)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := string(key); got != "a" {
		t.Fatalf("iterator key after close=%q want a", got)
	}
}

func TestAppendOnlyOrderedIteratorShortInlineKeyStableAfterCloseAndGrowth(t *testing.T) {
	m := NewAppendOnlyWithCapacityEstimatedEntryBytes(
		appendOnlyMinInitialEntries*appendOnlyEstimatedBytesPerEntryPointer,
		appendOnlyEstimatedBytesPerEntryPointer,
	)
	m.Set([]byte("a"), []byte("target"))
	m.Set([]byte("long-growth-key"), []byte("growth"))

	it := m.NewIterator(nil, nil)
	if !it.Valid() {
		t.Fatal("iterator unexpectedly invalid")
	}
	key := it.UnsafeKey()
	if got := string(key); got != "a" {
		t.Fatalf("iterator key=%q want a", got)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for i := 0; i < appendOnlyMinInitialEntries; i++ {
		growthKey := []byte(fmt.Sprintf("long-growth-key-%08d", i))
		m.Set(growthKey, []byte("growth"))
	}
	if got := string(key); got != "a" {
		t.Fatalf("iterator key after close+growth=%q want a", got)
	}
}

func TestAppendOnlyIteratorLongKeyDoesNotExposeIndexStorage(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	const (
		targetKey = "long-key-a"
		otherKey  = "long-key-b"
	)
	m.Set([]byte(otherKey), []byte("other"))
	m.Set([]byte(targetKey), []byte("target")) // force unordered latest-index path

	it := m.NewIterator(nil, nil)
	if !it.Valid() {
		t.Fatal("iterator unexpectedly invalid")
	}
	key := it.UnsafeKey()
	if got := string(key); got != targetKey {
		t.Fatalf("iterator key=%q want %q", got, targetKey)
	}
	key[0] = 'X'
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got, del, ok := m.Get([]byte(targetKey)); !ok || del || string(got) != "target" {
		t.Fatalf("Get(%q) after iterator key mutation = (%q,%v,%v), want (target,false,true)", targetKey, got, del, ok)
	}
	if _, _, ok := m.Get([]byte("Xong-key-a")); ok {
		t.Fatalf("mutating iterator key created lookup hit for corrupted key")
	}
}

var appendOnlyGetBenchSink []byte
var appendOnlyGetBenchSinkBool bool

func benchmarkAppendOnlyOrderedGetMemtable(b *testing.B, count int) *AppendOnly {
	b.Helper()
	m := NewAppendOnlyWithCapacity(0)
	for i := 0; i < count; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		m.Set(key[:], []byte("v"))
	}
	if !m.ordered {
		b.Fatalf("expected ordered memtable for benchmark")
	}
	return m
}

func benchmarkAppendOnlyGetOrderedVsReverseScan(b *testing.B, count int) {
	ordered := benchmarkAppendOnlyOrderedGetMemtable(b, count)
	reverse := benchmarkAppendOnlyOrderedGetMemtable(b, count)
	reverse.mu.Lock()
	reverse.ordered = false
	reverse.latestDirty = true
	clear(reverse.latest)
	clear(reverse.latestInline)
	clear(reverse.latest64)
	reverse.mu.Unlock()

	var first [8]byte
	binary.BigEndian.PutUint64(first[:], 0)
	var miss [8]byte
	binary.BigEndian.PutUint64(miss[:], uint64(count+1))

	b.Run("ordered_hit_first", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v, del, ok := ordered.Get(first[:])
			appendOnlyGetBenchSink = v
			appendOnlyGetBenchSinkBool = del || ok
		}
	})
	b.Run("reverse_scan_pattern_hit_first", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v, del, ok := reverse.Get(first[:])
			appendOnlyGetBenchSink = v
			appendOnlyGetBenchSinkBool = del || ok
		}
	})
	b.Run("ordered_miss", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v, del, ok := ordered.Get(miss[:])
			appendOnlyGetBenchSink = v
			appendOnlyGetBenchSinkBool = del || ok
		}
	})
	b.Run("reverse_scan_pattern_miss", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v, del, ok := reverse.Get(miss[:])
			appendOnlyGetBenchSink = v
			appendOnlyGetBenchSinkBool = del || ok
		}
	})
}

func BenchmarkAppendOnlyGetOrderedVsReverseScan_10K(b *testing.B) {
	benchmarkAppendOnlyGetOrderedVsReverseScan(b, 10_000)
}

func BenchmarkAppendOnlyGetOrderedVsReverseScan_100K(b *testing.B) {
	benchmarkAppendOnlyGetOrderedVsReverseScan(b, 100_000)
}

func TestAppendOnlyIteratorUnorderedDoesNotBlockWriter(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k1"), []byte("v1")) // force unordered path

	it := m.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()

	done := make(chan struct{})
	go func() {
		m.Set([]byte("k3"), []byte("v3"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("writer blocked while unordered iterator was open")
	}
}
