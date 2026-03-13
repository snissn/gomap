package memtable

import (
	"encoding/binary"
	"testing"
	"time"

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

func TestAppendOnlyInlineGrowthSkipsIntermediateDoublingBelowCutoff(t *testing.T) {
	mt := &AppendOnly{
		entries:        make([]appendOnlyEntry, appendOnlyMinInitialEntries),
		baseEntriesLen: appendOnlyMinInitialEntries,
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
			name: "non-8-byte",
			makeKey: func(v uint64) []byte {
				return []byte{byte('a' + v)}
			},
			latestIdx: func(m *AppendOnly, key []byte) (int, bool) {
				if m.latest == nil {
					return 0, false
				}
				idx, ok := m.latest[appendOnlyKeyString(key)]
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
				t.Fatalf("expected latest index to start dirty")
			}

			if got, del, ok := m.Get(lo); !ok || del || string(got) != "lo" {
				t.Fatalf("Get(lo) = (%q,%v,%v), want (lo,false,true)", string(got), del, ok)
			}

			m.mu.RLock()
			dirty = m.latestDirty
			latestSize := kk.latestSize(m)
			m.mu.RUnlock()
			if dirty {
				t.Fatalf("expected point read to rebuild latest index (latestDirty=false)")
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
