package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
)

func TestHashSortedApplyStealSortedBatch_AppendAndFallback(t *testing.T) {
	m := NewHashSorted()
	m.SetSteal([]byte("b"), []byte("v1"))

	appendEntries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("c"), Value: []byte("v2")},
		{Type: batchpkg.OpDelete, Key: []byte("d")},
	}
	m.ApplyStealSortedBatch(appendEntries, nil)

	if got, del, ok := m.Get([]byte("c")); !ok || del || string(got) != "v2" {
		t.Fatalf("key c mismatch: ok=%v del=%v val=%q", ok, del, string(got))
	}
	if got, del, ok := m.Get([]byte("d")); !ok || !del || got != nil {
		t.Fatalf("key d tombstone mismatch: ok=%v del=%v val=%v", ok, del, got)
	}

	// First key <= current max, so this should use the regular update path.
	fallbackEntries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("a"), Value: []byte("va")},
		{Type: batchpkg.OpPut, Key: []byte("b"), Value: []byte("vb")},
	}
	m.ApplyStealSortedBatch(fallbackEntries, nil)

	if got, del, ok := m.Get([]byte("a")); !ok || del || string(got) != "va" {
		t.Fatalf("key a mismatch: ok=%v del=%v val=%q", ok, del, string(got))
	}
	if got, del, ok := m.Get([]byte("b")); !ok || del || string(got) != "vb" {
		t.Fatalf("key b mismatch after update: ok=%v del=%v val=%q", ok, del, string(got))
	}
}

func TestHashSortedApplyCopySortedBatchTrusted_AppendAndFallback(t *testing.T) {
	m := NewHashSorted()
	m.Set([]byte("b"), []byte("v1"))

	keyC := []byte("c")
	valueC := []byte("v2")
	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: keyC, Value: valueC},
		{Type: batchpkg.OpDelete, Key: []byte("d")},
	}
	if borrowed := m.ApplyCopySortedBatchTrusted(entries, true, true, nil); !borrowed {
		t.Fatalf("HashSorted did not report borrowed values")
	}
	keyC[0] = 'z'
	valueC[0] = 'V'

	if got, del, ok := m.Get([]byte("c")); !ok || del || string(got) != "V2" {
		t.Fatalf("key c mismatch: ok=%v del=%v val=%q", ok, del, string(got))
	}
	if got, del, ok := m.Get([]byte("d")); !ok || !del || got != nil {
		t.Fatalf("key d tombstone mismatch: ok=%v del=%v val=%v", ok, del, got)
	}

	fallbackEntries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("a"), Value: []byte("va")},
		{Type: batchpkg.OpPut, Key: []byte("b"), Value: []byte("vb")},
	}
	if borrowed := m.ApplyCopySortedBatchTrusted(fallbackEntries, true, true, nil); !borrowed {
		t.Fatalf("fallback path did not report borrowed values")
	}

	if got, del, ok := m.Get([]byte("a")); !ok || del || string(got) != "va" {
		t.Fatalf("key a mismatch: ok=%v del=%v val=%q", ok, del, string(got))
	}
	if got, del, ok := m.Get([]byte("b")); !ok || del || string(got) != "vb" {
		t.Fatalf("key b mismatch after update: ok=%v del=%v val=%q", ok, del, string(got))
	}
}

func TestHashSortedApplyCopySortedBatchTrusted_CopyModeProtectsCallerMutation(t *testing.T) {
	m := NewHashSorted()
	key := []byte("a")
	value := []byte("value")
	want := bytes.Clone(value)

	if borrowed := m.ApplyCopySortedBatchTrusted([]batchpkg.Entry{{
		Type:  batchpkg.OpPut,
		Key:   key,
		Value: value,
	}}, false, true, nil); borrowed {
		t.Fatalf("HashSorted reported borrowed values with borrowValues=false")
	}
	key[0] = 'z'
	value[0] = 'X'

	got, del, ok := m.Get([]byte("a"))
	if !ok || del || !bytes.Equal(got, want) {
		t.Fatalf("stored value mismatch after caller mutation: ok=%v del=%v got=%q want=%q", ok, del, got, want)
	}
}

func TestHashSortedApplyStealSortedBatch_DuplicateKeyForcesFallback(t *testing.T) {
	m := NewHashSorted()
	m.SetSteal([]byte("b"), []byte("v0"))

	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("c"), Value: []byte("v1")},
		{Type: batchpkg.OpPut, Key: []byte("c"), Value: []byte("v2")},
	}
	m.ApplyStealSortedBatch(entries, nil)

	if got, del, ok := m.Get([]byte("c")); !ok || del || string(got) != "v2" {
		t.Fatalf("key c mismatch after duplicate fallback: ok=%v del=%v val=%q", ok, del, string(got))
	}
	if got := m.Len(); got != 2 {
		t.Fatalf("len=%d want 2", got)
	}
}

func TestHashSortedApplyCopySortedBatchTrusted_MixedPendingChunkNotMarkedSorted(t *testing.T) {
	indexer := &HashSortedIndexer{ch: make(chan hashSortedIndexWork, 1)}
	m := NewHashSortedWithCapacityAndIndexer(0, indexer)

	// Prior generic inserts leave an unsealed, unsorted pending chunk.
	m.Set([]byte("m"), []byte("vm"))
	m.Set([]byte("a"), []byte("va"))

	entries := make([]batchpkg.Entry, hashSortedSealKeysThreshold-2)
	for i := range entries {
		entries[i] = batchpkg.Entry{
			Type:  batchpkg.OpPut,
			Key:   []byte(fmt.Sprintf("n%05d", i)),
			Value: []byte("v"),
		}
	}
	m.ApplyCopySortedBatchTrusted(entries, false, true, nil)

	select {
	case work := <-indexer.ch:
		if work.sorted {
			t.Fatalf("mixed pending chunk marked sorted")
		}
		if len(work.keys) != hashSortedSealKeysThreshold {
			t.Fatalf("chunk len=%d want %d", len(work.keys), hashSortedSealKeysThreshold)
		}
		if sort.StringsAreSorted(work.keys) {
			t.Fatalf("test setup expected mixed chunk to require sorting")
		}
	default:
		t.Fatalf("expected sealed mixed pending chunk")
	}
}

func TestHashSortedApplyCopySortedBatchTrusted_ConsecutiveSortedPendingChunkMarkedSorted(t *testing.T) {
	indexer := &HashSortedIndexer{ch: make(chan hashSortedIndexWork, 1)}
	m := NewHashSortedWithCapacityAndIndexer(0, indexer)

	entries := make([]batchpkg.Entry, hashSortedSealKeysThreshold)
	for i := range entries {
		entries[i] = batchpkg.Entry{
			Type:  batchpkg.OpPut,
			Key:   []byte(fmt.Sprintf("n%05d", i)),
			Value: []byte("v"),
		}
	}
	m.ApplyCopySortedBatchTrusted(entries[:hashSortedSealKeysThreshold/2], false, true, nil)
	m.ApplyCopySortedBatchTrusted(entries[hashSortedSealKeysThreshold/2:], false, true, nil)

	select {
	case work := <-indexer.ch:
		if !work.sorted {
			t.Fatalf("pure sorted pending chunk not marked sorted")
		}
		if !sort.StringsAreSorted(work.keys) {
			t.Fatalf("pure sorted chunk is not sorted")
		}
	default:
		t.Fatalf("expected sealed sorted pending chunk")
	}
}

func TestHashSortedPendingKeysSmallSingleStartsSmall(t *testing.T) {
	m := NewHashSorted()
	m.Set([]byte("key"), []byte("value"))

	m.mu.RLock()
	gotLen := len(m.pendingKeys)
	gotCap := cap(m.pendingKeys)
	m.mu.RUnlock()

	if gotLen != 1 {
		t.Fatalf("pending len=%d want 1", gotLen)
	}
	if gotCap != hashSortedPendingKeysInitCap {
		t.Fatalf("pending cap=%d want %d", gotCap, hashSortedPendingKeysInitCap)
	}
}

func TestHashSortedPendingKeysSmallBatchStartsSmall(t *testing.T) {
	const batchSize = 32
	m := NewHashSorted()
	m.ApplyCopySortedBatchTrusted(hashSortedTestEntries(batchSize), false, true, nil)

	m.mu.RLock()
	gotLen := len(m.pendingKeys)
	gotCap := cap(m.pendingKeys)
	m.mu.RUnlock()

	if gotLen != batchSize {
		t.Fatalf("pending len=%d want %d", gotLen, batchSize)
	}
	if gotCap != hashSortedPendingKeysInitCap {
		t.Fatalf("pending cap=%d want %d", gotCap, hashSortedPendingKeysInitCap)
	}
}

func TestHashSortedPendingKeysUpgradeThresholdPreallocatesSealCap(t *testing.T) {
	const batchSize = hashSortedPendingKeysUpgradeThreshold
	m := NewHashSorted()
	m.ApplyCopySortedBatchTrusted(hashSortedTestEntries(batchSize), false, true, nil)

	m.mu.RLock()
	gotLen := len(m.pendingKeys)
	gotCap := cap(m.pendingKeys)
	m.mu.RUnlock()

	if gotLen != batchSize {
		t.Fatalf("pending len=%d want %d", gotLen, batchSize)
	}
	if gotCap != hashSortedSealKeysThreshold {
		t.Fatalf("pending cap=%d want %d", gotCap, hashSortedSealKeysThreshold)
	}
}

func TestHashSortedPendingKeysSealHandoffUsesFreshBackingArray(t *testing.T) {
	indexer := &HashSortedIndexer{ch: make(chan hashSortedIndexWork, 1)}
	m := NewHashSortedWithCapacityAndIndexer(0, indexer)

	m.ApplyCopySortedBatchTrusted(hashSortedTestEntries(hashSortedSealKeysThreshold), false, true, nil)

	var work hashSortedIndexWork
	select {
	case work = <-indexer.ch:
	default:
		t.Fatalf("expected sealed pending-key chunk")
	}
	if len(work.keys) != hashSortedSealKeysThreshold {
		t.Fatalf("sealed chunk len=%d want %d", len(work.keys), hashSortedSealKeysThreshold)
	}
	sealedFirst := work.keys[0]
	sealedLast := work.keys[len(work.keys)-1]
	sealedBacking := &work.keys[0]

	m.Set([]byte("z-next"), []byte("value"))

	m.mu.RLock()
	pendingLen := len(m.pendingKeys)
	var pendingBacking *string
	if pendingLen > 0 {
		pendingBacking = &m.pendingKeys[0]
	}
	m.mu.RUnlock()

	if pendingLen != 1 {
		t.Fatalf("pending len after post-seal append=%d want 1", pendingLen)
	}
	if pendingBacking == sealedBacking {
		t.Fatalf("post-seal append reused sealed chunk backing array")
	}
	if work.keys[0] != sealedFirst || work.keys[len(work.keys)-1] != sealedLast {
		t.Fatalf("sealed chunk mutated after post-seal append: first=%q/%q last=%q/%q", work.keys[0], sealedFirst, work.keys[len(work.keys)-1], sealedLast)
	}
}

func BenchmarkHashSortedApplyCopySortedBatchTrusted(b *testing.B) {
	const batchSize = 32
	const valueSize = 128
	var keyBufs [batchSize][8]byte
	entries := make([]batchpkg.Entry, batchSize)
	value := bytes.Repeat([]byte{0x7a}, valueSize)

	for _, tc := range []struct {
		name         string
		borrowValues bool
	}{
		{name: "copy_values", borrowValues: false},
		{name: "borrow_values", borrowValues: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			m := NewHashSorted()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				base := uint64(i * batchSize)
				for j := 0; j < batchSize; j++ {
					binary.BigEndian.PutUint64(keyBufs[j][:], base+uint64(j))
					entries[j] = batchpkg.Entry{
						Type:  batchpkg.OpPut,
						Key:   keyBufs[j][:],
						Value: value,
					}
				}
				m.ApplyCopySortedBatchTrusted(entries, tc.borrowValues, true, nil)
			}
		})
	}
}

func BenchmarkHashSortedPendingKeys(b *testing.B) {
	value := []byte("value")

	b.Run("single_key", func(b *testing.B) {
		key := []byte("key")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			m := NewHashSorted()
			m.Set(key, value)
		}
	})

	for _, tc := range []struct {
		name string
		n    int
	}{
		{name: "small_batch_32", n: 32},
		{name: "upgrade_threshold_4096", n: hashSortedPendingKeysUpgradeThreshold},
		{name: "seal_threshold_32768", n: hashSortedSealKeysThreshold},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			entries := hashSortedTestEntries(tc.n)
			indexer := &HashSortedIndexer{ch: make(chan hashSortedIndexWork, 1)}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				m := NewHashSortedWithCapacityAndIndexer(0, indexer)
				m.ApplyCopySortedBatchTrusted(entries, false, true, nil)
				if tc.n >= hashSortedSealKeysThreshold {
					<-indexer.ch
				}
			}
		})
	}
}

func hashSortedTestEntries(n int) []batchpkg.Entry {
	entries := make([]batchpkg.Entry, n)
	for i := range entries {
		entries[i] = batchpkg.Entry{
			Type:  batchpkg.OpPut,
			Key:   []byte(fmt.Sprintf("k%08d", i)),
			Value: []byte("value"),
		}
	}
	return entries
}
