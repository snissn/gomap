package db

import (
	"bytes"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafPageCacheTestFallback struct {
	readUnsafeCalls   int
	readUnsafeToCalls int
	err               error
	data              []byte
	checksumEnabled   bool
}

func (f *leafPageCacheTestFallback) Read(ptr page.ValuePtr) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	return append([]byte(nil), f.data...), nil
}

func (f *leafPageCacheTestFallback) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	f.readUnsafeCalls++
	if f.err != nil {
		return nil, f.err
	}
	return f.data, nil
}

func (f *leafPageCacheTestFallback) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	f.readUnsafeToCalls++
	if f.err != nil {
		return nil, false, f.err
	}
	if cap(dst) >= len(f.data) {
		dst = dst[:len(f.data)]
		copy(dst, f.data)
		return dst, true, nil
	}
	return f.data, false, nil
}

func (f *leafPageCacheTestFallback) ReadChecksumEnabled() bool {
	return f.checksumEnabled
}

type leafPageCacheUnsafeOnlyFallback struct {
	readUnsafeCalls int
	err             error
	data            []byte
}

func (f *leafPageCacheUnsafeOnlyFallback) Read(ptr page.ValuePtr) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	return append([]byte(nil), f.data...), nil
}

func (f *leafPageCacheUnsafeOnlyFallback) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	f.readUnsafeCalls++
	if f.err != nil {
		return nil, f.err
	}
	return f.data, nil
}

func TestCachedLeafPageReaderHitAvoidsFallback(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	readPtr := ptr
	readPtr.RecordLengthHint = 0
	leaf := bytes.Repeat([]byte{0x42}, page.PageSize)
	cache.store(ptr, leaf)

	fallback := &leafPageCacheTestFallback{err: errors.New("fallback should not be used")}
	reader := newCachedLeafPageReader(cache, fallback)

	dst := make([]byte, 0, page.PageSize)
	got, usedDst, err := reader.ReadUnsafeTo(readPtr.ValuePtr(), dst)
	if err != nil {
		t.Fatalf("ReadUnsafeTo: %v", err)
	}
	if !usedDst {
		t.Fatalf("expected cache hit to copy into caller dst")
	}
	if !bytes.Equal(got, leaf) {
		t.Fatalf("cached leaf mismatch")
	}
	if fallback.readUnsafeCalls != 0 || fallback.readUnsafeToCalls != 0 {
		t.Fatalf("fallback calls unsafe=%d unsafeTo=%d, want zero", fallback.readUnsafeCalls, fallback.readUnsafeToCalls)
	}

	stats := cache.stats()
	if stats.Hits != 1 || stats.Misses != 0 || stats.Stores != 1 {
		t.Fatalf("stats=%+v, want one hit and one store", stats)
	}
}

func TestCachedLeafPageReaderDisabledBypassesCache(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	cachedLeaf := bytes.Repeat([]byte{0x42}, page.PageSize)
	fallbackLeaf := bytes.Repeat([]byte{0x24}, page.PageSize)
	cache.store(ptr, cachedLeaf)
	cache.disabled.Store(true)

	fallback := &leafPageCacheTestFallback{data: fallbackLeaf}
	reader := newCachedLeafPageReader(cache, fallback)

	dst := make([]byte, 0, page.PageSize)
	got, usedDst, cacheHit, err := reader.ReadUnsafeToWithCacheHit(ptr.ValuePtr(), dst)
	if err != nil {
		t.Fatalf("ReadUnsafeToWithCacheHit: %v", err)
	}
	if cacheHit {
		t.Fatal("cacheHit=true, want disabled cache bypass")
	}
	if !usedDst {
		t.Fatalf("expected fallback to copy into caller dst")
	}
	if !bytes.Equal(got, fallbackLeaf) {
		t.Fatalf("got cached data while cache disabled")
	}
	if fallback.readUnsafeToCalls != 1 {
		t.Fatalf("fallback ReadUnsafeTo calls=%d want 1", fallback.readUnsafeToCalls)
	}
}

func TestCachedLeafPageReaderReadUnsafeToWithCacheHitReportsHit(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x42}, page.PageSize)
	cache.store(ptr, leaf)

	fallback := &leafPageCacheTestFallback{err: errors.New("fallback should not be used")}
	reader := newCachedLeafPageReader(cache, fallback)

	dst := make([]byte, 0, page.PageSize)
	got, usedDst, cacheHit, err := reader.ReadUnsafeToWithCacheHit(ptr.ValuePtr(), dst)
	if err != nil {
		t.Fatalf("ReadUnsafeToWithCacheHit: %v", err)
	}
	if !cacheHit {
		t.Fatalf("expected cache hit source")
	}
	if !usedDst {
		t.Fatalf("expected cache hit to copy into caller dst")
	}
	if !bytes.Equal(got, leaf) {
		t.Fatalf("cached leaf mismatch")
	}
	if fallback.readUnsafeCalls != 0 || fallback.readUnsafeToCalls != 0 {
		t.Fatalf("fallback calls unsafe=%d unsafeTo=%d, want zero", fallback.readUnsafeCalls, fallback.readUnsafeToCalls)
	}
}

func TestCachedLeafPageReaderHitReturnsOwnedBytes(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x42}, page.PageSize)
	cache.store(ptr, leaf)

	fallback := &leafPageCacheTestFallback{err: errors.New("fallback should not be used")}
	reader := newCachedLeafPageReader(cache, fallback)

	got, err := reader.ReadUnsafe(ptr.ValuePtr())
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	got[0] = 0
	dst := make([]byte, 0, page.PageSize)
	again, usedDst, err := reader.ReadUnsafeTo(ptr.ValuePtr(), dst)
	if err != nil {
		t.Fatalf("ReadUnsafeTo: %v", err)
	}
	if !usedDst {
		t.Fatalf("expected cache hit to copy into caller dst")
	}
	if again[0] != 0x42 {
		t.Fatalf("cache hit returned mutable cache backing; first byte=%x", again[0])
	}
}

func TestCachedLeafPageReaderHitWithSmallDstReturnsOwnedBytes(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x42}, page.PageSize)
	cache.store(ptr, leaf)

	fallback := &leafPageCacheTestFallback{err: errors.New("fallback should not be used")}
	reader := newCachedLeafPageReader(cache, fallback)

	got, usedDst, err := reader.ReadUnsafeTo(ptr.ValuePtr(), nil)
	if err != nil {
		t.Fatalf("ReadUnsafeTo: %v", err)
	}
	if usedDst {
		t.Fatalf("nil dst should not report usedDst")
	}
	got[0] = 0
	dst := make([]byte, 0, page.PageSize)
	again, usedDst, err := reader.ReadUnsafeTo(ptr.ValuePtr(), dst)
	if err != nil {
		t.Fatalf("ReadUnsafeTo again: %v", err)
	}
	if !usedDst {
		t.Fatalf("expected second cache hit to copy into caller dst")
	}
	if again[0] != 0x42 {
		t.Fatalf("small-dst cache hit returned mutable cache backing; first byte=%x", again[0])
	}
}

func TestCachedLeafPageReaderMissUsesFallback(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	fallbackData := bytes.Repeat([]byte{0x24}, page.PageSize)
	fallback := &leafPageCacheTestFallback{data: fallbackData}
	reader := newCachedLeafPageReader(cache, fallback)

	got, usedDst, err := reader.ReadUnsafeTo(ptr.ValuePtr(), nil)
	if err != nil {
		t.Fatalf("ReadUnsafeTo: %v", err)
	}
	if usedDst {
		t.Fatalf("fallback without dst should not report usedDst")
	}
	if !bytes.Equal(got, fallbackData) {
		t.Fatalf("fallback data mismatch")
	}
	if fallback.readUnsafeToCalls != 1 {
		t.Fatalf("fallback ReadUnsafeTo calls=%d, want 1", fallback.readUnsafeToCalls)
	}
	if stats := cache.stats(); stats.Misses != 1 {
		t.Fatalf("misses=%d, want 1", stats.Misses)
	}
}

func TestCachedLeafPageReaderMissReportsNoCacheHit(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x24}, page.PageSize)
	fallback := &leafPageCacheTestFallback{data: leaf}
	reader := newCachedLeafPageReader(cache, fallback)

	dst := make([]byte, 0, page.PageSize)
	got, usedDst, cacheHit, err := reader.ReadUnsafeToWithCacheHit(ptr.ValuePtr(), dst)
	if err != nil {
		t.Fatalf("ReadUnsafeToWithCacheHit: %v", err)
	}
	if cacheHit {
		t.Fatalf("first read should miss cache")
	}
	if !usedDst {
		t.Fatalf("expected fallback to copy into dst")
	}
	if !bytes.Equal(got, leaf) {
		t.Fatalf("fallback leaf mismatch")
	}
	if fallback.readUnsafeToCalls != 1 {
		t.Fatalf("fallback ReadUnsafeTo calls=%d, want 1", fallback.readUnsafeToCalls)
	}
	if stats := cache.stats(); stats.Misses != 1 || stats.Stores != 0 || stats.Hits != 0 {
		t.Fatalf("stats=%+v, want one miss and no store", stats)
	}
}

func TestCachedLeafPageReaderReadMissAdmissionStoresRepeatedMiss(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x24}, page.PageSize)
	fallback := &leafPageCacheTestFallback{data: leaf, checksumEnabled: true}
	reader := newCachedLeafPageReader(cache, fallback)

	dst := make([]byte, 0, page.PageSize)
	for i := 0; i < 2; i++ {
		got, usedDst, cacheHit, err := reader.ReadUnsafeToWithCacheHit(ptr.ValuePtr(), dst[:0])
		if err != nil {
			t.Fatalf("ReadUnsafeToWithCacheHit miss %d: %v", i, err)
		}
		if cacheHit {
			t.Fatalf("miss %d cacheHit=true, want false before admission is served", i)
		}
		if !usedDst || !bytes.Equal(got, leaf) {
			t.Fatalf("miss %d got usedDst=%v equal=%v", i, usedDst, bytes.Equal(got, leaf))
		}
	}
	if fallback.readUnsafeToCalls != 2 {
		t.Fatalf("fallback ReadUnsafeTo calls=%d, want 2", fallback.readUnsafeToCalls)
	}
	stats := cache.stats()
	if stats.Misses != 2 || stats.ReadMissAdmissionSkips != 1 || stats.ReadMissAdmissionStores != 1 || stats.Stores != 1 {
		t.Fatalf("stats after repeated miss=%+v, want one skipped candidate and one store", stats)
	}

	got, usedDst, cacheHit, err := reader.ReadUnsafeToWithCacheHit(ptr.ValuePtr(), dst[:0])
	if err != nil {
		t.Fatalf("ReadUnsafeToWithCacheHit hit: %v", err)
	}
	if !cacheHit || !usedDst || !bytes.Equal(got, leaf) {
		t.Fatalf("cache hit got cacheHit=%v usedDst=%v equal=%v", cacheHit, usedDst, bytes.Equal(got, leaf))
	}
	if fallback.readUnsafeToCalls != 2 {
		t.Fatalf("fallback calls after cache hit=%d, want 2", fallback.readUnsafeToCalls)
	}

	_, _, state, ok := cache.getToWithState(ptr, dst[:0])
	if !ok || !state.RecordChecksumVerified || state.PageChecksumVerified {
		t.Fatalf("checksum state=%+v ok=%v, want record verified only", state, ok)
	}
	if !cache.markPageChecksumVerified(ptr) {
		t.Fatal("record-verified read-miss admission should accept page checksum mark")
	}
}

func TestCachedLeafPageReaderReadMissAdmissionUnknownChecksumStaysUnverified(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x24}, page.PageSize)
	fallback := &leafPageCacheUnsafeOnlyFallback{data: leaf}
	reader := newCachedLeafPageReader(cache, fallback)

	for i := 0; i < 2; i++ {
		if _, _, err := reader.ReadUnsafeTo(ptr.ValuePtr(), make([]byte, 0, page.PageSize)); err != nil {
			t.Fatalf("ReadUnsafeTo miss %d: %v", i, err)
		}
	}
	if stats := cache.stats(); stats.ReadMissAdmissionStores != 1 {
		t.Fatalf("stats=%+v, want one read-miss admission store", stats)
	}
	if cache.markPageChecksumVerified(ptr) {
		t.Fatal("unknown-checksum read-miss admission should not accept page checksum mark")
	}
}

var cachedLeafPageReaderLZ4BenchSink byte

type leafPageCacheLZ4BenchFallback struct {
	encoded []byte
	calls   int
}

func (f *leafPageCacheLZ4BenchFallback) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	f.calls++
	out := make([]byte, page.PageSize)
	if _, err := lz4.UncompressBlock(f.encoded, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (f *leafPageCacheLZ4BenchFallback) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	f.calls++
	if cap(dst) < page.PageSize {
		dst = make([]byte, page.PageSize)
	} else {
		dst = dst[:page.PageSize]
	}
	n, err := lz4.UncompressBlock(f.encoded, dst)
	if err != nil {
		return nil, false, err
	}
	return dst[:n], true, nil
}

func (f *leafPageCacheLZ4BenchFallback) ReadChecksumEnabled() bool { return true }

func BenchmarkCachedLeafPageReaderRepeatedLZ4ReadMissAdmission(b *testing.B) {
	leaf := bytes.Repeat([]byte("leaf-page|"), (page.PageSize/10)+1)
	leaf = leaf[:page.PageSize]
	encoded := make([]byte, lz4.CompressBlockBound(len(leaf)))
	n, err := lz4.CompressBlock(leaf, encoded, nil)
	if err != nil || n <= 0 {
		b.Fatalf("compress: n=%d err=%v", n, err)
	}
	fallback := &leafPageCacheLZ4BenchFallback{encoded: encoded[:n]}
	cache := newLeafPageReadCache(64)
	reader := newCachedLeafPageReader(cache, fallback)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: uint32(n)}
	dst := make([]byte, 0, page.PageSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, _, _, err := reader.ReadUnsafeToWithCacheHit(ptr.ValuePtr(), dst[:0])
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		cachedLeafPageReaderLZ4BenchSink ^= got[0]
	}
	b.StopTimer()
	if b.N > 0 {
		b.ReportMetric(float64(fallback.calls)/float64(b.N), "fallback_calls/op")
	}
}

func TestCachedLeafPageReaderMissUsesReadUnsafeFallback(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	fallbackData := bytes.Repeat([]byte{0x24}, page.PageSize)
	fallback := &leafPageCacheUnsafeOnlyFallback{data: fallbackData}
	reader := newCachedLeafPageReader(cache, fallback)

	got, usedDst, err := reader.ReadUnsafeTo(ptr.ValuePtr(), make([]byte, 0, page.PageSize))
	if err != nil {
		t.Fatalf("ReadUnsafeTo: %v", err)
	}
	if usedDst {
		t.Fatalf("ReadUnsafe fallback should not report usedDst")
	}
	if !bytes.Equal(got, fallbackData) {
		t.Fatalf("fallback data mismatch")
	}
	if fallback.readUnsafeCalls != 1 {
		t.Fatalf("fallback ReadUnsafe calls=%d, want 1", fallback.readUnsafeCalls)
	}
	if stats := cache.stats(); stats.Misses != 1 {
		t.Fatalf("misses=%d, want 1", stats.Misses)
	}
}

func TestValueReaderLeafLogPageUnsafeToUsesCacheHit(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x33}, page.PageSize)
	cache.store(ptr, leaf)

	reader := valueReader{
		vlogs:         &leafPageCacheTestFallback{err: errors.New("fallback should not be used")},
		leafPageCache: cache,
	}
	dst := make([]byte, 0, page.PageSize)
	got, usedDst, err := reader.ReadLeafLogPageUnsafeTo(ptr, dst)
	if err != nil {
		t.Fatalf("ReadLeafLogPageUnsafeTo: %v", err)
	}
	if !usedDst {
		t.Fatal("expected cache hit to copy into dst")
	}
	if !bytes.Equal(got, leaf) {
		t.Fatal("cached leaf mismatch")
	}
	if stats := cache.stats(); stats.Hits != 1 || stats.Misses != 0 || stats.Stores != 1 {
		t.Fatalf("stats=%+v, want one hit and one store", stats)
	}
}

func TestValueReaderLeafLogPageUnsafeToSmallDstBypassesCache(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	cachedLeaf := bytes.Repeat([]byte{0x33}, page.PageSize)
	fallbackLeaf := bytes.Repeat([]byte{0x55}, page.PageSize)
	cache.store(ptr, cachedLeaf)

	fallback := &leafPageCacheTestFallback{data: fallbackLeaf}
	reader := valueReader{
		vlogs:         fallback,
		leafPageCache: cache,
	}
	got, usedDst, err := reader.ReadLeafLogPageUnsafeTo(ptr, nil)
	if err != nil {
		t.Fatalf("ReadLeafLogPageUnsafeTo: %v", err)
	}
	if usedDst {
		t.Fatal("nil dst should not report usedDst")
	}
	if !bytes.Equal(got, fallbackLeaf) {
		t.Fatal("small-dst read should use fallback instead of copying from cache")
	}
	if fallback.readUnsafeToCalls != 1 {
		t.Fatalf("fallback ReadUnsafeTo calls=%d, want 1", fallback.readUnsafeToCalls)
	}
	if stats := cache.stats(); stats.Hits != 0 || stats.Misses != 0 || stats.Stores != 1 {
		t.Fatalf("stats=%+v, want cache bypass without extra store", stats)
	}
}

func TestValueReaderLeafLogPageUnsafeViewLocksUntilRelease(t *testing.T) {
	cache := newLeafPageReadCache(1)
	ptrA := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	ptrB := page.LeafLogPtr{FileID: 9, Offset: 256, RecordLengthHint: 4096}
	leafA := bytes.Repeat([]byte{0x33}, page.PageSize)
	leafB := bytes.Repeat([]byte{0x55}, page.PageSize)
	cache.store(ptrA, leafA)

	reader := valueReader{
		vlogs:         &leafPageCacheTestFallback{err: errors.New("fallback should not be used")},
		leafPageCache: cache,
	}
	view, lease, ok, err := reader.ReadLeafLogPageUnsafeView(ptrA)
	if err != nil {
		t.Fatalf("ReadLeafLogPageUnsafeView: %v", err)
	}
	if !ok || lease == nil || !bytes.Equal(view, leafA) {
		t.Fatalf("cache view mismatch ok=%v leaseNil=%v", ok, lease == nil)
	}

	done := make(chan struct{})
	go func() {
		cache.store(ptrB, leafB)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("cache store completed while view lease was held")
	case <-time.After(10 * time.Millisecond):
	}
	lease.ReleaseLeafLogPageView()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cache store did not complete after view release")
	}
}

func TestValueReaderLeafLogPageUnsafeToAdmitsRepeatedReadMiss(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x44}, page.PageSize)
	fallback := &leafPageCacheTestFallback{data: leaf}
	reader := valueReader{
		vlogs:         fallback,
		leafPageCache: cache,
	}

	dst := make([]byte, 0, page.PageSize)
	got, usedDst, err := reader.ReadLeafLogPageUnsafeTo(ptr, dst)
	if err != nil {
		t.Fatalf("ReadLeafLogPageUnsafeTo: %v", err)
	}
	if !usedDst {
		t.Fatal("expected fallback ReadUnsafeTo to use dst")
	}
	if !bytes.Equal(got, leaf) {
		t.Fatal("fallback leaf mismatch")
	}
	if fallback.readUnsafeToCalls != 1 {
		t.Fatalf("fallback ReadUnsafeTo calls=%d, want 1", fallback.readUnsafeToCalls)
	}

	if stats := cache.stats(); stats.Hits != 0 || stats.Misses != 1 || stats.Stores != 0 || stats.ReadMissAdmissionSkips != 1 {
		t.Fatalf("stats=%+v, want first miss to skip read-miss admission", stats)
	}

	again, usedDst, err := reader.ReadLeafLogPageUnsafeTo(ptr, dst[:0])
	if err != nil {
		t.Fatalf("ReadLeafLogPageUnsafeTo second miss: %v", err)
	}
	if !usedDst || !bytes.Equal(again, leaf) {
		t.Fatalf("second fallback leaf mismatch usedDst=%v", usedDst)
	}
	if fallback.readUnsafeToCalls != 2 {
		t.Fatalf("fallback calls after second miss=%d, want 2", fallback.readUnsafeToCalls)
	}
	if stats := cache.stats(); stats.Hits != 0 || stats.Misses != 2 || stats.Stores != 1 || stats.ReadMissAdmissionStores != 1 {
		t.Fatalf("stats=%+v, want repeated read miss admitted", stats)
	}

	fallback.err = errors.New("fallback should not be used after repeated read-miss admission")
	third, usedDst, err := reader.ReadLeafLogPageUnsafeTo(ptr, dst[:0])
	if err != nil {
		t.Fatalf("ReadLeafLogPageUnsafeTo cached: %v", err)
	}
	if !usedDst || !bytes.Equal(third, leaf) {
		t.Fatalf("cached leaf mismatch usedDst=%v", usedDst)
	}
	if fallback.readUnsafeToCalls != 2 {
		t.Fatalf("fallback calls after cache hit=%d, want 2", fallback.readUnsafeToCalls)
	}
	if stats := cache.stats(); stats.Hits != 1 || stats.Misses != 2 || stats.Stores != 1 {
		t.Fatalf("stats=%+v, want two misses, one admitted store, one hit", stats)
	}
}

func TestLeafPageReadCacheSetAssociativeRetainsDirectMapCollisions(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptrs := findDirectSlotCollidingLeafPagePtrs(t, cache, leafPageReadCacheWays)
	oldSlot := leafPageReadCacheDirectSlotIndex(cache, newLeafPageReadCacheKey(ptrs[0]))
	bucket := cache.bucketIndex(newLeafPageReadCacheKey(ptrs[0]))
	for _, ptr := range ptrs[1:] {
		key := newLeafPageReadCacheKey(ptr)
		if got := leafPageReadCacheDirectSlotIndex(cache, key); got != oldSlot {
			t.Fatalf("test ptr did not collide under old direct slot: got %d want %d", got, oldSlot)
		}
		if got := cache.bucketIndex(key); got != bucket {
			t.Fatalf("test ptr did not map to same new bucket: got %d want %d", got, bucket)
		}
	}

	leaves := make([][]byte, len(ptrs))
	for i, ptr := range ptrs {
		leaves[i] = bytes.Repeat([]byte{byte(0x40 + i)}, page.PageSize)
		cache.storeWithRecordChecksumState(ptr, leaves[i], true)
		if !cache.markPageChecksumVerified(ptr) {
			t.Fatalf("ptr %d should accept page-checksum mark", i)
		}
	}

	dst := make([]byte, 0, page.PageSize)
	for i, ptr := range ptrs {
		got, usedDst, state, ok := cache.getToWithState(ptr, dst[:0])
		if !ok {
			t.Fatalf("direct-map-colliding ptr %d missed set-associative cache", i)
		}
		if !usedDst || !bytes.Equal(got, leaves[i]) {
			t.Fatalf("ptr %d cached bytes mismatch usedDst=%v", i, usedDst)
		}
		if !state.RecordChecksumVerified || !state.PageChecksumVerified {
			t.Fatalf("ptr %d verified state not preserved: %+v", i, state)
		}
	}

	stats := cache.stats()
	if stats.Entries != uint64(len(ptrs)) || stats.Stores != uint64(len(ptrs)) || stats.Evictions != 0 {
		t.Fatalf("stats=%+v, want retained colliding entries without eviction", stats)
	}
	if stats.Capacity != 8 || stats.Buckets != 2 || stats.Ways != leafPageReadCacheWays || stats.Bytes != uint64(len(ptrs))*page.PageSize {
		t.Fatalf("cache shape stats=%+v, want capacity=8 buckets=2 ways=%d bounded bytes", stats, leafPageReadCacheWays)
	}
}

func TestLeafPageReadCacheReadMissAdmissionsRetainDirectMapCollisions(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptrs := findDirectSlotCollidingLeafPagePtrs(t, cache, leafPageReadCacheWays)
	leaves := make([][]byte, len(ptrs))
	for i, ptr := range ptrs {
		leaves[i] = bytes.Repeat([]byte{byte(0x50 + i)}, page.PageSize)
		if cache.storeReadMiss(ptr, leaves[i], true) {
			t.Fatalf("first read miss for ptr %d should only arm admission", i)
		}
		if !cache.storeReadMiss(ptr, leaves[i], true) {
			t.Fatalf("second read miss for ptr %d should be admitted", i)
		}
	}

	dst := make([]byte, 0, page.PageSize)
	for i, ptr := range ptrs {
		got, usedDst, state, ok := cache.getToWithState(ptr, dst[:0])
		if !ok {
			t.Fatalf("admitted direct-map-colliding ptr %d missed set-associative cache", i)
		}
		if !usedDst || !bytes.Equal(got, leaves[i]) {
			t.Fatalf("ptr %d cached bytes mismatch usedDst=%v", i, usedDst)
		}
		if !state.RecordChecksumVerified {
			t.Fatalf("ptr %d record checksum state not preserved: %+v", i, state)
		}
	}

	stats := cache.stats()
	if stats.ReadMissAdmissionSkips != uint64(len(ptrs)) || stats.ReadMissAdmissionStores != uint64(len(ptrs)) {
		t.Fatalf("admission stats=%+v, want one first-miss skip and one store per ptr", stats)
	}
	if stats.Evictions != 0 || stats.ConflictEvictions != 0 || stats.CapacityEvictions != 0 {
		t.Fatalf("colliding read-miss admissions thrashed: stats=%+v", stats)
	}
}

func TestLeafPageReadCacheInterleavedReadMissAdmissionsRetainDirectMapCollisions(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptrs := findDirectSlotCollidingLeafPagePtrsWithDistinctAdmissionLanes(t, cache, leafPageReadCacheWays)
	leaves := make([][]byte, len(ptrs))
	for i, ptr := range ptrs {
		leaves[i] = bytes.Repeat([]byte{byte(0x58 + i)}, page.PageSize)
		if cache.storeReadMiss(ptr, leaves[i], true) {
			t.Fatalf("first interleaved read miss for ptr %d should only arm admission", i)
		}
	}
	for i, ptr := range ptrs {
		if !cache.storeReadMiss(ptr, leaves[i], true) {
			t.Fatalf("second interleaved read miss for ptr %d should be admitted", i)
		}
	}

	dst := make([]byte, 0, page.PageSize)
	for i, ptr := range ptrs {
		got, usedDst, _, ok := cache.getToWithState(ptr, dst[:0])
		if !ok {
			t.Fatalf("interleaved direct-map-colliding ptr %d missed set-associative cache", i)
		}
		if !usedDst || !bytes.Equal(got, leaves[i]) {
			t.Fatalf("ptr %d cached bytes mismatch usedDst=%v", i, usedDst)
		}
	}

	stats := cache.stats()
	if stats.ReadMissAdmissionSkips != uint64(len(ptrs)) || stats.ReadMissAdmissionStores != uint64(len(ptrs)) {
		t.Fatalf("interleaved admission stats=%+v, want one first-miss skip and one store per ptr", stats)
	}
	if stats.Evictions != 0 || stats.ConflictEvictions != 0 || stats.CapacityEvictions != 0 {
		t.Fatalf("interleaved colliding admissions thrashed: stats=%+v", stats)
	}
}

func TestLeafPageReadCacheEvictionAccountingAndMemoryCap(t *testing.T) {
	cache := newLeafPageReadCache(5)
	bucket0Ptrs := findBucketLeafPagePtrs(t, cache, 0, leafPageReadCacheWays+1)
	bucket1Ptrs := findBucketLeafPagePtrs(t, cache, 1, 2)

	for i, ptr := range bucket0Ptrs {
		cache.store(ptr, bytes.Repeat([]byte{byte(0x60 + i)}, page.PageSize))
	}
	stats := cache.stats()
	if stats.Entries != leafPageReadCacheWays || stats.Evictions != 1 || stats.ConflictEvictions != 1 || stats.CapacityEvictions != 0 {
		t.Fatalf("after bucket conflict stats=%+v, want entries=%d one conflict eviction", stats, leafPageReadCacheWays)
	}
	if stats.Bytes != stats.Entries*page.PageSize || stats.Bytes > stats.Capacity*page.PageSize {
		t.Fatalf("cache bytes not bounded by entries/capacity: stats=%+v", stats)
	}

	cache.store(bucket1Ptrs[0], bytes.Repeat([]byte{0x70}, page.PageSize))
	cache.store(bucket1Ptrs[1], bytes.Repeat([]byte{0x71}, page.PageSize))
	stats = cache.stats()
	if stats.Entries != stats.Capacity || stats.Evictions != 2 || stats.ConflictEvictions != 1 || stats.CapacityEvictions != 1 {
		t.Fatalf("after capacity eviction stats=%+v, want split conflict/capacity evictions", stats)
	}
	if stats.Bytes != stats.Capacity*page.PageSize {
		t.Fatalf("retained bytes=%d want capacity bytes=%d", stats.Bytes, stats.Capacity*page.PageSize)
	}
}

func TestLeafPageReadCacheConcurrentSetAssociativeAccess(t *testing.T) {
	cache := newLeafPageReadCache(8)
	ptrs := findBucketLeafPagePtrs(t, cache, 0, 8)
	leaves := make([][]byte, len(ptrs))
	for i := range ptrs {
		leaves[i] = bytes.Repeat([]byte{byte(0x80 + i)}, page.PageSize)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			dst := make([]byte, 0, page.PageSize)
			for i := 0; i < 200; i++ {
				idx := (i + g) % len(ptrs)
				ptr := ptrs[idx]
				leaf := leaves[idx]
				cache.storeReadMiss(ptr, leaf, true)
				cache.storeReadMiss(ptr, leaf, true)
				if got, _, state, ok := cache.getToWithState(ptr, dst[:0]); ok {
					if len(got) != page.PageSize {
						t.Errorf("cached len=%d want %d", len(got), page.PageSize)
						return
					}
					if state.RecordChecksumVerified {
						cache.markPageChecksumVerified(ptr)
					}
				}
				if view, lease, ok := cache.getViewLocked(ptr); ok {
					if len(view) != page.PageSize {
						t.Errorf("view len=%d want %d", len(view), page.PageSize)
						lease.ReleaseLeafLogPageView()
						return
					}
					lease.MarkLeafLogPageViewChecksumVerified()
					lease.ReleaseLeafLogPageView()
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()

	stats := cache.stats()
	if stats.Entries > stats.Capacity || stats.Bytes > stats.Capacity*page.PageSize {
		t.Fatalf("cache exceeded memory cap under concurrency: stats=%+v", stats)
	}
}

func findDirectSlotCollidingLeafPagePtrs(t *testing.T, cache *leafPageReadCache, want int) []page.LeafLogPtr {
	t.Helper()
	var targetSlot int
	haveTarget := false
	return findLeafPageReadCachePtrs(t, want, func(key leafPageReadCacheKey) bool {
		idx := leafPageReadCacheDirectSlotIndex(cache, key)
		if !haveTarget {
			targetSlot = idx
			haveTarget = true
		}
		return idx == targetSlot
	})
}

func findDirectSlotCollidingLeafPagePtrsWithDistinctAdmissionLanes(t *testing.T, cache *leafPageReadCache, want int) []page.LeafLogPtr {
	t.Helper()
	lanes := make(map[int]struct{}, want)
	var targetSlot int
	haveTarget := false
	return findLeafPageReadCachePtrs(t, want, func(key leafPageReadCacheKey) bool {
		idx := leafPageReadCacheDirectSlotIndex(cache, key)
		if !haveTarget {
			targetSlot = idx
			haveTarget = true
		}
		if idx != targetSlot {
			return false
		}
		lane := leafPageReadMissCandidateLaneIndex(leafPageReadMissFingerprint(key))
		if _, ok := lanes[lane]; ok {
			return false
		}
		lanes[lane] = struct{}{}
		return true
	})
}

func leafPageReadCacheDirectSlotIndex(cache *leafPageReadCache, key leafPageReadCacheKey) int {
	return int(leafPageReadCacheHash(key) % uint64(len(cache.slots)))
}

func findBucketLeafPagePtrs(t *testing.T, cache *leafPageReadCache, bucketIndex, want int) []page.LeafLogPtr {
	t.Helper()
	return findLeafPageReadCachePtrs(t, want, func(key leafPageReadCacheKey) bool {
		return cache.bucketIndex(key) == bucketIndex
	})
}

func findLeafPageReadCachePtrs(t *testing.T, want int, match func(leafPageReadCacheKey) bool) []page.LeafLogPtr {
	t.Helper()
	ptrs := make([]page.LeafLogPtr, 0, want)
	seen := make(map[leafPageReadCacheKey]struct{}, want)
	for i := 0; i < 1_000_000 && len(ptrs) < want; i++ {
		ptr := page.LeafLogPtr{
			FileID:           uint32(1 + i%251),
			Offset:           uint64(i+1) * uint64(page.PageSize),
			SubIndex:         uint16(i % 7),
			RecordLengthHint: page.PageSize,
		}
		key := newLeafPageReadCacheKey(ptr)
		if !match(key) {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ptrs = append(ptrs, ptr)
	}
	if len(ptrs) != want {
		t.Fatalf("found %d matching leaf-page ptrs, want %d", len(ptrs), want)
	}
	return ptrs
}

func TestLeafPageReadCacheChecksumVerifiedStateRequiresVerifiedRecord(t *testing.T) {
	cache := newLeafPageReadCache(2)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x77}, page.PageSize)
	dst := make([]byte, 0, page.PageSize)

	cache.store(ptr, leaf)
	if cache.markPageChecksumVerified(ptr) {
		t.Fatal("write-origin cache entry should not become page-checksum verified without record checksum state")
	}
	_, _, state, ok := cache.getToWithState(ptr, dst)
	if !ok {
		t.Fatal("expected cache hit for write-origin entry")
	}
	if state.PageChecksumVerified {
		t.Fatal("write-origin entry unexpectedly reported page checksum verified")
	}

	cache.storeWithRecordChecksumState(ptr, leaf, true)
	if !cache.markPageChecksumVerified(ptr) {
		t.Fatal("record-verified cache entry should accept page checksum mark")
	}
	_, _, state, ok = cache.getToWithState(ptr, dst[:0])
	if !ok {
		t.Fatal("expected cache hit for record-verified entry")
	}
	if !state.PageChecksumVerified {
		t.Fatal("record-verified entry did not report page checksum verified after mark")
	}

	stats := cache.stats()
	if stats.RecordChecksumVerifiedStores != 1 || stats.PageChecksumVerifiedMarks != 1 {
		t.Fatalf("stats=%+v, want one record-verified store and one page-checksum mark", stats)
	}
	if stats.PageChecksumMarkUnsafeSkips != 1 {
		t.Fatalf("unsafe mark skips=%d, want 1", stats.PageChecksumMarkUnsafeSkips)
	}
	if stats.PageChecksumUnverifiedHits != 1 || stats.PageChecksumVerifiedHits != 1 {
		t.Fatalf("verified/unverified hits=%d/%d, want 1/1", stats.PageChecksumVerifiedHits, stats.PageChecksumUnverifiedHits)
	}
}

func TestLeafPageReadCacheVerifiedStateEvictedAndRewriteFallback(t *testing.T) {
	cache := newLeafPageReadCache(1)
	oldPtr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	newPtr := page.LeafLogPtr{FileID: 7, Offset: 256, RecordLengthHint: 4096}
	oldLeaf := bytes.Repeat([]byte{0x11}, page.PageSize)
	newLeaf := bytes.Repeat([]byte{0x22}, page.PageSize)
	dst := make([]byte, 0, page.PageSize)

	cache.storeWithRecordChecksumState(oldPtr, oldLeaf, true)
	if !cache.markPageChecksumVerified(oldPtr) {
		t.Fatal("old page did not become verified")
	}
	cache.storeWithRecordChecksumState(newPtr, newLeaf, true)
	if !cache.markPageChecksumVerified(newPtr) {
		t.Fatal("new page did not become verified")
	}

	if got, _, _, ok := cache.getToWithState(oldPtr, dst); ok {
		t.Fatalf("old rewritten/evicted ptr unexpectedly hit cache: first byte=%x", got[0])
	}
	got, _, state, ok := cache.getToWithState(newPtr, dst[:0])
	if !ok {
		t.Fatal("new rewritten ptr should hit cache")
	}
	if !state.PageChecksumVerified {
		t.Fatal("new rewritten ptr should retain verified state")
	}
	if !bytes.Equal(got, newLeaf) {
		t.Fatal("new rewritten ptr returned wrong leaf bytes")
	}
	if stats := cache.stats(); stats.Evictions != 1 || stats.Misses != 1 {
		t.Fatalf("stats=%+v, want one eviction and one old-ptr miss", stats)
	}
}

func TestLeafPageReadCacheReadMissCandidateEpochPreventsStaleAdmission(t *testing.T) {
	cache := newLeafPageReadCache(1)
	bucket := &cache.buckets[0]
	keyA := newLeafPageReadCacheKey(page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096})
	keyB := newLeafPageReadCacheKey(page.LeafLogPtr{FileID: 9, Offset: 512, RecordLengthHint: 4096})
	fpA := leafPageReadMissFingerprint(keyA)

	laneA := bucket.readMissCandidateLane(fpA)
	laneA.candidateFP.Store(readMissCandidateToken(fpA, laneA.epoch.Load()))
	epochBefore, repeated := bucket.observeReadMissCandidate(fpA)
	if !repeated {
		t.Fatal("expected matching candidate to be treated as repeated miss")
	}

	cache.storeWithRecordChecksumState(page.LeafLogPtr{FileID: keyB.fileID, Offset: keyB.offset, SubIndex: keyB.subIndex, RecordLengthHint: 4096}, bytes.Repeat([]byte{0x61}, page.PageSize), false)

	epochAfterReset, repeated := bucket.observeReadMissCandidate(fpA)
	if repeated {
		t.Fatal("expected first post-reset miss to be non-repeated")
	}
	if epochAfterReset == epochBefore {
		t.Fatalf("read miss epoch did not advance after reset: before=%d after=%d", epochBefore, epochAfterReset)
	}
	if bucket.readMissCandidateStillCurrent(fpA, epochBefore) {
		t.Fatal("stale pre-reset candidate unexpectedly considered current")
	}
	if !bucket.readMissCandidateStillCurrent(fpA, epochAfterReset) {
		t.Fatal("fresh post-reset candidate should be current")
	}
}

func TestLeafPageReadCacheReadMissCandidateEpochTokenRejectsStaleCAS(t *testing.T) {
	cache := newLeafPageReadCache(1)
	bucket := &cache.buckets[0]
	keyA := newLeafPageReadCacheKey(page.LeafLogPtr{FileID: 11, Offset: 64, RecordLengthHint: 4096})
	keyB := newLeafPageReadCacheKey(page.LeafLogPtr{FileID: 12, Offset: 96, RecordLengthHint: 4096})
	fpA := leafPageReadMissFingerprint(keyA)
	fpB := leafPageReadMissFingerprint(keyB)
	laneA := bucket.readMissCandidateLane(fpA)
	oldEpoch := laneA.epoch.Load()
	oldTokenA := readMissCandidateToken(fpA, oldEpoch)
	oldTokenB := readMissCandidateToken(fpB, oldEpoch)

	laneA.candidateFP.Store(oldTokenA)

	bucket.mu.Lock()
	bucket.resetReadMissCandidateLocked()
	bucket.mu.Unlock()

	newEpoch := laneA.epoch.Load()
	if newEpoch == oldEpoch {
		t.Fatalf("epoch did not advance across reset: old=%d new=%d", oldEpoch, newEpoch)
	}
	newTokenA := readMissCandidateToken(fpA, newEpoch)
	if newTokenA == oldTokenA {
		t.Fatalf("candidate token unexpectedly reused across epochs: epoch=%d token=%d", newEpoch, newTokenA)
	}

	observedEpoch, repeated := bucket.observeReadMissCandidate(fpA)
	if repeated {
		t.Fatal("expected first post-reset miss for key A to be non-repeated")
	}
	if observedEpoch != newEpoch {
		t.Fatalf("observeReadMissCandidate epoch=%d, want %d", observedEpoch, newEpoch)
	}
	if got := laneA.candidateFP.Load(); got != newTokenA {
		t.Fatalf("post-reset candidate token=%d, want %d", got, newTokenA)
	}

	// Simulate a stale observer from oldEpoch trying to publish into newEpoch.
	if laneA.candidateFP.CompareAndSwap(oldTokenA, oldTokenB) {
		t.Fatal("stale epoch compare-and-swap unexpectedly succeeded")
	}
	if got := laneA.candidateFP.Load(); got != newTokenA {
		t.Fatalf("candidate token changed after stale CAS: got=%d want=%d", got, newTokenA)
	}
}

func TestLeafPageReadCacheReadMissCandidateOddEpochSpinBound(t *testing.T) {
	cache := newLeafPageReadCache(1)
	bucket := &cache.buckets[0]
	key := newLeafPageReadCacheKey(page.LeafLogPtr{FileID: 13, Offset: 4096, RecordLengthHint: 4096})
	fp := leafPageReadMissFingerprint(key)
	bucket.readMissCandidateLane(fp).epoch.Store(1) // odd epoch simulates reset in progress

	type result struct {
		epoch    uint64
		repeated bool
	}
	done := make(chan result, 1)
	go func() {
		epoch, repeated := bucket.observeReadMissCandidate(fp)
		done <- result{epoch: epoch, repeated: repeated}
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case got := <-done:
		if got.repeated {
			t.Fatal("odd-epoch spin bound should return non-repeated miss")
		}
		if got.epoch&1 == 0 {
			t.Fatalf("observeReadMissCandidate epoch=%d, want odd epoch while reset is in progress", got.epoch)
		}
	case <-timer.C:
		t.Fatal("observeReadMissCandidate did not return under sustained odd-epoch contention")
	}
}

func TestValueReaderLeafLogPageUnsafeToOneOffReadMissDoesNotEvictStoredLeaf(t *testing.T) {
	cache := newLeafPageReadCache(1)
	hotPtr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	coldPtr := page.LeafLogPtr{FileID: 7, Offset: 256, RecordLengthHint: 4096}
	hotLeaf := bytes.Repeat([]byte{0x33}, page.PageSize)
	coldLeaf := bytes.Repeat([]byte{0x55}, page.PageSize)
	cache.store(hotPtr, hotLeaf)

	fallback := &leafPageCacheTestFallback{data: coldLeaf}
	reader := valueReader{
		vlogs:         fallback,
		leafPageCache: cache,
	}
	dst := make([]byte, 0, page.PageSize)
	got, usedDst, err := reader.ReadLeafLogPageUnsafeTo(coldPtr, dst)
	if err != nil {
		t.Fatalf("ReadLeafLogPageUnsafeTo cold miss: %v", err)
	}
	if !usedDst || !bytes.Equal(got, coldLeaf) {
		t.Fatalf("cold fallback mismatch usedDst=%v", usedDst)
	}
	if fallback.readUnsafeToCalls != 1 {
		t.Fatalf("fallback calls=%d, want 1", fallback.readUnsafeToCalls)
	}
	if stats := cache.stats(); stats.Stores != 1 || stats.Evictions != 0 || stats.ReadMissAdmissionSkips != 1 {
		t.Fatalf("stats=%+v, want one-off read miss not to evict stored hot leaf", stats)
	}

	fallback.err = errors.New("hot leaf should still be cached")
	got, usedDst, err = reader.ReadLeafLogPageUnsafeTo(hotPtr, dst[:0])
	if err != nil {
		t.Fatalf("ReadLeafLogPageUnsafeTo hot hit: %v", err)
	}
	if !usedDst || !bytes.Equal(got, hotLeaf) {
		t.Fatalf("hot cache mismatch usedDst=%v", usedDst)
	}
	if stats := cache.stats(); stats.Hits != 1 || stats.Stores != 1 || stats.Evictions != 0 {
		t.Fatalf("stats=%+v, want hot cache entry retained", stats)
	}
}

func TestLeafPageReadCacheWriteAdmissionImmediatePreservesDefaultStoreBehavior(t *testing.T) {
	cache := newLeafPageReadCache(1)
	leafA := bytes.Repeat([]byte{0x77}, page.PageSize)
	leafB := bytes.Repeat([]byte{0x88}, page.PageSize)
	ptrA := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	ptrB := page.LeafLogPtr{FileID: 7, Offset: 256, RecordLengthHint: 4096}

	cache.storeWrite(ptrA, leafA)
	cache.storeWrite(ptrB, leafB)

	stats := cache.stats()
	if stats.Stores != 2 || stats.Evictions != 1 || stats.WriteAdmissionAttempts != 0 || stats.WriteAdmissionSkips != 0 {
		t.Fatalf("stats=%+v, want immediate stores with no adaptive admission counters", stats)
	}
	got, ok := cache.get(ptrB)
	if !ok || !bytes.Equal(got, leafB) {
		t.Fatalf("latest immediate write was not cached")
	}
}

func TestLeafPageReadCacheAdaptiveWriteAdmissionSkipsColdStreamAfterWarmup(t *testing.T) {
	cache := newLeafPageReadCacheWithWriteAdmission(2, LeafPageReadCacheWriteAdmissionAdaptive)
	leaf := bytes.Repeat([]byte{0x77}, page.PageSize)

	cache.storeWrite(page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}, leaf)
	cache.storeWrite(page.LeafLogPtr{FileID: 7, Offset: 256, RecordLengthHint: 4096}, leaf)
	for i := 0; i < 10; i++ {
		cache.storeWrite(page.LeafLogPtr{FileID: 7, Offset: uint64(384 + i*128), RecordLengthHint: 4096}, leaf)
	}

	stats := cache.stats()
	if stats.WriteAdmissionAttempts != 12 {
		t.Fatalf("write admission attempts=%d, want 12; stats=%+v", stats.WriteAdmissionAttempts, stats)
	}
	if stats.WriteAdmissionStores != 2 || stats.Stores != 2 || stats.Entries != 2 {
		t.Fatalf("stats=%+v, want only warm-up write stores retained", stats)
	}
	if stats.WriteAdmissionSkips != 10 {
		t.Fatalf("write admission skips=%d, want 10; stats=%+v", stats.WriteAdmissionSkips, stats)
	}
}

func TestLeafPageReadCacheAdaptiveWriteAdmissionAdmitsAfterHotReads(t *testing.T) {
	cache := newLeafPageReadCacheWithWriteAdmission(2, LeafPageReadCacheWriteAdmissionAdaptive)
	hotPtr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	otherPtr := page.LeafLogPtr{FileID: 7, Offset: 256, RecordLengthHint: 4096}
	newPtr := page.LeafLogPtr{FileID: 7, Offset: 384, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x88}, page.PageSize)

	cache.storeWrite(hotPtr, leaf)
	cache.storeWrite(otherPtr, leaf)
	for i := 0; i < leafPageReadCacheWriteAdmissionMinReadCount; i++ {
		if _, ok := cache.get(hotPtr); !ok {
			t.Fatalf("hot cache read %d missed", i)
		}
	}
	cache.storeWrite(newPtr, leaf)

	stats := cache.stats()
	if stats.Hits != leafPageReadCacheWriteAdmissionMinReadCount {
		t.Fatalf("cache hits=%d, want %d; stats=%+v", stats.Hits, leafPageReadCacheWriteAdmissionMinReadCount, stats)
	}
	if stats.WriteAdmissionStores != 3 || stats.Stores != 3 {
		t.Fatalf("stores=%d write stores=%d, want hot-read admission to store third page; stats=%+v", stats.Stores, stats.WriteAdmissionStores, stats)
	}
	if stats.WriteAdmissionSkips != 0 {
		t.Fatalf("write admission skips=%d, want 0 after hot reads; stats=%+v", stats.WriteAdmissionSkips, stats)
	}
}

func TestLeafPageReadCacheAdaptiveWriteAdmissionSmallCapacitySkipsColdWrites(t *testing.T) {
	cache := newLeafPageReadCacheWithWriteAdmission(1, LeafPageReadCacheWriteAdmissionAdaptive)
	leaf := bytes.Repeat([]byte{0x99}, page.PageSize)
	ptrA := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	ptrB := page.LeafLogPtr{FileID: 7, Offset: 256, RecordLengthHint: 4096}

	cache.storeWrite(ptrA, leaf)
	cache.storeWrite(ptrB, leaf)

	stats := cache.stats()
	if stats.WriteAdmissionAttempts != 2 || stats.WriteAdmissionStores != 1 || stats.WriteAdmissionSkips != 1 || stats.Stores != 1 {
		t.Fatalf("stats=%+v, want one warm-up store and one cold skip", stats)
	}
	if _, ok := cache.get(ptrA); !ok {
		t.Fatalf("warm-up write should remain cached")
	}
	if _, ok := cache.get(ptrB); ok {
		t.Fatalf("cold skipped write should not be cached")
	}
}

func TestLeafPageReadCacheAdaptiveWriteAdmissionInvalidPageSizeIsNoop(t *testing.T) {
	cache := newLeafPageReadCacheWithWriteAdmission(2, LeafPageReadCacheWriteAdmissionAdaptive)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}

	cache.storeWrite(ptr, []byte{0x01})

	stats := cache.stats()
	if stats.WriteAdmissionAttempts != 0 || stats.WriteAdmissionSkips != 0 || stats.Stores != 0 || stats.Entries != 0 {
		t.Fatalf("stats=%+v, want invalid page size to bypass cache admission", stats)
	}
}

func TestLeafPageReadCacheAdaptiveWriteAdmissionDisabledCacheIsNoop(t *testing.T) {
	var cache *leafPageReadCache
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0xaa}, page.PageSize)

	cache.storeWrite(ptr, leaf)
	if stats := cache.stats(); stats != (leafPageReadCacheStats{}) {
		t.Fatalf("nil cache stats=%+v, want stable zero values", stats)
	}

	db := &DB{}
	db.storeLeafPageReadCache(ptr, leaf)
}

func TestLeafPageReadCacheAdaptiveWriteAdmissionSkipsWhenSlotLockContended(t *testing.T) {
	cache := newLeafPageReadCacheWithWriteAdmission(1, LeafPageReadCacheWriteAdmissionAdaptive)
	ptr := page.LeafLogPtr{FileID: 9, Offset: 512, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x66}, page.PageSize)
	key := newLeafPageReadCacheKey(ptr)
	// With a one-entry cache there is only one victim slot; holding it ensures the
	// adaptive write path records a non-blocking lock skip instead of waiting.
	slot := &cache.slots[leafPageReadCacheDirectSlotIndex(cache, key)]
	slot.mu.Lock()
	defer slot.mu.Unlock()

	done := make(chan struct{})
	go func() {
		cache.storeWrite(ptr, leaf)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("storeWrite blocked on contended slot lock; expected non-blocking skip")
	}

	stats := cache.stats()
	if stats.WriteAdmissionAttempts != 1 || stats.WriteAdmissionSkips != 1 || stats.WriteAdmissionLockSkips != 1 || stats.WriteAdmissionStores != 0 || stats.Stores != 0 {
		t.Fatalf("stats=%+v, want one adaptive lock skip and no stores", stats)
	}
}

func TestLeafPageReadCacheAdaptiveWriteAdmissionPreservesChecksumState(t *testing.T) {
	cache := newLeafPageReadCacheWithWriteAdmission(2, LeafPageReadCacheWriteAdmissionAdaptive)
	ptr := page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0xbb}, page.PageSize)
	dst := make([]byte, 0, page.PageSize)

	cache.storeWriteWithRecordChecksumState(ptr, leaf, true)
	got, _, state, ok := cache.getToWithState(ptr, dst)
	if !ok || !bytes.Equal(got, leaf) {
		t.Fatalf("expected checksum-verified adaptive write store to be cached")
	}
	if !state.RecordChecksumVerified || state.PageChecksumVerified {
		t.Fatalf("initial checksum state=%+v, want record verified and page not yet marked", state)
	}
	if !cache.markPageChecksumVerified(ptr) {
		t.Fatalf("markPageChecksumVerified returned false")
	}
	_, _, state, ok = cache.getToWithState(ptr, dst[:0])
	if !ok || !state.RecordChecksumVerified || !state.PageChecksumVerified {
		t.Fatalf("post-mark checksum state=%+v ok=%v, want record+page verified", state, ok)
	}
}

func TestLeafPageReadCacheStoreReadMissSkipsWhenSlotLockContended(t *testing.T) {
	cache := newLeafPageReadCache(1)
	ptr := page.LeafLogPtr{FileID: 9, Offset: 512, RecordLengthHint: 4096}
	leaf := bytes.Repeat([]byte{0x66}, page.PageSize)
	key := newLeafPageReadCacheKey(ptr)
	slot := &cache.slots[leafPageReadCacheDirectSlotIndex(cache, key)]

	// First read miss only arms the admission fingerprint and should skip.
	cache.storeReadMiss(ptr, leaf, false)
	if stats := cache.stats(); stats.ReadMissAdmissionSkips != 1 || stats.ReadMissAdmissionStores != 0 {
		t.Fatalf("stats after first miss=%+v, want one admission skip and no stores", stats)
	}

	// Contend slot lock before second miss; with TryLock-based admission this
	// should return quickly and record another skip instead of blocking.
	slot.mu.Lock()
	defer slot.mu.Unlock()

	done := make(chan struct{})
	go func() {
		cache.storeReadMiss(ptr, leaf, false)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("storeReadMiss blocked on contended slot lock; expected non-blocking skip")
	}

	stats := cache.stats()
	if stats.ReadMissAdmissionSkips < 2 {
		t.Fatalf("expected additional skip on lock contention, stats=%+v", stats)
	}
	if stats.ReadMissAdmissionLockSkips != 1 {
		t.Fatalf("lock admission skips=%d, want 1; stats=%+v", stats.ReadMissAdmissionLockSkips, stats)
	}
	if stats.ReadMissAdmissionStores != 0 {
		t.Fatalf("unexpected admission store under lock contention, stats=%+v", stats)
	}
}

func TestConfiguredLeafPageReadCacheEntriesReadsEnvAtOpenTime(t *testing.T) {
	prev := LeafPageReadCacheEntries
	LeafPageReadCacheEntries = 8
	t.Cleanup(func() {
		LeafPageReadCacheEntries = prev
	})
	t.Setenv(LeafPageReadCacheEntriesEnvKey, "3")

	if got := configuredLeafPageReadCacheEntries(0); got != 3 {
		t.Fatalf("configuredLeafPageReadCacheEntries()=%d, want env override 3", got)
	}
}

func TestConfiguredLeafPageReadCacheEntriesDefaultCapsRetainedBytes(t *testing.T) {
	prev := LeafPageReadCacheEntries
	LeafPageReadCacheEntries = defaultLeafPageReadCacheEntries
	t.Cleanup(func() {
		LeafPageReadCacheEntries = prev
	})
	t.Setenv(LeafPageReadCacheEntriesEnvKey, "")

	got := configuredLeafPageReadCacheEntries(0)
	if got != 8192 {
		t.Fatalf("configuredLeafPageReadCacheEntries()=%d, want 8192", got)
	}
	if got*page.PageSize != 32<<20 {
		t.Fatalf("default leaf-page read cache data bytes=%d, want 32MiB", got*page.PageSize)
	}
}

func TestConfiguredLeafPageReadCacheEntriesOptionOverridesEnv(t *testing.T) {
	prev := LeafPageReadCacheEntries
	LeafPageReadCacheEntries = 8
	t.Cleanup(func() {
		LeafPageReadCacheEntries = prev
	})
	t.Setenv(LeafPageReadCacheEntriesEnvKey, "3")

	if got := configuredLeafPageReadCacheEntries(16); got != 16 {
		t.Fatalf("configuredLeafPageReadCacheEntries(16)=%d, want option override 16", got)
	}
}

func TestConfiguredLeafPageReadCacheEntriesNegativeOptionDisables(t *testing.T) {
	prev := LeafPageReadCacheEntries
	LeafPageReadCacheEntries = 8
	t.Cleanup(func() {
		LeafPageReadCacheEntries = prev
	})
	t.Setenv(LeafPageReadCacheEntriesEnvKey, "3")

	if got := configuredLeafPageReadCacheEntries(-1); got != 0 {
		t.Fatalf("configuredLeafPageReadCacheEntries(-1)=%d, want disabled cache 0", got)
	}
}

func TestValidateOptionsRejectsHugeLeafPageReadCacheEntries(t *testing.T) {
	err := validateOptions(Options{LeafPageReadCacheEntries: maxLeafPageReadCacheEntries + 1})
	if err == nil {
		t.Fatal("validateOptions unexpectedly accepted huge leaf page read cache")
	}
}

func TestValidateOptionsRejectsHugeLeafPageReadCacheEntriesForReadOnly(t *testing.T) {
	err := validateOptions(Options{ReadOnly: true, LeafPageReadCacheEntries: maxLeafPageReadCacheEntries + 1})
	if err == nil {
		t.Fatal("validateOptions unexpectedly accepted huge read-only leaf page read cache")
	}
}

func TestValidateOptionsRejectsHugeLeafPageReadCacheEntriesFromEnv(t *testing.T) {
	t.Setenv(LeafPageReadCacheEntriesEnvKey, strconv.Itoa(maxLeafPageReadCacheEntries+1))

	err := validateOptions(Options{})
	if err == nil {
		t.Fatal("validateOptions unexpectedly accepted huge env leaf page read cache")
	}
}

func TestParseLeafPageReadCacheWriteAdmissionPolicy(t *testing.T) {
	got, err := ParseLeafPageReadCacheWriteAdmissionPolicy("adaptive")
	if err != nil {
		t.Fatalf("parse adaptive: %v", err)
	}
	if got != LeafPageReadCacheWriteAdmissionAdaptive {
		t.Fatalf("policy=%v want adaptive", got)
	}
	got, err = ParseLeafPageReadCacheWriteAdmissionPolicy("default")
	if err != nil {
		t.Fatalf("parse default: %v", err)
	}
	if got != LeafPageReadCacheWriteAdmissionImmediate {
		t.Fatalf("policy=%v want immediate", got)
	}
	if _, err := ParseLeafPageReadCacheWriteAdmissionPolicy("invalid"); err == nil {
		t.Fatalf("expected invalid write admission policy error")
	}
}

func TestValidateOptionsRejectsInvalidLeafPageReadCacheWriteAdmission(t *testing.T) {
	err := validateOptions(Options{LeafPageReadCacheWriteAdmission: LeafPageReadCacheWriteAdmissionPolicy(99)})
	if err == nil {
		t.Fatal("validateOptions unexpectedly accepted invalid write admission policy")
	}
}

type leafPageCacheTestLog struct {
	ptr page.LeafLogPtr
}

func (l *leafPageCacheTestLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return l.ptr, nil
}

func (l *leafPageCacheTestLog) Flush() error { return nil }
func (l *leafPageCacheTestLog) Sync() error  { return nil }

func TestLeafPageLogStoresReadCache(t *testing.T) {
	ptr := page.LeafLogPtr{FileID: 11, Offset: 256, RecordLengthHint: 4096}
	db := &DB{leafPageReadCache: newLeafPageReadCache(8)}
	log := &leafPageLogWithRecordLengthHints{db: db, inner: &leafPageCacheTestLog{ptr: ptr}}
	leaf := bytes.Repeat([]byte{0x7a}, page.PageSize)

	gotPtr, err := log.AppendLeafPage(leaf)
	if err != nil {
		t.Fatalf("AppendLeafPage: %v", err)
	}
	if gotPtr != ptr {
		t.Fatalf("ptr=%+v want %+v", gotPtr, ptr)
	}

	got, ok := db.leafPageReadCache.get(ptr)
	if !ok {
		t.Fatalf("expected cached leaf page")
	}
	if !bytes.Equal(got, leaf) {
		t.Fatalf("cached leaf mismatch")
	}

	leaf[0] = 0
	got, ok = db.leafPageReadCache.get(ptr)
	if !ok {
		t.Fatalf("expected cached leaf page after source mutation")
	}
	if got[0] != 0x7a {
		t.Fatalf("cache should own a copy, got first byte=%x", got[0])
	}
}

type leafPageCacheBatchTestLog struct {
	ptrs []page.LeafLogPtr
}

func (l *leafPageCacheBatchTestLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	if len(l.ptrs) == 0 {
		return page.LeafLogPtr{}, nil
	}
	return l.ptrs[0], nil
}

func (l *leafPageCacheBatchTestLog) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	return l.ptrs[:len(leafPages)], nil
}

func (l *leafPageCacheBatchTestLog) Flush() error { return nil }
func (l *leafPageCacheBatchTestLog) Sync() error  { return nil }

func TestLeafPageLogStoresReadCacheForBatch(t *testing.T) {
	ptrs := []page.LeafLogPtr{
		{FileID: 11, Offset: 256, RecordLengthHint: page.ValuePtrMarkGrouped(4096, 0), SubIndex: 0},
		{FileID: 11, Offset: 256, RecordLengthHint: page.ValuePtrMarkGrouped(4096, 1), SubIndex: 1},
	}
	db := &DB{leafPageReadCache: newLeafPageReadCache(8)}
	log := &leafPageLogWithRecordLengthHints{db: db, inner: &leafPageCacheBatchTestLog{ptrs: ptrs}}
	leafPages := [][]byte{
		bytes.Repeat([]byte{0x7a}, page.PageSize),
		bytes.Repeat([]byte{0x7b}, page.PageSize),
	}

	gotPtrs, err := log.AppendLeafPages(leafPages)
	if err != nil {
		t.Fatalf("AppendLeafPages: %v", err)
	}
	if len(gotPtrs) != len(ptrs) {
		t.Fatalf("got ptrs=%d want %d", len(gotPtrs), len(ptrs))
	}
	for i, ptr := range ptrs {
		got, ok := db.leafPageReadCache.get(ptr)
		if !ok {
			t.Fatalf("expected cached leaf page %d", i)
		}
		if !bytes.Equal(got, leafPages[i]) {
			t.Fatalf("cached leaf %d mismatch", i)
		}
	}
	leafPages[0][0] = 0
	got, ok := db.leafPageReadCache.get(ptrs[0])
	if !ok {
		t.Fatalf("expected cached leaf page after source mutation")
	}
	if got[0] != 0x7a {
		t.Fatalf("cache should own a copy, got first byte=%x", got[0])
	}
}

type leafPageCacheMismatchBatchLog struct{}

func (l *leafPageCacheMismatchBatchLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, nil
}

func (l *leafPageCacheMismatchBatchLog) AppendLeafPages([][]byte) ([]page.LeafLogPtr, error) {
	return []page.LeafLogPtr{{FileID: 11, Offset: 256, RecordLengthHint: 4096}}, nil
}

func (l *leafPageCacheMismatchBatchLog) Flush() error { return nil }
func (l *leafPageCacheMismatchBatchLog) Sync() error  { return nil }

func TestLeafPageLogRejectsBatchPtrCountMismatch(t *testing.T) {
	db := &DB{leafPageReadCache: newLeafPageReadCache(8)}
	log := &leafPageLogWithRecordLengthHints{db: db, inner: &leafPageCacheMismatchBatchLog{}}
	leafPages := [][]byte{
		bytes.Repeat([]byte{0x01}, page.PageSize),
		bytes.Repeat([]byte{0x02}, page.PageSize),
	}

	if _, err := log.AppendLeafPages(leafPages); err == nil {
		t.Fatalf("expected batch ptr count mismatch error")
	}
	if stats := db.leafPageReadCache.stats(); stats.Stores != 0 {
		t.Fatalf("cache stores=%d, want 0 after rejected batch", stats.Stores)
	}
}

type leafPageCacheStableChildRefTestLog struct {
	ptr       page.LeafLogPtr
	resources *rootpublication.StableResourceSet
}

func (l *leafPageCacheStableChildRefTestLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return l.ptr, nil
}

func (l *leafPageCacheStableChildRefTestLog) AppendPreparedLeafPageChildRefsWithStableResources(_ [][]byte, _ [][]byte, refs []page.ChildRef) ([]page.ChildRef, *rootpublication.StableResourceSet, error) {
	return append(refs[:0], page.LeafLogChildRef(l.ptr)), l.resources, nil
}

func (*leafPageCacheStableChildRefTestLog) Flush() error { return nil }
func (*leafPageCacheStableChildRefTestLog) Sync() error  { return nil }

func TestLeafPageLogStableChildRefsDoNotCacheBeforeDependencyAppendSucceeds(t *testing.T) {
	ptr := page.LeafLogPtr{FileID: 11, Offset: 256, RecordLengthHint: 1024}
	resources := stableContractResourceSet(t, stableContractDescriptor{
		generation: uint64(ptr.ValueLogFileID()), kind: rootpublication.ResourceOuterLeafLog,
		reachability: rootpublication.ReachabilityOuterLeafRawPointer, frontier: 4096,
	})
	db := &DB{dir: t.TempDir(), leafPageReadCache: newLeafPageReadCache(8)}
	inner := &leafPageCacheStableChildRefTestLog{ptr: ptr, resources: resources}
	log := &leafPageLogWithRecordLengthHints{db: db, inner: inner}
	wantErr := errors.New("reject dependency append")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceOuterLeaf && event.Point == durabilitycut.AfterDependencyAppend {
			return wantErr
		}
		return nil
	})
	_, gotResources, err := log.AppendPreparedLeafPageChildRefsWithStableResources(
		[][]byte{bytes.Repeat([]byte{0x7a}, page.PageSize)},
		[][]byte{{0x01}},
		nil,
	)
	restore()
	if !errors.Is(err, wantErr) {
		t.Fatalf("append error=%v want %v", err, wantErr)
	}
	if gotResources != nil {
		t.Fatalf("resources=%v want nil after rejected append", gotResources)
	}
	if owner := resources.Owner(); owner != rootpublication.ResourceOwnerReleased {
		t.Fatalf("resource owner=%v want released", owner)
	}
	if stats := db.leafPageReadCache.stats(); stats.Stores != 0 {
		t.Fatalf("cache stores=%d want 0 after rejected dependency append", stats.Stores)
	}
	db.leafGenerationRecordLengthMu.Lock()
	indexedFiles := len(db.leafGenerationRecordLengthByFile)
	db.leafGenerationRecordLengthMu.Unlock()
	if indexedFiles != 0 {
		t.Fatalf("record-length indexes=%d want 0 after rejected dependency append", indexedFiles)
	}
}

func TestLeafPageReadCacheNilStatsAreStableZeros(t *testing.T) {
	var cache *leafPageReadCache
	stats := cache.stats()
	if stats != (leafPageReadCacheStats{}) {
		t.Fatalf("nil cache stats=%+v, want zero values", stats)
	}
}
