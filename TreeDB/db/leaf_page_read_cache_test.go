package db

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type leafPageCacheTestFallback struct {
	readUnsafeCalls   int
	readUnsafeToCalls int
	err               error
	data              []byte
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

func TestLeafPageReadCacheReadMissCandidateEpochPreventsStaleAdmission(t *testing.T) {
	cache := newLeafPageReadCache(1)
	slot := &cache.slots[0]
	keyA := newLeafPageReadCacheKey(page.LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 4096})
	keyB := newLeafPageReadCacheKey(page.LeafLogPtr{FileID: 9, Offset: 512, RecordLengthHint: 4096})
	fpA := leafPageReadMissFingerprint(keyA)

	slot.readMissCandidateFP.Store(readMissCandidateToken(fpA, slot.readMissEpoch.Load()))
	epochBefore, repeated := slot.observeReadMissCandidate(fpA)
	if !repeated {
		t.Fatal("expected matching candidate to be treated as repeated miss")
	}

	slot.mu.Lock()
	_ = slot.storeLocked(keyB, bytes.Repeat([]byte{0x61}, page.PageSize))
	slot.mu.Unlock()

	epochAfterReset, repeated := slot.observeReadMissCandidate(fpA)
	if repeated {
		t.Fatal("expected first post-reset miss to be non-repeated")
	}
	if epochAfterReset == epochBefore {
		t.Fatalf("read miss epoch did not advance after reset: before=%d after=%d", epochBefore, epochAfterReset)
	}
	if slot.readMissCandidateStillCurrent(fpA, epochBefore) {
		t.Fatal("stale pre-reset candidate unexpectedly considered current")
	}
	if !slot.readMissCandidateStillCurrent(fpA, epochAfterReset) {
		t.Fatal("fresh post-reset candidate should be current")
	}
}

func TestLeafPageReadCacheReadMissCandidateEpochTokenRejectsStaleCAS(t *testing.T) {
	cache := newLeafPageReadCache(1)
	slot := &cache.slots[0]
	keyA := newLeafPageReadCacheKey(page.LeafLogPtr{FileID: 11, Offset: 64, RecordLengthHint: 4096})
	keyB := newLeafPageReadCacheKey(page.LeafLogPtr{FileID: 12, Offset: 96, RecordLengthHint: 4096})
	fpA := leafPageReadMissFingerprint(keyA)
	fpB := leafPageReadMissFingerprint(keyB)
	oldEpoch := slot.readMissEpoch.Load()
	oldTokenA := readMissCandidateToken(fpA, oldEpoch)
	oldTokenB := readMissCandidateToken(fpB, oldEpoch)

	slot.readMissCandidateFP.Store(oldTokenA)

	slot.mu.Lock()
	slot.resetReadMissCandidateLocked()
	slot.mu.Unlock()

	newEpoch := slot.readMissEpoch.Load()
	if newEpoch == oldEpoch {
		t.Fatalf("epoch did not advance across reset: old=%d new=%d", oldEpoch, newEpoch)
	}
	newTokenA := readMissCandidateToken(fpA, newEpoch)
	if newTokenA == oldTokenA {
		t.Fatalf("candidate token unexpectedly reused across epochs: epoch=%d token=%d", newEpoch, newTokenA)
	}

	observedEpoch, repeated := slot.observeReadMissCandidate(fpA)
	if repeated {
		t.Fatal("expected first post-reset miss for key A to be non-repeated")
	}
	if observedEpoch != newEpoch {
		t.Fatalf("observeReadMissCandidate epoch=%d, want %d", observedEpoch, newEpoch)
	}
	if got := slot.readMissCandidateFP.Load(); got != newTokenA {
		t.Fatalf("post-reset candidate token=%d, want %d", got, newTokenA)
	}

	// Simulate a stale observer from oldEpoch trying to publish into newEpoch.
	if slot.readMissCandidateFP.CompareAndSwap(oldTokenA, oldTokenB) {
		t.Fatal("stale epoch compare-and-swap unexpectedly succeeded")
	}
	if got := slot.readMissCandidateFP.Load(); got != newTokenA {
		t.Fatalf("candidate token changed after stale CAS: got=%d want=%d", got, newTokenA)
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

func TestConfiguredLeafPageReadCacheEntriesReadsEnvAtOpenTime(t *testing.T) {
	prev := LeafPageReadCacheEntries
	LeafPageReadCacheEntries = 8
	t.Cleanup(func() {
		LeafPageReadCacheEntries = prev
	})
	t.Setenv(leafPageReadCacheEntriesEnvKey, "3")

	if got := configuredLeafPageReadCacheEntries(0); got != 3 {
		t.Fatalf("configuredLeafPageReadCacheEntries()=%d, want env override 3", got)
	}
}

func TestConfiguredLeafPageReadCacheEntriesOptionOverridesEnv(t *testing.T) {
	prev := LeafPageReadCacheEntries
	LeafPageReadCacheEntries = 8
	t.Cleanup(func() {
		LeafPageReadCacheEntries = prev
	})
	t.Setenv(leafPageReadCacheEntriesEnvKey, "3")

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
	t.Setenv(leafPageReadCacheEntriesEnvKey, "3")

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
	t.Setenv(leafPageReadCacheEntriesEnvKey, strconv.Itoa(maxLeafPageReadCacheEntries+1))

	err := validateOptions(Options{})
	if err == nil {
		t.Fatal("validateOptions unexpectedly accepted huge env leaf page read cache")
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

func TestLeafPageReadCacheNilStatsAreStableZeros(t *testing.T) {
	var cache *leafPageReadCache
	stats := cache.stats()
	if stats != (leafPageReadCacheStats{}) {
		t.Fatalf("nil cache stats=%+v, want zero values", stats)
	}
}
