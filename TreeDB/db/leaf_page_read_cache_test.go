package db

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type leafPageCacheTestFallback struct {
	readUnsafeCalls   int
	readUnsafeToCalls int
	err               error
	data              []byte
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
