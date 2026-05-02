package db

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/zipper"
)

const (
	leafPageReadCacheEntriesEnvKey  = "TREEDB_LEAF_PAGE_CACHE_ENTRIES"
	defaultLeafPageReadCacheEntries = 4096
)

var LeafPageReadCacheEntries = defaultLeafPageReadCacheEntries

func configuredLeafPageReadCacheEntries() int {
	if raw := strings.TrimSpace(os.Getenv(leafPageReadCacheEntriesEnvKey)); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
			return v
		}
	}
	return LeafPageReadCacheEntries
}

type leafPageReadCacheEntry struct {
	key  leafPageReadCacheKey
	data []byte
}

type leafPageReadCacheKey struct {
	fileID   uint32
	offset   uint64
	subIndex uint16
}

func newLeafPageReadCacheKey(ptr page.LeafLogPtr) leafPageReadCacheKey {
	return leafPageReadCacheKey{
		fileID:   ptr.FileID,
		offset:   ptr.Offset,
		subIndex: ptr.SubIndex,
	}
}

type leafPageReadCacheSlot struct {
	entry atomic.Pointer[leafPageReadCacheEntry]
}

type leafPageReadCache struct {
	slots []leafPageReadCacheSlot

	hits      atomic.Uint64
	misses    atomic.Uint64
	stores    atomic.Uint64
	evictions atomic.Uint64
	entries   atomic.Uint64
}

type leafPageReadCacheStats struct {
	Hits      uint64
	Misses    uint64
	Stores    uint64
	Evictions uint64
	Entries   uint64
	Capacity  uint64
	Bytes     uint64
}

func newLeafPageReadCache(entries int) *leafPageReadCache {
	if entries <= 0 {
		return nil
	}
	return &leafPageReadCache{slots: make([]leafPageReadCacheSlot, entries)}
}

func (c *leafPageReadCache) store(ptr page.LeafLogPtr, leafPage []byte) {
	if c == nil || len(c.slots) == 0 || len(leafPage) != page.PageSize {
		return
	}
	data := make([]byte, page.PageSize)
	copy(data, leafPage)
	key := newLeafPageReadCacheKey(ptr)
	entry := &leafPageReadCacheEntry{key: key, data: data}
	slot := &c.slots[c.slotIndex(key)]
	prev := slot.entry.Swap(entry)
	c.stores.Add(1)
	switch {
	case prev == nil:
		c.entries.Add(1)
	case prev.key != key:
		c.evictions.Add(1)
	}
}

func (c *leafPageReadCache) get(ptr page.LeafLogPtr) ([]byte, bool) {
	if c == nil || len(c.slots) == 0 {
		return nil, false
	}
	key := newLeafPageReadCacheKey(ptr)
	entry := c.slots[c.slotIndex(key)].entry.Load()
	if entry == nil || entry.key != key {
		c.misses.Add(1)
		return nil, false
	}
	c.hits.Add(1)
	return entry.data, true
}

func (c *leafPageReadCache) stats() leafPageReadCacheStats {
	if c == nil || len(c.slots) == 0 {
		return leafPageReadCacheStats{}
	}
	entries := c.entries.Load()
	return leafPageReadCacheStats{
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Stores:    c.stores.Load(),
		Evictions: c.evictions.Load(),
		Entries:   entries,
		Capacity:  uint64(len(c.slots)),
		Bytes:     entries * page.PageSize,
	}
}

func (c *leafPageReadCache) slotIndex(key leafPageReadCacheKey) int {
	h := uint64(key.fileID)
	h ^= key.offset + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	h ^= uint64(key.subIndex) + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	return int(h % uint64(len(c.slots)))
}

type cachedLeafPageReader struct {
	cache    *leafPageReadCache
	fallback interface {
		ReadUnsafe(ptr page.ValuePtr) ([]byte, error)
	}
}

func newCachedLeafPageReader(cache *leafPageReadCache, fallback interface {
	ReadUnsafe(ptr page.ValuePtr) ([]byte, error)
}) *cachedLeafPageReader {
	return &cachedLeafPageReader{cache: cache, fallback: fallback}
}

func (db *DB) leafPageReader(fallback zipper.LeafPageReader) zipper.LeafPageReader {
	if db != nil && db.leafPageReadCache != nil {
		return newCachedLeafPageReader(db.leafPageReadCache, fallback)
	}
	return fallback
}

func (r *cachedLeafPageReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if key, err := page.LeafLogPtrFromValuePtr(ptr); err == nil {
		if data, ok := r.cache.get(key); ok {
			return cloneLeafPageReadCacheData(data), nil
		}
	}
	return r.fallback.ReadUnsafe(ptr)
}

func (r *cachedLeafPageReader) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	if key, err := page.LeafLogPtrFromValuePtr(ptr); err == nil {
		if data, ok := r.cache.get(key); ok {
			if cap(dst) >= len(data) {
				dst = dst[:len(data)]
				copy(dst, data)
				return dst, true, nil
			}
			return cloneLeafPageReadCacheData(data), false, nil
		}
	}
	if toReader, ok := r.fallback.(interface {
		ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error)
	}); ok {
		return toReader.ReadUnsafeTo(ptr, dst)
	}
	data, err := r.fallback.ReadUnsafe(ptr)
	return data, false, err
}

func cloneLeafPageReadCacheData(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	owned := make([]byte, len(data))
	copy(owned, data)
	return owned
}

func (db *DB) storeLeafPageReadCache(ptr page.LeafLogPtr, leafPage []byte) {
	if db == nil {
		return
	}
	db.leafPageReadCache.store(ptr, leafPage)
}
