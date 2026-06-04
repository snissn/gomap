package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func groupedCacheOffsets(lengths ...int) [MaxFrameK + 1]uint32 {
	var offsets [MaxFrameK + 1]uint32
	var off uint32
	for i, n := range lengths {
		offsets[i] = off
		off += uint32(n)
	}
	offsets[len(lengths)] = off
	return offsets
}

func groupedCacheRaw(parts ...[]byte) []byte {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	raw := make([]byte, 0, total)
	for _, p := range parts {
		raw = append(raw, p...)
	}
	return raw
}

func newTestGroupedCacheFile(entries int, maxRaw int, maxBytes int64) *File {
	f := &File{
		groupedFrameCacheEntries:  entries,
		groupedFrameCacheMaxRaw:   maxRaw,
		groupedFrameCacheMaxBytes: maxBytes,
	}
	f.resetGroupedFrameCacheLocked()
	return f
}

func TestGroupedFrameCache_StateIsolationAndSubValues(t *testing.T) {
	f := newTestGroupedCacheFile(4, 1024, 0)
	raw := groupedCacheRaw([]byte("aa"), []byte("bbb"), []byte("cccc"))
	offsets := groupedCacheOffsets(2, 3, 4)
	if !f.groupedFrameCacheStore(100, false, 3, offsets, raw, false) {
		t.Fatalf("store grouped frame")
	}

	for i, want := range [][]byte{[]byte("aa"), []byte("bbb"), []byte("cccc")} {
		got, _, err, hit := f.groupedFrameCacheReadTo(100, false, 3, offsets, uint32(len(raw)), i, nil)
		if err != nil {
			t.Fatalf("read sub-value %d: %v", i, err)
		}
		if !hit || !bytes.Equal(got, want) {
			t.Fatalf("sub-value %d hit=%v got=%q want=%q", i, hit, got, want)
		}
	}

	kChangedOffsets := groupedCacheOffsets(2, 7)
	if _, _, err, hit := f.groupedFrameCacheReadTo(100, false, 2, kChangedOffsets, kChangedOffsets[2], 0, nil); err != nil || hit {
		t.Fatalf("cache hit crossed K identity: hit=%v err=%v", hit, err)
	}
	if _, _, _, hit := f.groupedFrameCacheReadTo(100, true, 3, offsets, uint32(len(raw)), 0, nil); hit {
		t.Fatalf("cache hit crossed verifyCRC identity")
	}
	if _, _, _, hit := f.groupedFrameCacheReadTo(101, false, 3, offsets, uint32(len(raw)), 0, nil); hit {
		t.Fatalf("cache hit crossed start identity")
	}
	changedOffsets := groupedCacheOffsets(1, 4, 4)
	if _, _, err, hit := f.groupedFrameCacheReadTo(100, false, 3, changedOffsets, changedOffsets[3], 0, nil); err != nil || hit {
		t.Fatalf("cache hit crossed offset-table identity: hit=%v err=%v", hit, err)
	}
	changedRawLenOffsets := groupedCacheOffsets(2, 3, 5)
	if _, _, err, hit := f.groupedFrameCacheReadTo(100, false, 3, changedRawLenOffsets, changedRawLenOffsets[3], 0, nil); err != nil || hit {
		t.Fatalf("cache hit crossed raw-length identity: hit=%v err=%v", hit, err)
	}
	invalidOffsets := offsets
	invalidOffsets[2] = invalidOffsets[1] - 1
	if _, _, err, hit := f.groupedFrameCacheReadTo(100, false, 3, invalidOffsets, offsets[3], 0, nil); !hit || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected invalid current offset state to fail closed, hit=%v err=%v", hit, err)
	}
}

func TestGroupedFrameCache_BudgetEvictionReleaseStats(t *testing.T) {
	f := newTestGroupedCacheFile(2, 1024, 96)
	offsets := groupedCacheOffsets(64)
	raw1 := bytes.Repeat([]byte{'a'}, 64)
	raw2 := bytes.Repeat([]byte{'b'}, 64)
	firstStart := int64(1)
	secondStart := int64(2)
	cache := f.ensureGroupedFrameCache()
	for cache.shardFor(secondStart, false) != cache.shardFor(firstStart, false) {
		secondStart++
	}
	if !f.groupedFrameCacheStore(firstStart, false, 1, offsets, raw1, true) {
		t.Fatalf("store first frame")
	}
	if !f.groupedFrameCacheStore(secondStart, false, 1, offsets, raw2, true) {
		t.Fatalf("store second frame")
	}
	stats := f.groupedFrameCacheDetailedStats()
	if stats.Entries != 1 {
		t.Fatalf("expected budget eviction to leave one entry, got %d", stats.Entries)
	}
	if stats.RetainedBytes > 96 || stats.RetainedBytes != 64 {
		t.Fatalf("retained bytes=%d want=64 within budget", stats.RetainedBytes)
	}
	if stats.Evictions == 0 {
		t.Fatalf("expected eviction stat")
	}
	if stats.Releases == 0 {
		t.Fatalf("expected pooled release stat")
	}
}

func TestGroupedFrameCache_SkipsOversizeAndBudget(t *testing.T) {
	f := newTestGroupedCacheFile(4, 8, 0)
	offsets := groupedCacheOffsets(16)
	if f.groupedFrameCacheStore(1, false, 1, offsets, bytes.Repeat([]byte{'x'}, 16), false) {
		t.Fatalf("oversize frame was admitted")
	}
	stats := f.groupedFrameCacheDetailedStats()
	if stats.SkippedOversize == 0 || stats.Entries != 0 {
		t.Fatalf("expected oversize skip without entry: %+v", stats)
	}

	f = newTestGroupedCacheFile(4, 1024, 8)
	if f.groupedFrameCacheStore(1, false, 1, offsets, bytes.Repeat([]byte{'x'}, 16), false) {
		t.Fatalf("over-budget frame was admitted")
	}
	stats = f.groupedFrameCacheDetailedStats()
	if stats.SkippedBudget == 0 || stats.Entries != 0 {
		t.Fatalf("expected budget skip without entry: %+v", stats)
	}
}

func TestGroupedFrameCache_DoesNotAdmitAfterClose(t *testing.T) {
	f := newTestGroupedCacheFile(2, 1024, 0)
	f.closed.Store(true)
	offsets := groupedCacheOffsets(16)
	if f.groupedFrameCacheStore(1, false, 1, offsets, bytes.Repeat([]byte{'x'}, 16), true) {
		t.Fatalf("closed file admitted grouped-frame cache entry")
	}
	stats := f.groupedFrameCacheDetailedStats()
	if stats.Entries != 0 || stats.RetainedBytes != 0 {
		t.Fatalf("closed file retained cache state: %+v", stats)
	}
	if cache := f.ensureGroupedFrameCache(); cache != nil {
		t.Fatalf("closed file recreated grouped-frame cache")
	}
}

func TestGroupedFrameCache_InvalidCachedStateFailsClosed(t *testing.T) {
	f := newTestGroupedCacheFile(2, 1024, 0)
	raw := groupedCacheRaw([]byte("good"), []byte("value"))
	offsets := groupedCacheOffsets(4, 5)
	if !f.groupedFrameCacheStore(10, false, 2, offsets, raw, false) {
		t.Fatalf("store grouped frame")
	}

	cache := f.groupedFrameCache.Load()
	shard := cache.shardFor(10, false)
	shard.mu.Lock()
	var slot *groupedFrameCacheSlot
	for i := range shard.slots {
		if shard.slots[i].valid {
			slot = &shard.slots[i]
			break
		}
	}
	if slot == nil {
		shard.mu.Unlock()
		t.Fatalf("expected one entry")
	}
	slot.mu.Lock()
	slot.raw = slot.raw[:len(slot.raw)-1]
	slot.mu.Unlock()
	shard.mu.Unlock()

	_, _, err, hit := f.groupedFrameCacheReadTo(10, false, 2, offsets, uint32(len(raw)), 1, nil)
	if !hit {
		t.Fatalf("expected corrupt cached state to be detected on hit")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestGroupedFrameCache_AfterCloseDoesNotRecreateOrRetain(t *testing.T) {
	f := newTestGroupedCacheFile(2, 1024, 0)
	f.closed.Store(true)
	f.groupedFrameCache.Store(nil)

	raw := bytes.Repeat([]byte{'z'}, 32)
	offsets := groupedCacheOffsets(32)
	if f.groupedFrameCacheStore(1, false, 1, offsets, raw, false) {
		t.Fatalf("closed file admitted grouped-frame cache entry")
	}
	if cache := f.groupedFrameCache.Load(); cache != nil {
		t.Fatalf("closed file recreated grouped-frame cache")
	}
	stats := f.groupedFrameCacheDetailedStats()
	if stats.Entries != 0 || stats.RetainedBytes != 0 || stats.Stores != 0 {
		t.Fatalf("closed file retained grouped-frame cache state: %+v", stats)
	}
	if _, _, err, hit := f.groupedFrameCacheReadTo(1, false, 1, offsets, offsets[1], 0, nil); err != nil || hit {
		t.Fatalf("closed file returned cache hit/state: hit=%v err=%v", hit, err)
	}
}

func TestValueLogManager_GroupedFrameCache_CorruptSourceFailsClosedAfterCachedVerifyRead(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)
	ptrs, want := appendCompressedFrameForCacheTests(t, writer, 0, 4)
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	m.SetDisableReadChecksum(false)
	m.SetGroupedFrameCacheEntries(4)

	f := m.files[fileID]
	f.remapToFileSize()
	got, err := m.ReadUnsafe(ptrs[0])
	if err != nil {
		t.Fatalf("warm verified read: %v", err)
	}
	if !bytes.Equal(got, want[0]) {
		t.Fatalf("warm read mismatch")
	}
	if hits, _, entries, _ := f.groupedFrameCacheStats(); entries == 0 || hits != 0 {
		t.Fatalf("expected verified warm read to populate cache without hit: hits=%d entries=%d", hits, entries)
	}

	fh, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	corruptAt := int64(ptrs[0].Offset-4) + HeaderSize + FrameHeaderSize
	var one [1]byte
	if _, err := fh.ReadAt(one[:], corruptAt); err != nil {
		_ = fh.Close()
		t.Fatalf("read corrupt byte: %v", err)
	}
	one[0] ^= 0x80
	if _, err := fh.WriteAt(one[:], corruptAt); err != nil {
		_ = fh.Close()
		t.Fatalf("write corrupt byte: %v", err)
	}
	if err := fh.Sync(); err != nil {
		_ = fh.Close()
		t.Fatalf("sync corrupt byte: %v", err)
	}
	if err := fh.Close(); err != nil {
		t.Fatalf("close corrupt writer: %v", err)
	}

	_, err = m.ReadUnsafe(ptrs[1])
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("cached verified read masked source corruption: err=%v", err)
	}
}

func TestGroupedFrameCache_ReadMissDoesNotCreateIdleCache(t *testing.T) {
	f := &File{
		groupedFrameCacheEntries:  4,
		groupedFrameCacheMaxRaw:   1024,
		groupedFrameCacheMaxBytes: 1024,
	}
	offsets := groupedCacheOffsets(16)
	if got, _, err, hit := f.groupedFrameCacheReadTo(1, false, 1, offsets, offsets[1], 0, nil); err != nil || hit || got != nil {
		t.Fatalf("idle read should miss without value: hit=%v err=%v got=%q", hit, err, got)
	}
	if cache := f.groupedFrameCache.Load(); cache != nil {
		t.Fatalf("read miss created grouped cache")
	}
	if stats := f.groupedFrameCacheDetailedStats(); stats != (GroupedFrameCacheStats{}) {
		t.Fatalf("read miss retained stats: %+v", stats)
	}
	if !f.groupedFrameCacheStore(1, false, 1, offsets, bytes.Repeat([]byte{'x'}, 16), false) {
		t.Fatalf("store after idle read miss")
	}
	if stats := f.groupedFrameCacheDetailedStats(); stats.Stores != 1 || stats.Entries != 1 || stats.Capacity != 4 {
		t.Fatalf("expected live cache stats after store, got %+v", stats)
	}
}

func TestGroupedFrameCache_StatsDoesNotCreateIdleCache(t *testing.T) {
	f := &File{
		groupedFrameCacheEntries:  4,
		groupedFrameCacheMaxRaw:   1024,
		groupedFrameCacheMaxBytes: 1024,
	}
	if cache := f.groupedFrameCache.Load(); cache != nil {
		t.Fatalf("new file unexpectedly has grouped cache")
	}
	stats := f.groupedFrameCacheDetailedStats()
	if stats != (GroupedFrameCacheStats{}) {
		t.Fatalf("idle stats should be zero, got %+v", stats)
	}
	if cache := f.groupedFrameCache.Load(); cache != nil {
		t.Fatalf("stats created grouped cache")
	}
	offsets := groupedCacheOffsets(16)
	if !f.groupedFrameCacheStore(1, false, 1, offsets, bytes.Repeat([]byte{'x'}, 16), false) {
		t.Fatalf("store after idle stats")
	}
	if stats = f.groupedFrameCacheDetailedStats(); stats.Stores != 1 || stats.Entries != 1 || stats.Capacity != 4 {
		t.Fatalf("expected live cache stats after store, got %+v", stats)
	}
}

func TestGroupedFrameCache_ConfigSettersDoNotCreateIdleCache(t *testing.T) {
	f := &File{}
	f.setGroupedFrameCacheEntries(4)
	f.setGroupedFrameCacheMaxRawBytes(1024)
	f.setGroupedFrameCacheMaxBytes(1024)
	f.setGroupedFrameCacheBudget(newGroupedFrameCacheBudget(2048))
	if cache := f.groupedFrameCache.Load(); cache != nil {
		t.Fatalf("config setters created grouped cache")
	}
	if stats := f.groupedFrameCacheDetailedStats(); stats != (GroupedFrameCacheStats{}) {
		t.Fatalf("idle stats should remain zero after config setters, got %+v", stats)
	}
	offsets := groupedCacheOffsets(16)
	if !f.groupedFrameCacheStore(1, false, 1, offsets, bytes.Repeat([]byte{'x'}, 16), false) {
		t.Fatalf("store after idle config setters")
	}
	stats := f.groupedFrameCacheDetailedStats()
	if stats.Stores != 1 || stats.Entries != 1 || stats.Capacity != 4 || stats.BudgetBytes != 2048 {
		t.Fatalf("expected configured live cache after store, got %+v", stats)
	}
}

func TestGroupedFrameCache_StatsConcurrentAdmissions(t *testing.T) {
	f := newTestGroupedCacheFile(16, 1024, 1024)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = f.groupedFrameCacheDetailedStats()
			}
		}
	}()
	for i := 0; i < 1000; i++ {
		raw := bytes.Repeat([]byte{byte(i)}, 32)
		offsets := groupedCacheOffsets(32)
		_ = f.groupedFrameCacheStore(int64(i), false, 1, offsets, raw, false)
	}
	close(stop)
	wg.Wait()
}

func TestGroupedFrameCache_ConcurrentReadsAndEvictions(t *testing.T) {
	f := newTestGroupedCacheFile(8, 1024, 96)
	baseRaw := groupedCacheRaw([]byte("left"), []byte("right"))
	baseOffsets := groupedCacheOffsets(4, 5)
	if !f.groupedFrameCacheStore(1, false, 2, baseOffsets, append([]byte(nil), baseRaw...), false) {
		t.Fatalf("store base frame")
	}
	if got, _, err, hit := f.groupedFrameCacheReadTo(1, false, 2, baseOffsets, uint32(len(baseRaw)), 0, nil); err != nil || !hit || !bytes.Equal(got, []byte("left")) {
		t.Fatalf("warm cache hit failed: hit=%v err=%v got=%q", hit, err, got)
	}

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				got, _, err, hit := f.groupedFrameCacheReadTo(1, false, 2, baseOffsets, uint32(len(baseRaw)), i&1, nil)
				if err != nil {
					t.Errorf("cache read: %v", err)
					return
				}
				if hit {
					want := []byte("left")
					if i&1 == 1 {
						want = []byte("right")
					}
					if !bytes.Equal(got, want) {
						t.Errorf("cache value mismatch got=%q want=%q", got, want)
						return
					}
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			raw := bytes.Repeat([]byte{byte(i)}, 32)
			offsets := groupedCacheOffsets(32)
			_ = f.groupedFrameCacheStore(int64(100+i), false, 1, offsets, raw, false)
		}
	}()
	wg.Wait()

	stats := f.groupedFrameCacheDetailedStats()
	if stats.Hits == 0 || stats.Misses == 0 || stats.Evictions == 0 {
		t.Fatalf("expected mixed hits/misses/evictions, got %+v", stats)
	}
	if stats.RetainedBytes > stats.BudgetBytes && stats.BudgetBytes > 0 {
		t.Fatalf("retained bytes exceed budget: %+v", stats)
	}
}
