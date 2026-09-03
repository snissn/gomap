package valuelog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

const (
	// Keep decoded grouped-frame retention bounded while allowing enough entries
	// to cover hot read working sets. Bytes are additionally capped by the
	// manager-level budget below.
	defaultGroupedFrameCacheEntries     = 2048
	defaultGroupedFrameCacheMaxRawBytes = 1 << 20
	defaultGroupedFrameCacheMaxBytes    = 64 << 20
	valueLogRecordCRCPrefixBytes        = HeaderSize - headerWithoutCRC
)

type sealedLazyMmapDenyReason uint8

const (
	sealedLazyMmapDenyNone sealedLazyMmapDenyReason = iota
	sealedLazyMmapDenyCountCap
	sealedLazyMmapDenyBytesCap
)

// File represents a value-log segment on disk.
type File struct {
	ID                 uint32
	Path               string
	File               *os.File
	manager            *Manager
	RefCount           atomic.Int64
	IsZombie           atomic.Bool
	retryDeletePending atomic.Bool
	stableIdentity     rootpublication.StableIdentity
	stableNamespace    string
	stableObserved     bool
	currentWritable    atomic.Bool
	dictLookup         DictLookup
	templateLookup     TemplateLookup
	templateDecodeOpts templ.DecodeOptions
	templateDefCache   *templateDefCache
	// compactLeafPayloadAllowed is derived at open time from the file ID and
	// path. Hot reads should not re-run filepath parsing for every leaf payload.
	// Tests may still construct File literals directly; those fall back to
	// deriving eligibility from ID/Path in allowsCompactLeafPayload.
	compactLeafPayloadAllowed    bool
	compactLeafPayloadAllowedSet bool

	cacheMu    sync.RWMutex
	groupedMu  sync.Mutex
	scratchMu  sync.Mutex
	cacheStart atomic.Int64
	cacheK     int
	cacheFlags byte
	cacheLen   int
	cacheOffs  [MaxFrameK + 1]uint32
	cacheRaw   []byte
	// decodeScratch retains reusable decode buffers for compressed grouped reads,
	// reducing sync.Pool churn across adjacent and parallel frame misses.
	decodeScratch      []byte
	decodeScratchSpare [][]byte
	// cacheRawPooled tracks whether cacheRaw currently owns a pooled decode
	// scratch buffer that must be returned on eviction.
	cacheRawPooled bool

	groupedFrameCacheEntries  int
	groupedFrameCacheMaxRaw   int
	groupedFrameCacheMaxBytes int64
	groupedFrameCacheBudget   *groupedFrameCacheBudget
	groupedFrameCache         atomic.Pointer[groupedFrameCache]

	closed atomic.Bool

	// mmapData holds the current read-only mapping. Readers load it without locks.
	mmapData         atomic.Value // stores []byte (may be nil slice)
	fileSize         atomic.Int64 // last known on-disk size; 0 means unknown
	verifiedFileSize atomic.Int64 // size observed from stat/barrier; 0 means unknown

	remapMu        sync.Mutex
	remapRequested atomic.Bool

	deadMappings           [][]byte
	deadMappedBytes        atomic.Uint64
	remapCount             atomic.Uint64
	deadMappingsCount      atomic.Uint64
	sealedMapDeniedByCount atomic.Uint64
	sealedMapDeniedByBytes atomic.Uint64
	sealedLazyMmapDenied   atomic.Bool
	// Deny-budget snapshot used to preserve cheap deny fast-paths while still
	// allowing recovery when global sealed mmap budgets are raised.
	sealedLazyMmapDeniedCountCap atomic.Int64
	sealedLazyMmapDeniedBytesCap atomic.Int64
	mmapReadHits                 atomic.Uint64
	mmapReadMissNoMapping        atomic.Uint64
	// mmapReadMissOutOfRange counts reads that miss because the requested record
	// range is not currently covered by the active mmap.
	mmapReadMissOutOfRange atomic.Uint64
	// mmapReadMissDeadMappingCap counts fallback reads where remap growth is
	// skipped because the dead-mapping budget is exhausted.
	mmapReadMissDeadMappingCap atomic.Uint64
	// mmapReadFallbackReadAt counts reads that ultimately fall back to ReadAt.
	mmapReadFallbackReadAt atomic.Uint64
	// readRecordCRCChecks counts value-log record checksum computations on read paths.
	readRecordCRCChecks atomic.Uint64
}

// IsCurrentWritable reports whether this segment may still receive appends.
func (f *File) IsCurrentWritable() bool {
	return f != nil && !f.IsZombie.Load() && f.currentWritable.Load()
}

func (f *File) allowsCompactLeafPayload() bool {
	if f == nil {
		return false
	}
	if !f.compactLeafPayloadAllowedSet {
		return allowsCompactLeafLogPayload(f.ID, f.Path)
	}
	return f.compactLeafPayloadAllowed
}

func (f *File) maybeDecodeLeafLogPayloadTo(payload, dst []byte) ([]byte, bool, bool, error) {
	return maybeDecodeLeafLogPayloadToAllowed(f.allowsCompactLeafPayload(), payload, dst)
}

func (f *File) appendMaybeDecodeLeafLogPayload(dst, payload []byte) ([]byte, error) {
	return appendMaybeDecodeLeafLogPayloadAllowed(f.allowsCompactLeafPayload(), dst, payload)
}

func openFile(path string, id uint32, dictLookup DictLookup, templateLookup TemplateLookup, templateOpts templ.DecodeOptions, templateCache *templateDefCache) (*File, error) {
	f, err := openSegmentReadHandle(path)
	if err != nil {
		return nil, err
	}
	vf := &File{
		ID:                           id,
		Path:                         path,
		File:                         f,
		dictLookup:                   dictLookup,
		templateLookup:               templateLookup,
		templateDecodeOpts:           templateOpts,
		templateDefCache:             templateCache,
		compactLeafPayloadAllowed:    allowsCompactLeafLogPayload(id, path),
		compactLeafPayloadAllowedSet: true,
		groupedFrameCacheEntries:     defaultGroupedFrameCacheEntries,
		groupedFrameCacheMaxRaw:      defaultGroupedFrameCacheMaxRawBytes,
		groupedFrameCacheMaxBytes:    defaultGroupedFrameCacheMaxBytes,
	}
	vf.mmapData.Store([]byte(nil))
	if info, err := f.Stat(); err == nil {
		vf.noteVerifiedFileSize(info.Size())
	}
	return vf, nil
}

func (f *File) noteVerifiedFileSize(size int64) {
	if f == nil || size <= 0 {
		return
	}
	f.fileSize.Store(size)
	f.verifiedFileSize.Store(size)
}

var (
	managerTestHookMu sync.RWMutex
	openSegmentFile   = openFile
	scanSegmentPaths  = listSegments
)

func swapOpenSegmentFileForTest(hook func(path string, id uint32, dictLookup DictLookup, templateLookup TemplateLookup, templateOpts templ.DecodeOptions, templateCache *templateDefCache) (*File, error)) (prev func(path string, id uint32, dictLookup DictLookup, templateLookup TemplateLookup, templateOpts templ.DecodeOptions, templateCache *templateDefCache) (*File, error)) {
	managerTestHookMu.Lock()
	prev = openSegmentFile
	openSegmentFile = hook
	managerTestHookMu.Unlock()
	return prev
}

func swapScanSegmentPathsForTest(hook func(string) ([]segmentInfo, error)) (prev func(string) ([]segmentInfo, error)) {
	managerTestHookMu.Lock()
	prev = scanSegmentPaths
	scanSegmentPaths = hook
	managerTestHookMu.Unlock()
	return prev
}

func currentOpenSegmentFile() func(path string, id uint32, dictLookup DictLookup, templateLookup TemplateLookup, templateOpts templ.DecodeOptions, templateCache *templateDefCache) (*File, error) {
	managerTestHookMu.RLock()
	hook := openSegmentFile
	managerTestHookMu.RUnlock()
	return hook
}

func currentScanSegmentPaths() func(string) ([]segmentInfo, error) {
	managerTestHookMu.RLock()
	hook := scanSegmentPaths
	managerTestHookMu.RUnlock()
	return hook
}

func (f *File) setCacheRawLocked(raw []byte, pooled bool) {
	if f.cacheRawPooled && cap(f.cacheRaw) > 0 {
		f.stashDecodeScratch(f.cacheRaw)
	}
	f.cacheRaw = raw
	f.cacheRawPooled = pooled
}

const (
	fileDecodeScratchSpareKeep      = 2
	fileDecodeScratchRetainMaxBytes = 128 << 10
)

func (f *File) stashDecodeScratch(buf []byte) {
	if cap(buf) == 0 {
		return
	}
	if cap(buf) > maxDecodeScratchKeep {
		putDecodeScratch(buf)
		return
	}
	f.scratchMu.Lock()
	defer f.scratchMu.Unlock()
	f.stashDecodeScratchLocked(buf[:0])
}

func (f *File) stashDecodeScratchLocked(buf []byte) {
	if cap(buf) == 0 {
		return
	}
	if cap(buf) > fileDecodeScratchRetainMaxBytes {
		putDecodeScratch(buf)
		return
	}
	if cap(f.decodeScratch) == 0 {
		f.decodeScratch = buf
		if len(f.decodeScratchSpare) != 0 {
			f.trimDecodeScratchLocked()
		}
		return
	}
	if cap(buf) > cap(f.decodeScratch) {
		old := f.decodeScratch
		f.decodeScratch = buf
		buf = old
	}
	if len(f.decodeScratchSpare) < fileDecodeScratchSpareKeep {
		f.decodeScratchSpare = append(f.decodeScratchSpare, buf[:0])
		f.trimDecodeScratchLocked()
		return
	}
	putDecodeScratch(buf)
	f.trimDecodeScratchLocked()
}

func (f *File) trimDecodeScratchLocked() {
	total := cap(f.decodeScratch)
	for _, spare := range f.decodeScratchSpare {
		total += cap(spare)
	}
	for total > fileDecodeScratchRetainMaxBytes && len(f.decodeScratchSpare) > 0 {
		last := len(f.decodeScratchSpare) - 1
		buf := f.decodeScratchSpare[last]
		f.decodeScratchSpare[last] = nil
		f.decodeScratchSpare = f.decodeScratchSpare[:last]
		total -= cap(buf)
		putDecodeScratch(buf)
	}
	if total > fileDecodeScratchRetainMaxBytes && cap(f.decodeScratch) > 0 {
		buf := f.decodeScratch
		f.decodeScratch = nil
		putDecodeScratch(buf)
	}
}

func (f *File) decodeScratchRetainedLocked() (buffers int, bytes uint64) {
	if cap(f.decodeScratch) > 0 {
		buffers++
		bytes += uint64(cap(f.decodeScratch))
	}
	for _, buf := range f.decodeScratchSpare {
		if cap(buf) == 0 {
			continue
		}
		buffers++
		bytes += uint64(cap(buf))
	}
	return buffers, bytes
}

func (f *File) addDecodeScratchStats(stats *DecodeScratchStats) {
	if f == nil || stats == nil {
		return
	}
	f.scratchMu.Lock()
	buffers, bytes := f.decodeScratchRetainedLocked()
	f.scratchMu.Unlock()
	if buffers == 0 {
		return
	}
	stats.FileRetainedFiles++
	stats.FileRetainedBuffers += uint64(buffers)
	stats.FileRetainedBytes += bytes
}

func (f *File) takeDecodeScratch(minCap int) []byte {
	if minCap <= 0 {
		return nil
	}
	f.scratchMu.Lock()
	if cap(f.decodeScratch) >= minCap {
		scratch := f.decodeScratch
		f.decodeScratch = nil
		f.scratchMu.Unlock()
		return scratch[:0]
	}
	for i := len(f.decodeScratchSpare) - 1; i >= 0; i-- {
		if cap(f.decodeScratchSpare[i]) < minCap {
			continue
		}
		scratch := f.decodeScratchSpare[i]
		last := len(f.decodeScratchSpare) - 1
		f.decodeScratchSpare[i] = f.decodeScratchSpare[last]
		f.decodeScratchSpare[last] = nil
		f.decodeScratchSpare = f.decodeScratchSpare[:last]
		f.scratchMu.Unlock()
		return scratch[:0]
	}
	f.scratchMu.Unlock()
	return getDecodeScratch(minCap)
}

func (f *File) releaseDecodeScratch(buf []byte) {
	if cap(buf) == 0 {
		return
	}
	if cap(buf) > maxDecodeScratchKeep {
		putDecodeScratch(buf)
		return
	}
	buf = buf[:0]
	f.scratchMu.Lock()
	f.stashDecodeScratchLocked(buf)
	f.scratchMu.Unlock()
}

func (f *File) resetGroupedFrameCacheLocked() {
	if f == nil {
		return
	}
	old := f.groupedFrameCache.Load()
	f.groupedFrameCache.Store(nil)
	if old == nil {
		return
	}
	old.clear()
	if f.closed.Load() {
		return
	}
	f.groupedFrameCache.Store(newGroupedFrameCache(f, f.groupedFrameCacheEntries, f.groupedFrameCacheMaxRaw, f.groupedFrameCacheMaxBytes, f.groupedFrameCacheBudget))
}

func (f *File) resetGroupedFrameCacheIfPresentLocked() {
	if f == nil || f.groupedFrameCache.Load() == nil {
		return
	}
	f.resetGroupedFrameCacheLocked()
}

func (f *File) setGroupedFrameCacheBudget(budget *groupedFrameCacheBudget) {
	f.groupedMu.Lock()
	if f.groupedFrameCacheBudget == budget {
		f.groupedMu.Unlock()
		return
	}
	f.groupedFrameCacheBudget = budget
	f.resetGroupedFrameCacheIfPresentLocked()
	f.groupedMu.Unlock()
}

func (f *File) setGroupedFrameCacheEntries(entries int) {
	if entries < 0 {
		entries = 0
	}
	f.groupedMu.Lock()
	if f.groupedFrameCacheEntries == entries {
		f.groupedMu.Unlock()
		return
	}
	f.groupedFrameCacheEntries = entries
	f.resetGroupedFrameCacheIfPresentLocked()
	f.groupedMu.Unlock()
}

func (f *File) setGroupedFrameCacheMaxRawBytes(maxRaw int) {
	if maxRaw < 0 {
		maxRaw = 0
	}
	f.groupedMu.Lock()
	if f.groupedFrameCacheMaxRaw == maxRaw {
		f.groupedMu.Unlock()
		return
	}
	f.groupedFrameCacheMaxRaw = maxRaw
	f.resetGroupedFrameCacheIfPresentLocked()
	f.groupedMu.Unlock()
}

func (f *File) setGroupedFrameCacheMaxBytes(maxBytes int64) {
	if maxBytes < 0 {
		maxBytes = 0
	}
	f.groupedMu.Lock()
	if f.groupedFrameCacheMaxBytes == maxBytes {
		f.groupedMu.Unlock()
		return
	}
	f.groupedFrameCacheMaxBytes = maxBytes
	f.resetGroupedFrameCacheIfPresentLocked()
	f.groupedMu.Unlock()
}

func (f *File) ensureGroupedFrameCache() *groupedFrameCache {
	if f == nil || f.closed.Load() {
		return nil
	}
	if cache := f.groupedFrameCache.Load(); cache != nil {
		return cache
	}
	f.groupedMu.Lock()
	cache := f.groupedFrameCache.Load()
	if cache == nil && !f.closed.Load() {
		cache = newGroupedFrameCache(f, f.groupedFrameCacheEntries, f.groupedFrameCacheMaxRaw, f.groupedFrameCacheMaxBytes, f.groupedFrameCacheBudget)
		f.groupedFrameCache.Store(cache)
	}
	f.groupedMu.Unlock()
	return cache
}

func (f *File) groupedFrameCacheReadTo(start int64, verifyCRC bool, expectedK int, expectedOffsets *[MaxFrameK + 1]uint32, expectedRawLen uint32, subIndex int, dst []byte) (out []byte, usedDst bool, err error, hit bool) {
	if f == nil || f.closed.Load() {
		return nil, false, nil, false
	}
	cache := f.groupedFrameCache.Load()
	return cache.readTo(start, verifyCRC, expectedK, expectedOffsets, expectedRawLen, subIndex, dst, f)
}

func (f *File) groupedFrameCacheStore(start int64, verifyCRC bool, k int, offsets [MaxFrameK + 1]uint32, raw []byte, pooled bool) bool {
	if f == nil || f.closed.Load() {
		return false
	}
	cache := f.ensureGroupedFrameCache()
	stored := cache.store(start, verifyCRC, k, offsets, raw, pooled)
	if stored && (f.closed.Load() || f.groupedFrameCache.Load() != cache) {
		// A rare configuration reset/close raced with admission. The cache took
		// ownership of raw; clear the now-unpublished/closed cache so pooled buffers
		// and budget reservations are released instead of leaking. Keep returning
		// true so callers do not release an already-owned pooled raw buffer again.
		cache.clear()
	}
	return stored
}

func (f *File) groupedFrameCacheStats() (hits, misses uint64, entries, capacity int) {
	stats := f.groupedFrameCacheDetailedStats()
	return stats.Hits, stats.Misses, stats.Entries, stats.Capacity
}

func (f *File) groupedFrameCacheDetailedStats() GroupedFrameCacheStats {
	if f == nil {
		return GroupedFrameCacheStats{}
	}
	cache := f.groupedFrameCache.Load()
	if cache == nil {
		return GroupedFrameCacheStats{}
	}
	return cache.stats()
}

func (f *File) groupedFrameCacheAllowsRaw(rawLen int) bool {
	if f == nil || f.closed.Load() {
		return false
	}
	f.groupedMu.Lock()
	entries := f.groupedFrameCacheEntries
	maxRaw := f.groupedFrameCacheMaxRaw
	maxBytes := f.groupedFrameCacheMaxBytes
	f.groupedMu.Unlock()
	if entries <= 0 || rawLen <= 0 {
		return false
	}
	if maxRaw > 0 && rawLen > maxRaw {
		return false
	}
	return maxBytes <= 0 || int64(rawLen) <= maxBytes
}

func (f *File) Close() error {
	if f == nil || f.File == nil {
		return nil
	}
	var scratch []byte
	if !f.closed.CompareAndSwap(false, true) {
		return nil
	}
	f.cacheMu.Lock()
	f.cacheK = 0
	f.cacheFlags = 0
	f.cacheLen = 0
	f.setCacheRawLocked(nil, false)
	f.cacheStart.Store(0)
	f.cacheMu.Unlock()
	f.scratchMu.Lock()
	scratch = f.decodeScratch
	f.decodeScratch = nil
	scratchSpare := f.decodeScratchSpare
	f.decodeScratchSpare = nil
	f.scratchMu.Unlock()
	f.groupedMu.Lock()
	cache := f.groupedFrameCache.Load()
	f.groupedFrameCache.Store(nil)
	f.groupedMu.Unlock()
	if cache != nil {
		cache.clear()
	}
	if scratch != nil {
		putDecodeScratch(scratch)
	}
	for _, buf := range scratchSpare {
		putDecodeScratch(buf)
	}

	f.remapMu.Lock()
	data, _ := f.mmapData.Load().([]byte)
	if data != nil {
		_ = munmap(data)
	}
	for _, b := range f.deadMappings {
		_ = munmap(b)
	}
	f.deadMappings = nil
	f.deadMappingsCount.Store(0)
	f.deadMappedBytes.Store(0)
	f.mmapData.Store([]byte(nil))
	f.remapMu.Unlock()

	return f.File.Close()
}

func (f *File) Read(ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("valuelog: nil file")
	}
	if err := f.ensureCurrentWritableReadableFor(ptr); err != nil {
		return nil, err
	}
	if val, err, ok := f.readViaMmap(ptr, verifyCRC); ok {
		f.mmapReadHits.Add(1)
		if err != nil {
			return nil, err
		}
		val, _, _, err = f.maybeDecodeLeafLogPayloadTo(val, nil)
		if err != nil {
			return nil, err
		}
		return val, nil
	}
	data, _ := f.mmapData.Load().([]byte)
	if data != nil && deadMappingsCapExhausted(f.deadMappingsCount.Load(), len(data)) {
		f.mmapReadMissDeadMappingCap.Add(1)
	}
	if !verifyCRC {
		if val, _, err, ok := f.readGroupedCompressedFromFileTo(ptr, nil); ok {
			f.mmapReadFallbackReadAt.Add(1)
			if err != nil {
				return nil, err
			}
			val, _, _, err = f.maybeDecodeLeafLogPayloadTo(val, nil)
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	}
	f.mmapReadFallbackReadAt.Add(1)
	return f.readAtWithDict(ptr, verifyCRC)
}

func (f *File) ReadUnsafe(ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("valuelog: nil file")
	}
	if err := f.ensureCurrentWritableReadableFor(ptr); err != nil {
		return nil, err
	}
	if val, err, ok := f.readViaMmapView(ptr, verifyCRC); ok {
		f.mmapReadHits.Add(1)
		if err != nil {
			return nil, err
		}
		val, _, _, err = f.maybeDecodeLeafLogPayloadTo(val, nil)
		if err != nil {
			return nil, err
		}
		return val, nil
	}
	if !f.usesPersistentMmap() {
		if f.tryEnableSealedLazyMmap() {
			if val, err, ok := f.readViaMmapView(ptr, verifyCRC); ok {
				f.mmapReadHits.Add(1)
				if err != nil {
					return nil, err
				}
				val, _, _, err = f.maybeDecodeLeafLogPayloadTo(val, nil)
				if err != nil {
					return nil, err
				}
				return val, nil
			}
		}
		f.mmapReadFallbackReadAt.Add(1)
		return f.readAtWithDict(ptr, verifyCRC)
	}
	// Avoid per-read Stat/lock churn once we have exhausted the dead-mapping
	// budget: remapToFileSize won't be able to grow the mapping safely again.
	data, _ := f.mmapData.Load().([]byte)
	if !deadMappingsCapExhausted(f.deadMappingsCount.Load(), len(data)) {
		f.remapToFileSize()
		if val, err, ok := f.readViaMmapView(ptr, verifyCRC); ok {
			f.mmapReadHits.Add(1)
			if err != nil {
				return nil, err
			}
			val, _, _, err = f.maybeDecodeLeafLogPayloadTo(val, nil)
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	} else {
		f.mmapReadMissDeadMappingCap.Add(1)
	}
	f.mmapReadFallbackReadAt.Add(1)
	return f.readAtWithDict(ptr, verifyCRC)
}

// ReadUnsafeTo is like ReadUnsafe, but it may return a slice backed by dst
// when dst has enough capacity for the decoded value. Callers must keep dst
// alive and avoid reusing it while they use the returned slice when usedDst is true.
func (f *File) ReadUnsafeTo(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, bool, error) {
	if f == nil || f.File == nil {
		return nil, false, errors.New("valuelog: nil file")
	}
	if err := f.ensureCurrentWritableReadableFor(ptr); err != nil {
		return nil, false, err
	}
	if val, usedDst, err, ok := f.readViaMmapViewTo(ptr, verifyCRC, dst); ok {
		f.mmapReadHits.Add(1)
		if err != nil {
			return nil, false, err
		}
		val, compactUsedDst, compactDecoded, err := f.maybeDecodeLeafLogPayloadTo(val, dst)
		if err != nil {
			return nil, false, err
		}
		if compactDecoded {
			return val, compactUsedDst, nil
		}
		return val, usedDst, nil
	}
	if !f.usesPersistentMmap() {
		if f.tryEnableSealedLazyMmap() {
			if val, usedDst, err, ok := f.readViaMmapViewTo(ptr, verifyCRC, dst); ok {
				f.mmapReadHits.Add(1)
				if err != nil {
					return nil, false, err
				}
				val, compactUsedDst, compactDecoded, err := f.maybeDecodeLeafLogPayloadTo(val, dst)
				if err != nil {
					return nil, false, err
				}
				if compactDecoded {
					return val, compactUsedDst, nil
				}
				return val, usedDst, nil
			}
		}
		f.mmapReadFallbackReadAt.Add(1)
		if !verifyCRC {
			if val, usedDst, err, ok := f.readGroupedCompressedFromFileTo(ptr, dst); ok {
				if err != nil {
					return nil, false, err
				}
				val, compactUsedDst, compactDecoded, err := f.maybeDecodeLeafLogPayloadTo(val, dst)
				if err != nil {
					return nil, false, err
				}
				if compactDecoded {
					return val, compactUsedDst, nil
				}
				return val, usedDst, nil
			}
		}
		return f.readAtWithDictTo(ptr, verifyCRC, dst)
	}
	// Avoid per-read Stat/lock churn once we have exhausted the dead-mapping
	// budget: remapToFileSize won't be able to grow the mapping safely again.
	data, _ := f.mmapData.Load().([]byte)
	if !deadMappingsCapExhausted(f.deadMappingsCount.Load(), len(data)) {
		f.remapToFileSize()
		if val, usedDst, err, ok := f.readViaMmapViewTo(ptr, verifyCRC, dst); ok {
			f.mmapReadHits.Add(1)
			if err != nil {
				return nil, false, err
			}
			val, compactUsedDst, compactDecoded, err := f.maybeDecodeLeafLogPayloadTo(val, dst)
			if err != nil {
				return nil, false, err
			}
			if compactDecoded {
				return val, compactUsedDst, nil
			}
			return val, usedDst, nil
		}
	} else {
		f.mmapReadMissDeadMappingCap.Add(1)
	}
	f.mmapReadFallbackReadAt.Add(1)
	if !verifyCRC {
		if val, usedDst, err, ok := f.readGroupedCompressedFromFileTo(ptr, dst); ok {
			if err != nil {
				return nil, false, err
			}
			val, compactUsedDst, compactDecoded, err := f.maybeDecodeLeafLogPayloadTo(val, dst)
			if err != nil {
				return nil, false, err
			}
			if compactDecoded {
				return val, compactUsedDst, nil
			}
			return val, usedDst, nil
		}
	}
	return f.readAtWithDictTo(ptr, verifyCRC, dst)
}

// readGroupedCompressedFromFileTo handles grouped+compressed reads on the
// non-mmap fallback path while reusing File grouped-frame cache entries.
//
// ok=false means the caller should fall back to the generic ReadAtWithDictTo
// decoder path (for non-grouped or uncompressed cases).
func (f *File) readGroupedCompressedFromFileTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error, bool) {
	return f.readGroupedCompressedFromFileToVerify(ptr, false, dst)
}

func (f *File) readGroupedCompressedFromFileToVerify(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, bool, error, bool) {
	if f == nil || f.File == nil {
		return nil, false, errors.New("valuelog: nil file"), true
	}
	if ptr.Offset < 4 || !page.ValuePtrIsGrouped(ptr) {
		return nil, false, nil, false
	}

	start := int64(ptr.Offset - 4)
	header := getHeaderScratch()
	defer putHeaderScratch(header)
	if _, err := f.File.ReadAt(header[:], start); err != nil {
		return nil, false, err, true
	}
	if header[4] != Version {
		return nil, false, ErrCorrupt, true
	}
	if header[5]&recordFlagGrouped == 0 {
		return nil, false, nil, false
	}
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return nil, false, ErrRecordTooLarge, true
	}
	expectedLen := uint32(headerWithoutCRC) + valueLen
	if !page.ValuePtrRecordLengthHintMatches(ptr, expectedLen) {
		return nil, false, ErrCorrupt, true
	}
	if int(valueLen) < FrameHeaderSize {
		return nil, false, ErrCorrupt, true
	}
	var verifiedPayload []byte
	if verifyCRC {
		payloadScratch := getDecodeScratch(int(valueLen))
		verifiedPayload = payloadScratch[:valueLen]
		defer putDecodeScratch(payloadScratch)
		if _, err := f.File.ReadAt(verifiedPayload, start+HeaderSize); err != nil {
			return nil, false, err, true
		}
		f.noteRecordCRCCheck()
		if crc.ChecksumParts(header[4:], verifiedPayload) != binary.LittleEndian.Uint32(header[:4]) {
			return nil, false, ErrCorrupt, true
		}
	}

	frameOff := start + HeaderSize
	var frameHeader [FrameHeaderSize]byte
	if verifyCRC {
		copy(frameHeader[:], verifiedPayload[:FrameHeaderSize])
	} else {
		if _, err := f.File.ReadAt(frameHeader[:], frameOff); err != nil {
			return nil, false, err, true
		}
	}
	if frameHeader[0] != FrameVersion {
		return nil, false, ErrCorrupt, true
	}
	k := int(frameHeader[2])
	if k <= 0 || k > MaxFrameK {
		return nil, false, ErrCorrupt, true
	}
	compressed := frameHeader[1]&FrameFlagCompressed != 0
	if !compressed && !verifyCRC {
		return nil, false, nil, false
	}

	subIndex := int(page.ValuePtrSubIndex(ptr))
	if subIndex < 0 || subIndex >= k {
		return nil, false, ErrCorrupt, true
	}
	ridBytes := k * 8
	offsetBytes := (k + 1) * 4
	prefixLen := FrameHeaderSize + ridBytes + offsetBytes
	if int(valueLen) < prefixLen {
		return nil, false, ErrCorrupt, true
	}

	off := FrameHeaderSize + ridBytes
	const maxPrefixLen = FrameHeaderSize + (MaxFrameK * 8) + ((MaxFrameK + 1) * 4)
	var prefix [maxPrefixLen]byte
	if verifyCRC {
		copy(prefix[:prefixLen], verifiedPayload[:prefixLen])
	} else {
		if _, err := f.File.ReadAt(prefix[:prefixLen], frameOff); err != nil {
			return nil, false, err, true
		}
	}

	var offsets [MaxFrameK + 1]uint32
	prev := uint32(0)
	for i := 0; i < k+1; i++ {
		cur := binary.LittleEndian.Uint32(prefix[off : off+4])
		if cur < prev {
			return nil, false, ErrCorrupt, true
		}
		offsets[i] = cur
		prev = cur
		off += 4
	}
	rawLen := offsets[k]
	if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
		return nil, false, ErrRecordTooLarge, true
	}
	valStart := offsets[subIndex]
	valEnd := offsets[subIndex+1]
	if valEnd < valStart || valEnd > rawLen {
		return nil, false, ErrCorrupt, true
	}
	cacheableRaw := compressed && f.groupedFrameCacheAllowsRaw(int(rawLen))
	if compressed {
		if out, usedDst, err, hit := f.groupedFrameCacheReadTo(start, verifyCRC, k, &offsets, rawLen, subIndex, dst); hit {
			return out, usedDst, err, true
		}
	}

	frame := FrameHeader{
		Version:  frameHeader[0],
		Flags:    frameHeader[1],
		K:        uint8(k),
		Reserved: frameHeader[3],
		DictID:   binary.LittleEndian.Uint64(frameHeader[4:12]),
	}

	framePayloadLen := int(valueLen) - prefixLen
	if framePayloadLen < 0 {
		return nil, false, ErrCorrupt, true
	}
	var framePayload []byte
	var payloadScratch []byte
	if verifyCRC {
		framePayload = verifiedPayload[prefixLen:]
	} else {
		payloadScratch = getDecodeScratch(framePayloadLen)
		framePayload = payloadScratch[:framePayloadLen]
		if _, err := f.File.ReadAt(framePayload, frameOff+int64(prefixLen)); err != nil {
			putDecodeScratch(payloadScratch)
			return nil, false, err, true
		}
	}

	raw := framePayload
	pooledRaw := false
	var err error
	if compressed {
		raw = f.takeDecodeScratch(int(rawLen))
		pooledRaw = true
		raw, err = decodeFramePayloadTo(frame, framePayload, f.dictLookup, rawLen, raw)
	}
	if payloadScratch != nil {
		putDecodeScratch(payloadScratch)
	}
	if err != nil {
		if pooledRaw {
			f.releaseDecodeScratch(raw)
		}
		return nil, false, err, true
	}
	if uint32(len(raw)) != rawLen {
		if pooledRaw {
			f.releaseDecodeScratch(raw)
		}
		return nil, false, ErrCorrupt, true
	}
	val := raw[valStart:valEnd]
	if f.templateLookup != nil && templ.IsEncodedPayload(val) {
		encoded := append([]byte(nil), val...)
		cachedRaw := false
		if cacheableRaw {
			cachedRaw = f.groupedFrameCacheStore(start, verifyCRC, k, offsets, raw, pooledRaw)
		}
		if pooledRaw && !cachedRaw {
			f.releaseDecodeScratch(raw)
		}
		decoded, err := templ.DecodePayloadAppend(nil, encoded, func(id uint64) (templ.TemplateDef, error) {
			return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
		}, f.templateDecodeOpts)
		if err != nil {
			return nil, false, err, true
		}
		return decoded, false, nil, true
	}

	if dst != nil && cap(dst) >= len(val) {
		out := dst[:len(val)]
		copy(out, val)
		cachedRaw := false
		if cacheableRaw {
			cachedRaw = f.groupedFrameCacheStore(start, verifyCRC, k, offsets, raw, pooledRaw)
		}
		if pooledRaw && !cachedRaw {
			f.releaseDecodeScratch(raw)
		}
		return out, true, nil, true
	}
	out := make([]byte, len(val))
	copy(out, val)
	cachedRaw := false
	if cacheableRaw {
		cachedRaw = f.groupedFrameCacheStore(start, verifyCRC, k, offsets, raw, pooledRaw)
	}
	if pooledRaw && !cachedRaw {
		f.releaseDecodeScratch(raw)
	}
	return out, false, nil, true
}

func (f *File) ReadAppend(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("valuelog: nil file")
	}
	if err := f.ensureCurrentWritableReadableFor(ptr); err != nil {
		return nil, err
	}
	if val, err, ok := f.readViaMmapAppend(ptr, verifyCRC, dst); ok {
		f.mmapReadHits.Add(1)
		return val, err
	}
	if verifyCRC {
		if val, usedDst, err, ok := f.readGroupedCompressedFromFileToVerify(ptr, true, dst[len(dst):cap(dst)]); ok {
			f.mmapReadFallbackReadAt.Add(1)
			if err != nil {
				return nil, err
			}
			if !usedDst {
				noteGrowReadAppendCompressedFallback(len(val))
				noteGrowReadAppendCompressedFallbackDst(dst, len(val))
				if len(dst) == 0 {
					out, _, _, err := f.maybeDecodeLeafLogPayloadTo(val, nil)
					return out, err
				}
			}
			return f.appendMaybeDecodeLeafLogPayload(dst, val)
		}
	}
	// Fast path (bench/unsafe reads): grouped + uncompressed + no CRC.
	if !verifyCRC && page.ValuePtrIsGrouped(ptr) && ptr.Offset >= 4 {
		f.mmapReadFallbackReadAt.Add(1)
		start := int64(ptr.Offset - 4)

		// Read header to discover the frame length/flags.
		var header [HeaderSize]byte
		if _, err := f.File.ReadAt(header[:], start); err != nil {
			return nil, err
		}
		if header[4] != Version {
			return nil, ErrCorrupt
		}
		if header[5]&recordFlagGrouped == 0 {
			return nil, ErrCorrupt
		}
		valueLen := binary.LittleEndian.Uint32(header[16:20])
		if recordSizeExceedsMax(valueLen) {
			return nil, ErrRecordTooLarge
		}

		frameOff := start + HeaderSize
		subIndex := int(page.ValuePtrSubIndex(ptr))

		// Cache hit?
		if f.cacheStart.Load() == start {
			f.cacheMu.RLock()
			hit := f.cacheStart.Load() == start && f.cacheK > 0 && f.cacheLen > 0 && subIndex < f.cacheK && f.cacheFlags&FrameFlagCompressed == 0
			if hit {
				prefixLen := f.cacheLen
				valStart := f.cacheOffs[subIndex]
				valEnd := f.cacheOffs[subIndex+1]
				rawLen := f.cacheOffs[f.cacheK]
				f.cacheMu.RUnlock()

				if valEnd < valStart || valEnd > rawLen {
					return nil, ErrCorrupt
				}
				if prefixLen+int(rawLen) != int(valueLen) {
					return nil, ErrCorrupt
				}

				return f.appendPayloadFromFile(dst, frameOff+int64(prefixLen)+int64(valStart), int(valEnd-valStart))
			}
			f.cacheMu.RUnlock()
		}

		// Cache miss: read frame header to get k/flags, then read full prefix.
		var frameHeader [FrameHeaderSize]byte
		if _, err := f.File.ReadAt(frameHeader[:], frameOff); err != nil {
			return nil, err
		}
		if frameHeader[0] != FrameVersion {
			return nil, ErrCorrupt
		}
		k := int(frameHeader[2])
		if k <= 0 || k > MaxFrameK {
			return nil, ErrCorrupt
		}
		fFlags := frameHeader[1]
		if fFlags&FrameFlagCompressed != 0 {
			// Fallback to the full decoder path. Prefer decode-to-tail so hot
			// append callers can reuse dst capacity and avoid per-read allocs.
			return f.appendDecodedRecordTo(ptr, verifyCRC, dst)
		}
		ridBytes := k * 8
		offsetBytes := (k + 1) * 4
		prefixLen := FrameHeaderSize + ridBytes + offsetBytes
		if int(valueLen) < prefixLen {
			return nil, ErrCorrupt
		}

		const maxPrefixLen = FrameHeaderSize + (MaxFrameK * 8) + ((MaxFrameK + 1) * 4)
		var prefix [maxPrefixLen]byte
		if _, err := f.File.ReadAt(prefix[:prefixLen], frameOff); err != nil {
			return nil, err
		}
		if subIndex < 0 || subIndex >= k {
			return nil, ErrCorrupt
		}

		off := FrameHeaderSize + ridBytes
		var offsets [MaxFrameK + 1]uint32
		prev := uint32(0)
		for i := 0; i < k+1; i++ {
			cur := binary.LittleEndian.Uint32(prefix[off : off+4])
			if cur < prev {
				return nil, ErrCorrupt
			}
			offsets[i] = cur
			prev = cur
			off += 4
		}

		rawLen := offsets[k]
		if limits.MaxRecordSize > 0 && int64(rawLen) > limits.MaxRecordSize {
			return nil, ErrRecordTooLarge
		}
		if prefixLen+int(rawLen) != int(valueLen) {
			return nil, ErrCorrupt
		}
		valStart := offsets[subIndex]
		valEnd := offsets[subIndex+1]
		if valEnd < valStart || valEnd > rawLen {
			return nil, ErrCorrupt
		}

		// Publish prefix cache for future reads from this same grouped record.
		f.cacheMu.Lock()
		f.cacheK = k
		f.cacheFlags = fFlags
		f.cacheLen = prefixLen
		f.cacheOffs = offsets
		f.cacheStart.Store(start)
		f.cacheMu.Unlock()

		return f.appendPayloadFromFile(dst, frameOff+int64(prefixLen)+int64(valStart), int(valEnd-valStart))
	}

	// Slow path: use existing decoder and append.
	f.mmapReadFallbackReadAt.Add(1)
	return f.appendDecodedRecordTo(ptr, verifyCRC, dst)
}

func (f *File) readAtWithDict(ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	if verifyCRC {
		f.noteRecordCRCCheck()
	}
	return ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
}

func (f *File) readAtWithDictTo(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, bool, error) {
	if verifyCRC {
		f.noteRecordCRCCheck()
	}
	return readAtWithDictToScratch(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts, dst, f.takeDecodeScratch, f.releaseDecodeScratch)
}

func (f *File) appendDecodedRecordTo(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("valuelog: nil file")
	}
	oldLen := len(dst)
	var tail []byte
	if oldLen <= cap(dst) {
		tail = dst[oldLen:cap(dst)]
	}
	val, usedDst, err := f.readAtWithDictTo(ptr, verifyCRC, tail[:0])
	if err != nil {
		return nil, err
	}
	if usedDst {
		return dst[:oldLen+len(val)], nil
	}
	noteGrowReadAppendCompressedFallback(len(val))
	noteGrowReadAppendCompressedFallbackDst(dst, len(val))
	// When the caller started with an empty destination, ReadAtWithDictTo has
	// already produced the decoded value as a standalone slice. Returning it
	// directly avoids a second allocation+copy in the compressed fallback path.
	if oldLen == 0 {
		return val, nil
	}
	dst = grow(dst, len(val))
	copy(dst[oldLen:], val)
	return dst, nil
}

func (f *File) ReadUnsafeAppend(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error) {
	return f.ReadAppend(ptr, verifyCRC, dst)
}

// ReadRIDUnverified reads only the record id metadata for a same-process
// command-WAL reference. It intentionally skips payload verification.
func (f *File) ReadRIDUnverified(ptr page.ValuePtr) (uint64, error) {
	if f == nil || f.File == nil {
		return 0, errors.New("valuelog: nil file")
	}
	if err := f.ensureCurrentWritableReadableFor(ptr); err != nil {
		return 0, err
	}
	return ReadRIDAtUnverified(f.File, f.ID, ptr)
}

func (f *File) appendPayloadFromFile(dst []byte, off int64, payloadLen int) ([]byte, error) {
	oldLen := len(dst)
	noteGrowReadAppendPayload(payloadLen)
	dst = grow(dst, payloadLen)
	payload := dst[oldLen : oldLen+payloadLen]
	if _, err := f.File.ReadAt(payload, off); err != nil {
		return nil, err
	}
	if f.templateLookup == nil || !templ.IsEncodedPayload(payload) {
		return f.appendMaybeDecodeLeafLogPayload(dst[:oldLen], payload)
	}
	encoded := append([]byte(nil), payload...)
	decoded, err := appendDecodedTemplatePayload(dst[:oldLen], encoded, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
	if err != nil {
		return nil, err
	}
	return f.appendMaybeDecodeLeafLogPayload(decoded[:oldLen], decoded[oldLen:])
}

// Set is an immutable snapshot of value-log files for snapshot isolation.
type Set struct {
	Files               map[uint32]*File
	RefCount            atomic.Int64
	disableReadChecksum bool
}

func (s *Set) ReadChecksumEnabled() bool {
	if s == nil {
		return true
	}
	return !s.disableReadChecksum
}

func (s *Set) Read(ptr page.ValuePtr) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, &fileNotFoundError{id: ptr.FileID, inSnapshot: true}
	}
	return f.Read(ptr, !s.disableReadChecksum)
}

func (s *Set) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, &fileNotFoundError{id: ptr.FileID, inSnapshot: true}
	}
	return f.ReadUnsafe(ptr, !s.disableReadChecksum)
}

func (s *Set) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, false, &fileNotFoundError{id: ptr.FileID, inSnapshot: true}
	}
	return f.ReadUnsafeTo(ptr, !s.disableReadChecksum, dst)
}

func (s *Set) ReadAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, &fileNotFoundError{id: ptr.FileID, inSnapshot: true}
	}
	return f.ReadAppend(ptr, !s.disableReadChecksum, dst)
}

func (s *Set) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, &fileNotFoundError{id: ptr.FileID, inSnapshot: true}
	}
	return f.ReadUnsafeAppend(ptr, !s.disableReadChecksum, dst)
}

// ReadUnsafeAppendBatch resolves pointers in order, reusing file lookups for
// contiguous same-file runs to reduce scan-path overhead.
func (s *Set) ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error) {
	if len(ptrs) == 0 {
		return dst[:0], nil
	}
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	var (
		fileID    uint32
		f         *File
		verifyCRC = !s.disableReadChecksum
	)
	for i := 0; i < len(ptrs); {
		ptr := ptrs[i]
		if i == 0 || ptr.FileID != fileID {
			next, ok := s.Files[ptr.FileID]
			if !ok {
				return nil, &fileNotFoundError{id: ptr.FileID, inSnapshot: true}
			}
			fileID = ptr.FileID
			f = next
		}
		runEnd := groupedRecordBatchRun(ptrs, i)
		if runEnd-i > 1 {
			ok, err := f.readUnsafeAppendGroupedRecordBatch(ptrs[i:runEnd], verifyCRC, dst[i:runEnd])
			if err != nil {
				return nil, err
			}
			if ok {
				i = runEnd
				continue
			}
		}
		var err error
		dst[i], err = f.ReadUnsafeAppend(ptr, verifyCRC, dst[i][:0])
		if err != nil {
			return nil, err
		}
		i++
	}
	return dst, nil
}

type Manager struct {
	dir           string
	extraScanDirs []string

	mu    sync.RWMutex
	files map[uint32]*File
	// currentWritableByLane tracks segments that may still grow and therefore are
	// allowed to remap aggressively for zero-copy unsafe views. Most lanes have a
	// single current writer keyed by lane id; lanes listed in
	// multiCurrentWritableLanes are keyed by file id so independent physical
	// writers that share an encoded lane can remain current simultaneously.
	currentWritableByLane     map[uint32]uint32
	multiCurrentWritableLanes map[uint32]bool

	refreshScans atomic.Uint64

	disableReadChecksum         bool
	dictLookup                  DictLookup
	templateLookup              TemplateLookup
	templateDecodeOpts          templ.DecodeOptions
	templateDefCache            *templateDefCache
	groupedFrameCacheEntries    int
	groupedFrameCacheMaxRaw     int
	groupedFrameCacheMaxBytes   int64
	groupedFrameCacheBudget     *groupedFrameCacheBudget
	currentWritableMmap         atomic.Bool
	currentWritableReadBarrier  atomic.Value
	stableResourcePins          *rootpublication.IdentityPinRegistry
	recoveredStableDeleteDirs   map[string]bool
	stableDeleteRecoveryEnabled bool
	deferredDeletionSync        func(dir string, resource durabilitycut.Resource) error
}

func NewManager(dir string) (*Manager, error) {
	return NewManagerWithStableResourcePinRegistry(dir, nil)
}

// NewManagerWithStableResourcePinRegistry installs the DB-scoped physical
// identity gate before the initial directory scan, so every tracked handle is
// observed from the moment it becomes deletable.
func NewManagerWithStableResourcePinRegistry(dir string, registry *rootpublication.IdentityPinRegistry) (*Manager, error) {
	return newManagerWithStableResourcePinRegistry(dir, registry, true, true)
}

// NewManagerForBoundedRecoveryWithStableResourcePinRegistry installs the
// identity gate and reconciles stable-delete intents without enumerating value
// log segments. Recovery callers must RegisterSegment every exact path named by
// their bounded durable inventory before taking a CurrentSetNoRefresh snapshot.
func NewManagerForBoundedRecoveryWithStableResourcePinRegistry(dir string, registry *rootpublication.IdentityPinRegistry) (*Manager, error) {
	return newManagerWithStableResourcePinRegistry(dir, registry, true, false)
}

// NewReadOnlyManagerWithStableResourcePinRegistry opens segment handles
// without reconciling interrupted stable deletions. Pending quarantine intents
// fail with ErrStableDeleteRecoveryRequired and are left untouched.
func NewReadOnlyManagerWithStableResourcePinRegistry(dir string, registry *rootpublication.IdentityPinRegistry) (*Manager, error) {
	return newManagerWithStableResourcePinRegistry(dir, registry, false, true)
}

// NewReadOnlyManagerForBoundedRecoveryWithStableResourcePinRegistry is the
// read-only counterpart of NewManagerForBoundedRecoveryWithStableResourcePinRegistry.
// It rejects pending stable-delete intents but does not enumerate segments.
func NewReadOnlyManagerForBoundedRecoveryWithStableResourcePinRegistry(dir string, registry *rootpublication.IdentityPinRegistry) (*Manager, error) {
	return newManagerWithStableResourcePinRegistry(dir, registry, false, false)
}

func newManagerWithStableResourcePinRegistry(dir string, registry *rootpublication.IdentityPinRegistry, recoverStableDeletes, refresh bool) (*Manager, error) {
	m := &Manager{
		dir:                         dir,
		files:                       make(map[uint32]*File),
		currentWritableByLane:       make(map[uint32]uint32),
		groupedFrameCacheEntries:    defaultGroupedFrameCacheEntries,
		groupedFrameCacheMaxRaw:     defaultGroupedFrameCacheMaxRawBytes,
		groupedFrameCacheMaxBytes:   defaultGroupedFrameCacheMaxBytes,
		stableResourcePins:          registry,
		recoveredStableDeleteDirs:   make(map[string]bool),
		stableDeleteRecoveryEnabled: recoverStableDeletes,
	}
	m.groupedFrameCacheBudget = newGroupedFrameCacheBudget(defaultGroupedFrameCacheMaxBytes)
	var err error
	if refresh {
		err = m.Refresh()
	} else {
		err = m.prepareScanDir(dir)
	}
	if err != nil {
		return nil, errors.Join(err, m.Close())
	}
	return m, nil
}

func (m *Manager) SetDisableReadChecksum(disable bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disableReadChecksum = disable
}

// SetDeferredDeletionSync installs the durability boundary used when a zombie
// segment can only be unlinked after its final snapshot or identity pin is
// released. Immediate maintenance removals remain owned by their caller so a
// caller can batch one directory sync across multiple unlinks.
func (m *Manager) SetDeferredDeletionSync(sync func(dir string, resource durabilitycut.Resource) error) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.deferredDeletionSync = sync
	m.mu.Unlock()
}

func (m *Manager) syncDeferredDeletion(path string) error {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	sync := m.deferredDeletionSync
	m.mu.RUnlock()
	if sync == nil {
		return nil
	}
	return sync(filepath.Dir(path), segmentNamespaceResource(path))
}

// SetCurrentWritableMmapEnabled enables persistent read-only mmap views for
// current writable value-log segments owned by this manager.
func (m *Manager) SetCurrentWritableMmapEnabled(enabled bool) {
	if m == nil {
		return
	}
	m.currentWritableMmap.Store(enabled)
}

// SetMultiCurrentWritableLane allows multiple physical files with the same
// encoded lane id to remain current-writable at the same time. This is used for
// TreeDB leaf-log append lanes, where the reserved lane id identifies the leaf
// log class while file ids still identify independent writer streams.
func (m *Manager) SetMultiCurrentWritableLane(lane uint32, enabled bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !enabled {
		if m.multiCurrentWritableLanes != nil {
			delete(m.multiCurrentWritableLanes, lane)
		}
		return
	}
	if m.multiCurrentWritableLanes == nil {
		m.multiCurrentWritableLanes = make(map[uint32]bool)
	}
	m.multiCurrentWritableLanes[lane] = true
}

// SetCurrentWritableReadBarrier installs an optional callback that is invoked
// before reading a segment still marked currentWritable. Cached mode uses this
// to flush the owning writer so backend-internal tree reads do not observe EOF
// from still-buffered grouped records.
func (m *Manager) SetCurrentWritableReadBarrier(fn func(fileID uint32) error) {
	if fn == nil {
		m.SetCurrentWritableReadBarrierWithSize(nil)
		return
	}
	m.SetCurrentWritableReadBarrierWithSize(func(fileID uint32) (int64, error) {
		return -1, fn(fileID)
	})
}

// SetCurrentWritableReadBarrierWithSize is like SetCurrentWritableReadBarrier,
// but lets the callback return the current on-disk size after its flush barrier.
// A positive size is used as a file-size hint for current-writable mmap reads.
func (m *Manager) SetCurrentWritableReadBarrierWithSize(fn func(fileID uint32) (int64, error)) {
	if m == nil {
		return
	}
	if fn == nil {
		var cleared func(fileID uint32) (int64, error)
		m.currentWritableReadBarrier.Store(cleared)
		return
	}
	m.currentWritableReadBarrier.Store(fn)
}

func (m *Manager) currentWritableBarrier() func(uint32) (int64, error) {
	if m == nil {
		return nil
	}
	if v := m.currentWritableReadBarrier.Load(); v != nil {
		if fn, ok := v.(func(uint32) (int64, error)); ok {
			return fn
		}
	}
	return nil
}

func (f *File) EnsureCurrentWritableReadableFor(ptr page.ValuePtr) error {
	return f.ensureCurrentWritableReadableFor(ptr)
}

func (f *File) ensureCurrentWritableReadable() error {
	return f.ensureCurrentWritableReadableFor(page.ValuePtr{})
}

func (f *File) ensureCurrentWritableReadableFor(ptr page.ValuePtr) error {
	if f == nil || !f.currentWritable.Load() || f.manager == nil {
		return nil
	}
	if currentWritableRecordKnownReadable(f, ptr) {
		return nil
	}
	if barrier := f.manager.currentWritableBarrier(); barrier != nil {
		size, err := barrier(f.ID)
		if err != nil {
			return err
		}
		if size > 0 {
			f.noteVerifiedFileSize(size)
		}
	}
	return nil
}

func currentWritableRecordKnownReadable(f *File, ptr page.ValuePtr) bool {
	// ValuePtr offsets point just after the record CRC prefix. Offsets before
	// that prefix cannot be converted back to a complete record range.
	if f == nil || ptr.Offset < valueLogRecordCRCPrefixBytes {
		return false
	}
	recordLen := page.ValuePtrRecordLength(ptr)
	if recordLen == 0 {
		return false
	}
	end := ptr.Offset + uint64(recordLen)
	// verifiedFileSize is int64-backed, so only use the fast path when the
	// computed exclusive end is representable without overflow or sign loss.
	if end < ptr.Offset || end > uint64(math.MaxInt64) {
		return false
	}
	return f.verifiedFileSize.Load() >= int64(end)
}

func (m *Manager) ReadChecksumEnabled() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return !m.disableReadChecksum
}

func (m *Manager) SetDictLookup(lookup DictLookup) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dictLookup = lookup
	for _, f := range m.files {
		f.dictLookup = lookup
	}
}

func (m *Manager) SetTemplateLookup(lookup TemplateLookup, opts templ.DecodeOptions) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.templateLookup = lookup
	m.templateDecodeOpts = opts
	m.templateDefCache = newTemplateDefCache(opts.DefCacheSize)
	for _, f := range m.files {
		f.templateLookup = lookup
		f.templateDecodeOpts = opts
		f.templateDefCache = m.templateDefCache
	}
}

func (m *Manager) SetGroupedFrameCacheEntries(entries int) {
	if entries < 0 {
		entries = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groupedFrameCacheEntries = entries
	for _, f := range m.files {
		f.setGroupedFrameCacheEntries(entries)
	}
}

func (m *Manager) SetGroupedFrameCacheMaxRawBytes(maxRaw int) {
	if maxRaw < 0 {
		maxRaw = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groupedFrameCacheMaxRaw = maxRaw
	for _, f := range m.files {
		f.setGroupedFrameCacheMaxRawBytes(maxRaw)
	}
}

// SetGroupedFrameCacheMaxBytes configures the manager-wide decoded grouped-frame
// byte budget. A value <= 0 disables the byte budget while preserving entry and
// per-frame raw-size bounds.
func (m *Manager) SetGroupedFrameCacheMaxBytes(maxBytes int64) {
	if m == nil {
		return
	}
	if maxBytes < 0 {
		maxBytes = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groupedFrameCacheMaxBytes = maxBytes
	if m.groupedFrameCacheBudget == nil {
		m.groupedFrameCacheBudget = newGroupedFrameCacheBudget(maxBytes)
	} else {
		m.groupedFrameCacheBudget.setLimit(maxBytes)
	}
	for _, f := range m.files {
		f.setGroupedFrameCacheMaxBytes(maxBytes)
	}
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var err error
	for _, f := range m.files {
		if e := m.unobserveStableFileLocked(f); e != nil {
			err = errors.Join(err, e)
		}
		if e := f.Close(); e != nil {
			err = errors.Join(err, e)
		}
	}
	m.files = nil
	return err
}

func (m *Manager) SegmentPath(id uint32) string {
	if m == nil {
		return segmentPath("", id)
	}
	m.mu.RLock()
	if f := m.files[id]; f != nil && strings.TrimSpace(f.Path) != "" {
		path := f.Path
		m.mu.RUnlock()
		return path
	}
	rootDir := m.dir
	m.mu.RUnlock()
	return segmentPath(rootDir, id)
}

// SegmentPath returns the canonical on-disk path for an encoded value-log file
// ID, such as an ID produced by EncodeFileID.
func SegmentPath(dir string, id uint32) string {
	return segmentPath(dir, id)
}

func segmentPath(dir string, id uint32) string {
	seg := page.ValueLogSegmentID(id)
	lane, seq := DecodeSegmentID(seg)
	name := fmt.Sprintf("value-l%d-%06d.log", lane, seq)
	if dir == "" {
		return name
	}
	return filepath.Join(dir, name)
}

// HasSegment reports whether id is already registered and not marked zombie.
func (m *Manager) HasSegment(id uint32) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	f := m.files[id]
	m.mu.RUnlock()
	return f != nil && !f.IsZombie.Load()
}

// RefreshScanCount returns the number of directory scans performed by Refresh().
// This is used in tests and profiling to guard against accidental rescan loops.
func (m *Manager) RefreshScanCount() uint64 {
	if m == nil {
		return 0
	}
	return m.refreshScans.Load()
}

// Refresh scans the directory and registers any new segments.
func (m *Manager) Refresh() error {
	m.refreshScans.Add(1)
	dirs := []string{m.dir}
	m.mu.RLock()
	if len(m.extraScanDirs) > 0 {
		dirs = append(dirs, m.extraScanDirs...)
	}
	m.mu.RUnlock()
	for _, dir := range dirs {
		if m.stableDeleteRecoveryEnabled {
			if err := m.recoverStableDeleteDirOnce(dir); err != nil {
				return err
			}
		} else if err := requireNoStableDeleteQuarantines(dir); err != nil {
			return err
		}
		segments, err := currentScanSegmentPaths()(dir)
		if err != nil {
			return err
		}
		m.mu.Lock()
		for _, seg := range segments {
			if err := m.registerSegmentLocked(seg.path, seg.id); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					// Online rewrite/GC can delete a segment after listSegments() sees it
					// but before we open it. Treat that as a benign refresh race and
					// keep scanning the remaining segments.
					continue
				}
				m.mu.Unlock()
				return err
			}
		}
		m.mu.Unlock()
	}
	return nil
}

func (m *Manager) recoverStableDeleteDirOnce(dir string) error {
	if m == nil {
		return nil
	}
	dir = filepath.Clean(dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recoveredStableDeleteDirs == nil {
		m.recoveredStableDeleteDirs = make(map[string]bool)
	}
	if m.recoveredStableDeleteDirs[dir] {
		return nil
	}
	if err := recoverStableDeleteQuarantines(dir); err != nil {
		return err
	}
	m.recoveredStableDeleteDirs[dir] = true
	return nil
}

func (m *Manager) AddScanDir(dir string) error {
	return m.addScanDir(dir, true)
}

// AddScanDirForBoundedRecovery adds a permitted resource namespace and
// reconciles its stable-delete state without enumerating segment files.
func (m *Manager) AddScanDirForBoundedRecovery(dir string) error {
	return m.addScanDir(dir, false)
}

func (m *Manager) addScanDir(dir string, refresh bool) error {
	if m == nil || dir == "" || dir == m.dir {
		return nil
	}
	m.mu.Lock()
	for _, existing := range m.extraScanDirs {
		if existing == dir {
			m.mu.Unlock()
			return nil
		}
	}
	m.extraScanDirs = append(m.extraScanDirs, dir)
	m.mu.Unlock()
	if refresh {
		return m.Refresh()
	}
	return m.prepareScanDir(dir)
}

func (m *Manager) prepareScanDir(dir string) error {
	if m == nil {
		return nil
	}
	if m.stableDeleteRecoveryEnabled {
		return m.recoverStableDeleteDirOnce(dir)
	}
	return requireNoStableDeleteQuarantines(dir)
}

// RegisterSegment registers a newly created segment without scanning the
// filesystem. Callers that create value-log segments in-process should use this
// on the hot path so CurrentSetNoRefresh remains sufficient for immediate
// publish/read-after-write visibility.
func (m *Manager) RegisterSegment(path string, id uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.registerSegmentLocked(path, id)
}

func (m *Manager) registerSegmentLocked(path string, id uint32) error {
	if m.files == nil {
		m.files = make(map[uint32]*File, 16)
	}
	if existing, ok := m.files[id]; ok {
		if filepath.Clean(existing.Path) != filepath.Clean(path) {
			return fmt.Errorf("%w: value-log file id %d already names %q, not %q", rootpublication.ErrResourceConflict, id, existing.Path, path)
		}
		openInfo, openErr := existing.File.Stat()
		pathInfo, pathErr := os.Stat(path)
		if openErr != nil || pathErr != nil {
			return errors.Join(openErr, pathErr)
		}
		if !os.SameFile(openInfo, pathInfo) {
			return fmt.Errorf("%w: value-log file id %d path was rebound", rootpublication.ErrResourceConflict, id)
		}
		return nil
	}
	f, err := currentOpenSegmentFile()(path, id, m.dictLookup, m.templateLookup, m.templateDecodeOpts, m.templateDefCache)
	if err != nil {
		return err
	}
	f.manager = m
	f.setGroupedFrameCacheBudget(m.groupedFrameCacheBudget)
	f.setGroupedFrameCacheMaxBytes(m.groupedFrameCacheMaxBytes)
	f.setGroupedFrameCacheEntries(m.groupedFrameCacheEntries)
	f.setGroupedFrameCacheMaxRawBytes(m.groupedFrameCacheMaxRaw)
	if err := m.observeStableFileLocked(f); err != nil {
		_ = f.Close()
		return err
	}
	m.files[id] = f
	return nil
}

// PromoteCurrentWritable marks fileID as current writable. By default this
// seals the previous current segment in the same encoded lane. Lanes enabled via
// SetMultiCurrentWritableLane instead use one current slot per file id unless a
// caller provides a previous file id with PromoteCurrentWritableReplacing.
func (m *Manager) PromoteCurrentWritable(fileID uint32) error {
	return m.PromoteCurrentWritableReplacing(fileID, 0)
}

// PromoteCurrentWritableReplacing marks fileID current-writable and, when
// previousFileID is non-zero, seals that writer's prior current segment even for
// lanes that allow multiple current writers.
func (m *Manager) PromoteCurrentWritableReplacing(fileID, previousFileID uint32) error {
	if m == nil {
		return nil
	}
	lane, _ := DecodeFileID(fileID)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.currentWritableByLane == nil {
		m.currentWritableByLane = make(map[uint32]uint32)
	}
	f, ok := m.files[fileID]
	if !ok {
		return &fileNotFoundError{id: fileID}
	}
	key := m.currentWritableKeyLocked(lane, fileID)
	if previousFileID != 0 && previousFileID != fileID {
		m.demoteCurrentWritableLocked(previousFileID, fileID)
	}
	if prevID, ok := m.currentWritableByLane[key]; ok && prevID != 0 && prevID != fileID {
		m.demoteCurrentWritableLocked(prevID, fileID)
	}
	f.currentWritable.Store(true)
	m.currentWritableByLane[key] = fileID
	return nil
}

// DemoteCurrentWritable seals fileID if it is currently tracked as writable.
func (m *Manager) DemoteCurrentWritable(fileID uint32) {
	if m == nil || fileID == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.demoteCurrentWritableLocked(fileID, 0)
}

func (m *Manager) demoteCurrentWritableLocked(fileID, nextCurrentID uint32) {
	if m == nil || fileID == 0 {
		return
	}
	for key, id := range m.currentWritableByLane {
		if id == fileID {
			delete(m.currentWritableByLane, key)
		}
	}
	prev := m.files[fileID]
	if prev == nil {
		return
	}
	prev.remapMu.Lock()
	prev.currentWritable.Store(false)
	if data, _ := prev.mmapData.Load().([]byte); len(data) > 0 {
		if nextCurrentID != 0 && m.allowDemotedCurrentMmapLocked(prev, nextCurrentID) {
			prev.sealedLazyMmapDenied.Store(false)
			prev.sealedLazyMmapDeniedCountCap.Store(0)
			prev.sealedLazyMmapDeniedBytesCap.Store(0)
		} else {
			prev.retirePersistentMmapToDeadLocked()
		}
	}
	prev.remapMu.Unlock()
}

func (m *Manager) currentWritableKeyLocked(lane, fileID uint32) uint32 {
	if m != nil && m.multiCurrentWritableLanes != nil && m.multiCurrentWritableLanes[lane] {
		return fileID
	}
	return lane
}

// CurrentWritableFileIDs returns the registered current writable value-log
// segment IDs. These files may still be held open or mapped and must not be
// physically removed by maintenance cleanup.
func (m *Manager) CurrentWritableFileIDs() []uint32 {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.currentWritableByLane) == 0 {
		return nil
	}
	ids := make([]uint32, 0, len(m.currentWritableByLane))
	for _, id := range m.currentWritableByLane {
		if id == 0 {
			continue
		}
		if f := m.files[id]; f != nil && !f.IsZombie.Load() && f.currentWritable.Load() {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// RewriteLaneHint returns a best-effort lane/start-seq hint for creating new
// rewrite segments without scanning the filesystem.
//
// It considers all currently tracked segments (including zombies) to avoid
// immediate lane/seq reuse while deletes are pending.
func (m *Manager) RewriteLaneHint() (lane uint32, startSeq uint32, ok bool) {
	if m == nil {
		return 0, 0, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	const lanes = 256
	var used [lanes]bool
	var maxSeq [lanes]uint32
	for id := range m.files {
		l, s := DecodeFileID(id)
		if l >= lanes {
			continue
		}
		used[l] = true
		if s > maxSeq[l] {
			maxSeq[l] = s
		}
	}
	// Lane 255 is the format-reserved outer-leaf namespace and must never be
	// offered for ordinary value-log rewrites, even when it is currently empty.
	for l := uint32(ReservedLeafLogLaneID - 1); l > 0; l-- {
		if !used[l] {
			return l, 0, true
		}
	}
	return 0, maxSeq[0], true
}

// CurrentSet returns a snapshot of the current value-log files.
func (m *Manager) CurrentSet() *Set {
	_ = m.Refresh()
	return m.CurrentSetNoRefresh()
}

// CurrentSetNoRefresh returns a snapshot of the currently registered value-log
// files without scanning the filesystem for newly created segments.
//
// Callers in hot paths should prefer this when they know no new value-log
// segments can be referenced by the state being published.
func (m *Manager) CurrentSetNoRefresh() *Set {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentSetLocked()
}

// currentSetLocked builds a ref-counted snapshot.
// m.mu must be held (read or write).
func (m *Manager) currentSetLocked() *Set {
	files := make(map[uint32]*File, len(m.files))
	for id, f := range m.files {
		if f.IsZombie.Load() {
			continue
		}
		files[id] = f
		f.RefCount.Add(1)
	}
	s := &Set{
		Files:               files,
		disableReadChecksum: m.disableReadChecksum,
	}
	s.RefCount.Store(1)
	return s
}

func (m *Manager) Read(ptr page.ValuePtr) ([]byte, error) {
	f, err := m.fileFor(ptr.FileID)
	if err != nil {
		return nil, err
	}
	return f.Read(ptr, !m.disableReadChecksum)
}

func (m *Manager) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	f, err := m.fileFor(ptr.FileID)
	if err != nil {
		return nil, err
	}
	return f.ReadUnsafe(ptr, !m.disableReadChecksum)
}

func (m *Manager) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	f, err := m.fileFor(ptr.FileID)
	if err != nil {
		return nil, false, err
	}
	return f.ReadUnsafeTo(ptr, !m.disableReadChecksum, dst)
}

func (m *Manager) ReadAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	f, err := m.fileFor(ptr.FileID)
	if err != nil {
		return nil, err
	}
	return f.ReadAppend(ptr, !m.disableReadChecksum, dst)
}

func (m *Manager) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	f, err := m.fileFor(ptr.FileID)
	if err != nil {
		return nil, err
	}
	return f.ReadUnsafeAppend(ptr, !m.disableReadChecksum, dst)
}

func (m *Manager) ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error) {
	if len(ptrs) == 0 {
		return dst[:0], nil
	}
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	var (
		fileID    uint32
		f         *File
		verifyCRC = !m.disableReadChecksum
	)
	for i := 0; i < len(ptrs); {
		ptr := ptrs[i]
		if i == 0 || ptr.FileID != fileID {
			next, err := m.fileFor(ptr.FileID)
			if err != nil {
				return nil, err
			}
			fileID = ptr.FileID
			f = next
		}
		runEnd := groupedRecordBatchRun(ptrs, i)
		if runEnd-i > 1 {
			ok, err := f.readUnsafeAppendGroupedRecordBatch(ptrs[i:runEnd], verifyCRC, dst[i:runEnd])
			if err != nil {
				return nil, err
			}
			if ok {
				i = runEnd
				continue
			}
		}
		var err error
		dst[i], err = f.ReadUnsafeAppend(ptr, verifyCRC, dst[i][:0])
		if err != nil {
			return nil, err
		}
		i++
	}
	return dst, nil
}

// ReadRIDUnverified reads only record id metadata through the manager's
// registered segment handle, avoiding per-lookup file opens on hot paths.
func (m *Manager) ReadRIDUnverified(ptr page.ValuePtr) (uint64, error) {
	f, err := m.fileFor(ptr.FileID)
	if err != nil {
		return 0, err
	}
	return f.ReadRIDUnverified(ptr)
}

func (m *Manager) fileFor(id uint32) (*File, error) {
	m.mu.RLock()
	f, ok := m.files[id]
	m.mu.RUnlock()
	if ok {
		return f, nil
	}
	if err := m.Refresh(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	f, ok = m.files[id]
	m.mu.RUnlock()
	if ok {
		return f, nil
	}
	return nil, &fileNotFoundError{id: id}
}

// Acquire increments the Set refcount (O(1)).
func (m *Manager) Acquire(set *Set) {
	if set != nil {
		set.RefCount.Add(1)
	}
}

// Release decrements the Set refcount and removes zombie files once unpinned.
func (m *Manager) Release(set *Set) error {
	if set == nil {
		return nil
	}
	if set.RefCount.Add(-1) > 0 {
		return nil
	}

	var err error
	for _, f := range set.Files {
		newRef := f.RefCount.Add(-1)
		if newRef == 0 && f.IsZombie.Load() {
			if e := m.deleteZombieFile(f); e != nil {
				err = errors.Join(err, e)
			}
		}
	}
	return err
}

func (m *Manager) retryZombieDelete(f *File) {
	if m == nil || f == nil {
		return
	}
	defer f.retryDeletePending.Store(false)

	backoff := 200 * time.Millisecond
	for {
		m.mu.RLock()
		cur, exists := m.files[f.ID]
		keepRetrying := exists && cur == f && f.RefCount.Load() == 0 && f.IsZombie.Load()
		m.mu.RUnlock()
		if !keepRetrying {
			return
		}
		lease, leaseErr := m.stableDeleteLease(f)
		if errors.Is(leaseErr, ErrFilePinned) {
			// The conflict can be either an identity pin or a different identity
			// holding the pathname lease. Backoff handles both without spinning on
			// an already-ready per-identity pin channel.
			time.Sleep(backoff)
			if backoff < 2*time.Second {
				backoff *= 2
			}
			continue
		}
		if leaseErr != nil {
			log.Printf("valuelog: retry zombie delete gate failed for %s: %v", f.Path, leaseErr)
			return
		}
		if lease != nil {
			if err := validateStableDeletePathIdentity(f.Path, f.stableIdentity); err != nil {
				abortStableDeleteLease(lease)
				log.Printf("valuelog: retry zombie delete path validation failed for %s: %v", f.Path, err)
				return
			}
		}

		deleted, removeErr := closeAndRemoveStableSegmentFileResult(f, lease != nil)
		if deleted {
			syncErr := m.syncDeferredDeletion(f.Path)
			commitStableDeleteLease(lease)
			m.mu.Lock()
			var unobserveErr error
			if cur, exists := m.files[f.ID]; exists && cur == f && f.RefCount.Load() == 0 && f.IsZombie.Load() {
				delete(m.files, f.ID)
				unobserveErr = m.unobserveStableFileLocked(f)
			}
			m.mu.Unlock()
			if finishErr := errors.Join(removeErr, syncErr, unobserveErr); finishErr != nil {
				log.Printf("valuelog: removed retired zombie %s with finalization error: %v", f.Path, finishErr)
			}
			return
		} else if !isWindowsSharingViolationError(removeErr) {
			abortStableDeleteLease(lease)
			log.Printf("valuelog: retry zombie delete failed for %s: %v", f.Path, removeErr)
			return
		}
		abortStableDeleteLease(lease)

		time.Sleep(backoff)
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

func (m *Manager) MarkZombie(id uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[id]
	if !ok {
		return &fileNotFoundError{id: id}
	}
	f.IsZombie.Store(true)
	return nil
}

// MarkZombieIfTracked marks id zombie when the manager still tracks it,
// including files that were already marked zombie by a previous cleanup pass.
func (m *Manager) MarkZombieIfTracked(id uint32) (tracked bool, newlyMarked bool, err error) {
	if m == nil {
		return false, false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[id]
	if !ok {
		return false, false, nil
	}
	wasZombie := f.IsZombie.Load()
	f.IsZombie.Store(true)
	return true, !wasZombie, nil
}

// EvictSegment closes and forgets a segment without deleting it from disk.
// This is useful when another component owns lifecycle/deletion.
func (m *Manager) EvictSegment(id uint32) error {
	m.mu.Lock()
	f, ok := m.files[id]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	if f.RefCount.Load() != 0 {
		m.mu.Unlock()
		return &filePinnedError{id: id, op: "evict"}
	}
	delete(m.files, id)
	unobserveErr := m.unobserveStableFileLocked(f)
	m.mu.Unlock()
	return errors.Join(unobserveErr, f.Close())
}

// RemapStats reports aggregate remap executions and tracked dead mappings.
func (m *Manager) RemapStats() (remaps uint64, deadMappings uint64) {
	if m == nil {
		return 0, 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, f := range m.files {
		if f == nil {
			continue
		}
		remaps += f.remapCount.Load()
		deadMappings += f.deadMappingsCount.Load()
	}
	return remaps, deadMappings
}

func valueLogFileSizeBestEffort(f *File) uint64 {
	if f == nil {
		return 0
	}
	if known := f.fileSize.Load(); known > 0 {
		return uint64(known)
	}
	if data, _ := f.mmapData.Load().([]byte); len(data) > 0 {
		return uint64(len(data))
	}
	if f.Path != "" {
		if info, err := os.Stat(f.Path); err == nil && info.Size() > 0 {
			return uint64(info.Size())
		}
	}
	return 0
}

func valueLogFileSizeNoStat(f *File) uint64 {
	if f == nil {
		return 0
	}
	if known := f.fileSize.Load(); known > 0 {
		return uint64(known)
	}
	if data, _ := f.mmapData.Load().([]byte); len(data) > 0 {
		return uint64(len(data))
	}
	return 0
}

// SizeBestEffort returns the current best-effort segment size in bytes.
// It prefers cached size/mmap metadata and only falls back to stat when needed.
func (f *File) SizeBestEffort() int64 {
	return int64(valueLogFileSizeBestEffort(f))
}

// ZombieStats reports tracked zombie segments and their approximate byte totals.
// A zombie remains on disk until all snapshots release it (RefCount reaches 0).
func (m *Manager) ZombieStats() (segments uint64, bytes uint64, pinnedSegments uint64, pinnedBytes uint64, unpinnedSegments uint64, unpinnedBytes uint64) {
	if m == nil {
		return 0, 0, 0, 0, 0, 0
	}
	m.mu.RLock()
	for _, f := range m.files {
		if f == nil || !f.IsZombie.Load() {
			continue
		}
		segments++
		// Keep ZombieStats lock-friendly: avoid filesystem stats while the
		// manager lock is held and rely on cached segment sizes only.
		size := valueLogFileSizeNoStat(f)
		bytes += size
		if f.RefCount.Load() > 0 {
			pinnedSegments++
			pinnedBytes += size
			continue
		}
		unpinnedSegments++
		unpinnedBytes += size
	}
	m.mu.RUnlock()
	return segments, bytes, pinnedSegments, pinnedBytes, unpinnedSegments, unpinnedBytes
}

// MmapResidencyStats reports aggregate mmap residency split by segment type:
// current writable segments, sealed segments, and dead mappings/bytes.
func (m *Manager) MmapResidencyStats() (currentSegments uint64, currentBytes uint64, sealedSegments uint64, sealedBytes uint64, deadMappings uint64, deadBytes uint64) {
	if m == nil {
		return 0, 0, 0, 0, 0, 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, f := range m.files {
		if f == nil {
			continue
		}
		data, _ := f.mmapData.Load().([]byte)
		if len(data) > 0 {
			if f.currentWritable.Load() {
				currentSegments++
				currentBytes += uint64(len(data))
			} else {
				sealedSegments++
				sealedBytes += uint64(len(data))
			}
		}
		deadMappings += f.deadMappingsCount.Load()
		deadBytes += f.deadMappedBytes.Load()
	}
	return currentSegments, currentBytes, sealedSegments, sealedBytes, deadMappings, deadBytes
}

// SealedMapDeniedStats reports the total sealed-lazy-map deny count.
func (m *Manager) SealedMapDeniedStats() uint64 {
	byCount, byBytes := m.SealedMapDeniedByReasonStats()
	return byCount + byBytes
}

// SealedMapDeniedCount is an alias for SealedMapDeniedStats.
func (m *Manager) SealedMapDeniedCount() uint64 {
	return m.SealedMapDeniedStats()
}

// SealedMapDeniedByReasonStats reports sealed lazy-map denials split by budget:
// count-cap denials and byte-cap denials.
func (m *Manager) SealedMapDeniedByReasonStats() (countCap uint64, bytesCap uint64) {
	if m == nil {
		return 0, 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, f := range m.files {
		if f == nil {
			continue
		}
		countCap += f.sealedMapDeniedByCount.Load()
		bytesCap += f.sealedMapDeniedByBytes.Load()
	}
	return countCap, bytesCap
}

func (m *Manager) allowSealedLazyMmapLocked(target *File, targetSize int64) (bool, sealedLazyMmapDenyReason) {
	if m == nil || target == nil {
		return false, sealedLazyMmapDenyCountCap
	}
	targetIsLeaf := isLeafLogFileID(target.ID)
	maxSegments := MaxMappedSealedSegments
	maxBytes := MaxMappedSealedBytes
	if targetIsLeaf {
		maxSegments = MaxMappedLeafSealedSegments
		maxBytes = MaxMappedLeafSealedBytes
	}
	if maxSegments <= 0 {
		return false, sealedLazyMmapDenyCountCap
	}
	if target.currentWritable.Load() {
		return true, sealedLazyMmapDenyNone
	}
	targetData, _ := target.mmapData.Load().([]byte)
	targetMapped := len(targetData) > 0
	mappedSealed := 0
	var mappedSealedBytes uint64
	for _, f := range m.files {
		if f == nil || f.currentWritable.Load() {
			continue
		}
		if isLeafLogFileID(f.ID) != targetIsLeaf {
			continue
		}
		data, _ := f.mmapData.Load().([]byte)
		if len(data) > 0 {
			mappedSealed++
			mappedSealedBytes += uint64(len(data))
			if !targetMapped && mappedSealed >= maxSegments {
				return false, sealedLazyMmapDenyCountCap
			}
		}
	}
	if maxBytes > 0 {
		targetBytes := uint64(targetSize)
		if targetBytes == 0 {
			if known := target.fileSize.Load(); known > 0 {
				targetBytes = uint64(known)
			} else if info, err := target.File.Stat(); err == nil {
				if sz := info.Size(); sz > 0 {
					targetBytes = uint64(sz)
					target.noteVerifiedFileSize(sz)
				}
			}
		}
		if targetBytes > 0 {
			limit := uint64(maxBytes)
			if targetMapped {
				currentBytes := uint64(len(targetData))
				if targetBytes > currentBytes {
					projected := targetBytes
					if mappedSealedBytes > currentBytes {
						projected += mappedSealedBytes - currentBytes
					}
					if projected > limit {
						return false, sealedLazyMmapDenyBytesCap
					}
				}
			} else if mappedSealedBytes+targetBytes > limit {
				return false, sealedLazyMmapDenyBytesCap
			}
		}
	}
	return true, sealedLazyMmapDenyNone
}

func (m *Manager) allowDemotedCurrentMmapLocked(target *File, nextCurrentID uint32) bool {
	if m == nil || target == nil {
		return false
	}
	targetIsLeaf := isLeafLogFileID(target.ID)
	maxSegments := MaxMappedSealedSegments
	maxBytes := MaxMappedSealedBytes
	if targetIsLeaf {
		maxSegments = MaxMappedLeafSealedSegments
		maxBytes = MaxMappedLeafSealedBytes
	}
	if maxSegments <= 0 {
		return false
	}
	mappedSealed := 0
	var mappedSealedBytes uint64
	targetData, _ := target.mmapData.Load().([]byte)
	targetMapped := len(targetData) > 0
	for _, f := range m.files {
		if f == nil || f == target || f.ID == nextCurrentID || f.currentWritable.Load() {
			continue
		}
		if isLeafLogFileID(f.ID) != targetIsLeaf {
			continue
		}
		data, _ := f.mmapData.Load().([]byte)
		if len(data) == 0 {
			continue
		}
		mappedSealed++
		mappedSealedBytes += uint64(len(data))
	}
	projectedMappedSealed := mappedSealed
	if targetMapped {
		projectedMappedSealed++
	}
	if projectedMappedSealed > maxSegments {
		return false
	}
	if maxBytes > 0 {
		targetBytes := uint64(len(targetData))
		if known := target.fileSize.Load(); known > int64(targetBytes) {
			targetBytes = uint64(known)
		} else if target.File != nil {
			if info, err := target.File.Stat(); err == nil {
				if sz := info.Size(); sz > int64(targetBytes) {
					targetBytes = uint64(sz)
					target.noteVerifiedFileSize(sz)
				}
			}
		}
		limit := uint64(maxBytes)
		if targetMapped {
			if mappedSealedBytes+targetBytes > limit {
				return false
			}
		} else if mappedSealedBytes+targetBytes > limit {
			return false
		}
	}
	return true
}

func (m *Manager) MmapReadStats() (hits uint64, missesOutOfRange uint64, missesNoMapping uint64, missesDeadMappingCap uint64, fallbacksReadAt uint64) {
	if m == nil {
		return 0, 0, 0, 0, 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, f := range m.files {
		hits += f.mmapReadHits.Load()
		missesOutOfRange += f.mmapReadMissOutOfRange.Load()
		missesNoMapping += f.mmapReadMissNoMapping.Load()
		missesDeadMappingCap += f.mmapReadMissDeadMappingCap.Load()
		fallbacksReadAt += f.mmapReadFallbackReadAt.Load()
	}
	return hits, missesOutOfRange, missesNoMapping, missesDeadMappingCap, fallbacksReadAt
}

func (m *Manager) TemplateDefCacheStats() (hits, misses uint64, entries, capacity int) {
	if m == nil {
		return 0, 0, 0, 0
	}
	m.mu.RLock()
	cache := m.templateDefCache
	m.mu.RUnlock()
	if cache == nil {
		return 0, 0, 0, 0
	}
	return cache.Stats()
}

func (m *Manager) GroupedFrameCacheStats() (hits, misses uint64, entries, capacity int) {
	stats := m.GroupedFrameCacheDetailedStats()
	return stats.Hits, stats.Misses, stats.Entries, stats.Capacity
}

func (m *Manager) GroupedFrameCacheDetailedStats() GroupedFrameCacheStats {
	if m == nil {
		return GroupedFrameCacheStats{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var stats GroupedFrameCacheStats
	for _, f := range m.files {
		stats.add(f.groupedFrameCacheDetailedStats())
	}
	if m.groupedFrameCacheBudget != nil {
		stats.RetainedBytes = m.groupedFrameCacheBudget.retainedBytes()
		stats.BudgetBytes = m.groupedFrameCacheBudget.budgetBytes()
	}
	return stats
}

func (m *Manager) DecodeScratchStats() DecodeScratchStats {
	stats := DecodeScratchStatsSnapshot()
	if m == nil {
		return stats
	}
	m.mu.RLock()
	files := make([]*File, 0, len(m.files))
	for _, f := range m.files {
		files = append(files, f)
	}
	m.mu.RUnlock()
	for _, f := range files {
		f.addDecodeScratchStats(&stats)
	}
	return stats
}

func (m *Manager) RemoveSegment(id uint32) error {
	return m.RemoveSegmentExpectedIdentity(id, rootpublication.StableIdentity{})
}

// RemoveSegmentExpectedIdentity removes id only when the manager-observed
// physical identity matches expected. A zero expected identity preserves the
// ordinary RemoveSegment behavior.
func (m *Manager) RemoveSegmentExpectedIdentity(id uint32, expected rootpublication.StableIdentity) error {
	m.mu.Lock()
	f, ok := m.files[id]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	if f.RefCount.Load() != 0 {
		m.mu.Unlock()
		return &filePinnedError{id: id, op: "remove"}
	}
	if expected != (rootpublication.StableIdentity{}) {
		actual := f.stableIdentity
		if actual == (rootpublication.StableIdentity{}) {
			var err error
			actual, err = rootpublication.StableIdentityFromFile(f.File)
			if err != nil {
				m.mu.Unlock()
				return err
			}
		}
		if !rootpublication.SamePhysicalIdentity(expected, actual) {
			m.mu.Unlock()
			return fmt.Errorf("%w: value-log file id %d identity changed", rootpublication.ErrResourceConflict, id)
		}
	}
	lease, err := m.stableDeleteLease(f)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if lease != nil {
		if err := validateStableDeletePath(f); err != nil {
			abortStableDeleteLease(lease)
			m.mu.Unlock()
			return err
		}
	}
	delete(m.files, id)
	unobserveErr := m.unobserveStableFileLocked(f)
	m.mu.Unlock()

	deleted, removeErr := closeAndRemoveStableSegmentFileResult(f, lease != nil)
	finishStableDeleteLease(lease, deleted)
	return errors.Join(unobserveErr, removeErr)
}

// RemoveSegmentIfUnpinned removes a tracked segment only when no live snapshot
// pins it. It returns false with no error when the segment is absent or still
// pinned.
func (m *Manager) RemoveSegmentIfUnpinned(id uint32) (bool, error) {
	if m == nil {
		return false, nil
	}
	m.mu.Lock()
	f, ok := m.files[id]
	if !ok {
		m.mu.Unlock()
		return false, nil
	}
	if f.RefCount.Load() != 0 {
		m.mu.Unlock()
		return false, nil
	}
	lease, err := m.stableDeleteLease(f)
	if errors.Is(err, ErrFilePinned) {
		m.mu.Unlock()
		if f.IsZombie.Load() && f.retryDeletePending.CompareAndSwap(false, true) {
			go m.retryZombieDelete(f)
		}
		return false, nil
	}
	if err != nil {
		m.mu.Unlock()
		return false, err
	}
	if lease != nil {
		if err := validateStableDeletePath(f); err != nil {
			abortStableDeleteLease(lease)
			m.mu.Unlock()
			return false, err
		}
	}
	delete(m.files, id)
	unobserveErr := m.unobserveStableFileLocked(f)
	m.mu.Unlock()

	deleted, removeErr := closeAndRemoveStableSegmentFileResult(f, lease != nil)
	finishStableDeleteLease(lease, deleted)
	return true, errors.Join(unobserveErr, removeErr)
}

// RemoveSegmentForce removes a segment without refcount checks.
// Intended for recovery cleanup before any snapshots are live.
func (m *Manager) RemoveSegmentForce(id uint32) error {
	m.mu.Lock()
	f, ok := m.files[id]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	lease, err := m.stableDeleteLease(f)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if lease != nil {
		if err := validateStableDeletePath(f); err != nil {
			abortStableDeleteLease(lease)
			m.mu.Unlock()
			return err
		}
	}
	delete(m.files, id)
	unobserveErr := m.unobserveStableFileLocked(f)
	m.mu.Unlock()

	deleted, removeErr := closeAndRemoveStableSegmentFileResult(f, lease != nil)
	finishStableDeleteLease(lease, deleted)
	return errors.Join(unobserveErr, removeErr)
}

var removeSegmentPath = os.Remove

func segmentNamespaceResource(path string) durabilitycut.Resource {
	if filepath.Base(filepath.Dir(filepath.Clean(path))) == "leaf_vlog" {
		return durabilitycut.ResourceOuterLeaf
	}
	return durabilitycut.ResourceValueLog
}

func removeSegmentFileOnce(path string) (bool, error) {
	err := removeSegmentPath(path)
	if err == nil {
		return true, durabilitycut.EmitNamespace(durabilitycut.NamespaceUnlink, segmentNamespaceResource(path), filepath.Dir(path), path, "")
	}
	if os.IsNotExist(err) {
		return true, nil
	}
	return false, err
}

func removeSegmentFileWithRetry(path string) (bool, error) {
	const attempts = 40
	backoff := 25 * time.Millisecond
	var lastErr error
	for i := 0; i < attempts; i++ {
		removed, err := removeSegmentFileOnce(path)
		if removed {
			return true, err
		}
		if err == nil {
			return false, nil
		}
		lastErr = err
		// On non-Windows, or on the final retry attempt, stop retrying.
		if runtime.GOOS != "windows" || i >= attempts-1 {
			break
		}
		if !isWindowsSharingViolationError(err) {
			break
		}
		time.Sleep(backoff)
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
	return false, lastErr
}

func removeSegmentFileOnceStable(path string, identity rootpublication.StableIdentity, stable bool) (bool, error) {
	if stable {
		return removeStableSegmentFileOnce(path, identity)
	}
	return removeSegmentFileOnce(path)
}

func removeSegmentFileWithRetryStable(path string, identity rootpublication.StableIdentity) (bool, error) {
	const attempts = 40
	backoff := 25 * time.Millisecond
	var lastErr error
	for i := 0; i < attempts; i++ {
		removed, err := removeSegmentFileOnceStable(path, identity, true)
		if removed {
			return true, err
		}
		if err == nil {
			return false, nil
		}
		lastErr = err
		if runtime.GOOS != "windows" || i >= attempts-1 || !isWindowsSharingViolationError(err) {
			break
		}
		time.Sleep(backoff)
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
	return false, lastErr
}

func isWindowsSharingViolationError(err error) bool {
	if runtime.GOOS != "windows" || err == nil {
		return false
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && pathErr.Err != nil {
		err = pathErr.Err
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "sharing violation") || strings.Contains(msg, "used by another process")
}

type segmentInfo struct {
	id   uint32
	path string
}

func listSegments(dir string) ([]segmentInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	segments := make([]segmentInfo, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		const prefix = "value-"
		const suffix = ".log"
		if len(name) <= len(prefix)+len(suffix) || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}
		var (
			id uint32
			ok bool
		)
		core := name[len(prefix) : len(name)-len(suffix)]
		if len(core) == 0 {
			continue
		}
		if core[0] == 'l' {
			laneSeq := core[1:]
			dash := strings.IndexByte(laneSeq, '-')
			if dash <= 0 || dash >= len(laneSeq)-1 {
				continue
			}
			lane, err := strconv.ParseUint(laneSeq[:dash], 10, 32)
			if err != nil {
				continue
			}
			seq, err := strconv.ParseUint(laneSeq[dash+1:], 10, 32)
			if err != nil {
				continue
			}
			id, err = EncodeFileID(uint32(lane), uint32(seq))
			ok = err == nil
		} else {
			seq, parseErr := strconv.ParseUint(core, 10, 32)
			if parseErr != nil {
				continue
			}
			if seq > maxSegmentSeq {
				// Legacy segments use raw seq; skip ones that would collide with lane-encoded IDs.
				continue
			}
			id = page.ValueLogFileID(uint32(seq))
			ok = true
		}
		if !ok {
			continue
		}

		segments = append(segments, segmentInfo{
			id:   id,
			path: filepath.Join(dir, name),
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].id < segments[j].id
	})
	return segments, nil
}
