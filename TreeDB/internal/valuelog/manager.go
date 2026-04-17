package valuelog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

const (
	// Keep grouped-frame cache modest by default so restore workloads with many
	// open segments do not retain large decoded-raw payloads per file.
	defaultGroupedFrameCacheEntries     = 2
	defaultGroupedFrameCacheMaxRawBytes = 1 << 20
)

type groupedFrameCacheEntry struct {
	start     int64
	verifyCRC bool
	k         int
	offsets   [MaxFrameK + 1]uint32
	raw       []byte
	rawPooled bool
	used      uint64
}

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
	currentWritable    atomic.Bool
	dictLookup         DictLookup
	templateLookup     TemplateLookup
	templateDecodeOpts templ.DecodeOptions
	templateDefCache   *templateDefCache

	cacheMu    sync.Mutex
	cacheStart atomic.Int64
	cacheK     int
	cacheFlags byte
	cacheLen   int
	cacheOffs  [MaxFrameK + 1]uint32
	cacheRaw   []byte
	// decodeScratch retains one reusable decode buffer for compressed grouped
	// reads, reducing sync.Pool churn across adjacent frame misses.
	decodeScratch []byte
	// cacheRawPooled tracks whether cacheRaw currently owns a pooled decode
	// scratch buffer that must be returned on eviction.
	cacheRawPooled bool

	groupedFrameCacheEntries int
	groupedFrameCacheMaxRaw  int
	groupedFrameCacheClock   uint64
	groupedFrameCache        []groupedFrameCacheEntry
	groupedFrameCacheHits    uint64
	groupedFrameCacheMisses  uint64

	closed atomic.Bool

	// mmapData holds the current read-only mapping. Readers load it without locks.
	mmapData atomic.Value // stores []byte (may be nil slice)
	fileSize atomic.Int64 // last known on-disk size; 0 means unknown

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
}

func openFile(path string, id uint32, dictLookup DictLookup, templateLookup TemplateLookup, templateOpts templ.DecodeOptions, templateCache *templateDefCache) (*File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	vf := &File{
		ID:                       id,
		Path:                     path,
		File:                     f,
		dictLookup:               dictLookup,
		templateLookup:           templateLookup,
		templateDecodeOpts:       templateOpts,
		templateDefCache:         templateCache,
		groupedFrameCacheEntries: defaultGroupedFrameCacheEntries,
		groupedFrameCacheMaxRaw:  defaultGroupedFrameCacheMaxRawBytes,
	}
	vf.mmapData.Store([]byte(nil))
	if info, err := f.Stat(); err == nil {
		if sz := info.Size(); sz > 0 {
			vf.fileSize.Store(sz)
		}
	}
	return vf, nil
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
		f.stashDecodeScratchLocked(f.cacheRaw)
	}
	f.cacheRaw = raw
	f.cacheRawPooled = pooled
}

func (f *File) stashDecodeScratchLocked(buf []byte) {
	if cap(buf) == 0 {
		return
	}
	if cap(buf) > maxDecodeScratchKeep {
		putDecodeScratch(buf)
		return
	}
	buf = buf[:0]
	if cap(f.decodeScratch) == 0 {
		f.decodeScratch = buf
		return
	}
	if cap(buf) > cap(f.decodeScratch) {
		old := f.decodeScratch
		f.decodeScratch = buf
		putDecodeScratch(old)
		return
	}
	putDecodeScratch(buf)
}

func (f *File) takeDecodeScratch(minCap int) []byte {
	if minCap <= 0 {
		return nil
	}
	f.cacheMu.Lock()
	scratch := f.decodeScratch
	f.decodeScratch = nil
	f.cacheMu.Unlock()
	if cap(scratch) >= minCap {
		return scratch[:0]
	}
	if scratch != nil {
		putDecodeScratch(scratch)
	}
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
	f.cacheMu.Lock()
	if cap(f.decodeScratch) == 0 {
		f.decodeScratch = buf
		f.cacheMu.Unlock()
		return
	}
	if cap(buf) > cap(f.decodeScratch) {
		old := f.decodeScratch
		f.decodeScratch = buf
		f.cacheMu.Unlock()
		putDecodeScratch(old)
		return
	}
	f.cacheMu.Unlock()
	putDecodeScratch(buf)
}

func (f *File) clearGroupedFrameCacheLocked() {
	for i := range f.groupedFrameCache {
		if f.groupedFrameCache[i].rawPooled {
			f.stashDecodeScratchLocked(f.groupedFrameCache[i].raw)
		}
		f.groupedFrameCache[i].raw = nil
		f.groupedFrameCache[i].k = 0
		f.groupedFrameCache[i].rawPooled = false
	}
	f.groupedFrameCache = nil
	f.groupedFrameCacheClock = 0
}

func (f *File) setGroupedFrameCacheEntries(entries int) {
	if entries < 0 {
		entries = 0
	}
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	if f.groupedFrameCacheEntries == entries {
		return
	}
	f.clearGroupedFrameCacheLocked()
	f.groupedFrameCacheEntries = entries
	f.groupedFrameCacheHits = 0
	f.groupedFrameCacheMisses = 0
}

func (f *File) groupedFrameCacheReadTo(start int64, verifyCRC bool, subIndex int, dst []byte) (out []byte, usedDst bool, err error, hit bool) {
	f.cacheMu.Lock()
	if f.groupedFrameCacheEntries <= 0 {
		f.cacheMu.Unlock()
		return nil, false, nil, false
	}
	if len(f.groupedFrameCache) == 0 {
		f.groupedFrameCacheMisses++
		f.cacheMu.Unlock()
		return nil, false, nil, false
	}
	for i := range f.groupedFrameCache {
		e := &f.groupedFrameCache[i]
		if e.k <= 0 || e.start != start || e.verifyCRC != verifyCRC || subIndex < 0 || subIndex >= e.k {
			continue
		}
		valStart := e.offsets[subIndex]
		valEnd := e.offsets[subIndex+1]
		rawLen := e.offsets[e.k]
		if valEnd < valStart || valEnd > rawLen || uint32(len(e.raw)) != rawLen {
			continue
		}
		f.groupedFrameCacheClock++
		e.used = f.groupedFrameCacheClock
		f.groupedFrameCacheHits++
		val := e.raw[valStart:valEnd]
		if f.templateLookup != nil && templ.IsEncodedPayload(val) {
			// Copy payload while holding cacheMu so callers do not race with cache
			// entry eviction/reuse after unlock.
			encoded := append([]byte(nil), val...)
			f.cacheMu.Unlock()
			decoded, decErr := templ.DecodePayloadAppend(nil, encoded, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
			}, f.templateDecodeOpts)
			if decErr != nil {
				return nil, false, decErr, true
			}
			return decoded, false, nil, true
		}
		if dst != nil && cap(dst) >= len(val) {
			out := dst[:len(val)]
			copy(out, val)
			f.cacheMu.Unlock()
			return out, true, nil, true
		}
		out := make([]byte, len(val))
		copy(out, val)
		f.cacheMu.Unlock()
		return out, false, nil, true
	}
	f.groupedFrameCacheMisses++
	f.cacheMu.Unlock()
	return nil, false, nil, false
}

func (f *File) groupedFrameCacheStore(start int64, verifyCRC bool, k int, offsets [MaxFrameK + 1]uint32, raw []byte, pooled bool) bool {
	if k <= 0 || len(raw) == 0 {
		return false
	}
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	if f.groupedFrameCacheEntries <= 0 {
		return false
	}
	if f.groupedFrameCacheMaxRaw > 0 && len(raw) > f.groupedFrameCacheMaxRaw {
		return false
	}

	f.groupedFrameCacheClock++
	used := f.groupedFrameCacheClock

	for i := range f.groupedFrameCache {
		e := &f.groupedFrameCache[i]
		if e.k > 0 && e.start == start && e.verifyCRC == verifyCRC {
			if e.rawPooled {
				f.stashDecodeScratchLocked(e.raw)
			}
			e.k = k
			e.offsets = offsets
			e.raw = raw
			e.rawPooled = pooled
			e.used = used
			return true
		}
	}

	idx := -1
	if len(f.groupedFrameCache) < f.groupedFrameCacheEntries {
		f.groupedFrameCache = append(f.groupedFrameCache, groupedFrameCacheEntry{})
		idx = len(f.groupedFrameCache) - 1
	} else {
		oldest := f.groupedFrameCache[0].used
		idx = 0
		for i := 1; i < len(f.groupedFrameCache); i++ {
			if f.groupedFrameCache[i].used < oldest {
				oldest = f.groupedFrameCache[i].used
				idx = i
			}
		}
	}

	if idx >= 0 && idx < len(f.groupedFrameCache) && f.groupedFrameCache[idx].rawPooled {
		f.stashDecodeScratchLocked(f.groupedFrameCache[idx].raw)
	}
	f.groupedFrameCache[idx] = groupedFrameCacheEntry{
		start:     start,
		verifyCRC: verifyCRC,
		k:         k,
		offsets:   offsets,
		raw:       raw,
		rawPooled: pooled,
		used:      used,
	}
	return true
}

func (f *File) groupedFrameCacheStats() (hits, misses uint64, entries, capacity int) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	return f.groupedFrameCacheHits, f.groupedFrameCacheMisses, len(f.groupedFrameCache), f.groupedFrameCacheEntries
}

func (f *File) setGroupedFrameCacheMaxRawBytes(maxRaw int) {
	if maxRaw < 0 {
		maxRaw = 0
	}
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	if f.groupedFrameCacheMaxRaw == maxRaw {
		return
	}
	f.clearGroupedFrameCacheLocked()
	f.groupedFrameCacheMaxRaw = maxRaw
	f.groupedFrameCacheHits = 0
	f.groupedFrameCacheMisses = 0
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
	scratch = f.decodeScratch
	f.decodeScratch = nil
	f.clearGroupedFrameCacheLocked()
	f.cacheStart.Store(0)
	f.cacheMu.Unlock()
	if scratch != nil {
		putDecodeScratch(scratch)
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
	if err := f.ensureCurrentWritableReadable(); err != nil {
		return nil, err
	}
	if val, err, ok := f.readViaMmap(ptr, verifyCRC); ok {
		f.mmapReadHits.Add(1)
		return val, err
	}
	data, _ := f.mmapData.Load().([]byte)
	if data != nil && deadMappingsCapExhausted(f.deadMappingsCount.Load(), len(data)) {
		f.mmapReadMissDeadMappingCap.Add(1)
	}
	if !verifyCRC {
		if val, _, err, ok := f.readGroupedCompressedFromFileTo(ptr, nil); ok {
			f.mmapReadFallbackReadAt.Add(1)
			return val, err
		}
	}
	f.mmapReadFallbackReadAt.Add(1)
	return ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
}

func (f *File) ReadUnsafe(ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("valuelog: nil file")
	}
	if err := f.ensureCurrentWritableReadable(); err != nil {
		return nil, err
	}
	if val, err, ok := f.readViaMmapView(ptr, verifyCRC); ok {
		f.mmapReadHits.Add(1)
		return val, err
	}
	if !f.usesPersistentMmap() {
		if f.tryEnableSealedLazyMmap() {
			if val, err, ok := f.readViaMmapView(ptr, verifyCRC); ok {
				f.mmapReadHits.Add(1)
				return val, err
			}
		}
		f.mmapReadFallbackReadAt.Add(1)
		return ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
	}
	// Avoid per-read Stat/lock churn once we have exhausted the dead-mapping
	// budget: remapToFileSize won't be able to grow the mapping safely again.
	data, _ := f.mmapData.Load().([]byte)
	if !deadMappingsCapExhausted(f.deadMappingsCount.Load(), len(data)) {
		f.remapToFileSize()
		if val, err, ok := f.readViaMmapView(ptr, verifyCRC); ok {
			f.mmapReadHits.Add(1)
			return val, err
		}
	} else {
		f.mmapReadMissDeadMappingCap.Add(1)
	}
	f.mmapReadFallbackReadAt.Add(1)
	return ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
}

// ReadUnsafeTo is like ReadUnsafe, but it may return a slice backed by dst
// when dst has enough capacity for the decoded value. Callers must keep dst
// alive and avoid reusing it while they use the returned slice when usedDst is true.
func (f *File) ReadUnsafeTo(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, bool, error) {
	if f == nil || f.File == nil {
		return nil, false, errors.New("valuelog: nil file")
	}
	if err := f.ensureCurrentWritableReadable(); err != nil {
		return nil, false, err
	}
	if val, usedDst, err, ok := f.readViaMmapViewTo(ptr, verifyCRC, dst); ok {
		f.mmapReadHits.Add(1)
		return val, usedDst, err
	}
	if !f.usesPersistentMmap() {
		if f.tryEnableSealedLazyMmap() {
			if val, usedDst, err, ok := f.readViaMmapViewTo(ptr, verifyCRC, dst); ok {
				f.mmapReadHits.Add(1)
				return val, usedDst, err
			}
		}
		f.mmapReadFallbackReadAt.Add(1)
		if !verifyCRC {
			if val, usedDst, err, ok := f.readGroupedCompressedFromFileTo(ptr, dst); ok {
				return val, usedDst, err
			}
		}
		return ReadAtWithDictTo(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts, dst)
	}
	// Avoid per-read Stat/lock churn once we have exhausted the dead-mapping
	// budget: remapToFileSize won't be able to grow the mapping safely again.
	data, _ := f.mmapData.Load().([]byte)
	if !deadMappingsCapExhausted(f.deadMappingsCount.Load(), len(data)) {
		f.remapToFileSize()
		if val, usedDst, err, ok := f.readViaMmapViewTo(ptr, verifyCRC, dst); ok {
			f.mmapReadHits.Add(1)
			return val, usedDst, err
		}
	} else {
		f.mmapReadMissDeadMappingCap.Add(1)
	}
	f.mmapReadFallbackReadAt.Add(1)
	if !verifyCRC {
		if val, usedDst, err, ok := f.readGroupedCompressedFromFileTo(ptr, dst); ok {
			return val, usedDst, err
		}
	}
	return ReadAtWithDictTo(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts, dst)
}

// readGroupedCompressedFromFileTo handles grouped+compressed reads on the
// non-mmap fallback path while reusing File grouped-frame cache entries.
//
// ok=false means the caller should fall back to the generic ReadAtWithDictTo
// decoder path (for non-grouped / uncompressed / checksum-verified cases).
func (f *File) readGroupedCompressedFromFileTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error, bool) {
	if f == nil || f.File == nil {
		return nil, false, errors.New("valuelog: nil file"), true
	}
	if ptr.Offset < 4 || !page.ValuePtrIsGrouped(ptr) {
		return nil, false, nil, false
	}

	start := int64(ptr.Offset - 4)
	var header [HeaderSize]byte
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

	frameOff := start + HeaderSize
	var frameHeader [FrameHeaderSize]byte
	if _, err := f.File.ReadAt(frameHeader[:], frameOff); err != nil {
		return nil, false, err, true
	}
	if frameHeader[0] != FrameVersion {
		return nil, false, ErrCorrupt, true
	}
	k := int(frameHeader[2])
	if k <= 0 || k > MaxFrameK {
		return nil, false, ErrCorrupt, true
	}
	if frameHeader[1]&FrameFlagCompressed == 0 {
		return nil, false, nil, false
	}

	subIndex := int(page.ValuePtrSubIndex(ptr))
	if subIndex < 0 || subIndex >= k {
		return nil, false, ErrCorrupt, true
	}
	if out, usedDst, err, hit := f.groupedFrameCacheReadTo(start, false, subIndex, dst); hit {
		return out, usedDst, err, true
	}

	ridBytes := k * 8
	offsetBytes := (k + 1) * 4
	prefixLen := FrameHeaderSize + ridBytes + offsetBytes
	if int(valueLen) < prefixLen {
		return nil, false, ErrCorrupt, true
	}

	payload := make([]byte, int(valueLen))
	if _, err := f.File.ReadAt(payload, start+HeaderSize); err != nil {
		return nil, false, err, true
	}

	off := FrameHeaderSize + ridBytes
	var offsets [MaxFrameK + 1]uint32
	prev := uint32(0)
	for i := 0; i < k+1; i++ {
		cur := binary.LittleEndian.Uint32(payload[off : off+4])
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

	frame := FrameHeader{
		Version:  frameHeader[0],
		Flags:    frameHeader[1],
		K:        uint8(k),
		Reserved: frameHeader[3],
		DictID:   binary.LittleEndian.Uint64(frameHeader[4:12]),
	}

	raw := f.takeDecodeScratch(int(rawLen))
	pooledRaw := true
	raw, err := decodeFramePayloadTo(frame, payload[prefixLen:], f.dictLookup, rawLen, raw)
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
		cachedRaw := f.groupedFrameCacheStore(start, false, k, offsets, raw, pooledRaw)
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
		cachedRaw := f.groupedFrameCacheStore(start, false, k, offsets, raw, pooledRaw)
		if pooledRaw && !cachedRaw {
			f.releaseDecodeScratch(raw)
		}
		return out, true, nil, true
	}
	out := make([]byte, len(val))
	copy(out, val)
	cachedRaw := f.groupedFrameCacheStore(start, false, k, offsets, raw, pooledRaw)
	if pooledRaw && !cachedRaw {
		f.releaseDecodeScratch(raw)
	}
	return out, false, nil, true
}

func (f *File) ReadAppend(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("valuelog: nil file")
	}
	if err := f.ensureCurrentWritableReadable(); err != nil {
		return nil, err
	}
	if val, err, ok := f.readViaMmapAppend(ptr, verifyCRC, dst); ok {
		f.mmapReadHits.Add(1)
		return val, err
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
			f.cacheMu.Lock()
			hit := f.cacheStart.Load() == start && f.cacheK > 0 && f.cacheLen > 0 && subIndex < f.cacheK && f.cacheFlags&FrameFlagCompressed == 0
			if hit {
				prefixLen := f.cacheLen
				valStart := f.cacheOffs[subIndex]
				valEnd := f.cacheOffs[subIndex+1]
				rawLen := f.cacheOffs[f.cacheK]
				f.cacheMu.Unlock()

				if valEnd < valStart || valEnd > rawLen {
					return nil, ErrCorrupt
				}
				if prefixLen+int(rawLen) != int(valueLen) {
					return nil, ErrCorrupt
				}

				return f.appendPayloadFromFile(dst, frameOff+int64(prefixLen)+int64(valStart), int(valEnd-valStart))
			}
			f.cacheMu.Unlock()
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

func (f *File) appendDecodedRecordTo(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("valuelog: nil file")
	}
	oldLen := len(dst)
	var tail []byte
	if oldLen <= cap(dst) {
		tail = dst[oldLen:cap(dst)]
	}
	val, usedDst, err := ReadAtWithDictTo(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts, tail[:0])
	if err != nil {
		return nil, err
	}
	if usedDst {
		return dst[:oldLen+len(val)], nil
	}
	noteGrowReadAppendCompressedFallback(len(val))
	noteGrowReadAppendCompressedFallbackDst(dst, len(val))
	dst = grow(dst, len(val))
	copy(dst[oldLen:], val)
	return dst, nil
}

func (f *File) ReadUnsafeAppend(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error) {
	return f.ReadAppend(ptr, verifyCRC, dst)
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
		return dst, nil
	}
	encoded := append([]byte(nil), payload...)
	return appendDecodedTemplatePayload(dst[:oldLen], encoded, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
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
		fileID uint32
		f      *File
	)
	for i, ptr := range ptrs {
		if i == 0 || ptr.FileID != fileID {
			next, ok := s.Files[ptr.FileID]
			if !ok {
				return nil, &fileNotFoundError{id: ptr.FileID, inSnapshot: true}
			}
			fileID = ptr.FileID
			f = next
		}
		var err error
		dst[i], err = f.ReadUnsafeAppend(ptr, !s.disableReadChecksum, dst[i][:0])
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

type Manager struct {
	dir           string
	extraScanDirs []string

	mu    sync.RWMutex
	files map[uint32]*File
	// currentWritableByLane tracks the one segment per lane that may still grow
	// and therefore is allowed to remap aggressively for zero-copy unsafe views.
	currentWritableByLane map[uint32]uint32

	refreshScans atomic.Uint64

	disableReadChecksum        bool
	dictLookup                 DictLookup
	templateLookup             TemplateLookup
	templateDecodeOpts         templ.DecodeOptions
	templateDefCache           *templateDefCache
	groupedFrameCacheEntries   int
	groupedFrameCacheMaxRaw    int
	currentWritableReadBarrier atomic.Value
}

func NewManager(dir string) (*Manager, error) {
	m := &Manager{
		dir:                      dir,
		files:                    make(map[uint32]*File),
		currentWritableByLane:    make(map[uint32]uint32),
		groupedFrameCacheEntries: defaultGroupedFrameCacheEntries,
		groupedFrameCacheMaxRaw:  defaultGroupedFrameCacheMaxRawBytes,
	}
	if err := m.Refresh(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) SetDisableReadChecksum(disable bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disableReadChecksum = disable
}

// SetCurrentWritableReadBarrier installs an optional callback that is invoked
// before reading a segment still marked currentWritable. Cached mode uses this
// to flush the owning writer so backend-internal tree reads do not observe EOF
// from still-buffered grouped records.
func (m *Manager) SetCurrentWritableReadBarrier(fn func(fileID uint32) error) {
	if m == nil {
		return
	}
	if fn == nil {
		var cleared func(fileID uint32) error
		m.currentWritableReadBarrier.Store(cleared)
		return
	}
	m.currentWritableReadBarrier.Store(fn)
}

func (m *Manager) currentWritableBarrier() func(uint32) error {
	if m == nil {
		return nil
	}
	if v := m.currentWritableReadBarrier.Load(); v != nil {
		if fn, ok := v.(func(uint32) error); ok {
			return fn
		}
	}
	return nil
}

func (f *File) ensureCurrentWritableReadable() error {
	if f == nil || !f.currentWritable.Load() || f.manager == nil {
		return nil
	}
	if barrier := f.manager.currentWritableBarrier(); barrier != nil {
		return barrier(f.ID)
	}
	return nil
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

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var err error
	for _, f := range m.files {
		if e := f.Close(); e != nil {
			err = e
		}
	}
	m.files = nil
	return err
}

func (m *Manager) SegmentPath(id uint32) string {
	seg := page.ValueLogSegmentID(id)
	lane, seq := DecodeSegmentID(seg)
	name := fmt.Sprintf("value-l%d-%06d.log", lane, seq)
	if m == nil {
		return name
	}
	m.mu.RLock()
	if f := m.files[id]; f != nil && strings.TrimSpace(f.Path) != "" {
		path := f.Path
		m.mu.RUnlock()
		return path
	}
	rootDir := m.dir
	m.mu.RUnlock()
	return filepath.Join(rootDir, name)
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

func (m *Manager) AddScanDir(dir string) error {
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
	return m.Refresh()
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
	if _, ok := m.files[id]; ok {
		return nil
	}
	f, err := currentOpenSegmentFile()(path, id, m.dictLookup, m.templateLookup, m.templateDecodeOpts, m.templateDefCache)
	if err != nil {
		return err
	}
	f.manager = m
	f.setGroupedFrameCacheEntries(m.groupedFrameCacheEntries)
	f.setGroupedFrameCacheMaxRawBytes(m.groupedFrameCacheMaxRaw)
	m.files[id] = f
	return nil
}

// PromoteCurrentWritable marks fileID as the current writable segment for its
// lane and seals the previous current segment in that lane.
func (m *Manager) PromoteCurrentWritable(fileID uint32) error {
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
	if prevID, ok := m.currentWritableByLane[lane]; ok && prevID != 0 && prevID != fileID {
		if prev := m.files[prevID]; prev != nil {
			prev.currentWritable.Store(false)
		}
	}
	f.currentWritable.Store(true)
	m.currentWritableByLane[lane] = fileID
	return nil
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
	for l := uint32(lanes - 1); l > 0; l-- {
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
		fileID uint32
		f      *File
	)
	for i, ptr := range ptrs {
		if i == 0 || ptr.FileID != fileID {
			next, err := m.fileFor(ptr.FileID)
			if err != nil {
				return nil, err
			}
			fileID = ptr.FileID
			f = next
		}
		var err error
		dst[i], err = f.ReadUnsafeAppend(ptr, !m.disableReadChecksum, dst[i][:0])
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
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
			shouldRemove := false
			m.mu.Lock()
			if f.RefCount.Load() == 0 {
				if cur, exists := m.files[f.ID]; exists && cur == f {
					shouldRemove = true
				}
			}
			m.mu.Unlock()
			if shouldRemove {
				if e := f.Close(); e != nil {
					err = e
				}
				if e := removeSegmentFileWithRetry(f.Path); e != nil {
					if isWindowsSharingViolationError(e) {
						if f.retryDeletePending.CompareAndSwap(false, true) {
							go m.retryZombieDelete(f)
						}
						continue
					}
					err = e
					continue
				}
				m.mu.Lock()
				if cur, exists := m.files[f.ID]; exists && cur == f && f.RefCount.Load() == 0 && f.IsZombie.Load() {
					delete(m.files, f.ID)
				}
				m.mu.Unlock()
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

		if err := removeSegmentFileOnce(f.Path); err == nil {
			m.mu.Lock()
			if cur, exists := m.files[f.ID]; exists && cur == f && f.RefCount.Load() == 0 && f.IsZombie.Load() {
				delete(m.files, f.ID)
			}
			m.mu.Unlock()
			return
		} else if !isWindowsSharingViolationError(err) {
			log.Printf("valuelog: retry zombie delete failed for %s: %v", f.Path, err)
			return
		}

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
		return fmt.Errorf("cannot evict valuelog file %d: still pinned", id)
	}
	delete(m.files, id)
	m.mu.Unlock()
	return f.Close()
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
					target.fileSize.Store(sz)
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
	if m == nil {
		return 0, 0, 0, 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, f := range m.files {
		h, miss, e, capPerFile := f.groupedFrameCacheStats()
		hits += h
		misses += miss
		entries += e
		capacity += capPerFile
	}
	return hits, misses, entries, capacity
}

func (m *Manager) RemoveSegment(id uint32) error {
	m.mu.Lock()
	f, ok := m.files[id]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	if f.RefCount.Load() != 0 {
		m.mu.Unlock()
		return fmt.Errorf("cannot remove valuelog file %d: still pinned", id)
	}
	delete(m.files, id)
	m.mu.Unlock()

	_ = f.Close()
	return removeSegmentFileWithRetry(f.Path)
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
	delete(m.files, id)
	m.mu.Unlock()

	_ = f.Close()
	return removeSegmentFileWithRetry(f.Path)
}

var removeSegmentPath = os.Remove

func removeSegmentFileOnce(path string) error {
	err := removeSegmentPath(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	return err
}

func removeSegmentFileWithRetry(path string) error {
	const attempts = 40
	backoff := 25 * time.Millisecond
	var lastErr error
	for i := 0; i < attempts; i++ {
		err := removeSegmentFileOnce(path)
		if err == nil {
			return nil
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
	return lastErr
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
