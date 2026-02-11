package valuelog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

const defaultGroupedFrameCacheEntries = 4

type groupedFrameCacheEntry struct {
	start     int64
	verifyCRC bool
	k         int
	offsets   [MaxFrameK + 1]uint32
	raw       []byte
	used      uint64
}

// File represents a value-log segment on disk.
type File struct {
	ID                 uint32
	Path               string
	File               *os.File
	RefCount           atomic.Int64
	IsZombie           atomic.Bool
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
	// cacheRawPooled tracks whether cacheRaw currently owns a pooled decode
	// scratch buffer that must be returned on eviction.
	cacheRawPooled bool

	groupedFrameCacheEntries int
	groupedFrameCacheClock   uint64
	groupedFrameCache        []groupedFrameCacheEntry
	groupedFrameCacheHits    uint64
	groupedFrameCacheMisses  uint64

	closed atomic.Bool

	// mmapData holds the current read-only mapping. Readers load it without locks.
	mmapData atomic.Value // stores []byte (may be nil slice)

	remapMu        sync.Mutex
	remapRequested atomic.Bool

	deadMappings      [][]byte
	remapCount        atomic.Uint64
	deadMappingsCount atomic.Uint64
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
	}
	vf.mmapData.Store([]byte(nil))
	vf.maybeScheduleRemap()
	return vf, nil
}

func (f *File) setCacheRawLocked(raw []byte, pooled bool) {
	if f.cacheRawPooled && len(f.cacheRaw) > 0 {
		putDecodeScratch(f.cacheRaw)
	}
	f.cacheRaw = raw
	f.cacheRawPooled = pooled
}

func (f *File) clearGroupedFrameCacheLocked() {
	for i := range f.groupedFrameCache {
		f.groupedFrameCache[i].raw = nil
		f.groupedFrameCache[i].k = 0
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

func (f *File) groupedFrameCacheLookup(start int64, verifyCRC bool, subIndex int) (raw []byte, valStart, valEnd, rawLen uint32, ok bool) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	if f.groupedFrameCacheEntries <= 0 || len(f.groupedFrameCache) == 0 {
		return nil, 0, 0, 0, false
	}
	for i := range f.groupedFrameCache {
		e := &f.groupedFrameCache[i]
		if e.k <= 0 || e.start != start || e.verifyCRC != verifyCRC || subIndex < 0 || subIndex >= e.k {
			continue
		}
		valStart = e.offsets[subIndex]
		valEnd = e.offsets[subIndex+1]
		rawLen = e.offsets[e.k]
		if valEnd < valStart || valEnd > rawLen || uint32(len(e.raw)) != rawLen {
			continue
		}
		f.groupedFrameCacheClock++
		e.used = f.groupedFrameCacheClock
		f.groupedFrameCacheHits++
		return e.raw, valStart, valEnd, rawLen, true
	}
	f.groupedFrameCacheMisses++
	return nil, 0, 0, 0, false
}

func (f *File) groupedFrameCacheStore(start int64, verifyCRC bool, k int, offsets [MaxFrameK + 1]uint32, raw []byte) {
	if k <= 0 || len(raw) == 0 {
		return
	}
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	if f.groupedFrameCacheEntries <= 0 {
		return
	}

	f.groupedFrameCacheClock++
	used := f.groupedFrameCacheClock

	for i := range f.groupedFrameCache {
		e := &f.groupedFrameCache[i]
		if e.k > 0 && e.start == start && e.verifyCRC == verifyCRC {
			e.k = k
			e.offsets = offsets
			e.raw = raw
			e.used = used
			return
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

	f.groupedFrameCache[idx] = groupedFrameCacheEntry{
		start:     start,
		verifyCRC: verifyCRC,
		k:         k,
		offsets:   offsets,
		raw:       raw,
		used:      used,
	}
}

func (f *File) groupedFrameCacheStats() (hits, misses uint64, entries, capacity int) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	return f.groupedFrameCacheHits, f.groupedFrameCacheMisses, len(f.groupedFrameCache), f.groupedFrameCacheEntries
}

func (f *File) groupedFrameCacheStarts() []int64 {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	starts := make([]int64, 0, len(f.groupedFrameCache))
	for i := range f.groupedFrameCache {
		if f.groupedFrameCache[i].k > 0 {
			starts = append(starts, f.groupedFrameCache[i].start)
		}
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	return starts
}

func (f *File) Close() error {
	if f == nil || f.File == nil {
		return nil
	}
	f.closed.Store(true)
	f.cacheMu.Lock()
	f.cacheK = 0
	f.cacheFlags = 0
	f.cacheLen = 0
	f.setCacheRawLocked(nil, false)
	f.clearGroupedFrameCacheLocked()
	f.cacheStart.Store(0)
	f.cacheMu.Unlock()

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
	f.mmapData.Store([]byte(nil))
	f.remapMu.Unlock()

	return f.File.Close()
}

func (f *File) Read(ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("valuelog: nil file")
	}
	if val, err, ok := f.readViaMmap(ptr, verifyCRC); ok {
		return val, err
	}
	return ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
}

func (f *File) ReadUnsafe(ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("valuelog: nil file")
	}
	if val, err, ok := f.readViaMmapView(ptr, verifyCRC); ok {
		return val, err
	}
	f.remapToFileSize()
	if val, err, ok := f.readViaMmapView(ptr, verifyCRC); ok {
		return val, err
	}
	return ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
}

func (f *File) ReadAppend(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("valuelog: nil file")
	}
	if val, err, ok := f.readViaMmapAppend(ptr, verifyCRC, dst); ok {
		return val, err
	}
	// Fast path (bench/unsafe reads): grouped + uncompressed + no CRC.
	if !verifyCRC && page.ValuePtrIsGrouped(ptr) && ptr.Offset >= 4 {
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

				n := int(valEnd - valStart)
				oldLen := len(dst)
				dst = grow(dst, n)
				if _, err := f.File.ReadAt(dst[oldLen:], frameOff+int64(prefixLen)+int64(valStart)); err != nil {
					return nil, err
				}
				if f.templateLookup != nil && templ.IsEncodedPayload(dst[oldLen:]) {
					payload := dst[oldLen:]
					decodedStart := len(dst)
					dst, err := templ.DecodePayloadAppend(dst, payload, func(id uint64) (templ.TemplateDef, error) {
						return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
					}, f.templateDecodeOpts)
					if err != nil {
						return nil, err
					}
					decodedLen := len(dst) - decodedStart
					copy(dst[oldLen:], dst[decodedStart:])
					dst = dst[:oldLen+decodedLen]
				}
				return dst, nil
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
			// Fallback to the full decoder (will allocate).
			val, err := ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
			if err != nil {
				return nil, err
			}
			oldLen := len(dst)
			dst = grow(dst, len(val))
			copy(dst[oldLen:], val)
			return dst, nil
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

		n := int(valEnd - valStart)
		oldLen := len(dst)
		dst = grow(dst, n)
		if _, err := f.File.ReadAt(dst[oldLen:], frameOff+int64(prefixLen)+int64(valStart)); err != nil {
			return nil, err
		}
		if f.templateLookup != nil && templ.IsEncodedPayload(dst[oldLen:]) {
			payload := dst[oldLen:]
			decodedStart := len(dst)
			dst, err := templ.DecodePayloadAppend(dst, payload, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
			}, f.templateDecodeOpts)
			if err != nil {
				return nil, err
			}
			decodedLen := len(dst) - decodedStart
			copy(dst[oldLen:], dst[decodedStart:])
			dst = dst[:oldLen+decodedLen]
		}
		return dst, nil
	}

	// Slow path: use existing decoder and append.
	val, err := ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup, f.templateLookup, f.templateDefCache, f.templateDecodeOpts)
	if err != nil {
		return nil, err
	}
	oldLen := len(dst)
	dst = grow(dst, len(val))
	copy(dst[oldLen:], val)
	return dst, nil
}

func (f *File) ReadUnsafeAppend(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error) {
	return f.ReadAppend(ptr, verifyCRC, dst)
}

// Set is an immutable snapshot of value-log files for snapshot isolation.
type Set struct {
	Files               map[uint32]*File
	RefCount            atomic.Int64
	disableReadChecksum bool
}

func (s *Set) Read(ptr page.ValuePtr) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, fmt.Errorf("valuelog file %d not found in snapshot", ptr.FileID)
	}
	return f.Read(ptr, !s.disableReadChecksum)
}

func (s *Set) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, fmt.Errorf("valuelog file %d not found in snapshot", ptr.FileID)
	}
	return f.ReadUnsafe(ptr, !s.disableReadChecksum)
}

func (s *Set) ReadAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, fmt.Errorf("valuelog file %d not found in snapshot", ptr.FileID)
	}
	return f.ReadAppend(ptr, !s.disableReadChecksum, dst)
}

func (s *Set) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, fmt.Errorf("valuelog file %d not found in snapshot", ptr.FileID)
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
				return nil, fmt.Errorf("valuelog file %d not found in snapshot", ptr.FileID)
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
	dir string

	mu    sync.RWMutex
	files map[uint32]*File

	disableReadChecksum      bool
	dictLookup               DictLookup
	templateLookup           TemplateLookup
	templateDecodeOpts       templ.DecodeOptions
	templateDefCache         *templateDefCache
	groupedFrameCacheEntries int
}

func NewManager(dir string) (*Manager, error) {
	m := &Manager{
		dir:                      dir,
		files:                    make(map[uint32]*File),
		groupedFrameCacheEntries: defaultGroupedFrameCacheEntries,
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
	return filepath.Join(m.dir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
}

// Refresh scans the directory and registers any new segments.
func (m *Manager) Refresh() error {
	segments, err := listSegments(m.dir)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, seg := range segments {
		if _, ok := m.files[seg.id]; ok {
			continue
		}
		f, err := openFile(seg.path, seg.id, m.dictLookup, m.templateLookup, m.templateDecodeOpts, m.templateDefCache)
		if err != nil {
			return err
		}
		f.setGroupedFrameCacheEntries(m.groupedFrameCacheEntries)
		m.files[seg.id] = f
	}
	return nil
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
	return nil, fmt.Errorf("valuelog file %d not found", id)
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
			m.mu.Lock()
			if f.RefCount.Load() == 0 {
				if _, exists := m.files[f.ID]; exists {
					if e := f.Close(); e != nil {
						err = e
					}
					if e := os.Remove(f.Path); e != nil {
						err = e
					}
					delete(m.files, f.ID)
				}
			}
			m.mu.Unlock()
		}
	}
	return err
}

func (m *Manager) MarkZombie(id uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[id]
	if !ok {
		return fmt.Errorf("valuelog file %d not found", id)
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

func (m *Manager) RemapStats() (remaps uint64, deadMappings uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, f := range m.files {
		remaps += f.remapCount.Load()
		deadMappings += f.deadMappingsCount.Load()
	}
	return remaps, deadMappings
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
	return os.Remove(f.Path)
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
	return os.Remove(f.Path)
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

	var segments []segmentInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		const prefix = "value-"
		const suffix = ".log"
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}

		parseLaneSeq := func(rest string) (uint32, uint32, bool) {
			parts := strings.SplitN(rest, "-", 2)
			if len(parts) != 2 {
				return 0, 0, false
			}
			lane, err := strconv.ParseUint(parts[0], 10, 32)
			if err != nil {
				return 0, 0, false
			}
			seq, err := strconv.ParseUint(parts[1], 10, 32)
			if err != nil {
				return 0, 0, false
			}
			return uint32(lane), uint32(seq), true
		}

		var (
			id  uint32
			err error
		)
		core := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
		if strings.HasPrefix(core, "l") {
			lane, seq, ok := parseLaneSeq(strings.TrimPrefix(core, "l"))
			if !ok {
				continue
			}
			id, err = EncodeFileID(lane, seq)
			if err != nil {
				continue
			}
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
