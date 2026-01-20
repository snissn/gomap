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

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/slab"
)

// File represents a value-log segment on disk.
type File struct {
	ID         uint32
	Path       string
	File       *os.File
	RefCount   atomic.Int64
	IsZombie   atomic.Bool
	dictLookup DictLookup

	cacheMu    sync.Mutex
	cacheStart atomic.Int64
	cacheK     int
	cacheFlags byte
	cacheLen   int
	cacheOffs  [MaxFrameK + 1]uint32

	closed atomic.Bool
}

func openFile(path string, id uint32, lookup DictLookup) (*File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &File{ID: id, Path: path, File: f, dictLookup: lookup}, nil
}

func (f *File) Close() error {
	if f == nil || f.File == nil {
		return nil
	}
	f.closed.Store(true)
	return f.File.Close()
}

func (f *File) Read(ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	return ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup)
}

func (f *File) ReadUnsafe(ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	return ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup)
}

func (f *File) ReadAppend(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("valuelog: nil file")
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
			val, err := ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup)
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
		if slab.MaxRecordSize > 0 && int64(rawLen) > slab.MaxRecordSize {
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
		return dst, nil
	}

	// Slow path: use existing decoder and append.
	val, err := ReadAtWithDict(f.File, ptr, verifyCRC, f.dictLookup)
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

type Manager struct {
	dir string

	mu    sync.RWMutex
	files map[uint32]*File

	disableReadChecksum bool
	dictLookup          DictLookup
}

func NewManager(dir string) (*Manager, error) {
	m := &Manager{
		dir:   dir,
		files: make(map[uint32]*File),
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
		f, err := openFile(seg.path, seg.id, m.dictLookup)
		if err != nil {
			return err
		}
		m.files[seg.id] = f
	}
	return nil
}

// CurrentSet returns a snapshot of the current value-log files.
func (m *Manager) CurrentSet() *Set {
	_ = m.Refresh()
	m.mu.RLock()
	defer m.mu.RUnlock()

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

// RemapStats returns zeros for compatibility with legacy stats.
func (m *Manager) RemapStats() (remaps uint64, deadMappings uint64) {
	return 0, 0
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
