package vlog

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/page"
)

// File represents a value-log segment on disk.
type File struct {
	ID       uint32
	Path     string
	File     *os.File
	RefCount atomic.Int64
	IsZombie atomic.Bool

	closed atomic.Bool

	// mmapData holds the current read-only mapping. Readers load it without locks.
	mmapData atomic.Value // stores []byte (may be nil slice)

	remapMu        sync.Mutex
	remapRequested atomic.Bool

	deadMappings      [][]byte
	remapCount        atomic.Uint64
	deadMappingsCount atomic.Uint64
}

func openFile(path string, id uint32) (*File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	vf := &File{ID: id, Path: path, File: f}
	vf.mmapData.Store([]byte(nil))
	return vf, nil
}

func (f *File) Close() error {
	if f == nil || f.File == nil {
		return nil
	}
	f.closed.Store(true)

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

// Set is an immutable snapshot of value-log files for snapshot isolation.
type Set struct {
	Files               map[uint32]*File
	RefCount            atomic.Int64
	disableReadChecksum bool
}

func (s *Set) Read(ptr page.ValuePtr) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, fmt.Errorf("vlog file %d not found in snapshot", ptr.FileID)
	}
	return f.Read(ptr, !s.disableReadChecksum)
}

func (s *Set) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, fmt.Errorf("vlog file %d not found in snapshot", ptr.FileID)
	}
	return f.ReadUnsafe(ptr, !s.disableReadChecksum)
}

type Manager struct {
	dir string

	mu    sync.RWMutex
	files map[uint32]*File

	disableReadChecksum bool
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
	return filepath.Join(m.dir, fmt.Sprintf("vlog-%06d.log", seg))
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
		f, err := openFile(seg.path, seg.id)
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
	return nil, fmt.Errorf("vlog file %d not found", id)
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
		return fmt.Errorf("vlog file %d not found", id)
	}
	f.IsZombie.Store(true)
	return nil
}

// RemapStats returns cumulative mmap remap counts across value-log files.
func (m *Manager) RemapStats() (remaps uint64, deadMappings uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, f := range m.files {
		remaps += f.remapCount.Load()
		deadMappings += f.deadMappingsCount.Load()
	}
	return remaps, deadMappings
}

func (m *Manager) SegmentSize(id uint32) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	f, ok := m.files[id]
	if !ok {
		return 0, fmt.Errorf("vlog file %d not found", id)
	}
	info, err := f.File.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
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
		return fmt.Errorf("cannot remove vlog file %d: still pinned", id)
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
		const prefix = "vlog-"
		const suffix = ".log"
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}
		core := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
		seq, err := strconv.ParseUint(core, 10, 32)
		if err != nil {
			continue
		}
		id := page.ValueLogFileID(uint32(seq))
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
