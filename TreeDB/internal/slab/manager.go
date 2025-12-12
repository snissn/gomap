package slab

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"treedb/internal/page"
)

var (
	ErrActiveTailBeyondEOF = errors.New("slab: active tail beyond eof")
	ErrManagerClosed       = errors.New("slab: manager closed")
)

type SlabFile struct {
	FileID uint32
	Handle *os.File
	path   string

	RefCount atomic.Int64
	IsZombie atomic.Bool

	DeadBytes  atomic.Uint64
	TotalBytes atomic.Uint64
}

func (f *SlabFile) AddDeadBytes(n uint64) {
	f.DeadBytes.Add(n)
}

func (f *SlabFile) AddTotalBytes(n uint64) {
	f.TotalBytes.Add(n)
}

func (f *SlabFile) MarkDead(ptr page.ValuePtr) {
	f.AddDeadBytes(uint64(ptr.Length) + 4)
}

func (f *SlabFile) Stats() SlabStats {
	return SlabStats{
		DeadBytes:  f.DeadBytes.Load(),
		TotalBytes: f.TotalBytes.Load(),
	}
}

func (f *SlabFile) Pin() {
	f.RefCount.Add(1)
}

func (f *SlabFile) Unpin() error {
	if f.RefCount.Add(-1) == 0 && f.IsZombie.Load() {
		return f.delete()
	}
	return nil
}

func (f *SlabFile) MarkZombie() error {
	f.IsZombie.Store(true)
	if f.RefCount.Load() == 0 {
		return f.delete()
	}
	return nil
}

func (f *SlabFile) delete() error {
	if f.Handle != nil {
		_ = f.Handle.Close()
		f.Handle = nil
	}
	if f.path != "" {
		if err := os.Remove(f.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

type SlabSet struct {
	Files map[uint32]*SlabFile
}

func NewSlabSet(files map[uint32]*SlabFile) *SlabSet {
	cp := make(map[uint32]*SlabFile, len(files))
	for id, f := range files {
		cp[id] = f
	}
	return &SlabSet{Files: cp}
}

func (s *SlabSet) Get(id uint32) (*SlabFile, bool) {
	f, ok := s.Files[id]
	return f, ok
}

func (s *SlabSet) IDs() []uint32 {
	ids := make([]uint32, 0, len(s.Files))
	for id := range s.Files {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

type SlabManager struct {
	dir        string
	mu         sync.Mutex
	active     *SlabFile
	activeTail uint64
	set        *SlabSet
	closed     bool
}

const (
	// maxBatchBuffer caps in-memory buffering for batched slab appends.
	// This bounds memory use for very large batches while still amortizing syscalls.
	maxBatchBuffer = 4 << 20 // 4MB
)

var pwritevFunc = unix.Pwritev

var recordHeaderPool = sync.Pool{
	New: func() any { return new([recordHeaderLen]byte) },
}

func Load(dir string, activeID uint32, activeTail uint64) (*SlabManager, *SlabSet, error) {
	if dir == "" {
		return nil, nil, fmt.Errorf("slab: dir required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}

	files := make(map[uint32]*SlabFile)
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		id, ok := parseSlabFileName(ent.Name())
		if !ok {
			continue
		}
		path := filepath.Join(dir, ent.Name())
		if id > activeID {
			_ = os.Remove(path)
			continue
		}

		flags := os.O_RDONLY
		if id == activeID {
			flags = os.O_RDWR
		}
		fh, err := os.OpenFile(path, flags, 0o600)
		if err != nil {
			closeFiles(files)
			return nil, nil, err
		}
		sf := &SlabFile{FileID: id, Handle: fh, path: path}
		if fi, err := fh.Stat(); err == nil {
			sf.TotalBytes.Store(uint64(fi.Size()))
		}
		files[id] = sf
	}

	active, ok := files[activeID]
	if !ok {
		path := slabPath(dir, activeID)
		fh, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			closeFiles(files)
			return nil, nil, err
		}
		active = &SlabFile{FileID: activeID, Handle: fh, path: path}
		files[activeID] = active
	}

	fi, err := active.Handle.Stat()
	if err != nil {
		closeFiles(files)
		return nil, nil, err
	}
	if activeTail > uint64(fi.Size()) {
		closeFiles(files)
		return nil, nil, ErrActiveTailBeyondEOF
	}
	if err := active.Handle.Truncate(int64(activeTail)); err != nil {
		closeFiles(files)
		return nil, nil, err
	}
	if _, err := active.Handle.Seek(int64(activeTail), io.SeekStart); err != nil {
		closeFiles(files)
		return nil, nil, err
	}
	active.TotalBytes.Store(activeTail)

	set := NewSlabSet(files)
	mgr := &SlabManager{
		dir:        dir,
		active:     active,
		activeTail: activeTail,
		set:        set,
	}
	return mgr, set, nil
}

func closeFiles(files map[uint32]*SlabFile) {
	for _, f := range files {
		if f != nil && f.Handle != nil {
			_ = f.Handle.Close()
		}
	}
}

func (m *SlabManager) SlabSet() *SlabSet {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.set
}

func (m *SlabManager) ActiveID() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil {
		return 0
	}
	return m.active.FileID
}

func (m *SlabManager) ActiveTail() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeTail
}

func (m *SlabManager) AppendLarge(key, value []byte) (page.ValuePtr, error) {
	if key == nil {
		return page.ValuePtr{}, fmt.Errorf("slab: nil key")
	}
	if value == nil {
		return page.ValuePtr{}, fmt.Errorf("slab: nil value")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return page.ValuePtr{}, ErrManagerClosed
	}
	return m.appendLargeLocked(key, value)
}

func (m *SlabManager) appendLargeLocked(key, value []byte) (page.ValuePtr, error) {
	if m.active == nil || m.active.Handle == nil {
		return page.ValuePtr{}, fmt.Errorf("slab: no active slab")
	}

	hdr := recordHeaderPool.Get().(*[recordHeaderLen]byte)
	defer recordHeaderPool.Put(hdr)

	base := m.activeTail

	keyLen, valueLen, protectedLen, recordLen, err := recordLengths(key, value)
	if err != nil {
		return page.ValuePtr{}, err
	}
	sum := recordChecksum(keyLen, valueLen, key, value)
	encodeRecordHeader(hdr[:], sum, keyLen, valueLen)

	ptr := page.ValuePtr{
		Offset: base + recordCRCLen,
		Length: uint32(protectedLen),
		FileID: m.active.FileID,
	}

	off := int64(base)
	if pwritevFunc != nil {
		parts := [3][]byte{hdr[:], key, value}
		if err := pwritevAll(int(m.active.Handle.Fd()), parts[:], off); err != nil {
			if !errors.Is(err, unix.ENOSYS) {
				return page.ValuePtr{}, err
			}
		} else {
			m.activeTail += uint64(recordLen)
			m.active.AddTotalBytes(uint64(recordLen))
			if m.activeTail >= page.SlabRotateSize {
				if err := m.rotateLocked(); err != nil {
					return page.ValuePtr{}, err
				}
			}
			return ptr, nil
		}
	}

	// Fallback: use three WriteAt calls. This path is primarily for tests/ENOSYS.
	if err := writeAtAll(m.active.Handle, hdr[:], off); err != nil {
		return page.ValuePtr{}, err
	}
	off += recordHeaderLen
	if err := writeAtAll(m.active.Handle, key, off); err != nil {
		return page.ValuePtr{}, err
	}
	off += int64(len(key))
	if err := writeAtAll(m.active.Handle, value, off); err != nil {
		return page.ValuePtr{}, err
	}

	m.activeTail += uint64(recordLen)
	m.active.AddTotalBytes(uint64(recordLen))

	if m.activeTail >= page.SlabRotateSize {
		if err := m.rotateLocked(); err != nil {
			return page.ValuePtr{}, err
		}
	}

	return ptr, nil
}

// AppendLargeBatch appends multiple large records in order and returns their pointers.
// The implementation buffers records and flushes in bounded chunks to avoid OOM.
// Callers must already hold any higher-level writer lock; SlabManager serializes via m.mu.
func (m *SlabManager) AppendLargeBatch(keys, values [][]byte) ([]page.ValuePtr, error) {
	if len(keys) != len(values) {
		return nil, fmt.Errorf("slab: keys/values length mismatch")
	}
	if len(keys) == 0 {
		return nil, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrManagerClosed
	}

	ptrs := make([]page.ValuePtr, 0, len(keys))
	buf := make([]byte, 0, maxBatchBuffer)
	var chunkStart uint64

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		n, err := m.active.Handle.WriteAt(buf, int64(chunkStart))
		if err != nil {
			return err
		}
		if n != len(buf) {
			return io.ErrShortWrite
		}
		buf = buf[:0]
		return nil
	}

	for i := 0; i < len(keys); i++ {
		key, value := keys[i], values[i]
		if key == nil {
			return nil, fmt.Errorf("slab: nil key")
		}
		if value == nil {
			return nil, fmt.Errorf("slab: nil value")
		}

		base := m.activeTail
		keyLen, valueLen, protectedLen, recordLen, err := recordLengths(key, value)
		if err != nil {
			return nil, err
		}

		// Oversized records bypass buffering to preserve the memory cap.
		if recordLen > maxBatchBuffer {
			if err := flush(); err != nil {
				return nil, err
			}
			ptr, err := m.appendLargeLocked(key, value)
			if err != nil {
				return nil, err
			}
			ptrs = append(ptrs, ptr)
			continue
		}

		if len(buf) == 0 {
			chunkStart = base
		}
		// Flush current chunk if adding recordLen would exceed the cap.
		if len(buf) > 0 && len(buf)+recordLen > maxBatchBuffer {
			if err := flush(); err != nil {
				return nil, err
			}
			chunkStart = base
		}

		pos := len(buf)
		buf = buf[:pos+recordLen]
		dst := buf[pos : pos+recordLen]
		sum := recordChecksum(keyLen, valueLen, key, value)
		encodeRecordHeader(dst[0:recordHeaderLen], sum, keyLen, valueLen)
		copy(dst[recordHeaderLen:recordHeaderLen+len(key)], key)
		copy(dst[recordHeaderLen+len(key):], value)

		ptrs = append(ptrs, page.ValuePtr{
			Offset: base + recordCRCLen,
			Length: uint32(protectedLen),
			FileID: m.active.FileID,
		})
		m.activeTail += uint64(recordLen)
		m.active.AddTotalBytes(uint64(recordLen))

		// Rotate slabs at boundary; flush buffered bytes first so pointers remain valid.
		if m.activeTail >= page.SlabRotateSize {
			if err := flush(); err != nil {
				return nil, err
			}
			if err := m.rotateLocked(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return ptrs, nil
}

func pwritevAll(fd int, iovs [][]byte, off int64) error {
	for len(iovs) > 0 {
		n, err := pwritevFunc(fd, iovs, off)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		off += int64(n)
		for n > 0 && len(iovs) > 0 {
			if n >= len(iovs[0]) {
				n -= len(iovs[0])
				iovs = iovs[1:]
				continue
			}
			iovs[0] = iovs[0][n:]
			n = 0
		}
	}
	return nil
}

func writeAtAll(f *os.File, b []byte, off int64) error {
	for len(b) > 0 {
		n, err := f.WriteAt(b, off)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		off += int64(n)
		b = b[n:]
	}
	return nil
}

// ForceRotate seals the current active slab and creates a new active slab.
// This is used by compaction to obtain a fresh target slab.
func (m *SlabManager) ForceRotate() (*SlabFile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrManagerClosed
	}
	if err := m.rotateLocked(); err != nil {
		return nil, err
	}
	return m.active, nil
}

// RemoveFromSet removes a non-active slab from the active set and returns it.
// The caller should MarkZombie() the returned slab to enable deferred deletion.
func (m *SlabManager) RemoveFromSet(fileID uint32) (*SlabFile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrManagerClosed
	}
	if m.active != nil && m.active.FileID == fileID {
		return nil, fmt.Errorf("slab: cannot remove active slab %d", fileID)
	}
	f, ok := m.set.Files[fileID]
	if !ok {
		return nil, nil
	}
	newMap := make(map[uint32]*SlabFile, len(m.set.Files)-1)
	for id, sf := range m.set.Files {
		if id == fileID {
			continue
		}
		newMap[id] = sf
	}
	m.set = NewSlabSet(newMap)
	return f, nil
}

func (m *SlabManager) rotateLocked() error {
	old := m.active
	if old == nil {
		return nil
	}
	if err := old.Handle.Sync(); err != nil {
		return err
	}

	newID := old.FileID + 1
	path := slabPath(m.dir, newID)
	fh, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if err := fsyncDir(m.dir); err != nil {
		_ = fh.Close()
		return err
	}

	newFile := &SlabFile{FileID: newID, Handle: fh, path: path}
	newMap := make(map[uint32]*SlabFile, len(m.set.Files)+1)
	for id, f := range m.set.Files {
		newMap[id] = f
	}
	newMap[newID] = newFile
	m.set = NewSlabSet(newMap)
	m.active = newFile
	m.activeTail = 0
	return nil
}

func fsyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer df.Close()
	return df.Sync()
}

const (
	slabNamePrefix = "data-"
	slabNameSuffix = ".slab"
)

func slabPath(dir string, id uint32) string {
	return filepath.Join(dir, fmt.Sprintf("data-%04d.slab", id))
}

func parseSlabFileName(name string) (uint32, bool) {
	if !strings.HasPrefix(name, slabNamePrefix) || !strings.HasSuffix(name, slabNameSuffix) {
		return 0, false
	}
	numStr := strings.TrimSuffix(strings.TrimPrefix(name, slabNamePrefix), slabNameSuffix)
	if numStr == "" {
		return 0, false
	}
	id64, err := strconv.ParseUint(numStr, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(id64), true
}

func (m *SlabManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	var first error
	for _, f := range m.set.Files {
		if f != nil && f.Handle != nil {
			if err := f.Handle.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}
