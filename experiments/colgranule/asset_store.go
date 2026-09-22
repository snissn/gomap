package colgranule

import (
	"errors"
	"fmt"
	crc32 "github.com/snissn/go-crc32-asm"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type ColumnAssetKind string

const (
	ColumnAssetKindTCS1PartImage ColumnAssetKind = "tcs1_part_image"

	maxColumnAssetReadBytes int64 = 512 << 20
)

type ColumnAssetRef struct {
	Kind     ColumnAssetKind `json:"kind"`
	FileID   uint32          `json:"file_id"`
	Offset   int64           `json:"offset"`
	Length   int64           `json:"length"`
	Checksum uint32          `json:"checksum"`
}

type ColumnAssetStore interface {
	Put(kind ColumnAssetKind, payload []byte) (ColumnAssetRef, error)
	// Read may return store-owned bytes. Callers must treat the returned bytes as read-only.
	Read(ref ColumnAssetRef) ([]byte, error)
	// ReadTo appends or writes the asset bytes into dst[:0] and returns caller-owned bytes.
	// The returned slice may alias dst and remains valid until the caller mutates or reuses it.
	ReadTo(ref ColumnAssetRef, dst []byte) ([]byte, error)
}

type ColumnAssetRangeReader interface {
	ReadRange(ref ColumnAssetRef, offset int64, length int) ([]byte, error)
}

type columnAssetOwnedStore interface {
	PutOwned(kind ColumnAssetKind, payload []byte) (ColumnAssetRef, error)
}

type columnAssetVerifier interface {
	Verify(ref ColumnAssetRef) error
}

type columnAssetSyncer interface {
	Sync() error
}

type MemoryColumnAssetStore struct {
	mu      sync.Mutex
	nextOff int64
	assets  map[columnAssetKey]memoryColumnAsset
}

type columnAssetKey struct {
	fileID uint32
	offset int64
}

type memoryColumnAsset struct {
	kind     ColumnAssetKind
	length   int64
	payload  []byte
	checksum uint32
}

func NewMemoryColumnAssetStore() *MemoryColumnAssetStore {
	return &MemoryColumnAssetStore{assets: make(map[columnAssetKey]memoryColumnAsset)}
}

func (s *MemoryColumnAssetStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ref := range s.assets {
		delete(s.assets, ref)
	}
	s.nextOff = 0
}

func (s *MemoryColumnAssetStore) Put(kind ColumnAssetKind, payload []byte) (ColumnAssetRef, error) {
	if s == nil {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: nil memory asset store")
	}
	return s.put(kind, append([]byte(nil), payload...))
}

func (s *MemoryColumnAssetStore) PutOwned(kind ColumnAssetKind, payload []byte) (ColumnAssetRef, error) {
	if s == nil {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: nil memory asset store")
	}
	return s.put(kind, payload)
}

func (s *MemoryColumnAssetStore) put(kind ColumnAssetKind, payload []byte) (ColumnAssetRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref, err := newColumnAssetRef(kind, 1, s.nextOff, len(payload), payload)
	if err != nil {
		return ColumnAssetRef{}, err
	}
	s.nextOff += ref.Length
	s.assets[ref.key()] = memoryColumnAsset{
		kind:     ref.Kind,
		length:   ref.Length,
		payload:  payload,
		checksum: ref.Checksum,
	}
	return ref, nil
}

func (s *MemoryColumnAssetStore) Read(ref ColumnAssetRef) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("colgranule: nil memory asset store")
	}
	if err := validateColumnAssetRef(ref); err != nil {
		return nil, err
	}
	s.mu.Lock()
	asset, ok := s.assets[ref.key()]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("colgranule: missing asset ref file=%d offset=%d", ref.FileID, ref.Offset)
	}
	if asset.kind != ref.Kind {
		return nil, fmt.Errorf("colgranule: asset kind=%s want %s", asset.kind, ref.Kind)
	}
	if asset.length != ref.Length {
		return nil, fmt.Errorf("colgranule: asset length=%d want %d", asset.length, ref.Length)
	}
	if asset.checksum != ref.Checksum {
		return nil, fmt.Errorf("colgranule: asset ref checksum=%08x want %08x", asset.checksum, ref.Checksum)
	}
	return asset.payload, nil
}

func (s *MemoryColumnAssetStore) ReadTo(ref ColumnAssetRef, dst []byte) ([]byte, error) {
	payload, err := s.Read(ref)
	if err != nil {
		return nil, err
	}
	out := append(dst[:0], payload...)
	return out, nil
}

func (s *MemoryColumnAssetStore) ReadRange(ref ColumnAssetRef, offset int64, length int) ([]byte, error) {
	payload, err := s.Read(ref)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("colgranule: negative asset range offset %d", offset)
	}
	if length < 0 {
		return nil, fmt.Errorf("colgranule: negative asset range length %d", length)
	}
	if offset > int64(len(payload)) || int64(length) > int64(len(payload))-offset {
		return nil, fmt.Errorf("colgranule: asset range offset=%d length=%d outside payload bytes=%d", offset, length, len(payload))
	}
	start := int(offset)
	out := make([]byte, length)
	copy(out, payload[start:start+length])
	return out, nil
}

func (s *MemoryColumnAssetStore) Verify(ref ColumnAssetRef) error {
	if s == nil {
		return fmt.Errorf("colgranule: nil memory asset store")
	}
	if err := validateColumnAssetRef(ref); err != nil {
		return err
	}
	s.mu.Lock()
	asset, ok := s.assets[ref.key()]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("colgranule: missing asset ref file=%d offset=%d", ref.FileID, ref.Offset)
	}
	if asset.kind != ref.Kind {
		return fmt.Errorf("colgranule: asset kind=%s want %s", asset.kind, ref.Kind)
	}
	if asset.length != ref.Length {
		return fmt.Errorf("colgranule: asset length=%d want %d", asset.length, ref.Length)
	}
	if checksum := crc32.ChecksumIEEE(asset.payload); checksum != ref.Checksum {
		return fmt.Errorf("colgranule: asset ref checksum=%08x want %08x", checksum, ref.Checksum)
	}
	return nil
}

type SegmentColumnAssetStore struct {
	mu              sync.Mutex
	dir             string
	path            string
	file            *os.File
	fileID          uint32
	size            int64
	dirSyncRequired bool
	closing         bool
	activeFileIO    int
	fileIOCond      *sync.Cond
}

func OpenSegmentColumnAssetStore(dir string) (*SegmentColumnAssetStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("colgranule: empty segment asset dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "column-assets-000001.seg")
	file, err := openOrCreateColumnAssetSegment(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &SegmentColumnAssetStore{
		dir:    dir,
		path:   path,
		file:   file,
		fileID: 1,
		size:   info.Size(),
		// Reopen deliberately treats the directory entry as unsealed: the store
		// cannot prove a prior process synced it before publishing refs, so the
		// next publish Sync seals the file and its directory entry together.
		dirSyncRequired: true,
	}, nil
}

func openOrCreateColumnAssetSegment(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err == nil {
		return file, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (s *SegmentColumnAssetStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	s.closing = true
	cond := s.ensureFileIOCondLocked()
	for s.activeFileIO > 0 {
		cond.Wait()
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *SegmentColumnAssetStore) Sync() error {
	if s == nil {
		return fmt.Errorf("colgranule: nil segment asset store")
	}
	file, dir, dirSyncRequired, err := s.beginFileSync()
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		if endErr := s.endFileIO(); endErr != nil {
			return errors.Join(err, endErr)
		}
		return err
	}
	clearDirSync := false
	if dirSyncRequired {
		if err := syncColumnAssetDirectory(dir); err != nil {
			if endErr := s.endFileIO(); endErr != nil {
				return errors.Join(err, endErr)
			}
			return err
		}
		clearDirSync = true
	}
	return s.finishFileIO(clearDirSync)
}

func syncColumnAssetDirectory(dir string) error {
	if dir == "" {
		return fmt.Errorf("colgranule: empty segment asset dir")
	}
	if runtime.GOOS == "windows" {
		// Go does not expose portable directory fsync on Windows. File Sync is
		// still required; POSIX directory-entry durability is best-effort here.
		return nil
	}
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func (s *SegmentColumnAssetStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *SegmentColumnAssetStore) Put(kind ColumnAssetKind, payload []byte) (ColumnAssetRef, error) {
	if s == nil {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: nil segment asset store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.file == nil {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: closed segment asset store")
	}
	ref, err := newColumnAssetRef(kind, s.fileID, s.size, len(payload), payload)
	if err != nil {
		return ColumnAssetRef{}, err
	}
	n, err := s.file.WriteAt(payload, ref.Offset)
	if err != nil {
		return ColumnAssetRef{}, err
	}
	if n != len(payload) {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: short asset write bytes=%d want %d", n, len(payload))
	}
	s.size += ref.Length
	return ref, nil
}

func (s *SegmentColumnAssetStore) Read(ref ColumnAssetRef) ([]byte, error) {
	return s.ReadTo(ref, nil)
}

func (s *SegmentColumnAssetStore) ReadTo(ref ColumnAssetRef, dst []byte) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("colgranule: nil segment asset store")
	}
	if err := validateColumnAssetRef(ref); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.file == nil {
		return nil, fmt.Errorf("colgranule: closed segment asset store")
	}
	if ref.FileID != s.fileID {
		return nil, fmt.Errorf("colgranule: asset file id=%d want %d", ref.FileID, s.fileID)
	}
	if ref.Offset > s.size || ref.Length > s.size-ref.Offset {
		return nil, fmt.Errorf("colgranule: asset range offset=%d length=%d outside segment bytes=%d", ref.Offset, ref.Length, s.size)
	}
	if ref.Length > int64(math.MaxInt) {
		return nil, fmt.Errorf("colgranule: asset length=%d exceeds host int", ref.Length)
	}
	if ref.Length > maxColumnAssetReadBytes {
		return nil, fmt.Errorf("colgranule: asset length=%d exceeds max read bytes=%d", ref.Length, maxColumnAssetReadBytes)
	}
	length := int(ref.Length)
	if cap(dst) < length {
		dst = make([]byte, length)
	} else {
		dst = dst[:length]
	}
	reader := io.NewSectionReader(s.file, ref.Offset, ref.Length)
	if _, err := io.ReadFull(reader, dst); err != nil {
		return nil, err
	}
	if checksum := crc32.ChecksumIEEE(dst); checksum != ref.Checksum {
		return nil, fmt.Errorf("colgranule: asset ref checksum=%08x want %08x", checksum, ref.Checksum)
	}
	return dst, nil
}

func (s *SegmentColumnAssetStore) ReadRange(ref ColumnAssetRef, offset int64, length int) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("colgranule: nil segment asset store")
	}
	if err := validateColumnAssetRef(ref); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("colgranule: negative asset range offset %d", offset)
	}
	if length < 0 {
		return nil, fmt.Errorf("colgranule: negative asset range length %d", length)
	}
	if offset > ref.Length || int64(length) > ref.Length-offset {
		return nil, fmt.Errorf("colgranule: asset range offset=%d length=%d outside ref length=%d", offset, length, ref.Length)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.file == nil {
		return nil, fmt.Errorf("colgranule: closed segment asset store")
	}
	if ref.FileID != s.fileID {
		return nil, fmt.Errorf("colgranule: asset file id=%d want %d", ref.FileID, s.fileID)
	}
	absolute := ref.Offset + offset
	if absolute > s.size || int64(length) > s.size-absolute {
		return nil, fmt.Errorf("colgranule: asset range offset=%d length=%d outside segment bytes=%d", absolute, length, s.size)
	}
	out := make([]byte, length)
	reader := io.NewSectionReader(s.file, absolute, int64(length))
	if _, err := io.ReadFull(reader, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SegmentColumnAssetStore) Verify(ref ColumnAssetRef) (err error) {
	if s == nil {
		return fmt.Errorf("colgranule: nil segment asset store")
	}
	if err := validateColumnAssetRef(ref); err != nil {
		return err
	}
	file, fileID, size, err := s.beginFileIO()
	if err != nil {
		return err
	}
	defer func() {
		if endErr := s.endFileIO(); err == nil && endErr != nil {
			err = endErr
		}
	}()
	if ref.FileID != fileID {
		return fmt.Errorf("colgranule: asset file id=%d want %d", ref.FileID, fileID)
	}
	if ref.Offset > size || ref.Length > size-ref.Offset {
		return fmt.Errorf("colgranule: asset range offset=%d length=%d outside segment bytes=%d", ref.Offset, ref.Length, size)
	}
	h := crc32.NewIEEE()
	reader := io.NewSectionReader(file, ref.Offset, ref.Length)
	var buf [32 << 10]byte
	if _, err := io.CopyBuffer(h, reader, buf[:]); err != nil {
		return err
	}
	if checksum := h.Sum32(); checksum != ref.Checksum {
		return fmt.Errorf("colgranule: asset ref checksum=%08x want %08x", checksum, ref.Checksum)
	}
	return nil
}

func (s *SegmentColumnAssetStore) beginFileIO() (*os.File, uint32, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.file == nil {
		return nil, 0, 0, fmt.Errorf("colgranule: closed segment asset store")
	}
	s.activeFileIO++
	return s.file, s.fileID, s.size, nil
}

func (s *SegmentColumnAssetStore) beginFileSync() (*os.File, string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.file == nil {
		return nil, "", false, fmt.Errorf("colgranule: closed segment asset store")
	}
	// Snapshot the file after all prior Put calls have released the mutex, then
	// drop the mutex during fsync so reads are not blocked by slow storage.
	s.activeFileIO++
	return s.file, s.dir, s.dirSyncRequired, nil
}

func (s *SegmentColumnAssetStore) endFileIO() error {
	return s.finishFileIO(false)
}

func (s *SegmentColumnAssetStore) finishFileIO(clearDirSync bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeFileIO <= 0 {
		return fmt.Errorf("colgranule: segment asset store file IO ended without matching begin")
	}
	if clearDirSync {
		// On Windows this records the strongest platform-supported sync attempt;
		// reopen still conservatively requires another directory sync attempt.
		s.dirSyncRequired = false
	}
	s.activeFileIO--
	if s.activeFileIO == 0 && s.fileIOCond != nil {
		s.fileIOCond.Broadcast()
	}
	return nil
}

func (s *SegmentColumnAssetStore) ensureFileIOCondLocked() *sync.Cond {
	if s.fileIOCond == nil {
		s.fileIOCond = sync.NewCond(&s.mu)
	}
	return s.fileIOCond
}

func newColumnAssetRef(kind ColumnAssetKind, fileID uint32, offset int64, length int, payload []byte) (ColumnAssetRef, error) {
	if err := validateColumnAssetKind(kind); err != nil {
		return ColumnAssetRef{}, err
	}
	if fileID == 0 {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: zero column asset file id")
	}
	if offset < 0 {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: negative column asset offset %d", offset)
	}
	if length < 0 {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: negative column asset length %d", length)
	}
	if length == 0 {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: empty column asset payload")
	}
	if len(payload) != length {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: column asset payload bytes=%d want length=%d", len(payload), length)
	}
	return ColumnAssetRef{
		Kind:     kind,
		FileID:   fileID,
		Offset:   offset,
		Length:   int64(length),
		Checksum: crc32.ChecksumIEEE(payload),
	}, nil
}

func validateColumnAssetRef(ref ColumnAssetRef) error {
	if err := validateColumnAssetKind(ref.Kind); err != nil {
		return err
	}
	if ref.FileID == 0 {
		return fmt.Errorf("colgranule: zero column asset ref file id")
	}
	if ref.Offset < 0 {
		return fmt.Errorf("colgranule: negative column asset ref offset %d", ref.Offset)
	}
	if ref.Length < 0 {
		return fmt.Errorf("colgranule: negative column asset ref length %d", ref.Length)
	}
	if ref.Length == 0 {
		return fmt.Errorf("colgranule: empty column asset ref payload")
	}
	return nil
}

func (ref ColumnAssetRef) key() columnAssetKey {
	return columnAssetKey{
		fileID: ref.FileID,
		offset: ref.Offset,
	}
}

func validateColumnAssetKind(kind ColumnAssetKind) error {
	switch kind {
	case "":
		return fmt.Errorf("colgranule: empty column asset kind")
	case ColumnAssetKindTCS1PartImage:
		return nil
	default:
		return fmt.Errorf("colgranule: unsupported column asset kind %s", kind)
	}
}
