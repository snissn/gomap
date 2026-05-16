package colgranule

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type ColumnAssetKind string

const (
	ColumnAssetKindTCS1PartImage ColumnAssetKind = "tcs1_part_image"
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
	ReadTo(ref ColumnAssetRef, dst []byte) ([]byte, error)
}

type MemoryColumnAssetStore struct {
	mu      sync.Mutex
	nextOff int64
	assets  map[columnAssetKey][]byte
}

type columnAssetKey struct {
	kind   ColumnAssetKind
	fileID uint32
	offset int64
	length int64
}

func NewMemoryColumnAssetStore() *MemoryColumnAssetStore {
	return &MemoryColumnAssetStore{assets: make(map[columnAssetKey][]byte)}
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
	value := append([]byte(nil), payload...)
	s.mu.Lock()
	defer s.mu.Unlock()
	ref, err := newColumnAssetRef(kind, 1, s.nextOff, len(payload), payload)
	if err != nil {
		return ColumnAssetRef{}, err
	}
	s.nextOff += ref.Length
	s.assets[ref.key()] = value
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
	payload, ok := s.assets[ref.key()]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("colgranule: missing asset ref file=%d offset=%d length=%d kind=%s", ref.FileID, ref.Offset, ref.Length, ref.Kind)
	}
	if checksum := crc32.ChecksumIEEE(payload); checksum != ref.Checksum {
		return nil, fmt.Errorf("colgranule: asset ref checksum=%08x want %08x", checksum, ref.Checksum)
	}
	return payload, nil
}

func (s *MemoryColumnAssetStore) ReadTo(ref ColumnAssetRef, dst []byte) ([]byte, error) {
	payload, err := s.Read(ref)
	if err != nil {
		return nil, err
	}
	out := append(dst[:0], payload...)
	return out, nil
}

type SegmentColumnAssetStore struct {
	mu     sync.Mutex
	dir    string
	path   string
	file   *os.File
	fileID uint32
	size   int64
}

func OpenSegmentColumnAssetStore(dir string) (*SegmentColumnAssetStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("colgranule: empty segment asset dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "column-assets-000001.seg")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
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
	}, nil
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
	err := s.file.Close()
	s.file = nil
	return err
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
	if s.file == nil {
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
	if s.file == nil {
		return nil, fmt.Errorf("colgranule: closed segment asset store")
	}
	if ref.FileID != s.fileID {
		return nil, fmt.Errorf("colgranule: asset file id=%d want %d", ref.FileID, s.fileID)
	}
	if ref.Offset > s.size || ref.Length > s.size-ref.Offset {
		return nil, fmt.Errorf("colgranule: asset range offset=%d length=%d outside segment bytes=%d", ref.Offset, ref.Length, s.size)
	}
	if ref.Length > int64(int(ref.Length)) {
		return nil, fmt.Errorf("colgranule: asset length=%d exceeds host int", ref.Length)
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

func newColumnAssetRef(kind ColumnAssetKind, fileID uint32, offset int64, length int, payload []byte) (ColumnAssetRef, error) {
	if kind == "" {
		return ColumnAssetRef{}, fmt.Errorf("colgranule: empty column asset kind")
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
	if ref.Kind == "" {
		return fmt.Errorf("colgranule: empty column asset ref kind")
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
	return nil
}

func (ref ColumnAssetRef) key() columnAssetKey {
	return columnAssetKey{
		kind:   ref.Kind,
		fileID: ref.FileID,
		offset: ref.Offset,
		length: ref.Length,
	}
}
