package slab

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync"
	"sync/atomic"
)

const (
	// HeaderSize: CRC(4) + KeyLen(2) + ValueLen(4)
	HeaderSize = 10
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

var (
	// MaxSlabSize is 4GB (hard limit for rotation).
	// Variable to allow testing overrides.
	MaxSlabSize int64 = 4 * 1024 * 1024 * 1024
)

var (
	ErrChecksumMismatch = errors.New("slab record checksum mismatch")
	ErrRecordTooLarge   = errors.New("record too large")
	ErrSlabFull         = errors.New("slab file is full")
)

// SlabFile represents a single physical .slab file.
type SlabFile struct {
	ID       uint32
	Path     string
	File     *os.File
	RefCount atomic.Int64
	IsZombie atomic.Bool
	Size     int64 // Track size for rotation

	mmapMu   sync.Mutex
	mmapData []byte // Read-only mapping (best-effort), sized once and never resized.
}

// OpenSlab opens or creates a slab file.
func OpenSlab(path string, id uint32) (*SlabFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &SlabFile{
		ID:   id,
		Path: path,
		File: f,
		Size: info.Size(),
	}, nil
}

func (s *SlabFile) Close() error {
	s.mmapMu.Lock()
	if s.mmapData != nil {
		_ = munmap(s.mmapData)
		s.mmapData = nil
	}
	s.mmapMu.Unlock()
	return s.File.Close()
}

func (s *SlabFile) Sync() error {
	return s.File.Sync()
}

// Truncate resizes the slab file. Used for crash recovery.
func (s *SlabFile) Truncate(size int64) error {
	if err := s.File.Truncate(size); err != nil {
		return err
	}
	s.Size = size
	return nil
}

// ReadAt reads a record at the given offset (which points to KeyLen, skipping CRC).
func (s *SlabFile) Read(offset int64) ([]byte, error) {
	// Offset points to KeyLen. CRC is at Offset - 4.
	realStart := offset - 4
	if realStart < 0 {
		return nil, errors.New("invalid slab offset")
	}

	// Fast path: mmap view (best-effort, read-only).
	if val, err, ok := s.readViaMmap(realStart); ok {
		return val, err
	}

	// Fallback path: pread into buffers.
	var headerArr [HeaderSize]byte
	headerBuf := headerArr[:]
	if _, err := s.File.ReadAt(headerBuf, realStart); err != nil {
		return nil, err
	}

	crc := binary.LittleEndian.Uint32(headerBuf[0:4])
	keyLen := binary.LittleEndian.Uint16(headerBuf[4:6])
	valLen := binary.LittleEndian.Uint32(headerBuf[6:10])

	totalLen64 := int64(keyLen) + int64(valLen)
	if totalLen64 < 0 || totalLen64 > int64(int(totalLen64)) {
		return nil, ErrRecordTooLarge
	}
	totalLen := int(totalLen64)

	dataBuf := make([]byte, totalLen)
	if _, err := s.File.ReadAt(dataBuf, realStart+HeaderSize); err != nil {
		return nil, err
	}

	sum := crc32.Update(0, crc32cTable, headerBuf[4:10])
	sum = crc32.Update(sum, crc32cTable, dataBuf)
	if sum != crc {
		return nil, ErrChecksumMismatch
	}

	// Return only Value (skipping Key).
	// Spec says Read(valuePtr) returns Value.
	// But we read the whole record.
	// Do we verify Key? The pointer doesn't store the key.
	// So we just return the value.

	return dataBuf[keyLen:], nil
}

func (s *SlabFile) readViaMmap(realStart int64) ([]byte, error, bool) {
	data := s.mmapData
	if data == nil {
		s.mmapMu.Lock()
		if s.mmapData == nil {
			info, err := s.File.Stat()
			if err == nil && info.Size() > 0 && info.Size() <= int64(int(info.Size())) {
				b, err := mmapReadOnly(s.File, int(info.Size()))
				if err == nil {
					s.mmapData = b
				}
			}
		}
		data = s.mmapData
		s.mmapMu.Unlock()
	}
	if data == nil {
		return nil, nil, false
	}

	// Bounds checks to prevent SIGBUS/panics.
	if realStart < 0 || realStart+HeaderSize > int64(len(data)) {
		return nil, nil, false
	}

	header := data[realStart : realStart+HeaderSize]
	crc := binary.LittleEndian.Uint32(header[0:4])
	keyLen := binary.LittleEndian.Uint16(header[4:6])
	valLen := binary.LittleEndian.Uint32(header[6:10])

	totalLen64 := int64(keyLen) + int64(valLen)
	if totalLen64 < 0 || totalLen64 > int64(int(totalLen64)) {
		return nil, ErrRecordTooLarge, true
	}
	dataStart := realStart + HeaderSize
	dataEnd := dataStart + totalLen64
	if dataEnd > int64(len(data)) {
		// Mapping doesn't cover this record (active slab grew, etc); fall back to ReadAt.
		return nil, nil, false
	}

	record := data[dataStart:dataEnd]
	sum := crc32.Update(0, crc32cTable, header[4:10])
	sum = crc32.Update(sum, crc32cTable, record)
	if sum != crc {
		return nil, ErrChecksumMismatch, true
	}

	valStart := int64(keyLen)
	return record[valStart:], nil, true
}

// Write appends a record to the slab and returns the offset.
// Thread-safety: This should be called by a single writer (SlabManager mutex).
func (s *SlabFile) Write(key, value []byte) (int64, error) {
	keyLen := len(key)
	valLen := len(value)

	if int64(s.Size)+int64(HeaderSize+keyLen+valLen) > MaxSlabSize {
		return 0, ErrSlabFull
	}

	// Calculate CRC
	crcHasher := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	// Write KeyLen/ValLen to CRC
	lenBuf := make([]byte, 6)
	binary.LittleEndian.PutUint16(lenBuf[0:2], uint16(keyLen))
	binary.LittleEndian.PutUint32(lenBuf[2:6], uint32(valLen))
	crcHasher.Write(lenBuf)

	// Write Data to CRC
	crcHasher.Write(key)
	crcHasher.Write(value)

	checksum := crcHasher.Sum32()

	// Prepare full record buffer
	recordLen := HeaderSize + keyLen + valLen
	buf := make([]byte, recordLen)

	binary.LittleEndian.PutUint32(buf[0:4], checksum)
	copy(buf[4:10], lenBuf)
	copy(buf[10:10+keyLen], key)
	copy(buf[10+keyLen:], value)

	// Write to file
	// Since we opened with O_APPEND, Write writes to end.
	// But we need the offset.
	// We trust s.Size matches current end?
	// To be safe, we can use stat or seek?
	// Or we use atomic offset tracking?
	// Ideally SlabManager handles serialization, so s.Size is accurate.

	offset := s.Size
	n, err := s.File.Write(buf)
	if err != nil {
		return 0, err
	}

	s.Size += int64(n)
	return offset, nil
}
