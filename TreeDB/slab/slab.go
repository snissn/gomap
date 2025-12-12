package slab

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync/atomic"
)

const (
	// HeaderSize: CRC(4) + KeyLen(2) + ValueLen(4)
	HeaderSize = 10
)

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
	return s.File.Close()
}

func (s *SlabFile) Sync() error {
	return s.File.Sync()
}

// ReadAt reads a record at the given offset.
func (s *SlabFile) Read(offset int64) ([]byte, error) {
	// 1. Read Header (10 bytes)
	headerBuf := make([]byte, HeaderSize)
	if _, err := s.File.ReadAt(headerBuf, offset); err != nil {
		return nil, err
	}

	crc := binary.LittleEndian.Uint32(headerBuf[0:4])
	keyLen := binary.LittleEndian.Uint16(headerBuf[4:6])
	valLen := binary.LittleEndian.Uint32(headerBuf[6:10])
	
	totalLen := int(keyLen) + int(valLen)
	
	// 2. Read Data (Key + Value)
	dataBuf := make([]byte, totalLen)
	if _, err := s.File.ReadAt(dataBuf, offset+HeaderSize); err != nil {
		return nil, err
	}
	
	// 3. Verify CRC
	// CRC covers KeyLen(2) + ValueLen(4) + KeyBytes + ValBytes.
	// We need to reconstruct the byte stream for CRC calculation OR feed parts.
	// Spec says: "CRC32C (Castagnoli) of (KeyLen..ValueBytes)"
	// So we need bytes [4:10] of header + dataBuf.
	
	crcHasher := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	crcHasher.Write(headerBuf[4:10])
	crcHasher.Write(dataBuf)
	
	if crcHasher.Sum32() != crc {
		return nil, ErrChecksumMismatch
	}
	
	// Return only Value (skipping Key).
	// Spec says Read(valuePtr) returns Value.
	// But we read the whole record.
	// Do we verify Key? The pointer doesn't store the key.
	// So we just return the value.
	
	return dataBuf[keyLen:], nil
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
