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

	// If we had an mmap, it may now be larger than the file. Clear it to avoid
	// SIGBUS on reads and to allow remapping to the new size.
	s.mmapMu.Lock()
	if s.mmapData != nil {
		_ = munmap(s.mmapData)
		s.mmapData = nil
	}
	s.mmapMu.Unlock()

	return nil
}

// RepairTail scans the slab file to find the last complete (and checksummed)
// record and truncates any partial/corrupt tail bytes. This is primarily a
// crash-recovery helper.
func (s *SlabFile) RepairTail() error {
	info, err := s.File.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if size == 0 {
		s.Size = 0
		return nil
	}

	// Track the last few record starts so we can drop a corrupted tail record
	// without needing a second full scan.
	const keepStarts = 4
	var starts [keepStarts]int64
	var startsN int

	var headerArr [HeaderSize]byte
	offset := int64(0)
	lastGoodEnd := int64(0)

	for {
		if offset+HeaderSize > size {
			break
		}
		if _, err := s.File.ReadAt(headerArr[:], offset); err != nil {
			break
		}
		keyLen := binary.LittleEndian.Uint16(headerArr[4:6])
		valLen := binary.LittleEndian.Uint32(headerArr[6:10])
		recLen := int64(HeaderSize) + int64(keyLen) + int64(valLen)
		if recLen <= HeaderSize || offset+recLen > size {
			break
		}

		// Record this start in a ring.
		starts[startsN%keepStarts] = offset
		startsN++

		offset += recLen
		lastGoodEnd = offset
	}

	trimTo := lastGoodEnd
	// Verify CRC for the last complete record; if it fails, drop it (and retry
	// a few times using our ring buffer).
	for tries := 0; tries < keepStarts; tries++ {
		if startsN == 0 {
			trimTo = 0
			break
		}
		start := starts[(startsN-1-tries)%keepStarts]
		if start < 0 || start+HeaderSize > trimTo {
			continue
		}
		if _, err := s.File.ReadAt(headerArr[:], start); err != nil {
			continue
		}
		crc := binary.LittleEndian.Uint32(headerArr[0:4])
		keyLen := binary.LittleEndian.Uint16(headerArr[4:6])
		valLen := binary.LittleEndian.Uint32(headerArr[6:10])
		dataLen := int64(keyLen) + int64(valLen)
		dataStart := start + HeaderSize
		dataEnd := dataStart + dataLen
		if dataEnd > trimTo || dataLen < 0 || dataLen > int64(int(dataLen)) {
			trimTo = start
			continue
		}
		payload := make([]byte, int(dataLen))
		if _, err := s.File.ReadAt(payload, dataStart); err != nil {
			trimTo = start
			continue
		}
		sum := crc32.Update(0, crc32cTable, headerArr[4:10])
		sum = crc32.Update(sum, crc32cTable, payload)
		if sum == crc {
			// Tail record is valid.
			break
		}
		trimTo = start
	}

	if trimTo < size {
		if err := s.Truncate(trimTo); err != nil {
			return err
		}
	}
	s.Size = trimTo
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
	
	// Fast check: if data exists and covers the request, use it.
	// We don't know totalLen64 yet, but we check if we can at least read the header.
	canReadHeader := data != nil && realStart >= 0 && realStart+HeaderSize <= int64(len(data))
	
	if canReadHeader {
		header := data[realStart : realStart+HeaderSize]
		keyLen := binary.LittleEndian.Uint16(header[4:6])
		valLen := binary.LittleEndian.Uint32(header[6:10])
		totalLen64 := int64(keyLen) + int64(valLen)
		if totalLen64 < 0 || totalLen64 > int64(int(totalLen64)) {
			return nil, ErrRecordTooLarge, true
		}
		dataEnd := realStart + HeaderSize + totalLen64
		if dataEnd <= int64(len(data)) {
			// All good, perform read
			record := data[realStart+HeaderSize : dataEnd]
			// Optimize: Single CRC pass over header-fields + payload
			// Header CRC is at 0:4. Fields are 4:10.
			sum := crc32.Update(0, crc32cTable, header[4:10])
			sum = crc32.Update(sum, crc32cTable, record)
			
			// Verify against stored CRC
			crc := binary.LittleEndian.Uint32(header[0:4])
			if sum != crc {
				return nil, ErrChecksumMismatch, true
			}
			return record[int64(keyLen):], nil, true
		}
	}

	// Slow path: Remap if needed
	s.mmapMu.Lock()
	defer s.mmapMu.Unlock()
	
	// Double check under lock
	data = s.mmapData
	// We need file size to know if remapping is worth it
	info, err := s.File.Stat()
	if err != nil {
		return nil, nil, false
	}
	currentSize := info.Size()
	
	if data == nil || int64(len(data)) < currentSize {
		// Remap
		if data != nil {
			_ = munmap(data)
		}
		if currentSize > 0 && currentSize <= int64(int(currentSize)) {
			b, err := mmapReadOnly(s.File, int(currentSize))
			if err == nil {
				s.mmapData = b
				data = b
			}
		}
	}
	
	if data == nil {
		return nil, nil, false
	}
	
	// Retry logic with new data
	if realStart < 0 || realStart+HeaderSize > int64(len(data)) {
		return nil, nil, false
	}
	header := data[realStart : realStart+HeaderSize]
	keyLen := binary.LittleEndian.Uint16(header[4:6])
	valLen := binary.LittleEndian.Uint32(header[6:10])
	totalLen64 := int64(keyLen) + int64(valLen)
	
	dataEnd := realStart + HeaderSize + totalLen64
	if dataEnd > int64(len(data)) {
		return nil, nil, false
	}
	
	record := data[realStart+HeaderSize : dataEnd]
	sum := crc32.Update(0, crc32cTable, header[4:10])
	sum = crc32.Update(sum, crc32cTable, record)
	crc := binary.LittleEndian.Uint32(header[0:4])
	if sum != crc {
		return nil, ErrChecksumMismatch, true
	}
	
	return record[int64(keyLen):], nil, true
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
	written := 0
	for written < len(buf) {
		n, err := s.File.Write(buf[written:])
		if n > 0 {
			written += n
		}
		if err != nil {
			// Best-effort: remove any partial tail bytes so the active tail stays aligned.
			_ = s.Truncate(offset)
			return 0, err
		}
		if n == 0 {
			_ = s.Truncate(offset)
			return 0, errors.New("short write")
		}
	}

	s.Size += int64(written)
	return offset, nil
}
