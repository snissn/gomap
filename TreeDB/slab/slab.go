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
	// MaxRecordSize bounds a single slab record (header + key + value).
	// Set <= 0 to disable the cap.
	MaxRecordSize int64 = 64 * 1024 * 1024
)

var (
	ErrChecksumMismatch = errors.New("slab record checksum mismatch")
	ErrRecordTooLarge   = errors.New("record too large")
	ErrSlabFull         = errors.New("slab file is full")
)

func recordSizeExceedsMax(keyLen uint16, valLen uint32) bool {
	if MaxRecordSize <= 0 {
		return false
	}
	recordLen := int64(HeaderSize) + int64(keyLen) + int64(valLen)
	return recordLen > MaxRecordSize
}

// SlabFile represents a single physical .slab file.
type SlabFile struct {
	ID       uint32
	Path     string
	File     *os.File
	RefCount atomic.Int64
	IsZombie atomic.Bool
	Size     int64 // Track size for rotation

	closed atomic.Bool

	// mmapData holds the current read-only mapping. Readers load it without taking
	// locks; remaps publish a new mapping and keep the old mapping alive until
	// Close to avoid use-after-free.
	mmapData atomic.Value // stores []byte (may be nil slice)

	remapMu        sync.Mutex
	remapRequested atomic.Bool

	deadMappings [][]byte // Old mappings retained for safety (prevent use-after-free)

	// writeScratch is a reusable buffer for appending records. SlabManager
	// serializes writers, so this is safe without additional locking.
	writeScratch []byte
}

// OpenSlab opens or creates a slab file.
func OpenSlab(path string, id uint32) (*SlabFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	sf := &SlabFile{
		ID:   id,
		Path: path,
		File: f,
		Size: info.Size(),
	}
	// Initialize atomic.Value with the concrete type so Load is safe.
	sf.mmapData.Store([]byte(nil))
	return sf, nil
}

func (s *SlabFile) Close() error {
	s.closed.Store(true)

	s.remapMu.Lock()
	data, _ := s.mmapData.Load().([]byte)
	if data != nil {
		_ = munmap(data)
	}
	for _, b := range s.deadMappings {
		_ = munmap(b)
	}
	s.deadMappings = nil
	s.mmapData.Store([]byte(nil))
	s.remapMu.Unlock()
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
	s.remapMu.Lock()
	data, _ := s.mmapData.Load().([]byte)
	if data != nil {
		_ = munmap(data)
	}
	s.mmapData.Store([]byte(nil))
	s.remapMu.Unlock()

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
		if recordSizeExceedsMax(keyLen, valLen) {
			break
		}
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
		if recordSizeExceedsMax(keyLen, valLen) {
			trimTo = start
			continue
		}
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
func (s *SlabFile) Read(offset int64, verifyCRC bool) ([]byte, error) {
	return s.read(offset, verifyCRC, false)
}

// ReadUnsafe returns a zero-copy view of the record. It forces a synchronous
// remap if the current mapping does not cover the record.
func (s *SlabFile) ReadUnsafe(offset int64, verifyCRC bool) ([]byte, error) {
	return s.read(offset, verifyCRC, true)
}

func (s *SlabFile) read(offset int64, verifyCRC bool, unsafe bool) ([]byte, error) {
	// Offset points to KeyLen. CRC is at Offset - 4.
	realStart := offset - 4
	if realStart < 0 {
		return nil, errors.New("invalid slab offset")
	}

	if !unsafe {
		// Fast path: mmap view (best-effort, read-only).
		if val, err, ok := s.readViaMmap(realStart, verifyCRC); ok {
			return val, err
		}
	} else {
		// Guaranteed zero-copy path: force remap if needed.
		if val, err, ok := s.readViaMmap(realStart, verifyCRC); ok {
			return val, err
		}
		s.remapToFileSize()
		if val, err, ok := s.readViaMmap(realStart, verifyCRC); ok {
			return val, err
		}
		// If still not ok (e.g. file shrunk or offset really out of bounds), fall back.
	}

	// Fallback path: pread into buffers.
	var headerArr [HeaderSize]byte
	headerBuf := headerArr[:]
	if _, err := s.File.ReadAt(headerBuf, realStart); err != nil {
		return nil, err
	}

	keyLen := binary.LittleEndian.Uint16(headerBuf[4:6])
	valLen := binary.LittleEndian.Uint32(headerBuf[6:10])
	if recordSizeExceedsMax(keyLen, valLen) {
		return nil, ErrRecordTooLarge
	}

	totalLen64 := int64(keyLen) + int64(valLen)
	if totalLen64 < 0 || totalLen64 > int64(int(totalLen64)) {
		return nil, ErrRecordTooLarge
	}
	totalLen := int(totalLen64)

	dataBuf := make([]byte, totalLen)
	if _, err := s.File.ReadAt(dataBuf, realStart+HeaderSize); err != nil {
		return nil, err
	}

	if verifyCRC {
		crc := binary.LittleEndian.Uint32(headerBuf[0:4])
		sum := crc32.Update(0, crc32cTable, headerBuf[4:10])
		sum = crc32.Update(sum, crc32cTable, dataBuf)
		if sum != crc {
			return nil, ErrChecksumMismatch
		}
	}

	// Return only Value (skipping Key).
	return dataBuf[keyLen:], nil
}

func (s *SlabFile) readViaMmap(realStart int64, verifyCRC bool) ([]byte, error, bool) {
	data, _ := s.mmapData.Load().([]byte)

	// Fast check: if data exists and covers the request, use it.
	// We don't know totalLen64 yet, but we check if we can at least read the header.
	canReadHeader := data != nil && realStart >= 0 && realStart+HeaderSize <= int64(len(data))

	if canReadHeader {
		header := data[realStart : realStart+HeaderSize]
		keyLen := binary.LittleEndian.Uint16(header[4:6])
		valLen := binary.LittleEndian.Uint32(header[6:10])
		if recordSizeExceedsMax(keyLen, valLen) {
			return nil, ErrRecordTooLarge, true
		}
		totalLen64 := int64(keyLen) + int64(valLen)
		if totalLen64 < 0 || totalLen64 > int64(int(totalLen64)) {
			return nil, ErrRecordTooLarge, true
		}
		dataEnd := realStart + HeaderSize + totalLen64
		if dataEnd <= int64(len(data)) {
			// All good, perform read
			record := data[realStart+HeaderSize : dataEnd]

			if verifyCRC {
				// Optimize: Single CRC pass over header-fields + payload
				// Header CRC is at 0:4. Fields are 4:10.
				sum := crc32.Update(0, crc32cTable, header[4:10])
				sum = crc32.Update(sum, crc32cTable, record)

				// Verify against stored CRC
				crc := binary.LittleEndian.Uint32(header[0:4])
				if sum != crc {
					return nil, ErrChecksumMismatch, true
				}
			}
			return record[int64(keyLen):], nil, true
		}
	}

	// Mapping missing or too small. Remap asynchronously and fall back to pread.
	s.maybeScheduleRemap()
	return nil, nil, false
}

func (s *SlabFile) maybeScheduleRemap() {
	if s == nil || s.closed.Load() {
		return
	}
	if !s.remapRequested.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer s.remapRequested.Store(false)
		s.remapToFileSize()
	}()
}

func (s *SlabFile) remapToFileSize() {
	if s == nil || s.closed.Load() {
		return
	}

	// Serialize with Close/Truncate and other remaps.
	s.remapMu.Lock()
	defer s.remapMu.Unlock()

	if s.closed.Load() {
		return
	}

	info, err := s.File.Stat()
	if err != nil {
		return
	}
	currentSize := info.Size()
	if currentSize <= 0 || currentSize > int64(int(currentSize)) {
		return
	}

	data, _ := s.mmapData.Load().([]byte)
	if data != nil && int64(len(data)) >= currentSize {
		return
	}

	if data != nil {
		// Don't unmap immediately; existing readers might hold views.
		s.deadMappings = append(s.deadMappings, data)
	}

	b, err := mmapReadOnly(s.File, int(currentSize))
	if err != nil {
		return
	}
	s.mmapData.Store(b)
}

// Write appends a record to the slab and returns the offset.
// Thread-safety: This should be called by a single writer (SlabManager mutex).
func (s *SlabFile) Write(key, value []byte) (int64, error) {
	keyLen := len(key)
	valLen := len(value)

	if keyLen > int(^uint16(0)) || valLen > int(^uint32(0)) {
		return 0, ErrRecordTooLarge
	}
	if recordSizeExceedsMax(uint16(keyLen), uint32(valLen)) {
		return 0, ErrRecordTooLarge
	}

	recordLen64 := int64(HeaderSize) + int64(keyLen) + int64(valLen)
	if recordLen64 < 0 || recordLen64 > int64(int(recordLen64)) {
		return 0, ErrRecordTooLarge
	}
	if recordLen64 > MaxSlabSize {
		return 0, ErrRecordTooLarge
	}
	if int64(s.Size)+recordLen64 > MaxSlabSize {
		return 0, ErrSlabFull
	}

	recordLen := int(recordLen64)
	var lenArr [6]byte
	binary.LittleEndian.PutUint16(lenArr[0:2], uint16(keyLen))
	binary.LittleEndian.PutUint32(lenArr[2:6], uint32(valLen))

	// CRC(header-fields + payload)
	checksum := crc32.Update(0, crc32cTable, lenArr[:])
	checksum = crc32.Update(checksum, crc32cTable, key)
	checksum = crc32.Update(checksum, crc32cTable, value)

	// Prepare record buffer. Reuse scratch for common case to avoid allocating per record.
	// Cap reuse so a single large value doesn't pin a huge buffer forever.
	const maxScratch = 1 << 20 // 1 MiB
	var buf []byte
	if recordLen <= maxScratch {
		if cap(s.writeScratch) < recordLen {
			s.writeScratch = make([]byte, recordLen)
		}
		buf = s.writeScratch[:recordLen]
	} else {
		buf = make([]byte, recordLen)
	}

	binary.LittleEndian.PutUint32(buf[0:4], checksum)
	copy(buf[4:10], lenArr[:])
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

// WriteBatch appends a pre-built record stream to the slab and returns the
// starting file offset. Thread-safety: This should be called by a single writer
// (SlabManager mutex).
func (s *SlabFile) WriteBatch(buf []byte) (int64, error) {
	if len(buf) == 0 {
		return s.Size, nil
	}
	if int64(s.Size)+int64(len(buf)) > MaxSlabSize {
		return 0, ErrSlabFull
	}

	offset := s.Size
	written := 0
	for written < len(buf) {
		n, err := s.File.Write(buf[written:])
		if n > 0 {
			written += n
		}
		if err != nil {
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
