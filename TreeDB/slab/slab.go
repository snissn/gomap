package slab

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

const (
	// HeaderSize: CRC(4) + KeyLen(2) + ValueLen(4)
	HeaderSize = 10
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

var syncDirFn = syncDir

var (
	// MaxSlabSize is 4GB (hard limit for rotation).
	// Variable to allow testing overrides.
	MaxSlabSize int64 = 4 * 1024 * 1024 * 1024
	// MaxRecordSize bounds a single slab record (header + key + value).
	// Set <= 0 to disable the cap.
	MaxRecordSize int64 = 64 * 1024 * 1024
	// MaxDeadMappings caps the number of old mmaps retained to avoid exhausting
	// vm.max_map_count. Set <= 0 to disable the cap.
	MaxDeadMappings = 64
)

var (
	ErrChecksumMismatch = errors.New("slab record checksum mismatch")
	ErrRecordTooLarge   = errors.New("record too large")
	ErrSlabFull         = errors.New("slab file is full")
	ErrReadOnly         = errors.New("slab is read-only")
	ErrV2HeaderMismatch = errors.New("slab: v2 header magic mismatch")
)

func debugRecordTooLargeEnabled() bool {
	val := os.Getenv("TREEDB_SLAB_DEBUG_RECORD_TOO_LARGE")
	if val == "" {
		return false
	}
	ok, err := strconv.ParseBool(val)
	if err != nil {
		return false
	}
	return ok
}

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
	readOnly bool
	RefCount atomic.Int64
	IsZombie atomic.Bool
	Size     int64 // Track size for rotation
	// writeOffset tracks the file offset for sequential appends.
	writeOffset int64

	closed atomic.Bool
	openMu sync.Mutex

	// mmapData holds the current read-only mapping. Readers load it without taking
	// locks; remaps publish a new mapping and keep the old mapping alive until
	// Close to avoid use-after-free.
	mmapData atomic.Value // stores []byte (may be nil slice)

	remapMu        sync.Mutex
	remapRequested atomic.Bool

	deadMappings      [][]byte // Old mappings retained for safety (prevent use-after-free)
	remapCount        atomic.Uint64
	deadMappingsCount atomic.Uint64

	// writeScratch is a reusable buffer for appending records. SlabManager
	// serializes writers, so this is safe without additional locking.
	writeScratch []byte

	// V2 Support
	version    uint8
	globalDict []byte
	globalDecs *sync.Pool

	// recent local dictionaries for USE_REF emission
	recentDicts     map[uint64]dictRefEntry
	recentDictOrder []uint64
}

type dictRefEntry struct {
	zoneID uint32
	crc    uint32
}

func newSlabFile(path string, id uint32, f *os.File, size int64, readOnly bool, version uint8, globalDict []byte, globalDecs *sync.Pool) *SlabFile {
	sf := &SlabFile{
		ID:          id,
		Path:        path,
		File:        f,
		readOnly:    readOnly,
		Size:        size,
		writeOffset: size,
		version:     version,
		globalDict:  globalDict,
		globalDecs:  globalDecs,
	}
	// Initialize atomic.Value with the concrete type so Load is safe.
	sf.mmapData.Store([]byte(nil))
	return sf
}

func (s *SlabFile) detectV2Locked() error {
	if s.Size < SlabV2DataStart {
		return nil
	}
	var magic [8]byte
	if _, err := s.File.ReadAt(magic[:], 0); err != nil {
		return nil // Ignore read error, might not be V2
	}
	if string(magic[:]) != MagicV2 {
		return nil
	}
	headerBuf := make([]byte, FileHeaderSizeV2)
	if _, err := s.File.ReadAt(headerBuf, 0); err != nil {
		return err
	}
	s.version = headerBuf[8]
	if s.version != Version2 {
		return ErrV2HeaderMismatch
	}
	dictBuf := make([]byte, GlobalDictSize)
	if _, err := s.File.ReadAt(dictBuf, FileHeaderSizeV2); err != nil {
		return err
	}
	hasDict := false
	for _, b := range dictBuf {
		if b != 0 {
			hasDict = true
			break
		}
	}
	if hasDict {
		s.globalDict = dictBuf
		s.globalDecs = &sync.Pool{
			New: func() any {
				dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(dictBuf))
				return dec
			},
		}
	}
	return nil
}

func (s *SlabFile) checkBoundary(currentSize, recordLen int64) error {
	if s.version < Version2 {
		return nil
	}
	if recordLen > maxV2RecordSize {
		return ErrRecordTooLarge
	}
	// Check if we are at or will cross a boundary.
	// Boundaries are at ZoneSize, 2*ZoneSize, etc.
	// Slab start (0) and Zone 0 start (64KB) are NOT boundaries for headers.
	if currentSize >= ZoneSize {
		if currentSize%ZoneSize == 0 {
			return ErrRecordTooLarge
		}
		nextBoundary := ((currentSize / ZoneSize) + 1) * ZoneSize
		if currentSize+recordLen > nextBoundary {
			return ErrRecordTooLarge
		}
	}
	return nil
}

// OpenSlab opens or creates a slab file.
func OpenSlab(path string, id uint32) (*SlabFile, error) {
	created := false
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		created = true
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if created {
		if err := syncDirFn(filepath.Dir(path)); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	sf := newSlabFile(path, id, f, info.Size(), false, 0, nil, nil)
	if err := sf.detectV2Locked(); err != nil {
		f.Close()
		return nil, err
	}
	return sf, nil
}

// OpenSlabReadOnly opens an existing slab file in read-only mode.
func OpenSlabReadOnly(path string, id uint32) (*SlabFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	sf := newSlabFile(path, id, f, info.Size(), true, 0, nil, nil)
	if err := sf.detectV2Locked(); err != nil {
		f.Close()
		return nil, err
	}
	return sf, nil
}

// OpenSlabLazy registers a slab without opening its file descriptor.
func OpenSlabLazy(path string, id uint32, size int64) *SlabFile {
	return newSlabFile(path, id, nil, size, false, 0, nil, nil)
}

// OpenSlabLazyReadOnly registers a slab without opening its file descriptor in read-only mode.
func OpenSlabLazyReadOnly(path string, id uint32, size int64) *SlabFile {
	return newSlabFile(path, id, nil, size, true, 0, nil, nil)
}

func (s *SlabFile) readRaw(offset int64, buf []byte) error {
	// Try mmap
	data, _ := s.mmapData.Load().([]byte)
	if data != nil && offset >= 0 && offset+int64(len(buf)) <= int64(len(data)) {
		copy(buf, data[offset:])
		return nil
	}
	// Fallback
	if err := s.ensureOpen(); err != nil {
		return err
	}
	_, err := s.File.ReadAt(buf, offset)
	return err
}

func (s *SlabFile) dictSlice(offset int64, length int) ([]byte, bool) {
	data, _ := s.mmapData.Load().([]byte)
	if data == nil || offset < 0 || offset+int64(length) > int64(len(data)) {
		return nil, false
	}
	return data[offset : offset+int64(length)], true
}

func (s *SlabFile) GetDecoder(offset int64) (*zstd.Decoder, func(), error) {
	if s.version != Version2 {
		return nil, nil, errors.New("not v2 slab")
	}

	// Zone 0 uses Global Dict by definition.
	if offset < ZoneSize {
		if s.globalDecs == nil {
			// Fallback to raw ZSTD if no dictionary is present in Zone 0.
			dec, _ := zstd.NewReader(nil)
			return dec, func() {}, nil
		}
		dec := s.globalDecs.Get().(*zstd.Decoder)
		return dec, func() { s.globalDecs.Put(dec) }, nil
	}

	zoneID := offset / ZoneSize
	headerOffset := zoneID * ZoneSize

	var headerBuf [ZoneHeaderSize]byte
	if err := s.readRaw(headerOffset, headerBuf[:]); err != nil {
		return nil, nil, err
	}

	var zh ZoneHeader
	if err := zh.Unmarshal(headerBuf[:]); err != nil {
		return nil, nil, err
	}

	switch zh.DictType {
	case ZoneDictGlobal:
		if s.globalDecs == nil {
			dec, _ := zstd.NewReader(nil)
			return dec, func() {}, nil
		}
		dec := s.globalDecs.Get().(*zstd.Decoder)
		return dec, func() { s.globalDecs.Put(dec) }, nil
	case ZoneDictLocal:
		if zh.DictCRC == 0 {
			return nil, nil, errors.New("slab: local dictionary CRC missing")
		}
		key := dictCacheKey{slab: s, zoneID: uint32(zoneID)}
		if pool, ok := dictPools.get(key); ok {
			dec := pool.Get().(*zstd.Decoder)
			return dec, func() { pool.Put(dec) }, nil
		}

		// Load from disk. Dict is 32KB starting after 64B header.
		dictOffset := headerOffset + ZoneHeaderSize
		dictBuf, ok := s.dictSlice(dictOffset, GlobalDictSize)
		if !ok {
			if err := s.ensureOpen(); err == nil {
				s.remapToFileSize()
				dictBuf, ok = s.dictSlice(dictOffset, GlobalDictSize)
			}
		}
		if !ok {
			buf := make([]byte, GlobalDictSize)
			if err := s.readRaw(dictOffset, buf); err != nil {
				return nil, nil, err
			}
			dictBuf = buf
		}

		// Verify CRC
		if !zh.VerifyDict(dictBuf) {
			return nil, nil, errors.New("slab: local dictionary CRC mismatch")
		}

		// Create pool
		pool := &sync.Pool{
			New: func() any {
				dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(dictBuf))
				return dec
			},
		}
		pool = dictPools.getOrAdd(key, pool)
		dec := pool.Get().(*zstd.Decoder)
		return dec, func() { pool.Put(dec) }, nil
	case ZoneDictRef:
		refZoneID := int64(zh.DictLength)
		if refZoneID <= 0 {
			return nil, nil, errors.New("slab: invalid ref zone id")
		}
		const maxRefDepth = 4
		for depth := 0; depth < maxRefDepth; depth++ {
			refHeaderOffset := refZoneID * ZoneSize
			var refHeaderBuf [ZoneHeaderSize]byte
			if err := s.readRaw(refHeaderOffset, refHeaderBuf[:]); err != nil {
				return nil, nil, err
			}
			var refHeader ZoneHeader
			if err := refHeader.Unmarshal(refHeaderBuf[:]); err != nil {
				return nil, nil, err
			}

			switch refHeader.DictType {
			case ZoneDictGlobal:
				if s.globalDecs == nil {
					dec, _ := zstd.NewReader(nil)
					return dec, func() {}, nil
				}
				dec := s.globalDecs.Get().(*zstd.Decoder)
				return dec, func() { s.globalDecs.Put(dec) }, nil
			case ZoneDictLocal:
				if zh.DictCRC == 0 {
					return nil, nil, errors.New("slab: ref dictionary CRC missing")
				}
				if refHeader.DictCRC != 0 && refHeader.DictCRC != zh.DictCRC {
					return nil, nil, errors.New("slab: ref dictionary CRC mismatch")
				}
				refKey := dictCacheKey{slab: s, zoneID: uint32(refZoneID)}
				if pool, ok := dictPools.get(refKey); ok {
					dec := pool.Get().(*zstd.Decoder)
					return dec, func() { pool.Put(dec) }, nil
				}
				dictOffset := refHeaderOffset + ZoneHeaderSize
				dictBuf, ok := s.dictSlice(dictOffset, GlobalDictSize)
				if !ok {
					if err := s.ensureOpen(); err == nil {
						s.remapToFileSize()
						dictBuf, ok = s.dictSlice(dictOffset, GlobalDictSize)
					}
				}
				if !ok {
					buf := make([]byte, GlobalDictSize)
					if err := s.readRaw(dictOffset, buf); err != nil {
						return nil, nil, err
					}
					dictBuf = buf
				}
				sum := crc32.Checksum(dictBuf, crc32cTable)
				if sum != zh.DictCRC {
					return nil, nil, errors.New("slab: ref dictionary CRC mismatch")
				}
				pool := &sync.Pool{
					New: func() any {
						dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(dictBuf))
						return dec
					},
				}
				pool = dictPools.getOrAdd(refKey, pool)
				dec := pool.Get().(*zstd.Decoder)
				return dec, func() { pool.Put(dec) }, nil
			case ZoneDictRef:
				refZoneID = int64(refHeader.DictLength)
				if refZoneID <= 0 {
					return nil, nil, errors.New("slab: invalid nested ref zone id")
				}
				continue
			default:
				return nil, nil, errors.New("slab: ref dicts not implemented")
			}
		}
		return nil, nil, errors.New("slab: ref dict depth exceeded")
	default:
		return nil, nil, errors.New("ref dicts not implemented")
	}
}

func (s *SlabFile) Close() error {
	s.closed.Store(true)
	dictPools.purgeSlab(s)

	s.remapMu.Lock()
	data, _ := s.mmapData.Load().([]byte)
	if data != nil {
		_ = munmap(data)
	}
	for _, b := range s.deadMappings {
		_ = munmap(b)
	}
	s.deadMappings = nil
	s.deadMappingsCount.Store(0)
	s.mmapData.Store([]byte(nil))
	s.remapMu.Unlock()
	if s.File == nil {
		return nil
	}
	return s.File.Close()
}

func (s *SlabFile) Sync() error {
	if s.readOnly {
		return ErrReadOnly
	}
	return s.File.Sync()
}

func (s *SlabFile) ensureOpen() error {
	if s == nil {
		return os.ErrClosed
	}
	if s.closed.Load() {
		return os.ErrClosed
	}
	if s.File != nil {
		return nil
	}

	s.openMu.Lock()
	defer s.openMu.Unlock()
	if s.File != nil {
		return nil
	}
	var f *os.File
	var err error
	if s.readOnly {
		f, err = os.Open(s.Path)
	} else {
		f, err = os.OpenFile(s.Path, os.O_RDWR, 0600)
	}
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	s.File = f
	s.Size = info.Size()
	s.writeOffset = s.Size

	// Detect V2
	if err := s.detectV2Locked(); err != nil {
		_ = s.File.Close()
		s.File = nil
		return err
	}

	if _, err := s.File.Seek(s.writeOffset, io.SeekStart); err != nil {
		_ = s.File.Close()
		s.File = nil
		return err
	}
	return nil
}

// Truncate resizes the slab file. Used for crash recovery.
func (s *SlabFile) Truncate(size int64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	if s.version >= Version2 && size < SlabV2DataStart {
		size = SlabV2DataStart
	}
	if err := s.File.Truncate(size); err != nil {
		return err
	}
	s.Size = size
	s.writeOffset = size
	if _, err := s.File.Seek(s.writeOffset, io.SeekStart); err != nil {
		return err
	}

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
	if s.readOnly {
		return ErrReadOnly
	}
	info, err := s.File.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if size == 0 {
		s.Size = 0
		return nil
	}

	startOffset := int64(0)
	if s.version >= Version2 {
		if size < SlabV2DataStart {
			s.Size = size
			return nil
		}
		startOffset = SlabV2DataStart
	}

	// Track the last few record starts so we can drop a corrupted tail record
	// without needing a second full scan.
	const keepStarts = 4
	var starts [keepStarts]int64
	var startsN int

	var headerArr [HeaderSize]byte
	offset := startOffset
	lastGoodEnd := startOffset

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
	maxTries := keepStarts
	if startsN < maxTries {
		maxTries = startsN
	}
	for tries := 0; tries < maxTries; tries++ {
		if startsN == 0 {
			trimTo = startOffset
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
	s.writeOffset = trimTo
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
		if err := s.ensureOpen(); err != nil {
			return nil, err
		}
		s.remapToFileSize()
		if val, err, ok := s.readViaMmap(realStart, verifyCRC); ok {
			return val, err
		}
		// If still not ok (e.g. file shrunk or offset really out of bounds), fall back.
	}

	// Fallback path: pread into buffers.
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	var headerArr [HeaderSize]byte
	headerBuf := headerArr[:]
	if _, err := s.File.ReadAt(headerBuf, realStart); err != nil {
		return nil, err
	}

	_ = headerBuf[9]
	keyLen := uint16(headerBuf[4]) | uint16(headerBuf[5])<<8
	valLen := uint32(headerBuf[6]) | uint32(headerBuf[7])<<8 | uint32(headerBuf[8])<<16 | uint32(headerBuf[9])<<24
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
		crc := uint32(headerBuf[0]) | uint32(headerBuf[1])<<8 | uint32(headerBuf[2])<<16 | uint32(headerBuf[3])<<24
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
		_ = header[9]
		keyLen := uint16(header[4]) | uint16(header[5])<<8
		valLen := uint32(header[6]) | uint32(header[7])<<8 | uint32(header[8])<<16 | uint32(header[9])<<24
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
				crc := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16 | uint32(header[3])<<24
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
	if s.File == nil {
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
	if s.File == nil {
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

	if data != nil && MaxDeadMappings > 0 && len(s.deadMappings) >= MaxDeadMappings {
		// Keep the current mapping to avoid unbounded map growth; reads beyond it
		// will fall back to pread.
		return
	}

	if data != nil {
		// Don't unmap immediately; existing readers might hold views.
		s.deadMappings = append(s.deadMappings, data)
		s.deadMappingsCount.Add(1)
	}

	b, err := mmapReadOnly(s.File, int(currentSize))
	if err != nil {
		return
	}
	s.mmapData.Store(b)
	s.remapCount.Add(1)
}

// Write appends a record to the slab and returns the offset.
// Thread-safety: This should be called by a single writer (SlabManager mutex).
func (s *SlabFile) Write(key, value []byte) (int64, error) {
	if s.readOnly {
		return 0, ErrReadOnly
	}
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

	// For V2, records must not straddle 2MB boundaries.
	if s.version >= Version2 {
		if recordLen64 > maxV2RecordSize {
			return 0, ErrRecordTooLarge
		}
		// If we are EXACTLY at a boundary (that requires a header), signal manager.
		// Zone 0 (64KB) is special and doesn't need a header here.
		if s.Size >= ZoneSize && s.Size%ZoneSize == 0 {
			return 0, ErrRecordTooLarge
		}
		nextBoundary := ((s.Size / ZoneSize) + 1) * ZoneSize
		if s.Size+recordLen64 > nextBoundary {
			return 0, ErrRecordTooLarge // Signal SlabManager to rotate zone
		}
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

	offset := s.Size
	if s.writeOffset != s.Size {
		if _, err := s.File.Seek(s.Size, io.SeekStart); err != nil {
			return 0, err
		}
		s.writeOffset = s.Size
	}
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
	s.writeOffset = s.Size
	return offset, nil
}

func (s *SlabFile) EncodeRecord(key, value []byte, buf []byte) []byte {
	keyLen := len(key)
	valLen := len(value)
	recordLen := HeaderSize + keyLen + valLen

	if cap(buf) < recordLen {
		buf = make([]byte, recordLen)
	}
	buf = buf[:recordLen]

	var lenArr [6]byte
	binary.LittleEndian.PutUint16(lenArr[0:2], uint16(keyLen))
	binary.LittleEndian.PutUint32(lenArr[2:6], uint32(valLen))

	checksum := crc32.Update(0, crc32cTable, lenArr[:])
	checksum = crc32.Update(checksum, crc32cTable, key)
	checksum = crc32.Update(checksum, crc32cTable, value)

	binary.LittleEndian.PutUint32(buf[0:4], checksum)
	copy(buf[4:10], lenArr[:])
	copy(buf[10:10+keyLen], key)
	copy(buf[10+keyLen:], value)
	return buf
}

// WriteBatch appends a pre-built record stream to the slab and returns the
// starting file offset. Thread-safety: This should be called by a single writer
// (SlabManager mutex).
func (s *SlabFile) WriteBatch(buf []byte, ignoreBoundary bool) (int64, error) {
	if s.readOnly {
		return 0, ErrReadOnly
	}
	if len(buf) == 0 {
		return s.Size, nil
	}
	if int64(s.Size)+int64(len(buf)) > MaxSlabSize {
		return 0, ErrSlabFull
	}

	// For V2, batches must not straddle 2MB boundaries.
	if s.version >= Version2 && !ignoreBoundary {
		// If we are EXACTLY at a boundary (that requires a header), signal manager.
		if s.Size >= ZoneSize && s.Size%ZoneSize == 0 {
			if debugRecordTooLargeEnabled() {
				log.Printf("slab: batch too large at boundary size=%d buf=%d", s.Size, len(buf))
			}
			return 0, ErrRecordTooLarge
		}
		nextBoundary := ((s.Size / ZoneSize) + 1) * ZoneSize
		if s.Size+int64(len(buf)) > nextBoundary {
			if debugRecordTooLargeEnabled() {
				log.Printf("slab: batch crosses boundary size=%d buf=%d next=%d", s.Size, len(buf), nextBoundary)
			}
			return 0, ErrRecordTooLarge // Signal SlabManager to rotate zone
		}
	}

	offset := s.Size
	if s.writeOffset != s.Size {
		if _, err := s.File.Seek(s.Size, io.SeekStart); err != nil {
			return 0, err
		}
		s.writeOffset = s.Size
	}
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
	s.writeOffset = s.Size
	return offset, nil
}

func syncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}
