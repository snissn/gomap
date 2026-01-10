package slab

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/page"
)

// SlabSet is an immutable list of SlabFiles active at a specific point in time.
type SlabSet struct {
	Files               map[uint32]*SlabFile
	RefCount            atomic.Int64
	disableReadChecksum bool
	compression         *compressionConfig
}
type SlabManager struct {
	dir        string
	readOnly   bool
	activeSlab *SlabFile
	slabs      map[uint32]*SlabFile // The master list of all live + zombie slabs
	mu         sync.RWMutex

	disableReadChecksum bool
	compression         compressionConfig
	omitSlabKeys        bool
	appendManyStreams   int

	appendManyScratch        []byte
	appendManyScratchStreams [][]byte
}

func NewSlabManager(dir string) (*SlabManager, error) {
	return NewSlabManagerWithOptions(dir, Options{})
}

func NewSlabManagerWithOptions(dir string, opts Options) (*SlabManager, error) {
	return newSlabManager(dir, false, opts)
}

func NewSlabManagerReadOnly(dir string, opts Options) (*SlabManager, error) {
	return newSlabManager(dir, true, opts)
}

func newSlabManager(dir string, readOnly bool, opts Options) (*SlabManager, error) {
	compression, err := normalizeCompressionOptions(opts.Compression)
	if err != nil {
		return nil, err
	}
	appendManyStreams := opts.AppendManyStreams
	if appendManyStreams <= 1 {
		appendManyStreams = 1
	}
	sm := &SlabManager{
		dir:               dir,
		readOnly:          readOnly,
		slabs:             make(map[uint32]*SlabFile),
		compression:       compression,
		omitSlabKeys:      opts.OmitSlabKeys,
		appendManyStreams: appendManyStreams,
	}

	matches, err := filepath.Glob(filepath.Join(dir, "data-*.slab"))
	if err != nil {
		return nil, err
	}

	type slabInfo struct {
		id   uint32
		path string
		size int64
	}

	var (
		maxID uint32
		found bool
		infos []slabInfo
	)

	for _, path := range matches {
		var id uint32
		_, err := fmt.Sscanf(filepath.Base(path), "data-%04d.slab", &id)
		if err != nil {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		infos = append(infos, slabInfo{id: id, path: path, size: info.Size()})
		if id >= maxID {
			maxID = id
			found = true
		}
	}

	if !found {
		if readOnly {
			return nil, fmt.Errorf("no slab files found in %q", dir)
		}
		s, err := OpenSlab(filepath.Join(dir, "data-0000.slab"), 0)
		if err != nil {
			return nil, err
		}
		sm.slabs[0] = s
		sm.activeSlab = s
	} else {
		for _, info := range infos {
			if info.id == maxID {
				var s *SlabFile
				var err error
				if readOnly {
					s, err = OpenSlabReadOnly(info.path, info.id)
				} else {
					s, err = OpenSlab(info.path, info.id)
				}
				if err != nil {
					return nil, err
				}
				sm.slabs[info.id] = s
				sm.activeSlab = s
				continue
			}
			if readOnly {
				sm.slabs[info.id] = OpenSlabLazyReadOnly(info.path, info.id, info.size)
			} else {
				sm.slabs[info.id] = OpenSlabLazy(info.path, info.id, info.size)
			}
		}
	}

	return sm, nil
}

func (sm *SlabManager) Compression() CompressionKind {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.compression.kind
}

func (sm *SlabManager) OmitSlabKeys() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.omitSlabKeys
}

func (sm *SlabManager) SetDisableReadChecksum(disable bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.disableReadChecksum = disable
}

func decodeValue(ptr page.ValuePtr, val []byte, compression *compressionConfig) ([]byte, error) {
	if !page.ValuePtrIsCompressed(ptr) {
		return val, nil
	}
	if compression == nil {
		return nil, errCompressedCorrupt
	}
	if page.ValuePtrIsFullCompressed(ptr) {
		_, v, err := compression.decompressRecord(val)
		return v, err
	}
	return compression.decompressValue(val)
}

func (sm *SlabManager) DecodeValueForCompactor(ptr page.ValuePtr, val []byte) ([]byte, error) {
	sm.mu.RLock()
	compression := &sm.compression
	sm.mu.RUnlock()
	return decodeValue(ptr, val, compression)
}

func (sm *SlabManager) DecodeRecordForCompactor(ptr page.ValuePtr, val []byte) ([]byte, []byte, error) {
	if !page.ValuePtrIsFullCompressed(ptr) {
		return nil, nil, fmt.Errorf("not a full compressed record")
	}
	sm.mu.RLock()
	compression := &sm.compression
	sm.mu.RUnlock()
	if compression == nil {
		return nil, nil, errCompressedCorrupt
	}
	return compression.decompressRecord(val)
}

func (sm *SlabManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, s := range sm.slabs {
		_ = s.Close()
	}
	sm.slabs = nil
	return nil
}

// GetSlabPath returns the path for a given slab ID.
func (sm *SlabManager) GetSlabPath(id uint32) string {
	return filepath.Join(sm.dir, fmt.Sprintf("data-%04d.slab", id))
}

// Read reads from the slab file identified by ptr.FileID.
// For Snapshot Isolation, the caller should ensure the file is pinned via a Snapshot.
// If accessing without snapshot (e.g. during Compaction or internal ops), care must be taken.
// Current impl uses RLock on the master map, so it's safe against concurrent Close() initiated by Prune/Compaction
// IF Prune/Compaction removes from map.
func (sm *SlabManager) Read(ptr page.ValuePtr) ([]byte, error) {
	sm.mu.RLock()
	s, ok := sm.slabs[ptr.FileID]
	verifyCRC := !sm.disableReadChecksum
	compression := &sm.compression
	sm.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("slab file %d not found", ptr.FileID)
	}

	val, err := s.Read(int64(ptr.Offset), verifyCRC)
	if err != nil {
		return nil, err
	}
	return decodeValue(ptr, val, compression)
}

func (sm *SlabManager) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	sm.mu.RLock()
	s, ok := sm.slabs[ptr.FileID]
	verifyCRC := !sm.disableReadChecksum
	compression := &sm.compression
	sm.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("slab file %d not found", ptr.FileID)
	}

	val, err := s.ReadUnsafe(int64(ptr.Offset), verifyCRC)
	if err != nil {
		return nil, err
	}
	return decodeValue(ptr, val, compression)
}

func (sm *SlabManager) Append(key, value []byte) (page.ValuePtr, error) {
	encoded := value
	encodedKey := key
	compressed := false
	fullCompressed := false
	omittedKey := false
	var err error
	if sm.compression.kind != CompressionNone {
		// Try full record compression first
		if enc, ok, err := sm.compression.compressRecord(key, value); err == nil && ok {
			encoded = enc
			encodedKey = nil
			compressed = true
			fullCompressed = true
		} else if err != nil {
			return page.ValuePtr{}, err
		} else {
			// Fall back to value-only compression
			encoded, compressed, err = sm.compression.compressValue(value)
			if err != nil {
				return page.ValuePtr{}, err
			}
		}
	}

	if !fullCompressed && sm.omitSlabKeys {
		encodedKey = nil
		omittedKey = true
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return page.ValuePtr{}, ErrReadOnly
	}

	offset, err := sm.activeSlab.Write(encodedKey, encoded)
	if err == ErrSlabFull {
		if err := sm.rotateLocked(); err != nil {
			return page.ValuePtr{}, err
		}
		offset, err = sm.activeSlab.Write(encodedKey, encoded)
	}

	if err != nil {
		return page.ValuePtr{}, err
	}

	length := uint32(2 + 4 + len(encodedKey) + len(encoded))
	if fullCompressed {
		length = page.ValuePtrMarkFullCompressed(length)
	} else if omittedKey {
		length = page.ValuePtrMarkOmittedKey(length)
		if compressed {
			length = page.ValuePtrMarkCompressed(length)
		}
	} else if compressed {
		length = page.ValuePtrMarkCompressed(length)
	}

	return page.ValuePtr{
		Offset: uint64(offset + 4),
		Length: length,
		FileID: sm.activeSlab.ID,
	}, nil
}

type appendManyMeta struct {
	idx            int
	start          int
	keyLen         int
	valLen         int
	compressed     bool
	fullCompressed bool
	omittedKey     bool
}

type appendManyPrep struct {
	keyLen    int
	valLen    int
	recordLen int
	crc       uint32
}

type appendManyPrepared struct {
	encodedKeys       [][]byte
	encodedValues     [][]byte
	prep              []appendManyPrep
	compressedFlags   []bool
	fullCompressed    []bool
	omittedKeyFlags   []bool
	appendManyStreams int
}

// AppendMany appends multiple records while amortizing system calls. It returns
// a pointer for each key/value pair (same order as inputs).
//
// Thread-safety: This is serialized by the SlabManager mutex.
func (sm *SlabManager) AppendMany(keys [][]byte, values [][]byte) ([]page.ValuePtr, error) {
	if len(keys) != len(values) {
		return nil, fmt.Errorf("AppendMany: keys/values length mismatch (%d != %d)", len(keys), len(values))
	}
	if len(keys) == 0 {
		return nil, nil
	}

	prepared, err := sm.prepareAppendMany(keys, values)
	if err != nil {
		return nil, err
	}
	if prepared.appendManyStreams <= 1 {
		return sm.appendManySingle(keys, prepared)
	}
	return sm.appendManyMulti(keys, prepared)
}

func (sm *SlabManager) prepareAppendMany(keys [][]byte, values [][]byte) (appendManyPrepared, error) {
	encodedValues := values
	encodedKeys := keys
	compressedFlags := make([]bool, len(values))
	fullCompressedFlags := make([]bool, len(values))
	omittedKeyFlags := make([]bool, len(values))
	if sm.compression.kind != CompressionNone {
		encodedValues = make([][]byte, len(values))
		encodedKeys = make([][]byte, len(values))
		for i := range values {
			// Try full record compression first
			if enc, ok, err := sm.compression.compressRecord(keys[i], values[i]); err == nil && ok {
				encodedValues[i] = enc
				encodedKeys[i] = nil
				compressedFlags[i] = true
				fullCompressedFlags[i] = true
			} else if err != nil {
				return appendManyPrepared{}, err
			} else {
				// Fall back to value-only compression
				encoded, compressed, err := sm.compression.compressValue(values[i])
				if err != nil {
					return appendManyPrepared{}, err
				}
				encodedValues[i] = encoded
				encodedKeys[i] = keys[i]
				compressedFlags[i] = compressed
			}
		}
	}

	if sm.omitSlabKeys {
		if sm.compression.kind == CompressionNone {
			encodedKeys = make([][]byte, len(values))
			copy(encodedKeys, keys)
		}
		for i := range values {
			if !fullCompressedFlags[i] {
				encodedKeys[i] = nil
				omittedKeyFlags[i] = true
			}
		}
	}

	prep := make([]appendManyPrep, len(keys))
	var lenArr [6]byte

	for i := 0; i < len(keys); i++ {
		key := encodedKeys[i]
		value := encodedValues[i]

		keyLen := len(key)
		valLen := len(value)

		if keyLen > int(^uint16(0)) || valLen > int(^uint32(0)) {
			return appendManyPrepared{}, ErrRecordTooLarge
		}
		if recordSizeExceedsMax(uint16(keyLen), uint32(valLen)) {
			return appendManyPrepared{}, ErrRecordTooLarge
		}
		recordLen64 := int64(HeaderSize) + int64(keyLen) + int64(valLen)
		if recordLen64 < 0 || recordLen64 > int64(int(recordLen64)) || recordLen64 > MaxSlabSize {
			return appendManyPrepared{}, ErrRecordTooLarge
		}
		recordLen := int(recordLen64)

		binary.LittleEndian.PutUint16(lenArr[0:2], uint16(keyLen))
		binary.LittleEndian.PutUint32(lenArr[2:6], uint32(valLen))
		sum := crc32.Update(0, crc32cTable, lenArr[:])
		sum = crc32.Update(sum, crc32cTable, key)
		sum = crc32.Update(sum, crc32cTable, value)

		prep[i] = appendManyPrep{
			keyLen:    keyLen,
			valLen:    valLen,
			recordLen: recordLen,
			crc:       sum,
		}
	}

	return appendManyPrepared{
		encodedKeys:       encodedKeys,
		encodedValues:     encodedValues,
		prep:              prep,
		compressedFlags:   compressedFlags,
		fullCompressed:    fullCompressedFlags,
		omittedKeyFlags:   omittedKeyFlags,
		appendManyStreams: sm.appendManyStreams,
	}, nil
}

func (sm *SlabManager) appendManySingle(keys [][]byte, prepared appendManyPrepared) ([]page.ValuePtr, error) {
	// Keep buffers bounded so we don't double memory usage for extremely large
	// batches or values.
	const maxBatchBytes = 8 << 20   // 8 MiB
	const maxKeepScratch = 16 << 20 // 16 MiB

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return nil, ErrReadOnly
	}

	ptrs := make([]page.ValuePtr, len(keys))

	buf := sm.appendManyScratch[:0]
	metas := make([]appendManyMeta, 0, min(len(keys), 1024))

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		s := sm.activeSlab
		id := s.ID

		for {
			base, err := s.WriteBatch(buf)
			if err == ErrSlabFull {
				// Should be rare (pre-checked), but handle if MaxSlabSize changed or
				// the slab was concurrently advanced by another writer.
				if err := sm.rotateLocked(); err != nil {
					return err
				}
				s = sm.activeSlab
				id = s.ID
				continue
			}
			if err != nil {
				return err
			}

			for _, meta := range metas {
				length := uint32(2 + 4 + meta.keyLen + meta.valLen)
				if meta.fullCompressed {
					length = page.ValuePtrMarkFullCompressed(length)
				} else if meta.omittedKey {
					length = page.ValuePtrMarkOmittedKey(length)
					if meta.compressed {
						length = page.ValuePtrMarkCompressed(length)
					}
				} else if meta.compressed {
					length = page.ValuePtrMarkCompressed(length)
				}
				ptrs[meta.idx] = page.ValuePtr{
					Offset: uint64(base + int64(meta.start) + 4),
					Length: length,
					FileID: id,
				}
			}

			buf = buf[:0]
			metas = metas[:0]
			return nil
		}
	}

	growBuf := func(target int) {
		if cap(buf) >= target {
			return
		}
		newCap := cap(buf) * 2
		if newCap < target {
			newCap = target
		}
		if newCap < 1024 {
			newCap = 1024
		}
		nb := make([]byte, len(buf), newCap)
		copy(nb, buf)
		buf = nb
	}

	for i := 0; i < len(keys); i++ {
		key := prepared.encodedKeys[i]
		value := prepared.encodedValues[i]
		recPrep := prepared.prep[i]
		keyLen := recPrep.keyLen
		valLen := recPrep.valLen
		recordLen := recPrep.recordLen

		// Ensure the record fits in the active slab, flushing/rotating as needed.
		if int64(sm.activeSlab.Size)+int64(len(buf))+int64(recordLen) > MaxSlabSize {
			if err := flush(); err != nil {
				return nil, err
			}
			if int64(sm.activeSlab.Size)+int64(recordLen) > MaxSlabSize {
				if err := sm.rotateLocked(); err != nil {
					return nil, err
				}
			}
		}

		// Bound the in-memory buffer size.
		if len(buf) >= maxBatchBytes {
			if err := flush(); err != nil {
				return nil, err
			}
		}

		start := len(buf)
		end := start + recordLen
		growBuf(end)
		buf = buf[:end]
		rec := buf[start:end]

		binary.LittleEndian.PutUint32(rec[0:4], recPrep.crc)
		binary.LittleEndian.PutUint16(rec[4:6], uint16(keyLen))
		binary.LittleEndian.PutUint32(rec[6:10], uint32(valLen))
		copy(rec[10:10+keyLen], key)
		copy(rec[10+keyLen:], value)

		metas = append(metas, appendManyMeta{
			idx:            i,
			start:          start,
			keyLen:         keyLen,
			valLen:         valLen,
			compressed:     prepared.compressedFlags[i],
			fullCompressed: prepared.fullCompressed[i],
			omittedKey:     prepared.omittedKeyFlags[i],
		})
	}

	if err := flush(); err != nil {
		return nil, err
	}

	if cap(buf) > maxKeepScratch {
		sm.appendManyScratch = nil
	} else {
		sm.appendManyScratch = buf[:0]
	}

	return ptrs, nil
}

func (sm *SlabManager) appendManyMulti(keys [][]byte, prepared appendManyPrepared) ([]page.ValuePtr, error) {
	// Keep buffers bounded so we don't double memory usage for extremely large
	// batches or values.
	const maxBatchBytes = 8 << 20   // 8 MiB
	const maxKeepScratch = 16 << 20 // 16 MiB

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return nil, ErrReadOnly
	}

	streams := prepared.appendManyStreams
	if streams > len(keys) {
		streams = len(keys)
	}

	ptrs := make([]page.ValuePtr, len(keys))
	buffers := sm.appendManyScratchStreams
	if len(buffers) < streams {
		buffers = make([][]byte, streams)
	}
	buffers = buffers[:streams]
	for i := range buffers {
		buffers[i] = buffers[i][:0]
	}
	metas := make([][]appendManyMeta, streams)

	totalPending := 0

	flushStream := func(stream int) error {
		if len(buffers[stream]) == 0 {
			return nil
		}
		s := sm.activeSlab
		id := s.ID

		for {
			base, err := s.WriteBatch(buffers[stream])
			if err == ErrSlabFull {
				if err := sm.rotateLocked(); err != nil {
					return err
				}
				s = sm.activeSlab
				id = s.ID
				continue
			}
			if err != nil {
				return err
			}

			for _, meta := range metas[stream] {
				length := uint32(2 + 4 + meta.keyLen + meta.valLen)
				if meta.fullCompressed {
					length = page.ValuePtrMarkFullCompressed(length)
				} else if meta.omittedKey {
					length = page.ValuePtrMarkOmittedKey(length)
					if meta.compressed {
						length = page.ValuePtrMarkCompressed(length)
					}
				} else if meta.compressed {
					length = page.ValuePtrMarkCompressed(length)
				}
				ptrs[meta.idx] = page.ValuePtr{
					Offset: uint64(base + int64(meta.start) + 4),
					Length: length,
					FileID: id,
				}
			}

			totalPending -= len(buffers[stream])
			buffers[stream] = buffers[stream][:0]
			metas[stream] = metas[stream][:0]
			return nil
		}
	}

	flushAll := func() error {
		for i := 0; i < streams; i++ {
			if err := flushStream(i); err != nil {
				return err
			}
		}
		return nil
	}

	growBuf := func(buf []byte, target int) []byte {
		if cap(buf) >= target {
			return buf
		}
		newCap := cap(buf) * 2
		if newCap < target {
			newCap = target
		}
		if newCap < 1024 {
			newCap = 1024
		}
		nb := make([]byte, len(buf), newCap)
		copy(nb, buf)
		return nb
	}

	for i := 0; i < len(keys); i++ {
		recPrep := prepared.prep[i]
		recordLen := recPrep.recordLen
		stream := i % streams

		// Ensure the record fits in the active slab, flushing/rotating as needed.
		if int64(sm.activeSlab.Size)+int64(totalPending)+int64(recordLen) > MaxSlabSize {
			if err := flushAll(); err != nil {
				return nil, err
			}
			if int64(sm.activeSlab.Size)+int64(recordLen) > MaxSlabSize {
				if err := sm.rotateLocked(); err != nil {
					return nil, err
				}
			}
		}

		// Bound the in-memory buffer size.
		if totalPending >= maxBatchBytes {
			if err := flushAll(); err != nil {
				return nil, err
			}
		}

		buf := buffers[stream]
		start := len(buf)
		end := start + recordLen
		buf = growBuf(buf, end)
		buf = buf[:end]
		rec := buf[start:end]

		binary.LittleEndian.PutUint32(rec[0:4], recPrep.crc)
		binary.LittleEndian.PutUint16(rec[4:6], uint16(recPrep.keyLen))
		binary.LittleEndian.PutUint32(rec[6:10], uint32(recPrep.valLen))
		copy(rec[10:10+recPrep.keyLen], prepared.encodedKeys[i])
		copy(rec[10+recPrep.keyLen:], prepared.encodedValues[i])

		metas[stream] = append(metas[stream], appendManyMeta{
			idx:            i,
			start:          start,
			keyLen:         recPrep.keyLen,
			valLen:         recPrep.valLen,
			compressed:     prepared.compressedFlags[i],
			fullCompressed: prepared.fullCompressed[i],
			omittedKey:     prepared.omittedKeyFlags[i],
		})

		buffers[stream] = buf
		totalPending += recordLen
	}

	if err := flushAll(); err != nil {
		return nil, err
	}

	for i := range buffers {
		if cap(buffers[i]) > maxKeepScratch {
			buffers[i] = nil
		} else {
			buffers[i] = buffers[i][:0]
		}
	}
	sm.appendManyScratchStreams = buffers

	return ptrs, nil
}

// Rotate forces creation of a new active slab.
func (sm *SlabManager) Rotate() (*SlabFile, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return nil, ErrReadOnly
	}
	if err := sm.rotateLocked(); err != nil {
		return nil, err
	}
	return sm.activeSlab, nil
}

func (sm *SlabManager) rotateLocked() error {
	if sm.readOnly {
		return ErrReadOnly
	}
	if err := sm.activeSlab.Sync(); err != nil {
		return err
	}

	newID := sm.activeSlab.ID + 1
	filename := fmt.Sprintf("data-%04d.slab", newID)
	path := filepath.Join(sm.dir, filename)

	newSlab, err := OpenSlab(path, newID)
	if err != nil {
		return err
	}

	sm.slabs[newID] = newSlab
	sm.activeSlab = newSlab

	// Ensure the directory entry is durable (best-effort).
	if dir, err := os.Open(sm.dir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	return nil
}

func (sm *SlabManager) Sync() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return ErrReadOnly
	}
	return sm.activeSlab.Sync()
}

func (sm *SlabManager) ActiveSlabID() uint32 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.activeSlab.ID
}

func (sm *SlabManager) ActiveSlabTail() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return uint64(sm.activeSlab.Size)
}

func (sm *SlabManager) SetActiveSlab(id uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	s, ok := sm.slabs[id]
	if !ok {
		path := filepath.Join(sm.dir, fmt.Sprintf("data-%04d.slab", id))
		var err error
		if sm.readOnly {
			s, err = OpenSlabReadOnly(path, id)
		} else {
			s, err = OpenSlab(path, id)
		}
		if err != nil {
			return err
		}
		sm.slabs[id] = s
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}
	sm.activeSlab = s
	return nil
}

func (sm *SlabManager) TruncateActiveSlab(offset uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return ErrReadOnly
	}
	if err := sm.activeSlab.ensureOpen(); err != nil {
		return err
	}
	return sm.activeSlab.Truncate(int64(offset))
}

// RepairActiveSlabTail scans and truncates any partial/corrupt tail records on
// the active slab (best-effort crash recovery).
func (sm *SlabManager) RepairActiveSlabTail() (uint64, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return 0, ErrReadOnly
	}
	if sm.activeSlab == nil {
		return 0, fmt.Errorf("no active slab")
	}
	if err := sm.activeSlab.ensureOpen(); err != nil {
		return 0, err
	}
	if err := sm.activeSlab.RepairTail(); err != nil {
		return 0, err
	}
	return uint64(sm.activeSlab.Size), nil
}

func (sm *SlabManager) PruneSlabs(maxID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return ErrReadOnly
	}

	for id, s := range sm.slabs {
		if id > maxID {
			if err := s.Close(); err != nil {
				return err
			}
			if err := os.Remove(s.Path); err != nil {
				return err
			}
			delete(sm.slabs, id)
		}
	}
	return nil
}

// ZombieCount returns the number of zombie slabs.
func (sm *SlabManager) ZombieCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	count := 0
	for _, s := range sm.slabs {
		if s.IsZombie.Load() {
			count++
		}
	}
	return count
}

// RemapStats returns cumulative mmap remap counts across slabs.
func (sm *SlabManager) RemapStats() (remaps uint64, deadMappings uint64) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, s := range sm.slabs {
		remaps += s.remapCount.Load()
		deadMappings += s.deadMappingsCount.Load()
	}
	return remaps, deadMappings
}

// CurrentSlabSet returns a snapshot of the current slabs.
func (sm *SlabManager) CurrentSlabSet() *SlabSet {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	files := make(map[uint32]*SlabFile, len(sm.slabs))
	for k, v := range sm.slabs {
		if v.IsZombie.Load() {
			continue
		}
		files[k] = v
		v.RefCount.Add(1) // Pin files for this Set
	}
	s := &SlabSet{
		Files:               files,
		disableReadChecksum: sm.disableReadChecksum,
		compression:         &sm.compression,
	}
	s.RefCount.Store(1) // Manager owns one reference
	return s
}
func (s *SlabSet) Read(ptr page.ValuePtr) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, fmt.Errorf("slab file %d not found in snapshot", ptr.FileID)
	}
	val, err := f.Read(int64(ptr.Offset), !s.disableReadChecksum)
	if err != nil {
		return nil, err
	}
	return decodeValue(ptr, val, s.compression)
}

func (s *SlabSet) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	f, ok := s.Files[ptr.FileID]
	if !ok {
		return nil, fmt.Errorf("slab file %d not found in snapshot", ptr.FileID)
	}
	val, err := f.ReadUnsafe(int64(ptr.Offset), !s.disableReadChecksum)
	if err != nil {
		return nil, err
	}
	return decodeValue(ptr, val, s.compression)
}

// AcquireSlabs increments the RefCount for the Set (O(1)).
func (sm *SlabManager) AcquireSlabs(set *SlabSet) {
	if set != nil {
		set.RefCount.Add(1)
	}
}

// ReleaseSlabs decrements the Set RefCount. If 0, unpins files and cleans zombies.
func (sm *SlabManager) ReleaseSlabs(set *SlabSet) error {
	if set == nil {
		return nil
	}

	// Decrement Set RefCount
	if set.RefCount.Add(-1) > 0 {
		return nil
	}

	// Set dropped to 0. Unpin files.
	var err error
	for _, s := range set.Files {
		newRef := s.RefCount.Add(-1)
		if newRef == 0 && s.IsZombie.Load() {
			sm.mu.Lock()
			if s.RefCount.Load() == 0 {
				if _, exists := sm.slabs[s.ID]; exists {
					if e := s.Close(); e != nil {
						err = e
					}
					if e := os.Remove(s.Path); e != nil {
						err = e
					}
					delete(sm.slabs, s.ID)
				}
			}
			sm.mu.Unlock()
		}
	}
	return err
}

// MarkZombie removes a slab from the active set so future snapshots stop
// pinning it. The file is deleted once all snapshots release it.
func (sm *SlabManager) MarkZombie(id uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.activeSlab != nil && sm.activeSlab.ID == id {
		return fmt.Errorf("cannot mark active slab %d as zombie", id)
	}
	s, ok := sm.slabs[id]
	if !ok {
		return fmt.Errorf("slab file %d not found", id)
	}
	s.IsZombie.Store(true)
	return nil
}

// RemoveSlab deletes a slab file from disk and unregisters it from the manager.
// It is only safe to call when the slab is not referenced by any snapshots.
func (sm *SlabManager) RemoveSlab(id uint32) error {
	sm.mu.Lock()
	if sm.activeSlab != nil && sm.activeSlab.ID == id {
		sm.mu.Unlock()
		return fmt.Errorf("cannot remove active slab %d", id)
	}
	s, ok := sm.slabs[id]
	if !ok {
		sm.mu.Unlock()
		return nil
	}
	if s.RefCount.Load() != 0 {
		sm.mu.Unlock()
		return fmt.Errorf("cannot remove slab %d: still pinned", id)
	}
	delete(sm.slabs, id)
	sm.mu.Unlock()

	_ = s.Close()
	if err := os.Remove(s.Path); err != nil {
		return err
	}
	return nil
}
