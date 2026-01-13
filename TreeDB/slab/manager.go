package slab

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
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
	activeCompression   compressionConfig
	currentProfile      atomic.Pointer[ActiveCompressionProfile]
	omitSlabKeys        bool
	compressionMetrics  compressionMetrics
	compressionTrainer  *compressionTrainer

	appendManyScratch         []byte
	compressionPauseRemaining atomic.Uint64
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
	sm := &SlabManager{
		dir:                dir,
		readOnly:           readOnly,
		slabs:              make(map[uint32]*SlabFile),
		compression:        compression,
		activeCompression:  compression, // Start with base compression
		omitSlabKeys:       opts.OmitSlabKeys,
		compressionMetrics: newCompressionMetrics(opts),
		compressionTrainer: newCompressionTrainer(opts, compression, readOnly),
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

		// Use V2 if a compression profile is ready (Adaptive) OR if ZSTD is enabled.
		if s.version == 0 && s.Size == 0 {
			profile, ok := sm.activeProfile()
			if ok && profile != nil || sm.compression.kind == CompressionZSTD {
				if err := sm.writeSlabV2Header(s, profile); err == nil {
					s.version = Version2
					if ok && profile != nil && len(profile.Dict) > 0 {
						s.globalDict = profile.Dict
						s.globalDecs = &sync.Pool{
							New: func() any {
								dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(profile.Dict))
								return dec
							},
						}
						// Ensure activeCompression is ZSTD if we have a dictionary.
						if sm.compression.kind != CompressionZSTD {
							sm.activeCompression, _ = normalizeCompressionOptions(CompressionOptions{Kind: CompressionZSTD})
						} else {
							sm.activeCompression = sm.compression
						}
						sm.activeCompression.zstdEncs = &sync.Pool{
							New: func() any {
								enc, _ := zstd.NewWriter(nil, zstd.WithEncoderDict(profile.Dict), zstd.WithEncoderLevel(sm.activeCompression.level))
								return enc
							},
						}
						sm.currentProfile.Store(profile)
					} else {
						// V2 but no initial dictionary (Zone 0 will be raw ZSTD).
						if sm.compression.kind != CompressionZSTD {
							sm.activeCompression, _ = normalizeCompressionOptions(CompressionOptions{Kind: CompressionZSTD})
						} else {
							sm.activeCompression = sm.compression
						}
						sm.currentProfile.Store(nil)
					}
				} else if err != ErrSlabFull && err != ErrRecordTooLarge {
					_ = sm.Close()
					return nil, err
				}
			}
		}
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
	if sm.compressionMetrics.enabled && sm.activeSlab != nil {
		sm.compressionMetrics.setSlab(sm.activeSlab.ID)
	}

	return sm, nil
}

func (sm *SlabManager) Compression() CompressionKind {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.compression.kind
}

func (sm *SlabManager) activeProfile() (*ActiveCompressionProfile, bool) {
	if sm == nil || sm.compressionTrainer == nil {
		if profile := sm.currentProfile.Load(); profile != nil {
			return profile, true
		}
		return nil, false
	}
	if profile, ok := sm.compressionTrainer.ActiveProfile(); ok && profile != nil {
		return profile, true
	}
	if profile := sm.currentProfile.Load(); profile != nil {
		return profile, true
	}
	return nil, false
}

func (sm *SlabManager) CompressionTrainConfig() (CompressionTrainConfig, bool) {
	sm.mu.RLock()
	trainer := sm.compressionTrainer
	sm.mu.RUnlock()
	if trainer == nil {
		return CompressionTrainConfig{}, false
	}
	return trainer.config(), true
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

func (sm *SlabManager) shouldCompress(rawLen int) bool {
	if rawLen <= 0 {
		return true
	}
	remaining := sm.compressionPauseRemaining.Load()
	for remaining > 0 {
		next := uint64(0)
		if uint64(rawLen) < remaining {
			next = remaining - uint64(rawLen)
		}
		if sm.compressionPauseRemaining.CompareAndSwap(remaining, next) {
			return false
		}
		remaining = sm.compressionPauseRemaining.Load()
	}
	return true
}

// EstimateCompression reports the raw/stored byte counts for a record if it were
// appended with the current compression settings. It does not write anything.
func (sm *SlabManager) EstimateCompression(key, value []byte) (rawLen int, storedLen int, err error) {
	rawLen = len(value)
	storedLen = len(value)
	if sm.compression.kind == CompressionNone || !sm.shouldCompress(len(value)) {
		return rawLen, storedLen, nil
	}

	if enc, ok, err := sm.compression.compressRecord(key, value); err != nil {
		return rawLen, storedLen, err
	} else if ok {
		rawLen = 2 + len(key) + len(value)
		return rawLen, len(enc), nil
	}

	enc, _, err := sm.compression.compressValue(value)
	if err != nil {
		return rawLen, storedLen, err
	}
	return rawLen, len(enc), nil
}

func decodeValue(ptr page.ValuePtr, val []byte, compression *compressionConfig) ([]byte, error) {
	if page.ValuePtrIsGrouped(ptr) {
		if compression == nil {
			return nil, errCompressedCorrupt
		}
		return decompressFrameGroup(compression, val, int(page.ValuePtrSubIndex(ptr)))
	}
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
	s, ok := sm.slabs[ptr.FileID]
	compression := &sm.compression
	sm.mu.RUnlock()

	if ok && s.version >= Version2 && int64(ptr.Offset) >= SlabV2DataStart {
		dec, put, err := s.GetDecoder(int64(ptr.Offset))
		if err != nil {
			return nil, nil, err
		}
		defer put()

		if len(val) < 4 {
			return nil, nil, errCompressedCorrupt
		}
		rawLen := binary.LittleEndian.Uint32(val[:4])
		payload := val[4:]

		decompressed, err := dec.DecodeAll(payload, make([]byte, 0, rawLen))
		if err != nil {
			return nil, nil, err
		}
		if len(decompressed) < 2 {
			return nil, nil, errCompressedCorrupt
		}
		keyLen := int(binary.LittleEndian.Uint16(decompressed[:2]))
		if len(decompressed) < 2+keyLen {
			return nil, nil, errCompressedCorrupt
		}
		return decompressed[2 : 2+keyLen], decompressed[2+keyLen:], nil
	}

	if compression == nil {
		return nil, nil, errCompressedCorrupt
	}
	return compression.decompressRecord(val)
}

func (sm *SlabManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.compressionMetrics.enabled && sm.activeSlab != nil {
		sm.compressionMetrics.finish("close")
	}
	if sm.compressionTrainer != nil {
		sm.compressionTrainer.Close()
	}
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

	if s.version >= Version2 && int64(ptr.Offset) >= SlabV2DataStart && page.ValuePtrIsCompressed(ptr) {
		dec, put, err := s.GetDecoder(int64(ptr.Offset))
		if err != nil {
			return nil, err
		}
		defer put()

		if page.ValuePtrIsGrouped(ptr) {
			return decompressFrameGroupWithDecoder(dec, val, int(page.ValuePtrSubIndex(ptr)))
		}

		if len(val) < 4 {
			return nil, errCompressedCorrupt
		}
		rawLen := binary.LittleEndian.Uint32(val[:4])
		payload := val[4:]

		if page.ValuePtrIsFullCompressed(ptr) {
			decompressed, err := dec.DecodeAll(payload, make([]byte, 0, rawLen))
			if err != nil {
				return nil, err
			}
			if len(decompressed) < 2 {
				return nil, errCompressedCorrupt
			}
			keyLen := binary.LittleEndian.Uint16(decompressed[:2])
			if len(decompressed) < 2+int(keyLen) {
				return nil, errCompressedCorrupt
			}
			return decompressed[2+keyLen:], nil
		}
		return dec.DecodeAll(payload, make([]byte, 0, rawLen))
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

	if s.version >= Version2 && int64(ptr.Offset) >= SlabV2DataStart && page.ValuePtrIsCompressed(ptr) {
		dec, put, err := s.GetDecoder(int64(ptr.Offset))
		if err != nil {
			return nil, err
		}
		defer put()

		if page.ValuePtrIsGrouped(ptr) {
			return decompressFrameGroupWithDecoder(dec, val, int(page.ValuePtrSubIndex(ptr)))
		}

		if len(val) < 4 {
			return nil, errCompressedCorrupt
		}
		rawLen := binary.LittleEndian.Uint32(val[:4])
		payload := val[4:]

		if page.ValuePtrIsFullCompressed(ptr) {
			decompressed, err := dec.DecodeAll(payload, make([]byte, 0, rawLen))
			if err != nil {
				return nil, err
			}
			if len(decompressed) < 2 {
				return nil, errCompressedCorrupt
			}
			keyLen := binary.LittleEndian.Uint16(decompressed[:2])
			if len(decompressed) < 2+int(keyLen) {
				return nil, errCompressedCorrupt
			}
			return decompressed[2+keyLen:], nil
		}
		return dec.DecodeAll(payload, make([]byte, 0, rawLen))
	}

	return decodeValue(ptr, val, compression)
}

type AppendOptions struct {
	DisableCompression bool
	SkipTraining       bool
	SkipMetrics        bool
}

func (sm *SlabManager) Append(key, value []byte) (page.ValuePtr, error) {
	return sm.AppendWithOptions(key, value, AppendOptions{})
}

func (sm *SlabManager) AppendWithOptions(key, value []byte, opts AppendOptions) (page.ValuePtr, error) {
	return sm.appendWithOptions(key, value, opts)
}

func (sm *SlabManager) appendWithOptions(key, value []byte, opts AppendOptions) (page.ValuePtr, error) {
	encoded := value
	encodedKey := key
	compressed := false
	fullCompressed := false
	omittedKey := false
	var err error
	if !opts.SkipTraining && sm.compressionTrainer != nil && sm.compressionTrainer.shouldCollect() {
		sm.compressionTrainer.collect(value)
	}
	if !opts.DisableCompression && sm.activeCompression.kind != CompressionNone && sm.shouldCompress(len(value)) {
		// Try full record compression first
		if enc, ok, errInner := sm.activeCompression.compressRecord(key, value); errInner == nil && ok {
			encoded = enc
			encodedKey = nil
			compressed = true
			fullCompressed = true
		} else if errInner != nil {
			return page.ValuePtr{}, errInner
		} else {
			// Fall back to value-only compression
			encoded, compressed, errInner = sm.activeCompression.compressValue(value)
			if errInner != nil {
				return page.ValuePtr{}, errInner
			}
		}
	}

	if !fullCompressed && sm.omitSlabKeys {
		encodedKey = nil
		omittedKey = true
	}

	rawLen := len(value)
	if fullCompressed {
		rawLen = 2 + len(key) + len(value)
	}
	storedLen := len(encoded)

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return page.ValuePtr{}, ErrReadOnly
	}

	// Pre-check for V2 absolute size limit.
	if sm.activeSlab.version >= Version2 {
		if int64(HeaderSize+len(encodedKey)+len(encoded)) > maxV2RecordSize {
			return page.ValuePtr{}, ErrRecordTooLarge
		}
	}

	var offset int64
	for attempt := 0; attempt < 3; attempt++ {
		offset, err = sm.activeSlab.Write(encodedKey, encoded)
		if err == ErrSlabFull {
			if err = sm.rotateLocked(); err != nil {
				return page.ValuePtr{}, err
			}
			continue
		}
		if err == ErrRecordTooLarge && sm.activeSlab.version >= Version2 {
			// Zonal rotation requested.
			if _, err = sm.maybeRotateZoneLocked(len(encoded) + len(encodedKey) + HeaderSize); err != nil {
				return page.ValuePtr{}, err
			}
			continue
		}
		break
	}

	if err != nil {
		return page.ValuePtr{}, err
	}

	if !opts.SkipMetrics && !opts.DisableCompression && sm.compressionMetrics.enabled && sm.activeCompression.kind != CompressionNone {
		compressedCount := 0
		fullCount := 0
		if compressed {
			compressedCount = 1
		}
		if fullCompressed {
			fullCount = 1
		}
		if pauseBytes := sm.compressionMetrics.add(sm.activeSlab.ID, rawLen, storedLen, 1, compressedCount, fullCount); pauseBytes > 0 {
			sm.compressionPauseRemaining.Store(pauseBytes)
			if sm.compressionTrainer != nil {
				sm.compressionTrainer.signalDegraded(sm.activeSlab.ID)
			}
		}
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
	rawLen         int
	storedLen      int
	compressed     bool
	fullCompressed bool
	omittedKey     bool
}

// appendManyGrouped writes grouped frame records with K>1 values per frame.
// Thread-safety: This is serialized by the SlabManager mutex.
func (sm *SlabManager) appendManyGrouped(keys [][]byte, values [][]byte, k int) ([]page.ValuePtr, error) {
	if k <= 1 || len(values) == 0 {
		return sm.appendWithOptionsMany(keys, values)
	}
	ptrs := make([]page.ValuePtr, len(values))

	sm.mu.Lock()
	defer sm.mu.Unlock()

	writeRecord := func(rec []byte) (int64, error) {
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			var offset int64
			offset, err = sm.activeSlab.WriteBatch(rec, false)
			if err == ErrSlabFull {
				if err = sm.rotateLocked(); err != nil {
					return 0, err
				}
				continue
			}
			if err == ErrRecordTooLarge && sm.activeSlab.version >= Version2 {
				if _, err = sm.maybeRotateZoneLocked(len(rec)); err != nil {
					return 0, err
				}
				continue
			}
			return offset, err
		}
		return 0, err
	}

	idx := 0
	for idx < len(values) {
		end := idx + k
		if end > len(values) {
			end = len(values)
		}
		group := values[idx:end]

		record, actualK, err := buildFrameGroupRecord(group, &sm.activeCompression)
		if err != nil {
			return nil, err
		}

		if sm.activeSlab.version >= Version2 {
			if int64(len(record)) > maxV2RecordSize {
				return nil, ErrRecordTooLarge
			}
		}

		offset, err := writeRecord(record)
		if err != nil {
			return nil, err
		}
		length := uint32(len(record) - 4) // exclude CRC
		length = page.ValuePtrMarkCompressed(length)
		if sm.omitSlabKeys {
			length = page.ValuePtrMarkOmittedKey(length)
		}
		for j := 0; j < actualK && idx+j < len(ptrs); j++ {
			ptrs[idx+j] = page.ValuePtr{
				Offset: uint64(offset + 4),
				Length: page.ValuePtrMarkGrouped(length, uint8(j)),
				FileID: sm.activeSlab.ID,
			}
		}
		idx = end
	}
	return ptrs, nil
}

// appendWithOptionsMany is the legacy AppendMany path (K=1).
// Thread-safety: This is serialized by the SlabManager mutex.
func (sm *SlabManager) appendWithOptionsMany(keys [][]byte, values [][]byte) ([]page.ValuePtr, error) {
	if len(keys) != len(values) {
		return nil, fmt.Errorf("AppendMany: keys/values length mismatch (%d != %d)", len(keys), len(values))
	}
	if len(keys) == 0 {
		return nil, nil
	}

	// Keep buffers bounded so we don't double memory usage for extremely large
	// batches or values.
	const defaultMaxBatchBytes = 8 << 20 // 8 MiB
	const maxKeepScratch = 16 << 20      // 16 MiB

	type appendManyPrep struct {
		keyLen    int
		valLen    int
		recordLen int
		crc       uint32
	}

	encodedValues := values
	encodedKeys := keys
	compressedFlags := make([]bool, len(values))
	fullCompressedFlags := make([]bool, len(values))
	omittedKeyFlags := make([]bool, len(values))
	rawLens := make([]int, len(values))
	storedLens := make([]int, len(values))
	if sm.activeCompression.kind != CompressionNone {
		encodedValues = make([][]byte, len(values))
		encodedKeys = make([][]byte, len(values))
		for i := range values {
			if !sm.shouldCompress(len(values[i])) {
				encodedValues[i] = values[i]
				encodedKeys[i] = keys[i]
				continue
			}
			// Try full record compression first
			if enc, ok, err := sm.activeCompression.compressRecord(keys[i], values[i]); err == nil && ok {
				encodedValues[i] = enc
				encodedKeys[i] = nil
				compressedFlags[i] = true
				fullCompressedFlags[i] = true
			} else if err != nil {
				return nil, err
			} else {
				// Fall back to value-only compression
				var encoded []byte
				var compressed bool
				var err error
				encoded, compressed, err = sm.activeCompression.compressValue(values[i])
				if err != nil {
					return nil, err
				}
				encodedValues[i] = encoded
				encodedKeys[i] = keys[i]
				compressedFlags[i] = compressed
			}
		}
	}

	if sm.omitSlabKeys {
		if sm.activeCompression.kind == CompressionNone {
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
			return nil, ErrRecordTooLarge
		}
		if recordSizeExceedsMax(uint16(keyLen), uint32(valLen)) {
			return nil, ErrRecordTooLarge
		}
		recordLen64 := int64(HeaderSize) + int64(keyLen) + int64(valLen)
		if recordLen64 < 0 || recordLen64 > int64(int(recordLen64)) || recordLen64 > MaxSlabSize {
			return nil, ErrRecordTooLarge
		}
		if sm.activeSlab.version >= Version2 {
			if recordLen64 > maxV2RecordSize {
				return nil, fmt.Errorf("record too large (v2 record=%d max=%d key=%d val=%d): %w", recordLen64, maxV2RecordSize, keyLen, valLen, ErrRecordTooLarge)
			}
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

		rawLen := len(values[i])
		if fullCompressedFlags[i] {
			rawLen = 2 + len(keys[i]) + len(values[i])
		}
		rawLens[i] = rawLen
		storedLens[i] = len(value)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return nil, ErrReadOnly
	}

	ptrs := make([]page.ValuePtr, len(keys))

	maxBatchBytes := defaultMaxBatchBytes
	if sm.activeSlab.version >= Version2 {
		if maxBatchBytes > maxV2RecordSize {
			maxBatchBytes = maxV2RecordSize
		}
	}

	buf := sm.appendManyScratch[:0]
	metas := make([]appendManyMeta, 0, min(len(keys), 1024))

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		var err error
		s := sm.activeSlab
		id := s.ID

		for attempt := 0; attempt < 3; attempt++ {
			var base int64
			base, err = s.WriteBatch(buf, false)
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
			if err == ErrRecordTooLarge && s.version >= Version2 {
				// Zonal rotation requested.
				// We pass len(buf) as a conservative estimate for rotation.
				if _, err := sm.maybeRotateZoneLocked(len(buf)); err != nil {
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

			if sm.compressionMetrics.enabled && sm.activeCompression.kind != CompressionNone && len(metas) > 0 {
				rawTotal := 0
				storedTotal := 0
				compressedCount := 0
				fullCount := 0
				for _, meta := range metas {
					rawTotal += meta.rawLen
					storedTotal += meta.storedLen
					if meta.compressed {
						compressedCount++
					}
					if meta.fullCompressed {
						fullCount++
					}
				}
				if pauseBytes := sm.compressionMetrics.add(id, rawTotal, storedTotal, len(metas), compressedCount, fullCount); pauseBytes > 0 {
					sm.compressionPauseRemaining.Store(pauseBytes)
					if sm.compressionTrainer != nil {
						sm.compressionTrainer.signalDegraded(id)
					}
				}
			}

			buf = buf[:0]
			metas = metas[:0]
			return nil
		}
		return err
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
		key := keys[i]
		value := encodedValues[i]
		recPrep := prep[i]
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

		// For V2 slabs, avoid crossing zone boundaries with the batch buffer.
		if sm.activeSlab.version >= Version2 {
			nextBoundary := ((sm.activeSlab.Size / ZoneSize) + 1) * ZoneSize
			remaining := nextBoundary - (sm.activeSlab.Size + int64(len(buf)))
			if remaining < int64(recordLen) {
				if err := flush(); err != nil {
					return nil, err
				}
				if sm.activeSlab.version >= Version2 {
					nextBoundary = ((sm.activeSlab.Size / ZoneSize) + 1) * ZoneSize
					if sm.activeSlab.Size+int64(recordLen) > nextBoundary {
						if _, err := sm.maybeRotateZoneLocked(recordLen); err != nil {
							return nil, err
						}
					}
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
			rawLen:         rawLens[i],
			storedLen:      storedLens[i],
			compressed:     compressedFlags[i],
			fullCompressed: fullCompressedFlags[i],
			omittedKey:     omittedKeyFlags[i],
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

// AppendMany dispatches between grouped writes (K>1) and legacy per-row writes.
// Thread-safety: This is serialized by the SlabManager mutex.
func (sm *SlabManager) AppendMany(keys [][]byte, values [][]byte) ([]page.ValuePtr, error) {
	if len(keys) != len(values) {
		return nil, fmt.Errorf("AppendMany: keys/values length mismatch (%d != %d)", len(keys), len(values))
	}
	if len(keys) == 0 {
		return nil, nil
	}

	groupK := 1
	if profile := sm.currentProfile.Load(); profile != nil && profile.K > 1 && sm.activeCompression.kind == CompressionZSTD && sm.omitSlabKeys {
		groupK = profile.K
	}
	if sm.compressionTrainer != nil && sm.compressionTrainer.shouldCollect() {
		for i := range values {
			if !sm.compressionTrainer.shouldCollect() {
				break
			}
			sm.compressionTrainer.collect(values[i])
		}
	}

	if groupK > 1 {
		return sm.appendManyGrouped(keys, values, groupK)
	}
	return sm.appendWithOptionsMany(keys, values)
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

func (sm *SlabManager) writeSlabV2Header(s *SlabFile, profile *ActiveCompressionProfile) error {
	header := make([]byte, SlabV2DataStart)
	copy(header[0:8], MagicV2)
	header[8] = Version2
	if profile != nil && len(profile.Dict) > 0 {
		copy(header[FileHeaderSizeV2:], profile.Dict)
	}
	// Use WriteBatch with ignoreBoundary=true to correctly update size/offset and handle seek.
	_, err := s.WriteBatch(header, true)
	return err
}

func (sm *SlabManager) maybeRotateZoneLocked(recordLen int) (bool, error) {
	s := sm.activeSlab
	if s == nil || s.version < Version2 {
		return false, nil
	}

	// 1. Are we EXACTLY at a boundary (Zone 1+)?
	if s.Size >= ZoneSize && s.Size%ZoneSize == 0 {
		if err := sm.forceRotateZoneLocked(); err != nil {
			return false, err
		}
		return true, nil
	}

	// 2. Will the record cross a boundary?
	nextBoundary := ((s.Size / ZoneSize) + 1) * ZoneSize
	if s.Size+int64(recordLen) <= nextBoundary {
		return false, nil
	}

	// Crossing boundary. Pad to boundary.
	if err := s.Truncate(nextBoundary); err != nil {
		return false, err
	}
	// After padding, we are EXACTLY at the boundary. Write header.
	if err := sm.forceRotateZoneLocked(); err != nil {
		return false, err
	}
	return true, nil
}

const recentDictWindow = 3

func (s *SlabFile) lookupDictRef(hash uint64) (dictRefEntry, bool) {
	if s.recentDicts == nil {
		return dictRefEntry{}, false
	}
	entry, ok := s.recentDicts[hash]
	return entry, ok
}

func (s *SlabFile) recordDictRef(hash uint64, zoneID uint32, crc uint32) {
	if s.recentDicts == nil {
		s.recentDicts = make(map[uint64]dictRefEntry, recentDictWindow)
	}
	if _, ok := s.recentDicts[hash]; ok {
		for i, h := range s.recentDictOrder {
			if h == hash {
				copy(s.recentDictOrder[i:], s.recentDictOrder[i+1:])
				s.recentDictOrder = s.recentDictOrder[:len(s.recentDictOrder)-1]
				break
			}
		}
	}
	s.recentDicts[hash] = dictRefEntry{zoneID: zoneID, crc: crc}
	s.recentDictOrder = append(s.recentDictOrder, hash)
	if len(s.recentDictOrder) > recentDictWindow {
		evict := s.recentDictOrder[0]
		s.recentDictOrder = s.recentDictOrder[1:]
		delete(s.recentDicts, evict)
	}
}

func (sm *SlabManager) forceRotateZoneLocked() error {
	s := sm.activeSlab
	// Decide dictionary for new zone.
	profile, ok := sm.activeProfile()
	dictType := ZoneDictGlobal
	var localDict []byte
	var refEntry dictRefEntry
	var dictHash uint64

	if ok && profile != nil && len(profile.Dict) > 0 {
		dictHash = profile.DictHash
		if dictHash == 0 {
			dictHash = xxhash.Sum64(profile.Dict)
		}
		// Use LOCAL if:
		// 1. Global dict is empty.
		// 2. Profile is different/better than global dict (Adaptive).
		if len(s.globalDict) == 0 || !bytes.Equal(profile.Dict, s.globalDict) {
			if entry, ok := s.lookupDictRef(dictHash); ok {
				dictType = ZoneDictRef
				refEntry = entry
			} else {
				dictType = ZoneDictLocal
				localDict = profile.Dict
			}
		}
	}

	// Write Zone Header.
	zh := ZoneHeader{
		Magic:    ZoneHeaderMagic,
		DictType: uint8(dictType),
	}
	if dictType == ZoneDictLocal {
		if len(localDict) > GlobalDictSize {
			localDict = localDict[:GlobalDictSize]
		}
		dictBuf := make([]byte, GlobalDictSize)
		copy(dictBuf, localDict)
		localDict = dictBuf
		zh.DictCRC = crc32.Checksum(localDict, crc32cTable)
		zh.DictLength = uint32(len(localDict))
	} else if dictType == ZoneDictRef {
		zh.DictCRC = refEntry.crc
		zh.DictLength = refEntry.zoneID
	}

	if _, err := s.WriteBatch(zh.Marshal(), true); err != nil {
		return err
	}

	if dictType == ZoneDictLocal {
		// Dictionary size must be padded to GlobalDictSize (32KB) for alignment if we want
		// predictable zone data starts, but the spec says "immediately following".
		// We'll write exactly GlobalDictSize to keep everything aligned to 2MB zones + 32KB dicts.
		if _, err := s.WriteBatch(localDict, true); err != nil {
			return err
		}

		// Update manager's active compression to use the new local dict.
		sm.activeCompression = sm.compression // shallow copy base settings
		sm.activeCompression.zstdEncs = &sync.Pool{
			New: func() any {
				enc, _ := zstd.NewWriter(nil, zstd.WithEncoderDict(localDict), zstd.WithEncoderLevel(sm.compression.level))
				return enc
			},
		}
		sm.activeCompression.zstdDecs = &sync.Pool{
			New: func() any {
				dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(localDict))
				return dec
			},
		}
		sm.currentProfile.Store(profile)
		zoneID := uint32(s.Size / ZoneSize)
		s.recordDictRef(dictHash, zoneID, zh.DictCRC)
	} else if dictType == ZoneDictRef {
		// Keep using the current dict for compression but don't rewrite it.
		sm.activeCompression = sm.compression
		sm.activeCompression.zstdEncs = &sync.Pool{
			New: func() any {
				enc, _ := zstd.NewWriter(nil, zstd.WithEncoderDict(profile.Dict), zstd.WithEncoderLevel(sm.compression.level))
				return enc
			},
		}
		sm.activeCompression.zstdDecs = &sync.Pool{
			New: func() any {
				dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(profile.Dict))
				return dec
			},
		}
		sm.currentProfile.Store(profile)
	} else {
		// USE_GLOBAL.
		// If we were using a local dict, revert to global (or base if global is empty).
		if len(s.globalDict) > 0 {
			sm.activeCompression = sm.compression
			sm.activeCompression.zstdEncs = &sync.Pool{
				New: func() any {
					enc, _ := zstd.NewWriter(nil, zstd.WithEncoderDict(s.globalDict), zstd.WithEncoderLevel(sm.compression.level))
					return enc
				},
			}
			sm.activeCompression.zstdDecs = &sync.Pool{
				New: func() any {
					dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(s.globalDict))
					return dec
				},
			}
		} else {
			// Global is also empty. Revert to base (no dict).
			sm.activeCompression = sm.compression
			sm.currentProfile.Store(nil)
		}
	}

	return nil
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

	// Use V2 if a compression profile is ready (Adaptive) OR if ZSTD is enabled.
	profile, ok := sm.activeProfile()
	if ok && profile != nil || sm.compression.kind == CompressionZSTD {
		if err := sm.writeSlabV2Header(newSlab, profile); err == nil {
			newSlab.version = Version2
			if ok && profile != nil && len(profile.Dict) > 0 {
				newSlab.globalDict = profile.Dict
				newSlab.globalDecs = &sync.Pool{
					New: func() any {
						dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(profile.Dict))
						return dec
					},
				}
				// Ensure activeCompression is ZSTD if we have a dictionary.
				if sm.compression.kind != CompressionZSTD {
					sm.activeCompression, _ = normalizeCompressionOptions(CompressionOptions{Kind: CompressionZSTD})
				} else {
					sm.activeCompression = sm.compression
				}
				sm.activeCompression.zstdEncs = &sync.Pool{
					New: func() any {
						enc, _ := zstd.NewWriter(nil, zstd.WithEncoderDict(profile.Dict), zstd.WithEncoderLevel(sm.activeCompression.level))
						return enc
					},
				}
				sm.currentProfile.Store(profile)
			} else {
				// V2 but no initial dictionary (Zone 0 will be raw ZSTD).
				if sm.compression.kind != CompressionZSTD {
					sm.activeCompression, _ = normalizeCompressionOptions(CompressionOptions{Kind: CompressionZSTD})
				} else {
					sm.activeCompression = sm.compression
				}
				sm.currentProfile.Store(nil)
			}
		} else if err == ErrSlabFull || err == ErrRecordTooLarge {
			// MaxSlabSize is too small for V2 header (common in rotation tests).
			// Fallback to V1.
			sm.activeCompression = sm.compression
			sm.currentProfile.Store(nil)
		} else {
			_ = newSlab.Close()
			return err
		}
	} else {
		// Fallback to V1.
		sm.activeCompression = sm.compression
		sm.currentProfile.Store(nil)
	}

	if sm.compressionMetrics.enabled {
		sm.compressionMetrics.finish("rotate")
	}
	sm.slabs[newID] = newSlab
	sm.activeSlab = newSlab
	if sm.compressionMetrics.enabled {
		sm.compressionMetrics.reset(newID)
	}

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
	if sm.compressionMetrics.enabled {
		sm.compressionMetrics.reset(s.ID)
	}
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

func (sm *SlabManager) CompressionTrainerStats() (CompressionTrainerStats, bool) {
	if sm.compressionTrainer == nil {
		return CompressionTrainerStats{}, false
	}
	stats := sm.compressionTrainer.stats()
	if !stats.Enabled {
		return CompressionTrainerStats{}, false
	}
	return stats, true
}

// ForceTrainerCollecting forces the compression trainer into collecting mode
// (best-effort; no-op if trainer is nil or disabled). Intended for diagnostics.
func (sm *SlabManager) ForceTrainerCollecting() {
	if sm.compressionTrainer == nil || !sm.compressionTrainer.enabled.Load() {
		return
	}
	sm.compressionTrainer.collecting.Store(true)
}

// ForceAcceptProfileForTesting immediately installs a compression profile.
// Internal use for deterministic end-to-end tests only.
func (sm *SlabManager) ForceAcceptProfileForTesting(p *ActiveCompressionProfile) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.currentProfile.Store(p)
	// Ensure activeCompression is ZSTD if we have a dictionary.
	if sm.compression.kind != CompressionZSTD {
		sm.activeCompression, _ = normalizeCompressionOptions(CompressionOptions{Kind: CompressionZSTD})
	} else {
		sm.activeCompression = sm.compression
	}

	dict := p.Dict
	if len(dict) == 0 {
		sm.activeCompression.zstdEncs = &sync.Pool{
			New: func() any {
				enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(sm.activeCompression.level), zstd.WithEncoderCRC(false))
				return enc
			},
		}
		sm.activeCompression.zstdDecs = &sync.Pool{
			New: func() any {
				dec, _ := zstd.NewReader(nil)
				return dec
			},
		}
		return
	}
	sm.activeCompression.zstdEncs = &sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(sm.activeCompression.level))
			if err != nil {
				enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(sm.activeCompression.level), zstd.WithEncoderCRC(false))
			}
			return enc
		},
	}
	sm.activeCompression.zstdDecs = &sync.Pool{
		New: func() any {
			dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
			if err != nil {
				dec, _ = zstd.NewReader(nil)
			}
			return dec
		},
	}
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

	if f.version >= Version2 && int64(ptr.Offset) >= SlabV2DataStart && page.ValuePtrIsCompressed(ptr) {
		dec, put, err := f.GetDecoder(int64(ptr.Offset))
		if err != nil {
			return nil, err
		}
		defer put()

		if page.ValuePtrIsGrouped(ptr) {
			return decompressFrameGroupWithDecoder(dec, val, int(page.ValuePtrSubIndex(ptr)))
		}

		if len(val) < 4 {
			return nil, errCompressedCorrupt
		}
		rawLen := binary.LittleEndian.Uint32(val[:4])
		payload := val[4:]

		if page.ValuePtrIsFullCompressed(ptr) {
			decompressed, err := dec.DecodeAll(payload, make([]byte, 0, rawLen))
			if err != nil {
				return nil, err
			}
			if len(decompressed) < 2 {
				return nil, errCompressedCorrupt
			}
			keyLen := binary.LittleEndian.Uint16(decompressed[:2])
			if len(decompressed) < 2+int(keyLen) {
				return nil, errCompressedCorrupt
			}
			return decompressed[2+keyLen:], nil
		}
		return dec.DecodeAll(payload, make([]byte, 0, rawLen))
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

	if f.version >= Version2 && int64(ptr.Offset) >= SlabV2DataStart && page.ValuePtrIsCompressed(ptr) {
		dec, put, err := f.GetDecoder(int64(ptr.Offset))
		if err != nil {
			return nil, err
		}
		defer put()

		if page.ValuePtrIsGrouped(ptr) {
			return decompressFrameGroupWithDecoder(dec, val, int(page.ValuePtrSubIndex(ptr)))
		}

		if len(val) < 4 {
			return nil, errCompressedCorrupt
		}
		rawLen := binary.LittleEndian.Uint32(val[:4])
		payload := val[4:]

		if page.ValuePtrIsFullCompressed(ptr) {
			decompressed, err := dec.DecodeAll(payload, make([]byte, 0, rawLen))
			if err != nil {
				return nil, err
			}
			if len(decompressed) < 2 {
				return nil, errCompressedCorrupt
			}
			keyLen := binary.LittleEndian.Uint16(decompressed[:2])
			if len(decompressed) < 2+int(keyLen) {
				return nil, errCompressedCorrupt
			}
			return decompressed[2+keyLen:], nil
		}
		return dec.DecodeAll(payload, make([]byte, 0, rawLen))
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
