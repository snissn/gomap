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

func decodeValue(ptr page.ValuePtr, val []byte, compression *compressionConfig, decPool *sync.Pool) ([]byte, error) {
	if !page.ValuePtrIsCompressed(ptr) {
		return val, nil
	}
	if compression == nil {
		return nil, errCompressedCorrupt
	}
	useDict := page.ValuePtrIsDictCompressed(ptr) && decPool != nil
	if page.ValuePtrIsFullCompressed(ptr) {
		if useDict {
			_, v, err := compression.decompressRecordWithPool(val, decPool)
			return v, err
		}
		_, v, err := compression.decompressRecord(val)
		return v, err
	}
	if useDict {
		return compression.decompressValueWithPool(val, decPool)
	}
	return compression.decompressValue(val)
}

func (sm *SlabManager) DecodeValueForCompactor(ptr page.ValuePtr, val []byte) ([]byte, error) {
	sm.mu.RLock()
	s, ok := sm.slabs[ptr.FileID]
	compression := &sm.compression
	var decPool *sync.Pool
	if ok {
		_, decPool = s.dictPools(compression)
	}
	sm.mu.RUnlock()
	return decodeValue(ptr, val, compression, decPool)
}

func (sm *SlabManager) DecodeRecordForCompactor(ptr page.ValuePtr, val []byte) ([]byte, []byte, error) {
	if !page.ValuePtrIsFullCompressed(ptr) {
		return nil, nil, fmt.Errorf("not a full compressed record")
	}
	sm.mu.RLock()
	s, ok := sm.slabs[ptr.FileID]
	compression := &sm.compression
	var decPool *sync.Pool
	if ok {
		_, decPool = s.dictPools(compression)
	}
	sm.mu.RUnlock()
	if compression == nil {
		return nil, nil, errCompressedCorrupt
	}
	if decPool != nil {
		return compression.decompressRecordWithPool(val, decPool)
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
	_, decPool := s.dictPools(compression)
	return decodeValue(ptr, val, compression, decPool)
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
	_, decPool := s.dictPools(compression)
	return decodeValue(ptr, val, compression, decPool)
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
	sm.mu.RLock()
	useDict := !opts.DisableCompression &&
		sm.compression.kind == CompressionZSTD &&
		sm.activeSlab != nil &&
		sm.activeSlab.version == slabVersionV2 &&
		sm.shouldCompress(len(value))
	dictReady := useDict && sm.activeSlab.dictReady.Load()
	sm.mu.RUnlock()

	if useDict && (dictReady || !opts.SkipTraining) {
		return sm.appendWithDict(key, value, opts)
	}
	return sm.appendWithOptionsNoDict(key, value, opts)
}

func (sm *SlabManager) appendWithOptionsNoDict(key, value []byte, opts AppendOptions) (page.ValuePtr, error) {
	encoded := value
	encodedKey := key
	compressed := false
	fullCompressed := false
	omittedKey := false
	var err error
	if !opts.SkipTraining && sm.compressionTrainer != nil && sm.compressionTrainer.shouldCollect() {
		sm.compressionTrainer.collect(value)
	}
	if !opts.DisableCompression && sm.compression.kind != CompressionNone && sm.shouldCompress(len(value)) {
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

	if !opts.SkipMetrics && !opts.DisableCompression && sm.compressionMetrics.enabled && sm.compression.kind != CompressionNone {
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

func (sm *SlabManager) appendWithDict(key, value []byte, opts AppendOptions) (page.ValuePtr, error) {
	if !opts.SkipTraining && sm.compressionTrainer != nil && sm.compressionTrainer.shouldCollect() {
		sm.compressionTrainer.collect(value)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return page.ValuePtr{}, ErrReadOnly
	}

	s := sm.activeSlab
	if s == nil {
		return page.ValuePtr{}, ErrReadOnly
	}

	if !opts.SkipTraining {
		if err := s.initDictFromSample(&sm.compression, value); err != nil {
			return page.ValuePtr{}, err
		}
	}

	encoded := value
	encodedKey := key
	compressed := false
	dictCompressed := false
	omittedKey := false

	encPool, _ := s.dictPools(&sm.compression)
	if encPool != nil {
		var err error
		encoded, dictCompressed, err = sm.compression.compressValueWithPool(value, encPool)
		if err != nil {
			return page.ValuePtr{}, err
		}
		compressed = dictCompressed
	}

	if !dictCompressed && !opts.DisableCompression && sm.compression.kind != CompressionNone && sm.shouldCompress(len(value)) {
		var err error
		encoded, compressed, err = sm.compression.compressValue(value)
		if err != nil {
			return page.ValuePtr{}, err
		}
	}

	if sm.omitSlabKeys {
		encodedKey = nil
		omittedKey = true
	}

	keyLen := len(encodedKey)
	valLen := len(encoded)
	if keyLen > int(^uint16(0)) || valLen > int(^uint32(0)) {
		return page.ValuePtr{}, ErrRecordTooLarge
	}
	if recordSizeExceedsMax(uint16(keyLen), uint32(valLen)) {
		return page.ValuePtr{}, ErrRecordTooLarge
	}

	offset, err := s.Write(encodedKey, encoded)
	if err == ErrSlabFull {
		if err := sm.rotateLocked(); err != nil {
			return page.ValuePtr{}, err
		}
		s = sm.activeSlab
		if s == nil {
			return page.ValuePtr{}, ErrReadOnly
		}
		if !opts.SkipTraining {
			if err := s.initDictFromSample(&sm.compression, value); err != nil {
				return page.ValuePtr{}, err
			}
		}
		encPool, _ = s.dictPools(&sm.compression)
		if encPool != nil {
			encoded, dictCompressed, err = sm.compression.compressValueWithPool(value, encPool)
			if err != nil {
				return page.ValuePtr{}, err
			}
			compressed = dictCompressed
		} else {
			dictCompressed = false
		}
		if !dictCompressed && !opts.DisableCompression && sm.compression.kind != CompressionNone && sm.shouldCompress(len(value)) {
			encoded, compressed, err = sm.compression.compressValue(value)
			if err != nil {
				return page.ValuePtr{}, err
			}
		}
		encodedKey = key
		omittedKey = false
		if sm.omitSlabKeys {
			encodedKey = nil
			omittedKey = true
		}
		keyLen = len(encodedKey)
		valLen = len(encoded)
		if keyLen > int(^uint16(0)) || valLen > int(^uint32(0)) {
			return page.ValuePtr{}, ErrRecordTooLarge
		}
		if recordSizeExceedsMax(uint16(keyLen), uint32(valLen)) {
			return page.ValuePtr{}, ErrRecordTooLarge
		}
		offset, err = s.Write(encodedKey, encoded)
	}

	if err != nil {
		return page.ValuePtr{}, err
	}

	if !opts.SkipMetrics && !opts.DisableCompression && sm.compressionMetrics.enabled && sm.compression.kind != CompressionNone {
		compressedCount := 0
		if compressed {
			compressedCount = 1
		}
		if pauseBytes := sm.compressionMetrics.add(s.ID, len(value), len(encoded), 1, compressedCount, 0); pauseBytes > 0 {
			sm.compressionPauseRemaining.Store(pauseBytes)
			if sm.compressionTrainer != nil {
				sm.compressionTrainer.signalDegraded(s.ID)
			}
		}
	}

	length := uint32(2 + 4 + len(encodedKey) + len(encoded))
	if omittedKey {
		length = page.ValuePtrMarkOmittedKey(length)
		if dictCompressed {
			length = page.ValuePtrMarkDictCompressed(length)
		} else if compressed {
			length = page.ValuePtrMarkCompressed(length)
		}
	} else if dictCompressed {
		length = page.ValuePtrMarkDictCompressed(length)
	} else if compressed {
		length = page.ValuePtrMarkCompressed(length)
	}

	return page.ValuePtr{
		Offset: uint64(offset + 4),
		Length: length,
		FileID: s.ID,
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
	dictCompressed bool
	fullCompressed bool
	omittedKey     bool
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
	if sm.compressionTrainer != nil && sm.compressionTrainer.shouldCollect() {
		for i := range values {
			if !sm.compressionTrainer.shouldCollect() {
				break
			}
			sm.compressionTrainer.collect(values[i])
		}
	}

	sm.mu.RLock()
	useDict := sm.compression.kind == CompressionZSTD && sm.activeSlab != nil && sm.activeSlab.version == slabVersionV2
	sm.mu.RUnlock()
	if useDict {
		return sm.appendManyWithDict(keys, values)
	}

	// Keep buffers bounded so we don't double memory usage for extremely large
	// batches or values.
	const maxBatchBytes = 8 << 20   // 8 MiB
	const maxKeepScratch = 16 << 20 // 16 MiB

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
	if sm.compression.kind != CompressionNone {
		encodedValues = make([][]byte, len(values))
		encodedKeys = make([][]byte, len(values))
		for i := range values {
			if !sm.shouldCompress(len(values[i])) {
				encodedValues[i] = values[i]
				encodedKeys[i] = keys[i]
				continue
			}
			// Try full record compression first
			if enc, ok, err := sm.compression.compressRecord(keys[i], values[i]); err == nil && ok {
				encodedValues[i] = enc
				encodedKeys[i] = nil
				compressedFlags[i] = true
				fullCompressedFlags[i] = true
			} else if err != nil {
				return nil, err
			} else {
				// Fall back to value-only compression
				encoded, compressed, err := sm.compression.compressValue(values[i])
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
			return nil, ErrRecordTooLarge
		}
		if recordSizeExceedsMax(uint16(keyLen), uint32(valLen)) {
			return nil, ErrRecordTooLarge
		}
		recordLen64 := int64(HeaderSize) + int64(keyLen) + int64(valLen)
		if recordLen64 < 0 || recordLen64 > int64(int(recordLen64)) || recordLen64 > MaxSlabSize {
			return nil, ErrRecordTooLarge
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
					if meta.dictCompressed {
						length = page.ValuePtrMarkDictCompressed(length)
					} else if meta.compressed {
						length = page.ValuePtrMarkCompressed(length)
					}
				} else if meta.dictCompressed {
					length = page.ValuePtrMarkDictCompressed(length)
				} else if meta.compressed {
					length = page.ValuePtrMarkCompressed(length)
				}
				ptrs[meta.idx] = page.ValuePtr{
					Offset: uint64(base + int64(meta.start) + 4),
					Length: length,
					FileID: id,
				}
			}

			if sm.compressionMetrics.enabled && sm.compression.kind != CompressionNone && len(metas) > 0 {
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

func (sm *SlabManager) appendManyWithDict(keys [][]byte, values [][]byte) ([]page.ValuePtr, error) {
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
		base, err := s.WriteBatch(buf)
		if err == ErrSlabFull {
			return err
		}
		if err != nil {
			return err
		}

		for _, meta := range metas {
			length := uint32(2 + 4 + meta.keyLen + meta.valLen)
			if meta.omittedKey {
				length = page.ValuePtrMarkOmittedKey(length)
				if meta.dictCompressed {
					length = page.ValuePtrMarkDictCompressed(length)
				} else if meta.compressed {
					length = page.ValuePtrMarkCompressed(length)
				}
			} else if meta.dictCompressed {
				length = page.ValuePtrMarkDictCompressed(length)
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

	var lenArr [6]byte
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		value := values[i]
		encodedKey := key
		omittedKey := false
		if sm.omitSlabKeys {
			encodedKey = nil
			omittedKey = true
		}

		canCompress := sm.shouldCompress(len(value))
		encodedValue := value
		dictCompressed := false
		compressed := false
		if canCompress {
			if err := sm.activeSlab.initDictFromSample(&sm.compression, value); err != nil {
				return nil, err
			}
			encPool, _ := sm.activeSlab.dictPools(&sm.compression)
			if encPool != nil {
				var err error
				encodedValue, dictCompressed, err = sm.compression.compressValueWithPool(value, encPool)
				if err != nil {
					return nil, err
				}
				compressed = dictCompressed
			}
			if !dictCompressed {
				var err error
				encodedValue, compressed, err = sm.compression.compressValue(value)
				if err != nil {
					return nil, err
				}
			}
		}

		keyLen := len(encodedKey)
		valLen := len(encodedValue)
		if keyLen > int(^uint16(0)) || valLen > int(^uint32(0)) {
			return nil, ErrRecordTooLarge
		}
		if dictCompressed && recordSizeExceedsMax(uint16(keyLen), uint32(valLen)) {
			encodedValue, compressed, err = sm.compression.compressValue(value)
			if err != nil {
				return nil, err
			}
			dictCompressed = false
			valLen = len(encodedValue)
			if keyLen > int(^uint16(0)) || valLen > int(^uint32(0)) {
				return nil, ErrRecordTooLarge
			}
			if recordSizeExceedsMax(uint16(keyLen), uint32(valLen)) {
				return nil, ErrRecordTooLarge
			}
		}
		if recordSizeExceedsMax(uint16(keyLen), uint32(valLen)) {
			return nil, ErrRecordTooLarge
		}
		recordLen64 := int64(HeaderSize) + int64(keyLen) + int64(valLen)
		if recordLen64 < 0 || recordLen64 > int64(int(recordLen64)) || recordLen64 > MaxSlabSize {
			return nil, ErrRecordTooLarge
		}
		recordLen := int(recordLen64)

		if int64(sm.activeSlab.Size)+int64(len(buf))+int64(recordLen) > MaxSlabSize {
			if err := flush(); err != nil {
				return nil, err
			}
			if int64(sm.activeSlab.Size)+int64(recordLen) > MaxSlabSize {
				if err := sm.rotateLocked(); err != nil {
					return nil, err
				}
				canCompress = sm.shouldCompress(len(value))
				encodedValue = value
				dictCompressed = false
				compressed = false
				if canCompress {
					if err := sm.activeSlab.initDictFromSample(&sm.compression, value); err != nil {
						return nil, err
					}
					encPool, _ = sm.activeSlab.dictPools(&sm.compression)
					if encPool != nil {
						encodedValue, dictCompressed, err = sm.compression.compressValueWithPool(value, encPool)
						if err != nil {
							return nil, err
						}
						compressed = dictCompressed
					}
					if !dictCompressed {
						encodedValue, compressed, err = sm.compression.compressValue(value)
						if err != nil {
							return nil, err
						}
					}
				}
				keyLen = len(encodedKey)
				valLen = len(encodedValue)
				if dictCompressed && recordSizeExceedsMax(uint16(keyLen), uint32(valLen)) {
					encodedValue, compressed, err = sm.compression.compressValue(value)
					if err != nil {
						return nil, err
					}
					dictCompressed = false
					valLen = len(encodedValue)
				}
				if keyLen > int(^uint16(0)) || valLen > int(^uint32(0)) {
					return nil, ErrRecordTooLarge
				}
				if recordSizeExceedsMax(uint16(keyLen), uint32(valLen)) {
					return nil, ErrRecordTooLarge
				}
				recordLen64 = int64(HeaderSize) + int64(keyLen) + int64(valLen)
				if recordLen64 < 0 || recordLen64 > int64(int(recordLen64)) || recordLen64 > MaxSlabSize {
					return nil, ErrRecordTooLarge
				}
				recordLen = int(recordLen64)
			}
		}

		if len(buf) >= maxBatchBytes {
			if err := flush(); err != nil {
				return nil, err
			}
		}

		binary.LittleEndian.PutUint16(lenArr[0:2], uint16(keyLen))
		binary.LittleEndian.PutUint32(lenArr[2:6], uint32(valLen))
		sum := crc32.Update(0, crc32cTable, lenArr[:])
		sum = crc32.Update(sum, crc32cTable, encodedKey)
		sum = crc32.Update(sum, crc32cTable, encodedValue)

		start := len(buf)
		end := start + recordLen
		growBuf(end)
		buf = buf[:end]
		rec := buf[start:end]

		binary.LittleEndian.PutUint32(rec[0:4], sum)
		binary.LittleEndian.PutUint16(rec[4:6], uint16(keyLen))
		binary.LittleEndian.PutUint32(rec[6:10], uint32(valLen))
		copy(rec[10:10+keyLen], encodedKey)
		copy(rec[10+keyLen:], encodedValue)

		metas = append(metas, appendManyMeta{
			idx:            i,
			start:          start,
			keyLen:         keyLen,
			valLen:         valLen,
			compressed:     compressed,
			dictCompressed: dictCompressed,
			omittedKey:     omittedKey,
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
	_, decPool := f.dictPools(s.compression)
	return decodeValue(ptr, val, s.compression, decPool)
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
	_, decPool := f.dictPools(s.compression)
	return decodeValue(ptr, val, s.compression, decPool)
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
