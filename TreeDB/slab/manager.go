package slab

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/page"
)

// SlabSet is an immutable list of SlabFiles active at a specific point in time.
type SlabSet struct {
	Files               map[uint32]*SlabFile
	RefCount            atomic.Int64
	disableReadChecksum bool
	compression         *compression.Config
}
type SlabManager struct {
	dir              string
	readOnly         bool
	activeSlab       *SlabFile
	activeSlabWriter *SlabWriter
	slabs            map[uint32]*SlabFile // The master list of all live + zombie slabs
	mu               sync.RWMutex

	disableReadChecksum          bool
	compression                  compression.Config
	activeCompression            compression.Config
	currentProfile               atomic.Pointer[compression.ActiveProfile]
	omitSlabKeys                 bool
	disableFullRecordCompression bool
	compressionProbeBytes        uint64
	pausedSampleStride           uint64
	compressionMetrics           compression.Metrics
	compressionTrainer           *compression.Trainer

	appendManyScratch         []byte
	compressionPauseRemaining atomic.Uint64
	compressionProbeRemaining atomic.Uint64
	pausedSampleCounter       atomic.Uint64
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
	compCfg, err := compression.NormalizeOptions(opts.Compression)
	if err != nil {
		return nil, err
	}
	metricsOpts := opts.ToMetricsOptions()
	if opts.Compression.Kind != CompressionNone && metricsOpts.AdaptiveRatio == 0 {
		metricsOpts.AdaptiveRatio = 0.995
	}
	if metricsOpts.WindowBytes <= 0 {
		metricsOpts.WindowBytes = 1 << 20
	}
	if metricsOpts.PauseBytes <= 0 {
		metricsOpts.PauseBytes = 16 << 20
	}
	sm := &SlabManager{
		dir:                          dir,
		readOnly:                     readOnly,
		slabs:                        make(map[uint32]*SlabFile),
		compression:                  compCfg,
		activeCompression:            compCfg, // Start with base compression
		omitSlabKeys:                 opts.OmitSlabKeys,
		disableFullRecordCompression: opts.CompressionDisableFullRecord,
		compressionMetrics:           compression.NewMetrics(metricsOpts),
		compressionTrainer:           compression.NewTrainer(opts.ToTrainConfig(), compCfg, readOnly, opts.CompressionMetrics),
	}
	probeBytes := opts.CompressionAdaptiveProbeBytes
	if probeBytes < 0 {
		probeBytes = 0
	}
	if probeBytes == 0 && sm.compressionMetrics.Enabled {
		probeBytes = int(sm.compressionMetrics.PauseBytes / 4)
		if probeBytes < 64<<10 {
			probeBytes = 64 << 10
		}
	}
	sm.compressionProbeBytes = uint64(probeBytes)
	pausedStride := opts.CompressionAdaptivePauseSampleStride
	if pausedStride <= 0 {
		pausedStride = 256
	}
	sm.pausedSampleStride = uint64(pausedStride)

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
		sm.activeSlabWriter = NewSlabWriter(s, 0)

		// Use V2 if a compression profile is ready (Adaptive) OR if ZSTD is enabled.
		if s.version == 0 && s.Size == 0 {

			profile, ok := sm.activeProfile()

			if ok && profile != nil || sm.compression.Kind == CompressionZSTD {

				if err := sm.writeSlabV2Header(s, profile); err == nil {

					s.version = Version2

					if ok && profile != nil && len(profile.Dict) > 0 {

						// PAD IMMEDIATELY for consistency
						paddedDict := make([]byte, GlobalDictSize)
						copy(paddedDict, profile.Dict)
						s.globalDict = paddedDict

						s.globalDecs = &sync.Pool{

							New: func() any {

								dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(paddedDict))

								return dec

							},
						}

						// Ensure activeCompression is ZSTD if we have a dictionary.

						if sm.compression.Kind != CompressionZSTD {

							sm.activeCompression, _ = compression.NormalizeOptions(compression.Options{Kind: compression.KindZSTD})

						} else {

							sm.activeCompression = sm.compression

						}

						sm.activeCompression.ZstdEncs = &sync.Pool{
							New: func() any {
								enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(paddedDict), zstd.WithEncoderLevel(sm.activeCompression.Level))
								if err != nil {
									enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(sm.activeCompression.Level), zstd.WithEncoderCRC(false))
								}
								return enc
							},
						}

						paddedProfile := *profile
						paddedProfile.Dict = paddedDict
						paddedProfile.DictHash = xxhash.Sum64(paddedDict)
						sm.currentProfile.Store(&paddedProfile)

					} else {

						// V2 but no initial dictionary (Zone 0 will be raw ZSTD).

						if sm.compression.Kind != CompressionZSTD {

							sm.activeCompression, _ = compression.NormalizeOptions(compression.Options{Kind: compression.KindZSTD})

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
				sm.activeSlabWriter = NewSlabWriter(s, 0)
				continue
			}
			if readOnly {
				sm.slabs[info.id] = OpenSlabLazyReadOnly(info.path, info.id, info.size)
			} else {
				sm.slabs[info.id] = OpenSlabLazy(info.path, info.id, info.size)
			}
		}
	}
	if sm.compressionMetrics.Enabled && sm.activeSlab != nil {
		sm.compressionMetrics.SetSlab(sm.activeSlab.ID)
	}

	return sm, nil
}

func (sm *SlabManager) Compression() CompressionKind {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.compression.Kind
}

func (sm *SlabManager) activeProfile() (*compression.ActiveProfile, bool) {
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

func (sm *SlabManager) CompressionTrainConfig() (compression.TrainConfig, bool) {
	sm.mu.RLock()
	trainer := sm.compressionTrainer
	sm.mu.RUnlock()
	if trainer == nil {
		return compression.TrainConfig{}, false
	}
	return trainer.Config(), true
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

func (sm *SlabManager) shouldAttemptCompression(rawLen int) (bool, bool, bool) {
	if rawLen <= 0 {
		return true, false, false
	}
	remaining := sm.compressionPauseRemaining.Load()
	for remaining > 0 {
		next := uint64(0)
		if uint64(rawLen) < remaining {
			next = remaining - uint64(rawLen)
		}
		if sm.compressionPauseRemaining.CompareAndSwap(remaining, next) {
			if sm.compressionProbeBytes == 0 {
				return false, false, true
			}
			probeRemaining := sm.compressionProbeRemaining.Load()
			for {
				if probeRemaining <= uint64(rawLen) {
					if sm.compressionProbeRemaining.CompareAndSwap(probeRemaining, sm.compressionProbeBytes) {
						return true, true, true
					}
				} else if sm.compressionProbeRemaining.CompareAndSwap(probeRemaining, probeRemaining-uint64(rawLen)) {
					return false, false, true
				}
				probeRemaining = sm.compressionProbeRemaining.Load()
			}
		}
		remaining = sm.compressionPauseRemaining.Load()
	}
	return true, false, false
}

func (sm *SlabManager) shouldCollectPaused() bool {
	if sm.pausedSampleStride <= 1 {
		return true
	}
	return sm.pausedSampleCounter.Add(1)%sm.pausedSampleStride == 0
}

// EstimateCompression reports the raw/stored byte counts for a record if it were
// appended with the current compression settings. It does not write anything.
func (sm *SlabManager) EstimateCompression(key, value []byte) (rawLen int, storedLen int, err error) {
	rawLen = len(value)
	storedLen = len(value)
	if sm.compression.Kind == CompressionNone {
		return rawLen, storedLen, nil
	}

	if !sm.disableFullRecordCompression {
		if enc, ok, err := sm.compression.CompressRecord(key, value); err != nil {
			return rawLen, storedLen, err
		} else if ok {
			rawLen = 2 + len(key) + len(value)
			return rawLen, len(enc), nil
		}
	}

	enc, _, err := sm.compression.CompressValue(value)
	if err != nil {
		return rawLen, storedLen, err
	}
	return rawLen, len(enc), nil
}

func decodeValue(ptr page.ValuePtr, val []byte, cfg *compression.Config) ([]byte, error) {
	if page.ValuePtrIsGrouped(ptr) {
		if cfg == nil {
			return nil, compression.ErrCorrupt
		}
		return decompressFrameGroup(cfg, val, int(page.ValuePtrSubIndex(ptr)))
	}
	if !page.ValuePtrIsCompressed(ptr) {
		return val, nil
	}
	if cfg == nil {
		return nil, compression.ErrCorrupt
	}
	if page.ValuePtrIsFullCompressed(ptr) {
		_, v, err := cfg.DecompressRecord(val)
		return v, err
	}
	return cfg.DecompressValue(val)
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
	cfg := &sm.compression
	sm.mu.RUnlock()

	if ok && s.version >= Version2 && int64(ptr.Offset) >= SlabV2DataStart {
		dec, put, err := s.GetDecoder(int64(ptr.Offset))
		if err != nil {
			return nil, nil, err
		}
		defer put()

		if len(val) < 4 {
			return nil, nil, compression.ErrCorrupt
		}
		rawLen := binary.LittleEndian.Uint32(val[:4])
		payload := val[4:]

		decompressed, err := dec.DecodeAll(payload, make([]byte, 0, uint64(rawLen)))
		if err != nil {
			return nil, nil, err
		}
		if len(decompressed) < 2 {
			return nil, nil, compression.ErrCorrupt
		}
		keyLen := int(binary.LittleEndian.Uint16(decompressed[:2]))
		if len(decompressed) < 2+keyLen {
			return nil, nil, compression.ErrCorrupt
		}
		return decompressed[2 : 2+keyLen], decompressed[2+keyLen:], nil
	}

	if cfg == nil {
		return nil, nil, compression.ErrCorrupt
	}
	return cfg.DecompressRecord(val)
}

func (sm *SlabManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.compressionMetrics.Enabled && sm.activeSlab != nil {
		sm.compressionMetrics.Finish("close")
	}
	if sm.compressionTrainer != nil {
		sm.compressionTrainer.Close()
	}
	if sm.activeSlabWriter != nil {
		_ = sm.activeSlabWriter.Close()
		sm.activeSlabWriter = nil
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
	cfg := &sm.compression
	sm.mu.RUnlock()
	if ok {
		if err := sm.WaitForPtr(ptr); err != nil {
			return nil, err
		}
	}

	if !ok || s == nil {
		return nil, fmt.Errorf("slab file %d not found", ptr.FileID)
	}

	val, err := s.Read(int64(ptr.Offset), verifyCRC)
	if err != nil {
		return nil, err
	}

	if s.version >= Version2 && page.ValuePtrIsCompressed(ptr) {
		dec, put, err := s.GetDecoder(int64(ptr.Offset))
		if err != nil {
			return nil, err
		}
		defer put()

		if page.ValuePtrIsGrouped(ptr) {
			return decompressFrameGroupWithDecoder(dec, val, int(page.ValuePtrSubIndex(ptr)))
		}

		if len(val) < 4 {
			return nil, compression.ErrCorrupt
		}
		rawLen := binary.LittleEndian.Uint32(val[:4])
		payload := val[4:]

		if page.ValuePtrIsFullCompressed(ptr) {
			decompressed, err := dec.DecodeAll(payload, make([]byte, 0, uint64(rawLen)))
			if err != nil {
				return nil, err
			}
			if len(decompressed) < 2 {
				return nil, compression.ErrCorrupt
			}
			keyLen := binary.LittleEndian.Uint16(decompressed[:2])
			if len(decompressed) < 2+int(keyLen) {
				return nil, compression.ErrCorrupt
			}
			return decompressed[2+keyLen:], nil
		}
		return dec.DecodeAll(payload, make([]byte, 0, uint64(rawLen)))
	}

	return decodeValue(ptr, val, cfg)
}

func (sm *SlabManager) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	sm.mu.RLock()
	s, ok := sm.slabs[ptr.FileID]
	verifyCRC := !sm.disableReadChecksum
	cfg := &sm.compression
	sm.mu.RUnlock()
	if ok {
		if err := sm.WaitForPtr(ptr); err != nil {
			return nil, err
		}
	}

	if !ok || s == nil {
		return nil, fmt.Errorf("slab file %d not found", ptr.FileID)
	}

	val, err := s.ReadUnsafe(int64(ptr.Offset), verifyCRC)
	if err != nil {
		return nil, err
	}

	if s.version >= Version2 && page.ValuePtrIsCompressed(ptr) {
		dec, put, err := s.GetDecoder(int64(ptr.Offset))
		if err != nil {
			return nil, err
		}
		defer put()

		if page.ValuePtrIsGrouped(ptr) {
			return decompressFrameGroupWithDecoder(dec, val, int(page.ValuePtrSubIndex(ptr)))
		}

		if len(val) < 4 {
			return nil, compression.ErrCorrupt
		}
		rawLen := binary.LittleEndian.Uint32(val[:4])
		payload := val[4:]

		if page.ValuePtrIsFullCompressed(ptr) {
			decompressed, err := dec.DecodeAll(payload, make([]byte, 0, uint64(rawLen)))
			if err != nil {
				return nil, err
			}
			if len(decompressed) < 2 {
				return nil, compression.ErrCorrupt
			}
			keyLen := binary.LittleEndian.Uint16(decompressed[:2])
			if len(decompressed) < 2+int(keyLen) {
				return nil, compression.ErrCorrupt
			}
			return decompressed[2+keyLen:], nil
		}
		return dec.DecodeAll(payload, make([]byte, 0, uint64(rawLen)))
	}

	return decodeValue(ptr, val, cfg)
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
	var errInner error
	var releaseEncoded func()
	attemptCompression := false
	probeCompression := false
	paused := false
	if !opts.DisableCompression && sm.activeCompression.Kind != CompressionNone {
		attemptCompression, probeCompression, paused = sm.shouldAttemptCompression(len(value))
	}
	if !opts.SkipTraining && sm.compressionTrainer != nil && sm.compressionTrainer.ShouldCollect() {
		if !paused || sm.shouldCollectPaused() {
			sm.compressionTrainer.Collect(value)
		}
	}
	if attemptCompression && !opts.DisableCompression && sm.activeCompression.Kind != CompressionNone {
		// Try full record compression first
		if !sm.disableFullRecordCompression {
			if enc, ok, errInner := sm.activeCompression.CompressRecord(key, value); errInner == nil && ok {
				encoded = enc
				encodedKey = nil
				compressed = true
				fullCompressed = true
			} else if errInner != nil {
				return page.ValuePtr{}, errInner
			}
		}
		if !fullCompressed {
			// Fall back to value-only compression
			encoded, compressed, releaseEncoded, errInner = sm.activeCompression.CompressValuePooled(value)
			if errInner != nil {
				return page.ValuePtr{}, errInner
			}
		}
	}
	if releaseEncoded != nil {
		defer releaseEncoded()
	}
	if probeCompression && (compressed || fullCompressed) {
		sm.compressionPauseRemaining.Store(0)
		sm.compressionProbeRemaining.Store(sm.compressionProbeBytes)
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
	if sm.activeSlabWriter == nil || sm.activeSlabWriter.s != sm.activeSlab {
		if sm.activeSlabWriter != nil {
			_ = sm.activeSlabWriter.Close()
		}
		sm.activeSlabWriter = NewSlabWriter(sm.activeSlab, 0)
	}

	// Pre-check for V2 absolute size limit.
	if sm.activeSlab.version >= Version2 {
		if int64(HeaderSize+len(encodedKey)+len(encoded)) > maxV2RecordSize {
			log.Printf("treedb: record too large (v2 precheck) key=%d val=%d record=%d max=%d full=%v compressed=%v omitted_key=%v",
				len(encodedKey), len(encoded), HeaderSize+len(encodedKey)+len(encoded), maxV2RecordSize, fullCompressed, compressed, omittedKey)
			return page.ValuePtr{}, ErrRecordTooLarge
		}
	}

	var offset int64
	// Reuse writeScratch from SlabFile via Manager
	sm.activeSlab.writeScratch = sm.activeSlab.EncodeRecord(encodedKey, encoded, sm.activeSlab.writeScratch)
	rec := sm.activeSlab.writeScratch

	for attempt := 0; attempt < 3; attempt++ {
		// Check for V2 boundaries BEFORE passing to async writer
		var currentSize int64
		if sm.activeSlabWriter != nil && sm.activeSlab == sm.activeSlabWriter.s {
			currentSize = sm.activeSlabWriter.Size()

			// If adding this record pushes the async buffer across a zone boundary,
			// force a sync first so that checkBoundary (and the file writer) sees
			// the exact state and can trigger rotation/padding correctly.
			// V2 ZoneSize = 2MB.
			if sm.activeSlab.version >= Version2 {
				nextBoundary := ((currentSize / ZoneSize) + 1) * ZoneSize
				if currentSize+int64(len(rec)) > nextBoundary {
					if err := sm.activeSlabWriter.Flush(); err != nil {
						return page.ValuePtr{}, err
					}
					currentSize = sm.activeSlabWriter.Size()
				}
			}
		} else {
			currentSize = sm.activeSlab.Size
		}

		if currentSize+int64(len(rec)) > MaxSlabSize {
			if err = sm.rotateLocked(); err != nil {
				return page.ValuePtr{}, err
			}
			continue
		}

		if err := sm.activeSlab.checkBoundary(currentSize, int64(len(rec))); err != nil {
			if err == ErrRecordTooLarge {
				if currentSize >= ZoneSize && currentSize%ZoneSize == 0 {
					if err = sm.forceRotateZoneLocked(); err != nil {
						return page.ValuePtr{}, err
					}
				} else {
					if _, err = sm.maybeRotateZoneLocked(len(rec)); err != nil {
						return page.ValuePtr{}, err
					}
				}
				continue
			}
			return page.ValuePtr{}, err
		}

		offset, err = sm.activeSlabWriter.Write(rec)
		if err == ErrSlabFull {
			if err = sm.rotateLocked(); err != nil {
				return page.ValuePtr{}, err
			}
			continue
		}
		break
	}

	if err != nil {
		return page.ValuePtr{}, err
	}

	if !opts.SkipMetrics && !opts.DisableCompression && sm.compressionMetrics.Enabled && sm.activeCompression.Kind != CompressionNone {
		compressedCount := 0
		fullCount := 0
		if compressed {
			compressedCount = 1
		}
		if fullCompressed {
			fullCount = 1
		}
		if pauseBytes := sm.compressionMetrics.Add(sm.activeSlab.ID, rawLen, storedLen, 1, compressedCount, fullCount); pauseBytes > 0 {
			sm.compressionPauseRemaining.Store(pauseBytes)
			if sm.compressionProbeBytes > 0 {
				sm.compressionProbeRemaining.Store(sm.compressionProbeBytes)
			}
			if sm.compressionTrainer != nil {
				sm.compressionTrainer.SignalDegraded(sm.activeSlab.ID)
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
	if sm.activeSlabWriter == nil || sm.activeSlabWriter.s != sm.activeSlab {
		if sm.activeSlabWriter != nil {
			_ = sm.activeSlabWriter.Close()
		}
		sm.activeSlabWriter = NewSlabWriter(sm.activeSlab, 0)
	}
	if sm.activeSlabWriter != nil && sm.activeSlab == sm.activeSlabWriter.s {
		if err := sm.activeSlabWriter.Flush(); err != nil {
			return nil, err
		}
		sm.activeSlabWriter.ResetOffset(sm.activeSlab.Size)
	}

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
			if err == ErrRecordTooLarge {
				groupBytes := 0
				for _, val := range group {
					groupBytes += len(val)
				}
				log.Printf("treedb: record too large (v2 group build) group_k=%d group_bytes=%d max=%d",
					len(group), groupBytes, maxV2RecordSize)
			}
			return nil, err
		}

		if sm.activeSlab.version >= Version2 {
			if int64(len(record)) > maxV2RecordSize {
				groupBytes := 0
				for _, val := range group {
					groupBytes += len(val)
				}
				log.Printf("treedb: record too large (v2 group) group_k=%d group_bytes=%d record=%d max=%d",
					len(group), groupBytes, len(record), maxV2RecordSize)
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
	if sm.activeSlabWriter != nil && sm.activeSlab == sm.activeSlabWriter.s {
		sm.activeSlabWriter.ResetOffset(sm.activeSlab.Size)
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
	releases := make([]func(), len(values))
	defer func() {
		for _, release := range releases {
			if release != nil {
				release()
			}
		}
	}()
	if sm.activeCompression.Kind != CompressionNone {
		encodedValues = make([][]byte, len(values))
		encodedKeys = make([][]byte, len(values))
		for i := range values {
			attemptCompression, probeCompression, _ := sm.shouldAttemptCompression(len(values[i]))
			if !attemptCompression {
				encodedValues[i] = values[i]
				encodedKeys[i] = keys[i]
				continue
			}
			// Try full record compression first
			if !sm.disableFullRecordCompression {
				if enc, ok, err := sm.activeCompression.CompressRecord(keys[i], values[i]); err == nil && ok {
					encodedValues[i] = enc
					encodedKeys[i] = nil
					compressedFlags[i] = true
					fullCompressedFlags[i] = true
				} else if err != nil {
					return nil, err
				}
			}
			if !fullCompressedFlags[i] {
				// Fall back to value-only compression
				var encoded []byte
				var compressed bool
				var release func()
				var err error
				encoded, compressed, release, err = sm.activeCompression.CompressValuePooled(values[i])
				if err != nil {
					return nil, err
				}
				encodedValues[i] = encoded
				encodedKeys[i] = keys[i]
				compressedFlags[i] = compressed
				releases[i] = release
			}
			if probeCompression && (compressedFlags[i] || fullCompressedFlags[i]) {
				sm.compressionPauseRemaining.Store(0)
				sm.compressionProbeRemaining.Store(sm.compressionProbeBytes)
			}
		}
	}

	if sm.omitSlabKeys {
		if sm.activeCompression.Kind == CompressionNone {
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
			log.Printf("treedb: record too large (max) key=%d val=%d record=%d max=%d full=%v compressed=%v omitted_key=%v",
				keyLen, valLen, recordLen64, MaxSlabSize, fullCompressedFlags[i], compressedFlags[i], omittedKeyFlags[i])
			return nil, ErrRecordTooLarge
		}
		if sm.activeSlab.version >= Version2 {
			if recordLen64 > maxV2RecordSize {
				log.Printf("treedb: record too large (v2) key=%d val=%d record=%d max=%d full=%v compressed=%v omitted_key=%v",
					keyLen, valLen, recordLen64, maxV2RecordSize, fullCompressedFlags[i], compressedFlags[i], omittedKeyFlags[i])
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
	if sm.activeSlabWriter == nil || sm.activeSlabWriter.s != sm.activeSlab {
		if sm.activeSlabWriter != nil {
			_ = sm.activeSlabWriter.Close()
		}
		sm.activeSlabWriter = NewSlabWriter(sm.activeSlab, 0)
	}
	if sm.activeSlabWriter != nil && sm.activeSlab == sm.activeSlabWriter.s {
		if err := sm.activeSlabWriter.Flush(); err != nil {
			return nil, err
		}
		sm.activeSlabWriter.ResetOffset(sm.activeSlab.Size)
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
				nextBoundary := ((s.Size / ZoneSize) + 1) * ZoneSize
				log.Printf("treedb: batch buffer crosses zone size=%d buf=%d next_boundary=%d",
					s.Size, len(buf), nextBoundary)
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

			if sm.compressionMetrics.Enabled && sm.activeCompression.Kind != CompressionNone && len(metas) > 0 {
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
				if pauseBytes := sm.compressionMetrics.Add(id, rawTotal, storedTotal, len(metas), compressedCount, fullCount); pauseBytes > 0 {
					sm.compressionPauseRemaining.Store(pauseBytes)
					if sm.compressionProbeBytes > 0 {
						sm.compressionProbeRemaining.Store(sm.compressionProbeBytes)
					}
					if sm.compressionTrainer != nil {
						sm.compressionTrainer.SignalDegraded(id)
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

	if sm.activeSlabWriter != nil && sm.activeSlab == sm.activeSlabWriter.s {
		sm.activeSlabWriter.ResetOffset(sm.activeSlab.Size)
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
	if profile := sm.currentProfile.Load(); profile != nil && profile.K > 1 && sm.activeCompression.Kind == CompressionZSTD && sm.omitSlabKeys {
		groupK = profile.K
	}
	if sm.compressionTrainer != nil && sm.compressionTrainer.ShouldCollect() {
		for i := range values {
			if !sm.compressionTrainer.ShouldCollect() {
				break
			}
			sm.compressionTrainer.Collect(values[i])
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

func (sm *SlabManager) writeSlabV2Header(s *SlabFile, profile *compression.ActiveProfile) error {
	header := make([]byte, SlabV2DataStart)
	copy(header[0:8], MagicV2)
	header[8] = Version2
	if profile != nil && len(profile.Dict) > 0 {
		copy(header[FileHeaderSizeV2:], profile.Dict)
	}
	// Use activeSlabWriter if available (usually it is for new slabs)
	if sm.activeSlabWriter != nil && sm.activeSlab == s {
		_, err := sm.activeSlabWriter.WriteBatch(header, true)
		return err
	}
	// Fallback for manually passed slab
	_, err := s.WriteBatch(header, true)
	return err
}

func (sm *SlabManager) maybeRotateZoneLocked(recordLen int) (bool, error) {
	s := sm.activeSlab
	if s == nil || s.version < Version2 {
		return false, nil
	}

	currentSize := s.Size
	if sm.activeSlabWriter != nil && sm.activeSlab == sm.activeSlabWriter.s {
		currentSize = sm.activeSlabWriter.Size()
	}

	// 1. Are we EXACTLY at a boundary (Zone 1+)?
	if currentSize >= ZoneSize && currentSize%ZoneSize == 0 {
		if err := sm.forceRotateZoneLocked(); err != nil {
			return false, err
		}
		return true, nil
	}

	// 2. Will the record cross a boundary?
	nextBoundary := ((currentSize / ZoneSize) + 1) * ZoneSize
	if currentSize+int64(recordLen) <= nextBoundary {
		return false, nil
	}

	// Crossing boundary. Pad to boundary.
	if sm.activeSlabWriter != nil && sm.activeSlab == sm.activeSlabWriter.s {
		if err := sm.activeSlabWriter.Sync(); err != nil {
			return false, err
		}
	}
	if err := s.Truncate(nextBoundary); err != nil {
		return false, err
	}
	if sm.activeSlabWriter != nil && sm.activeSlab == sm.activeSlabWriter.s {
		sm.activeSlabWriter.ResetOffset(s.Size)
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
				if len(profile.Dict) <= GlobalDictSize {
					dictType = ZoneDictLocal
					// PAD IMMEDIATELY for consistency
					localDict = make([]byte, GlobalDictSize)
					copy(localDict, profile.Dict)
					// Update dictHash to match padded dict
					dictHash = xxhash.Sum64(localDict)
				}
				// If too large, we fall back to global or none (already dictType=ZoneDictGlobal).
			}
		}
	}

	// Validate and potentially downgrade dictType if encoder creation fails.
	var finalEncs *sync.Pool
	var finalDecs *sync.Pool

	for attempt := 0; attempt < 2; attempt++ {
		switch dictType {
		case ZoneDictLocal:
			encPool := &sync.Pool{
				New: func() any {
					enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(localDict), zstd.WithEncoderLevel(sm.compression.Level))
					if err != nil {
						return nil
					}
					return enc
				},
			}
			// Test one encoder immediately
			testEnc := encPool.Get()
			if testEnc == nil {
				// Downgrade
				dictType = ZoneDictGlobal
				localDict = nil
				continue
			}
			encPool.Put(testEnc)
			finalEncs = encPool
			finalDecs = &sync.Pool{
				New: func() any {
					dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(localDict))
					return dec
				},
			}
		case ZoneDictRef:
			// profile.Dict here might be unpadded if it's from currentProfile,
			// but we should ensure all stored dicts are padded.
			// Actually, let's just ensure we use what we have.
			encPool := &sync.Pool{
				New: func() any {
					enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(profile.Dict), zstd.WithEncoderLevel(sm.compression.Level))
					if err != nil {
						return nil
					}
					return enc
				},
			}
			testEnc := encPool.Get()
			if testEnc == nil {
				dictType = ZoneDictGlobal
				continue
			}
			encPool.Put(testEnc)
			finalEncs = encPool
			finalDecs = &sync.Pool{
				New: func() any {
					dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(profile.Dict))
					return dec
				},
			}
		case ZoneDictGlobal:
			if len(s.globalDict) > 0 {
				finalEncs = &sync.Pool{
					New: func() any {
						enc, _ := zstd.NewWriter(nil, zstd.WithEncoderDict(s.globalDict), zstd.WithEncoderLevel(sm.compression.Level))
						return enc
					},
				}
				finalDecs = &sync.Pool{
					New: func() any {
						dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(s.globalDict))
						return dec
					},
				}
			} else {
				// No dictionary
				finalEncs = &sync.Pool{
					New: func() any {
						enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(sm.compression.Level), zstd.WithEncoderCRC(false))
						return enc
					},
				}
				finalDecs = nil
			}
		}
		break
	}

	// Write Zone Header.
	zh := ZoneHeader{
		Magic:    ZoneHeaderMagic,
		DictType: uint8(dictType),
	}
	if dictType == ZoneDictLocal {
		zh.DictCRC = crc32.Checksum(localDict, crc32cTable)
		zh.DictLength = uint32(len(localDict))
	} else if dictType == ZoneDictRef {
		zh.DictCRC = refEntry.crc
		zh.DictLength = refEntry.zoneID
	}

	if _, err := sm.activeSlabWriter.WriteBatch(zh.Marshal(), true); err != nil {
		return err
	}

	if dictType == ZoneDictLocal {
		if _, err := sm.activeSlabWriter.WriteBatch(localDict, true); err != nil {
			return err
		}
		sm.activeCompression = sm.compression // shallow copy
		sm.activeCompression.ZstdEncs = finalEncs
		sm.activeCompression.ZstdDecs = finalDecs

		// Update profile with PADDED dict so USE_REF works correctly later
		paddedProfile := *profile
		paddedProfile.Dict = localDict
		sm.currentProfile.Store(&paddedProfile)

		zoneID := uint32(s.Size / ZoneSize)
		s.recordDictRef(dictHash, zoneID, zh.DictCRC)
	} else if dictType == ZoneDictRef {
		sm.activeCompression = sm.compression
		sm.activeCompression.ZstdEncs = finalEncs
		sm.activeCompression.ZstdDecs = finalDecs
		sm.currentProfile.Store(profile)
	} else {
		sm.activeCompression = sm.compression
		sm.activeCompression.ZstdEncs = finalEncs
		sm.activeCompression.ZstdDecs = finalDecs
		if len(s.globalDict) == 0 {
			sm.currentProfile.Store(nil)
		}
	}

	return nil
}

func (sm *SlabManager) rotateLocked() error {
	if sm.readOnly {
		return ErrReadOnly
	}
	if sm.activeSlabWriter != nil {
		if err := sm.activeSlabWriter.Flush(); err != nil {
			return err
		}
		_ = sm.activeSlabWriter.Close()
	} else {
		if err := sm.activeSlab.Sync(); err != nil {
			return err
		}
	}

	newID := sm.activeSlab.ID + 1
	filename := fmt.Sprintf("data-%04d.slab", newID)
	path := filepath.Join(sm.dir, filename)

	newSlab, err := OpenSlab(path, newID)
	if err != nil {
		return err
	}
	newWriter := NewSlabWriter(newSlab, 0)

	// Update manager's active slab and writer BEFORE writing header so that
	// writeSlabV2Header can use the writer correctly.
	oldSlab := sm.activeSlab
	sm.activeSlab = newSlab
	sm.activeSlabWriter = newWriter

	// Use V2 if a compression profile is ready (Adaptive) OR if ZSTD is enabled.
	profile, ok := sm.activeProfile()
	if ok && profile != nil || sm.compression.Kind == CompressionZSTD {
		if err := sm.writeSlabV2Header(newSlab, profile); err == nil {
			newSlab.version = Version2
			if ok && profile != nil && len(profile.Dict) > 0 && len(profile.Dict) <= GlobalDictSize {
				// PAD IMMEDIATELY for consistency
				paddedDict := make([]byte, GlobalDictSize)
				copy(paddedDict, profile.Dict)

				// Try to create a reader/writer to validate the dict
				enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(paddedDict), zstd.WithEncoderLevel(sm.activeCompression.Level))
				if err == nil {
					enc.Close()
					newSlab.globalDict = paddedDict
					newSlab.globalDecs = &sync.Pool{
						New: func() any {
							dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(paddedDict))
							if err != nil {
								panic(err)
							}
							return dec
						},
					}
					// Ensure activeCompression is ZSTD if we have a dictionary.
					if sm.compression.Kind != CompressionZSTD {
						sm.activeCompression, _ = compression.NormalizeOptions(compression.Options{Kind: compression.KindZSTD})
					} else {
						sm.activeCompression = sm.compression
					}
					sm.activeCompression.ZstdEncs = &sync.Pool{
						New: func() any {
							enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(paddedDict), zstd.WithEncoderLevel(sm.activeCompression.Level))
							if err != nil {
								enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(sm.activeCompression.Level), zstd.WithEncoderCRC(false))
							}
							return enc
						},
					}
					sm.activeCompression.ZstdDecs = &sync.Pool{
						New: func() any {
							dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(paddedDict))
							return dec
						},
					}
					paddedProfile := *profile
					paddedProfile.Dict = paddedDict
					paddedProfile.DictHash = xxhash.Sum64(paddedDict)
					sm.currentProfile.Store(&paddedProfile)
				} else {
					// Dict invalid, fall back to raw V2
					sm.activeCompression = sm.compression
					if sm.activeCompression.Kind != CompressionZSTD {
						sm.activeCompression, _ = compression.NormalizeOptions(compression.Options{Kind: compression.KindZSTD})
					}
					sm.currentProfile.Store(nil)
				}
			} else {
				// V2 but no initial dictionary (Zone 0 will be raw ZSTD).
				if sm.compression.Kind != CompressionZSTD {
					sm.activeCompression, _ = compression.NormalizeOptions(compression.Options{Kind: compression.KindZSTD})
				} else {
					sm.activeCompression = sm.compression
				}
				sm.activeCompression.ZstdEncs = &sync.Pool{
					New: func() any {
						enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(sm.activeCompression.Level), zstd.WithEncoderCRC(false))
						return enc
					},
				}
				sm.activeCompression.ZstdDecs = &sync.Pool{
					New: func() any {
						dec, _ := zstd.NewReader(nil)
						return dec
					},
				}
				sm.currentProfile.Store(nil)
			}
		} else if err == ErrSlabFull || err == ErrRecordTooLarge {
			// MaxSlabSize is too small for V2 header (common in rotation tests).
			// Fallback to V1.
			sm.activeCompression = sm.compression
			sm.currentProfile.Store(nil)
		} else {
			// Restore on failure
			sm.activeSlab = oldSlab
			_ = newWriter.Close()
			_ = newSlab.Close()
			return err
		}
	} else {
		// Fallback to V1.
		sm.activeCompression = sm.compression
		sm.currentProfile.Store(nil)
	}

	if sm.compressionMetrics.Enabled {
		sm.compressionMetrics.Finish("rotate")
	}
	// sm.activeSlab and sm.activeSlabWriter already updated above
	sm.slabs[newID] = newSlab
	if sm.compressionMetrics.Enabled {
		sm.compressionMetrics.Reset(newID)
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
	if sm.activeSlabWriter != nil {
		return sm.activeSlabWriter.Sync()
	}
	return sm.activeSlab.Sync()
}

// Flush ensures buffered writes are pushed to the slab without fsync.
func (sm *SlabManager) Flush() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.readOnly {
		return ErrReadOnly
	}
	if sm.activeSlabWriter != nil {
		return sm.activeSlabWriter.Flush()
	}
	return nil
}

// WaitForPtr blocks until the pointer's data is durable if it targets the active slab.
func (sm *SlabManager) WaitForPtr(ptr page.ValuePtr) error {
	end := int64(ptr.Offset) + int64(page.ValuePtrRecordLength(ptr))
	return sm.WaitForOffset(ptr.FileID, uint64(end))
}

// WaitForOffset blocks until the given offset is durable for the active slab.
func (sm *SlabManager) WaitForOffset(fileID uint32, offset uint64) error {
	sm.mu.RLock()
	s, ok := sm.slabs[fileID]
	active := sm.activeSlab
	writer := sm.activeSlabWriter
	sm.mu.RUnlock()
	if !ok || s == nil || active == nil || writer == nil || s != active {
		return nil
	}
	if err := writer.WaitForOffset(int64(offset)); err != nil {
		sm.mu.RLock()
		activeNow := sm.activeSlab == s && sm.activeSlabWriter == writer
		sm.mu.RUnlock()
		if activeNow {
			return err
		}
	}
	return nil
}

func (sm *SlabManager) ActiveSlabID() uint32 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.activeSlab.ID
}

func (sm *SlabManager) ActiveSlabTail() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.activeSlabWriter != nil && sm.activeSlab == sm.activeSlabWriter.s {
		return uint64(sm.activeSlabWriter.Size())
	}
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
	if sm.activeSlabWriter != nil {
		_ = sm.activeSlabWriter.Close()
	}
	sm.activeSlabWriter = NewSlabWriter(s, 0)
	if sm.compressionMetrics.Enabled {
		sm.compressionMetrics.Reset(s.ID)
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
	// Writer must flush before truncate to avoid data loss
	if sm.activeSlabWriter != nil {
		if err := sm.activeSlabWriter.Sync(); err != nil {
			return err
		}
	}
	if err := sm.activeSlab.Truncate(int64(offset)); err != nil {
		return err
	}
	if sm.activeSlabWriter != nil {
		sm.activeSlabWriter.ResetOffset(int64(offset))
	}
	return nil
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
	// Writer must flush before repair
	if sm.activeSlabWriter != nil {
		if err := sm.activeSlabWriter.Sync(); err != nil {
			return 0, err
		}
	}
	if err := sm.activeSlab.RepairTail(); err != nil {
		return 0, err
	}
	if sm.activeSlabWriter != nil {
		sm.activeSlabWriter.ResetOffset(sm.activeSlab.Size)
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

func (sm *SlabManager) CompressionTrainerStats() (compression.TrainerStats, bool) {
	if sm.compressionTrainer == nil {
		return compression.TrainerStats{}, false
	}
	stats := sm.compressionTrainer.Stats()
	if !stats.Enabled {
		return compression.TrainerStats{}, false
	}
	return stats, true
}

// ForceTrainerCollecting forces the compression trainer into collecting mode
// (best-effort; no-op if trainer is nil or disabled). Intended for diagnostics.
func (sm *SlabManager) ForceTrainerCollecting() {
	if sm.compressionTrainer == nil {
		return
	}
	sm.compressionTrainer.ForceCollecting()
}

// ForceAcceptProfileForTesting immediately installs a compression profile.
// Internal use for deterministic end-to-end tests only.
func (sm *SlabManager) ForceAcceptProfileForTesting(p *compression.ActiveProfile) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if p != nil && p.K <= 0 {
		p = &compression.ActiveProfile{
			DictHash:         p.DictHash,
			DictBytes:        p.DictBytes,
			Dict:             p.Dict,
			K:                1,
			PayloadRatio:     p.PayloadRatio,
			TotalRatio:       p.TotalRatio,
			DecodeNsEstimate: p.DecodeNsEstimate,
			Samples:          p.Samples,
			Timestamp:        p.Timestamp,
		}
	}
	sm.currentProfile.Store(p)
	// Ensure activeCompression is ZSTD if we have a dictionary.
	if sm.compression.Kind != CompressionZSTD {
		sm.activeCompression, _ = compression.NormalizeOptions(compression.Options{Kind: compression.KindZSTD})
	} else {
		sm.activeCompression = sm.compression
	}

	dict := p.Dict
	if len(dict) == 0 {
		sm.activeCompression.ZstdEncs = &sync.Pool{
			New: func() any {
				enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(sm.activeCompression.Level), zstd.WithEncoderCRC(false))
				return enc
			},
		}
		sm.activeCompression.ZstdDecs = &sync.Pool{
			New: func() any {
				dec, _ := zstd.NewReader(nil)
				return dec
			},
		}
		return
	}
	sm.activeCompression.ZstdEncs = &sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(sm.activeCompression.Level))
			if err != nil {
				enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(sm.activeCompression.Level), zstd.WithEncoderCRC(false))
			}
			return enc
		},
	}
	sm.activeCompression.ZstdDecs = &sync.Pool{
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
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			recordLen := page.ValuePtrRecordLength(ptr)
			flags := ptr.Length &^ recordLen
			log.Printf("treedb: slab read EOF file=%d offset=%d length=%d flags=0x%x",
				ptr.FileID,
				ptr.Offset,
				ptr.Length,
				flags,
			)
		}
		return nil, err
	}

	if f.version >= Version2 && page.ValuePtrIsCompressed(ptr) {
		dec, put, err := f.GetDecoder(int64(ptr.Offset))
		if err != nil {
			return nil, err
		}
		defer put()

		if page.ValuePtrIsGrouped(ptr) {
			return decompressFrameGroupWithDecoder(dec, val, int(page.ValuePtrSubIndex(ptr)))
		}

		if len(val) < 4 {
			return nil, compression.ErrCorrupt
		}
		rawLen := binary.LittleEndian.Uint32(val[:4])
		payload := val[4:]

		if page.ValuePtrIsFullCompressed(ptr) {
			decompressed, err := dec.DecodeAll(payload, make([]byte, 0, uint64(rawLen)))
			if err != nil {
				return nil, err
			}
			if len(decompressed) < 2 {
				return nil, compression.ErrCorrupt
			}
			keyLen := binary.LittleEndian.Uint16(decompressed[:2])
			if len(decompressed) < 2+int(keyLen) {
				return nil, compression.ErrCorrupt
			}
			return decompressed[2+keyLen:], nil
		}
		return dec.DecodeAll(payload, make([]byte, 0, uint64(rawLen)))
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

	if f.version >= Version2 && page.ValuePtrIsCompressed(ptr) {
		dec, put, err := f.GetDecoder(int64(ptr.Offset))
		if err != nil {
			return nil, err
		}
		defer put()

		if page.ValuePtrIsGrouped(ptr) {
			return decompressFrameGroupWithDecoder(dec, val, int(page.ValuePtrSubIndex(ptr)))
		}

		if len(val) < 4 {
			return nil, compression.ErrCorrupt
		}
		rawLen := binary.LittleEndian.Uint32(val[:4])
		payload := val[4:]

		if page.ValuePtrIsFullCompressed(ptr) {
			decompressed, err := dec.DecodeAll(payload, make([]byte, 0, uint64(rawLen)))
			if err != nil {
				return nil, err
			}
			if len(decompressed) < 2 {
				return nil, compression.ErrCorrupt
			}
			keyLen := binary.LittleEndian.Uint16(decompressed[:2])
			if len(decompressed) < 2+int(keyLen) {
				return nil, compression.ErrCorrupt
			}
			return decompressed[2+keyLen:], nil
		}
		return dec.DecodeAll(payload, make([]byte, 0, uint64(rawLen)))
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
