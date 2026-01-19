package compression

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/compress/zstd"
)

const (
	DefaultTrainBytes          = 1 << 20
	DefaultTrainDictBytes      = 32 << 10
	DefaultTrainMinRecords     = 64
	DefaultTrainMaxRecordBytes = 64 << 10
	DefaultTrainQueue          = 128
	DefaultTrainDedupWindow    = 16

	// Adaptive gating constants for dict+K refresh.
	MinProfileBytes       = 64 << 20 // 64 MiB
	MinProfileRecords     = 250_000  // records
	MinProfileInterval    = 10 * time.Minute
	ProfileDriftThreshold = 0.07 // 7%
	ProfileImproveThresh  = 0.02 // 2% better to accept
)

type Trainer struct {
	enabled     atomic.Bool
	collecting  atomic.Bool
	training    atomic.Bool
	targetBytes uint64
	minRecords  uint64
	maxRecord   int
	dictBytes   int
	level       zstd.EncoderLevel
	logged      atomic.Bool

	mu            sync.Mutex
	sampleBytes   uint64
	sampleRecords uint64
	samples       [][]byte
	lastSlabID    uint32

	sampleGen           atomic.Uint64
	sampleStride        uint64
	sampleStrideCounter atomic.Uint64
	sampleCh            chan trainerSample
	closed              atomic.Bool
	closeOnce           sync.Once
	measureCollect      bool
	samplePool          sync.Pool

	enqueued             atomic.Uint64
	dropped              atomic.Uint64
	maxQueueLen          atomic.Uint64
	trainCount           atomic.Uint64
	lastTrainRatio       atomic.Uint64
	lastTrainSamples     atomic.Uint64
	lastTrainDict        atomic.Uint64
	lastTrainDictHash    atomic.Uint64
	lastTrainDedupMode   atomic.Uint64
	lastTrainDedupFlag   atomic.Uint64
	lastTrainDedupRef    atomic.Uint64
	dictDedupLookups     atomic.Uint64
	dictDedupHits        atomic.Uint64
	dictDedupGlobal      atomic.Uint64
	dictDedupRef         atomic.Uint64
	dictDedupCache       atomic.Uint64
	dictDedupBytes       atomic.Uint64
	dictDedupBytesGlobal atomic.Uint64
	dictDedupBytesRef    atomic.Uint64
	dictDedupBytesCache  atomic.Uint64
	collectNanos         atomic.Uint64
	collectCount         atomic.Uint64
	collectMaxNanos      atomic.Uint64

	lastProfile atomic.Value // *ActiveProfile

	// workload counters
	totalBytesSeen   atomic.Uint64
	totalRecordsSeen atomic.Uint64

	// anti-thrash gating
	lastAcceptTime    atomic.Value // time.Time
	lastAcceptBytes   atomic.Uint64
	lastAcceptRecords atomic.Uint64
	rollingRatioBase  atomic.Uint64 // math.Float64bits(last_profile_total_ratio)
	rollingRatioCur   atomic.Uint64 // math.Float64bits(observed_total_ratio)
	attempts          atomic.Uint64
	accepts           atomic.Uint64
	rejects           atomic.Uint64
	lastRejectReason  atomic.Value // string

	dictDedupWindow     int
	dictHashes          []uint64
	dictHashPos         int
	dictHashIndex       map[uint64]int
	globalSlabID        uint32
	globalDictHash      uint64
	dictCacheHashes     []uint64
	dictCacheDicts      [][]byte
	dictCacheDictHashes []uint64
	dictCachePos        int
	dictCacheIndex      map[uint64]int
}

type trainerSample struct {
	gen    uint64
	sample []byte
}

type TrainerStats struct {
	Enabled              bool
	Collecting           bool
	Training             bool
	QueueLen             int
	QueueCap             int
	Enqueued             uint64
	Dropped              uint64
	MaxQueueLen          uint64
	TrainCount           uint64
	LastTrainRatio       float64
	LastTrainSamples     uint64
	LastTrainDict        uint64
	LastTrainDictHash    uint64
	LastTrainDedupMode   string
	LastTrainDedupFlag   string
	LastTrainDedupRef    uint64
	DictDedupLookups     uint64
	DictDedupHits        uint64
	DictDedupGlobal      uint64
	DictDedupRef         uint64
	DictDedupCache       uint64
	DictDedupBytes       uint64
	DictDedupBytesGlobal uint64
	DictDedupBytesRef    uint64
	DictDedupBytesCache  uint64
	CollectCount         uint64
	CollectNanos         uint64
	CollectMaxNanos      uint64
	ProfileK             int
	ProfileTotalRatio    float64
	ProfilePayloadRatio  float64
	ProfileTimestamp     time.Time
	ProfileAttempts      uint64
	ProfileAccepts       uint64
	ProfileRejects       uint64
	ProfileRejectReason  string
	RollingRatioBaseline float64
	RollingRatioCurrent  float64
	LastAcceptBytes      uint64
	LastAcceptRecords    uint64
	LastAcceptTimestamp  time.Time
}

type dictDedupMode uint8

const (
	dictDedupNone dictDedupMode = iota
	dictDedupGlobal
	dictDedupRef
	dictDedupCache
)

type DictUseFlag uint8

const (
	DictUseGlobal DictUseFlag = iota
	DictUseLocal
	DictUseRef
)

func NewTrainer(opts TrainConfig, cfg Config, readOnly bool, metricsEnabled bool) *Trainer {
	if readOnly || opts.TrainBytes < 0 {
		return nil
	}
	target := opts.TrainBytes
	if target == 0 {
		target = DefaultTrainBytes
	}
	dictBytes := opts.DictBytes
	if dictBytes <= 0 {
		dictBytes = DefaultTrainDictBytes
	}
	minRecords := opts.MinRecords
	if minRecords <= 0 {
		minRecords = DefaultTrainMinRecords
	}
	maxRecord := opts.MaxRecordBytes
	if maxRecord <= 0 {
		maxRecord = DefaultTrainMaxRecordBytes
	}
	sampleStride := opts.SampleStride
	if sampleStride <= 1 {
		sampleStride = 1
	}
	dedupWindow := opts.DedupWindow
	if dedupWindow <= 0 {
		dedupWindow = DefaultTrainDedupWindow
	}

	trainer := &Trainer{
		targetBytes:     uint64(target),
		minRecords:      uint64(minRecords),
		maxRecord:       maxRecord,
		dictBytes:       dictBytes,
		level:           cfg.Level,
		sampleStride:    uint64(sampleStride),
		sampleCh:        make(chan trainerSample, DefaultTrainQueue),
		measureCollect:  metricsEnabled,
		dictDedupWindow: dedupWindow,
	}
	trainer.enabled.Store(true)
	trainer.collecting.Store(true)
	go trainer.run()
	return trainer
}

func (t *Trainer) Close() {
	if t == nil {
		return
	}
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		close(t.sampleCh)
	})
}

func (t *Trainer) ShouldCollect() bool {
	if t == nil || t.closed.Load() || t.targetBytes == 0 {
		return false
	}
	return t.collecting.Load()
}

func (t *Trainer) ForceCollecting() {
	if t == nil || !t.enabled.Load() {
		return
	}
	t.collecting.Store(true)
}

func (t *Trainer) SignalDegraded(slabID uint32) {
	if t == nil || !t.enabled.Load() {
		return
	}
	if t.training.Load() || t.collecting.Load() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.training.Load() || t.collecting.Load() {
		return
	}
	if len(t.samples) > 0 {
		t.releaseSamples(t.samples)
		t.samples = nil
	}
	if slabID != t.globalSlabID {
		t.globalSlabID = slabID
		t.globalDictHash = 0
		t.dictHashes = nil
		t.dictHashPos = 0
		t.dictHashIndex = nil
		t.dictCacheHashes = nil
		t.dictCacheDicts = nil
		t.dictCacheDictHashes = nil
		t.dictCachePos = 0
		t.dictCacheIndex = nil
	}
	t.sampleBytes = 0
	t.sampleRecords = 0
	t.lastSlabID = slabID
	t.sampleGen.Add(1)
	t.sampleStrideCounter.Store(0)
	if base := math.Float64frombits(t.rollingRatioBase.Load()); base > 0 {
		t.rollingRatioCur.Store(math.Float64bits(base * (1 + ProfileDriftThreshold + 0.01)))
	}
	t.collecting.Store(true)
}

func (t *Trainer) Collect(value []byte) {
	var started time.Time
	if t == nil || t.closed.Load() || !t.collecting.Load() || len(value) == 0 {
		return
	}
	t.totalBytesSeen.Add(uint64(len(value)))
	t.totalRecordsSeen.Add(1)
	if t.measureCollect {
		started = time.Now()
	}
	if t.sampleStride > 1 {
		if t.sampleStrideCounter.Add(1)%t.sampleStride != 0 {
			return
		}
	}
	if len(t.sampleCh) == cap(t.sampleCh) {
		t.dropped.Add(1)
		t.recordCollect(started)
		return
	}
	sample := value
	if len(sample) > t.maxRecord {
		sample = sample[:t.maxRecord]
	}
	cp := t.getSampleBuf(len(sample))
	copy(cp, sample)
	gen := t.sampleGen.Load()
	select {
	case t.sampleCh <- trainerSample{gen: gen, sample: cp}:
		t.enqueued.Add(1)
	default:
		t.dropped.Add(1)
		t.putSampleBuf(cp)
	}
	t.updateMaxQueueLen()
	t.recordCollect(started)
}

func (t *Trainer) run() {
	for sample := range t.sampleCh {
		t.appendSample(sample)
	}
}

func (t *Trainer) appendSample(sample trainerSample) {
	if t == nil {
		return
	}
	if !t.collecting.Load() {
		t.putSampleBuf(sample.sample)
		return
	}
	if sample.gen != t.sampleGen.Load() {
		t.putSampleBuf(sample.sample)
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.collecting.Load() || sample.gen != t.sampleGen.Load() {
		t.putSampleBuf(sample.sample)
		return
	}
	t.samples = append(t.samples, sample.sample)
	t.sampleBytes += uint64(len(sample.sample))
	t.sampleRecords++

	if t.sampleBytes < t.targetBytes || t.sampleRecords < t.minRecords {
		return
	}
	samples := t.samples
	slabID := t.lastSlabID
	dictBytes := t.dictBytes
	level := t.level
	t.samples = nil
	t.collecting.Store(false)
	t.training.Store(true)
	go t.train(samples, dictBytes, level, slabID)
}

func (t *Trainer) train(samples [][]byte, dictBytes int, level zstd.EncoderLevel, slabID uint32) {
	defer t.training.Store(false)
	defer func() {
		if r := recover(); r != nil {
			// Suppress panic from zstd or internal logic, treat as failed training.
			// Log as warning instead of PANIC.
			log.Printf("treedb: slab compression training skipped slab=%d err=%v\n%s", slabID, r, debug.Stack())
		}
	}()
	if len(samples) == 0 {
		return
	}
	now := time.Now()
	if !t.allowRetrain(now) {
		return
	}
	defer t.releaseSamples(samples)

	var validSamples [][]byte
	rawTotal := 0
	for _, sample := range samples {
		if len(sample) > 0 {
			validSamples = append(validSamples, sample)
			rawTotal += len(sample)
		}
	}
	if rawTotal < 4096 || len(validSamples) < 8 {
		return
	}
	if dictBytes > rawTotal {
		dictBytes = rawTotal
	}
	history := make([]byte, 0, dictBytes)
	for _, sample := range validSamples {
		if len(history) >= dictBytes {
			break
		}
		need := dictBytes - len(history)
		if len(sample) > need {
			history = append(history, sample[:need]...)
		} else {
			history = append(history, sample...)
		}
	}
	if len(history) < 8 {
		return
	}

	samplesHash := hashSamples(validSamples)
	if cached, dictHash, ok := t.lookupCachedDict(samplesHash); ok {
		t.trainCount.Add(1)
		t.lastTrainDictHash.Store(dictHash)
		t.lastTrainSamples.Store(uint64(len(validSamples)))
		t.lastTrainDict.Store(uint64(len(cached)))
		t.lastTrainDedupMode.Store(uint64(dictDedupCache))
		t.lastTrainDedupFlag.Store(uint64(DictUseRef))
		t.lastTrainDedupRef.Store(0)
		if t.logOnce() {
			log.Printf("treedb: slab compression dict dedup slab=%d dict_bytes=%d samples=%d hash=%x mode=%s ref=%d",
				slabID,
				len(cached),
				len(validSamples),
				dictHash,
				dedupModeString(dictDedupCache),
				0,
			)
		}
		return
	}

	t.trainCount.Add(1)

	nonEmptySamples := make([][]byte, 0, len(validSamples))
	for _, s := range validSamples {
		if len(s) > 0 {
			nonEmptySamples = append(nonEmptySamples, s)
		}
	}
	validSamples = nonEmptySamples

	if len(validSamples) < 8 {
		return
	}

	dictID := slabID + 1
	if dictID == 0 {
		dictID = 1
	}

	dict, err := buildAndValidateDict(dictID, validSamples, history, level)
	if err != nil || len(dict) == 0 {
		return
	}

	dictHash := xxhash.Sum64(dict)
	t.lastTrainDictHash.Store(dictHash)
	mode, ref := t.recordDictHash(dictHash)
	t.lastTrainDedupMode.Store(uint64(mode))
	t.lastTrainDedupFlag.Store(uint64(dedupFlagFromMode(mode)))
	if mode == dictDedupRef && ref >= 0 {
		t.lastTrainDedupRef.Store(uint64(ref))
	} else {
		t.lastTrainDedupRef.Store(0)
	}
	if mode != dictDedupNone {
		bytes := uint64(len(dict))
		t.dictDedupBytes.Add(bytes)
		switch mode {
		case dictDedupGlobal:
			t.dictDedupBytesGlobal.Add(bytes)
		case dictDedupRef:
			t.dictDedupBytesRef.Add(bytes)
		}
		t.lastTrainSamples.Store(uint64(len(samples)))
		t.lastTrainDict.Store(uint64(len(dict)))
		if t.logOnce() {
			log.Printf("treedb: slab compression dict dedup slab=%d dict_bytes=%d samples=%d hash=%x mode=%s ref=%d",
				slabID,
				len(dict),
				len(samples),
				dictHash,
				dedupModeString(mode),
				ref,
			)
		}
		return
	}
	t.storeCachedDict(samplesHash, dictHash, dict)

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level), zstd.WithEncoderCRC(false), zstd.WithEncoderDict(dict))
	if err != nil {
		log.Printf("treedb: slab compression training encode setup failed slab=%d err=%v", slabID, err)
		return
	}
	defer enc.Close()

	storedTotal := 0
	for _, sample := range samples {
		storedTotal += len(enc.EncodeAll(sample, nil))
	}
	ratio := 1.0
	if rawTotal > 0 {
		ratio = float64(storedTotal) / float64(rawTotal)
	}
	t.lastTrainRatio.Store(math.Float64bits(ratio))
	t.lastTrainSamples.Store(uint64(len(samples)))
	t.lastTrainDict.Store(uint64(len(dict)))
	if t.logOnce() {
		log.Printf("treedb: slab compression trained dict slab=%d dict_bytes=%d samples=%d raw=%d stored=%d ratio=%.3f",
			slabID,
			len(dict),
			len(samples),
			rawTotal,
			storedTotal,
			ratio,
		)
	}
	t.rollingRatioCur.Store(math.Float64bits(ratio))
	if profile := ChooseKForDict(dict, samples); profile != nil {
		t.maybeAcceptProfile(profile)
	}
}

func buildAndValidateDict(dictID uint32, samples [][]byte, history []byte, level zstd.EncoderLevel) (dict []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			dict = nil
			err = fmt.Errorf("zstd.BuildDict panic: %v", r)
		}
	}()
	dict, err = zstd.BuildDict(zstd.BuildDictOptions{
		ID:       dictID,
		Contents: samples,
		History:  history,
		Level:    level,
	})
	if err != nil || len(dict) == 0 {
		if err != nil {
			log.Printf("treedb: slab compression training failed slab=%d err=%v", dictID-1, err)
		}
		return nil, err
	}
	// Strict cap for V2 compatibility (matching GlobalDictSize)
	if len(dict) > 40960 {
		dict = dict[:40960]
	} else if len(dict) < 40960 {
		// PAD to exactly GlobalDictSize
		padded := make([]byte, 40960)
		copy(padded, dict)
		dict = padded
	}
	if err := validateDict(dict, level); err != nil {
		// Retry with a smaller dict to avoid invalid offset failures.
		reduced := dict[:len(dict)/2]
		if err2 := validateDict(reduced, level); err2 == nil {
			padded := make([]byte, 40960)
			copy(padded, reduced)
			if err3 := validateDict(padded, level); err3 == nil {
				return padded, nil
			}
		}
		log.Printf("treedb: slab compression training dict rejected slab=%d err=%v", dictID-1, err)
		return nil, err
	}
	return dict, nil
}

func validateDict(dict []byte, level zstd.EncoderLevel) error {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level), zstd.WithEncoderCRC(false), zstd.WithEncoderDict(dict))
	if err != nil {
		return err
	}
	defer enc.Close()

	// Verify the dictionary actually works for round-trip.
	// We use a small dummy payload.
	dummy := []byte("test_payload_validation")
	compressed := enc.EncodeAll(dummy, nil)

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	if err != nil {
		return err
	}
	defer dec.Close()

	decompressed, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return err
	}
	if string(decompressed) != string(dummy) {
		return fmt.Errorf("dictionary round-trip mismatch")
	}
	return nil
}

func (t *Trainer) recordDictHash(hash uint64) (dictDedupMode, int) {
	if t == nil {
		return dictDedupNone, -1
	}
	t.dictDedupLookups.Add(1)
	t.mu.Lock()
	defer t.mu.Unlock()

	window := t.dictDedupWindow
	if window <= 0 {
		window = DefaultTrainDedupWindow
		t.dictDedupWindow = window
	}
	if window <= 0 {
		return dictDedupNone, -1
	}
	if t.dictHashes == nil {
		t.dictHashes = make([]uint64, window)
		t.dictHashIndex = make(map[uint64]int, window)
	}
	if len(t.dictHashes) == 0 {
		return dictDedupNone, -1
	}
	if t.globalDictHash != 0 && t.globalDictHash == hash {
		t.dictDedupHits.Add(1)
		t.dictDedupGlobal.Add(1)
		return dictDedupGlobal, -1
	}
	if idx, ok := t.dictHashIndex[hash]; ok && hash != 0 {
		t.dictDedupHits.Add(1)
		t.dictDedupRef.Add(1)
		return dictDedupRef, idx
	}
	if t.globalDictHash == 0 {
		t.globalDictHash = hash
		return dictDedupNone, -1
	}
	old := t.dictHashes[t.dictHashPos]
	if old != 0 {
		delete(t.dictHashIndex, old)
	}
	t.dictHashes[t.dictHashPos] = hash
	t.dictHashIndex[hash] = t.dictHashPos
	t.dictHashPos = (t.dictHashPos + 1) % len(t.dictHashes)
	return dictDedupNone, -1
}

func (t *Trainer) logOnce() bool {
	if t == nil {
		return false
	}
	return t.logged.CompareAndSwap(false, true)
}

func (t *Trainer) getSampleBuf(n int) []byte {
	if t == nil || n <= 0 {
		return make([]byte, n)
	}
	if v := t.samplePool.Get(); v != nil {
		buf := v.([]byte)
		if cap(buf) >= n {
			return buf[:n]
		}
	}
	return make([]byte, n)
}

func (t *Trainer) putSampleBuf(buf []byte) {
	if t == nil || len(buf) == 0 {
		return
	}
	if t.maxRecord > 0 && cap(buf) > t.maxRecord {
		return
	}
	t.samplePool.Put(buf[:0])
}

func (t *Trainer) releaseSamples(samples [][]byte) {
	if t == nil {
		return
	}
	for _, sample := range samples {
		t.putSampleBuf(sample)
	}
}

func (t *Trainer) updateMaxQueueLen() {
	if t == nil {
		return
	}
	cur := uint64(len(t.sampleCh))
	for {
		prev := t.maxQueueLen.Load()
		if cur <= prev {
			return
		}
		if t.maxQueueLen.CompareAndSwap(prev, cur) {
			return
		}
	}
}

func (t *Trainer) recordCollect(started time.Time) {
	if t == nil || !t.measureCollect || started.IsZero() {
		return
	}
	elapsed := uint64(time.Since(started))
	t.collectCount.Add(1)
	t.collectNanos.Add(elapsed)
	for {
		prev := t.collectMaxNanos.Load()
		if elapsed <= prev {
			return
		}
		if t.collectMaxNanos.CompareAndSwap(prev, elapsed) {
			return
		}
	}
}

func (t *Trainer) allowRetrain(now time.Time) bool {
	if t == nil {
		return false
	}
	lastTime, _ := t.lastAcceptTime.Load().(time.Time)
	if !lastTime.IsZero() && now.Sub(lastTime) < MinProfileInterval {
		return false
	}
	bytesSeen := t.totalBytesSeen.Load()
	recordsSeen := t.totalRecordsSeen.Load()
	lastBytes := t.lastAcceptBytes.Load()
	lastRecords := t.lastAcceptRecords.Load()
	if lastTime.IsZero() {
		return true
	}
	if bytesSeen <= lastBytes && recordsSeen <= lastRecords {
		return false
	}
	if bytesSeen-lastBytes < MinProfileBytes && recordsSeen-lastRecords < MinProfileRecords {
		return false
	}
	base := math.Float64frombits(t.rollingRatioBase.Load())
	cur := math.Float64frombits(t.rollingRatioCur.Load())
	if base > 0 && cur > 0 && cur <= base*(1.0+ProfileDriftThreshold) {
		return false
	}
	return true
}

func (t *Trainer) maybeAcceptProfile(profile *ActiveProfile) {
	if t == nil || profile == nil {
		return
	}
	t.attempts.Add(1)
	old, _ := t.lastProfile.Load().(*ActiveProfile)
	if old != nil {
		t.rollingRatioBase.Store(math.Float64bits(old.TotalRatio))
	}
	t.rollingRatioCur.Store(math.Float64bits(profile.TotalRatio))
	if old != nil {
		if profile.TotalRatio > old.TotalRatio*(1.0-ProfileImproveThresh) {
			t.rejects.Add(1)
			t.lastRejectReason.Store("not_better")
			return
		}
	}
	t.acceptProfile(profile)
}

func (t *Trainer) AcceptProfile(profile *ActiveProfile) {
	t.acceptProfile(profile)
}

func (t *Trainer) acceptProfile(profile *ActiveProfile) {
	if t == nil || profile == nil {
		return
	}
	t.lastProfile.Store(profile)
	t.accepts.Add(1)
	t.lastAcceptTime.Store(profile.Timestamp)
	t.lastAcceptBytes.Store(t.totalBytesSeen.Load())
	t.lastAcceptRecords.Store(t.totalRecordsSeen.Load())
	t.rollingRatioBase.Store(math.Float64bits(profile.TotalRatio))
	t.rollingRatioCur.Store(math.Float64bits(profile.TotalRatio))
	t.lastRejectReason.Store("")
}

func (t *Trainer) Stats() TrainerStats {
	if t == nil {
		return TrainerStats{}
	}
	dedupMode := dictDedupMode(t.lastTrainDedupMode.Load())
	dedupFlag := DictUseFlag(t.lastTrainDedupFlag.Load())
	var profileK int
	var profileTotal float64
	var profilePayload float64
	var profileTS time.Time
	var profileReject string
	if p, ok := t.lastProfile.Load().(*ActiveProfile); ok && p != nil {
		profileK = p.K
		profileTotal = p.TotalRatio
		profilePayload = p.PayloadRatio
		profileTS = p.Timestamp
	}
	lastAcceptTime, _ := t.lastAcceptTime.Load().(time.Time)
	if r, ok := t.lastRejectReason.Load().(string); ok {
		profileReject = r
	}
	return TrainerStats{
		Enabled:              t.enabled.Load(),
		Collecting:           t.collecting.Load(),
		Training:             t.training.Load(),
		QueueLen:             len(t.sampleCh),
		QueueCap:             cap(t.sampleCh),
		Enqueued:             t.enqueued.Load(),
		Dropped:              t.dropped.Load(),
		MaxQueueLen:          t.maxQueueLen.Load(),
		TrainCount:           t.trainCount.Load(),
		LastTrainRatio:       math.Float64frombits(t.lastTrainRatio.Load()),
		LastTrainSamples:     t.lastTrainSamples.Load(),
		LastTrainDict:        t.lastTrainDict.Load(),
		LastTrainDictHash:    t.lastTrainDictHash.Load(),
		LastTrainDedupMode:   dedupModeString(dedupMode),
		LastTrainDedupFlag:   dedupFlagString(dedupFlag),
		LastTrainDedupRef:    t.lastTrainDedupRef.Load(),
		DictDedupLookups:     t.dictDedupLookups.Load(),
		DictDedupHits:        t.dictDedupHits.Load(),
		DictDedupGlobal:      t.dictDedupGlobal.Load(),
		DictDedupRef:         t.dictDedupRef.Load(),
		DictDedupCache:       t.dictDedupCache.Load(),
		DictDedupBytes:       t.dictDedupBytes.Load(),
		DictDedupBytesGlobal: t.dictDedupBytesGlobal.Add(0),
		DictDedupBytesRef:    t.dictDedupBytesRef.Add(0),
		DictDedupBytesCache:  t.dictDedupBytesCache.Add(0),
		CollectCount:         t.collectCount.Load(),
		CollectNanos:         t.collectNanos.Load(),
		CollectMaxNanos:      t.collectMaxNanos.Load(),
		ProfileK:             profileK,
		ProfileTotalRatio:    profileTotal,
		ProfilePayloadRatio:  profilePayload,
		ProfileTimestamp:     profileTS,
		ProfileAttempts:      t.attempts.Load(),
		ProfileAccepts:       t.accepts.Load(),
		ProfileRejects:       t.rejects.Load(),
		ProfileRejectReason:  profileReject,
		RollingRatioBaseline: math.Float64frombits(t.rollingRatioBase.Load()),
		RollingRatioCurrent:  math.Float64frombits(t.rollingRatioCur.Load()),
		LastAcceptBytes:      t.lastAcceptBytes.Load(),
		LastAcceptRecords:    t.lastAcceptRecords.Load(),
		LastAcceptTimestamp:  lastAcceptTime,
	}
}

func (t *Trainer) Config() TrainConfig {
	if t == nil {
		return TrainConfig{}
	}
	return TrainConfig{
		TrainBytes:     int(t.targetBytes),
		DictBytes:      t.dictBytes,
		MinRecords:     int(t.minRecords),
		MaxRecordBytes: t.maxRecord,
		SampleStride:   int(t.sampleStride),
		Level:          int(t.level),
	}
}

func (t *Trainer) ActiveProfile() (*ActiveProfile, bool) {
	if t == nil {
		return nil, false
	}
	if p, ok := t.lastProfile.Load().(*ActiveProfile); ok && p != nil {
		return p, true
	}
	return nil, false
}

func dedupModeString(mode dictDedupMode) string {
	switch mode {
	case dictDedupGlobal:
		return "global"
	case dictDedupRef:
		return "ref"
	case dictDedupCache:
		return "cache"
	default:
		return "none"
	}
}

func dedupFlagFromMode(mode dictDedupMode) DictUseFlag {
	switch mode {
	case dictDedupGlobal:
		return DictUseGlobal
	case dictDedupRef:
		return DictUseRef
	case dictDedupCache:
		return DictUseRef
	default:
		return DictUseLocal
	}
}

func dedupFlagString(flag DictUseFlag) string {
	switch flag {
	case DictUseGlobal:
		return "use_global"
	case DictUseRef:
		return "use_ref"
	default:
		return "use_local"
	}
}

func hashSamples(samples [][]byte) uint64 {
	hasher := xxhash.New()
	var lenBuf [4]byte
	for _, sample := range samples {
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(sample)))
		_, _ = hasher.Write(lenBuf[:])
		_, _ = hasher.Write(sample)
	}
	return hasher.Sum64()
}

func (t *Trainer) lookupCachedDict(samplesHash uint64) ([]byte, uint64, bool) {
	if t == nil {
		return nil, 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dictCacheHashes == nil || t.dictCacheDicts == nil {
		return nil, 0, false
	}
	if idx, ok := t.dictCacheIndex[samplesHash]; ok && samplesHash != 0 {
		dict := t.dictCacheDicts[idx]
		dictHash := t.dictCacheDictHashes[idx]
		if len(dict) == 0 {
			return nil, 0, false
		}
		t.dictDedupHits.Add(1)
		t.dictDedupCache.Add(1)
		bytes := uint64(len(dict))
		t.dictDedupBytes.Add(bytes)
		t.dictDedupBytesCache.Add(bytes)
		return dict, dictHash, true
	}
	return nil, 0, false
}

func (t *Trainer) storeCachedDict(samplesHash, dictHash uint64, dict []byte) {
	if t == nil {
		return
	}
	window := t.dictDedupWindow
	if window <= 0 {
		window = DefaultTrainDedupWindow
	}
	if window <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dictCacheHashes == nil {
		t.dictCacheHashes = make([]uint64, window)
		t.dictCacheDicts = make([][]byte, window)
		t.dictCacheDictHashes = make([]uint64, window)
		t.dictCachePos = 0
		t.dictCacheIndex = make(map[uint64]int, window)
	}
	if dictHash == 0 || samplesHash == 0 {
		return
	}
	if len(t.dictCacheHashes) == 0 {
		return
	}
	entry := make([]byte, len(dict))
	copy(entry, dict)
	old := t.dictCacheHashes[t.dictCachePos]
	if old != 0 {
		delete(t.dictCacheIndex, old)
	}
	t.dictCacheHashes[t.dictCachePos] = samplesHash
	t.dictCacheDicts[t.dictCachePos] = entry
	t.dictCacheDictHashes[t.dictCachePos] = dictHash
	t.dictCacheIndex[samplesHash] = t.dictCachePos
	t.dictCachePos = (t.dictCachePos + 1) % len(t.dictCacheHashes)
}
