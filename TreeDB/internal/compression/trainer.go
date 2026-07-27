package compression

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"runtime"
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
	// Keep enough buffered samples to bootstrap a first dict during short,
	// front-end-heavy ingest windows without dropping most candidates.
	DefaultTrainQueue           = 512
	DefaultTrainDedupWindow     = 16
	DefaultTrainMaxHistoryBytes = 128 << 10

	// Bootstrap defaults. These are intentionally smaller than the steady-state
	// TrainBytes/DictBytes targets so dict compression becomes active quickly,
	// reducing sensitivity to TrainBytes tuning.
	DefaultTrainBootstrapBytes     = 32 << 10
	DefaultTrainBootstrapDictBytes = 8 << 10
	// Keep bootstrap record count high enough to evaluate K candidates beyond
	// tiny groups on large-value streams.
	DefaultTrainBootstrapMinRecords = 32

	// Adaptive gating constants for dict+K refresh.
	MinProfileBytes       = 64 << 20 // 64 MiB
	MinProfileRecords     = 250_000  // records
	MinProfileInterval    = 10 * time.Minute
	ProfileDriftThreshold = 0.07 // 7%
	ProfileImproveThresh  = 0.02 // 2% better to accept

	// More aggressive gating when the stream is degraded (e.g., dict compression
	// is paused due to poor observed savings).
	MinProfileBytesDegraded    = 1 << 20 // 1 MiB
	MinProfileRecordsDegraded  = 8_000   // records
	MinProfileIntervalDegraded = 5 * time.Second
)

type Trainer struct {
	enabled             atomic.Bool
	collecting          atomic.Bool
	training            atomic.Bool
	targetBytes         uint64
	minRecords          uint64
	maxRecord           int
	dictBytes           int
	bootstrapBytes      uint64
	bootstrapMinRecords uint64
	bootstrapDictBytes  int
	upgradePending      atomic.Bool
	forceNextTrain      atomic.Bool
	level               zstd.EncoderLevel
	logged              atomic.Bool
	buildDictWS         zstd.BuildDictWorkspace
	dictEncodeWS        zstd.DictEncodeWorkspace

	mu            sync.Mutex
	sampleBytes   uint64
	sampleRecords uint64
	samples       [][]byte
	lastSlabID    uint32

	sampleGen           atomic.Uint64
	sampleStride        uint64
	sampleStrideCounter atomic.Uint64
	sampleCh            chan trainerSample
	trainWG             sync.WaitGroup
	doneCh              chan struct{}
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
	onAccept    atomic.Value // func(*ActiveProfile)

	// workload counters
	totalBytesSeen   atomic.Uint64
	totalRecordsSeen atomic.Uint64

	// anti-thrash gating
	lastAcceptTime    atomic.Value // time.Time
	lastAcceptBytes   atomic.Uint64
	lastAcceptRecords atomic.Uint64
	rollingRatioBase  atomic.Uint64 // math.Float64bits(last_profile_total_ratio)
	rollingRatioCur   atomic.Uint64 // math.Float64bits(observed_total_ratio)
	degraded          atomic.Bool
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

	candidateK            []int
	candidateHistoryBytes []int
	candidateDictBytes    []int
	ioNsPerStoredByte     atomic.Uint64
	encodeNsPerRawByte    float64
	decodeNsPerRawByte    float64
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

	bootstrapBytes := uint64(target)
	if bootstrapBytes > DefaultTrainBootstrapBytes {
		bootstrapBytes = DefaultTrainBootstrapBytes
	}
	bootstrapMinRecords := uint64(minRecords)
	if bootstrapMinRecords > DefaultTrainBootstrapMinRecords {
		bootstrapMinRecords = DefaultTrainBootstrapMinRecords
	}
	bootstrapDictBytes := dictBytes
	if bootstrapDictBytes > DefaultTrainBootstrapDictBytes {
		bootstrapDictBytes = DefaultTrainBootstrapDictBytes
	}

	trainer := &Trainer{
		targetBytes:         uint64(target),
		minRecords:          uint64(minRecords),
		maxRecord:           maxRecord,
		dictBytes:           dictBytes,
		bootstrapBytes:      bootstrapBytes,
		bootstrapMinRecords: bootstrapMinRecords,
		bootstrapDictBytes:  bootstrapDictBytes,
		level:               cfg.Level,
		sampleStride:        uint64(sampleStride),
		sampleCh:            make(chan trainerSample, DefaultTrainQueue),
		doneCh:              make(chan struct{}),
		measureCollect:      metricsEnabled,
		dictDedupWindow:     dedupWindow,
		encodeNsPerRawByte:  opts.EncodeNsPerRawByte,
		decodeNsPerRawByte:  opts.DecodeNsPerRawByte,
	}
	trainer.enabled.Store(true)
	trainer.collecting.Store(true)
	go trainer.run()
	return trainer
}

func (t *Trainer) SetAutotuneCandidates(candidateK, candidateHistoryBytes, candidateDictBytes []int) {
	if t == nil {
		return
	}
	if len(candidateK) > 0 {
		t.candidateK = append([]int(nil), candidateK...)
	}
	if len(candidateHistoryBytes) > 0 {
		t.candidateHistoryBytes = append([]int(nil), candidateHistoryBytes...)
	}
	if len(candidateDictBytes) > 0 {
		t.candidateDictBytes = append([]int(nil), candidateDictBytes...)
	}
}

// SetOnAccept installs a callback invoked when the trainer accepts a new active
// profile. The callback must be fast/non-blocking; it may be called from a
// background goroutine.
func (t *Trainer) SetOnAccept(fn func(*ActiveProfile)) {
	if t == nil || fn == nil {
		return
	}
	t.onAccept.Store(fn)
}

func (t *Trainer) SetAutotuneIOCost(ioNsPerStoredByte float64) {
	if t == nil {
		return
	}
	if ioNsPerStoredByte <= 0 || math.IsNaN(ioNsPerStoredByte) || math.IsInf(ioNsPerStoredByte, 0) {
		return
	}
	t.ioNsPerStoredByte.Store(math.Float64bits(ioNsPerStoredByte))
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

// Wait blocks until the trainer has drained every sample accepted before
// Close and its background worker has stopped. Call Close before Wait.
func (t *Trainer) Wait() {
	if t == nil || t.doneCh == nil {
		return
	}
	<-t.doneCh
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

func (t *Trainer) hasActiveProfile() bool {
	if t == nil {
		return false
	}
	if p, ok := t.lastProfile.Load().(*ActiveProfile); ok && p != nil {
		return true
	}
	return false
}

func (t *Trainer) restartCollecting(slabID uint32) {
	if t == nil || t.closed.Load() || !t.enabled.Load() || t.targetBytes == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.samples) > 0 {
		t.releaseSamples(t.samples)
		t.samples = nil
	}
	t.sampleBytes = 0
	t.sampleRecords = 0
	t.lastSlabID = slabID
	t.sampleGen.Add(1)
	t.sampleStrideCounter.Store(0)
	t.collecting.Store(true)
}

func (t *Trainer) SignalDegraded(slabID uint32) {
	if t == nil || !t.enabled.Load() {
		return
	}
	t.degraded.Store(true)
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
		// Best-effort backpressure: on single-core / constrained environments,
		// a tight producer loop can fill the sample queue faster than the trainer
		// goroutine can drain it, preventing the first dict from ever training.
		//
		// Yield a few times before dropping so we can bootstrap an initial profile
		// without turning dict sampling into a blocking hot-path.
		if !t.hasActiveProfile() {
			for i := 0; i < 16 && len(t.sampleCh) == cap(t.sampleCh); i++ {
				runtime.Gosched()
			}
		}
		if len(t.sampleCh) == cap(t.sampleCh) {
			t.dropped.Add(1)
			t.recordCollect(started)
			return
		}
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
	// appendSample is the only producer of asynchronous training jobs. Once the
	// sample channel is closed and drained, no later Add can race this Wait.
	t.trainWG.Wait()
	close(t.doneCh)
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

	hasProfile := t.hasActiveProfile()
	targetBytes := t.targetBytes
	minRecords := t.minRecords
	dictBytes := t.dictBytes
	if !hasProfile {
		if t.bootstrapBytes > 0 && t.bootstrapBytes < targetBytes {
			targetBytes = t.bootstrapBytes
		}
		if t.bootstrapMinRecords > 0 && t.bootstrapMinRecords < minRecords {
			minRecords = t.bootstrapMinRecords
		}
		if t.bootstrapDictBytes > 0 && t.bootstrapDictBytes < dictBytes {
			dictBytes = t.bootstrapDictBytes
		}
	}

	if t.sampleBytes < targetBytes || t.sampleRecords < minRecords {
		return
	}
	samples := t.samples
	slabID := t.lastSlabID
	level := t.level
	t.samples = nil
	force := hasProfile && t.upgradePending.CompareAndSwap(true, false)
	t.collecting.Store(false)
	t.training.Store(true)
	if force {
		t.forceNextTrain.Store(true)
	}
	t.trainWG.Add(1)
	go func() {
		defer t.trainWG.Done()
		t.train(samples, dictBytes, level, slabID)
	}()
}

func (t *Trainer) train(samples [][]byte, dictBytes int, level zstd.EncoderLevel, slabID uint32) {
	defer func() {
		if t != nil {
			t.training.Store(false)
		}
		if r := recover(); r != nil {
			// Suppress panic from zstd or internal logic, treat as failed training.
			// Log as warning instead of PANIC.
			log.Printf("treedb: dict training skipped stream=%d err=%v\n%s", slabID, r, debug.Stack())
		}
		// Continue collecting if we don't have a usable profile yet, or if the
		// stream is degraded and we want to converge to a better dict/K.
		if t == nil || t.closed.Load() || !t.enabled.Load() {
			return
		}
		if t.degraded.Load() || !t.hasActiveProfile() {
			t.restartCollecting(slabID)
		}
	}()
	if len(samples) == 0 {
		return
	}
	defer t.releaseSamples(samples)

	now := time.Now()
	force := false
	if t != nil {
		force = t.forceNextTrain.Swap(false)
	}
	if !force && !t.allowRetrain(now) {
		return
	}

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

	dictID := slabID + 1
	if dictID == 0 {
		dictID = 1
	}

	t.trainCount.Add(1)

	const minHistoryBytes = 8
	if dictBytes <= 0 {
		dictBytes = DefaultTrainDictBytes
	}
	maxHistoryBytes := dictBytes
	if maxHistoryBytes < minHistoryBytes {
		maxHistoryBytes = minHistoryBytes
	}
	if maxHistoryBytes > DefaultTrainMaxHistoryBytes {
		maxHistoryBytes = DefaultTrainMaxHistoryBytes
	}

	historyCandidates := make([]int, 0, 8)
	if len(t.candidateHistoryBytes) > 0 {
		seen := make(map[int]struct{}, len(t.candidateHistoryBytes))
		for _, v := range t.candidateHistoryBytes {
			if v <= 0 {
				continue
			}
			if v > DefaultTrainMaxHistoryBytes {
				continue
			}
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			historyCandidates = append(historyCandidates, v)
			if v > maxHistoryBytes {
				maxHistoryBytes = v
			}
		}
	} else {
		for _, v := range []int{16 << 10, 32 << 10, 40 << 10} {
			if v > maxHistoryBytes {
				continue
			}
			historyCandidates = append(historyCandidates, v)
		}
		isStd := dictBytes == 16<<10 || dictBytes == 32<<10 || dictBytes == 40<<10
		if !isStd && dictBytes <= maxHistoryBytes {
			historyCandidates = append(historyCandidates, dictBytes)
		}
	}
	if len(historyCandidates) == 0 {
		historyCandidates = append(historyCandidates, dictBytes)
	}

	dictCandidates := make([]int, 0, 4)
	if len(t.candidateDictBytes) > 0 {
		seen := make(map[int]struct{}, len(t.candidateDictBytes))
		for _, v := range t.candidateDictBytes {
			if v < minHistoryBytes {
				continue
			}
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			dictCandidates = append(dictCandidates, v)
		}
	}
	if len(dictCandidates) == 0 {
		dictCandidates = append(dictCandidates, dictBytes)
	}
	if maxHistoryBytes > rawTotal {
		maxHistoryBytes = rawTotal
	}
	if maxHistoryBytes < minHistoryBytes {
		return
	}

	// Build a single max-sized history buffer and slice it for smaller candidates.
	maxCandidate := maxHistoryBytes
	if maxCandidate > rawTotal {
		maxCandidate = rawTotal
	}
	historyMax := make([]byte, 0, maxCandidate)
	for _, sample := range validSamples {
		if len(historyMax) >= maxCandidate {
			break
		}
		need := maxCandidate - len(historyMax)
		if len(sample) > need {
			historyMax = append(historyMax, sample[:need]...)
		} else {
			historyMax = append(historyMax, sample...)
		}
	}
	if len(historyMax) < minHistoryBytes {
		return
	}

	samplesHash := hashSamples(validSamples)
	type dictCandidate struct {
		profile   *ActiveProfile
		fromCache bool
	}
	ioNsPerStoredByte := math.Float64frombits(t.ioNsPerStoredByte.Load())
	candProfiles := make([]dictCandidate, 0, len(historyCandidates)*len(dictCandidates))
	for _, historyBytes := range historyCandidates {
		if historyBytes <= 0 {
			continue
		}
		if historyBytes > len(historyMax) {
			historyBytes = len(historyMax)
		}
		if historyBytes < minHistoryBytes {
			continue
		}
		cacheKey := dictCacheKey(samplesHash, historyBytes)
		dict, dictHash, ok := t.lookupCachedDict(cacheKey)
		fromCache := ok
		if !ok {
			var err error
			dict, err = buildAndValidateDict(dictID, validSamples, historyMax[:historyBytes], level, &t.buildDictWS, &t.dictEncodeWS)
			if err != nil || len(dict) == 0 {
				continue
			}
			dictHash = xxhash.Sum64(dict)
			t.storeCachedDict(cacheKey, dictHash, dict)
		}
		for _, dictCandidateBytes := range dictCandidates {
			shaped, err := shapeAndValidateDict(dict, dictCandidateBytes, level, &t.dictEncodeWS)
			if err != nil || len(shaped) == 0 {
				continue
			}
			profile := ChooseKForDictOptions(shaped, validSamples, ChooseKOptions{
				CandidateK:         t.candidateK,
				IoNsPerStoredByte:  ioNsPerStoredByte,
				EncodeNsPerRawByte: t.encodeNsPerRawByte,
				DecodeNsPerRawByte: t.decodeNsPerRawByte,
				EncoderWorkspace:   &t.dictEncodeWS,
			})
			if profile == nil || len(profile.Dict) == 0 {
				continue
			}
			profile.DictHash = xxhash.Sum64(shaped)
			profile.HistoryBytes = historyBytes
			candProfiles = append(candProfiles, dictCandidate{profile: profile, fromCache: fromCache && len(shaped) == len(dict)})
		}
	}
	if len(candProfiles) == 0 {
		return
	}

	// Select best dict bytes using throughput when IO cost is known; otherwise
	// fall back to ratio-first, speed-second policy.
	const ratioSlack = 0.01
	bestIdx := -1
	bestProfile := candProfiles[0].profile
	bestFromCache := candProfiles[0].fromCache
	bestScore := -1.0
	bestEncCost := math.Inf(1)
	if ioNsPerStoredByte > 0 {
		scores := make([]float64, len(candProfiles))
		encCosts := make([]float64, len(candProfiles))
		for i := range candProfiles {
			p := candProfiles[i].profile
			if p == nil || p.TotalRatio <= 0 {
				continue
			}
			encodeNsPerRaw := 0.0
			if p.EncodeNsEstimate > 0 && p.AvgSampleBytes > 0 {
				encodeNsPerRaw = float64(p.EncodeNsEstimate) / float64(p.AvgSampleBytes)
			}
			denom := encodeNsPerRaw + ioNsPerStoredByte*p.TotalRatio
			if denom <= 0 {
				continue
			}
			scores[i] = 1.0 / denom
			encCosts[i] = encodeNsPerRaw
			if scores[i] > bestScore {
				bestScore = scores[i]
			}
		}
		if bestScore > 0 {
			cut := bestScore * (1.0 - ratioSlack)
			for i := range candProfiles {
				if scores[i] <= 0 || scores[i] < cut {
					continue
				}
				if bestIdx < 0 {
					bestIdx = i
					bestEncCost = encCosts[i]
					continue
				}
				if scores[i] > bestScore {
					bestIdx = i
					bestScore = scores[i]
					bestEncCost = encCosts[i]
					continue
				}
				if encCosts[i] < bestEncCost {
					bestIdx = i
					bestEncCost = encCosts[i]
					continue
				}
				if encCosts[i] == bestEncCost && candProfiles[i].profile.HistoryBytes < candProfiles[bestIdx].profile.HistoryBytes {
					bestIdx = i
					bestEncCost = encCosts[i]
					continue
				}
			}
		}
	}
	if bestIdx < 0 {
		bestTotal := candProfiles[0].profile.TotalRatio
		for i := 1; i < len(candProfiles); i++ {
			if candProfiles[i].profile.TotalRatio < bestTotal {
				bestTotal = candProfiles[i].profile.TotalRatio
			}
		}
		bestCut := bestTotal * (1.0 + ratioSlack)
		for i := range candProfiles {
			p := candProfiles[i].profile
			if p.TotalRatio > bestCut {
				continue
			}
			if bestIdx < 0 {
				bestIdx = i
				continue
			}
			best := candProfiles[bestIdx].profile
			if p.EncodeNsEstimate > 0 && (best.EncodeNsEstimate == 0 || p.EncodeNsEstimate < best.EncodeNsEstimate) {
				bestIdx = i
				continue
			}
			if p.EncodeNsEstimate == best.EncodeNsEstimate && p.HistoryBytes < best.HistoryBytes {
				bestIdx = i
				continue
			}
		}
		if bestIdx < 0 {
			bestIdx = 0
		}
	}
	bestProfile = candProfiles[bestIdx].profile
	bestFromCache = candProfiles[bestIdx].fromCache

	t.lastTrainDictHash.Store(bestProfile.DictHash)
	mode, ref := t.recordDictHash(bestProfile.DictHash)
	dedupMode := mode
	if dedupMode == dictDedupNone && bestFromCache {
		dedupMode = dictDedupCache
	}
	t.lastTrainDedupMode.Store(uint64(dedupMode))
	t.lastTrainDedupFlag.Store(uint64(dedupFlagFromMode(dedupMode)))
	if dedupMode == dictDedupRef && ref >= 0 {
		t.lastTrainDedupRef.Store(uint64(ref))
	} else {
		t.lastTrainDedupRef.Store(0)
	}
	if mode != dictDedupNone {
		bytes := uint64(len(bestProfile.Dict))
		t.dictDedupBytes.Add(bytes)
		switch mode {
		case dictDedupGlobal:
			t.dictDedupBytesGlobal.Add(bytes)
		case dictDedupRef:
			t.dictDedupBytesRef.Add(bytes)
		}
		t.lastTrainSamples.Store(uint64(len(validSamples)))
		t.lastTrainDict.Store(uint64(len(bestProfile.Dict)))
		if t.logOnce() {
			log.Printf("treedb: dict training dedup stream=%d dict_bytes=%d samples=%d hash=%x mode=%s ref=%d",
				slabID,
				len(bestProfile.Dict),
				len(validSamples),
				bestProfile.DictHash,
				dedupModeString(mode),
				ref,
			)
		}
	}

	storedTotal := 0
	var encoded []byte
	for _, sample := range validSamples {
		var err error
		encoded, err = t.dictEncodeWS.EncodeAllWithDict(sample, encoded[:0], bestProfile.Dict, level)
		if err != nil {
			log.Printf("treedb: dict training encode failed stream=%d err=%v", slabID, err)
			return
		}
		storedTotal += len(encoded)
	}
	ratio := 1.0
	if rawTotal > 0 {
		ratio = float64(storedTotal) / float64(rawTotal)
	}
	t.lastTrainRatio.Store(math.Float64bits(ratio))
	t.lastTrainSamples.Store(uint64(len(validSamples)))
	t.lastTrainDict.Store(uint64(len(bestProfile.Dict)))
	if t.logOnce() {
		log.Printf("treedb: dict training trained dict stream=%d dict_bytes=%d samples=%d raw=%d stored=%d ratio=%.3f",
			slabID,
			len(bestProfile.Dict),
			len(validSamples),
			rawTotal,
			storedTotal,
			ratio,
		)
	}
	t.rollingRatioCur.Store(math.Float64bits(ratio))
	t.maybeAcceptProfile(bestProfile)
}

func buildAndValidateDict(dictID uint32, samples [][]byte, history []byte, level zstd.EncoderLevel, buildWS *zstd.BuildDictWorkspace, encodeWS *zstd.DictEncodeWorkspace) (dict []byte, err error) {
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
		// Important: BuildDict does not guarantee it will discover 3 non-zero
		// offsets when content is small/degenerate (it only updates offsets it
		// observes). If we pass zero offsets, it can emit dictionaries that fail
		// to load with "invalid offset in dictionary" (issue #117).
		Offsets:   [3]int{1, 4, 8},
		Level:     level,
		Workspace: buildWS,
	})
	if err != nil || len(dict) == 0 {
		if err != nil {
			log.Printf("treedb: dict training failed stream=%d err=%v", dictID-1, err)
		}
		return nil, err
	}
	if err := validateDict(dict, level, encodeWS); err != nil {
		// Retry with a smaller dict to avoid invalid offset failures.
		reduced := dict
		for i := 0; i < 3 && len(reduced) > 64; i++ {
			reduced = reduced[:len(reduced)/2]
			if err2 := validateDict(reduced, level, encodeWS); err2 == nil {
				return reduced, nil
			}
		}
		log.Printf("treedb: dict training dict rejected stream=%d err=%v", dictID-1, err)
		return nil, err
	}
	return dict, nil
}

func shapeAndValidateDict(dict []byte, dictBytes int, level zstd.EncoderLevel, encodeWS *zstd.DictEncodeWorkspace) ([]byte, error) {
	if len(dict) == 0 {
		return nil, fmt.Errorf("empty dict")
	}
	if dictBytes <= 0 {
		return nil, fmt.Errorf("invalid dict size %d", dictBytes)
	}
	shaped := dict
	if len(dict) > dictBytes {
		shaped = append([]byte(nil), dict[:dictBytes]...)
	} else if len(dict) < dictBytes {
		shaped = make([]byte, dictBytes)
		copy(shaped, dict)
	}
	if err := validateDict(shaped, level, encodeWS); err != nil {
		return nil, err
	}
	return shaped, nil
}

func validateDict(dict []byte, level zstd.EncoderLevel, encodeWS *zstd.DictEncodeWorkspace) error {
	// Verify the dictionary actually works for round-trip.
	// We use a small dummy payload.
	dummy := []byte("test_payload_validation")
	var compressed []byte
	if encodeWS != nil {
		var err error
		compressed, err = encodeWS.EncodeAllWithDict(dummy, nil, dict, level)
		if err != nil {
			return err
		}
	} else {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(level),
			zstd.WithEncoderCRC(false),
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderDict(dict),
		)
		if err != nil {
			return err
		}
		defer enc.Close()
		compressed = enc.EncodeAll(dummy, nil)
	}

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
	degraded := t.degraded.Load()
	minInterval := MinProfileInterval
	minBytes := uint64(MinProfileBytes)
	minRecords := uint64(MinProfileRecords)
	if degraded {
		minInterval = MinProfileIntervalDegraded
		minBytes = uint64(MinProfileBytesDegraded)
		minRecords = uint64(MinProfileRecordsDegraded)
		if t.targetBytes > 0 {
			dynBytes := t.targetBytes / 2
			if dynBytes < 256<<10 {
				dynBytes = 256 << 10
			}
			if dynBytes < minBytes {
				minBytes = dynBytes
			}
		}
		if t.minRecords > 0 {
			dynRecords := t.minRecords * 4
			if dynRecords < 2_000 {
				dynRecords = 2_000
			}
			if dynRecords < minRecords {
				minRecords = dynRecords
			}
		}
	} else {
		if t.targetBytes > 0 {
			dynBytes := t.targetBytes * 4
			if dynBytes < 4<<20 {
				dynBytes = 4 << 20
			}
			if dynBytes < minBytes {
				minBytes = dynBytes
			}
		}
		if t.minRecords > 0 {
			dynRecords := t.minRecords * 8
			if dynRecords < 20_000 {
				dynRecords = 20_000
			}
			if dynRecords < minRecords {
				minRecords = dynRecords
			}
		}
		if minInterval > 2*time.Minute {
			minInterval = 2 * time.Minute
		}
	}
	if !lastTime.IsZero() && now.Sub(lastTime) < minInterval {
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
	if bytesSeen-lastBytes < minBytes && recordsSeen-lastRecords < minRecords {
		return false
	}
	if degraded {
		return true
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
	first := !t.hasActiveProfile()
	t.lastProfile.Store(profile)
	t.accepts.Add(1)
	t.degraded.Store(false)
	t.lastAcceptTime.Store(profile.Timestamp)
	t.lastAcceptBytes.Store(t.totalBytesSeen.Load())
	t.lastAcceptRecords.Store(t.totalRecordsSeen.Load())
	t.rollingRatioBase.Store(math.Float64bits(profile.TotalRatio))
	t.rollingRatioCur.Store(math.Float64bits(profile.TotalRatio))
	t.lastRejectReason.Store("")
	if first && (t.bootstrapBytes < t.targetBytes || t.bootstrapMinRecords < t.minRecords || t.bootstrapDictBytes < t.dictBytes) {
		t.upgradePending.Store(true)
		// Bootstrap training uses a small sample window to get an initial dict
		// quickly. Once the first profile is accepted, continue collecting so the
		// trainer can run a fuller follow-up pass and potentially improve dict
		// size/history/K from steady-state data.
		t.collecting.Store(true)
	}
	if v := t.onAccept.Load(); v != nil {
		if fn, ok := v.(func(*ActiveProfile)); ok && fn != nil {
			fn(profile)
		}
	}
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

func dictCacheKey(samplesHash uint64, dictBytes int) uint64 {
	if samplesHash == 0 {
		return 0
	}
	const prime = uint64(0x9e3779b97f4a7c15)
	return samplesHash ^ (uint64(uint32(dictBytes)) * prime)
}

func (t *Trainer) lookupCachedDict(cacheKey uint64) ([]byte, uint64, bool) {
	if t == nil {
		return nil, 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dictCacheHashes == nil || t.dictCacheDicts == nil {
		return nil, 0, false
	}
	if idx, ok := t.dictCacheIndex[cacheKey]; ok && cacheKey != 0 {
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

func (t *Trainer) storeCachedDict(cacheKey, dictHash uint64, dict []byte) {
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
	if dictHash == 0 || cacheKey == 0 {
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
	t.dictCacheHashes[t.dictCachePos] = cacheKey
	t.dictCacheDicts[t.dictCachePos] = entry
	t.dictCacheDictHashes[t.dictCachePos] = dictHash
	t.dictCacheIndex[cacheKey] = t.dictCachePos
	t.dictCachePos = (t.dictCachePos + 1) % len(t.dictCacheHashes)
}
