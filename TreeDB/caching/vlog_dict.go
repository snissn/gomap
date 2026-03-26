package caching

import (
	"context"
	"log"
	"math/bits"
	"time"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	// Scale trainer sampling by payload bytes so large batches of small values
	// can bootstrap a dictionary quickly.
	valueLogDictCollectMinPerBatchRecords = 32
	// Large batches of tiny values (e.g. forced-pointer streams) need more than a
	// couple thousand samples to hit bootstrap bytes quickly. Keep a hard cap to
	// bound hot-path CPU even when TrainBytes is large.
	valueLogDictCollectMaxPerBatchRecords = 16384
	valueLogDictCollectMinPerBatchBytes   = 32 << 10
	// Once a dictionary has been published, large payload streams are better
	// judged by observed frame ratios than by byte-alphabet heuristics.
	valueLogDictClassifierLargePayloadBypassMin = 32 << 10
	// Large payload streams recover slower from pause/hold when probe intervals
	// are sized for generic traffic. Clamp probe cadence for large records so
	// dict mode can re-engage earlier after a transient degraded period.
	valueLogDictLargeProbeMinPayloadBytes   = 16 << 10
	valueLogDictLargeProbeIntervalClampByte = 2 << 20
)

type dictStoreWriter interface {
	PutDictBytes(context.Context, []byte) (uint64, error)
	SetCurrent(context.Context, uint64) error
}

type dictStoreK interface {
	SetK(context.Context, uint64, int) error
	GetK(context.Context, uint64) (int, error)
}

func (db *DB) valueLogDictTrainingEnabled() bool {
	// Dict training is enabled when TrainBytes > 0. caching.Open defaults this
	// to a safe value for dict/auto compression modes so "turning it on" does
	// not require additional flag choreography.
	if db == nil || db.valueLogDictTrain.TrainBytes <= 0 {
		return false
	}
	if !db.valueLogEnabled() {
		return false
	}
	if db.dictStore == nil {
		return false
	}
	// Dict compression is only used for split value-log records.
	if !db.splitValueLogEnabled() {
		return false
	}
	return true
}

func likelyCompressibleSample(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	n := len(value)
	if n > 512 {
		n = 512
	}
	var seen [4]uint64
	for i := 0; i < n; i++ {
		b := value[i]
		seen[b>>6] |= 1 << (b & 63)
	}
	unique := bits.OnesCount64(seen[0]) + bits.OnesCount64(seen[1]) + bits.OnesCount64(seen[2]) + bits.OnesCount64(seen[3])
	// Heuristic: if the sample uses most of the byte alphabet, it's likely
	// high-entropy / already-compressed data (where zstd dictionaries won't help).
	if unique > 200 {
		return false
	}
	return true
}

func saturatingRawPayloadBytes(records []valuelog.Record) uint64 {
	total := uint64(0)
	for i := range records {
		n := uint64(len(records[i].Value))
		if total > ^uint64(0)-n {
			return ^uint64(0)
		}
		total += n
	}
	return total
}

func (db *DB) valueLogDictMinSavingsRatio() float64 {
	if db == nil {
		return 0.02
	}
	if db.valueLogDictMinPayloadSavings > 0 {
		return db.valueLogDictMinPayloadSavings
	}
	if db.forceValueLogPointers || db.disableJournal {
		return 0.05
	}
	return 0.02
}

func (db *DB) valueLogDictHasPublishedDict() bool {
	return db != nil && db.valueLogDictLastAppliedDictID.Load() != 0
}

func (db *DB) valueLogDictIgnoreValueForSignal(value []byte) bool {
	if db == nil || len(value) == 0 {
		return false
	}
	// Outer-leaf pages (fixed 4KiB pages) are structurally different from large
	// non-page payloads and can dominate dict signal/training in pointer-heavy
	// runs. Keep them on block codecs and avoid letting them steer dict state.
	return db.indexOuterLeavesInValueLog && len(value) == page.PageSize
}

func (db *DB) seedVlogCompressionSelectorsDictRatio(payloadRatio, totalRatio float64) {
	if db == nil {
		return
	}
	seedRatio := payloadRatio
	if seedRatio <= 0 {
		seedRatio = totalRatio
	}
	if totalRatio > seedRatio {
		seedRatio = totalRatio
	}
	seedRatio = normalizeMetricRatio(seedRatio)
	// Keep selector seeding conservative: if the active profile ratio is close
	// to raw, defer to normal per-frame selector learning.
	if seedRatio >= 0.98 {
		return
	}
	for i := range db.lanes {
		if s := db.lanes[i].vlogCompressionSelector; s != nil {
			s.seedDictCandidate(seedRatio)
		}
	}
}

func (db *DB) armValueLogDictPauseBytes(pauseBytes uint64) {
	if db == nil {
		return
	}
	if pauseBytes == 0 {
		pause := db.valueLogDictMetricsPauseBytes
		if pause <= 0 {
			pauseBytes = 64 << 20
		} else {
			pauseBytes = uint64(pause)
		}
	}
	for {
		cur := db.valueLogDictPauseRemaining.Load()
		if cur >= pauseBytes {
			break
		}
		if db.valueLogDictPauseRemaining.CompareAndSwap(cur, pauseBytes) {
			break
		}
	}
	if db.valueLogDictProbeBytes > 0 {
		db.valueLogDictProbeRemaining.Store(db.valueLogDictProbeBytes)
	}
}

func (db *DB) armValueLogDictIncompressibleHoldBytes(holdBytes uint64) {
	if db == nil {
		return
	}
	if holdBytes == 0 {
		holdBytes = db.valueLogDictIncompressibleHoldBytes
		if holdBytes == 0 {
			return
		}
	}
	for {
		cur := db.valueLogDictIncompressibleHoldRemaining.Load()
		if cur >= holdBytes {
			break
		}
		if db.valueLogDictIncompressibleHoldRemaining.CompareAndSwap(cur, holdBytes) {
			break
		}
	}
	probeBytes := db.valueLogDictIncompressibleProbeBytes
	if probeBytes == 0 {
		return
	}
	if probeBytes > holdBytes {
		probeBytes = holdBytes
	}
	db.valueLogDictIncompressibleProbeRemaining.Store(probeBytes)
	db.valueLogDictIncompressibleHolds.Add(1)
}

func (db *DB) valueLogDictIncompressibleDecision(rawLen uint64, allowProbe bool) (attempt bool, probe bool, holding bool) {
	if db == nil || rawLen == 0 {
		return true, false, false
	}
	remaining := db.valueLogDictIncompressibleHoldRemaining.Load()
	for remaining > 0 {
		next := uint64(0)
		if rawLen < remaining {
			next = remaining - rawLen
		}
		if db.valueLogDictIncompressibleHoldRemaining.CompareAndSwap(remaining, next) {
			db.valueLogDictIncompressibleBypassBytes.Add(rawLen)
			if !allowProbe {
				return false, false, true
			}
			probeBytes := db.valueLogDictIncompressibleProbeBytes
			if probeBytes == 0 {
				return false, false, true
			}
			probeBytes = valueLogDictProbeIntervalForPayload(probeBytes, rawLen)
			probeRemaining := db.valueLogDictIncompressibleProbeRemaining.Load()
			for {
				if probeRemaining > probeBytes {
					if db.valueLogDictIncompressibleProbeRemaining.CompareAndSwap(probeRemaining, probeBytes) {
						probeRemaining = probeBytes
					} else {
						probeRemaining = db.valueLogDictIncompressibleProbeRemaining.Load()
					}
					continue
				}
				if probeRemaining <= rawLen {
					nextProbe := probeBytes
					if next > 0 && nextProbe > next {
						nextProbe = next
					}
					if db.valueLogDictIncompressibleProbeRemaining.CompareAndSwap(probeRemaining, nextProbe) {
						return true, true, true
					}
				} else if db.valueLogDictIncompressibleProbeRemaining.CompareAndSwap(probeRemaining, probeRemaining-rawLen) {
					return false, false, true
				}
				probeRemaining = db.valueLogDictIncompressibleProbeRemaining.Load()
			}
		}
		remaining = db.valueLogDictIncompressibleHoldRemaining.Load()
	}
	return true, false, false
}

func (db *DB) valueLogDictClassifierBypass(value []byte, probeCompression bool) bool {
	if db == nil || probeCompression {
		return false
	}
	if db.valueLogDictIgnoreValueForSignal(value) {
		return false
	}
	if db.valueLogDictHasPublishedDict() && len(value) >= valueLogDictClassifierLargePayloadBypassMin {
		return false
	}
	if attempt, _, holding := db.valueLogDictIncompressibleDecision(uint64(len(value)), false); holding && !attempt {
		return true
	}
	// Tiny values are already cheap; avoid classifier churn.
	if len(value) < 4096 {
		return false
	}
	db.valueLogDictClassifySampled.Add(1)
	if likelyCompressibleSample(value) {
		db.valueLogDictIncompressibleHitStreak.Store(0)
		return false
	}
	hits := db.valueLogDictIncompressibleHitStreak.Add(1)
	db.valueLogDictIncompressibleHits.Add(1)
	if hits >= 1 {
		db.armValueLogDictIncompressibleHoldBytes(0)
	}
	db.armValueLogDictPauseBytes(0)
	db.valueLogDictClassifySkipped.Add(1)
	return true
}

func (db *DB) shouldBypassValueLogDictForValue(value []byte, probeCompression bool) bool {
	return db.valueLogDictClassifierBypass(value, probeCompression)
}

func (db *DB) shouldBypassValueLogDictForRecords(records []valuelog.Record, probeCompression bool) bool {
	if db == nil || probeCompression || len(records) == 0 {
		return false
	}
	rawBytes := saturatingRawPayloadBytes(records)
	if db.valueLogDictHasPublishedDict() && rawBytes/uint64(len(records)) >= valueLogDictClassifierLargePayloadBypassMin {
		return false
	}
	if attempt, _, holding := db.valueLogDictIncompressibleDecision(rawBytes, false); holding && !attempt {
		return true
	}
	step := len(records) / 4
	if step < 1 {
		step = 1
	}
	samples := 0
	incompressible := 0
	for i := 0; i < len(records) && samples < 4; i += step {
		v := records[i].Value
		if db.valueLogDictIgnoreValueForSignal(v) {
			continue
		}
		if len(v) < 4096 {
			continue
		}
		samples++
		if !likelyCompressibleSample(v) {
			incompressible++
		}
	}
	if samples == 0 {
		return false
	}
	// Count classification decisions (not payload samples) so sampled/skipped share units.
	db.valueLogDictClassifySampled.Add(1)
	// Bypass dict work when sampled payloads are predominantly high-entropy.
	if incompressible*4 >= samples*3 {
		db.valueLogDictIncompressibleHits.Add(1)
		hits := db.valueLogDictIncompressibleHitStreak.Add(1)
		if hits >= 1 {
			db.armValueLogDictIncompressibleHoldBytes(0)
		}
		db.armValueLogDictPauseBytes(0)
		db.valueLogDictClassifySkipped.Add(1)
		return true
	}
	db.valueLogDictIncompressibleHitStreak.Store(0)
	return false
}

func (db *DB) valueLogDictCollectSamples(records []valuelog.Record) {
	if db == nil {
		return
	}
	tr := db.valueLogDictTrainer
	if tr == nil || !tr.ShouldCollect() {
		return
	}
	if db.valueLogDictIncompressibleHoldRemaining.Load() > 0 {
		return
	}
	paused := db.valueLogDictPaused()
	if paused && !db.valueLogDictShouldCollectPausedBatch(len(records)) {
		return
	}
	// Seed the trainer's IO cost model early so the initial dict/K selection can
	// optimize for end-to-end throughput (encode + IO), rather than falling back
	// to the decode-cost heuristic when no profile has been published yet.
	//
	// This avoids pathological small-K choices (e.g. k=2/4) that increase frame
	// overhead and reduce write throughput for small values.
	if db.valueLogAutotuneOptions.Mode != valuelog.AutotuneOff {
		tr.SetAutotuneIOCost(db.valueLogAutotuneMetrics.snapshot().IoNsPerStoredByte)
	}
	stride := db.valueLogDictSampleStride
	if stride <= 1 {
		stride = 1
	}
	var base uint64
	if stride > 1 {
		n := uint64(len(records))
		if n == 0 {
			return
		}
		// One atomic for the entire batch: treat the stride counter as a global
		// record index, then sample records where (index % stride) == 0.
		base = db.valueLogDictSampleStrideCount.Add(n) - n
	}
	collectBudget := db.valueLogDictCollectBudget(records, paused)
	if collectBudget <= 0 {
		return
	}
	collected := 0
	for i := range records {
		if stride > 1 && (base+uint64(i)+1)%stride != 0 {
			continue
		}
		v := records[i].Value
		if db.valueLogDictIgnoreValueForSignal(v) {
			continue
		}
		if db.valueLogDictClassifierBypass(v, false) {
			// One high-entropy sample is enough to stop this collect pass and keep
			// the write path cheap on incompressible streams.
			return
		}
		tr.Collect(v)
		collected++
		if collected >= collectBudget {
			return
		}
	}
}

func (db *DB) valueLogDictCollectBudget(records []valuelog.Record, paused bool) int {
	n := len(records)
	if n == 0 {
		return 0
	}
	if paused {
		if n > 1 {
			return 1
		}
		return n
	}

	targetBytes := compression.DefaultTrainBootstrapBytes
	if db != nil && db.valueLogDictTrain.TrainBytes > 0 && db.valueLogDictTrain.TrainBytes < targetBytes {
		targetBytes = db.valueLogDictTrain.TrainBytes
	}
	if targetBytes < valueLogDictCollectMinPerBatchBytes {
		targetBytes = valueLogDictCollectMinPerBatchBytes
	}

	rawBytes := saturatingRawPayloadBytes(records)
	avgBytes := uint64(1)
	if rawBytes > 0 {
		avgBytes = rawBytes / uint64(n)
		if avgBytes == 0 {
			avgBytes = 1
		}
	}

	// Use ceil division so small values don't under-collect and delay the first
	// dictionary publication.
	budget := (uint64(targetBytes) + avgBytes - 1) / avgBytes
	if budget < valueLogDictCollectMinPerBatchRecords {
		budget = valueLogDictCollectMinPerBatchRecords
	}
	if budget > valueLogDictCollectMaxPerBatchRecords {
		budget = valueLogDictCollectMaxPerBatchRecords
	}
	if budget > uint64(n) {
		budget = uint64(n)
	}
	return int(budget)
}

func (db *DB) valueLogDictCollectSample(value []byte) {
	if db == nil {
		return
	}
	tr := db.valueLogDictTrainer
	if tr == nil || !tr.ShouldCollect() {
		return
	}
	if db.valueLogDictIncompressibleHoldRemaining.Load() > 0 {
		return
	}
	if db.valueLogAutotuneOptions.Mode != valuelog.AutotuneOff {
		tr.SetAutotuneIOCost(db.valueLogAutotuneMetrics.snapshot().IoNsPerStoredByte)
	}
	stride := db.valueLogDictSampleStride
	if stride <= 1 {
		stride = 1
	}
	if stride > 1 && db.valueLogDictSampleStrideCount.Add(1)%stride != 0 {
		return
	}
	if db.valueLogDictPaused() && !db.valueLogDictShouldCollectPaused() {
		return
	}
	if db.valueLogDictIgnoreValueForSignal(value) {
		return
	}
	if db.valueLogDictClassifierBypass(value, false) {
		return
	}
	tr.Collect(value)
}

func (db *DB) ensureValueLogDictTrainer() {
	if db == nil || !db.valueLogDictTrainingEnabled() {
		return
	}
	db.valueLogDictTrainerMu.Lock()
	defer db.valueLogDictTrainerMu.Unlock()
	if db.valueLogDictTrainer != nil {
		return
	}
	if db.valueLogDictKickCh == nil {
		db.valueLogDictKickCh = make(chan struct{}, 1)
	}
	// Trainer only needs an encoder level; use SpeedFastest to minimize CPU overhead
	// for value-log dict compression (workloads are frequently CPU-bound).
	cfg := compression.Config{Kind: compression.KindZSTD, Level: zstd.SpeedFastest}
	trainCfg := db.valueLogDictTrain
	stride := trainCfg.SampleStride
	if stride <= 1 {
		stride = 1
	}
	db.valueLogDictSampleStride = uint64(stride)
	db.valueLogDictSampleStrideCount.Store(0)
	// Apply sample stride gating in the write path so we don't run the
	// compressibility heuristic on every record (the trainer itself samples
	// every Collect call when SampleStride=1).
	trainCfg.SampleStride = 1
	tr := compression.NewTrainer(trainCfg, cfg, false, false)
	if tr == nil {
		return
	}
	tr.SetOnAccept(func(_ *compression.ActiveProfile) {
		// Publish accepted profiles immediately so short ingest benchmarks can
		// start writing dict frames before teardown.
		db.applyValueLogDictProfile()
		db.valueLogDictKick()
	})
	candidateK := db.valueLogDictCandidateK()
	if len(candidateK) > 0 {
		seen := make(map[int]struct{}, len(candidateK))
		filtered := make([]int, 0, len(candidateK))
		for _, k := range candidateK {
			k = db.clampValueLogDictK(k)
			if k <= 0 {
				continue
			}
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			filtered = append(filtered, k)
		}
		candidateK = filtered
	}
	tr.SetAutotuneCandidates(candidateK, db.valueLogAutotuneOptions.CandidateHistoryBytes, db.valueLogAutotuneOptions.CandidateDictBytes)
	db.valueLogDictTrainer = tr
	db.valueLogDictMetrics = compression.NewMetrics(compression.MetricsOptions{
		AdaptiveRatio:  db.valueLogDictAdaptiveRatio,
		WindowBytes:    db.valueLogDictMetricsWindow,
		MinRecords:     db.valueLogDictMetricsMinRecords,
		PauseBytes:     db.valueLogDictMetricsPauseBytes,
		MetricsEnabled: false,
	})
	db.valueLogDictMetrics.SetSlab(1)
	db.wg.Add(1)
	go db.valueLogDictLoop()
}

func (db *DB) valueLogDictCandidateK() []int {
	if db == nil {
		return nil
	}
	defaultCandidateK := []int{1, 2, 4, 8, 16, 32}
	forcePointerCandidateK := []int{8, 16, 32, 64, 96, 128}
	if len(db.valueLogAutotuneOptions.CandidateK) > 0 {
		if db.forceValueLogPointers && !db.valueLogAutotuneCandidateKSet && intSlicesEqual(db.valueLogAutotuneOptions.CandidateK, defaultCandidateK) {
			out := make([]int, len(forcePointerCandidateK))
			copy(out, forcePointerCandidateK)
			return out
		}
		out := make([]int, len(db.valueLogAutotuneOptions.CandidateK))
		copy(out, db.valueLogAutotuneOptions.CandidateK)
		return out
	}
	// Force-pointer mode is write-heavy and benefits from evaluating larger frame
	// group sizes. Avoid very small K defaults that bias toward read cost.
	if db.forceValueLogPointers {
		out := make([]int, len(forcePointerCandidateK))
		copy(out, forcePointerCandidateK)
		return out
	}
	return nil
}

func intSlicesEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (db *DB) valueLogDictLoop() {
	defer db.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
		case <-db.valueLogDictKickCh:
		}
		db.applyValueLogDictProfile()
	}
}

func (db *DB) valueLogDictKick() {
	if db == nil || db.valueLogDictKickCh == nil {
		return
	}
	select {
	case db.valueLogDictKickCh <- struct{}{}:
	default:
	}
}

func (db *DB) applyValueLogDictProfile() {
	if db == nil {
		return
	}
	db.valueLogDictTrainerMu.Lock()
	tr := db.valueLogDictTrainer
	store := db.dictStore
	db.valueLogDictTrainerMu.Unlock()
	if tr == nil || store == nil {
		return
	}
	writer, ok := store.(dictStoreWriter)
	if !ok {
		return
	}
	profile, ok := tr.ActiveProfile()
	if !ok || profile == nil || len(profile.Dict) == 0 {
		return
	}
	db.seedVlogCompressionSelectorsDictRatio(profile.PayloadRatio, profile.TotalRatio)
	ioNsPerStoredByte := 0.0
	if db.valueLogAutotuneOptions.Mode != valuelog.AutotuneOff {
		ioNsPerStoredByte = db.valueLogAutotuneMetrics.snapshot().IoNsPerStoredByte
		tr.SetAutotuneIOCost(ioNsPerStoredByte)
	}
	profileK := db.clampValueLogDictK(profile.K)
	candidate := db.valueLogAutotuneCandidate(profile, profileK)
	if candidate == nil {
		return
	}
	prevHash := db.valueLogDictLastAppliedDictHash.Load()
	if prevHash == profile.DictHash {
		// Dict bytes unchanged; allow updating K for the current dict.
		if profileK <= 1 {
			return
		}
		if curK := int(db.valueLogDictCurrentK.Load()); curK == profileK {
			return
		}
		if !db.valueLogAutotuneShouldSwitch(candidate, ioNsPerStoredByte) {
			return
		}
		if ks, ok := store.(dictStoreK); ok {
			dictID := db.valueLogDictLastAppliedDictID.Load()
			if dictID == 0 {
				dictID = db.dictCurrentCached.Load()
			}
			if dictID == 0 {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := ks.SetK(ctx, dictID, profileK); err != nil {
				db.reportError(err)
				return
			}
			db.valueLogDictCurrentK.Store(uint32(profileK))
			db.valueLogDictKMu.Lock()
			if db.valueLogDictKCache == nil {
				db.valueLogDictKCache = make(map[uint64]int)
			}
			db.valueLogDictKCache[dictID] = profileK
			db.valueLogDictKMu.Unlock()
			db.valueLogDictLastKUpdateUnixNano.Store(time.Now().UnixNano())
			log.Printf("treedb: value-log dict updated k dict_id=%d k=%d", dictID, profileK)
		}
		db.valueLogAutotuneRecordSwitch(candidate)
		return
	}
	minSavings := db.valueLogDictMinSavingsRatio()
	if profile.PayloadRatio >= 1.0-minSavings {
		// Do not publish no-op dictionaries (common for incompressible payloads).
		db.valueLogDictLastAppliedDictHash.Store(profile.DictHash)
		return
	}
	if !db.valueLogAutotuneShouldSwitch(candidate, ioNsPerStoredByte) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dictID, err := writer.PutDictBytes(ctx, profile.Dict)
	if err != nil {
		db.reportError(err)
		return
	}
	if err := writer.SetCurrent(ctx, dictID); err != nil {
		db.reportError(err)
		return
	}
	// Make new dictionaries visible to the write path immediately. We intentionally
	// avoid per-write dictdb reads (currentDictID refreshes only every N uses), so
	// a background publish must also refresh the cached current ID.
	db.dictCurrentCached.Store(dictID)
	db.dictCurrentOps.Store(0)
	if ks, ok := store.(dictStoreK); ok {
		if err := ks.SetK(ctx, dictID, profileK); err != nil {
			db.reportError(err)
		}
	}
	db.valueLogDictLastAppliedDictHash.Store(profile.DictHash)
	db.valueLogDictLastAppliedDictID.Store(dictID)
	db.valueLogDictCurrentK.Store(uint32(profileK))
	db.valueLogDictLastPublishUnixNano.Store(time.Now().UnixNano())

	// Reset ratio tracking for the new dict.
	db.valueLogDictMetrics.SetSlab(1)
	db.valueLogDictMetrics.Reset(1)

	log.Printf("treedb: value-log dict published dict_id=%d k=%d payload_ratio=%.3f total_ratio=%.3f",
		dictID, profileK, profile.PayloadRatio, profile.TotalRatio)
	db.valueLogAutotuneRecordSwitch(candidate)
}

func (db *DB) valueLogDictPaused() bool {
	if db == nil {
		return false
	}
	return db.valueLogDictPauseRemaining.Load() > 0
}

func (db *DB) valueLogDictShouldCollectPaused() bool {
	if db == nil {
		return false
	}
	if db.valueLogDictPausedSampleStride <= 1 {
		return true
	}
	return db.valueLogDictPausedSampleCounter.Add(1)%db.valueLogDictPausedSampleStride == 0
}

func (db *DB) valueLogDictShouldCollectPausedBatch(records int) bool {
	if db == nil {
		return false
	}
	if records <= 0 {
		return false
	}
	if db.valueLogDictPausedSampleStride <= 1 {
		return true
	}
	n := uint64(records)
	next := db.valueLogDictPausedSampleCounter.Add(n)
	prev := next - n
	stride := db.valueLogDictPausedSampleStride
	return prev/stride != next/stride
}

// valueLogDictShouldAttemptCompression consumes pause bytes (when set) and returns
// whether dictionary compression should be attempted, including periodic probes
// while paused.
//
// Returns:
//   - attemptCompression: true if the caller should attempt dict compression.
//   - probeCompression: true if this attempt is a paused-state probe.
//   - paused: true if pauseRemaining was non-zero when called (even if it reaches
//     zero due to consumption during this call).
func (db *DB) valueLogDictShouldAttemptCompression(rawLen int) (bool, bool, bool) {
	if db == nil || rawLen <= 0 {
		return true, false, false
	}
	rawBytes := uint64(rawLen)
	attemptIncompressible, probeIncompressible, _ := db.valueLogDictIncompressibleDecision(rawBytes, true)
	if !attemptIncompressible {
		return false, false, false
	}
	if probeIncompressible {
		return true, true, db.valueLogDictPauseRemaining.Load() > 0
	}
	remaining := db.valueLogDictPauseRemaining.Load()
	for remaining > 0 {
		next := uint64(0)
		if rawBytes < remaining {
			next = remaining - rawBytes
		}
		if db.valueLogDictPauseRemaining.CompareAndSwap(remaining, next) {
			probeBytes := db.valueLogDictProbeBytes
			if probeBytes == 0 {
				return false, false, true
			}
			probeBytes = valueLogDictProbeIntervalForPayload(probeBytes, rawBytes)
			probeRemaining := db.valueLogDictProbeRemaining.Load()
			for {
				if probeRemaining > probeBytes {
					if db.valueLogDictProbeRemaining.CompareAndSwap(probeRemaining, probeBytes) {
						probeRemaining = probeBytes
					} else {
						probeRemaining = db.valueLogDictProbeRemaining.Load()
					}
					continue
				}
				if probeRemaining <= rawBytes {
					if db.valueLogDictProbeRemaining.CompareAndSwap(probeRemaining, probeBytes) {
						return true, true, true
					}
				} else if db.valueLogDictProbeRemaining.CompareAndSwap(probeRemaining, probeRemaining-rawBytes) {
					return false, false, true
				}
				probeRemaining = db.valueLogDictProbeRemaining.Load()
			}
		}
		remaining = db.valueLogDictPauseRemaining.Load()
	}
	return true, false, false
}

func valueLogDictProbeIntervalForPayload(baseProbeBytes, rawBytes uint64) uint64 {
	if baseProbeBytes == 0 {
		return 0
	}
	probeBytes := baseProbeBytes
	if rawBytes >= valueLogDictLargeProbeMinPayloadBytes && probeBytes > valueLogDictLargeProbeIntervalClampByte {
		probeBytes = valueLogDictLargeProbeIntervalClampByte
	}
	// Avoid probe-on-every-write behavior when payload meets/exceeds the clamped
	// interval; keep at least one full payload between probes.
	if rawBytes > 0 && probeBytes <= rawBytes {
		if rawBytes == ^uint64(0) {
			return rawBytes
		}
		probeBytes = rawBytes + 1
	}
	return probeBytes
}

func (db *DB) valueLogDictObservePayload(rawPayloadBytes, storedPayloadBytes uint64, records int) {
	if db == nil || db.valueLogDictAdaptiveRatio <= 0 {
		return
	}
	if rawPayloadBytes == 0 || records <= 0 {
		return
	}
	if storedPayloadBytes == 0 {
		storedPayloadBytes = rawPayloadBytes
	}
	pause := db.valueLogDictMetrics.Add(
		1,
		int(rawPayloadBytes),
		int(storedPayloadBytes),
		records,
		0,
		0,
	)
	if pause == 0 {
		return
	}
	// Pause dict compression for subsequent frames.
	//
	// NOTE: We intentionally do not immediately retrigger training here. On
	// low-savings / incompressible streams, repeatedly re-collecting samples can
	// dominate CPU even while compression is paused. A future optimization can
	// re-enable retraining with backoff / probe budgets.
	db.armValueLogDictPauseBytes(pause)
	if tr := db.valueLogDictTrainer; tr != nil {
		tr.SignalDegraded(1)
	}
}

func (db *DB) valueLogDictK(dictID uint64) int {
	if dictID == 0 {
		return 1
	}
	if db == nil {
		return 1
	}
	if lastID := db.valueLogDictLastAppliedDictID.Load(); lastID == dictID {
		if k := int(db.valueLogDictCurrentK.Load()); k > 0 {
			return k
		}
	}
	db.valueLogDictKMu.RLock()
	if db.valueLogDictKCache != nil {
		if k, ok := db.valueLogDictKCache[dictID]; ok && k > 0 {
			db.valueLogDictKMu.RUnlock()
			return k
		}
	}
	db.valueLogDictKMu.RUnlock()

	if ks, ok := db.dictStore.(dictStoreK); ok {
		if k, err := ks.GetK(context.Background(), dictID); err == nil && k > 0 {
			k = db.clampValueLogDictK(k)
			db.valueLogDictKMu.Lock()
			if db.valueLogDictKCache == nil {
				db.valueLogDictKCache = make(map[uint64]int)
			}
			db.valueLogDictKCache[dictID] = k
			db.valueLogDictKMu.Unlock()
			return k
		}
	}
	return 1
}

func (db *DB) clampValueLogDictK(k int) int {
	if k <= 1 {
		return 1
	}
	maxK := valuelog.MaxFrameK
	if db != nil && db.valueLogDictMaxK > 0 && db.valueLogDictMaxK < maxK {
		maxK = db.valueLogDictMaxK
	}
	if k > maxK {
		return maxK
	}
	return k
}

func (db *DB) chooseValueLogDictWriteK(baseK, records, rawPayloadBytes int) int {
	k := db.clampValueLogDictK(baseK)
	if records <= 1 || rawPayloadBytes <= 0 {
		return k
	}
	avg := rawPayloadBytes / records
	// For tiny values, larger grouped frames materially reduce per-frame metadata
	// and lock/write overhead in dict mode.
	switch {
	case avg <= 160 && k < 96:
		k = 96
	case avg <= 192 && k < 64:
		k = 64
	case avg <= 256 && k < 32:
		k = 32
	}
	return db.clampValueLogDictK(k)
}
