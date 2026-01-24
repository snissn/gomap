package caching

import (
	"context"
	"log"
	"math/bits"
	"time"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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
	// Default-off: dictionary training is CPU-heavy and should be enabled
	// explicitly (TrainBytes > 0).
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

func (db *DB) valueLogDictCollectSamples(records []valuelog.Record) {
	if db == nil {
		return
	}
	tr := db.valueLogDictTrainer
	if tr == nil || !tr.ShouldCollect() {
		return
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
	for i := range records {
		if stride > 1 && (base+uint64(i)+1)%stride != 0 {
			continue
		}
		if db.valueLogDictPaused() && !db.valueLogDictShouldCollectPaused() {
			continue
		}
		v := records[i].Value
		if !likelyCompressibleSample(v) {
			if db.valueLogDictPaused() {
				continue
			}
			pause := db.valueLogDictMetricsPauseBytes
			if pause <= 0 {
				pause = 64 << 20
			}
			db.valueLogDictPauseRemaining.Store(uint64(pause))
			if db.valueLogDictProbeBytes > 0 {
				db.valueLogDictProbeRemaining.Store(db.valueLogDictProbeBytes)
			}
			return
		}
		tr.Collect(v)
	}
}

func (db *DB) valueLogDictCollectSample(value []byte) {
	if db == nil {
		return
	}
	tr := db.valueLogDictTrainer
	if tr == nil || !tr.ShouldCollect() {
		return
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
	if !likelyCompressibleSample(value) {
		if db.valueLogDictPaused() {
			return
		}
		pause := db.valueLogDictMetricsPauseBytes
		if pause <= 0 {
			pause = 64 << 20
		}
		db.valueLogDictPauseRemaining.Store(uint64(pause))
		if db.valueLogDictProbeBytes > 0 {
			db.valueLogDictProbeRemaining.Store(db.valueLogDictProbeBytes)
		}
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

func (db *DB) valueLogDictLoop() {
	defer db.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
		}
		db.applyValueLogDictProfile()
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
	profileK := db.clampValueLogDictK(profile.K)
	prevHash := db.valueLogDictLastAppliedDictHash.Load()
	if prevHash == profile.DictHash {
		// Dict bytes unchanged; allow updating K for the current dict.
		if profileK <= 1 {
			return
		}
		if curK := int(db.valueLogDictCurrentK.Load()); curK == profileK {
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
			log.Printf("treedb: value-log dict updated k dict_id=%d k=%d", dictID, profileK)
		}
		return
	}
	minSavings := db.valueLogDictMinPayloadSavings
	if minSavings <= 0 {
		minSavings = 0.005
	}
	if profile.PayloadRatio >= 1.0-minSavings {
		// Do not publish no-op dictionaries (common for incompressible payloads).
		db.valueLogDictLastAppliedDictHash.Store(profile.DictHash)
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

	// Reset ratio tracking for the new dict.
	db.valueLogDictMetrics.SetSlab(1)
	db.valueLogDictMetrics.Reset(1)

	log.Printf("treedb: value-log dict published dict_id=%d k=%d payload_ratio=%.3f total_ratio=%.3f",
		dictID, profileK, profile.PayloadRatio, profile.TotalRatio)
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
	remaining := db.valueLogDictPauseRemaining.Load()
	for remaining > 0 {
		next := uint64(0)
		if uint64(rawLen) < remaining {
			next = remaining - uint64(rawLen)
		}
		if db.valueLogDictPauseRemaining.CompareAndSwap(remaining, next) {
			if db.valueLogDictProbeBytes == 0 {
				return false, false, true
			}
			probeRemaining := db.valueLogDictProbeRemaining.Load()
			for {
				if probeRemaining <= uint64(rawLen) {
					if db.valueLogDictProbeRemaining.CompareAndSwap(probeRemaining, db.valueLogDictProbeBytes) {
						return true, true, true
					}
				} else if db.valueLogDictProbeRemaining.CompareAndSwap(probeRemaining, probeRemaining-uint64(rawLen)) {
					return false, false, true
				}
				probeRemaining = db.valueLogDictProbeRemaining.Load()
			}
		}
		remaining = db.valueLogDictPauseRemaining.Load()
	}
	return true, false, false
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
	db.valueLogDictPauseRemaining.Store(pause)
	if db.valueLogDictProbeBytes > 0 {
		db.valueLogDictProbeRemaining.Store(db.valueLogDictProbeBytes)
	}
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
	if k > valuelog.MaxFrameK {
		return valuelog.MaxFrameK
	}
	return k
}
