package caching

import (
	"context"
	"log"
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
	var freq [256]uint16
	unique := 0
	max := uint16(0)
	for i := 0; i < n; i++ {
		b := value[i]
		if freq[b] == 0 {
			unique++
		}
		freq[b]++
		if freq[b] > max {
			max = freq[b]
		}
	}
	// Heuristic: skip only when the inspected prefix looks strongly
	// incompressible (near-uniform byte distribution). Keep this conservative:
	// false negatives (training on incompressible samples) are handled by the
	// adaptive pause logic, while false positives can prevent useful dictionaries.
	if unique > 240 && max < 6 {
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
	if db.valueLogDictPaused() {
		return
	}
	for i := range records {
		v := records[i].Value
		if !likelyCompressibleSample(v) {
			continue
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
	if db.valueLogDictPaused() {
		return
	}
	if !likelyCompressibleSample(value) {
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
	tr := compression.NewTrainer(db.valueLogDictTrain, cfg, false, false)
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
	if prev := db.valueLogDictLastAppliedDictHash.Load(); prev == profile.DictHash {
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
	profileK := db.clampValueLogDictK(profile.K)
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

func (db *DB) valueLogDictConsumePause(rawPayloadBytes uint64) {
	if db == nil || rawPayloadBytes == 0 {
		return
	}
	for {
		cur := db.valueLogDictPauseRemaining.Load()
		if cur == 0 {
			return
		}
		var next uint64
		if rawPayloadBytes >= cur {
			next = 0
		} else {
			next = cur - rawPayloadBytes
		}
		if db.valueLogDictPauseRemaining.CompareAndSwap(cur, next) {
			return
		}
	}
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
