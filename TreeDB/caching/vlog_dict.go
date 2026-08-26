package caching

import (
	"context"
	"errors"
	"log"
	"math/bits"
	"time"

	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func (db *DB) reportValueLogDictPublishError(err error) {
	// Dictionary publication is retried from the unchanged active profile.
	// Monotonic command-WAL appends and rotations preserve cleanup authority;
	// only unsafe publication or ownership changes stale it and retain the WAL.
	if errors.Is(err, backenddb.ErrDurableWALCleanupProofStale) {
		return
	}
	db.reportError(err)
}

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
	// Outer-leaf block codec is encoded in byte 5 of TOL2 payload headers.
	outerLeafCodecHeaderOffset = 5
	outerLeafCodecNoneID       = 0
	outerLeafCodecSnappyID     = 1
	outerLeafCodecLZ4ID        = 2
)

type vlogDictClassMode uint8

const (
	vlogDictClassModeSingle vlogDictClassMode = iota
	vlogDictClassModeSplitOuterLeaf
)

type vlogDictClass uint8

const (
	vlogDictClassSingleValue vlogDictClass = iota
	vlogDictClassOuterLeaf
)

const vlogDictClassCount = int(vlogDictClassOuterLeaf) + 1

func normalizeVlogDictClassMode(v uint8) vlogDictClassMode {
	switch vlogDictClassMode(v) {
	case vlogDictClassModeSingle, vlogDictClassModeSplitOuterLeaf:
		return vlogDictClassMode(v)
	default:
		return vlogDictClassModeSingle
	}
}

func (db *DB) dictClassMode() vlogDictClassMode {
	if db == nil {
		return vlogDictClassModeSingle
	}
	return normalizeVlogDictClassMode(db.valueLogDictClassMode)
}

func (db *DB) valueLogDictClassForPayloadKind(kind vlogPayloadKind) vlogDictClass {
	if db.dictClassMode() != vlogDictClassModeSplitOuterLeaf {
		return vlogDictClassSingleValue
	}
	switch kind {
	case vlogPayloadKindOuterLeaf:
		return vlogDictClassOuterLeaf
	default:
		return vlogDictClassSingleValue
	}
}

func (db *DB) valueLogDictClassForValue(value []byte) vlogDictClass {
	if db.dictClassMode() != vlogDictClassModeSplitOuterLeaf {
		return vlogDictClassSingleValue
	}
	return db.valueLogDictClassForPayloadKind(db.classifyVlogPayloadKindForValue(value))
}

func (db *DB) valueLogDictClassForLaneValue(l *lane, value []byte) vlogDictClass {
	if db.dictClassMode() != vlogDictClassModeSplitOuterLeaf {
		return vlogDictClassSingleValue
	}
	if l != nil && l.id == leafLogLaneID {
		return vlogDictClassOuterLeaf
	}
	return db.valueLogDictClassForValue(value)
}

func (db *DB) valueLogDictClassForRecordSplit(split vlogPayloadRecordSplit) vlogDictClass {
	if db.dictClassMode() != vlogDictClassModeSplitOuterLeaf {
		return vlogDictClassSingleValue
	}
	switch split.Kind {
	case vlogPayloadKindOuterLeaf:
		return vlogDictClassOuterLeaf
	case vlogPayloadKindMixed:
		if split.OuterLeafRawBytes > split.SingleValueRawBytes {
			return vlogDictClassOuterLeaf
		}
		return vlogDictClassSingleValue
	default:
		return vlogDictClassSingleValue
	}
}

func (db *DB) valueLogDictClassForRecords(records []valuelog.Record) vlogDictClass {
	if db.dictClassMode() != vlogDictClassModeSplitOuterLeaf {
		return vlogDictClassSingleValue
	}
	return db.valueLogDictClassForRecordSplit(db.classifyVlogPayloadSplitForRecords(records))
}

func vlogDictClassSuffix(class vlogDictClass) string {
	switch class {
	case vlogDictClassOuterLeaf:
		return "outer_leaf"
	default:
		return "single_value"
	}
}

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

func (db *DB) valueLogDictTrainerIOCost() float64 {
	if db == nil {
		return 0
	}
	if db.valueLogAutotuneOptions.Mode == valuelog.AutotuneOff {
		return 0
	}
	// In size policy, keep dictionary training ratio-first so profile selection
	// tracks compression ratio improvements instead of throughput trade-offs.
	if normalizeVlogAutoPolicy(db.valueLogAutoPolicy) == vlogAutoSize {
		return 0
	}
	return db.valueLogAutotuneMetrics.snapshot().IoNsPerStoredByte
}

func (db *DB) valueLogDictHasPublishedDict() bool {
	return db != nil && db.valueLogDictLastAppliedDictID.Load() != 0
}

func (db *DB) valueLogDictAllowOuterLeaf() bool {
	if db == nil || !db.indexOuterLeavesInValueLog {
		return false
	}
	// Keep default behavior conservative: only permit outer-leaf dict writes on
	// explicitly size-biased runs.
	if normalizeVlogAutoPolicy(db.valueLogAutoPolicy) != vlogAutoSize {
		return false
	}
	switch normalizeVlogCompressionMode(db.valueLogCompressionMode) {
	case vlogCompressionDict:
		// Require autotune to be enabled so poor dict fits can back off.
		return db.valueLogAutotuneOptions.Mode != valuelog.AutotuneOff
	case vlogCompressionAuto:
		return true
	default:
		return false
	}
}

func (db *DB) valueLogDictShouldTrainClass(class vlogDictClass) bool {
	if class != vlogDictClassOuterLeaf {
		return true
	}
	return db != nil &&
		db.indexOuterLeavesInValueLog &&
		db.dictClassMode() == vlogDictClassModeSplitOuterLeaf
}

func (db *DB) isOuterLeafValueLogPayload(value []byte) bool {
	if db == nil || len(value) == 0 {
		return false
	}
	if !db.indexOuterLeavesInValueLog {
		return false
	}
	return outerleaf.HasMagic(value) || valuelog.HasCompactLeafLogPayload(value)
}

func (db *DB) classifyVlogPayloadKindForValue(value []byte) vlogPayloadKind {
	if db.isOuterLeafValueLogPayload(value) {
		return vlogPayloadKindOuterLeaf
	}
	return vlogPayloadKindSingleValue
}

type vlogPayloadRecordSplit struct {
	Kind                vlogPayloadKind
	OuterLeafRawBytes   int
	SingleValueRawBytes int
	OuterLeafRecords    int
	SingleValueRecords  int
}

func (s vlogPayloadRecordSplit) totalRawBytes() int {
	return s.OuterLeafRawBytes + s.SingleValueRawBytes
}

func (s vlogPayloadRecordSplit) totalRecords() int {
	return s.OuterLeafRecords + s.SingleValueRecords
}

func (db *DB) classifyVlogPayloadKindForRecords(records []valuelog.Record) vlogPayloadKind {
	return db.classifyVlogPayloadSplitForRecords(records).Kind
}

func (db *DB) classifyVlogPayloadSplitForRecords(records []valuelog.Record) vlogPayloadRecordSplit {
	var out vlogPayloadRecordSplit
	if len(records) == 0 {
		out.Kind = vlogPayloadKindMixed
		return out
	}
	for i := range records {
		rawBytes := len(records[i].Value)
		if db.isOuterLeafValueLogPayload(records[i].Value) {
			out.OuterLeafRawBytes += rawBytes
			out.OuterLeafRecords++
		} else {
			out.SingleValueRawBytes += rawBytes
			out.SingleValueRecords++
		}
	}
	switch {
	case out.OuterLeafRecords > 0 && out.SingleValueRecords > 0:
		out.Kind = vlogPayloadKindMixed
	case out.OuterLeafRecords > 0:
		out.Kind = vlogPayloadKindOuterLeaf
	case out.SingleValueRecords > 0:
		out.Kind = vlogPayloadKindSingleValue
	default:
		out.Kind = vlogPayloadKindMixed
	}
	return out
}

func recordLaneVlogPayloadSplitFromSummary(l *lane, split vlogPayloadRecordSplit, storedPayloadBytes int) {
	if l == nil {
		return
	}
	outerRawBytes := split.OuterLeafRawBytes
	if outerRawBytes < 0 {
		outerRawBytes = 0
	}
	singleRawBytes := split.SingleValueRawBytes
	if singleRawBytes < 0 {
		singleRawBytes = 0
	}
	outerRawU := uint64(outerRawBytes)
	singleRawU := uint64(singleRawBytes)
	totalRawU := outerRawU + singleRawU
	if totalRawU < outerRawU {
		totalRawU = ^uint64(0)
	}
	if totalRawU == 0 {
		return
	}
	if storedPayloadBytes <= 0 {
		maxInt := int(^uint(0) >> 1)
		if totalRawU > uint64(maxInt) {
			storedPayloadBytes = maxInt
		} else {
			storedPayloadBytes = int(totalRawU)
		}
	}
	outerStored := 0
	singleStored := 0
	switch {
	case outerRawU > 0 && singleRawU > 0:
		uStored := uint64(storedPayloadBytes)
		uOuter := outerRawU
		uTotal := totalRawU
		var outerStoredU uint64
		if uTotal > 0 {
			hi, lo := bits.Mul64(uStored, uOuter)
			if hi == 0 {
				outerStoredU = lo / uTotal
			} else if hi < uTotal {
				outerStoredU, _ = bits.Div64(hi, lo, uTotal)
			} else {
				// Defensive clamp for impossible overflow cases.
				outerStoredU = uStored
			}
		}
		if outerStoredU > uStored {
			outerStoredU = uStored
		}
		outerStored = int(outerStoredU)
		singleStored = storedPayloadBytes - outerStored
	case outerRawU > 0:
		outerStored = storedPayloadBytes
	case singleRawU > 0:
		singleStored = storedPayloadBytes
	}
	if singleRawBytes > 0 {
		recordLaneVlogPayloadSplitObservation(l, vlogPayloadSplitSingleValue, singleRawBytes, singleStored, split.SingleValueRecords)
	}
	if outerRawBytes > 0 {
		recordLaneVlogPayloadSplitObservation(l, vlogPayloadSplitOuterLeaf, outerRawBytes, outerStored, split.OuterLeafRecords)
	}
}

func (db *DB) classifyVlogOuterLeafCodecKindForValue(value []byte) vlogOuterLeafCodecKind {
	if !db.isOuterLeafValueLogPayload(value) {
		return vlogOuterLeafCodecMixed
	}
	if !outerleaf.HasMagic(value) {
		// Legacy fixed-size leaf pages (4KiB) predate TOL2 block headers.
		return vlogOuterLeafCodecLegacyPage
	}
	if len(value) <= outerLeafCodecHeaderOffset {
		return vlogOuterLeafCodecUnknown
	}
	switch value[outerLeafCodecHeaderOffset] {
	case outerLeafCodecNoneID:
		return vlogOuterLeafCodecNone
	case outerLeafCodecSnappyID:
		return vlogOuterLeafCodecSnappy
	case outerLeafCodecLZ4ID:
		return vlogOuterLeafCodecLZ4
	default:
		return vlogOuterLeafCodecUnknown
	}
}

func (db *DB) classifyVlogOuterLeafCodecKindForRecords(records []valuelog.Record) vlogOuterLeafCodecKind {
	if len(records) == 0 {
		return vlogOuterLeafCodecMixed
	}
	kind := vlogOuterLeafCodecMixed
	for i := range records {
		next := db.classifyVlogOuterLeafCodecKindForValue(records[i].Value)
		if next == vlogOuterLeafCodecMixed {
			return vlogOuterLeafCodecMixed
		}
		if kind == vlogOuterLeafCodecMixed {
			kind = next
			continue
		}
		if next != kind {
			return vlogOuterLeafCodecMixed
		}
	}
	return kind
}

func (db *DB) valueLogDictIgnoreValueForSignal(value []byte) bool {
	// Outer-leaf payloads are structurally different from value streams and can
	// dominate dict signal/training in pointer-heavy runs. Keep both legacy fixed
	// 4KiB pages and v3 outer-leaf blocks on block codecs by default.
	if db.isOuterLeafValueLogPayload(value) {
		return !db.valueLogDictAllowOuterLeaf()
	}
	return false
}

func (db *DB) seedVlogCompressionSelectorsDictRatio(payloadRatio, totalRatio float64) {
	if db == nil {
		return
	}
	if normalizeVlogCompressionMode(db.valueLogCompressionMode) == vlogCompressionDict &&
		db.valueLogAutotuneOptions.Mode == valuelog.AutotuneAggressive {
		// In explicit dict+aggressive mode, let selector decisions be driven by
		// observed frame outcomes. Profile-level dict seeds can be overly
		// optimistic for small-value streams and delay block fallback.
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
	if db.isOuterLeafValueLogPayload(value) && db.valueLogDictAllowOuterLeaf() {
		// Size-biased outer-leaf runs should lean on observed frame outcomes
		// instead of byte-alphabet heuristics so we do not suppress dict tries
		// on structured pages with high local entropy.
		return false
	}
	if db.valueLogDictIgnoreValueForSignal(value) {
		// Outer-leaf pages should stay on block codecs and should not arm
		// incompressible hold state for dictionary sampling.
		return true
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
	if db.valueLogDictAllowOuterLeaf() && db.classifyVlogPayloadKindForRecords(records) == vlogPayloadKindOuterLeaf {
		return false
	}
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
	ignored := 0
	for i := 0; i < len(records) && samples < 4; i += step {
		v := records[i].Value
		if db.valueLogDictIgnoreValueForSignal(v) {
			ignored++
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
		// Only bypass on ignored samples when the entire batch is outer-leaf
		// payloads. Sparse stride sampling can otherwise miss regular values.
		if ignored > 0 && db.classifyVlogPayloadKindForRecords(records) == vlogPayloadKindOuterLeaf {
			return true
		}
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

func (db *DB) valueLogDictTrainerForClass(class vlogDictClass) *compression.Trainer {
	if db == nil {
		return nil
	}
	db.valueLogDictTrainerMu.RLock()
	defer db.valueLogDictTrainerMu.RUnlock()
	if int(class) >= vlogDictClassCount {
		class = vlogDictClassSingleValue
	}
	if tr := db.valueLogDictTrainerByClass[class]; tr != nil {
		return tr
	}
	return db.valueLogDictTrainer
}

func (db *DB) valueLogDictCollectSamples(records []valuelog.Record) {
	class := db.valueLogDictClassForRecords(records)
	db.valueLogDictCollectSamplesForClass(records, class)
}

func (db *DB) valueLogDictCollectSamplesForClass(records []valuelog.Record, class vlogDictClass) {
	if db == nil {
		return
	}
	tr := db.valueLogDictTrainerForClass(class)
	if tr == nil || !tr.ShouldCollect() {
		return
	}
	if !db.valueLogDictShouldTrainClass(class) {
		return
	}
	outerLeafTraining := class == vlogDictClassOuterLeaf
	if !outerLeafTraining && db.valueLogDictIncompressibleHoldRemaining.Load() > 0 {
		return
	}
	paused := false
	if !outerLeafTraining {
		paused = db.valueLogDictPaused()
		if paused && !db.valueLogDictShouldCollectPausedBatch(len(records)) {
			return
		}
	}
	// Seed the trainer's IO cost model early so the initial dict/K selection can
	// optimize for end-to-end throughput (encode + IO), rather than falling back
	// to the decode-cost heuristic when no profile has been published yet.
	//
	// This avoids pathological small-K choices (e.g. k=2/4) that increase frame
	// overhead and reduce write throughput for small values.
	if ioNsPerStoredByte := db.valueLogDictTrainerIOCost(); ioNsPerStoredByte > 0 {
		tr.SetAutotuneIOCost(ioNsPerStoredByte)
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
		if !outerLeafTraining {
			if db.valueLogDictIgnoreValueForSignal(v) {
				continue
			}
			if db.valueLogDictClassifierBypass(v, false) {
				// One high-entropy sample is enough to stop this collect pass and keep
				// the write path cheap on incompressible streams.
				return
			}
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
	class := db.valueLogDictClassForValue(value)
	db.valueLogDictCollectSampleForClass(value, class)
}

func (db *DB) valueLogDictCollectSampleForLane(l *lane, value []byte) {
	class := db.valueLogDictClassForLaneValue(l, value)
	db.valueLogDictCollectSampleForClass(value, class)
}

func (db *DB) valueLogDictCollectSampleForClass(value []byte, class vlogDictClass) {
	if db == nil {
		return
	}
	tr := db.valueLogDictTrainerForClass(class)
	if tr == nil || !tr.ShouldCollect() {
		return
	}
	if !db.valueLogDictShouldTrainClass(class) {
		return
	}
	outerLeafTraining := class == vlogDictClassOuterLeaf
	if !outerLeafTraining && db.valueLogDictIncompressibleHoldRemaining.Load() > 0 {
		return
	}
	if ioNsPerStoredByte := db.valueLogDictTrainerIOCost(); ioNsPerStoredByte > 0 {
		tr.SetAutotuneIOCost(ioNsPerStoredByte)
	}
	stride := db.valueLogDictSampleStride
	if stride <= 1 {
		stride = 1
	}
	if stride > 1 && db.valueLogDictSampleStrideCount.Add(1)%stride != 0 {
		return
	}
	if !outerLeafTraining && db.valueLogDictPaused() && !db.valueLogDictShouldCollectPaused() {
		return
	}
	if !outerLeafTraining {
		if db.valueLogDictIgnoreValueForSignal(value) {
			return
		}
		if db.valueLogDictClassifierBypass(value, false) {
			return
		}
	}
	tr.Collect(value)
}

func (db *DB) ensureValueLogDictTrainer() {
	if db == nil || db.valueLogDictQuiesced.Load() || !db.valueLogDictTrainingEnabled() {
		return
	}
	db.valueLogDictTrainerMu.Lock()
	defer db.valueLogDictTrainerMu.Unlock()
	if db.valueLogDictQuiesced.Load() {
		return
	}
	if db.valueLogDictTrainerByClass[vlogDictClassSingleValue] != nil {
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
	buildTrainer := func(class vlogDictClass) *compression.Trainer {
		tr := compression.NewTrainer(trainCfg, cfg, false, false)
		if tr == nil {
			return nil
		}
		tr.SetOnAccept(func(_ *compression.ActiveProfile) {
			// Publish accepted profiles immediately so short ingest benchmarks can
			// start writing dict frames before teardown.
			db.applyValueLogDictProfileForClass(class)
			db.valueLogDictKick()
		})
		tr.SetAutotuneCandidates(candidateK, db.valueLogAutotuneOptions.CandidateHistoryBytes, db.valueLogAutotuneOptions.CandidateDictBytes)
		return tr
	}
	tr := buildTrainer(vlogDictClassSingleValue)
	if tr == nil {
		return
	}
	db.valueLogDictTrainer = tr
	db.valueLogDictTrainerByClass[vlogDictClassSingleValue] = tr
	if db.dictClassMode() == vlogDictClassModeSplitOuterLeaf {
		db.valueLogDictTrainerByClass[vlogDictClassOuterLeaf] = buildTrainer(vlogDictClassOuterLeaf)
	}
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

// QuiesceValueLogDictTraining permanently stops dictionary training and
// profile publication for this DB handle while keeping published dictionaries
// available for reads and writes. It is a lifecycle boundary for callers that
// require a live handle with no future asynchronous dictionary mutations.
func (db *DB) QuiesceValueLogDictTraining() {
	if db == nil {
		return
	}
	db.valueLogDictQuiesceMu.Lock()
	defer db.valueLogDictQuiesceMu.Unlock()
	if db.valueLogDictQuiesced.Load() {
		return
	}

	// Match checkpoint/close lock order. Holding both locks prevents a writer or
	// flush from retaining a trainer pointer while its sample channel is closed.
	db.flushMu.Lock()
	db.writeMu.Lock()
	db.valueLogDictQuiesced.Store(true)
	db.valueLogDictTrainerMu.Lock()
	trainers := make([]*compression.Trainer, 0, 1+vlogDictClassCount)
	if db.valueLogDictTrainer != nil {
		trainers = append(trainers, db.valueLogDictTrainer)
		db.valueLogDictTrainer = nil
	}
	for i := range db.valueLogDictTrainerByClass {
		if tr := db.valueLogDictTrainerByClass[i]; tr != nil {
			trainers = append(trainers, tr)
			db.valueLogDictTrainerByClass[i] = nil
		}
	}
	db.valueLogDictTrainerMu.Unlock()

	closed := make(map[*compression.Trainer]struct{}, len(trainers))
	for _, trainer := range trainers {
		if trainer == nil {
			continue
		}
		if _, ok := closed[trainer]; ok {
			continue
		}
		closed[trainer] = struct{}{}
		trainer.Close()
	}
	db.writeMu.Unlock()
	db.flushMu.Unlock()
	for trainer := range closed {
		trainer.Wait()
	}

	// A final accepted-profile callback may already be inside publication when
	// the trainers are detached. Drain it before returning. Later periodic-loop
	// attempts see no trainer and cannot mutate the dictionary store.
	db.valueLogDictApplyMu.Lock()
	db.valueLogDictApplyMu.Unlock()
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
		db.applyValueLogDictProfileForClass(vlogDictClassSingleValue)
		if db.dictClassMode() == vlogDictClassModeSplitOuterLeaf {
			db.applyValueLogDictProfileForClass(vlogDictClassOuterLeaf)
		}
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
	db.applyValueLogDictProfileForClass(vlogDictClassSingleValue)
}

func (db *DB) applyValueLogDictProfileForClass(class vlogDictClass) {
	if db == nil {
		return
	}
	// Profile application is a check-then-publish transaction: the applied hash
	// is checked before the dictionary and current marker are persisted, then the
	// hash is advanced only after publication succeeds. Serialize callers so the
	// trainer callback and periodic publisher cannot both observe the old hash
	// and persist the same accepted profile twice.
	db.valueLogDictApplyMu.Lock()
	defer db.valueLogDictApplyMu.Unlock()
	if db.closing.Load() {
		return
	}
	if int(class) >= vlogDictClassCount {
		class = vlogDictClassSingleValue
	}
	db.valueLogDictTrainerMu.Lock()
	tr := db.valueLogDictTrainerByClass[class]
	if tr == nil && class == vlogDictClassSingleValue {
		tr = db.valueLogDictTrainer
	}
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
	}
	if trainerIoNsPerStoredByte := db.valueLogDictTrainerIOCost(); trainerIoNsPerStoredByte > 0 {
		tr.SetAutotuneIOCost(trainerIoNsPerStoredByte)
	}
	profileK := db.clampValueLogDictK(profile.K)
	candidate := db.valueLogAutotuneCandidate(profile, profileK)
	if candidate == nil {
		return
	}
	prevHash := db.valueLogDictLastAppliedDictHashByClass[class].Load()
	if prevHash == profile.DictHash {
		// Dict bytes unchanged; allow updating K for the current dict.
		if profileK <= 1 {
			return
		}
		if curK := int(db.valueLogDictCurrentKByClass[class].Load()); curK == profileK {
			return
		}
		if !db.valueLogAutotuneShouldSwitch(candidate, ioNsPerStoredByte) {
			return
		}
		if ks, ok := store.(dictStoreK); ok {
			dictID := db.valueLogDictLastAppliedDictIDByClass[class].Load()
			if dictID == 0 {
				if id, err := db.currentDictIDForClass(context.Background(), class); err == nil {
					dictID = id
				}
			}
			if dictID == 0 {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := ks.SetK(ctx, dictID, profileK); err != nil {
				db.reportValueLogDictPublishError(err)
				return
			}
			db.valueLogDictCurrentKByClass[class].Store(uint32(profileK))
			if class == vlogDictClassSingleValue {
				db.valueLogDictCurrentK.Store(uint32(profileK))
			}
			db.valueLogDictKMu.Lock()
			if db.valueLogDictKCache == nil {
				db.valueLogDictKCache = make(map[uint64]int)
			}
			db.valueLogDictKCache[dictID] = profileK
			db.valueLogDictKMu.Unlock()
			db.valueLogDictLastKUpdateUnixNano.Store(time.Now().UnixNano())
			log.Printf("treedb: value-log dict updated class=%s dict_id=%d k=%d", vlogDictClassSuffix(class), dictID, profileK)
		}
		db.valueLogAutotuneRecordSwitch(candidate)
		return
	}
	minSavings := db.valueLogDictMinSavingsRatio()
	if profile.PayloadRatio >= 1.0-minSavings {
		// Do not publish no-op dictionaries (common for incompressible payloads).
		db.valueLogDictLastAppliedDictHashByClass[class].Store(profile.DictHash)
		if class == vlogDictClassSingleValue {
			db.valueLogDictLastAppliedDictHash.Store(profile.DictHash)
		}
		return
	}
	if !db.valueLogAutotuneShouldSwitch(candidate, ioNsPerStoredByte) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dictID, err := writer.PutDictBytes(ctx, profile.Dict)
	if err != nil {
		db.reportValueLogDictPublishError(err)
		return
	}
	// Persist the dictionary's compression parameter before publishing its
	// current marker. A retryable metadata failure must leave the old profile
	// current so a later publisher pass can retry the complete operation.
	if ks, ok := store.(dictStoreK); ok {
		if err := ks.SetK(ctx, dictID, profileK); err != nil {
			db.reportValueLogDictPublishError(err)
			return
		}
	}
	classMode := db.dictClassMode()
	publishedViaGlobalCurrent := false
	if classMode == vlogDictClassModeSplitOuterLeaf {
		byClassWriter, hasClassWriter := store.(dictStoreWriterByClass)
		_, hasClassReader := store.(dictStoreCurrentByClass)
		if hasClassWriter && hasClassReader {
			if err := byClassWriter.SetCurrentForClass(ctx, vlogDictClassSuffix(class), dictID); err != nil {
				db.reportValueLogDictPublishError(err)
				return
			}
			if class == vlogDictClassSingleValue {
				// Keep legacy global current in sync for mode switches/reopen paths
				// that read only the global marker.
				if err := writer.SetCurrent(ctx, dictID); err != nil {
					db.reportValueLogDictPublishError(err)
					return
				}
				publishedViaGlobalCurrent = true
			}
		} else if err := writer.SetCurrent(ctx, dictID); err != nil {
			db.reportValueLogDictPublishError(err)
			return
		} else {
			publishedViaGlobalCurrent = true
		}
	} else if err := writer.SetCurrent(ctx, dictID); err != nil {
		db.reportValueLogDictPublishError(err)
		return
	} else {
		publishedViaGlobalCurrent = true
	}
	// Make new dictionaries visible to the write path immediately. We intentionally
	// avoid per-write dictdb reads (currentDictID refreshes only every N uses), so
	// a background publish must also refresh the cached current ID.
	classIdx := int(class)
	if classIdx < 0 || classIdx >= vlogDictClassCount {
		classIdx = int(vlogDictClassSingleValue)
	}
	db.dictCurrentCachedByClass[classIdx].Store(dictID)
	db.dictCurrentOpsByClass[classIdx].Store(0)
	if class == vlogDictClassSingleValue || classMode != vlogDictClassModeSplitOuterLeaf || publishedViaGlobalCurrent {
		db.dictCurrentCached.Store(dictID)
		db.dictCurrentOps.Store(0)
	}
	db.valueLogDictLastAppliedDictHashByClass[class].Store(profile.DictHash)
	db.valueLogDictLastAppliedDictIDByClass[class].Store(dictID)
	db.valueLogDictCurrentKByClass[class].Store(uint32(profileK))
	if class == vlogDictClassSingleValue {
		db.valueLogDictLastAppliedDictHash.Store(profile.DictHash)
		db.valueLogDictLastAppliedDictID.Store(dictID)
		db.valueLogDictCurrentK.Store(uint32(profileK))
	}
	db.valueLogDictLastPublishUnixNano.Store(time.Now().UnixNano())

	// Reset shared ratio tracking only when the publish updates the global current.
	// In split mode, a class-specific publish should not wipe the shared window.
	if class == vlogDictClassSingleValue || classMode != vlogDictClassModeSplitOuterLeaf || publishedViaGlobalCurrent {
		db.valueLogDictMetrics.SetSlab(1)
		db.valueLogDictMetrics.Reset(1)
	}

	log.Printf("treedb: value-log dict published class=%s dict_id=%d k=%d payload_ratio=%.3f total_ratio=%.3f",
		vlogDictClassSuffix(class), dictID, profileK, profile.PayloadRatio, profile.TotalRatio)
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
	tinyTarget := 96
	smallTarget := 64
	if db != nil && db.forceValueLogPointers {
		tinyTarget = 128
		smallTarget = 96
	}
	// For tiny values, larger grouped frames materially reduce per-frame metadata
	// and lock/write overhead in dict mode.
	switch {
	case avg <= 160 && k < tinyTarget:
		k = tinyTarget
	case avg <= 192 && k < smallTarget:
		k = smallTarget
	case avg <= 256 && k < 32:
		k = 32
	}
	return db.clampValueLogDictK(k)
}
