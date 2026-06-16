package caching

import (
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	flushBacklogCoalescingPPM = uint64(1_000_000)

	flushBacklogCoalescingDefaultSingleOpSpanRatioPPM = uint64(500_000)
	flushBacklogCoalescingDefaultMaxOpsPerSpanPPM     = uint64(4_000_000)
	flushBacklogCoalescingDefaultMaxMemtables         = 64
	flushBacklogCoalescingHardMaxMemtables            = 128
	flushBacklogCoalescingDefaultMaxBytes             = int64(512 << 20)
	flushBacklogCoalescingDefaultMaxOps               = 2 << 20
)

type flushBacklogCoalescingSkipReason uint8

const (
	flushBacklogCoalescingSkipDisabled flushBacklogCoalescingSkipReason = iota
	flushBacklogCoalescingSkipNoPressure
	flushBacklogCoalescingSkipQueueDepth
	flushBacklogCoalescingSkipQueueAge
	flushBacklogCoalescingSkipMemoryBudget
	flushBacklogCoalescingSkipOpsBudget
	flushBacklogCoalescingSkipMemtableBudget
	flushBacklogCoalescingSkipRangeBarrier
	flushBacklogCoalescingSkipLaneBarrier
	flushBacklogCoalescingSkipWriterStallBudget
	flushBacklogCoalescingSkipCheckpoint
	flushBacklogCoalescingSkipClose
	flushBacklogCoalescingSkipStopPressure
	flushBacklogCoalescingSkipReasonCount
)

func (r flushBacklogCoalescingSkipReason) String() string {
	switch r {
	case flushBacklogCoalescingSkipDisabled:
		return "disabled"
	case flushBacklogCoalescingSkipNoPressure:
		return "no_pressure"
	case flushBacklogCoalescingSkipQueueDepth:
		return "queue_depth"
	case flushBacklogCoalescingSkipQueueAge:
		return "queue_age"
	case flushBacklogCoalescingSkipMemoryBudget:
		return "memory_budget"
	case flushBacklogCoalescingSkipOpsBudget:
		return "ops_budget"
	case flushBacklogCoalescingSkipMemtableBudget:
		return "memtable_budget"
	case flushBacklogCoalescingSkipRangeBarrier:
		return "range_barrier"
	case flushBacklogCoalescingSkipLaneBarrier:
		return "lane_barrier"
	case flushBacklogCoalescingSkipWriterStallBudget:
		return "writer_stall_budget"
	case flushBacklogCoalescingSkipCheckpoint:
		return "checkpoint"
	case flushBacklogCoalescingSkipClose:
		return "close"
	case flushBacklogCoalescingSkipStopPressure:
		return "stop_pressure"
	default:
		return "unknown"
	}
}

type flushCollectionMode uint8

const (
	flushCollectionBackground flushCollectionMode = iota
	flushCollectionForeground
	flushCollectionStop
	flushCollectionCheckpoint
	flushCollectionClose
)

type backendFlushApplyPressureSnapshotter interface {
	FlushApplyPressureSnapshot() backenddb.FlushApplyPressureSnapshot
}

type backendBatchSpanNativeFallbackSetter interface {
	SetFlushApplySpanNativeFallback(backenddb.FlushSpanRunFallbackReason)
}

func flushSpanNativeFallbackReasonForCollectionMode(mode flushCollectionMode) backenddb.FlushSpanRunFallbackReason {
	switch mode {
	case flushCollectionCheckpoint, flushCollectionClose:
		return backenddb.FlushSpanRunFallbackCloseOrCheckpoint
	default:
		return backenddb.FlushSpanRunFallbackUnknown
	}
}

func setBackendBatchSpanNativeFallback(backendBatch batch.Interface, reason backenddb.FlushSpanRunFallbackReason) {
	if backendBatch == nil || !reason.Valid() || reason == backenddb.FlushSpanRunFallbackUnknown {
		return
	}
	if setter, ok := backendBatch.(backendBatchSpanNativeFallbackSetter); ok {
		setter.SetFlushApplySpanNativeFallback(reason)
	}
}

type flushBacklogPressure struct {
	spans              uint64
	singleOpSpans      uint64
	spanOps            uint64
	applyOps           uint64
	oldLeafDecodeBytes uint64
	singleOpRatioPPM   uint64
	opsPerSpanPPM      uint64
	oldLeafBytesPerOp  float64
}

type flushCollectStopReason uint8

const (
	flushCollectStopNone flushCollectStopReason = iota
	flushCollectStopEmpty
	flushCollectStopMemtableBudget
	flushCollectStopByteBudget
	flushCollectStopOpsBudget
	flushCollectStopRangeBarrier
	flushCollectStopLaneBarrier
)

type flushUnitBudgetScan struct {
	count     int
	bytes     int64
	ops       int
	queueLen  int
	stop      flushCollectStopReason
	nextIndex int
}

func normalizeFlushBacklogCoalescingOptions(opts *Options) {
	if opts == nil {
		return
	}
	if opts.FlushBacklogCoalescingMaxMemtables <= 0 {
		opts.FlushBacklogCoalescingMaxMemtables = flushBacklogCoalescingDefaultMaxMemtables
	}
	if opts.FlushBacklogCoalescingMaxMemtables < 1 {
		opts.FlushBacklogCoalescingMaxMemtables = 1
	}
	if opts.FlushBacklogCoalescingMaxMemtables > flushBacklogCoalescingHardMaxMemtables {
		opts.FlushBacklogCoalescingMaxMemtables = flushBacklogCoalescingHardMaxMemtables
	}
	if opts.FlushBacklogCoalescingMaxBytes <= 0 {
		opts.FlushBacklogCoalescingMaxBytes = flushBacklogCoalescingDefaultMaxBytes
	}
	if opts.FlushBacklogCoalescingMaxOps <= 0 {
		opts.FlushBacklogCoalescingMaxOps = flushBacklogCoalescingDefaultMaxOps
	}
}

func ratioToPPM(v float64, def uint64) uint64 {
	if v <= 0 {
		return def
	}
	if v > 1 {
		v = 1
	}
	return uint64(v * float64(flushBacklogCoalescingPPM))
}

func opsPerSpanToPPM(v float64, def uint64) uint64 {
	if v <= 0 {
		return def
	}
	return uint64(v * float64(flushBacklogCoalescingPPM))
}

func (db *DB) baseFlushUnitBudget() (maxMemtables int, targetBytes int64) {
	maxMemtables = 1
	if db == nil {
		return maxMemtables, 0
	}
	if db.flushBuildConcurrency > 1 || db.deferredValueLogEnabled() {
		maxMemtables = flushCombineMaxMemtables
		targetBytes = flushCombineTargetBytes
		desired := db.flushThreshold * 4
		if desired > flushCombineTargetBytesMax {
			desired = flushCombineTargetBytesMax
		}
		if desired > targetBytes {
			targetBytes = desired
		}
	}
	return maxMemtables, targetBytes
}

func (db *DB) flushCollectionMode(defaultMode flushCollectionMode) flushCollectionMode {
	if db == nil {
		return defaultMode
	}
	if db.closing.Load() {
		return flushCollectionClose
	}
	if db.checkpointing.Load() {
		return flushCollectionCheckpoint
	}
	if defaultMode == flushCollectionStop {
		return flushCollectionStop
	}
	return defaultMode
}

func (db *DB) observeFlushBacklogCoalescingPressure() flushBacklogPressure {
	var p flushBacklogPressure
	if db == nil {
		return p
	}
	if snapper, ok := db.backend.(backendFlushApplyPressureSnapshotter); ok {
		snap := snapper.FlushApplyPressureSnapshot()
		p.spans = snap.ReadOnlyPrepareSpans
		p.singleOpSpans = snap.ReadOnlyPrepareSingleOpSpans
		p.spanOps = snap.ReadOnlyPrepareSpanOps
		p.applyOps = snap.ApplyOps
		p.oldLeafDecodeBytes = snap.OldLeafReadDecodeBytes
	}
	if p.spans == 0 {
		p.spans = db.flushSpanRunTargetLeafSpans.Load()
		p.singleOpSpans = db.flushSpanRunSingleOpSpans.Load()
		p.spanOps = db.flushSpanRunSpanOps.Load()
	}
	if p.spans > 0 {
		p.singleOpRatioPPM = p.singleOpSpans * flushBacklogCoalescingPPM / p.spans
		p.opsPerSpanPPM = p.spanOps * flushBacklogCoalescingPPM / p.spans
		db.flushBacklogCoalescingLastSingleOpSpanRatioPPM.Store(p.singleOpRatioPPM)
		db.flushBacklogCoalescingLastOpsPerSpanPPM.Store(p.opsPerSpanPPM)
	}
	if p.applyOps > 0 {
		p.oldLeafBytesPerOp = float64(p.oldLeafDecodeBytes) / float64(p.applyOps)
		db.flushBacklogCoalescingLastOldLeafBytesPerOpPPM.Store(uint64(p.oldLeafBytesPerOp * float64(flushBacklogCoalescingPPM)))
	}
	return p
}

func (p flushBacklogPressure) highSingleOpPressure(singleRatioThresholdPPM, maxOpsPerSpanPPM uint64, minOldLeafBytesPerOp float64) bool {
	if p.spans == 0 {
		return false
	}
	if p.singleOpRatioPPM < singleRatioThresholdPPM {
		return false
	}
	if maxOpsPerSpanPPM > 0 && p.opsPerSpanPPM > maxOpsPerSpanPPM {
		return false
	}
	if minOldLeafBytesPerOp > 0 && p.oldLeafBytesPerOp < minOldLeafBytesPerOp {
		return false
	}
	return true
}

func (db *DB) observeFlushBacklogCoalescingSkip(reason flushBacklogCoalescingSkipReason) {
	if db == nil || reason >= flushBacklogCoalescingSkipReasonCount {
		return
	}
	db.flushBacklogCoalescingSkipReasons[reason].Add(1)
}

func (db *DB) queueOldestAgeLocked(now time.Time) time.Duration {
	if db == nil || now.IsZero() {
		return 0
	}
	oldest := int64(0)
	for i := range db.queueEnqueueNS {
		ts := db.queueEnqueueNS[i]
		if ts <= 0 {
			continue
		}
		if oldest == 0 || ts < oldest {
			oldest = ts
		}
	}
	if oldest <= 0 {
		return 0
	}
	ageNS := now.UnixNano() - oldest
	if ageNS <= 0 {
		return 0
	}
	return time.Duration(ageNS)
}

func (db *DB) scanFlushUnitsBudgetLocked(laneID int, maxMemtables int, targetBytes int64, maxOps int) flushUnitBudgetScan {
	var out flushUnitBudgetScan
	if db == nil {
		out.stop = flushCollectStopEmpty
		return out
	}
	queueLen := len(db.queue)
	out.queueLen = queueLen
	if queueLen == 0 {
		out.stop = flushCollectStopEmpty
		return out
	}
	if maxMemtables <= 0 {
		maxMemtables = flushCombineMaxMemtables
	}
	if maxMemtables > flushBacklogCoalescingHardMaxMemtables {
		maxMemtables = flushBacklogCoalescingHardMaxMemtables
	}
	spanOnly := false
	for i := 0; i < queueLen; i++ {
		unitLimit := maxMemtables
		if spanOnly {
			unitLimit = flushRangeSpanCombineMaxUnits
		}
		if out.count >= unitLimit {
			out.stop = flushCollectStopMemtableBudget
			out.nextIndex = i
			return out
		}
		var spans []batch.DeleteRange
		if i < len(db.queueRangeSpans) {
			spans = db.queueRangeSpans[i]
		}
		hasSpans := len(spans) > 0
		if hasSpans {
			if !spanOnly && i != 0 {
				out.stop = flushCollectStopRangeBarrier
				out.nextIndex = i
				return out
			}
			if out.count > 0 && !spanOnly {
				out.stop = flushCollectStopRangeBarrier
				out.nextIndex = i
				return out
			}
		} else if spanOnly {
			out.stop = flushCollectStopRangeBarrier
			out.nextIndex = i
			return out
		}
		unitLaneID := 0
		if i < len(db.queueLaneIDs) {
			unitLaneID = int(db.queueLaneIDs[i])
		}
		if laneID >= 0 {
			if unitLaneID != laneID {
				out.stop = flushCollectStopLaneBarrier
				out.nextIndex = i
				return out
			}
		} else if !spanOnly && i >= maxMemtables {
			out.stop = flushCollectStopMemtableBudget
			out.nextIndex = i
			return out
		} else if spanOnly && i >= flushRangeSpanCombineMaxUnits {
			out.stop = flushCollectStopMemtableBudget
			out.nextIndex = i
			return out
		}
		mem := db.queue[i]
		memBytes := mem.Size()
		memLen := mem.Len()
		if !spanOnly && out.count > 0 && targetBytes > 0 && out.bytes >= targetBytes {
			out.stop = flushCollectStopByteBudget
			out.nextIndex = i
			return out
		}
		if !spanOnly && out.count > 0 && maxOps > 0 && out.ops >= maxOps {
			out.stop = flushCollectStopOpsBudget
			out.nextIndex = i
			return out
		}
		out.count++
		out.bytes += memBytes
		out.ops += memLen
		if hasSpans {
			spanOnly = true
		}
	}
	out.stop = flushCollectStopNone
	out.nextIndex = queueLen
	return out
}

func coalescingSkipReasonForCollectStop(stop flushCollectStopReason) flushBacklogCoalescingSkipReason {
	switch stop {
	case flushCollectStopMemtableBudget:
		return flushBacklogCoalescingSkipMemtableBudget
	case flushCollectStopByteBudget:
		return flushBacklogCoalescingSkipMemoryBudget
	case flushCollectStopOpsBudget:
		return flushBacklogCoalescingSkipOpsBudget
	case flushCollectStopRangeBarrier:
		return flushBacklogCoalescingSkipRangeBarrier
	case flushCollectStopLaneBarrier:
		return flushBacklogCoalescingSkipLaneBarrier
	default:
		return flushBacklogCoalescingSkipQueueDepth
	}
}

func (db *DB) selectFlushUnitBudgetLocked(laneID int, mode flushCollectionMode) (maxMemtables int, targetBytes int64, maxOps int) {
	baseMaxMemtables, baseTargetBytes := db.baseFlushUnitBudget()
	maxMemtables, targetBytes = baseMaxMemtables, baseTargetBytes
	mode = db.flushCollectionMode(mode)
	if db == nil {
		return maxMemtables, targetBytes, 0
	}
	if !db.flushBacklogCoalescing {
		db.observeFlushBacklogCoalescingSkip(flushBacklogCoalescingSkipDisabled)
		return maxMemtables, targetBytes, 0
	}

	db.flushBacklogCoalescingDecisions.Add(1)
	queueLen := len(db.queue)
	if queueLen > 0 {
		updateAtomicMaxUint64(&db.flushBacklogCoalescingQueuedMemtablesMax, uint64(queueLen))
	}
	queuedBytes := db.queueBacklogBytes.Load()
	if queuedBytes > 0 {
		updateAtomicMaxUint64(&db.flushBacklogCoalescingQueuedBytesMax, uint64(queuedBytes))
	}
	age := db.queueOldestAgeLocked(time.Now())
	if age > 0 {
		updateAtomicMaxUint64(&db.flushBacklogCoalescingQueuedAgeNsMax, uint64(age.Nanoseconds()))
	}

	switch mode {
	case flushCollectionClose:
		db.observeFlushBacklogCoalescingSkip(flushBacklogCoalescingSkipClose)
		return maxMemtables, targetBytes, 0
	case flushCollectionCheckpoint:
		db.observeFlushBacklogCoalescingSkip(flushBacklogCoalescingSkipCheckpoint)
		return maxMemtables, targetBytes, 0
	case flushCollectionStop:
		db.observeFlushBacklogCoalescingSkip(flushBacklogCoalescingSkipStopPressure)
		return maxMemtables, targetBytes, 0
	case flushCollectionForeground:
		db.observeFlushBacklogCoalescingSkip(flushBacklogCoalescingSkipWriterStallBudget)
		return maxMemtables, targetBytes, 0
	}

	baseScan := db.scanFlushUnitsBudgetLocked(laneID, baseMaxMemtables, baseTargetBytes, 0)
	if baseScan.queueLen == 0 || baseScan.count == 0 {
		db.observeFlushBacklogCoalescingSkip(flushBacklogCoalescingSkipQueueDepth)
		return maxMemtables, targetBytes, 0
	}
	if baseScan.count >= baseScan.queueLen || baseScan.stop == flushCollectStopNone {
		db.observeFlushBacklogCoalescingSkip(flushBacklogCoalescingSkipQueueDepth)
		return maxMemtables, targetBytes, 0
	}
	if baseScan.stop == flushCollectStopRangeBarrier || baseScan.stop == flushCollectStopLaneBarrier {
		db.observeFlushBacklogCoalescingSkip(coalescingSkipReasonForCollectStop(baseScan.stop))
		return maxMemtables, targetBytes, 0
	}
	if db.flushBacklogCoalescingMinAge > 0 && age < db.flushBacklogCoalescingMinAge {
		db.observeFlushBacklogCoalescingSkip(flushBacklogCoalescingSkipQueueAge)
		return maxMemtables, targetBytes, 0
	}

	pressure := db.observeFlushBacklogCoalescingPressure()
	if !pressure.highSingleOpPressure(db.flushBacklogCoalescingSingleOpSpanRatioPPM, db.flushBacklogCoalescingMaxOpsPerSpanPPM, db.flushBacklogCoalescingMinOldLeafBytesPerOp) {
		db.observeFlushBacklogCoalescingSkip(flushBacklogCoalescingSkipNoPressure)
		return maxMemtables, targetBytes, 0
	}

	maxMemtables = db.flushBacklogCoalescingMaxMemtables
	if maxMemtables < baseMaxMemtables {
		maxMemtables = baseMaxMemtables
	}
	if maxMemtables > flushBacklogCoalescingHardMaxMemtables {
		maxMemtables = flushBacklogCoalescingHardMaxMemtables
	}
	targetBytes = db.flushBacklogCoalescingMaxBytes
	if targetBytes < baseTargetBytes {
		targetBytes = baseTargetBytes
	}
	maxOps = db.flushBacklogCoalescingMaxOps

	coalesced := db.scanFlushUnitsBudgetLocked(laneID, maxMemtables, targetBytes, maxOps)
	if coalesced.count <= baseScan.count {
		db.observeFlushBacklogCoalescingSkip(coalescingSkipReasonForCollectStop(coalesced.stop))
		return baseMaxMemtables, baseTargetBytes, 0
	}

	db.flushBacklogCoalescingAdmittedRuns.Add(1)
	extraMemtables := coalesced.count - baseScan.count
	if extraMemtables > 0 {
		db.flushBacklogCoalescingAdmittedExtraMemtables.Add(uint64(extraMemtables))
	}
	if extraBytes := coalesced.bytes - baseScan.bytes; extraBytes > 0 {
		db.flushBacklogCoalescingAdmittedExtraBytes.Add(uint64(extraBytes))
	}
	if extraOps := coalesced.ops - baseScan.ops; extraOps > 0 {
		db.flushBacklogCoalescingAdmittedExtraOps.Add(uint64(extraOps))
	}
	db.flushBacklogCoalescingSelectedMemtables.Add(uint64(coalesced.count))
	updateAtomicMaxUint64(&db.flushBacklogCoalescingSelectedMemtablesMax, uint64(coalesced.count))
	if coalesced.bytes > 0 {
		db.flushBacklogCoalescingSelectedBytes.Add(uint64(coalesced.bytes))
		updateAtomicMaxUint64(&db.flushBacklogCoalescingSelectedBytesMax, uint64(coalesced.bytes))
	}
	if coalesced.ops > 0 {
		db.flushBacklogCoalescingSelectedOps.Add(uint64(coalesced.ops))
		updateAtomicMaxUint64(&db.flushBacklogCoalescingSelectedOpsMax, uint64(coalesced.ops))
	}
	if coalesced.count < coalesced.queueLen && coalesced.stop != flushCollectStopNone {
		db.observeFlushBacklogCoalescingSkip(coalescingSkipReasonForCollectStop(coalesced.stop))
	}
	return maxMemtables, targetBytes, maxOps
}
