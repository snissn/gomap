package adaptive

import (
	"sync"
)

const (
	DefaultInlineThreshold = 512
	InlineHardMin          = DefaultInlineThreshold
	InlineHardMax          = 2048
	DefaultStep            = 64
	UpdateInterval         = 100 // K commits

	// ZipperLeafLogOutputLaneStatsMax is the highest worker-selected leaf-log
	// lane index tracked directly in fixed-size apply metrics. Larger lane indexes
	// are folded into an overflow counter to avoid per-append maps or allocation.
	ZipperLeafLogOutputLaneStatsMax = 64
)

// Metrics represents the telemetry gathered during a single commit.
type Metrics struct {
	LeafFill        float64 // 0..1
	Splits          int
	IndexWriteBytes int
	SlabWriteBytes  int
	SlabDeadBytes   int

	// ZipperApply* fields are coarse counters/timers gathered while applying a
	// batch to a B+Tree. Keep timings at scheduling/output boundaries rather than
	// in per-node recursion so benchmark reports do not add avoidable clock-read
	// overhead to the core apply path.
	ZipperApplyOps                       int
	ZipperNodeLoads                      int
	ZipperPagerNodeLoads                 int
	ZipperLeafLogNodeLoads               int
	ZipperLeafLogCacheHits               int
	ZipperLeafLogReaderCalls             int
	ZipperLeafLogViewReads               int
	ZipperLeafLogScratchReads            int
	ZipperPagerNodeBytesRead             int
	ZipperLeafLogNodeBytesRead           int
	ZipperLeafLogRecordHintBytesRead     int
	ZipperLeafMerges                     int
	ZipperInternalMerges                 int
	ZipperInternalParallelMerges         int
	ZipperInternalParallelChildren       int
	ZipperInternalParallelWorkers        int
	ZipperInternalParallelOps            int
	ZipperLeafPagesWritten               int
	ZipperPagerLeafPagesWritten          int
	ZipperLeafLogPagesWritten            int
	ZipperLeafPageBytesWritten           int
	ZipperPagerLeafPageBytesWritten      int
	ZipperLeafLogPageBytesWritten        int
	ZipperLeafLogRecordHintBytesWritten  int
	ZipperInternalPagesWritten           int
	ZipperInternalPageBytesWritten       int
	ZipperInternalChildRefs              int
	ZipperInternalPageChildRefs          int
	ZipperInternalLeafLogRefs            int
	ZipperInternalLeafLogRefCopies       int
	ZipperRootSplitLevels                int
	ZipperApplyWallNs                    int64
	ZipperRootReduceNs                   int64
	ZipperLeafLogOutputReservationWaitNs int64
	ZipperLeafLogOutputAppendWaitNs      int64
	ZipperLeafLogOutputAppendCalls       int
	ZipperLeafLogOutputAppendPages       int
	ZipperLeafLogOutputLaneTaskTotal     uint64
	ZipperLeafLogOutputLaneTasks         [ZipperLeafLogOutputLaneStatsMax + 1]uint64
	ZipperLeafLogOutputLaneTaskOverflow  uint64
	ZipperSpanNativeWorkerBusyNs         int64
	ZipperSpanNativeWorkerIdleNs         int64
	ZipperSpanNativeWorkerWaitNs         int64
	ZipperSpanNativeReadyTasks           int
	ZipperSpanNativeDispatchedTasks      int
	ZipperSpanNativeCompletedTasks       int
	ZipperSpanNativeQueueDepthMax        int
	ZipperSpanNativeScheduledWorkers     int
	ZipperSpanNativeScheduledWorkersMax  int
	ZipperSpanNativeTaskSpansTotal       int
	ZipperSpanNativeTaskSpansMin         int
	ZipperSpanNativeTaskSpansMax         int
	ZipperSpanNativeTaskOpsTotal         int
	ZipperSpanNativeTaskOpsMin           int
	ZipperSpanNativeTaskOpsMax           int
	ZipperSpanNativeTaskBytesTotal       int
	ZipperSpanNativeTaskBytesMin         int
	ZipperSpanNativeTaskBytesMax         int
	ZipperSpanNativeSingleSpanTasks      int

	// SlabWriteBytesByFile tracks bytes appended to each slab file during this
	// commit (keyed by FileID).
	SlabWriteBytesByFile map[uint32]int64
	// SlabDeadBytesByFile tracks bytes that became dead in each slab file during
	// this commit due to overwrite/delete of pointer values.
	SlabDeadBytesByFile map[uint32]int64
}

// Controller manages the adaptive inline threshold.
type Controller struct {
	mu                 sync.Mutex
	currentThreshold   int
	commitsSinceUpdate int

	// EWMA State (Exponentially Weighted Moving Averages)
	leafFillAvg       float64
	splitRateAvg      float64
	slabDeadRatioAvg  float64
	slabWriteBytesAvg float64

	// Config (Weights)
	alpha float64 // EWMA alpha
}

func New() *Controller {
	return &Controller{
		currentThreshold: DefaultInlineThreshold,
		leafFillAvg:      0.85, // Assume healthy
		alpha:            0.1,  // Roughly 10-20 commits half-life?
	}
}

// GetThreshold returns the current threshold to use for a new batch.
func (c *Controller) GetThreshold() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentThreshold
}

// RecordCommit updates the controller with metrics from a completed commit.
func (c *Controller) RecordCommit(m Metrics) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Update EWMAs
	c.leafFillAvg = ewma(c.leafFillAvg, m.LeafFill, c.alpha)
	// Split rate is abstract, maybe per commit?
	c.splitRateAvg = ewma(c.splitRateAvg, float64(m.Splits), c.alpha)

	slabDeadRatio := 0.0
	if m.SlabWriteBytes > 0 { // Avoid div by zero, approximate
		// This is tricky. Dead bytes vs Total bytes written?
		// Spec says "slab_dead_ratio: sum(DeadBytes) / sum(TotalBytes)"
		// If we track it per commit:
		slabDeadRatio = float64(m.SlabDeadBytes) / float64(m.SlabWriteBytes)
	}
	c.slabDeadRatioAvg = ewma(c.slabDeadRatioAvg, slabDeadRatio, c.alpha)
	c.slabWriteBytesAvg = ewma(c.slabWriteBytesAvg, float64(m.SlabWriteBytes), c.alpha)

	// 2. Check Interval
	c.commitsSinceUpdate++
	if c.commitsSinceUpdate >= UpdateInterval {
		c.adjustThreshold()
		c.commitsSinceUpdate = 0
	}
}

func (c *Controller) adjustThreshold() {
	// Pressure Functions (from spec)
	// Defaults:
	targetLeafFill := 0.85
	targetSplitRate := 0.0 // Ideal
	targetSlabDead := 0.35
	targetSlabWriteBytes := 64.0

	// Weights (Index weights higher)
	w1 := 2.0 // Leaf Fill
	w2 := 2.0 // Split Rate

	v1 := 1.0 // Slab Dead Ratio
	v2 := 1.0 // Slab Write Bytes

	indexPressure := w1*max(0, targetLeafFill-c.leafFillAvg) +
		w2*max(0, c.splitRateAvg-targetSplitRate)

	slabWritePressure := 0.0
	if targetSlabWriteBytes > 0 {
		slabWritePressure = max(0, (c.slabWriteBytesAvg/targetSlabWriteBytes)-1.0)
	}

	slabPressure := v1*max(0, c.slabDeadRatioAvg-targetSlabDead) +
		v2*slabWritePressure

	// Update Rule
	delta := 0
	diff := slabPressure - indexPressure

	// If diff is significant, move step
	// We use a simple heuristic: if difference > 0.1?
	if diff > 0.1 {
		delta = DefaultStep
	} else if diff < -0.1 {
		delta = -DefaultStep
	}

	newT := c.currentThreshold + delta
	if newT < InlineHardMin {
		newT = InlineHardMin
	}
	if newT > InlineHardMax {
		newT = InlineHardMax
	}
	c.currentThreshold = newT
}

func ewma(old, new, alpha float64) float64 {
	return (1-alpha)*old + alpha*new
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
