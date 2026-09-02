package zipper

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

var (
	// ErrSpanNativeOutputOwnership marks failures where a span-native worker
	// produced durable output whose child-ref kind/ownership cannot be accepted by
	// the reducer for the configured output mode.
	ErrSpanNativeOutputOwnership = errors.New("zipper: span-native output ownership failure")
	// ErrSpanNativeReducerValidation marks fail-closed reducer validation failures
	// before publishing a reconstructed root.
	ErrSpanNativeReducerValidation = errors.New("zipper: span-native reducer validation failed")
)

// spanNativeLeafLogOutputGate prepares leaf-log payloads in span-native
// workers, then bounds the durable append fan-out to the apply worker count.
// The resulting refs are still only published by the coordinator after every
// worker succeeds and the reducer validates the replacement root; any output
// written before a later failure is accounted as abandoned by the DB flush path.
type spanNativeLeafLogOutputGate struct {
	sem chan struct{}
}

type spanNativeLeafLogOutputLane struct {
	gate *spanNativeLeafLogOutputGate
	log  LeafPageLog
}

const spanNativeLeafLogPayloadArenaMaxCap = 16 << 20

type spanNativeLeafLogPayloadScratch struct {
	buf []byte
}

type spanNativeLeafLogPayloadArena struct {
	buf []byte
}

type spanNativeLeafLogPayloadSlice struct {
	items [][]byte
}

var (
	spanNativeLeafLogPayloadScratchPool sync.Pool
	spanNativeLeafLogPayloadArenaPool   sync.Pool
	spanNativeLeafLogPayloadSlicePool   sync.Pool
)

func newSpanNativeLeafLogOutputGate(limit int) *spanNativeLeafLogOutputGate {
	if limit <= 0 {
		return &spanNativeLeafLogOutputGate{}
	}
	return &spanNativeLeafLogOutputGate{sem: make(chan struct{}, limit)}
}

func acquireSpanNativeLeafLogPayloadScratch() *spanNativeLeafLogPayloadScratch {
	if v := spanNativeLeafLogPayloadScratchPool.Get(); v != nil {
		if scratch, ok := v.(*spanNativeLeafLogPayloadScratch); ok && scratch != nil && cap(scratch.buf) <= page.PageSize*2 {
			scratch.buf = scratch.buf[:0]
			return scratch
		}
	}
	return &spanNativeLeafLogPayloadScratch{buf: make([]byte, 0, page.PageSize)}
}

func releaseSpanNativeLeafLogPayloadScratch(scratch *spanNativeLeafLogPayloadScratch) {
	if scratch == nil || cap(scratch.buf) == 0 || cap(scratch.buf) > page.PageSize*2 {
		return
	}
	scratch.buf = scratch.buf[:0]
	spanNativeLeafLogPayloadScratchPool.Put(scratch)
}

func acquireSpanNativeLeafLogPayloadArena(need int) *spanNativeLeafLogPayloadArena {
	if need < 0 {
		need = 0
	}
	if need <= spanNativeLeafLogPayloadArenaMaxCap {
		if v := spanNativeLeafLogPayloadArenaPool.Get(); v != nil {
			if arena, ok := v.(*spanNativeLeafLogPayloadArena); ok && arena != nil && cap(arena.buf) >= need && cap(arena.buf) <= spanNativeLeafLogPayloadArenaMaxCap {
				arena.buf = arena.buf[:0]
				return arena
			}
		}
	}
	arena := &spanNativeLeafLogPayloadArena{}
	if need > 0 {
		arena.buf = make([]byte, 0, need)
	}
	return arena
}

func releaseSpanNativeLeafLogPayloadArena(arena *spanNativeLeafLogPayloadArena) {
	if arena == nil || cap(arena.buf) > spanNativeLeafLogPayloadArenaMaxCap {
		return
	}
	arena.buf = arena.buf[:0]
	spanNativeLeafLogPayloadArenaPool.Put(arena)
}

func acquireSpanNativeLeafLogPayloadSlice(n int) *spanNativeLeafLogPayloadSlice {
	if n <= 0 {
		return &spanNativeLeafLogPayloadSlice{}
	}
	if v := spanNativeLeafLogPayloadSlicePool.Get(); v != nil {
		if lease, ok := v.(*spanNativeLeafLogPayloadSlice); ok && lease != nil && cap(lease.items) >= n && cap(lease.items) <= 1<<20 {
			lease.items = lease.items[:n]
			return lease
		}
	}
	return &spanNativeLeafLogPayloadSlice{items: make([][]byte, n)}
}

func releaseSpanNativeLeafLogPayloadSlice(lease *spanNativeLeafLogPayloadSlice) {
	if lease == nil || cap(lease.items) == 0 || cap(lease.items) > 1<<20 {
		return
	}
	clear(lease.items[:cap(lease.items)])
	lease.items = lease.items[:0]
	spanNativeLeafLogPayloadSlicePool.Put(lease)
}

func estimateSpanNativeLeafLogPayloadArenaCap(leafPages [][]byte) int {
	if len(leafPages) == 0 {
		return 0
	}
	samples := len(leafPages)
	if samples > 8 {
		samples = 8
	}
	compactSamples := 0
	totalCompactLen := 0
	for i := 0; i < samples; i++ {
		compactLen, compacted := valuelog.MaybeCompactLeafLogPayloadLength(leafPages[i])
		if !compacted {
			continue
		}
		compactSamples++
		totalCompactLen += compactLen
	}
	if compactSamples != samples {
		for i := samples; i < len(leafPages); i++ {
			compactLen, compacted := valuelog.MaybeCompactLeafLogPayloadLength(leafPages[i])
			if !compacted {
				continue
			}
			totalCompactLen += compactLen
		}
		maxCap := page.PageSize * len(leafPages)
		if totalCompactLen > maxCap {
			return maxCap
		}
		return totalCompactLen
	}
	avgCompactLen := (totalCompactLen + compactSamples - 1) / compactSamples
	maxCap := page.PageSize * len(leafPages)
	if avgCompactLen > maxCap/len(leafPages) {
		return maxCap
	}
	return avgCompactLen * len(leafPages)
}

func (g *spanNativeLeafLogOutputGate) selectedLeafPageLogForWorker(z *Zipper, workerIndex int) (LeafPageLog, bool) {
	if z == nil || z.leafPageLog == nil || workerIndex <= 0 {
		return nil, false
	}
	if provider, ok := z.leafPageLog.(LeafPageLogLaneProvider); ok {
		if laneLog, ok := provider.LeafPageLogLane(workerIndex); ok && laneLog != nil {
			return laneLog, true
		}
	}
	if provider, ok := z.leafPageLog.(leafPageLogLaneAnyProvider); ok {
		if lane, ok := provider.LeafPageLogLaneAny(workerIndex); ok && lane != nil {
			if laneLog, ok := lane.(LeafPageLog); ok && laneLog != nil {
				return laneLog, true
			}
		}
	}
	return nil, false
}

func (g *spanNativeLeafLogOutputGate) leafPageLogForWorker(z *Zipper, workerIndex int) LeafPageLog {
	if laneLog, ok := g.selectedLeafPageLogForWorker(z, workerIndex); ok {
		return laneLog
	}
	if z == nil {
		return nil
	}
	return z.leafPageLog
}

func (g *spanNativeLeafLogOutputGate) persistLeafPageData(z *Zipper, leafPage []byte, metrics *adaptive.Metrics) (page.ChildRef, error) {
	return g.persistLeafPageDataForWorker(z, 0, leafPage, metrics)
}

func (g *spanNativeLeafLogOutputLane) persistLeafPageData(z *Zipper, leafPage []byte, metrics *adaptive.Metrics) (page.ChildRef, error) {
	if g == nil || g.gate == nil {
		return z.persistLeafPageData(leafPage, metrics)
	}
	return g.gate.persistLeafPageDataToLog(z, g.log, leafPage, metrics)
}

func (g *spanNativeLeafLogOutputGate) persistLeafPageDataForWorker(z *Zipper, workerIndex int, leafPage []byte, metrics *adaptive.Metrics) (page.ChildRef, error) {
	return g.persistLeafPageDataToLog(z, g.leafPageLogForWorker(z, workerIndex), leafPage, metrics)
}

func (g *spanNativeLeafLogOutputGate) persistLeafPageDataToLog(z *Zipper, log LeafPageLog, leafPage []byte, metrics *adaptive.Metrics) (page.ChildRef, error) {
	prepared := leafPageLogConsumesPreparedPayloads(log, false)
	var payload []byte
	var scratch *spanNativeLeafLogPayloadScratch
	if prepared {
		scratch = acquireSpanNativeLeafLogPayloadScratch()
		var err error
		payload, _, err = valuelog.MaybeCompactLeafLogPayloadTo(scratch.buf[:0], leafPage)
		if err != nil {
			releaseSpanNativeLeafLogPayloadScratch(scratch)
			return page.ChildRef{}, err
		}
	}
	waitStart := time.Now()
	if g != nil && g.sem != nil {
		g.sem <- struct{}{}
		defer func() { <-g.sem }()
	}
	reservationWait := time.Since(waitStart)
	if metrics != nil && reservationWait > 0 {
		metrics.ZipperLeafLogOutputReservationWaitNs += reservationWait.Nanoseconds()
	}
	if !prepared {
		return z.persistLeafPageDataToLog(log, leafPage, metrics)
	}
	ref, err := z.persistPreparedLeafPageDataToLog(log, leafPage, payload, metrics)
	releaseSpanNativeLeafLogPayloadScratch(scratch)
	return ref, err
}

func (g *spanNativeLeafLogOutputGate) persistLeafPageBatchDataTo(z *Zipper, leafPages [][]byte, refs []page.ChildRef, metrics *adaptive.Metrics) ([]page.ChildRef, error) {
	return g.persistLeafPageBatchDataToForWorker(z, 0, leafPages, refs, metrics)
}

func (g *spanNativeLeafLogOutputLane) persistLeafPageBatchDataTo(z *Zipper, leafPages [][]byte, refs []page.ChildRef, metrics *adaptive.Metrics) ([]page.ChildRef, error) {
	if g == nil || g.gate == nil {
		return z.persistLeafPageBatchDataTo(leafPages, refs, metrics)
	}
	return g.gate.persistLeafPageBatchDataToLog(z, g.log, leafPages, refs, metrics)
}

func (g *spanNativeLeafLogOutputGate) persistLeafPageBatchDataToForWorker(z *Zipper, workerIndex int, leafPages [][]byte, refs []page.ChildRef, metrics *adaptive.Metrics) ([]page.ChildRef, error) {
	return g.persistLeafPageBatchDataToLog(z, g.leafPageLogForWorker(z, workerIndex), leafPages, refs, metrics)
}

func (g *spanNativeLeafLogOutputGate) persistLeafPageBatchDataToLog(z *Zipper, log LeafPageLog, leafPages [][]byte, refs []page.ChildRef, metrics *adaptive.Metrics) ([]page.ChildRef, error) {
	refs = refs[:0]
	if len(leafPages) == 0 {
		return refs, nil
	}
	if len(leafPages) == 1 {
		ref, err := g.persistLeafPageDataToLog(z, log, leafPages[0], metrics)
		if err != nil {
			return nil, err
		}
		return append(refs, ref), nil
	}
	prepared := leafPageLogConsumesPreparedPayloads(log, true)
	var payloads [][]byte
	var payloadSlice *spanNativeLeafLogPayloadArena
	var payloadLease *spanNativeLeafLogPayloadSlice
	if prepared {
		payloadLease = acquireSpanNativeLeafLogPayloadSlice(len(leafPages))
		payloads = payloadLease.items
		payloadSlice = acquireSpanNativeLeafLogPayloadArena(estimateSpanNativeLeafLogPayloadArenaCap(leafPages))
		for i, leafPage := range leafPages {
			var err error
			payloadSlice.buf, payloads[i], _, err = valuelog.MaybeAppendCompactLeafLogPayloadTo(payloadSlice.buf, leafPage)
			if err != nil {
				releaseSpanNativeLeafLogPayloadSlice(payloadLease)
				releaseSpanNativeLeafLogPayloadArena(payloadSlice)
				return nil, err
			}
		}
	}
	waitStart := time.Now()
	acquiredSemaphore := false
	if g != nil && g.sem != nil {
		g.sem <- struct{}{}
		acquiredSemaphore = true
	}
	reservationWait := time.Since(waitStart)
	if metrics != nil && reservationWait > 0 {
		metrics.ZipperLeafLogOutputReservationWaitNs += reservationWait.Nanoseconds()
	}
	var out []page.ChildRef
	var err error
	if prepared {
		out, err = z.persistPreparedLeafPageBatchDataToLog(log, leafPages, payloads, refs, metrics)
	} else {
		out, err = z.persistLeafPageBatchDataToLog(log, leafPages, refs, metrics)
	}
	if acquiredSemaphore {
		<-g.sem
	}
	releaseSpanNativeLeafLogPayloadSlice(payloadLease)
	releaseSpanNativeLeafLogPayloadArena(payloadSlice)
	return out, err
}

func (z *Zipper) applySpanNativeWithPrepared(rootID uint64, ops []batch.Entry, prepared ReadOnlyPrepareResult, opts ApplyOptions, workers int, workerPool *ApplyWorkerPool) (ApplyResult, bool, error) {
	if !validateSpanNativePreparedPlan(ops, prepared, opts) {
		return ApplyResult{}, false, nil
	}

	applyStart := time.Now()
	var metrics adaptive.Metrics
	metrics.ZipperApplyOps = len(ops)
	coordinatorScratch := z.acquireApplyScratch()
	defer z.releaseApplyScratch(coordinatorScratch)

	spanCount := len(prepared.LeafSpans)
	if workers <= 0 {
		workers = 1
	}
	if workers > spanCount {
		workers = spanCount
	}

	rangeCapacity := workers
	if rangeCapacity > spanCount {
		rangeCapacity = spanCount
	}
	workerRanges := coordinatorScratch.acquireSpanWorkerRanges(rangeCapacity)
	workerRanges = prepared.AppendLeafSpanWorkUnitRanges(workerRanges, workers)
	if len(workerRanges) == 0 {
		workerRanges = append(workerRanges, ReadOnlyLeafSpanWorkerRange{FirstSpan: 0, SpanCount: spanCount, Ops: len(ops)})
	}
	defer coordinatorScratch.releaseSpanWorkerRanges(workerRanges)
	scheduledWorkers := workers
	if scheduledWorkers > len(workerRanges) {
		scheduledWorkers = len(workerRanges)
	}
	if scheduledWorkers <= 0 {
		scheduledWorkers = 1
	}
	outputs := coordinatorScratch.acquireSpanNativeLeafOutputs(spanCount, z.outerLeavesInValueLog)
	defer outputs.release()
	var leafLogOutput *spanNativeLeafLogOutputGate
	if z.outerLeavesInValueLog {
		if z.leafPageLog == nil {
			return ApplyResult{}, true, errors.New("zipper: outer leaves in value log enabled without leaf page log")
		}
		appendLimit := 1
		if concurrent, ok := z.leafPageLog.(LeafPageConcurrentAppendLog); ok && concurrent.ConcurrentLeafPageAppends() {
			appendLimit = scheduledWorkers
		}
		leafLogOutput = newSpanNativeLeafLogOutputGate(appendLimit)
	}
	workerScratches := coordinatorScratch.acquireSpanWorkerScratchSlots(len(workerRanges))
	defer func() {
		for i := range workerScratches {
			z.releaseApplyScratch(workerScratches[i])
			workerScratches[i] = nil
		}
		coordinatorScratch.releaseSpanWorkerScratchSlots(workerScratches)
	}()
	rangeMetrics := coordinatorScratch.acquireSpanRangeMetrics(len(workerRanges))
	defer coordinatorScratch.releaseSpanRangeMetrics(rangeMetrics)
	rangeRetired := coordinatorScratch.acquireSpanRangeRetired(len(workerRanges))
	defer coordinatorScratch.releaseSpanRangeRetired(rangeRetired)
	rangeSplits := coordinatorScratch.acquireSpanRangeSplits(len(workerRanges))
	defer coordinatorScratch.releaseSpanRangeSplits(rangeSplits)
	var rangeOldPointerRefs []PointerRefCounts
	var rangeOldEntriesRemoved []uint64
	if opts.CollectOldPointerRefs {
		rangeOldPointerRefs = make([]PointerRefCounts, len(workerRanges))
		rangeOldEntriesRemoved = make([]uint64, len(workerRanges))
	}
	var firstErr error
	var errOnce sync.Once
	var workerBusyNs atomic.Int64
	var dispatchedTasks atomic.Uint64
	var completedTasks atomic.Uint64
	runRange := func(workerID int, job int) {
		dispatchedTasks.Add(1)
		busyStart := time.Now()
		defer func() {
			workerBusyNs.Add(int64(elapsedNsSince(busyStart)))
			completedTasks.Add(1)
		}()
		workerScratch := z.acquireApplyScratch()
		workerScratches[job] = workerScratch
		workerRange := workerRanges[job]
		end := workerRange.FirstSpan + workerRange.SpanCount
		var localMetrics adaptive.Metrics
		var localRetired []uint64
		var localSplits [][]Split
		releaseLocalSplits := func() {
			if localSplits != nil {
				releaseSpanNativeSplitSlices(localSplits)
				localSplits = nil
			}
		}
		recordFailedRange := func() {
			rangeMetrics[job] = localMetrics
			rangeRetired[job] = localRetired
		}
		var leafPagePersister leafPagePersistSink = leafLogOutput
		if leafLogOutput != nil && scheduledWorkers > 1 {
			laneIndex := workerID + 1
			if laneLog, ok := leafLogOutput.selectedLeafPageLogForWorker(z, laneIndex); ok {
				localMetrics.ZipperLeafLogOutputLaneTaskTotal++
				if laneIndex >= 0 && laneIndex <= adaptive.ZipperLeafLogOutputLaneStatsMax {
					localMetrics.ZipperLeafLogOutputLaneTasks[laneIndex]++
				} else {
					localMetrics.ZipperLeafLogOutputLaneTaskOverflow++
				}
				leafPagePersister = &spanNativeLeafLogOutputLane{gate: leafLogOutput, log: laneLog}
			}
		}
		workerApplyCfg := applyRunConfig{maxParallelWorkers: 1, leafPagePersister: leafPagePersister}
		if rangeOldPointerRefs != nil {
			workerApplyCfg.oldPointerRefs = &rangeOldPointerRefs[job]
			workerApplyCfg.oldEntriesRemoved = &rangeOldEntriesRemoved[job]
		}
		for i := workerRange.FirstSpan; i < end; i++ {
			span := prepared.LeafSpans[i]
			newRef, splits, err := z.writeRecursive(span.Ref, ops[span.PointOpStart:span.PointOpEnd], nil, false, nil, &localMetrics, span.LowKey, span.HighKey, &localRetired, workerScratch, false, workerApplyCfg)
			if err != nil {
				recordFailedRange()
				releaseLocalSplits()
				errOnce.Do(func() { firstErr = err })
				return
			}
			if err := outputs.setRef(i, newRef); err != nil {
				recordFailedRange()
				releaseLocalSplits()
				errOnce.Do(func() {
					firstErr = fmt.Errorf("%w: span %d ref kind %d: %v", ErrSpanNativeOutputOwnership, i, newRef.Kind, err)
				})
				return
			}
			if len(splits) > 0 {
				if localSplits == nil {
					localSplits = acquireSpanNativeSplitSlices(workerRange.SpanCount)
				}
				localSplits[i-workerRange.FirstSpan] = splits
			}
		}
		rangeMetrics[job] = localMetrics
		rangeRetired[job] = localRetired
		if localSplits != nil {
			rangeSplits[job] = spanNativeLeafSplitRange{start: workerRange.FirstSpan, splits: localSplits}
		}
	}
	scheduleStart := time.Now()
	finishScheduleStats := func() {
		waitNs := int64(elapsedNsSince(scheduleStart))
		busyNs := workerBusyNs.Load()
		idleNs := waitNs*int64(scheduledWorkers) - busyNs
		if idleNs < 0 {
			idleNs = 0
		}
		metrics.ZipperSpanNativeWorkerBusyNs += busyNs
		metrics.ZipperSpanNativeWorkerIdleNs += idleNs
		metrics.ZipperSpanNativeWorkerWaitNs += waitNs
		metrics.ZipperSpanNativeReadyTasks += len(workerRanges)
		metrics.ZipperSpanNativeDispatchedTasks += int(dispatchedTasks.Load())
		metrics.ZipperSpanNativeCompletedTasks += int(completedTasks.Load())
		if len(workerRanges) > metrics.ZipperSpanNativeQueueDepthMax {
			metrics.ZipperSpanNativeQueueDepthMax = len(workerRanges)
		}
		metrics.ZipperSpanNativeScheduledWorkers += scheduledWorkers
		if scheduledWorkers > metrics.ZipperSpanNativeScheduledWorkersMax {
			metrics.ZipperSpanNativeScheduledWorkersMax = scheduledWorkers
		}
		recordSpanNativeWorkUnitMetrics(&metrics, workerRanges)
	}
	if workerPool != nil {
		var err error
		if leafLogOutput != nil && scheduledWorkers > 1 {
			err = workerPool.RunSeeded(scheduledWorkers, len(workerRanges), runRange)
		} else {
			err = workerPool.Run(scheduledWorkers, len(workerRanges), runRange)
		}
		if err != nil {
			finishScheduleStats()
			metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
			return ApplyResult{Metrics: metrics}, true, err
		}
	} else if scheduledWorkers <= 1 {
		for job := range workerRanges {
			runRange(0, job)
		}
	} else if leafLogOutput != nil {
		var nextJob int64 = int64(scheduledWorkers - 1)
		var wg sync.WaitGroup
		worker := func(workerID int) {
			defer wg.Done()
			if workerID < len(workerRanges) {
				runRange(workerID, workerID)
			}
			for {
				job := int(atomic.AddInt64(&nextJob, 1))
				if job >= len(workerRanges) {
					return
				}
				runRange(workerID, job)
			}
		}
		for workerID := 0; workerID < scheduledWorkers; workerID++ {
			wg.Add(1)
			go worker(workerID)
		}
		wg.Wait()
	} else {
		var nextJob int64 = -1
		var wg sync.WaitGroup
		worker := func(workerID int) {
			defer wg.Done()
			for {
				job := int(atomic.AddInt64(&nextJob, 1))
				if job >= len(workerRanges) {
					return
				}
				runRange(workerID, job)
			}
		}
		for workerID := 0; workerID < scheduledWorkers; workerID++ {
			wg.Add(1)
			go worker(workerID)
		}
		wg.Wait()
	}
	finishScheduleStats()

	var retired []uint64
	for i := range rangeMetrics {
		mergeMetrics(&metrics, &rangeMetrics[i])
		if len(rangeRetired[i]) > 0 {
			retired = append(retired, rangeRetired[i]...)
		}
		if len(rangeSplits[i].splits) > 0 {
			outputs.splitRanges = append(outputs.splitRanges, rangeSplits[i])
		}
	}
	if firstErr != nil {
		metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
		return ApplyResult{Metrics: metrics, PendingRetiredPages: retired}, true, firstErr
	}
	var oldPointerRefs PointerRefCounts
	var oldEntriesRemoved uint64
	if opts.CollectOldPointerRefs {
		for i := range rangeOldPointerRefs {
			oldPointerRefs.merge(&rangeOldPointerRefs[i])
			oldEntriesRemoved += rangeOldEntriesRemoved[i]
		}
	}
	rootReduceStart := time.Now()
	newRootID, err := z.reduceSpanNativeRootWithContext(rootID, spanNativeLeafReplacements{spans: prepared.LeafSpans, outputs: &outputs}, &metrics, &retired, coordinatorScratch)
	metrics.ZipperRootReduceNs += time.Since(rootReduceStart).Nanoseconds()
	metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
	if err != nil {
		return ApplyResult{Metrics: metrics, PendingRetiredPages: retired}, true, err
	}
	return ApplyResult{
		RootID:                  newRootID,
		PendingRetiredPages:     retired,
		Metrics:                 metrics,
		SpanNativeEligible:      true,
		SpanNativeWorkers:       scheduledWorkers,
		SpanNativeUsed:          true,
		OldPointerRefs:          oldPointerRefs,
		OldEntriesRemoved:       oldEntriesRemoved,
		OldPointerRefsCollected: opts.CollectOldPointerRefs,
	}, true, nil
}

func recordSpanNativeWorkUnitMetrics(metrics *adaptive.Metrics, ranges []ReadOnlyLeafSpanWorkerRange) {
	if metrics == nil || len(ranges) == 0 {
		return
	}
	observed := 0
	for i := range ranges {
		r := ranges[i]
		if r.SpanCount <= 0 {
			continue
		}
		metrics.ZipperSpanNativeTaskSpansTotal += r.SpanCount
		metrics.ZipperSpanNativeTaskOpsTotal += r.Ops
		metrics.ZipperSpanNativeTaskBytesTotal += r.Bytes
		if observed == 0 || r.SpanCount < metrics.ZipperSpanNativeTaskSpansMin {
			metrics.ZipperSpanNativeTaskSpansMin = r.SpanCount
		}
		if r.SpanCount > metrics.ZipperSpanNativeTaskSpansMax {
			metrics.ZipperSpanNativeTaskSpansMax = r.SpanCount
		}
		if observed == 0 || r.Ops < metrics.ZipperSpanNativeTaskOpsMin {
			metrics.ZipperSpanNativeTaskOpsMin = r.Ops
		}
		if r.Ops > metrics.ZipperSpanNativeTaskOpsMax {
			metrics.ZipperSpanNativeTaskOpsMax = r.Ops
		}
		if observed == 0 || r.Bytes < metrics.ZipperSpanNativeTaskBytesMin {
			metrics.ZipperSpanNativeTaskBytesMin = r.Bytes
		}
		if r.Bytes > metrics.ZipperSpanNativeTaskBytesMax {
			metrics.ZipperSpanNativeTaskBytesMax = r.Bytes
		}
		if r.SpanCount == 1 {
			metrics.ZipperSpanNativeSingleSpanTasks++
		}
		observed++
	}
}

func validateSpanNativePreparedPlan(ops []batch.Entry, prepared ReadOnlyPrepareResult, opts ApplyOptions) bool {
	if prepared.OmitKeys || prepared.DeleteRanges != 0 || prepared.ColdBuild || !prepared.ExactLeafSpans || len(prepared.LeafSpans) == 0 {
		return false
	}
	if prepared.Maintenance && !(opts.SpanNativeAllowMaintenancePointOps && prepared.PointOps > 0) {
		return false
	}
	spans := prepared.LeafSpans
	expectedPointStart := 0
	var prevHigh []byte
	for i := range spans {
		span := spans[i]
		if span.DeleteRangeStart != span.DeleteRangeEnd || span.PointOpStart < 0 || span.PointOpEnd < 0 || span.PointOpStart != expectedPointStart || span.PointOpEnd > len(ops) || span.PointOpEnd <= span.PointOpStart {
			return false
		}
		if span.LowKey != nil && span.HighKey != nil && bytes.Compare(span.LowKey, span.HighKey) >= 0 {
			return false
		}
		if i > 0 {
			if prevHigh == nil || span.LowKey == nil || bytes.Compare(span.LowKey, prevHigh) < 0 {
				return false
			}
		}
		if span.LowKey != nil && bytes.Compare(ops[span.PointOpStart].Key, span.LowKey) < 0 {
			return false
		}
		if span.HighKey != nil && bytes.Compare(ops[span.PointOpEnd-1].Key, span.HighKey) >= 0 {
			return false
		}
		prevHigh = span.HighKey
		expectedPointStart = span.PointOpEnd
	}
	return expectedPointStart == len(ops)
}

type spanNativeLeafOutput struct {
	ref    page.ChildRef
	splits []Split
}

type spanNativeLeafOutputMode uint8

const (
	spanNativeLeafOutputPages spanNativeLeafOutputMode = iota
	spanNativeLeafOutputLeafLogs
)

type spanNativeLeafSplitRange struct {
	start  int
	splits [][]Split
}

type spanNativeLeafOutputs struct {
	mode        spanNativeLeafOutputMode
	pageIDs     []uint64
	leafLogRefs []byte
	splitRanges []spanNativeLeafSplitRange
}

const spanNativeOutputPoolKeep = 8

var (
	spanNativeLogRefPool     spanNativeByteSlicePool
	spanNativePageIDPool     spanNativeUint64SlicePool
	spanNativeSplitSlicePool spanNativeSplitSlicePoolType
)

type spanNativeByteSlicePool struct {
	mu   sync.Mutex
	bufs [][]byte
}

func (p *spanNativeByteSlicePool) get(size int) []byte {
	if size <= 0 {
		return nil
	}
	p.mu.Lock()
	for i := len(p.bufs) - 1; i >= 0; i-- {
		buf := p.bufs[i]
		if cap(buf) >= size {
			last := len(p.bufs) - 1
			p.bufs[i] = p.bufs[last]
			p.bufs[last] = nil
			p.bufs = p.bufs[:last]
			p.mu.Unlock()
			return buf[:size]
		}
	}
	p.mu.Unlock()
	return make([]byte, size)
}

func (p *spanNativeByteSlicePool) put(buf []byte, maxCap int) {
	if cap(buf) == 0 || cap(buf) > maxCap {
		return
	}
	p.mu.Lock()
	if len(p.bufs) < spanNativeOutputPoolKeep {
		p.bufs = append(p.bufs, buf[:0])
	}
	p.mu.Unlock()
}

type spanNativeUint64SlicePool struct {
	mu   sync.Mutex
	bufs [][]uint64
}

func (p *spanNativeUint64SlicePool) get(size int) []uint64 {
	if size <= 0 {
		return nil
	}
	p.mu.Lock()
	for i := len(p.bufs) - 1; i >= 0; i-- {
		buf := p.bufs[i]
		if cap(buf) >= size {
			last := len(p.bufs) - 1
			p.bufs[i] = p.bufs[last]
			p.bufs[last] = nil
			p.bufs = p.bufs[:last]
			p.mu.Unlock()
			return buf[:size]
		}
	}
	p.mu.Unlock()
	return make([]uint64, size)
}

func (p *spanNativeUint64SlicePool) put(buf []uint64, maxCap int) {
	if cap(buf) == 0 || cap(buf) > maxCap {
		return
	}
	p.mu.Lock()
	if len(p.bufs) < spanNativeOutputPoolKeep {
		p.bufs = append(p.bufs, buf[:0])
	}
	p.mu.Unlock()
}

type spanNativeSplitSlicePoolType struct {
	mu   sync.Mutex
	bufs [][][]Split
}

func acquireSpanNativeSplitSlices(size int) [][]Split {
	if size <= 0 {
		return nil
	}
	spanNativeSplitSlicePool.mu.Lock()
	for i := len(spanNativeSplitSlicePool.bufs) - 1; i >= 0; i-- {
		buf := spanNativeSplitSlicePool.bufs[i]
		if cap(buf) >= size {
			last := len(spanNativeSplitSlicePool.bufs) - 1
			spanNativeSplitSlicePool.bufs[i] = spanNativeSplitSlicePool.bufs[last]
			spanNativeSplitSlicePool.bufs[last] = nil
			spanNativeSplitSlicePool.bufs = spanNativeSplitSlicePool.bufs[:last]
			spanNativeSplitSlicePool.mu.Unlock()
			return buf[:size]
		}
	}
	spanNativeSplitSlicePool.mu.Unlock()
	return make([][]Split, size)
}

func releaseSpanNativeSplitSlices(buf [][]Split) {
	if cap(buf) == 0 || cap(buf) > mergeSpanNativeOutputKeepCap {
		return
	}
	clear(buf)
	spanNativeSplitSlicePool.mu.Lock()
	if len(spanNativeSplitSlicePool.bufs) < spanNativeOutputPoolKeep {
		spanNativeSplitSlicePool.bufs = append(spanNativeSplitSlicePool.bufs, buf[:0])
	}
	spanNativeSplitSlicePool.mu.Unlock()
}

func (s *mergeScratch) acquireSpanNativeLeafOutputs(count int, leafLogs bool) spanNativeLeafOutputs {
	if count <= 0 {
		return spanNativeLeafOutputs{}
	}
	if leafLogs {
		bytesNeeded := count * page.LogRecordRefSize
		return spanNativeLeafOutputs{mode: spanNativeLeafOutputLeafLogs, leafLogRefs: spanNativeLogRefPool.get(bytesNeeded)}
	}
	return spanNativeLeafOutputs{mode: spanNativeLeafOutputPages, pageIDs: spanNativePageIDPool.get(count)}
}

func (o *spanNativeLeafOutputs) release() {
	if o == nil {
		return
	}
	switch o.mode {
	case spanNativeLeafOutputLeafLogs:
		spanNativeLogRefPool.put(o.leafLogRefs, mergeSpanNativeOutputLogKeepBytes)
	case spanNativeLeafOutputPages:
		spanNativePageIDPool.put(o.pageIDs, mergeSpanNativeOutputPageKeepCap)
	}
	for i := range o.splitRanges {
		releaseSpanNativeSplitSlices(o.splitRanges[i].splits)
		o.splitRanges[i] = spanNativeLeafSplitRange{}
	}
	*o = spanNativeLeafOutputs{}
}

func (o *spanNativeLeafOutputs) setRef(index int, ref page.ChildRef) error {
	if o == nil || index < 0 {
		return page.ErrInvalidPageType
	}
	switch o.mode {
	case spanNativeLeafOutputLeafLogs:
		if ref.Kind != page.ChildRefLeafLog {
			return page.ErrInvalidPageType
		}
		offset := index * page.LogRecordRefSize
		if offset < 0 || offset+page.LogRecordRefSize > len(o.leafLogRefs) {
			return page.ErrInvalidPageType
		}
		page.EncodeLogRecordRef(o.leafLogRefs[offset:offset+page.LogRecordRefSize], ref.Log)
		return nil
	case spanNativeLeafOutputPages:
		if ref.Kind != page.ChildRefPage || index >= len(o.pageIDs) {
			return page.ErrInvalidPageType
		}
		o.pageIDs[index] = ref.Page
		return nil
	default:
		return page.ErrInvalidPageType
	}
}

func (o *spanNativeLeafOutputs) output(index int) spanNativeLeafOutput {
	if o == nil || index < 0 {
		return spanNativeLeafOutput{}
	}
	out := spanNativeLeafOutput{splits: o.splits(index)}
	switch o.mode {
	case spanNativeLeafOutputLeafLogs:
		offset := index * page.LogRecordRefSize
		if offset >= 0 && offset+page.LogRecordRefSize <= len(o.leafLogRefs) {
			out.ref = page.LeafLogChildRef(page.DecodeLogRecordRef(o.leafLogRefs[offset : offset+page.LogRecordRefSize]))
		}
	case spanNativeLeafOutputPages:
		if index < len(o.pageIDs) {
			out.ref = page.PageChildRef(o.pageIDs[index])
		}
	}
	return out
}

func (o *spanNativeLeafOutputs) splits(index int) []Split {
	if o == nil || len(o.splitRanges) == 0 {
		return nil
	}
	for i := range o.splitRanges {
		r := &o.splitRanges[i]
		if index < r.start {
			return nil
		}
		local := index - r.start
		if local >= 0 && local < len(r.splits) {
			return r.splits[local]
		}
	}
	return nil
}

type spanNativeLeafReplacements struct {
	spans   []ReadOnlyLeafSpan
	outputs *spanNativeLeafOutputs
	start   int
}

func (r spanNativeLeafReplacements) len() int {
	return len(r.spans)
}

func (r spanNativeLeafReplacements) output(index int) spanNativeLeafOutput {
	if r.outputs == nil || index < 0 || index >= len(r.spans) {
		return spanNativeLeafOutput{}
	}
	return r.outputs.output(r.start + index)
}

func (r spanNativeLeafReplacements) slice(start, end int) spanNativeLeafReplacements {
	return spanNativeLeafReplacements{spans: r.spans[start:end], outputs: r.outputs, start: r.start + start}
}

func (z *Zipper) reduceSpanNativeRootWithContext(rootID uint64, replacements spanNativeLeafReplacements, metrics *adaptive.Metrics, retired *[]uint64, scratch *mergeScratch) (uint64, error) {
	if replacements.len() == 0 {
		return 0, page.ErrInvalidPageType
	}
	if spanNativeReplacementsCoverWholeRoot(replacements) {
		if err := z.retireSpanNativeWholeRootInternalPages(rootID, replacements, retired, scratch); err != nil {
			return 0, err
		}
		if replacements.len() == 1 {
			out := replacements.output(0)
			if len(out.splits) == 0 {
				var one [1]Split
				one[0] = spanNativeReplacementHeadSplit(replacements.spans[0], out)
				return z.reduceSpanNativeRootWithScratch(one[:], metrics, scratch)
			}
		}
		refCap := replacements.len()
		for i := 0; i < replacements.len(); i++ {
			refCap += len(replacements.output(i).splits)
		}
		refs := scratch.acquireSpanRootRefs(refCap)
		defer scratch.releaseSpanRootRefs(refs)
		for i := 0; i < replacements.len(); i++ {
			out := replacements.output(i)
			refs = append(refs, spanNativeReplacementHeadSplit(replacements.spans[i], out))
			refs = append(refs, out.splits...)
		}
		return z.reduceSpanNativeRootWithScratch(refs, metrics, scratch)
	}
	ref, splits, changed, err := z.stitchSpanNativeRecursive(page.PageChildRef(rootID), nil, nil, replacements, metrics, retired, scratch)
	if err != nil {
		return 0, err
	}
	if !changed {
		return 0, fmt.Errorf("%w: no replacement changed root", ErrSpanNativeReducerValidation)
	}
	if len(splits) == 0 {
		var one [1]Split
		one[0] = Split{Key: []byte{}, Ref: ref}
		return z.reduceSpanNativeRootWithScratch(one[:], metrics, scratch)
	}
	refs := scratch.acquireSpanRootRefs(len(splits) + 1)
	defer scratch.releaseSpanRootRefs(refs)
	refs = append(refs, Split{Key: []byte{}, Ref: ref})
	refs = append(refs, splits...)
	return z.reduceSpanNativeRootWithScratch(refs, metrics, scratch)
}

func spanNativeReplacementHeadSplit(span ReadOnlyLeafSpan, output spanNativeLeafOutput) Split {
	spanKey := span.LowKey
	if spanKey == nil {
		spanKey = []byte{}
	}
	return Split{Key: spanKey, Ref: output.ref}
}

func spanNativeReplacementParentKey(currentKey []byte, replacements spanNativeLeafReplacements) []byte {
	if replacements.len() == 0 {
		return currentKey
	}
	low := replacements.spans[0].LowKey
	if low == nil {
		return []byte{}
	}
	if currentKey == nil || bytes.Compare(low, currentKey) < 0 {
		return low
	}
	return currentKey
}

func (z *Zipper) retireSpanNativeWholeRootInternalPages(rootID uint64, replacements spanNativeLeafReplacements, retired *[]uint64, scratch *mergeScratch) error {
	if retired == nil || replacements.len() == 0 {
		return nil
	}
	spanIdx := 0
	var walk func(page.ChildRef) error
	walk = func(ref page.ChildRef) error {
		if spanIdx < replacements.len() && replacements.spans[spanIdx].Ref == ref {
			spanIdx++
			return nil
		}
		if ref.Kind != page.ChildRefPage {
			return nil
		}
		oldNode, oldFromPager, leafScratch, leafScratchRef, _, err := z.loadNodeRef(ref, scratch)
		if err != nil {
			return err
		}
		if leafScratchRef {
			defer releaseLeafPageScratch(scratch, leafScratch)
		}
		switch oldNode.Type() {
		case page.PageTypeLeaf, 0:
			return nil
		case page.PageTypeInternal:
			if oldFromPager && ref.Page != 0 {
				*retired = append(*retired, ref.Page)
			}
			for i := uint16(0); i < oldNode.Count(); i++ {
				childRef, err := oldNode.GetInternalChildRef(i)
				if err != nil {
					return err
				}
				if err := walk(childRef); err != nil {
					return err
				}
			}
			return nil
		default:
			return page.ErrInvalidPageType
		}
	}
	if err := walk(page.PageChildRef(rootID)); err != nil {
		return err
	}
	if spanIdx != replacements.len() {
		return fmt.Errorf("%w: whole-root replacement coverage mismatch", ErrSpanNativeReducerValidation)
	}
	return nil
}

func spanNativeReplacementsCoverWholeRoot(replacements spanNativeLeafReplacements) bool {
	if replacements.len() == 0 || replacements.spans[0].LowKey != nil || replacements.spans[replacements.len()-1].HighKey != nil {
		return false
	}
	prevHigh := replacements.spans[0].HighKey
	for i := 1; i < replacements.len(); i++ {
		if !bytes.Equal(replacements.spans[i].LowKey, prevHigh) {
			return false
		}
		prevHigh = replacements.spans[i].HighKey
	}
	return true
}

func (z *Zipper) stitchSpanNativeRecursive(ref page.ChildRef, low, high []byte, replacements spanNativeLeafReplacements, metrics *adaptive.Metrics, retired *[]uint64, scratch *mergeScratch) (page.ChildRef, []Split, bool, error) {
	oldNode, oldFromPager, leafScratch, leafScratchRef, loadSource, err := z.loadNodeRef(ref, scratch)
	if err != nil {
		return page.ChildRef{}, nil, false, err
	}
	recordZipperNodeLoad(metrics, ref, oldNode, loadSource)
	if leafScratchRef {
		defer releaseLeafPageScratch(scratch, leafScratch)
	}

	switch oldNode.Type() {
	case page.PageTypeLeaf, 0:
		for i := 0; i < replacements.len(); i++ {
			if replacements.spans[i].Ref == ref {
				out := replacements.output(i)
				return out.ref, out.splits, true, nil
			}
		}
		return ref, nil, false, nil
	case page.PageTypeInternal:
		count := oldNode.Count()
		copyKeys := oldNode.InternalBaseDeltaEnabled()
		writer := spanNativeSplitLevelWriter{z: z, metrics: metrics, scratch: scratch}
		defer writer.abort()
		changed := false
		replacementIdx := 0
		for i := uint16(0); i < count; i++ {
			key, childRef, err := oldNode.GetInternalEntryRefView(i)
			if err != nil {
				return page.ChildRef{}, nil, false, err
			}
			if key == nil {
				key = []byte{}
			} else if copyKeys {
				key = append([]byte(nil), key...)
			}
			childLow := low
			if len(key) != 0 {
				childLow = key
			}
			childHigh := high
			if i+1 < count {
				nextKey, _, err := oldNode.GetInternalEntryRefView(i + 1)
				if err != nil {
					return page.ChildRef{}, nil, false, err
				}
				if nextKey == nil {
					nextKey = []byte{}
				} else if copyKeys {
					nextKey = append([]byte(nil), nextKey...)
				}
				childHigh = nextKey
			}

			for replacementIdx < replacements.len() && spanNativeReplacementBeforeRange(replacements.spans[replacementIdx], childLow) {
				replacementIdx++
			}
			childReplacementStart := replacementIdx
			childReplacementEnd := childReplacementStart
			for childReplacementEnd < replacements.len() && spanNativeReplacementOverlapsRange(replacements.spans[childReplacementEnd], childLow, childHigh) {
				childReplacementEnd++
			}
			childReplacements := replacements.slice(childReplacementStart, childReplacementEnd)
			replacementIdx = childReplacementEnd
			if childReplacements.len() == 0 {
				if err := writer.append(Split{Key: key, Ref: childRef}); err != nil {
					return page.ChildRef{}, nil, false, err
				}
				continue
			}
			if childReplacements.len() == 1 && childReplacements.spans[0].Ref == childRef {
				changed = true
				out := childReplacements.output(0)
				if err := writer.append(Split{Key: spanNativeReplacementParentKey(key, childReplacements), Ref: out.ref}); err != nil {
					return page.ChildRef{}, nil, false, err
				}
				for _, s := range out.splits {
					if err := writer.append(s); err != nil {
						return page.ChildRef{}, nil, false, err
					}
				}
				continue
			}

			newRef, childSplits, childChanged, err := z.stitchSpanNativeRecursive(childRef, childLow, childHigh, childReplacements, metrics, retired, scratch)
			if err != nil {
				return page.ChildRef{}, nil, false, err
			}
			if !childChanged {
				return page.ChildRef{}, nil, false, fmt.Errorf("%w: replacement span did not match child range", ErrSpanNativeReducerValidation)
			}
			changed = true
			if err := writer.append(Split{Key: spanNativeReplacementParentKey(key, childReplacements), Ref: newRef}); err != nil {
				return page.ChildRef{}, nil, false, err
			}
			for _, s := range childSplits {
				if err := writer.append(s); err != nil {
					return page.ChildRef{}, nil, false, err
				}
			}
		}
		if !changed {
			return ref, nil, false, nil
		}
		if oldFromPager && retired != nil && ref.Kind == page.ChildRefPage && ref.Page != 0 {
			*retired = append(*retired, ref.Page)
		}
		refs, err := writer.finish()
		if err != nil {
			return page.ChildRef{}, nil, false, err
		}
		if len(refs) == 0 {
			return page.ChildRef{}, nil, false, fmt.Errorf("%w: empty stitched split level", ErrSpanNativeReducerValidation)
		}
		return refs[0].Ref, refs[1:], true, nil
	default:
		return page.ChildRef{}, nil, false, page.ErrInvalidPageType
	}
}

type spanNativeSplitLevelWriter struct {
	z                *Zipper
	metrics          *adaptive.Metrics
	scratch          *mergeScratch
	nextLevelSegment mergeSplitSegment
	nextLevelNodes   []Split
	currentBuilder   *node.Builder
	currentStartKey  []byte
}

func (w *spanNativeSplitLevelWriter) append(child Split) error {
	if w.currentBuilder == nil {
		allocHint := uint64(0)
		if child.Ref.Kind == page.ChildRefPage {
			allocHint = child.Ref.Page
		}
		pid, err := w.z.allocator.Alloc(allocHint)
		if err != nil {
			return err
		}
		data, err := w.z.pager.GetForWrite(pid)
		if err != nil {
			return err
		}
		w.currentBuilder = w.z.newPooledBuilderForType(data, page.PageTypeInternal, nil)
		w.currentBuilder.SetPageID(pid)
		w.currentStartKey = child.Key
		w.currentBuilder.SetInternalFenceBoundsBorrowed(w.currentStartKey, nil)
	}

	childKey := child.Key
	if childKey == nil {
		childKey = []byte{}
	}
	childSize := 2 + 8 + len(childKey)
	if child.Ref.Kind == page.ChildRefLeafLog {
		childSize = 2 + page.LogRecordRefSize + len(childKey)
	} else if w.z.indexInternalBaseDelta {
		childSize = 2 + 4 + len(childKey)
	}
	var err error
	if w.z.internalSoftFull(w.currentBuilder, childSize) {
		err = node.ErrNodeFull
	} else {
		err = w.currentBuilder.AddInternalChildRef(childKey, child.Ref)
		if err == nil {
			recordZipperInternalChildRef(w.metrics, child.Ref)
		}
	}
	if err == node.ErrNodeFull {
		finishedPageID := w.finishCurrent()

		pid, allocErr := w.z.allocator.Alloc(finishedPageID)
		if allocErr != nil {
			return allocErr
		}
		data, getErr := w.z.pager.GetForWrite(pid)
		if getErr != nil {
			return getErr
		}
		w.currentBuilder = w.z.newPooledBuilderForType(data, page.PageTypeInternal, nil)
		w.currentBuilder.SetPageID(pid)
		w.currentStartKey = child.Key
		w.currentBuilder.SetInternalFenceBoundsBorrowed(w.currentStartKey, nil)

		if addErr := w.currentBuilder.AddInternalChildRef(childKey, child.Ref); addErr != nil {
			return fmt.Errorf("zipper: span split retry page=%d key_len=%d ref=%+v: %w", pid, len(childKey), child.Ref, addErr)
		}
		recordZipperInternalChildRef(w.metrics, child.Ref)
	} else if err != nil {
		return err
	}
	return nil
}

func (w *spanNativeSplitLevelWriter) finishCurrent() uint64 {
	pageID := w.currentBuilder.PageID()
	w.currentBuilder.FinishNoNode()
	recordZipperInternalPageWrite(w.metrics)
	if w.nextLevelNodes == nil {
		w.nextLevelSegment = newMergeSplitSegment(w.scratch)
		w.nextLevelNodes = w.nextLevelSegment.data
	}
	w.nextLevelNodes = w.nextLevelSegment.append(Split{Key: w.currentStartKey, Ref: page.PageChildRef(pageID)})
	releasePooledBuilder(w.currentBuilder)
	w.currentBuilder = nil
	return pageID
}

func (w *spanNativeSplitLevelWriter) finish() ([]Split, error) {
	if w.currentBuilder != nil {
		w.finishCurrent()
	}
	return w.nextLevelNodes, nil
}

func (w *spanNativeSplitLevelWriter) abort() {
	if w.currentBuilder != nil {
		releasePooledBuilder(w.currentBuilder)
		w.currentBuilder = nil
	}
}

func spanNativeReplacementBeforeRange(span ReadOnlyLeafSpan, low []byte) bool {
	return span.HighKey != nil && low != nil && bytes.Compare(span.HighKey, low) <= 0
}

func spanNativeReplacementOverlapsRange(span ReadOnlyLeafSpan, low, high []byte) bool {
	if span.HighKey != nil && low != nil && bytes.Compare(span.HighKey, low) <= 0 {
		return false
	}
	if high != nil && span.LowKey != nil && bytes.Compare(span.LowKey, high) >= 0 {
		return false
	}
	return true
}

func (z *Zipper) reduceSpanNativeRoot(currentLevelNodes []Split, metrics *adaptive.Metrics) (uint64, error) {
	return z.reduceSpanNativeRootWithScratch(currentLevelNodes, metrics, nil)
}

func (z *Zipper) reduceSpanNativeRootWithScratch(currentLevelNodes []Split, metrics *adaptive.Metrics, scratch *mergeScratch) (uint64, error) {
	if err := validateSpanNativeReducerRefs(currentLevelNodes); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrSpanNativeReducerValidation, err)
	}
	for {
		if len(currentLevelNodes) == 1 {
			return z.ensureRootPage(currentLevelNodes[0].Key, currentLevelNodes[0].Ref, metrics)
		}
		metrics.ZipperRootSplitLevels++
		nextLevelNodes, err := z.reduceSpanNativeSplitLevel(currentLevelNodes, metrics, scratch)
		if err != nil {
			return 0, err
		}
		currentLevelNodes = nextLevelNodes
	}
}

func validateSpanNativeReducerRefs(refs []Split) error {
	if len(refs) == 0 {
		return page.ErrInvalidPageType
	}
	if len(refs[0].Key) != 0 {
		return page.ErrInvalidPageType
	}
	prev := refs[0].Key
	for i := 1; i < len(refs); i++ {
		if len(refs[i].Key) == 0 || bytes.Compare(refs[i].Key, prev) <= 0 {
			return page.ErrInvalidPageType
		}
		prev = refs[i].Key
	}
	return nil
}

func (z *Zipper) reduceSpanNativeSplitLevel(currentLevelNodes []Split, metrics *adaptive.Metrics, scratch *mergeScratch) ([]Split, error) {
	nextLevelSegment := newMergeSplitSegment(scratch)
	nextLevelNodes := nextLevelSegment.data
	var currentBuilder *node.Builder
	var currentStartKey []byte

	for i, child := range currentLevelNodes {
		if currentBuilder == nil {
			allocHint := uint64(0)
			if child.Ref.Kind == page.ChildRefPage {
				allocHint = child.Ref.Page
			}
			pid, err := z.allocator.Alloc(allocHint)
			if err != nil {
				return nil, err
			}
			data, err := z.pager.GetForWrite(pid)
			if err != nil {
				return nil, err
			}
			currentBuilder = z.newBuilderForType(data, page.PageTypeInternal, nil)
			currentBuilder.SetPageID(pid)
			currentStartKey = child.Key
			currentBuilder.SetInternalFenceBoundsBorrowed(currentStartKey, nil)
		}

		childKey := child.Key
		if childKey == nil {
			childKey = []byte{}
		}
		childSize := 2 + 8 + len(childKey)
		if child.Ref.Kind == page.ChildRefLeafLog {
			childSize = 2 + page.LogRecordRefSize + len(childKey)
		} else if z.indexInternalBaseDelta {
			childSize = 2 + 4 + len(childKey)
		}
		var err error
		if z.internalSoftFull(currentBuilder, childSize) {
			err = node.ErrNodeFull
		} else {
			err = currentBuilder.AddInternalChildRef(childKey, child.Ref)
			if err == nil {
				recordZipperInternalChildRef(metrics, child.Ref)
			}
		}
		if err == node.ErrNodeFull {
			currentBuilder.FinishNoNode()
			recordZipperInternalPageWrite(metrics)
			nextLevelNodes = nextLevelSegment.append(Split{Key: currentStartKey, Ref: page.PageChildRef(currentBuilder.PageID())})

			pid, allocErr := z.allocator.Alloc(currentBuilder.PageID())
			if allocErr != nil {
				return nil, allocErr
			}
			data, getErr := z.pager.GetForWrite(pid)
			if getErr != nil {
				return nil, getErr
			}
			currentBuilder = z.newBuilderForType(data, page.PageTypeInternal, nil)
			currentBuilder.SetPageID(pid)
			currentStartKey = child.Key
			currentBuilder.SetInternalFenceBoundsBorrowed(currentStartKey, nil)

			if addErr := currentBuilder.AddInternalChildRef(childKey, child.Ref); addErr != nil {
				return nil, fmt.Errorf("zipper: span root retry page=%d key_len=%d ref=%+v: %w", pid, len(childKey), child.Ref, addErr)
			}
			recordZipperInternalChildRef(metrics, child.Ref)
		} else if err != nil {
			return nil, err
		}

		if i == len(currentLevelNodes)-1 {
			currentBuilder.FinishNoNode()
			recordZipperInternalPageWrite(metrics)
			nextLevelNodes = nextLevelSegment.append(Split{Key: currentStartKey, Ref: page.PageChildRef(currentBuilder.PageID())})
			currentBuilder = nil
		}
	}
	return nextLevelNodes, nil
}
