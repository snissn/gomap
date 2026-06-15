package caching

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type backendFlushSpanRunPlanner interface {
	PlanFlushSpanRun(backenddb.FlushSpanRunPlanRequest) (backenddb.FlushSpanRunMetadata, error)
}

type backendFlushSpanRunChunkPlanner interface {
	PlanFlushSpanRunChunks(backenddb.FlushSpanRunPlanRequest, int) (backenddb.FlushSpanRunChunkPlan, error)
}

type canonicalFlushRun struct {
	pointOps []batch.Entry

	sourceMemtables  int
	sourcePointOps   int
	plannedPointOps  int
	shadowedPointOps int
	deletePointOps   int
	rangeDeleteOps   int
	rangeBarriers    int
	laneBarriers     int
}

func (r *canonicalFlushRun) release() {
	if r == nil {
		return
	}
	putEntrySlice(r.pointOps)
	r.pointOps = nil
}

func (db *DB) buildCanonicalFlushRun(units []flushUnit, totalLen int, totalSpans int) (*canonicalFlushRun, error) {
	if db == nil {
		return nil, errors.New("cachingdb: nil db")
	}
	if len(units) == 0 {
		return &canonicalFlushRun{}, nil
	}
	if totalLen < 0 {
		totalLen = 0
	}

	chunkCap := db.flushBuildChunkCap
	if chunkCap < 0 {
		chunkCap = 8192
	}
	if chunkCap <= 0 {
		chunkCap = 8192
	}

	useParallel := totalSpans == 0 &&
		db.flushBuildConcurrency > 1 &&
		totalLen >= db.flushBuildMinEntries &&
		len(units) >= db.flushBuildMinUnits &&
		runtime.GOMAXPROCS(0) > 1

	type buildResult struct {
		idx       int
		runs      [][]batch.Entry
		deleteOps int
		err       error
	}

	unitRuns := getUnitRuns(len(units))
	unitDeleteOps := make([]int, len(units))
	releaseUnitRuns := func() {
		for i := range unitRuns {
			for _, run := range unitRuns[i] {
				putEntrySlice(run)
			}
			putEntryRuns(unitRuns[i])
		}
		putUnitRuns(unitRuns)
	}
	defer releaseUnitRuns()

	if useParallel {
		jobs := make(chan int, len(units))
		results := make(chan buildResult, len(units))
		for i := range units {
			jobs <- i
		}
		close(jobs)

		workers := db.flushBuildConcurrency
		if workers <= 0 {
			workers = 1
		}
		if db.flushBuildAutoConcurrency && totalLen > 0 {
			bytesPerEntry := int64(0)
			for i := range units {
				bytesPerEntry += units[i].memBytes
			}
			bytesPerEntry /= int64(totalLen)
			switch {
			case bytesPerEntry <= 64:
				if workers > 4 {
					workers = 4
				}
			case bytesPerEntry <= 256:
				if workers > 6 {
					workers = 6
				}
			}
		}
		if workers > len(units) {
			workers = len(units)
		}
		if workers < 1 {
			workers = 1
		}

		done := make(chan struct{}, workers)
		closeCh := db.closeCh
		for w := 0; w < workers; w++ {
			go func() {
				defer func() { done <- struct{}{} }()
				for idx := range jobs {
					select {
					case <-closeCh:
						results <- buildResult{idx: idx, err: errDBClosing}
						continue
					default:
					}
					runs, deleteOps, err := buildOpRuns(units[idx].mem, chunkCap)
					results <- buildResult{idx: idx, runs: runs, deleteOps: deleteOps, err: err}
				}
			}()
		}
		go func() {
			for i := 0; i < workers; i++ {
				<-done
			}
			close(results)
		}()

		failed := false
		var firstErr error
		for res := range results {
			if res.err != nil {
				if !failed {
					firstErr = res.err
				}
				failed = true
				for _, run := range res.runs {
					putEntrySlice(run)
				}
				putEntryRuns(res.runs)
				continue
			}
			if failed {
				for _, run := range res.runs {
					putEntrySlice(run)
				}
				putEntryRuns(res.runs)
				continue
			}
			unitRuns[res.idx] = res.runs
			unitDeleteOps[res.idx] = res.deleteOps
		}
		if failed {
			return nil, fmt.Errorf("cachingdb: flush build failed: %w", firstErr)
		}
	} else {
		for i := range units {
			runs, deleteOps, err := buildOpRuns(units[i].mem, chunkCap)
			if err != nil {
				return nil, err
			}
			unitRuns[i] = runs
			unitDeleteOps[i] = deleteOps
		}
	}

	builtPointOps := 0
	for i := range unitRuns {
		for _, run := range unitRuns[i] {
			builtPointOps += len(run)
		}
	}
	out := &canonicalFlushRun{
		sourceMemtables: len(units),
		sourcePointOps:  builtPointOps,
		rangeDeleteOps:  totalSpans,
		rangeBarriers:   flushUnitRangeBarrierCount(units),
		pointOps:        getEntrySlice(builtPointOps),
	}
	for _, n := range unitDeleteOps {
		out.deletePointOps += n
	}

	heap := getOpMergeHeap(len(unitRuns))
	defer func() { putOpMergeHeap(heap) }()
	for i := range unitRuns {
		if len(unitRuns[i]) == 0 {
			continue
		}
		it := newOpRunIter(unitRuns[i])
		if it.Valid() {
			priority := len(unitRuns) - 1 - i
			heap = append(heap, opMergeItem{iter: it, priority: priority, key: it.Key()})
		}
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		(&heap).down(i, len(heap))
	}

	plannedDeletes := 0
	for len(heap) > 0 {
		top := heap.pop()
		currentKey := top.key

		for len(heap) > 0 {
			next := heap.peek()
			if next != nil && bytes.Equal(next.key, currentKey) {
				shadowed := heap.pop()
				out.shadowedPointOps++
				shadowed.iter.Next()
				if shadowed.iter.Valid() {
					shadowed.key = shadowed.iter.Key()
					heap.push(shadowed)
				}
				continue
			}
			break
		}

		entry := top.iter.Entry()
		out.pointOps = append(out.pointOps, entry)
		if entry.Type == batch.OpDelete {
			plannedDeletes++
		}

		top.iter.Next()
		if top.iter.Valid() {
			top.key = top.iter.Key()
			heap.push(top)
		}
	}
	out.plannedPointOps = len(out.pointOps)
	out.deletePointOps = plannedDeletes
	return out, nil
}

func (db *DB) materializeCanonicalRunDeferredValueLogPointers(run *canonicalFlushRun, syncFlush bool, laneID int) error {
	if db == nil || run == nil || len(run.pointOps) == 0 || !db.deferredValueLogEnabled() {
		return nil
	}
	allowPointers := db.allowValueLogPointers()
	if !allowPointers {
		return nil
	}
	const maxDeferredInlineGroupKeys = 32768

	vlogLane := (*lane)(nil)
	ensureVlogLane := func() error {
		if vlogLane != nil {
			return nil
		}
		l, err := db.pickLane(false, laneID)
		if err != nil {
			return err
		}
		vlogLane = l
		return nil
	}

	durability := journalDurabilityNone
	_ = syncFlush // value-log flush/sync is performed once before backend publish.

	idxs := make([]int, 0, 1024)
	keys := getValueLogKeys(1024)
	vals := getValueLogKeys(1024)
	defer func() {
		putValueLogKeys(keys)
		putValueLogKeys(vals)
	}()

	flushGroup := func() error {
		if len(idxs) == 0 {
			return nil
		}
		if len(keys) != len(vals) || len(keys) != len(idxs) {
			return errors.New("cachingdb: canonical deferred value-log group mismatch")
		}
		if err := ensureVlogLane(); err != nil {
			return err
		}
		records, groups, outerArena, err := db.buildOuterLeafValueRecords(keys, vals)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			putValueLogRecordsNoClear(records)
			putOuterLeafArena(outerArena)
			idxs = idxs[:0]
			keys = keys[:0]
			vals = vals[:0]
			return nil
		}
		startRID := db.nextRID.Add(uint64(len(records))) - uint64(len(records)) + 1
		for i := range records {
			records[i].RID = startRID + uint64(i)
		}
		ptrs, err := db.appendValueLogForRecords(vlogLane, records, durability)
		putValueLogRecordsNoClear(records)
		putOuterLeafArena(outerArena)
		if err != nil {
			return err
		}
		if len(ptrs) != len(records) {
			putValueLogPtrs(ptrs)
			return fmt.Errorf("cachingdb: deferred value-log returned %d ptrs for %d records", len(ptrs), len(records))
		}
		if len(ptrs) != len(groups) {
			putValueLogPtrs(ptrs)
			return errors.New("cachingdb: canonical deferred value-log group/pointer count mismatch")
		}
		for i := range groups {
			ptr := ptrs[i]
			group := groups[i]
			if group.start < 0 || group.end < group.start || group.end > len(idxs) {
				putValueLogPtrs(ptrs)
				return errors.New("cachingdb: canonical deferred value-log source group out of range")
			}
			for srcPos := group.start; srcPos < group.end; srcPos++ {
				opIdx := idxs[srcPos]
				run.pointOps[opIdx].IsPtr = true
				run.pointOps[opIdx].ValuePtr = ptr
				run.pointOps[opIdx].Value = nil
			}
		}
		putValueLogPtrs(ptrs)
		if durability == journalDurabilityNone {
			db.backendReadVlogDirtySeq.Add(1)
		}
		idxs = idxs[:0]
		keys = keys[:0]
		vals = vals[:0]
		return nil
	}

	for i := range run.pointOps {
		op := &run.pointOps[i]
		if op.Type != batch.OpPut || op.IsPtr || !db.shouldWriteViaValueLogForKeyValue(op.Key, op.Value) {
			continue
		}
		idxs = append(idxs, i)
		keys = append(keys, op.Key)
		vals = append(vals, op.Value)
		if len(idxs) >= maxDeferredInlineGroupKeys {
			if err := flushGroup(); err != nil {
				return err
			}
		}
	}
	if err := flushGroup(); err != nil {
		return err
	}
	if vlogLane != nil {
		retainPath := db.currentValueLogPath(vlogLane)
		if retainPath != "" {
			db.markValueLogRetain(retainPath)
		}
	}
	return nil
}

func flushRunEntryByteCount(op batch.Entry) int {
	n := len(op.Key) + len(op.Value)
	if op.IsPtr {
		n += 16
	}
	return n
}

func buildEntryCountFlushSpanRunChunks(ops []batch.Entry, capEntries int) []backenddb.FlushSpanRunBackendChunk {
	if len(ops) == 0 {
		return nil
	}
	if capEntries <= 0 {
		capEntries = len(ops)
	}
	chunks := make([]backenddb.FlushSpanRunBackendChunk, 0, (len(ops)+capEntries-1)/capEntries)
	for start := 0; start < len(ops); {
		end := start + capEntries
		if end > len(ops) {
			end = len(ops)
		}
		byteCount := 0
		for i := start; i < end; i++ {
			byteCount += flushRunEntryByteCount(ops[i])
		}
		chunks = append(chunks, backenddb.FlushSpanRunBackendChunk{ChunkIndex: len(chunks), PointOpStart: start, PointOpEnd: end, ByteCount: byteCount})
		start = end
	}
	return chunks
}

func buildLeafAwareFlushSpanRunChunks(ops []batch.Entry, spans []backenddb.FlushSpanRunTargetLeafSpan, capEntries int) ([]backenddb.FlushSpanRunBackendChunk, bool) {
	if len(ops) == 0 {
		return nil, true
	}
	if capEntries <= 0 {
		capEntries = len(ops)
	}
	if len(spans) == 0 {
		return buildEntryCountFlushSpanRunChunks(ops, capEntries), false
	}
	if spans[0].PointOpStart != 0 || spans[len(spans)-1].PointOpEnd != len(ops) {
		return buildEntryCountFlushSpanRunChunks(ops, capEntries), false
	}
	chunks := make([]backenddb.FlushSpanRunBackendChunk, 0, len(spans))
	chunkStart := 0
	chunkEnd := 0
	chunkBytes := 0
	emit := func() {
		if chunkEnd <= chunkStart {
			return
		}
		chunks = append(chunks, backenddb.FlushSpanRunBackendChunk{ChunkIndex: len(chunks), PointOpStart: chunkStart, PointOpEnd: chunkEnd, ByteCount: chunkBytes})
		chunkStart = chunkEnd
		chunkBytes = 0
	}
	for i := range spans {
		span := spans[i]
		if span.PointOpStart != chunkEnd {
			return buildEntryCountFlushSpanRunChunks(ops, capEntries), false
		}
		spanOps := span.PointOpEnd - span.PointOpStart
		if spanOps <= 0 {
			continue
		}
		spanBytes := span.ByteCount
		if spanBytes <= 0 {
			for j := span.PointOpStart; j < span.PointOpEnd; j++ {
				spanBytes += flushRunEntryByteCount(ops[j])
			}
		}
		if spanOps > capEntries {
			emit()
			for start := span.PointOpStart; start < span.PointOpEnd; {
				end := start + capEntries
				if end > span.PointOpEnd {
					end = span.PointOpEnd
				}
				byteCount := 0
				for j := start; j < end; j++ {
					byteCount += flushRunEntryByteCount(ops[j])
				}
				chunks = append(chunks, backenddb.FlushSpanRunBackendChunk{ChunkIndex: len(chunks), PointOpStart: start, PointOpEnd: end, ByteCount: byteCount})
				start = end
			}
			chunkStart = span.PointOpEnd
			chunkEnd = span.PointOpEnd
			chunkBytes = 0
			continue
		}
		if chunkEnd > chunkStart && (chunkEnd-chunkStart)+spanOps > capEntries {
			emit()
		}
		if chunkEnd == chunkStart {
			chunkStart = span.PointOpStart
		}
		chunkEnd = span.PointOpEnd
		chunkBytes += spanBytes
	}
	if chunkEnd != len(ops) {
		return buildEntryCountFlushSpanRunChunks(ops, capEntries), false
	}
	emit()
	for i := range chunks {
		chunks[i].ChunkIndex = i
	}
	return chunks, true
}

type canonicalFlushRunSpanStats struct {
	targetLeafSpans int
	singleOpSpans   int
	spanOps         int
	spanBytes       int
	splitSummary    backenddb.FlushSpanRunChunkSplitSummary
}

func (db *DB) planCanonicalFlushRunMetadata(run *canonicalFlushRun, backendEntriesCap int) (backenddb.FlushSpanRunMetadata, []backenddb.FlushSpanRunBackendChunk, canonicalFlushRunSpanStats) {
	meta := backenddb.FlushSpanRunMetadata{
		SourceMemtables:  run.sourceMemtables,
		SourcePointOps:   run.sourcePointOps,
		PlannedPointOps:  run.plannedPointOps,
		ShadowedPointOps: run.shadowedPointOps,
		RangeBarriers:    run.rangeBarriers,
		LaneBarriers:     run.laneBarriers,
	}
	var spanStats canonicalFlushRunSpanStats
	if db == nil || run == nil || len(run.pointOps) == 0 {
		return meta, nil, spanStats
	}
	needTargetPlan := run.sourceMemtables > 1 || len(run.pointOps) > backendEntriesCap
	if needTargetPlan {
		req := backenddb.FlushSpanRunPlanRequest{
			SourceMemtables:  run.sourceMemtables,
			SourcePointOps:   run.sourcePointOps,
			PlannedPointOps:  run.plannedPointOps,
			ShadowedPointOps: run.shadowedPointOps,
			RangeBarriers:    run.rangeBarriers,
			LaneBarriers:     run.laneBarriers,
			PointOps:         run.pointOps,
		}
		if planner, ok := db.backend.(backendFlushSpanRunChunkPlanner); ok {
			planned, err := planner.PlanFlushSpanRunChunks(req, backendEntriesCap)
			if err == nil {
				meta = planned.Metadata
				chunks := planned.BackendChunks
				if len(chunks) == 0 {
					chunks = buildEntryCountFlushSpanRunChunks(run.pointOps, backendEntriesCap)
					meta.BackendChunks = chunks
				}
				spanStats = canonicalFlushRunSpanStats{
					targetLeafSpans: planned.TargetLeafSpans,
					singleOpSpans:   planned.SingleOpSpans,
					spanOps:         planned.SpanOps,
					spanBytes:       planned.SpanBytes,
					splitSummary:    planned.SplitSummary,
				}
				return meta, chunks, spanStats
			}
		} else if planner, ok := db.backend.(backendFlushSpanRunPlanner); ok {
			planned, err := planner.PlanFlushSpanRun(req)
			if err == nil {
				meta = planned
			}
		}
	}
	chunks, _ := buildLeafAwareFlushSpanRunChunks(run.pointOps, meta.TargetLeafSpans, backendEntriesCap)
	meta.BackendChunks = chunks
	if err := backenddb.ValidateFlushSpanRunMetadata(meta); err != nil {
		chunks = buildEntryCountFlushSpanRunChunks(run.pointOps, backendEntriesCap)
		meta.BackendChunks = chunks
	}
	splitSummary := backenddb.SummarizeFlushSpanRunChunkSplits(meta.TargetLeafSpans, chunks)
	spanStats = canonicalFlushRunSpanStats{splitSummary: splitSummary}
	for i := range meta.TargetLeafSpans {
		span := meta.TargetLeafSpans[i]
		spanStats.targetLeafSpans++
		spanStats.spanOps += span.OpCount
		spanStats.spanBytes += span.ByteCount
		if span.OpCount == 1 {
			spanStats.singleOpSpans++
		}
	}
	return meta, chunks, spanStats
}

func appendCanonicalFlushRunChunkToBackendBatch(backendBatch batch.Interface, ops []batch.Entry) error {
	if backendBatch == nil {
		return errors.New("cachingdb: missing backend batch")
	}
	sv, _ := backendBatch.(backendBatchSetViewer)
	dv, _ := backendBatch.(backendBatchDeleteViewer)
	psv, _ := backendBatch.(backendBatchPointerSetterView)
	ps, _ := backendBatch.(backendBatchPointerSetter)
	var single [1]batch.Entry
	for i := range ops {
		op := ops[i]
		switch {
		case op.Type == batch.OpDelete:
			if dv != nil {
				if err := dv.DeleteView(op.Key); err != nil {
					return err
				}
			} else if err := backendBatch.Delete(op.Key); err != nil {
				return err
			}
		case op.IsPtr:
			if psv != nil {
				if err := psv.SetPointerView(op.Key, op.ValuePtr); err != nil {
					return err
				}
			} else if ps != nil {
				if err := ps.SetPointer(op.Key, op.ValuePtr); err != nil {
					return err
				}
			} else {
				single[0] = op
				if err := backendBatch.SetOps(single[:]); err != nil {
					return err
				}
			}
		default:
			if sv != nil {
				if err := sv.SetView(op.Key, op.Value); err != nil {
					return err
				}
			} else if err := backendBatch.Set(op.Key, op.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) writeCanonicalFlushRunChunks(run *canonicalFlushRun, chunks []backenddb.FlushSpanRunBackendChunk, syncFlush bool, commandPublish *checkpointCommandWALPublish) (int, error) {
	if db == nil || run == nil || len(run.pointOps) == 0 {
		return 0, nil
	}
	if len(chunks) == 0 {
		chunks = buildEntryCountFlushSpanRunChunks(run.pointOps, len(run.pointOps))
	}
	emitted := 0
	for i := range chunks {
		chunk := chunks[i]
		if chunk.PointOpEnd <= chunk.PointOpStart {
			continue
		}
		if chunk.PointOpStart < 0 || chunk.PointOpEnd > len(run.pointOps) {
			return emitted, fmt.Errorf("cachingdb: flush chunk [%d,%d) out of bounds %d", chunk.PointOpStart, chunk.PointOpEnd, len(run.pointOps))
		}
		opCount := chunk.PointOpEnd - chunk.PointOpStart
		backendBatch := db.newBackendBatchWithSize(opCount)
		reserveBackendBatchOps(backendBatch, opCount)
		if err := appendCanonicalFlushRunChunkToBackendBatch(backendBatch, run.pointOps[chunk.PointOpStart:chunk.PointOpEnd]); err != nil {
			_ = backendBatch.Close()
			return emitted, err
		}
		last := true
		for j := i + 1; j < len(chunks); j++ {
			if chunks[j].PointOpEnd > chunks[j].PointOpStart {
				last = false
				break
			}
		}
		commandPublishAttached := false
		if last {
			attached, attachErr := commandPublish.attach(backendBatch)
			if attachErr != nil {
				_ = backendBatch.Close()
				return emitted, attachErr
			}
			commandPublishAttached = attached
		}
		db.backendWriteBatchesTotal.Add(1)
		db.observeFlushSpanRunBackendChunks(1)
		var err error
		if last && syncFlush {
			err = backendBatch.WriteSync()
		} else {
			err = backendBatch.Write()
		}
		cerr := backendBatch.Close()
		if err == nil {
			err = cerr
		}
		if err != nil {
			return emitted, err
		}
		if commandPublishAttached {
			commandPublish.consumed = true
			db.commandWALCheckpointPublishPiggybacked.Add(1)
		}
		emitted++
	}
	return emitted, nil
}

func (db *DB) finishFlushedCanonicalUnits(syncFlush bool, units []flushUnit, ids []uint64, totalBytes int64) {
	db.mu.Lock()
	removeIDs := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		removeIDs[id] = struct{}{}
	}
	db.removeQueuedUnitsLocked(removeIDs, units, totalBytes)

	deletable := make([]string, 0, len(units))
	if syncFlush {
		inUse := make(map[string]struct{})
		for _, path := range db.currentWALPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueWALPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
		seen := make(map[string]struct{})
		for _, unit := range units {
			for _, walPath := range unit.walPaths {
				if walPath == "" {
					continue
				}
				if _, ok := inUse[walPath]; ok {
					continue
				}
				if _, ok := seen[walPath]; ok {
					continue
				}
				if db.valueLogRetained(walPath) {
					continue
				}
				seen[walPath] = struct{}{}
				deletable = append(deletable, walPath)
			}
		}
	}
	db.mu.Unlock()
	removed := false
	for _, walPath := range deletable {
		db.dropValueLogSegment(walPath)
		if err := db.removeFileRetry(walPath); err != nil {
			continue
		}
		removed = true
		db.mu.Lock()
		db.untrackWALSegmentLocked(walPath)
		db.mu.Unlock()
		db.forgetValueLogRetain(walPath)
	}
	if removed {
		db.syncDirBestEffort(db.dir)
	}
	db.checkValueLogRetention()
}

func (db *DB) flushCanonicalPointUnits(syncFlush bool, laneID int, commandPublish *checkpointCommandWALPublish, units []flushUnit, ids []uint64, totalBytes int64, totalLen int, totalSpans int) bool {
	flushStart := time.Now()
	buildStart := flushStart
	run, err := db.buildCanonicalFlushRun(units, totalLen, totalSpans)
	if err != nil {
		db.reportError(err)
		return false
	}
	defer run.release()
	db.observeFlushApplyBuild(time.Since(buildStart))

	if run.shadowedPointOps > 0 {
		flushMergeShadowedOpsTotal.Add(uint64(run.shadowedPointOps))
		flushMergeParallelShadowedOpsTotal.Add(uint64(run.shadowedPointOps))
		db.observeFlushSpanRunShadowedOps(run.shadowedPointOps)
	}
	if run.plannedPointOps > 0 {
		flushMergeAppliedOpsTotal.Add(uint64(run.plannedPointOps))
		flushMergeParallelAppliedOpsTotal.Add(uint64(run.plannedPointOps))
	}
	if run.sourcePointOps != run.plannedPointOps+run.shadowedPointOps {
		db.reportError(fmt.Errorf("cachingdb: canonical flush run source=%d planned=%d shadowed=%d", run.sourcePointOps, run.plannedPointOps, run.shadowedPointOps))
		return false
	}

	if err := db.materializeCanonicalRunDeferredValueLogPointers(run, syncFlush, laneID); err != nil {
		db.reportError(fmt.Errorf("cachingdb: flush failed (defer vlog): %w", err))
		return false
	}

	backendEntriesCap := db.flushBackendEntriesCapForOps(run.plannedPointOps, run.deletePointOps, syncFlush)
	_, chunks, spanStats := db.planCanonicalFlushRunMetadata(run, backendEntriesCap)
	db.observeFlushSpanRunTargetLeafSpanSummary(spanStats.targetLeafSpans, spanStats.singleOpSpans, spanStats.spanOps, spanStats.spanBytes, spanStats.splitSummary)
	db.observeFlushSpanRunPlannedOps(run.plannedPointOps, totalSpans)

	if db.valueLogEnabled() {
		if err := db.flushValueLog(laneID); err != nil {
			db.reportError(fmt.Errorf("cachingdb: flush failed (vlog): %w", err))
			return false
		}
		if syncFlush && !db.relaxedSync {
			if err := db.syncValueLog(laneID); err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
				return false
			}
		}
	}

	writeStart := time.Now()
	if _, err := db.writeCanonicalFlushRunChunks(run, chunks, syncFlush, commandPublish); err != nil {
		db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
		return false
	}
	db.observeFlushApplyBackendWrite(time.Since(writeStart))

	db.finishFlushedCanonicalUnits(syncFlush, units, ids, totalBytes)
	flushDur := time.Since(flushStart)
	if flushDur > 0 && totalBytes > 0 {
		sample := float64(totalBytes) / flushDur.Seconds()
		db.bpMu.Lock()
		if db.flushBpsEWMA <= 0 {
			db.flushBpsEWMA = sample
		} else {
			db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
		}
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	}
	return true
}
