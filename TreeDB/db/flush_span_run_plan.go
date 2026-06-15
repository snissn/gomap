package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// PlanFlushSpanRun plans exact target-leaf spans for an already-canonical flush
// run. It is side-effect-free: it captures the current root, runs the read-only
// prepare pass against the supplied point/range operations, and returns M8/M9
// span-run metadata for cache-layer chunk planning and future span-native jobs.
func (db *DB) PlanFlushSpanRun(req FlushSpanRunPlanRequest) (FlushSpanRunMetadata, error) {
	meta, prepared, err := db.prepareFlushSpanRun(req, false, nil)
	if err != nil {
		return meta, err
	}
	meta.TargetLeafSpans = appendFlushSpanRunTargetLeafSpans(nil, prepared.LeafSpans)
	if err := ValidateFlushSpanRunMetadata(meta); err != nil {
		return meta, err
	}
	return meta, nil
}

// PlanFlushSpanRunChunks plans target-leaf-aware backend chunks for an
// already-canonical flush run without copying every read-only span into the M8
// exported span struct. This is the cache flush hot-path form: it still runs the
// same side-effect-free read-only prepare pass and emits aggregate target-span
// counters plus split evidence.
func (db *DB) PlanFlushSpanRunChunks(req FlushSpanRunPlanRequest, maxPointOpsPerChunk int) (FlushSpanRunChunkPlan, error) {
	prepareBuf := db.acquireFlushApplyReadOnlyPrepareBuffer(zipper.ApplyOptions{PrepareReadOnly: true})
	meta, prepared, err := db.prepareFlushSpanRun(req, true, prepareBuf)
	defer db.releaseFlushApplyReadOnlyPreparePlanBuffer(prepareBuf, &prepared)
	out := FlushSpanRunChunkPlan{Metadata: meta}
	if err != nil {
		return out, err
	}
	summary := prepared.LeafSpanSummary()
	out.TargetLeafSpans = summary.Spans
	out.SingleOpSpans = summary.SingleOpSpans
	out.SpanOps = summary.SpanOps
	out.SpanBytes = summary.SpanBytes
	chunks, _ := buildReadOnlyLeafAwareFlushSpanRunChunks(req.PointOps, prepared.LeafSpans, maxPointOpsPerChunk)
	out.BackendChunks = chunks
	out.Metadata.BackendChunks = chunks
	out.SplitSummary = summarizeReadOnlyFlushSpanRunChunkSplits(prepared.LeafSpans, chunks)
	return out, nil
}

func (db *DB) prepareFlushSpanRun(req FlushSpanRunPlanRequest, omitKeys bool, prepareBuf *flushApplyReadOnlyPrepareBuffer) (FlushSpanRunMetadata, zipper.ReadOnlyPrepareResult, error) {
	meta := FlushSpanRunMetadata{
		RunID:            req.RunID,
		SourceMemtables:  req.SourceMemtables,
		SourcePointOps:   req.SourcePointOps,
		PlannedPointOps:  req.PlannedPointOps,
		ShadowedPointOps: req.ShadowedPointOps,
		RangeBarriers:    req.RangeBarriers,
		LaneBarriers:     req.LaneBarriers,
	}
	var prepared zipper.ReadOnlyPrepareResult
	if db == nil {
		return meta, prepared, ErrClosed
	}
	if req.PlannedPointOps != len(req.PointOps) {
		return meta, prepared, fmt.Errorf("treedb: flush span run planned point ops=%d but point slice has %d", req.PlannedPointOps, len(req.PointOps))
	}
	if req.SourcePointOps != req.PlannedPointOps+req.ShadowedPointOps {
		return meta, prepared, fmt.Errorf("treedb: flush span run source point ops=%d must equal planned=%d plus shadowed=%d", req.SourcePointOps, req.PlannedPointOps, req.ShadowedPointOps)
	}

	planOpts := ReadOnlyApplyPlanOptions{Workers: db.flushApplyConcurrency}
	if prepareBuf != nil {
		planOpts.Zipper = prepareBuf.opts
	}
	planOpts.Zipper.OmitKeys = omitKeys

	var plan ReadOnlyApplyPlan
	var err error
	if len(req.DeleteRanges) == 0 {
		plan, err = db.prepareReadOnlyApplyPlanFromOps(req.PointOps, nil, planOpts)
	} else {
		reserve := len(req.PointOps) + len(req.DeleteRanges)
		bif := db.newBatchWithReserveHint(reserve)
		b, ok := bif.(*Batch)
		if !ok || b == nil || b.batch == nil {
			if bif != nil {
				_ = bif.Close()
			}
			return meta, prepared, fmt.Errorf("treedb: flush span run planner could not create backend batch")
		}
		defer func() { _ = b.Close() }()

		if len(req.PointOps) > 0 {
			if err := b.batch.SetOps(req.PointOps); err != nil {
				return meta, prepared, err
			}
		}
		for i := range req.DeleteRanges {
			r := req.DeleteRanges[i]
			if err := b.batch.DeleteRange(r.Start, r.End); err != nil {
				return meta, prepared, err
			}
		}
		plan, err = db.PrepareReadOnlyApplyPlan(b, planOpts)
	}
	prepared = plan.Prepare
	if prepared.RootID != 0 {
		meta.BaseRoot.CapturedRootID = prepared.RootID
		meta.BaseRoot.CurrentRootID = prepared.RootID
		meta.BaseRoot.Matched = true
	}
	if err != nil {
		return meta, prepared, err
	}
	return meta, prepared, nil
}

func appendFlushSpanRunTargetLeafSpans(dst []FlushSpanRunTargetLeafSpan, spans []zipper.ReadOnlyLeafSpan) []FlushSpanRunTargetLeafSpan {
	if len(spans) == 0 {
		return dst
	}
	for i := range spans {
		span := spans[i]
		dst = append(dst, FlushSpanRunTargetLeafSpan{
			SpanIndex:        i,
			Ref:              span.Ref,
			LowKey:           cloneFlushSpanRunKey(span.LowKey),
			HighKey:          cloneFlushSpanRunKey(span.HighKey),
			FirstOpKey:       cloneFlushSpanRunKey(span.FirstOpKey),
			LastOpKey:        cloneFlushSpanRunKey(span.LastOpKey),
			PointOpStart:     span.PointOpStart,
			PointOpEnd:       span.PointOpEnd,
			DeleteRangeStart: span.DeleteRangeStart,
			DeleteRangeEnd:   span.DeleteRangeEnd,
			OpCount:          span.OpCount,
			ByteCount:        span.ByteCount,
		})
	}
	return dst
}

func cloneFlushSpanRunKey(key []byte) []byte {
	// ReadOnlyPrepareResult already owns stable key-arena bytes for span bounds
	// and first/last op keys. Returning those views keeps the arena live through
	// FlushSpanRunMetadata without adding a second per-span key copy on the flush
	// hot path.
	return key
}

func flushSpanRunEntryByteCount(op batch.Entry) int {
	n := len(op.Key) + len(op.Value)
	if op.IsPtr {
		n += 16
	}
	return n
}

func buildReadOnlyEntryCountFlushSpanRunChunks(ops []batch.Entry, capEntries int) []FlushSpanRunBackendChunk {
	if len(ops) == 0 {
		return nil
	}
	if capEntries <= 0 {
		capEntries = len(ops)
	}
	chunks := make([]FlushSpanRunBackendChunk, 0, (len(ops)+capEntries-1)/capEntries)
	for start := 0; start < len(ops); {
		end := start + capEntries
		if end > len(ops) {
			end = len(ops)
		}
		byteCount := 0
		for i := start; i < end; i++ {
			byteCount += flushSpanRunEntryByteCount(ops[i])
		}
		chunks = append(chunks, FlushSpanRunBackendChunk{ChunkIndex: len(chunks), PointOpStart: start, PointOpEnd: end, ByteCount: byteCount})
		start = end
	}
	return chunks
}

func buildReadOnlyLeafAwareFlushSpanRunChunks(ops []batch.Entry, spans []zipper.ReadOnlyLeafSpan, capEntries int) ([]FlushSpanRunBackendChunk, bool) {
	if len(ops) == 0 {
		return nil, true
	}
	if capEntries <= 0 {
		capEntries = len(ops)
	}
	if len(spans) == 0 {
		return buildReadOnlyEntryCountFlushSpanRunChunks(ops, capEntries), false
	}
	if spans[0].PointOpStart != 0 || spans[len(spans)-1].PointOpEnd != len(ops) {
		return buildReadOnlyEntryCountFlushSpanRunChunks(ops, capEntries), false
	}
	chunks := make([]FlushSpanRunBackendChunk, 0, len(spans))
	chunkStart := 0
	chunkEnd := 0
	chunkBytes := 0
	emit := func() {
		if chunkEnd <= chunkStart {
			return
		}
		chunks = append(chunks, FlushSpanRunBackendChunk{ChunkIndex: len(chunks), PointOpStart: chunkStart, PointOpEnd: chunkEnd, ByteCount: chunkBytes})
		chunkStart = chunkEnd
		chunkBytes = 0
	}
	for i := range spans {
		span := spans[i]
		if span.PointOpStart != chunkEnd {
			return buildReadOnlyEntryCountFlushSpanRunChunks(ops, capEntries), false
		}
		spanOps := span.PointOpEnd - span.PointOpStart
		if spanOps <= 0 {
			continue
		}
		spanBytes := span.ByteCount
		if spanBytes <= 0 {
			for j := span.PointOpStart; j < span.PointOpEnd; j++ {
				spanBytes += flushSpanRunEntryByteCount(ops[j])
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
					byteCount += flushSpanRunEntryByteCount(ops[j])
				}
				chunks = append(chunks, FlushSpanRunBackendChunk{ChunkIndex: len(chunks), PointOpStart: start, PointOpEnd: end, ByteCount: byteCount})
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
		return buildReadOnlyEntryCountFlushSpanRunChunks(ops, capEntries), false
	}
	emit()
	for i := range chunks {
		chunks[i].ChunkIndex = i
	}
	return chunks, true
}

func summarizeReadOnlyFlushSpanRunChunkSplits(spans []zipper.ReadOnlyLeafSpan, chunks []FlushSpanRunBackendChunk) FlushSpanRunChunkSplitSummary {
	summary := FlushSpanRunChunkSplitSummary{BackendChunks: len(chunks), TargetLeafSpans: len(spans)}
	for i := range spans {
		span := spans[i]
		if span.PointOpEnd <= span.PointOpStart {
			continue
		}
		overlaps := 0
		for j := range chunks {
			chunk := chunks[j]
			if chunk.PointOpEnd <= chunk.PointOpStart {
				continue
			}
			if chunk.PointOpStart < span.PointOpEnd && chunk.PointOpEnd > span.PointOpStart {
				overlaps++
			}
		}
		if overlaps > summary.MaxChunksPerTargetLeaf {
			summary.MaxChunksPerTargetLeaf = overlaps
		}
		if overlaps > 1 {
			summary.TargetLeavesSplitAcrossChunks++
		}
	}
	return summary
}
