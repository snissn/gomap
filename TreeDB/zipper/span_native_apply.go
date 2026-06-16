package zipper

import (
	"bytes"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func (z *Zipper) applySpanNativeWithPrepared(rootID uint64, ops []batch.Entry, prepared ReadOnlyPrepareResult, workers int, workerPool *ApplyWorkerPool) (ApplyResult, bool, error) {
	if !validateSpanNativePreparedPlan(ops, prepared) {
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

	workerRanges := prepared.AppendLeafSpanWorkerRanges(nil, workers)
	if len(workerRanges) == 0 {
		workerRanges = append(workerRanges, ReadOnlyLeafSpanWorkerRange{FirstSpan: 0, SpanCount: spanCount, Ops: len(ops)})
	}
	workers = len(workerRanges)
	outputs := coordinatorScratch.acquireSpanNativeLeafOutputs(spanCount, z.outerLeavesInValueLog)
	defer outputs.release()
	workerScratches := make([]*mergeScratch, len(workerRanges))
	defer func() {
		for i := range workerScratches {
			z.releaseApplyScratch(workerScratches[i])
			workerScratches[i] = nil
		}
	}()
	rangeMetrics := make([]adaptive.Metrics, len(workerRanges))
	rangeRetired := make([][]uint64, len(workerRanges))
	rangeSplits := make([]spanNativeLeafSplitRange, len(workerRanges))
	var firstErr error
	var errOnce sync.Once
	runRange := func(_ int, job int) {
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
		for i := workerRange.FirstSpan; i < end; i++ {
			span := prepared.LeafSpans[i]
			newRef, splits, err := z.writeRecursive(span.Ref, ops[span.PointOpStart:span.PointOpEnd], nil, false, nil, &localMetrics, span.LowKey, span.HighKey, &localRetired, workerScratch, false, applyRunConfig{maxParallelWorkers: 1})
			if err != nil {
				releaseLocalSplits()
				errOnce.Do(func() { firstErr = err })
				return
			}
			if err := outputs.setRef(i, newRef); err != nil {
				releaseLocalSplits()
				errOnce.Do(func() { firstErr = err })
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
	if err := workerPool.Run(workers, len(workerRanges), runRange); err != nil {
		metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
		return ApplyResult{Metrics: metrics}, true, err
	}

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

	rootReduceStart := time.Now()
	newRootID, err := z.reduceSpanNativeRootWithContext(rootID, spanNativeLeafReplacements{spans: prepared.LeafSpans, outputs: &outputs}, &metrics, &retired, coordinatorScratch)
	metrics.ZipperRootReduceNs += time.Since(rootReduceStart).Nanoseconds()
	metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
	if err != nil {
		return ApplyResult{Metrics: metrics, PendingRetiredPages: retired}, true, err
	}
	return ApplyResult{
		RootID:              newRootID,
		PendingRetiredPages: retired,
		Metrics:             metrics,
		SpanNativeEligible:  true,
		SpanNativeWorkers:   workers,
		SpanNativeUsed:      true,
	}, true, nil
}

func validateSpanNativePreparedPlan(ops []batch.Entry, prepared ReadOnlyPrepareResult) bool {
	if prepared.DeleteRanges != 0 || prepared.ColdBuild || prepared.Maintenance || !prepared.ExactLeafSpans || len(prepared.LeafSpans) == 0 {
		return false
	}
	spans := prepared.LeafSpans
	expectedPointStart := 0
	var prevHigh []byte
	for i := range spans {
		span := spans[i]
		if span.DeleteRangeStart != span.DeleteRangeEnd || span.PointOpStart != expectedPointStart || span.PointOpEnd > len(ops) || span.PointOpEnd <= span.PointOpStart {
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
		var refs []Split
		for i := 0; i < replacements.len(); i++ {
			out := replacements.output(i)
			refs = append(refs, spanNativeReplacementHeadSplit(replacements.spans[i], out))
			refs = append(refs, out.splits...)
		}
		return z.reduceSpanNativeRoot(refs, metrics)
	}
	ref, splits, changed, err := z.stitchSpanNativeRecursive(page.PageChildRef(rootID), nil, nil, replacements, metrics, retired, scratch)
	if err != nil {
		return 0, err
	}
	if !changed {
		return 0, page.ErrInvalidPageType
	}
	refs := make([]Split, 0, len(splits)+1)
	refs = append(refs, Split{Key: []byte{}, Ref: ref})
	refs = append(refs, splits...)
	return z.reduceSpanNativeRoot(refs, metrics)
}

func spanNativeReplacementHeadSplit(span ReadOnlyLeafSpan, output spanNativeLeafOutput) Split {
	spanKey := span.LowKey
	if spanKey == nil {
		spanKey = []byte{}
	}
	return Split{Key: spanKey, Ref: output.ref}
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
		return page.ErrInvalidPageType
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
		writer := spanNativeSplitLevelWriter{z: z, metrics: metrics}
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
			}
			childLow := low
			if len(key) != 0 {
				childLow = key
				if copyKeys {
					childLow = append([]byte(nil), key...)
				}
			}
			childHigh := high
			if i+1 < count {
				nextKey, _, err := oldNode.GetInternalEntryRefView(i + 1)
				if err != nil {
					return page.ChildRef{}, nil, false, err
				}
				if nextKey == nil {
					nextKey = []byte{}
				}
				childHigh = nextKey
				if copyKeys {
					childHigh = append([]byte(nil), nextKey...)
				}
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
				if err := writer.append(Split{Key: key, Ref: out.ref}); err != nil {
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
				return page.ChildRef{}, nil, false, page.ErrInvalidPageType
			}
			changed = true
			if err := writer.append(Split{Key: key, Ref: newRef}); err != nil {
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
			return page.ChildRef{}, nil, false, page.ErrInvalidPageType
		}
		return refs[0].Ref, refs[1:], true, nil
	default:
		return page.ChildRef{}, nil, false, page.ErrInvalidPageType
	}
}

type spanNativeSplitLevelWriter struct {
	z               *Zipper
	metrics         *adaptive.Metrics
	nextLevelNodes  []Split
	currentBuilder  *node.Builder
	currentStartKey []byte
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
		w.currentBuilder.SetInternalFenceBounds(w.currentStartKey, nil)
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
		w.currentBuilder.SetInternalFenceBounds(w.currentStartKey, nil)

		if addErr := w.currentBuilder.AddInternalChildRef(childKey, child.Ref); addErr != nil {
			return addErr
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
	w.nextLevelNodes = append(w.nextLevelNodes, Split{Key: w.currentStartKey, Ref: page.PageChildRef(pageID)})
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
	if err := validateSpanNativeReducerRefs(currentLevelNodes); err != nil {
		return 0, err
	}
	for {
		if len(currentLevelNodes) == 1 {
			return z.ensureRootPage(currentLevelNodes[0].Key, currentLevelNodes[0].Ref, metrics)
		}
		metrics.ZipperRootSplitLevels++
		nextLevelNodes, err := z.reduceSpanNativeSplitLevel(currentLevelNodes, metrics)
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

func (z *Zipper) reduceSpanNativeSplitLevel(currentLevelNodes []Split, metrics *adaptive.Metrics) ([]Split, error) {
	var nextLevelNodes []Split
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
			currentBuilder.SetInternalFenceBounds(currentStartKey, nil)
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
			nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, Ref: page.PageChildRef(currentBuilder.PageID())})

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
			currentBuilder.SetInternalFenceBounds(currentStartKey, nil)

			if addErr := currentBuilder.AddInternalChildRef(childKey, child.Ref); addErr != nil {
				return nil, addErr
			}
			recordZipperInternalChildRef(metrics, child.Ref)
		} else if err != nil {
			return nil, err
		}

		if i == len(currentLevelNodes)-1 {
			currentBuilder.FinishNoNode()
			recordZipperInternalPageWrite(metrics)
			nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, Ref: page.PageChildRef(currentBuilder.PageID())})
			currentBuilder = nil
		}
	}
	return nextLevelNodes, nil
}
