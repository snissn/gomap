package zipper

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestSpanNativeApplySingleLeafParity(t *testing.T) {
	_, serial := newReadOnlyPrepareZipper(t)
	_, native := newReadOnlyPrepareZipper(t)
	serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 40)
	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 40)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for i := 10; i < 20; i++ {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("native-%06d", i))); err != nil {
			t.Fatalf("Set update: %v", err)
		}
	}

	assertSpanNativeParity(t, serial, native, serialRoot, nativeRoot, delta)
}

func TestSpanNativeApplySingleLeafSplitReducerParity(t *testing.T) {
	_, serial := newReadOnlyPrepareZipper(t)
	_, native := newReadOnlyPrepareZipper(t)
	serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 1)
	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 1)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	value := bytes.Repeat([]byte("v"), 200)
	for i := 1; i < 140; i++ {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", i)), value); err != nil {
			t.Fatalf("Set update: %v", err)
		}
	}

	result := assertSpanNativeParity(t, serial, native, serialRoot, nativeRoot, delta)
	if result.Metrics.Splits == 0 {
		t.Fatalf("span-native split reducer test did not produce splits; metrics=%+v", result.Metrics)
	}
	if result.Metrics.ZipperRootSplitLevels == 0 {
		t.Fatalf("root split levels=%d want >0", result.Metrics.ZipperRootSplitLevels)
	}
}

func TestSpanNativeApplyPartialMultiLeafParentStitchParity(t *testing.T) {
	_, serial := newReadOnlyPrepareZipper(t)
	_, native := newReadOnlyPrepareZipper(t)
	serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 1000)
	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 1000)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for i := 200; i < 700; i++ {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("partial-%06d", i))); err != nil {
			t.Fatalf("Set update: %v", err)
		}
	}

	serialNewRoot, _, _, err := serial.Apply(serialRoot, delta)
	if err != nil {
		t.Fatalf("serial Apply: %v", err)
	}
	result, err := native.ApplyWithOptions(nativeRoot, delta, ApplyOptions{SpanNativeApply: true, ParallelApplyConcurrency: 2})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	if !result.SpanNativeEligible || !result.SpanNativeUsed {
		t.Fatalf("span-native flags eligible/used=%v/%v want parent-stitch path", result.SpanNativeEligible, result.SpanNativeUsed)
	}
	if !bytes.Equal(collectRootLeafPairs(t, serial, serialNewRoot), collectRootLeafPairs(t, native, result.RootID)) {
		t.Fatalf("partial fallback output mismatch")
	}
}

func TestSpanNativeApplyOmitKeysOptionKeepsBoundariesForPartialSpan(t *testing.T) {
	_, serial := newReadOnlyPrepareZipper(t)
	_, native := newReadOnlyPrepareZipper(t)
	serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 4096)
	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 4096)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for i := 1800; i < 1820; i++ {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("omit-%06d", i))); err != nil {
			t.Fatalf("Set omit-keys update %d: %v", i, err)
		}
	}

	serialNewRoot, _, _, err := serial.Apply(serialRoot, delta)
	if err != nil {
		t.Fatalf("serial Apply: %v", err)
	}
	result, err := native.ApplyWithOptions(nativeRoot, delta, ApplyOptions{
		SpanNativeApply: true,
		ReadOnlyPrepare: ReadOnlyPrepareOptions{OmitKeys: true},
	})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	if result.ReadOnlyPrepare.OmitKeys {
		t.Fatalf("span-native prepare kept OmitKeys=true; execution needs exact boundaries")
	}
	if !result.ReadOnlyPrepare.OmitOpKeys {
		t.Fatalf("span-native prepare OmitOpKeys=%v want true", result.ReadOnlyPrepare.OmitOpKeys)
	}
	var boundaryBytes int
	for i, span := range result.ReadOnlyPrepare.LeafSpans {
		boundaryBytes += len(span.LowKey) + len(span.HighKey)
		if span.FirstOpKey != nil || span.LastOpKey != nil {
			t.Fatalf("span-native span %d retained op keys under OmitOpKeys", i)
		}
	}
	if boundaryBytes == 0 {
		t.Fatalf("span-native prepare retained no boundary bytes")
	}
	if !result.SpanNativeEligible || !result.SpanNativeUsed {
		t.Fatalf("span-native flags eligible/used=%v/%v", result.SpanNativeEligible, result.SpanNativeUsed)
	}
	if !bytes.Equal(collectRootLeafPairs(t, serial, serialNewRoot), collectRootLeafPairs(t, native, result.RootID)) {
		t.Fatalf("span-native omit-keys partial output mismatch")
	}
}

func TestSpanNativeApplyInternalBaseDeltaPartialStitchCopiesSeparatorKeys(t *testing.T) {
	_, serial := newReadOnlyPrepareZipper(t)
	_, native := newReadOnlyPrepareZipper(t)
	serial.SetIndexInternalBaseDelta(true)
	native.SetIndexInternalBaseDelta(true)
	serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 4096)
	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 4096)
	if got := collectSpanNativeTestInternalPageIDs(t, native, nativeRoot); len(got) == 0 {
		t.Fatalf("native root has no base-delta internal pages; test requires partial internal stitching")
	}

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for i := 900; i < 3200; i++ {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("bd-%06d", i))); err != nil {
			t.Fatalf("Set base-delta update %d: %v", i, err)
		}
	}

	serialNewRoot, _, _, err := serial.Apply(serialRoot, delta)
	if err != nil {
		t.Fatalf("serial Apply: %v", err)
	}
	result, err := native.ApplyWithOptions(nativeRoot, delta, ApplyOptions{SpanNativeApply: true, ParallelApplyConcurrency: 2})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	if !result.SpanNativeEligible || !result.SpanNativeUsed {
		t.Fatalf("span-native flags eligible/used=%v/%v", result.SpanNativeEligible, result.SpanNativeUsed)
	}
	if !bytes.Equal(collectRootLeafPairs(t, serial, serialNewRoot), collectRootLeafPairs(t, native, result.RootID)) {
		t.Fatalf("base-delta partial stitch output mismatch")
	}
}

func TestSpanNativeApplySparseMultiLeafParentStitchParity(t *testing.T) {
	_, serial := newReadOnlyPrepareZipper(t)
	_, native := newReadOnlyPrepareZipper(t)
	serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 4096)
	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 4096)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for _, i := range []int{17, 511, 1029, 2053, 3079, 4095} {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("sparse-%06d", i))); err != nil {
			t.Fatalf("Set sparse %d: %v", i, err)
		}
	}

	prepared, err := native.PrepareReadOnly(nativeRoot, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	if len(prepared.LeafSpans) < 2 {
		t.Fatalf("prepared spans=%d want sparse multi-leaf", len(prepared.LeafSpans))
	}
	sawGap := false
	for i := 1; i < len(prepared.LeafSpans); i++ {
		if !bytes.Equal(prepared.LeafSpans[i].LowKey, prepared.LeafSpans[i-1].HighKey) {
			sawGap = true
			break
		}
	}
	if !sawGap {
		t.Fatalf("prepared spans were contiguous; want sparse parent-stitch coverage")
	}
	if !validateSpanNativePreparedPlan(delta.SortedEntries(), prepared, ApplyOptions{}) {
		t.Fatalf("sparse prepared plan rejected by span-native validator")
	}

	result := assertSpanNativeParity(t, serial, native, serialRoot, nativeRoot, delta)
	if result.Metrics.ZipperInternalPagesWritten == 0 {
		t.Fatalf("internal pages written=%d want sparse parent stitch", result.Metrics.ZipperInternalPagesWritten)
	}
}

func TestSpanNativeApplyConcurrentWorkerLocalScratchSplitParity(t *testing.T) {
	_, serial := newReadOnlyPrepareZipper(t)
	_, native := newReadOnlyPrepareZipper(t)
	serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 4096)
	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 4096)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	value := bytes.Repeat([]byte("v"), 180)
	for _, anchor := range []int{17, 1029, 2053, 3079} {
		for j := 0; j < 48; j++ {
			key := []byte(fmt.Sprintf("key-%06d-%03d", anchor, j))
			if err := delta.Set(key, value); err != nil {
				t.Fatalf("Set anchor=%d j=%d: %v", anchor, j, err)
			}
		}
	}

	prepared, err := native.PrepareReadOnly(nativeRoot, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	if len(prepared.LeafSpans) < 4 {
		t.Fatalf("prepared spans=%d want at least 4 worker ranges", len(prepared.LeafSpans))
	}
	pool := NewApplyWorkerPool(4)
	defer pool.Close()

	serialNewRoot, _, _, err := serial.Apply(serialRoot, delta)
	if err != nil {
		t.Fatalf("serial Apply: %v", err)
	}
	result, err := native.ApplyWithOptions(nativeRoot, delta, ApplyOptions{
		SpanNativeApply:          true,
		ParallelApplyConcurrency: 4,
		ParallelApplyWorkerPool:  pool,
	})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	if !result.SpanNativeEligible || !result.SpanNativeUsed || result.SpanNativeWorkers < 2 {
		t.Fatalf("span-native flags eligible/used/workers=%v/%v/%d", result.SpanNativeEligible, result.SpanNativeUsed, result.SpanNativeWorkers)
	}
	if result.Metrics.Splits == 0 {
		t.Fatalf("splits=%d want split outputs from worker-local scratch", result.Metrics.Splits)
	}
	if result.Metrics.ZipperSpanNativeScheduledWorkersMax < 2 {
		t.Fatalf("span-native scheduled workers max=%d, want >=2", result.Metrics.ZipperSpanNativeScheduledWorkersMax)
	}
	if result.Metrics.ZipperSpanNativeReadyTasks == 0 || result.Metrics.ZipperSpanNativeDispatchedTasks != result.Metrics.ZipperSpanNativeReadyTasks || result.Metrics.ZipperSpanNativeCompletedTasks != result.Metrics.ZipperSpanNativeDispatchedTasks {
		t.Fatalf("span-native scheduler ready/dispatched/completed=%d/%d/%d", result.Metrics.ZipperSpanNativeReadyTasks, result.Metrics.ZipperSpanNativeDispatchedTasks, result.Metrics.ZipperSpanNativeCompletedTasks)
	}
	if !bytes.Equal(collectRootLeafPairs(t, serial, serialNewRoot), collectRootLeafPairs(t, native, result.RootID)) {
		t.Fatalf("concurrent span-native output mismatch")
	}
}

type laneRecordingLeafPageLog struct {
	mu                     sync.Mutex
	next                   uint64
	pages                  map[page.LeafLogPtr][]byte
	appendCounts           map[int]int
	batchCalls             map[int]int
	preparedSingleCalls    map[int]int
	preparedBatchCalls     map[int]int
	preparedBatchPageLens  []int
	activeMu               sync.Mutex
	activeLanes            map[int]int
	sameLaneConcurrent     bool
	sameLaneConcurrentCh   chan struct{}
	sameLaneConcurrentOnce sync.Once
	blockLane              int
	blockEntered           chan struct{}
	blockRelease           chan struct{}
	blockOnce              sync.Once
	failLane               int
	failErr                error
}

type laneRecordingLeafPageLogView struct {
	parent *laneRecordingLeafPageLog
	lane   int
}

func newLaneRecordingLeafPageLog() *laneRecordingLeafPageLog {
	return &laneRecordingLeafPageLog{
		pages:               make(map[page.LeafLogPtr][]byte),
		appendCounts:        make(map[int]int),
		batchCalls:          make(map[int]int),
		preparedSingleCalls: make(map[int]int),
		preparedBatchCalls:  make(map[int]int),
	}
}

func (l *laneRecordingLeafPageLog) resetObservations() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.appendCounts = make(map[int]int)
	l.batchCalls = make(map[int]int)
	l.preparedSingleCalls = make(map[int]int)
	l.preparedBatchCalls = make(map[int]int)
	l.preparedBatchPageLens = nil
}

func (l *laneRecordingLeafPageLog) blockFirstAppendOnLane(lane int) (entered <-chan struct{}, release func()) {
	enteredCh := make(chan struct{})
	releaseCh := make(chan struct{})
	l.activeMu.Lock()
	l.activeLanes = make(map[int]int)
	l.sameLaneConcurrent = false
	l.sameLaneConcurrentCh = make(chan struct{})
	l.sameLaneConcurrentOnce = sync.Once{}
	l.blockLane = lane
	l.blockEntered = enteredCh
	l.blockRelease = releaseCh
	l.blockOnce = sync.Once{}
	l.activeMu.Unlock()
	return enteredCh, func() { close(releaseCh) }
}

func (l *laneRecordingLeafPageLog) observedSameLaneConcurrentAppend() bool {
	l.activeMu.Lock()
	defer l.activeMu.Unlock()
	return l.sameLaneConcurrent
}

func (l *laneRecordingLeafPageLog) sameLaneConcurrentSignal() <-chan struct{} {
	l.activeMu.Lock()
	defer l.activeMu.Unlock()
	return l.sameLaneConcurrentCh
}

func (l *laneRecordingLeafPageLog) enterAppendLane(lane int) func() {
	l.activeMu.Lock()
	if l.activeLanes == nil && l.blockEntered == nil && l.sameLaneConcurrentCh == nil {
		l.activeMu.Unlock()
		return func() {}
	}
	if l.activeLanes == nil {
		l.activeLanes = make(map[int]int)
	}
	if l.activeLanes[lane] > 0 {
		l.sameLaneConcurrent = true
		if l.sameLaneConcurrentCh != nil {
			l.sameLaneConcurrentOnce.Do(func() { close(l.sameLaneConcurrentCh) })
		}
	}
	l.activeLanes[lane]++
	var wait <-chan struct{}
	if lane == l.blockLane && l.blockEntered != nil && l.blockRelease != nil {
		l.blockOnce.Do(func() {
			close(l.blockEntered)
			wait = l.blockRelease
		})
	}
	l.activeMu.Unlock()
	if wait != nil {
		<-wait
	}
	return func() {
		l.activeMu.Lock()
		if l.activeLanes[lane] <= 1 {
			delete(l.activeLanes, lane)
		} else {
			l.activeLanes[lane]--
		}
		l.activeMu.Unlock()
	}
}

func (l *laneRecordingLeafPageLog) LeafPageLogLane(workerIndex int) (LeafPageLog, bool) {
	if workerIndex <= 0 {
		return l, true
	}
	return &laneRecordingLeafPageLogView{parent: l, lane: workerIndex}, true
}

func (l *laneRecordingLeafPageLog) ConcurrentLeafPageAppends() bool { return true }

func (l *laneRecordingLeafPageLog) PreparedLeafPageAppends() bool { return true }

func (l *laneRecordingLeafPageLog) PreparedLeafPageBatchAppends() bool { return true }

func (l *laneRecordingLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	ptrs, err := l.appendLeafPagesForLane(0, [][]byte{leafPage}, false, false)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return ptrs[0], nil
}

func (l *laneRecordingLeafPageLog) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	return l.appendLeafPagesForLane(0, leafPages, true, false)
}

func (l *laneRecordingLeafPageLog) AppendPreparedLeafPage(leafPage []byte, _ []byte) (page.LeafLogPtr, error) {
	ptrs, err := l.appendLeafPagesForLane(0, [][]byte{leafPage}, false, true)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return ptrs[0], nil
}

func (l *laneRecordingLeafPageLog) AppendPreparedLeafPages(leafPages [][]byte, _ [][]byte) ([]page.LeafLogPtr, error) {
	return l.appendLeafPagesForLane(0, leafPages, true, true)
}

func (v *laneRecordingLeafPageLogView) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	ptrs, err := v.parent.appendLeafPagesForLane(v.lane, [][]byte{leafPage}, false, false)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return ptrs[0], nil
}

func (v *laneRecordingLeafPageLogView) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	return v.parent.appendLeafPagesForLane(v.lane, leafPages, true, false)
}

func (v *laneRecordingLeafPageLogView) AppendPreparedLeafPage(leafPage []byte, _ []byte) (page.LeafLogPtr, error) {
	ptrs, err := v.parent.appendLeafPagesForLane(v.lane, [][]byte{leafPage}, false, true)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return ptrs[0], nil
}

func (v *laneRecordingLeafPageLogView) AppendPreparedLeafPages(leafPages [][]byte, _ [][]byte) ([]page.LeafLogPtr, error) {
	return v.parent.appendLeafPagesForLane(v.lane, leafPages, true, true)
}

func (v *laneRecordingLeafPageLogView) PreparedLeafPageAppends() bool { return true }

func (v *laneRecordingLeafPageLogView) PreparedLeafPageBatchAppends() bool { return true }

func (l *laneRecordingLeafPageLog) appendLeafPagesForLane(lane int, leafPages [][]byte, batchCall bool, prepared bool) ([]page.LeafLogPtr, error) {
	leave := l.enterAppendLane(lane)
	defer leave()

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failErr != nil && (lane == l.failLane || (l.failLane < 0 && lane > 0)) {
		return nil, l.failErr
	}
	if l.next == 0 {
		l.next = 4
	}
	baseOffset := l.next
	l.next += page.PageSize + 32
	fileID := uint32(10_000 + lane)
	ptrs := make([]page.LeafLogPtr, len(leafPages))
	for i, leafPage := range leafPages {
		recordLen := uint32(page.PageSize)
		if len(leafPages) > 1 {
			recordLen = page.ValuePtrMarkGrouped(page.PageSize, uint8(i))
		}
		ptr := page.LeafLogPtr{FileID: fileID, Offset: baseOffset, RecordLengthHint: recordLen, SubIndex: uint16(i)}
		ptrs[i] = ptr
		l.pages[ptr] = append([]byte(nil), leafPage...)
	}
	l.appendCounts[lane] += len(leafPages)
	if batchCall {
		l.batchCalls[lane]++
	}
	if prepared {
		if batchCall {
			l.preparedBatchCalls[lane]++
			l.preparedBatchPageLens = append(l.preparedBatchPageLens, len(leafPages))
		} else {
			l.preparedSingleCalls[lane]++
		}
	}
	return ptrs, nil
}

func (l *laneRecordingLeafPageLog) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	leafPtr, err := page.LeafLogPtrFromValuePtr(ptr)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	data, ok := l.pages[leafPtr]
	if !ok {
		return nil, io.EOF
	}
	return append([]byte(nil), data...), nil
}

func (l *laneRecordingLeafPageLog) observedAppendCounts() map[int]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[int]int, len(l.appendCounts))
	for lane, count := range l.appendCounts {
		out[lane] = count
	}
	return out
}

func (l *laneRecordingLeafPageLog) observedPreparedBatchCalls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	total := 0
	for _, count := range l.preparedBatchCalls {
		total += count
	}
	return total
}

func TestSpanNativeApplyLeafLogOutputPreparedWorkerAppends(t *testing.T) {
	_, serial := newReadOnlyPrepareZipper(t)
	serial.SetOuterLeavesInValueLog(true)
	serialStore := newBatchMemoryLeafPageStore()
	serial.SetLeafPageLog(serialStore)
	serial.SetLeafPageReader(serialStore)

	_, native := newReadOnlyPrepareZipper(t)
	native.SetOuterLeavesInValueLog(true)
	nativeStore := newBatchMemoryLeafPageStore()
	native.SetLeafPageLog(nativeStore)
	native.SetLeafPageReader(nativeStore)

	serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 4096)
	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 4096)
	native.SetLeafPageReader(nativeStore)
	nativeStore.batchLens = nil
	nativeStore.singleCalls = 0

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	value := bytes.Repeat([]byte("v"), 180)
	for _, anchor := range []int{17, 1029, 2053, 3079} {
		for j := 0; j < 48; j++ {
			key := []byte(fmt.Sprintf("key-%06d-%03d", anchor, j))
			if err := delta.Set(key, value); err != nil {
				t.Fatalf("Set anchor=%d j=%d: %v", anchor, j, err)
			}
		}
	}

	serialNewRoot, _, _, err := serial.Apply(serialRoot, delta)
	if err != nil {
		t.Fatalf("serial Apply: %v", err)
	}
	result, err := native.ApplyWithOptions(nativeRoot, delta, ApplyOptions{SpanNativeApply: true, ParallelApplyConcurrency: 4})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	if !result.SpanNativeUsed || result.Metrics.ZipperLeafLogPagesWritten < 2 {
		t.Fatalf("span-native/leaf-log metrics used=%v pages=%d", result.SpanNativeUsed, result.Metrics.ZipperLeafLogPagesWritten)
	}
	if result.Metrics.ZipperLeafLogOutputAppendCalls == 0 || result.Metrics.ZipperLeafLogOutputAppendPages != result.Metrics.ZipperLeafLogPagesWritten {
		t.Fatalf("leaf-log append calls=%d appendPages=%d pagesWritten=%d", result.Metrics.ZipperLeafLogOutputAppendCalls, result.Metrics.ZipperLeafLogOutputAppendPages, result.Metrics.ZipperLeafLogPagesWritten)
	}
	if got := nativeStore.singleCalls; got != 0 {
		t.Fatalf("single leaf-log appends=%d want prepared batch append path", got)
	}
	if len(nativeStore.batchLens) == 0 {
		t.Fatalf("batch lens empty; want worker prepared appends")
	}
	var appended int
	for _, n := range nativeStore.batchLens {
		appended += n
	}
	if appended != result.Metrics.ZipperLeafLogPagesWritten {
		t.Fatalf("batch lens=%v appended=%d pagesWritten=%d", nativeStore.batchLens, appended, result.Metrics.ZipperLeafLogPagesWritten)
	}
	if !bytes.Equal(collectRootLeafPairs(t, serial, serialNewRoot), collectRootLeafPairs(t, native, result.RootID)) {
		t.Fatalf("span-native prepared leaf-log output mismatch")
	}
}

func TestSpanNativeApplyLeafLogOutputRoutesWorkerRangesToSelectedLanes(t *testing.T) {
	_, serial := newReadOnlyPrepareZipper(t)
	serial.SetOuterLeavesInValueLog(true)
	serialStore := newBatchMemoryLeafPageStore()
	serial.SetLeafPageLog(serialStore)
	serial.SetLeafPageReader(serialStore)

	_, native := newReadOnlyPrepareZipper(t)
	native.SetOuterLeavesInValueLog(true)
	nativeStore := newLaneRecordingLeafPageLog()
	native.SetLeafPageLog(nativeStore)
	native.SetLeafPageReader(nativeStore)

	serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 4096)
	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 4096)
	nativeStore.resetObservations()

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	value := bytes.Repeat([]byte("v"), 180)
	for _, anchor := range []int{17, 1029, 2053, 3079} {
		for j := 0; j < 48; j++ {
			key := []byte(fmt.Sprintf("key-%06d-%03d", anchor, j))
			if err := delta.Set(key, value); err != nil {
				t.Fatalf("Set anchor=%d j=%d: %v", anchor, j, err)
			}
		}
	}

	serialNewRoot, _, _, err := serial.Apply(serialRoot, delta)
	if err != nil {
		t.Fatalf("serial Apply: %v", err)
	}
	result, err := native.ApplyWithOptions(nativeRoot, delta, ApplyOptions{SpanNativeApply: true, ParallelApplyConcurrency: 4})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	if !result.SpanNativeUsed || result.SpanNativeWorkers < 2 {
		t.Fatalf("span-native used/workers=%v/%d", result.SpanNativeUsed, result.SpanNativeWorkers)
	}
	counts := nativeStore.observedAppendCounts()
	if counts[0] != 0 {
		t.Fatalf("default lane appends after reset=%d want 0; counts=%v", counts[0], counts)
	}
	selectedLanes := 0
	for lane, count := range counts {
		if lane <= 0 || count <= 0 {
			continue
		}
		selectedLanes++
	}
	if selectedLanes < 2 {
		t.Fatalf("selected lanes with appends=%d counts=%v want >=2", selectedLanes, counts)
	}
	if preparedBatches := nativeStore.observedPreparedBatchCalls(); preparedBatches == 0 {
		t.Fatalf("prepared batch calls=0 counts=%v", counts)
	}
	if !bytes.Equal(collectRootLeafPairs(t, serial, serialNewRoot), collectRootLeafPairs(t, native, result.RootID)) {
		t.Fatalf("span-native lane-routed leaf-log output mismatch")
	}
}

func TestSpanNativeApplyLeafLogOutputLanesFollowWorkerID(t *testing.T) {
	_, native := newReadOnlyPrepareZipper(t)
	native.SetOuterLeavesInValueLog(true)
	store := newLaneRecordingLeafPageLog()
	native.SetLeafPageLog(store)
	native.SetLeafPageReader(store)

	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 8192)
	store.resetObservations()
	entered, release := store.blockFirstAppendOnLane(1)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	value := bytes.Repeat([]byte("v"), 180)
	for anchor := 0; anchor < 8192; anchor += 257 {
		for j := 0; j < 16; j++ {
			key := []byte(fmt.Sprintf("key-%06d-%03d", anchor, j))
			if err := delta.Set(key, value); err != nil {
				t.Fatalf("Set anchor=%d j=%d: %v", anchor, j, err)
			}
		}
	}

	type applyResult struct {
		result ApplyResult
		err    error
	}
	done := make(chan applyResult, 1)
	go func() {
		result, err := native.ApplyWithOptions(nativeRoot, delta, ApplyOptions{SpanNativeApply: true, ParallelApplyConcurrency: 2})
		done <- applyResult{result: result, err: err}
	}()

	select {
	case <-entered:
	case res := <-done:
		t.Fatalf("ApplyWithOptions completed before lane 1 append blocked: result=%+v err=%v", res.result, res.err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for lane 1 append to block")
	}

	select {
	case <-store.sameLaneConcurrentSignal():
	case <-time.After(100 * time.Millisecond):
	}
	release()

	res := <-done
	if res.err != nil {
		t.Fatalf("ApplyWithOptions: %v", res.err)
	}
	if !res.result.SpanNativeUsed || res.result.SpanNativeWorkers != 2 {
		t.Fatalf("span-native used/workers=%v/%d", res.result.SpanNativeUsed, res.result.SpanNativeWorkers)
	}
	if store.observedSameLaneConcurrentAppend() {
		t.Fatalf("observed concurrent appends to the same selected lane; lane selection must follow worker ID")
	}
}

func TestSpanNativeApplyLeafLogOutputSelectedLaneFailureReturnsBeforeReduce(t *testing.T) {
	_, native := newReadOnlyPrepareZipper(t)
	native.SetOuterLeavesInValueLog(true)
	store := newLaneRecordingLeafPageLog()
	store.failLane = -1
	store.failErr = errors.New("test selected leaf-log lane append failure")
	native.SetLeafPageLog(store)
	native.SetLeafPageReader(store)

	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 4096)
	store.resetObservations()

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	value := bytes.Repeat([]byte("v"), 180)
	for _, anchor := range []int{17, 1029, 2053, 3079} {
		for j := 0; j < 48; j++ {
			key := []byte(fmt.Sprintf("key-%06d-%03d", anchor, j))
			if err := delta.Set(key, value); err != nil {
				t.Fatalf("Set anchor=%d j=%d: %v", anchor, j, err)
			}
		}
	}

	result, err := native.ApplyWithOptions(nativeRoot, delta, ApplyOptions{SpanNativeApply: true, ParallelApplyConcurrency: 4})
	if !errors.Is(err, store.failErr) {
		t.Fatalf("ApplyWithOptions err=%v want %v", err, store.failErr)
	}
	if result.RootID != 0 {
		t.Fatalf("RootID=%d want zero because selected-lane output failed before reducer publication", result.RootID)
	}
}

func TestSpanNativeApplyLeafLogOutputCommitFailureReturnsBeforeReduce(t *testing.T) {
	_, native := newReadOnlyPrepareZipper(t)
	native.SetOuterLeavesInValueLog(true)
	store := newBatchMemoryLeafPageStore()
	native.SetLeafPageLog(store)
	native.SetLeafPageReader(store)

	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 4096)
	native.SetLeafPageReader(store)
	store.batchLens = nil
	store.singleCalls = 0

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	value := bytes.Repeat([]byte("v"), 180)
	for _, anchor := range []int{17, 1029, 2053, 3079} {
		for j := 0; j < 48; j++ {
			key := []byte(fmt.Sprintf("key-%06d-%03d", anchor, j))
			if err := delta.Set(key, value); err != nil {
				t.Fatalf("Set anchor=%d j=%d: %v", anchor, j, err)
			}
		}
	}

	appendErr := errors.New("test span-native prepared leaf-log append failure")
	native.SetLeafPageLog(&failingBatchLeafPageLog{batchMemoryLeafPageStore: store, err: appendErr})
	result, err := native.ApplyWithOptions(nativeRoot, delta, ApplyOptions{SpanNativeApply: true, ParallelApplyConcurrency: 4})
	if !errors.Is(err, appendErr) {
		t.Fatalf("ApplyWithOptions err=%v want %v", err, appendErr)
	}
	if result.RootID != 0 {
		t.Fatalf("RootID=%d want zero because prepared output failed before reducer publication", result.RootID)
	}
	if result.Metrics.ZipperLeafLogOutputAppendCalls == 0 {
		t.Fatalf("leaf-log append calls=%d want at least one failed worker append", result.Metrics.ZipperLeafLogOutputAppendCalls)
	}
	if result.Metrics.ZipperLeafLogOutputAppendPages != 0 {
		t.Fatalf("successful append pages=%d want 0 on failed buffered commit", result.Metrics.ZipperLeafLogOutputAppendPages)
	}
}

func TestSpanNativeApplyGroupsTinyLeafSpansIntoWorkerRanges(t *testing.T) {
	prevProcs := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(prevProcs) })

	_, serial := newReadOnlyPrepareZipper(t)
	_, native := newReadOnlyPrepareZipper(t)
	serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 20000)
	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 20000)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for i := 0; i < 20000; i += 31 {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("grouped-%06d", i))); err != nil {
			t.Fatalf("Set grouped %d: %v", i, err)
		}
	}

	prepared, err := native.PrepareReadOnly(nativeRoot, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	if len(prepared.LeafSpans) <= 4 {
		t.Fatalf("prepared spans=%d want enough tiny spans to prove grouping", len(prepared.LeafSpans))
	}
	serialNewRoot, _, _, err := serial.Apply(serialRoot, delta)
	if err != nil {
		t.Fatalf("serial Apply: %v", err)
	}
	pool := NewApplyWorkerPool(4)
	defer pool.Close()
	result, err := native.ApplyWithOptions(nativeRoot, delta, ApplyOptions{
		SpanNativeApply:          true,
		ParallelApplyConcurrency: 4,
		ParallelApplyWorkerPool:  pool,
	})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	if !result.SpanNativeEligible || !result.SpanNativeUsed || result.SpanNativeWorkers != 4 {
		t.Fatalf("span-native flags eligible/used/workers=%v/%v/%d", result.SpanNativeEligible, result.SpanNativeUsed, result.SpanNativeWorkers)
	}
	ready := result.Metrics.ZipperSpanNativeReadyTasks
	if ready != result.SpanNativeWorkers {
		t.Fatalf("ready work units=%d want one range per active worker %d", ready, result.SpanNativeWorkers)
	}
	if ready >= len(prepared.LeafSpans) {
		t.Fatalf("ready work units=%d should group %d leaf spans", ready, len(prepared.LeafSpans))
	}
	if got := result.Metrics.ZipperSpanNativeTaskSpansTotal; got != len(prepared.LeafSpans) {
		t.Fatalf("task spans total=%d want prepared spans %d", got, len(prepared.LeafSpans))
	}
	if got := result.Metrics.ZipperSpanNativeTaskOpsTotal; got != prepared.PointOps {
		t.Fatalf("task ops total=%d want point ops %d", got, prepared.PointOps)
	}
	if result.Metrics.ZipperSpanNativeTaskSpansMax <= 1 {
		t.Fatalf("task spans max=%d want grouped work units", result.Metrics.ZipperSpanNativeTaskSpansMax)
	}
	if result.Metrics.ZipperSpanNativeSingleSpanTasks >= ready {
		t.Fatalf("single-span tasks=%d ready=%d want grouped distribution", result.Metrics.ZipperSpanNativeSingleSpanTasks, ready)
	}
	if !bytes.Equal(collectRootLeafPairs(t, serial, serialNewRoot), collectRootLeafPairs(t, native, result.RootID)) {
		t.Fatalf("grouped span-native output mismatch")
	}
}

func TestSpanNativeApplyRejectsInvalidPreparedPlanBeforeOutput(t *testing.T) {
	p, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 40)
	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for i := 0; i < 40; i++ {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("guard-%06d", i))); err != nil {
			t.Fatalf("Set update: %v", err)
		}
	}
	prepared, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	if len(prepared.LeafSpans) == 0 {
		t.Fatalf("expected prepared spans")
	}
	ops := delta.SortedEntries()
	if !validateSpanNativePreparedPlan(ops, prepared, ApplyOptions{}) {
		t.Fatalf("fresh whole-root plan unexpectedly ineligible")
	}

	bad := prepared
	bad.LeafSpans = append([]ReadOnlyLeafSpan(nil), prepared.LeafSpans...)
	bad.LeafSpans[0].PointOpStart = 1
	beforePages := p.PageCount()
	_, used, err := z.applySpanNativeWithPrepared(rootID, ops, bad, ApplyOptions{}, 1, nil)
	if err != nil {
		t.Fatalf("applySpanNativeWithPrepared invalid plan err=%v", err)
	}
	if used {
		t.Fatalf("invalid prepared plan used span-native output path")
	}
	if got := p.PageCount(); got != beforePages {
		t.Fatalf("invalid prepared plan allocated pages: got %d want %d", got, beforePages)
	}

	bad = prepared
	bad.OmitKeys = true
	bad.LeafSpans = append([]ReadOnlyLeafSpan(nil), prepared.LeafSpans...)
	if validateSpanNativePreparedPlan(ops, bad, ApplyOptions{}) {
		t.Fatalf("validateSpanNativePreparedPlan accepted OmitKeys plan")
	}
	_, used, err = z.applySpanNativeWithPrepared(rootID, ops, bad, ApplyOptions{}, 1, nil)
	if err != nil {
		t.Fatalf("applySpanNativeWithPrepared OmitKeys err=%v", err)
	}
	if used {
		t.Fatalf("OmitKeys plan used span-native output path")
	}
	if got := p.PageCount(); got != beforePages {
		t.Fatalf("OmitKeys plan allocated pages: got %d want %d", got, beforePages)
	}

	bad = prepared
	bad.LeafSpans = append([]ReadOnlyLeafSpan(nil), prepared.LeafSpans...)
	bad.LeafSpans[0].PointOpStart = -1
	if validateSpanNativePreparedPlan(ops, bad, ApplyOptions{}) {
		t.Fatalf("validateSpanNativePreparedPlan accepted negative PointOpStart")
	}
	_, used, err = z.applySpanNativeWithPrepared(rootID, ops, bad, ApplyOptions{}, 1, nil)
	if err != nil {
		t.Fatalf("applySpanNativeWithPrepared negative PointOpStart err=%v", err)
	}
	if used {
		t.Fatalf("negative PointOpStart used span-native output path")
	}
	if got := p.PageCount(); got != beforePages {
		t.Fatalf("negative PointOpStart allocated pages: got %d want %d", got, beforePages)
	}

	bad = prepared
	bad.LeafSpans = append([]ReadOnlyLeafSpan(nil), prepared.LeafSpans...)
	bad.LeafSpans[0].PointOpEnd = -1
	if validateSpanNativePreparedPlan(ops, bad, ApplyOptions{}) {
		t.Fatalf("validateSpanNativePreparedPlan accepted negative PointOpEnd")
	}

	bad = prepared
	bad.LeafSpans = append([]ReadOnlyLeafSpan(nil), prepared.LeafSpans...)
	bad.LeafSpans[0].LowKey = []byte("z")
	bad.LeafSpans[0].HighKey = []byte("a")
	if validateSpanNativePreparedPlan(ops, bad, ApplyOptions{}) {
		t.Fatalf("validateSpanNativePreparedPlan accepted non-monotonic span bounds")
	}
}

func TestSpanNativeReducerRejectsNondeterministicRefOrder(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	_, err := z.reduceSpanNativeRoot([]Split{
		{Key: []byte{}, Ref: page.PageChildRef(1)},
		{Key: []byte("b"), Ref: page.PageChildRef(2)},
		{Key: []byte("a"), Ref: page.PageChildRef(3)},
	}, nil)
	if err == nil {
		t.Fatalf("reduceSpanNativeRoot accepted out-of-order refs")
	}
}

func TestSpanNativeApplyWholeRootMultiLeafReducerParity(t *testing.T) {
	_, serial := newReadOnlyPrepareZipper(t)
	_, native := newReadOnlyPrepareZipper(t)
	serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 200)
	nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 200)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for i := 0; i < 200; i++ {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("multi-%06d", i))); err != nil {
			t.Fatalf("Set update: %v", err)
		}
	}

	result := assertSpanNativeParity(t, serial, native, serialRoot, nativeRoot, delta)
	if result.Metrics.ZipperLeafMerges < 2 {
		t.Fatalf("leaf merges=%d want multi-leaf", result.Metrics.ZipperLeafMerges)
	}
	if result.Metrics.ZipperInternalPagesWritten == 0 {
		t.Fatalf("internal pages written=%d want context reducer to rebuild parent/root pages", result.Metrics.ZipperInternalPagesWritten)
	}
}

func TestSpanNativeApplyWholeRootMultiLeafRetiresInternalPages(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 1024)
	oldInternalPages := collectSpanNativeTestInternalPageIDs(t, z, rootID)
	if len(oldInternalPages) == 0 {
		t.Fatalf("old root has no internal pages; test requires whole-root multi-leaf rewrite")
	}

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for i := 0; i < 1024; i++ {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("retire-%06d", i))); err != nil {
			t.Fatalf("Set update: %v", err)
		}
	}

	result, err := z.ApplyWithOptions(rootID, delta, ApplyOptions{SpanNativeApply: true})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	if !result.SpanNativeEligible || !result.SpanNativeUsed {
		t.Fatalf("span-native flags eligible/used=%v/%v", result.SpanNativeEligible, result.SpanNativeUsed)
	}
	retired := make(map[uint64]struct{}, len(result.PendingRetiredPages))
	for _, pageID := range result.PendingRetiredPages {
		retired[pageID] = struct{}{}
	}
	for _, pageID := range oldInternalPages {
		if _, ok := retired[pageID]; !ok {
			t.Fatalf("old internal page %d missing from pending retired pages %v", pageID, result.PendingRetiredPages)
		}
	}
}

func assertSpanNativeParity(t *testing.T, serial, native *Zipper, serialRoot, nativeRoot uint64, delta *batch.Batch) ApplyResult {
	t.Helper()
	serialNewRoot, _, _, err := serial.Apply(serialRoot, delta)
	if err != nil {
		t.Fatalf("serial Apply: %v", err)
	}
	result, err := native.ApplyWithOptions(nativeRoot, delta, ApplyOptions{SpanNativeApply: true, ParallelApplyConcurrency: 2})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	if !result.SpanNativeEligible || !result.SpanNativeUsed {
		t.Fatalf("span-native flags eligible/used=%v/%v", result.SpanNativeEligible, result.SpanNativeUsed)
	}
	if result.Metrics.ZipperLeafMerges < 1 {
		t.Fatalf("leaf merges=%d want at least 1", result.Metrics.ZipperLeafMerges)
	}
	serialPairs := collectRootLeafPairs(t, serial, serialNewRoot)
	nativePairs := collectRootLeafPairs(t, native, result.RootID)
	if !bytes.Equal(serialPairs, nativePairs) {
		t.Fatalf("span-native output mismatch\nserial=%q\nnative=%q", serialPairs, nativePairs)
	}
	return result
}

func collectRootLeafPairs(t *testing.T, z *Zipper, rootID uint64) []byte {
	t.Helper()
	return collectChildRefLeafPairs(t, z, page.PageChildRef(rootID))
}

func collectSpanNativeTestInternalPageIDs(t *testing.T, z *Zipper, rootID uint64) []uint64 {
	t.Helper()
	return collectSpanNativeTestInternalPageIDsRef(t, z, page.PageChildRef(rootID))
}

func collectSpanNativeTestInternalPageIDsRef(t *testing.T, z *Zipper, ref page.ChildRef) []uint64 {
	t.Helper()
	if ref.Kind != page.ChildRefPage {
		return nil
	}
	data, err := z.pager.Get(ref.Page)
	if err != nil {
		t.Fatalf("get page %d: %v", ref.Page, err)
	}
	n := node.NewNode(data)
	if n.Type() != page.PageTypeInternal {
		return nil
	}
	out := []uint64{ref.Page}
	for i := uint16(0); i < n.Count(); i++ {
		child, err := n.GetInternalChildRef(i)
		if err != nil {
			t.Fatalf("internal child %d: %v", i, err)
		}
		out = append(out, collectSpanNativeTestInternalPageIDsRef(t, z, child)...)
	}
	return out
}

func collectChildRefLeafPairs(t *testing.T, z *Zipper, ref page.ChildRef) []byte {
	t.Helper()
	n, _, leafScratch, leafScratchRef, _, err := z.loadNodeRef(ref, nil)
	if err != nil {
		t.Fatalf("load child ref %+v: %v", ref, err)
	}
	if leafScratchRef {
		releaseLeafPageScratch(nil, leafScratch)
	}
	switch n.Type() {
	case page.PageTypeLeaf, 0:
		var out []byte
		for i := uint16(0); i < n.Count(); i++ {
			entry, err := n.GetLeafEntry(i)
			if err != nil {
				t.Fatalf("leaf entry %d: %v", i, err)
			}
			out = append(out, entry.Key...)
			out = append(out, '=')
			out = append(out, entry.Value...)
			out = append(out, '\n')
		}
		return out
	case page.PageTypeInternal:
		var out []byte
		for i := uint16(0); i < n.Count(); i++ {
			child, err := n.GetInternalChildRef(i)
			if err != nil {
				t.Fatalf("internal child %d: %v", i, err)
			}
			out = append(out, collectChildRefLeafPairs(t, z, child)...)
		}
		return out
	default:
		t.Fatalf("node type=%d want leaf/internal", n.Type())
		return nil
	}
}
