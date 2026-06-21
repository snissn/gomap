package zipper

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
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
	if !validateSpanNativePreparedPlan(delta.SortedEntries(), prepared) {
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

func TestSpanNativeApplyLeafLogOutputPreparedWorkerAppends(t *testing.T) {
	_, serial := newReadOnlyPrepareZipper(t)
	serial.SetOuterLeavesInValueLog(true)
	serialStore := newBatchMemoryLeafPageStore()
	serial.SetLeafPageLog(serialStore)
	serial.SetLeafPageReader(serialStore)

	_, native := newReadOnlyPrepareZipper(t)
	native.SetOuterLeavesInValueLog(true)
	nativeStore := newConcurrentBatchMemoryLeafPageStore(200 * time.Microsecond)
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
	if got := nativeStore.concurrentCalls.Load(); got == 0 {
		t.Fatal("concurrent leaf-log append support was not queried")
	}
	if !bytes.Equal(collectRootLeafPairs(t, serial, serialNewRoot), collectRootLeafPairs(t, native, result.RootID)) {
		t.Fatalf("span-native prepared leaf-log output mismatch")
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

type preparedBatchMemoryLeafPageStore struct {
	*batchMemoryLeafPageStore
	preparedSingleCalls int
	preparedBatchLens   []int
}

func newPreparedBatchMemoryLeafPageStore() *preparedBatchMemoryLeafPageStore {
	return &preparedBatchMemoryLeafPageStore{batchMemoryLeafPageStore: newBatchMemoryLeafPageStore()}
}

func (s *preparedBatchMemoryLeafPageStore) PreparedLeafPageAppends() bool { return true }

func (s *preparedBatchMemoryLeafPageStore) PreparedLeafPageBatchAppends() bool { return true }

func (s *preparedBatchMemoryLeafPageStore) AppendPreparedLeafPage(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(leafPage) != page.PageSize {
		return page.LeafLogPtr{}, fmt.Errorf("prepared leaf page size=%d want=%d", len(leafPage), page.PageSize)
	}
	s.preparedSingleCalls++
	if s.next == 0 {
		s.next = 4
	}
	ptr := page.LeafLogPtr{FileID: 1, Offset: uint64(s.next), RecordLengthHint: page.PageSize}
	s.next += page.PageSize + 32
	if !s.discardAppends {
		s.pages[ptr] = append([]byte(nil), preparedPayload...)
	}
	return ptr, nil
}

func (s *preparedBatchMemoryLeafPageStore) AppendPreparedLeafPages(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(leafPages) != len(preparedPayloads) {
		return nil, fmt.Errorf("prepared leaf page batch has %d payloads for %d leaf pages", len(preparedPayloads), len(leafPages))
	}
	if len(leafPages) == 0 {
		return nil, nil
	}
	s.preparedBatchLens = append(s.preparedBatchLens, len(leafPages))
	if s.next == 0 {
		s.next = 4
	}
	offset := uint64(s.next)
	s.next += page.PageSize + 32
	ptrs := make([]page.LeafLogPtr, len(leafPages))
	for i, leafPage := range leafPages {
		if len(leafPage) != page.PageSize {
			return nil, fmt.Errorf("prepared leaf page %d size=%d want=%d", i, len(leafPage), page.PageSize)
		}
		ptr := page.LeafLogPtr{FileID: 1, Offset: offset, RecordLengthHint: page.ValuePtrMarkGrouped(page.PageSize, uint8(i)), SubIndex: uint16(i)}
		ptrs[i] = ptr
		if !s.discardAppends {
			s.pages[ptr] = append([]byte(nil), preparedPayloads[i]...)
		}
	}
	return ptrs, nil
}

type concurrentBatchMemoryLeafPageStore struct {
	*batchMemoryLeafPageStore
	appendDelay     time.Duration
	concurrentCalls atomic.Int64
}

func newConcurrentBatchMemoryLeafPageStore(delay time.Duration) *concurrentBatchMemoryLeafPageStore {
	return &concurrentBatchMemoryLeafPageStore{
		batchMemoryLeafPageStore: newBatchMemoryLeafPageStore(),
		appendDelay:              delay,
	}
}

func (s *concurrentBatchMemoryLeafPageStore) ConcurrentLeafPageAppends() bool {
	s.concurrentCalls.Add(1)
	return true
}

func (s *concurrentBatchMemoryLeafPageStore) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	ptrs, err := s.AppendLeafPages([][]byte{leafPage})
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return ptrs[0], nil
}

func (s *concurrentBatchMemoryLeafPageStore) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if s.appendDelay > 0 {
		time.Sleep(s.appendDelay)
	}
	return s.batchMemoryLeafPageStore.AppendLeafPages(leafPages)
}

func TestSpanNativeLeafLogOutputRequestBatchHelper(t *testing.T) {
	makeLeafPage := func(ch byte) []byte {
		return bytes.Repeat([]byte{ch}, page.PageSize)
	}

	t.Run("raw", func(t *testing.T) {
		_, z := newReadOnlyPrepareZipper(t)
		z.SetOuterLeavesInValueLog(true)
		store := newBatchMemoryLeafPageStore()
		z.SetLeafPageLog(store)
		gate := newSpanNativeLeafLogOutputGate(4)
		reqs := []*spanNativeLeafLogOutputRequest{
			{leafPages: [][]byte{makeLeafPage('a')}, done: make(chan spanNativeLeafLogOutputResult, 1)},
			{leafPages: [][]byte{makeLeafPage('b'), makeLeafPage('c')}, done: make(chan spanNativeLeafLogOutputResult, 1)},
		}
		if err := gate.appendQueuedLeafLogOutputRequests(z, reqs); err != nil {
			t.Fatalf("appendQueuedLeafLogOutputRequests raw: %v", err)
		}
		res1 := <-reqs[0].done
		res2 := <-reqs[1].done
		if res1.err != nil || res2.err != nil {
			t.Fatalf("raw batch result errs=%v/%v", res1.err, res2.err)
		}
		if len(res1.refs) != 1 || len(res2.refs) != 2 {
			t.Fatalf("raw refs lens=%d/%d want 1/2", len(res1.refs), len(res2.refs))
		}
		if len(store.batchLens) != 1 || store.batchLens[0] != 3 {
			t.Fatalf("raw batch lens=%v want [3]", store.batchLens)
		}
		if store.singleCalls != 0 {
			t.Fatalf("raw single appends=%d want 0", store.singleCalls)
		}
	})

	t.Run("prepared", func(t *testing.T) {
		_, z := newReadOnlyPrepareZipper(t)
		z.SetOuterLeavesInValueLog(true)
		store := newPreparedBatchMemoryLeafPageStore()
		z.SetLeafPageLog(store)
		gate := newSpanNativeLeafLogOutputGate(4)
		reqs := []*spanNativeLeafLogOutputRequest{
			{leafPages: [][]byte{makeLeafPage('d')}, preparedPayloads: [][]byte{[]byte("prepared-d")}, done: make(chan spanNativeLeafLogOutputResult, 1)},
			{leafPages: [][]byte{makeLeafPage('e'), makeLeafPage('f')}, preparedPayloads: [][]byte{[]byte("prepared-e"), []byte("prepared-f")}, done: make(chan spanNativeLeafLogOutputResult, 1)},
		}
		if err := gate.appendQueuedLeafLogOutputRequests(z, reqs); err != nil {
			t.Fatalf("appendQueuedLeafLogOutputRequests prepared: %v", err)
		}
		res1 := <-reqs[0].done
		res2 := <-reqs[1].done
		if res1.err != nil || res2.err != nil {
			t.Fatalf("prepared batch result errs=%v/%v", res1.err, res2.err)
		}
		if len(res1.refs) != 1 || len(res2.refs) != 2 {
			t.Fatalf("prepared refs lens=%d/%d want 1/2", len(res1.refs), len(res2.refs))
		}
		if len(store.preparedBatchLens) != 1 || store.preparedBatchLens[0] != 3 {
			t.Fatalf("prepared batch lens=%v want [3]", store.preparedBatchLens)
		}
		if store.preparedSingleCalls != 0 {
			t.Fatalf("prepared single appends=%d want 0", store.preparedSingleCalls)
		}
		if got := store.pages[res1.refs[0].Log]; !bytes.Equal(got, []byte("prepared-d")) {
			t.Fatalf("prepared single payload=%q want %q", got, []byte("prepared-d"))
		}
		if got := store.pages[res2.refs[0].Log]; !bytes.Equal(got, []byte("prepared-e")) {
			t.Fatalf("prepared first batch payload=%q want %q", got, []byte("prepared-e"))
		}
		if got := store.pages[res2.refs[1].Log]; !bytes.Equal(got, []byte("prepared-f")) {
			t.Fatalf("prepared second batch payload=%q want %q", got, []byte("prepared-f"))
		}
	})

	t.Run("failure", func(t *testing.T) {
		_, z := newReadOnlyPrepareZipper(t)
		z.SetOuterLeavesInValueLog(true)
		store := newBatchMemoryLeafPageStore()
		z.SetLeafPageLog(&failingBatchLeafPageLog{batchMemoryLeafPageStore: store, err: errors.New("append failed")})
		gate := newSpanNativeLeafLogOutputGate(4)
		reqs := []*spanNativeLeafLogOutputRequest{
			{leafPages: [][]byte{makeLeafPage('g')}, done: make(chan spanNativeLeafLogOutputResult, 1)},
			{leafPages: [][]byte{makeLeafPage('h')}, done: make(chan spanNativeLeafLogOutputResult, 1)},
		}
		if err := gate.appendQueuedLeafLogOutputRequests(z, reqs); err == nil {
			t.Fatal("appendQueuedLeafLogOutputRequests failure: expected error")
		}
		res1 := <-reqs[0].done
		res2 := <-reqs[1].done
		if res1.err == nil || res2.err == nil {
			t.Fatalf("failure results errs=%v/%v want non-nil", res1.err, res2.err)
		}
		if len(store.batchLens) != 0 || store.singleCalls != 0 {
			t.Fatalf("failure should not publish appends: batchLens=%v singleCalls=%d", store.batchLens, store.singleCalls)
		}
	})

	t.Run("collector", func(t *testing.T) {
		_, z := newReadOnlyPrepareZipper(t)
		z.SetOuterLeavesInValueLog(true)
		store := newConcurrentBatchMemoryLeafPageStore(200 * time.Microsecond)
		z.SetLeafPageLog(store)
		gate := newSpanNativeLeafLogOutputGate(4)
		gate.startCollector(z)
		defer gate.closeCollector()

		type result struct {
			ref page.ChildRef
			err error
		}
		resCh := make(chan result, 4)
		for _, ch := range []byte{'i', 'j', 'k', 'l'} {
			ch := ch
			go func() {
				ref, err := gate.persistLeafPageData(z, makeLeafPage(ch), nil)
				resCh <- result{ref: ref, err: err}
			}()
		}
		for range []byte{'i', 'j', 'k', 'l'} {
			select {
			case res := <-resCh:
				if res.err != nil {
					t.Fatalf("collector persistLeafPageData: %v", res.err)
				}
				if !res.ref.IsLeafLog() {
					t.Fatalf("collector ref kind=%v want leaf-log", res.ref.Kind)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("collector persistLeafPageData timed out")
			}
		}
		if len(store.batchLens) == 0 {
			t.Fatal("collector batch lens empty")
		}
		var total int
		for _, n := range store.batchLens {
			total += n
		}
		if total != 4 {
			t.Fatalf("collector batch lens=%v total=%d want 4", store.batchLens, total)
		}
		if store.singleCalls != 0 {
			t.Fatalf("collector single appends=%d want 0", store.singleCalls)
		}
	})
}

func TestSpanNativeApplyGroupsTinyLeafSpansIntoBoundedWorkUnits(t *testing.T) {
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
	if len(prepared.LeafSpans) <= 4*readOnlyLeafSpanWorkUnitQueueFactor {
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
	if ready <= result.SpanNativeWorkers || ready > result.SpanNativeWorkers*readOnlyLeafSpanWorkUnitQueueFactor {
		t.Fatalf("ready work units=%d want bounded queue > workers and <= factor cap", ready)
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
	if !validateSpanNativePreparedPlan(ops, prepared) {
		t.Fatalf("fresh whole-root plan unexpectedly ineligible")
	}

	bad := prepared
	bad.LeafSpans = append([]ReadOnlyLeafSpan(nil), prepared.LeafSpans...)
	bad.LeafSpans[0].PointOpStart = 1
	beforePages := p.PageCount()
	_, used, err := z.applySpanNativeWithPrepared(rootID, ops, bad, 1, nil)
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
	if validateSpanNativePreparedPlan(ops, bad) {
		t.Fatalf("validateSpanNativePreparedPlan accepted OmitKeys plan")
	}
	_, used, err = z.applySpanNativeWithPrepared(rootID, ops, bad, 1, nil)
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
	if validateSpanNativePreparedPlan(ops, bad) {
		t.Fatalf("validateSpanNativePreparedPlan accepted negative PointOpStart")
	}
	_, used, err = z.applySpanNativeWithPrepared(rootID, ops, bad, 1, nil)
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
	if validateSpanNativePreparedPlan(ops, bad) {
		t.Fatalf("validateSpanNativePreparedPlan accepted negative PointOpEnd")
	}

	bad = prepared
	bad.LeafSpans = append([]ReadOnlyLeafSpan(nil), prepared.LeafSpans...)
	bad.LeafSpans[0].LowKey = []byte("z")
	bad.LeafSpans[0].HighKey = []byte("a")
	if validateSpanNativePreparedPlan(ops, bad) {
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
