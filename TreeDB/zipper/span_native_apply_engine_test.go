package zipper

import (
	"bytes"
	"fmt"
	"testing"

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
	if result.Metrics.ZipperSpanNativeWorkerBusyNs <= 0 || result.Metrics.ZipperSpanNativeWorkerWaitNs <= 0 {
		t.Fatalf("span-native worker busy/wait ns=%d/%d, want non-zero", result.Metrics.ZipperSpanNativeWorkerBusyNs, result.Metrics.ZipperSpanNativeWorkerWaitNs)
	}
	if result.Metrics.ZipperSpanNativeReadyTasks == 0 || result.Metrics.ZipperSpanNativeDispatchedTasks != result.Metrics.ZipperSpanNativeReadyTasks || result.Metrics.ZipperSpanNativeCompletedTasks != result.Metrics.ZipperSpanNativeDispatchedTasks {
		t.Fatalf("span-native scheduler ready/dispatched/completed=%d/%d/%d", result.Metrics.ZipperSpanNativeReadyTasks, result.Metrics.ZipperSpanNativeDispatchedTasks, result.Metrics.ZipperSpanNativeCompletedTasks)
	}
	if !bytes.Equal(collectRootLeafPairs(t, serial, serialNewRoot), collectRootLeafPairs(t, native, result.RootID)) {
		t.Fatalf("concurrent span-native output mismatch")
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
	if ref.Kind != page.ChildRefPage {
		t.Fatalf("child kind=%d want page", ref.Kind)
	}
	data, err := z.pager.Get(ref.Page)
	if err != nil {
		t.Fatalf("get page %d: %v", ref.Page, err)
	}
	n := node.NewNode(data)
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
