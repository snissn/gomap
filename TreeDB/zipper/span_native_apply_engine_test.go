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

func TestSpanNativeApplyPartialMultiLeafFallsBackBeforeSpanOutput(t *testing.T) {
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
	if !result.SpanNativeEligible {
		t.Fatalf("SpanNativeEligible=false want candidate eligible before parent-stitch fallback")
	}
	if result.SpanNativeUsed {
		t.Fatalf("SpanNativeUsed=true want fallback before partial multi-leaf prepared output")
	}
	if !bytes.Equal(collectRootLeafPairs(t, serial, serialNewRoot), collectRootLeafPairs(t, native, result.RootID)) {
		t.Fatalf("partial fallback output mismatch")
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
	if result.Metrics.ZipperRootSplitLevels == 0 {
		t.Fatalf("root split levels=%d want reducer to build internal root", result.Metrics.ZipperRootSplitLevels)
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
