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
	if result.Metrics.ZipperLeafMerges != 1 {
		t.Fatalf("leaf merges=%d want 1", result.Metrics.ZipperLeafMerges)
	}
	serialPairs := collectRootLeafPairs(t, serial, serialNewRoot)
	nativePairs := collectRootLeafPairs(t, native, result.RootID)
	if !bytes.Equal(serialPairs, nativePairs) {
		t.Fatalf("span-native output mismatch\nserial=%q\nnative=%q", serialPairs, nativePairs)
	}
}

func collectRootLeafPairs(t *testing.T, z *Zipper, rootID uint64) []byte {
	t.Helper()
	data, err := z.pager.Get(rootID)
	if err != nil {
		t.Fatalf("get root %d: %v", rootID, err)
	}
	n := node.NewNode(data)
	if n.Type() == page.PageTypeInternal {
		if n.Count() != 1 {
			t.Fatalf("internal root count=%d want 1 for test helper", n.Count())
		}
		ref, err := n.GetInternalChildRef(0)
		if err != nil {
			t.Fatalf("get root child: %v", err)
		}
		if ref.Kind != page.ChildRefPage {
			t.Fatalf("root child kind=%d want page", ref.Kind)
		}
		data, err = z.pager.Get(ref.Page)
		if err != nil {
			t.Fatalf("get child %d: %v", ref.Page, err)
		}
		n = node.NewNode(data)
	}
	if n.Type() != page.PageTypeLeaf {
		t.Fatalf("node type=%d want leaf", n.Type())
	}
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
}
