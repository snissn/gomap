package zipper

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func requireValidReadOnlyPrepare(tb testing.TB, prepared ReadOnlyPrepareResult) {
	tb.Helper()
	if err := prepared.ValidateLeafSpans(); err != nil {
		tb.Fatalf("ValidateLeafSpans: %v", err)
	}
}

func newReadOnlyPrepareZipper(tb testing.TB) (*pager.Pager, *Zipper) {
	tb.Helper()
	p, err := pager.Open(filepath.Join(tb.TempDir(), "index.db"), 65536)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = p.Close() })
	z := New(p, &MockAllocator{p: p})
	return p, z
}

func buildReadOnlyPrepareRootWithKeys(tb testing.TB, z *Zipper, count int) uint64 {
	tb.Helper()
	rootID, err := z.pager.Alloc(1)
	if err != nil {
		tb.Fatalf("alloc root: %v", err)
	}
	data, err := z.pager.Get(rootID)
	if err != nil {
		tb.Fatalf("get root: %v", err)
	}
	n := node.NewNode(data)
	n.SetPageID(rootID)
	n.SetType(page.PageTypeLeaf)
	n.UpdateChecksum()

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("val-%06d", i))
		if err := b.Set(key, val); err != nil {
			tb.Fatalf("Set seed: %v", err)
		}
	}
	newRootID, _, _, err := z.Apply(rootID, b)
	if err != nil {
		tb.Fatalf("build root apply: %v", err)
	}
	return newRootID
}

func buildReadOnlyPrepareWideBaseDeltaRoot(tb testing.TB, z *Zipper, children int) uint64 {
	tb.Helper()
	leafID, err := z.pager.Alloc(1)
	if err != nil {
		tb.Fatalf("alloc leaf: %v", err)
	}
	leafData, err := z.pager.Get(leafID)
	if err != nil {
		tb.Fatalf("get leaf: %v", err)
	}
	leaf := node.NewNode(leafData)
	leaf.SetPageID(leafID)
	leaf.SetType(page.PageTypeLeaf)
	leaf.UpdateChecksum()

	rootID, err := z.pager.Alloc(1)
	if err != nil {
		tb.Fatalf("alloc root: %v", err)
	}
	rootData, err := z.pager.Get(rootID)
	if err != nil {
		tb.Fatalf("get root: %v", err)
	}
	builder := node.NewBuilderWithOptions(rootData, page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: true})
	builder.SetPageID(rootID)
	for i := 0; i < children; i++ {
		key := []byte(fmt.Sprintf("child-%06d", i))
		if err := builder.AddInternalChild(key, leafID); err != nil {
			tb.Fatalf("AddInternalChild(%d): %v", i, err)
		}
	}
	root := builder.Finish()
	if root.Type() != page.PageTypeInternal || !root.InternalBaseDeltaEnabled() {
		tb.Fatalf("synthetic root type/base-delta=%v/%v want internal/base-delta", root.Type(), root.InternalBaseDeltaEnabled())
	}
	return rootID
}

func readOnlySpanSignature(prepared ReadOnlyPrepareResult) []string {
	out := make([]string, len(prepared.LeafSpans))
	for i, span := range prepared.LeafSpans {
		out[i] = fmt.Sprintf("%d:%d:%d:%d:%d:%x:%x:%x:%x:%d:%d",
			span.Ref.Kind, span.Ref.Page, span.OpCount, span.PointOpCount, span.DeleteRangeCount,
			span.LowKey, span.HighKey, span.FirstOpKey, span.LastOpKey,
			span.PointOpStart, span.DeleteRangeStart)
	}
	return out
}

func TestZipperPrepareReadOnlyEmptyBatchDoesNotTraverse(t *testing.T) {
	p, z := newReadOnlyPrepareZipper(t)
	rootID := buildOuterLeafInternalRoot(t, z)
	beforePages := p.PageCount()

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	prepared, err := z.PrepareReadOnly(rootID, b, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if prepared.RootID != rootID || prepared.Ops != 0 || !prepared.ExactLeafSpans {
		t.Fatalf("prepared root/ops/exact=%d/%d/%v want %d/0/true", prepared.RootID, prepared.Ops, prepared.ExactLeafSpans, rootID)
	}
	if len(prepared.LeafSpans) != 0 {
		t.Fatalf("empty batch spans=%d want 0", len(prepared.LeafSpans))
	}
	if prepared.Metrics.ZipperNodeLoads != 0 {
		t.Fatalf("empty batch traversed tree metrics=%+v", prepared.Metrics)
	}
	if got := p.PageCount(); got != beforePages {
		t.Fatalf("page count changed during empty read-only prepare: got %d want %d", got, beforePages)
	}
}

func TestZipperPrepareReadOnlyLeafLogRefDoesNotReadLeafBody(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	store := newBatchMemoryLeafPageStore()
	z.SetOuterLeavesInValueLog(true)
	z.SetLeafPageLog(store)
	z.SetLeafPageReader(store)

	rootID := buildOuterLeafInternalRoot(t, z)
	if store.readCalls != 0 {
		t.Fatalf("seed apply leaf-log reads=%d want 0", store.readCalls)
	}

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	if err := delta.Set([]byte("key-099"), []byte("new")); err != nil {
		t.Fatalf("Set delta: %v", err)
	}

	prepared, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if len(prepared.LeafSpans) == 0 {
		t.Fatalf("expected leaf-log span")
	}
	for i, span := range prepared.LeafSpans {
		if span.Ref.Kind != page.ChildRefLeafLog {
			t.Fatalf("span %d ref kind=%d want leaf-log", i, span.Ref.Kind)
		}
	}
	if store.readCalls != 0 {
		t.Fatalf("PrepareReadOnly leaf-log reads=%d want 0", store.readCalls)
	}
}

func TestReadOnlyPrepareResultResetForReuseKeepsBoundedBuffers(t *testing.T) {
	bounded := ReadOnlyPrepareResult{
		LeafSpans: make([]ReadOnlyLeafSpan, 3, readOnlyPrepareResultReuseLeafSpanKeepCap),
		keyArena:  make([]byte, 7, readOnlyPrepareResultReuseKeyArenaKeepCap),
	}
	bounded.ResetForReuse()
	if len(bounded.LeafSpans) != 0 || cap(bounded.LeafSpans) != readOnlyPrepareResultReuseLeafSpanKeepCap {
		t.Fatalf("bounded leaf spans len/cap=%d/%d", len(bounded.LeafSpans), cap(bounded.LeafSpans))
	}
	if len(bounded.keyArena) != 0 || cap(bounded.keyArena) != readOnlyPrepareResultReuseKeyArenaKeepCap {
		t.Fatalf("bounded key arena len/cap=%d/%d", len(bounded.keyArena), cap(bounded.keyArena))
	}

	oversized := ReadOnlyPrepareResult{
		LeafSpans: make([]ReadOnlyLeafSpan, 1, readOnlyPrepareResultReuseLeafSpanKeepCap+1),
		keyArena:  make([]byte, 1, readOnlyPrepareResultReuseKeyArenaKeepCap+1),
	}
	oversized.ResetForReuse()
	if oversized.LeafSpans != nil || oversized.keyArena != nil {
		t.Fatalf("oversized buffers retained: leaf cap=%d key cap=%d", cap(oversized.LeafSpans), cap(oversized.keyArena))
	}

	omitKeys := ReadOnlyPrepareResult{
		OmitKeys:  true,
		LeafSpans: make([]ReadOnlyLeafSpan, 1, readOnlyPrepareResultReuseLeafSpanKeepCap+1),
		keyArena:  make([]byte, 1, 32),
	}
	omitKeys.ResetForReuse()
	if !omitKeys.OmitKeys {
		t.Fatalf("omit-keys reset did not preserve OmitKeys")
	}
	if len(omitKeys.LeafSpans) != 0 || cap(omitKeys.LeafSpans) != readOnlyPrepareResultReuseLeafSpanKeepCap+1 {
		t.Fatalf("omit-keys reset leaf spans len/cap=%d/%d", len(omitKeys.LeafSpans), cap(omitKeys.LeafSpans))
	}
	if omitKeys.keyArena != nil {
		t.Fatalf("omit-keys reset retained key arena cap=%d", cap(omitKeys.keyArena))
	}
	if opts := omitKeys.ReuseOptions(); !opts.OmitKeys || cap(opts.leafSpans) != readOnlyPrepareResultReuseLeafSpanKeepCap+1 {
		t.Fatalf("omit-keys reuse options OmitKeys=%v leaf cap=%d", opts.OmitKeys, cap(opts.leafSpans))
	}
}

func TestReadOnlyPrepareResultResetForReuseClearsLeafSpanReferences(t *testing.T) {
	arena := []byte("oversized-key-arena")
	spans := make([]ReadOnlyLeafSpan, 1, 4)
	spans[0].LowKey = arena[:4]
	spans = spans[:cap(spans)]
	spans[3].HighKey = arena[4:]
	spans = spans[:1]

	r := ReadOnlyPrepareResult{
		LeafSpans: spans,
		keyArena:  make([]byte, 1, readOnlyPrepareResultReuseKeyArenaKeepCap+1),
	}
	r.ResetForReuse()
	if len(r.LeafSpans) != 0 || cap(r.LeafSpans) != cap(spans) {
		t.Fatalf("leaf spans len/cap after reset=%d/%d", len(r.LeafSpans), cap(r.LeafSpans))
	}
	cleared := r.LeafSpans[:cap(r.LeafSpans)]
	for i, span := range cleared {
		if span.LowKey != nil || span.HighKey != nil || span.FirstOpKey != nil || span.LastOpKey != nil {
			t.Fatalf("span %d retained key refs after reset: %+v", i, span)
		}
	}
	if r.keyArena != nil {
		t.Fatalf("oversized key arena retained after reset: cap=%d", cap(r.keyArena))
	}
}

func TestReadOnlyPrepareResultReuseOptionsClearsFullLeafSpanCapacity(t *testing.T) {
	arena := []byte("previous-key-arena")
	spans := make([]ReadOnlyLeafSpan, 1, 3)
	spans[0].FirstOpKey = arena[:4]
	spans = spans[:cap(spans)]
	spans[2].LastOpKey = arena[4:]
	spans = spans[:1]

	opts := (ReadOnlyPrepareResult{LeafSpans: spans}).ReuseOptions()
	if len(opts.leafSpans) != 0 || cap(opts.leafSpans) != cap(spans) {
		t.Fatalf("reuse leaf spans len/cap=%d/%d", len(opts.leafSpans), cap(opts.leafSpans))
	}
	cleared := opts.leafSpans[:cap(opts.leafSpans)]
	for i, span := range cleared {
		if span.LowKey != nil || span.HighKey != nil || span.FirstOpKey != nil || span.LastOpKey != nil {
			t.Fatalf("span %d retained key refs after reuse options: %+v", i, span)
		}
	}
}

func TestReadOnlyPrepareResultEnsureInitialCapacityPresizesBoundedBuffers(t *testing.T) {
	ops := make([]batch.Entry, 128)
	var r ReadOnlyPrepareResult
	r.ensureInitialCapacity(ops, nil)
	if cap(r.LeafSpans) != len(ops) {
		t.Fatalf("leaf span cap=%d want %d", cap(r.LeafSpans), len(ops))
	}
	if cap(r.keyArena) != len(ops)*32 {
		t.Fatalf("key arena cap=%d want %d", cap(r.keyArena), len(ops)*32)
	}

	r = ReadOnlyPrepareResult{OmitKeys: true}
	r.ensureInitialCapacity(ops, nil)
	if cap(r.LeafSpans) != len(ops) {
		t.Fatalf("omit-keys leaf span cap=%d want %d", cap(r.LeafSpans), len(ops))
	}
	if cap(r.keyArena) != 0 {
		t.Fatalf("omit-keys key arena cap=%d want 0", cap(r.keyArena))
	}

	ops = make([]batch.Entry, readOnlyPrepareResultReuseLeafSpanKeepCap+100)
	r = ReadOnlyPrepareResult{}
	r.ensureInitialCapacity(ops, nil)
	if cap(r.LeafSpans) != readOnlyPrepareResultReuseLeafSpanKeepCap {
		t.Fatalf("capped leaf span cap=%d want %d", cap(r.LeafSpans), readOnlyPrepareResultReuseLeafSpanKeepCap)
	}
}

func TestZipperPrepareReadOnlyColdAndSingleLeafPlans(t *testing.T) {
	cold := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = cold.Close() }()
	_ = cold.Set([]byte("a"), []byte("1"))
	_ = cold.Set([]byte("z"), []byte("2"))
	var nilZ Zipper
	prepared, err := nilZ.PrepareReadOnly(0, cold, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("cold PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if !prepared.ColdBuild || !prepared.ExactLeafSpans || prepared.Maintenance || len(prepared.LeafSpans) != 1 {
		t.Fatalf("cold flags/spans = cold:%v exact:%v maintenance:%v spans:%d", prepared.ColdBuild, prepared.ExactLeafSpans, prepared.Maintenance, len(prepared.LeafSpans))
	}

	_, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 8)
	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	_ = delta.Set([]byte("key-000003"), []byte("old"))
	_ = delta.Set([]byte("key-000003"), []byte("new"))
	prepared, err = z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("single leaf PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if prepared.Ops != 1 || prepared.PointOps != 1 || prepared.DeleteRanges != 0 || len(prepared.LeafSpans) != 1 {
		t.Fatalf("single leaf counts ops/point/range/spans=%d/%d/%d/%d want 1/1/0/1", prepared.Ops, prepared.PointOps, prepared.DeleteRanges, len(prepared.LeafSpans))
	}
	span := prepared.LeafSpans[0]
	if span.OpCount != 1 || span.PointOpCount != 1 || string(span.FirstOpKey) != "key-000003" || string(span.LastOpKey) != "key-000003" {
		t.Fatalf("single leaf span=%+v want compacted duplicate key", span)
	}
}

func TestZipperPrepareReadOnlyManyLeafSplitBoundaryDeterministic(t *testing.T) {
	p, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 768)
	beforePages := p.PageCount()

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for _, idx := range []int{0, 127, 128, 255, 256, 511, 767} {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", idx)), []byte("new")); err != nil {
			t.Fatalf("Set delta: %v", err)
		}
	}

	first, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly first: %v", err)
	}
	requireValidReadOnlyPrepare(t, first)
	if got := p.PageCount(); got != beforePages {
		t.Fatalf("page count changed during read-only prepare: got %d want %d", got, beforePages)
	}
	if first.Maintenance || !first.ExactLeafSpans || len(first.LeafSpans) < 2 {
		t.Fatalf("many leaf flags/spans maintenance=%v exact=%v spans=%d", first.Maintenance, first.ExactLeafSpans, len(first.LeafSpans))
	}
	direct, err := z.PrepareReadOnlyPlan(rootID, delta.SortedEntries(), nil, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnlyPlan: %v", err)
	}
	requireValidReadOnlyPrepare(t, direct)
	wantSig := readOnlySpanSignature(first)
	if gotSig := readOnlySpanSignature(direct); !reflect.DeepEqual(gotSig, wantSig) {
		t.Fatalf("direct prepare signature mismatch\n got=%v\nwant=%v", gotSig, wantSig)
	}
	for i := 0; i < 5; i++ {
		prepared, err := z.PrepareReadOnly(rootID, delta, first.ReuseOptions())
		if err != nil {
			t.Fatalf("PrepareReadOnly repeat %d: %v", i, err)
		}
		requireValidReadOnlyPrepare(t, prepared)
		if gotSig := readOnlySpanSignature(prepared); !reflect.DeepEqual(gotSig, wantSig) {
			t.Fatalf("repeat %d signature mismatch\n got=%v\nwant=%v", i, gotSig, wantSig)
		}
	}

	applied, err := z.ApplyWithOptions(rootID, delta, ApplyOptions{PrepareReadOnly: true})
	if err != nil {
		t.Fatalf("ApplyWithOptions: %v", err)
	}
	requireValidReadOnlyPrepare(t, applied.ReadOnlyPrepare)
	if gotSig := readOnlySpanSignature(applied.ReadOnlyPrepare); !reflect.DeepEqual(gotSig, wantSig) {
		t.Fatalf("ApplyWithOptions prepare signature mismatch\n got=%v\nwant=%v", gotSig, wantSig)
	}
	if applied.RootID == 0 || applied.RootID == rootID || len(applied.PendingRetiredPages) == 0 {
		t.Fatalf("ApplyWithOptions root/retired=%d/%d want changed root and retired pages", applied.RootID, len(applied.PendingRetiredPages))
	}
}

func TestZipperPrepareReadOnlyOmitKeysSkipsSpanKeyCopies(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 768)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for _, idx := range []int{0, 127, 128, 255, 256, 511, 767} {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", idx)), []byte("new")); err != nil {
			t.Fatalf("Set delta: %v", err)
		}
	}

	prepared, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{OmitKeys: true})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if !prepared.OmitKeys {
		t.Fatalf("prepared OmitKeys=%v want true", prepared.OmitKeys)
	}
	if len(prepared.LeafSpans) < 2 {
		t.Fatalf("omit-keys prepare spans=%d want multiple", len(prepared.LeafSpans))
	}
	if len(prepared.keyArena) != 0 || cap(prepared.keyArena) != 0 {
		t.Fatalf("omit-keys key arena len/cap=%d/%d want 0/0", len(prepared.keyArena), cap(prepared.keyArena))
	}
	pointEnd := 0
	for i, span := range prepared.LeafSpans {
		if span.LowKey != nil || span.HighKey != nil || span.FirstOpKey != nil || span.LastOpKey != nil {
			t.Fatalf("span %d retained key bytes under OmitKeys: %+v", i, span)
		}
		if span.PointOpCount <= 0 || span.DeleteRangeCount != 0 {
			t.Fatalf("span %d counts=%d/%d want point-only", i, span.PointOpCount, span.DeleteRangeCount)
		}
		if span.PointOpStart != pointEnd || span.PointOpEnd <= span.PointOpStart {
			t.Fatalf("span %d point range=[%d,%d) after end %d", i, span.PointOpStart, span.PointOpEnd, pointEnd)
		}
		pointEnd = span.PointOpEnd
	}
	if pointEnd != prepared.PointOps {
		t.Fatalf("omit-keys spans cover %d point ops, want %d", pointEnd, prepared.PointOps)
	}

	reused, err := z.PrepareReadOnly(rootID, delta, prepared.ReuseOptions())
	if err != nil {
		t.Fatalf("PrepareReadOnly with reuse options: %v", err)
	}
	requireValidReadOnlyPrepare(t, reused)
	if !reused.OmitKeys || len(reused.keyArena) != 0 || cap(reused.keyArena) != 0 {
		t.Fatalf("reused omit-keys result OmitKeys=%v keyArena len/cap=%d/%d", reused.OmitKeys, len(reused.keyArena), cap(reused.keyArena))
	}
}

func TestZipperPrepareReadOnlyPointOnlyDefersUntouchedBaseDeltaSeparatorClones(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareWideBaseDeltaRoot(t, z, 384)

	const touched = 250
	key := []byte(fmt.Sprintf("child-%06d", touched))
	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	if err := delta.Set(key, []byte("new")); err != nil {
		t.Fatalf("Set delta: %v", err)
	}

	prepared, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if len(prepared.LeafSpans) != 1 {
		t.Fatalf("leaf spans=%d want 1", len(prepared.LeafSpans))
	}

	retainedKeyBytes := readOnlyPrepareSpanBoundaryBytes(prepared.LeafSpans) + readOnlyPrepareSpanOpKeyBytes(prepared.LeafSpans)
	separatorKeyBytes := len([]byte(fmt.Sprintf("child-%06d", touched))) + len([]byte(fmt.Sprintf("child-%06d", touched+1)))
	if got, max := len(prepared.keyArena), retainedKeyBytes+separatorKeyBytes; got > max {
		t.Fatalf("key arena bytes=%d, retained=%d separator temp=%d; likely cloned untouched separators", got, retainedKeyBytes, separatorKeyBytes)
	}
}

func TestZipperPrepareReadOnlyOmitOpKeysKeepsSpanBoundaries(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 768)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for _, idx := range []int{0, 127, 128, 255, 256, 511, 767} {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", idx)), []byte("new")); err != nil {
			t.Fatalf("Set delta: %v", err)
		}
	}

	full, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly full: %v", err)
	}
	requireValidReadOnlyPrepare(t, full)

	prepared, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{OmitOpKeys: true})
	if err != nil {
		t.Fatalf("PrepareReadOnly omit op keys: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if prepared.OmitKeys {
		t.Fatalf("prepared OmitKeys=%v want false", prepared.OmitKeys)
	}
	if !prepared.OmitOpKeys {
		t.Fatalf("prepared OmitOpKeys=%v want true", prepared.OmitOpKeys)
	}
	if len(prepared.LeafSpans) != len(full.LeafSpans) {
		t.Fatalf("leaf spans=%d want %d", len(prepared.LeafSpans), len(full.LeafSpans))
	}
	var boundaryBytes int
	pointEnd := 0
	for i, span := range prepared.LeafSpans {
		want := full.LeafSpans[i]
		if !bytes.Equal(span.LowKey, want.LowKey) || !bytes.Equal(span.HighKey, want.HighKey) {
			t.Fatalf("span %d boundaries mismatch got low/high=%q/%q want %q/%q", i, span.LowKey, span.HighKey, want.LowKey, want.HighKey)
		}
		boundaryBytes += len(span.LowKey) + len(span.HighKey)
		if span.FirstOpKey != nil || span.LastOpKey != nil {
			t.Fatalf("span %d retained op keys under OmitOpKeys: first=%q last=%q", i, span.FirstOpKey, span.LastOpKey)
		}
		if span.PointOpCount <= 0 || span.DeleteRangeCount != 0 {
			t.Fatalf("span %d counts=%d/%d want point-only", i, span.PointOpCount, span.DeleteRangeCount)
		}
		if span.PointOpStart != pointEnd || span.PointOpEnd <= span.PointOpStart {
			t.Fatalf("span %d point range=[%d,%d) after end %d", i, span.PointOpStart, span.PointOpEnd, pointEnd)
		}
		pointEnd = span.PointOpEnd
	}
	if boundaryBytes == 0 || len(prepared.keyArena) == 0 {
		t.Fatalf("omit-op-keys retained boundary bytes=%d keyArena=%d, want exact boundaries", boundaryBytes, len(prepared.keyArena))
	}
	if pointEnd != prepared.PointOps {
		t.Fatalf("omit-op-keys spans cover %d point ops, want %d", pointEnd, prepared.PointOps)
	}

	reused, err := z.PrepareReadOnly(rootID, delta, prepared.ReuseOptions())
	if err != nil {
		t.Fatalf("PrepareReadOnly with reuse options: %v", err)
	}
	requireValidReadOnlyPrepare(t, reused)
	if reused.OmitKeys || !reused.OmitOpKeys {
		t.Fatalf("reused omit-op-keys result OmitKeys/OmitOpKeys=%v/%v", reused.OmitKeys, reused.OmitOpKeys)
	}
	for i, span := range reused.LeafSpans {
		if span.FirstOpKey != nil || span.LastOpKey != nil {
			t.Fatalf("reused span %d retained op keys under OmitOpKeys", i)
		}
	}
}

func TestZipperPrepareReadOnlyPlanCallbackCanDiscardSpans(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 256)
	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for _, idx := range []int{0, 128, 255} {
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", idx)), []byte("new")); err != nil {
			t.Fatalf("Set delta: %v", err)
		}
	}
	callbacks := 0
	spanOps := 0
	prepared, err := z.PrepareReadOnlyPlan(rootID, delta.SortedEntries(), nil, ReadOnlyPrepareOptions{
		OmitKeys:         true,
		DiscardLeafSpans: true,
		LeafSpanCallback: func(span ReadOnlyLeafSpan) {
			callbacks++
			spanOps += span.OpCount
			if span.FirstOpKey != nil || span.LastOpKey != nil || span.LowKey != nil || span.HighKey != nil {
				t.Fatalf("discard callback received copied keys: %+v", span)
			}
		},
	})
	if err != nil {
		t.Fatalf("PrepareReadOnlyPlan: %v", err)
	}
	if !prepared.OmitKeys || len(prepared.LeafSpans) != 0 || cap(prepared.LeafSpans) != 0 {
		t.Fatalf("discard result OmitKeys=%v spans len/cap=%d/%d", prepared.OmitKeys, len(prepared.LeafSpans), cap(prepared.LeafSpans))
	}
	if callbacks == 0 || spanOps != prepared.PointOps {
		t.Fatalf("callback spans=%d ops=%d want point ops %d", callbacks, spanOps, prepared.PointOps)
	}
}

func TestZipperPrepareReadOnlyAllowsEmptyPointKey(t *testing.T) {
	cold := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = cold.Close() }()
	if err := cold.Set([]byte{}, []byte("empty")); err != nil {
		t.Fatalf("cold Set empty key: %v", err)
	}
	var nilZ Zipper
	prepared, err := nilZ.PrepareReadOnly(0, cold, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("cold PrepareReadOnly empty key: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if len(prepared.LeafSpans) != 1 || prepared.LeafSpans[0].FirstOpKey == nil || len(prepared.LeafSpans[0].FirstOpKey) != 0 {
		t.Fatalf("cold empty-key span=%+v", prepared.LeafSpans)
	}

	_, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 16)
	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	if err := delta.Set([]byte{}, []byte("empty")); err != nil {
		t.Fatalf("warm Set empty key: %v", err)
	}
	prepared, err = z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("warm PrepareReadOnly empty key: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if len(prepared.LeafSpans) != 1 || prepared.LeafSpans[0].FirstOpKey == nil || len(prepared.LeafSpans[0].FirstOpKey) != 0 {
		t.Fatalf("warm empty-key span=%+v", prepared.LeafSpans)
	}
}

func TestZipperPrepareReadOnlyDeleteRangeSpans(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 512)
	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	if err := delta.DeleteRange([]byte("key-000050"), []byte("key-000220")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	prepared, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if prepared.PointOps != 0 || prepared.DeleteRanges != 1 || prepared.Ops != 1 || !prepared.ExactLeafSpans || prepared.Maintenance {
		t.Fatalf("range prepare counts/flags point=%d ranges=%d ops=%d exact=%v maintenance=%v", prepared.PointOps, prepared.DeleteRanges, prepared.Ops, prepared.ExactLeafSpans, prepared.Maintenance)
	}
	if len(prepared.LeafSpans) == 0 {
		t.Fatal("delete range produced no touched leaf spans")
	}
	for _, span := range prepared.LeafSpans {
		if span.PointOpCount != 0 || span.DeleteRangeCount != 1 || span.DeleteRangeEnd-span.DeleteRangeStart != 1 {
			t.Fatalf("unexpected range span: %+v", span)
		}
		if len(span.FirstOpKey) != 0 || len(span.LastOpKey) != 0 {
			t.Fatalf("range-only span has point keys: %+v", span)
		}
	}
	summary := prepared.LeafSpanSummary()
	if summary.SpanOps != len(prepared.LeafSpans) || summary.SpanBytes == 0 {
		t.Fatalf("range summary spanOps/spanBytes=%d/%d want one op per span and bytes", summary.SpanOps, summary.SpanBytes)
	}

	withOmitKeys, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{OmitKeys: true})
	if err != nil {
		t.Fatalf("PrepareReadOnly with OmitKeys: %v", err)
	}
	requireValidReadOnlyPrepare(t, withOmitKeys)
	if withOmitKeys.OmitKeys {
		t.Fatalf("delete-range prepare kept OmitKeys=true; range planning needs span bounds")
	}
	if got, want := readOnlySpanSignature(withOmitKeys), readOnlySpanSignature(prepared); !reflect.DeepEqual(got, want) {
		t.Fatalf("delete-range omit-keys signature mismatch\n got=%v\nwant=%v", got, want)
	}
}

func TestZipperPrepareReadOnlyInternalBaseDeltaRangeSpans(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	z.SetIndexInternalBaseDelta(true)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 2048)
	rootData, err := z.pager.Get(rootID)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	rootNode := node.NewNode(rootData)
	if rootNode.Type() != page.PageTypeInternal || !rootNode.InternalBaseDeltaEnabled() {
		t.Fatalf("root type/base-delta=%v/%v want internal base-delta", rootNode.Type(), rootNode.InternalBaseDeltaEnabled())
	}

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	if err := delta.DeleteRange([]byte("key-000500"), []byte("key-000700")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	prepared, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if len(prepared.LeafSpans) == 0 {
		t.Fatal("base-delta range prepare produced no touched spans")
	}
	for _, span := range prepared.LeafSpans {
		if span.DeleteRangeCount != 1 {
			t.Fatalf("base-delta span missing delete range: %+v", span)
		}
	}
}

func TestZipperPrepareReadOnlyDoesNotAppendOuterLeafOutput(t *testing.T) {
	p, z := newReadOnlyPrepareZipper(t)
	z.SetOuterLeavesInValueLog(true)
	store := newBatchMemoryLeafPageStore()
	z.SetLeafPageLog(store)
	z.SetLeafPageReader(store)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 256)
	beforePages := p.PageCount()
	beforeSingle := store.singleCalls
	beforeBatches := len(store.batchLens)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	_ = delta.Set([]byte("key-000001"), []byte("new"))
	_ = delta.Set([]byte("key-000200"), []byte("new"))
	prepared, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if got := p.PageCount(); got != beforePages {
		t.Fatalf("read-only prepare allocated pages: got page count %d want %d", got, beforePages)
	}
	if store.singleCalls != beforeSingle || len(store.batchLens) != beforeBatches {
		t.Fatalf("read-only prepare appended leaf-log output: single %d->%d batches %d->%d", beforeSingle, store.singleCalls, beforeBatches, len(store.batchLens))
	}
	if prepared.Metrics.IndexWriteBytes != 0 || prepared.Metrics.ZipperLeafPagesWritten != 0 || prepared.Metrics.ZipperInternalPagesWritten != 0 {
		t.Fatalf("read-only prepare wrote output metrics=%+v", prepared.Metrics)
	}
}

func TestReadOnlyPrepareResultValidateLeafSpansContracts(t *testing.T) {
	validSpan := ReadOnlyLeafSpan{LowKey: []byte("a"), HighKey: []byte("z"), FirstOpKey: []byte("b"), LastOpKey: []byte("c"), OpCount: 2}
	tests := []struct {
		name string
		in   ReadOnlyPrepareResult
	}{
		{name: "zero ops with span", in: ReadOnlyPrepareResult{Ops: 0, LeafSpans: []ReadOnlyLeafSpan{validSpan}}},
		{name: "missing spans", in: ReadOnlyPrepareResult{Ops: 1}},
		{name: "cold build multiple spans", in: ReadOnlyPrepareResult{Ops: 2, ColdBuild: true, LeafSpans: []ReadOnlyLeafSpan{{FirstOpKey: []byte("a"), LastOpKey: []byte("a"), OpCount: 1}, {FirstOpKey: []byte("b"), LastOpKey: []byte("b"), OpCount: 1}}}},
		{name: "empty op key", in: ReadOnlyPrepareResult{Ops: 1, LeafSpans: []ReadOnlyLeafSpan{{LastOpKey: []byte("b"), OpCount: 1}}}},
		{name: "reversed op keys", in: ReadOnlyPrepareResult{Ops: 1, LeafSpans: []ReadOnlyLeafSpan{{FirstOpKey: []byte("c"), LastOpKey: []byte("b"), OpCount: 1}}}},
		{name: "duplicate boundary key", in: ReadOnlyPrepareResult{Ops: 2, LeafSpans: []ReadOnlyLeafSpan{{FirstOpKey: []byte("b"), LastOpKey: []byte("d"), OpCount: 1}, {FirstOpKey: []byte("d"), LastOpKey: []byte("e"), OpCount: 1}}}},
		{name: "bad bounds", in: ReadOnlyPrepareResult{Ops: 1, LeafSpans: []ReadOnlyLeafSpan{{LowKey: []byte("z"), HighKey: []byte("a"), FirstOpKey: []byte("m"), LastOpKey: []byte("m"), OpCount: 1}}}},
		{name: "out of order bounds", in: ReadOnlyPrepareResult{Ops: 2, LeafSpans: []ReadOnlyLeafSpan{{HighKey: []byte("m"), FirstOpKey: []byte("a"), LastOpKey: []byte("b"), OpCount: 1}, {LowKey: []byte("c"), FirstOpKey: []byte("d"), LastOpKey: []byte("d"), OpCount: 1}}}},
		{name: "op before low", in: ReadOnlyPrepareResult{Ops: 1, LeafSpans: []ReadOnlyLeafSpan{{LowKey: []byte("c"), FirstOpKey: []byte("b"), LastOpKey: []byte("b"), OpCount: 1}}}},
		{name: "op at high", in: ReadOnlyPrepareResult{Ops: 1, LeafSpans: []ReadOnlyLeafSpan{{HighKey: []byte("b"), FirstOpKey: []byte("b"), LastOpKey: []byte("b"), OpCount: 1}}}},
		{name: "omit keys empty point range", in: ReadOnlyPrepareResult{Ops: 1, PointOps: 1, OmitKeys: true, LeafSpans: []ReadOnlyLeafSpan{{OpCount: 1, PointOpCount: 1, PointOpStart: 0, PointOpEnd: 0}}}},
		{name: "omit keys point range mismatch", in: ReadOnlyPrepareResult{Ops: 1, PointOps: 1, OmitKeys: true, LeafSpans: []ReadOnlyLeafSpan{{OpCount: 1, PointOpCount: 1, PointOpStart: 0, PointOpEnd: 2}}}},
		{name: "omit keys overlapping point ranges", in: ReadOnlyPrepareResult{Ops: 2, PointOps: 2, OmitKeys: true, LeafSpans: []ReadOnlyLeafSpan{{OpCount: 1, PointOpCount: 1, PointOpStart: 0, PointOpEnd: 1}, {OpCount: 1, PointOpCount: 1, PointOpStart: 0, PointOpEnd: 1}}}},
		{name: "omit keys gapped point ranges", in: ReadOnlyPrepareResult{Ops: 2, PointOps: 2, OmitKeys: true, LeafSpans: []ReadOnlyLeafSpan{{OpCount: 1, PointOpCount: 1, PointOpStart: 0, PointOpEnd: 1}, {OpCount: 1, PointOpCount: 1, PointOpStart: 2, PointOpEnd: 3}}}},
		{name: "range index mismatch", in: ReadOnlyPrepareResult{Ops: 1, DeleteRanges: 1, LeafSpans: []ReadOnlyLeafSpan{{OpCount: 1, DeleteRangeCount: 1, DeleteRangeStart: 2, DeleteRangeEnd: 2}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.in.ValidateLeafSpans(); err == nil {
				t.Fatal("ValidateLeafSpans returned nil, want error")
			}
		})
	}

	longKey := []byte("0123456789abcdef")
	err := (ReadOnlyPrepareResult{Ops: 1, LeafSpans: []ReadOnlyLeafSpan{{FirstOpKey: longKey, LastOpKey: []byte("0"), OpCount: 1}}}).ValidateLeafSpans()
	if err == nil {
		t.Fatal("ValidateLeafSpans long-key case returned nil")
	}
	msg := err.Error()
	if strings.Contains(msg, string(longKey)) || !strings.Contains(msg, "len=16") || !strings.Contains(msg, "hex_prefix=3031323334353637") {
		t.Fatalf("unsafe or incomplete key formatting: %s", msg)
	}
}

func TestReadOnlyPrepareResultValidateAllowsEmptyPointKey(t *testing.T) {
	prepared := ReadOnlyPrepareResult{Ops: 1, PointOps: 1, ExactLeafSpans: true, LeafSpans: []ReadOnlyLeafSpan{{FirstOpKey: []byte{}, LastOpKey: []byte{}, OpCount: 1, PointOpCount: 1, PointOpStart: 0, PointOpEnd: 1, ByteCount: 1}}}
	requireValidReadOnlyPrepare(t, prepared)
}

func TestReadOnlyPrepareResultWorkerRanges(t *testing.T) {
	prepared := ReadOnlyPrepareResult{Ops: 4, PointOps: 4, ExactLeafSpans: true, LeafSpans: []ReadOnlyLeafSpan{
		{HighKey: []byte("b"), FirstOpKey: []byte("a"), LastOpKey: []byte("a"), OpCount: 1, PointOpCount: 1, PointOpStart: 0, PointOpEnd: 1, ByteCount: 2},
		{LowKey: []byte("b"), HighKey: []byte("d"), FirstOpKey: []byte("b"), LastOpKey: []byte("c"), OpCount: 2, PointOpCount: 2, PointOpStart: 1, PointOpEnd: 3, ByteCount: 4},
		{LowKey: []byte("d"), FirstOpKey: []byte("d"), LastOpKey: []byte("d"), OpCount: 1, PointOpCount: 1, PointOpStart: 3, PointOpEnd: 4, ByteCount: 2},
	}}
	requireValidReadOnlyPrepare(t, prepared)
	ranges := prepared.AppendLeafSpanWorkerRanges(nil, 2)
	if len(ranges) != 2 || ranges[0].FirstSpan != 0 || ranges[1].FirstSpan+ranges[1].SpanCount != len(prepared.LeafSpans) {
		t.Fatalf("worker ranges do not cover spans: %+v", ranges)
	}
	ops, bytesTotal := 0, 0
	for _, r := range ranges {
		if r.SpanCount <= 0 {
			t.Fatalf("empty worker range: %+v", r)
		}
		ops += r.Ops
		bytesTotal += r.Bytes
	}
	if ops != prepared.LeafSpanSummary().SpanOps || bytesTotal != prepared.LeafSpanSummary().SpanBytes {
		t.Fatalf("ranges cover ops/bytes=%d/%d want %d/%d", ops, bytesTotal, prepared.LeafSpanSummary().SpanOps, prepared.LeafSpanSummary().SpanBytes)
	}
	summary := prepared.LeafSpanWorkerRangeSummary(8)
	if summary.TargetWorkers != 8 || summary.Ranges != len(prepared.LeafSpans) || summary.MaxRangeOps == 0 || summary.Bytes != prepared.LeafSpanSummary().SpanBytes {
		t.Fatalf("worker summary=%+v", summary)
	}
}

func TestReadOnlyPrepareResultWorkUnitRangesOneRangePerWorker(t *testing.T) {
	const spans = 64
	prepared := ReadOnlyPrepareResult{Ops: spans, PointOps: spans, ExactLeafSpans: true}
	for i := 0; i < spans; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		span := ReadOnlyLeafSpan{
			FirstOpKey:   key,
			LastOpKey:    key,
			OpCount:      1,
			PointOpCount: 1,
			PointOpStart: i,
			PointOpEnd:   i + 1,
			ByteCount:    100,
		}
		if i > 0 {
			span.LowKey = key
		}
		if i+1 < spans {
			span.HighKey = []byte(fmt.Sprintf("k%03d", i+1))
		}
		prepared.LeafSpans = append(prepared.LeafSpans, span)
	}
	requireValidReadOnlyPrepare(t, prepared)

	ranges := prepared.AppendLeafSpanWorkUnitRanges(nil, 4)
	if len(ranges) != 4 {
		t.Fatalf("work-unit ranges=%d want one range per active worker: %+v", len(ranges), ranges)
	}
	if ranges[0].SpanCount <= 1 {
		t.Fatalf("first work unit spans=%d want grouped adjacent spans", ranges[0].SpanCount)
	}
	totalSpans, totalOps, totalBytes := 0, 0, 0
	for i, r := range ranges {
		if r.SpanCount <= 0 {
			t.Fatalf("range %d empty: %+v", i, r)
		}
		if i > 0 && r.FirstSpan != ranges[i-1].FirstSpan+ranges[i-1].SpanCount {
			t.Fatalf("range %d starts at %d after prior %+v", i, r.FirstSpan, ranges[i-1])
		}
		totalSpans += r.SpanCount
		totalOps += r.Ops
		totalBytes += r.Bytes
	}
	if totalSpans != spans || totalOps != spans || totalBytes != spans*100 {
		t.Fatalf("work units cover spans/ops/bytes=%d/%d/%d want %d/%d/%d", totalSpans, totalOps, totalBytes, spans, spans, spans*100)
	}

	for _, workers := range []int{8, 16} {
		workerRanges := prepared.AppendLeafSpanWorkUnitRanges(nil, workers)
		if len(workerRanges) != workers {
			t.Fatalf("workers=%d emitted %d ranges, want one range per active worker", workers, len(workerRanges))
		}
	}

	serialRanges := prepared.AppendLeafSpanWorkUnitRanges(nil, 1)
	if len(serialRanges) != 1 || serialRanges[0].SpanCount != spans {
		t.Fatalf("single-worker work units=%+v want one range covering all spans", serialRanges)
	}

	tooManyWorkerRanges := prepared.AppendLeafSpanWorkUnitRanges(nil, spans+10)
	if len(tooManyWorkerRanges) != spans {
		t.Fatalf("over-subscribed workers emitted %d ranges, want clamp to %d non-empty spans", len(tooManyWorkerRanges), spans)
	}

	emptyRanges := (ReadOnlyPrepareResult{}).AppendLeafSpanWorkUnitRanges(nil, 8)
	if len(emptyRanges) != 0 {
		t.Fatalf("empty prepare emitted ranges: %+v", emptyRanges)
	}
}

func TestZipperPrepareReadOnlyRandomizedPartition(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, 1024)
	rng := rand.New(rand.NewSource(2745))
	for iter := 0; iter < 50; iter++ {
		delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
		for j := 0; j < 64; j++ {
			idx := rng.Intn(1024)
			key := []byte(fmt.Sprintf("key-%06d", idx))
			switch rng.Intn(5) {
			case 0:
				_ = delta.Delete(key)
			case 1:
				end := idx + 1 + rng.Intn(32)
				if end > 1024 {
					end = 1024
				}
				_ = delta.DeleteRange(key, []byte(fmt.Sprintf("key-%06d", end)))
			default:
				_ = delta.Set(key, []byte("v"))
			}
		}
		prepared, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
		_ = delta.Close()
		if err != nil {
			t.Fatalf("iter %d PrepareReadOnly: %v", iter, err)
		}
		requireValidReadOnlyPrepare(t, prepared)
		for i := 1; i < len(prepared.LeafSpans); i++ {
			prev := prepared.LeafSpans[i-1]
			cur := prepared.LeafSpans[i]
			if prev.HighKey != nil && cur.LowKey != nil && bytes.Compare(cur.LowKey, prev.HighKey) < 0 {
				t.Fatalf("iter %d overlapping spans %d/%d: prev=%+v cur=%+v", iter, i-1, i, prev, cur)
			}
		}
	}
}

func BenchmarkZipperPrepareReadOnlyOneLeafRandom(b *testing.B) {
	benchmarkZipperPrepareReadOnlyRandom(b, 16, 4)
}

func BenchmarkZipperPrepareReadOnlyMultiLeafRandom(b *testing.B) {
	benchmarkZipperPrepareReadOnlyRandom(b, 512, 64)
}

func BenchmarkZipperPrepareReadOnlyManyLeafRandom(b *testing.B) {
	benchmarkZipperPrepareReadOnlyRandom(b, 8192, 1024)
}

func BenchmarkZipperPrepareReadOnlyManyLeafRandomFresh(b *testing.B) {
	benchmarkZipperPrepareReadOnlyRandomFresh(b, 8192, 1024)
}

func BenchmarkZipperPrepareReadOnlyManyLeafRandomKeyModes(b *testing.B) {
	_, z := newReadOnlyPrepareZipper(b)
	rootID := buildReadOnlyPrepareRootWithKeys(b, z, 8192)
	delta := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for i := 0; i < 1024; i++ {
		idx := (i*7919 + 17) % 8192
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", idx)), []byte("new-value")); err != nil {
			b.Fatalf("Set delta: %v", err)
		}
	}
	delta.SortedEntries()

	cases := []struct {
		name string
		opts ReadOnlyPrepareOptions
	}{
		{name: "full_keys"},
		{name: "omit_op_keys", opts: ReadOnlyPrepareOptions{OmitOpKeys: true}},
		{name: "omit_all_keys", opts: ReadOnlyPrepareOptions{OmitKeys: true}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for _, reusePrepareBuffers := range []bool{true, false} {
				name := "reuse"
				if !reusePrepareBuffers {
					name = "fresh"
				}
				b.Run(name, func(b *testing.B) {
					first, err := z.PrepareReadOnly(rootID, delta, tc.opts)
					if err != nil {
						b.Fatalf("initial PrepareReadOnly: %v", err)
					}
					requireValidReadOnlyPrepare(b, first)
					initialSummary := first.LeafSpanSummary()
					if initialSummary.Spans == 0 || initialSummary.SpanOps == 0 {
						b.Fatalf("initial prepare spans/ops=%d/%d want non-zero", initialSummary.Spans, initialSummary.SpanOps)
					}
					opts := tc.opts
					if reusePrepareBuffers {
						opts = first.ReuseOptions()
					}
					lastSummary := initialSummary
					lastKeyArenaBytes := len(first.keyArena)
					lastBoundaryKeyBytes := readOnlyPrepareSpanBoundaryBytes(first.LeafSpans)
					lastOpKeyBytes := readOnlyPrepareSpanOpKeyBytes(first.LeafSpans)
					b.ReportAllocs()
					b.SetBytes(int64(initialSummary.SpanBytes))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						prepared, err := z.PrepareReadOnly(rootID, delta, opts)
						if err != nil {
							b.Fatalf("PrepareReadOnly: %v", err)
						}
						lastSummary = prepared.LeafSpanSummary()
						lastKeyArenaBytes = len(prepared.keyArena)
						lastBoundaryKeyBytes = readOnlyPrepareSpanBoundaryBytes(prepared.LeafSpans)
						lastOpKeyBytes = readOnlyPrepareSpanOpKeyBytes(prepared.LeafSpans)
						if reusePrepareBuffers {
							opts = prepared.ReuseOptions()
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(lastSummary.Spans), "leaf_spans/op")
					b.ReportMetric(float64(lastSummary.SpanOps), "span_ops/op")
					b.ReportMetric(float64(lastSummary.SpanBytes), "span_bytes/op")
					b.ReportMetric(float64(lastKeyArenaBytes), "key_arena_bytes/op")
					b.ReportMetric(float64(lastBoundaryKeyBytes), "boundary_key_bytes/op")
					b.ReportMetric(float64(lastOpKeyBytes), "op_key_bytes/op")
				})
			}
		})
	}
}

func BenchmarkZipperPrepareReadOnlyWideBaseDeltaSparsePoint(b *testing.B) {
	_, z := newReadOnlyPrepareZipper(b)
	rootID := buildReadOnlyPrepareWideBaseDeltaRoot(b, z, 384)
	delta := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	if err := delta.Set([]byte("child-000250"), []byte("new-value")); err != nil {
		b.Fatalf("Set delta: %v", err)
	}
	delta.SortedEntries()

	first, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		b.Fatalf("initial PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(b, first)
	opts := first.ReuseOptions()
	lastSummary := first.LeafSpanSummary()
	lastKeyArenaBytes := len(first.keyArena)
	lastRetainedKeyBytes := readOnlyPrepareSpanBoundaryBytes(first.LeafSpans) + readOnlyPrepareSpanOpKeyBytes(first.LeafSpans)
	b.ReportAllocs()
	b.SetBytes(int64(lastSummary.SpanBytes))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prepared, err := z.PrepareReadOnly(rootID, delta, opts)
		if err != nil {
			b.Fatalf("PrepareReadOnly: %v", err)
		}
		lastSummary = prepared.LeafSpanSummary()
		lastKeyArenaBytes = len(prepared.keyArena)
		lastRetainedKeyBytes = readOnlyPrepareSpanBoundaryBytes(prepared.LeafSpans) + readOnlyPrepareSpanOpKeyBytes(prepared.LeafSpans)
		opts = prepared.ReuseOptions()
	}
	b.StopTimer()
	b.ReportMetric(float64(lastSummary.Spans), "leaf_spans/op")
	b.ReportMetric(float64(lastSummary.SpanOps), "span_ops/op")
	b.ReportMetric(float64(lastKeyArenaBytes), "key_arena_bytes/op")
	b.ReportMetric(float64(lastRetainedKeyBytes), "retained_key_bytes/op")
}

func benchmarkZipperPrepareReadOnlyRandom(b *testing.B, keyCount, batchSize int) {
	benchmarkZipperPrepareReadOnlyRandomImpl(b, keyCount, batchSize, true)
}

func benchmarkZipperPrepareReadOnlyRandomFresh(b *testing.B, keyCount, batchSize int) {
	benchmarkZipperPrepareReadOnlyRandomImpl(b, keyCount, batchSize, false)
}

func benchmarkZipperPrepareReadOnlyRandomImpl(b *testing.B, keyCount, batchSize int, reusePrepareBuffers bool) {
	_, z := newReadOnlyPrepareZipper(b)
	rootID := buildReadOnlyPrepareRootWithKeys(b, z, keyCount)
	delta := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	for i := 0; i < batchSize; i++ {
		idx := (i*7919 + 17) % keyCount
		if err := delta.Set([]byte(fmt.Sprintf("key-%06d", idx)), []byte("new-value")); err != nil {
			b.Fatalf("Set delta: %v", err)
		}
	}
	delta.SortedEntries()

	first, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		b.Fatalf("initial PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(b, first)
	summary := first.LeafSpanSummary()
	if summary.Spans == 0 || summary.SpanOps == 0 {
		b.Fatalf("initial prepare spans/ops=%d/%d want non-zero", summary.Spans, summary.SpanOps)
	}
	opts := ReadOnlyPrepareOptions{}
	if reusePrepareBuffers {
		opts = first.ReuseOptions()
	}
	lastSummary := first.LeafSpanSummary()
	lastWorkerSummary := first.LeafSpanWorkerRangeSummary(4)
	b.ReportAllocs()
	b.SetBytes(int64(summary.SpanBytes))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prepared, err := z.PrepareReadOnly(rootID, delta, opts)
		if err != nil {
			b.Fatalf("PrepareReadOnly: %v", err)
		}
		lastSummary = prepared.LeafSpanSummary()
		lastWorkerSummary = prepared.LeafSpanWorkerRangeSummary(4)
		if reusePrepareBuffers {
			opts = prepared.ReuseOptions()
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(lastSummary.Spans), "leaf_spans/op")
	b.ReportMetric(float64(lastSummary.SpanOps), "span_ops/op")
	b.ReportMetric(float64(lastSummary.SpanBytes), "span_bytes/op")
	b.ReportMetric(float64(lastWorkerSummary.Ranges), "worker_ranges/op")
	b.ReportMetric(float64(lastWorkerSummary.MaxRangeOps), "max_worker_ops/op")
}

func readOnlyPrepareSpanBoundaryBytes(spans []ReadOnlyLeafSpan) int {
	total := 0
	for i := range spans {
		total += len(spans[i].LowKey) + len(spans[i].HighKey)
	}
	return total
}

func readOnlyPrepareSpanOpKeyBytes(spans []ReadOnlyLeafSpan) int {
	total := 0
	for i := range spans {
		total += len(spans[i].FirstOpKey) + len(spans[i].LastOpKey)
	}
	return total
}

func BenchmarkZipperApplyWarmRandomManyLeaf(b *testing.B) {
	_, z := newReadOnlyPrepareZipper(b)
	rootID := buildReadOnlyPrepareRootWithKeys(b, z, 8192)
	left := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	right := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = left.Close() }()
	defer func() { _ = right.Close() }()
	for i := 0; i < 1024; i++ {
		idx := (i*7919 + 17) % 8192
		key := []byte(fmt.Sprintf("key-%06d", idx))
		_ = left.Set(key, []byte("left"))
		_ = right.Set(key, []byte("right"))
	}
	left.SortedEntries()
	right.SortedEntries()

	var totalLeafMerges int64
	var totalInternalMerges int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		delta := left
		if i&1 == 1 {
			delta = right
		}
		newRoot, _, metrics, err := z.Apply(rootID, delta)
		if err != nil {
			b.Fatalf("Apply: %v", err)
		}
		if newRoot == 0 || newRoot == rootID {
			b.Fatalf("new root=%d old root=%d want changed non-zero root", newRoot, rootID)
		}
		rootID = newRoot
		totalLeafMerges += int64(metrics.ZipperLeafMerges)
		totalInternalMerges += int64(metrics.ZipperInternalMerges)
	}
	if b.N > 0 {
		b.ReportMetric(float64(totalLeafMerges)/float64(b.N), "leaf_merges/op")
		b.ReportMetric(float64(totalInternalMerges)/float64(b.N), "internal_merges/op")
	}
}
