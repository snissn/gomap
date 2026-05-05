package zipper

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

type MockAllocator struct {
	p *pager.Pager
}

func (m *MockAllocator) Alloc(hint uint64) (uint64, error) {
	return m.p.Alloc(1)
}

type panicValueReader struct{}

func (panicValueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	panic("unexpected value pointer read in zipper test")
}

func (panicValueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	panic("unexpected value pointer read in zipper test")
}

type countingLeafPageReader struct {
	calls atomic.Int64
}

func (r *countingLeafPageReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	r.calls.Add(1)
	return nil, io.EOF
}

type sourceReportingLeafPageReader struct {
	leaf     []byte
	cacheHit bool
	calls    int
}

func (r *sourceReportingLeafPageReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	r.calls++
	return append([]byte(nil), r.leaf...), nil
}

func (r *sourceReportingLeafPageReader) ReadUnsafeToWithCacheHit(ptr page.ValuePtr, dst []byte) ([]byte, bool, bool, error) {
	r.calls++
	if cap(dst) >= len(r.leaf) {
		out := dst[:len(r.leaf)]
		copy(out, r.leaf)
		return out, true, r.cacheHit, nil
	}
	return append([]byte(nil), r.leaf...), false, r.cacheHit, nil
}

type stubLeafPageLog struct {
	next uint32
}

func (l *stubLeafPageLog) AppendLeafPage(_ []byte) (page.LeafLogPtr, error) {
	if l.next == 0 {
		l.next = 4
	}
	ptr := page.LeafLogPtr{
		FileID: 1,
		Offset: uint64(l.next),
	}
	l.next += 4096 + 32
	return ptr, nil
}

type memoryLeafPageStore struct {
	z *Zipper

	next  uint32
	pages map[page.LeafLogPtr][]byte

	readCalls  int
	sawCache   int
	sawNoCache int
}

func newMemoryLeafPageStore(z *Zipper) *memoryLeafPageStore {
	return &memoryLeafPageStore{
		z:     z,
		pages: make(map[page.LeafLogPtr][]byte),
	}
}

func (s *memoryLeafPageStore) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if s.z != nil {
		s.z.leafRefCacheMu.RLock()
		if s.z.leafRefCache != nil {
			s.sawCache++
		} else {
			s.sawNoCache++
		}
		s.z.leafRefCacheMu.RUnlock()
	}
	if s.next == 0 {
		s.next = 4
	}
	ptr := page.LeafLogPtr{
		FileID: 1,
		Offset: uint64(s.next),
	}
	s.next += 4096 + 32
	s.pages[ptr] = append([]byte(nil), leafPage...)
	return ptr, nil
}

func (s *memoryLeafPageStore) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	s.readCalls++
	leafPtr, err := page.LeafLogPtrFromValuePtr(ptr)
	if err != nil {
		return nil, err
	}
	data, ok := s.pages[leafPtr]
	if !ok {
		return nil, io.EOF
	}
	return data, nil
}

func (s *memoryLeafPageStore) resetObservations() {
	s.readCalls = 0
	s.sawCache = 0
	s.sawNoCache = 0
}

func buildOuterLeafInternalRoot(tb testing.TB, z *Zipper) uint64 {
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
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := []byte(fmt.Sprintf("val-%03d", i))
		b.Set(key, val)
	}

	newRootID, _, _, err := z.Apply(rootID, b)
	if err != nil {
		tb.Fatalf("build root apply: %v", err)
	}
	rootData, err := z.pager.Get(newRootID)
	if err != nil {
		tb.Fatalf("get new root: %v", err)
	}
	if got := node.NewNode(rootData).Type(); got != page.PageTypeInternal {
		tb.Fatalf("new root type=%d want %d", got, page.PageTypeInternal)
	}
	return newRootID
}

func buildInternalRootWithKeys(tb testing.TB, z *Zipper, count int) uint64 {
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
	value := bytes.Repeat([]byte("v"), 128)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		b.Set(key, value)
	}

	newRootID, _, _, err := z.Apply(rootID, b)
	if err != nil {
		tb.Fatalf("build %d-key root apply: %v", count, err)
	}
	return newRootID
}

func rootHasInternalChild(tb testing.TB, z *Zipper, rootID uint64) bool {
	tb.Helper()

	scratch := z.acquireApplyScratch()
	defer z.releaseApplyScratch(scratch)

	root, _, leafScratch, leafScratchRef, _, err := z.loadNodeRef(page.PageChildRef(rootID), scratch)
	if err != nil {
		tb.Fatalf("load root: %v", err)
	}
	if leafScratchRef {
		defer releaseLeafPageScratch(scratch, leafScratch)
	}
	if root.Type() != page.PageTypeInternal {
		return false
	}
	for i := uint16(0); i < root.Count(); i++ {
		_, childRef, err := root.GetInternalEntryRefView(i)
		if err != nil {
			tb.Fatalf("root child %d: %v", i, err)
		}
		child, _, childLeafScratch, childLeafScratchRef, _, err := z.loadNodeRef(childRef, scratch)
		if err != nil {
			tb.Fatalf("load child %d: %v", i, err)
		}
		childType := child.Type()
		if childLeafScratchRef {
			releaseLeafPageScratch(scratch, childLeafScratch)
		}
		if childType == page.PageTypeInternal {
			return true
		}
	}
	return false
}

func buildMultiLevelInternalRoot(tb testing.TB, z *Zipper) (uint64, int) {
	tb.Helper()

	for count := 1024; count <= 32768; count *= 2 {
		rootID := buildInternalRootWithKeys(tb, z, count)
		if rootHasInternalChild(tb, z, rootID) {
			return rootID, count
		}
	}
	tb.Fatal("failed to build a multi-level internal root")
	return 0, 0
}

func requireValidReadOnlyPrepare(tb testing.TB, prepared ReadOnlyPrepareResult) {
	tb.Helper()
	if err := prepared.ValidateLeafSpans(); err != nil {
		tb.Fatalf("ValidateLeafSpans: %v", err)
	}
}

func TestZipperPrepareReadOnlyColdBuildDoesNotLoadOrWrite(t *testing.T) {
	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	b.Set([]byte("a"), []byte("1"))
	b.Set([]byte("z"), []byte("2"))

	var z Zipper
	prepared, err := z.PrepareReadOnly(0, b, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if !prepared.ColdBuild {
		t.Fatal("ColdBuild=false want true")
	}
	if prepared.Maintenance || !prepared.ExactLeafSpans {
		t.Fatalf("cold prepare maintenance/exact=%v/%v want false/true", prepared.Maintenance, prepared.ExactLeafSpans)
	}
	if prepared.RootID != 0 || prepared.Ops != 2 {
		t.Fatalf("prepared root/ops=%d/%d want 0/2", prepared.RootID, prepared.Ops)
	}
	if len(prepared.LeafSpans) != 1 {
		t.Fatalf("leaf spans=%d want 1", len(prepared.LeafSpans))
	}
	span := prepared.LeafSpans[0]
	if span.Ref != (page.ChildRef{}) {
		t.Fatalf("cold span ref=%+v want zero ChildRef", span.Ref)
	}
	if span.OpCount != 2 || string(span.FirstOpKey) != "a" || string(span.LastOpKey) != "z" {
		t.Fatalf("cold span=%+v want two ops from a to z", span)
	}
	if prepared.Metrics.ZipperNodeLoads != 0 || prepared.Metrics.IndexWriteBytes != 0 {
		t.Fatalf("cold prepare metrics=%+v want no node load/write", prepared.Metrics)
	}
}

func TestZipperPrepareReadOnlyEmptyBatchDoesNotTraverse(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	rootID := buildOuterLeafInternalRoot(t, z)
	beforePages := p.PageCount()

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	prepared, err := z.PrepareReadOnly(rootID, b, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if got := p.PageCount(); got != beforePages {
		t.Fatalf("page count changed during empty read-only prepare: got %d want %d", got, beforePages)
	}
	if prepared.RootID != rootID || prepared.Ops != 0 || !prepared.ExactLeafSpans {
		t.Fatalf("prepared=%+v want root %d zero ops exact", prepared, rootID)
	}
	if len(prepared.LeafSpans) != 0 {
		t.Fatalf("empty batch spans=%d want 0", len(prepared.LeafSpans))
	}
	if prepared.Metrics.ZipperNodeLoads != 0 {
		t.Fatalf("empty batch traversed tree metrics=%+v", prepared.Metrics)
	}
}

func TestZipperPrepareReadOnlyExistingLeafRoot(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	rootID := buildInternalRootWithKeys(t, z, 8)
	rootData, err := p.Get(rootID)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	if got := node.NewNode(rootData).Type(); got != page.PageTypeLeaf {
		t.Fatalf("root type=%d want leaf", got)
	}

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	b.Set([]byte("key-000003"), []byte("new"))

	prepared, err := z.PrepareReadOnly(rootID, b, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if prepared.ColdBuild || prepared.Maintenance || !prepared.ExactLeafSpans {
		t.Fatalf("prepare cold/maintenance/exact=%v/%v/%v want false/false/true", prepared.ColdBuild, prepared.Maintenance, prepared.ExactLeafSpans)
	}
	if len(prepared.LeafSpans) != 1 {
		t.Fatalf("leaf spans=%d want 1", len(prepared.LeafSpans))
	}
	span := prepared.LeafSpans[0]
	if span.Ref != page.PageChildRef(rootID) {
		t.Fatalf("span ref=%+v want page root %d", span.Ref, rootID)
	}
	if string(span.FirstOpKey) != "key-000003" || string(span.LastOpKey) != "key-000003" || span.OpCount != 1 {
		t.Fatalf("span=%+v want one key-000003 op", span)
	}
}

func TestZipperPrepareReadOnlyDiscoversLeafSpansWithoutWrites(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	rootID := buildOuterLeafInternalRoot(t, z)
	beforePages := p.PageCount()

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	b.Set([]byte("key-001"), []byte("new-001"))
	b.Set([]byte("key-199"), []byte("new-199"))

	prepared, err := z.PrepareReadOnly(rootID, b, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if got := p.PageCount(); got != beforePages {
		t.Fatalf("page count changed during read-only prepare: got %d want %d", got, beforePages)
	}
	if prepared.ColdBuild {
		t.Fatal("ColdBuild=true for existing root")
	}
	if prepared.Maintenance || !prepared.ExactLeafSpans {
		t.Fatalf("prepare maintenance/exact=%v/%v want false/true", prepared.Maintenance, prepared.ExactLeafSpans)
	}
	if prepared.RootID != rootID || prepared.Ops != 2 {
		t.Fatalf("prepared root/ops=%d/%d want %d/2", prepared.RootID, prepared.Ops, rootID)
	}
	if len(prepared.LeafSpans) < 2 {
		t.Fatalf("leaf spans=%d want at least 2 for distant keys", len(prepared.LeafSpans))
	}
	if prepared.Metrics.ZipperNodeLoads == 0 {
		t.Fatalf("node loads=0 want read-only traversal loads")
	}
	if prepared.Metrics.IndexWriteBytes != 0 ||
		prepared.Metrics.ZipperLeafPagesWritten != 0 ||
		prepared.Metrics.ZipperInternalPagesWritten != 0 {
		t.Fatalf("read-only prepare wrote output metrics=%+v", prepared.Metrics)
	}
	for _, span := range prepared.LeafSpans {
		if span.OpCount <= 0 {
			t.Fatalf("empty span recorded: %+v", span)
		}
		if len(span.FirstOpKey) == 0 || len(span.LastOpKey) == 0 {
			t.Fatalf("span missing op bounds: %+v", span)
		}
	}
}

func TestZipperPrepareReadOnlyReuseOptions(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	rootID := buildOuterLeafInternalRoot(t, z)

	firstBatch := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = firstBatch.Close() }()
	firstBatch.Set([]byte("key-001"), []byte("one"))
	firstBatch.Set([]byte("key-199"), []byte("two"))

	first, err := z.PrepareReadOnly(rootID, firstBatch, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("first PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, first)
	if len(first.LeafSpans) == 0 {
		t.Fatal("first prepare returned no spans")
	}
	opts := first.ReuseOptions()

	secondBatch := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = secondBatch.Close() }()
	secondBatch.Set([]byte("key-067"), []byte("three"))

	second, err := z.PrepareReadOnly(rootID, secondBatch, opts)
	if err != nil {
		t.Fatalf("second PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, second)
	if len(second.LeafSpans) != 1 {
		t.Fatalf("second spans=%d want 1", len(second.LeafSpans))
	}
	span := second.LeafSpans[0]
	if string(span.FirstOpKey) != "key-067" || string(span.LastOpKey) != "key-067" || span.OpCount != 1 {
		t.Fatalf("second reused span=%+v want key-067", span)
	}
}

func TestZipperPrepareReadOnlyMarksDeleteMaintenanceSpansNonExact(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	rootID := buildOuterLeafInternalRoot(t, z)
	beforePages := p.PageCount()

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	b.Delete([]byte("key-001"))
	b.Delete([]byte("key-199"))

	prepared, err := z.PrepareReadOnly(rootID, b, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if got := p.PageCount(); got != beforePages {
		t.Fatalf("page count changed during read-only prepare: got %d want %d", got, beforePages)
	}
	if !prepared.Maintenance {
		t.Fatal("Maintenance=false want true for delete-containing batch")
	}
	if prepared.ExactLeafSpans {
		t.Fatal("ExactLeafSpans=true want false for delete maintenance")
	}
	if len(prepared.LeafSpans) == 0 {
		t.Fatal("delete maintenance still should expose direct planning spans")
	}
	if prepared.Metrics.IndexWriteBytes != 0 ||
		prepared.Metrics.ZipperLeafPagesWritten != 0 ||
		prepared.Metrics.ZipperInternalPagesWritten != 0 {
		t.Fatalf("delete read-only prepare wrote output metrics=%+v", prepared.Metrics)
	}
}

func TestZipperPrepareReadOnlyInternalBaseDeltaKeyBoundsAreStable(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetIndexInternalBaseDelta(true)
	rootID := buildOuterLeafInternalRoot(t, z)
	beforePages := p.PageCount()

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	b.Set([]byte("key-001"), []byte("new-001"))
	b.Set([]byte("key-199"), []byte("new-199"))

	prepared, err := z.PrepareReadOnly(rootID, b, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if got := p.PageCount(); got != beforePages {
		t.Fatalf("page count changed during read-only prepare: got %d want %d", got, beforePages)
	}
	if prepared.Maintenance || !prepared.ExactLeafSpans {
		t.Fatalf("prepare maintenance/exact=%v/%v want false/true", prepared.Maintenance, prepared.ExactLeafSpans)
	}
	if len(prepared.LeafSpans) < 2 {
		t.Fatalf("leaf spans=%d want at least 2", len(prepared.LeafSpans))
	}
	for _, span := range prepared.LeafSpans {
		if len(span.LowKey) > 0 && bytes.Compare(span.LowKey, span.FirstOpKey) > 0 {
			t.Fatalf("span low bound %q is after first op %q; span=%+v", span.LowKey, span.FirstOpKey, span)
		}
		if len(span.HighKey) > 0 && bytes.Compare(span.FirstOpKey, span.HighKey) >= 0 {
			t.Fatalf("span high bound %q is not after first op %q; span=%+v", span.HighKey, span.FirstOpKey, span)
		}
	}
}

func TestZipperPrepareReadOnlyNestedInternalBoundsInheritParentRange(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetIndexInternalBaseDelta(true)
	rootID, count := buildMultiLevelInternalRoot(t, z)
	beforePages := p.PageCount()

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	opKeys := [][]byte{
		[]byte("key-000001"),
		[]byte(fmt.Sprintf("key-%06d", count/4)),
		[]byte(fmt.Sprintf("key-%06d", count/2)),
		[]byte(fmt.Sprintf("key-%06d", count-2)),
	}
	for _, key := range opKeys {
		b.Set(key, []byte("new"))
	}

	prepared, err := z.PrepareReadOnly(rootID, b, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)
	if got := p.PageCount(); got != beforePages {
		t.Fatalf("page count changed during read-only prepare: got %d want %d", got, beforePages)
	}
	if prepared.Maintenance || !prepared.ExactLeafSpans {
		t.Fatalf("prepare maintenance/exact=%v/%v want false/true", prepared.Maintenance, prepared.ExactLeafSpans)
	}
	if len(prepared.LeafSpans) < 3 {
		t.Fatalf("leaf spans=%d want at least 3 for sparse multi-level keys", len(prepared.LeafSpans))
	}
	for _, span := range prepared.LeafSpans {
		if len(span.FirstOpKey) == 0 {
			t.Fatalf("span missing first op key: %+v", span)
		}
		if bytes.Compare(span.FirstOpKey, []byte("key-000001")) > 0 && len(span.LowKey) == 0 {
			t.Fatalf("span for non-leftmost op has empty inherited low bound: %+v", span)
		}
		if span.LowKey != nil && bytes.Compare(span.LowKey, span.FirstOpKey) > 0 {
			t.Fatalf("span low bound %q is after first op %q; span=%+v", span.LowKey, span.FirstOpKey, span)
		}
		if span.HighKey != nil && bytes.Compare(span.FirstOpKey, span.HighKey) >= 0 {
			t.Fatalf("span high bound %q is not after first op %q; span=%+v", span.HighKey, span.FirstOpKey, span)
		}
	}
}

func TestReadOnlyPrepareResultValidateLeafSpansRejectsInvalidPlans(t *testing.T) {
	validSpan := ReadOnlyLeafSpan{
		LowKey:     []byte("a"),
		HighKey:    []byte("z"),
		FirstOpKey: []byte("b"),
		LastOpKey:  []byte("c"),
		OpCount:    2,
	}
	tests := []struct {
		name string
		in   ReadOnlyPrepareResult
	}{
		{
			name: "zero ops with span",
			in: ReadOnlyPrepareResult{
				Ops:       0,
				LeafSpans: []ReadOnlyLeafSpan{validSpan},
			},
		},
		{
			name: "missing spans",
			in:   ReadOnlyPrepareResult{Ops: 1},
		},
		{
			name: "cold build multiple spans",
			in: ReadOnlyPrepareResult{
				Ops:       2,
				ColdBuild: true,
				LeafSpans: []ReadOnlyLeafSpan{
					{FirstOpKey: []byte("a"), LastOpKey: []byte("a"), OpCount: 1},
					{FirstOpKey: []byte("b"), LastOpKey: []byte("b"), OpCount: 1},
				},
			},
		},
		{
			name: "empty op key",
			in: ReadOnlyPrepareResult{
				Ops:       1,
				LeafSpans: []ReadOnlyLeafSpan{{LastOpKey: []byte("b"), OpCount: 1}},
			},
		},
		{
			name: "reversed op keys",
			in: ReadOnlyPrepareResult{
				Ops:       1,
				LeafSpans: []ReadOnlyLeafSpan{{FirstOpKey: []byte("c"), LastOpKey: []byte("b"), OpCount: 1}},
			},
		},
		{
			name: "overlapping op key ranges",
			in: ReadOnlyPrepareResult{
				Ops: 2,
				LeafSpans: []ReadOnlyLeafSpan{
					{FirstOpKey: []byte("b"), LastOpKey: []byte("d"), OpCount: 1},
					{FirstOpKey: []byte("d"), LastOpKey: []byte("e"), OpCount: 1},
				},
			},
		},
		{
			name: "bad bounds",
			in: ReadOnlyPrepareResult{
				Ops:       1,
				LeafSpans: []ReadOnlyLeafSpan{{LowKey: []byte("z"), HighKey: []byte("a"), FirstOpKey: []byte("m"), LastOpKey: []byte("m"), OpCount: 1}},
			},
		},
		{
			name: "op before low bound",
			in: ReadOnlyPrepareResult{
				Ops:       1,
				LeafSpans: []ReadOnlyLeafSpan{{LowKey: []byte("c"), FirstOpKey: []byte("b"), LastOpKey: []byte("b"), OpCount: 1}},
			},
		},
		{
			name: "op at high bound",
			in: ReadOnlyPrepareResult{
				Ops:       1,
				LeafSpans: []ReadOnlyLeafSpan{{HighKey: []byte("b"), FirstOpKey: []byte("b"), LastOpKey: []byte("b"), OpCount: 1}},
			},
		},
		{
			name: "op count mismatch",
			in: ReadOnlyPrepareResult{
				Ops:       3,
				LeafSpans: []ReadOnlyLeafSpan{validSpan},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.in.ValidateLeafSpans(); err == nil {
				t.Fatal("ValidateLeafSpans returned nil, want error")
			}
		})
	}
}

func TestReadOnlyPrepareResultValidateLeafSpansFormatsKeysSafely(t *testing.T) {
	longKey := []byte("0123456789abcdef")
	prepared := ReadOnlyPrepareResult{
		Ops: 1,
		LeafSpans: []ReadOnlyLeafSpan{
			{FirstOpKey: longKey, LastOpKey: []byte("0"), OpCount: 1},
		},
	}

	err := prepared.ValidateLeafSpans()
	if err == nil {
		t.Fatal("ValidateLeafSpans returned nil, want key-order error")
	}
	msg := err.Error()
	if strings.Contains(msg, string(longKey)) {
		t.Fatalf("error leaked full raw key %q: %s", longKey, msg)
	}
	if !strings.Contains(msg, "len=16") || !strings.Contains(msg, "hex_prefix=3031323334353637") {
		t.Fatalf("error missing safe key summary: %s", msg)
	}
}

func TestReadOnlyPrepareResultValidateLeafSpansAcceptsOpenBounds(t *testing.T) {
	prepared := ReadOnlyPrepareResult{
		Ops:            3,
		ExactLeafSpans: true,
		LeafSpans: []ReadOnlyLeafSpan{
			{HighKey: []byte("m"), FirstOpKey: []byte("a"), LastOpKey: []byte("b"), OpCount: 2},
			{LowKey: []byte("m"), FirstOpKey: []byte("m"), LastOpKey: []byte("m"), OpCount: 1},
		},
	}
	requireValidReadOnlyPrepare(t, prepared)
}

func TestReadOnlyPrepareResultLeafSpanSummary(t *testing.T) {
	prepared := ReadOnlyPrepareResult{
		Ops:            6,
		ExactLeafSpans: true,
		LeafSpans: []ReadOnlyLeafSpan{
			{FirstOpKey: []byte("a"), LastOpKey: []byte("a"), OpCount: 1, HighKey: []byte("d")},
			{LowKey: []byte("d"), HighKey: []byte("m"), FirstOpKey: []byte("e"), LastOpKey: []byte("h"), OpCount: 2},
			{LowKey: []byte("m"), FirstOpKey: []byte("q"), LastOpKey: []byte("z"), OpCount: 3},
		},
	}

	summary := prepared.LeafSpanSummary()
	if summary.Ops != 6 || summary.Spans != 3 || !summary.ExactLeafSpans {
		t.Fatalf("summary ops/spans/exact=%d/%d/%v want 6/3/true", summary.Ops, summary.Spans, summary.ExactLeafSpans)
	}
	if summary.MinSpanOps != 1 || summary.MaxSpanOps != 3 || summary.SingleOpSpans != 1 {
		t.Fatalf("summary op distribution min/max/single=%d/%d/%d want 1/3/1", summary.MinSpanOps, summary.MaxSpanOps, summary.SingleOpSpans)
	}
	if summary.OpenLowSpans != 1 || summary.OpenHighSpans != 1 {
		t.Fatalf("summary open bounds low/high=%d/%d want 1/1", summary.OpenLowSpans, summary.OpenHighSpans)
	}
}

func TestReadOnlyPrepareResultLeafSpanSummaryEmptyPlan(t *testing.T) {
	summary := (ReadOnlyPrepareResult{ExactLeafSpans: true}).LeafSpanSummary()
	if summary.Ops != 0 || summary.Spans != 0 || !summary.ExactLeafSpans {
		t.Fatalf("empty summary ops/spans/exact=%d/%d/%v want 0/0/true", summary.Ops, summary.Spans, summary.ExactLeafSpans)
	}
	if summary.MinSpanOps != 0 || summary.MaxSpanOps != 0 || summary.SingleOpSpans != 0 {
		t.Fatalf("empty summary op distribution min/max/single=%d/%d/%d want 0/0/0", summary.MinSpanOps, summary.MaxSpanOps, summary.SingleOpSpans)
	}
	if summary.OpenLowSpans != 0 || summary.OpenHighSpans != 0 {
		t.Fatalf("empty summary open bounds low/high=%d/%d want 0/0", summary.OpenLowSpans, summary.OpenHighSpans)
	}
}

func TestZipperPrepareReadOnlyLeafSpanSummaryMatchesPlan(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	rootID := buildOuterLeafInternalRoot(t, z)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	delta.Set([]byte("key-001"), []byte("new-001"))
	delta.Set([]byte("key-067"), []byte("new-067"))
	delta.Set([]byte("key-133"), []byte("new-133"))
	delta.Set([]byte("key-199"), []byte("new-199"))

	prepared, err := z.PrepareReadOnly(rootID, delta, ReadOnlyPrepareOptions{})
	if err != nil {
		t.Fatalf("PrepareReadOnly: %v", err)
	}
	requireValidReadOnlyPrepare(t, prepared)

	summary := prepared.LeafSpanSummary()
	if summary.Ops != prepared.Ops || summary.Spans != len(prepared.LeafSpans) {
		t.Fatalf("summary ops/spans=%d/%d want %d/%d", summary.Ops, summary.Spans, prepared.Ops, len(prepared.LeafSpans))
	}
	wantMin, wantMax, wantSingle, wantOpenLow, wantOpenHigh := 0, 0, 0, 0, 0
	for i, span := range prepared.LeafSpans {
		if i == 0 || span.OpCount < wantMin {
			wantMin = span.OpCount
		}
		if i == 0 || span.OpCount > wantMax {
			wantMax = span.OpCount
		}
		if span.OpCount == 1 {
			wantSingle++
		}
		if span.LowKey == nil {
			wantOpenLow++
		}
		if span.HighKey == nil {
			wantOpenHigh++
		}
	}
	if summary.MinSpanOps != wantMin || summary.MaxSpanOps != wantMax || summary.SingleOpSpans != wantSingle {
		t.Fatalf("summary op distribution min/max/single=%d/%d/%d want %d/%d/%d", summary.MinSpanOps, summary.MaxSpanOps, summary.SingleOpSpans, wantMin, wantMax, wantSingle)
	}
	if summary.OpenLowSpans != wantOpenLow || summary.OpenHighSpans != wantOpenHigh {
		t.Fatalf("summary open bounds low/high=%d/%d want %d/%d", summary.OpenLowSpans, summary.OpenHighSpans, wantOpenLow, wantOpenHigh)
	}
}

var readOnlyLeafSpanSummaryBenchmarkSink ReadOnlyLeafSpanSummary

func BenchmarkReadOnlyPrepareResultLeafSpanSummary(b *testing.B) {
	prepared := ReadOnlyPrepareResult{
		Ops:            4,
		ExactLeafSpans: true,
		LeafSpans: []ReadOnlyLeafSpan{
			{FirstOpKey: []byte("a"), LastOpKey: []byte("a"), OpCount: 1, HighKey: []byte("d")},
			{LowKey: []byte("d"), HighKey: []byte("m"), FirstOpKey: []byte("e"), LastOpKey: []byte("h"), OpCount: 2},
			{LowKey: []byte("m"), FirstOpKey: []byte("q"), LastOpKey: []byte("z"), OpCount: 1},
		},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		readOnlyLeafSpanSummaryBenchmarkSink = prepared.LeafSpanSummary()
	}
}

func BenchmarkZipperPrepareReadOnlyWarmSparse(b *testing.B) {
	dir := b.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	rootID := buildOuterLeafInternalRoot(b, z)

	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = delta.Close() }()
	delta.Set([]byte("key-001"), []byte("new-001"))
	delta.Set([]byte("key-067"), []byte("new-067"))
	delta.Set([]byte("key-133"), []byte("new-133"))
	delta.Set([]byte("key-199"), []byte("new-199"))

	opts := ReadOnlyPrepareOptions{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prepared, err := z.PrepareReadOnly(rootID, delta, opts)
		if err != nil {
			b.Fatalf("PrepareReadOnly: %v", err)
		}
		if len(prepared.LeafSpans) == 0 {
			b.Fatal("no prepared leaf spans")
		}
		opts = prepared.ReuseOptions()
	}
}

func TestZipperLeafRefCacheAvoidsUnflushedReads(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetOuterLeavesInValueLog(true)
	log := &stubLeafPageLog{}
	reader := &countingLeafPageReader{}
	z.SetLeafPageLog(log)
	z.SetLeafPageReader(reader)

	// Simulate Apply() scope: enable the in-flight leaf-ref cache so loadNode can
	// resolve freshly appended leaf pages before the leafPageLog is flushed.
	z.leafRefCache = make(map[page.LeafLogPtr][]byte)

	data := make([]byte, page.PageSize)
	b := node.NewBuilder(data, page.PageTypeLeaf)
	b.SetPageID(0)
	if err := b.AddLeafEntry([]byte("k"), []byte("v"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry: %v", err)
	}
	b.FinishNoNode()

	leafID, err := z.persistLeafPage(b)
	if err != nil {
		t.Fatalf("persistLeafPage: %v", err)
	}

	loaded, fromPager, leafScratch, leafScratchRef, loadSource, err := z.loadNodeRef(leafID, nil)
	if err != nil {
		t.Fatalf("loadNode: %v", err)
	}
	if leafScratchRef {
		putLeafPageScratch(leafScratch)
	}
	if fromPager {
		t.Fatalf("fromPager=%t want false", fromPager)
	}
	if loadSource != zipperNodeLoadLeafLogCache {
		t.Fatalf("loadSource=%d want leaf-log cache", loadSource)
	}
	if loaded.Type() != page.PageTypeLeaf {
		t.Fatalf("loaded.Type()=%d want %d", loaded.Type(), page.PageTypeLeaf)
	}
	if got := reader.calls.Load(); got != 0 {
		t.Fatalf("leafPageReader calls=%d want 0", got)
	}
}

func TestZipperLoadNodeRefAttributesLeafPageReaderCacheHit(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetOuterLeavesInValueLog(true)

	leaf := make([]byte, page.PageSize)
	leafNode := node.NewNode(leaf)
	leafNode.SetPageID(0)
	leafNode.SetType(page.PageTypeLeaf)
	leafNode.UpdateChecksum()
	reader := &sourceReportingLeafPageReader{leaf: leaf, cacheHit: true}
	z.SetLeafPageReader(reader)

	ptr := page.LeafLogPtr{FileID: 1, Offset: 128, RecordLengthHint: page.PageSize}
	loaded, fromPager, leafScratch, leafScratchRef, loadSource, err := z.loadNodeRef(page.LeafLogChildRef(ptr), nil)
	if err != nil {
		t.Fatalf("loadNodeRef: %v", err)
	}
	if leafScratchRef {
		putLeafPageScratch(leafScratch)
	}
	if fromPager {
		t.Fatalf("fromPager=%t want false", fromPager)
	}
	if loadSource != zipperNodeLoadLeafLogCache {
		t.Fatalf("loadSource=%d want leaf-log cache", loadSource)
	}
	if loaded.Type() != page.PageTypeLeaf {
		t.Fatalf("loaded.Type()=%d want %d", loaded.Type(), page.PageTypeLeaf)
	}
	if reader.calls != 1 {
		t.Fatalf("reader calls=%d want 1", reader.calls)
	}
}

func TestZipperLoadNodeRefAnnotatesLeafPageReaderCacheValidationError(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetOuterLeavesInValueLog(true)
	z.SetLeafPageReader(&sourceReportingLeafPageReader{leaf: []byte("short"), cacheHit: true})

	ptr := page.LeafLogPtr{FileID: 1, Offset: 128, RecordLengthHint: page.PageSize}
	_, _, _, _, _, err = z.loadNodeRef(page.LeafLogChildRef(ptr), nil)
	if err == nil {
		t.Fatal("loadNodeRef unexpectedly succeeded")
	}
	msg := err.Error()
	if !strings.Contains(msg, "leaf-page reader cache invalid leaf-log page") || !strings.Contains(msg, "invalid size") {
		t.Fatalf("error=%q, want source context and validation detail", msg)
	}
}

func TestZipperApply_NonMaintenanceRestorePathDoesNotInstallLeafRefCache(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetOuterLeavesInValueLog(true)
	store := newMemoryLeafPageStore(z)
	z.SetLeafPageLog(store)
	z.SetLeafPageReader(store)

	rootID := buildOuterLeafInternalRoot(t, z)
	store.resetObservations()

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	b.Set([]byte("key-050"), []byte("updated"))

	if _, _, _, err := z.Apply(rootID, b); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if store.readCalls == 0 {
		t.Fatalf("expected restore path to read existing outer leaf refs")
	}
	if store.sawNoCache == 0 {
		t.Fatalf("expected fresh outer leaf writes to observe no in-flight cache on non-maintenance apply")
	}
	if store.sawCache != 0 {
		t.Fatalf("saw cache on non-maintenance apply: sawCache=%d sawNoCache=%d", store.sawCache, store.sawNoCache)
	}
	if z.leafRefCache != nil {
		t.Fatalf("leafRefCache not cleared after Apply")
	}
}

func TestZipperApply_MaintenanceRestorePathInstallsLeafRefCache(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetOuterLeavesInValueLog(true)
	store := newMemoryLeafPageStore(z)
	z.SetLeafPageLog(store)
	z.SetLeafPageReader(store)

	rootID := buildOuterLeafInternalRoot(t, z)
	store.resetObservations()

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	b.Delete([]byte("key-050"))

	if _, _, _, err := z.Apply(rootID, b); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if store.readCalls == 0 {
		t.Fatalf("expected restore path to read existing outer leaf refs")
	}
	if store.sawCache == 0 {
		t.Fatalf("expected maintenance apply to install the in-flight outer leaf cache")
	}
	if store.sawNoCache != 0 {
		t.Fatalf("unexpected cache miss observation during maintenance apply: sawCache=%d sawNoCache=%d", store.sawCache, store.sawNoCache)
	}
	if z.leafRefCache != nil {
		t.Fatalf("leafRefCache not cleared after Apply")
	}
}

func TestZipperMetricsRecordLeafLogLoadSourcesAndChildRefs(t *testing.T) {
	var metrics adaptive.Metrics
	leafRef := page.LeafLogChildRef(page.LogRecordRef{FileID: 1, Offset: 2, RecordLengthHint: page.PageSize})
	pageRef := page.PageChildRef(7)
	recordZipperNodeLoad(&metrics, leafRef, node.Node{}, zipperNodeLoadLeafLogCache)
	recordZipperNodeLoad(&metrics, leafRef, node.Node{}, zipperNodeLoadLeafLogView)
	recordZipperNodeLoad(&metrics, leafRef, node.Node{}, zipperNodeLoadLeafLogScratch)
	recordZipperNodeLoad(&metrics, pageRef, node.Node{}, zipperNodeLoadPager)
	recordZipperInternalChildRef(&metrics, leafRef)
	recordZipperInternalChildRef(&metrics, pageRef)
	recordZipperInternalLeafLogRefCopy(&metrics)
	recordZipperLeafPageWrite(&metrics, true)
	recordZipperLeafLogPageRecordHintWrite(&metrics, leafRef)
	recordZipperLeafPageWrite(&metrics, false)
	recordZipperInternalPageWrite(&metrics)

	if metrics.ZipperNodeLoads != 4 || metrics.ZipperLeafLogNodeLoads != 3 || metrics.ZipperPagerNodeLoads != 1 {
		t.Fatalf("node load metrics=%+v, want 3 leaf-log and 1 pager load", metrics)
	}
	if metrics.ZipperLeafLogNodeBytesRead != 3*page.PageSize || metrics.ZipperPagerNodeBytesRead != page.PageSize {
		t.Fatalf("node byte metrics=%+v, want leaf-log=%d pager=%d", metrics, 3*page.PageSize, page.PageSize)
	}
	if metrics.ZipperLeafLogRecordHintBytesRead != 3*page.PageSize {
		t.Fatalf("leaf-log record-hint read bytes=%d want %d", metrics.ZipperLeafLogRecordHintBytesRead, 3*page.PageSize)
	}
	if metrics.ZipperLeafLogCacheHits != 1 || metrics.ZipperLeafLogReaderCalls != 2 || metrics.ZipperLeafLogViewReads != 1 || metrics.ZipperLeafLogScratchReads != 1 {
		t.Fatalf("leaf-log source metrics=%+v, want cache/view/scratch attribution", metrics)
	}
	if metrics.ZipperInternalChildRefs != 2 || metrics.ZipperInternalLeafLogRefs != 1 || metrics.ZipperInternalPageChildRefs != 1 {
		t.Fatalf("internal child-ref metrics=%+v, want one leaf-log and one pager child ref", metrics)
	}
	if metrics.ZipperInternalLeafLogRefCopies != 1 {
		t.Fatalf("internal leaf-log ref copy metrics=%+v, want one copied leaf-log ref", metrics)
	}
	if metrics.ZipperLeafPagesWritten != 2 || metrics.ZipperLeafLogPagesWritten != 1 || metrics.ZipperPagerLeafPagesWritten != 1 {
		t.Fatalf("leaf write metrics=%+v, want one leaf-log and one pager leaf write", metrics)
	}
	if metrics.ZipperLeafPageBytesWritten != 2*page.PageSize || metrics.ZipperLeafLogPageBytesWritten != page.PageSize || metrics.ZipperPagerLeafPageBytesWritten != page.PageSize {
		t.Fatalf("leaf write byte metrics=%+v, want logical bytes for one leaf-log and one pager write", metrics)
	}
	if metrics.ZipperLeafLogRecordHintBytesWritten != page.PageSize {
		t.Fatalf("leaf-log record-hint written bytes=%d want %d", metrics.ZipperLeafLogRecordHintBytesWritten, page.PageSize)
	}
	if metrics.ZipperInternalPagesWritten != 1 || metrics.ZipperInternalPageBytesWritten != page.PageSize {
		t.Fatalf("internal page write metrics=%+v, want one page write", metrics)
	}
}

func TestZipperInsertSplit(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)

	// Create initial root (Leaf)
	rootID, _ := p.Alloc(1)
	data, _ := p.Get(rootID)
	n := node.NewNode(data)
	n.SetPageID(rootID)
	n.SetType(page.PageTypeLeaf)
	n.UpdateChecksum()

	// Batch 1: Insert enough to cause split
	// PageSize = 4096. Entry overhead ~10 bytes.
	// If key=10 bytes, val=10 bytes -> 30 bytes/entry.
	// 4000 / 30 = ~133 entries.

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	// Insert 200 items
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := []byte(fmt.Sprintf("val-%03d", i))
		b.Set(key, val)
	}

	newRootID, _, _, err := z.Apply(rootID, b)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if newRootID == rootID {
		t.Error("Expected new root ID (COW)")
	}

	// Verify Data using Tree
	tr := tree.New(p, panicValueReader{}, newRootID)

	val, err := tr.Get([]byte("key-000"))
	if err != nil {
		t.Error("Failed to get key-000")
	}
	if !bytes.Equal(val, []byte("val-000")) {
		t.Errorf("Value mismatch: %s", val)
	}

	val, err = tr.Get([]byte("key-199"))
	if err != nil {
		t.Error("Failed to get key-199")
	}
	if !bytes.Equal(val, []byte("val-199")) {
		t.Errorf("Value mismatch: %s", val)
	}

	// Check Old Root untouched (basic check)
	// We can't easily check content without parsing, but ID check passed.
}

func TestZipperUpdates(t *testing.T) {
	// Setup same as above...
	dir := t.TempDir()
	p, _ := pager.Open(filepath.Join(dir, "index.db"), 65536)
	defer p.Close()
	alloc := &MockAllocator{p: p}
	z := New(p, alloc)

	// Init Root
	rootID, _ := p.Alloc(1)
	data, _ := p.Get(rootID)
	n := node.NewNode(data)
	n.SetPageID(rootID)
	n.SetType(page.PageTypeLeaf)
	n.UpdateChecksum()

	// Batch 1: Insert A, B
	b1 := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b1.Close() }()
	b1.Set([]byte("A"), []byte("valA"))
	b1.Set([]byte("B"), []byte("valB"))

	root2, _, _, err := z.Apply(rootID, b1)
	if err != nil {
		t.Fatal(err)
	}

	// Batch 2: Update A, Delete B, Insert C
	b2 := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b2.Close() }()
	b2.Set([]byte("A"), []byte("valA2"))
	b2.Delete([]byte("B"))
	b2.Set([]byte("C"), []byte("valC"))

	root3, _, _, err := z.Apply(root2, b2)
	if err != nil {
		t.Fatal(err)
	}

	tr := tree.New(p, panicValueReader{}, root3)

	// Check A updated
	val, err := tr.Get([]byte("A"))
	if !bytes.Equal(val, []byte("valA2")) {
		t.Errorf("A mismatch: %s", val)
	}

	// Check B deleted
	_, err = tr.Get([]byte("B"))
	if err != tree.ErrKeyNotFound {
		t.Errorf("B should be deleted, got %v", err)
	}

	// Check C inserted
	val, err = tr.Get([]byte("C"))
	if !bytes.Equal(val, []byte("valC")) {
		t.Errorf("C mismatch: %s", val)
	}
}

func TestCoalesceInternalChildren_SkipsLeafRefsWhenOuterLeavesInValueLog(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetOuterLeavesInValueLog(true)

	leftID := page.LeafLogChildRef(page.LeafLogPtr{FileID: 1, Offset: 4})
	rightID := page.LeafLogChildRef(page.LeafLogPtr{FileID: 1, Offset: 4096})

	entries := []internalEntry{
		{key: []byte("a"), child: leftID},
		{key: []byte("b"), child: rightID},
	}

	got, retired, err := z.coalesceInternalChildren(entries, nil, &adaptive.Metrics{})
	if err != nil {
		t.Fatalf("coalesceInternalChildren: %v", err)
	}
	if len(retired) != 0 {
		t.Fatalf("retired=%v want none", retired)
	}
	if len(got) != len(entries) {
		t.Fatalf("len(got)=%d want %d", len(got), len(entries))
	}
	for i := range entries {
		if !bytes.Equal(got[i].key, entries[i].key) || got[i].child != entries[i].child {
			t.Fatalf("entry[%d]=(%q,%v) want (%q,%v)", i, got[i].key, got[i].child, entries[i].key, entries[i].child)
		}
	}
}

func TestCoalesceLeafChildrenPrefixCompression(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetLeafPrefixCompression(true)

	buildLeaf := func(keys []string, valSize int) uint64 {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc leaf: %v", err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get leaf: %v", err)
		}
		b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: true})
		b.SetPageID(id)
		val := bytes.Repeat([]byte("v"), valSize)
		for _, k := range keys {
			if err := b.AddLeafEntry([]byte(k), val, node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("add leaf entry %q: %v", k, err)
			}
		}
		b.Finish()
		return id
	}

	leftID := buildLeaf([]string{"a0"}, 100)
	rightID := buildLeaf([]string{"b0", "b1", "b2", "b3"}, 1000)

	entries := []internalEntry{
		{key: []byte{}, child: page.PageChildRef(leftID)},
		{key: []byte("b0"), child: page.PageChildRef(rightID)},
	}

	out, _, err := z.coalesceLeafChildren(entries, nil, &adaptive.Metrics{}, nil)
	if err != nil {
		t.Fatalf("coalesceLeafChildren: %v", err)
	}

	var got []string
	for _, e := range out {
		if e.child.Kind != page.ChildRefPage {
			t.Fatalf("child=%v want page child", e.child)
		}
		data, err := p.Get(e.child.Page)
		if err != nil {
			t.Fatalf("load leaf %d: %v", e.child.Page, err)
		}
		n := node.NewNode(data)
		for i := uint16(0); i < n.Count(); i++ {
			entry, err := n.GetLeafEntry(i)
			if err != nil {
				t.Fatalf("leaf entry %d: %v", i, err)
			}
			got = append(got, string(entry.Key))
		}
	}

	expected := []string{"a0", "b0", "b1", "b2", "b3"}
	if len(got) != len(expected) {
		t.Fatalf("expected %d keys, got %d (%v)", len(expected), len(got), got)
	}
	for i, k := range expected {
		if got[i] != k {
			t.Fatalf("key[%d] = %q, want %q (all=%v)", i, got[i], k, got)
		}
	}
}

func TestShortestSeparatorBE8Bounds(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	left := make([]byte, 8)
	right := make([]byte, 8)

	for i := 0; i < 200000; i++ {
		a := rng.Uint64()
		b := rng.Uint64()
		if a == b {
			continue
		}
		if a > b {
			a, b = b, a
		}
		binary.BigEndian.PutUint64(left, a)
		binary.BigEndian.PutUint64(right, b)

		sep := shortestSeparator(left, right)
		if bytes.Compare(sep, left) <= 0 {
			t.Fatalf("separator not > left: left=%x right=%x sep=%x", left, right, sep)
		}
		if bytes.Compare(sep, right) > 0 {
			t.Fatalf("separator > right: left=%x right=%x sep=%x", left, right, sep)
		}
	}
}

func TestMergeLeaf_SplitKeysDoNotAliasBatchKeys(t *testing.T) {
	z := New(nil, nil)
	z.SetOuterLeavesInValueLog(true)
	z.SetLeafPageLog(&stubLeafPageLog{})

	oldData := make([]byte, page.PageSize)
	oldNode := node.NewNode(oldData)
	oldNode.SetType(page.PageTypeLeaf)
	oldNode.UpdateChecksum()

	prefix := bytes.Repeat([]byte{'k'}, 1022)
	value := bytes.Repeat([]byte("v"), 8)
	ops := make([]batch.Entry, 0, 240)
	for i := 0; i < 240; i++ {
		key := make([]byte, len(prefix)+2)
		copy(key, prefix)
		binary.BigEndian.PutUint16(key[len(prefix):], uint16(i))
		ops = append(ops, batch.Entry{Type: batch.OpPut, Key: key, Value: value})
	}

	builder := z.newLeafBuilder(make([]byte, page.PageSize), ops)
	builder.SetPageID(0)
	scratch := newMergeScratch()

	var metrics adaptive.Metrics
	_, splits, err := z.mergeLeaf(oldNode, builder, ops, &metrics, scratch, false)
	if err != nil {
		t.Fatalf("mergeLeaf failed: %v", err)
	}
	if len(splits) == 0 {
		t.Fatalf("expected large keys to force at least one split")
	}

	wantKeys := make([][]byte, len(splits))
	for i := range splits {
		wantKeys[i] = append([]byte(nil), splits[i].Key...)
	}

	for i := range ops {
		for j := range ops[i].Key {
			ops[i].Key[j] ^= 0xff
		}
	}

	for i := range splits {
		if !bytes.Equal(splits[i].Key, wantKeys[i]) {
			t.Fatalf("split key %d mutated with source key buffer", i)
		}
	}
}

func TestApplyScratch_ReusedAcrossAcquireRelease(t *testing.T) {
	z := New(nil, nil)

	first := z.acquireApplyScratch()
	key := first.cloneSplitKey([]byte("split-key"))
	if string(key) != "split-key" {
		t.Fatalf("cloneSplitKey returned %q", key)
	}
	z.releaseApplyScratch(first)

	second := z.acquireApplyScratch()
	if second != first {
		t.Fatalf("expected apply scratch reuse, got different instance")
	}
	if got := len(second.splitKeyArena); got != 0 {
		t.Fatalf("len(splitKeyArena)=%d want 0 after reset", got)
	}
	z.releaseApplyScratch(second)
}

func TestApplyScratch_TrimsOversizedArena(t *testing.T) {
	z := New(nil, nil)

	s := z.acquireApplyScratch()
	s.splitKeyArena = make([]byte, 0, mergeSplitKeyArenaKeepCap+1024)
	z.releaseApplyScratch(s)

	reused := z.acquireApplyScratch()
	if cap(reused.splitKeyArena) > mergeSplitKeyArenaKeepCap {
		t.Fatalf("cap(splitKeyArena)=%d exceeds keep cap %d", cap(reused.splitKeyArena), mergeSplitKeyArenaKeepCap)
	}
	z.releaseApplyScratch(reused)
}

func TestApplyScratch_ReusesOuterLeafBuildPages(t *testing.T) {
	z := New(nil, nil)

	first := z.acquireApplyScratch()
	p := first.acquireOuterLeafBuildPage()
	first.releaseOuterLeafBuildPage(p)
	z.releaseApplyScratch(first)

	second := z.acquireApplyScratch()
	reused := second.acquireOuterLeafBuildPage()
	if reused != p {
		t.Fatalf("expected outer-leaf build page reuse across apply scratch lifecycle")
	}
	second.releaseOuterLeafBuildPage(reused)
	z.releaseApplyScratch(second)
}

func TestApplyScratch_TrimsOversizedOuterLeafBuildPageCache(t *testing.T) {
	z := New(nil, nil)

	s := z.acquireApplyScratch()
	s.outerLeafBuildPages = make([]*outerLeafBuildPage, 0, mergeOuterLeafPageKeepCap+32)
	for i := 0; i < mergeOuterLeafPageKeepCap+16; i++ {
		s.outerLeafBuildPages = append(s.outerLeafBuildPages, &outerLeafBuildPage{})
	}
	z.releaseApplyScratch(s)

	reused := z.acquireApplyScratch()
	if cap(reused.outerLeafBuildPages) > mergeOuterLeafPageKeepCap {
		t.Fatalf("cap(outerLeafBuildPages)=%d exceeds keep cap %d", cap(reused.outerLeafBuildPages), mergeOuterLeafPageKeepCap)
	}
	z.releaseApplyScratch(reused)
}
