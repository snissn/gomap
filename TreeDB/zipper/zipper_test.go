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

type batchMemoryLeafPageStore struct {
	next        uint32
	pages       map[page.LeafLogPtr][]byte
	batchLens   []int
	singleCalls int
	readCalls   int
}

func newBatchMemoryLeafPageStore() *batchMemoryLeafPageStore {
	return &batchMemoryLeafPageStore{pages: make(map[page.LeafLogPtr][]byte)}
}

func (s *batchMemoryLeafPageStore) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	s.singleCalls++
	if s.next == 0 {
		s.next = 4
	}
	ptr := page.LeafLogPtr{FileID: 1, Offset: uint64(s.next), RecordLengthHint: page.PageSize}
	s.next += page.PageSize + 32
	s.pages[ptr] = append([]byte(nil), leafPage...)
	return ptr, nil
}

func (s *batchMemoryLeafPageStore) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	s.batchLens = append(s.batchLens, len(leafPages))
	if s.next == 0 {
		s.next = 4
	}
	offset := uint64(s.next)
	s.next += page.PageSize + 32
	ptrs := make([]page.LeafLogPtr, len(leafPages))
	for i, leafPage := range leafPages {
		ptr := page.LeafLogPtr{FileID: 1, Offset: offset, RecordLengthHint: page.PageSize, SubIndex: uint16(i)}
		ptrs[i] = ptr
		s.pages[ptr] = append([]byte(nil), leafPage...)
	}
	return ptrs, nil
}

func (s *batchMemoryLeafPageStore) Read(ptr page.ValuePtr) ([]byte, error) {
	return s.ReadUnsafe(ptr)
}

func (s *batchMemoryLeafPageStore) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	leafPtr, err := page.LeafLogPtrFromValuePtr(ptr)
	if err != nil {
		return nil, err
	}
	data, ok := s.pages[leafPtr]
	if !ok {
		return nil, io.EOF
	}
	s.readCalls++
	return append([]byte(nil), data...), nil
}

func (s *batchMemoryLeafPageStore) ReadLeafLogPageUnsafeTo(ptr page.LeafLogPtr, dst []byte) ([]byte, bool, error) {
	data, ok := s.pages[ptr]
	if !ok {
		return nil, false, io.EOF
	}
	s.readCalls++
	if cap(dst) >= len(data) {
		out := dst[:len(data)]
		copy(out, data)
		return out, true, nil
	}
	return append([]byte(nil), data...), false, nil
}

func buildOuterLeafInternalRoot(t *testing.T, z *Zipper) uint64 {
	t.Helper()

	rootID, err := z.pager.Alloc(1)
	if err != nil {
		t.Fatalf("alloc root: %v", err)
	}
	data, err := z.pager.Get(rootID)
	if err != nil {
		t.Fatalf("get root: %v", err)
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
		t.Fatalf("build root apply: %v", err)
	}
	rootData, err := z.pager.Get(newRootID)
	if err != nil {
		t.Fatalf("get new root: %v", err)
	}
	if got := node.NewNode(rootData).Type(); got != page.PageTypeInternal {
		t.Fatalf("new root type=%d want %d", got, page.PageTypeInternal)
	}
	return newRootID
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

func TestZipperApply_BatchesLiveOuterLeafWrites(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetOuterLeavesInValueLog(true)
	store := newBatchMemoryLeafPageStore()
	z.SetLeafPageLog(store)
	z.SetLeafPageReader(store)

	rootID, _ := p.Alloc(1)
	data, _ := p.Get(rootID)
	n := node.NewNode(data)
	n.SetPageID(rootID)
	n.SetType(page.PageTypeLeaf)
	n.UpdateChecksum()

	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	for i := 0; i < 400; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := []byte(fmt.Sprintf("val-%03d", i))
		b.Set(key, val)
	}

	newRootID, _, _, err := z.Apply(rootID, b)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	batched := false
	for _, n := range store.batchLens {
		if n > 1 {
			batched = true
			break
		}
	}
	if !batched {
		t.Fatalf("expected at least one multi-page AppendLeafPages call, batch lens=%v singleCalls=%d", store.batchLens, store.singleCalls)
	}
	if store.singleCalls != 0 {
		t.Fatalf("single leaf appends=%d, want batched path only", store.singleCalls)
	}

	tr := tree.New(p, store, newRootID)
	for _, idx := range []int{0, 199, 399} {
		key := []byte(fmt.Sprintf("key-%03d", idx))
		got, err := tr.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		want := []byte(fmt.Sprintf("val-%03d", idx))
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q)=%q want %q", key, got, want)
		}
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
