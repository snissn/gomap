package zipper

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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

type benchmarkCyclingAllocator struct {
	ids  []uint64
	next int
}

func (a *benchmarkCyclingAllocator) reset() {
	a.next = 0
}

func (a *benchmarkCyclingAllocator) Alloc(hint uint64) (uint64, error) {
	if len(a.ids) == 0 {
		return 0, fmt.Errorf("zipper benchmark allocator has no page IDs")
	}
	id := a.ids[a.next%len(a.ids)]
	a.next++
	return id, nil
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
	mu             sync.Mutex
	next           uint32
	pages          map[page.LeafLogPtr][]byte
	batchLens      []int
	singleCalls    int
	readCalls      int
	discardAppends bool
}

func newBatchMemoryLeafPageStore() *batchMemoryLeafPageStore {
	return &batchMemoryLeafPageStore{pages: make(map[page.LeafLogPtr][]byte)}
}

func (s *batchMemoryLeafPageStore) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.singleCalls++
	if s.next == 0 {
		s.next = 4
	}
	ptr := page.LeafLogPtr{FileID: 1, Offset: uint64(s.next), RecordLengthHint: page.PageSize}
	s.next += page.PageSize + 32
	if !s.discardAppends {
		s.pages[ptr] = append([]byte(nil), leafPage...)
	}
	return ptr, nil
}

func (s *batchMemoryLeafPageStore) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchLens = append(s.batchLens, len(leafPages))
	if s.next == 0 {
		s.next = 4
	}
	offset := uint64(s.next)
	s.next += page.PageSize + 32
	ptrs := make([]page.LeafLogPtr, len(leafPages))
	for i, leafPage := range leafPages {
		ptr := page.LeafLogPtr{FileID: 1, Offset: offset, RecordLengthHint: page.ValuePtrMarkGrouped(page.PageSize, uint8(i)), SubIndex: uint16(i)}
		ptrs[i] = ptr
		if !s.discardAppends {
			s.pages[ptr] = append([]byte(nil), leafPage...)
		}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.pages[leafPtr]
	if !ok {
		return nil, io.EOF
	}
	s.readCalls++
	return append([]byte(nil), data...), nil
}

func (s *batchMemoryLeafPageStore) ReadUnsafeToWithCacheHit(ptr page.ValuePtr, dst []byte) ([]byte, bool, bool, error) {
	leafPtr, err := page.LeafLogPtrFromValuePtr(ptr)
	if err != nil {
		return nil, false, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.pages[leafPtr]
	if !ok {
		return nil, false, false, io.EOF
	}
	s.readCalls++
	if cap(dst) >= len(data) {
		out := dst[:len(data)]
		copy(out, data)
		return out, true, false, nil
	}
	return append([]byte(nil), data...), false, false, nil
}

func (s *batchMemoryLeafPageStore) ReadLeafLogPageUnsafeTo(ptr page.LeafLogPtr, dst []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	z.beginLeafRefCache(nil)
	defer z.endLeafRefCache()

	data := make([]byte, page.PageSize)
	b := node.NewBuilder(data, page.PageTypeLeaf)
	b.SetPageID(0)
	if err := b.AddLeafEntry([]byte("k"), []byte("v"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry: %v", err)
	}
	b.FinishNoNode()

	leafID, err := z.persistLeafPage(b, nil)
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

func TestZipperLeafRefCacheOwnsPersistedLeafBytes(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetOuterLeavesInValueLog(true)
	z.SetLeafPageLog(&stubLeafPageLog{})
	z.SetLeafPageReader(&countingLeafPageReader{})
	z.beginLeafRefCache(nil)
	defer z.endLeafRefCache()

	data := make([]byte, page.PageSize)
	b := node.NewBuilder(data, page.PageTypeLeaf)
	b.SetPageID(0)
	if err := b.AddLeafEntry([]byte("k"), []byte("v"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("AddLeafEntry: %v", err)
	}
	b.FinishNoNode()

	leafID, err := z.persistLeafPage(b, nil)
	if err != nil {
		t.Fatalf("persistLeafPage: %v", err)
	}

	// Simulate the scratch/builder backing buffer being reused for a non-leaf
	// page after the leaf is persisted. loadNodeRef must still see the original
	// cached leaf bytes, not the mutated caller buffer.
	mutated := node.NewNode(data)
	mutated.SetType(page.PageTypeInternal)
	mutated.UpdateChecksum()

	loaded, fromPager, leafScratch, leafScratchRef, loadSource, err := z.loadNodeRef(leafID, nil)
	if err != nil {
		t.Fatalf("loadNodeRef after caller buffer reuse: %v", err)
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
	if got := loaded.Type(); got != page.PageTypeLeaf {
		t.Fatalf("loaded.Type()=%d want %d", got, page.PageTypeLeaf)
	}
}

func TestZipperLeafRefCacheReusesOwnedPagesWithoutServingScratchMutations(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetOuterLeavesInValueLog(true)
	z.SetLeafPageLog(&stubLeafPageLog{})
	reader := &countingLeafPageReader{}
	z.SetLeafPageReader(reader)

	writeLeaf := func(dst []byte, key, val string) {
		t.Helper()
		clear(dst)
		b := node.NewBuilder(dst, page.PageTypeLeaf)
		b.SetPageID(0)
		if err := b.AddLeafEntry([]byte(key), []byte(val), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%q): %v", key, err)
		}
		b.FinishNoNode()
	}
	assertCachedLeaf := func(ref page.ChildRef, wantKey, wantVal string) {
		t.Helper()
		loaded, fromPager, leafScratch, leafScratchRef, loadSource, err := z.loadNodeRef(ref, nil)
		if err != nil {
			t.Fatalf("loadNodeRef(%q): %v", wantKey, err)
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
		gotKey, gotVal, _, flags, err := loaded.GetLeafEntryView(0)
		if err != nil {
			t.Fatalf("GetLeafEntryView: %v", err)
		}
		if flags != node.FlagInline {
			t.Fatalf("flags=0x%x want inline value", flags)
		}
		if string(gotKey) != wantKey || string(gotVal) != wantVal {
			t.Fatalf("cached leaf entry=(%q,%q), want (%q,%q)", gotKey, gotVal, wantKey, wantVal)
		}
	}
	cachedBytePtrs := func() map[*byte]struct{} {
		t.Helper()
		z.leafRefCacheMu.RLock()
		defer z.leafRefCacheMu.RUnlock()
		ptrs := make(map[*byte]struct{}, len(z.leafRefCache))
		for _, cached := range z.leafRefCache {
			if len(cached) != page.PageSize {
				t.Fatalf("cached len=%d want %d", len(cached), page.PageSize)
			}
			ptrs[&cached[0]] = struct{}{}
		}
		return ptrs
	}

	source := make([]byte, page.PageSize)
	scratch := z.acquireApplyScratch()
	z.beginLeafRefCache(scratch)

	writeLeaf(source, "first", "one")
	firstRef, err := z.persistLeafPageData(source, nil)
	if err != nil {
		t.Fatalf("persist first leaf: %v", err)
	}
	writeLeaf(source, "second", "two")
	secondRef, err := z.persistLeafPageData(source, nil)
	if err != nil {
		t.Fatalf("persist second leaf: %v", err)
	}

	// Reuse the caller scratch for an incompatible page after both persists. The
	// cache must keep serving the two leaf snapshots it owns.
	clear(source)
	mutated := node.NewNode(source)
	mutated.SetType(page.PageTypeInternal)
	mutated.UpdateChecksum()

	assertCachedLeaf(firstRef, "first", "one")
	assertCachedLeaf(secondRef, "second", "two")
	firstApplyCachePtrs := cachedBytePtrs()
	z.endLeafRefCache()
	z.releaseApplyScratch(scratch)

	reusedScratch := z.acquireApplyScratch()
	if reusedScratch != scratch {
		t.Fatalf("expected apply scratch reuse")
	}
	z.beginLeafRefCache(reusedScratch)
	writeLeaf(source, "third", "three")
	thirdRef, err := z.persistLeafPageData(source, nil)
	if err != nil {
		t.Fatalf("persist third leaf: %v", err)
	}

	z.leafRefCacheMu.RLock()
	thirdCached := z.leafRefCache[thirdRef.Log]
	z.leafRefCacheMu.RUnlock()
	if _, ok := firstApplyCachePtrs[&thirdCached[0]]; !ok {
		t.Fatalf("expected second apply to reuse a prior cache-owned page buffer")
	}

	clear(source)
	mutated = node.NewNode(source)
	mutated.SetType(page.PageTypeInternal)
	mutated.UpdateChecksum()
	assertCachedLeaf(thirdRef, "third", "three")
	z.endLeafRefCache()
	z.releaseApplyScratch(reusedScratch)

	if got := reader.calls.Load(); got != 0 {
		t.Fatalf("leafPageReader calls=%d want 0", got)
	}
}

func TestZipperLeafRefCacheAdoptsBuildPagesUntilCacheClose(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetOuterLeavesInValueLog(true)
	z.SetLeafPageLog(&stubLeafPageLog{})
	reader := &countingLeafPageReader{}
	z.SetLeafPageReader(reader)

	writeLeaf := func(dst []byte, key, val string) {
		t.Helper()
		clear(dst)
		b := node.NewBuilder(dst, page.PageTypeLeaf)
		b.SetPageID(0)
		if err := b.AddLeafEntry([]byte(key), []byte(val), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%q): %v", key, err)
		}
		b.FinishNoNode()
	}

	scratch := z.acquireApplyScratch()
	z.beginLeafRefCache(scratch)

	pageBuf := scratch.acquireLeafRefCacheBuildPage()
	writeLeaf(pageBuf, "first", "one")
	ref, err := z.persistLeafPageDataWithCacheOwnership(pageBuf, nil, leafPageCacheAdoptOwned)
	if err != nil {
		t.Fatalf("persist leaf: %v", err)
	}

	reusedWhileActive := scratch.acquireLeafRefCacheBuildPage()
	if &reusedWhileActive[0] == &pageBuf[0] {
		t.Fatalf("active cache build page was reused before cache close")
	}

	loaded, fromPager, leafScratch, leafScratchRef, loadSource, err := z.loadNodeRef(ref, scratch)
	if err != nil {
		t.Fatalf("loadNodeRef: %v", err)
	}
	if leafScratchRef {
		releaseLeafPageScratch(scratch, leafScratch)
	}
	if fromPager {
		t.Fatalf("fromPager=%t want false", fromPager)
	}
	if loadSource != zipperNodeLoadLeafLogCache {
		t.Fatalf("loadSource=%d want leaf-log cache", loadSource)
	}
	gotKey, gotVal, _, flags, err := loaded.GetLeafEntryView(0)
	if err != nil {
		t.Fatalf("GetLeafEntryView: %v", err)
	}
	if flags != node.FlagInline || string(gotKey) != "first" || string(gotVal) != "one" {
		t.Fatalf("cached leaf entry=(%q,%q flags=0x%x), want (first,one inline)", gotKey, gotVal, flags)
	}

	z.endLeafRefCache()
	z.releaseApplyScratch(scratch)

	reusedScratch := z.acquireApplyScratch()
	z.beginLeafRefCache(reusedScratch)
	reusedAfterClose := reusedScratch.acquireLeafRefCacheBuildPage()
	if &reusedAfterClose[0] != &pageBuf[0] && &reusedAfterClose[0] != &reusedWhileActive[0] {
		t.Fatalf("expected cache-owned build page reuse after cache close")
	}
	z.endLeafRefCache()
	z.releaseApplyScratch(reusedScratch)

	if got := reader.calls.Load(); got != 0 {
		t.Fatalf("leafPageReader calls=%d want 0", got)
	}
}

func TestMergeScratchLeafRefCacheSpillsOverflowToGlobalPool(t *testing.T) {
	globalLeafRefCachePages.mu.Lock()
	savedGlobal := append([]*leafRefCachePage(nil), globalLeafRefCachePages.pages...)
	clear(globalLeafRefCachePages.pages)
	globalLeafRefCachePages.pages = globalLeafRefCachePages.pages[:0]
	globalLeafRefCachePages.mu.Unlock()
	t.Cleanup(func() {
		globalLeafRefCachePages.mu.Lock()
		clear(globalLeafRefCachePages.pages)
		globalLeafRefCachePages.pages = append(globalLeafRefCachePages.pages[:0], savedGlobal...)
		globalLeafRefCachePages.mu.Unlock()
	})

	source := bytes.Repeat([]byte{0x7b}, page.PageSize)
	firstScratch := newMergeScratch()
	pageCount := mergeLeafRefCachePageKeep + mergeLeafRefCachePagesPerChunk
	seen := make(map[*byte]struct{}, pageCount)
	overflow := make(map[*byte]struct{}, mergeLeafRefCachePagesPerChunk)
	for i := 0; i < pageCount; i++ {
		buf := firstScratch.cloneLeafRefCachePage(source)
		ptr := &buf[0]
		if _, ok := seen[ptr]; ok {
			t.Fatalf("active leaf-ref cache page reused before release at index %d", i)
		}
		seen[ptr] = struct{}{}
		if i >= mergeLeafRefCachePageKeep {
			overflow[ptr] = struct{}{}
		}
	}
	firstScratch.releaseLeafRefCachePages()

	globalLeafRefCachePages.mu.Lock()
	globalCount := len(globalLeafRefCachePages.pages)
	globalLeafRefCachePages.mu.Unlock()
	if globalCount != mergeLeafRefCachePagesPerChunk {
		t.Fatalf("global leaf-ref page pool len=%d want %d", globalCount, mergeLeafRefCachePagesPerChunk)
	}

	secondScratch := newMergeScratch()
	for i := 0; i < mergeLeafRefCachePagesPerChunk; i++ {
		buf := secondScratch.acquireLeafRefCacheBuildPage()
		if _, ok := overflow[&buf[0]]; !ok {
			t.Fatalf("second scratch page %d did not reuse overflow page", i)
		}
	}
	secondScratch.releaseLeafRefCachePages()
}

func TestZipperCachePersistedLeafPageNoopAvoidsLockWhenCacheInactive(t *testing.T) {
	z := &Zipper{}
	z.leafRefCacheMu.Lock()
	defer z.leafRefCacheMu.Unlock()

	done := make(chan struct{})
	go func() {
		z.cachePersistedLeafPage(page.LeafLogPtr{FileID: 1, Offset: 128}, []byte("leaf"), leafPageCacheClone)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cachePersistedLeafPage blocked on leafRefCacheMu with inactive cache")
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

func TestZipperSplitPreservesLeafRevisionEncoding(t *testing.T) {
	for _, tc := range []struct {
		name             string
		outerLeafLog     bool
		hasEarlyRevision bool
	}{
		{name: "pager_legacy", outerLeafLog: false},
		{name: "pager_mixed_revisions", outerLeafLog: false, hasEarlyRevision: true},
		{name: "outer_leaf_log_legacy", outerLeafLog: true},
		{name: "outer_leaf_log_mixed_revisions", outerLeafLog: true, hasEarlyRevision: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()

			z := New(p, &MockAllocator{p: p})
			var leafLog *memoryLeafPageStore
			if tc.outerLeafLog {
				z.SetOuterLeavesInValueLog(true)
				leafLog = newMemoryLeafPageStore(z)
				z.SetLeafPageLog(leafLog)
				z.SetLeafPageReader(leafLog)
			}
			rootID, err := p.Alloc(1)
			if err != nil {
				t.Fatal(err)
			}
			rootData, err := p.Get(rootID)
			if err != nil {
				t.Fatal(err)
			}
			root := node.NewNode(rootData)
			root.SetPageID(rootID)
			root.SetType(page.PageTypeLeaf)
			root.UpdateChecksum()

			b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
			defer b.Close()
			for i := 0; i < 200; i++ {
				key := []byte(fmt.Sprintf("key-%03d", i))
				if tc.hasEarlyRevision && i == 0 {
					if err := b.SetWithRevision(key, []byte("value"), 1); err != nil {
						t.Fatal(err)
					}
				} else if err := b.Set(key, []byte("value")); err != nil {
					t.Fatal(err)
				}
			}
			newRootID, _, _, err := z.Apply(rootID, b)
			if err != nil {
				t.Fatal(err)
			}

			if tc.outerLeafLog {
				if len(leafLog.pages) < 2 {
					t.Fatalf("leaf-log pages=%d, want split", len(leafLog.pages))
				}
				for _, data := range leafLog.pages {
					leaf := node.NewNodeView(data)
					if got := leaf.LeafEntryRevisionsEnabled(); got != tc.hasEarlyRevision {
						t.Fatalf("leaf revision flag=%v, want %v", got, tc.hasEarlyRevision)
					}
				}
				return
			}

			rootData, err = p.Get(newRootID)
			if err != nil {
				t.Fatal(err)
			}
			root = node.NewNode(rootData)
			if root.Type() != page.PageTypeInternal || root.Count() < 2 {
				t.Fatalf("root type/count=%v/%d, want split internal root", root.Type(), root.Count())
			}
			for i := uint16(0); i < root.Count(); i++ {
				ref, err := root.GetInternalChildRef(i)
				if err != nil {
					t.Fatal(err)
				}
				data, err := p.Get(ref.Page)
				if err != nil {
					t.Fatal(err)
				}
				if got := node.NewNode(data).LeafEntryRevisionsEnabled(); got != tc.hasEarlyRevision {
					t.Fatalf("leaf %d revision flag=%v, want %v", i, got, tc.hasEarlyRevision)
				}
			}
		})
	}
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
	_, splits, err := z.mergeLeaf(oldNode, builder, ops, nil, &metrics, scratch, false, false, applyRunConfig{})
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

func TestZipperMergeScratch_ReusesAndClearsPendingLeafPageScratch(t *testing.T) {
	s := newMergeScratch()
	buf := s.acquirePendingLeafPagePersists(3)
	buf = append(buf,
		pendingLeafPagePersist{data: []byte("page-a"), pooled: &outerLeafBuildPage{}},
		pendingLeafPagePersist{data: []byte("page-b"), root: true, splitIdx: 1, pooled: &outerLeafBuildPage{}},
		pendingLeafPagePersist{data: []byte("page-c"), root: true, splitIdx: 2, pooled: &outerLeafBuildPage{}},
	)
	s.releasePendingLeafPagePersists(buf)

	reused := s.acquirePendingLeafPagePersists(2)
	if cap(reused) < 3 {
		t.Fatalf("reused cap=%d want >=3", cap(reused))
	}
	reused = append(reused, pendingLeafPagePersist{data: []byte("page-d"), pooled: &outerLeafBuildPage{}})
	s.releasePendingLeafPagePersists(reused)

	reused = s.acquirePendingLeafPagePersists(2)
	full := reused[:cap(reused)]
	for i, entry := range full {
		if entry.data != nil || entry.pooled != nil || entry.root || entry.splitIdx != 0 {
			t.Fatalf("pending leaf persist scratch retained entry %d: %+v", i, entry)
		}
	}
	s.releasePendingLeafPagePersists(reused)
}

func TestZipperMergeScratch_ReusesAndClearsLeafPageBatchScratch(t *testing.T) {
	s := newMergeScratch()
	buf := s.acquireLeafPageBatch(3)
	buf = append(buf, []byte("page-a"), []byte("page-b"), []byte("page-c"))
	s.releaseLeafPageBatch(buf)

	reused := s.acquireLeafPageBatch(2)
	if cap(reused) < 3 {
		t.Fatalf("reused cap=%d want >=3", cap(reused))
	}
	reused = append(reused, []byte("page-d"))
	s.releaseLeafPageBatch(reused)

	reused = s.acquireLeafPageBatch(2)
	for i, page := range reused[:cap(reused)] {
		if page != nil {
			t.Fatalf("leaf page batch scratch retained page %d", i)
		}
	}
	s.releaseLeafPageBatch(reused)
}

func TestZipperMergeScratch_ReusesSplitArenaSegments(t *testing.T) {
	s := newMergeScratch()
	firstSegment := newMergeSplitSegment(s)
	first := firstSegment.append(Split{Key: []byte("a"), Ref: page.PageChildRef(1)})
	if len(first) != 1 || !bytes.Equal(first[0].Key, []byte("a")) {
		t.Fatalf("unexpected first split segment: %+v", first)
	}

	secondSegment := newMergeSplitSegment(s)
	second := secondSegment.append(Split{Key: []byte("b"), Ref: page.PageChildRef(2)})
	if len(second) != 1 || !bytes.Equal(second[0].Key, []byte("b")) {
		t.Fatalf("unexpected second split segment: %+v", second)
	}
	if len(first) != 1 || !bytes.Equal(first[0].Key, []byte("a")) {
		t.Fatalf("first segment changed after second segment append: %+v", first)
	}

	extendedFirst := firstSegment.append(Split{Key: []byte("c"), Ref: page.PageChildRef(3)})
	if len(extendedFirst) != 2 {
		t.Fatalf("extended first segment len=%d want 2", len(extendedFirst))
	}
	if !bytes.Equal(extendedFirst[0].Key, []byte("a")) || !bytes.Equal(extendedFirst[1].Key, []byte("c")) {
		t.Fatalf("extended first segment mismatch: %+v", extendedFirst)
	}
	if len(s.splitArena) != 2 {
		t.Fatalf("fallback append should not grow arena, len=%d", len(s.splitArena))
	}

	arena := s.splitArena
	s.reset()
	if len(s.splitArena) != 0 {
		t.Fatalf("split arena len=%d want 0 after reset", len(s.splitArena))
	}
	for i, split := range arena[:2] {
		if split.Key != nil || split.Ref != (page.ChildRef{}) {
			t.Fatalf("split arena retained slot %d: %+v", i, split)
		}
	}

	s.splitArena = make([]Split, 0, mergeSplitArenaKeepCap+1)
	s.reset()
	if cap(s.splitArena) > mergeSplitArenaKeepCap {
		t.Fatalf("oversized split arena cap=%d exceeds keep %d", cap(s.splitArena), mergeSplitArenaKeepCap)
	}
}

func TestZipperMergeScratch_ReusesChildRefBatchScratchWithoutClearing(t *testing.T) {
	s := newMergeScratch()
	buf := s.acquireChildRefBatch(2)
	buf = append(buf, page.PageChildRef(1), page.PageChildRef(2))
	s.releaseChildRefBatch(buf)

	reused := s.acquireChildRefBatch(2)
	if len(reused) != 0 {
		t.Fatalf("reused len=%d want 0", len(reused))
	}
	if cap(reused) < 2 {
		t.Fatalf("reused cap=%d want >=2", cap(reused))
	}
	// page.ChildRef contains no heap pointers; release intentionally avoids a
	// full-cap clear so hot batch persistence does not pay avoidable memclr cost.
	reused = append(reused, page.PageChildRef(3), page.PageChildRef(4))
	s.releaseChildRefBatch(reused)
}

func TestZipperMergeScratch_TrimsPendingLeafPageScratchCaches(t *testing.T) {
	z := New(nil, nil)
	s := z.acquireApplyScratch()
	for i := 0; i < mergePendingLeafPersistKeep+16; i++ {
		s.releasePendingLeafPagePersists(make([]pendingLeafPagePersist, 0, 1))
	}
	for i := 0; i < mergeLeafPageBatchKeep+16; i++ {
		s.releaseLeafPageBatch(make([][]byte, 0, 1))
	}
	for i := 0; i < mergeChildRefBatchKeep+16; i++ {
		s.releaseChildRefBatch(make([]page.ChildRef, 0, 1))
	}
	s.spanWorkerRangeScratch = make([]ReadOnlyLeafSpanWorkerRange, 0, mergeSpanNativeRangeScratchMaxCap+1)
	s.spanWorkerScratchScratch = make([]*mergeScratch, 0, mergeSpanNativeRangeScratchMaxCap+1)
	s.spanRangeMetricsScratch = make([]adaptive.Metrics, 0, mergeSpanNativeRangeScratchMaxCap+1)
	s.spanRangeRetiredScratch = make([][]uint64, 0, mergeSpanNativeRangeScratchMaxCap+1)
	s.spanRangeSplitsScratch = make([]spanNativeLeafSplitRange, 0, mergeSpanNativeRangeScratchMaxCap+1)
	s.spanRootRefsScratch = make([]Split, 0, mergeSpanNativeRootRefMaxCap+1)
	z.releaseApplyScratch(s)

	reused := z.acquireApplyScratch()
	if len(reused.pendingLeafPersistScratch) > mergePendingLeafPersistKeep {
		t.Fatalf("pending leaf persist scratch len=%d exceeds keep %d", len(reused.pendingLeafPersistScratch), mergePendingLeafPersistKeep)
	}
	if len(reused.leafPageBatchScratch) > mergeLeafPageBatchKeep {
		t.Fatalf("leaf page batch scratch len=%d exceeds keep %d", len(reused.leafPageBatchScratch), mergeLeafPageBatchKeep)
	}
	if len(reused.childRefBatchScratch) > mergeChildRefBatchKeep {
		t.Fatalf("child ref batch scratch len=%d exceeds keep %d", len(reused.childRefBatchScratch), mergeChildRefBatchKeep)
	}
	if cap(reused.spanWorkerRangeScratch) > mergeSpanNativeRangeScratchMaxCap {
		t.Fatalf("span worker range scratch cap=%d exceeds max %d", cap(reused.spanWorkerRangeScratch), mergeSpanNativeRangeScratchMaxCap)
	}
	if cap(reused.spanWorkerScratchScratch) > mergeSpanNativeRangeScratchMaxCap {
		t.Fatalf("span worker scratch slots cap=%d exceeds max %d", cap(reused.spanWorkerScratchScratch), mergeSpanNativeRangeScratchMaxCap)
	}
	if cap(reused.spanRangeMetricsScratch) > mergeSpanNativeRangeScratchMaxCap {
		t.Fatalf("span range metrics cap=%d exceeds max %d", cap(reused.spanRangeMetricsScratch), mergeSpanNativeRangeScratchMaxCap)
	}
	if cap(reused.spanRangeRetiredScratch) > mergeSpanNativeRangeScratchMaxCap {
		t.Fatalf("span range retired cap=%d exceeds max %d", cap(reused.spanRangeRetiredScratch), mergeSpanNativeRangeScratchMaxCap)
	}
	if cap(reused.spanRangeSplitsScratch) > mergeSpanNativeRangeScratchMaxCap {
		t.Fatalf("span range splits cap=%d exceeds max %d", cap(reused.spanRangeSplitsScratch), mergeSpanNativeRangeScratchMaxCap)
	}
	if cap(reused.spanRootRefsScratch) > mergeSpanNativeRootRefMaxCap {
		t.Fatalf("span root refs cap=%d exceeds max %d", cap(reused.spanRootRefsScratch), mergeSpanNativeRootRefMaxCap)
	}
	z.releaseApplyScratch(reused)
}

func TestZipperMergeScratch_ConcurrentPendingLeafPageScratchReuse(t *testing.T) {
	s := newMergeScratch()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 128; i++ {
				pending := s.acquirePendingLeafPagePersists(4)
				pending = append(pending, pendingLeafPagePersist{data: []byte{byte(id), byte(i)}})
				pages := s.acquireLeafPageBatch(4)
				pages = append(pages, []byte{byte(i)})
				refs := s.acquireChildRefBatch(4)
				refs = append(refs, page.PageChildRef(uint64(i)))
				s.releaseChildRefBatch(refs)
				s.releaseLeafPageBatch(pages)
				s.releasePendingLeafPagePersists(pending)
			}
		}(g)
	}
	wg.Wait()
}

func buildCompactEstimateLeaf(t *testing.T) []byte {
	t.Helper()
	compactLeaf := make([]byte, page.PageSize)
	builder := node.NewBuilderWithOptions(compactLeaf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	for i := 0; i < 4; i++ {
		if err := builder.AddLeafEntry([]byte{byte('k'), byte('0' + i)}, []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	builder.FinishNoNode()
	return compactLeaf
}

func TestEstimateSpanNativeLeafLogPayloadArenaCapUsesCompactableRatio(t *testing.T) {
	compactLeaf := buildCompactEstimateLeaf(t)
	compactLen, compacted := valuelog.MaybeCompactLeafLogPayloadLength(compactLeaf)
	if !compacted {
		t.Fatalf("test leaf did not compact")
	}
	nonCompactLeaf := bytes.Repeat([]byte{'x'}, page.PageSize)
	leafPages := [][]byte{compactLeaf, nonCompactLeaf, compactLeaf, nonCompactLeaf}
	got := estimateSpanNativeLeafLogPayloadArenaCap(leafPages)
	want := compactLen * 2
	if got != want {
		t.Fatalf("estimate=%d want %d", got, want)
	}
}

func TestEstimateSpanNativeLeafLogPayloadArenaCapScansUnsampledMixedPages(t *testing.T) {
	compactLeaf := buildCompactEstimateLeaf(t)
	compactLen, compacted := valuelog.MaybeCompactLeafLogPayloadLength(compactLeaf)
	if !compacted {
		t.Fatalf("test leaf did not compact")
	}
	nonCompactLeaf := bytes.Repeat([]byte{'x'}, page.PageSize)
	leafPages := make([][]byte, 0, 10)
	for i := 0; i < 8; i++ {
		leafPages = append(leafPages, nonCompactLeaf)
	}
	leafPages = append(leafPages, compactLeaf, compactLeaf)
	got := estimateSpanNativeLeafLogPayloadArenaCap(leafPages)
	want := compactLen * 2
	if got != want {
		t.Fatalf("estimate=%d want %d", got, want)
	}
}

type childRefPreparedBatchTestLog struct {
	called bool
	next   uint64
}

func (l *childRefPreparedBatchTestLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, fmt.Errorf("unexpected AppendLeafPage fallback")
}

func (l *childRefPreparedBatchTestLog) AppendPreparedLeafPageChildRefs(leafPages [][]byte, _ [][]byte, refs []page.ChildRef) ([]page.ChildRef, error) {
	l.called = true
	if cap(refs) < len(leafPages) {
		refs = make([]page.ChildRef, len(leafPages))
	} else {
		refs = refs[:len(leafPages)]
	}
	if l.next == 0 {
		l.next = 4
	}
	for i := range leafPages {
		ptr := page.LeafLogPtr{FileID: 7, Offset: l.next, RecordLengthHint: page.PageSize, SubIndex: uint16(i)}
		refs[i] = page.LeafLogChildRef(ptr)
		l.next += page.PageSize + 32
	}
	return refs, nil
}

func TestPersistPreparedLeafPageBatchDataUsesChildRefBatcher(t *testing.T) {
	z := New(nil, nil)
	log := &childRefPreparedBatchTestLog{}
	leafPages := [][]byte{
		bytes.Repeat([]byte{'a'}, page.PageSize),
		bytes.Repeat([]byte{'b'}, page.PageSize),
		bytes.Repeat([]byte{'c'}, page.PageSize),
	}
	payloads := [][]byte{[]byte("payload-a"), []byte("payload-b"), []byte("payload-c")}
	reusable := make([]page.ChildRef, 0, len(leafPages))
	var metrics adaptive.Metrics

	refs, err := z.persistPreparedLeafPageBatchDataToLog(log, leafPages, payloads, reusable, &metrics)
	if err != nil {
		t.Fatalf("persistPreparedLeafPageBatchDataToLog: %v", err)
	}
	requireChildRefPreparedBatchTestRefs(t, refs, leafPages, reusable, log, metrics)
}

func TestPersistLeafPageBatchDataDetectsChildRefPreparedBatcher(t *testing.T) {
	z := New(nil, nil)
	log := &childRefPreparedBatchTestLog{}
	leafPages := [][]byte{
		buildCompactEstimateLeaf(t),
		buildCompactEstimateLeaf(t),
		buildCompactEstimateLeaf(t),
	}
	reusable := make([]page.ChildRef, 0, len(leafPages))
	var metrics adaptive.Metrics
	gate := newSpanNativeLeafLogOutputGate(0)

	refs, err := gate.persistLeafPageBatchDataToLog(z, log, leafPages, reusable, &metrics)
	if err != nil {
		t.Fatalf("persistLeafPageBatchDataToLog: %v", err)
	}
	requireChildRefPreparedBatchTestRefs(t, refs, leafPages, reusable, log, metrics)
}

func requireChildRefPreparedBatchTestRefs(t *testing.T, refs []page.ChildRef, leafPages [][]byte, reusable []page.ChildRef, log *childRefPreparedBatchTestLog, metrics adaptive.Metrics) {
	t.Helper()
	if !log.called {
		t.Fatalf("child-ref batch appender was not called")
	}
	if len(refs) != len(leafPages) {
		t.Fatalf("refs len=%d want %d", len(refs), len(leafPages))
	}
	if cap(refs) != cap(reusable) {
		t.Fatalf("refs cap=%d want reusable cap %d", cap(refs), cap(reusable))
	}
	if len(refs) > 0 && cap(reusable) > 0 {
		if &refs[:cap(refs)][0] != &reusable[:cap(reusable)][0] {
			t.Fatalf("refs did not reuse reusable backing array")
		}
	}
	for i, ref := range refs {
		if !ref.IsLeafLog() || ref.Log.FileID != 7 || ref.Log.SubIndex != uint16(i) {
			t.Fatalf("ref[%d]=%+v", i, ref)
		}
	}
	if metrics.ZipperLeafLogOutputAppendCalls != 1 || metrics.ZipperLeafLogOutputAppendPages != len(leafPages) {
		t.Fatalf("append metrics calls=%d pages=%d", metrics.ZipperLeafLogOutputAppendCalls, metrics.ZipperLeafLogOutputAppendPages)
	}
}

type benchmarkLeafBatchLog struct {
	next uint64
	ptrs []page.LeafLogPtr
}

func (l *benchmarkLeafBatchLog) AppendLeafPage(_ []byte) (page.LeafLogPtr, error) {
	if l.next == 0 {
		l.next = 4
	}
	ptr := page.LeafLogPtr{FileID: 1, Offset: l.next, RecordLengthHint: page.PageSize}
	l.next += page.PageSize + 32
	return ptr, nil
}

func (l *benchmarkLeafBatchLog) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if cap(l.ptrs) < len(leafPages) {
		l.ptrs = make([]page.LeafLogPtr, len(leafPages))
	}
	out := l.ptrs[:len(leafPages)]
	if l.next == 0 {
		l.next = 4
	}
	for i := range out {
		out[i] = page.LeafLogPtr{FileID: 1, Offset: l.next, RecordLengthHint: page.ValuePtrMarkGrouped(page.PageSize, uint8(i)), SubIndex: uint16(i)}
	}
	l.next += uint64(len(out)) * (page.PageSize + 32)
	return out, nil
}

func BenchmarkMergeLeafOuterLeafBatchScratch(b *testing.B) {
	z := New(nil, nil)
	z.SetOuterLeavesInValueLog(true)
	z.SetLeafPageLog(&benchmarkLeafBatchLog{})

	oldData := make([]byte, page.PageSize)
	oldNode := node.NewNode(oldData)
	oldNode.SetType(page.PageTypeLeaf)
	oldNode.UpdateChecksum()

	prefix := bytes.Repeat([]byte{'b'}, 96)
	value := bytes.Repeat([]byte{'v'}, 64)
	ops := make([]batch.Entry, 0, 512)
	for i := 0; i < 512; i++ {
		key := make([]byte, len(prefix)+8)
		copy(key, prefix)
		binary.BigEndian.PutUint64(key[len(prefix):], uint64(i))
		ops = append(ops, batch.Entry{Type: batch.OpPut, Key: key, Value: value})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var data [page.PageSize]byte
		builder := z.newLeafBuilder(data[:], ops)
		builder.SetPageID(0)
		scratch := z.acquireApplyScratch()
		var metrics adaptive.Metrics
		_, _, err := z.mergeLeaf(oldNode, builder, ops, nil, &metrics, scratch, true, false, applyRunConfig{})
		z.releaseApplyScratch(scratch)
		if err != nil {
			b.Fatalf("mergeLeaf: %v", err)
		}
	}
}

func BenchmarkReduceSpanNativeSplitLevelScratch(b *testing.B) {
	const (
		refCount      = 4096
		scratchPages  = 128
		benchmarkSeed = 0x3524
	)

	dir := b.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()

	ids := make([]uint64, scratchPages)
	for i := range ids {
		id, err := p.Alloc(1)
		if err != nil {
			b.Fatalf("alloc scratch page: %v", err)
		}
		ids[i] = id
	}
	alloc := &benchmarkCyclingAllocator{ids: ids}
	z := New(p, alloc)

	refs := make([]Split, refCount)
	for i := range refs {
		key := make([]byte, 16)
		binary.BigEndian.PutUint64(key[:8], benchmarkSeed)
		binary.BigEndian.PutUint64(key[8:], uint64(i))
		refs[i] = Split{Key: key, Ref: page.PageChildRef(uint64(i + 1))}
	}

	b.Run("heap", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			alloc.reset()
			var metrics adaptive.Metrics
			out, err := z.reduceSpanNativeSplitLevel(refs, &metrics, nil)
			if err != nil {
				b.Fatalf("reduceSpanNativeSplitLevel: %v", err)
			}
			if len(out) == 0 {
				b.Fatalf("empty reducer output")
			}
		}
	})
	b.Run("scratch", func(b *testing.B) {
		scratch := newMergeScratch()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			alloc.reset()
			var metrics adaptive.Metrics
			out, err := z.reduceSpanNativeSplitLevel(refs, &metrics, scratch)
			if err != nil {
				b.Fatalf("reduceSpanNativeSplitLevel: %v", err)
			}
			if len(out) == 0 {
				b.Fatalf("empty reducer output")
			}
			scratch.reset()
		}
	})
}

func BenchmarkZipperApplyOuterLeafRandomOverwrite(b *testing.B) {
	const (
		keyCount  = 1 << 16
		batchSize = 8192
		valueSize = 128
	)

	dir := b.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()

	z := New(p, &MockAllocator{p: p})
	z.SetOuterLeavesInValueLog(true)
	store := newBatchMemoryLeafPageStore()
	z.SetLeafPageLog(store)
	z.SetLeafPageReader(store)

	rootID, err := p.Alloc(1)
	if err != nil {
		b.Fatalf("alloc root: %v", err)
	}
	data, err := p.Get(rootID)
	if err != nil {
		b.Fatalf("get root: %v", err)
	}
	root := node.NewNode(data)
	root.SetPageID(rootID)
	root.SetType(page.PageTypeLeaf)
	root.UpdateChecksum()

	keys := make([][]byte, keyCount)
	loadValue := bytes.Repeat([]byte{'v'}, valueSize)
	loadBatch := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	for i := 0; i < keyCount; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		keys[i] = key
		loadBatch.Set(key, loadValue)
	}
	rootID, _, _, err = z.Apply(rootID, loadBatch)
	loadBatch.Close()
	if err != nil {
		b.Fatalf("initial apply: %v", err)
	}

	updateValue := bytes.Repeat([]byte{'u'}, valueSize)
	updateBatch := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	for i := 0; i < batchSize; i++ {
		idx := (i*7919 + 17) & (keyCount - 1)
		updateBatch.Set(keys[idx], updateValue)
	}
	updateBatch.SortedEntries()
	defer updateBatch.Close()

	store.discardAppends = true
	store.readCalls = 0

	var leafMerges, leafLogLoads, leafLogWrites, leafBytesRead, leafBytesWritten int64
	b.ReportAllocs()
	b.SetBytes(int64(batchSize * (8 + valueSize)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, metrics, err := z.Apply(rootID, updateBatch)
		if err != nil {
			b.Fatalf("apply overwrite batch: %v", err)
		}
		leafMerges += int64(metrics.ZipperLeafMerges)
		leafLogLoads += int64(metrics.ZipperLeafLogNodeLoads)
		leafLogWrites += int64(metrics.ZipperLeafLogPagesWritten)
		leafBytesRead += int64(metrics.ZipperLeafLogNodeBytesRead)
		leafBytesWritten += int64(metrics.ZipperLeafLogPageBytesWritten)
	}
	b.StopTimer()

	n := float64(b.N)
	if n > 0 {
		b.ReportMetric(float64(batchSize), "batch_ops/op")
		b.ReportMetric(float64(leafMerges)/n, "leaf_merges/op")
		b.ReportMetric(float64(leafLogLoads)/n, "leaflog_loads/op")
		b.ReportMetric(float64(leafLogWrites)/n, "leaflog_writes/op")
		b.ReportMetric(float64(leafBytesRead)/n, "leaflog_read_B/op")
		b.ReportMetric(float64(leafBytesWritten)/n, "leaflog_write_B/op")
		b.ReportMetric(float64(store.readCalls)/n, "store_reads/op")
	}
}
