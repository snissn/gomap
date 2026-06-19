package zipper

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type failAfterAllocator struct {
	p         *pager.Pager
	remaining atomic.Int64
}

func newFailAfterAllocator(p *pager.Pager, remaining int64) *failAfterAllocator {
	a := &failAfterAllocator{p: p}
	a.remaining.Store(remaining)
	return a
}

func (a *failAfterAllocator) Alloc(uint64) (uint64, error) {
	if a.remaining.Add(-1) < 0 {
		return 0, errors.New("test allocator failure")
	}
	return a.p.Alloc(1)
}

type parallelSafeLeafPageReader struct {
	pages map[page.LeafLogPtr][]byte
}

func (r parallelSafeLeafPageReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return r.ReadUnsafe(ptr)
}

func (r parallelSafeLeafPageReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	leafPtr, err := page.LeafLogPtrFromValuePtr(ptr)
	if err != nil {
		return nil, err
	}
	data, ok := r.pages[leafPtr]
	if !ok {
		return nil, io.EOF
	}
	return append([]byte(nil), data...), nil
}

func (r parallelSafeLeafPageReader) ReadUnsafeToWithCacheHit(ptr page.ValuePtr, dst []byte) ([]byte, bool, bool, error) {
	leafPtr, err := page.LeafLogPtrFromValuePtr(ptr)
	if err != nil {
		return nil, false, false, err
	}
	data, ok := r.pages[leafPtr]
	if !ok {
		return nil, false, false, io.EOF
	}
	if cap(dst) >= len(data) {
		out := dst[:len(data)]
		copy(out, data)
		return out, true, false, nil
	}
	return append([]byte(nil), data...), false, false, nil
}

type failingBatchLeafPageLog struct {
	*batchMemoryLeafPageStore
	err error
}

func (l *failingBatchLeafPageLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, l.err
}

func (l *failingBatchLeafPageLog) AppendLeafPages([][]byte) ([]page.LeafLogPtr, error) {
	return nil, l.err
}

func TestApplyWorkerPoolNilRunFallsBackSerial(t *testing.T) {
	var p *ApplyWorkerPool
	var jobs []int
	if err := p.Run(4, 3, func(workerID, job int) {
		if workerID != 0 {
			t.Fatalf("workerID=%d want serial fallback worker 0", workerID)
		}
		jobs = append(jobs, job)
	}); err != nil {
		t.Fatalf("nil Run: %v", err)
	}
	if got, want := fmt.Sprint(jobs), "[0 1 2]"; got != want {
		t.Fatalf("jobs=%s want %s", got, want)
	}
}

func newParallelApplyTestZipper(t *testing.T) (*pager.Pager, *Zipper) {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, New(p, &MockAllocator{p: p})
}

func newParallelApplyEmptyLeafRoot(t *testing.T, z *Zipper) uint64 {
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
	return rootID
}

func buildParallelApplyBatch(t *testing.T, count int, prefix string) *batch.Batch {
	t.Helper()
	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("%s-%06d", prefix, i))
		if err := b.Set(key, val); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	return b
}

func TestResolveParallelApplyWorkersBoundsAndThresholds(t *testing.T) {
	summary := ReadOnlyLeafSpanSummary{Spans: 3, SpanOps: 900, SpanBytes: 90 << 10}
	workers := resolveParallelApplyWorkers(ApplyOptions{
		ParallelApplyConcurrency:  8,
		ParallelApplyMinSpans:     2,
		ParallelApplyMinSpanOps:   100,
		ParallelApplyMinSpanBytes: 1 << 10,
	}, summary)
	if workers <= 1 || workers > summary.Spans {
		t.Fatalf("workers=%d want bounded by spans=%d and >1", workers, summary.Spans)
	}
	if got := resolveParallelApplyWorkers(ApplyOptions{ParallelApplyConcurrency: 8, ParallelApplyMinSpans: 4}, summary); got != 0 {
		t.Fatalf("span threshold workers=%d want 0", got)
	}
	if got := resolveParallelApplyWorkers(ApplyOptions{ParallelApplyConcurrency: 8, ParallelApplyMinSpanOps: 901}, summary); got != 0 {
		t.Fatalf("op threshold workers=%d want 0", got)
	}
	if got := resolveParallelApplyWorkers(ApplyOptions{ParallelApplyConcurrency: 8, ParallelApplyMinSpanBytes: (90 << 10) + 1}, summary); got != 0 {
		t.Fatalf("byte threshold workers=%d want 0", got)
	}
}

func TestApplyWithOptionsParallelWorkerFailureReturnsError(t *testing.T) {
	p, z := newParallelApplyTestZipper(t)
	seedRoot := newParallelApplyEmptyLeafRoot(t, z)
	seed := buildParallelApplyBatch(t, 12000, "base")
	rootID, _, _, err := z.Apply(seedRoot, seed)
	_ = seed.Close()
	if err != nil {
		t.Fatalf("seed Apply: %v", err)
	}

	pool := NewApplyWorkerPool(4)
	defer pool.Close()
	failing := z.CloneWithAllocator(newFailAfterAllocator(p, 1))
	update := buildParallelApplyBatch(t, 9000, "upd")
	defer func() { _ = update.Close() }()
	result, err := failing.ApplyWithOptions(rootID, update, ApplyOptions{
		PrepareReadOnly:           true,
		ReadOnlyPrepareWorkers:    4,
		ParallelApplyConcurrency:  4,
		ParallelApplyWorkerPool:   pool,
		ParallelApplyMinSpans:     1,
		ParallelApplyMinSpanOps:   1,
		ParallelApplyMinSpanBytes: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "test allocator failure") {
		t.Fatalf("ApplyWithOptions err=%v want allocator failure", err)
	}
	if !result.ReadOnlyPrepareRequested || result.ReadOnlyPrepare.LeafSpanSummary().Spans == 0 {
		t.Fatalf("missing read-only prepare result after worker failure: %+v", result)
	}
}

func TestApplyWithOptionsParallelReadErrorFailsBeforeOutput(t *testing.T) {
	_, z := newParallelApplyTestZipper(t)
	store := newBatchMemoryLeafPageStore()
	z.SetOuterLeavesInValueLog(true)
	z.SetLeafPageLog(store)
	z.SetLeafPageReader(store)
	seedRoot := newParallelApplyEmptyLeafRoot(t, z)
	seed := buildParallelApplyBatch(t, 512, "base")
	rootID, _, _, err := z.Apply(seedRoot, seed)
	_ = seed.Close()
	if err != nil {
		t.Fatalf("seed Apply: %v", err)
	}
	store.pages = make(map[page.LeafLogPtr][]byte)
	z.SetLeafPageReader(store)
	update := buildParallelApplyBatch(t, 128, "upd")
	defer func() { _ = update.Close() }()
	pool := NewApplyWorkerPool(2)
	defer pool.Close()
	_, err = z.ApplyWithOptions(rootID, update, ApplyOptions{
		PrepareReadOnly:           true,
		ReadOnlyPrepareWorkers:    2,
		ParallelApplyConcurrency:  2,
		ParallelApplyWorkerPool:   pool,
		ParallelApplyMinSpans:     1,
		ParallelApplyMinSpanOps:   1,
		ParallelApplyMinSpanBytes: 1,
	})
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ApplyWithOptions read err=%v want EOF", err)
	}
	if len(store.pages) != 0 {
		t.Fatalf("read-only prepare failure should not append output, pages=%d", len(store.pages))
	}
}

func TestApplyWithOptionsParallelLeafLogEncodeError(t *testing.T) {
	_, z := newParallelApplyTestZipper(t)
	store := newBatchMemoryLeafPageStore()
	z.SetOuterLeavesInValueLog(true)
	z.SetLeafPageLog(store)
	z.SetLeafPageReader(store)
	seedRoot := newParallelApplyEmptyLeafRoot(t, z)
	seed := buildParallelApplyBatch(t, 512, "base")
	rootID, _, _, err := z.Apply(seedRoot, seed)
	_ = seed.Close()
	if err != nil {
		t.Fatalf("seed Apply: %v", err)
	}

	appendErr := errors.New("test leaf-log append failure")
	z.SetLeafPageReader(store)
	z.SetLeafPageLog(&failingBatchLeafPageLog{batchMemoryLeafPageStore: store, err: appendErr})
	update := buildParallelApplyBatch(t, 512, "upd")
	defer func() { _ = update.Close() }()
	pool := NewApplyWorkerPool(2)
	defer pool.Close()
	_, err = z.ApplyWithOptions(rootID, update, ApplyOptions{
		PrepareReadOnly:           true,
		ReadOnlyPrepareWorkers:    2,
		ParallelApplyConcurrency:  2,
		ParallelApplyWorkerPool:   pool,
		ParallelApplyMinSpans:     1,
		ParallelApplyMinSpanOps:   1,
		ParallelApplyMinSpanBytes: 1,
	})
	if !errors.Is(err, appendErr) {
		t.Fatalf("ApplyWithOptions append err=%v want %v", err, appendErr)
	}
}
