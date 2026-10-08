package zipper

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

func privateCloneApplyFixture(tb testing.TB) (*Zipper, uint64, *batch.Batch, *benchmarkCyclingAllocator) {
	tb.Helper()
	p, err := pager.Open(filepath.Join(tb.TempDir(), "index.db"), 65536)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = p.Close() })
	rootID, err := p.Alloc(1)
	if err != nil {
		tb.Fatal(err)
	}
	data, err := p.Get(rootID)
	if err != nil {
		tb.Fatal(err)
	}
	builder := node.NewBuilder(data, page.PageTypeLeaf)
	builder.SetPageID(rootID)
	for i := 0; i < 64; i++ {
		if err := builder.AddLeafEntry([]byte(fmt.Sprintf("k%03d", i)), []byte("old value"), node.FlagInline, page.ValuePtr{}); err != nil {
			tb.Fatal(err)
		}
	}
	builder.FinishNoNode()
	ids := make([]uint64, 16)
	for i := range ids {
		ids[i], err = p.Alloc(1)
		if err != nil {
			tb.Fatal(err)
		}
	}
	allocator := &benchmarkCyclingAllocator{ids: ids}
	owner := New(p, allocator)
	updates := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	for i := 0; i < 8; i++ {
		updates.Set([]byte(fmt.Sprintf("k%03d", i)), []byte("new value"))
	}
	updates.SortedEntries()
	tb.Cleanup(func() { _ = updates.Close() })
	return owner, rootID, updates, allocator
}

type privateCloneFailingAllocator struct{ err error }

func (a privateCloneFailingAllocator) Alloc(uint64) (uint64, error) { return 0, a.err }

type privateCloneFailingLeafLog struct{ err error }

func (l privateCloneFailingLeafLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, l.err
}

type privateClonePanickingLeafLog struct{ value any }

func (l privateClonePanickingLeafLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	panic(l.value)
}

func TestPrivateCloneApplyCanceledAllocatorReturnsScratch(t *testing.T) {
	owner, root, updates, _ := privateCloneApplyFixture(t)
	seed := owner.acquireApplyScratch()
	owner.releaseApplyScratch(seed)
	clone := owner.CloneWithAllocator(privateCloneFailingAllocator{context.Canceled})
	if _, _, _, err := clone.Apply(root, updates); !errors.Is(err, context.Canceled) {
		t.Fatalf("Apply error=%v, want canceled allocator", err)
	}
	returned := owner.acquireApplyScratch()
	defer owner.releaseApplyScratch(returned)
	if returned != seed {
		t.Fatal("canceled allocator lost original scratch")
	}
}

func TestPrivateCloneApplyPanicEndsLeafCache(t *testing.T) {
	owner, root, _, allocator := privateCloneApplyFixture(t)
	seed := owner.acquireApplyScratch()
	owner.releaseApplyScratch(seed)
	clone := owner.CloneWithAllocator(allocator)
	clone.SetOuterLeavesInValueLog(true)
	wantPanic := errors.New("private append panic")
	clone.SetLeafPageLog(privateClonePanickingLeafLog{wantPanic})
	deletes := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	defer deletes.Close()
	deletes.Delete([]byte("k000"))
	func() {
		defer func() {
			if got := recover(); got != wantPanic {
				t.Fatalf("panic=%v, want original append panic", got)
			}
		}()
		_, _, _, _ = clone.Apply(root, deletes)
	}()
	if clone.leafRefCacheActive.Load() || clone.leafRefCache != nil || clone.leafRefCacheScratch != nil {
		t.Fatal("panic left the private leaf cache active")
	}
	returned := owner.acquireApplyScratch()
	defer owner.releaseApplyScratch(returned)
	if returned != seed || len(returned.leafRefCacheActivePages) != 0 || len(returned.splitArena) != 0 {
		t.Fatal("panic did not return reset original scratch")
	}
}

func TestPrivateCloneApplyDoesNotRetainBorrowedSnapshotPages(t *testing.T) {
	owner, root, updates, allocator := privateCloneApplyFixture(t)
	clone := owner.CloneWithAllocator(&MockAllocator{p: owner.pager})
	clone.SetOuterLeavesInValueLog(true)
	snapshot := newMemoryLeafPageStore(clone)
	clone.SetLeafPageLog(snapshot)
	clone.SetLeafPageReader(snapshot)
	snapshotRoot := buildOuterLeafInternalRoot(t, clone)
	// Keep the immutable input snapshot reader separate from this attempt's
	// output appender. Its ReadUnsafe returns borrowed snapshot page bytes.
	output := newMemoryLeafPageStore(clone)
	output.next = snapshot.next
	clone.SetLeafPageLog(output)
	snapshot.readCalls = 0
	seed := owner.acquireApplyScratch()
	owner.releaseApplyScratch(seed)
	delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer delta.Close()
	delta.Delete([]byte("key-050"))
	if _, _, _, err := clone.Apply(snapshotRoot, delta); err != nil {
		t.Fatal(err)
	}
	if snapshot.readCalls == 0 {
		t.Fatal("Apply did not exercise borrowed snapshot reads")
	}
	returned := owner.acquireApplyScratch()
	if returned != seed {
		t.Fatal("snapshot Apply lost original scratch")
	}
	borrowed := make(map[*byte]bool)
	for _, data := range snapshot.pages {
		for i := range data {
			borrowed[&data[i]] = true
		}
	}
	for _, buffers := range [][][]byte{returned.leafPageScratch, returned.nodeKeyScratch} {
		for _, buf := range buffers {
			if cap(buf) > 0 && borrowed[&buf[:cap(buf)][0]] {
				t.Error("returned scratch retained borrowed snapshot bytes")
			}
		}
	}
	if clone.leafRefCache != nil || clone.leafRefCacheScratch != nil || owner.leafPageReader != nil || owner.leafPageLog != nil {
		t.Error("snapshot reader/appender/cache escaped into original owner")
	}
	owner.releaseApplyScratch(returned)
	// Retire the snapshot's transient pages, then reuse scratch against the
	// original pager. Output from a later Apply must not depend on those bytes.
	for _, data := range snapshot.pages {
		clear(data)
	}
	allocator.reset()
	newRoot, _, _, err := owner.Apply(root, updates)
	if err != nil {
		t.Fatal(err)
	}
	got, err := tree.New(owner.pager, panicValueReader{}, newRoot).Get([]byte("k000"))
	if err != nil || string(got) != "new value" {
		t.Fatalf("output after retiring snapshot=%q error=%v", got, err)
	}
}

func TestPrivateCloneApplyScratchReturnsToOriginalOwner(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprintf("failure=%t", failure), func(t *testing.T) {
			owner, root, updates, allocator := privateCloneApplyFixture(t)
			seed := owner.acquireApplyScratch()
			owner.releaseApplyScratch(seed)
			var alloc PageAllocator = allocator
			wantErr := errors.New("private allocation rejected")
			if failure {
				alloc = privateCloneFailingAllocator{wantErr}
			}
			// Nested private views must return directly to the original lifetime.
			clone := owner.CloneWithAllocator(alloc).CloneWithAllocator(alloc)
			_, _, _, err := clone.Apply(root, updates)
			if failure && !errors.Is(err, wantErr) || !failure && err != nil {
				t.Fatalf("Apply error=%v", err)
			}
			reused := clone.acquireApplyScratch()
			if reused != seed {
				t.Fatal("private Apply lost the original owner's warm scratch")
			}
			clone.releaseApplyScratch(reused)
			returned := owner.acquireApplyScratch()
			if returned != seed {
				t.Fatal("private Apply returned scratch to the discarded clone")
			}
			owner.releaseApplyScratch(returned)
			if owner.allocator != allocator || owner.leafPageLog != nil || owner.leafPageReader != nil {
				t.Fatal("private configuration escaped into original owner")
			}
		})
	}
}

func TestPrivateCloneApplyErrorEndsLeafCache(t *testing.T) {
	owner, root, _, allocator := privateCloneApplyFixture(t)
	seed := owner.acquireApplyScratch()
	owner.releaseApplyScratch(seed)
	clone := owner.CloneWithAllocator(allocator)
	clone.SetOuterLeavesInValueLog(true)
	wantErr := errors.New("private append rejected")
	clone.SetLeafPageLog(privateCloneFailingLeafLog{wantErr})
	clone.SetLeafPageReader(&countingLeafPageReader{})
	deletes := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	defer deletes.Close()
	deletes.Delete([]byte("k000"))
	if _, _, _, err := clone.Apply(root, deletes); !errors.Is(err, wantErr) {
		t.Fatalf("Apply error=%v, want append rejection", err)
	}
	if clone.leafRefCacheActive.Load() || clone.leafRefCache != nil || clone.leafRefCacheScratch != nil {
		t.Fatal("failed Apply retained its private leaf cache")
	}
	reused := owner.acquireApplyScratch()
	defer owner.releaseApplyScratch(reused)
	if reused != seed {
		t.Fatal("failed Apply did not return original scratch")
	}
	if len(reused.leafRefCacheActivePages) != 0 || len(reused.splitArena) != 0 {
		t.Fatal("failed Apply left active page/key aliases in returned scratch")
	}
	for _, batches := range reused.pendingLeafPersistScratch {
		for _, entry := range batches[:cap(batches)] {
			if entry.data != nil || entry.pooled != nil {
				t.Fatal("returned scratch retained pending append buffers")
			}
		}
	}
	if owner.outerLeavesInValueLog || owner.leafPageLog != nil || owner.leafPageReader != nil {
		t.Fatal("failed private configuration escaped into original owner")
	}
}

func TestPrivateCloneScratchConcurrentCheckoutIsBounded(t *testing.T) {
	owner := New(nil, nil)
	const workers = applyScratchKeep + 4
	ready := make(chan *mergeScratch, workers)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clone := owner.CloneWithAllocator(nil)
			s := clone.acquireApplyScratch()
			ready <- s
			<-release
			clone.releaseApplyScratch(s)
		}()
	}
	seen := make(map[*mergeScratch]bool)
	for i := 0; i < workers; i++ {
		s := <-ready
		if seen[s] {
			t.Error("concurrent private clones shared active mutable scratch")
		}
		seen[s] = true
	}
	close(release)
	wg.Wait()
	retained := len(owner.applyScratchFree)
	if owner.applyScratch != nil {
		retained++
	}
	if retained != applyScratchKeep+1 {
		t.Fatalf("original owner retained %d scratches, want existing bound %d", retained, applyScratchKeep+1)
	}
}

func TestPrivateCloneLeafCacheActiveBookkeepingIsBoundedAfterClose(t *testing.T) {
	owner := New(nil, nil)
	clone := owner.CloneWithAllocator(nil)
	s := clone.acquireApplyScratch()
	clone.beginLeafRefCache(s)
	const activePages = mergeLeafRefCachePageKeep + 1
	seen := make(map[*byte]bool, activePages)
	for i := 0; i < activePages; i++ {
		data := s.acquireLeafRefCacheBuildPage()
		if seen[&data[0]] {
			t.Fatal("active cache reused a page before close")
		}
		seen[&data[0]] = true
		data[0] = byte(i)
	}
	if len(s.leafRefCacheActivePages) != activePages {
		t.Fatal("retention bound limited active work")
	}
	for i, p := range s.leafRefCacheActivePages {
		if p.buf[0] != byte(i) {
			t.Fatal("active cache page was modified before close")
		}
	}
	clone.endLeafRefCache()
	if cap(s.leafRefCacheActivePages) > mergeLeafRefCachePageKeep {
		t.Fatal("closed cache retained oversized active bookkeeping")
	}
	clone.releaseApplyScratch(s)
	returned := owner.acquireApplyScratch()
	defer owner.releaseApplyScratch(returned)
	if returned != s || len(returned.leafRefCacheActivePages) != 0 || len(returned.leafRefCachePages) > mergeLeafRefCachePageKeep {
		t.Fatal("closed cache did not return bounded idle scratch")
	}
	// Reset also discards oversized inactive bookkeeping, independently of
	// cache close. It must not preserve capacity left by a prior attempt.
	returned.leafRefCacheActivePages = make([]*leafRefCachePage, 0, activePages)
	returned.reset()
	if cap(returned.leafRefCacheActivePages) > mergeLeafRefCachePageKeep {
		t.Fatal("reset retained oversized inactive bookkeeping")
	}
}

func TestPrivateCloneConcurrentApplyKeepsPrivateOutput(t *testing.T) {
	owner, root, _, _ := privateCloneApplyFixture(t)
	const workers = 4
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			clone := owner.CloneWithAllocator(&MockAllocator{p: owner.pager})
			updates := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
			defer updates.Close()
			value := []byte(fmt.Sprintf("worker-%d", worker))
			updates.Set([]byte("k000"), value)
			<-start
			for attempt := 0; attempt < 16; attempt++ {
				newRoot, _, _, err := clone.Apply(root, updates)
				if err != nil {
					t.Errorf("private Apply: %v", err)
					return
				}
				got, err := tree.New(owner.pager, panicValueReader{}, newRoot).Get([]byte("k000"))
				if err != nil || string(got) != string(value) {
					t.Errorf("private output=%q error=%v, want %q", got, err, value)
					return
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	got, err := tree.New(owner.pager, panicValueReader{}, root).Get([]byte("k000"))
	if err != nil || string(got) != "old value" {
		t.Fatalf("original root changed: value=%q error=%v", got, err)
	}
}

func TestPrivateCloneApplyScratchAcrossPrivatePager(t *testing.T) {
	owner, root, updates, allocator := privateCloneApplyFixture(t)
	overlay, overlayRoot, _, overlayAllocator := privateCloneApplyFixture(t)
	seed := owner.acquireApplyScratch()
	owner.releaseApplyScratch(seed)
	clone := owner.CloneWithPagerAllocator(overlay.pager, overlayAllocator)
	if _, _, _, err := clone.Apply(overlayRoot, updates); err != nil {
		t.Fatal(err)
	}
	if owner.pager == clone.pager || owner.allocator != allocator {
		t.Fatal("private pager/allocator escaped into the original owner")
	}
	returned := owner.acquireApplyScratch()
	if returned != seed {
		t.Fatal("private pager Apply lost original scratch")
	}
	owner.releaseApplyScratch(returned)
	// Reusing buffers produced with another pager must not reuse its pages.
	newRoot, _, _, err := owner.Apply(root, updates)
	if err != nil {
		t.Fatal(err)
	}
	got, err := tree.New(owner.pager, panicValueReader{}, newRoot).Get([]byte("k000"))
	if err != nil || string(got) != "new value" {
		t.Fatalf("owner output after private pager Apply=%q error=%v", got, err)
	}
}

func BenchmarkPrivateCloneApplyScratch(b *testing.B) {
	for _, private := range []bool{false, true} {
		b.Run(fmt.Sprintf("private=%t", private), func(b *testing.B) {
			owner, root, updates, allocator := privateCloneApplyFixture(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				allocator.reset()
				z := owner
				if private {
					z = owner.CloneWithAllocator(allocator)
				}
				if _, _, _, err := z.Apply(root, updates); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
