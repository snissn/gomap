package zipper

import (
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
