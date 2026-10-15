package zipper

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Batch.writeOptimistic creates an allocator clone for every public write.
// Behavioral results alone cannot distinguish scratch allocation from reuse;
// this selection test protects the actual clone seam used by that caller.
func TestAllocatorCloneReusesApplyScratch(t *testing.T) {
	z := New(nil, nil)
	first := z.CloneWithAllocator(nil)
	s := first.acquireApplyScratch()
	s.cloneSplitKey([]byte("old operation"))
	first.releaseApplyScratch(s)
	second := z.CloneWithPagerAllocator(nil, nil).CloneWithAllocator(nil)
	reused := second.acquireApplyScratch()
	defer second.releaseApplyScratch(reused)
	if reused != s {
		t.Fatal("allocator clones discarded reusable apply scratch")
	}
	if len(reused.splitKeyArena) != 0 {
		t.Fatal("clone retained live split keys")
	}
}

func TestAllocatorCloneConcurrentScratchIsolation(t *testing.T) {
	z := New(nil, nil)
	const workers = applyScratchKeep + 4
	acquired := make(chan *mergeScratch, workers)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clone := z.CloneWithAllocator(nil)
			s := clone.acquireApplyScratch()
			acquired <- s
			<-release
			clone.releaseApplyScratch(s)
		}()
	}
	seen := make(map[*mergeScratch]bool)
	for i := 0; i < workers; i++ {
		s := <-acquired
		if seen[s] {
			t.Error("simultaneous operations shared mutable scratch")
		}
		seen[s] = true
	}
	close(release)
	wg.Wait()
	// Borrow more than the retention limit after every borrower has returned.
	// Exactly the bounded retained set should be reused, even across clones.
	var borrowed []*mergeScratch
	reused := 0
	for i := 0; i < workers; i++ {
		s := z.CloneWithAllocator(nil).acquireApplyScratch()
		borrowed = append(borrowed, s)
		if seen[s] {
			reused++
		}
	}
	for _, s := range borrowed {
		z.releaseApplyScratch(s)
	}
	if reused != applyScratchKeep+1 {
		t.Fatalf("reused %d scratches, want bounded %d", reused, applyScratchKeep+1)
	}
}

func TestAllocatorCloneApplyRetainsRootsAndRetries(t *testing.T) {
	p, z := newParallelApplyTestZipper(t)
	root := newParallelApplyEmptyLeafRoot(t, z)
	seed := buildParallelApplyBatch(t, 2400, "seed")
	defer seed.Close()
	root, _, _, err := z.CloneWithAllocator(&MockAllocator{p: p}).Apply(root, seed)
	if err != nil {
		t.Fatal(err)
	}
	seedRoot := root
	seedPage, err := p.Get(seedRoot)
	if err != nil {
		t.Fatal(err)
	}
	seedImage := bytes.Clone(seedPage)
	var retainedRetired [][]uint64
	var retiredCopies [][]uint64
	for iteration := 0; iteration < 6; iteration++ {
		b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
		for i := 0; i < 2400; i++ {
			key := []byte(fmt.Sprintf("key-%06d", i))
			if iteration%2 == 0 && i%3 == 0 {
				err = b.Delete(key)
			} else {
				err = b.Set(key, []byte(fmt.Sprintf("update-%d-%06d", iteration, i)))
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if iteration == 0 {
			_, _, _, failErr := z.CloneWithAllocator(newFailAfterAllocator(p, 1)).Apply(root, b)
			if failErr == nil {
				t.Fatal("expected allocator error before retry")
			}
		}
		var retired []uint64
		root, retired, _, err = z.CloneWithAllocator(&MockAllocator{p: p}).Apply(root, b)
		b.Close()
		if err != nil {
			t.Fatal(err)
		}
		retainedRetired = append(retainedRetired, retired)
		retiredCopies = append(retiredCopies, append([]uint64(nil), retired...))
		for j := range retainedRetired {
			if fmt.Sprint(retainedRetired[j]) != fmt.Sprint(retiredCopies[j]) {
				t.Fatal("later Apply mutated retained retired pages")
			}
		}
		tr := tree.New(p, panicValueReader{}, root)
		got, getErr := tr.Get([]byte("key-000001"))
		want := fmt.Sprintf("update-%d-%06d", iteration, 1)
		if getErr != nil || string(got) != want {
			t.Fatalf("iteration %d get=%q err=%v want=%q", iteration, got, getErr, want)
		}
	}
	gotImage, err := p.Get(seedRoot)
	if err != nil || !bytes.Equal(gotImage, seedImage) {
		t.Fatal("later Apply mutated retained root page")
	}
	oldTree := tree.New(p, panicValueReader{}, seedRoot)
	got, err := oldTree.Get([]byte("key-000000"))
	if err != nil || string(got) != "seed-000000" {
		t.Fatalf("retained seed tree get=%q err=%v", got, err)
	}
}

func TestAllocatorCloneScratchDropsOversizedLeafRefMetadata(t *testing.T) {
	z := New(nil, nil)
	clone := z.CloneWithAllocator(nil)
	s := clone.acquireApplyScratch()
	first, second := &leafRefCachePage{}, &leafRefCachePage{}
	s.leafRefCachePages = make([]*leafRefCachePage, 2, mergeLeafRefCachePageKeep+1024)
	s.leafRefCachePages[0], s.leafRefCachePages[1] = first, second
	s.leafRefCacheActivePages = make([]*leafRefCachePage, 0, mergeLeafRefCachePageKeep+1024)
	clone.releaseApplyScratch(s)
	next := z.CloneWithAllocator(nil)
	reused := next.acquireApplyScratch()
	defer next.releaseApplyScratch(reused)
	if reused != s {
		t.Fatal("scratch not reused across allocator clones")
	}
	if cap(reused.leafRefCachePages) > mergeLeafRefCachePageKeep || cap(reused.leafRefCacheActivePages) > mergeLeafRefCachePageKeep {
		t.Fatalf("retained oversized pointer metadata: free cap=%d active cap=%d", cap(reused.leafRefCachePages), cap(reused.leafRefCacheActivePages))
	}
	if len(reused.leafRefCachePages) != 2 || reused.leafRefCachePages[0] != first || reused.leafRefCachePages[1] != second {
		t.Fatal("metadata trim lost reusable page ownership")
	}
	if len(reused.leafRefCacheActivePages) != 0 {
		t.Fatal("retained active operation page references")
	}
	other := New(nil, nil)
	unrelated := other.acquireApplyScratch()
	defer other.releaseApplyScratch(unrelated)
	if unrelated == s {
		t.Fatal("unrelated zipper family shared scratch")
	}
}
