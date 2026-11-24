package caching

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestCOWReadCutSizeClass(t *testing.T) {
	size := unsafe.Sizeof(cowReadCut{})
	t.Logf("unsafe.Sizeof(cowReadCut{}) = %d", size)
	if size > 448 {
		t.Fatalf("cowReadCut size=%d exceeds 448-byte size class", size)
	}
}

func TestCOWReadCutFrozenSetSharingZeroSliceAllocs(t *testing.T) {
	wantWriteCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadCut{})))
	if got := cowWriteCutCharge(cowReadCutInlineShards); got != wantWriteCharge {
		t.Fatalf("cowWriteCutCharge(%d) = %d, want %d", cowReadCutInlineShards, got, wantWriteCharge)
	}
	for _, frozen := range []int{0, 1, 4, 8} {
		t.Run(fmt.Sprintf("frozen=%d", frozen), func(t *testing.T) {
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			var fs *cowFrozenSet
			if frozen > 0 {
				fs, err = admitCOWFrozenSet(budget, frozen, frozen)
				if err != nil {
					budget.Close()
					t.Fatal(err)
				}
				for i := range fs.tables {
					fs.tables[i].shard = i % cowReadCutInlineShards
				}
			}
			old := cowReadCut{frozenSet: fs}
			if fs != nil {
				old.frozen = fs.tables
			}
			var next cowReadCut
			allocs := testing.AllocsPerRun(200, func() {
				next = cowReadCut{}
				next.initShards(cowReadCutInlineShards)
				for i := range next.shards {
					next.shards[i].shard = i
				}
				if old.frozenSet != nil {
					next.frozenSet = old.frozenSet
					next.frozenSet.retain()
					next.frozen = old.frozen
				}
				if len(next.shards) != cowReadCutInlineShards || len(next.frozen) != frozen || next.sources != nil || next.domains != nil {
					t.Fatalf("unexpected compact write cut shape: shards=%d frozen=%d sources=%v domains=%v",
						len(next.shards), len(next.frozen), next.sources != nil, next.domains != nil)
				}
				detached := next.frozenSet
				next.frozenSet, next.frozen = nil, nil
				detached.release()
			})
			if allocs != 0 {
				t.Fatalf("frozen=%d compact write cut allocated %.0f objects, want 0", frozen, allocs)
			}
			if fs != nil {
				if refs := fs.refs.Load(); refs != 1 {
					t.Fatalf("frozen=%d refs after release loop = %d, want 1", frozen, refs)
				}
				fs.release()
			}
			budget.Close()
			stats := budget.Stats()
			if stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("frozen=%d leaked frozenSet admission: %+v", frozen, stats)
			}
		})
	}
}

func TestCOWLazyCutDomainsConcurrentRetainRelease(t *testing.T) {
	db := cowPointReadFixture(t)
	frozenEntries := make(map[string][]byte, 8)
	mutableEntries := make(map[string][]byte, 8)
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("frozen-key-%02d", i)
		v := []byte(fmt.Sprintf("frozen-val-%02d", i))
		if err := db.Set([]byte(k), v); err != nil {
			t.Fatal(err)
		}
		frozenEntries[k] = v
	}
	if err := db.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("mutable-key-%02d", i)
		v := []byte(fmt.Sprintf("mutable-val-%02d", i))
		if err := db.Set([]byte(k), v); err != nil {
			t.Fatal(err)
		}
		mutableEntries[k] = v
	}
	// Overwrite one frozen key in the mutable shard to verify precedence.
	overrideKey := "frozen-key-00"
	overrideVal := []byte("mutable-override-00")
	if err := db.Set([]byte(overrideKey), overrideVal); err != nil {
		t.Fatal(err)
	}
	frozenEntries[overrideKey] = overrideVal

	cut := db.cow.cut
	if cut.domains != nil || cut.sources != nil {
		t.Fatalf("expected compact write cut without eager domains/sources: domains=%v sources=%v", cut.domains != nil, cut.sources != nil)
	}
	if cut.frozenSet == nil || len(cut.frozen) == 0 {
		t.Fatalf("expected shared frozenSet on compact write cut: frozenSet=%v len=%d", cut.frozenSet != nil, len(cut.frozen))
	}

	// Direct point reads use lookupShardEntryWithRevision and must not build lazy domains.
	for k, want := range frozenEntries {
		got, err := db.Get([]byte(k))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("direct Get(%q) = (%q, %v), want %q", k, got, err, want)
		}
	}
	for k, want := range mutableEntries {
		got, err := db.Get([]byte(k))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("direct Get(%q) = (%q, %v), want %q", k, got, err, want)
		}
	}
	if cut.domainsCache.Load() != nil {
		t.Fatal("direct point reads unexpectedly populated lazy domainsCache")
	}

	before := db.cow.budget.Stats()
	const workers = 8
	const iters = 50
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				snap := db.AcquireSnapshot()
				if snap == nil {
					errCh <- fmt.Errorf("worker %d iter %d: nil snapshot", id, i)
					return
				}
				k := fmt.Sprintf("frozen-key-%02d", i%8)
				got, err := snap.Get([]byte(k))
				if err != nil || !bytes.Equal(got, frozenEntries[k]) {
					_ = snap.Close()
					errCh <- fmt.Errorf("worker %d iter %d: snap.Get(%q) = (%q, %v), want %q", id, i, k, got, err, frozenEntries[k])
					return
				}
				mk := fmt.Sprintf("mutable-key-%02d", i%8)
				got, err = snap.Get([]byte(mk))
				if err != nil || !bytes.Equal(got, mutableEntries[mk]) {
					_ = snap.Close()
					errCh <- fmt.Errorf("worker %d iter %d: snap.Get(%q) = (%q, %v), want %q", id, i, mk, got, err, mutableEntries[mk])
					return
				}
				if i%10 == 0 {
					it, err := snap.Iterator(nil, nil)
					if err != nil {
						_ = snap.Close()
						errCh <- fmt.Errorf("worker %d iter %d: Iterator: %v", id, i, err)
						return
					}
					count := 0
					for ; it.Valid(); it.Next() {
						count++
					}
					itErr := it.Error()
					closeErr := it.Close()
					if itErr != nil || closeErr != nil || count != len(frozenEntries)+len(mutableEntries) {
						_ = snap.Close()
						errCh <- fmt.Errorf("worker %d iter %d: iterator count=%d want=%d itErr=%v closeErr=%v", id, i, count, len(frozenEntries)+len(mutableEntries), itErr, closeErr)
						return
					}
				}
				if err := snap.Close(); err != nil {
					errCh <- fmt.Errorf("worker %d iter %d: snap.Close: %v", id, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if cut.domainsCache.Load() != nil {
		t.Fatal("lazy domainsCache survived after all snapshots closed")
	}
	after := db.cow.budget.Stats()
	after.PeakBytes = before.PeakBytes
	if after != before {
		t.Fatalf("lazy snapshot domain lifecycle leaked budget: before=%+v after=%+v", before, after)
	}
}

func TestNewActiveCOWSnapshotInitialState(t *testing.T) {
	s1 := newActiveCOWSnapshot()
	s2 := newActiveCOWSnapshot()
	if s1 == s2 {
		t.Fatal("newActiveCOWSnapshot reused pointer identity")
	}
	if s1.generation.Load() != 1 || s1.closed.Load() || s1.readState.Load() != 0 {
		t.Fatalf("unexpected initial snapshot state: gen=%d closed=%v readState=%d",
			s1.generation.Load(), s1.closed.Load(), s1.readState.Load())
	}
}

func TestCOWReadCutRecycleAndDrain(t *testing.T) {
	db := cowPointReadFixture(t)
	budget := db.cow.budget

	// Sequential point writes with no pinned snapshots should recycle the prior
	// drained cowReadCut pointer across iterations.
	seen := make(map[*cowReadCut]struct{})
	reused := false
	for i := 0; i < 64; i++ {
		k := fmt.Sprintf("recycle-k-%04d", i)
		v := fmt.Sprintf("recycle-v-%04d", i)
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
		cur := db.cow.liveCut.Load()
		if _, ok := seen[cur]; ok {
			reused = true
		}
		seen[cur] = struct{}{}
	}
	if !reused {
		t.Fatalf("expected cowReadCut pointer reuse across sequential point writes (unique=%d)", len(seen))
	}

	// Stress concurrent lock-free retainCut() readers alongside rapid point
	// writes and deletes under -race to verify no ABA or torn-zeroing races.
	const readers = 6
	const writes = 400
	stop := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, readers)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("race-k-%04d", (i+id)%32)
				if _, err := db.Get([]byte(k)); err != nil {
					errCh <- fmt.Errorf("reader %d Get(%q): %v", id, k, err)
					return
				}
				if i%16 == 0 {
					snap := db.AcquireSnapshot()
					if snap == nil {
						errCh <- fmt.Errorf("reader %d AcquireSnapshot returned nil", id)
						return
					}
					_, err := snap.Get([]byte(k))
					closeErr := snap.Close()
					if (err != nil && !errors.Is(err, tree.ErrKeyNotFound)) || closeErr != nil {
						errCh <- fmt.Errorf("reader %d snap Get/Close: err=%v closeErr=%v", id, err, closeErr)
						return
					}
				}
			}
		}(r)
	}

	expected := make(map[string]string, 32)
	for i := 0; i < writes; i++ {
		k := fmt.Sprintf("race-k-%04d", i%32)
		if i%5 == 4 {
			if err := db.Delete([]byte(k)); err != nil {
				close(stop)
				wg.Wait()
				t.Fatalf("Delete %d: %v", i, err)
			}
			delete(expected, k)
		} else {
			v := fmt.Sprintf("race-v-%04d", i)
			if err := db.Set([]byte(k), []byte(v)); err != nil {
				close(stop)
				wg.Wait()
				t.Fatalf("Set %d: %v", i, err)
			}
			expected[k] = v
		}
	}
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	for i := 0; i < 32; i++ {
		k := fmt.Sprintf("race-k-%04d", i)
		got, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("final Get(%q): %v", k, err)
		}
		want, ok := expected[k]
		if !ok {
			if got != nil {
				t.Fatalf("final Get(%q) = %q, want nil", k, got)
			}
		} else if string(got) != want {
			t.Fatalf("final Get(%q) = %q, want %q", k, got, want)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if db.cow.recycledCut.Load() != nil {
		t.Fatal("recycledCut was not cleared on Close")
	}
	if got := budget.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 {
		t.Fatalf("budget did not drain to zero after Close: %+v", got)
	}
}
