package primaryarena

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestPrimarySplitClaimDetectsConcurrentMetadataAllocation(t *testing.T) {
	a, err := Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.SetMetadataDecoder(func(Class, []byte) ([]MetadataEdge, error) { return nil, nil })
	c, ready, err := a.PrepareClaim(Component, nil)
	if !ready || err != nil {
		t.Fatalf("prepare: ready=%t err=%v", ready, err)
	}
	var manifest [1]Ref
	if err := a.ClaimMetadataBanksIntoOrdinary(Manifest, manifest[:]); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := c.Step(nil); ready || !errors.Is(err, ErrStale) {
		t.Fatalf("split claim accepted another allocation's epoch: ready=%t err=%v", ready, err)
	}
	if ready, err := c.Cancel(nil); !ready || err != nil {
		t.Fatalf("cancel invalidated private claim: ready=%t err=%v", ready, err)
	}
	if a.valid(manifest[0]) == nil || c.arena != nil || c.growth != nil || c.chunk != nil || c.metadataCharge != 0 {
		t.Fatal("cancel lost installed metadata custody or retained private construction")
	}
}

func TestPrimaryOrdinaryClaimPreservesWorkAndRefusal(t *testing.T) {
	var receipts [2]iterator.OrdinalScanWork
	var counters [2]Counters
	for mode := range 2 {
		a, err := Open(filepath.Join(t.TempDir(), "index.db.primary"))
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		w := &receipts[mode]
		w.RecordLimit, w.ByteLimit = ^uint64(0), ^uint64(0)
		var ref Ref
		var ready bool
		if mode == 0 {
			var c *Claim
			c, ready, err = a.PrepareClaim(Component, w)
			if ready && err == nil {
				ref, ready, err = c.Step(w)
			}
		} else {
			ref, ready, err = a.ClaimOrdinary(Component, w)
		}
		if !ready || err != nil || a.valid(ref) == nil {
			t.Fatalf("mode=%d ready=%t ref=%+v err=%v", mode, ready, ref, err)
		}
		counters[mode] = a.Counters()
		if mode == 1 {
			if _, err := a.Drop(ref, nil); err != nil {
				t.Fatal(err)
			}
			testDrain(t, a)
			before := a.MetadataOwner().Bytes()
			// A single record admits preparation on the reusable bank, then
			// refuses installation. Its private wrapper must be canceled.
			limited := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: ^uint64(0)}
			if got, ready, err := a.ClaimOrdinary(Component, limited); ready || err != nil || got != (Ref{}) {
				t.Fatalf("bounded refusal: ref=%+v ready=%t err=%v", got, ready, err)
			}
			if got := a.MetadataOwner().Bytes(); got != before {
				t.Fatalf("refusal retained private claim: got=%d want=%d", got, before)
			}
			if got, ready, err := a.ClaimOrdinary(Component, nil); !ready || err != nil || a.valid(got) == nil {
				t.Fatalf("retry after refusal: ref=%+v ready=%t err=%v", got, ready, err)
			}
		}
	}
	if receipts[0] != receipts[1] || receipts[0].Records == 0 || receipts[0].Bytes == 0 || counters[0] != counters[1] {
		t.Fatalf("ordinary claim changed work: split=%+v atomic=%+v counters=%+v", receipts[0], receipts[1], counters)
	}
}

func TestPrimaryOrdinaryClaimsWithConcurrentMetadataAllocation(t *testing.T) {
	a, err := Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.SetMetadataDecoder(func(Class, []byte) ([]MetadataEdge, error) { return nil, nil })
	const workers, count = 4, 128
	refs := make([]Ref, (workers+1)*count)
	errors := make(chan error, workers+1)
	var wg sync.WaitGroup
	for worker := range workers + 1 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := range count {
				out := &refs[worker*count+i]
				if worker == workers {
					var metadata [1]Ref
					if err := a.ClaimMetadataBanksIntoOrdinary(Manifest, metadata[:]); err != nil {
						errors <- err
						return
					}
					*out = metadata[0]
				} else {
					// Match the actual zipper caller's nonnil ordinary meter.
					work := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
					ref, ready, err := a.ClaimOrdinary(Component, work)
					if !ready || err != nil {
						errors <- fmt.Errorf("component ready=%t: %w", ready, err)
						return
					}
					*out = ref
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	seen := make(map[Ref]bool, len(refs))
	for _, ref := range refs {
		if seen[ref] || a.valid(ref) == nil {
			t.Fatalf("duplicate or invalid installed claim: %+v", ref)
		}
		seen[ref] = true
		if ready, err := a.Drop(ref, nil); !ready || err != nil {
			t.Fatalf("drop ref=%+v ready=%t err=%v", ref, ready, err)
		}
	}
}

func TestPrimaryOrdinaryClaimFailedDisposalRetainsOriginal(t *testing.T) {
	a, err := Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	// The injected failure below owns no failed OS unmap. Release the ordinary
	// pager file after checking that Arena.Close preserves the failed owner.
	defer a.Pager().Close()
	c, ready, err := a.PrepareClaim(Component, nil)
	if !ready || err != nil {
		t.Fatalf("prepare: ready=%t err=%v", ready, err)
	}
	before := a.MetadataOwner().Bytes()
	primary, cleanup := errors.New("growth failed"), errors.New("private unmap failed")
	a.mu.Lock()
	receipt := a.retainFailedOrdinaryClaimLocked(c, primary, cleanup)
	a.mu.Unlock()
	if !errors.Is(receipt, primary) || !errors.Is(receipt, cleanup) {
		t.Fatal("failed disposal lost original or cleanup cause")
	}
	for range 2 {
		if _, ready, err := a.PrepareClaim(Component, nil); ready || err != receipt {
			t.Fatalf("allocation escaped failed owner: ready=%t err=%v", ready, err)
		}
		if err := a.Close(); err != receipt {
			t.Fatalf("close discarded the original failed disposal: %v", err)
		}
		if a.ordinaryClaimFailure.claim != c || c.arena != a || c.chunk == nil || c.growth == nil || c.metadataCharge == 0 || a.MetadataOwner().Bytes() != before {
			t.Fatal("failed disposal replaced, retried, or refunded the original claim")
		}
	}
}
