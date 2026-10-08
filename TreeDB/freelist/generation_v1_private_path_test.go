package freelist

import (
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/pager"
)

func privatePathBase() *FreelistGenerationV1 {
	free := make([]uint64, 0, 512)
	for id := uint64(2); id < 200; id++ {
		free = append(free, id, id+512)
	}
	return MustNewFreelistGenerationV1(1, 2048, free, nil)
}

func privatePathSnapshot(n *stateNode) *stateNode {
	if n == nil {
		return nil
	}
	out := *n
	for i, child := range n.child {
		out.child[i] = privatePathSnapshot(child)
	}
	if n.chunk != nil {
		chunk := *n.chunk
		out.chunk = &chunk
	}
	return &out
}

// This oracle retains the original persistent mutation for each selected ID.
func allocatePersistent(t *FreelistTxn, hint uint64) (uint64, error) {
	if err := t.valid(); err != nil {
		return 0, err
	}
	id, ok := chooseUnreservedFreePage(t.root, hint, t.ledger, &t.stats.PageVisits)
	if !ok {
		return t.allocateAppend()
	}
	t.mutate(id>>freelistChunkShift, func(c *stateChunk) { c.setFree(id&(freelistChunkSize-1), false) })
	t.allocated = append(t.allocated, allocatedPage{id, ReservationReusedData})
	t.stats.ReuseAllocations++
	return id, nil
}

func TestFreelistPrivatePathAllocationDifferential(t *testing.T) {
	for _, shape := range []string{"same-chunk", "switch-chunks", "reserved-preferred", "reserved-all"} {
		t.Run(shape, func(t *testing.T) {
			base := privatePathBase()
			one, two := NewReservationLedger(), NewReservationLedger()
			var reserved []uint64
			if shape == "reserved-preferred" || shape == "reserved-all" {
				for id := uint64(2); id < 200; id++ {
					reserved = append(reserved, id)
				}
				if shape == "reserved-all" {
					for id := uint64(514); id < 712; id++ {
						reserved = append(reserved, id)
					}
				}
				for _, ledger := range []*ReservationLedger{one, two} {
					if err := ledger.reserve(candidateIDFromString("blocker"), reserved); err != nil {
						t.Fatal(err)
					}
				}
			}
			got, want := NewFreelistTxn(base, one), NewFreelistTxn(base, two)
			for i := 0; i < 420; i++ {
				hint := uint64(0)
				if shape == "switch-chunks" && i%2 != 0 {
					hint = 600
				}
				id, err := got.Allocate(hint)
				expected, expectedErr := allocatePersistent(want, hint)
				if id != expected || !errors.Is(err, expectedErr) {
					t.Fatalf("step%d: (%d,%v) want (%d,%v)", i, id, err, expected, expectedErr)
				}
				if !reflect.DeepEqual(got.root, want.root) || !reflect.DeepEqual(got.allocated, want.allocated) || got.stats != want.stats {
					t.Fatalf("step%d changed state or accounting", i)
				}
			}
			if base.FreeCount() != 396 {
				t.Fatal("immutable base changed")
			}
			candidate := candidateIDFromString("same-candidate")
			a, err := got.MaterializeCandidate(2, 2, candidate, NewMemoryPageStoreV1())
			if err != nil {
				t.Fatal(err)
			}
			b, err := want.MaterializeCandidate(2, 2, candidate, NewMemoryPageStoreV1())
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(a.Pages(), b.Pages()) || a.GenerationRef() != b.GenerationRef() || !reflect.DeepEqual(a.ReservationRecord(), b.ReservationRecord()) {
				t.Fatal("materialized bytes or authority changed")
			}
		})
	}
}

func TestFreelistPrivatePathPrepareForkIsolation(t *testing.T) {
	original := NewFreelistTxn(privatePathBase(), nil)
	for i := 0; i < 4; i++ {
		if _, err := original.Allocate(10); err != nil {
			t.Fatal(err)
		}
	}
	staged, err := original.cloneForAllocatorPrepare()
	if err != nil {
		t.Fatal(err)
	}
	stagedBefore := detachUnmaterialized(staged.root, 0)
	if _, err := original.Allocate(10); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(staged.root, stagedBefore) {
		t.Fatal("original changed staged shared root")
	}
	originalBefore := detachUnmaterialized(original.root, 0)
	for _, hint := range []uint64{10, 10, 600, 600} {
		if _, err := staged.Allocate(hint); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(original.root, originalBefore) {
		t.Fatal("staged changed rollback root")
	}
	nested, err := staged.cloneForAllocatorPrepare()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	for _, branch := range []*FreelistTxn{staged, nested} {
		go func(txn *FreelistTxn) {
			for i := 0; i < 64; i++ {
				if _, err := txn.Allocate(10); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(branch)
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(staged.root, nested.root) || staged.stats != nested.stats || !reflect.DeepEqual(original.root, originalBefore) {
		t.Fatal("nested concurrent branches changed each other's state or rollback root")
	}
}

func TestAllocatorPrivatePathWarmRollback(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "abort", true: "sink-failure"}[failure], func(t *testing.T) {
			p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.Alloc(2048); err != nil {
				t.Fatal(err)
			}
			a := New(p, 0)
			base := privatePathBase()
			if err := a.EnableCOWV1(base, nil); err != nil {
				t.Fatal(err)
			}
			oracle := NewFreelistTxn(base, nil)
			ids, err := a.AllocMany(4, 10)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				want, err := allocatePersistent(oracle, 10)
				if err != nil || id != want {
					t.Fatalf("warm id=%d want=%d err=%v", id, want, err)
				}
			}
			rollback := a.cow.txn
			before := detachUnmaterialized(rollback.root, 0)
			stats := a.Counters()
			capability, err := NewReuseCapability(1, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			var sink AppendPageSink = NewMemoryPageStoreV1()
			if failure {
				sink = &overwritingMetadataSink4627{store: NewMemoryPageStoreV1(), remaining: 1}
			}
			prepared, err := a.PrepareCOWCandidateV1(2, 2, candidateIDFromString("warm-stage"), capability, 3, sink)
			var retained *stateNode
			if failure {
				if err == nil {
					t.Fatal("missing sink failure")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				retained = prepared.Candidate().Generation().root
				if err := a.AbortCOWCandidateV1(prepared); err != nil {
					t.Fatal(err)
				}
			}
			if a.cow.txn != rollback || !reflect.DeepEqual(rollback.root, before) || a.Counters() != stats {
				t.Fatal("rollback changed live transaction or counters")
			}
			retainedBefore := privatePathSnapshot(retained)
			got, err := a.Alloc(10)
			want, expectedErr := allocatePersistent(oracle, 10)
			if got != want || !errors.Is(err, expectedErr) || !reflect.DeepEqual(a.cow.txn.root, oracle.root) {
				t.Fatalf("post-rollback allocation=%d,%v want=%d,%v", got, err, want, expectedErr)
			}
			if !reflect.DeepEqual(retained, retainedBefore) {
				t.Fatal("post-abort allocation changed retained candidate")
			}
		})
	}
}

func TestFreelistPrivatePathMaterializeRetainedRoot(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			txn := NewFreelistTxn(privatePathBase(), nil)
			for i := 0; i < 4; i++ {
				if _, err := txn.Allocate(10); err != nil {
					t.Fatal(err)
				}
			}
			held := txn.root
			before := detachUnmaterialized(held, 0)
			var sink AppendPageSink = NewMemoryPageStoreV1()
			if fail {
				sink = failingPageSinkV1{}
			}
			_, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("retained"), sink)
			if (err != nil) != fail {
				t.Fatalf("materialize=%v", err)
			}
			if !reflect.DeepEqual(held, before) {
				t.Fatal("metadata selection/emission mutated retained root")
			}
			if _, err := txn.Allocate(10); !errors.Is(err, ErrCandidateConsumed) {
				t.Fatalf("post-materialize=%v", err)
			}
		})
	}
}

func TestFreelistPrivatePathAppendOverflow(t *testing.T) {
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, math.MaxUint64, nil, nil), nil)
	if _, err := txn.Allocate(0); !errors.Is(err, ErrNoAllocatablePage) {
		t.Fatalf("overflow=%v", err)
	}
}

func TestAllocatorPrivatePathAllocManyPartialError(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(8); err != nil {
		t.Fatal(err)
	}
	a := New(p, 0)
	if err := a.EnableCOWV1(MustNewFreelistGenerationV1(1, 8, []uint64{3, 4}, nil), nil); err != nil {
		t.Fatal(err)
	}
	// Reach the append-overflow boundary without allocating an enormous file.
	a.cow.txn.highWater = math.MaxUint64
	ids, err := a.AllocMany(4, 0)
	if !errors.Is(err, ErrNoAllocatablePage) || !reflect.DeepEqual(ids, []uint64{4, 3}) {
		t.Fatalf("partial allocation=%v,%v", ids, err)
	}
	if a.cow.txn.root.freeCount != 0 || a.Counters().AllocPages != 2 || len(a.cow.txn.allocated) != 2 {
		t.Fatal("partial error changed completed allocation accounting")
	}
}

func TestFreelistPrivatePathAllocationBound(t *testing.T) {
	base := privatePathBase()
	allocs := testing.AllocsPerRun(20, func() {
		txn := NewFreelistTxn(base, nil)
		for i := 0; i < 64; i++ {
			if _, err := txn.Allocate(10); err != nil {
				panic(err)
			}
		}
	})
	t.Logf("64 real allocations use %.0f heap allocations", allocs)
	// Include transaction/maps and allocation-ID slice growth; a single copied
	// path fits comfortably, whereas 64 full paths require over 1,000 objects.
	if allocs > 96 {
		t.Fatalf("64 real allocations use %.0f heap allocations; want <=96", allocs)
	}
}

func BenchmarkFreelistAllocatePrivatePath(b *testing.B) {
	base := privatePathBase()
	for _, shape := range []string{"same-chunk", "switch-chunks"} {
		b.Run(shape, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				txn := NewFreelistTxn(base, nil)
				for j := 0; j < 128; j++ {
					hint := uint64(10)
					if shape == "switch-chunks" && j%2 != 0 {
						hint = 600
					}
					if _, err := txn.Allocate(hint); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
