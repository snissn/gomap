package freelist

import (
	"errors"
	"fmt"
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
			a, err := got.MaterializeCandidate(base.GenerationID()+1, base.CommitSeq()+1, candidate, NewMemoryPageStoreV1())
			if err != nil {
				t.Fatal(err)
			}
			b, err := want.MaterializeCandidate(base.GenerationID()+1, base.CommitSeq()+1, candidate, NewMemoryPageStoreV1())
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
	for _, hint := range []uint64{10, 600, 10, 600} {
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
				hint := uint64(10)
				if i%2 != 0 {
					hint = 600
				}
				if _, err := txn.Allocate(hint); err != nil {
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
			for _, hint := range []uint64{600, 10, 600, 10} {
				got, err := a.Alloc(hint)
				want, expectedErr := allocatePersistent(oracle, hint)
				if got != want || !errors.Is(err, expectedErr) || !reflect.DeepEqual(a.cow.txn.root, oracle.root) {
					t.Fatalf("post-rollback allocation=%d,%v want=%d,%v", got, err, want, expectedErr)
				}
				if !reflect.DeepEqual(retained, retainedBefore) {
					t.Fatal("post-abort allocation changed retained candidate")
				}
			}
		})
	}
}

func TestFreelistPrivatePathMaterializeRetainedRoot(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			txn := NewFreelistTxn(privatePathBase(), nil)
			for i := 0; i < 4; i++ {
				hint := uint64(10)
				if i%2 != 0 {
					hint = 600
				}
				if _, err := txn.Allocate(hint); err != nil {
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

func commonPrefixBase(other uint64, count int) *FreelistGenerationV1 {
	free := make([]uint64, 0, count*2)
	for offset := uint64(2); offset < uint64(count)+2; offset++ {
		free = append(free, offset, other<<freelistChunkShift|offset)
	}
	return MustNewFreelistGenerationV1(1, (other+1)<<freelistChunkShift, free, nil)
}

func TestFreelistPrivateCommonPrefixAllocationBound(t *testing.T) {
	for _, other := range []uint64{1, 16, 70} {
		t.Run(fmt.Sprint(other), func(t *testing.T) {
			base := commonPrefixBase(other, 128)
			allocs := testing.AllocsPerRun(20, func() {
				txn := NewFreelistTxn(base, nil)
				for i := 0; i < 64; i++ {
					hint := uint64(0)
					if i%2 != 0 {
						hint = other << freelistChunkShift
					}
					if _, err := txn.Allocate(hint); err != nil {
						panic(err)
					}
				}
			})
			t.Logf("64 alternating Allocate calls: %.0f heap allocations", allocs)
			// Includes chunk copies, the divergent suffix, transaction bookkeeping,
			// and the first full path. Recopying every complete path exceeds 1,000.
			if allocs > 256 {
				t.Fatalf("64 switching allocations use %.0f allocations; want <=256", allocs)
			}
		})
	}
}

func TestFreelistPrivateCommonPrefixDifferential(t *testing.T) {
	for _, other := range []uint64{1, 16, 70, 1 << 52} {
		for _, reserved := range []bool{false, true} {
			t.Run(fmt.Sprintf("chunk%d/reserved%t", other, reserved), func(t *testing.T) {
				seed := NewFreelistTxn(commonPrefixBase(other, 32), nil)
				materialized, err := seed.MaterializeCandidate(2, 2, candidateIDFromString("prefix-base"), NewMemoryPageStoreV1())
				if err != nil {
					t.Fatal(err)
				}
				base := materialized.Generation()
				before := privatePathSnapshot(base.root)
				a, b := NewReservationLedger(), NewReservationLedger()
				if reserved {
					ids := []uint64{33, other<<freelistChunkShift | 33, other<<freelistChunkShift | 32}
					for _, ledger := range []*ReservationLedger{a, b} {
						if err := ledger.reserve(candidateIDFromString("prefix-blocker"), ids); err != nil {
							t.Fatal(err)
						}
					}
				}
				got, want := NewFreelistTxn(base, a), NewFreelistTxn(base, b)
				// Exhaust both leaves, including switching away from a private empty
				// leaf, then exercise append fallback with identical reservations.
				for i := 0; i < 70; i++ {
					hint := uint64(0)
					if i%2 != 0 {
						hint = other << freelistChunkShift
					}
					id, err := got.Allocate(hint)
					wid, werr := allocatePersistent(want, hint)
					if id != wid || !errors.Is(err, werr) || !reflect.DeepEqual(got.root, want.root) || !reflect.DeepEqual(got.allocated, want.allocated) || !reflect.DeepEqual(got.changedChunks, want.changedChunks) || !reflect.DeepEqual(got.replacedMetadata, want.replacedMetadata) || got.highWater != want.highWater || got.stats != want.stats {
						t.Fatalf("step%d changed allocation semantics: got %d,%v want %d,%v", i, id, err, wid, werr)
					}
				}
				if !reflect.DeepEqual(base.root, before) {
					t.Fatal("immutable base changed")
				}
				candidate := candidateIDFromString("prefix-differential")
				x, err := got.MaterializeCandidate(base.GenerationID()+1, base.CommitSeq()+1, candidate, NewMemoryPageStoreV1())
				if err != nil {
					t.Fatal(err)
				}
				y, err := want.MaterializeCandidate(base.GenerationID()+1, base.CommitSeq()+1, candidate, NewMemoryPageStoreV1())
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(x.Pages(), y.Pages()) || x.GenerationRef() != y.GenerationRef() || !reflect.DeepEqual(x.ReservationRecord(), y.ReservationRecord()) {
					t.Fatal("materialized bytes or authority changed")
				}
			})
		}
	}
}

func BenchmarkFreelistAllocateCommonPrefix(b *testing.B) {
	for _, other := range []uint64{1, 16, 70, 1 << 52} {
		base := commonPrefixBase(other, 198)
		b.Run(fmt.Sprintf("switch-%d", other), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				txn := NewFreelistTxn(base, nil)
				for j := 0; j < 128; j++ {
					hint := uint64(0)
					if j%2 != 0 {
						hint = other << freelistChunkShift
					}
					if _, err := txn.Allocate(hint); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func TestFreelistPrivateCommonPrefixRequiresCompleteProof(t *testing.T) {
	for _, depth := range []int{0, 5, 14, 15} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			txn := NewFreelistTxn(commonPrefixBase(1, 32), nil)
			if _, err := txn.Allocate(0); err != nil {
				t.Fatal(err)
			}
			n := txn.root
			for d := 0; d < depth && d < chunkTrieDepth; d++ {
				n = n.child[chunkNibble(0, d)]
			}
			if depth == 15 {
				n.chunk.pageID = 99
			} else {
				n.pageID = 99
			}
			held := txn.root
			before := privatePathSnapshot(held)
			if _, err := txn.Allocate(1 << freelistChunkShift); err != nil {
				t.Fatal(err)
			}
			if txn.root == held || !reflect.DeepEqual(held, before) {
				t.Fatal("incomplete old proof reused mutable common prefix")
			}
		})
	}
}
