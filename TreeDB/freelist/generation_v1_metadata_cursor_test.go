package freelist

import (
	"fmt"
	"maps"
	"math"
	"math/rand"
	"slices"
	"sync"
	"testing"
)

func TestMetadataReservationMaskBoundaries4627(t *testing.T) {
	for _, base := range []uint64{0, 256, math.MaxUint64 &^ 255} {
		for _, start := range []uint64{0, 1, 63, 64, 255, 256, base, base + 1, base + 63, math.MaxUint64} {
			for _, count := range []uint64{0, 1, 63, 64, 65, 256, math.MaxUint64} {
				free := [4]uint64{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}
				clearReservedChunkRange(&free, base, start, count)
				for offset := uint64(0); offset < freelistChunkSize; offset++ {
					id := base + offset
					want := !(id >= start && id-start < count)
					if got := free[offset/64]&(uint64(1)<<(offset%64)) != 0; got != want {
						t.Fatalf("base=%d interval=(%d,%d) id=%d free=%v want %v", base, start, count, id, got, want)
					}
				}
			}
		}
	}
}

func TestMetadataChunkRunMatchesReservationOracle4627(t *testing.T) {
	rng := rand.New(rand.NewSource(4627))
	for _, chunkNo := range []uint64{0, 1, math.MaxUint64 >> freelistChunkShift} {
		for trial := 0; trial < 100; trial++ {
			chunk := &stateChunk{chunkNo: chunkNo}
			for i := range chunk.free {
				chunk.free[i] = rng.Uint64()
			}
			ledger := NewReservationLedger()
			base := chunkNo << freelistChunkShift
			for i := 0; i < 8; i++ {
				id := candidateIDFromString(fmt.Sprint(i))
				start := base + uint64(rng.Intn(256))
				if i == 0 && base >= 64 {
					start = base - 64
				}
				count := []uint64{0, 1, 63, 64, 65, 255, 300, math.MaxUint64}[i]
				ledger.candidates[id] = &reservation{state: CandidateState(i % 5), tailReserved: i%3 != 0, tailStart: start, tailCount: count}
				ledger.owners[base+uint64(rng.Intn(256))] = id
				if i%2 == 0 {
					ledger.burnedTails = append(ledger.burnedTails, reservationInterval{start, count})
				}
			}
			var wantStart, wantCount, start, count uint64
			for offset := uint64(0); offset < freelistChunkSize; offset++ {
				id := base + offset
				if chunk.isFree(offset) && !ledger.reservedLocked(id) {
					if count == 0 {
						start = id
					}
					count++
					if count > wantCount {
						wantStart, wantCount = start, count
					}
				} else {
					count = 0
				}
			}
			txn := &FreelistTxn{ledger: ledger}
			gotStart, gotCount := txn.reusableChunkRun(chunk)
			if gotStart != wantStart || gotCount != wantCount {
				t.Fatalf("chunk=%d trial=%d run=(%d,%d) want (%d,%d)", chunkNo, trial, gotStart, gotCount, wantStart, wantCount)
			}
		}
	}
}

func TestMetadataFreshPathDoesNotDetachTwice4627(t *testing.T) {
	base := MustNewFreelistGenerationV1(1, 512, []uint64{2, 3, 4, 5}, nil)
	root := mutateChunk(base.root, 0, 0, func(c *stateChunk) { c.setFree(2, false) })
	var detached *stateNode
	if allocs := testing.AllocsPerRun(100, func() {
		detached = detachMetadataSiblings(root, 0, 0)
	}); allocs != 0 {
		t.Fatalf("fresh metadata path detached %g allocations, want zero", allocs)
	}
	if detached != root || !lookupChunk(base.root, 0).isFree(2) {
		t.Fatal("fresh path identity or source isolation lost")
	}
}

func TestMetadataLosingClaimPreservesTransaction4627(t *testing.T) {
	free := make([]uint64, 198)
	for i := range free {
		free[i] = uint64(i + 2)
	}
	ledger := NewReservationLedger()
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 512, free, nil), ledger)
	if err := txn.ReservePage(190); err != nil {
		t.Fatal(err)
	}
	before, err := txn.cloneForAllocatorPrepare()
	if err != nil {
		t.Fatal(err)
	}
	blocker := candidateIDFromString("data-owner")
	if err := ledger.reserve(blocker, []uint64{190}); err != nil {
		t.Fatal(err)
	}
	id := candidateIDFromString("losing-metadata")
	if _, _, _, ok := txn.tryReusedMetadata(id); ok {
		t.Fatal("accepted conflicting data ownership")
	}
	if txn.root != before.root || !maps.Equal(txn.replacedMetadata, before.replacedMetadata) || !maps.Equal(txn.changedChunks, before.changedChunks) || !slices.Equal(txn.allocated, before.allocated) || !slices.Equal(txn.abandonedAppends, before.abandonedAppends) {
		t.Fatal("losing claim changed staged transaction")
	}
	if ledger.candidates[id] != nil || ledger.owners[190] != blocker {
		t.Fatal("losing claim changed ownership")
	}
}

func TestMetadataImpossibleRunDoesNotAllocate4627(t *testing.T) {
	ledger := NewReservationLedger()
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 512, []uint64{2, 3, 4, 5}, nil), ledger)
	root := txn.root
	id := candidateIDFromString("too-small")
	allocs := testing.AllocsPerRun(100, func() {
		if _, _, _, ok := txn.tryReusedMetadata(id); ok {
			t.Fatal("four pages cannot hold this dirty state plus reservation and header")
		}
	})
	if allocs != 0 {
		t.Fatalf("impossible placement allocated %g times, want zero", allocs)
	}
	if txn.root != root || len(ledger.candidates) != 0 || len(ledger.owners) != 0 {
		t.Fatal("impossible placement changed transaction or ownership")
	}
}

func TestMetadataCursorConcurrentClaims4627(t *testing.T) {
	var free []uint64
	for id := uint64(2); id < 1536; id++ {
		free = append(free, id)
	}
	g := MustNewFreelistGenerationV1(1, 2048, free, nil)
	ledger := NewReservationLedger()
	var wg sync.WaitGroup
	const workers = 8
	results := make([]*FreelistCandidateV1, workers)
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			txn := NewFreelistTxn(g, ledger)
			results[i], errs[i] = txn.MaterializeCandidate(2, 2, candidateIDFromString(fmt.Sprint(i)), NewMemoryPageStoreV1())
		}(i)
	}
	wg.Wait()
	seen := map[uint64]bool{}
	for i, c := range results {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		for _, id := range c.DirtyPageIDs() {
			if seen[id] {
				t.Fatalf("duplicate physical claim %d", id)
			}
			seen[id] = true
		}
	}
}

func TestMetadataCursorOccupiedChunkAdvancesWithoutOwnershipLoss4627(t *testing.T) {
	var free []uint64
	for id := uint64(2); id < 256; id++ {
		free = append(free, id)
	}
	ledger := NewReservationLedger()
	blocker := candidateIDFromString("blocker")
	if err := ledger.reserve(blocker, free); err != nil {
		t.Fatal(err)
	}
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 512, free, nil), ledger)
	_, _, _, ok := txn.tryReusedMetadata(candidateIDFromString("denied"))
	if ok || ledger.nextReuseChunk != 1 {
		t.Fatal("occupied attempt must advance hint only")
	}
	for _, id := range free {
		if !ledger.Reserved(id) {
			t.Fatalf("lost blocker page %d", id)
		}
	}
	if err := ledger.Abandon(blocker); err != nil {
		t.Fatal(err)
	}
	_, _, _, ok = txn.tryReusedMetadata(candidateIDFromString("retry"))
	if !ok {
		t.Fatal("wrap retry failed after blocker release")
	}
}

type capabilityCheckedMetadataSink4627 struct {
	store *MemoryPageStoreV1
	safe  *FreelistGenerationV1
}

func TestMetadataCursorCompletionPolicy4627(t *testing.T) {
	ledger := NewReservationLedger()
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 1024, []uint64{258, 514}, nil), ledger)
	ledger.nextReuseChunk = 1
	search := txn.metadataChunkSearch()
	if chunk := search.next(txn); chunk == nil || chunk.chunkNo != 1 {
		t.Fatal("first chunk")
	}
	search.finish(txn, true)
	if ledger.nextReuseChunk != 1 {
		t.Fatal("successful chunk must stay sticky")
	}
	search = txn.metadataChunkSearch()
	search.next(txn)
	ledger.nextReuseChunk = 9 // Another completed search changes the hint.
	search.finish(txn, false)
	if ledger.nextReuseChunk != 9 {
		t.Fatal("overwrote concurrent hint")
	}
	search = txn.metadataChunkSearch()
	search.finish(txn, false)
	if ledger.nextReuseChunk != 9 {
		t.Fatal("empty observation changed hint")
	}
	search = txn.metadataChunkSearch()
	for search.next(txn) != nil {
	}
	search.finish(txn, false)
	if ledger.nextReuseChunk != 3 {
		t.Fatalf("wrapped failure lost last observation: %d", ledger.nextReuseChunk)
	}
}

func (s capabilityCheckedMetadataSink4627) WritePage(id uint64, data []byte) error {
	if _, exists := s.store.Pages[id]; exists && !s.safe.Allocatable(id) {
		return fmt.Errorf("overwrite page %d without staged capability", id)
	}
	return (&overwritingMetadataSink4627{store: s.store, remaining: 1}).WritePage(id, data)
}

func TestMetadataChunkSearchWrapAndBound4627(t *testing.T) {
	for _, tc := range []struct {
		name   string
		chunks []uint64
		hint   uint64
		want   []uint64
	}{
		{"gap", []uint64{0, 2, 5}, 3, []uint64{5, 0, 2}},
		{"one", []uint64{2}, 2, []uint64{2}},
		{"after", []uint64{2}, 3, []uint64{2}},
		{"limit", []uint64{0, 1, 2, 3, 4, 5}, 3, []uint64{3, 4, 5, 0}},
		{"sentinel", []uint64{0, math.MaxUint64 >> freelistChunkShift}, (math.MaxUint64 >> freelistChunkShift) + 1, []uint64{0, math.MaxUint64 >> freelistChunkShift}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var free []uint64
			for _, chunk := range tc.chunks {
				free = append(free, chunk<<freelistChunkShift|2)
			}
			ledger := NewReservationLedger()
			ledger.nextReuseChunk = tc.hint
			txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, math.MaxUint64, free, nil), ledger)
			search := txn.metadataChunkSearch()
			var got []uint64
			for chunk := search.next(txn); chunk != nil; chunk = search.next(txn) {
				got = append(got, chunk.chunkNo)
				txn.reusableChunkRun(chunk)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("chunks=%v want%v", got, tc.want)
			}
			if len(got) > 4 {
				t.Fatal("unbounded search")
			}
		})
	}
}

func TestMetadataAuxiliarySharesCursor4627(t *testing.T) {
	var free []uint64
	for id := uint64(2); id < 768; id++ {
		free = append(free, id)
	}
	ledger := NewReservationLedger()
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 1024, free, nil), ledger)
	aux, ok := txn.allocateReusedRange(2)
	if !ok || aux[0]>>freelistChunkShift != 0 {
		t.Fatal("aux placement")
	}
	id := candidateIDFromString("shared")
	c, err := txn.MaterializeCandidate(2, 2, id, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	if c.GenerationRef().HeaderPageID>>freelistChunkShift != 0 {
		t.Fatalf("metadata failed to follow auxiliary cursor: %d", c.GenerationRef().HeaderPageID)
	}
}

func TestMetadataSearchMakesBoundedProgress4627(t *testing.T) {
	free := []uint64{2, 258, 514, 770}
	for id := uint64(1024); id < 1280; id++ {
		free = append(free, id)
	}
	g := MustNewFreelistGenerationV1(1, 2048, free, nil)
	ledger := NewReservationLedger()
	store := NewMemoryPageStoreV1()
	for i := 0; i < 2; i++ {
		before := g.HighWater()
		txn := NewFreelistTxn(g, ledger)
		id := candidateIDFromString("progress")
		c, err := txn.MaterializeCandidate(g.GenerationID()+1, g.CommitSeq()+1, id, store)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && c.Generation().HighWater() <= before {
			t.Fatal("first attempt exceeded four-chunk search contract")
		}
		if i == 1 && c.Generation().HighWater() != before {
			t.Fatalf("second publication failed to reach fifth safe chunk: %d -> %d", before, c.Generation().HighWater())
		}
		if err := ledger.MarkVisible(id); err != nil {
			t.Fatal(err)
		}
		if err := ledger.Publish(id); err != nil {
			t.Fatal(err)
		}
		g = c.Generation()
		decoded, err := LoadGenerationV1(store, c.GenerationRef())
		if err != nil {
			t.Fatal(err)
		}
		for _, page := range c.DirtyPageIDs() {
			if decoded.Allocatable(page) {
				t.Fatalf("owned page %d free", page)
			}
		}
	}
}
