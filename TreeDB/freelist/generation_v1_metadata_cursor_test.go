package freelist

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
)

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
