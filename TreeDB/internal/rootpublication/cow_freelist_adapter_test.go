package rootpublication

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
)

func TestCowFreelistExtensionCarriesExactOrderedReferences(t *testing.T) {
	store := freelist.NewMemoryPageStoreV1()
	base := freelist.MustNewFreelistGenerationV1(1, 8, []uint64{2}, nil)
	txn := freelist.NewFreelistTxn(base, nil)
	firstCandidate, err := txn.MaterializeCandidate(2, 2, freelist.CandidateIDV1{1}, store)
	if err != nil {
		t.Fatal(err)
	}
	first, err := newPreparedRootCandidateWithCowFreelist(CandidateSpec{Frontier: NewFrontier(2, 2, 2, 2, 2)}, firstCandidate.Generation())
	if err != nil {
		t.Fatal(err)
	}
	txn = freelist.NewFreelistTxn(firstCandidate.Generation(), nil)
	secondStore := freelist.NewMemoryPageStoreV1()
	secondCandidate, err := txn.MaterializeCandidate(3, 3, freelist.CandidateIDV1{2}, secondStore)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newPreparedRootCandidateWithCowFreelist(CandidateSpec{Frontier: NewFrontier(3, 3, 3, 3, 3)}, secondCandidate.Generation())
	if err != nil {
		t.Fatal(err)
	}
	coalesced, err := coalesceCandidates([]*PreparedRootCandidate{first, second})
	if err != nil {
		t.Fatal(err)
	}
	extension, ok := coalesced.extensions.cowFreelist.(cowFreelistExtension)
	if !ok || len(extension.chain) != 2 {
		t.Fatalf("extension=%#v", coalesced.extensions.cowFreelist)
	}
	if extension.chain[0] != firstCandidate.GenerationRef() || extension.chain[1] != secondCandidate.GenerationRef() {
		t.Fatal("coalescing lost exact order")
	}
}

func TestCowFreelistExtensionRejectsWrongType(t *testing.T) {
	_, err := (cowFreelistExtension{}).union(testExtension(1))
	if !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("union error=%v, want ErrResourceConflict", err)
	}
}
