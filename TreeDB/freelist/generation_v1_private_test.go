package freelist

import (
	"bytes"
	"errors"
	"testing"
)

// This representation regression intentionally inspects identity: externally
// observable allocation/recovery behavior must remain equivalent.
func TestFreelistTxnPrivateRepeatedMutationReusesChunk(t *testing.T) {
	base := MustNewFreelistGenerationV1(1, 1024, nil, nil)
	txn := NewFreelistTxn(base, NewReservationLedger())
	txn.Retire(260, 1)
	first := lookupChunk(txn.root, 1)
	txn.Retire(261, 1)
	if got := lookupChunk(txn.root, 1); got != first {
		t.Fatal("repeated mutation copied a transaction-private chunk")
	}
	if stats := txn.Stats(); stats.PrivateMutationPaths != 2 || stats.PrivateChunkReuses != 1 {
		t.Fatalf("private activation stats=%+v", stats)
	}
	if base.RetiredCount() != 0 {
		t.Fatal("private mutation changed the immutable base")
	}
	candidate, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("private-repeated"), NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := candidate.Generation().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestFreelistTxnCloneRevokesPrivateAliases(t *testing.T) {
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 1024, nil, nil), NewReservationLedger())
	txn.Retire(260, 1)
	clone, err := txn.cloneForAllocatorPrepare()
	if err != nil {
		t.Fatal(err)
	}
	shared := lookupChunk(clone.root, 1)
	txn.Retire(261, 1)
	if shared.retired[5] != 0 {
		t.Fatal("original modified rollback clone")
	}
	clone.Retire(262, 1)
	if lookupChunk(txn.root, 1).retired[6] != 0 {
		t.Fatal("clone modified original")
	}
	if shared.retired[4] != 1 || shared.retired[5] != 0 || shared.retired[6] != 0 {
		t.Fatal("retained rollback alias changed")
	}
}

func TestFreelistTxnPrivateMaterializationFreezesRetainedGeneration(t *testing.T) {
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 1024, nil, nil), NewReservationLedger())
	txn.Retire(260, 1)
	store := NewMemoryPageStoreV1()
	candidate, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("private-retained"), store)
	if err != nil {
		t.Fatal(err)
	}
	before := candidate.Pages()
	next := NewFreelistTxn(candidate.Generation(), NewReservationLedger())
	next.Retire(261, 2)
	next.Retire(262, 2)
	if lookupChunk(candidate.Generation().root, 1).retired[5] != 0 {
		t.Fatal("next transaction changed retained generation")
	}
	if _, err := next.MaterializeCandidate(3, 3, candidateIDFromString("private-next"), store); err != nil {
		t.Fatal(err)
	}
	after := candidate.Pages()
	for i := range before {
		if before[i].PageID != after[i].PageID || !bytes.Equal(before[i].Data, after[i].Data) {
			t.Fatal("retained candidate page changed")
		}
	}
	reopened, err := LoadGenerationV1(store, candidate.GenerationRef())
	if err != nil {
		t.Fatal(err)
	}
	if reopened.RetiredCount() != 1 {
		t.Fatalf("retained reopened count=%d", reopened.RetiredCount())
	}
	txn.Retire(263, 2)
	if _, err := txn.Allocate(0); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatalf("consumed allocate error=%v", err)
	}
}

func TestFreelistTxnPrivatePrepareDetachesRollbackState(t *testing.T) {
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 1024, nil, nil), NewReservationLedger())
	txn.Retire(260, 1)
	before := lookupChunk(txn.root, 1)
	staged, err := txn.cloneForPrivateAllocatorPrepare()
	if err != nil {
		t.Fatal(err)
	}
	if lookupChunk(staged.root, 1) == before {
		t.Fatal("staging did not detach dirty rollback state")
	}
	staged.Retire(261, 1)
	retained := lookupChunk(staged.root, 1)
	staged.Retire(262, 1)
	if retained != lookupChunk(staged.root, 1) {
		t.Fatal("staging did not reuse private copy")
	}
	if _, err := staged.MaterializeCandidate(2, 2, candidateIDFromString("private-staged"), NewMemoryPageStoreV1()); err != nil {
		t.Fatal(err)
	}
	if before.pageID != 0 || before.retired[5] != 0 || before.retired[6] != 0 {
		t.Fatal("materialization changed rollback state")
	}
	txn.Retire(263, 1)
	if retained.retired[7] != 0 {
		t.Fatal("rollback continuation changed retained candidate")
	}
}

func TestFreelistTxnPrivateFirstCopyIsolatesDirtyChildren(t *testing.T) {
	base := MustNewFreelistGenerationV1(1, 1024, nil, map[uint64]uint64{260: 1, 516: 1})
	// Synthetic materialized parents may still contain unmaterialized child
	// aliases. Acquiring the parent does not confer ownership of those aliases.
	base.root.pageID = 900
	beforeLeft, beforeRight := lookupChunk(base.root, 1), lookupChunk(base.root, 2)
	txn := NewFreelistTxn(base, NewReservationLedger())
	txn.Retire(261, 1)
	txn.Retire(517, 1)
	txn.Retire(518, 1)
	if beforeLeft.retired[5] != 0 || beforeRight.retired[5] != 0 || beforeRight.retired[6] != 0 {
		t.Fatal("first durable copy granted private access to shared dirty children")
	}
	if stats := txn.Stats(); stats.PrivateChunkReuses == 0 {
		t.Fatal("first-copy path never reused its detached chunk")
	}
}

func TestFreelistTxnMaterializeValidationRevokesPrivateAuthority(t *testing.T) {
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 1024, nil, nil), NewReservationLedger())
	txn.Retire(260, 1)
	before := lookupChunk(txn.root, 1)
	if _, err := txn.MaterializeCandidate(0, 0, CandidateIDV1{}, nil); !errors.Is(err, ErrGenerationFormat) {
		t.Fatalf("validation error=%v", err)
	}
	if txn.privatePreparation {
		t.Fatal("validation error retained private permission")
	}
	txn.Retire(261, 1)
	if lookupChunk(txn.root, 1) == before || before.retired[5] != 0 {
		t.Fatal("post-error mutation changed retained dirty alias")
	}
	if _, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("private-validation-retry"), NewMemoryPageStoreV1()); err != nil {
		t.Fatal(err)
	}
}

func TestFreelistTxnPrivateSinkFailureConsumesAndRevokes(t *testing.T) {
	base := MustNewFreelistGenerationV1(1, 1024, nil, map[uint64]uint64{260: 1})
	original := lookupChunk(base.root, 1)
	txn := NewFreelistTxn(base, NewReservationLedger())
	txn.Retire(261, 1)
	if _, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("private-sink-failure"), failingPageSinkV1{}); err == nil {
		t.Fatal("sink failure unexpectedly succeeded")
	}
	if !txn.consumed || txn.privatePreparation {
		t.Fatal("sink failure retained mutation authority")
	}
	if original.pageID != 0 || original.retired[5] != 0 {
		t.Fatal("sink failure annotated or changed immutable base")
	}
	if _, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("private-failed-retry"), NewMemoryPageStoreV1()); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatalf("retry error=%v", err)
	}
}
