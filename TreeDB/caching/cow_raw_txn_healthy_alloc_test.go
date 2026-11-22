package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Keep the actual batch-owner call, including its nil decisions, ticket
// install, and pair lifetime. No DB stage/commit/walltime is inferred.
func TestRawTxnPairHealthyAdmissionAllocations(t *testing.T) {
	cache, _ := cowAutomaticFallbackFixture(t, "cow_btree")
	b := cache.NewBatch()
	defer b.Close()
	if err := b.Set([]byte("admission"), []byte("witness")); err != nil {
		t.Fatal(err)
	}
	pair := &cowTxnState{pairAdmission: true}
	b.cowState.txn = &ConditionalTxn{cow: pair}
	defer func() { b.cowState.txn = nil }()
	check := func() {
		if err := b.admitCOWAccepted(); err != nil {
			t.Fatalf("healthy raw admission returned %v", err)
		}
		if !b.cowState.accepted.Valid() || !pair.barrierSpare.Valid() {
			t.Fatal("healthy admission returned fewer than two real tickets")
		}
		if err := b.cowState.accepted.CancelAccepted(); err != nil {
			t.Fatal(err)
		}
		b.cowState.accepted = rootpublication.AcceptedTicket{}
		if err := pair.cancelBarrierSpare(); err != nil {
			t.Fatal(err)
		}
	}
	if got := testing.AllocsPerRun(1000, check); got != 0 {
		t.Fatalf("healthy raw admission allocated %.2f objects per run", got)
	}
	t.Log("healthy raw admission + two ticket refunds: 0 allocations per run")
}
