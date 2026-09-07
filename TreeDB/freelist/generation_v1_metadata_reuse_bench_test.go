package freelist

import "testing"

// Model boundary: advancing certified horizon, one persistent ledger, ordinary
// allocate/retire plus publication each iteration. Not a real DB seal benchmark.
func BenchmarkFreelistMetadataCertifiedChurn4627(b *testing.B) {
	ledger := NewReservationLedger()
	seed := NewFreelistTxn(MustNewFreelistGenerationV1(1, 2, nil, nil), ledger)
	for i := 0; i < 512; i++ {
		id, err := seed.Allocate(0)
		if err != nil {
			b.Fatal(err)
		}
		seed.Retire(id, 1)
	}
	id := candidateIDFromString("seed")
	candidate, err := seed.MaterializeCandidate(2, 2, id, NewMemoryPageStoreV1())
	if err != nil {
		b.Fatal(err)
	}
	if err := ledger.MarkVisible(id); err != nil {
		b.Fatal(err)
	}
	if err := ledger.Publish(id); err != nil {
		b.Fatal(err)
	}
	g := candidate.Generation()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := NewFreelistTxn(g, ledger)
		capability, err := NewReuseCapability(g.CommitSeq(), g.CommitSeq(), 0)
		if err != nil {
			b.Fatal(err)
		}
		txn.PruneWithCapability(capability)
		page, err := txn.Allocate(0)
		if err != nil {
			b.Fatal(err)
		}
		txn.Retire(page, g.CommitSeq()+1)
		id := candidateIDFromString("churn")
		candidate, err := txn.MaterializeCandidate(g.GenerationID()+1, g.CommitSeq()+1, id, NewMemoryPageStoreV1())
		if err != nil {
			b.Fatal(err)
		}
		if err := ledger.MarkVisible(id); err != nil {
			b.Fatal(err)
		}
		if err := ledger.Publish(id); err != nil {
			b.Fatal(err)
		}
		g = candidate.Generation()
	}
	b.ReportMetric(float64(g.HighWater()), "high-water")
	b.ReportMetric(float64(g.FreeCount()), "free-pages")
}
