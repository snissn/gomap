package freelist

import (
	"math/rand"
	"testing"
)

// TestFreelistGenerationV1_ModelLifecycle exercises the standalone state
// machine used by publication/recovery adapters: candidates may be abandoned,
// old generations remain readable, and pins/fallbacks hold reuse back.
func TestFreelistGenerationV1_ModelLifecycle(t *testing.T) {
	rng := rand.New(rand.NewSource(3678))
	g := MustNewFreelistGenerationV1(1, 64, nil, nil)
	ledger := NewReservationLedger()
	for seq := uint64(1); seq <= 1000; seq++ {
		txn := NewFreelistTxn(g, ledger)
		id, err := txn.Allocate(rng.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		txn.Retire(id, seq)
		name := "candidate"
		if err := txn.Reserve(name); err != nil {
			t.Fatal(err)
		}
		if seq%3 == 0 {
			if err := ledger.Supersede(name, "successor"); err != nil {
				t.Fatal(err)
			}
			name = "successor"
		}
		if seq%5 == 0 { // crash/fallback: candidate never became durable.
			if err := ledger.Fail(name); err != nil {
				t.Fatal(err)
			}
		} else if err := ledger.Publish(name); err != nil {
			t.Fatal(err)
		}
		txn.Prune(RecoveryHorizon{OldestRecoverableCommitSeq: seq, MinPinnedSnapshotCommitSeq: seq - 1, HistoryFloorCommitSeq: seq})
		g, err = txn.Materialize(seq + 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Validate(); err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
		encoded, err := g.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		g, err = DecodeFreelistGenerationV1(encoded)
		if err != nil {
			t.Fatalf("reopen seq %d: %v", seq, err)
		}
	}
}

func BenchmarkFreelistGenerationV1_Churn(b *testing.B) {
	for _, pages := range []int{64, 1024} {
		b.Run("pages="+itoa(pages), func(b *testing.B) {
			free := make([]uint64, pages)
			for i := range free {
				free[i] = uint64(i + 1)
			}
			g := MustNewFreelistGenerationV1(1, uint64(pages+1), free, nil)
			ledger := NewReservationLedger()
			var stats FreelistTxnStats
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := NewFreelistTxn(g, ledger)
				id, err := txn.Allocate(0)
				if err != nil {
					b.Fatal(err)
				}
				txn.Retire(id, uint64(i+1))
				txn.Prune(RecoveryHorizon{OldestRecoverableCommitSeq: uint64(i + 2), MinPinnedSnapshotCommitSeq: ^uint64(0), HistoryFloorCommitSeq: ^uint64(0)})
				g, err = txn.Materialize(g.GenerationID() + 1)
				if err != nil {
					b.Fatal(err)
				}
				stats = txn.Stats()
			}
			b.ReportMetric(float64(stats.COWPages), "cow-pages/op")
			b.ReportMetric(float64(stats.COWBytes), "cow-bytes/op")
			b.ReportMetric(float64(g.HighWater()), "high-water")
		})
	}
}

func itoa(n int) string {
	if n == 64 {
		return "64"
	}
	return "1024"
}
