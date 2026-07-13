package freelist

import "testing"

func TestFreelistGenerationV1_100KLogicalChurnConverges(t *testing.T) {
	g := MustNewFreelistGenerationV1(1, 64, nil, nil)
	ledger := NewReservationLedger()
	candidateID := candidateIDFromString("candidate")
	for seq := uint64(1); seq <= 100_000; seq++ {
		txn := NewFreelistTxn(g, ledger)
		id, err := txn.Allocate(0)
		if err != nil {
			t.Fatal(err)
		}
		txn.Retire(id, seq)
		if err := txn.Reserve(candidateID); err != nil {
			t.Fatal(err)
		}
		if err := ledger.MarkVisible(candidateID); err != nil {
			t.Fatal(err)
		}
		if err := ledger.Publish(candidateID); err != nil {
			t.Fatal(err)
		}
		txn.Prune(RecoveryHorizon{OldestRecoverableCommitSeq: seq, MinPinnedSnapshotCommitSeq: seq - 1, HistoryFloorCommitSeq: seq})
		g = &FreelistGenerationV1{generationID: seq + 1, commitSeq: seq + 1, parentGenerationID: g.generationID, parentCommitSeq: g.commitSeq, highWater: txn.highWater, root: txn.root}
		if g.root.freeCount+g.root.retiredCount > 4 {
			t.Fatalf("seq %d retained state=%d", seq, g.root.freeCount+g.root.retiredCount)
		}
	}
	if g.HighWater() > 67 {
		t.Fatalf("high-water=%d", g.HighWater())
	}
	if ledger.Reservations() != 0 {
		t.Fatalf("reservations=%d", ledger.Reservations())
	}
}

func FuzzFreelistGenerationV1_Model(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5})
	f.Add([]byte{255, 0, 17, 3, 91, 22})
	f.Fuzz(func(t *testing.T, actions []byte) {
		if len(actions) > 512 {
			actions = actions[:512]
		}
		g := MustNewFreelistGenerationV1(1, 64, []uint64{2, 3, 4, 5}, nil)
		for step, action := range actions {
			txn := NewFreelistTxn(g, NewReservationLedger())
			seq := uint64(step + 2)
			switch action % 3 {
			case 0:
				id, err := txn.Allocate(uint64(action) << 8)
				if err == nil {
					txn.Retire(id, seq)
				}
			case 1:
				txn.Retire(uint64(action)+2, seq)
			case 2:
				capability, _ := NewReuseCapability(seq, seq, seq)
				txn.PruneWithCapability(capability)
			}
			g = &FreelistGenerationV1{generationID: seq, commitSeq: seq, parentGenerationID: g.generationID, parentCommitSeq: g.commitSeq, highWater: txn.highWater, root: txn.root}
			if err := g.Validate(); err != nil {
				t.Fatalf("step=%d action=%d: %v", step, action, err)
			}
		}
	})
}

func BenchmarkFreelistGenerationV1_Churn(b *testing.B) {
	for _, pages := range []int{64, 1024, 1 << 20} {
		b.Run(benchmarkSizeName(pages), func(b *testing.B) {
			free := make([]uint64, 0, min(pages, 4096))
			step := max(1, pages/4096)
			for id := 2; id < pages; id += step {
				free = append(free, uint64(id))
			}
			g := MustNewFreelistGenerationV1(1, uint64(pages+2), free, nil)
			store := NewMemoryPageStoreV1()
			g = materializeTestGeneration(b, g, 2, store)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := NewFreelistTxn(g, NewReservationLedger())
				id, err := txn.Allocate(uint64(i))
				if err != nil {
					b.Fatal(err)
				}
				txn.Retire(id, uint64(i+1))
				sink := NewMemoryPageStoreV1()
				candidate, err := txn.MaterializeCandidate(g.GenerationID()+1, g.CommitSeq()+1, candidateIDFromString("bench"), sink)
				if err != nil {
					b.Fatal(err)
				}
				g = candidate.Generation()
				stats := txn.Stats()
				b.ReportMetric(float64(stats.COWPages), "cow-pages/op")
				b.ReportMetric(float64(stats.COWBytes), "cow-bytes/op")
				b.ReportMetric(float64(g.HighWater()), "high-water")
			}
		})
	}
}

func materializeTestGeneration(tb testing.TB, g *FreelistGenerationV1, id uint64, store *MemoryPageStoreV1) *FreelistGenerationV1 {
	tb.Helper()
	txn := NewFreelistTxn(g, NewReservationLedger())
	candidate, err := txn.MaterializeCandidate(id, id, candidateIDFromString("test"), store)
	if err != nil {
		tb.Fatal(err)
	}
	return candidate.Generation()
}

func benchmarkSizeName(n int) string {
	switch n {
	case 64:
		return "pages=64"
	case 1024:
		return "pages=1024"
	default:
		return "pages=1048576"
	}
}
