package freelist

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestFreelistGenerationV1_OlderGenerationRemainsImmutable(t *testing.T) {
	base := MustNewFreelistGenerationV1(7, 100, []uint64{4, 8, 12}, nil)
	txn := NewFreelistTxn(base, NewReservationLedger())
	if _, err := txn.Allocate(0); err != nil {
		t.Fatal(err)
	}
	next, err := txn.Materialize(8)
	if err != nil {
		t.Fatal(err)
	}
	if !base.Allocatable(12) || next.Allocatable(12) {
		t.Fatalf("old generation changed or new generation retained reservation")
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("old generation invalid: %v", err)
	}
}

func TestFreelistGenerationV1_HorizonPreventsPrematureReuse(t *testing.T) {
	base := MustNewFreelistGenerationV1(1, 10, nil, map[uint64]uint64{5: 9})
	txn := NewFreelistTxn(base, NewReservationLedger())
	txn.Prune(RecoveryHorizon{OldestRecoverableCommitSeq: 9, MinPinnedSnapshotCommitSeq: ^uint64(0), HistoryFloorCommitSeq: ^uint64(0)})
	id, err := txn.Allocate(0)
	if err != nil || id != 10 {
		t.Fatalf("got (%d, %v), want append 10", id, err)
	}
	txn = NewFreelistTxn(base, NewReservationLedger())
	txn.Prune(RecoveryHorizon{OldestRecoverableCommitSeq: 10, MinPinnedSnapshotCommitSeq: ^uint64(0), HistoryFloorCommitSeq: ^uint64(0)})
	id, err = txn.Allocate(0)
	if err != nil || id != 5 {
		t.Fatalf("got (%d, %v), want reusable 5", id, err)
	}
}

func TestFreelistTxn_CandidateReservationsSurviveSupersedeAndFailure(t *testing.T) {
	ledger := NewReservationLedger()
	base := MustNewFreelistGenerationV1(1, 20, []uint64{3}, nil)
	first := NewFreelistTxn(base, ledger)
	if _, err := first.Allocate(0); err != nil {
		t.Fatal(err)
	}
	if err := first.Reserve("one"); err != nil {
		t.Fatal(err)
	}
	second := NewFreelistTxn(base, ledger)
	if err := second.ReservePage(3); !errors.Is(err, ErrPageReserved) {
		t.Fatalf("double reservation: %v", err)
	}
	if err := ledger.Supersede("one", "two"); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Fail("two"); err != nil {
		t.Fatal(err)
	}
	third := NewFreelistTxn(base, ledger)
	id, err := third.Allocate(0)
	if err != nil || id != 3 {
		t.Fatalf("failed candidate did not release reservation: %d %v", id, err)
	}
}

func TestReservationLedger_VisibleFailureRetainsOwnershipUntilDurablePublish(t *testing.T) {
	ledger := NewReservationLedger()
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 9, []uint64{2}, nil), ledger)
	if _, err := txn.Allocate(0); err != nil {
		t.Fatal(err)
	}
	if err := txn.Reserve("candidate"); err != nil {
		t.Fatal(err)
	}
	if err := ledger.MarkVisible("candidate"); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Fail("candidate"); err == nil {
		t.Fatal("visible failure released ownership")
	}
	if !ledger.Reserved(2) {
		t.Fatal("visible failure lost reservation")
	}
	if err := ledger.Retry("candidate"); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Poison("candidate"); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Shutdown("candidate"); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Publish("candidate"); err != nil {
		t.Fatal(err)
	}
	if ledger.Reserved(2) {
		t.Fatal("durable publish retained reservation")
	}
}

func TestFreelistTxn_RegionHintIsDeterministic(t *testing.T) {
	base := MustNewFreelistGenerationV1(1, 30_000, []uint64{10, 8_191, 8_192, 16_383}, nil)
	for i := 0; i < 20; i++ {
		id, err := NewFreelistTxn(base, NewReservationLedger()).Allocate(8_200)
		if err != nil || id != 16_383 {
			t.Fatalf("run %d: got %d, %v", i, id, err)
		}
	}
}

func TestFreelistGenerationV1_DecodeRejectsCountOverflow(t *testing.T) {
	g := MustNewFreelistGenerationV1(1, 2, []uint64{1}, nil)
	b, err := g.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint64(b[24:], ^uint64(0))
	binary.LittleEndian.PutUint32(b[40:], generationChecksum(b))
	if _, err := DecodeFreelistGenerationV1(b); !errors.Is(err, ErrGenerationFormat) {
		t.Fatalf("err=%v", err)
	}
}

func TestFreelistGenerationV1_CodecRejectsCorruptionAndRoundTrips(t *testing.T) {
	g := MustNewFreelistGenerationV1(3, 99, []uint64{2, 5, 8}, map[uint64]uint64{11: 7})
	b, err := g.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeFreelistGenerationV1(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 1
	if _, err := DecodeFreelistGenerationV1(b); !errors.Is(err, ErrGenerationChecksum) {
		t.Fatalf("corruption error=%v", err)
	}
}

func TestFreelistGenerationV1_TinyChunkChurnIsBounded(t *testing.T) {
	g := MustNewFreelistGenerationV1(1, 100, []uint64{1, 2, 3, 4, 5, 6, 7, 8}, nil)
	for i := uint64(0); i < 1000; i++ {
		txn := NewFreelistTxn(g, NewReservationLedger())
		if _, err := txn.Allocate(i); err != nil {
			t.Fatal(err)
		}
		var err error
		g, err = txn.Materialize(g.GenerationID() + 1)
		if err != nil {
			t.Fatal(err)
		}
		if stats := txn.Stats(); stats.COWChunks > 1 {
			t.Fatalf("COW chunks=%d, want <=1", stats.COWChunks)
		}
	}
}
