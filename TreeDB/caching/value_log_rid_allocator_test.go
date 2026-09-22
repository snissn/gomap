package caching

import (
	"math"
	"testing"
)

func TestReserveValueLogRIDs_SequentialRanges(t *testing.T) {
	db := &DB{}

	start, err := db.ReserveValueLogRIDs(3)
	if err != nil {
		t.Fatalf("ReserveValueLogRIDs(3): %v", err)
	}
	if start != 1 {
		t.Fatalf("first range start=%d want=1", start)
	}
	if got := db.nextRID.Load(); got != 3 {
		t.Fatalf("nextRID after first reserve=%d want=3", got)
	}

	start, err = db.ReserveValueLogRIDs(2)
	if err != nil {
		t.Fatalf("ReserveValueLogRIDs(2): %v", err)
	}
	if start != 4 {
		t.Fatalf("second range start=%d want=4", start)
	}
	if got := db.nextRID.Load(); got != 5 {
		t.Fatalf("nextRID after second reserve=%d want=5", got)
	}
}

func TestReserveValueLogRIDs_InvalidCount(t *testing.T) {
	db := &DB{}
	if _, err := db.ReserveValueLogRIDs(0); err == nil {
		t.Fatal("expected error for count=0")
	}
	if _, err := db.ReserveValueLogRIDs(-1); err == nil {
		t.Fatal("expected error for count<0")
	}
}

func TestReserveValueLogRIDs_OverflowDoesNotAdvance(t *testing.T) {
	db := &DB{}
	db.nextRID.Store(math.MaxUint64 - 1)

	if _, err := db.ReserveValueLogRIDs(2); err == nil {
		t.Fatal("expected overflow error for count=2 near max")
	}
	if got := db.nextRID.Load(); got != math.MaxUint64-1 {
		t.Fatalf("nextRID changed after failed reserve: got=%d want=%d", got, uint64(math.MaxUint64-1))
	}

	start, err := db.ReserveValueLogRIDs(1)
	if err != nil {
		t.Fatalf("ReserveValueLogRIDs(1): %v", err)
	}
	if start != math.MaxUint64 {
		t.Fatalf("range start=%d want=%d", start, uint64(math.MaxUint64))
	}
	if got := db.nextRID.Load(); got != math.MaxUint64 {
		t.Fatalf("nextRID after reserve=%d want=%d", got, uint64(math.MaxUint64))
	}

	if _, err := db.ReserveValueLogRIDs(1); err == nil {
		t.Fatal("expected exhaustion at max rid")
	}
	if got := db.nextRID.Load(); got != math.MaxUint64 {
		t.Fatalf("nextRID changed after exhaustion: got=%d want=%d", got, uint64(math.MaxUint64))
	}
}
