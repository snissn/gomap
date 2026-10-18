package memtable

import (
	"errors"
	"testing"
)

func TestCOWRawSourceReservationSharesBudgetAndLastRootLifetime(t *testing.T) {
	limits := DefaultCOWLimits()
	limits.MaxSources = 1
	b, w := cowTestWriter(t, limits)
	other, err := NewCOWWriter(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.ReserveRawRolloverSource(); err != nil {
		t.Fatal(err)
	}
	if err = other.Freeze(); !errors.Is(err, ErrCOWCapacity) {
		t.Fatalf("standalone bypassed shared source bound: %v", err)
	}
	w.CancelRawRolloverSource()
	if b.Stats().Sources != 0 {
		t.Fatal("canceled source retained")
	}
	if err = w.ReserveRawRolloverSource(); err != nil {
		t.Fatal(err)
	}
	root := cowTestPublish(t, w, cowTestEntries(1))
	if err = w.Freeze(); err != nil {
		t.Fatal(err)
	}
	if b.Stats().Sources != 1 {
		t.Fatal("freeze double charged source")
	}
	cowTestClose(w)
	if b.Stats().Sources != 1 {
		t.Fatal("writer Close refunded held root")
	}
	cowTestRelease(root)
	if b.Stats().Sources != 0 {
		t.Fatal("last root did not refund source")
	}
	if err = other.Freeze(); err != nil {
		t.Fatal(err)
	}
	cowTestClose(other)
	b.Close()
	if stats := b.Stats(); stats.TotalBytes != 0 || stats.Sources != 0 || stats.Generations != 0 {
		t.Fatalf("final ownership: %+v", stats)
	}
}
