package memtable

import (
	"errors"
	"sync"
	"testing"
	"unsafe"
)

func TestCOWMaintenanceReservationSurvivesProducerPressureAndLastFinalizer(t *testing.T) {
	limits := DefaultCOWLimits()
	limits.MaxTotalBytes, limits.MaxInFlightBytes, limits.MaxRetiredBytes, limits.MaxGenerationBytes = 64<<10, 64<<10, 64<<10, 16<<10
	parent, err := NewCOWBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := parent.ReserveMaintenance(32 << 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maintenance.ReserveMaintenance(1024); !errors.Is(err, ErrCOWCapacity) {
		t.Fatalf("recursive partition accepted: %v", err)
	}
	before := parent.Stats()
	overhead := COWAllocationCharge(uint64(unsafe.Sizeof(COWExternalLease{})))
	pressure, err := parent.AcquireExternal(limits.MaxTotalBytes - before.TotalBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.AcquireExternal(1); !errors.Is(err, ErrCOWCapacity) {
		t.Fatalf("producer spent maintenance capacity: %v", err)
	}
	child, err := maintenance.AcquireExternal(4096)
	if err != nil {
		t.Fatalf("reserved lane blocked by producer: %v", err)
	}
	parent.Close()
	parent.Close()
	if parent.Stats().TotalBytes == 0 {
		t.Fatal("parent refunded pending child finalizer")
	}
	if _, err := maintenance.AcquireExternal(1); !errors.Is(err, ErrCOWClosed) {
		t.Fatalf("closed partition admitted: %v", err)
	}
	var join sync.WaitGroup
	join.Add(2)
	go func() { defer join.Done(); child.Close(); child.Close() }()
	go func() { defer join.Done(); parent.Close(); maintenance.Close(); pressure.Close() }()
	join.Wait()
	if s := parent.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 || s.ControlBytes != 0 {
		t.Fatalf("original parent reservation retained: %+v", s)
	}
	if s := maintenance.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 || s.ControlBytes != 0 {
		t.Fatalf("child did not finish: %+v", s)
	}
}

func TestCOWMaintenanceReservationRejectsMandatoryLeaseOverlap(t *testing.T) {
	parent, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	maintenance, err := parent.ReserveMaintenance(32 << 10)
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	live, err := maintenance.AcquireExternal(16 << 10)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	candidate, err := maintenance.AcquireExternal(8 << 10)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Close()
	foreign, err := parent.AcquireExternal(16 << 10)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()

	const successorBytes = 8 << 10
	if successor, err := maintenance.AcquireExternal(successorBytes); !errors.Is(err, ErrCOWCapacity) {
		successor.Close()
		t.Fatalf("allocator admitted overlapping live, candidate and successor leases: %v", err)
	}
	if maintenance.ExternalAllocationFitsWith(successorBytes, live, candidate) {
		t.Fatal("mandatory live leases were classified as refundable pressure")
	}
	if !maintenance.ExternalAllocationFitsWith(successorBytes, live, foreign) {
		t.Fatal("independently refundable or foreign leases were classified as mandatory pressure")
	}
	candidate.Close()
	successor, err := maintenance.AcquireExternal(successorBytes)
	if err != nil {
		t.Fatalf("released pressure did not make the actual successor reservation fit: %v", err)
	}
	successor.Close()
}
