package caching

import (
	"sync"
	"testing"
)

func TestPrimaryAllocationObserverOwnershipAndSeam(t *testing.T) {
	o, err := BeginPrimaryAllocationObservation()
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if primaryAllocationObservation.Load() == o { _ = o.Close() } })
	if _, err := BeginPrimaryAllocationObservation(); err == nil { t.Fatal("overlap accepted") }
	primaryAllocationCheckpointEntered(false)
	primaryAllocationCheckpointEntered(true)
	x := primaryAllocationReadDirStarted(64)
	if x != o { t.Fatal("wrong observer") }
	x.readDirCompleted(false)
	x = primaryAllocationReadDirStarted(80)
	x.readDirCompleted(true)
	s := o.Snapshot()
	if s.CheckpointCalls != 2 || s.AutomaticCheckpointCalls != 1 || s.ReadDirCalls != 2 || s.ReadDirFinished != 2 || s.ReadDirErrors != 1 || s.PathBytes != 144 || s.MinPathBytes != 64 || s.MaxPathBytes != 80 { t.Fatalf("counters: %+v", s) }
	if err := o.Close(); err != nil { t.Fatal(err) }
	if err := o.Close(); err == nil { t.Fatal("double release accepted") }
	if x := primaryAllocationReadDirStarted(90); x != nil { t.Fatal("observer-off retained owner") }
}

func TestPrimaryAllocationObserverLateFinishDoesNotChargeSuccessor(t *testing.T) {
	old, err := BeginPrimaryAllocationObservation()
	if err != nil { t.Fatal(err) }
	borrowed := primaryAllocationReadDirStarted(64)
	if err := old.Close(); err != nil { t.Fatal(err) }
	next, err := BeginPrimaryAllocationObservation()
	if err != nil { t.Fatal(err) }
	defer func() { if err := next.Close(); err != nil { t.Error(err) } }()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); borrowed.readDirCompleted(true) }()
	wg.Wait()
	if next.Snapshot() != (PrimaryAllocationSeamCounters{}) { t.Fatal("late finish charged successor") }
	if old.Snapshot().ReadDirErrors != 1 { t.Fatal("old completion lost") }
}
