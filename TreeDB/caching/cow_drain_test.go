package caching

import (
	"bytes"
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"testing"
	"unsafe"
)

func TestCOWDrainPublishesActivePointersAndTombstones(t *testing.T) {
	db := cowPointerFixture(t)
	oldValue := bytes.Repeat([]byte("old"), 1024)
	for _, key := range []string{"a", "gone"} {
		if err := db.Set([]byte(key), oldValue); err != nil {
			t.Fatal(err)
		}
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	newer := bytes.Repeat([]byte("new"), 1024)
	if err = db.Set([]byte("a"), newer); err != nil {
		t.Fatal(err)
	}
	if err = db.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if found, err := db.backend.Has([]byte("a")); err != nil || found {
		t.Fatalf("unexpected pre-drain backend: %v %v", found, err)
	}
	if err = db.Drain(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.backend.Get([]byte("a")); err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("Drain omitted current pointer: %q %v", got, err)
	}
	if found, err := db.backend.Has([]byte("gone")); err != nil || found {
		t.Fatalf("Drain omitted tombstone: %v %v", found, err)
	}
	for _, key := range []string{"a", "gone"} {
		if got, err := old.Get([]byte(key)); err != nil || !bytes.Equal(got, oldValue) {
			t.Fatalf("old cut %s: %q %v", key, got, err)
		}
	}
	if err = db.Set([]byte("a"), []byte("late")); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("a")); err != nil || string(got) != "late" {
		t.Fatalf("late write: %q %v", got, err)
	}
}

func TestCOWDrainRolloverRefusalPreservesActiveCut(t *testing.T) {
	db := cowPointerFixture(t)
	if err := db.Set([]byte("a"), []byte("active")); err != nil {
		t.Fatal(err)
	}
	before := db.cow.budget.Stats()
	cut := db.cow.cut
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Drain()
	pressure.Close()
	if !errors.Is(err, memtable.ErrCOWCapacity) || db.cow.cut != cut {
		t.Fatalf("rollover refusal changed cut: %v", err)
	}
	if found, err := db.backend.Has([]byte("a")); err != nil || found {
		t.Fatalf("refused Drain published: %v %v", found, err)
	}
	if err = db.Drain(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.backend.Get([]byte("a")); err != nil || string(got) != "active" {
		t.Fatalf("drain retry: %q %v", got, err)
	}
}

func TestCOWDrainReturnsAcceptedHandoffRefusalAndRetriesWithoutReapply(t *testing.T) {
	db := cowPointerFixture(t)
	native := db.backend.(*backenddb.DB)
	backend := &cowRefusingBasisBackend{DB: native}
	db.backend = backend
	if err := db.Set([]byte("a"), []byte("covered")); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	backend.refuse = true
	if err = db.Drain(); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("hidden handoff refusal: %v", err)
	}
	if db.cow.handoff == nil || !db.cow.handoff.accepted || len(db.cow.cut.frozen) == 0 {
		t.Fatal("accepted prefix lost")
	}
	seq := native.State().CommitSeq
	if got, err := native.Get([]byte("a")); err != nil || string(got) != "covered" {
		t.Fatalf("accepted backend: %q %v", got, err)
	}
	backend.refuse = false
	if err = db.Drain(); err != nil {
		t.Fatal(err)
	}
	if native.State().CommitSeq != seq || db.cow.handoff != nil || len(db.cow.cut.frozen) != 0 {
		t.Fatal("retry reapplied accepted prefix")
	}
	if got, err := old.Get([]byte("a")); err != nil || string(got) != "covered" {
		t.Fatalf("old captured pointer: %q %v", got, err)
	}
}
