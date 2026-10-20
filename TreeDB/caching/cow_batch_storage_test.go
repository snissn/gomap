package caching

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCOWBatchStorageAdmissionAndReset(t *testing.T) {
	db := cowPointerFixture(t)
	baseline := db.cow.budget.Stats()
	b := db.NewBatchWithSize(1)
	defer b.Close()
	lease := b.cowState.lease
	for i := 0; i < 65; i++ {
		if err := b.Set([]byte{byte(i)}, make([]byte, 1025)); err != nil {
			t.Fatal(err)
		}
	}
	if cap(b.entries) < 65 {
		t.Fatal("missing admitted growth")
	}
	opened := db.cow.budget.Stats()
	if opened.ExternalBytes <= baseline.ExternalBytes || opened.ExternalLeases != baseline.ExternalLeases+1 {
		t.Fatalf("backing growth did not share the batch admission: before=%+v after=%+v", baseline, opened)
	}
	b.Reset()
	if len(b.entries) != 0 || b.copyArena != nil {
		t.Fatal("reset retained batch storage")
	}
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	ownerBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchOwner{}))) + leaseBytes
	reset := db.cow.budget.Stats()
	if reset.ExternalBytes != baseline.ExternalBytes+ownerBytes || reset.ExternalLeases != baseline.ExternalLeases+1 || b.cowState.lease != lease {
		t.Fatalf("reset lost the original owner admission: before=%+v after=%+v", baseline, reset)
	}
	if err := b.Set([]byte("again"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b.Reset()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(b.Set([]byte("closed"), nil), ErrBatchClosed) {
		t.Fatal("closed batch accepted growth")
	}
	closed := db.cow.budget.Stats()
	if closed.ExternalBytes != baseline.ExternalBytes || closed.ExternalLeases != baseline.ExternalLeases {
		t.Fatalf("external charge leaked: before=%+v after=%+v", baseline, closed)
	}
}

func TestCOWBatchGuardReservationSurvivesReset(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		externalWAL, owner   bool
		wantGuardReservation bool
	}{
		{name: "no external WAL", owner: true},
		{name: "no transaction owner", externalWAL: true},
		{name: "reserved guard", externalWAL: true, owner: true, wantGuardReservation: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := nativeBridgeFixture(t, false)
			owner := db.cow.txnOwner
			db.externalCommandWAL = tc.externalWAL
			if !tc.owner {
				db.cow.txnOwner = nil
			}
			defer func() {
				db.externalCommandWAL = false
				db.cow.txnOwner = owner
			}()
			before := db.cow.budget.Stats()
			b := db.NewBatchWithSize(1)
			defer b.Close()
			if b == &cowDeniedBatch || b.cowState.guardReserved != tc.wantGuardReservation || b.cowState.guard != nil {
				t.Fatal("constructor did not capture the guard reservation before guard acquisition")
			}
			lease := b.cowState.lease
			ownerBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchOwner{})))
			ownerBytes += memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			if tc.wantGuardReservation {
				ownerBytes += memtable.COWAllocationCharge(backenddb.CommandWALStagingGuardAllocationBytesV1())
			}
			// Change both constructor inputs before growth and reuse. Neither the
			// mutable DB configuration nor the absent guard owns this reservation.
			db.externalCommandWAL = !tc.wantGuardReservation
			if tc.wantGuardReservation {
				db.cow.txnOwner = nil
			} else {
				db.cow.txnOwner = owner
			}
			for i := 0; i < 2; i++ {
				if err := b.Set([]byte("key"), make([]byte, 1025)); err != nil {
					t.Fatal(err)
				}
				grown := db.cow.budget.Stats()
				if grown.ExternalBytes != before.ExternalBytes+ownerBytes+b.cowState.storageBytes || grown.ExternalLeases != before.ExternalLeases+1 {
					t.Fatalf("growth changed the constructor reservation: before=%+v after=%+v", before, grown)
				}
				b.Reset()
				reset := db.cow.budget.Stats()
				if reset.ExternalBytes != before.ExternalBytes+ownerBytes || reset.ExternalLeases != before.ExternalLeases+1 || b.cowState.lease != lease || b.cowState.storageBytes != 0 {
					t.Fatalf("reset changed the constructor reservation: before=%+v after=%+v", before, reset)
				}
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			closed := db.cow.budget.Stats()
			if closed.ExternalBytes != before.ExternalBytes || closed.ExternalLeases != before.ExternalLeases {
				t.Fatalf("close leaked the constructor reservation: before=%+v after=%+v", before, closed)
			}
		})
	}
}

func TestCOWBatchGrowthRefusalKeepsExistingStorage(t *testing.T) {
	db := cowPointerFixture(t)
	b := db.newCOWBatch(1)
	defer b.Close()
	if err := b.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	before := db.cow.budget.Stats()
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	filler, err := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - leaseBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	full := db.cow.budget.Stats()
	capacity := cap(b.entries)
	b.Reserve(capacity + 1)
	if !errors.Is(b.cowState.err, memtable.ErrCOWCapacity) || cap(b.entries) != capacity || len(b.entries) != 1 || string(b.entries[0].Value) != "value" {
		t.Fatal("refused growth changed existing storage")
	}
	if after := db.cow.budget.Stats(); after.TotalBytes != full.TotalBytes || after.ReservedBytes != full.ReservedBytes || after.ExternalBytes != full.ExternalBytes || after.ExternalLeases != full.ExternalLeases {
		t.Fatalf("refused growth changed admission: before=%+v after=%+v", full, after)
	}
	b.Reset()
	if after := db.cow.budget.Stats(); after.ExternalBytes >= full.ExternalBytes || after.ExternalLeases != full.ExternalLeases {
		t.Fatalf("reset failed to refund admitted backing: before=%+v after=%+v", full, after)
	}
}

func TestCOWBatchResetAfterBudgetCloseReleasesStorage(t *testing.T) {
	db := cowPointerFixture(t)
	b := db.NewBatchWithSize(1)
	defer b.Close()
	if err := b.Set([]byte("key"), make([]byte, 1025)); err != nil {
		t.Fatal(err)
	}
	db.cow.budget.Close()
	before := db.cow.budget.Stats()
	backingBytes := b.cowState.storageBytes
	b.Reset()
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	ownerBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchOwner{}))) + leaseBytes
	if after := db.cow.budget.Stats(); after.ExternalBytes != before.ExternalBytes-backingBytes || after.ExternalLeases != before.ExternalLeases {
		t.Fatalf("closed budget prevented backing refund: before=%+v after=%+v", before, after)
	}
	if !errors.Is(b.Set([]byte("refused"), nil), memtable.ErrCOWClosed) {
		t.Fatal("closed budget admitted new backing")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if after := db.cow.budget.Stats(); after.ExternalBytes != before.ExternalBytes-backingBytes-ownerBytes || after.ExternalLeases != before.ExternalLeases-1 {
		t.Fatalf("close leaked admission: before=%+v after=%+v", before, after)
	}
}

func TestCOWBatchRefusalBeforeStagingAndCallback(t *testing.T) {
	db := cowPointerFixture(t)
	b := db.NewBatch()
	defer b.Close()
	if err := b.SetOps([]batch.Entry{{Type: batch.OpPut, Key: []byte("good"), Value: []byte("v")}, {Type: batch.OpDeleteRange, Key: []byte("a"), Value: []byte("z")}}); !errors.Is(err, ErrCOWUnsupported) {
		t.Fatal(err)
	}
	if len(b.entries) != 0 {
		t.Fatal("unsupported group partially staged")
	}
	b.Reserve(1 << 30)
	if !errors.Is(b.Set([]byte("key"), []byte("v")), memtable.ErrCOWCapacity) {
		t.Fatal("reserve refusal lost")
	}
	called := false
	if err := b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error { called = true; return nil }); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatal(err)
	}
	if called {
		t.Fatal("refused batch invoked append")
	}
	db.cow.budget.Close()
	denied := db.NewBatch()
	if denied != &cowDeniedBatch {
		t.Fatal("refused constructor allocated a wrapper")
	}
	denied.Reserve(8)
	denied.Reset()
	_ = denied.Close()
	if !errors.Is(denied.Set([]byte("x"), []byte("v")), memtable.ErrCOWCapacity) {
		t.Fatal("denied carrier changed")
	}
}

func TestCOWHugeFiniteInputsRefuseBeforeAllocation(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	if _, _, err := validateCOWOpenOptions(Options{DisableWAL: true, MemtableShards: maxInt}); err == nil {
		t.Fatal("overflowing shard input accepted")
	}
	db := cowPointerFixture(t)
	baseline := db.cow.budget.Stats().ExternalBytes
	for _, n := range []int{maxInt, maxInt / int(unsafe.Sizeof(batch.Entry{})), 1 << 30} {
		b := db.NewBatchWithSize(n)
		// The public hint normalizer may cap a constructor hint. An explicit
		// Reserve must still refuse the original finite input before make.
		b.Reserve(n)
		if !errors.Is(b.Set([]byte("x"), []byte("v")), memtable.ErrCOWCapacity) {
			t.Fatalf("Reserve(%d) accepted", n)
		}
		_ = b.Close()
	}
	if got := db.cow.budget.Stats().ExternalBytes; got != baseline {
		t.Fatalf("refusal retained charge: %d want %d", got, baseline)
	}
	b := db.NewBatch()
	defer b.Close()
	before := db.cow.budget.Stats()
	for _, bytes := range []uint64{^uint64(0), ^uint64(0) - b.cowState.storageBytes} {
		if !errors.Is(b.cowAdmitStorage(bytes), memtable.ErrCOWCapacity) {
			t.Fatal("cumulative backing charge wrapped")
		}
	}
	if after := db.cow.budget.Stats(); after.TotalBytes != before.TotalBytes || after.ReservedBytes != before.ReservedBytes || after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases {
		t.Fatalf("overflow changed admission: before=%+v after=%+v", before, after)
	}
}
