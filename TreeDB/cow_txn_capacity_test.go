package treedb

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestPublicCOWTxnRepeatedClosedScanReleasesTransientCapacity(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
	tx := cowTxnOpen(t, db)
	for n := 0; n < 2; n++ {
		end := []byte(fmt.Sprintf("p%04d", n))
		it, err := tx.Iterator([]byte("a"), end)
		if err != nil {
			t.Fatal(err)
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
	}
	before := db.cached.COWMemoryStats()
	for n := 2; n < 258; n++ {
		for _, open := range []func([]byte, []byte) (Iterator, error){tx.Iterator, tx.ReverseIterator} {
			it, err := open([]byte("a"), []byte(fmt.Sprintf("p%04d", n)))
			if err != nil {
				t.Fatalf("scan%d=%v", n, err)
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
		}
		after := db.cached.COWMemoryStats()
		if after.TotalBytes != before.TotalBytes || after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases || after.Views != before.Views {
			t.Fatalf("closed scan%d retained transient capacity: before=%+v after=%+v", n, before, after)
		}
	}
	if err := tx.Set([]byte("result"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestPublicCOWTxnBorrowedAdapterRefusalReleasesOriginalCustody(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	db, _, _ := cowPublicContractOpen(t, ProfileCommandWALDurable, func(o *Options) { o.COWMemtableLimits = limits })
	warm := cowTxnOpen(t, db)
	if err := warm.Close(); err != nil {
		t.Fatal(err)
	}
	before := db.cached.COWMemoryStats()
	for n := 0; n < 16; n++ {
		tx := cowTxnOpen(t, db)
		if _, err := tx.Get([]byte("dependency")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Set([]byte("key"), []byte("value")); err != nil {
			t.Fatal(err)
		}
		used := db.cached.COWMemoryStats().ReservedBytes
		leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
		filler, err := db.cached.AcquireCOWAllocation(limits.MaxInFlightBytes - used - leaseBytes)
		if err != nil {
			t.Fatal(err)
		}
		next := db.backend.CommandWALNextLSN()
		err = tx.Commit()
		filler.Close()
		if !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("adapter refusal%d=%v", n, err)
		}
		if db.backend.CommandWALNextLSN() != next {
			t.Fatal("adapter refusal appended")
		}
		after := db.cached.COWMemoryStats()
		if after.TotalBytes != before.TotalBytes || after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases || after.Views != before.Views {
			t.Fatalf("adapter refusal%d stranded custody: before=%+v after=%+v", n, before, after)
		}
	}
}

func TestPublicCOWTxnConcurrentAdapterRefusalsKeepSharedCarriersImmutable(t *testing.T) {
	const count = 8
	type refusedTxn struct {
		db     *DB
		tx     *ConditionalTxn
		filler *memtable.COWExternalLease
		next   uint64
		before memtable.COWStats
	}
	transactions := make([]refusedTxn, count)
	limits := memtable.DefaultCOWLimits()
	for n := range transactions {
		db, _, _ := cowPublicContractOpen(t, ProfileCommandWALDurable, func(o *Options) { o.COWMemtableLimits = limits })
		warm := cowTxnOpen(t, db)
		if err := warm.Close(); err != nil {
			t.Fatal(err)
		}
		before := db.cached.COWMemoryStats()
		tx := cowTxnOpen(t, db)
		t.Cleanup(func() { _ = tx.Close() })
		if err := tx.Set([]byte("key"), []byte("value")); err != nil {
			t.Fatal(err)
		}
		used := db.cached.COWMemoryStats().ReservedBytes
		leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
		filler, err := db.cached.AcquireCOWAllocation(limits.MaxInFlightBytes - used - leaseBytes)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(filler.Close)
		transactions[n] = refusedTxn{db: db, tx: tx, filler: filler, next: db.backend.CommandWALNextLSN(), before: before}
	}
	start := make(chan struct{})
	results := make([]error, count)
	var writers sync.WaitGroup
	for n := range transactions {
		writers.Add(1)
		go func(n int) {
			defer writers.Done()
			<-start
			results[n] = transactions[n].tx.Commit()
			// Refused cleanup is also harmless for both immutable carrier kinds.
			cowDeniedPublicBatch.closeCOWAdapter()
			cowClosedPublicBatch.closeCOWAdapter()
		}(n)
	}
	close(start)
	writers.Wait()
	for n, transaction := range transactions {
		transaction.filler.Close()
		if !errors.Is(results[n], memtable.ErrCOWCapacity) || transaction.db.backend.CommandWALNextLSN() != transaction.next {
			t.Fatalf("refused transaction%d: err=%v appended=%t", n, results[n], transaction.db.backend.CommandWALNextLSN() != transaction.next)
		}
		after := transaction.db.cached.COWMemoryStats()
		before := transaction.before
		if after.TotalBytes != before.TotalBytes || after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases || after.Views != before.Views {
			t.Fatalf("refused transaction%d leaked custody: before=%+v after=%+v", n, before, after)
		}
	}
	for _, carrier := range []*commandWALPublicBatch{&cowDeniedPublicBatch, &cowClosedPublicBatch} {
		want := commandWALPublicBatch{cowState: &commandWALPublicCOWState{batchError: carrier.cowState.batchError}}
		if !reflect.DeepEqual(*carrier, want) {
			t.Fatal("shared refusal carrier mutated")
		}
	}
}

func TestPublicCOWTxnNewMethodsPreserveClosedError(t *testing.T) {
	for _, tx := range []*ConditionalTxn{nil, new(ConditionalTxn)} {
		for _, open := range []func([]byte, []byte) (Iterator, error){tx.Iterator, tx.ReverseIterator} {
			if it, err := open(nil, nil); it != nil || !errors.Is(err, ErrConditionalTxnClosed) {
				t.Fatalf("closed iterator=%v %v", it, err)
			}
		}
		if err := tx.DeleteRange(nil, nil); !errors.Is(err, ErrConditionalTxnClosed) {
			t.Fatalf("closed range=%v", err)
		}
	}
}
