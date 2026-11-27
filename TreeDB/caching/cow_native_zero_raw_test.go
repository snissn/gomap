package caching

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
)

func TestCOWNativeOnlyZeroRawPrepareAndCancel(t *testing.T) {
	registerNativeBridgeTestCommand()
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancel), func(t *testing.T) {
			cache, db := nativeBridgeFixture(t, true)

			// BeginPreparedNativeConditionalTxn exercises deferRawDomains=true,
			// newCOWBatchDeferred(), and zero-raw PrepareExternalCommandWALPublication
			// -> prepareNativeOnly with embedded inlinePrep.
			tx, err := db.BeginPreparedNativeConditionalTxn()
			if err != nil {
				t.Fatal(err)
			}
			pub, plan := nativeTxnPublication(t, tx, "zero-raw-key")
			pub.Entries = nil // zero raw KV entries -> pure native-only path
			if cancel {
				plan.err = errors.New("synthetic native reprepare cancel")
			}

			res, err := tx.CommitNative(pub, nativeTxnIntent(t, db, true), false)
			if cancel {
				if err == nil || !errors.Is(err, plan.err) {
					t.Fatalf("expected cancel err %v, got res=%+v err=%v", plan.err, res, err)
				}
				if res.Assigned || res.LogicalInstalled {
					t.Fatalf("cancelled zero-raw txn installed: %+v", res)
				}
			} else {
				if err != nil || !res.LogicalInstalled {
					t.Fatalf("zero-raw commit failed: res=%+v err=%v", res, err)
				}
			}
			if err := tx.Close(); err != nil {
				t.Fatalf("tx.Close: %v", err)
			}
			if !cancel {
				if err := cache.Checkpoint(); err != nil {
					t.Fatalf("Checkpoint: %v", err)
				}
			}
			if err := cache.Close(); err != nil {
				t.Fatalf("cache.Close: %v", err)
			}
			stats := cache.COWMemoryStats()
			if stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("non-zero COW memory stats after close: %+v", stats)
			}
		})
	}
}

func TestCOWSemanticCleanupWakeAndJoinDrain(t *testing.T) {
	registerNativeBridgeTestCommand()
	cache, db := nativeBridgeFixture(t, true)

	const (
		writers = 4
		commits = 25
	)
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < commits; i++ {
				tx, err := db.BeginPreparedNativeConditionalTxn()
				if err != nil {
					errCh <- fmt.Errorf("w%d/%d begin: %w", w, i, err)
					return
				}
				key := fmt.Sprintf("k-%d-%d", w, i)
				pub, _ := nativeTxnPublication(t, tx, key)
				pub.Entries = nil
				pub.Effects = []txnhistory.Effect{{
					Namespace: txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: uint64(w + 1), Domain: 1, Interpretation: 1},
					Kind:      txnhistory.PointEffect,
					Key:       []byte(key),
				}}
				res, err := tx.CommitNative(pub, nativeTxnIntent(t, db, true), false)
				closeErr := tx.Close()
				if err != nil {
					if !errors.Is(err, backenddb.ErrConcurrentModification) {
						errCh <- fmt.Errorf("w%d/%d commit: %w", w, i, err)
						return
					}
					continue
				}
				if closeErr != nil {
					errCh <- fmt.Errorf("w%d/%d close: %w", w, i, closeErr)
					return
				}
				if !res.LogicalInstalled {
					errCh <- fmt.Errorf("w%d/%d not installed: %+v", w, i, res)
					return
				}
			}
		}()
	}

	// Concurrently exercise joinSemanticCleanupFrontier while writers are waking
	// and draining semantic cleanup records (verifying semanticWaiters Broadcast).
	var joinWG sync.WaitGroup
	joinErrCh := make(chan error, 4)
	for j := 0; j < 2; j++ {
		joinWG.Add(1)
		go func() {
			defer joinWG.Done()
			for i := 0; i < 10; i++ {
				if err := cache.joinSemanticCleanupFrontier(); err != nil {
					joinErrCh <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	joinWG.Wait()
	close(joinErrCh)
	for err := range joinErrCh {
		t.Fatal(err)
	}

	if err := cache.joinSemanticCleanupFrontier(); err != nil {
		t.Fatalf("final joinSemanticCleanupFrontier: %v", err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("cache.Close: %v", err)
	}
	stats := cache.COWMemoryStats()
	if stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("non-zero COW memory stats after close: %+v", stats)
	}
}

func TestCOWNativeActivationDrainsPreActivationIdleBatchScratch(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	budget := cache.cow.budget

	// Simulate pre-activation large raw writes (as in TestCOWNativePackedDictionaryLeaf)
	// that park idle raw-batch scratch inside budget.idleBatch on Close.
	b := cache.NewBatchWithSize(4096)
	val := bytes.Repeat([]byte("v"), 64)
	for i := 0; i < 4096; i++ {
		if err := b.Set([]byte(fmt.Sprintf("pre-%05d", i)), val); err != nil {
			t.Fatal(err)
		}
	}
	f := &cowPressureRolloverFixture{db: cache, backend: backend}
	if err := f.write(b, true, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Activating native semantic ownership must drain disposable pre-activation
	// idle raw-batch scratch so deferred native batches do not leave reclaimable
	// reservations sitting in COWBudget.
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		t.Fatalf("ActivateNativeSemanticOwner: %v", err)
	}
	tx, err := backend.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatalf("BeginNativeConditionalTxn: %v", err)
	}
	defer func() {
		if err := tx.Close(); err != nil {
			t.Errorf("tx.Close: %v", err)
		}
	}()

	before := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatalf("AcquireExternal(pressure): %v", err)
	}
	probe, probeErr := budget.AcquireExternal(128)
	pressure.Close()
	if probe != nil {
		probe.Close()
	}
	if !errors.Is(probeErr, memtable.ErrCOWCapacity) {
		t.Fatalf("expected ErrCOWCapacity with budget exhausted after activation, got lease=%v err=%v", probe, probeErr)
	}
}
