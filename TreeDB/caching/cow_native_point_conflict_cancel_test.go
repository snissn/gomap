package caching

import (
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCOWNativeBSONSetConflictIncludesAcceptedCancelError(t *testing.T) {
	registerNativeBridgeTestCommand()
	cache, db := nativeBridgeFixture(t, true)
	original, err := db.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := original.Close(); err != nil {
			t.Error(err)
		}
	}()
	writer, err := db.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	newer, _ := nativeTxnPublication(t, writer, "row")
	newer.Entries = nil
	if _, err := writer.CommitNative(newer, nativeTxnIntent(t, db, true), false); err != nil {
		t.Fatal(err)
	}
	plan := &nativeTxnTestPlan{}
	update := &backenddb.NativePreparedPublication{
		Basis: original.State(), StartSequence: original.LogicalSequence(), Reprepare: plan,
		ObservePointConflict: 1, Dependencies: newer.Effects, Effects: newer.Effects,
	}
	batch := original.(*nativeConditionalTxn).batch
	staled := 0
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWPreparation && e.Root == cache.dir {
			if !batch.cowState.accepted.Valid() {
				return errors.New("expected original accepted reservation")
			}
			// Remove the reservation while deliberately leaving its stale copy
			// in the batch. The original cleanup must observe the failed cancel.
			if err := batch.cowState.accepted.CancelAccepted(); err != nil {
				return err
			}
			staled++
		}
		return nil
	})
	before := db.CommandWALNextLSN()
	result, err := original.CommitNative(update, nativeTxnIntent(t, db, true), false)
	restore()
	if staled != 1 || plan.calls != 0 || !result.OnlyPointConflict || result.Assigned || result.LogicalInstalled ||
		!errors.Is(err, backenddb.ErrConcurrentModification) || !errors.Is(err, rootpublication.ErrAcceptedReceipt) || db.CommandWALNextLSN() != before {
		t.Fatalf("stale cancellation outcome=%+v err=%v staled=%d plan calls=%d", result, err, staled, plan.calls)
	}
	if err := cache.Close(); !errors.Is(err, rootpublication.ErrAcceptedReceipt) {
		t.Fatalf("close after stale accepted reservation = %v", err)
	}
	if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("close retained cancellation debt: %+v", stats)
	}
}

func TestCOWNativeProjectedRefusalRequiresAcceptedCancel(t *testing.T) {
	registerNativeBridgeTestCommand()
	for _, name := range []string{"clean", "stale"} {
		t.Run(name, func(t *testing.T) {
			cache, db := nativeBridgeFixture(t, true)
			tx, err := db.BeginNativeConditionalTxn()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := tx.Close(); err != nil {
					t.Error(err)
				}
			}()
			// Reprepare runs inside final validation after the accepted-prefix
			// reservation. Model the collection's wrapper around a projection
			// refusal to test exact error identity at the return boundary.
			refusal := fmt.Errorf("native insert refusal: %w", &memtable.COWProjectedGenerationRefusal{})
			plan := &nativeTxnTestPlan{state: nativeBridgeState(t, tx), err: refusal}
			publication := &backenddb.NativePreparedPublication{
				Basis: tx.State(), StartSequence: tx.LogicalSequence(), Reprepare: plan,
			}
			b := tx.(*nativeConditionalTxn).batch
			staled := 0
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Point != durabilitycut.BeforeCOWPreparation || e.Root != cache.dir {
					return nil
				}
				if !b.cowState.accepted.Valid() {
					return errors.New("expected accepted reservation")
				}
				if name == "stale" {
					// Recycle the real ticket while preserving the batch's copy.
					if err := b.cowState.accepted.CancelAccepted(); err != nil {
						return err
					}
					staled++
				}
				return nil
			})
			defer restore()
			lsn, seq := db.CommandWALNextLSN(), cache.cow.prefix.LogicalFrontiers().V
			result, err := tx.CommitNative(publication, nativeTxnIntent(t, db, true), false)
			if plan.calls != 1 || result.OnlyPointConflict || result.Assigned || result.AssignedLSN != 0 || result.Uncertain ||
				result.LogicalInstalled || result.WALCompleted || result.WALStable || db.CommandWALNextLSN() != lsn ||
				cache.cow.prefix.LogicalFrontiers().V != seq || cache.cow.prefix.AcceptedPendingCount() != 0 {
				t.Fatalf("invalid preaccept outcome: result=%+v err=%v staled=%d plan calls=%d", result, err, staled, plan.calls)
			}
			if name == "stale" {
				if staled != 1 || err == refusal || !errors.Is(err, refusal) || !errors.Is(err, rootpublication.ErrAcceptedReceipt) {
					t.Errorf("failed cancellation must invalidate exact refusal identity: err=%v staled=%d", err, staled)
				}
				if err := cache.Close(); !errors.Is(err, rootpublication.ErrAcceptedReceipt) {
					t.Fatalf("close after stale accepted reservation = %v", err)
				}
			} else {
				if err != refusal {
					t.Errorf("clean cancellation lost exact refusal identity: err=%v", err)
				}
				if err := cache.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("close retained cancellation debt: %+v", stats)
			}
		})
	}
}
