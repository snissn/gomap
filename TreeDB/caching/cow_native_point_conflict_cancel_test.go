package caching

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
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
