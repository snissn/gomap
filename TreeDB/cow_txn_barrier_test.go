package treedb

import (
	"errors"
	"fmt"
	"testing"
)

func TestPublicCOWTxnCapacityBarrierReportsAcceptedFallbackAndAbort(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast, func(o *Options) {
		o.ValueLog.ForcePointers = false
		o.ValueLog.PointerThreshold = 1 << 20
	})
	tx := cowTxnOpen(t, db)
	if _, err := tx.Get([]byte("disjoint-read")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set([]byte("disjoint-write"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	b := db.NewBatchWithSize(4097)
	for n := 0; n < 4097; n++ {
		if err := b.Set([]byte(fmt.Sprintf("bulk/%04d", n)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if got := db.Stats()["treedb.cache.cow.txn.barrier_effect_capacity_total"]; got != "1" {
		t.Fatalf("accepted capacity barrier=%s", got)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("conservative conflict=%v", err)
	}
	stats := db.Stats()
	if stats["treedb.cache.cow.txn.barrier_conflicts_total"] != "1" || stats["treedb.cache.cow.txn.too_old_total"] != "0" {
		t.Fatalf("fallback abort classification: barrier=%s old=%s", stats["treedb.cache.cow.txn.barrier_conflicts_total"], stats["treedb.cache.cow.txn.too_old_total"])
	}
}
