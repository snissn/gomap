package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestTxnJournalCapacityScaling(t *testing.T) {
	t.Run("default_limits_stay_at_4096", func(t *testing.T) {
		limits := memtable.DefaultCOWLimits()
		budget, err := memtable.NewCOWBudget(limits)
		if err != nil {
			t.Fatalf("NewCOWBudget: %v", err)
		}
		c := &cowCache{budget: budget}
		if got := c.txnJournalCapacity(); got != 4096 {
			t.Fatalf("DefaultCOWLimits txnJournalCapacity=%d want 4096", got)
		}
	})

	t.Run("standalone_limits_scale_to_65536", func(t *testing.T) {
		limits := memtable.DefaultCOWLimits()
		limits.MaxInFlightBytes = 512 << 20
		limits.MaxTotalBytes = 1 << 30
		limits.MaxRetiredBytes = 1 << 30
		budget, err := memtable.NewCOWBudget(limits)
		if err != nil {
			t.Fatalf("NewCOWBudget: %v", err)
		}
		c := &cowCache{budget: budget}
		got := c.txnJournalCapacity()
		if got != 65536 {
			t.Fatalf("512MiB txnJournalCapacity=%d want 65536", got)
		}
		if charge, maxCharge := txnJournalControlCharge(got), limits.MaxInFlightBytes/16; charge > maxCharge {
			t.Fatalf("txnJournalControlCharge(%d)=%d > MaxInFlightBytes/16=%d", got, charge, maxCharge)
		}
	})

	t.Run("small_budget_shrinks_within_bound", func(t *testing.T) {
		limits := memtable.DefaultCOWLimits()
		limits.MaxInFlightBytes = 1 << 20
		budget, err := memtable.NewCOWBudget(limits)
		if err != nil {
			t.Fatalf("NewCOWBudget: %v", err)
		}
		c := &cowCache{budget: budget}
		got := c.txnJournalCapacity()
		if got < 8 || got&(got-1) != 0 || got >= 4096 {
			t.Fatalf("1MiB txnJournalCapacity=%d want power-of-two in [8, 4096)", got)
		}
		if charge, maxCharge := txnJournalControlCharge(got), limits.MaxInFlightBytes/16; got > 8 && charge > maxCharge {
			t.Fatalf("txnJournalControlCharge(%d)=%d > MaxInFlightBytes/16=%d", got, charge, maxCharge)
		}
	})
}
