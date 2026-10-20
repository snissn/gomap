package rootpublication

import (
	"context"
	"errors"
	"testing"
)

func TestAcceptedPrefixActualContiguousGroupRange(t *testing.T) {
	newOwner := func(t *testing.T) *Coordinator {
		t.Helper()
		c, err := New(Options{StartupSynchronous: true, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishSucceeded}
		})})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.CloseAccepted(); stopClean(t, c) })
		if err := c.ConfigureAcceptedPrefix(4, LogicalFrontiers{V: 2, C: 7, W: 7}); err != nil {
			t.Fatal(err)
		}
		return c
	}
	t.Run("one-range-one-install-and-original-singleton", func(t *testing.T) {
		c := newOwner(t)
		ticket, err := c.ReserveAccepted(false)
		if err != nil {
			t.Fatal(err)
		}
		if err := ticket.RecordAssignedRange(8, 11, nil, true); err != nil {
			t.Fatal(err)
		}
		if got := c.LogicalFrontiers(); got.V != 2 || got.C != 7 {
			t.Fatalf("assignment installed a prefix: %+v", got)
		}
		if err := ticket.InstallAccepted(AcceptedDescriptor{Sequence: 3}); err != nil {
			t.Fatal(err)
		}
		if got := c.LogicalFrontiers(); got.V != 3 || got.C != 11 {
			t.Fatalf("group did not install exactly once: %+v", got)
		}
		// Original scalar assignment remains exactly the next singleton.
		next, err := c.ReserveAccepted(false)
		if err != nil {
			t.Fatal(err)
		}
		if err := next.RecordAssigned(12, nil, true); err != nil {
			t.Fatal(err)
		}
		if err := next.InstallAccepted(AcceptedDescriptor{Sequence: 4}); err != nil {
			t.Fatal(err)
		}
		if got := c.LogicalFrontiers(); got.V != 4 || got.C != 12 {
			t.Fatalf("singleton after group: %+v", got)
		}
	})
	for _, test := range []struct {
		name        string
		first, last uint64
	}{
		{"gap", 9, 11}, {"overlap", 7, 11}, {"reversed", 11, 8}, {"missing-first", 0, 11},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newOwner(t)
			ticket, err := c.ReserveAccepted(false)
			if err != nil {
				t.Fatal(err)
			}
			err = ticket.RecordAssignedRange(test.first, test.last, nil, true)
			if err == nil {
				err = ticket.InstallAccepted(AcceptedDescriptor{Sequence: 3})
			}
			if !errors.Is(err, ErrAcceptedReceipt) {
				t.Fatalf("invalid range accepted: %v", err)
			}
			if got := c.LogicalFrontiers(); got.V != 2 || got.C != 7 {
				t.Fatalf("invalid range changed original frontier: %+v", got)
			}
		})
	}
	t.Run("assigned-error-retains-prefix-without-install", func(t *testing.T) {
		c := newOwner(t)
		ticket, err := c.ReserveAccepted(false)
		if err != nil {
			t.Fatal(err)
		}
		injected := errors.New("actual assigned prefix failure")
		if err := ticket.RecordAssignedRange(8, 9, injected, false); !errors.Is(err, injected) {
			t.Fatal(err)
		}
		c.accepted.mu.Lock()
		_, entry := ticket.entryLocked()
		receipt, first := entry.receipt, entry.firstCanonicalLSN
		c.accepted.mu.Unlock()
		if first != 8 || receipt.CanonicalLSN != 9 || !receipt.Assigned || !receipt.Uncertain || receipt.Installed || receipt.WALCompleted || receipt.WALStable {
			t.Fatalf("actual failed range receipt=%+v first=%d", receipt, first)
		}
		if err := ticket.InstallAccepted(AcceptedDescriptor{Sequence: 3}); !errors.Is(err, injected) {
			t.Fatalf("poisoned prefix installed: %v", err)
		}
		if err := ticket.CancelAccepted(); !errors.Is(err, ErrAcceptedReceipt) {
			t.Fatalf("assigned prefix refunded: %v", err)
		}
		if got := c.LogicalFrontiers(); got.V != 2 || got.C != 7 {
			t.Fatalf("failed range changed cut: %+v", got)
		}
	})
	t.Run("stale-and-double-install", func(t *testing.T) {
		c := newOwner(t)
		stale, err := c.ReserveAccepted(false)
		if err != nil {
			t.Fatal(err)
		}
		if err := stale.CancelAccepted(); err != nil {
			t.Fatal(err)
		}
		live, err := c.ReserveAccepted(false)
		if err != nil {
			t.Fatal(err)
		}
		if err := stale.RecordAssignedRange(8, 11, nil, true); !errors.Is(err, ErrAcceptedReceipt) {
			t.Fatalf("stale assignment: %v", err)
		}
		if err := live.RecordAssignedRange(8, 11, nil, true); err != nil {
			t.Fatal(err)
		}
		if err := live.InstallAccepted(AcceptedDescriptor{Sequence: 3}); err != nil {
			t.Fatal(err)
		}
		if err := live.InstallAccepted(AcceptedDescriptor{Sequence: 4}); !errors.Is(err, ErrAcceptedReceipt) {
			t.Fatalf("second install accepted: %v", err)
		}
		if got := c.LogicalFrontiers(); got.V != 3 || got.C != 11 {
			t.Fatalf("double install moved frontier: %+v", got)
		}
	})
}
