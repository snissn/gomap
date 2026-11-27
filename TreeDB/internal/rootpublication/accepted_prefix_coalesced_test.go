package rootpublication

import (
	"context"
	"errors"
	"testing"
)

func TestAcceptedPrefixCoalescesRawButPreservesMarkedCutAndSparseCustody(t *testing.T) {
	for _, wal := range []bool{false, true} {
		t.Run(map[bool]string{false: "NoWAL", true: "WAL"}[wal], func(t *testing.T) {
			c, err := New(Options{StartupSynchronous: true, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
				return PublishResult{Outcome: PublishSucceeded}
			})})
			if err != nil {
				t.Fatal(err)
			}
			defer stopClean(t, c)
			defer c.CloseAccepted()
			if err = c.ConfigureAcceptedPrefix(8, LogicalFrontiers{}); err != nil {
				t.Fatal(err)
			}
			var seq, lsn uint64
			appendRaw := func(d AcceptedDescriptor) AcceptedTicket {
				t.Helper()
				ticket, e := c.ReserveAccepted(false)
				if e != nil {
					t.Fatal(e)
				}
				seq++
				if wal {
					lsn++
				}
				if e = ticket.RecordAssigned(lsn, nil, false); e != nil {
					t.Fatal(e)
				}
				d.Sequence = seq
				if e = ticket.InstallAccepted(d); e != nil {
					t.Fatal(e)
				}
				return ticket
			}
			first := appendRaw(AcceptedDescriptor{})
			for i := 0; i < 4096; i++ {
				appendRaw(AcceptedDescriptor{})
			}
			if c.accepted.count != 1 {
				t.Fatalf("raw commands retained %d slots", c.accepted.count)
			}
			through := SealedPrefix{LogicalSequence: seq, CanonicalLSN: lsn}
			if err = c.MarkAcceptedBoundary(through); err != nil {
				t.Fatal(err)
			}
			// New writes arrive BEFORE CaptureAcceptedCut, not just after it. One native
			// original stays separate between two coalesced raw runs.
			releases := 0
			custody := &prefixOriginalCustody{release: func() error { releases++; return nil }}
			appendRaw(AcceptedDescriptor{Custody: custody, Projections: []ParticipantProjection{{Domain: 1}}})
			for i := 0; i < 4097; i++ {
				appendRaw(AcceptedDescriptor{})
			}
			roots := LogicalRootSet{Raw: new(int)}
			cut, e := c.CaptureAcceptedCut(through, roots)
			if e != nil {
				t.Fatal(e)
			}
			visits := 0
			if e = cut.Range(func(d AcceptedDescriptor, r AcceptedReceipt) error {
				visits++
				if d.Sequence != through.LogicalSequence || r.CanonicalLSN != through.CanonicalLSN {
					t.Fatal("suffix changed frozen endpoint")
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			if visits != 1 || c.accepted.count != 3 {
				t.Fatalf("visits=%d slots=%d", visits, c.accepted.count)
			}
			builder, e := c.AcquireBuilder(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			accepted, e := c.EnqueueBuiltAcceptedCut(prefixPhysicalCandidate(t, 1, through.CanonicalLSN), builder, cut)
			if e != nil {
				t.Fatal(e)
			}
			if e = cut.CompleteHandoff(accepted.SealReceipt()); e != nil {
				t.Fatal(e)
			}
			if e = accepted.SealReceipt().Wait(context.Background()); e != nil {
				t.Fatal(e)
			}
			if e = c.WaitAccepted(context.Background(), through); e != nil {
				t.Fatal(e)
			}
			if releases != 0 || c.accepted.count != 2 {
				t.Fatalf("suffix prematurely retired: releases=%d slots=%d", releases, c.accepted.count)
			}
			if e = first.CancelAccepted(); !errors.Is(e, ErrAcceptedReceipt) {
				t.Fatalf("stale ticket: %v", e)
			}
			final := SealedPrefix{LogicalSequence: seq, CanonicalLSN: lsn}
			cut, e = c.CaptureAcceptedCut(final, roots)
			if e != nil {
				t.Fatal(e)
			}
			builder, e = c.AcquireBuilder(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			accepted, e = c.EnqueueBuiltAcceptedCut(prefixPhysicalCandidate(t, 2, lsn), builder, cut)
			if e != nil {
				t.Fatal(e)
			}
			if e = cut.CompleteHandoff(accepted.SealReceipt()); e != nil {
				t.Fatal(e)
			}
			if e = accepted.SealReceipt().Wait(context.Background()); e != nil {
				t.Fatal(e)
			}
			if e = c.WaitAccepted(context.Background(), final); e != nil {
				t.Fatal(e)
			}
			if releases != 1 || c.accepted.count != 0 {
				t.Fatalf("final releases=%d slots=%d", releases, c.accepted.count)
			}
		})
	}
}

func TestAcceptedPrefixCannotCaptureUnmarkedInteriorOfRawRun(t *testing.T) {
	c, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	defer c.CloseAccepted()
	if err = c.ConfigureAcceptedPrefix(2, LogicalFrontiers{}); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		ticket, e := c.ReserveAccepted(false)
		if e != nil {
			t.Fatal(e)
		}
		if e = ticket.RecordAssigned(i, nil, false); e != nil {
			t.Fatal(e)
		}
		if e = ticket.InstallAccepted(AcceptedDescriptor{Sequence: i}); e != nil {
			t.Fatal(e)
		}
	}
	if _, err = c.CaptureAcceptedCut(SealedPrefix{LogicalSequence: 2, CanonicalLSN: 2}, LogicalRootSet{}); !errors.Is(err, ErrAcceptedReceipt) {
		t.Fatal("invented interior coverage", err)
	}
	if err = c.MarkAcceptedBoundary(SealedPrefix{LogicalSequence: 2, CanonicalLSN: 2}); !errors.Is(err, ErrAcceptedReceipt) {
		t.Fatal("late mark accepted", err)
	}
}

func TestAcceptedRootCustodyNeedsCompletedUnstableSingletonAndNoSparseObligations(t *testing.T) {
	failure := errors.New("original uncertain append")
	for _, tc := range []struct {
		name        string
		first, last uint64
		stable      bool
		maintenance bool
		appendErr   error
		descriptor  AcceptedDescriptor
		returns     bool
	}{
		{name: "covered", first: 1, last: 1, descriptor: AcceptedDescriptor{Sequence: 1}, returns: true},
		{name: "stable", first: 1, last: 1, stable: true, descriptor: AcceptedDescriptor{Sequence: 1}},
		{name: "uncertain", first: 1, last: 1, appendErr: failure, descriptor: AcceptedDescriptor{Sequence: 1}},
		{name: "no-wal", descriptor: AcceptedDescriptor{Sequence: 1}},
		{name: "grouped", first: 1, last: 2, descriptor: AcceptedDescriptor{Sequence: 1}},
		{name: "projection", first: 1, last: 1, descriptor: AcceptedDescriptor{Sequence: 1, Projections: []ParticipantProjection{{Domain: 1}}}},
		{name: "exact-seal", first: 1, last: 1, descriptor: AcceptedDescriptor{Sequence: 1, SealTarget: new(prefixTestSealTarget)}},
		{name: "admitted-bytes", first: 1, last: 1, descriptor: AcceptedDescriptor{Sequence: 1, AdmittedBytes: 1}},
		{name: "cleanup-bytes", first: 1, last: 1, descriptor: AcceptedDescriptor{Sequence: 1, CleanupBytes: 1}},
		{name: "root-neutral", first: 1, last: 1, descriptor: AcceptedDescriptor{RootNeutral: true}},
		{name: "maintenance", first: 1, last: 1, maintenance: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New(Options{StartupSynchronous: true, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
				return PublishResult{Outcome: PublishSucceeded}
			})})
			if err != nil {
				t.Fatal(err)
			}
			defer stopClean(t, c)
			if err := c.ConfigureAcceptedPrefix(2, LogicalFrontiers{}); err != nil {
				t.Fatal(err)
			}
			ticket, err := c.ReserveAccepted(tc.maintenance)
			if err != nil {
				t.Fatal(err)
			}
			released := 0
			original := &prefixOriginalCustody{release: func() error { released++; return nil }}
			if err := ticket.BindPreassignedCustody(original); err != nil {
				t.Fatal(err)
			}
			if err := ticket.RecordAssignedRange(tc.first, tc.last, tc.appendErr, tc.stable); err != tc.appendErr {
				t.Fatalf("append: got %v, want %v", err, tc.appendErr)
			}
			d := tc.descriptor
			d.Custody = original
			install, err := ticket.BeginInstall(d)
			if err != tc.appendErr {
				t.Fatalf("install: got %v, want %v", err, tc.appendErr)
			}
			if err == nil {
				if returned := install.CommitReturningRootCustody(); returned != tc.returns {
					t.Fatalf("returned custody=%t, want %t", returned, tc.returns)
				}
			}
			c.accepted.mu.Lock()
			_, entry := ticket.entryLocked()
			stillOwned := entry != nil && entry.descriptor.Custody == original
			c.accepted.mu.Unlock()
			if stillOwned == tc.returns || released != 0 {
				t.Fatalf("original still owned=%t, released=%d, expected returned=%t", stillOwned, released, tc.returns)
			}
			if err := c.CloseAccepted(); err != tc.appendErr {
				t.Fatalf("close: got %v, want %v", err, tc.appendErr)
			}
			if tc.returns {
				if released != 0 {
					t.Fatal("queue released custody already returned to caller")
				}
				if err := original.ReleaseAccepted(); err != nil {
					t.Fatal(err)
				}
			}
			if released != 1 {
				t.Fatalf("original released %d times", released)
			}
		})
	}
}

type prefixTestSealTarget struct{}

func (*prefixTestSealTarget) BindAcceptedPhysicalSeal(PhysicalSealReceipt) {}

func TestAcceptedNoWALRootCustodyCoalescesAndGuardsSparseObligations(t *testing.T) {
	for _, tc := range []struct {
		name        string
		preassign   bool
		assignLSN   uint64
		maintenance bool
		descriptor  AcceptedDescriptor
		returns     bool
	}{
		{name: "nowal-root-covered", descriptor: AcceptedDescriptor{Sequence: 1}, returns: true},
		{name: "preassigned-wal-custody-refused", preassign: true, descriptor: AcceptedDescriptor{Sequence: 1}},
		{name: "assigned-lsn-refused", assignLSN: 1, descriptor: AcceptedDescriptor{Sequence: 1}},
		{name: "projection-refused", descriptor: AcceptedDescriptor{Sequence: 1, Projections: []ParticipantProjection{{Domain: 1}}}},
		{name: "exact-seal-refused", descriptor: AcceptedDescriptor{Sequence: 1, SealTarget: new(prefixTestSealTarget)}},
		{name: "admitted-bytes-refused", descriptor: AcceptedDescriptor{Sequence: 1, AdmittedBytes: 1}},
		{name: "cleanup-bytes-refused", descriptor: AcceptedDescriptor{Sequence: 1, CleanupBytes: 1}},
		{name: "root-neutral-refused", descriptor: AcceptedDescriptor{RootNeutral: true}},
		{name: "maintenance-refused", maintenance: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New(Options{StartupSynchronous: true, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
				return PublishResult{Outcome: PublishSucceeded}
			})})
			if err != nil {
				t.Fatal(err)
			}
			defer stopClean(t, c)
			if err := c.ConfigureAcceptedPrefix(4, LogicalFrontiers{}); err != nil {
				t.Fatal(err)
			}
			ticket, err := c.ReserveAccepted(tc.maintenance)
			if err != nil {
				t.Fatal(err)
			}
			released := 0
			original := &prefixOriginalCustody{release: func() error { released++; return nil }}
			if tc.preassign {
				if err := ticket.BindPreassignedCustody(original); err != nil {
					t.Fatal(err)
				}
			}
			if tc.assignLSN != 0 {
				if err := ticket.RecordAssigned(tc.assignLSN, nil, false); err != nil {
					t.Fatal(err)
				}
			}
			d := tc.descriptor
			d.Custody = original
			install, err := ticket.BeginInstall(d)
			if err != nil {
				t.Fatalf("BeginInstall: %v", err)
			}
			if returned := install.CommitReturningNoWALRootCustody(); returned != tc.returns {
				t.Fatalf("CommitReturningNoWALRootCustody=%t, want %t", returned, tc.returns)
			}
			c.accepted.mu.Lock()
			_, entry := ticket.entryLocked()
			stillOwned := entry != nil && entry.descriptor.Custody == original
			c.accepted.mu.Unlock()
			if stillOwned == tc.returns || released != 0 {
				t.Fatalf("original still owned=%t, released=%d, want returned=%t", stillOwned, released, tc.returns)
			}
			if tc.returns {
				// Subsequent NoWAL root-covered entries must coalesce into the same FIFO slot.
				for seq := uint64(2); seq <= 16; seq++ {
					nextTicket, err := c.ReserveAccepted(false)
					if err != nil {
						t.Fatalf("ReserveAccepted seq=%d: %v", seq, err)
					}
					nextCustody := &prefixOriginalCustody{release: func() error { return nil }}
					nextInstall, err := nextTicket.BeginInstall(AcceptedDescriptor{Sequence: seq, Custody: nextCustody})
					if err != nil {
						t.Fatalf("BeginInstall seq=%d: %v", seq, err)
					}
					if !nextInstall.CommitReturningNoWALRootCustody() {
						t.Fatalf("seq=%d did not return NoWAL custody", seq)
					}
				}
				if c.accepted.count != 1 {
					t.Fatalf("coalesced NoWAL root-covered entries retained %d slots, want 1", c.accepted.count)
				}
			}
			if err := c.CloseAccepted(); err != nil {
				t.Fatalf("CloseAccepted: %v", err)
			}
			if tc.returns {
				if released != 0 {
					t.Fatal("queue released custody already returned to caller")
				}
				if err := original.ReleaseAccepted(); err != nil {
					t.Fatal(err)
				}
			}
			if released != 1 {
				t.Fatalf("original released %d times, want 1", released)
			}
		})
	}
}
