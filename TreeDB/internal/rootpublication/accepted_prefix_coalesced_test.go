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
