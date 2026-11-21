package rootpublication

import (
	"context"
	"testing"
	"time"
)

func TestAcceptedPressureWakeRequiresInstallationAndRearmsAfterRetirement(t *testing.T) {
	const capacity, trigger = 8, 6
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := New(Options{StartupSynchronous: true, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.ConfigureAcceptedPrefix(capacity, LogicalFrontiers{}); err != nil {
		t.Fatal(err)
	}
	defer c.CloseAccepted()
	wake := make(chan struct{}, 1)
	c.SetAcceptedPressureWake(wake)
	requireQuiet := func() {
		t.Helper()
		select {
		case <-wake:
			t.Fatal("unexpected pressure wake")
		default:
		}
	}
	reserve := func(maintenance bool) AcceptedTicket {
		t.Helper()
		ticket, err := c.ReserveAccepted(maintenance)
		if err != nil {
			t.Fatal(err)
		}
		return ticket
	}
	install := func(ticket AcceptedTicket, lsn uint64) {
		t.Helper()
		if err := ticket.RecordAssigned(lsn, nil, true); err != nil {
			t.Fatal(err)
		}
		custody := &prefixOriginalCustody{release: func() error { return nil }}
		if err := ticket.InstallAccepted(AcceptedDescriptor{Sequence: lsn, Custody: custody}); err != nil {
			t.Fatal(err)
		}
	}
	roots := LogicalRootSet{Raw: c, Semantic: c}
	for i := uint64(0); i < 2*trigger; i += trigger {
		for lsn := i + 1; lsn < i+trigger; lsn++ {
			install(reserve(false), lsn)
		}
		maintenance := reserve(true)
		requireQuiet()
		if c.NeedsAcceptedPressure() {
			t.Fatal("maintenance used ordinary headroom")
		}
		if err := maintenance.CancelAccepted(); err != nil {
			t.Fatal(err)
		}
		last := reserve(false)
		requireQuiet()
		end := i + trigger
		install(last, end)
		select {
		case <-wake:
		default:
			t.Fatal("installed custody at the threshold did not request pressure work")
		}
		if !c.NeedsAcceptedPressure() {
			t.Fatal("original latch missed actionable pressure")
		}
		through := SealedPrefix{LogicalSequence: end, CanonicalLSN: end}
		cut, err := c.CaptureAcceptedCut(through, roots)
		if err != nil {
			t.Fatal(err)
		}
		builder, err := c.AcquireBuilder(ctx)
		if err != nil {
			t.Fatal(err)
		}
		physical, err := c.EnqueueBuiltAcceptedCut(prefixPhysicalCandidate(t, end, end), builder, cut)
		if err != nil {
			t.Fatal(err)
		}
		if err := cut.CompleteHandoff(physical.SealReceipt()); err != nil {
			t.Fatal(err)
		}
		if err := c.WaitAccepted(ctx, through); err != nil {
			t.Fatal(err)
		}
		requireQuiet()
		if c.NeedsAcceptedPressure() {
			t.Fatal("original retirement did not clear the stale wake")
		}
	}
}
