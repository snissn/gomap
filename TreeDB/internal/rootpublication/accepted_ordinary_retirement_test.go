package rootpublication

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestAcceptedOrdinaryRecheckNeedsOriginalRetirement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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
	defer c.CloseAccepted()

	hold := func() AcceptedTicket {
		t.Helper()
		ticket, err := c.ReserveAccepted(false)
		if err != nil {
			t.Fatal(err)
		}
		return ticket
	}
	install := func(ticket AcceptedTicket, n uint64) {
		t.Helper()
		if err := ticket.RecordAssigned(n, nil, false); err != nil {
			t.Fatal(err)
		}
		if err := ticket.InstallAccepted(AcceptedDescriptor{Sequence: n}); err != nil {
			t.Fatal(err)
		}
	}

	first := hold()
	install(first, 1)
	if err := c.MarkAcceptedBoundary(SealedPrefix{LogicalSequence: 1, CanonicalLSN: 1}); err != nil {
		t.Fatal(err)
	}
	second := hold()

	roots := &struct{}{}
	through := SealedPrefix{LogicalSequence: 1, CanonicalLSN: 1}
	cut, err := c.CaptureAcceptedCut(through, LogicalRootSet{Raw: roots, Semantic: roots})
	if err != nil {
		t.Fatal(err)
	}
	deny := func() AcceptedOrdinaryRefusal {
		t.Helper()
		ticket, refusal, err := c.ReserveAcceptedOrdinaryWithRefusal()
		if ticket.Valid() || !refusal.Valid() || err != ErrAcceptedCapacity ||
			refusal.state.owner != c || refusal.state.denied.V != 1 || refusal.state.denied.C != 1 || refusal.state.denied.S != (SealedPrefix{}) {
			t.Fatalf("wrong ordinary denied sample: %v %+v (%v)", ticket.Valid(), refusal, err)
		}
		return refusal
	}
	early, durableNoWAL, relaxedOK, rival1, rival2, terminal := deny(), deny(), deny(), deny(), deny(), deny()

	miss := func(refusal AcceptedOrdinaryRefusal, requireStableWAL bool) {
		t.Helper()
		ticket, err := c.ReserveAcceptedOrdinaryAfterRetirement(refusal, requireStableWAL)
		if ticket.Valid() || err != nil {
			t.Fatalf("want clean miss, got %v %v", ticket.Valid(), err)
		}
	}
	if err := second.CancelAccepted(); err != nil {
		t.Fatal(err)
	}
	miss(early, false) // no physical S yet, even after cancellation freed a slot

	builder, err := c.AcquireBuilder(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := c.EnqueueBuiltAcceptedCut(prefixPhysicalCandidate(t, 1, 1), builder, cut)
	if err != nil {
		t.Fatal(err)
	}
	if err := cut.CompleteHandoff(pub.SealReceipt()); err != nil {
		t.Fatal(err)
	}
	if err := pub.SealReceipt().Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitAccepted(ctx, through); err != nil {
		t.Fatal(err)
	}
	if f := c.LogicalFrontiers(); f.V != 1 || f.C != 1 || f.W != 0 || f.S != through || c.AcceptedPendingCount() != 0 {
		t.Fatalf("bad fixture: frontiers=%+v pending=%d", f, c.AcceptedPendingCount())
	}

	// Durable callers require W >= before.C; relaxed callers rely on S covering (before.V, before.C).
	miss(durableNoWAL, true)
	relaxedTicket, err := c.ReserveAcceptedOrdinaryAfterRetirement(relaxedOK, false)
	if err != nil || !relaxedTicket.Valid() {
		t.Fatalf("relaxed retirement recheck failed despite S covering denied prefix: %v %v", relaxedTicket.Valid(), err)
	}
	// One-shot witness cannot be reused via copy.
	miss(relaxedOK, false)
	if err := relaxedTicket.CancelAccepted(); err != nil {
		t.Fatal(err)
	}

	c.ObserveStableWAL(1)
	miss(early, false) // consumed earlier; stays one-shot

	foreign := &Coordinator{}
	if err := foreign.ConfigureAcceptedPrefix(2, c.LogicalFrontiers()); err != nil {
		t.Fatal(err)
	}
	defer foreign.CloseAccepted()
	if ticket, err := foreign.ReserveAcceptedOrdinaryAfterRetirement(rival1, true); ticket.Valid() || err != nil {
		t.Fatalf("foreign logical owner reused matching frontiers: %v %v", ticket.Valid(), err)
	}

	// Hold 1 of 2 ordinary slots so only one of the two rivals can succeed.
	occupied := hold()
	defer occupied.CancelAccepted()
	type result struct {
		witness AcceptedOrdinaryRefusal
		ticket  AcceptedTicket
		err     error
	}
	start, results := make(chan struct{}), make(chan result, 2)
	for _, refused := range []AcceptedOrdinaryRefusal{rival1, rival2} {
		go func(refused AcceptedOrdinaryRefusal) {
			<-start
			ticket, err := c.ReserveAcceptedOrdinaryAfterRetirement(refused, true)
			results <- result{refused, ticket, err}
		}(refused)
	}
	close(start)
	x, y := <-results, <-results
	if !x.ticket.Valid() {
		x, y = y, x
	}
	if x.err != nil || y.err != nil || !x.ticket.Valid() || y.ticket.Valid() {
		t.Fatalf("expected exactly one rival winner, got %+v / %+v", x, y)
	}
	if err := x.ticket.CancelAccepted(); err != nil {
		t.Fatal(err)
	}
	miss(x.witness, false)
	miss(y.witness, false)

	want := fmt.Errorf("original terminal error: %w", ErrAcceptedCapacity)
	c.PoisonAccepted(want)
	if ticket, denied, err := c.ReserveAcceptedOrdinaryWithRefusal(); ticket.Valid() || denied.Valid() || err != want {
		t.Fatalf("terminal state minted an initial witness: %v %v (%v)", ticket.Valid(), denied.Valid(), err)
	}
	if ticket, err := c.ReserveAcceptedOrdinaryAfterRetirement(terminal, false); ticket.Valid() || err != want {
		t.Fatalf("terminal capacity-like error masqueraded as retry miss: %v %v", ticket.Valid(), err)
	}
}

func TestAcceptedOrdinaryCancellationIsNotRetirement(t *testing.T) {
	c := &Coordinator{}
	initial := LogicalFrontiers{V: 2, C: 2, W: 2, S: SealedPrefix{LogicalSequence: 2, CanonicalLSN: 2}}
	if err := c.ConfigureAcceptedPrefix(1, initial); err != nil {
		t.Fatal(err)
	}
	defer c.CloseAccepted()
	held, err := c.ReserveAccepted(false)
	if err != nil {
		t.Fatal(err)
	}
	ticket, denied, err := c.ReserveAcceptedOrdinaryWithRefusal()
	if err != ErrAcceptedCapacity || ticket.Valid() || !denied.Valid() {
		t.Fatalf("expected a real no-debt denial, got %v %v (%v)", ticket.Valid(), denied.Valid(), err)
	}
	if err := held.CancelAccepted(); err != nil {
		t.Fatal(err)
	}
	if ticket, err := c.ReserveAcceptedOrdinaryAfterRetirement(denied, false); ticket.Valid() || err != nil {
		t.Fatalf("cancellation alone admitted an ordinary ticket: %v (%v)", ticket.Valid(), err)
	}
}
