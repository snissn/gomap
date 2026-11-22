package rootpublication

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestAcceptedOrdinaryPairRecheckNeedsOriginalRetirementAndAtomicSlots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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
	first, second := hold(), hold()
	install(first, 1)
	if err := c.MarkAcceptedBoundary(SealedPrefix{LogicalSequence: 1, CanonicalLSN: 1}); err != nil {
		t.Fatal(err)
	}
	install(second, 2)
	private := hold()
	// Use real captured-root and candidate seal/retirement APIs, not a
	// manufactured frontier. Leave the later raw suffix outside this cut.
	roots := &struct{}{}
	through := SealedPrefix{LogicalSequence: 2, CanonicalLSN: 2}
	cut, err := c.CaptureAcceptedCut(through, LogicalRootSet{Raw: roots, Semantic: roots})
	if err != nil {
		t.Fatal(err)
	}
	deny := func() AcceptedOrdinaryPairRefusal {
		t.Helper()
		a, b, refusal, err := c.ReserveAcceptedOrdinaryPairWithRefusal()
		if a.Valid() || b.Valid() || !refusal.Valid() || err != ErrAcceptedCapacity ||
			refusal.state.owner != c || refusal.state.denied.V != 2 || refusal.state.denied.C != 2 || refusal.state.denied.S != (SealedPrefix{}) {
			t.Fatalf("wrong atomic denied sample: %v/%v %+v (%v)", a.Valid(), b.Valid(), refusal, err)
		}
		return refusal
	}
	early, noWAL, rival1, rival2, terminal := deny(), deny(), deny(), deny(), deny()
	miss := func(refusal AcceptedOrdinaryPairRefusal) {
		t.Helper()
		a, b, err := c.ReserveAcceptedOrdinaryPairAfterRetirement(refusal)
		if a.Valid() || b.Valid() || err != nil {
			t.Fatalf("want clean no-pair result, got %v/%v %v", a.Valid(), b.Valid(), err)
		}
	}
	if err := private.CancelAccepted(); err != nil {
		t.Fatal(err)
	}
	miss(early) // no physical S, even with this real cancellation refund
	suffix := hold()
	install(suffix, 3)
	builder, err := c.AcquireBuilder(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := c.EnqueueBuiltAcceptedCut(prefixPhysicalCandidate(t, 1, 2), builder, cut)
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
	if f := c.LogicalFrontiers(); f.V != 3 || f.C != 3 || f.W != 0 || f.S != through || c.AcceptedPendingCount() != 1 {
		t.Fatalf("bad fixture: physical S=%+v pending=%d", f, c.AcceptedPendingCount())
	}
	miss(noWAL) // physical seal/retirement cannot invent stable canonical W
	c.ObserveStableWAL(2)
	miss(early) // still a one-shot disposition even though there is now proof

	foreign := &Coordinator{}
	if err := foreign.ConfigureAcceptedPrefix(3, c.LogicalFrontiers()); err != nil {
		t.Fatal(err)
	}
	defer foreign.CloseAccepted()
	if a, b, err := foreign.ReserveAcceptedOrdinaryPairAfterRetirement(rival1); a.Valid() || b.Valid() || err != nil {
		t.Fatalf("foreign logical owner reused matching frontiers: %v/%v %v", a.Valid(), b.Valid(), err)
	}
	type result struct {
		witness AcceptedOrdinaryPairRefusal
		a, b    AcceptedTicket
		err     error
	}
	start, results := make(chan struct{}), make(chan result, 2)
	for _, refused := range []AcceptedOrdinaryPairRefusal{rival1, rival2} {
		go func(refused AcceptedOrdinaryPairRefusal) {
			<-start
			a, b, err := c.ReserveAcceptedOrdinaryPairAfterRetirement(refused)
			results <- result{refused, a, b, err}
		}(refused)
	}
	close(start)
	x, y := <-results, <-results
	if !x.a.Valid() {
		x, y = y, x
	}
	if x.err != nil || y.err != nil || !x.a.Valid() || !x.b.Valid() || y.a.Valid() || y.b.Valid() ||
		x.a.slot == x.b.slot || x.a.nonce == x.b.nonce {
		t.Fatalf("rival got more or less than one exact pair: %+v / %+v", x, y)
	}
	for _, original := range []AcceptedTicket{first, second} {
		if err := original.CancelAccepted(); !errors.Is(err, ErrAcceptedReceipt) {
			t.Fatalf("stale ticket reached recycled ordinary slot: %v", err)
		}
	}
	if err := errors.Join(x.a.CancelAccepted(), x.b.CancelAccepted()); err != nil {
		t.Fatalf("rival tickets were tampered with: %v", err)
	}
	copiedWitness := x.witness
	miss(copiedWitness) // copying the handle cannot fork a one-shot denial
	miss(y.witness)
	want := fmt.Errorf("original terminal error: %w", ErrAcceptedCapacity)
	c.PoisonAccepted(want)
	if a, b, denied, err := c.ReserveAcceptedOrdinaryPairWithRefusal(); a.Valid() || b.Valid() || denied.Valid() || err != want {
		t.Fatalf("terminal state minted an initial witness: %v/%v %v (%v)", a.Valid(), b.Valid(), denied.Valid(), err)
	}
	if a, b, err := c.ReserveAcceptedOrdinaryPairAfterRetirement(terminal); a.Valid() || b.Valid() || err != want {
		t.Fatalf("terminal capacity-like error masqueraded as retry miss: %v/%v %v", a.Valid(), b.Valid(), err)
	}
}

func TestAcceptedOrdinaryPairCancellationIsNotRetirement(t *testing.T) {
	c := &Coordinator{}
	initial := LogicalFrontiers{V: 2, C: 2, W: 2, S: SealedPrefix{LogicalSequence: 2, CanonicalLSN: 2}}
	if err := c.ConfigureAcceptedPrefix(2, initial); err != nil {
		t.Fatal(err)
	}
	defer c.CloseAccepted()
	a, b, err := c.ReserveAcceptedOrdinaryPair()
	if err != nil {
		t.Fatal(err)
	}
	p, s, denied, err := c.ReserveAcceptedOrdinaryPairWithRefusal()
	if err != ErrAcceptedCapacity || p.Valid() || s.Valid() || !denied.Valid() {
		t.Fatalf("expected a real no-debt denial, got %v/%v %v (%v)", p.Valid(), s.Valid(), denied.Valid(), err)
	}
	if err := errors.Join(a.CancelAccepted(), b.CancelAccepted()); err != nil {
		t.Fatal(err)
	}
	if got := c.LogicalFrontiers(); got != initial {
		t.Fatalf("cancellation changed physical proof: %+v", got)
	}
	if p, s, err := c.ReserveAcceptedOrdinaryPairAfterRetirement(denied); p.Valid() || s.Valid() || err != nil {
		t.Fatalf("cancellation alone admitted a pair: %v/%v (%v)", p.Valid(), s.Valid(), err)
	}
}

func TestAcceptedOrdinaryPairDoesNotConfuseRootNeutralSuffixWithDeniedSeal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := New(Options{StartupSynchronous: true, Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer stopClean(t, c)
	if err := c.ConfigureAcceptedPrefix(3, LogicalFrontiers{}); err != nil {
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
	install := func(lsn uint64, neutral bool) {
		t.Helper()
		ticket := hold()
		if err := ticket.RecordAssigned(lsn, nil, false); err != nil {
			t.Fatal(err)
		}
		if err := ticket.InstallAccepted(AcceptedDescriptor{Sequence: 1, RootNeutral: neutral}); err != nil {
			t.Fatal(err)
		}
	}
	install(1, false)
	root := &struct{}{}
	through := SealedPrefix{LogicalSequence: 1, CanonicalLSN: 1}
	cut, err := c.CaptureAcceptedCut(through, LogicalRootSet{Raw: root, Semantic: root})
	if err != nil {
		t.Fatal(err)
	}
	install(2, true) // New canonical endpoint but no new logical version.
	p, q, denied, err := c.ReserveAcceptedOrdinaryPairWithRefusal()
	if err != ErrAcceptedCapacity || p.Valid() || q.Valid() || !denied.Valid() ||
		denied.state.denied.V != 1 || denied.state.denied.C != 2 {
		t.Fatalf("capture must see exact canonical-only suffix: %v/%v %+v %v", p.Valid(), q.Valid(), denied.state, err)
	}
	private := hold()
	if err := private.CancelAccepted(); err != nil {
		t.Fatal(err)
	}
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
	c.ObserveStableWAL(2)
	f := c.LogicalFrontiers()
	if f.V != 1 || f.C != 2 || f.W != 2 || f.S != through || c.AcceptedPendingCount() != 1 {
		t.Fatalf("bad fixture: %+v pending=%d", f, c.AcceptedPendingCount())
	}
	if a, b, err := c.ReserveAcceptedOrdinaryPairAfterRetirement(denied); a.Valid() || b.Valid() || err != nil {
		t.Fatalf("covered logical version was mistaken for denied canonical endpoint: %v/%v %v", a.Valid(), b.Valid(), err)
	}
	// Slots really are available; the missed recheck was the proof, not capacity.
	a, b, err := c.ReserveAcceptedOrdinaryPair()
	if err != nil || !a.Valid() || !b.Valid() {
		t.Fatalf("missing pair after prior real retirement: %v/%v %v", a.Valid(), b.Valid(), err)
	}
	if err := errors.Join(a.CancelAccepted(), b.CancelAccepted()); err != nil {
		t.Fatal(err)
	}
}
