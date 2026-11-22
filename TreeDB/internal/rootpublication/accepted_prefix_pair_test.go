package rootpublication

import (
	"errors"
	"testing"
)

func TestReserveAcceptedOrdinaryPairIsAllOrNothing(t *testing.T) {
	c := &Coordinator{}
	if err := c.ConfigureAcceptedPrefix(3, LogicalFrontiers{}); err != nil {
		t.Fatal(err)
	}
	defer c.CloseAccepted()
	reserve := func(maintenance bool) AcceptedTicket {
		t.Helper()
		ticket, err := c.ReserveAccepted(maintenance)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ticket.CancelAccepted() })
		return ticket
	}
	cancel := func(tickets ...AcceptedTicket) {
		t.Helper()
		for _, ticket := range tickets {
			if err := ticket.CancelAccepted(); err != nil {
				t.Fatal(err)
			}
		}
	}

	held1, held2 := reserve(false), reserve(false)
	a, z, err := c.ReserveAcceptedOrdinaryPair()
	if a.Valid() || z.Valid() || !errors.Is(err, ErrAcceptedCapacity) {
		t.Fatalf("partial pair on full refusal: %v/%v %v", a.Valid(), z.Valid(), err)
	}
	// A refused pair must not consume either the remaining ordinary entry
	// or the permanently separate maintenance entry.
	last := reserve(false)
	maintenance := reserve(true)
	cancel(held2, last)
	a, z, err = c.ReserveAcceptedOrdinaryPair()
	if err != nil || !a.Valid() || !z.Valid() || a.slot == z.slot || a.nonce == z.nonce {
		t.Fatalf("did not reserve two distinct original ordinary tickets: %v/%v %v", a.Valid(), z.Valid(), err)
	}
	if other, err := c.ReserveAccepted(false); other.Valid() || !errors.Is(err, ErrAcceptedCapacity) {
		t.Fatalf("pair exceeded configured capacity: %v %v", other.Valid(), err)
	}
	if other, err := c.ReserveAccepted(true); other.Valid() || !errors.Is(err, ErrAcceptedCapacity) {
		t.Fatalf("pair changed the separate occupied maintenance slot: %v %v", other.Valid(), err)
	}
	cancel(a, z, held1, maintenance)
	again, again2, err := c.ReserveAcceptedOrdinaryPair()
	if err != nil {
		t.Fatal(err)
	}
	cancel(again, again2)
}
