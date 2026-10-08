package rootpublication

import (
	"errors"
	"testing"
)

func TestTerminalDeleteReservationDrainsExactPinsAndRejectsStaleLease(t *testing.T) {
	finiteMetadataTestPlatform(t)
	registry := NewIdentityPinRegistry()
	identity := testStableIdentity(3)
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	pin, err := registry.Pin(identity)
	if err != nil {
		t.Fatal(err)
	}
	account := &testStableMetadataAccount{}
	lease, err := registry.PrepareTerminalDeleteAt(identity, "segments/exact", []*IdentityPin{pin}, account)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.Pin(identity); !errors.Is(err, ErrResourceDeletionInProgress) {
		t.Fatalf("foreign pin during reservation: %v", err)
	}
	if err = lease.CheckDrained(); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("undrained lease: %v", err)
	}
	pin.Release()
	if err = lease.CheckDrained(); err != nil {
		t.Fatal(err)
	}
	lease.Abort()
	if err = lease.CheckDrained(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("aborted lease reused: %v", err)
	}
	next, err := registry.PrepareTerminalDeleteAt(identity, "segments/exact", nil, account)
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.CheckDrained(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("stale lease accepted new generation: %v", err)
	}
	next.CommitDeleted()
	if _, err = registry.Pin(identity); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("pin after retirement: %v", err)
	}
	if account.retained() != 0 {
		t.Fatalf("lease account retained: %+v", account.retained())
	}
}

func TestTerminalDeleteReservationRefusesForeignDuplicateAndReleasedPinsBeforeDebit(t *testing.T) {
	finiteMetadataTestPlatform(t)
	registry := NewIdentityPinRegistry()
	identity := testStableIdentity(4)
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	own, err := registry.Pin(identity)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := registry.Pin(identity)
	if err != nil {
		t.Fatal(err)
	}
	account := &testStableMetadataAccount{}
	beforeRefs, beforeBytes := account.retained(), account.bytes
	for _, pins := range [][]*IdentityPin{{own}, {own, own}, {nil, foreign}} {
		if lease, err := registry.PrepareTerminalDeleteAt(identity, "segments/exact", pins, account); lease != nil || !errors.Is(err, ErrResourcePinned) {
			t.Fatalf("incomplete/duplicate group reservation: %v %v", lease, err)
		}
		if account.retained() != beforeRefs || account.bytes != beforeBytes {
			t.Fatalf("refusal changed debit/retention: bytes=%d refs=%d", account.bytes, account.retained())
		}
		if registry.PinCount(identity) != 2 {
			t.Fatal("refusal changed real pin membership")
		}
	}
	foreign.Release()
	if _, err = registry.PrepareTerminalDeleteAt(identity, "segments/exact", []*IdentityPin{own, foreign}, account); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("released control accepted: %v", err)
	}
	lease, err := registry.PrepareTerminalDeleteAt(identity, "segments/exact", []*IdentityPin{own}, account)
	if err != nil {
		t.Fatal(err)
	}
	lease.Abort()
	reopened, err := registry.Pin(identity)
	if err != nil {
		t.Fatal(err)
	}
	reopened.Release()
	own.Release()
	if account.retained() != 0 {
		t.Fatal("aborted reservation leaked account")
	}
}

func TestTerminalDeleteReservationRefusalLeavesOrdinaryGateUsable(t *testing.T) {
	finiteMetadataTestPlatform(t)
	registry := NewIdentityPinRegistry()
	identity := testStableIdentity(5)
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	pin, err := registry.Pin(identity)
	if err != nil {
		t.Fatal(err)
	}
	account := &testStableMetadataAccount{deny: true}
	if lease, err := registry.PrepareTerminalDeleteAt(identity, "segments/exact", []*IdentityPin{pin}, account); lease != nil || err == nil {
		t.Fatalf("underfunded lease: %v %v", lease, err)
	}
	second, err := registry.Pin(identity)
	if err != nil {
		t.Fatalf("refusal reserved deletion gate: %v", err)
	}
	second.Release()
	pin.Release()
	ordinary, err := registry.BeginDeleteAt(identity, "segments/exact")
	if err != nil {
		t.Fatal(err)
	}
	if err = ordinary.CheckDrained(); err != nil {
		t.Fatal(err)
	}
	ordinary.Abort()
}
