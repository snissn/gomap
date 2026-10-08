package rootpublication

import (
	"errors"
	"testing"
	"unsafe"
)

func TestStableTerminalReservedGateRetryUsesImmutableGenerationAndNoLateDebit(t *testing.T) {
	finiteMetadataTestPlatform(t)
	registry := NewIdentityPinRegistry()
	identity := testStableIdentity(30)
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	request, resident := &terminalScratchCredit{}, &terminalScratchCredit{}
	reservation, err := registry.NewStableTerminalDeleteReservation([]StableTerminalDeleteBinding{{Identity: identity, Namespace: "segments/reserved"}}, request, resident)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := registry.Pin(identity)
	if err != nil {
		t.Fatal(err)
	}
	beforeSource, beforeResident := request.bytes, resident.bytes
	request.deny = true
	resident.deny = true
	lease, err := reservation.PrepareTerminalDeleteAt(identity, "segments/reserved", []*IdentityPin{pin})
	if err != nil {
		t.Fatal("prepaid gate constructed a late credit", err)
	}
	stale := lease
	if err = reservation.Close(); !errors.Is(err, ErrResourceDeletionInProgress) {
		t.Fatal("active gate lost its backing", err)
	}
	if _, err = registry.Pin(identity); !errors.Is(err, ErrResourceDeletionInProgress) {
		t.Fatal("foreign pin crossed exact reservation", err)
	}
	pin.Release()
	if err = lease.CheckDrained(); err != nil {
		t.Fatal(err)
	}
	lease.Abort()
	next, err := reservation.PrepareTerminalDeleteAt(identity, "segments/reserved", nil)
	if err != nil {
		t.Fatal(err)
	}
	stale.Abort()
	if err = next.CheckDrained(); err != nil {
		t.Fatal("old copied generation aborted next real gate", err)
	}
	if err = stale.CheckDrained(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatal("old copied value acquired current authority", err)
	}
	if request.bytes != beforeSource || resident.bytes != beforeResident {
		t.Fatal("gate retry allocated credit after WAL")
	}
	next.CommitDeleted()
	if err = registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
	if err = reservation.Close(); err != nil {
		t.Fatal(err)
	}
	if resident.refs != 0 {
		t.Fatal("creator/registry loan survived actual terminal", resident.refs)
	}
	if err = stale.CheckDrained(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatal("closed binding remained usable", err)
	}
}

func TestStableTerminalReservedBackingChargesCurrentAndFutureRegistryBorrowers(t *testing.T) {
	finiteMetadataTestPlatform(t)
	registry := NewIdentityPinRegistry()
	identity := testStableIdentity(31)
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	originalHeld := true
	defer func() {
		if originalHeld {
			if err := registry.Unobserve(identity); err != nil {
				t.Error("original observation cleanup", err)
			}
		}
	}()
	existing := &terminalScratchCredit{}
	borrow, err := registry.acquireBorrower(existing)
	if err != nil {
		t.Fatal(err)
	}
	borrowHeld := true
	defer func() {
		if borrowHeld {
			borrow.release()
		}
	}()
	before := existing.bytes
	request, resident := &terminalScratchCredit{}, &terminalScratchCredit{}
	reservation, err := registry.NewStableTerminalDeleteReservation([]StableTerminalDeleteBinding{{Identity: identity, Namespace: "segments/retained-inactive"}}, request, resident)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reservation.Close(); err != nil {
			t.Error("reservation cleanup", err)
		}
	}()
	if existing.bytes-before < reservation.ClassBytes() {
		t.Fatal("current borrower omitted reserved claims")
	}
	registry.mu.Lock()
	census, err := registry.retainedBackingCensusLocked()
	registry.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	future := &terminalScratchCredit{}
	futureBorrow, err := registry.acquireBorrower(future)
	if err != nil {
		t.Fatal(err)
	}
	futureHeld := true
	defer func() {
		if futureHeld {
			futureBorrow.release()
		}
	}()
	if future.bytes < census.LiveClassBytes {
		t.Fatal("future borrower omitted inactive reservation/history")
	}

	type registrySnapshot struct {
		census                            BackingCensus
		stamp                             uint64
		states, namespaces                int
		reservations                      *StableTerminalDeleteReservation
		borrowers                         *stableRegistryBorrower
		original                          *identityPinState
		observers, pins, deleteGeneration uint64
		deleting, retired                 bool
	}
	snapshot := func() registrySnapshot {
		registry.mu.Lock()
		defer registry.mu.Unlock()
		census, err := registry.retainedBackingCensusLocked()
		if err != nil {
			t.Fatal(err)
		}
		state := registry.states.get(physicalStableIdentity(identity))
		if state == nil {
			t.Fatal("original physical protection disappeared")
		}
		return registrySnapshot{census: census, stamp: nextBackingIdentity.Load(), states: registry.states.count, namespaces: registry.namespaces.count, reservations: registry.terminalReservations, borrowers: registry.borrowers, original: state, observers: state.observers, pins: state.pins, deleteGeneration: registry.deleteGeneration, deleting: state.deleting, retired: state.retired}
	}
	bytes := func() [3]uint64 { return [3]uint64{existing.bytes, resident.bytes, future.bytes} }
	refs := func() [3]int { return [3]int{existing.refs, resident.refs, future.refs} }
	beforeRegistry, beforeBytes, beforeRefs := snapshot(), bytes(), refs()
	future.deny = true

	// Logical generations alias one physical object. Reobserve changes only
	// its scalar observer count and needs no additional retained backing.
	alias := testStableIdentity(32)
	if !SamePhysicalIdentity(identity, alias) {
		t.Fatal("generation alias is not the same physical identity")
	}
	aliasHeld := false
	defer func() {
		if aliasHeld {
			if err := registry.Unobserve(alias); err != nil {
				t.Error("alias observation cleanup", err)
			}
		}
	}()
	if err = registry.Observe(alias); err != nil {
		t.Fatal("scalar generation alias consulted denying growth credit", err)
	}
	aliasHeld = true
	afterAlias := snapshot()
	expectedAlias := beforeRegistry
	expectedAlias.observers++
	if afterAlias != expectedAlias || bytes() != beforeBytes || refs() != beforeRefs {
		t.Fatal("generation alias allocated or changed physical protection/credit")
	}
	if err = registry.Unobserve(alias); err != nil {
		t.Fatal(err)
	}
	aliasHeld = false
	if snapshot() != beforeRegistry {
		t.Fatal("alias unobserve did not restore exact scalar state")
	}

	foreign := testStableIdentity(32)
	foreign.ObjectID[0] = 9
	if SamePhysicalIdentity(identity, foreign) {
		t.Fatal("foreign growth fixture still aliases original physical object")
	}
	registry.mu.Lock()
	nodeBytes, planErr := registry.states.plannedInsertBytes(physicalStableIdentity(foreign))
	registry.mu.Unlock()
	if planErr != nil {
		t.Fatal(planErr)
	}
	stateBytes, err := StableBackingClassBytes(uint64(unsafe.Sizeof(identityPinState{})), true)
	if err != nil {
		t.Fatal(err)
	}
	plannedBytes, err := finiteStableAdd(stateBytes, nodeBytes)
	if err != nil {
		t.Fatal(err)
	}
	foreignHeld := false
	defer func() {
		if foreignHeld {
			if err := registry.Unobserve(foreign); err != nil {
				t.Error("foreign observation cleanup", err)
			}
		}
	}()
	err = registry.Observe(foreign)
	foreignHeld = err == nil
	if !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatal("foreign growth escaped live borrower predebit", err)
	}
	if registry.ObserverCount(foreign) != 0 || snapshot() != beforeRegistry || bytes() != beforeBytes || refs() != beforeRefs {
		t.Fatal("denied foreign birth changed census/stamp/state/namespace/reservation/credit")
	}

	// Retry the SAME distinct identity. The plan includes its owned platform
	// key as well as the actual state and AVL node classes, for every borrower.
	future.deny = false
	if err = registry.Observe(foreign); err != nil {
		t.Fatal("admitted foreign growth", err)
	}
	foreignHeld = true
	for i, after := range bytes() {
		if after-beforeBytes[i] != plannedBytes {
			t.Fatalf("borrower %d debit=%d want full planned state/node/key classes=%d", i, after-beforeBytes[i], plannedBytes)
		}
	}
	afterGrowth := snapshot()
	if registry.ObserverCount(foreign) != 1 || afterGrowth.states != beforeRegistry.states+1 || afterGrowth.census.LiveClassBytes-beforeRegistry.census.LiveClassBytes != plannedBytes || afterGrowth.census.AllocatedClassBytes-beforeRegistry.census.AllocatedClassBytes != plannedBytes || afterGrowth.namespaces != beforeRegistry.namespaces || afterGrowth.reservations != beforeRegistry.reservations || afterGrowth.borrowers != beforeRegistry.borrowers || afterGrowth.original != beforeRegistry.original || afterGrowth.observers != beforeRegistry.observers || afterGrowth.pins != beforeRegistry.pins || afterGrowth.deleteGeneration != beforeRegistry.deleteGeneration || refs() != beforeRefs {
		t.Fatal("admitted growth did not publish exactly one predebited physical state")
	}
	if err = registry.Unobserve(foreign); err != nil {
		t.Fatal(err)
	}
	foreignHeld = false
	futureBorrow.release()
	futureHeld = false
	borrow.release()
	borrowHeld = false
	if err = reservation.Close(); err != nil {
		t.Fatal(err)
	}
	if err = registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
	originalHeld = false
	if resident.refs != 0 || existing.refs != 0 || future.refs != 0 || registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		t.Fatal("registry loan/observation credit not balanced")
	}
}

func TestStableTerminalReservedWholeInputRefusesBeforeConstructorEffects(t *testing.T) {
	finiteMetadataTestPlatform(t)
	registry := NewIdentityPinRegistry()
	identity := testStableIdentity(33)
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	request, resident := &terminalScratchCredit{}, &terminalScratchCredit{}
	bad := []StableTerminalDeleteBinding{{Identity: identity, Namespace: "segments/good"}, {Identity: testStableIdentity(34), Namespace: "segments/missing"}}
	if reservation, err := registry.NewStableTerminalDeleteReservation(bad, request, resident); reservation != nil || err == nil {
		t.Fatal("unknown closure admitted", err)
	}
	if request.bytes != 0 || resident.bytes != 0 || resident.refs != 0 || registry.namespaces.count != 0 || registry.terminalReservations != nil {
		t.Fatal("later input refusal copied/claimed partial earlier input")
	}
}
