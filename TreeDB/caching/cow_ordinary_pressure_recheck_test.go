package caching

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestOrdinaryHealthyAdmissionAllocations(t *testing.T) {
	cache, _ := cowAutomaticFallbackFixture(t, "cow_btree")
	b := cache.NewBatch()
	defer b.Close()
	if err := b.Set([]byte("admission"), []byte("witness")); err != nil {
		t.Fatal(err)
	}
	check := func() {
		if err := b.admitCOWAccepted(); err != nil {
			t.Fatalf("healthy ordinary admission returned %v", err)
		}
		if !b.cowState.accepted.Valid() {
			t.Fatal("healthy ordinary admission returned invalid ticket")
		}
		if err := b.cowState.accepted.CancelAccepted(); err != nil {
			t.Fatal(err)
		}
		b.cowState.accepted = rootpublication.AcceptedTicket{}
	}
	if got := testing.AllocsPerRun(1000, check); got != 0 {
		t.Fatalf("healthy ordinary admission allocated %.2f objects per run", got)
	}
}

func realOrdinaryPressureRefusal(t *testing.T, cache *DB) rootpublication.AcceptedOrdinaryRefusal {
	t.Helper()
	prefix := cache.cow.prefix
	var hold []rootpublication.AcceptedTicket
	defer func() {
		for _, ticket := range hold {
			if err := ticket.CancelAccepted(); err != nil {
				t.Errorf("return fixture-only ticket: %v", err)
			}
		}
	}()
	for {
		ticket, refused, err := prefix.ReserveAcceptedOrdinaryWithRefusal()
		if refused.Valid() && err == rootpublication.ErrAcceptedCapacity {
			if ticket.Valid() || len(hold) == 0 {
				t.Fatalf("invalid ordinary refusal: valid=%t held=%d", ticket.Valid(), len(hold))
			}
			return refused
		}
		if err != nil || !ticket.Valid() || len(hold) > 8192 {
			t.Fatalf("failed to reach controlled ordinary capacity with original debt: %v", err)
		}
		hold = append(hold, ticket)
	}
}

func holdAllOrdinarySlots(t *testing.T, prefix *rootpublication.Coordinator) []rootpublication.AcceptedTicket {
	t.Helper()
	var hold []rootpublication.AcceptedTicket
	for {
		ticket, err := prefix.ReserveAccepted(false)
		if errors.Is(err, rootpublication.ErrAcceptedCapacity) {
			return hold
		}
		if err != nil || !ticket.Valid() || len(hold) > 8192 {
			for _, h := range hold {
				_ = h.CancelAccepted()
			}
			t.Fatalf("failed to fill ordinary slots: %v (held=%d)", err, len(hold))
		}
		hold = append(hold, ticket)
	}
}

func releaseOrdinarySlots(t *testing.T, hold []rootpublication.AcceptedTicket) {
	t.Helper()
	for _, ticket := range hold {
		if err := ticket.CancelAccepted(); err != nil {
			t.Errorf("cancel held ordinary ticket: %v", err)
		}
	}
}

func TestOrdinaryRelaxedPressureRecheckReusesPriorCheckpoint(t *testing.T) {
	t.Setenv(envDisableVlogGenerationLoop, "1")
	cache, backend := cowAutomaticFallbackFixture(t, "cow_btree")
	cache.relaxedSync = true
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	cache.SetCommandWALCheckpointCleanupHook(backend.CleanupCommandWALCoveredSegmentsAtCheckpoint)
	cache.SetAutoCheckpointWALBytesHook(backend.CommandWALActiveBytes)
	cache.StartAutoCheckpoint(0, 2<<30, 0)
	if !cache.nativePressureDrainEnabled() {
		t.Fatal("fixture did not arm size-maintenance worker")
	}

	writeCOWAutoFallbackRaw(t, cache, backend, "initial")
	denied1 := realOrdinaryPressureRefusal(t, cache)
	denied2 := realOrdinaryPressureRefusal(t, cache)

	// First waiter seals and retires the refused prefix.
	before := cache.checkpointRuns.Load()
	ticket1, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, denied1, rootpublication.ErrAcceptedCapacity)
	if err != nil || !ticket1.Valid() {
		t.Fatalf("first ordinary pressure admission failed: valid=%t err=%v", ticket1.Valid(), err)
	}
	defer ticket1.CancelAccepted()
	if got := cache.checkpointRuns.Load() - before; got != 1 {
		t.Fatalf("first waiter checkpoints=%d, want 1", got)
	}

	// Second waiter refused against the same now-retired prefix reuses that
	// retirement under flushMu without triggering a redundant checkpoint.
	ticket2, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, denied2, rootpublication.ErrAcceptedCapacity)
	if err != nil || !ticket2.Valid() {
		t.Fatalf("second ordinary pressure admission failed: valid=%t err=%v", ticket2.Valid(), err)
	}
	defer ticket2.CancelAccepted()
	if got := cache.checkpointRuns.Load() - before; got != 1 {
		t.Fatalf("second waiter ran redundant checkpoint: total checkpoints=%d, want 1", got)
	}
}

func TestOrdinaryPressureRecheckRetainsOriginalGuards(t *testing.T) {
	modes := []string{
		"sticky-basis",
		"terminal-prefix",
		"closing",
		"disabled-size-worker",
		"maintenance-active",
		"parked-handoff",
		"durable-requires-stable-wal",
	}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(envDisableVlogGenerationLoop, "1")
			cache, backend := cowAutomaticFallbackFixture(t, "cow_btree")
			cache.relaxedSync = true
			if mode != "disabled-size-worker" {
				cache.SetCommandWALCheckpointCutoverHook(func() {})
				cache.SetCommandWALCheckpointCleanupHook(backend.CleanupCommandWALCoveredSegmentsAtCheckpoint)
				cache.SetAutoCheckpointWALBytesHook(backend.CommandWALActiveBytes)
				cache.StartAutoCheckpoint(0, 2<<30, 0)
				if !cache.nativePressureDrainEnabled() {
					t.Fatal("fixture did not arm size worker")
				}
			} else if cache.nativePressureDrainEnabled() {
				t.Fatal("fixture unexpectedly armed size worker")
			}
			writeCOWAutoFallbackRaw(t, cache, backend, "initial")
			denied := realOrdinaryPressureRefusal(t, cache)
			var relaxedDenied rootpublication.AcceptedOrdinaryRefusal
			if mode == "durable-requires-stable-wal" {
				relaxedDenied = realOrdinaryPressureRefusal(t, cache)
				// Pressure-only materialization seals and retires S without
				// rotating/syncing the WAL, leaving S >= (before.V, before.C)
				// while W remains 0 < before.C.
				if err := cache.materializeCOWPrefix(context.Background(), false, nil, cowSynchronousPressureMaterialization); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := cache.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			before := cache.checkpointRuns.Load()
			switch mode {
			case "sticky-basis":
				cache.cow.refreshRequired.Store(true)
				ticket, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, denied, rootpublication.ErrAcceptedCapacity)
				if err != nil || !ticket.Valid() {
					t.Fatalf("original full fallback: %v", err)
				}
				defer ticket.CancelAccepted()
				if got := cache.checkpointRuns.Load() - before; got != 1 || cache.cow.refreshRequired.Load() {
					t.Fatalf("available ordinary slot bypassed required refresh: checkpoints=%d sticky=%t", got, cache.cow.refreshRequired.Load())
				}
			case "terminal-prefix":
				want := errors.New("original ordinary accepted-prefix terminal error")
				cache.cow.prefix.PoisonAccepted(want)
				ticket, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, denied, rootpublication.ErrAcceptedCapacity)
				if ticket.Valid() || err != want {
					t.Fatalf("terminal refusal returned ticket=%t or wrong error: %v", ticket.Valid(), err)
				}
				if got := cache.checkpointRuns.Load() - before; got != 0 {
					t.Fatalf("terminal refusal entered a full checkpoint: %d", got)
				}
			case "closing":
				cache.closing.Store(true)
				defer cache.closing.Store(false)
				ticket, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, denied, rootpublication.ErrAcceptedCapacity)
				if ticket.Valid() || err != errDBClosing {
					t.Fatalf("closing refusal returned ticket=%t err=%v, want errDBClosing", ticket.Valid(), err)
				}
				if got := cache.checkpointRuns.Load() - before; got != 0 {
					t.Fatalf("closing refusal entered a full checkpoint: %d", got)
				}
			case "disabled-size-worker":
				ticket, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, denied, rootpublication.ErrAcceptedCapacity)
				if err != nil || !ticket.Valid() {
					t.Fatalf("disabled size-worker fallback failed: valid=%t err=%v", ticket.Valid(), err)
				}
				defer ticket.CancelAccepted()
				if got := cache.checkpointRuns.Load() - before; got != 1 {
					t.Fatalf("disabled size worker bypassed full checkpoint fallback: checkpoints=%d, want 1", got)
				}
			case "maintenance-active":
				cache.maintenanceActive.Store(true)
				defer cache.maintenanceActive.Store(false)
				ticket, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, denied, rootpublication.ErrAcceptedCapacity)
				if err != nil || !ticket.Valid() {
					t.Fatalf("maintenance-active fallback failed: valid=%t err=%v", ticket.Valid(), err)
				}
				defer ticket.CancelAccepted()
				if got := cache.checkpointRuns.Load() - before; got != 1 {
					t.Fatalf("maintenanceActive bypassed full checkpoint fallback: checkpoints=%d, want 1", got)
				}
			case "parked-handoff":
				writeCOWAutoFallbackRaw(t, cache, backend, "parked")
				wrapper := &rawTxnHandoffBackend{cowAcceptedReportBackend: &cowAcceptedReportBackend{DB: backend}}
				cache.backend = wrapper
				wrapper.inject.Store(true)
				if err := cache.Checkpoint(); !errors.Is(err, errCOWAcceptedReport) {
					t.Fatalf("expected parked accepted handoff error, got %v", err)
				}
				cache.flushMu.Lock()
				parked := cache.cow.handoff != nil && cache.cow.handoff.accepted
				cache.flushMu.Unlock()
				if !parked {
					t.Fatal("fixture did not park an accepted handoff")
				}
				wrapper.refuseCapture.Store(false)
				beforeHandoff := cache.checkpointRuns.Load()
				ticket, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, denied, rootpublication.ErrAcceptedCapacity)
				if err != nil || !ticket.Valid() {
					t.Fatalf("parked-handoff fallback failed: valid=%t err=%v", ticket.Valid(), err)
				}
				defer ticket.CancelAccepted()
				cache.flushMu.Lock()
				stillParked := cache.cow.handoff != nil
				cache.flushMu.Unlock()
				if got := cache.checkpointRuns.Load() - beforeHandoff; got != 1 || stillParked {
					t.Fatalf("ordinary pressure bypassed parked handoff: checkpoints=%d stillParked=%t", got, stillParked)
				}
			case "durable-requires-stable-wal":
				if f := cache.cow.prefix.LogicalFrontiers(); f.S.CanonicalLSN < 1 || f.W >= f.S.CanonicalLSN {
					t.Fatalf("expected retired S with unstably-synced W, got %+v", f)
				}
				// Relaxed caller (requireStableWAL=false) reuses S retirement with 0 checkpoints.
				relaxedTicket, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, relaxedDenied, rootpublication.ErrAcceptedCapacity)
				if err != nil || !relaxedTicket.Valid() {
					t.Fatalf("relaxed caller failed to reuse retired S when W < before.C: valid=%t err=%v", relaxedTicket.Valid(), err)
				}
				defer relaxedTicket.CancelAccepted()
				if got := cache.checkpointRuns.Load() - before; got != 0 {
					t.Fatalf("relaxed caller unexpectedly ran checkpoint when S retired: checkpoints=%d, want 0", got)
				}
				// Durable caller (externalCommandWAL && !relaxedSync -> requireStableWAL=true)
				// must miss the shortcut and run a full checkpoint when W < before.C.
				cache.relaxedSync = false
				ticket, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, denied, rootpublication.ErrAcceptedCapacity)
				if err != nil || !ticket.Valid() {
					t.Fatalf("durable stable-WAL fallback failed: valid=%t err=%v", ticket.Valid(), err)
				}
				defer ticket.CancelAccepted()
				if got := cache.checkpointRuns.Load() - before; got != 1 {
					t.Fatalf("durable caller bypassed full checkpoint when W < before.C: checkpoints=%d, want 1", got)
				}
			}
		})
	}
}

func TestAcceptedPressureStuckRingTerminatesWithErrCOWCapacity(t *testing.T) {
	t.Run("ordinary", func(t *testing.T) {
		t.Setenv(envDisableVlogGenerationLoop, "1")
		cache, _ := cowAutomaticFallbackFixture(t, "cow_btree")
		cache.relaxedSync = true
		hold := holdAllOrdinarySlots(t, cache.cow.prefix)
		defer releaseOrdinarySlots(t, hold)

		ticket, denied, err := cache.cow.prefix.ReserveAcceptedOrdinaryWithRefusal()
		if ticket.Valid() || !denied.Valid() || denied.HasPending() || err != rootpublication.ErrAcceptedCapacity {
			t.Fatalf("unexpected stuck ordinary denial: valid=%t denied=%t pending=%t err=%v", ticket.Valid(), denied.Valid(), denied.HasPending(), err)
		}
		before := cache.checkpointRuns.Load()
		got, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, denied, err)
		if got.Valid() || !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("stuck ordinary ring: valid=%t err=%v, want ErrCOWCapacity", got.Valid(), err)
		}
		if runs := cache.checkpointRuns.Load() - before; runs != 1 {
			t.Fatalf("stuck ordinary ring ran %d checkpoints, want 1 (bounded termination)", runs)
		}
	})

	t.Run("pair", func(t *testing.T) {
		t.Setenv(envDisableVlogGenerationLoop, "1")
		cache, _ := cowAutomaticFallbackFixture(t, "cow_btree")
		hold := holdAllOrdinarySlots(t, cache.cow.prefix)
		defer releaseOrdinarySlots(t, hold)

		a, b, denied, err := cache.cow.prefix.ReserveAcceptedOrdinaryPairWithRefusal()
		if a.Valid() || b.Valid() || !denied.Valid() || denied.HasPending() || err != rootpublication.ErrAcceptedCapacity {
			t.Fatalf("unexpected stuck pair denial: %t/%t denied=%t pending=%t err=%v", a.Valid(), b.Valid(), denied.Valid(), denied.HasPending(), err)
		}
		before := cache.checkpointRuns.Load()
		gotA, gotB, err := cache.reserveRawTxnPairAfterPressure(denied)
		if gotA.Valid() || gotB.Valid() || !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("stuck pair ring: %t/%t err=%v, want ErrCOWCapacity", gotA.Valid(), gotB.Valid(), err)
		}
		if runs := cache.checkpointRuns.Load() - before; runs != 1 {
			t.Fatalf("stuck pair ring ran %d checkpoints, want 1 (bounded termination)", runs)
		}
	})
}

func TestAcceptedPressureEmptyFrontierRefillRetriesAndSucceeds(t *testing.T) {
	t.Run("ordinary", func(t *testing.T) {
		t.Setenv(envDisableVlogGenerationLoop, "1")
		cache, backend := cowAutomaticFallbackFixture(t, "cow_btree")
		cache.relaxedSync = true
		cache.SetCommandWALCheckpointCutoverHook(func() {})
		writeCOWAutoFallbackRaw(t, cache, backend, "initial")
		denied := realOrdinaryPressureRefusal(t, cache)
		if err := cache.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		// Frontier is now empty (V == S, C == S). On the first checkpoint pass,
		// refill the ring with a real pending entry + held slots right after
		// rotateCOWFrontier captures the empty cut and opens post-frontier admission.
		var calls atomic.Int32
		var refillHold []rootpublication.AcceptedTicket
		defer func() { releaseOrdinarySlots(t, refillHold) }()
		cache.testAfterCheckpointFrontierCapture = func() {
			if calls.Add(1) == 1 {
				writeCOWAutoFallbackRaw(t, cache, backend, "refill")
				refillHold = holdAllOrdinarySlots(t, cache.cow.prefix)
			}
		}
		defer func() { cache.testAfterCheckpointFrontierCapture = nil }()

		before := cache.checkpointRuns.Load()
		ticket, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, denied, rootpublication.ErrAcceptedCapacity)
		if err != nil || !ticket.Valid() {
			t.Fatalf("empty-frontier refill ordinary retry failed: valid=%t err=%v", ticket.Valid(), err)
		}
		defer ticket.CancelAccepted()
		if runs := cache.checkpointRuns.Load() - before; runs != 2 {
			t.Fatalf("empty-frontier refill ordinary checkpoints=%d, want 2", runs)
		}
	})

	t.Run("pair", func(t *testing.T) {
		t.Setenv(envDisableVlogGenerationLoop, "1")
		cache, backend := cowAutomaticFallbackFixture(t, "cow_btree")
		cache.SetCommandWALCheckpointCutoverHook(func() {})
		writeCOWAutoFallbackRaw(t, cache, backend, "initial")
		denied := realRawTxnPressureRefusal(t, cache)
		if err := cache.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		var calls atomic.Int32
		var refillHold []rootpublication.AcceptedTicket
		defer func() { releaseOrdinarySlots(t, refillHold) }()
		cache.testAfterCheckpointFrontierCapture = func() {
			if calls.Add(1) == 1 {
				writeCOWAutoFallbackRaw(t, cache, backend, "refill-1")
				f := cache.cow.prefix.LogicalFrontiers()
				if err := cache.cow.prefix.MarkAcceptedBoundary(rootpublication.SealedPrefix{
					LogicalSequence: f.V,
					CanonicalLSN:    f.C,
				}); err != nil {
					t.Errorf("mark refill boundary: %v", err)
					return
				}
				writeCOWAutoFallbackRaw(t, cache, backend, "refill-2")
				refillHold = holdAllOrdinarySlots(t, cache.cow.prefix)
			}
		}
		defer func() { cache.testAfterCheckpointFrontierCapture = nil }()

		before := cache.checkpointRuns.Load()
		a, b, err := cache.reserveRawTxnPairAfterPressure(denied)
		if err != nil || !a.Valid() || !b.Valid() {
			t.Fatalf("empty-frontier refill pair retry failed: %t/%t err=%v", a.Valid(), b.Valid(), err)
		}
		defer a.CancelAccepted()
		defer b.CancelAccepted()
		if runs := cache.checkpointRuns.Load() - before; runs != 2 {
			t.Fatalf("empty-frontier refill pair checkpoints=%d, want 2", runs)
		}
	})
}

func TestAcceptedPressureConcurrentDrainTOCTOU(t *testing.T) {
	t.Run("ordinary", func(t *testing.T) {
		t.Setenv(envDisableVlogGenerationLoop, "1")
		cache, backend := cowAutomaticFallbackFixture(t, "cow_btree")
		cache.relaxedSync = true
		cache.SetCommandWALCheckpointCutoverHook(func() {})
		cache.SetCommandWALCheckpointCleanupHook(backend.CleanupCommandWALCoveredSegmentsAtCheckpoint)
		cache.SetAutoCheckpointWALBytesHook(backend.CommandWALActiveBytes)
		cache.StartAutoCheckpoint(0, 2<<30, 0)

		before := cache.cow.prefix.LogicalFrontiers()
		writeCOWAutoFallbackRaw(t, cache, backend, "pending")
		hold := holdAllOrdinarySlots(t, cache.cow.prefix)
		ticket, nextDenied, err := cache.cow.prefix.ReserveAcceptedOrdinaryWithRefusal()
		releaseOrdinarySlots(t, hold)
		if ticket.Valid() || !nextDenied.Valid() || !nextDenied.HasPending() || err != rootpublication.ErrAcceptedCapacity {
			t.Fatalf("expected pending ordinary denial: valid=%t denied=%t pending=%t err=%v", ticket.Valid(), nextDenied.Valid(), nextDenied.HasPending(), err)
		}

		// Simulate a concurrent checkpoint draining the ring to
		// AcceptedPendingCount() == 0 between Reserve...WithRefusal() and the
		// retry continuation check.
		if err := cache.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		after := cache.cow.prefix.LogicalFrontiers()
		if got := cache.cow.prefix.AcceptedPendingCount(); got != 0 {
			t.Fatalf("expected drained queue (pending=0), got %d", got)
		}
		if !cowAcceptedPressureShouldRetry(before, after, nextDenied.HasPending()) {
			t.Fatalf("concurrent drain caused spurious refusal: before=%+v after=%+v hasPending=%t", before, after, nextDenied.HasPending())
		}
		// Continuing the loop with nextDenied reuses the completed retirement
		// without running another checkpoint.
		beforeRuns := cache.checkpointRuns.Load()
		got, err := cache.reserveCOWAcceptedOrdinaryAfterPressure(cache.cow, nextDenied, rootpublication.ErrAcceptedCapacity)
		if err != nil || !got.Valid() {
			t.Fatalf("retry after concurrent drain failed: valid=%t err=%v", got.Valid(), err)
		}
		defer got.CancelAccepted()
		if runs := cache.checkpointRuns.Load() - beforeRuns; runs != 0 {
			t.Fatalf("reused retired ordinary refusal ran %d extra checkpoints, want 0", runs)
		}
	})

	t.Run("pair", func(t *testing.T) {
		t.Setenv(envDisableVlogGenerationLoop, "1")
		cache, backend := cowAutomaticFallbackFixture(t, "cow_btree")
		cache.SetCommandWALCheckpointCutoverHook(func() {})
		cache.SetCommandWALCheckpointCleanupHook(backend.CleanupCommandWALCoveredSegmentsAtCheckpoint)
		cache.SetAutoCheckpointWALBytesHook(backend.CommandWALActiveBytes)
		cache.StartAutoCheckpoint(0, 2<<30, 0)

		before := cache.cow.prefix.LogicalFrontiers()
		writeCOWAutoFallbackRaw(t, cache, backend, "pending")
		hold := holdAllOrdinarySlots(t, cache.cow.prefix)
		a, b, nextDenied, err := cache.cow.prefix.ReserveAcceptedOrdinaryPairWithRefusal()
		releaseOrdinarySlots(t, hold)
		if a.Valid() || b.Valid() || !nextDenied.Valid() || !nextDenied.HasPending() || err != rootpublication.ErrAcceptedCapacity {
			t.Fatalf("expected pending pair denial: %t/%t denied=%t pending=%t err=%v", a.Valid(), b.Valid(), nextDenied.Valid(), nextDenied.HasPending(), err)
		}

		if err := cache.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		after := cache.cow.prefix.LogicalFrontiers()
		if got := cache.cow.prefix.AcceptedPendingCount(); got != 0 {
			t.Fatalf("expected drained queue (pending=0), got %d", got)
		}
		if !cowAcceptedPressureShouldRetry(before, after, nextDenied.HasPending()) {
			t.Fatalf("concurrent drain caused spurious pair refusal: before=%+v after=%+v hasPending=%t", before, after, nextDenied.HasPending())
		}
		beforeRuns := cache.checkpointRuns.Load()
		gotA, gotB, err := cache.reserveRawTxnPairAfterPressure(nextDenied)
		if err != nil || !gotA.Valid() || !gotB.Valid() {
			t.Fatalf("pair retry after concurrent drain failed: %t/%t err=%v", gotA.Valid(), gotB.Valid(), err)
		}
		defer gotA.CancelAccepted()
		defer gotB.CancelAccepted()
		if runs := cache.checkpointRuns.Load() - beforeRuns; runs != 0 {
			t.Fatalf("reused retired pair refusal ran %d extra checkpoints, want 0", runs)
		}
	})
}
