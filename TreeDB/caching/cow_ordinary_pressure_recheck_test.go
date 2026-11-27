package caching

import (
	"errors"
	"testing"

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
	for _, mode := range []string{"sticky-basis", "terminal-prefix", "closing", "disabled-size-worker"} {
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
			if err := cache.Checkpoint(); err != nil {
				t.Fatal(err)
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
			}
		})
	}
}
