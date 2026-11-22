package caching

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

type rawTxnHandoffOutcome struct {
	frontier      rootpublication.LogicalFrontiers
	applied, next uint64
}

// The canonical COW writer passes its exact op count to this batch factory.
// An empty final build group can publish a maintenance root without replaying
// the parked raw data. Keep both counts to identify which actually occurred.
type rawTxnHandoffBackend struct {
	*cowAcceptedReportBackend
	dataAccepted, emptyAccepted atomic.Int64
}

type rawTxnHandoffBatch struct {
	*cowAcceptedReportBatch
	owner *rawTxnHandoffBackend
	data  bool
}

func (b *rawTxnHandoffBackend) NewPhysicalBatch() batch.Interface {
	return b.NewPhysicalBatchWithSize(0)
}
func (b *rawTxnHandoffBackend) NewPhysicalBatchWithSize(ops int) batch.Interface {
	return &rawTxnHandoffBatch{
		cowAcceptedReportBatch: b.cowAcceptedReportBackend.NewPhysicalBatchWithSize(ops).(*cowAcceptedReportBatch),
		owner:                  b,
		data:                   ops > 0,
	}
}
func (b *rawTxnHandoffBatch) observe(err error) error {
	if b.final && b.group.Accepted() {
		if b.data {
			b.owner.dataAccepted.Add(1)
		} else {
			b.owner.emptyAccepted.Add(1)
		}
	}
	return err
}
func (b *rawTxnHandoffBatch) Write() error { return b.observe(b.cowAcceptedReportBatch.Write()) }
func (b *rawTxnHandoffBatch) WriteSync() error {
	return b.observe(b.cowAcceptedReportBatch.WriteSync())
}

func TestRawTxnPressureRecheckDoesNotBypassParkedAcceptedHandoff(t *testing.T) {
	var want, got rawTxnHandoffOutcome
	t.Run("original-full-checkpoint", func(t *testing.T) { want = rawTxnPressureParkedHandoff(t, false) })
	t.Run("pressure-recheck", func(t *testing.T) { got = rawTxnPressureParkedHandoff(t, true) })
	if got != want {
		t.Fatalf("pressure changed original physical completion: got %+v want %+v", got, want)
	}
}

func rawTxnPressureParkedHandoff(t *testing.T, usePressure bool) rawTxnHandoffOutcome {
	t.Helper()
	t.Setenv(envDisableVlogGenerationLoop, "1")
	cache, backend := cowAutomaticFallbackFixture(t, "cow_btree")
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	cache.SetCommandWALCheckpointCleanupHook(backend.CleanupCommandWALCoveredSegmentsAtCheckpoint)
	cache.SetAutoCheckpointWALBytesHook(backend.CommandWALActiveBytes)
	cache.StartAutoCheckpoint(0, 2<<30, 0)
	if !cache.nativePressureDrainEnabled() {
		t.Fatal("fixture did not arm the original size-maintenance capability")
	}
	// A separate original denial becomes eligible after its own full drain.
	// Park the NEW suffix later to make the handoff guard independently
	// decisive; merely filling and cancelling ordinary slots proves no S.
	writeCOWAutoFallbackRaw(t, cache, backend, "initial")
	denied := realRawTxnPressureRefusal(t, cache)
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	writeCOWAutoFallbackRaw(t, cache, backend, "accepted")
	held := cache.AcquireSnapshot()
	if held == nil {
		t.Fatal("no retained before-cut view")
	}
	defer held.Close()
	key := []byte("key")
	heldValue, heldRevision, err := held.GetVersioned(key)
	if err != nil || string(heldValue) != "accepted" {
		t.Fatalf("original held cut did not contain its exact raw value: %q revision=%d: %v", heldValue, heldRevision, err)
	}

	// The physical publication is actually accepted. The wrapper reports a
	// late error and disallows further root captures, as in the established
	// accepted-error regression. A future healthy WAL readiness check is not
	// proof this original handoff finished.
	wrapper := &rawTxnHandoffBackend{cowAcceptedReportBackend: &cowAcceptedReportBackend{DB: backend}}
	cache.backend = wrapper
	wrapper.inject.Store(true)
	if err := cache.Checkpoint(); !errors.Is(err, errCOWAcceptedReport) {
		t.Fatalf("expected real accepted report, got %v", err)
	}
	cache.flushMu.Lock()
	var original backenddb.StateToken
	var seal rootpublication.PhysicalSealReceipt
	var parked bool
	if h := cache.cow.handoff; h != nil && h.accepted {
		original, parked = h.basis.snapshot.StateToken()
		seal = h.seal
	}
	refresh := cache.cow.refreshRequired.Load()
	cache.flushMu.Unlock()
	if !parked || !seal.Valid() || refresh || cache.maintenanceActive.Load() ||
		backendPublicationReady(cache.backend) != nil || wrapper.dataAccepted.Load() != 1 || wrapper.emptyAccepted.Load() != 0 {
		t.Fatalf("parked accepted handoff must be independent of other guards: parked=%t refresh=%t publications=%d", parked, refresh, wrapper.publications.Load())
	}
	value, originalRevision, err := backend.GetVersioned(key)
	if err != nil || string(value) != "accepted" || originalRevision == page.LegacyEntryRevision {
		t.Fatalf("parked physical key = %q, revision=%d, err=%v", value, originalRevision, err)
	}

	wrapper.refuseCapture.Store(false)
	before := cache.checkpointRuns.Load()
	raw := backend.Stats()["treedb.command_wal.append.entry_scan.count_total"]
	if raw == "" {
		t.Fatal("missing original raw frame count")
	}
	// Capture at the original full checkpoint's frontier, after handoff
	// settlement but before any later empty group can advance physical state.
	var installedOriginal bool
	var observeErr error
	cache.testBeforeCheckpointFrontierCapture = func() {
		if active := cache.cow.retainCut(); active != nil {
			token, ok := active.basis.snapshot.StateToken()
			installedOriginal = ok && token == original && cache.cow.handoff == nil
			observeErr = cache.cow.releaseCut(active)
		}
	}
	defer func() { cache.testBeforeCheckpointFrontierCapture = nil }()
	var own, marker rootpublication.AcceptedTicket
	if usePressure {
		own, marker, err = cache.reserveRawTxnPairAfterPressure(denied)
	} else {
		if err := cache.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		own, marker, err = cache.cow.prefix.ReserveAcceptedOrdinaryPair()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer own.CancelAccepted()
	defer marker.CancelAccepted()
	if !own.Valid() || !marker.Valid() {
		t.Fatal("original reclamation failed to return two private slots")
	}
	cache.flushMu.Lock()
	stillParked := cache.cow.handoff != nil
	cache.flushMu.Unlock()
	if got := cache.checkpointRuns.Load() - before; got != 1 || stillParked || !installedOriginal || observeErr != nil || !seal.Sealed() {
		t.Fatalf("must install the exact parked basis before capturing a new frontier: checkpoints=%d parked=%t original=%t sealed=%t err=%v",
			got, stillParked, installedOriginal, seal.Sealed(), observeErr)
	}
	if data := wrapper.dataAccepted.Load(); data != 1 {
		t.Fatalf("must not republish parked raw data: got %d accepted data groups; want one", data)
	}
	t.Logf("exact accepted handoff installed: data groups=%d empty groups=%d", wrapper.dataAccepted.Load(), wrapper.emptyAccepted.Load())
	if got := backend.Stats()["treedb.command_wal.append.entry_scan.count_total"]; got != raw {
		t.Fatalf("actual raw frame count changed during original completion: %s -> %s", raw, got)
	}
	// The low-level fixture's held cut predates physical revision
	// materialization. It must keep its own version; current readers must
	// see the exact version of the original accepted physical publication.
	for _, reader := range []struct {
		name string
		read func([]byte) ([]byte, page.EntryRevision, error)
		want page.EntryRevision
	}{
		{"held", held.GetVersioned, heldRevision},
		{"cache", cache.GetVersioned, originalRevision},
		{"physical", backend.GetVersioned, originalRevision},
	} {
		data, revision, err := reader.read(key)
		if err != nil || string(data) != "accepted" || revision != reader.want {
			t.Fatalf("physical handoff changed %s key/value/revision %q, %d want %d: %v", reader.name, data, revision, reader.want, err)
		}
	}
	return rawTxnHandoffOutcome{cache.cow.prefix.LogicalFrontiers(), backend.State().AppliedCommandLSN, backend.CommandWALNextLSN()}
}

func TestRawTxnPressureRecheckRetainsOriginalAdmissionErrors(t *testing.T) {
	for _, mode := range []string{"sticky-basis", "terminal-prefix"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(envDisableVlogGenerationLoop, "1")
			cache, backend := cowAutomaticFallbackFixture(t, "cow_btree")
			cache.SetCommandWALCheckpointCutoverHook(func() {})
			cache.SetCommandWALCheckpointCleanupHook(backend.CleanupCommandWALCoveredSegmentsAtCheckpoint)
			cache.SetAutoCheckpointWALBytesHook(backend.CommandWALActiveBytes)
			cache.StartAutoCheckpoint(0, 2<<30, 0)
			if !cache.nativePressureDrainEnabled() {
				t.Fatal("fixture did not arm size worker")
			}
			writeCOWAutoFallbackRaw(t, cache, backend, "initial")
			denied := realRawTxnPressureRefusal(t, cache)
			if err := cache.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			var a, b rootpublication.AcceptedTicket
			var err error
			before := cache.checkpointRuns.Load()
			if mode == "sticky-basis" {
				cache.cow.refreshRequired.Store(true)
				a, b, err = cache.reserveRawTxnPairAfterPressure(denied)
				if err != nil || !a.Valid() || !b.Valid() {
					t.Fatalf("original full fallback: %v", err)
				}
				defer a.CancelAccepted()
				defer b.CancelAccepted()
				if got := cache.checkpointRuns.Load() - before; got != 1 || cache.cow.refreshRequired.Load() {
					t.Fatalf("available pair bypassed required refresh: full=%d sticky=%t", got, cache.cow.refreshRequired.Load())
				}
				return
			}
			want := errors.New("original accepted-prefix terminal error")
			cache.cow.prefix.PoisonAccepted(want)
			a, b, err = cache.reserveRawTxnPairAfterPressure(denied)
			if a.Valid() || b.Valid() || err != want {
				t.Fatalf("terminal refusal returned tickets %t/%t or wrong error: %v", a.Valid(), b.Valid(), err)
			}
			if got := cache.checkpointRuns.Load() - before; got != 0 {
				t.Fatalf("terminal refusal entered a full checkpoint: %d", got)
			}
		})
	}
}

func realRawTxnPressureRefusal(t *testing.T, cache *DB) rootpublication.AcceptedOrdinaryPairRefusal {
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
		a, b, refused, err := prefix.ReserveAcceptedOrdinaryPairWithRefusal()
		if refused.Valid() && err == rootpublication.ErrAcceptedCapacity {
			if a.Valid() || b.Valid() || len(hold) == 0 {
				t.Fatalf("invalid ordinary pair refusal: %t/%t held=%d", a.Valid(), b.Valid(), len(hold))
			}
			return refused
		}
		if err != nil || !a.Valid() || !b.Valid() || len(hold) > 8192 {
			t.Fatalf("failed to reach controlled bounded capacity with original debt: %v", err)
		}
		hold = append(hold, a, b)
	}
}

func TestRawTxnPressureRecheckCannotStartCheckpointDuringClose(t *testing.T) {
	t.Setenv(envDisableVlogGenerationLoop, "1")
	cache, backend := cowAutomaticFallbackFixture(t, "cow_btree")
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	cache.SetCommandWALCheckpointCleanupHook(backend.CleanupCommandWALCoveredSegmentsAtCheckpoint)
	cache.SetAutoCheckpointWALBytesHook(backend.CommandWALActiveBytes)
	cache.StartAutoCheckpoint(0, 2<<30, 0)
	writeCOWAutoFallbackRaw(t, cache, backend, "closing")
	denied := realRawTxnPressureRefusal(t, cache)
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	entered, proceed := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(proceed) })
	defer resume()
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWAcceptedPairPressureWait && e.Resource == durabilitycut.ResourceAuxiliary && e.Root == cache.dir {
			close(entered)
			<-proceed
		}
		return nil
	})
	defer restore()
	count := cache.checkpointRuns.Load()
	type result struct {
		accepted, spare rootpublication.AcceptedTicket
		err             error
	}
	waiting := make(chan result, 1)
	go func() {
		a, b, err := cache.reserveRawTxnPairAfterPressure(denied)
		waiting <- result{a, b, err}
	}()
	select {
	case <-entered: // Passed initial close check, not yet entered flush fence.
	case <-time.After(5 * time.Second):
		t.Fatal("the already-denied writer did not reach the existing fence")
	}

	closed := make(chan error, 1)
	go func() { closed <- cache.Close() }()
	done := time.NewTimer(5 * time.Second)
	defer done.Stop()
	for !cache.closing.Load() {
		select {
		case <-done.C:
			t.Fatal("original Close did not withdraw admitted writers")
		case <-time.After(time.Millisecond):
		}
	}
	resume()
	select {
	case got := <-waiting:
		if got.accepted.Valid() || got.spare.Valid() || got.err != errDBClosing {
			t.Fatalf("in-flight pressure obtained authority while closing: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pressure and original Close failed to finish")
	}
	select {
	case got := <-closed:
		if got != nil {
			t.Fatalf("checked original Close: %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original Close did not finish")
	}
	if got := cache.checkpointRuns.Load(); got != count {
		t.Fatalf("superseded pressure entered the original full checkpoint: %d -> %d", count, got)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: backend.Dir(), CommandWAL: true, Durability: backenddb.DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Get([]byte("key")); err != nil || string(got) != "closing" {
		_ = reopened.Close()
		t.Fatalf("original Close changed durable data: %q %v", got, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
