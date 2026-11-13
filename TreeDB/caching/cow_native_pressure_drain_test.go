package caching

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func nativeAcceptedPublication(t *testing.T, backend *backenddb.DB, custody backenddb.NativeSemanticCustody) (backenddb.NativeSemanticCapture, *backenddb.NativePreparedPublication, *backenddb.CommandWALIntent) {
	t.Helper()
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("native", []commitlog.CollectionDocument{{ID: []byte("id"), Document: []byte(`{"v":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := backend.NewCommandWALIntent(commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)
	if err != nil {
		t.Fatal(err)
	}
	publication := &backenddb.NativePreparedPublication{Basis: capture.State(), State: nativeBridgeState(t, capture), StartSequence: capture.LogicalSequence(), Custody: custody}
	return capture, publication, intent
}

// Fill the original queue with real installed native publications. A temporary
// ordinary reservation only probes the real finite capacity and is canceled
// before the next publication; no frontier, queue or accounting is rewritten.
func fillNativeAcceptedPrefix(t *testing.T, cache *DB, backend *backenddb.DB, first backenddb.NativeSemanticCustody) int {
	t.Helper()
	registerNativeBridgeTestCommand()
	limit := cache.cow.budget.Limits()
	for count := 0; count <= limit.MaxSources*limit.MaxGenerations; count++ {
		ticket, err := cache.cow.prefix.ReserveAccepted(false)
		if errors.Is(err, rootpublication.ErrAcceptedCapacity) {
			if count == 0 {
				t.Fatal("queue started full")
			}
			return count
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := ticket.CancelAccepted(); err != nil {
			t.Fatal(err)
		}
		var custody backenddb.NativeSemanticCustody
		if count == 0 {
			custody = first
		}
		capture, publication, intent := nativeAcceptedPublication(t, backend, custody)
		result, writeErr := backend.WriteNativePublication(publication, intent, false)
		closeErr := capture.Close()
		if writeErr != nil || closeErr != nil || !result.LogicalInstalled || !result.Assigned {
			t.Fatalf("fill %d: result=%+v write=%v close=%v", count, result, writeErr, closeErr)
		}
	}
	t.Fatal("native accepted prefix did not fill its configured finite capacity")
	return 0
}

func TestCOWNativeAcceptedCapacityUsesPressureService(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	cache.SetAutoCheckpointWALBytesHook(func() int64 { return 0 })
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	cache.StartAutoCheckpoint(time.Hour, 1<<30, 0)
	count := fillNativeAcceptedPrefix(t, cache, backend, nil)
	runs := cache.checkpointRuns.Load()
	active := backend.CommandWALActiveBytes()
	var cutoverBytes atomic.Int64
	cache.SetCommandWALCheckpointCutoverHook(func() { cutoverBytes.Store(backend.CommandWALActiveBytes()) })
	capture, publication, intent := nativeAcceptedPublication(t, backend, nil)
	result, err := backend.WriteNativePublication(publication, intent, false)
	closeErr := capture.Close()
	if err != nil || closeErr != nil || !result.LogicalInstalled || !result.Assigned {
		t.Fatalf("accepted pressure: result=%+v write=%v close=%v", result, err, closeErr)
	}
	if got := cache.checkpointRuns.Load() - runs; got != 0 {
		t.Fatalf("native accepted capacity used %d full checkpoint boundaries after %d actual publications", got, count)
	}
	if got := cutoverBytes.Load(); active <= 0 || got != active {
		t.Fatalf("pressure service rotated active journal: before=%d cutover=%d", active, got)
	}
}

func TestCOWNativeAcceptedCapacityFullCheckpointFallbacks(t *testing.T) {
	for _, mode := range []string{"raw", "interval-only", "missing-cutover"} {
		t.Run(mode, func(t *testing.T) {
			cache, backend := nativeBridgeFixture(t, true)
			cache.SetAutoCheckpointWALBytesHook(func() int64 { return 0 })
			cache.SetCommandWALCheckpointCutoverHook(func() {})
			threshold := int64(1 << 30)
			if mode == "interval-only" {
				threshold = 0
			}
			cache.StartAutoCheckpoint(time.Hour, threshold, 0)
			fillNativeAcceptedPrefix(t, cache, backend, nil)
			if mode == "missing-cutover" {
				cache.SetCommandWALCheckpointCutoverHook(nil)
			}
			runs := cache.checkpointRuns.Load()
			if mode == "raw" {
				b := cache.NewBatch()
				defer b.Close()
				if err := b.Set([]byte("raw"), []byte("value")); err != nil {
					t.Fatal(err)
				}
				f := &cowPressureRolloverFixture{db: cache, backend: backend}
				if err := f.write(b, true, nil); err != nil {
					t.Fatal(err)
				}
			} else {
				capture, publication, intent := nativeAcceptedPublication(t, backend, nil)
				result, err := backend.WriteNativePublication(publication, intent, false)
				closeErr := capture.Close()
				if err != nil || closeErr != nil || !result.LogicalInstalled {
					t.Fatalf("fallback result=%+v write=%v close=%v", result, err, closeErr)
				}
			}
			if got := cache.checkpointRuns.Load() - runs; got != 1 {
				t.Fatalf("fallback used %d full checkpoints", got)
			}
		})
	}
}

func TestCOWNativeAcceptedCapacityJoinsOriginalSealAndCustody(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	cache.SetAutoCheckpointWALBytesHook(func() int64 { return 0 })
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	cache.StartAutoCheckpoint(time.Hour, 1<<30, 0)
	seed := cache.NewBatch()
	if err := seed.Set([]byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := (&cowPressureRolloverFixture{db: cache, backend: backend}).write(seed, true, nil); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	old := cache.AcquireSnapshot()
	if old == nil {
		t.Fatal("missing original reader")
	}
	defer old.Close()
	custody := &checkpointRetirementCustody{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseCustodyOnce, physicalOnce, releasePhysicalOnce sync.Once
	unpauseCustody := func() { releaseCustodyOnce.Do(func() { close(custody.release) }) }
	physicalEntered, physicalRelease := make(chan struct{}), make(chan struct{})
	unpausePhysical := func() { releasePhysicalOnce.Do(func() { close(physicalRelease) }) }
	defer func() { unpausePhysical(); unpauseCustody() }()
	fillNativeAcceptedPrefix(t, cache, backend, custody)
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root == backend.Dir() && event.Resource == durabilitycut.ResourceMeta && event.Point == durabilitycut.BeforeMetaWrite {
			physicalOnce.Do(func() { close(physicalEntered); <-physicalRelease })
		}
		return nil
	})
	defer restore()
	capture, publication, intent := nativeAcceptedPublication(t, backend, nil)
	defer capture.Close()
	publication.Entries = []batch.Entry{{Type: batch.OpPut, Key: []byte("key"), Value: []byte("new")}}
	var assignments atomic.Int32
	publication.ObserveAssigned = func(uint64) error { assignments.Add(1); return nil }
	b := cache.NewBatch()
	defer b.Close()
	before, lsn := cache.cow.prefix.LogicalFrontiers(), backend.CommandWALNextLSN()
	type outcome struct {
		result backenddb.NativePublicationResult
		err    error
	}
	done := make(chan outcome, 1)
	joined := false
	defer func() {
		unpausePhysical()
		unpauseCustody()
		if !joined {
			<-done
		}
	}()
	go func() {
		result, err := cache.writeNativePublication(b, publication, intent, false)
		done <- outcome{result, err}
	}()
	wait := func(ch <-chan struct{}, label string) {
		t.Helper()
		select {
		case <-ch:
		case got := <-done:
			joined = true
			t.Fatalf("returned before %s: %+v", label, got)
		case <-time.After(10 * time.Second):
			t.Fatalf("did not reach %s", label)
		}
		if b.cowState.accepted.Valid() || assignments.Load() != 0 || backend.CommandWALNextLSN() != lsn || cache.cow.prefix.LogicalFrontiers().V != before.V {
			t.Fatalf("waiting command acquired acceptance/assignment before %s", label)
		}
		select {
		case got := <-done:
			joined = true
			t.Fatalf("escaped %s: %+v", label, got)
		default:
		}
	}
	wait(physicalEntered, "original physical seal")
	unpausePhysical()
	wait(custody.entered, "original accepted custody")
	unpauseCustody()
	select {
	case got := <-done:
		joined = true
		if got.err != nil || !got.result.Assigned || !got.result.LogicalInstalled || assignments.Load() != 1 || backend.CommandWALNextLSN() != lsn+1 || got.result.LogicalSequence != before.V+1 || custody.calls.Load() != 1 {
			t.Fatalf("not one original publication: %+v assignments=%d custody=%d", got, assignments.Load(), custody.calls.Load())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("accepted capacity service stalled")
	}
	if got, err := old.Get([]byte("key")); err != nil || string(got) != "old" {
		t.Fatalf("held reader changed: %q %v", got, err)
	}
	if got, err := cache.Get([]byte("key")); err != nil || string(got) != "new" {
		t.Fatalf("acknowledged value: %q %v", got, err)
	}
}

// Close must keep joining the original drain while a staged, still-unassigned
// command waits. Releasing custody cannot authorize that command after closing.
func TestCOWNativeAcceptedCapacityCloseDuringOriginalDrain(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	cache.SetAutoCheckpointWALBytesHook(func() int64 { return 0 })
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	cache.StartAutoCheckpoint(time.Hour, 1<<30, 0)
	custody := &checkpointRetirementCustody{entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	unpause := func() { once.Do(func() { close(custody.release) }) }
	defer unpause()
	fillNativeAcceptedPrefix(t, cache, backend, custody)
	capture, publication, intent := nativeAcceptedPublication(t, backend, nil)
	var assignments atomic.Int32
	publication.ObserveAssigned = func(uint64) error { assignments.Add(1); return nil }
	type outcome struct {
		result backenddb.NativePublicationResult
		err    error
	}
	done := make(chan outcome, 1)
	closed := make(chan error, 1)
	writerJoined, closeStarted, closeJoined := false, false, false
	defer func() {
		unpause()
		if !writerJoined {
			<-done
		}
		_ = capture.Close()
		if closeStarted && !closeJoined {
			<-closed
		}
	}()
	go func() {
		result, err := backend.WriteNativePublication(publication, intent, false)
		done <- outcome{result, err}
	}()
	select {
	case <-custody.entered:
	case got := <-done:
		writerJoined = true
		t.Fatalf("command escaped original custody: %+v", got)
	case <-time.After(10 * time.Second):
		t.Fatal("accepted drain did not reach original custody")
	}
	closeStarted = true
	go func() { closed <- cache.Close() }()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for !cache.closing.Load() {
		select {
		case err := <-closed:
			closeJoined = true
			t.Fatalf("Close returned before original custody: %v", err)
		case <-deadline.C:
			t.Fatal("Close did not enter its original admission barrier")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case err := <-closed:
		closeJoined = true
		t.Fatalf("Close escaped original drain: %v", err)
	default:
	}
	unpause()
	select {
	case got := <-done:
		writerJoined = true
		if !errors.Is(got.err, backenddb.ErrClosed) || got.result.Assigned || got.result.LogicalInstalled || assignments.Load() != 0 || intent.AssignedLSN() != 0 {
			t.Fatalf("closing command gained authority: %+v assignments=%d", got, assignments.Load())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("closing command stalled after original custody")
	}
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		closeJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not finish original drain")
	}
	if custody.calls.Load() != 1 {
		t.Fatalf("custody released %d times", custody.calls.Load())
	}
}

func TestCOWNativeAcceptedCapacityUnassignedRefusalAndErrors(t *testing.T) {
	for _, mode := range []string{"unassigned", "original-error", "cleanup-callback"} {
		t.Run(mode, func(t *testing.T) {
			cache, backend := nativeBridgeFixture(t, true)
			cache.SetAutoCheckpointWALBytesHook(func() int64 { return 0 })
			cache.SetCommandWALCheckpointCutoverHook(func() {})
			cache.StartAutoCheckpoint(time.Hour, 1<<30, 0)
			var tickets []rootpublication.AcceptedTicket
			defer func() {
				for _, ticket := range tickets {
					_ = ticket.CancelAccepted()
				}
			}()
			for {
				ticket, err := cache.cow.prefix.ReserveAccepted(false)
				if errors.Is(err, rootpublication.ErrAcceptedCapacity) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				tickets = append(tickets, ticket)
			}
			capture, publication, _ := nativeAcceptedPublication(t, backend, nil)
			defer capture.Close()
			b := cache.NewBatch()
			defer b.Close()
			if err := b.SetNativeSemanticPublication(cache.cow.txnOwner, publication); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("original accepted-prefix refusal")
			var captures atomic.Int32
			cache.testBeforeCheckpointFrontierCapture = func() {
				captures.Add(1)
				if mode == "original-error" {
					cache.cow.prefix.PoisonAccepted(failure)
				}
			}
			want := error(memtable.ErrCOWCapacity)
			if mode == "original-error" {
				want = failure
			}
			if mode == "cleanup-callback" {
				if err := cache.ownedCloseGuard.EnterCleanup(); err != nil {
					t.Fatal(err)
				}
				defer cache.ownedCloseGuard.LeaveCleanup()
				want = cache.ownedCloseGuard.CheckClose()
				if want == nil {
					t.Fatal("missing original callback guard")
				}
			}
			before, lsn := cache.cow.prefix.LogicalFrontiers(), backend.CommandWALNextLSN()
			err := b.admitCOWAccepted()
			if !errors.Is(err, want) || b.cowState.accepted.Valid() || b.COWReceipt().Assigned || backend.CommandWALNextLSN() != lsn || cache.cow.prefix.LogicalFrontiers() != before {
				t.Fatalf("unassigned refusal changed authority: error=%v want=%v receipt=%+v", err, want, b.COWReceipt())
			}
			wantCaptures := int32(1)
			if mode == "cleanup-callback" {
				wantCaptures = 0
			}
			if got := captures.Load(); got != wantCaptures {
				t.Fatalf("service captures=%d want=%d", got, wantCaptures)
			}
		})
	}
}

func TestCOWNativePressureDrainRequiresActualSizeWorker(t *testing.T) {
	for _, tc := range []struct {
		name                string
		first, reconfigured int64
		want                bool
	}{
		{"interval-only", 0, 0, false},
		{"interval-only-then-positive", 0, 1 << 30, false},
		{"size-enabled", 1 << 30, 1 << 30, true},
		{"size-enabled-then-disabled-notification", 1 << 30, -1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache, _ := nativeBridgeFixture(t, true)
			// Real worker, no effective byte debt: no timer or physical maintenance
			// races this capability classification. Start preserves its old semantics.
			cache.SetAutoCheckpointWALBytesHook(func() int64 { return 0 })
			cache.SetCommandWALCheckpointCutoverHook(func() {})
			cache.StartAutoCheckpoint(time.Hour, tc.first, 0)
			cache.StartAutoCheckpoint(time.Hour, tc.reconfigured, 0)
			if got := cache.autoCheckpointWorkerMaxWALBytes.Load(); got != tc.first {
				t.Fatalf("actual worker threshold=%d want first=%d", got, tc.first)
			}
			if got := cache.nativePressureDrainEnabled(); got != tc.want {
				t.Fatalf("pressure drain=%t want=%t", got, tc.want)
			}
			cache.SetAutoCheckpointWALBytesHook(nil)
			if cache.nativePressureDrainEnabled() {
				t.Fatal("missing external byte inventory must use full checkpoint")
			}
			cache.SetAutoCheckpointWALBytesHook(func() int64 { return 0 })
			cache.SetCommandWALCheckpointCutoverHook(nil)
			if cache.nativePressureDrainEnabled() {
				t.Fatal("missing original cutover hook must use full checkpoint")
			}
		})
	}
}

// Reuses the actual accepted publication/custody and physical publisher hook.
// No substituted checkpoint backend releases the physical fence: pressure-only
// completion itself must join both the seal and original accepted retirement.
func TestCOWNativePressureDrainJoinsSealAndAcceptedRetirement(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	registerNativeBridgeTestCommand()
	custody := &checkpointRetirementCustody{entered: make(chan struct{}), release: make(chan struct{})}
	physicalEntered, physicalRelease := make(chan struct{}), make(chan struct{})
	var physicalOnce, releaseOnce, custodyOnce sync.Once
	unpause := func() { releaseOnce.Do(func() { close(physicalRelease) }) }
	releaseCustody := func() { custodyOnce.Do(func() { close(custody.release) }) }
	defer func() { unpause(); releaseCustody() }()
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root == backend.Dir() && event.Resource == durabilitycut.ResourceMeta && event.Point == durabilitycut.BeforeMetaWrite {
			physicalOnce.Do(func() { close(physicalEntered); <-physicalRelease })
		}
		return nil
	})
	defer restore()
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("native", []commitlog.CollectionDocument{{ID: []byte("id"), Document: []byte(`{"v":1}`)}})
	if err != nil {
		_ = capture.Close()
		t.Fatal(err)
	}
	intent, err := backend.NewCommandWALIntent(commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)
	if err != nil {
		_ = capture.Close()
		t.Fatal(err)
	}
	publication := &backenddb.NativePreparedPublication{Basis: capture.State(), State: nativeBridgeState(t, capture), StartSequence: capture.LogicalSequence(), Custody: custody}
	result, writeErr := backend.WriteNativePublication(publication, intent, false)
	closeErr := capture.Close()
	if writeErr != nil || closeErr != nil || !result.LogicalInstalled || !result.Assigned {
		t.Fatalf("original publication=%+v write=%v capture=%v", result, writeErr, closeErr)
	}
	through := cache.cow.prefix.LogicalFrontiers()
	done := make(chan error, 1)
	go func() { done <- cache.materializeCOWPrefix(context.Background(), false, nil, true) }()
	wait := func(ch <-chan struct{}, label string) {
		t.Helper()
		select {
		case <-ch:
		case err := <-done:
			t.Fatalf("pressure drain returned before %s: %v", label, err)
		case <-time.After(10 * time.Second):
			t.Fatalf("pressure drain did not reach %s", label)
		}
	}
	wait(physicalEntered, "original physical seal")
	select {
	case err := <-done:
		t.Fatalf("pressure drain returned before physical durability: %v", err)
	default:
	}
	unpause()
	wait(custody.entered, "original accepted retirement")
	select {
	case err := <-done:
		t.Fatalf("pressure drain returned before original custody refund: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseCustody()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pressure drain did not join original retirement")
	}
	if got := cache.cow.prefix.LogicalFrontiers(); got.S.LogicalSequence != through.V || got.S.CanonicalLSN != through.C || custody.calls.Load() != 1 {
		t.Fatalf("original retirement=%+v through=%+v calls=%d", got, through, custody.calls.Load())
	}
	if err := cache.joinSemanticCleanupFrontier(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := cache.cow.budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 || stats.Sources != 0 {
		t.Fatalf("final original accounting: %+v", stats)
	}
}

func TestCOWFlushCompletionRetainsOriginalSealAfterHandoff(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	b := cache.NewBatch()
	defer b.Close()
	if err := b.AuthorizeCOWAppend(cache.cow.txnOwner); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error {
		lsn, err := backend.AppendRawKVCommandWALOrderedEntryScanWithHintPreparedFinalizedAndMode(b.PrepareExternalCommandWALPublication, b.finalizeCOWPublication, b.Replay, 1, backenddb.RawKVCommandWALAppendRelaxed)
		if receiptErr := b.RecordCOWAppend(cache.cow.txnOwner, lsn, err, false); err == nil {
			err = receiptErr
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	var completed checkpointCommandWALPublish
	cache.flushMu.Lock()
	err := cache.checkpointFlushValueLogLanes()
	if err == nil {
		err = cache.flushCOWFrozen(true, &completed)
	}
	cache.flushMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !completed.seal.Valid() || cache.cow.handoff != nil || completed.appliedLSN == 0 || !completed.consumed {
		t.Fatalf("completion lost original accepted receipt: %+v handoff=%p", completed, cache.cow.handoff)
	}
	if err := completed.seal.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !completed.seal.Sealed() {
		t.Fatal("original receipt not durable")
	}
}
