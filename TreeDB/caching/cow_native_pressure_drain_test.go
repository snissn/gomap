package caching

import (
	"context"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

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
