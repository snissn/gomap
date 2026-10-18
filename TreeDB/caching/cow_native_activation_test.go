package caching

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/page"
)

func nativeActivationFixture(t *testing.T, wal bool) (*DB, *backenddb.DB) {
	t.Helper()
	cache, backend := nativeBridgeFixture(t, wal)
	t.Cleanup(func() {
		if !cache.closing.Load() {
			if err := cache.Close(); err != nil {
				t.Errorf("active owner Close=%v", err)
			}
		}
	})
	return cache, backend
}

func TestCOWNativeActivationConstructorAndAliasFence(t *testing.T) {
	for _, wal := range []bool{false, true} {
		t.Run(map[bool]string{false: "NoWAL", true: "WAL"}[wal], func(t *testing.T) {
			cache, backend := nativeActivationFixture(t, wal)
			release, err := backend.AdmitLegacyCommandProducer()
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.ActivateNativeSemanticOwner(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("hook-free live constructor activation=%v", err)
			}
			release()
			release()
			calls := 0
			unregister := backend.RegisterCommandWALRawPublishBarrier(func() error { calls++; return nil })
			if err := backend.ActivateNativeSemanticOwner(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("legacy barrier activation=%v", err)
			}
			if err := cache.Set([]byte("blocked"), []byte("v")); !errors.Is(err, backenddb.ErrConditionalTxnUnsupported) || calls != 0 {
				t.Fatalf("P1 raw barrier=%v calls=%d", err, calls)
			}
			unregister()
			physical := backend.NewPhysicalBatch()
			defer physical.Close()
			if err := physical.Set([]byte("bypass"), []byte("no")); err != nil {
				t.Fatal(err)
			}
			if wal {
				guard, err := backend.LockCommandWALStagingGuardV1()
				if err != nil {
					t.Fatal(err)
				}
				if err := backend.ActivateNativeSemanticOwner(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
					t.Fatalf("live staging activation=%v", err)
				}
				guard.Release()
			}
			if err := backend.ActivateNativeSemanticOwner(); err != nil {
				t.Fatal(err)
			}
			snap := backend.AcquireSnapshot()
			before, ok := snap.StateToken()
			if !ok {
				t.Fatal("basis")
			}
			snap.Close()
			next := backend.CommandWALNextLSN()
			if release, err := backend.AdmitLegacyCommandProducer(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || release != nil {
				t.Fatalf("live constructor admitted=%v", err)
			}
			late := backend.RegisterCommandWALRawPublishBarrier(func() error { calls++; return nil })
			defer late()
			if err := physical.Write(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("preobtained physical alias=%v", err)
			}
			if err := backend.CommitAtState(before.RootPageID, before); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) && !errors.Is(err, backenddb.ErrCommandWALUnsupported) {
				t.Fatalf("common finalizer alias=%v", err)
			}
			callbacks := 0
			err = backend.FinalizeRawKVEntryScanForCachedPublication(func() error { callbacks++; return nil }, func([]byte, func(page.ValuePtr) (uint64, bool)) error { return nil }, func(func(batch.Entry) error) error { return nil }, 0)
			if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || callbacks != 0 {
				t.Fatalf("ordinary NoWAL finalizer=%v calls=%d", err, callbacks)
			}
			if wal {
				if g, err := backend.LockCommandWALStagingGuardV1(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || g != nil {
					t.Fatalf("ordinary staging=%v", err)
				}
				payload, err := commitlog.EncodeRawKVBatchPayload(nil)
				if err != nil {
					t.Fatal(err)
				}
				intent, err := backend.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
				if err != nil {
					t.Fatal(err)
				}
				if lsn, err := backend.AppendStagedCommandWALIntent(intent, false); lsn != 0 || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
					t.Fatalf("staged alias=%d,%v", lsn, err)
				}
				if lsn, err := backend.AppendCommandWALIntent(intent, false); lsn != 0 || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
					t.Fatalf("ordinary append=%d,%v", lsn, err)
				}
			}
			snap = backend.AcquireSnapshot()
			after, _ := snap.StateToken()
			snap.Close()
			if after != before || backend.CommandWALNextLSN() != next || calls != 0 {
				t.Fatalf("rejected aliases moved roots/LSN/callbacks: %+v -> %+v", before, after)
			}
			// NoWAL raw writes use the original owner; WAL uses the same original guard
			// through the actual native bridge below.
			if !wal {
				if err := cache.Set([]byte("owner"), []byte("yes")); err != nil {
					t.Fatal(err)
				}
				if err := cache.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			} else {
				registerNativeBridgeTestCommand()
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
				if err := intent.PrepareSealedReceiptWithAdmission(capture.NativeAllocator()); err != nil {
					t.Fatal(err)
				}
				defer intent.ReleaseNativeSealedReceipt()
				publication := &backenddb.NativePreparedPublication{Basis: capture.State(), State: nativeBridgeState(t, capture), StartSequence: capture.LogicalSequence(), Entries: []batch.Entry{{Type: batch.OpPut, Key: []byte("owner"), Value: []byte("yes")}}}
				result, err := backend.WriteNativePublication(publication, intent, false)
				capture.Close()
				if err != nil || !result.LogicalInstalled || !result.Assigned || !result.Seal.Sealed() {
					t.Fatalf("original owner publication=%+v %v", result, err)
				}
			}
			got, err := cache.Get([]byte("owner"))
			if err != nil || string(got) != "yes" {
				t.Fatalf("owner read=%q %v", got, err)
			}
		})
	}
}

func TestCOWNativeActivationRefusesOriginalInFlightAppend(t *testing.T) {
	_, backend := nativeActivationFixture(t, true)
	payload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := backend.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	refusal := errors.New("original append preframe refusal")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeDependencyAppend && e.Resource == durabilitycut.ResourceCommandWAL {
			close(entered)
			<-release
			return refusal
		}
		return nil
	})
	defer restore()
	done := make(chan error, 1)
	go func() {
		lsn, err := backend.AppendStagedCommandWALIntent(intent, false)
		if lsn != 0 {
			done <- errors.New("refused frame got LSN")
			return
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("append did not reach actual preframe sink")
	}
	if err := backend.ActivateNativeSemanticOwner(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("activation crossed original append=%v", err)
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, refusal) {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("append did not end")
	}
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		t.Fatalf("activation after original refusal=%v", err)
	}
}

func TestCOWNativeActivationRefusesPendingAndRetainedOpening(t *testing.T) {
	cache, backend := nativeActivationFixture(t, false)
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.ActivateNativeSemanticOwner(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("retained opening=%v", err)
	}
	capture.Close()
	if err := cache.Set([]byte("pending"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := backend.ActivateNativeSemanticOwner(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("pending raw work=%v", err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		t.Fatalf("compatible drained basis=%v", err)
	}
}
