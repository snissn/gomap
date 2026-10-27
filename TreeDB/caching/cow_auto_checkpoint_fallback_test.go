package caching

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

func cowAutomaticFallbackFixture(t *testing.T, engine string) (*DB, *backenddb.DB) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend"), CommandWAL: true, Durability: backenddb.DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	cache, err := Open(dir, backend, Options{TransactionOwner: owner, MemtableMode: engine, ExternalCommandWAL: true, MemtableShards: 2, FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionOff)})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	if err := backend.BindNativeSemanticBridge(owner, cache); err != nil {
		t.Fatal(err)
	}
	if cache.cow == nil || cache.cow.ordered != (engine == "cow_ordered") {
		t.Fatalf("COW engine %q did not activate", engine)
	}
	t.Cleanup(func() {
		if !cache.closing.Load() {
			_ = cache.Close()
		}
		_ = backend.Close()
	})
	return cache, backend
}

func writeCOWAutoFallbackRaw(t *testing.T, cache *DB, backend *backenddb.DB, value string) {
	t.Helper()
	b := cache.NewBatch()
	defer b.Close()
	if err := b.AuthorizeCOWAppend(cache.cow.txnOwner); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("key"), []byte(value)); err != nil {
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
}

func writeCOWAutoFallbackNative(t *testing.T, backend *backenddb.DB) *testSemanticState {
	t.Helper()
	registerNativeBridgeTestCommand()
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	state := nativeBridgeState(t, capture)
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("native", []commitlog.CollectionDocument{{ID: []byte("id"), Document: []byte(`{"v":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := backend.NewCommandWALIntent(commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)
	if err != nil {
		t.Fatal(err)
	}
	result, err := backend.WriteNativePublication(&backenddb.NativePreparedPublication{Basis: capture.State(), State: state, StartSequence: capture.LogicalSequence()}, intent, false)
	if closeErr := capture.Close(); err != nil || closeErr != nil || !result.LogicalInstalled {
		t.Fatalf("native publication=%+v write=%v capture=%v", result, err, closeErr)
	}
	return state
}

type cowAutomaticFallbackRolloverBackend struct {
	*backenddb.DB
	rollover func()
}

func (b *cowAutomaticFallbackRolloverBackend) CommandWALCheckpointFallbackNeedsRefresh(basis backenddb.StateToken) (bool, error) {
	needed, err := b.DB.CommandWALCheckpointFallbackNeedsRefresh(basis)
	if err == nil && needed && b.rollover != nil {
		fn := b.rollover
		b.rollover = nil
		fn()
	}
	return needed, err
}

func TestCOWAutomaticCheckpointFallbackPreservesNewerSuffix(t *testing.T) {
	for _, engine := range []string{"cow_btree", "cow_ordered"} {
		t.Run(engine, func(t *testing.T) { testCOWAutomaticCheckpointFallbackPreservesNewerSuffix(t, engine) })
	}
}

func testCOWAutomaticCheckpointFallbackPreservesNewerSuffix(t *testing.T, engine string) {
	for _, mode := range []string{"mutable", "frozen", "rollover-during-convergence"} {
		t.Run(mode, func(t *testing.T) {
			frozen := mode == "frozen"
			cache, backend := cowAutomaticFallbackFixture(t, engine)
			cache.SetCommandWALCheckpointCutoverHook(func() {})
			writeCOWAutoFallbackRaw(t, cache, backend, "prefix")
			nativePrefix := writeCOWAutoFallbackNative(t, backend)
			opening := cache.AcquireSnapshot()
			if opening == nil {
				t.Fatal("missing retained opening")
			}
			defer opening.Close()
			prefix := cache.cow.prefix.LogicalFrontiers()
			before := backend.State().CommitSeq
			var nativeSuffix *testSemanticState
			cache.testAfterCheckpointFrontierCapture = func() {
				writeCOWAutoFallbackRaw(t, cache, backend, "suffix")
				nativeSuffix = writeCOWAutoFallbackNative(t, backend)
				if frozen {
					if err := cache.rolloverCOWForFlush(); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "rollover-during-convergence" {
					// Native ingress requires its concrete backend owner. Install
					// the observer only after both logical suffix writes finish.
					cache.backend = &cowAutomaticFallbackRolloverBackend{DB: backend, rollover: func() {
						if err := cache.rolloverCOWForFlush(); err != nil {
							t.Fatal(err)
						}
					}}
				}
			}
			if err := cache.checkpointContext(context.Background(), true); err != nil {
				t.Fatal(err)
			}
			cache.testAfterCheckpointFrontierCapture = nil
			state, live := backend.StateToken()
			if !live || state.AppliedCommandLSN != prefix.C || backend.CommandWALNextLSN() != prefix.C+3 {
				t.Fatalf("checkpoint consumed newer WAL suffix: %+v, prefix=%+v next=%d", state, prefix, backend.CommandWALNextLSN())
			}
			needed, err := backend.CommandWALCheckpointFallbackNeedsRefresh(state)
			if err != nil || needed != frozen {
				t.Fatalf("fallback needs refresh=%v,%v; want %v", needed, err, frozen)
			}
			wantPublications := uint64(2)
			if frozen {
				wantPublications = 1
			}
			if state.CommitSeq != before+wantPublications {
				t.Fatalf("commits=%d want %d", state.CommitSeq-before, wantPublications)
			}
			if value, err := backend.Get([]byte("key")); err != nil || string(value) != "prefix" {
				t.Fatalf("physical prefix=%q,%v", value, err)
			}
			if value, err := cache.Get([]byte("key")); err != nil || string(value) != "suffix" {
				t.Fatalf("logical suffix=%q,%v", value, err)
			}
			if value, err := opening.Get([]byte("key")); err != nil || string(value) != "prefix" {
				t.Fatalf("retained opening=%q,%v", value, err)
			}
			if got := cache.cow.prefix.LogicalFrontiers(); got.S.LogicalSequence != prefix.V || got.S.CanonicalLSN != prefix.C || got.C != prefix.C+2 {
				t.Fatalf("original boundary changed: %+v, prefix=%+v", got, prefix)
			}
			if cache.cow.cut.semanticRootSet != nativeSuffix || nativePrefix.calls != 0 || nativeSuffix.calls != 0 {
				t.Fatal("native suffix or retained semantic opening changed")
			}
			if mode == "rollover-during-convergence" && len(cache.cow.cut.frozen) == 0 {
				t.Fatal("convergence consumed the concurrent frozen suffix")
			}
			cache.runPendingCommandWALCleanup()
			if cache.commandWALCleanupPending.Load() || cache.commandWALCleanupRetries.Load() != 0 {
				t.Fatal("covered-prefix cleanup did not complete")
			}
			if !frozen && backend.Stats()["treedb.command_wal.cleanup.removed_segments_total"] == "0" {
				t.Fatal("converged natural checkpoint did not reclaim a covered segment")
			}
			// The next build must use the exact second publication's Snapshot,
			// rather than a stale token or a fabricated logical basis.
			if err := cache.Checkpoint(); err != nil {
				t.Fatalf("next checkpoint: %v", err)
			}
			if value, err := backend.Get([]byte("key")); err != nil || string(value) != "suffix" {
				t.Fatalf("next physical suffix=%q,%v", value, err)
			}
			if err := opening.Close(); err != nil {
				t.Fatal(err)
			}
			if err := cache.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := cache.cow.budget.Stats(); stats.TotalBytes != 0 || stats.Sources != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("original ownership retained: %+v", stats)
			}
			if nativePrefix.calls != 1 || nativeSuffix.calls != 1 {
				t.Fatalf("semantic cleanup calls=%d/%d", nativePrefix.calls, nativeSuffix.calls)
			}
			dir := backend.Dir()
			if err := backend.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, Durability: backenddb.DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if value, err := reopened.Get([]byte("key")); err != nil || string(value) != "suffix" {
				t.Fatalf("reopened suffix=%q,%v", value, err)
			}
		})
	}
}

type cowAutomaticFallbackReportBackend struct {
	*cowAcceptedReportBackend
	armed bool
}

func (b *cowAutomaticFallbackReportBackend) PrepareCommandWALCoveredPrefixCleanup() (bool, error) {
	pending, err := b.DB.PrepareCommandWALCoveredPrefixCleanup()
	if b.armed {
		b.armed = false
		b.inject.Store(true)
	}
	return pending, err
}

func TestCOWAutomaticFallbackAcceptedErrorRetainsOriginalHandoff(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	wrapped := &cowAutomaticFallbackReportBackend{cowAcceptedReportBackend: &cowAcceptedReportBackend{DB: backend}, armed: true}
	cache.backend = wrapped
	writeCOWAutoFallbackRaw(t, cache, backend, "prefix")
	prefix := cache.cow.prefix.LogicalFrontiers()
	if err := cache.checkpointContext(context.Background(), true); !errors.Is(err, errCOWAcceptedReport) {
		t.Fatalf("accepted fallback report=%v", err)
	}
	if wrapped.publications.Load() != 2 || cache.cow.handoff == nil || !cache.cow.handoff.accepted || len(cache.cow.handoff.cut.frozen) != 0 {
		t.Fatal("original accepted empty-cut handoff lost")
	}
	accepted := backend.State().CommitSeq
	if err := cache.completeCOWHandoff(); err != nil {
		t.Fatal(err)
	}
	if cache.cow.handoff != nil || backend.State().CommitSeq != accepted || wrapped.publications.Load() != 2 {
		t.Fatal("original fallback publication replayed")
	}
	if got := cache.cow.prefix.LogicalFrontiers(); got.S.CanonicalLSN != prefix.C || got.S.LogicalSequence != prefix.V {
		t.Fatalf("original sealed boundary lost: %+v", got)
	}
	// The accepted receipt works even while ordinary replacement capture is
	// refused. A subsequent writer/checkpoint must use its exact installed token.
	wrapped.refuseCapture.Store(false)
	writeCOWAutoFallbackRaw(t, cache, backend, "after-error")
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if value, err := backend.Get([]byte("key")); err != nil || string(value) != "after-error" {
		t.Fatalf("after accepted failure=%q,%v", value, err)
	}
}
