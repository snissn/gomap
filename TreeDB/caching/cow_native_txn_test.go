package caching

import (
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
)

func TestCOWNativeProducerPressureCannotDiscountRetainedReservations(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	held, err := backend.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	before := cache.cow.budget.Stats()
	leaseCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	filler, err := cache.cow.budget.AcquireExternal(cache.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - leaseCharge)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	next, prepared := backend.CommandWALNextLSN(), cache.cow.prepareCalls.Load()
	frontiers := cache.cow.prefix.LogicalFrontiers()
	tx, err := backend.BeginNativeConditionalTxn()
	if tx != nil || !errors.Is(err, memtable.ErrCOWCapacity) {
		if tx != nil {
			_ = tx.Close()
		}
		t.Fatalf("retained reservation admitted a producer: %v/%v", tx, err)
	}
	if backend.CommandWALNextLSN() != next || cache.cow.prepareCalls.Load() != prepared || cache.cow.prefix.LogicalFrontiers() != frontiers {
		t.Fatal("pressure refusal prepared, assigned or installed a publication")
	}
	// A retained opening and external owner cannot be flushed away. Repeating
	// producer admission with exactly the same debt must not run more no-op
	// checkpoints, and must still preserve the original capacity refusal.
	drainedRuns := cache.checkpointRuns.Load()
	for i := 0; i < 3; i++ {
		tx, err = backend.BeginNativeConditionalTxn()
		if tx != nil || !errors.Is(err, memtable.ErrCOWCapacity) {
			if tx != nil {
				_ = tx.Close()
			}
			t.Fatalf("retained repeated admission %d: %v/%v", i, tx, err)
		}
	}
	if got := cache.checkpointRuns.Load(); got != drainedRuns {
		t.Fatalf("unchanged retained debt repeated checkpoints: before=%d after=%d", drainedRuns, got)
	}
	// Changed retained ownership permits one new drain, even though the old
	// reader still pins the same logical image and byte pressure remains high.
	fillBytes := cache.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - leaseCharge
	if err := filler.Resize(fillBytes - 8<<20); err != nil {
		t.Fatal(err)
	}
	if err := backend.PrepareNativeProducer(); err != nil {
		t.Fatal(err)
	}
	if got := cache.checkpointRuns.Load(); got != drainedRuns+1 {
		t.Fatalf("changed ownership did not renew maintenance: before=%d after=%d", drainedRuns, got)
	}
	drainedRuns = cache.checkpointRuns.Load()
	if err := backend.PrepareNativeProducer(); err != nil {
		t.Fatal(err)
	}
	if cache.checkpointRuns.Load() != drainedRuns {
		t.Fatal("unchanged renewed ownership repeated maintenance")
	}
	// New logical work must never be hidden by the old no-progress memory.
	if err := cache.Set([]byte("new-pressure-work"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := backend.PrepareNativeProducer(); err != nil {
		t.Fatal(err)
	}
	if got := cache.checkpointRuns.Load(); got != drainedRuns+1 {
		t.Fatalf("new logical work did not renew maintenance: before=%d after=%d", drainedRuns, got)
	}
	filler.Close()
	tx, err = backend.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatalf("admission after retained reservation release: %v", err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCOWNativeProducerPressureDiscreteLimits(t *testing.T) {
	for _, kind := range []string{"sources", "generations"} {
		t.Run(kind, func(t *testing.T) {
			cache, backend := nativeBridgeFixture(t, false)
			if kind == "generations" {
				if err := cache.Set([]byte("pressure"), []byte("payload")); err != nil {
					t.Fatal(err)
				}
			}
			budget := cache.cow.budget
			var pins []*memtable.COWWriter
			release := func() {
				for _, writer := range pins {
					work := writer.Close()
					work.Drain()
				}
				pins = nil
			}
			defer release()
			for {
				stats, limits := budget.Stats(), budget.Limits()
				if (kind == "sources" && stats.Sources == limits.MaxSources) || (kind == "generations" && stats.Generations == limits.MaxGenerations) {
					break
				}
				var writer *memtable.COWWriter
				var err error
				if kind == "sources" {
					writer, err = budget.NewNativeCOWWriter(0)
				} else {
					writer, err = memtable.NewCOWWriter(budget)
				}
				if err != nil {
					t.Fatal(err)
				}
				pins = append(pins, writer)
			}
			if stats := budget.Stats(); stats.ReservedBytes >= budget.Limits().MaxInFlightBytes*3/4 {
				t.Fatalf("not independent of bytes: %+v", stats)
			}
			frontiers, next := cache.cow.prefix.LogicalFrontiers(), backend.CommandWALNextLSN()
			runs := cache.checkpointRuns.Load()
			capture, err := backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			if err := capture.Close(); err != nil {
				t.Fatal(err)
			}
			if cache.checkpointRuns.Load() != runs {
				t.Fatal("read capture ran producer maintenance")
			}
			err = backend.PrepareNativeProducer()
			if err != nil {
				t.Fatal(err)
			}
			if cache.checkpointRuns.Load() != runs+1 {
				t.Fatal("expected exactly one checkpoint")
			}
			if cache.cow.prefix.LogicalFrontiers().V != frontiers.V || backend.CommandWALNextLSN() != next {
				t.Fatal("preflight changed logical publication")
			}
			if kind == "sources" {
				writer, err := budget.NewNativeCOWWriter(0)
				if writer != nil || !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("lost final source refusal: %v/%v", writer, err)
				}
				tx, err := backend.BeginPreparedNativeConditionalTxn()
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				if cache.checkpointRuns.Load() != runs+1 {
					t.Fatal("prepared opening checked pressure again")
				}
			} else {
				if budget.Stats().Generations >= budget.Limits().MaxGenerations {
					t.Fatal("checkpoint did not reclaim the nonempty raw generation")
				}
				for budget.Stats().Generations < budget.Limits().MaxGenerations {
					writer, err := memtable.NewCOWWriter(budget)
					if err != nil {
						t.Fatal(err)
					}
					pins = append(pins, writer)
				}
				writer, err := memtable.NewCOWWriter(budget)
				if writer != nil || !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("lost finite generation refusal: %v/%v", writer, err)
				}
			}
			release()
			tx, err := backend.BeginNativeConditionalTxn()
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			if err := cache.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 {
				t.Fatalf("final debt=%+v", stats)
			}
		})
	}
}

type nativeTxnTestPlan struct {
	state    *testSemanticState
	latest   backenddb.NativeSemanticState
	snapshot *backenddb.Snapshot
	calls    int
	err      error
}

func (p *nativeTxnTestPlan) PrepareNativePublication(latest backenddb.NativeSemanticState, exact *backenddb.Snapshot) (backenddb.NativeFinalizedPublication, error) {
	p.calls++
	p.latest, p.snapshot = latest, exact
	state := p.state
	p.state = nil
	return backenddb.NativeFinalizedPublication{State: state}, p.err
}
func nativeTxnIntent(t *testing.T, db *backenddb.DB, wal bool) *backenddb.CommandWALIntent {
	t.Helper()
	if !wal {
		return nil
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("native", []commitlog.CollectionDocument{{ID: []byte("id"), Document: []byte(`{"v":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := db.NewCommandWALIntent(commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}
func nativeTxnPublication(t *testing.T, tx backenddb.NativeConditionalTxn, key string) (*backenddb.NativePreparedPublication, *nativeTxnTestPlan) {
	t.Helper()
	plan := &nativeTxnTestPlan{state: nativeBridgeState(t, tx)}
	effects := []txnhistory.Effect{{Namespace: txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: 7, Domain: 9, Interpretation: 11}, Kind: txnhistory.PointEffect, Key: []byte(key)}}
	return &backenddb.NativePreparedPublication{Basis: tx.State(), StartSequence: tx.LogicalSequence(), Reprepare: plan, Entries: []batch.Entry{{Type: batch.OpPut, Key: []byte(key), Value: []byte(key)}}, Effects: effects}, plan
}
func TestCOWNativeConditionalTxnOriginalCore(t *testing.T) {
	registerNativeBridgeTestCommand()
	for _, wal := range []bool{false, true} {
		for _, handoff := range []bool{false, true} {
			t.Run(fmt.Sprintf("wal=%t/handoff=%t", wal, handoff), func(t *testing.T) {
				cache, db := nativeBridgeFixture(t, wal)
				a, err := db.BeginNativeConditionalTxn()
				if err != nil {
					t.Fatal(err)
				}
				b, err := db.BeginNativeConditionalTxn()
				if err != nil {
					t.Fatal(err)
				}
				if a.LogicalSequence() != b.LogicalSequence() || a.Snapshot() != b.Snapshot() || a.State() != b.State() {
					t.Fatal("not original paired opening")
				}
				ap, pa := nativeTxnPublication(t, a, "a")
				bp, pb := nativeTxnPublication(t, b, "b")
				ar, err := a.CommitNative(ap, nativeTxnIntent(t, db, wal), false)
				if err != nil || !ar.LogicalInstalled {
					t.Fatalf("A=%+v %v", ar, err)
				}
				first := cache.cow.cut.semanticRootSet
				if handoff {
					if err := cache.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				exact := cache.cow.cut.basis.snapshot
				br, err := b.CommitNative(bp, nativeTxnIntent(t, db, wal), false)
				if err != nil || !br.LogicalInstalled {
					t.Fatalf("B=%+v %v", br, err)
				}
				if pa.calls != 1 || pb.calls != 1 || pb.latest != first || pb.snapshot != exact {
					t.Fatal("did not prepare against latest compatible exact cut")
				}
				for _, k := range []string{"a", "b"} {
					got, err := cache.Get([]byte(k))
					if err != nil || string(got) != k {
						t.Fatalf("%s=%q %v", k, got, err)
					}
				}
				if err := cache.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if err := cache.Close(); err != nil {
					t.Fatal(err)
				}
				if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 {
					t.Fatalf("debt=%+v", stats)
				}
			})
		}
	}
}
func TestCOWNativeConditionalTxnRefusesBeforeReprepareAndAssignment(t *testing.T) {
	registerNativeBridgeTestCommand()
	for _, kind := range []string{"native-write", "raw-read", "partial"} {
		t.Run(kind, func(t *testing.T) {
			cache, db := nativeBridgeFixture(t, true)
			a, err := db.BeginNativeConditionalTxn()
			if err != nil {
				t.Fatal(err)
			}
			b, err := db.BeginNativeConditionalTxn()
			if err != nil {
				t.Fatal(err)
			}
			ap, _ := nativeTxnPublication(t, a, "a")
			bp, pb := nativeTxnPublication(t, b, "b")
			originalState := pb.state
			if kind == "native-write" {
				bp.Effects = ap.Effects
				bp.Entries = nil
			}
			if kind == "raw-read" {
				if _, err := b.(*nativeConditionalTxn).ConditionalTxn.Get([]byte("a")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := a.CommitNative(ap, nativeTxnIntent(t, db, true), false); err != nil {
				t.Fatal(err)
			}
			before := cache.cow.prefix.LogicalFrontiers()
			next := db.CommandWALNextLSN()
			wanted := backenddb.ErrConcurrentModification
			if kind == "partial" {
				wanted = errors.New("original private construction failed")
				pb.err = wanted
			}
			result, err := b.CommitNative(bp, nativeTxnIntent(t, db, true), false)
			if !errors.Is(err, wanted) || result.Assigned || result.LogicalInstalled || db.CommandWALNextLSN() != next || cache.cow.prefix.LogicalFrontiers() != before {
				t.Fatalf("refusal=%+v err=%v", result, err)
			}
			if kind == "partial" {
				if pb.calls != 1 || bp.State != nil {
					t.Fatal("partial state not adopted/released")
				}
			} else {
				if pb.calls != 0 {
					t.Fatal("conflict ran planner")
				}
				// Uncalled preadmitted test result still belongs to this original test plan.
				cache.cow.semanticMu.Lock()
				cache.cow.enqueueSemanticLocked(pb.state.Release())
				cache.cow.semanticMu.Unlock()
				pb.state = nil
				cache.cow.wakeSemanticCleanup()
			}
			if err := cache.Close(); err != nil {
				t.Fatal(err)
			}
			if originalState.calls != 1 || originalState.refs.Load() != 0 {
				t.Fatal("original private state not destroyed exactly once")
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 {
				t.Fatalf("debt=%+v", stats)
			}
		})
	}
}
