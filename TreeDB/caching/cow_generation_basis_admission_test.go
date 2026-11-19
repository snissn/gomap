package caching

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCOWGenerationWorkerRetriesBasisRefreshSuccessorAdmission(t *testing.T) {
	cache, db := nativeBridgeFixture(t, true)
	backend := &generationBasisCaptureBackend{DB: db}
	cache.backend = backend
	key, value := []byte("key"), []byte("held successor")
	if err := cache.Set(key, value); err != nil {
		t.Fatal(err)
	}
	if err := cache.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	hold := cache.AcquireSnapshot()
	if hold == nil {
		t.Fatal("cannot hold the original cut")
	}
	t.Cleanup(func() { _ = hold.Close() })
	original := cache.cow.cut
	prefix := cache.cow.prefix
	frontiers, commit, nextLSN := prefix.LogicalFrontiers(), db.State().CommitSeq, db.CommandWALNextLSN()
	through := rootpublication.SealedPrefix{LogicalSequence: original.frozenLogicalSeq, CanonicalLSN: original.frozenCommandLSN}
	if through == frontiers.S {
		t.Fatalf("no pending frozen prefix: %+v", frontiers)
	}

	budget := cache.cow.maintenance
	var pressure *memtable.COWExternalLease
	backend.afterCapture = func() {
		// Allow the original snapshot and scratch to finish, then retain
		// the remaining maintenance headroom before the successor reserves.
		leaseCell := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
		var err error
		pressure, err = budget.AcquireExternal(budget.Limits().MaxTotalBytes - budget.Stats().TotalBytes - leaseCell)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pressure.Close)
	}
	if err := cache.RunBackendMaintenance(nil); !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "install COW basis") {
		t.Fatalf("central maintenance did not reach successor allocation: %v", err)
	}
	loaded := budget.Stats().TotalBytes
	reported := make(chan error, 1)
	cache.notifyError = func(err error) {
		select {
		case reported <- err:
		default:
		}
	}
	captures := backend.captures.Load()
	retry, refusal := cache.maintainCOWGenerationPressure()
	if !retry || refusal == nil || refusal.through != through {
		select {
		case err := <-reported:
			t.Fatalf("generation worker lost the pre-swap retry: retry=%t, reported=%v", retry, err)
		default:
			t.Fatalf("generation worker lost exact refresh retry: retry=%t, refusal=%v", retry, refusal)
		}
	}
	if backend.captures.Load() != captures+1 {
		t.Fatal("worker did not reach the successor admission")
	}
	if cache.cow.cut != original || !cache.cow.refreshRequired.Load() || budget.Stats().TotalBytes != loaded ||
		prefix.LogicalFrontiers() != frontiers || db.State().CommitSeq != commit || db.CommandWALNextLSN() != nextLSN {
		t.Fatal("refused successor changed the original authority or retained candidate memory")
	}

	backend.captureEvents = make(chan struct{}, 4)
	cache.StartAutoCheckpoint(0, 1<<40, 0)
	cache.nativeGenerationFlushCh <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for range 2 {
		select {
		case <-backend.captureEvents:
		case err := <-reported:
			t.Fatalf("temporary successor pressure reported a hard failure: %v", err)
		case <-ctx.Done():
			t.Fatal("the worker did not confirm the private basis refusal")
		}
	}
	if err := lockCheckpointMutexContext(ctx, &cache.flushMu); err != nil {
		t.Fatal(err)
	}
	cache.flushMu.Unlock()
	quiet := time.NewTimer(2 * cowGenerationPressureRetryDelay)
	defer quiet.Stop()
	select {
	case <-backend.captureEvents:
		t.Fatal("unchanged pressure repeated private snapshot capture")
	case err := <-reported:
		t.Fatalf("temporary successor pressure reported a hard failure: %v", err)
	case <-quiet.C:
	}
	pressure.Close()
	if err := prefix.WaitAccepted(ctx, through); err != nil || cache.cow.refreshRequired.Load() {
		t.Fatalf("original generation did not complete after the pressure release: %v", err)
	}
	if after := prefix.LogicalFrontiers(); after.S != through || prefix.AcceptedPendingCount() != 0 ||
		db.State().CommitSeq != commit+1 || db.CommandWALNextLSN() != nextLSN {
		t.Fatalf("retry changed the pending authority or published more than once: before=%+v after=%+v", frontiers, after)
	}
	if got, err := hold.Get(key); err != nil || string(got) != string(value) {
		t.Fatalf("held cut = %q, %v", got, err)
	}
	if err := hold.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("checked Close after capacity recovery: %v", err)
	}
	closedStats := budget.Stats()
	closedStats.PeakBytes = 0
	if closedStats != (memtable.COWStats{}) {
		t.Fatalf("private basis left maintenance bytes or pins after Close: %+v", closedStats)
	}
	select {
	case err := <-reported:
		t.Fatalf("capacity recovery reported a background failure: %v", err)
	default:
	}
}

func TestCOWGenerationWorkerRejectsImpossibleBasisRefreshSuccessor(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxGenerations = 1 << 20
	limits.MaxSources = limits.MaxGenerations
	cache, db := nativeBridgeFixtureWithLimits(t, true, limits)
	backend := &generationBasisCaptureBackend{DB: db}
	cache.backend = backend
	if charge := cowCutCharge(len(cache.cow.writers), limits.MaxSources); charge <= cache.cow.maintenance.Limits().MaxTotalBytes {
		t.Fatalf("fixed successor vectors fit the maintenance budget: %d", charge)
	}
	if err := cache.RunBackendMaintenance(nil); !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "install COW basis") {
		t.Fatalf("real successor allocation did not refuse fixed geometry: %v", err)
	}
	var reported error
	cache.notifyError = func(err error) { reported = err }
	captures := backend.captures.Load()
	if retry, refusal := cache.maintainCOWGenerationPressure(); retry || refusal != nil {
		t.Fatalf("worker retried a successor that exceeds the whole maintenance budget: retry=%t, refusal=%v", retry, refusal)
	}
	if backend.captures.Load() != captures+1 || !errors.Is(reported, memtable.ErrCOWCapacity) || !strings.Contains(reported.Error(), "install COW basis") {
		t.Fatalf("worker lost the hard successor error: %v", reported)
	}
	if err := cache.Close(); !errors.Is(err, reported) {
		t.Fatalf("checked Close lost the original hard refusal: %v", err)
	}
}

func TestCOWGenerationWorkerRejectsImpossibleBasisRefreshOverlap(t *testing.T) {
	t.Setenv(envDisableVlogGenerationLoop, "1")
	limits := memtable.DefaultCOWLimits()
	limits.MaxSources, limits.MaxGenerations = 512, 512
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	successorCharge := cowBasisInstallCharge(2, limits.MaxSources) + leaseBytes
	limits.MaxInFlightBytes = 2 * (successorCharge + 1024)
	cache, db := nativeBridgeFixtureWithLimits(t, true, limits)
	backend := &generationBasisCaptureBackend{DB: db}
	cache.backend = backend
	budget := cache.cow.maintenance
	limit := budget.Limits().MaxTotalBytes
	openingCharge, _ := budget.MaintenanceUsage()
	var capturedCharge uint64
	backend.afterCapture = func() {
		capturedCharge, _ = budget.MaintenanceUsage()
	}
	prefix, commit, nextLSN := cache.cow.prefix.LogicalFrontiers(), db.State().CommitSeq, db.CommandWALNextLSN()
	original := cache.cow.cut
	if err := cache.RunBackendMaintenance(nil); !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "install COW basis") {
		t.Fatalf("actual private successor admission did not refuse: %v", err)
	}
	after, _ := budget.MaintenanceUsage()
	if successorCharge > limit-budget.Stats().ControlBytes || capturedCharge <= openingCharge || capturedCharge+successorCharge <= limit || after != openingCharge {
		t.Fatalf("fixture did not reach mandatory overlap: live=%d captured=%d after=%d successor=%d limit=%d", openingCharge, capturedCharge, after, successorCharge, limit)
	}
	var reported error
	cache.notifyError = func(err error) { reported = err }
	if retry, allocation := cache.maintainCOWGenerationPressure(); retry || allocation != nil {
		t.Fatalf("fixed overlap kept retrying the pending basis: live=%d captured=%d successor=%d limit=%d retry=%t allocation=%v", openingCharge, capturedCharge, successorCharge, limit, retry, allocation)
	}
	if !errors.Is(reported, memtable.ErrCOWCapacity) || !strings.Contains(reported.Error(), "install COW basis") {
		t.Fatalf("worker lost the hard successor refusal: %v", reported)
	}
	if cache.cow.cut != original || cache.cow.prefix.LogicalFrontiers() != prefix || db.State().CommitSeq != commit || db.CommandWALNextLSN() != nextLSN {
		t.Fatal("pre-swap refusal changed the current publication authority")
	}
	if err := cache.Set([]byte("key"), []byte("value")); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("write crossed the unresolved basis refresh: %v", err)
	}
	if err := cache.Close(); !errors.Is(err, reported) {
		t.Fatalf("checked Close lost the original hard refusal: %v", err)
	}
}

type generationBasisCaptureBackend struct {
	*backenddb.DB
	afterCapture  func()
	captures      atomic.Int64
	captureEvents chan struct{}
}

func (b *generationBasisCaptureBackend) AcquireSnapshotWithAllocationAdmission(admit func(backenddb.SnapshotAllocationSizes) error) (*backenddb.Snapshot, error) {
	snapshot, err := b.DB.AcquireSnapshotWithAllocationAdmission(admit)
	if err == nil {
		b.captures.Add(1)
		if after := b.afterCapture; after != nil {
			b.afterCapture = nil
			after()
		}
		select {
		case b.captureEvents <- struct{}{}:
		default:
		}
	}
	return snapshot, err
}
