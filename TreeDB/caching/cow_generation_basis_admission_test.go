package caching

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
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

func TestCOWGenerationBasisRefreshReservesActualSources(t *testing.T) {
	const heldSourceCapacity = 1 << 20
	for _, capacity := range []uint64{50_784, 56_000, 63_599, 108_000, heldSourceCapacity} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			t.Setenv(envDisableVlogGenerationLoop, "1")
			limits := memtable.DefaultCOWLimits()
			limits.MaxSources, limits.MaxGenerations = 512, 512
			limits.MaxInFlightBytes = 2 * capacity
			cache, db := nativeBridgeFixtureWithLimits(t, true, limits)
			budget := cache.cow.maintenance
			backend := &generationBasisCaptureBackend{DB: db}
			cache.backend = backend

			// At the larger cap, preserve a real frozen source across both swaps.
			var hold *Snapshot
			key, value := []byte("key"), []byte("frozen suffix")
			if capacity == heldSourceCapacity {
				if err := cache.Set(key, value); err != nil {
					t.Fatal(err)
				}
				if err := cache.rolloverCOWForFlush(); err != nil {
					t.Fatal(err)
				}
				hold = cache.AcquireSnapshot()
				if hold == nil {
					t.Fatal("cannot hold the original frozen source")
				}
				t.Cleanup(func() { _ = hold.Close() })
			}
			original := cache.cow.cut
			frozen := len(original.frozen)
			if hold != nil && frozen == 0 {
				t.Fatal("no actually frozen source")
			}
			frontiers, commit, nextLSN := cache.cow.prefix.LogicalFrontiers(), db.State().CommitSeq, db.CommandWALNextLSN()
			for attempt := 1; attempt <= 2; attempt++ {
				old := cache.cow.cut
				oldBasis := old.basis
				if err := cache.RunBackendMaintenance(nil); err != nil {
					t.Fatalf("basis refresh %d: %v", attempt, err)
				}
				installed := cache.cow.cut
				if installed == old || installed.basis == oldBasis || len(installed.frozen) != frozen || cap(installed.frozen) != frozen ||
					cap(installed.sources) != len(installed.shards)+frozen || cache.cow.refreshRequired.Load() {
					t.Fatalf("refresh %d did not install an exact-size successor: frozen length=%d capacity=%d source capacity=%d", attempt, len(installed.frozen), cap(installed.frozen), cap(installed.sources))
				}
				if cache.cow.prefix.LogicalFrontiers() != frontiers || db.State().CommitSeq != commit || db.CommandWALNextLSN() != nextLSN {
					t.Fatalf("refresh %d changed logical authority", attempt)
				}
				if hold != nil {
					if installed.frozen[0].root != original.frozen[0].root {
						t.Fatalf("refresh %d replaced the original frozen root", attempt)
					}
					if got, err := hold.Get(key); err != nil || string(got) != string(value) {
						t.Fatalf("held frozen source after refresh %d: %q, %v", attempt, got, err)
					}
					if got, err := cache.Get(key); err != nil || string(got) != string(value) {
						t.Fatalf("current frozen source after refresh %d: %q, %v", attempt, got, err)
					}
				}
			}
			if backend.captures.Load() != 2 {
				t.Fatalf("expected two actual backend captures, got %d", backend.captures.Load())
			}
			if hold != nil {
				if err := hold.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := cache.Close(); err != nil {
				t.Fatal(err)
			}
			closedStats := budget.Stats()
			closedStats.PeakBytes = 0
			if closedStats != (memtable.COWStats{}) {
				t.Fatalf("basis refreshes left maintenance memory or pins after Close: %+v", closedStats)
			}
		})
	}
}

func TestCOWGenerationBasisRefreshDrainsAdmittedWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		t.Setenv(envDisableVlogGenerationLoop, "1")
		cache, db := nativeBridgeFixture(t, true)
		backend := &generationBasisCaptureBackend{DB: db}
		cache.backend = backend
		if err := cache.beginDirectWrite(); err != nil {
			t.Fatal(err)
		}
		writerAdmitted := true
		cache.cow.refreshRequired.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		done := make(chan struct{})
		var outcome error
		go func() {
			defer close(done)
			outcome = cache.materializeCOWPrefix(ctx, false, nil, cowGenerationPressureMaterialization)
		}()
		defer func() {
			if writerAdmitted {
				cache.writeMu.RUnlock()
			}
			cancel()
			<-done
		}()

		// The exact generation worker reaches its checkpoint fence while the
		// original writer still holds admission. Capture must wait for release.
		synctest.Wait()
		if !cache.checkpointing.Load() || backend.captures.Load() != 0 {
			t.Fatalf("basis capture crossed admitted writer: checkpoint=%t captures=%d", cache.checkpointing.Load(), backend.captures.Load())
		}
		cache.writeMu.RUnlock()
		writerAdmitted = false
		<-done
		if outcome != nil || backend.captures.Load() != 1 || cache.cow.refreshRequired.Load() {
			t.Fatalf("basis refresh did not resume after admitted writer: captures=%d error=%v", backend.captures.Load(), outcome)
		}
		if err := cache.Close(); err != nil {
			t.Fatal(err)
		}
	})
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
