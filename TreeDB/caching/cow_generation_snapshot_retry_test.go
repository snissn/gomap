package caching

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/lifecycle"
)

func TestCOWGenerationBasisRefreshDistinguishesFixedAndTemporaryLimits(t *testing.T) {
	t.Run("fixed-value-log-shape", func(t *testing.T) {
		limits := memtable.DefaultCOWLimits()
		limits.MaxResources = 8
		cache, db := nativeBridgeFixtureWithLimits(t, true, limits)
		before, seq, nextLSN := cache.cow.prefix.LogicalFrontiers(), db.State().CommitSeq, db.CommandWALNextLSN()
		valueDir := backenddb.ValueLogDirPath(db.Dir())
		// Equivalent backend maintenance can change the retained file set
		// without advancing either the captured roots or the WAL frontier.
		err := cache.RunBackendMaintenance(func() error {
			for i := 0; i <= limits.MaxResources; i++ {
				const lane = 0
				seq := uint32(100 + i)
				id, err := valuelog.EncodeFileID(lane, seq)
				if err != nil {
					return err
				}
				path := filepath.Join(valueDir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
				writer, err := valuelog.NewWriter(path, id)
				if err != nil {
					return err
				}
				_, err = writer.Append(0, nil, uint64(i+1), []byte("retained"))
				if err = errors.Join(err, writer.Close()); err != nil {
					return err
				}
				if err := db.RegisterValueLogSegment(path, id); err != nil {
					return err
				}
			}
			return nil
		})
		if !errors.Is(err, memtable.ErrCOWCapacity) || !cache.cow.refreshRequired.Load() {
			t.Fatalf("backend maintenance did not leave the original basis pending: %v", err)
		}
		inspect := errors.New("inspected value-log shape")
		var fileCount int
		snapshot, err := db.AcquireSnapshotWithAllocationAdmission(func(s backenddb.SnapshotAllocationSizes) error {
			fileCount = s.ValueLog.FileCount
			return inspect
		})
		if snapshot != nil || !errors.Is(err, inspect) || fileCount <= limits.MaxResources {
			if snapshot != nil {
				t.Error(snapshot.Close())
			}
			t.Fatalf("actual snapshot admitted shape: snapshot=%p err=%v files=%d limit=%d", snapshot, err, fileCount, limits.MaxResources)
		}
		var reported error
		cache.notifyError = func(err error) { reported = err }
		if retry, allocation := cache.maintainCOWGenerationPressure(); retry || allocation != nil {
			t.Fatalf("generation worker retried a fixed value-log shape: files=%d limit=%d allocation=%v", fileCount, limits.MaxResources, allocation)
		}
		if !errors.Is(reported, memtable.ErrCOWCapacity) || !strings.Contains(reported.Error(), "snapshot value-log shape exceeds COW resource limit") {
			t.Fatalf("worker lost the fixed resource failure: %v", reported)
		}
		if after := cache.cow.prefix.LogicalFrontiers(); after != before || db.State().CommitSeq != seq || db.CommandWALNextLSN() != nextLSN {
			t.Fatalf("fixed shape refusal changed publication authority: %+v -> %+v", before, after)
		}
		if err := cache.Close(); !errors.Is(err, reported) {
			t.Fatalf("checked Close lost the original fixed resource error: %v", err)
		}
	})

	t.Run("temporarily-held-maintenance-bytes", func(t *testing.T) {
		cache, _ := nativeBridgeFixture(t, true)
		budget := cache.cow.maintenance
		overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
		lease, err := budget.AcquireExternal(budget.Limits().MaxTotalBytes - budget.Stats().TotalBytes - overhead)
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Close()
		if err := cache.RunBackendMaintenance(nil); !errors.Is(err, memtable.ErrCOWCapacity) || !cache.cow.refreshRequired.Load() {
			t.Fatalf("backend maintenance did not leave a temporarily refused basis: %v", err)
		}
		if retry, allocation := cache.maintainCOWGenerationPressure(); !retry || allocation != nil {
			t.Fatalf("generation worker lost the temporary retry: retry=%v allocation=%v", retry, allocation)
		}
		lease.Close()
		if retry, allocation := cache.maintainCOWGenerationPressure(); retry || allocation != nil || cache.cow.refreshRequired.Load() {
			t.Fatalf("basis did not refresh after releasing the original bytes: retry=%v allocation=%v", retry, allocation)
		}
		if err := cache.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

type generationSnapshotTestBackend struct {
	*backenddb.DB
	once             atomic.Bool
	before           func() error
	duringRetry      func(func() error) error
	releaseUnclaimed func()
	done             chan error
	retries          chan error
	resume           chan struct{}
}

type generationSnapshotTestBatch struct {
	*backenddb.Batch
	owner *generationSnapshotTestBackend
}

func (b *generationSnapshotTestBackend) NewPhysicalBatchWithSize(size int) batch.Interface {
	return &generationSnapshotTestBatch{Batch: b.DB.NewPhysicalBatchWithSize(size).(*backenddb.Batch), owner: b}
}

func (b *generationSnapshotTestBatch) WriteSync() error {
	owner := b.owner
	if !owner.once.CompareAndSwap(false, true) {
		write := b.Batch.WriteSync
		var err error
		if owner.duringRetry != nil {
			err = owner.duringRetry(write)
		} else {
			err = write()
		}
		if owner.retries != nil {
			select {
			case owner.retries <- err:
			default:
			}
		}
		return err
	}
	var err error
	if owner.before != nil {
		err = owner.before()
	}
	if err == nil {
		err = b.Batch.WriteSync()
	}
	owner.done <- err
	<-owner.resume
	if owner.releaseUnclaimed != nil {
		owner.releaseUnclaimed()
	}
	return err
}

func TestCOWGenerationWorkerRetriesFinalSnapshotAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		closeTogether            bool
		pairedClose              bool
		requirePoison            bool
		memory                   bool
		refundDuringConfirmation bool
	}{
		{name: "lone-frozen-cut"},
		{name: "concurrent-close", closeTogether: true},
		{name: "snapshot-allocation-pressure", memory: true},
		{name: "snapshot-allocation-pressure-refund-during-confirmation", memory: true, refundDuringConfirmation: true},
		{name: "snapshot-allocation-pressure-concurrent-close", memory: true, closeTogether: true},
		{name: "snapshot-allocation-pressure-paired-close", memory: true, closeTogether: true, pairedClose: true},
		{name: "capacity-outside-snapshot-receipt", requirePoison: true},
	} {
		run := func(t *testing.T) {
			if tc.memory && !tc.closeTogether {
				// Only the generation-pressure worker may rescue this cut.
				t.Setenv(envDisableVlogGenerationLoop, "1")
			}
			cache, db := nativeBridgeFixture(t, true)
			// Fill every bounded cohort with distinct, naturally installed
			// backend sequences. The cache's current basis owns the last
			// slot. The build group can join that slot, but its predicted
			// successor cannot register until an older real reader closes.
			var readers []*backenddb.Snapshot
			closeReaders := func() {
				for _, reader := range readers {
					if err := reader.Close(); err != nil {
						t.Error(err)
					}
				}
				readers = nil
			}
			defer closeReaders()
			if !tc.requirePoison && !tc.memory {
				for i := 0; i < lifecycle.FastReaderShardCount-1; i++ {
					reader, err := db.AcquireSnapshotWithAllocationAdmission(func(backenddb.SnapshotAllocationSizes) error { return nil })
					if err != nil {
						t.Fatal(err)
					}
					readers = append(readers, reader)
					if err := cache.Set([]byte("seed"), []byte{byte(i)}); err != nil {
						t.Fatal(err)
					}
					if err := cache.Checkpoint(); err != nil {
						t.Fatalf("seed checkpoint %d: %v", i, err)
					}
				}
			}
			releaseReader := func() {
				t.Helper()
				if err := readers[0].Close(); err != nil {
					t.Fatal(err)
				}
				readers = readers[1:]
			}
			key, value := []byte("key"), []byte("covered")
			b := cache.NewBatch()
			if err := b.AuthorizeCOWAppend(cache.cow.txnOwner); err != nil {
				t.Fatal(err)
			}
			if err := b.Set(key, value); err != nil {
				t.Fatal(err)
			}
			if err := b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error {
				lsn, err := db.AppendRawKVCommandWALOrderedEntryScanWithHintPreparedFinalizedAndMode(b.PrepareExternalCommandWALPublication, b.finalizeCOWPublication, b.Replay, 1, backenddb.RawKVCommandWALAppendRelaxed)
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
			prefix := cache.cow.prefix
			before, beforeSeq, nextLSN := prefix.LogicalFrontiers(), db.State().CommitSeq, db.CommandWALNextLSN()
			if before.C == 0 || before.S.CanonicalLSN >= before.C {
				t.Fatalf("missing an unapplied canonical prefix: %+v", before)
			}

			owner := &generationSnapshotTestBackend{
				DB: db, done: make(chan error, 1), resume: make(chan struct{}),
			}
			if tc.memory && !tc.closeTogether {
				owner.retries = make(chan error, 2)
			}
			var pressure, replacement *memtable.COWExternalLease
			var pressureBytes uint64
			var pressureClaimed bool
			var beforeMaintenanceBytes, beforeRefunds uint64
			var beforeLeases int
			releasePressure := func() {
				if tc.memory {
					pressure.Close()
					replacement.Close()
				} else {
					releaseReader()
				}
			}
			if tc.memory {
				budget := cache.cow.maintenance
				beforeLeases = budget.Stats().ExternalLeases
				reservePressure := func() (*memtable.COWExternalLease, uint64, error) {
					headroom := budget.Limits().MaxTotalBytes - budget.Stats().TotalBytes
					leaseCell := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
					// Leave space for the smaller dependency build so
					// that the final receipt is the phase that refuses.
					const headroomBeforeBuild = 14 << 10
					if headroom <= leaseCell+headroomBeforeBuild {
						return nil, 0, errors.New("insufficient free maintenance budget before the test reservation")
					}
					bytes := headroom - leaseCell - headroomBeforeBuild
					lease, err := budget.AcquireExternal(bytes)
					return lease, bytes, err
				}
				owner.before = func() (err error) {
					pressure, pressureBytes, err = reservePressure()
					return err
				}
				owner.releaseUnclaimed = func() {
					if !pressureClaimed {
						pressure.Close()
					}
				}
				if tc.refundDuringConfirmation {
					var heldOnce bool
					owner.duringRetry = func(write func() error) error {
						if heldOnce {
							return write()
						}
						heldOnce = true
						lease, _, err := reservePressure()
						if err != nil {
							return err
						}
						defer lease.Close()
						return write()
					}
				}
			}
			var once sync.Once
			resume := func() { once.Do(func() { close(owner.resume) }) }
			cache.backend = owner
			reported := make(chan error, 1)
			cache.notifyError = func(err error) {
				select {
				case reported <- err:
				default:
				}
			}
			if tc.requirePoison {
				var inject atomic.Bool
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					// The same sentinel from another pre-publication phase
					// carries no Snapshot admission proof.
					if event.Point == durabilitycut.BeforeAppliedLSNAdvance && event.Resource == durabilitycut.ResourceMeta &&
						event.LSN == before.C && inject.CompareAndSwap(false, true) {
						return memtable.ErrCOWCapacity
					}
					return nil
				})
				defer restore()
			}

			workerDone := make(chan struct{})
			if tc.pairedClose {
				const idle = time.Second
				cache.nativeGenerationSeals.idleInterval.Store(int64(idle))
				through := rootpublication.SealedPrefix{LogicalSequence: before.V, CanonicalLSN: before.C}
				cache.nativeGenerationSeals.add(cowGenerationSealRequest{through: through, deadline: time.Now()})
				cache.wg.Add(1)
				go func() {
					defer close(workerDone)
					cache.autoCheckpointLoop(0, 1<<40, idle)
				}()
			} else {
				cache.StartAutoCheckpoint(0, 1<<40, 0)
				cache.nativeGenerationFlushCh <- struct{}{}
			}
			defer resume()
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			var refusal error
			select {
			case refusal = <-owner.done:
				if tc.memory && !tc.refundDuringConfirmation {
					pressureClaimed = true
					defer pressure.Close()
				}
			case <-ctx.Done():
				t.Fatal("generation worker did not attempt its final physical batch")
			}
			want := backenddb.ErrSnapshotCapacity
			if tc.requirePoison || tc.memory {
				want = memtable.ErrCOWCapacity
			}
			if !errors.Is(refusal, want) {
				t.Fatalf("final batch error = %v, want %v", refusal, want)
			}
			if !tc.requirePoison && !strings.Contains(refusal.Error(), "prepare accepted snapshot receipt") {
				t.Fatalf("final batch did not refuse the actual Snapshot receipt: %v", refusal)
			}
			if got := prefix.LogicalFrontiers(); got != before || db.State().CommitSeq != beforeSeq || db.CommandWALNextLSN() != nextLSN {
				t.Fatalf("capacity refusal changed captured authority: %+v -> %+v", before, got)
			}
			if !tc.requirePoison {
				if err := db.CheckCommandWALPublishReady(); err != nil {
					t.Fatalf("admission failed before an exact cut could enqueue, but poisoned its original WAL: %v", err)
				}
			}

			var confirmationAt time.Time
			switch {
			case tc.requirePoison:
				resume()
				select {
				case err := <-reported:
					if !errors.Is(err, memtable.ErrCOWCapacity) {
						t.Fatalf("original background error = %v", err)
					}
				case <-ctx.Done():
					t.Fatal("capacity without the original receipt was not reported")
				}
				if err := db.CheckCommandWALPublishReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
					t.Fatalf("original WAL poison = %v", err)
				}
				if err := cache.Close(); !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("checked Close lost the first error: %v", err)
				}
				return
			case tc.closeTogether:
				closeAtMeta := make(chan struct{})
				finishClose := make(chan struct{})
				var finishOnce sync.Once
				resumeClose := func() { finishOnce.Do(func() { close(finishClose) }) }
				defer resumeClose()
				if tc.pairedClose {
					var entered atomic.Bool
					restore := durabilitycut.Install(func(event durabilitycut.Event) error {
						if event.Point == durabilitycut.BeforeAppliedLSNAdvance && event.Resource == durabilitycut.ResourceMeta &&
							event.LSN == before.C && entered.CompareAndSwap(false, true) {
							close(closeAtMeta)
							<-finishClose
						}
						return nil
					})
					defer restore()
				}
				done := make(chan error, 1)
				go func() { done <- cache.Close() }()
				for !cache.closing.Load() {
					select {
					case <-ctx.Done():
						t.Fatal("Close did not reach the original write fence")
					case <-time.After(time.Millisecond):
					}
				}
				releasePressure()
				resume()
				if tc.pairedClose {
					select {
					case <-closeAtMeta:
					case err := <-done:
						t.Fatalf("Close finished before reaching the held publication: %v", err)
					case <-ctx.Done():
						t.Fatal("Close did not reach the original META advance")
					}
					select {
					case <-workerDone:
					case <-time.After(time.Second):
						t.Fatal("generation worker did not yield the refused request to held Close")
					}
					resumeClose()
				}
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("checked concurrent Close = %v", err)
					}
				case <-ctx.Done():
					t.Fatal("Close did not drain the exact frozen cut")
				}
			default:
				if tc.memory {
					resume()
					if tc.refundDuringConfirmation {
						if err := lockCheckpointMutexContext(ctx, &cache.flushMu); err != nil {
							t.Fatalf("first capacity refusal did not finish: %v", err)
						}
						budget := cache.cow.maintenance
						beforeMaintenanceBytes, beforeRefunds = budget.MaintenanceUsage()
						leases := budget.Stats().ExternalLeases
						cache.flushMu.Unlock()
						if leases != beforeLeases {
							t.Fatalf("unclaimed first lease was not refunded: leases %d -> %d", beforeLeases, leases)
						}
					}
					select {
					case err := <-owner.retries:
						confirmationAt = time.Now()
						if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "prepare accepted snapshot receipt") {
							t.Fatalf("still charged on retry: %v", err)
						}
					case <-ctx.Done():
						t.Fatal("initial capacity retry did not run")
					}
					// WriteSync reports before the worker unwinds and samples
					// maintenance usage. The same flush fence covers that sample.
					if err := lockCheckpointMutexContext(ctx, &cache.flushMu); err != nil {
						t.Fatalf("initial capacity retry did not finish: %v", err)
					}
					if tc.refundDuringConfirmation {
						budget := cache.cow.maintenance
						bytes, refunds := budget.MaintenanceUsage()
						leases := budget.Stats().ExternalLeases
						cache.flushMu.Unlock()
						if bytes != beforeMaintenanceBytes || leases != beforeLeases || refunds <= beforeRefunds {
							t.Fatalf("confirmation did not refund to the original charge and ownership: bytes %d -> %d, leases %d -> %d, refunds %d -> %d", beforeMaintenanceBytes, bytes, beforeLeases, leases, beforeRefunds, refunds)
						}
					} else {
						cache.flushMu.Unlock()
					}
				}
				if tc.memory && !tc.refundDuringConfirmation {
					quiet := time.NewTimer(2 * cowGenerationPressureRetryDelay)
					defer quiet.Stop()
					select {
					case err := <-owner.retries:
						t.Fatalf("unchanged maintenance charge repeated physical build: %v", err)
					case <-quiet.C:
					}

					// The first owner refunds part of its real charge and
					// a distinct owner reuses exactly that much capacity.
					// Fence the transfer so the worker sees the equal charge
					// and can only detect the refund sequence changing.
					budget := cache.cow.maintenance
					defer func() { replacement.Close() }()
					func() {
						if err := lockCheckpointMutexContext(ctx, &cache.flushMu); err != nil {
							t.Fatal(err)
						}
						defer cache.flushMu.Unlock()
						beforeSwap, beforeRefund := budget.MaintenanceUsage()
						const swapCharge = 1024
						if err := pressure.Resize(pressureBytes - swapCharge); err != nil {
							t.Fatal(err)
						}
						leaseCell := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
						var err error
						replacement, err = budget.AcquireExternal(swapCharge - leaseCell)
						if err != nil {
							t.Fatal(err)
						}
						if afterSwap, afterRefund := budget.MaintenanceUsage(); afterSwap != beforeSwap || afterRefund <= beforeRefund {
							t.Fatalf("owner replacement changed charge or lost refund: bytes %d -> %d, refunds %d -> %d", beforeSwap, afterSwap, beforeRefund, afterRefund)
						}
					}()
					refundCtx, cancelRefund := context.WithDeadline(ctx, confirmationAt.Add(cowGenerationQuietRetryFloor-cowGenerationPressureRetryDelay/2))
					defer cancelRefund()
					select {
					case err := <-owner.retries:
						if !errors.Is(err, memtable.ErrCOWCapacity) {
							t.Fatalf("equal-charge owner replacement retry before release: %v", err)
						}
					case <-refundCtx.Done():
						t.Fatal("equal-charge owner replacement did not wake the exact pending cut before quiet retry")
					}
				}
				releasePressure()
				resume()
				through := rootpublication.SealedPrefix{LogicalSequence: before.V, CanonicalLSN: before.C}
				if err := prefix.WaitAccepted(ctx, through); err != nil {
					t.Fatalf("lone frozen cut did not retry: %v", err)
				}
				after := prefix.LogicalFrontiers()
				pending := prefix.AcceptedPendingCount()
				if after.S != through || after.C != before.C || after.V != before.V || after.W < before.W || after.W > before.C || pending != 0 {
					t.Fatalf("retry retired the wrong range: before=%+v after=%+v pending=%d", before, after, pending)
				}
				if got, err := db.Get(key); err != nil || string(got) != string(value) {
					t.Fatalf("logical read after deferred physical publication = %q, %v", got, err)
				}
				if seq := db.State().CommitSeq; seq != beforeSeq+1 {
					t.Fatalf("physical generation published %d times", seq-beforeSeq)
				}
				if err := cache.Close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-reported:
				t.Fatalf("temporary Snapshot refusal was reported: %v", err)
			default:
			}

			closeReaders()
			reopened, err := backenddb.Open(backenddb.Options{Dir: db.Dir(), CommandWAL: true, Durability: backenddb.DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			if got, err := reopened.Get(key); err != nil || string(got) != string(value) {
				t.Fatalf("reopened exact prefix = %q, %v", got, err)
			}
			if token, ok := reopened.StateToken(); !ok || token.AppliedCommandLSN != before.C || reopened.CommandWALNextLSN() != nextLSN {
				t.Fatalf("reopened WAL coverage = %+v, next = %d, want applied %d and next %d", token, reopened.CommandWALNextLSN(), before.C, nextLSN)
			}
		}
		if tc.memory && !tc.closeTogether && !tc.refundDuringConfirmation {
			// Real worker timers share this clock. Once the quiet check wakes
			// the test, time cannot advance to a later quiet retry before
			// the runnable test has transferred the equal-charge lease.
			t.Run(tc.name, func(t *testing.T) { synctest.Test(t, run) })
		} else {
			t.Run(tc.name, run)
		}
	}
}
