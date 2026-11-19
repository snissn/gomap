package caching

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/lifecycle"
)

type generationSnapshotTestBackend struct {
	*backenddb.DB
	once    atomic.Bool
	before  func() error
	done    chan error
	retries chan error
	resume  chan struct{}
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
		err := b.Batch.WriteSync()
		if owner.retries != nil {
			select {
			case owner.retries <- err:
			default:
			}
		}
		return err
	}
	if owner.before != nil {
		if err := owner.before(); err != nil {
			owner.done <- err
			return err
		}
	}
	err := b.Batch.WriteSync()
	owner.done <- err
	<-owner.resume
	return err
}

func TestCOWGenerationWorkerRetriesFinalSnapshotAdmission(t *testing.T) {
	for _, tc := range []struct {
		name          string
		closeTogether bool
		requirePoison bool
		memory        bool
	}{
		{name: "lone-frozen-cut"},
		{name: "concurrent-close", closeTogether: true},
		{name: "snapshot-allocation-pressure", memory: true},
		{name: "snapshot-allocation-pressure-concurrent-close", memory: true, closeTogether: true},
		{name: "capacity-outside-snapshot-receipt", requirePoison: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				owner.before = func() (err error) {
					headroom := budget.Limits().MaxTotalBytes - budget.Stats().TotalBytes
					leaseCell := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
					// Leave space for the smaller dependency build so
					// that the final receipt is the phase that refuses.
					const headroomBeforeBuild = 14 << 10
					if headroom <= leaseCell+headroomBeforeBuild {
						return errors.New("insufficient free maintenance budget before the test reservation")
					}
					pressureBytes = headroom - leaseCell - headroomBeforeBuild
					pressure, err = budget.AcquireExternal(pressureBytes)
					return err
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

			cache.StartAutoCheckpoint(0, 1<<40, 0)
			cache.nativeGenerationFlushCh <- struct{}{}
			defer resume()
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			var refusal error
			select {
			case refusal = <-owner.done:
				if tc.memory {
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
					select {
					case err := <-owner.retries:
						if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "prepare accepted snapshot receipt") {
							t.Fatalf("still charged on retry: %v", err)
						}
					case <-ctx.Done():
						t.Fatal("initial capacity retry did not run")
					}
					quiet := time.NewTimer(2 * cowGenerationPressureRetryDelay)
					defer quiet.Stop()
					select {
					case err := <-owner.retries:
						t.Fatalf("unchanged maintenance charge repeated physical build: %v", err)
					case <-quiet.C:
					}

					// The first owner refunds part of its real charge and
					// a distinct owner reuses exactly that much capacity.
					// The allocator can still change ownership at equal use.
					budget := cache.cow.maintenance
					beforeSwap := budget.Stats().TotalBytes
					const swapCharge = 1024
					if err := pressure.Resize(pressureBytes - swapCharge); err != nil {
						t.Fatal(err)
					}
					leaseCell := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
					next, err := budget.AcquireExternal(swapCharge - leaseCell)
					if err != nil {
						t.Fatal(err)
					}
					replacement = next
					defer replacement.Close()
					if afterSwap := budget.Stats().TotalBytes; afterSwap != beforeSwap {
						t.Fatalf("owner replacement changed charge: %d -> %d", beforeSwap, afterSwap)
					}
					select {
					case err := <-owner.retries:
						if !errors.Is(err, memtable.ErrCOWCapacity) {
							t.Fatalf("equal-charge owner replacement retry before release: %v", err)
						}
					case <-ctx.Done():
						t.Fatal("equal-charge owner replacement did not wake the exact pending cut")
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
		})
	}
}
