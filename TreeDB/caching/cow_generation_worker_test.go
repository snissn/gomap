package caching

import (
	"context"
	"errors"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCOWGenerationWorkerRetriesLoneFrozenCutAfterAdmissionResumes(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	if err := cache.Set([]byte("key"), []byte("covered")); err != nil {
		t.Fatal(err)
	}
	if err := cache.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	old := cache.AcquireSnapshot()
	if old == nil {
		t.Fatal("fixture could not hold the original cut")
	}
	t.Cleanup(func() { _ = old.Close() })

	prefix := cache.cow.prefix
	before := prefix.LogicalFrontiers()
	backendSeq := backend.State().CommitSeq
	attempts := make(chan struct{}, 4)
	cache.testBeforeCheckpointFrontierDrain = func() {
		select {
		case attempts <- struct{}{}:
		default:
		}
	}
	notified := make(chan error, 1)
	cache.notifyError = func(err error) {
		select {
		case notified <- err:
		default:
		}
	}
	budget := cache.cow.maintenance
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxTotalBytes - budget.Stats().TotalBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pressure.Close)
	reached := func() {
		t.Helper()
		select {
		case <-attempts:
		case <-time.After(8 * time.Second):
			t.Fatal("the original worker did not attempt the frozen prefix")
		}
	}
	cache.StartAutoCheckpoint(0, 1<<40, 0)
	cache.nativeGenerationFlushCh <- struct{}{}
	reached()
	func() {
		cache.flushMu.Lock()
		defer cache.flushMu.Unlock()
		if got := backend.State().CommitSeq; got != backendSeq {
			t.Fatalf("capacity refusal advanced backend from %d to %d", backendSeq, got)
		}
		if got := prefix.LogicalFrontiers(); got != before {
			t.Fatalf("capacity refusal changed frontiers: %+v -> %+v", before, got)
		}
		pressure.Close()
	}()

	// Leave the target alone. With interval and idle disabled, progress must
	// come from the existing worker's deferred attempt and exact accepted cut.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	through := rootpublication.SealedPrefix{LogicalSequence: before.V, CanonicalLSN: before.C}
	if err := prefix.WaitAccepted(ctx, through); err != nil {
		t.Fatalf("the lone pending frozen prefix did not recover: %v", err)
	}
	reached()
	if got, err := old.Get([]byte("key")); err != nil || string(got) != "covered" {
		t.Fatalf("held original cut=%q, %v", got, err)
	}
	if got, err := backend.Get([]byte("key")); err != nil || string(got) != "covered" {
		t.Fatalf("recovered backend=%q, %v", got, err)
	}
	if got := backend.State().CommitSeq; got != backendSeq+1 {
		t.Fatalf("generation published %d roots, want one", got-backendSeq)
	}
	if got := prefix.LogicalFrontiers(); got.S != through || got.V != before.V || got.C != before.C || got.W != before.W || prefix.AcceptedPendingCount() != 0 {
		t.Fatalf("retry did not retire the original endpoint: %+v", got)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("checked Close after recovered admission: %v", err)
	}
	select {
	case err := <-notified:
		t.Fatalf("temporary refusal reported a background failure: %v", err)
	default:
	}
}

func TestCOWGenerationRetryOnlyForProvenRefusalWithoutCleanupFailure(t *testing.T) {
	cleanup := errors.New("original failed finalizer")
	refused := cowGenerationAdmissionError(true, memtable.ErrCOWCapacity)
	for _, tt := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"budget admission", refused, true},
		{"backend group admission", cowGenerationAdmissionError(true, errors.Join(backenddb.ErrSnapshotCapacity)), true},
		{"pre-admission cleanup failure", cowGenerationAdmissionError(true, errors.Join(memtable.ErrCOWCapacity, cleanup)), false},
		{"post-admission cleanup failure", joinCOWCleanupError(refused, cleanup), false},
		{"post-construction capacity failure", memtable.ErrCOWCapacity, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := cowGenerationRetryableOnly(tt.err); got != tt.retry {
				t.Fatalf("retry=%v, want %v for %v", got, tt.retry, tt.err)
			}
		})
	}
}
