package caching

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

func nativeNoWALFastBridgeFixtureWithLimits(t *testing.T, limits memtable.COWLimits) (*DB, *backenddb.DB) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.OpenForNativeSemanticOwner(backenddb.Options{
		Dir:                    filepath.Join(dir, "backend"),
		CommandWAL:             false,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		ResolvedProfile:        backenddb.ProfileNoWALFast,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	cache, err := Open(dir, backend, Options{
		TransactionOwner:         owner,
		MemtableMode:             "cow_btree",
		COWMemtableLimits:        limits,
		DisableWAL:               true,
		AllowUnsafe:              true,
		RelaxedSync:              true,
		ExternalCommandWAL:       false,
		MemtableShards:           2,
		FlushThreshold:           1 << 30,
		ValueLogPointerThreshold: 1,
		ValueLogCompression:      uint8(vlogCompressionOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	if err := backend.BindNativeSemanticBridge(owner, cache); err != nil {
		_ = cache.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !cache.closing.Load() {
			_ = cache.Close()
		}
	})
	return cache, backend
}

func TestCOWNoWALPressureDrainCapabilityAndLifecycle(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 4 << 20

	t.Run("nowal-cow-starts-drain-worker", func(t *testing.T) {
		cache, _ := nativeNoWALFastBridgeFixtureWithLimits(t, limits)
		if cache.externalCommandWAL {
			t.Fatal("fixture unexpectedly enabled externalCommandWAL")
		}
		if cache.nativePressureDrainEnabled() {
			t.Fatal("nativePressureDrainEnabled unexpectedly true before StartCOWPressureDrain")
		}
		if cache.cowPressureDrainOn.Load() || cache.autoCheckpointOn.Load() {
			t.Fatalf("unexpected background worker state before start: drain=%t auto=%t",
				cache.cowPressureDrainOn.Load(), cache.autoCheckpointOn.Load())
		}

		cache.StartCOWPressureDrain()
		// Second call must be an idempotent no-op.
		cache.StartCOWPressureDrain()

		if !cache.cowPressureDrainOn.Load() {
			t.Fatal("StartCOWPressureDrain did not mark cowPressureDrainOn")
		}
		if cache.autoCheckpointOn.Load() {
			t.Fatal("StartCOWPressureDrain unexpectedly started autoCheckpoint loop")
		}
		if !cache.nativePressureDrainEnabled() {
			t.Fatal("nativePressureDrainEnabled remained false after StartCOWPressureDrain")
		}
		if err := cache.Close(); err != nil {
			t.Fatalf("checked Close: %v", err)
		}
	})

	t.Run("command-wal-ignores-StartCOWPressureDrain", func(t *testing.T) {
		cache, _ := nativeDurableBridgeFixtureWithLimits(t, limits)
		cache.StartCOWPressureDrain()
		if cache.cowPressureDrainOn.Load() {
			t.Fatal("command-WAL DB unexpectedly started NoWAL COW pressure drain worker")
		}
		if cache.nativePressureDrainEnabled() {
			t.Fatal("command-WAL DB enabled pressure drain without StartAutoCheckpoint size worker")
		}
		if err := cache.Close(); err != nil {
			t.Fatalf("checked Close: %v", err)
		}
	})
}

func TestCOWNoWALSoftReservationPressureOverlapsForegroundWrite(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 4 << 20
	cache, backend := nativeNoWALFastBridgeFixtureWithLimits(t, limits)

	if err := cache.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	cache.testBeforeCheckpointFrontierDrain = func() {
		enterOnce.Do(func() {
			close(entered)
			<-release
		})
	}
	cache.StartCOWPressureDrain()
	if !cache.nativePressureDrainEnabled() {
		t.Fatal("expected NoWAL COW pressure drain worker to be enabled")
	}

	soft := limits.MaxInFlightBytes - limits.MaxInFlightBytes/4
	lease := acquireReservedToTarget(t, cache.cow.budget, soft+64<<10)
	t.Cleanup(lease.Close)

	before := cache.cow.prefix.LogicalFrontiers()
	beforeCheckpoints := cache.checkpointRuns.Load()
	if err := cache.PrepareNativeProducer(); err != nil {
		t.Fatalf("soft reservation PrepareNativeProducer: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(8 * time.Second):
		t.Fatal("NoWAL background reservation drain did not start")
	}

	// Foreground writes must still proceed while the older prefix drains.
	if err := cache.Set([]byte("fg"), []byte("during-nowal-drain")); err != nil {
		t.Fatalf("concurrent foreground write during NoWAL background drain: %v", err)
	}
	lease.Close()
	unblock()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	through := rootpublication.SealedPrefix{LogicalSequence: before.V, CanonicalLSN: before.C}
	if err := cache.cow.prefix.WaitAccepted(ctx, through); err != nil {
		t.Fatalf("NoWAL background reservation drain did not settle: %v", err)
	}
	if got := cache.checkpointRuns.Load() - beforeCheckpoints; got != 0 {
		t.Fatalf("NoWAL background pressure drain ran %d full checkpoints, want 0", got)
	}
	if got, err := backend.Get([]byte("k1")); err != nil || string(got) != "v1" {
		t.Fatalf("sealed backend k1=%q err=%v", got, err)
	}
	if got, err := cache.Get([]byte("fg")); err != nil || string(got) != "during-nowal-drain" {
		t.Fatalf("foreground key fg=%q err=%v", got, err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("checked Close: %v", err)
	}
}

func TestCOWNoWALGenerationWorkerRetriesLoneFrozenCutAfterAdmissionResumes(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 4 << 20
	cache, backend := nativeNoWALFastBridgeFixtureWithLimits(t, limits)

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
	beforeCheckpoints := cache.checkpointRuns.Load()
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
			t.Fatal("the NoWAL pressure worker did not attempt the frozen prefix")
		}
	}
	cache.StartCOWPressureDrain()
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

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	through := rootpublication.SealedPrefix{LogicalSequence: before.V, CanonicalLSN: before.C}
	if err := prefix.WaitAccepted(ctx, through); err != nil {
		t.Fatalf("lone NoWAL pending frozen prefix did not recover: %v", err)
	}
	reached()
	if got := cache.checkpointRuns.Load() - beforeCheckpoints; got != 0 {
		t.Fatalf("NoWAL generation worker ran %d full checkpoints, want 0", got)
	}
	if got, err := old.Get([]byte("key")); err != nil || string(got) != "covered" {
		t.Fatalf("held original cut=%q, %v", got, err)
	}
	if got, err := backend.Get([]byte("key")); err != nil || string(got) != "covered" {
		t.Fatalf("recovered backend=%q, %v", got, err)
	}
	if got := backend.State().CommitSeq; got != backendSeq+1 {
		t.Fatalf("generation published %d roots, want one", got-backendSeq)
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

func TestCOWNoWALSoftReservationBackgroundErrorReachesCheckedClose(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 4 << 20
	cache, backend := nativeNoWALFastBridgeFixtureWithLimits(t, limits)

	if err := cache.Set([]byte("k-err"), []byte("v-err")); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected nowal reservation drain meta write failure")
	var failOnce sync.Once
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root == backend.Dir() && event.Resource == durabilitycut.ResourceMeta && event.Point == durabilitycut.BeforeMetaWrite {
			var err error
			failOnce.Do(func() { err = injected })
			return err
		}
		return nil
	})
	defer restore()

	notified := make(chan error, 1)
	cache.notifyError = func(err error) {
		select {
		case notified <- err:
		default:
		}
	}
	cache.StartCOWPressureDrain()

	soft := limits.MaxInFlightBytes - limits.MaxInFlightBytes/4
	lease := acquireReservedToTarget(t, cache.cow.budget, soft+64<<10)
	t.Cleanup(lease.Close)

	if err := cache.PrepareNativeProducer(); err != nil {
		t.Fatalf("PrepareNativeProducer: %v", err)
	}
	select {
	case err := <-notified:
		if !errors.Is(err, injected) {
			t.Fatalf("notifyError=%v, want %v", err, injected)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("NoWAL background reservation drain failure was not reported")
	}
	lease.Close()
	if err := cache.Close(); !errors.Is(err, injected) {
		t.Fatalf("checked Close=%v, want %v", err, injected)
	}
}

func TestCOWNoWALRootCustodyCoalescesAcceptedPrefixWithoutCheckpoints(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 16 << 20
	limits.MaxTotalBytes = 64 << 20
	limits.MaxRetiredBytes = 64 << 20
	limits.MaxGenerationBytes = 8 << 20
	cache, backend := nativeNoWALFastBridgeFixtureWithLimits(t, limits)
	cache.StartCOWPressureDrain()

	beforeCheckpoints := cache.checkpointRuns.Load()
	const writes = 64
	for i := 0; i < writes; i++ {
		opening, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		ledger := &cowRootCustodyTestLedger{
			onFinish: func() error { return nil },
		}
		lease, err := cache.cow.budget.AcquireExternal(4 << 10)
		if err != nil {
			_ = opening.Close()
			t.Fatal(err)
		}
		ledger.lease, ledger.record.Work = lease, ledger
		pub := &backenddb.NativePreparedPublication{
			Basis:                  opening.State(),
			StartSequence:          opening.LogicalSequence(),
			Custody:                ledger,
			Reprepare:              &nativeTxnTestPlan{state: nativeBridgeState(t, opening)},
			ReturnCustodyOnInstall: true,
		}
		result, writeErr := backend.WriteNativePublication(pub, nil, false)
		closeErr := opening.Close()
		if writeErr != nil || closeErr != nil || !result.LogicalInstalled {
			t.Fatalf("iter %d: result=%+v write=%v close=%v", i, result, writeErr, closeErr)
		}
		if ledger.detach != 1 {
			t.Fatalf("iter %d: NoWAL root-covered publication did not return custody on install (detach=%d)", i, ledger.detach)
		}
	}
	if err := cache.joinSemanticCleanupFrontier(); err != nil {
		t.Fatal(err)
	}
	if got := cache.cow.prefix.AcceptedPendingCount(); got > 1 {
		t.Fatalf("NoWAL root-covered publications retained %d accepted FIFO slots, want <= 1", got)
	}
	if got := cache.checkpointRuns.Load() - beforeCheckpoints; got != 0 {
		t.Fatalf("NoWAL root-covered publications triggered %d checkpoints, want 0", got)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("checked Close: %v", err)
	}
	if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("closed NoWAL cache retained charges: %+v", stats)
	}
}
