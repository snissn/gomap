package caching

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

func nativeDurableBridgeFixtureWithLimits(t *testing.T, limits memtable.COWLimits) (*DB, *backenddb.DB) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    filepath.Join(dir, "backend"),
		CommandWAL:             true,
		Durability:             backenddb.DurabilityDurable,
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
		DisableWAL:               false,
		AllowUnsafe:              false,
		RelaxedSync:              false,
		ExternalCommandWAL:       true,
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
	cache.SetAutoCheckpointWALBytesHook(func() int64 { return 0 })
	cache.SetCommandWALCheckpointCutoverHook(func() {})
	t.Cleanup(func() {
		if !cache.closing.Load() {
			_ = cache.Close()
		}
	})
	return cache, backend
}

func acquireReservedToTarget(t *testing.T, budget *memtable.COWBudget, target uint64) *memtable.COWExternalLease {
	t.Helper()
	cur := budget.Stats().ReservedBytes
	if cur >= target {
		t.Fatalf("reserved bytes %d already >= target %d", cur, target)
	}
	lease, err := budget.AcquireExternal(target - cur)
	if err != nil {
		t.Fatalf("AcquireExternal(%d): %v", target-cur, err)
	}
	return lease
}

func TestCOWDurableSoftReservationPressureOverlapsForegroundWrite(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 4 << 20
	cache, backend := nativeDurableBridgeFixtureWithLimits(t, limits)

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
	cache.StartAutoCheckpoint(0, 1<<40, 0)
	if !cache.nativePressureDrainEnabled() || cache.relaxedSync {
		t.Fatalf("expected durable size-maintenance worker: enabled=%t relaxed=%t", cache.nativePressureDrainEnabled(), cache.relaxedSync)
	}

	soft := limits.MaxInFlightBytes - limits.MaxInFlightBytes/4
	lease := acquireReservedToTarget(t, cache.cow.budget, soft+64<<10)
	t.Cleanup(lease.Close)

	before := cache.cow.prefix.LogicalFrontiers()
	if err := cache.PrepareNativeProducer(); err != nil {
		t.Fatalf("soft reservation PrepareNativeProducer: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(8 * time.Second):
		t.Fatal("background reservation drain did not start")
	}

	// Foreground writes must still proceed while the older prefix drains.
	if err := cache.Set([]byte("fg"), []byte("during-drain")); err != nil {
		t.Fatalf("concurrent foreground write during background drain: %v", err)
	}
	lease.Close()
	unblock()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	through := rootpublication.SealedPrefix{LogicalSequence: before.V, CanonicalLSN: before.C}
	if err := cache.cow.prefix.WaitAccepted(ctx, through); err != nil {
		t.Fatalf("background reservation drain did not settle: %v", err)
	}
	if got, err := backend.Get([]byte("k1")); err != nil || string(got) != "v1" {
		t.Fatalf("sealed backend k1=%q err=%v", got, err)
	}
	if got, err := cache.Get([]byte("fg")); err != nil || string(got) != "during-drain" {
		t.Fatalf("foreground key fg=%q err=%v", got, err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("checked Close: %v", err)
	}
}

func TestCOWDurableSoftReservationPinnedReaderAvoidsFlushStorm(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 4 << 20
	cache, _ := nativeDurableBridgeFixtureWithLimits(t, limits)

	var drainCalls atomic.Int32
	drained := make(chan struct{}, 8)
	cache.testBeforeCheckpointFrontierDrain = func() {
		drainCalls.Add(1)
		select {
		case drained <- struct{}{}:
		default:
		}
	}
	cache.StartAutoCheckpoint(0, 1<<40, 0)

	soft := limits.MaxInFlightBytes - limits.MaxInFlightBytes/4
	pinned := acquireReservedToTarget(t, cache.cow.budget, soft+64<<10)
	t.Cleanup(pinned.Close)

	if err := cache.Set([]byte("k0"), []byte("v0")); err != nil {
		t.Fatal(err)
	}
	if err := cache.PrepareNativeProducer(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-drained:
	case <-time.After(8 * time.Second):
		t.Fatal("initial soft reservation drain did not run")
	}

	deadline := time.Now().Add(8 * time.Second)
	for cache.cow.reservationDrainFloor.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	floor := cache.cow.reservationDrainFloor.Load()
	if floor <= soft {
		t.Fatalf("expected elevated reservationDrainFloor after pinned drain, got=%d soft=%d", floor, soft)
	}

	// Additional small writes while pinned below the raised floor must not spin.
	for i := 0; i < 8; i++ {
		if err := cache.Set([]byte("small"), []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := cache.PrepareNativeProducer(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if got := drainCalls.Load(); got != 1 {
		t.Fatalf("pinned soft reservation triggered %d drains, want 1", got)
	}

	// Releasing the pinned lease clears the floor on the next producer check.
	pinned.Close()
	if err := cache.PrepareNativeProducer(); err != nil {
		t.Fatal(err)
	}
	if got := cache.cow.reservationDrainFloor.Load(); got != 0 {
		t.Fatalf("expected reservationDrainFloor cleared after pin release, got=%d", got)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("checked Close: %v", err)
	}
}

func TestCOWDurableHardReservationPressureRemainsSynchronous(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 4 << 20
	cache, _ := nativeDurableBridgeFixtureWithLimits(t, limits)

	if err := cache.Set([]byte("k-hard"), []byte("v-hard")); err != nil {
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
	cache.StartAutoCheckpoint(0, 1<<40, 0)

	hard := limits.MaxInFlightBytes - limits.MaxInFlightBytes/8
	lease := acquireReservedToTarget(t, cache.cow.budget, hard+64<<10)
	t.Cleanup(lease.Close)

	done := make(chan error, 1)
	go func() {
		done <- cache.PrepareNativeProducer()
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("hard reservation PrepareNativeProducer returned before drain: %v", err)
	case <-time.After(8 * time.Second):
		t.Fatal("hard reservation drain did not start")
	}
	select {
	case err := <-done:
		t.Fatalf("hard reservation PrepareNativeProducer returned while drain held: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("hard reservation PrepareNativeProducer failed: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("hard reservation PrepareNativeProducer did not finish")
	}
	lease.Close()
	if err := cache.Close(); err != nil {
		t.Fatalf("checked Close: %v", err)
	}
}

func TestCOWContendedSynchronousPressureSkipsRedundantSecondDrain(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 4 << 20
	cache, _ := nativeDurableBridgeFixtureWithLimits(t, limits)

	if err := cache.Set([]byte("k-contend"), []byte("v-contend")); err != nil {
		t.Fatal(err)
	}
	var drainCalls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	cache.testBeforeCheckpointFrontierDrain = func() {
		drainCalls.Add(1)
		enterOnce.Do(func() {
			close(entered)
			<-release
		})
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- cache.materializeCOWPrefix(context.Background(), false, nil, cowSynchronousPressureMaterialization)
	}()
	select {
	case <-entered:
	case err := <-firstDone:
		t.Fatalf("first drain returned early: %v", err)
	case <-time.After(8 * time.Second):
		t.Fatal("first drain did not enter")
	}

	// Second synchronous pressure caller contends on flushMu; once the first
	// drain finishes and pressure is absent, the second must not re-drain.
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- cache.materializeCOWPrefix(context.Background(), false, nil, cowSynchronousPressureMaterialization)
	}()
	time.Sleep(30 * time.Millisecond)
	unblock()

	if err := <-firstDone; err != nil {
		t.Fatalf("first drain error: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second drain error: %v", err)
	}
	if got := drainCalls.Load(); got != 1 {
		t.Fatalf("contended synchronous pressure performed %d drains, want 1", got)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("checked Close: %v", err)
	}
}

func TestCOWDurableSoftReservationBackgroundErrorReachesCheckedClose(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 4 << 20
	cache, backend := nativeDurableBridgeFixtureWithLimits(t, limits)

	if err := cache.Set([]byte("k-err"), []byte("v-err")); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected reservation drain meta write failure")
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
	cache.StartAutoCheckpoint(0, 1<<40, 0)

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
		t.Fatal("background reservation drain failure was not reported")
	}
	lease.Close()
	if err := cache.Close(); !errors.Is(err, injected) {
		t.Fatalf("checked Close=%v, want %v", err, injected)
	}
}

func TestCOWDurableGenerationPressureUsesStandardWALFlush(t *testing.T) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 8 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 4 << 20
	cache, backend := nativeDurableBridgeFixtureWithLimits(t, limits)

	if err := cache.Set([]byte("k-gen"), []byte("v-gen")); err != nil {
		t.Fatal(err)
	}
	if err := cache.rolloverCOWForFlush(); err != nil {
		t.Fatalf("rolloverCOWForFlush: %v", err)
	}
	before := cache.cow.prefix.LogicalFrontiers()
	notified := make(chan error, 1)
	cache.notifyError = func(err error) {
		select {
		case notified <- err:
		default:
		}
	}
	cache.StartAutoCheckpoint(0, 1<<40, 0)
	cache.nativeGenerationFlushCh <- struct{}{}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	through := rootpublication.SealedPrefix{LogicalSequence: before.V, CanonicalLSN: before.C}
	if err := cache.cow.prefix.WaitAccepted(ctx, through); err != nil {
		t.Fatalf("durable generation worker did not settle: %v", err)
	}
	if got, err := backend.Get([]byte("k-gen")); err != nil || string(got) != "v-gen" {
		t.Fatalf("backend k-gen=%q err=%v", got, err)
	}
	select {
	case err := <-notified:
		t.Fatalf("unexpected durable generation worker error: %v", err)
	default:
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("checked Close: %v", err)
	}
}
