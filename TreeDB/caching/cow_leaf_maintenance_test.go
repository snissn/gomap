package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// This adapter pauses the actual named backend operation immediately after its
// physical publication, before the cache can adopt that basis. Backend copy's
// own writeMu split is separately exercised by its maintained copy-hook tests.
type privateCOWPackHeldBackend struct {
	*backenddb.DB
	published chan backenddb.LeafGenerationPackRunOnceStats
	release   chan struct{}
	failure   error
	calls     int
	gcCalls   int
	last      backenddb.LeafGenerationPackRunOnceStats
}

func (b *privateCOWPackHeldBackend) LeafGenerationPackRunOnceForPrivateCOW(owner *transactionowner.Owner, ctx context.Context, opts backenddb.LeafGenerationPackFromPlanOptions) (backenddb.LeafGenerationPackRunOnceStats, error) {
	b.calls++
	stats, err := b.DB.LeafGenerationPackRunOnceForPrivateCOW(owner, ctx, opts)
	b.last = stats
	if b.published != nil {
		b.published <- stats
		select {
		case <-b.release:
		case <-ctx.Done():
			return stats, errors.Join(err, ctx.Err())
		}
	}
	return stats, errors.Join(err, b.failure)
}

func (b *privateCOWPackHeldBackend) LeafGenerationGC(ctx context.Context, opts backenddb.LeafGenerationGCOptions) (backenddb.LeafGenerationGCStats, error) {
	b.gcCalls++
	return b.DB.LeafGenerationGC(ctx, opts)
}

func privateCOWPackFixture(t *testing.T) (*DB, *privateCOWPackHeldBackend) {
	t.Helper()
	// No background work in the fixture; the actual periodic entrypoint below
	// is enabled only after all fixture setup has finished.
	t.Setenv(envDisableVlogGenerationLoop, "1")
	t.Setenv(envDisableVlogGenerationCheckpointKick, "1")
	t.Setenv(envEnableLeafGenerationPackMaintenance, "0")
	dir := t.TempDir()
	native, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend"), CommandWAL: true, Durability: backenddb.DurabilityWALOnRelaxed, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := native.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	cache, err := Open(dir, native, Options{TransactionOwner: owner, MemtableMode: "cow_ordered", ExternalCommandWAL: true,
		MemtableShards: 2, FlushThreshold: 1 << 30, IndexOuterLeavesInValueLog: true,
		ValueLogCompression: uint8(vlogCompressionOff), ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold), ValueLogGenerationLeafSegmentTargetBytes: 8 << 20})
	if err != nil {
		_ = native.Close()
		t.Fatal(err)
	}
	if err := native.BindNativeSemanticBridge(owner, cache); err != nil {
		t.Fatal(err)
	}
	cache.testSkipVlogCheckpointKick = true
	skipRetainedPrune(cache)
	held := &privateCOWPackHeldBackend{DB: native}
	cache.backend = held
	t.Cleanup(func() {
		if !cache.closing.Load() {
			_ = cache.Close()
		}
	})
	return cache, held
}

func privateCOWPackWrite(t *testing.T, cache *DB, disk *backenddb.DB, round, count int) {
	t.Helper()
	b := cache.NewBatchWithSize(4096)
	defer b.Close()
	for i := 0; i < count; i++ {
		if err := b.Set([]byte(fmt.Sprintf("pack-%05d", i)), bytes.Repeat([]byte{byte(round)}, 128)); err != nil {
			t.Fatal(err)
		}
	}
	// This is the maintained ordinary external-command-WAL public adapter seam,
	// including real revisions, command append, installed receipt and ACK.
	f := &cowPressureRolloverFixture{db: cache, backend: disk}
	if err := f.write(b, true, nil); err != nil {
		t.Fatal(err)
	}
}

func preparePrivateCOWPack(t *testing.T, cache *DB, disk *backenddb.DB) {
	t.Helper()
	privateCOWPackWrite(t, cache, disk, 1, 4096)
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Seal one genuinely partly-live source rather than making every tiny
	// segment wholly live/dead by replacing alternating keys in every leaf.
	cache.leafLog.vlogMu.Lock()
	rotateErr := cache.rotateValueLogMuHeld(&cache.leafLog)
	cache.leafLog.vlogMu.Unlock()
	if rotateErr != nil {
		t.Fatal(rotateErr)
	}
	for round := 2; round <= 4; round++ {
		privateCOWPackWrite(t, cache, disk, round, 2048)
		if err := cache.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envDisableVlogGenerationLoop, "0")
}

func TestPrivateCOWLeafPackPreservesConcurrentWALWritesAndHeldCut(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("actual leaf-pack promotion requires stable relative namespace")
	}
	cache, backend := privateCOWPackFixture(t)
	preparePrivateCOWPack(t, cache, backend.DB)
	held := cache.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing original COW snapshot")
	}
	defer held.Close()
	before := cache.cow.retainCut()
	if before == nil {
		t.Fatal("missing opening cut")
	}
	originalBasis := before.basis
	if err := cache.cow.releaseCut(before); err != nil {
		t.Fatal(err)
	}
	backend.published = make(chan backenddb.LeafGenerationPackRunOnceStats, 1)
	backend.release = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(backend.release) }) }
	done := make(chan error, 1)
	go func() { _, err := cache.runPrivateCOWLeafPackSlice(); done <- err }()
	joined := false
	var writer chan error
	writerJoined := false
	defer func() {
		release()
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("maintenance did not join")
			}
		}
		if writer != nil && !writerJoined {
			select {
			case <-writer:
			case <-time.After(10 * time.Second):
				t.Error("foreground writer did not join")
			}
		}
	}()
	select {
	case stats := <-backend.published:
		if !stats.Ran || stats.Pack.BytesCopied == 0 {
			t.Fatalf("fixture did not physically pack: %+v", stats)
		}
	case err := <-done:
		joined = true
		t.Fatalf("maintenance exited before publication hold: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no real pack publication")
	}
	if cache.maintenanceActive.Load() {
		t.Fatal("private copy acquired full cache admission fence")
	}
	writer = make(chan error, 1)
	go func() {
		b := cache.NewBatchWithSize(1)
		defer b.Close()
		if err := b.Set([]byte("new-during-pack"), []byte("retained")); err != nil {
			writer <- err
			return
		}
		f := &cowPressureRolloverFixture{db: cache, backend: backend.DB}
		writer <- f.write(b, true, nil)
	}()
	select {
	case err := <-writer:
		writerJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ordinary COW/WAL writer blocked by private maintenance")
	}
	release()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("basis adoption did not join")
	}
	current := cache.cow.retainCut()
	if current == nil {
		t.Fatal("missing successor cut")
	}
	if current.basis == originalBasis {
		t.Error("physically changed basis was not adopted")
	}
	if err := cache.cow.releaseCut(current); err != nil {
		t.Fatal(err)
	}
	if got, err := cache.Get([]byte("new-during-pack")); err != nil || string(got) != "retained" {
		t.Fatalf("new source lost: %q %v", got, err)
	}
	if got, err := held.Get([]byte("pack-00000")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{4}, 128)) {
		t.Fatalf("old snapshot changed: %x %v", got, err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, err := backend.DB.Get([]byte("new-during-pack")); err != nil || string(got) != "retained" {
		t.Fatalf("post-pack checkpoint: %q %v", got, err)
	}
}

func TestPrivateCOWLeafPackDefaultsOptOutAndNoProgressBackoff(t *testing.T) {
	cache, backend := privateCOWPackFixture(t)
	t.Setenv(envDisableVlogGenerationLoop, "0")
	if err := os.Unsetenv(envEnableLeafGenerationPackMaintenance); err != nil {
		t.Fatal(err)
	} // restore is already registered by the fixture
	if !cache.privateCOWLeafPackEnabled() {
		t.Fatal("eligible ordinary WAL-on private COW path not enabled")
	}
	for _, mode := range []string{"0", "false"} {
		t.Setenv(envEnableLeafGenerationPackMaintenance, mode)
		if cache.privateCOWLeafPackEnabled() {
			t.Fatalf("explicit opt-out %q ignored", mode)
		}
	}
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	cache.SetMaintenancePhase(MaintenancePhaseRestore)
	if attempted, err := cache.runPrivateCOWLeafPackSlice(); attempted || err != nil || backend.calls != 0 {
		t.Fatalf("restore admitted maintenance: %t %v calls%d", attempted, err, backend.calls)
	}
	cache.SetMaintenancePhase(MaintenancePhaseSteady)
	pause := filepath.Join(t.TempDir(), "pause")
	if err := os.WriteFile(pause, nil, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envVlogGenerationMaintenancePauseFile, pause)
	if attempted, err := cache.runPrivateCOWLeafPackSlice(); attempted || err != nil || backend.calls != 0 {
		t.Fatalf("explicit pause ignored: %t %v", attempted, err)
	}
	if err := os.Remove(pause); err != nil {
		t.Fatal(err)
	}
	if reached := cache.maybeRunPeriodicVlogGenerationMaintenance(false); !reached || backend.calls != 1 {
		t.Fatalf("default periodic route missed named owner: reached%t calls%d", reached, backend.calls)
	}
	if backend.last.Ran || backend.gcCalls != 1 {
		t.Fatalf("no-op planner did not reach actual GC: ran%t GC%d", backend.last.Ran, backend.gcCalls)
	}
	calls := backend.calls
	if attempted, err := cache.runPrivateCOWLeafPackSlice(); attempted || err != nil || backend.calls != calls {
		t.Fatalf("no-progress pass did not back off: %t %v calls%d", attempted, err, backend.calls)
	}
	cache.flushMu.Lock()
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(0)
	attempted, err := cache.runPrivateCOWLeafPackSlice()
	cache.flushMu.Unlock()
	if attempted || err != nil {
		t.Fatalf("background work queued behind flush owner: %t %v", attempted, err)
	}
	t.Setenv(envDisableVlogGenerationLoop, "1")
	if cache.privateCOWLeafPackEnabled() {
		t.Fatal("global loop disable ignored")
	}
}

func TestPrivateCOWLeafPackAcceptedErrorStillAdoptsBasis(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("actual leaf-pack promotion requires stable relative namespace")
	}
	cache, backend := privateCOWPackFixture(t)
	preparePrivateCOWPack(t, cache, backend.DB)
	failure := errors.New("report after actual durable leaf pack")
	backend.failure = failure
	attempted, err := cache.runPrivateCOWLeafPackSlice()
	if !backend.last.Ran || backend.last.Pack.BytesCopied == 0 {
		t.Fatalf("fixture did not physically pack before error: %+v", backend.last)
	}
	if !attempted || !errors.Is(err, failure) || cache.cow.refreshRequired.Load() {
		t.Fatalf("accepted-error handoff: attempted%t err%v refresh%t", attempted, err, cache.cow.refreshRequired.Load())
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, err := cache.Get([]byte("pack-03000")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{1}, 128)) {
		t.Fatalf("old live value lost: %x %v", got, err)
	}
}

// A logical capacity reservation is deliberately sparse: this exercises the
// whole-input admission cliff without allocating or reading four GiB of data.
// The existing live leaf fixture still has to pack and reach its real GC.
func TestPrivateCOWLeafPackLargeInputDoesNotDisableDefaultMaintenance(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("actual leaf-pack promotion requires stable relative namespace")
	}
	cache, backend := privateCOWPackFixture(t)
	preparePrivateCOWPack(t, cache, backend.DB)
	reservation := filepath.Join(backend.DB.Dir(), "capacity-reservation.test")
	f, err := os.OpenFile(reservation, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	truncateErr := f.Truncate((4 << 30) + 1)
	closeErr := f.Close()
	if truncateErr != nil || closeErr != nil {
		t.Fatal(errors.Join(truncateErr, closeErr))
	}
	// Corroborate that this exact input exceeds the old diagnostic-style cap.
	_, err = backend.DB.LeafGenerationPlan(context.Background(), backenddb.LeafGenerationPlanOptions{
		MaintenanceLimits: backenddb.LeafGenerationMaintenanceLimits{NativeEntries: 8192, NativeBytes: 4 << 30, PagerPages: 1 << 20},
	})
	if !errors.Is(err, backenddb.ErrLeafGenerationMaintenanceLimit) {
		t.Fatalf("large-input fixture did not exceed the old admission ceiling: %v", err)
	}
	attempted, err := cache.runPrivateCOWLeafPackSlice()
	if err != nil || !attempted || !backend.last.Ran || backend.last.Pack.BytesCopied == 0 || backend.gcCalls != 1 {
		t.Fatalf("large input disabled real default maintenance: attempted%t ran%t copied%d GC%d err%v", attempted, backend.last.Ran, backend.last.Pack.BytesCopied, backend.gcCalls, err)
	}
	if got, err := cache.Get([]byte("pack-03000")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{1}, 128)) {
		t.Fatalf("large-input maintenance lost live value: %x %v", got, err)
	}
}
