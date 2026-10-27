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
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// This adapter pauses the actual named backend operation immediately after its
// physical publication, before the cache can adopt that basis. Backend copy's
// own writeMu split is separately exercised by its maintained copy-hook tests.
type privateCOWPackHeldBackend struct {
	*backenddb.DB
	published       chan backenddb.LeafGenerationPackRunOnceStats
	release         chan struct{}
	failure         error
	fallbackFailure error
	calls           int
	gcCalls         int
	last            backenddb.LeafGenerationPackRunOnceStats
	gcCompleted     chan backenddb.LeafGenerationGCStats
	forceNoop       bool
	hideRanOnError  bool
}

func (b *privateCOWPackHeldBackend) LeafGenerationPackRunOnceForPrivateCOW(owner *transactionowner.Owner, ctx context.Context, opts backenddb.LeafGenerationPackFromPlanOptions) (backenddb.LeafGenerationPackRunOnceStats, error) {
	b.calls++
	if b.forceNoop {
		b.last = backenddb.LeafGenerationPackRunOnceStats{SkipReason: "test: no remaining pack candidate"}
		return b.last, nil
	}
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
	err = errors.Join(err, b.failure)
	if err != nil && b.hideRanOnError {
		// The real RunOnce wrapper can return before assigning Ran when the
		// selected pack accepted a root and then reported a finalization error.
		stats.Ran = false
	}
	return stats, err
}

func (b *privateCOWPackHeldBackend) CommandWALCheckpointFallbackNeedsRefresh(basis backenddb.StateToken) (bool, error) {
	if b.fallbackFailure != nil {
		return false, b.fallbackFailure
	}
	return b.DB.CommandWALCheckpointFallbackNeedsRefresh(basis)
}

func (b *privateCOWPackHeldBackend) LeafGenerationGC(ctx context.Context, opts backenddb.LeafGenerationGCOptions) (backenddb.LeafGenerationGCStats, error) {
	b.gcCalls++
	stats, err := b.DB.LeafGenerationGC(ctx, opts)
	if b.gcCompleted != nil {
		b.gcCompleted <- stats
	}
	return stats, err
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
	for _, frozen := range []bool{false, true} {
		t.Run(fmt.Sprintf("frozen=%t", frozen), func(t *testing.T) {
			testPrivateCOWLeafPackPreservesConcurrentWALWritesAndHeldCut(t, frozen)
		})
	}
}

func testPrivateCOWLeafPackPreservesConcurrentWALWritesAndHeldCut(t *testing.T, frozen bool) {
	requireLeafGenerationPackPromotionSupport(t)
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
	originalLogical := before.basis.logicalSeq
	originalState, _ := before.basis.snapshot.StateToken()
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
	if frozen {
		if err := cache.rolloverCOWForFlush(); err != nil {
			t.Fatal(err)
		}
	}
	nextLSN := backend.DB.CommandWALNextLSN()
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
	if current.basis.logicalSeq != originalLogical {
		t.Errorf("physical adoption relabeled logical basis: got %d want %d", current.basis.logicalSeq, originalLogical)
	}
	if frozen && len(current.frozen) == 0 {
		t.Error("physical maintenance consumed frozen suffix")
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
	state, live := backend.DB.StateToken()
	if !live || state.AppliedCommandLSN != originalState.AppliedCommandLSN || state.MaxEntryRevision != originalState.MaxEntryRevision || backend.DB.CommandWALNextLSN() != nextLSN {
		t.Fatalf("physical maintenance changed logical endpoint: %+v original=%+v next=%d want=%d", state, originalState, backend.DB.CommandWALNextLSN(), nextLSN)
	}
	needed, err := backend.DB.CommandWALCheckpointFallbackNeedsRefresh(state)
	if err != nil || needed != frozen {
		t.Fatalf("post-pack fallback needs refresh=%t err=%v want=%t before another checkpoint", needed, err, frozen)
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
	requireLeafGenerationPackPromotionSupport(t)
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
	cut := cache.cow.retainCut()
	if cut == nil {
		t.Fatal("accepted-error handoff lost current cut")
	}
	adopted, live := cut.basis.snapshot.StateToken()
	actual, actualLive := backend.DB.StateToken()
	if !live || !actualLive || adopted != actual {
		t.Errorf("accepted-error handoff did not install exact backend Snapshot: adopted=%+v actual=%+v", adopted, actual)
	}
	if err := cache.cow.releaseCut(cut); err != nil {
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
	requireLeafGenerationPackPromotionSupport(t)
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

// Exercise the real automatic slice rather than a public explicit Pack wrapper:
// those wrappers checkpoint first and can hide fallback-only source retention.
func TestPrivateCOWLeafPackConvergesBeforeGCAndUnlinksAfterReaderRelease(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%t", closed), func(t *testing.T) {
			testPrivateCOWLeafPackSourceRetirement(t, closed)
		})
	}
}

func testPrivateCOWLeafPackSourceRetirement(t *testing.T, closed bool) {
	requireLeafGenerationPackPromotionSupport(t)
	cache, backend := privateCOWPackFixture(t)
	preparePrivateCOWPack(t, cache, backend.DB)
	// Keep the already-closed source control as well as the actual active
	// source workload. Production must close only the selected active sources.
	if closed {
		for _, lane := range cache.leafLogAppendLanesSnapshot() {
			lane.vlogMu.Lock()
			err := cache.rotateValueLogMuHeld(lane)
			lane.vlogMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	held := cache.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing retained public reader")
	}
	defer func() {
		if held != nil {
			_ = held.Close()
		}
	}()
	plan, err := backend.DB.LeafGenerationPlan(context.Background(), backenddb.LeafGenerationPlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cache.leafLog.vlogMu.Lock()
	leafDir := filepath.Dir(cache.leafLog.vlogPath)
	cache.leafLog.vlogMu.Unlock()
	currentWriterPaths := make(map[string]*lane)
	rotations := make(map[*lane]uint64)
	for _, lane := range cache.leafLogAppendLanesSnapshot() {
		lane.vlogMu.Lock()
		currentWriterPaths[lane.vlogPath] = lane
		rotations[lane] = lane.vlogRotateTotal.Load()
		lane.vlogMu.Unlock()
	}
	type sourceFile struct {
		path string
		info os.FileInfo
	}
	sources := make(map[uint64][]sourceFile)
	for _, gen := range plan.Generations {
		for _, id := range gen.FileIDs {
			path := valuelog.SegmentPath(leafDir, id)
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			sources[gen.GenerationID] = append(sources[gen.GenerationID], sourceFile{path, info})
		}
	}
	before, live := backend.DB.StateToken()
	next, checkpoints := backend.DB.CommandWALNextLSN(), cache.checkpointRuns.Load()
	if !live {
		t.Fatal("missing initial backend state")
	}
	if attempted, err := cache.runPrivateCOWLeafPackSlice(); err != nil || !attempted || !backend.last.Ran {
		t.Fatalf("automatic pack: attempted=%t stats=%+v err=%v", attempted, backend.last, err)
	}
	state, live := backend.DB.StateToken()
	needed, err := backend.DB.CommandWALCheckpointFallbackNeedsRefresh(state)
	if !live || err != nil || needed || state.AppliedCommandLSN != before.AppliedCommandLSN || state.MaxEntryRevision != before.MaxEntryRevision || backend.DB.CommandWALNextLSN() != next || cache.checkpointRuns.Load() != checkpoints {
		t.Fatalf("pack did not converge original physical cut without checkpoint/append: before=%+v after=%+v needed=%t err=%v", before, state, needed, err)
	}
	var selected []sourceFile
	for _, id := range backend.last.Selection.GenerationIDs {
		files := sources[id]
		if len(files) == 0 {
			t.Fatalf("missing original file identity for selected generation %d", id)
		}
		selected = append(selected, files...)
	}
	if len(selected) == 0 {
		t.Fatal("no real packed source")
	}
	selectedPaths := make(map[string]bool)
	activeSelected, unselected := 0, 0
	for _, source := range selected {
		selectedPaths[source.path] = true
	}
	for path, lane := range currentWriterPaths {
		lane.vlogMu.Lock()
		currentPath, rotation := lane.vlogPath, lane.vlogRotateTotal.Load()
		lane.vlogMu.Unlock()
		if selectedPaths[path] {
			activeSelected++
			if currentPath == path || rotation != rotations[lane]+1 {
				t.Fatalf("selected active source not handed off once: %s -> %s rotations=%d -> %d", path, currentPath, rotations[lane], rotation)
			}
		} else {
			unselected++
			if currentPath != path || rotation != rotations[lane] {
				t.Fatalf("unselected lane changed: %s -> %s rotations=%d -> %d", path, currentPath, rotations[lane], rotation)
			}
		}
	}
	if !closed && (activeSelected == 0 || unselected == 0) {
		t.Fatalf("fixture must include selected and unrelated active writers: selected=%d unrelated=%d", activeSelected, unselected)
	}
	for _, source := range selected {
		info, err := os.Stat(source.path)
		if err != nil || !os.SameFile(source.info, info) {
			t.Fatalf("held reader lost original source inode %s: %v", source.path, err)
		}
	}
	if got, err := held.Get([]byte("pack-03000")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{1}, 128)) {
		t.Fatalf("retained source read: %x %v", got, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	var gcStats backenddb.LeafGenerationGCStats
	// This is the maintained GC/fence only, with skipCheckpoint=true. It does
	// not advance logical coverage or invoke explicit fallback convergence.
	if err := cache.RunBackendMaintenance(func() error {
		var err error
		gcStats, _, err = cache.runVlogGenerationLeafGC(context.Background())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if backend.DB.CommandWALNextLSN() != next || cache.checkpointRuns.Load() != checkpoints {
		t.Fatal("reader-release GC appended or checkpointed")
	}
	// Retirement can precede unlink: the collector's own pre-retirement
	// Snapshot releases after the first pass. A second normal GC reports and
	// prunes that deletion; neither pass checkpoints or republishes fallback.
	t.Logf("first reader-release GC: %+v", gcStats)
	if err := cache.RunBackendMaintenance(func() error {
		var err error
		gcStats, _, err = cache.runVlogGenerationLeafGC(context.Background())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if backend.DB.CommandWALNextLSN() != next || cache.checkpointRuns.Load() != checkpoints {
		t.Fatal("second reader-release GC appended or checkpointed")
	}
	for _, source := range selected {
		if _, err := os.Stat(source.path); !os.IsNotExist(err) {
			t.Fatalf("packed source inode still named after reader release and maintained GC: %s err=%v GC=%+v", source.path, err, gcStats)
		}
	}
	if gcStats.FilesDeleted == 0 {
		t.Fatalf("no actual deletion reported: %+v", gcStats)
	}
	t.Logf("selected closed source files=%d; post-release GC=%+v", len(selected), gcStats)
	// Reopen only after the measured unlink assertions, so Close cannot mask
	// failure to reclaim at the normal maintenance boundary.
	dir := backend.DB.Dir()
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, Durability: backenddb.DurabilityWALOnRelaxed, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, key := range []string{"pack-00000", "pack-03000"} {
		want := byte(4)
		if key == "pack-03000" {
			want = 1
		}
		if got, err := reopened.Get([]byte(key)); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{want}, 128)) {
			t.Fatalf("reopened %s: %x %v", key, got, err)
		}
	}
}

func TestPrivateCOWLeafPackFallbackFailureDoesNotCollect(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	cache, backend := privateCOWPackFixture(t)
	preparePrivateCOWPack(t, cache, backend.DB)
	held := cache.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing original reader")
	}
	defer held.Close()
	next := backend.DB.CommandWALNextLSN()
	failure := errors.New("fallback custody refused after accepted pack")
	backend.fallbackFailure = failure
	attempted, err := cache.runPrivateCOWLeafPackSlice()
	if !attempted || !backend.last.Ran || !errors.Is(err, failure) || backend.gcCalls != 0 || backend.DB.CommandWALNextLSN() != next {
		t.Fatalf("fallback refusal did not stop collection: attempted=%t pack=%+v GC=%d next=%d err=%v", attempted, backend.last, backend.gcCalls, backend.DB.CommandWALNextLSN(), err)
	}
	cut := cache.cow.retainCut()
	if cut == nil {
		t.Fatal("missing reconciled pack basis")
	}
	adopted, live := cut.basis.snapshot.StateToken()
	actual, actualLive := backend.DB.StateToken()
	if !live || !actualLive || adopted != actual {
		t.Errorf("fallback refusal lost adopted exact Snapshot: adopted=%+v actual=%+v", adopted, actual)
	}
	if err := cache.cow.releaseCut(cut); err != nil {
		t.Fatal(err)
	}
	if needed, err := backend.DB.CommandWALCheckpointFallbackNeedsRefresh(actual); err != nil || !needed {
		t.Fatalf("refused fallback did not retain old recovery slot: needed=%t err=%v", needed, err)
	}
	if got, err := held.Get([]byte("pack-03000")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{1}, 128)) {
		t.Fatalf("reader after fallback refusal: %x %v", got, err)
	}
	backend.fallbackFailure = nil
}

func TestPrivateCOWLeafPackDeferredFallbackResumesOnNoop(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	for _, hideRan := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted-error-ran-hidden-%v", hideRan), func(t *testing.T) {
			cache, backend := privateCOWPackFixture(t)
			preparePrivateCOWPack(t, cache, backend.DB)
			held := cache.AcquireSnapshot()
			if held == nil {
				t.Fatal("missing original reader")
			}
			defer held.Close()
			next := backend.DB.CommandWALNextLSN()
			failure := errors.New("finalization failed after durable pack")
			backend.failure, backend.hideRanOnError = failure, hideRan
			attempted, err := cache.runPrivateCOWLeafPackSlice()
			if !attempted || !backend.last.Ran || !errors.Is(err, failure) || cache.cow.refreshRequired.Load() || backend.gcCalls != 0 {
				t.Fatalf("accepted pack must retain its error and adopt: attempted=%v actual=%+v err=%v", attempted, backend.last, err)
			}
			cut := cache.cow.retainCut()
			if cut == nil {
				t.Fatal("missing accepted replacement")
			}
			basis, live := cut.basis.snapshot.StateToken()
			through := rootpublication.SealedPrefix{LogicalSequence: cut.basis.logicalSeq, CanonicalLSN: basis.AppliedCommandLSN}
			if err := cache.cow.releaseCut(cut); err != nil || !live {
				t.Fatalf("captured basis: live=%v err=%v", live, err)
			}
			if needed, err := backend.DB.CommandWALCheckpointFallbackNeedsRefresh(basis); err != nil || !needed {
				t.Fatalf("expected lawful pending convergence: needed=%v err=%v", needed, err)
			}
			if err := cache.cow.prefix.WaitAccepted(context.Background(), through); err != nil {
				t.Fatal(err)
			}
			// Already accepted custody must not let an expired optional slice
			// enter the synchronous physical build. No native I/O is abandoned.
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			defer cancel()
			before, _ := backend.DB.StateToken()
			cache.flushMu.Lock()
			err = cache.refreshCOWAutomaticCheckpointFallback(ctx, through)
			cache.flushMu.Unlock()
			after, _ := backend.DB.StateToken()
			if !errors.Is(err, context.DeadlineExceeded) || after != before || backend.DB.CommandWALNextLSN() != next {
				t.Fatalf("expired fallback published: before=%+v after=%+v err=%v", before, after, err)
			}
			backend.failure, backend.forceNoop = nil, true
			cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(0)
			if attempted, err = cache.runPrivateCOWLeafPackSlice(); !attempted || err != nil || backend.last.Ran || backend.gcCalls != 1 {
				t.Fatalf("no-op slice did not resume convergence and GC: attempted=%v ran=%v GC=%d err=%v", attempted, backend.last.Ran, backend.gcCalls, err)
			}
			after, _ = backend.DB.StateToken()
			if needed, err := backend.DB.CommandWALCheckpointFallbackNeedsRefresh(after); err != nil || needed || backend.DB.CommandWALNextLSN() != next || after.AppliedCommandLSN != basis.AppliedCommandLSN {
				t.Fatalf("no-op failed original-cut convergence: needed=%v state=%+v err=%v", needed, after, err)
			}
			if got, err := held.Get([]byte("pack-03000")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{1}, 128)) {
				t.Fatalf("held reader lost after resumed convergence: %x %v", got, err)
			}
			// Once converged, repeated healthy no-op slices must not manufacture
			// another root publication or command. They still run maintained GC.
			for range 2 {
				before, _ = backend.DB.StateToken()
				cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(0)
				if attempted, err = cache.runPrivateCOWLeafPackSlice(); !attempted || err != nil {
					t.Fatalf("healthy no-op: attempted=%v err=%v", attempted, err)
				}
				after, _ = backend.DB.StateToken()
				if after != before || backend.DB.CommandWALNextLSN() != next {
					t.Fatalf("healthy no-op published: before=%+v after=%+v", before, after)
				}
			}
		})
	}
}
