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

type privateCOWLeafGCResult struct {
	packet <-chan struct{}
	stats  backenddb.LeafGenerationGCStats
}

// This adapter pauses the actual named backend operation immediately after its
// physical publication, before the cache can adopt that basis. Backend copy's
// own writeMu split is separately exercised by its maintained copy-hook tests.
type privateCOWPackHeldBackend struct {
	*backenddb.DB
	published       chan backenddb.LeafGenerationPackRunOnceStats
	release         chan struct{}
	failure         error
	gcFailure       error
	gcDeleted       int
	fallbackFailure error
	calls           int
	gcCalls         int
	last            backenddb.LeafGenerationPackRunOnceStats
	lastOpts        backenddb.LeafGenerationPackFromPlanOptions
	packPacket      chan (<-chan struct{})
	gcCompleted     chan privateCOWLeafGCResult
	forceNoop       bool
	hideRanOnError  bool
	overrideDebt    bool
	debtGenerations int
	debtBytes       int64
}

func (b *privateCOWPackHeldBackend) LeafGenerationSealedDebt() (int, int64) {
	if b.overrideDebt {
		return b.debtGenerations, b.debtBytes
	}
	return b.DB.LeafGenerationSealedDebt()
}

func (b *privateCOWPackHeldBackend) LeafGenerationPackRunOnceForPrivateCOW(owner *transactionowner.Owner, ctx context.Context, opts backenddb.LeafGenerationPackFromPlanOptions) (backenddb.LeafGenerationPackRunOnceStats, error) {
	b.calls++
	b.lastOpts = opts
	if b.forceNoop {
		b.last = backenddb.LeafGenerationPackRunOnceStats{SkipReason: "test: no remaining pack candidate"}
		return b.last, b.failure
	}
	stats, err := b.DB.LeafGenerationPackRunOnceForPrivateCOW(owner, ctx, opts)
	b.last = stats
	if b.packPacket != nil {
		b.packPacket <- ctx.Done()
	}
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
	if b.gcDeleted > 0 {
		stats.GenerationsDeleted = b.gcDeleted
	}
	err = errors.Join(err, b.gcFailure)
	if b.gcCompleted != nil {
		b.gcCompleted <- privateCOWLeafGCResult{packet: ctx.Done(), stats: stats}
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

func TestPrivateCOWLeafPackDebtResponsivePolicyAndBackoff(t *testing.T) {
	cache, backend := privateCOWPackFixture(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envDisableVlogGenerationLoop, "0")
	backend.forceNoop = true

	// Below debt watermark: default 30s interval and 2-generation / 32 MiB limits.
	backend.overrideDebt = true
	backend.debtGenerations = 2
	backend.debtBytes = 8 << 20
	interval, maxGens, maxCopy := cache.privateCOWLeafPackPolicy()
	if interval != privateCOWLeafPackInterval || maxGens != privateCOWLeafPackGenerations || maxCopy != privateCOWLeafPackCopyBytes {
		t.Fatalf("default policy=(%v, %d, %d) want (%v, %d, %d)", interval, maxGens, maxCopy, privateCOWLeafPackInterval, privateCOWLeafPackGenerations, privateCOWLeafPackCopyBytes)
	}

	// Sealed-generation count crossing watermark tightens interval to 3s and
	// expands batch limits to 6 generations / 64 MiB.
	backend.debtGenerations = privateCOWLeafPackDebtGenerations
	backend.debtBytes = 8 << 20
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(time.Now().Add(-5 * time.Second).UnixNano())
	cache.storeVlogGenerationLeafPackLastSkipReason("")
	interval, maxGens, maxCopy = cache.privateCOWLeafPackPolicy()
	if interval != privateCOWLeafPackDebtInterval || maxGens != privateCOWLeafPackDebtGenerationsBatch || maxCopy != privateCOWLeafPackDebtCopyBytes {
		t.Fatalf("generation debt policy=(%v, %d, %d) want (%v, %d, %d)", interval, maxGens, maxCopy, privateCOWLeafPackDebtInterval, privateCOWLeafPackDebtGenerationsBatch, privateCOWLeafPackDebtCopyBytes)
	}

	// Verify runPrivateCOWLeafPackSlice enters after 5s (< 30s default) and
	// forwards the expanded batch limits to LeafGenerationPackRunOnceForPrivateCOW.
	attempted, err := cache.runPrivateCOWLeafPackSlice()
	if err != nil || !attempted {
		t.Fatalf("debt-responsive slice did not enter: attempted=%v err=%v", attempted, err)
	}
	if backend.lastOpts.MaxGenerations != privateCOWLeafPackDebtGenerationsBatch {
		t.Fatalf("MaxGenerations=%d want %d under debt", backend.lastOpts.MaxGenerations, privateCOWLeafPackDebtGenerationsBatch)
	}
	if backend.lastOpts.MaxBytesToCopy != privateCOWLeafPackDebtCopyBytes {
		t.Fatalf("MaxBytesToCopy=%d want %d under debt", backend.lastOpts.MaxBytesToCopy, privateCOWLeafPackDebtCopyBytes)
	}

	// Verify backoff on selection skip/error is preserved even under high debt:
	// 5s after a selection skip must NOT re-enter before 30s.
	callsBefore := backend.calls
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(time.Now().Add(-5 * time.Second).UnixNano())
	cache.vlogGenerationLeafPackGCLastDeletedGenerations.Store(0)
	cache.storeVlogGenerationLeafPackLastSkipReason("selection:below_threshold")
	if attempted, err := cache.runPrivateCOWLeafPackSlice(); attempted || err != nil || backend.calls != callsBefore {
		t.Fatalf("debt cadence bypassed no-progress backoff: attempted=%v err=%v calls=%d->%d", attempted, err, callsBefore, backend.calls)
	}

	// Verify transitioning from idle/no_candidates into high debt wakes up at 3s.
	cache.storeVlogGenerationLeafPackLastSkipReason("plan_admission:no_candidates")
	if attempted, err := cache.runPrivateCOWLeafPackSlice(); !attempted || err != nil || backend.calls != callsBefore+1 {
		t.Fatalf("debt cadence failed to wake from no_candidates: attempted=%v err=%v calls=%d->%d", attempted, err, callsBefore, backend.calls)
	}

	// Verify flushMu.TryLock() never blocks foreground writers under debt.
	callsBefore = backend.calls
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(time.Now().Add(-5 * time.Second).UnixNano())
	cache.storeVlogGenerationLeafPackLastSkipReason("")
	cache.flushMu.Lock()
	attempted, err = cache.runPrivateCOWLeafPackSlice()
	cache.flushMu.Unlock()
	if attempted || err != nil || backend.calls != callsBefore {
		t.Fatalf("debt cadence blocked or entered under held flushMu: attempted=%v err=%v", attempted, err)
	}

	// Verify byte-threshold debt trigger (even with < 6 sealed generations).
	backend.debtGenerations = 2
	backend.debtBytes = privateCOWLeafPackDebtBytes
	interval, maxGens, maxCopy = cache.privateCOWLeafPackPolicy()
	if interval != privateCOWLeafPackDebtInterval || maxGens != privateCOWLeafPackDebtGenerationsBatch || maxCopy != privateCOWLeafPackDebtCopyBytes {
		t.Fatalf("byte debt policy=(%v, %d, %d) want (%v, %d, %d)", interval, maxGens, maxCopy, privateCOWLeafPackDebtInterval, privateCOWLeafPackDebtGenerationsBatch, privateCOWLeafPackDebtCopyBytes)
	}

	// Verify explicit env disable still takes precedence.
	t.Setenv(envEnableLeafGenerationPackMaintenance, "0")
	if cache.privateCOWLeafPackEnabled() {
		t.Fatal("explicit env disable ignored under debt")
	}
}

func TestPrivateCOWLeafPackDebtResponsiveMultiGenerationPack(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	cache, backend := privateCOWPackFixture(t)
	writeRange := func(prefix string, start, end int, fill byte) {
		t.Helper()
		b := cache.NewBatchWithSize(end - start)
		defer b.Close()
		val := bytes.Repeat([]byte{fill}, 128)
		for i := start; i < end; i++ {
			if err := b.Set([]byte(fmt.Sprintf("%s-%05d", prefix, i)), val); err != nil {
				t.Fatal(err)
			}
		}
		f := &cowPressureRolloverFixture{db: cache, backend: backend.DB}
		if err := f.write(b, true, nil); err != nil {
			t.Fatal(err)
		}
	}
	rotateAllLeafLanes := func() {
		t.Helper()
		for _, l := range cache.leafLogAppendLanesSnapshot() {
			l.vlogMu.Lock()
			rotateErr := cache.rotateValueLogMuHeld(l)
			l.vlogMu.Unlock()
			if rotateErr != nil {
				t.Fatal(rotateErr)
			}
		}
	}

	// Build 6 distinct sealed generations, each holding its own sorted key range
	// (`gen-01-*` .. `gen-06-*`), then overwrite 75% of each range in a final
	// generation so all 6 sealed generations are deterministically ~75% dead /
	// ~25% live (~3,000,000 PPM reclaim-per-copy) and cross the debt watermark.
	const numGenerations = 6
	const keysPerGen = 1024
	const overwritePerGen = 768
	for round := 1; round <= numGenerations; round++ {
		writeRange(fmt.Sprintf("gen-%02d", round), 0, keysPerGen, byte(round))
		if err := cache.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		rotateAllLeafLanes()
	}
	for round := 1; round <= numGenerations; round++ {
		writeRange(fmt.Sprintf("gen-%02d", round), 0, overwritePerGen, byte(0x80+round))
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envDisableVlogGenerationLoop, "0")

	sealedGens, sealedBytes := backend.DB.LeafGenerationSealedDebt()
	if sealedGens < privateCOWLeafPackDebtGenerations {
		t.Fatalf("expected >= %d sealed generations for debt fixture, got %d (bytes=%d)", privateCOWLeafPackDebtGenerations, sealedGens, sealedBytes)
	}

	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(time.Now().Add(-5 * time.Second).UnixNano())
	cache.storeVlogGenerationLeafPackLastSkipReason("")

	attempted, err := cache.runPrivateCOWLeafPackSlice()
	if err != nil || !attempted || !backend.last.Ran {
		t.Fatalf("debt-responsive slice did not run: attempted=%v stats=%+v err=%v", attempted, backend.last, err)
	}
	if backend.lastOpts.MaxGenerations != privateCOWLeafPackDebtGenerationsBatch {
		t.Fatalf("MaxGenerations=%d want %d under debt", backend.lastOpts.MaxGenerations, privateCOWLeafPackDebtGenerationsBatch)
	}
	if len(backend.last.Selection.GenerationIDs) <= privateCOWLeafPackGenerations {
		t.Fatalf("expected > %d generations packed under debt, got %d (%v)", privateCOWLeafPackGenerations, len(backend.last.Selection.GenerationIDs), backend.last.Selection.GenerationIDs)
	}
}

func TestPrivateCOWLeafPackPeriodicWriteBurstGateAndRecovery(t *testing.T) {
	cache, backend := privateCOWPackFixture(t)
	t.Setenv(envDisableVlogGenerationLoop, "0")
	// Unset envEnableLeafGenerationPackMaintenance to exercise standalone COW default mode.
	if err := os.Unsetenv(envEnableLeafGenerationPackMaintenance); err != nil {
		t.Fatal(err)
	}
	backend.forceNoop = true
	backend.overrideDebt = true
	backend.debtGenerations = privateCOWLeafPackDebtGenerations
	backend.debtBytes = privateCOWLeafPackDebtBytes

	// 1. Periodic write-burst skip: active write suppresses periodic pack and
	// records telemetry even when TREEDB_ENABLE_LEAF_GENERATION_PACK_MAINTENANCE is unset.
	cache.noteWrite()
	if ran := cache.maybeRunPeriodicVlogGenerationMaintenance(false); ran || backend.calls != 0 {
		t.Fatalf("periodic write-burst skip failed: ran=%v calls=%d", ran, backend.calls)
	}
	if got := cache.vlogGenerationLeafPackSkipWriteBurst.Load(); got != 1 {
		t.Fatalf("skip.write_burst=%d want 1", got)
	}
	if got := cache.Stats()["treedb.cache.vlog_generation.leaf_pack.skip.write_burst"]; got != "1" {
		t.Fatalf("stats skip.write_burst=%q want %q", got, "1")
	}
	if got := atomicValueString(&cache.vlogGenerationLeafPackLastSkipReason); got != "write_burst" {
		t.Fatalf("lastSkipReason=%q want %q", got, "write_burst")
	}
	// Transient admission skips must not poison privateCOWLeafPackPolicy() into 30s backoff.
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(time.Now().Add(-5 * time.Second).UnixNano())
	for _, reason := range []string{"write_burst", "foreground_iterators", "queue_pressure"} {
		cache.storeVlogGenerationLeafPackLastSkipReason(reason)
		if interval, _, _ := cache.privateCOWLeafPackPolicy(); interval != privateCOWLeafPackDebtInterval {
			t.Fatalf("transient skip %q poisoned debt interval: got %v want %v", reason, interval, privateCOWLeafPackDebtInterval)
		}
	}

	// 2. Quiet recovery: once lastForegroundWriteUnixNano is past private-COW
	// write-burst grace, periodic maintenance runs normally.
	cache.storeVlogGenerationLeafPackLastSkipReason("write_burst")
	grace := privateCOWLeafPackWriteBurstGrace()
	// Still inside private-COW write-burst grace (e.g. 500ms < 2s) must keep skipping.
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-500 * time.Millisecond).UnixNano())
	if ran := cache.maybeRunPeriodicVlogGenerationMaintenance(false); ran || backend.calls != 0 {
		t.Fatalf("sub-grace write burst ran periodic pack: ran=%v calls=%d", ran, backend.calls)
	}
	cache.lastForegroundWriteUnixNano.Store(time.Now().Add(-grace - time.Second).UnixNano())
	if ran := cache.maybeRunPeriodicVlogGenerationMaintenance(false); !ran || backend.calls != 1 {
		t.Fatalf("quiet recovery failed: ran=%v calls=%d", ran, backend.calls)
	}

	// 3. Emergency debt override: under an active write burst with sealed debt
	// >= 256 MiB, periodic maintenance overrides write-burst gating.
	skipsBefore := cache.vlogGenerationLeafPackSkipWriteBurst.Load()
	backend.debtBytes = privateCOWLeafPackEmergencyDebtBytes
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(0)
	cache.noteWrite()
	if ran := cache.maybeRunPeriodicVlogGenerationMaintenance(false); !ran || backend.calls != 2 {
		t.Fatalf("emergency debt override failed: ran=%v calls=%d", ran, backend.calls)
	}
	if got := cache.vlogGenerationLeafPackSkipWriteBurst.Load(); got != skipsBefore {
		t.Fatalf("emergency debt override incremented skip.write_burst: %d -> %d", skipsBefore, got)
	}
}

func TestPrivateCOWLeafPackPeriodicGateSuppressedDuringMaintenanceAndClosing(t *testing.T) {
	cache, backend := privateCOWPackFixture(t)
	t.Setenv(envDisableVlogGenerationLoop, "0")
	if err := os.Unsetenv(envEnableLeafGenerationPackMaintenance); err != nil {
		t.Fatal(err)
	}
	backend.forceNoop = true
	cache.noteWrite()

	// Restore maintenance phase must suppress without incrementing skip.write_burst.
	cache.SetMaintenancePhase(MaintenancePhaseRestore)
	if ran := cache.maybeRunPeriodicVlogGenerationMaintenance(false); ran || backend.calls != 0 {
		t.Fatalf("restore phase ran periodic pack: ran=%v calls=%d", ran, backend.calls)
	}
	cache.SetMaintenancePhase(MaintenancePhaseSteady)

	// maintenanceActive must suppress without incrementing skip.write_burst.
	cache.maintenanceActive.Store(true)
	if ran := cache.maybeRunPeriodicVlogGenerationMaintenance(false); ran || backend.calls != 0 {
		t.Fatalf("maintenanceActive ran periodic pack: ran=%v calls=%d", ran, backend.calls)
	}
	cache.maintenanceActive.Store(false)

	// vlogGenerationMaintenanceActive must suppress without incrementing skip.write_burst.
	cache.vlogGenerationMaintenanceActive.Store(true)
	if ran := cache.maybeRunPeriodicVlogGenerationMaintenance(false); ran || backend.calls != 0 {
		t.Fatalf("vlogGenerationMaintenanceActive ran periodic pack: ran=%v calls=%d", ran, backend.calls)
	}
	cache.vlogGenerationMaintenanceActive.Store(false)

	// closing must suppress without incrementing skip.write_burst.
	cache.closing.Store(true)
	if ran := cache.maybeRunPeriodicVlogGenerationMaintenance(false); ran || backend.calls != 0 {
		t.Fatalf("closing ran periodic pack: ran=%v calls=%d", ran, backend.calls)
	}
	cache.closing.Store(false)

	if got := cache.vlogGenerationLeafPackSkipWriteBurst.Load(); got != 0 {
		t.Fatalf("suppressed states incremented skip.write_burst=%d want 0", got)
	}
}

func TestPrivateCOWLeafPackErrorBackoffPreserved(t *testing.T) {
	cache, backend := privateCOWPackFixture(t)
	t.Setenv(envDisableVlogGenerationLoop, "0")
	if err := os.Unsetenv(envEnableLeafGenerationPackMaintenance); err != nil {
		t.Fatal(err)
	}
	backend.forceNoop = true
	backend.overrideDebt = true
	backend.debtGenerations = privateCOWLeafPackDebtGenerations
	backend.debtBytes = privateCOWLeafPackDebtBytes

	// 1. Pack slice returning ErrLeafGenerationMaintenanceLimit returns (true, nil)
	// but MUST set error backoff and preserve the 30s interval even under debt
	// and even if a later write_burst admission skip overwrites lastSkipReason.
	backend.failure = backenddb.ErrLeafGenerationMaintenanceLimit
	attempted, err := cache.runPrivateCOWLeafPackSlice()
	if !attempted || err != nil {
		t.Fatalf("limit pack slice: attempted=%v err=%v want true, nil", attempted, err)
	}
	if !cache.vlogGenerationLeafPackLastAttemptErrored.Load() {
		t.Fatal("limit pack slice did not mark LastAttemptErrored")
	}
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(time.Now().Add(-5 * time.Second).UnixNano())
	cache.observeVlogGenerationLeafPackAdmissionSkip("write_burst")
	if interval, _, _ := cache.privateCOWLeafPackPolicy(); interval != privateCOWLeafPackInterval {
		t.Fatalf("limit error backoff overridden after write_burst skip: got %v want %v", interval, privateCOWLeafPackInterval)
	}

	// 2. Pack error with lastDeleted > 0 from a prior GC (e.g. failure before
	// line 156 clears lastDeleted, or GC failure after deleting generations)
	// MUST preserve the 30s backoff instead of dropping to 3s.
	backend.failure = nil
	backend.gcFailure = backenddb.ErrLeafGenerationMaintenanceLimit
	backend.gcDeleted = 2
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(0)
	attempted, err = cache.runPrivateCOWLeafPackSlice()
	if !attempted || err != nil {
		t.Fatalf("limit GC slice: attempted=%v err=%v want true, nil", attempted, err)
	}
	if !cache.vlogGenerationLeafPackLastAttemptErrored.Load() {
		t.Fatal("limit GC slice did not mark LastAttemptErrored")
	}
	if got := cache.vlogGenerationLeafPackGCLastDeletedGenerations.Load(); got != 2 {
		t.Fatalf("lastDeleted=%d want 2", got)
	}
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(time.Now().Add(-5 * time.Second).UnixNano())
	if interval, _, _ := cache.privateCOWLeafPackPolicy(); interval != privateCOWLeafPackInterval {
		t.Fatalf("GC limit error backoff overridden by lastDeleted>0: got %v want %v", interval, privateCOWLeafPackInterval)
	}

	// 3. Hard error during slice (e.g., refreshRequired before line 156 clears lastDeleted,
	// or packErr != nil) sets LastAttemptErrored and preserves 30s backoff even with lastDeleted > 0.
	backend.gcFailure = nil
	backend.gcDeleted = 0
	cache.vlogGenerationLeafPackLastAttemptErrored.Store(false)
	cache.vlogGenerationLeafPackGCLastDeletedGenerations.Store(3)
	cache.cow.refreshRequired.Store(true)
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(0)
	attempted, err = cache.runPrivateCOWLeafPackSlice()
	cache.cow.refreshRequired.Store(false)
	if attempted || !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("refreshRequired slice: attempted=%v err=%v", attempted, err)
	}
	if !cache.vlogGenerationLeafPackLastAttemptErrored.Load() {
		t.Fatal("pre-defer error did not mark LastAttemptErrored")
	}
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(time.Now().Add(-5 * time.Second).UnixNano())
	if interval, _, _ := cache.privateCOWLeafPackPolicy(); interval != privateCOWLeafPackInterval {
		t.Fatalf("pre-defer error backoff overridden by lastDeleted>0: got %v want %v", interval, privateCOWLeafPackInterval)
	}

	// Hard packErr != nil also preserves 30s backoff.
	backend.failure = errors.New("synthetic pack error")
	cache.vlogGenerationLeafPackLastAttemptErrored.Store(false)
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(0)
	attempted, err = cache.runPrivateCOWLeafPackSlice()
	if !attempted || !errors.Is(err, backend.failure) {
		t.Fatalf("hard pack error slice: attempted=%v err=%v", attempted, err)
	}
	if !cache.vlogGenerationLeafPackLastAttemptErrored.Load() {
		t.Fatal("hard pack error did not mark LastAttemptErrored")
	}
	cache.vlogGenerationLeafPackGCLastDeletedGenerations.Store(1)
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(time.Now().Add(-5 * time.Second).UnixNano())
	if interval, _, _ := cache.privateCOWLeafPackPolicy(); interval != privateCOWLeafPackInterval {
		t.Fatalf("hard pack error backoff overridden by lastDeleted>0: got %v want %v", interval, privateCOWLeafPackInterval)
	}

	// 4. Clean non-error completion clears LastAttemptErrored and restores 3s debt interval.
	backend.failure = nil
	cache.vlogGenerationPrivateCOWPackLastAttemptNS.Store(0)
	attempted, err = cache.runPrivateCOWLeafPackSlice()
	if !attempted || err != nil {
		t.Fatalf("clean recovery slice: attempted=%v err=%v", attempted, err)
	}
	if cache.vlogGenerationLeafPackLastAttemptErrored.Load() {
		t.Fatal("clean slice did not clear LastAttemptErrored")
	}
	cache.storeVlogGenerationLeafPackLastSkipReason("")
	if interval, _, _ := cache.privateCOWLeafPackPolicy(); interval != privateCOWLeafPackDebtInterval {
		t.Fatalf("clean recovery did not restore debt interval: got %v want %v", interval, privateCOWLeafPackDebtInterval)
	}
}

func TestPrivateCOWCheckpointReclaimsDeadSealedGenerations(t *testing.T) {
	cache, backend := privateCOWPackFixture(t)
	rotateAllLeafLanes := func() {
		t.Helper()
		for _, ln := range cache.leafLogAppendLanesSnapshot() {
			if ln == nil {
				continue
			}
			ln.vlogMu.Lock()
			err := cache.rotateValueLogMuHeld(ln)
			ln.vlogMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	writeRange := func(prefix string, start, count int, fill byte) {
		t.Helper()
		b := cache.NewBatchWithSize(count)
		defer b.Close()
		val := bytes.Repeat([]byte{fill}, 128)
		for i := start; i < start+count; i++ {
			if err := b.Set([]byte(fmt.Sprintf("%s-%05d", prefix, i)), val); err != nil {
				t.Fatal(err)
			}
		}
		f := &cowPressureRolloverFixture{db: cache, backend: backend.DB}
		if err := f.write(b, true, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Clean single-generation load with private COW leaf pack enabled must NOT
	// trigger leaf pack during explicit Checkpoint().
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envDisableVlogGenerationLoop, "0")
	writeRange("clean", 0, 512, 0x11)
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if backend.calls != 0 {
		t.Fatalf("clean single-generation Checkpoint triggered leaf pack: calls=%d want 0", backend.calls)
	}

	// Build 4 sealed generations with heavy overwrites (disabled during setup,
	// then enabled before final explicit Checkpoint()).
	t.Setenv(envDisableVlogGenerationLoop, "1")
	const (
		numGenerations  = 4
		keysPerGen      = 1024
		overwritePerGen = 768
	)
	for round := 1; round <= numGenerations; round++ {
		writeRange(fmt.Sprintf("gen-%02d", round), 0, keysPerGen, byte(round))
		if err := cache.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		rotateAllLeafLanes()
	}
	for round := 1; round <= numGenerations; round++ {
		writeRange(fmt.Sprintf("gen-%02d", round), 0, overwritePerGen, byte(0x80+round))
	}

	sealedBefore, bytesBefore := backend.DB.LeafGenerationSealedDebt()
	if sealedBefore < privateCOWCheckpointLeafPackDebtGenerations {
		t.Fatalf("expected >= %d sealed generations before checkpoint, got %d (bytes=%d)", privateCOWCheckpointLeafPackDebtGenerations, sealedBefore, bytesBefore)
	}

	t.Setenv(envDisableVlogGenerationLoop, "0")
	if !rootpublication.StableRelativeNamespaceSupported() {
		// On platforms without stable-relative-namespace promotion (e.g. macOS/Windows),
		// verify Checkpoint() still triggers the pack+GC maintenance hook without failing
		// the durable checkpoint boundary.
		backend.forceNoop = true
		if err := cache.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint with sealed debt (unsupported namespace): %v", err)
		}
		if backend.calls != 1 || backend.gcCalls != 1 {
			t.Fatalf("Checkpoint did not invoke pack+GC hook on sealed debt: calls=%d gcCalls=%d want 1,1", backend.calls, backend.gcCalls)
		}
		if backend.lastOpts.MaxGenerations < privateCOWLeafPackDebtGenerationsBatch {
			t.Fatalf("Checkpoint pack MaxGenerations=%d want >= %d", backend.lastOpts.MaxGenerations, privateCOWLeafPackDebtGenerationsBatch)
		}
		return
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint with sealed debt: %v", err)
	}
	if backend.calls != 1 || !backend.last.Ran {
		t.Fatalf("Checkpoint did not pack dead sealed generations: calls=%d ran=%v skip=%q", backend.calls, backend.last.Ran, backend.last.SkipReason)
	}
	if backend.gcCalls != 1 {
		t.Fatalf("Checkpoint did not run LeafGenerationGC after pack: gcCalls=%d want 1", backend.gcCalls)
	}
	sealedAfter, bytesAfter := backend.DB.LeafGenerationSealedDebt()
	if sealedAfter >= sealedBefore || bytesAfter >= bytesBefore {
		t.Fatalf("Checkpoint did not reduce sealed debt: before=(%d gens, %d bytes) after=(%d gens, %d bytes)", sealedBefore, bytesBefore, sealedAfter, bytesAfter)
	}
	// Verify overwritten and untouched keys remain readable after pack + GC.
	gotOverwritten, err := cache.Get([]byte("gen-01-00000"))
	if err != nil || !bytes.Equal(gotOverwritten, bytes.Repeat([]byte{0x81}, 128)) {
		t.Fatalf("overwritten key mismatch after checkpoint pack: err=%v len=%d", err, len(gotOverwritten))
	}
	gotLive, err := cache.Get([]byte("gen-01-01023"))
	if err != nil || !bytes.Equal(gotLive, bytes.Repeat([]byte{1}, 128)) {
		t.Fatalf("live key mismatch after checkpoint pack: err=%v len=%d", err, len(gotLive))
	}
}
