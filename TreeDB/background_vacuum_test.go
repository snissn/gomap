package treedb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func init() {
	setBackgroundIndexVacuumForegroundWriteQuietHookForTest(func(*DB) bool { return true })
}

func TestBackgroundIndexVacuumConcurrentMutationIsRetryOnly(t *testing.T) {
	concurrentMutation := fmt.Errorf("wrapped vacuum result: %w", backenddb.ErrVacuumConcurrentMutation)
	if backgroundIndexVacuumShouldReport(concurrentMutation) {
		t.Fatal("concurrent-mutation abort classified as permanent background error")
	}
	if !backgroundIndexVacuumShouldReport(errors.New("vacuum I/O failure")) {
		t.Fatal("ordinary vacuum failure classified as retry-only")
	}
	if backgroundIndexVacuumShouldReport(backenddb.ErrVacuumRecoverableRootSetRequired) {
		t.Fatal("recoverable-root-set fence classified as permanent background error")
	}
	if backgroundIndexVacuumShouldReport(backenddb.ErrRecoverableRootSetStale) {
		t.Fatal("stale recoverable-root-set classified as permanent background error")
	}
	if backgroundIndexVacuumShouldReport(backenddb.ErrVacuumUnsupported) {
		t.Fatal("unsupported capability classified as permanent background error")
	}
	if backgroundIndexVacuumShouldReport(backenddb.ErrDurableWALCleanupProofStale) {
		t.Fatal("stale checkpoint-cleanup proof classified as permanent background error")
	}
	if backgroundIndexVacuumShouldReport(rootpublication.ErrResourcePinned) {
		t.Fatal("pinned stable resource classified as permanent background error")
	}
}

func TestBackgroundIndexVacuumDurationStatsAccumulateMonotonically(t *testing.T) {
	var w bgIndexVacuumWorker
	w.recordProbeDuration(3 * time.Nanosecond)
	w.recordProbeDuration(7 * time.Nanosecond)
	w.vacuumAttempts.Add(2)
	w.recordVacuumDuration(5 * time.Nanosecond)
	w.recordVacuumDuration(11 * time.Nanosecond)
	w.vacuumWorkCompleted.Add(1)

	stats := w.Stats()
	if stats.ProbeDurationTotalNs != 10 || stats.ProbeDurationMaxNs != 7 || stats.ProbeDurationLastNs != 7 {
		t.Fatalf("probe durations=%+v want total=10 max=7 last=7", stats)
	}
	if stats.VacuumAttempts != 2 || stats.VacuumDurationTotalNs != 16 || stats.VacuumDurationMaxNs != 11 || stats.VacuumDurationLastNs != 11 || stats.VacuumWorkCompleted != 1 {
		t.Fatalf("vacuum durations=%+v want attempts=2 total=16 max=11 last=11 completed=1", stats)
	}
}

func TestBackgroundIndexVacuumFailedRunDoesNotRepublishStaleOnlineSnapshot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	seedBackgroundVacuumUserPages(t, d, 64)
	d.bgVac.spanRatioPPM = 1
	d.bgVac.freelistReclaimablePages = ^uint64(0)
	d.bgVac.collectionRootPages = ^uint64(0)
	stale := backenddb.VacuumOnlineStats{AttemptID: 7, WorkCompleted: true, TotalDuration: time.Nanosecond}
	d.bgVac.lastOnlineVacuum.Store(&stale)
	restore := setBackgroundIndexVacuumRunHookForTest(func(*DB, context.Context) (backenddb.VacuumOnlineStats, error) {
		return backenddb.VacuumOnlineStats{}, errors.New("cached checkpoint admission failed before backend vacuum")
	})
	defer restore()

	d.bgVac.runOnce(d)
	stats := d.bgVac.Stats()
	if stats.VacuumAttempts != 1 || stats.PermanentFailuresTotal != 1 {
		t.Fatalf("failed background attempt counters=%+v", stats)
	}
	if stats.LastOnlineVacuum != stale {
		t.Fatalf("last online snapshot=%+v want retained stale prior snapshot=%+v", stats.LastOnlineVacuum, stale)
	}
	if stats.VacuumWorkCompleted != 0 {
		t.Fatalf("completed work=%d want 0 for failed pre-backend attempt", stats.VacuumWorkCompleted)
	}
}

func TestBackgroundIndexVacuumUsesReturnedOnlineAttemptSnapshot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	seedBackgroundVacuumUserPages(t, d, 64)
	d.bgVac.spanRatioPPM = 1
	d.bgVac.freelistReclaimablePages = ^uint64(0)
	d.bgVac.collectionRootPages = ^uint64(0)

	if err := d.backend.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("seed backend vacuum: %v", err)
	}
	attemptA := d.backend.VacuumOnlineStats()
	if attemptA.AttemptID == 0 {
		t.Fatalf("seed backend snapshot=%+v want attempt ID", attemptA)
	}
	restore := setBackgroundIndexVacuumRunHookForTest(func(db *DB, ctx context.Context) (backenddb.VacuumOnlineStats, error) {
		if err := db.backend.VacuumIndexOnline(ctx); err != nil {
			return backenddb.VacuumOnlineStats{}, err
		}
		return attemptA, nil
	})
	defer restore()

	d.bgVac.runOnce(d)
	global := d.backend.VacuumOnlineStats()
	if global.AttemptID <= attemptA.AttemptID {
		t.Fatalf("global backend snapshot=%+v want newer than returned attempt=%+v", global, attemptA)
	}
	stats := d.bgVac.Stats()
	if stats.LastOnlineVacuum != attemptA {
		t.Fatalf("worker last online snapshot=%+v want hook-returned attempt=%+v", stats.LastOnlineVacuum, attemptA)
	}
	if stats.VacuumWorkCompleted != 1 || stats.Vacuums != 1 {
		t.Fatalf("successful returned snapshot counters=%+v want one completed worker vacuum", stats)
	}
}

func TestBackgroundIndexVacuumCountsCompletedBackendWorkAfterReconcileFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	seedBackgroundVacuumUserPages(t, d, 64)
	d.bgVac.spanRatioPPM = 1
	d.bgVac.freelistReclaimablePages = ^uint64(0)
	d.bgVac.collectionRootPages = ^uint64(0)
	completed := backenddb.VacuumOnlineStats{AttemptID: 41, WorkCompleted: true, TotalDuration: time.Nanosecond}
	reconcileErr := errors.New("cached backend maintenance reconcile failed")
	restore := setBackgroundIndexVacuumRunHookForTest(func(*DB, context.Context) (backenddb.VacuumOnlineStats, error) {
		return completed, reconcileErr
	})
	defer restore()

	d.bgVac.runOnce(d)
	stats := d.bgVac.Stats()
	if stats.LastOnlineVacuum != completed || stats.VacuumWorkCompleted != 1 {
		t.Fatalf("completed backend work stats=%+v want returned completed attempt counted once", stats)
	}
	if stats.Vacuums != 0 || stats.PermanentFailuresTotal != 1 || stats.LastOutcome != backgroundIndexVacuumOutcomePermanentFailure {
		t.Fatalf("reconcile failure outcome=%+v want failed background outcome", stats)
	}
}

func TestBackgroundIndexVacuumFailedBackendAttemptDoesNotCountWork(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	seedBackgroundVacuumUserPages(t, d, 64)
	d.bgVac.spanRatioPPM = 1
	d.bgVac.freelistReclaimablePages = ^uint64(0)
	d.bgVac.collectionRootPages = ^uint64(0)
	failed := backenddb.VacuumOnlineStats{AttemptID: 42, TotalDuration: time.Nanosecond}
	restore := setBackgroundIndexVacuumRunHookForTest(func(*DB, context.Context) (backenddb.VacuumOnlineStats, error) {
		return failed, errors.New("backend vacuum failed")
	})
	defer restore()

	d.bgVac.runOnce(d)
	stats := d.bgVac.Stats()
	if stats.LastOnlineVacuum != failed || stats.VacuumWorkCompleted != 0 {
		t.Fatalf("failed backend work stats=%+v want retained failed attempt without completed work", stats)
	}
	if stats.Vacuums != 0 || stats.PermanentFailuresTotal != 1 {
		t.Fatalf("failed backend outcome=%+v want failed background attempt", stats)
	}
}

func TestBackgroundIndexVacuumIdleUnchangedCommitSkipsStructuralWalks(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                           dir,
		KeepRecent:                    1,
		BackgroundIndexVacuumInterval: -1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	for i := 0; i < 128; i++ {
		key := []byte(fmt.Sprintf("idle-k%04d", i))
		if err := d.Set(key, []byte("value")); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	expected, err := d.backend.IndexVacuumTriggerReport()
	if err != nil {
		t.Fatalf("trigger report: %v", err)
	}
	d.bgVac.spanRatioPPM = ^uint32(0)
	d.bgVac.runOnce(d)
	if got := d.bgVac.lastPages.Load(); got != expected.UserPages {
		t.Fatalf("first probe lastPages=%d want %d", got, expected.UserPages)
	}
	if got := d.bgVac.lastSpanRatio.Load(); got != expected.UserSpanRatioPPM {
		t.Fatalf("first probe lastSpanRatio=%d want %d", got, expected.UserSpanRatioPPM)
	}

	counts, restore := installBackgroundVacuumProbeCounter()
	defer restore()

	d.bgVac.runOnce(d)
	assertNoBackgroundVacuumStructuralWalks(t, counts)

	allocs := testing.AllocsPerRun(100, func() {
		d.bgVac.runOnce(d)
	})
	if allocs > 1 {
		t.Fatalf("unchanged CommitSeq tick allocations = %.2f, want <= 1", allocs)
	}
	assertNoBackgroundVacuumStructuralWalks(t, counts)
}

func TestBackgroundIndexVacuumChangedCommitUsesCheapTrigger(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                           dir,
		KeepRecent:                    1,
		BackgroundIndexVacuumInterval: -1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	for i := 0; i < 128; i++ {
		key := []byte(fmt.Sprintf("changed-k%04d", i))
		if err := d.Set(key, []byte("value")); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	d.bgVac.spanRatioPPM = ^uint32(0)
	d.bgVac.runOnce(d)
	firstProbeSeq := d.bgVac.lastProbeCommitSeq
	if firstProbeSeq == 0 {
		t.Fatalf("first probe did not record commit seq")
	}

	if err := d.Set([]byte("changed-new-key"), []byte("value")); err != nil {
		t.Fatalf("set changed: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint changed: %v", err)
	}
	state := d.backend.State()
	if state == nil {
		t.Fatalf("missing backend state")
	}
	if state.CommitSeq == firstProbeSeq {
		t.Fatalf("write did not advance CommitSeq: %d", state.CommitSeq)
	}

	expected, err := d.backend.IndexVacuumTriggerReport()
	if err != nil {
		t.Fatalf("trigger report after change: %v", err)
	}
	counts, restore := installBackgroundVacuumProbeCounter()
	defer restore()

	d.bgVac.runOnce(d)

	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 1 {
		t.Fatalf("trigger report calls=%d want 1", got)
	}
	if got := counts[backenddb.FragmentationProbeEventTriggerUserTreeWalk]; got != 1 {
		t.Fatalf("trigger user-tree walks=%d want 1", got)
	}
	if got := counts[backenddb.FragmentationProbeEventTriggerFreelistCounters]; got != 1 {
		t.Fatalf("trigger freelist counter reads=%d want 1", got)
	}
	if got := counts[backenddb.FragmentationProbeEventTriggerCollectionRootWalk]; got != 1 {
		t.Fatalf("trigger collection-root walks=%d want 1", got)
	}
	if got := d.bgVac.lastPages.Load(); got != expected.UserPages {
		t.Fatalf("lastPages=%d want %d", got, expected.UserPages)
	}
	if got := d.bgVac.lastSpanRatio.Load(); got != expected.UserSpanRatioPPM {
		t.Fatalf("lastSpanRatio=%d want %d", got, expected.UserSpanRatioPPM)
	}
	if got := d.bgVac.lastProbeCommitSeq; got != expected.CommitSeq {
		t.Fatalf("lastProbeCommitSeq=%d want %d", got, expected.CommitSeq)
	}
	if d.bgVac.retryProbe {
		t.Fatalf("retryProbe left set after successful changed-state trigger")
	}
	assertNoBackgroundVacuumFullFragmentationWalks(t, counts)
}

func TestBackgroundIndexVacuumSustainedBacklogForcesTriggerProbe(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	seedBackgroundVacuumUserPages(t, d, 64)

	d.bgVac.maxBacklogSkips = 2
	d.bgVac.spanRatioPPM = ^uint32(0)

	restoreBacklog := setBackgroundIndexVacuumBacklogBytesHookForTest(func(*DB) int64 { return 4096 })
	defer restoreBacklog()
	counts, restoreCounts := installBackgroundVacuumProbeCounter()
	defer restoreCounts()

	d.bgVac.runOnce(d)
	d.bgVac.runOnce(d)
	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 0 {
		t.Fatalf("trigger probes before skip cap=%d want 0", got)
	}
	if got := d.bgVac.backlogConsecutiveSkips.Load(); got != 2 {
		t.Fatalf("consecutive backlog skips=%d want 2", got)
	}
	if got := d.bgVac.backlogSkips.Load(); got != 2 {
		t.Fatalf("total backlog skips=%d want 2", got)
	}

	d.bgVac.runOnce(d)
	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 1 {
		t.Fatalf("forced trigger probes=%d want 1", got)
	}
	if got := d.bgVac.backlogForcedRuns.Load(); got != 1 {
		t.Fatalf("forced-after-backlog runs=%d want 1", got)
	}
	if got := d.bgVac.backlogConsecutiveSkips.Load(); got != 0 {
		t.Fatalf("consecutive backlog skips after forced probe=%d want 0", got)
	}
	if got := d.bgVac.lastBacklogBytes.Load(); got != 4096 {
		t.Fatalf("last backlog bytes=%d want 4096", got)
	}
}

func TestBackgroundIndexVacuumBacklogClearsResetsSkipCounter(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	seedBackgroundVacuumUserPages(t, d, 32)

	d.bgVac.maxBacklogSkips = 3
	d.bgVac.spanRatioPPM = ^uint32(0)
	var backlog atomic.Int64
	backlog.Store(2048)
	restoreBacklog := setBackgroundIndexVacuumBacklogBytesHookForTest(func(*DB) int64 { return backlog.Load() })
	defer restoreBacklog()
	counts, restoreCounts := installBackgroundVacuumProbeCounter()
	defer restoreCounts()

	d.bgVac.runOnce(d)
	if got := d.bgVac.backlogConsecutiveSkips.Load(); got != 1 {
		t.Fatalf("consecutive backlog skips=%d want 1", got)
	}

	backlog.Store(0)
	d.bgVac.runOnce(d)
	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 1 {
		t.Fatalf("trigger probes after backlog cleared=%d want 1", got)
	}
	if got := d.bgVac.backlogConsecutiveSkips.Load(); got != 0 {
		t.Fatalf("consecutive backlog skips after clear=%d want 0", got)
	}
	if got := d.bgVac.backlogForcedRuns.Load(); got != 0 {
		t.Fatalf("forced-after-backlog runs=%d want 0", got)
	}
	if got := d.bgVac.lastBacklogBytes.Load(); got != 0 {
		t.Fatalf("last backlog bytes=%d want 0", got)
	}
}

func TestBackgroundIndexVacuumNativeRootWriteSkipsTriggerProbe(t *testing.T) {
	restoreForeground := setBackgroundIndexVacuumForegroundWriteQuietHookForTest(func(db *DB) bool {
		return db == nil || db.cached == nil || db.cached.BackgroundVacuumForegroundWriteQuiet()
	})
	defer restoreForeground()
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	d.bgVac.maxBacklogSkips = 2
	d.bgVac.spanRatioPPM = ^uint32(0)
	writeBackgroundVacuumNativeRoot(t, d)
	if got := d.cached.QueueBacklogBytes(); got != 0 {
		t.Fatalf("queue backlog after native root write=%d want 0", got)
	}
	counts, restore := installBackgroundVacuumProbeCounter()
	defer restore()

	d.bgVac.runOnce(d)
	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 0 {
		t.Fatalf("trigger probes after native root write=%d want 0", got)
	}
	if got := d.bgVac.foregroundConsecutiveSkips.Load(); got != 1 {
		t.Fatalf("consecutive foreground skips=%d want 1", got)
	}
	if got := d.bgVac.foregroundSkips.Load(); got != 1 {
		t.Fatalf("total foreground skips=%d want 1", got)
	}
	if got := d.bgVac.backlogSkips.Load(); got != 0 {
		t.Fatalf("backlog skips=%d want 0", got)
	}
	stats := d.Stats()
	if got := stats["treedb.bg_vacuum.foreground_skips_consecutive"]; got != "1" {
		t.Fatalf("public consecutive foreground skips=%q want 1", got)
	}
	if got := stats["treedb.bg_vacuum.foreground_skips_total"]; got != "1" {
		t.Fatalf("public total foreground skips=%q want 1", got)
	}
	if got := stats["treedb.bg_vacuum.foreground_forced_runs"]; got != "0" {
		t.Fatalf("public forced-after-foreground runs=%q want 0", got)
	}
}

func TestBackgroundIndexVacuumNativeRootWriteForcesProbeAfterSkipCap(t *testing.T) {
	restoreForeground := setBackgroundIndexVacuumForegroundWriteQuietHookForTest(func(db *DB) bool {
		return db == nil || db.cached == nil || db.cached.BackgroundVacuumForegroundWriteQuiet()
	})
	defer restoreForeground()
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	d.bgVac.maxBacklogSkips = 2
	d.bgVac.spanRatioPPM = ^uint32(0)
	writeBackgroundVacuumNativeRoot(t, d)
	counts, restore := installBackgroundVacuumProbeCounter()
	defer restore()

	d.bgVac.runOnce(d)
	d.bgVac.runOnce(d)
	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 0 {
		t.Fatalf("trigger probes before foreground skip cap=%d want 0", got)
	}
	d.bgVac.runOnce(d)
	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 1 {
		t.Fatalf("forced trigger probes=%d want 1", got)
	}
	if got := d.bgVac.foregroundForcedRuns.Load(); got != 1 {
		t.Fatalf("forced-after-foreground runs=%d want 1", got)
	}
	if got := d.bgVac.foregroundConsecutiveSkips.Load(); got != 0 {
		t.Fatalf("consecutive foreground skips after forced probe=%d want 0", got)
	}
	if got := d.bgVac.backlogForcedRuns.Load(); got != 0 {
		t.Fatalf("forced-after-backlog runs=%d want 0", got)
	}
}

func TestBackgroundIndexVacuumDeferredVectorBuildCoalescesHotTicks(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	seedBackgroundVacuumUserPages(t, d, 64)
	control := &DeferredVectorBuildMaintenance{db: d}
	epochID := control.AdmitInsert(context.Background(), "docs", 1)
	if epochID == 0 {
		t.Fatal("admit deferred vector-build epoch")
	}

	var probes, vacuums atomic.Uint64
	restoreProbe := setBackgroundIndexVacuumTriggerReportHookForTest(func(*DB, context.Context) (backenddb.IndexVacuumTriggerReport, error) {
		probes.Add(1)
		return backenddb.IndexVacuumTriggerReport{UserPages: 1, UserSpanRatioPPM: 2_000_000}, nil
	})
	defer restoreProbe()
	restoreVacuum := setBackgroundIndexVacuumRunHookForTest(func(*DB, context.Context) (backenddb.VacuumOnlineStats, error) {
		vacuums.Add(1)
		return backenddb.VacuumOnlineStats{}, nil
	})
	defer restoreVacuum()

	for range 10 {
		d.bgVac.runOnce(d)
	}
	stats := d.bgVac.Stats()
	if probes.Load() != 0 || vacuums.Load() != 0 {
		t.Fatalf("deferred ticks probes=%d vacuums=%d want 0/0", probes.Load(), vacuums.Load())
	}
	if !stats.DeferredVectorBuildActive || !stats.DeferredVectorBuildDebt || stats.DeferredVectorBuildSkips != 10 || stats.DeferredVectorBuildDebtSet != 1 {
		t.Fatalf("deferred stats=%+v want active, one coalesced debt, ten skips", stats)
	}
	if !control.CommitInsert(epochID) {
		t.Fatal("commit deferred vector-build epoch")
	}

	control.End()
	d.bgVac.spanRatioPPM = 1
	d.bgVac.runOnce(d)
	stats = d.bgVac.Stats()
	if probes.Load() != 1 || vacuums.Load() != 1 || stats.DeferredVectorBuildActive || stats.DeferredVectorBuildDebt {
		t.Fatalf("post-epoch probes=%d vacuums=%d stats=%+v want one normal vacuum and cleared debt", probes.Load(), vacuums.Load(), stats)
	}
}

func TestDeferredVectorBuildAdmissionWaitsForRunningVacuum(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	d.bgVac.spanRatioPPM = 1
	control := &DeferredVectorBuildMaintenance{db: d}
	restoreProbe := setBackgroundIndexVacuumTriggerReportHookForTest(func(*DB, context.Context) (backenddb.IndexVacuumTriggerReport, error) {
		return backenddb.IndexVacuumTriggerReport{UserPages: 1, UserSpanRatioPPM: 2_000_000}, nil
	})
	defer restoreProbe()
	vacuumStarted := make(chan struct{})
	releaseVacuum := make(chan struct{})
	restoreVacuum := setBackgroundIndexVacuumRunHookForTest(func(*DB, context.Context) (backenddb.VacuumOnlineStats, error) {
		close(vacuumStarted)
		<-releaseVacuum
		return backenddb.VacuumOnlineStats{}, nil
	})
	defer restoreVacuum()

	vacuumDone := make(chan struct{})
	go func() {
		d.bgVac.runOnce(d)
		close(vacuumDone)
	}()
	<-vacuumStarted
	admitted := make(chan uint64, 1)
	go func() { admitted <- control.AdmitInsert(context.Background(), "docs", 1) }()
	select {
	case <-admitted:
		t.Fatal("deferred admission overlapped an in-flight vacuum")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseVacuum)
	<-vacuumDone
	if epochID := <-admitted; epochID == 0 {
		t.Fatal("deferred admission failed after vacuum completed")
	}
}

func TestDeferredVectorBuildAdmissionHonorsCancellationWhileVacuumOwnsLock(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	control := &DeferredVectorBuildMaintenance{db: d}
	d.bgVac.runMu.Lock()
	defer d.bgVac.runMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	admitted := make(chan uint64, 1)
	go func() { admitted <- control.AdmitInsert(ctx, "docs", 1) }()
	select {
	case <-admitted:
		t.Fatal("admission did not wait for maintenance ownership")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case reservationID := <-admitted:
		if reservationID != 0 {
			t.Fatalf("canceled reservation=%d want 0", reservationID)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled admission did not return")
	}
	if d.bgVac.Stats().DeferredVectorBuildActive {
		t.Fatal("canceled admission published an epoch")
	}
}

func TestDeferredVectorBuildMaintenanceFinalizeOwnsWorkerAndRunsAtMostOneVacuum(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: time.Hour})
	control := &DeferredVectorBuildMaintenance{db: d}
	epochID := control.AdmitInsert(context.Background(), "docs", 1)
	if epochID == 0 || !control.CommitInsert(epochID) {
		t.Fatal("begin deferred vector-build epoch")
	}
	// One hot tick establishes coalesced scheduling debt without probing.
	d.bgVac.runOnce(d)

	var probes, vacuums atomic.Uint64
	var finalizerBuilt atomic.Bool
	restoreProbe := setBackgroundIndexVacuumTriggerReportHookForTest(func(*DB, context.Context) (backenddb.IndexVacuumTriggerReport, error) {
		probes.Add(1)
		if finalizerBuilt.Load() {
			return backenddb.IndexVacuumTriggerReport{}, nil
		}
		return backenddb.IndexVacuumTriggerReport{UserPages: 1, UserSpanRatioPPM: 2_000_000}, nil
	})
	defer restoreProbe()
	restoreVacuum := setBackgroundIndexVacuumRunHookForTest(func(*DB, context.Context) (backenddb.VacuumOnlineStats, error) {
		vacuums.Add(1)
		return backenddb.VacuumOnlineStats{WorkCompleted: true}, nil
	})
	defer restoreVacuum()

	buildEntered := make(chan struct{})
	releaseBuild := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- control.Finalize(context.Background(), "docs", 1, nil, func() error {
			close(buildEntered)
			<-releaseBuild
			finalizerBuilt.Store(true)
			return nil
		})
	}()
	<-buildEntered
	if !d.bgVac.Enabled() {
		t.Fatal("background worker is not enabled")
	}
	// The same run lock used by the enabled worker remains owned across source
	// build, publication, and cache work.
	if d.bgVac.runMu.TryLock() {
		d.bgVac.runMu.Unlock()
		t.Fatal("worker run lock was available during finalizer build")
	}
	workerDone := make(chan struct{})
	go func() {
		d.bgVac.runOnce(d)
		close(workerDone)
	}()
	if got := probes.Load(); got != 1 {
		t.Fatalf("probes during finalizer build=%d want 1", got)
	}
	close(releaseBuild)
	if err := <-done; err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	<-workerDone
	if probes.Load() < 1 || vacuums.Load() != 1 {
		t.Fatalf("finalizer probes=%d vacuums=%d want one debt-qualified vacuum", probes.Load(), vacuums.Load())
	}
	stats := d.bgVac.Stats()
	if stats.DeferredVectorBuildActive || stats.DeferredVectorBuildDebt {
		t.Fatalf("finalizer left active/debt state: %+v", stats)
	}
}

func TestDeferredVectorBuildMaintenanceFinalizeDebtAndFailureSemantics(t *testing.T) {
	tests := []struct {
		name             string
		coalescedDebt    bool
		physicalDebt     bool
		vacuumErr        error
		wantErr          bool
		wantProbes       uint64
		wantVacuums      uint64
		wantBuild        bool
		wantDebtRetained bool
	}{
		{name: "no scheduling debt does not probe", wantBuild: true},
		{name: "coalesced debt without physical debt is discharged by the successful probe", coalescedDebt: true, wantProbes: 1, wantBuild: true},
		{name: "successful eligible vacuum consumes debt", coalescedDebt: true, physicalDebt: true, wantProbes: 1, wantVacuums: 1, wantBuild: true},
		{name: "unsupported vacuum is skipped and build still completes", coalescedDebt: true, physicalDebt: true, vacuumErr: errVacuumUnsupported, wantProbes: 1, wantVacuums: 1, wantBuild: true},
		{name: "vacuum failure prevents build and retains debt", coalescedDebt: true, physicalDebt: true, vacuumErr: errors.New("injected vacuum failure"), wantErr: true, wantProbes: 1, wantVacuums: 1, wantDebtRetained: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
			control := &DeferredVectorBuildMaintenance{db: d}
			epochID := control.AdmitInsert(context.Background(), "docs", 1)
			if epochID == 0 || !control.CommitInsert(epochID) {
				t.Fatal("begin deferred vector-build epoch")
			}
			if tt.coalescedDebt {
				d.bgVac.runOnce(d)
			}
			var probes, vacuums atomic.Uint64
			restoreProbe := setBackgroundIndexVacuumTriggerReportHookForTest(func(*DB, context.Context) (backenddb.IndexVacuumTriggerReport, error) {
				probes.Add(1)
				if tt.physicalDebt {
					return backenddb.IndexVacuumTriggerReport{UserPages: 1, UserSpanRatioPPM: 2_000_000}, nil
				}
				return backenddb.IndexVacuumTriggerReport{}, nil
			})
			defer restoreProbe()
			restoreVacuum := setBackgroundIndexVacuumRunHookForTest(func(*DB, context.Context) (backenddb.VacuumOnlineStats, error) {
				vacuums.Add(1)
				return backenddb.VacuumOnlineStats{}, tt.vacuumErr
			})
			defer restoreVacuum()
			built := false
			err := control.Finalize(context.Background(), "docs", 1, nil, func() error {
				built = true
				return nil
			})
			if (err != nil) != tt.wantErr {
				t.Fatalf("Finalize error=%v wantErr=%t", err, tt.wantErr)
			}
			if probes.Load() != tt.wantProbes || vacuums.Load() != tt.wantVacuums || built != tt.wantBuild {
				t.Fatalf("probes=%d vacuums=%d built=%t want %d/%d/%t", probes.Load(), vacuums.Load(), built, tt.wantProbes, tt.wantVacuums, tt.wantBuild)
			}
			stats := d.bgVac.Stats()
			if stats.DeferredVectorBuildActive || stats.DeferredVectorBuildDebt != tt.wantDebtRetained {
				t.Fatalf("post-finalize stats=%+v want active=false debt=%t", stats, tt.wantDebtRetained)
			}
		})
	}
}

func TestDeferredVectorBuildMaintenanceFinalizeRejectsPendingAndCompetingOwner(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	control := &DeferredVectorBuildMaintenance{db: d}
	if epochID := control.AdmitInsert(context.Background(), "docs", 1); epochID == 0 {
		t.Fatal("admit pending insert")
	}
	drained := false
	if err := control.Finalize(context.Background(), "docs", 1, func() error { drained = true; return nil }, nil); err == nil || drained {
		t.Fatalf("pending Finalize err=%v drained=%t want fail before drain", err, drained)
	}
	epochID := control.AdmitInsert(context.Background(), "other", 2)
	if epochID == 0 || !control.CommitInsert(epochID) {
		t.Fatal("begin competing owner")
	}
	if err := control.Finalize(context.Background(), "docs", 1, nil, nil); err == nil {
		t.Fatal("competing-owner Finalize succeeded")
	}
	if !d.bgVac.Stats().DeferredVectorBuildActive {
		t.Fatal("competing-owner Finalize cleared the live owner")
	}
}

func TestDeferredVectorBuildMaintenanceAbortPreservesCompetingReservation(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	control := &DeferredVectorBuildMaintenance{db: d}
	first := control.AdmitInsert(context.Background(), "docs", 1)
	second := control.AdmitInsert(context.Background(), "docs", 1)
	if first == 0 || second == 0 {
		t.Fatal("admit reservations")
	}
	control.AbortInsert(first)
	if !control.CommitInsert(second) || control.CommitInsert(first) {
		t.Fatal("abort removed a competing reservation")
	}
}

func TestDeferredVectorBuildMaintenanceManualWrappersInvalidateEpoch(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	control := &DeferredVectorBuildMaintenance{db: d}
	begin := func() uint64 {
		t.Helper()
		epochID := control.AdmitInsert(context.Background(), "docs", 1)
		if epochID == 0 || !control.CommitInsert(epochID) {
			t.Fatal("begin epoch")
		}
		return epochID
	}

	epochID := begin()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.CompactStorage(ctx, CompactStorageOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("CompactStorage error=%v want canceled", err)
	}
	if control.CommitInsert(epochID) {
		t.Fatal("CompactStorage did not invalidate epoch before failing")
	}

	epochID = begin()
	_ = d.CompactIndex()
	if control.CommitInsert(epochID) {
		t.Fatal("CompactIndex did not invalidate epoch")
	}
}

func TestDeferredVectorBuildMaintenanceCloseWaitsForFinalizer(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), BackgroundIndexVacuumInterval: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	control := &DeferredVectorBuildMaintenance{db: d}
	epochID := control.AdmitInsert(context.Background(), "docs", 1)
	if epochID == 0 || !control.CommitInsert(epochID) {
		t.Fatal("begin epoch")
	}
	buildEntered := make(chan struct{})
	releaseBuild := make(chan struct{})
	finalizeDone := make(chan error, 1)
	go func() {
		finalizeDone <- control.Finalize(context.Background(), "docs", 1, nil, func() error {
			close(buildEntered)
			<-releaseBuild
			return nil
		})
	}()
	<-buildEntered
	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned during finalizer: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseBuild)
	if err := <-finalizeDone; err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := control.Finalize(context.Background(), "docs", 1, nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Finalize after Close error=%v want ErrClosed", err)
	}
}

func TestCloseRejectsQueuedManualMaintenance(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	d.bgVac.runMu.Lock()
	compactDone := make(chan error, 1)
	go func() { compactDone <- d.CompactIndex() }()
	time.Sleep(20 * time.Millisecond)
	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()
	for !d.bgVac.deferredVectorBuildClosed.Load() {
		time.Sleep(time.Millisecond)
	}
	d.bgVac.runMu.Unlock()
	if err := <-compactDone; !errors.Is(err, ErrClosed) {
		t.Fatalf("queued CompactIndex error=%v want ErrClosed", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCompactStorageMaintenanceWaitHonorsContext(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	d.maintenance.mu.Lock()
	defer d.maintenance.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := d.CompactStorage(ctx, CompactStorageOptions{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CompactStorage error=%v want deadline exceeded", err)
	}
	if !d.bgVac.runMu.TryLock() {
		t.Fatal("CompactStorage retained run lock after cancellation")
	}
	d.bgVac.runMu.Unlock()
}

func TestDeferredVectorBuildMaintenanceFailsBackOnAmbiguityAndManualVacuum(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	control := &DeferredVectorBuildMaintenance{db: d}
	epochID := control.AdmitInsert(context.Background(), "docs", 1)
	if epochID == 0 || !control.CommitInsert(epochID) {
		t.Fatal("begin docs generation 1")
	}
	if control.AdmitInsert(context.Background(), "other", 1) != 0 || d.bgVac.Stats().DeferredVectorBuildActive {
		t.Fatal("competing index did not end epoch and fail admission")
	}
	epochID = control.AdmitInsert(context.Background(), "docs", 1)
	if epochID == 0 || !control.CommitInsert(epochID) {
		t.Fatal("restart docs generation 1")
	}
	if control.AdmitInsert(context.Background(), "docs", 2) != 0 || d.bgVac.Stats().DeferredVectorBuildActive {
		t.Fatal("generation mismatch did not end epoch and fail admission")
	}
	epochID = control.AdmitInsert(context.Background(), "docs", 1)
	if epochID == 0 || !control.CommitInsert(epochID) {
		t.Fatal("restart before manual vacuum")
	}
	staleEpochID := control.AdmitInsert(context.Background(), "docs", 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.VacuumIndexOnline(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("manual vacuum error=%v want canceled", err)
	}
	if d.bgVac.Stats().DeferredVectorBuildActive {
		t.Fatal("manual vacuum did not restore ordinary maintenance policy")
	}
	if control.CommitInsert(staleEpochID) || d.bgVac.Stats().DeferredVectorBuildActive {
		t.Fatal("stale insert commit resurrected epoch after manual vacuum")
	}
}

func TestBackgroundIndexVacuumDeferredVectorBuildAbandonmentRestoresPolicy(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	d.bgVac.interval = time.Millisecond
	d.bgVac.spanRatioPPM = 1
	control := &DeferredVectorBuildMaintenance{db: d}
	epochID := control.AdmitInsert(context.Background(), "docs", 1)
	if epochID == 0 || !control.CommitInsert(epochID) {
		t.Fatal("begin epoch")
	}
	epoch := d.bgVac.deferredVectorBuildEpoch.Load()
	d.bgVac.deferredVectorBuildEpoch.Store(&deferredVectorBuildEpoch{
		id:           epoch.id,
		index:        epoch.index,
		generation:   epoch.generation,
		lastActivity: time.Now().Add(-3 * time.Millisecond).UnixNano(),
	})

	var vacuums atomic.Uint64
	restoreProbe := setBackgroundIndexVacuumTriggerReportHookForTest(func(*DB, context.Context) (backenddb.IndexVacuumTriggerReport, error) {
		return backenddb.IndexVacuumTriggerReport{UserPages: 1, UserSpanRatioPPM: 2_000_000}, nil
	})
	defer restoreProbe()
	restoreVacuum := setBackgroundIndexVacuumRunHookForTest(func(*DB, context.Context) (backenddb.VacuumOnlineStats, error) {
		vacuums.Add(1)
		return backenddb.VacuumOnlineStats{}, nil
	})
	defer restoreVacuum()

	d.bgVac.runOnce(d)
	stats := d.bgVac.Stats()
	if stats.DeferredVectorBuildActive || stats.DeferredVectorBuildExpired != 1 || vacuums.Load() != 1 {
		t.Fatalf("abandoned epoch stats=%+v vacuums=%d want expired normal vacuum", stats, vacuums.Load())
	}
}

func TestDeferredVectorBuildMaintenanceCloseAndReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, BackgroundIndexVacuumInterval: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	control := &DeferredVectorBuildMaintenance{db: d}
	epochID := control.AdmitInsert(context.Background(), "docs", 1)
	if epochID == 0 || !control.CommitInsert(epochID) {
		t.Fatal("begin epoch")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if control.CommitInsert(epochID) || d.bgVac.Stats().DeferredVectorBuildActive {
		t.Fatal("closed database admitted a deferred epoch")
	}

	reopened, err := Open(Options{Dir: dir, BackgroundIndexVacuumInterval: -1})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened database: %v", err)
		}
	})
	if reopened.bgVac.Stats().DeferredVectorBuildActive {
		t.Fatal("reopen restored process-local deferred epoch")
	}
}

func TestDeferredVectorBuildMaintenanceTracksConcurrentReservations(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	d.bgVac.interval = time.Millisecond
	control := &DeferredVectorBuildMaintenance{db: d}
	first := control.AdmitInsert(context.Background(), "docs", 1)
	second := control.AdmitInsert(context.Background(), "docs", 1)
	if first == 0 || second == 0 || first == second {
		t.Fatalf("reservations=%d,%d want distinct nonzero tokens", first, second)
	}
	if !control.CommitInsert(first) {
		t.Fatal("commit first reservation")
	}
	epoch := d.bgVac.deferredVectorBuildEpoch.Load()
	d.bgVac.deferredVectorBuildEpoch.Store(&deferredVectorBuildEpoch{
		id:           epoch.id,
		index:        epoch.index,
		generation:   epoch.generation,
		reservations: append([]uint64(nil), epoch.reservations...),
		lastActivity: time.Now().Add(-3 * time.Millisecond).UnixNano(),
	})
	if !d.bgVac.deferredVectorBuildActive(time.Now()) {
		t.Fatal("first commit expired an outstanding reservation")
	}
	if !control.CommitInsert(second) || control.CommitInsert(second) {
		t.Fatal("second reservation commit was not exactly once")
	}
}

func TestDeferredVectorBuildMaintenanceConcurrentWorkerAndLifecycle(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	control := &DeferredVectorBuildMaintenance{db: d}
	restoreProbe := setBackgroundIndexVacuumTriggerReportHookForTest(func(*DB, context.Context) (backenddb.IndexVacuumTriggerReport, error) {
		return backenddb.IndexVacuumTriggerReport{}, nil
	})
	defer restoreProbe()

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				epochID := control.AdmitInsert(context.Background(), "docs", 1)
				control.CommitInsert(epochID)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				d.bgVac.runOnce(d)
				control.End()
			}
		}()
	}
	wg.Wait()
	control.End()
	if d.bgVac.Stats().DeferredVectorBuildActive {
		t.Fatal("final end left a deferred epoch active")
	}
}

func TestBackgroundIndexVacuumBacklogSkipResetsForegroundSkipCap(t *testing.T) {
	restoreForeground := setBackgroundIndexVacuumForegroundWriteQuietHookForTest(func(*DB) bool { return false })
	defer restoreForeground()
	var backlog atomic.Int64
	restoreBacklog := setBackgroundIndexVacuumBacklogBytesHookForTest(func(*DB) int64 { return backlog.Load() })
	defer restoreBacklog()
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	d.bgVac.maxBacklogSkips = 2
	d.bgVac.spanRatioPPM = ^uint32(0)
	counts, restoreCounts := installBackgroundVacuumProbeCounter()
	defer restoreCounts()

	d.bgVac.runOnce(d)
	if got := d.bgVac.foregroundConsecutiveSkips.Load(); got != 1 {
		t.Fatalf("foreground skips before backlog=%d want 1", got)
	}
	backlog.Store(1)
	d.bgVac.runOnce(d)
	if got := d.bgVac.foregroundConsecutiveSkips.Load(); got != 0 {
		t.Fatalf("foreground skips after backlog=%d want 0", got)
	}
	backlog.Store(0)
	d.bgVac.runOnce(d)
	d.bgVac.runOnce(d)
	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 0 {
		t.Fatalf("trigger probes before fresh foreground skip cap=%d want 0", got)
	}
	if got := d.bgVac.foregroundConsecutiveSkips.Load(); got != 2 {
		t.Fatalf("foreground skips after reset=%d want 2", got)
	}
}

func TestBackgroundIndexVacuumNativeRootWriteQuietTransitionRunsEligibleVacuum(t *testing.T) {
	var quiet atomic.Bool
	restoreForeground := setBackgroundIndexVacuumForegroundWriteQuietHookForTest(func(*DB) bool {
		return quiet.Load()
	})
	defer restoreForeground()
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	d.bgVac.spanRatioPPM = 1
	writeBackgroundVacuumNativeRoot(t, d)

	restoreProbe := setBackgroundIndexVacuumTriggerReportHookForTest(func(*DB, context.Context) (backenddb.IndexVacuumTriggerReport, error) {
		return backenddb.IndexVacuumTriggerReport{UserPages: 1, UserSpanRatioPPM: 1}, nil
	})
	defer restoreProbe()
	var vacuums atomic.Uint64
	restoreVacuum := setBackgroundIndexVacuumRunHookForTest(func(*DB, context.Context) (backenddb.VacuumOnlineStats, error) {
		vacuums.Add(1)
		return backenddb.VacuumOnlineStats{}, nil
	})
	defer restoreVacuum()

	d.bgVac.runOnce(d)
	if got := d.bgVac.foregroundSkips.Load(); got != 1 {
		t.Fatalf("foreground skips while write is hot=%d want 1", got)
	}
	quiet.Store(true)
	d.bgVac.runOnce(d)
	if got := d.bgVac.probes.Load(); got != 1 {
		t.Fatalf("trigger probes after quiet transition=%d want 1", got)
	}
	if got := vacuums.Load(); got != 1 {
		t.Fatalf("eligible vacuums after quiet transition=%d want 1", got)
	}
}

func writeBackgroundVacuumNativeRoot(t *testing.T, d *DB) {
	t.Helper()
	manager := collections.NewCollectionManager(d.backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "background-vacuum-native-root",
		Options: collections.CollectionOptions{
			DocumentFormat:          collections.DocumentFormatJSON,
			DataRootStoragePolicy:   collections.RootStorageCompressed,
			IndexStateStoragePolicy: collections.RootStorageCompressed,
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	collection, err := manager.OpenCollection("background-vacuum-native-root")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := collection.InsertBatch(
		[][]byte{[]byte("doc-1")},
		[][]byte{[]byte(`{"_id":"doc-1","value":1}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestBackgroundIndexVacuumFreelistDebtInvalidatesUnchangedCommitSkip(t *testing.T) {
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	seedBackgroundVacuumUserPages(t, d, 32)

	d.bgVac.spanRatioPPM = ^uint32(0)
	d.bgVac.freelistReclaimablePages = 1
	d.bgVac.freelistReclaimableRatioPPM = 1
	d.bgVac.collectionRootPages = ^uint64(0)
	d.bgVac.collectionRootSpanRatioPPM = ^uint32(0)
	counts, restoreCounts := installBackgroundVacuumProbeCounter()
	defer restoreCounts()

	d.bgVac.runOnce(d)
	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 1 {
		t.Fatalf("initial trigger probes=%d want 1", got)
	}
	changed := backenddb.IndexVacuumFreelistDebtSnapshot{
		TotalPages:               128,
		FreelistHead:             1,
		FreelistReclaimable:      d.bgVac.lastProbeFreelistReclaimable + 1,
		FreelistReclaimablePPM:   d.bgVac.lastProbeFreelistReclaimablePPM + 1,
		FreelistReclaimableValid: true,
	}
	restoreDebt := setBackgroundIndexVacuumFreelistDebtSnapshotHookForTest(func(*DB) (backenddb.IndexVacuumFreelistDebtSnapshot, bool) {
		return changed, true
	})
	defer restoreDebt()

	d.bgVac.runOnce(d)
	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 2 {
		t.Fatalf("trigger probes after freelist debt change=%d want 2", got)
	}
	assertNoBackgroundVacuumFullFragmentationWalks(t, counts)
}

func TestBackgroundIndexVacuumTriggerPredicatesIndependentDebt(t *testing.T) {
	w := bgIndexVacuumWorker{
		spanRatioPPM:                2_000_000,
		freelistReclaimableRatioPPM: 1_000_000,
		freelistReclaimablePages:    10,
		collectionRootSpanRatioPPM:  2_000_000,
		collectionRootPages:         10,
	}

	tests := []struct {
		name string
		rep  backenddb.IndexVacuumTriggerReport
		want string
	}{
		{
			name: "absent optional debt is none",
			rep: backenddb.IndexVacuumTriggerReport{
				UserPages:                    4,
				UserSpanRatioPPM:             1_000_000,
				FreelistReclaimablePages:     100,
				FreelistReclaimableRatioPPM:  4_000_000,
				CollectionRootPages:          100,
				CollectionRootSpanRatioPPM:   4_000_000,
				CollectionRootSpanRatioValid: false,
				FreelistReclaimableValid:     false,
			},
			want: backgroundIndexVacuumDebtReasonNone,
		},
		{
			name: "user tree debt",
			rep:  backenddb.IndexVacuumTriggerReport{UserPages: 4, UserSpanRatioPPM: 2_000_000},
			want: backgroundIndexVacuumDebtReasonUser,
		},
		{
			name: "freelist debt",
			rep: backenddb.IndexVacuumTriggerReport{
				UserPages:                   4,
				UserSpanRatioPPM:            1_000_000,
				FreelistReclaimableValid:    true,
				FreelistReclaimablePages:    10,
				FreelistReclaimableRatioPPM: 1_000_000,
			},
			want: backgroundIndexVacuumDebtReasonFreelist,
		},
		{
			name: "collection root debt",
			rep: backenddb.IndexVacuumTriggerReport{
				UserPages:                    4,
				UserSpanRatioPPM:             1_000_000,
				CollectionRootSpanRatioValid: true,
				CollectionRootPages:          10,
				CollectionRootSpanRatioPPM:   2_000_000,
			},
			want: backgroundIndexVacuumDebtReasonCollectionRoots,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := w.triggerReason(tc.rep); got != tc.want {
				t.Fatalf("triggerReason=%q want %q", got, tc.want)
			}
		})
	}
}

func TestBackgroundIndexVacuumFreelistDebtTriggersVacuum(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d := openBackgroundVacuumTestDB(t, Options{
		KeepRecent:                    1,
		PreferAppendAlloc:             true,
		BackgroundIndexVacuumInterval: -1,
	})
	seedBackgroundVacuumFreelistDebt(t, d)

	rep, err := d.backend.IndexVacuumTriggerReport()
	if err != nil {
		t.Fatalf("trigger report: %v", err)
	}
	if !rep.FreelistReclaimableValid || rep.FreelistReclaimablePages == 0 || rep.FreelistReclaimableRatioPPM == 0 {
		t.Fatalf("freelist debt not present in trigger report: %+v", rep)
	}

	d.bgVac.spanRatioPPM = ^uint32(0)
	d.bgVac.freelistReclaimablePages = 1
	d.bgVac.freelistReclaimableRatioPPM = 1
	d.bgVac.collectionRootPages = ^uint64(0)
	d.bgVac.collectionRootSpanRatioPPM = ^uint32(0)
	d.bgVac.runOnce(d)

	if got := d.bgVac.vacuums.Load(); got != 1 {
		t.Fatalf("vacuums=%d want 1", got)
	}
	if got := lastBackgroundVacuumDebtReason(t, &d.bgVac); got != backgroundIndexVacuumDebtReasonFreelist {
		t.Fatalf("last debt reason=%q want freelist", got)
	}
}

func TestBackgroundIndexVacuumCollectionRootDebtTriggersVacuum(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d := openBackgroundVacuumTestDB(t, Options{
		KeepRecent:                    1,
		Durability:                    DurabilityWALOffRelaxed,
		ResolvedProfile:               backenddb.ProfileNoWALFast,
		BackgroundIndexVacuumInterval: -1,
	})
	seedBackgroundVacuumCollectionRootDebt(t, d)

	rep, err := d.backend.IndexVacuumTriggerReport()
	if err != nil {
		t.Fatalf("trigger report: %v", err)
	}
	if !rep.CollectionRootSpanRatioValid || rep.CollectionRootPages == 0 || rep.CollectionRootSpanRatioPPM == 0 {
		t.Fatalf("collection root debt not present in trigger report: %+v", rep)
	}

	d.bgVac.spanRatioPPM = ^uint32(0)
	d.bgVac.freelistReclaimablePages = ^uint64(0)
	d.bgVac.freelistReclaimableRatioPPM = ^uint32(0)
	d.bgVac.collectionRootPages = 1
	d.bgVac.collectionRootSpanRatioPPM = 1
	d.bgVac.runOnce(d)

	if got := d.bgVac.vacuums.Load(); got != 1 {
		t.Fatalf("vacuums=%d want 1", got)
	}
	if got := lastBackgroundVacuumDebtReason(t, &d.bgVac); got != backgroundIndexVacuumDebtReasonCollectionRoots {
		t.Fatalf("last debt reason=%q want collection_roots", got)
	}
}

func installBackgroundVacuumProbeCounter() (map[backenddb.FragmentationProbeEvent]int, func()) {
	counts := make(map[backenddb.FragmentationProbeEvent]int)
	restore := backenddb.SetFragmentationProbeHookForTest(func(event backenddb.FragmentationProbeEvent) {
		counts[event]++
	})
	return counts, restore
}

func assertNoBackgroundVacuumStructuralWalks(t *testing.T, counts map[backenddb.FragmentationProbeEvent]int) {
	t.Helper()
	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 0 {
		t.Fatalf("cheap trigger reports=%d want 0", got)
	}
	if got := counts[backenddb.FragmentationProbeEventTriggerUserTreeWalk]; got != 0 {
		t.Fatalf("cheap trigger user-tree walks=%d want 0", got)
	}
	if got := counts[backenddb.FragmentationProbeEventTriggerFreelistCounters]; got != 0 {
		t.Fatalf("cheap trigger freelist counter reads=%d want 0", got)
	}
	if got := counts[backenddb.FragmentationProbeEventTriggerCollectionRootWalk]; got != 0 {
		t.Fatalf("cheap trigger collection-root walks=%d want 0", got)
	}
	assertNoBackgroundVacuumFullFragmentationWalks(t, counts)
}

func assertNoBackgroundVacuumFullFragmentationWalks(t *testing.T, counts map[backenddb.FragmentationProbeEvent]int) {
	t.Helper()
	if got := counts[backenddb.FragmentationProbeEventFullReport]; got != 0 {
		t.Fatalf("full fragmentation reports=%d want 0", got)
	}
	if got := counts[backenddb.FragmentationProbeEventFullUserTreeWalk]; got != 0 {
		t.Fatalf("full user-tree walks=%d want 0", got)
	}
	if got := counts[backenddb.FragmentationProbeEventFullFreelistChainWalk]; got != 0 {
		t.Fatalf("full freelist-chain walks=%d want 0", got)
	}
	if got := counts[backenddb.FragmentationProbeEventFullCollectionRootWalk]; got != 0 {
		t.Fatalf("full collection-root walks=%d want 0", got)
	}
}

func TestBackgroundIndexVacuumStartsWhenConfigured(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                               dir,
		KeepRecent:                        1,
		BackgroundIndexVacuumInterval:     time.Hour,
		BackgroundIndexVacuumSpanRatioPPM: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	d.bgVac.lastOnlineVacuum.Store(&backenddb.VacuumOnlineStats{
		DirtyDescriptors:         11,
		UserTailMutations:        12,
		UserTailPointMutations:   13,
		UserTailRangeMutations:   14,
		DeferredCutovers:         15,
		ConcurrentMutationAborts: 16,
	})

	stats := d.Stats()
	if got := stats["treedb.bg_vacuum.enabled"]; got != "true" {
		t.Fatalf("background vacuum enabled=%q want true", got)
	}
	if got := stats["treedb.bg_vacuum.vacuums"]; got != "0" {
		t.Fatalf("background vacuum runs=%q want 0 before first tick", got)
	}
	for key, want := range map[string]string{
		"treedb.bg_vacuum.last_online.dirty_descriptors":          "11",
		"treedb.bg_vacuum.last_online.user_tail_mutations":        "12",
		"treedb.bg_vacuum.last_online.user_tail_point_mutations":  "13",
		"treedb.bg_vacuum.last_online.user_tail_range_mutations":  "14",
		"treedb.bg_vacuum.last_online.deferred_cutovers":          "15",
		"treedb.bg_vacuum.last_online.concurrent_mutation_aborts": "16",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("%s=%q want %q", key, got, want)
		}
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestBackgroundIndexVacuumDisabledIntervalStartsNoWorker(t *testing.T) {
	d, err := Open(Options{
		Dir:                           t.TempDir(),
		BackgroundIndexVacuumInterval: -1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	stats := d.Stats()
	if got := stats["treedb.bg_vacuum.enabled"]; got != "false" {
		t.Fatalf("background vacuum enabled=%q want false", got)
	}
	if got := stats["treedb.bg_vacuum.runs"]; got != "0" {
		t.Fatalf("background vacuum runs=%q want 0", got)
	}
	if got := stats["treedb.bg_vacuum.trigger_probes"]; got != "0" {
		t.Fatalf("background vacuum probes=%q want 0", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestBackgroundIndexVacuumErrorOutcomes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	tests := []struct {
		name                  string
		err                   error
		wantRetryReason       string
		wantOutcome           string
		wantConcurrent        uint64
		wantRecoverableRoots  uint64
		wantCheckpointCleanup uint64
		wantResourcePinned    uint64
		wantUnsupported       uint64
		wantPermanent         uint64
		wantReports           int64
	}{
		{
			name:            "concurrent mutation",
			err:             backenddb.ErrVacuumConcurrentMutation,
			wantRetryReason: backgroundIndexVacuumRetryReasonConcurrentMutation,
			wantOutcome:     backgroundIndexVacuumOutcomeRetry,
			wantConcurrent:  1,
		},
		{
			name:                 "stale recoverable roots",
			err:                  backenddb.ErrRecoverableRootSetStale,
			wantRetryReason:      backgroundIndexVacuumRetryReasonRecoverableRootSet,
			wantOutcome:          backgroundIndexVacuumOutcomeRetry,
			wantRecoverableRoots: 1,
		},
		{
			name:                  "stale checkpoint cleanup",
			err:                   backenddb.ErrDurableWALCleanupProofStale,
			wantRetryReason:       backgroundIndexVacuumRetryReasonCheckpointCleanup,
			wantOutcome:           backgroundIndexVacuumOutcomeRetry,
			wantCheckpointCleanup: 1,
		},
		{
			name:               "pinned stable resource",
			err:                rootpublication.ErrResourcePinned,
			wantRetryReason:    backgroundIndexVacuumRetryReasonResourcePinned,
			wantOutcome:        backgroundIndexVacuumOutcomeRetry,
			wantResourcePinned: 1,
		},
		{
			name:            "unsupported",
			err:             backenddb.ErrVacuumUnsupported,
			wantRetryReason: backgroundIndexVacuumRetryReasonNone,
			wantOutcome:     backgroundIndexVacuumOutcomeUnsupported,
			wantUnsupported: 1,
		},
		{
			name:            "permanent",
			err:             errors.New("vacuum I/O failure"),
			wantRetryReason: backgroundIndexVacuumRetryReasonNone,
			wantOutcome:     backgroundIndexVacuumOutcomePermanentFailure,
			wantPermanent:   1,
			wantReports:     1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var reports atomic.Int64
			d := openBackgroundVacuumTestDB(t, Options{
				BackgroundIndexVacuumInterval: -1,
				NotifyError: func(error) {
					reports.Add(1)
				},
			})
			seedBackgroundVacuumUserPages(t, d, 64)
			d.bgVac.spanRatioPPM = 1
			d.bgVac.freelistReclaimablePages = ^uint64(0)
			d.bgVac.collectionRootPages = ^uint64(0)

			var calls atomic.Int64
			restore := setBackgroundIndexVacuumRunHookForTest(func(*DB, context.Context) (backenddb.VacuumOnlineStats, error) {
				calls.Add(1)
				return backenddb.VacuumOnlineStats{}, tc.err
			})
			defer restore()

			d.bgVac.runOnce(d)
			stats := d.bgVac.Stats()
			if stats.LastRetryReason != tc.wantRetryReason || stats.LastOutcome != tc.wantOutcome {
				t.Fatalf("retry=%q outcome=%q want retry=%q outcome=%q", stats.LastRetryReason, stats.LastOutcome, tc.wantRetryReason, tc.wantOutcome)
			}
			if stats.RetryConcurrentMutationTotal != tc.wantConcurrent ||
				stats.RetryRecoverableRootSetTotal != tc.wantRecoverableRoots ||
				stats.RetryCheckpointCleanupTotal != tc.wantCheckpointCleanup ||
				stats.RetryResourcePinnedTotal != tc.wantResourcePinned ||
				stats.UnsupportedTotal != tc.wantUnsupported ||
				stats.PermanentFailuresTotal != tc.wantPermanent {
				t.Fatalf("unexpected counters: %+v", stats)
			}
			if got := reports.Load(); got != tc.wantReports {
				t.Fatalf("NotifyError calls=%d want %d", got, tc.wantReports)
			}
			publicStats := d.Stats()
			if got := publicStats["treedb.bg_vacuum.last_retry_reason"]; got != tc.wantRetryReason {
				t.Fatalf("public last retry reason=%q want %q", got, tc.wantRetryReason)
			}
			if got := publicStats["treedb.bg_vacuum.last_outcome"]; got != tc.wantOutcome {
				t.Fatalf("public last outcome=%q want %q", got, tc.wantOutcome)
			}
			if got := publicStats["treedb.bg_vacuum.retry_resource_pinned_total"]; got != fmt.Sprint(tc.wantResourcePinned) {
				t.Fatalf("public pinned retry total=%q want %d", got, tc.wantResourcePinned)
			}

			if tc.wantUnsupported != 0 || tc.wantPermanent != 0 {
				d.bgVac.runOnce(d)
				if got := calls.Load(); got != 1 {
					t.Fatalf("vacuum calls after terminal unchanged outcome=%d want 1", got)
				}
				if got := reports.Load(); got != tc.wantReports {
					t.Fatalf("NotifyError calls after unchanged tick=%d want %d", got, tc.wantReports)
				}
				if got := d.bgVac.Stats().LastErr; got != tc.err.Error() {
					t.Fatalf("retained last error=%q want %q", got, tc.err.Error())
				}
			} else if tc.wantResourcePinned != 0 {
				d.bgVac.runOnce(d)
				if got := calls.Load(); got != 2 {
					t.Fatalf("vacuum calls after pinned retry=%d want 2", got)
				}
				if got := reports.Load(); got != 0 {
					t.Fatalf("NotifyError calls after pinned retry=%d want 0", got)
				}
			}
		})
	}
}

func TestBackgroundIndexVacuumPermanentProbeErrorReportedOncePerState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	var reports atomic.Int64
	d := openBackgroundVacuumTestDB(t, Options{
		BackgroundIndexVacuumInterval: -1,
		NotifyError: func(error) {
			reports.Add(1)
		},
	})

	probeErr := errors.New("trigger report I/O failure")
	var probes atomic.Int64
	restore := setBackgroundIndexVacuumTriggerReportHookForTest(func(*DB, context.Context) (backenddb.IndexVacuumTriggerReport, error) {
		probes.Add(1)
		return backenddb.IndexVacuumTriggerReport{}, probeErr
	})
	defer restore()

	d.bgVac.runOnce(d)
	d.bgVac.runOnce(d)
	if got := probes.Load(); got != 1 {
		t.Fatalf("unchanged-state trigger probes=%d want 1", got)
	}
	if got := reports.Load(); got != 1 {
		t.Fatalf("unchanged-state NotifyError calls=%d want 1", got)
	}
	stats := d.bgVac.Stats()
	if stats.PermanentFailuresTotal != 1 || stats.LastOutcome != backgroundIndexVacuumOutcomeUnchanged || stats.LastErr != probeErr.Error() {
		t.Fatalf("unexpected unchanged-state stats: %+v", stats)
	}

	if err := d.Set([]byte("new-state"), []byte("value")); err != nil {
		t.Fatalf("mutate state: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint new state: %v", err)
	}
	d.bgVac.runOnce(d)
	if got := probes.Load(); got != 2 {
		t.Fatalf("changed-state trigger probes=%d want 2", got)
	}
	if got := reports.Load(); got != 2 {
		t.Fatalf("changed-state NotifyError calls=%d want 2", got)
	}
}

func TestBackgroundIndexVacuumReadOnlyStartsNoWorker(t *testing.T) {
	dir := t.TempDir()
	writable, err := Open(Options{Dir: dir, BackgroundIndexVacuumInterval: -1})
	if err != nil {
		t.Fatalf("open writable fixture: %v", err)
	}
	if err := writable.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := writable.Close(); err != nil {
		t.Fatalf("close writable fixture: %v", err)
	}

	readOnly, err := Open(Options{
		Dir:                           dir,
		ReadOnly:                      true,
		BackgroundIndexVacuumInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("open read-only: %v", err)
	}
	if got := readOnly.Stats()["treedb.bg_vacuum.enabled"]; got != "false" {
		t.Fatalf("read-only background vacuum enabled=%q want false", got)
	}
	if err := readOnly.Close(); err != nil {
		t.Fatalf("close read-only: %v", err)
	}
}

func TestBackgroundIndexVacuumCloseCancelsActivePass(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	restoreForeground := setBackgroundIndexVacuumForegroundWriteQuietHookForTest(func(db *DB) bool {
		return db == nil || db.cached == nil || db.cached.BackgroundVacuumForegroundWriteQuiet()
	})
	defer restoreForeground()
	d, err := Open(Options{
		Dir:                               t.TempDir(),
		BackgroundIndexVacuumInterval:     time.Hour,
		BackgroundIndexVacuumSpanRatioPPM: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	seedBackgroundVacuumUserPages(t, d, 64)
	d.bgVac.maxBacklogSkips = 1
	writeBackgroundVacuumNativeRoot(t, d)
	d.bgVac.runOnce(d)
	if got := d.bgVac.foregroundSkips.Load(); got != 1 {
		t.Fatalf("foreground skips before forced close pass=%d want 1", got)
	}

	started := make(chan struct{})
	restore := setBackgroundIndexVacuumRunHookForTest(func(_ *DB, ctx context.Context) (backenddb.VacuumOnlineStats, error) {
		close(started)
		<-ctx.Done()
		return backenddb.VacuumOnlineStats{}, ctx.Err()
	})
	defer restore()

	d.bgVac.Kick()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("background vacuum did not start")
	}

	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel active background vacuum")
	}
}

func TestVacuumIndexOnlineContextCancelsWhileWaitingForMaintenance(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	d.maintenance.mu.Lock()
	defer d.maintenance.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.VacuumIndexOnline(ctx) }()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("VacuumIndexOnline error=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("VacuumIndexOnline did not cancel while waiting for maintenance")
	}
}

func TestLockFullScanMaintenanceContextCancellationLeavesNoWaiters(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()

	const waiters = 64
	contexts := make([]context.CancelFunc, waiters)
	done := make(chan error, waiters)
	var callers sync.WaitGroup
	for i := range contexts {
		ctx, cancel := context.WithCancel(context.Background())
		contexts[i] = cancel
		callers.Add(1)
		go func() {
			defer callers.Done()
			done <- lockFullScanMaintenanceContext(ctx, &mu)
		}()
	}
	time.Sleep(20 * time.Millisecond)
	for _, cancel := range contexts {
		cancel()
	}
	for range contexts {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("lock wait error=%v, want context.Canceled", err)
		}
	}
	callers.Wait()

	mu.Unlock()
	if !mu.TryLock() {
		t.Fatal("canceled maintenance lock calls left queued mutex waiters")
	}
	mu.Unlock()
}

func TestLockFullScanMaintenanceContextRejectsCanceledContextBeforeIdleLock(t *testing.T) {
	var mu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := lockFullScanMaintenanceContext(ctx, &mu); !errors.Is(err, context.Canceled) {
		t.Fatalf("lock error=%v, want context.Canceled", err)
	}
	if !mu.TryLock() {
		t.Fatal("canceled maintenance lock call retained idle mutex")
	}
	mu.Unlock()

	d := &DB{}
	if _, _, err := d.beginFullScanMaintenanceContext(ctx, "vacuum"); !errors.Is(err, context.Canceled) {
		t.Fatalf("begin maintenance error=%v, want context.Canceled", err)
	}
	if !d.maintenance.mu.TryLock() {
		t.Fatal("canceled maintenance fast path retained mutex")
	}
	d.maintenance.mu.Unlock()
}

func TestVacuumIndexOnlineDoesNotHoldLifecycleWhileWaitingForMaintenance(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d := openBackgroundVacuumTestDB(t, Options{BackgroundIndexVacuumInterval: -1})
	d.maintenance.mu.Lock()

	ctx, cancel := context.WithCancel(context.Background())
	vacuumDone := make(chan error, 1)
	go func() { vacuumDone <- d.VacuumIndexOnline(ctx) }()
	time.Sleep(25 * time.Millisecond)

	lifecycleAcquired := make(chan struct{})
	releaseLifecycle := make(chan struct{})
	go func() {
		d.lifecycleMu.Lock()
		close(lifecycleAcquired)
		<-releaseLifecycle
		d.lifecycleMu.Unlock()
	}()
	select {
	case <-lifecycleAcquired:
	case <-time.After(100 * time.Millisecond):
		cancel()
		d.maintenance.mu.Unlock()
		<-lifecycleAcquired
		close(releaseLifecycle)
		<-vacuumDone
		t.Fatal("lifecycle writer blocked behind vacuum waiting for maintenance")
	}

	cancel()
	close(releaseLifecycle)
	d.maintenance.mu.Unlock()
	select {
	case err := <-vacuumDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("VacuumIndexOnline error=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("vacuum did not cancel after maintenance released")
	}
}

func BenchmarkBackgroundIndexVacuumBacklog(b *testing.B) {
	d := openBackgroundVacuumTestDB(b, Options{BackgroundIndexVacuumInterval: -1})
	seedBackgroundVacuumUserPages(b, d, 128)
	d.bgVac.maxBacklogSkips = ^uint32(0)
	restoreBacklog := setBackgroundIndexVacuumBacklogBytesHookForTest(func(*DB) int64 { return 1024 })
	defer restoreBacklog()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.bgVac.runOnce(d)
	}
}

func BenchmarkBackgroundIndexVacuumDeferredVectorBuild(b *testing.B) {
	d := openBackgroundVacuumTestDB(b, Options{BackgroundIndexVacuumInterval: -1})
	control := &DeferredVectorBuildMaintenance{db: d}
	epochID := control.AdmitInsert(context.Background(), "docs", 1)
	if epochID == 0 || !control.CommitInsert(epochID) {
		b.Fatal("begin deferred vector-build epoch")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.bgVac.runOnce(d)
	}
}

func BenchmarkBackgroundIndexVacuumTrigger(b *testing.B) {
	d := openBackgroundVacuumTestDB(b, Options{BackgroundIndexVacuumInterval: -1})
	seedBackgroundVacuumUserPages(b, d, 2048)
	d.bgVac.spanRatioPPM = ^uint32(0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.bgVac.lastProbeValid = false
		d.bgVac.runOnce(d)
	}
}

func openBackgroundVacuumTestDB(tb testing.TB, opts Options) *DB {
	tb.Helper()
	if opts.Dir == "" {
		opts.Dir = tb.TempDir()
	}
	if opts.BackgroundIndexVacuumInterval == 0 {
		opts.BackgroundIndexVacuumInterval = -1
	}
	d, err := Open(opts)
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	tb.Cleanup(func() { _ = d.Close() })
	return d
}

func seedBackgroundVacuumUserPages(tb testing.TB, d *DB, n int) {
	tb.Helper()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("bg-seed-%04d", i))
		if err := d.Set(key, []byte("value")); err != nil {
			tb.Fatalf("set seed: %v", err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		tb.Fatalf("checkpoint seed: %v", err)
	}
}

func seedBackgroundVacuumFreelistDebt(tb testing.TB, d *DB) {
	tb.Helper()
	for i := 0; i < 512; i++ {
		key := []byte(fmt.Sprintf("free-debt-%04d", i))
		if err := d.backend.Set(key, []byte("value")); err != nil {
			tb.Fatalf("backend set freelist fixture: %v", err)
		}
	}
	for i := 0; i < 512; i++ {
		key := []byte(fmt.Sprintf("free-debt-%04d", i))
		if err := d.backend.Delete(key); err != nil {
			tb.Fatalf("backend delete freelist fixture: %v", err)
		}
	}
}

func seedBackgroundVacuumCollectionRootDebt(tb testing.TB, d *DB) {
	tb.Helper()
	kvs := make([]string, 0, 4096)
	for i := 0; i < 2048; i++ {
		kvs = append(kvs, fmt.Sprintf("doc/%04d", i), fmt.Sprintf("value-%04d", i))
	}
	rootID, err := d.backend.PublishOrderedRootIterator(0, mustBackgroundVacuumStringMemtable(tb, kvs...).NewIterator(nil, nil))
	if err != nil {
		tb.Fatalf("publish collection root fixture: %v", err)
	}
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], rootID)
	_, err = d.backend.PublishSystemRootIterator(mustBackgroundVacuumBytesMemtable(tb, "collections/root/bg/primary", encoded[:]).NewIterator(nil, nil))
	if err != nil {
		tb.Fatalf("publish collection descriptor fixture: %v", err)
	}
}

func mustBackgroundVacuumStringMemtable(tb testing.TB, kvs ...string) memtable.Table {
	tb.Helper()
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		tb.Fatalf("new memtable: %v", err)
	}
	for i := 0; i+1 < len(kvs); i += 2 {
		mt.Set([]byte(kvs[i]), []byte(kvs[i+1]))
	}
	mt.Freeze()
	return mt
}

func mustBackgroundVacuumBytesMemtable(tb testing.TB, key string, value []byte) memtable.Table {
	tb.Helper()
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		tb.Fatalf("new memtable: %v", err)
	}
	mt.Set([]byte(key), value)
	mt.Freeze()
	return mt
}

func lastBackgroundVacuumDebtReason(t *testing.T, w *bgIndexVacuumWorker) string {
	t.Helper()
	v := w.lastDebtReason.Load()
	if v == nil {
		return ""
	}
	reason, _ := v.(string)
	return reason
}
