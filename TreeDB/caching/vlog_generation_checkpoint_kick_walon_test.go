package caching

import (
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestVlogGenerationCheckpointKick_SkipsWhenWALOn(t *testing.T) {
	t.Setenv(envDisableVlogGenerationCheckpointKick, "0")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	db, err := Open(dir, backend, Options{
		DisableWAL:               false,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
	})
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer db.Close()

	db.testSkipVlogCheckpointKick = false
	db.maybeKickVlogGenerationMaintenanceAfterCheckpoint()

	if got := db.vlogGenerationCheckpointKickRuns.Load(); got != 0 {
		t.Fatalf("checkpoint kick runs=%d want 0", got)
	}
	if db.vlogGenerationCheckpointKickActive.Load() {
		t.Fatal("checkpoint kick unexpectedly active")
	}
	if db.vlogGenerationCheckpointKickPending.Load() {
		t.Fatal("checkpoint kick unexpectedly pending")
	}
}

func TestVlogGenerationCheckpointKick_WALOnSteadyResumeRunsBoundedMaintenance(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envDisableVlogGenerationCheckpointKick, "0")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 11, BytesTotal: 64, BytesLive: 32, BytesStale: 32, StaleRatio: 0.5},
			},
			SegmentsSelected:   1,
			SelectedBytesTotal: 64,
			SelectedBytesLive:  32,
			SelectedBytesStale: 32,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       false,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)

	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.testSkipVlogCheckpointKick = false
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(time.Now().Add(-time.Second).UnixNano())
	forceVlogMaintenanceIdle(db)

	db.SetMaintenancePhase(MaintenancePhaseRestore)
	db.SetMaintenancePhase(MaintenancePhaseSteady)

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls >= 1 {
			break
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			t.Fatalf("rewrite calls=%d want >=1", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := db.vlogGenerationCheckpointKickRuns.Load(); got != 0 {
		t.Fatalf("checkpoint kick runs=%d want 0 for wal_on steady resume path", got)
	}
	if got := db.vlogGenerationWALOnSteadyResumeAttempts.Load(); got != 1 {
		t.Fatalf("steady resume attempts=%d want 1", got)
	}
	if got := db.vlogGenerationWALOnSteadyResumeRewriteRuns.Load(); got != 1 {
		t.Fatalf("steady resume rewrite runs=%d want 1", got)
	}
	if got := db.vlogGenerationWALOnSteadyResumeRemainingAttempts.Load(); got != 0 {
		t.Fatalf("steady resume remaining=%d want 0 after rewrite", got)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.wal_on_steady_resume"]; got != "1" {
		t.Fatalf("maintenance acquired source wal_on_steady_resume=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.wal_on_steady_resume"]; got != "1" {
		t.Fatalf("maintenance rewrite passes source wal_on_steady_resume=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.wal_on_steady_resume.attempts"]; got != "1" {
		t.Fatalf("steady resume stats attempts=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.wal_on_steady_resume.rewrite_runs"]; got != "1" {
		t.Fatalf("steady resume stats rewrite runs=%q want 1", got)
	}
}

func TestVlogGenerationCheckpointKick_WALOnSteadyResumeIsBounded(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envDisableVlogGenerationCheckpointKick, "0")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:           backend,
		planResponse: backenddb.ValueLogRewritePlan{},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       false,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)

	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.testSkipVlogCheckpointKick = false
	db.vlogGenerationRewriteBudgetTokensBytes.Store(0)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(0)
	forceVlogMaintenanceIdle(db)

	db.SetMaintenancePhase(MaintenancePhaseCatchUp)
	db.SetMaintenancePhase(MaintenancePhaseSteady)

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if got := db.vlogGenerationWALOnSteadyResumeAttempts.Load(); got >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("steady resume attempt 1 did not run")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want 0 on budget-seeding attempt", calls)
	}
	if got := db.vlogGenerationWALOnSteadyResumeRemainingAttempts.Load(); got != 1 {
		t.Fatalf("steady resume remaining=%d want 1 after budget-seeding attempt", got)
	}
	deadline = time.Now().Add(2 * schedulerTestWait(t))
	for db.vlogGenerationWALOnSteadyResumeActive.Load() {
		if time.Now().After(deadline) {
			t.Fatal("steady resume attempt 1 did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(time.Now().Add(-time.Second).UnixNano())
	db.maybeKickVlogGenerationMaintenanceAfterCheckpoint()
	deadline = time.Now().Add(2 * schedulerTestWait(t))
	for {
		if got := db.vlogGenerationWALOnSteadyResumeAttempts.Load(); got >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("steady resume attempt 2 did not run")
		}
		time.Sleep(10 * time.Millisecond)
	}

	db.maybeKickVlogGenerationMaintenanceAfterCheckpoint()
	time.Sleep(50 * time.Millisecond)

	if got := db.vlogGenerationWALOnSteadyResumeAttempts.Load(); got != uint64(vlogGenerationWALOnSteadyResumeAttempts) {
		t.Fatalf("steady resume attempts=%d want %d", got, vlogGenerationWALOnSteadyResumeAttempts)
	}
	if got := db.vlogGenerationWALOnSteadyResumeRemainingAttempts.Load(); got != 0 {
		t.Fatalf("steady resume remaining=%d want 0 after bounded attempts", got)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want 0 for empty-plan bounded test", calls)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.wal_on_steady_resume"]; got != "2" {
		t.Fatalf("maintenance acquired source wal_on_steady_resume=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_empty"]; got != "1" {
		t.Fatalf("rewrite plan empty=%q want 1 after second attempt", got)
	}
}

func TestVlogGenerationCheckpointKick_WALOnSteadyResumeExhaustedStillReachesStageConfirm(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envDisableVlogGenerationCheckpointKick, "0")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 11, BytesTotal: 64, BytesLive: 32, BytesStale: 32, StaleRatio: 0.5},
			},
			SegmentsSelected:   1,
			SelectedBytesTotal: 64,
			SelectedBytesLive:  32,
			SelectedBytesStale: 32,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       false,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)

	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.testSkipVlogCheckpointKick = false
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(time.Now().Add(-time.Second).UnixNano())
	forceVlogMaintenanceIdle(db)

	db.SetMaintenancePhase(MaintenancePhaseSteady)
	db.vlogGenerationWALOnSteadyResumeRemainingAttempts.Store(0)
	observedAt := time.Now().Add(-2 * vlogGenerationRewriteStageConfirmDelay).UnixNano()
	if err := db.setVlogGenerationRewriteLedgerWithStage([]backenddb.ValueLogRewritePlanSegment{{
		FileID:     11,
		BytesTotal: 64,
		BytesLive:  32,
		BytesStale: 32,
		StaleRatio: 0.5,
	}}, true, observedAt); err != nil {
		t.Fatalf("stage rewrite ledger: %v", err)
	}
	db.scheduleDueVlogGenerationDeferredMaintenance()

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stage-confirm rewrite did not run after steady-resume window was exhausted")
		}
		time.Sleep(10 * time.Millisecond)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.rewrite_stage_confirm"]; got == "0" {
		t.Fatalf("maintenance acquired source rewrite_stage_confirm=%q want >0", got)
	}
}
