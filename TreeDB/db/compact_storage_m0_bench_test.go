package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

type compactStorageM0FixtureSpec struct {
	metadata               compactStorageMeasurementFixture
	open                   func(*testing.B) *DB
	options                CompactStorageOptions
	minPackRuns            int
	expectRewrite          bool
	expectVacuumRun        bool
	minIndexShrinkRatioPPM uint64
	foreground             bool
}

var compactStorageM0FixtureSpecs = []compactStorageM0FixtureSpec{
	{
		metadata: compactStorageMeasurementFixture{
			Name: "one-generation-per-pass", Seed: 3733001,
			KeyDistribution:   "four-overwrite-generations-plus-generation-unique-keys",
			ValueDistribution: "fixed-256", ValueLogPointerThreshold: page.DefaultInlineThreshold,
			WALMode: "off-relaxed", ExpectedMaintenanceWork: "at least three forced one-generation leaf-pack passes",
		},
		open: func(b *testing.B) *DB { return openCompactStorageLeafPackBenchmarkFixture(b) },
		options: CompactStorageOptions{
			Mode: CompactStorageFull, LeafPackMaxPasses: 4,
			LeafPackMaxGenerationsPerPass: 1, LeafPackMinReclaimPerCopyPPM: 1,
		},
		minPackRuns:     3,
		expectVacuumRun: true,
	},
	{
		metadata: compactStorageMeasurementFixture{
			Name: "full-default", Seed: 3733002,
			KeyDistribution:   "four-overwrite-generations-plus-generation-unique-keys",
			ValueDistribution: "fixed-256", ValueLogPointerThreshold: page.DefaultInlineThreshold,
			WALMode: "off-relaxed", ExpectedMaintenanceWork: "default Full generation-unbounded leaf-pack with 1GiB byte cap",
		},
		open:            func(b *testing.B) *DB { return openCompactStorageLeafPackBenchmarkFixture(b) },
		options:         CompactStorageOptions{Mode: CompactStorageFull},
		minPackRuns:     1,
		expectVacuumRun: true,
	},
	{
		metadata: compactStorageMeasurementFixture{
			Name: "full-low-debt", Seed: 3733003,
			KeyDistribution:   "2047-live-1-stale-value-log-records",
			ValueDistribution: "fixed-1024", ValueLogPointerThreshold: 1,
			WALMode: "default", ExpectedMaintenanceWork: "Full audit with no value-log rewrite or index vacuum selected",
		},
		open: func(b *testing.B) *DB { return openCompactStorageRewritePolicyBenchmarkFixture(b, 2047, 1, 1024) },
		options: CompactStorageOptions{
			Mode: CompactStorageFull, DisableZeroByteValueLogCleanup: true,
		},
	},
	{
		metadata: compactStorageMeasurementFixture{
			Name: "full-high-debt", Seed: 3733004,
			KeyDistribution:   "vacuum-m0 user and collection generations with retired index pages",
			ValueDistribution: "mixed-inline-and-2048-byte-pointers", ValueLogPointerThreshold: 512,
			WALMode: "default", ExpectedMaintenanceWork: "Full production index vacuum with at least 40 percent index.db shrink",
		},
		open: func(b *testing.B) *DB {
			d, _ := openVacuumM0Fixture(b, vacuumM0Options(b.TempDir()))
			return d
		},
		options: CompactStorageOptions{
			Mode: CompactStorageFull, DisableZeroByteValueLogCleanup: true,
		},
		expectVacuumRun:        true,
		minIndexShrinkRatioPPM: 400_000,
	},
	{
		metadata: compactStorageMeasurementFixture{
			Name: "exhaustive-control", Seed: 3733005,
			KeyDistribution:   "four-overwrite-generations-plus-generation-unique-keys",
			ValueDistribution: "fixed-256", ValueLogPointerThreshold: page.DefaultInlineThreshold,
			WALMode: "off-relaxed", ExpectedMaintenanceWork: "Exhaustive mode processes all eligible leaf generations",
		},
		open:            func(b *testing.B) *DB { return openCompactStorageLeafPackBenchmarkFixture(b) },
		options:         CompactStorageOptions{Mode: CompactStorageExhaustive},
		minPackRuns:     1,
		expectVacuumRun: true,
	},
	{
		metadata: compactStorageMeasurementFixture{
			Name: "foreground-writes", Seed: 3733006,
			KeyDistribution:   "1024-live-2048-stale-value-log-records-plus-32-point-writes",
			ValueDistribution: "fixed-1024-maintenance-fixed-32-foreground", ValueLogPointerThreshold: 4096,
			WALMode: "default", ExpectedMaintenanceWork: "high-debt Full rewrite concurrent with 32 point writes and matched idle control",
		},
		open: func(b *testing.B) *DB {
			return openCompactStorageRewritePolicyBenchmarkFixtureWithThreshold(b, 1024, 2048, 1024, 4096)
		},
		options: CompactStorageOptions{
			Mode: CompactStorageFull, DisableZeroByteValueLogCleanup: true,
		},
		expectRewrite:   true,
		expectVacuumRun: true,
		foreground:      true,
	},
}

var compactStorageM0Fixtures = func() []compactStorageMeasurementFixture {
	out := make([]compactStorageMeasurementFixture, len(compactStorageM0FixtureSpecs))
	for i := range compactStorageM0FixtureSpecs {
		out[i] = compactStorageM0FixtureSpecs[i].metadata
	}
	return out
}()

func BenchmarkCompactStorageM0(b *testing.B) {
	for i := range compactStorageM0FixtureSpecs {
		spec := compactStorageM0FixtureSpecs[i]
		b.Run(spec.metadata.Name, func(b *testing.B) {
			benchmarkCompactStorageM0Fixture(b, spec)
		})
	}
}

func BenchmarkCompactStorageIndexVacuumDecisionNoDebt(b *testing.B) {
	db := openCompactStorageRewritePolicyBenchmarkFixture(b, 2047, 1, 1024)
	b.Cleanup(func() { _ = db.Close() })
	opts := CompactStorageOptions{Mode: CompactStorageFull}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		debt, err := db.compactStorageIndexVacuumDebt(ctx, opts)
		if err != nil {
			b.Fatal(err)
		}
		if debt.IndexVacuumRequired {
			b.Fatalf("unexpected no-debt vacuum plan: %+v", debt)
		}
	}
}

func benchmarkCompactStorageM0Fixture(b *testing.B, spec compactStorageM0FixtureSpec) {
	var totalWall, applyWall, reclaimedBytes, stableCalls int64
	var allocationBytes, allocationObjects uint64
	var foreground, idle compactStorageMeasurementLatency
	var lastFixture compactStorageMeasurementFixture
	allocsProfileDir := os.Getenv("TREEDB_COMPACT_STORAGE_M0_ALLOCS_PROFILE_DIR")
	if allocsProfileDir != "" && b.N != 1 {
		b.Fatalf("allocation profile snapshots require -benchtime=1x, got b.N=%d", b.N)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := spec.open(b)
		plan, err := db.CompactStoragePlan(context.Background(), spec.options)
		if err != nil {
			_ = db.Close()
			b.Fatalf("CompactStoragePlan: %v", err)
		}
		fixture := compactStorageM0ObservedFixture(spec.metadata, db, plan)
		lastFixture = fixture
		var idleLatencies []time.Duration
		if spec.foreground {
			control := spec.open(b)
			idleLatencies, err = runCompactStorageM0Writes(control, "idle", 32)
			closeErr := control.Close()
			if err != nil || closeErr != nil {
				_ = db.Close()
				b.Fatalf("idle control: write=%v close=%v", err, closeErr)
			}
		}

		instrumentationEnabled := os.Getenv("TREEDB_COMPACT_STORAGE_M0_INSTRUMENTATION") != "off"
		var recorder *compactStorageM0StableRecorder
		var foregroundStart chan struct{}
		var foregroundHandshake *compactStorageM0ForegroundHandshake
		var foregroundDone chan compactStorageM0WriteResult
		var foregroundStartOnce sync.Once
		var foregroundResult compactStorageM0WriteResult
		foregroundConsumed := false
		foregroundAttemptedObserved := false
		if spec.foreground {
			foregroundStart = make(chan struct{})
			foregroundHandshake = newCompactStorageM0ForegroundHandshake()
			foregroundDone = make(chan compactStorageM0WriteResult, 1)
		}
		restore := func() {}
		if instrumentationEnabled {
			recorder = newCompactStorageM0StableRecorder(db)
		}
		if recorder != nil || foregroundHandshake != nil {
			restore = compactStorageM0RestoreWithCleanup(
				b, installCompactStorageM0Recorder(recorder, foregroundHandshake),
			)
		}
		markersEnabled := os.Getenv("TREEDB_COMPACT_STORAGE_M0_STRACE_MARKERS") == "1"
		if compactStorageM0PhaseHooksRequired(recorder != nil, spec.foreground, markersEnabled) {
			db.compactStorageBeforePhase = func(name string) {
				if spec.foreground && name == "value-log-rewrite" {
					foregroundHandshake.arm()
					foregroundStartOnce.Do(func() { close(foregroundStart) })
					foregroundResult, foregroundConsumed, foregroundAttemptedObserved =
						waitForCompactStorageM0ForegroundAttempt(foregroundHandshake.attempted, foregroundDone)
				}
				if markersEnabled {
					fmt.Fprintf(os.Stderr, "TREEDB_M0_PHASE_BEGIN %s\n", name)
				}
				if recorder != nil {
					recorder.beginPhase(name)
				}
			}
			db.compactStorageAfterPhase = func(name string) {
				if recorder != nil {
					recorder.endPhase(name)
				}
				if markersEnabled {
					fmt.Fprintf(os.Stderr, "TREEDB_M0_PHASE_END %s\n", name)
				}
				if spec.foreground && name == "value-log-rewrite" {
					foregroundStartOnce.Do(func() { close(foregroundStart) })
					foregroundResult, foregroundConsumed = finishCompactStorageM0ForegroundWrite(
						foregroundDone, foregroundResult, foregroundConsumed,
					)
				}
			}
		}
		if allocsProfileDir != "" {
			if err := writeCompactStorageM0AllocsProfile(allocsProfileDir, spec.metadata.Name, "before"); err != nil {
				_ = db.Close()
				b.Fatalf("write before allocation profile: %v", err)
			}
		}
		var beforeMem, afterMem runtime.MemStats
		runtime.ReadMemStats(&beforeMem)
		if os.Getenv("TREEDB_COMPACT_STORAGE_M0_STRACE_MARKERS") == "1" {
			fmt.Fprintf(os.Stderr, "TREEDB_M0_BEGIN %s\n", spec.metadata.Name)
		}
		b.StartTimer()
		started := time.Now()
		if spec.foreground {
			go func() {
				<-foregroundStart
				latencies, writeErr := runCompactStorageM0Writes(db, "foreground", 32)
				foregroundDone <- compactStorageM0WriteResult{latencies: latencies, err: writeErr}
			}()
		}
		var stats CompactStorageStats
		var maintenanceErr error
		pprof.Do(context.Background(), pprof.Labels("compact-storage-m0", spec.metadata.Name), func(ctx context.Context) {
			stats, maintenanceErr = db.CompactStorage(ctx, spec.options)
		})
		elapsed := time.Since(started)
		if os.Getenv("TREEDB_COMPACT_STORAGE_M0_STRACE_MARKERS") == "1" {
			fmt.Fprintf(os.Stderr, "TREEDB_M0_END %s\n", spec.metadata.Name)
		}
		runtime.ReadMemStats(&afterMem)
		if spec.foreground {
			foregroundStartOnce.Do(func() { close(foregroundStart) })
			foregroundResult, foregroundConsumed = finishCompactStorageM0ForegroundWrite(
				foregroundDone, foregroundResult, foregroundConsumed,
			)
		}
		b.StopTimer()
		if allocsProfileDir != "" {
			if err := writeCompactStorageM0AllocsProfile(allocsProfileDir, spec.metadata.Name, "after"); err != nil {
				_ = db.Close()
				b.Fatalf("write after allocation profile: %v", err)
			}
		}
		restore()
		db.compactStorageBeforePhase = nil
		db.compactStorageAfterPhase = nil
		if maintenanceErr != nil || foregroundResult.err != nil {
			_ = db.Close()
			b.Fatalf("measured operation failed: maintenance=%v foreground=%v", maintenanceErr, foregroundResult.err)
		}
		if spec.foreground && !foregroundAttemptedObserved {
			_ = db.Close()
			b.Fatal("foreground writes completed without reaching the measured userspace-flush boundary before value-log rewrite")
		}
		if err := validateCompactStorageM0Work(spec, stats); err != nil {
			_ = db.Close()
			b.Fatal(err)
		}

		artifactName := compactStorageM0ArtifactName(spec.metadata.Name, i)
		var measurementRecorder compactStorageMeasurementRecorder
		if recorder != nil {
			measurementRecorder = recorder
		}
		measurement := newCompactStorageMeasurementWithPlan(
			fixture, artifactName, elapsed.Nanoseconds(), plan.ValueLogRewritePlan, stats, measurementRecorder,
		)
		measurement.Allocation = compactStorageMeasurementAllocation{
			TotalAllocBytes:   afterMem.TotalAlloc - beforeMem.TotalAlloc,
			AllocationObjects: afterMem.Mallocs - beforeMem.Mallocs,
			HeapAllocBefore:   beforeMem.HeapAlloc,
			HeapAllocAfter:    afterMem.HeapAlloc,
			HeapInuseBefore:   beforeMem.HeapInuse,
			HeapInuseAfter:    afterMem.HeapInuse,
			HeapSysBefore:     beforeMem.HeapSys,
			HeapSysAfter:      afterMem.HeapSys,
		}
		measurement.ForegroundWrites = compactStorageMeasurementLatencyFor(foregroundResult.latencies)
		measurement.IdleWrites = compactStorageMeasurementLatencyFor(idleLatencies)
		if err := writeCompactStorageM0Artifact(measurement); err != nil {
			_ = db.Close()
			b.Fatalf("write artifact: %v", err)
		}
		totalWall += measurement.TotalWallTimeNanos
		applyWall += measurement.ApplyWallTimeNanos
		reclaimedBytes += measurement.LeafPack.ReclaimedBytes + measurement.ValueLog.GCBytesDeleted + measurement.Vacuum.ReclaimedBytes
		if recorder != nil {
			stableCalls += int64(recorder.totalCalls())
		}
		allocationBytes += measurement.Allocation.TotalAllocBytes
		allocationObjects += measurement.Allocation.AllocationObjects
		foreground = measurement.ForegroundWrites
		idle = measurement.IdleWrites
		if err := db.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
	if b.N == 0 {
		return
	}
	operations := float64(b.N)
	b.ReportMetric(float64(totalWall)/operations, "m0_total_wall_ns/op")
	b.ReportMetric(float64(applyWall)/operations, "m0_apply_wall_ns/op")
	b.ReportMetric(float64(reclaimedBytes)/operations, "m0_reclaimed_bytes/op")
	b.ReportMetric(float64(stableCalls)/operations, "m0_stable_calls/op")
	b.ReportMetric(float64(allocationBytes)/operations, "m0_bytes/op")
	b.ReportMetric(float64(allocationObjects)/operations, "m0_allocs/op")
	b.ReportMetric(float64(lastFixture.DatabaseBytes), "fixture_database_bytes")
	b.ReportMetric(float64(lastFixture.PageCount), "fixture_pages")
	b.ReportMetric(float64(lastFixture.GenerationCount), "fixture_generations")
	if spec.foreground {
		b.ReportMetric(float64(foreground.P50), "foreground_p50_ns/op")
		b.ReportMetric(float64(foreground.P95), "foreground_p95_ns/op")
		b.ReportMetric(float64(foreground.P99), "foreground_p99_ns/op")
		b.ReportMetric(float64(foreground.Max), "foreground_max_ns/op")
		b.ReportMetric(float64(idle.P50), "idle_control_p50_ns/op")
		b.ReportMetric(float64(idle.P95), "idle_control_p95_ns/op")
		b.ReportMetric(float64(idle.P99), "idle_control_p99_ns/op")
		b.ReportMetric(float64(idle.Max), "idle_control_max_ns/op")
	}
}

type compactStorageM0WriteResult struct {
	latencies []time.Duration
	err       error
}

func compactStorageM0PhaseHooksRequired(recorder, foreground, markers bool) bool {
	return recorder || foreground || markers
}

func compactStorageM0RestoreWithCleanup(tb interface{ Cleanup(func()) }, restore func()) func() {
	var once sync.Once
	guarded := func() {
		once.Do(restore)
	}
	tb.Cleanup(guarded)
	return guarded
}

func waitForCompactStorageM0ForegroundAttempt(
	attempted <-chan struct{},
	done <-chan compactStorageM0WriteResult,
) (compactStorageM0WriteResult, bool, bool) {
	select {
	case <-attempted:
		return compactStorageM0WriteResult{}, false, true
	case result := <-done:
		select {
		case <-attempted:
			return result, true, true
		default:
			return result, true, false
		}
	}
}

func finishCompactStorageM0ForegroundWrite(
	done <-chan compactStorageM0WriteResult,
	result compactStorageM0WriteResult,
	consumed bool,
) (compactStorageM0WriteResult, bool) {
	if consumed {
		return result, true
	}
	return <-done, true
}

func runCompactStorageM0Writes(db *DB, label string, count int) ([]time.Duration, error) {
	value := []byte("fixed-foreground-value-32-bytes")
	latencies := make([]time.Duration, 0, count)
	for i := 0; i < count; i++ {
		started := time.Now()
		err := db.Set([]byte(fmt.Sprintf("m0/%s/%03d", label, i)), value)
		latencies = append(latencies, time.Since(started))
		if err != nil {
			return latencies, err
		}
	}
	return latencies, nil
}

func compactStorageM0ObservedFixture(f compactStorageMeasurementFixture, db *DB, plan CompactStorageStats) compactStorageMeasurementFixture {
	f.DatabaseBytesAvailability = "unavailable"
	for _, usage := range plan.Before {
		if usage.Name == "total" {
			f.DatabaseBytes, f.DatabaseBytesAvailability = usage.Bytes, "observed"
		}
	}
	f.PageCountAvailability = "unavailable"
	if generation := db.idx.Load(); generation != nil && generation.pager != nil {
		f.PageCount, f.PageCountAvailability = int(generation.pager.PageCount()), "observed"
	}
	f.GenerationCount = len(plan.LeafGenerationPlan.Generations)
	f.LiveDebtBytes = plan.LeafGenerationPlan.CandidateBytesLive + plan.ValueLogRewritePlan.BytesLive
	f.DeadDebtBytes = plan.LeafGenerationPlan.CandidateBytesDead + plan.ValueLogRewritePlan.BytesStale
	f.DebtAvailability = "observed"
	return f
}

func validateCompactStorageM0Work(spec compactStorageM0FixtureSpec, stats CompactStorageStats) error {
	runs := 0
	for _, pack := range stats.LeafGenerationPacks {
		if pack.Ran {
			runs++
		}
	}
	if runs < spec.minPackRuns {
		return fmt.Errorf("%s: leaf-pack runs=%d want >=%d", spec.metadata.Name, runs, spec.minPackRuns)
	}
	rewriteRan := stats.ValueLogRewrite.SourceSegmentsRequested > 0
	if rewriteRan != spec.expectRewrite {
		if spec.expectRewrite {
			return fmt.Errorf("%s: expected value-log rewrite did not run", spec.metadata.Name)
		}
		return fmt.Errorf("%s: unexpected value-log rewrite selected %d source segments",
			spec.metadata.Name, stats.ValueLogRewrite.SourceSegmentsRequested)
	}
	if spec.minIndexShrinkRatioPPM > 0 {
		before := compactStorageMeasurementUsageBytes(stats.Before, "index")
		after := compactStorageMeasurementUsageBytes(stats.After, "index")
		if before <= 0 || after < 0 || after > before {
			return fmt.Errorf("%s: invalid index shrink boundary before=%d after=%d", spec.metadata.Name, before, after)
		}
		ratioPPM := uint64(before-after) * 1_000_000 / uint64(before)
		if ratioPPM < spec.minIndexShrinkRatioPPM {
			return fmt.Errorf("%s: index shrink ratio=%dppm want >=%dppm (before=%d after=%d)",
				spec.metadata.Name, ratioPPM, spec.minIndexShrinkRatioPPM, before, after)
		}
	}
	for _, phase := range stats.Phases {
		if phase.Name != "index-vacuum" {
			continue
		}
		if spec.expectVacuumRun {
			if phase.Status != CompactStoragePhaseStatusSucceeded || !phase.Required || phase.Skipped {
				return fmt.Errorf("%s: expected successful required index vacuum, got status=%q required=%t skipped=%t reason=%q",
					spec.metadata.Name, phase.Status, phase.Required, phase.Skipped, phase.Reason)
			}
			return nil
		}
		if phase.Status != CompactStoragePhaseStatusNotRequired || phase.Required || !phase.Skipped || phase.SkipReason == "" {
			return fmt.Errorf("%s: expected not-required index vacuum, got status=%q required=%t skipped=%t reason=%q",
				spec.metadata.Name, phase.Status, phase.Required, phase.Skipped, phase.Reason)
		}
		return nil
	}
	return fmt.Errorf("%s: index-vacuum disposition phase missing", spec.metadata.Name)
}

func compactStorageM0ArtifactName(fixture string, iteration int) string {
	sample := iteration + 1
	if override := os.Getenv("TREEDB_COMPACT_STORAGE_M0_SAMPLE"); override != "" {
		if parsed, err := strconv.Atoi(override); err == nil && parsed > 0 {
			sample = parsed + iteration
		}
	}
	return filepath.Join("compact-storage-m0", fixture, "sample-"+strconv.Itoa(sample)+".json")
}

func compactStorageM0AllocsProfilePath(root, fixture, boundary string) string {
	return filepath.Join(root, "allocs_"+fixture+"_"+boundary+".pprof")
}

func writeCompactStorageM0AllocsProfile(root, fixture, boundary string) error {
	// runtime.MemProfile can lag by two GC cycles. Flush that lag so profile
	// subtraction assigns fixture setup to the before snapshot.
	runtime.GC()
	runtime.GC()
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	path := compactStorageM0AllocsProfilePath(root, fixture, boundary)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	writeErr := pprof.Lookup("allocs").WriteTo(file, 0)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func writeCompactStorageM0Artifact(measurement compactStorageMeasurement) error {
	root := os.Getenv("TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR")
	if root == "" {
		return nil
	}
	path := filepath.Join(root, measurement.ArtifactName)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(measurement, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func parseCompactStorageM0ArtifactName(name string) (fixture string, sample int, err error) {
	parts := strings.Split(filepath.ToSlash(name), "/")
	if len(parts) != 3 || parts[0] != "compact-storage-m0" || !strings.HasPrefix(parts[2], "sample-") || !strings.HasSuffix(parts[2], ".json") {
		return "", 0, fmt.Errorf("invalid CompactStorage M0 artifact name %q", name)
	}
	sample, err = strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(parts[2], "sample-"), ".json"))
	if err != nil || sample < 1 {
		return "", 0, fmt.Errorf("invalid CompactStorage M0 artifact sample %q", name)
	}
	return parts[1], sample, nil
}
