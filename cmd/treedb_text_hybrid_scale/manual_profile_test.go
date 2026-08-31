package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const manualRuntimeSamplingMinimum = 250 * time.Millisecond

// TestManualTextHybridScaleProfile4546 is intentionally opt-in: the wrapper
// starts one fresh go test process per phase with TREEDB_TEXT_PROFILE_PHASE set.
func TestManualTextHybridScaleProfile4546(t *testing.T) {
	phase := os.Getenv("TREEDB_TEXT_PROFILE_PHASE")
	if phase == "" {
		t.Skip("manual profiling only; use scripts/treedb_text_hybrid_scale_profile.sh")
	}
	rows, err := manualProfileRows()
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("TREEDB_TEXT_PROFILE_TINY") == "1" {
		rows = 96
	}
	cfg := config{outDir: t.TempDir(), rows: rows, batchSize: minInt(rows, 4096), dims: 4, m: 4, efConstruction: 32, efSearch: 32, topK: 5, candidateLimit: minInt(rows, 64), queries: 3, includeVector: true, maintenanceUpdates: minInt(rows, 8), maintenanceDeletes: minInt(rows, 4)}
	cfg.dbDir = filepath.Join(cfg.outDir, "primary_db")
	if err := os.MkdirAll(cfg.dbDir, 0o755); err != nil {
		t.Fatal(err)
	}
	setup := func(vector bool) scaleFixture {
		fixture, _, err := loadPrimaryFixtureWithVectorRebuild(cfg, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = fixture.db.Close(); fixture.cleanup() })
		if vector {
			if _, err := fixture.col.RebuildVectorIndex(vectorIndexName); err != nil {
				t.Fatal(err)
			}
		}
		return fixture
	}
	var action func() error
	var loadFixture scaleFixture
	var reopenedFixture scaleFixture
	switch phase {
	case "load":
		action = func() error {
			loadFixture, _, err = loadPrimaryFixtureWithVectorRebuild(cfg, false)
			return err
		}
	case "vector":
		fixture := setup(false)
		action = func() error {
			status, err := fixture.col.RebuildVectorIndex(vectorIndexName)
			if err != nil {
				return err
			}
			if status.State != collections.VectorIndexStateColumnGraphLoaded || !status.Loaded {
				return fmt.Errorf("unexpected vector status after rebuild: %+v", status)
			}
			return nil
		}
	case "phrase":
		cfg.textStorePositions = true
		fixture := setup(false)
		action = func() error {
			for i := 0; i < cfg.queries; i++ {
				got, err := fixture.col.SearchText(collections.TextSearchOptions{IndexName: textIndexName, Phrase: &collections.TextSearchPhraseQuery{Query: "refund policy"}, TopK: cfg.topK, CandidateLimit: cfg.rows, MaxPostingsScanned: cfg.rows * 4, ResultMode: collections.TextSearchResultModeScoreOnly})
				if err != nil {
					return fmt.Errorf("phrase search: %w", err)
				}
				if len(got.Results) == 0 {
					return errors.New("phrase search returned no results")
				}
			}
			return nil
		}
	case "broad":
		fixture := setup(true)
		cfg.queryRows = map[string]bool{queryRowHybridTextVecScalar: true}
		action = func() error {
			_, guards, err := runQueryMatrix(fixture.col, cfg)
			if err != nil {
				return err
			}
			return failOnGuardrails(guards, false)
		}
	case "maintenance":
		cfg.includeVector = false // text rewrite uses the same no-command-WAL fixture as runMaintenanceProbe.
		fixture := setup(false)
		action = func() error {
			for i := 0; i < cfg.maintenanceUpdates; i++ {
				id, replacement := scaleDocID(i), scaleDocument(i, cfg.dims, "maintenance-updated")
				if _, _, err := fixture.col.Update(id, func([]byte) ([]byte, bool, error) { return replacement, true, nil }); err != nil {
					return err
				}
			}
			ids := make([][]byte, cfg.maintenanceDeletes)
			for i := range ids {
				ids[i] = scaleDocID(cfg.maintenanceUpdates + i)
			}
			if len(ids) > 0 {
				if _, err := fixture.col.DeleteBatch(ids); err != nil {
					return err
				}
			}
			if _, err := fixture.col.RewriteTextIndex(textIndexName, collections.TextIndexRewriteOptions{}); err != nil {
				return err
			}
			return fixture.db.Checkpoint()
		}
	case "reopen":
		fixture := setup(true)
		action = func() error {
			_, reopenedFixture, err = runReopenProbe(fixture, cfg)
			return err
		}
	default:
		t.Fatalf("unknown manual profile phase %q", phase)
	}
	profileMode := os.Getenv("TREEDB_TEXT_PROFILE_MODE")
	if profileMode == "runtime" && (phase == "phrase" || phase == "broad") {
		readOnlyAction := action
		action = func() error { return repeatManualReadOnlyAction(readOnlyAction, manualRuntimeSamplingMinimum) }
		t.Logf("phase=%s runtime_read_only_sampling_minimum_seconds=%.3f", phase, manualRuntimeSamplingMinimum.Seconds())
	}
	before, err := dirSize(cfg.dbDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("phase=%s rows=%d setup_complete=true measured_boundary_starts_now db_bytes_before=%d", phase, rows, before)
	measured, profileErr := profileManualPhase(profileMode, os.Getenv("TREEDB_TEXT_PROFILE_DIR"), action)
	after, sizeErr := dirSize(cfg.dbDir)
	if loadFixture.db != nil {
		_ = loadFixture.db.Close()
		loadFixture.cleanup()
	}
	if reopenedFixture.db != nil {
		_ = reopenedFixture.db.Close()
		reopenedFixture.cleanup()
	}
	if sizeErr != nil {
		t.Fatal(sizeErr)
	}
	t.Logf("phase=%s rows=%d measured_seconds=%.9f db_bytes_after=%d", phase, rows, measured.Seconds(), after)
	if profileErr != nil {
		t.Fatal(profileErr)
	}
}

func manualProfileRows() (int, error) {
	switch os.Getenv("TREEDB_TEXT_PROFILE_ROWS") {
	case "", "10000":
		return 10_000, nil
	case "100000":
		return 100_000, nil
	default:
		return 0, fmt.Errorf("TREEDB_TEXT_PROFILE_ROWS must be 10000 or 100000")
	}
}

func profileManualPhase(mode, dir string, action func() error) (time.Duration, error) {
	if mode == "" || mode == "none" {
		return timeManualAction(action)
	}
	if dir == "" {
		return 0, fmt.Errorf("TREEDB_TEXT_PROFILE_DIR is required with profiling")
	}
	if mode == "alloc" {
		if err := requireExactMemProfileRate(true, runtime.MemProfileRate); err != nil {
			return 0, err
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	write := func(name, profile string) error {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		defer f.Close()
		return pprof.Lookup(profile).WriteTo(f, 0)
	}
	if mode == "alloc" {
		if err := write("alloc_before.pprof", "allocs"); err != nil {
			return 0, err
		}
		// Snapshot immediately around the action so profile serialization and any
		// profiler bookkeeping are not included in the allocation delta.
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		measured, err := timeManualAction(action)
		runtime.ReadMemStats(&after)
		allocationDelta := fmt.Sprintf("allocation_bytes=%d\nallocation_objects=%d\n", after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs)
		if writeErr := os.WriteFile(filepath.Join(dir, "alloc_delta.txt"), []byte(allocationDelta), 0o644); writeErr != nil {
			return measured, writeErr
		}
		if writeErr := write("alloc_after.pprof", "allocs"); writeErr != nil {
			return measured, writeErr
		}
		if writeErr := write("heap_after.pprof", "heap"); writeErr != nil {
			return measured, writeErr
		}
		return measured, err
	}
	if mode != "runtime" {
		return 0, fmt.Errorf("unknown profile mode %q", mode)
	}
	cpu, err := os.Create(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		return 0, err
	}
	traceFile, err := os.Create(filepath.Join(dir, "trace.out"))
	if err != nil {
		_ = cpu.Close()
		return 0, err
	}
	if err := trace.Start(traceFile); err != nil {
		_ = cpu.Close()
		_ = traceFile.Close()
		return 0, err
	}
	runtime.SetBlockProfileRate(1)
	previousMutex := runtime.SetMutexProfileFraction(1)
	if err := pprof.StartCPUProfile(cpu); err != nil {
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(previousMutex)
		trace.Stop()
		_ = cpu.Close()
		_ = traceFile.Close()
		return 0, err
	}
	measured, err := timeManualAction(action)
	runtime.SetBlockProfileRate(0)
	runtime.SetMutexProfileFraction(previousMutex)
	pprof.StopCPUProfile()
	trace.Stop()
	if closeErr := cpu.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if closeErr := traceFile.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if writeErr := write("block.pprof", "block"); writeErr != nil && err == nil {
		err = writeErr
	}
	if writeErr := write("mutex.pprof", "mutex"); writeErr != nil && err == nil {
		err = writeErr
	}
	return measured, err
}

func timeManualAction(action func() error) (time.Duration, error) {
	start := time.Now()
	err := action()
	return time.Since(start), err
}

func repeatManualReadOnlyAction(action func() error, minimum time.Duration) error {
	start := time.Now()
	for {
		if err := action(); err != nil {
			return err
		}
		if time.Since(start) >= minimum {
			return nil
		}
	}
}

func TestTimeManualActionMeasuresAction4546(t *testing.T) {
	duration, err := timeManualAction(func() error { time.Sleep(time.Millisecond); return nil })
	if err != nil || duration < time.Millisecond {
		t.Fatalf("duration=%s err=%v", duration, err)
	}
}

func TestManualAllocProfileRequiresExactRate4546(t *testing.T) {
	previous := runtime.MemProfileRate
	runtime.MemProfileRate = 512 * 1024
	t.Cleanup(func() { runtime.MemProfileRate = previous })
	if _, err := profileManualPhase("alloc", t.TempDir(), func() error { return nil }); err == nil || !strings.Contains(err.Error(), "GODEBUG=memprofilerate=1") {
		t.Fatalf("allocation precondition err=%v", err)
	}
}
