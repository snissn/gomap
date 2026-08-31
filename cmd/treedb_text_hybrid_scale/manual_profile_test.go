package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

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
	switch phase {
	case "load":
		action = func() error {
			loadFixture, _, err = loadPrimaryFixtureWithVectorRebuild(cfg, false)
			return err
		}
	case "vector":
		fixture := setup(false)
		action = func() error { _, err := fixture.col.RebuildVectorIndex(vectorIndexName); return err }
	case "phrase":
		cfg.textStorePositions = true
		fixture := setup(false)
		action = func() error {
			for i := 0; i < cfg.queries; i++ {
				got, err := fixture.col.SearchText(collections.TextSearchOptions{IndexName: textIndexName, Phrase: &collections.TextSearchPhraseQuery{Query: "refund policy"}, TopK: cfg.topK, CandidateLimit: cfg.rows, MaxPostingsScanned: cfg.rows * 4, ResultMode: collections.TextSearchResultModeScoreOnly})
				if err != nil || len(got.Results) == 0 {
					return fmt.Errorf("phrase search: %w", err)
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
			_, reopened, err := runReopenProbe(fixture, cfg)
			if reopened.db != nil {
				_ = reopened.db.Close()
			}
			return err
		}
	default:
		t.Fatalf("unknown manual profile phase %q", phase)
	}
	before, err := dirSize(cfg.dbDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("phase=%s rows=%d setup_complete=true measured_boundary_starts_now db_bytes_before=%d", phase, rows, before)
	err = profileManualPhase(os.Getenv("TREEDB_TEXT_PROFILE_MODE"), os.Getenv("TREEDB_TEXT_PROFILE_DIR"), action)
	after, sizeErr := dirSize(cfg.dbDir)
	if loadFixture.db != nil {
		_ = loadFixture.db.Close()
		loadFixture.cleanup()
	}
	if sizeErr != nil {
		t.Fatal(sizeErr)
	}
	t.Logf("phase=%s rows=%d db_bytes_after=%d", phase, rows, after)
	if err != nil {
		t.Fatal(err)
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

func profileManualPhase(mode, dir string, action func() error) error {
	if mode == "" || mode == "none" {
		return action()
	}
	if dir == "" {
		return fmt.Errorf("TREEDB_TEXT_PROFILE_DIR is required with profiling")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
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
			return err
		}
		err := action()
		runtime.GC()
		if writeErr := write("alloc_after.pprof", "allocs"); writeErr != nil {
			return writeErr
		}
		if writeErr := write("heap_after.pprof", "heap"); writeErr != nil {
			return writeErr
		}
		return err
	}
	if mode != "runtime" {
		return fmt.Errorf("unknown profile mode %q", mode)
	}
	cpu, err := os.Create(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		return err
	}
	if err := pprof.StartCPUProfile(cpu); err != nil {
		_ = cpu.Close()
		return err
	}
	traceFile, err := os.Create(filepath.Join(dir, "trace.out"))
	if err != nil {
		pprof.StopCPUProfile()
		_ = cpu.Close()
		return err
	}
	if err := trace.Start(traceFile); err != nil {
		pprof.StopCPUProfile()
		_ = cpu.Close()
		_ = traceFile.Close()
		return err
	}
	runtime.SetBlockProfileRate(1)
	previousMutex := runtime.SetMutexProfileFraction(1)
	err = action()
	trace.Stop()
	pprof.StopCPUProfile()
	runtime.SetBlockProfileRate(0)
	runtime.SetMutexProfileFraction(previousMutex)
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
	return err
}
