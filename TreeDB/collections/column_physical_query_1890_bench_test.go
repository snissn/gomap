package collections

import (
	"fmt"
	"os"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	columnPhysicalQueryQ1BenchmarkRows1890      = 1_000_000
	columnPhysicalQueryQ1BenchmarkBatchRows1890 = 100_000
)

var columnPhysicalQueryQ1BenchmarkFixture1890 struct {
	once       sync.Once
	collection *Collection
	dir        string
	err        error
}

func TestMain(m *testing.M) {
	code := m.Run()
	cleanupColumnPhysicalQueryQ1BenchmarkFixture1890()
	os.Exit(code)
}

func cleanupColumnPhysicalQueryQ1BenchmarkFixture1890() {
	fixture := &columnPhysicalQueryQ1BenchmarkFixture1890
	if fixture.collection != nil && fixture.collection.db != nil {
		_ = fixture.collection.db.Close()
		fixture.collection = nil
	}
	if fixture.dir != "" {
		_ = os.RemoveAll(fixture.dir)
		fixture.dir = ""
	}
}

func BenchmarkColumnPhysicalQueryQ1DirectPrepared1890(b *testing.B) {
	fixture := columnPhysicalQueryQ1Fixture1890(b)
	req := ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}

	b.Run("direct_RunColumnPhysicalQuery", func(b *testing.B) {
		preview, err := fixture.RunColumnPhysicalQuery(req)
		if err != nil {
			b.Fatalf("preview RunColumnPhysicalQuery: %v", err)
		}
		validateColumnPhysicalQueryQ1BenchmarkResult1890(b, preview)
		reportColumnPhysicalQueryQ1BenchmarkShape1890(b, preview)

		b.ReportAllocs()
		b.ResetTimer()
		var scannedRows, reducedRows, resultGroups int64
		var physicalBytes int64
		for i := 0; i < b.N; i++ {
			result, err := fixture.RunColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("RunColumnPhysicalQuery: %v", err)
			}
			validateColumnPhysicalQueryQ1BenchmarkResult1890(b, result)
			diag := result.Diagnostics
			scannedRows += int64(diag.RowsScanned)
			reducedRows += int64(diag.ReduceRows)
			resultGroups += int64(diag.ResultGroups)
			physicalBytes += diag.PhysicalBytesScanned
		}
		reportColumnPhysicalQueryQ1BenchmarkMetrics1890(b, scannedRows, reducedRows, resultGroups, physicalBytes)
		b.StopTimer()
		reportColumnPhysicalQueryObservabilityMetrics1890(b, preview.Diagnostics)
	})

	b.Run("prepare_only", func(b *testing.B) {
		previewRunner, err := fixture.PrepareColumnPhysicalQuery(req)
		if err != nil {
			b.Fatalf("preview PrepareColumnPhysicalQuery: %v", err)
		}
		preview, err := previewRunner.Run()
		if closeErr := previewRunner.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		if err != nil {
			b.Fatalf("preview prepare/run/close: %v", err)
		}
		validateColumnPhysicalQueryQ1BenchmarkResult1890(b, preview)
		reportColumnPhysicalQueryQ1BenchmarkShape1890(b, preview)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runner, err := fixture.PrepareColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("PrepareColumnPhysicalQuery: %v", err)
			}
			if err := runner.Close(); err != nil {
				b.Fatalf("runner Close: %v", err)
			}
		}
		b.StopTimer()
		reportColumnPhysicalQueryObservabilityMetrics1890(b, preview.Diagnostics)
	})

	b.Run("prepared_runner_Run", func(b *testing.B) {
		runner, err := fixture.PrepareColumnPhysicalQuery(req)
		if err != nil {
			b.Fatalf("PrepareColumnPhysicalQuery: %v", err)
		}
		defer func() { _ = runner.Close() }()

		preview, err := runner.Run()
		if err != nil {
			b.Fatalf("preview runner Run: %v", err)
		}
		validateColumnPhysicalQueryQ1BenchmarkResult1890(b, preview)
		reportColumnPhysicalQueryQ1BenchmarkShape1890(b, preview)

		b.ReportAllocs()
		b.ResetTimer()
		var scannedRows, reducedRows, resultGroups int64
		var physicalBytes int64
		for i := 0; i < b.N; i++ {
			result, err := runner.Run()
			if err != nil {
				b.Fatalf("runner Run: %v", err)
			}
			validateColumnPhysicalQueryQ1BenchmarkResult1890(b, result)
			diag := result.Diagnostics
			scannedRows += int64(diag.RowsScanned)
			reducedRows += int64(diag.ReduceRows)
			resultGroups += int64(diag.ResultGroups)
			physicalBytes += diag.PhysicalBytesScanned
		}
		reportColumnPhysicalQueryQ1BenchmarkMetrics1890(b, scannedRows, reducedRows, resultGroups, physicalBytes)
		b.StopTimer()
		reportColumnPhysicalQueryObservabilityMetrics1890(b, preview.Diagnostics)
	})

	b.Run("prepare_run_close", func(b *testing.B) {
		previewRunner, err := fixture.PrepareColumnPhysicalQuery(req)
		if err != nil {
			b.Fatalf("preview PrepareColumnPhysicalQuery: %v", err)
		}
		preview, err := previewRunner.Run()
		if closeErr := previewRunner.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		if err != nil {
			b.Fatalf("preview prepare/run/close: %v", err)
		}
		validateColumnPhysicalQueryQ1BenchmarkResult1890(b, preview)
		reportColumnPhysicalQueryQ1BenchmarkShape1890(b, preview)

		b.ReportAllocs()
		b.ResetTimer()
		var scannedRows, reducedRows, resultGroups int64
		var physicalBytes int64
		for i := 0; i < b.N; i++ {
			runner, err := fixture.PrepareColumnPhysicalQuery(req)
			if err != nil {
				b.Fatalf("PrepareColumnPhysicalQuery: %v", err)
			}
			result, runErr := runner.Run()
			closeErr := runner.Close()
			if runErr != nil {
				b.Fatalf("runner Run: %v", runErr)
			}
			if closeErr != nil {
				b.Fatalf("runner Close: %v", closeErr)
			}
			validateColumnPhysicalQueryQ1BenchmarkResult1890(b, result)
			diag := result.Diagnostics
			scannedRows += int64(diag.RowsScanned)
			reducedRows += int64(diag.ReduceRows)
			resultGroups += int64(diag.ResultGroups)
			physicalBytes += diag.PhysicalBytesScanned
		}
		reportColumnPhysicalQueryQ1BenchmarkMetrics1890(b, scannedRows, reducedRows, resultGroups, physicalBytes)
		b.StopTimer()
		reportColumnPhysicalQueryObservabilityMetrics1890(b, preview.Diagnostics)
	})
}

func columnPhysicalQueryQ1Fixture1890(tb testing.TB) *Collection {
	tb.Helper()
	columnPhysicalQueryQ1BenchmarkFixture1890.once.Do(func() {
		columnPhysicalQueryQ1BenchmarkFixture1890.collection, columnPhysicalQueryQ1BenchmarkFixture1890.dir, columnPhysicalQueryQ1BenchmarkFixture1890.err = openColumnPhysicalQueryQ1BenchmarkFixture1890(columnPhysicalQueryQ1BenchmarkRows1890)
	})
	if err := columnPhysicalQueryQ1BenchmarkFixture1890.err; err != nil {
		tb.Fatalf("open q1 benchmark fixture: %v", err)
	}
	tb.Logf("q1 1890 benchmark fixture rows=%d dir=%s", columnPhysicalQueryQ1BenchmarkRows1890, columnPhysicalQueryQ1BenchmarkFixture1890.dir)
	return columnPhysicalQueryQ1BenchmarkFixture1890.collection
}

func openColumnPhysicalQueryQ1BenchmarkFixture1890(rows int) (*Collection, string, error) {
	dir, err := os.MkdirTemp("", "treedb_q1_1890_bench_*")
	if err != nil {
		return nil, "", err
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		return nil, dir, err
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return nil, dir, err
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		_ = d.Close()
		return nil, dir, err
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		return nil, dir, err
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		return nil, dir, err
	}
	const baseTimeUS = int64(1_700_000_000_000_000)
	for start := 0; start < rows; start += columnPhysicalQueryQ1BenchmarkBatchRows1890 {
		end := min(start+columnPhysicalQueryQ1BenchmarkBatchRows1890, rows)
		ids := make([][]byte, end-start)
		docs := make([][]byte, end-start)
		for i := start; i < end; i++ {
			batchIdx := i - start
			ids[batchIdx] = []byte(fmt.Sprintf("e%07d", i))
			docs[batchIdx] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"kind_%02d","did":"did_%02d","payload":"ignored_%d"}`,
				baseTimeUS+int64(i%24)*3_600_000_000+int64(i/24), i%4, i%12, i))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			_ = d.Close()
			return nil, dir, err
		}
	}
	if err := d.Close(); err != nil {
		return nil, dir, err
	}

	reopen, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return nil, dir, err
	}
	reopened, err := NewCollectionManager(reopen).OpenCollection("events")
	if err != nil {
		_ = reopen.Close()
		return nil, dir, err
	}
	return reopened, dir, nil
}

func validateColumnPhysicalQueryQ1BenchmarkResult1890(tb testing.TB, result ColumnPhysicalQueryResult) {
	tb.Helper()
	if got := len(result.Groups); got != 4 {
		tb.Fatalf("q1 groups=%d want 4 diagnostics=%+v", got, result.Diagnostics)
	}
	if got := result.Diagnostics.ResultGroups; got != 4 {
		tb.Fatalf("q1 diagnostic result groups=%d want 4 diagnostics=%+v", got, result.Diagnostics)
	}
	if got := result.Diagnostics.RowsScanned; got != columnPhysicalQueryQ1BenchmarkRows1890 {
		tb.Fatalf("q1 rows scanned=%d want %d diagnostics=%+v", got, columnPhysicalQueryQ1BenchmarkRows1890, result.Diagnostics)
	}
	if got := result.Diagnostics.ReduceRows; got != columnPhysicalQueryQ1BenchmarkRows1890 {
		tb.Fatalf("q1 reduce rows=%d want %d diagnostics=%+v", got, columnPhysicalQueryQ1BenchmarkRows1890, result.Diagnostics)
	}
	if result.Diagnostics.PhysicalBytesScanned <= columnDictionaryCodeOneShotMaxBytes {
		tb.Fatalf("q1 physical bytes=%d want large dictionary-code sidecar > %d diagnostics=%+v", result.Diagnostics.PhysicalBytesScanned, columnDictionaryCodeOneShotMaxBytes, result.Diagnostics)
	}
}

func reportColumnPhysicalQueryQ1BenchmarkShape1890(b *testing.B, preview ColumnPhysicalQueryResult) {
	b.Helper()
	b.SetBytes(preview.Diagnostics.PhysicalBytesScanned)
	b.ReportMetric(float64(preview.Diagnostics.RowsScanned), "scanned_rows/op")
	b.ReportMetric(float64(preview.Diagnostics.ReduceRows), "reduced_rows/op")
	b.ReportMetric(float64(preview.Diagnostics.ResultGroups), "result_groups/op")
	b.ReportMetric(float64(preview.Diagnostics.PhysicalBytesScanned), "physical_bytes/op")
	reportColumnPhysicalQueryObservabilityMetrics1890(b, preview.Diagnostics)
}

func reportColumnPhysicalQueryObservabilityMetrics1890(b *testing.B, diag ColumnPhysicalQueryDiagnostics) {
	b.Helper()
	if diag.StorageSource != "" {
		b.ReportMetric(1, "storage_source_"+string(diag.StorageSource))
	}
	if diag.FallbackReason != "" {
		b.ReportMetric(1, "fallback_"+string(diag.FallbackReason))
	}
	reportColumnPhysicalQueryUint64BenchmarkMetric1890(b, diag.ManifestRoot, "manifest_root_id")
	reportColumnPhysicalQueryUint64BenchmarkMetric1890(b, diag.ManifestGeneration, "manifest_generation")
	reportColumnPhysicalQueryUint64BenchmarkMetric1890(b, diag.ActiveManifestChecksum, "active_manifest_checksum")
}

func reportColumnPhysicalQueryUint64BenchmarkMetric1890(b *testing.B, value uint64, name string) {
	b.Helper()
	b.ReportMetric(float64(uint32(value>>32)), name+"_hi")
	b.ReportMetric(float64(uint32(value)), name+"_lo")
}

func reportColumnPhysicalQueryQ1BenchmarkMetrics1890(b *testing.B, scannedRows, reducedRows, resultGroups, physicalBytes int64) {
	b.Helper()
	if b.N == 0 {
		return
	}
	elapsed := b.Elapsed()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
		b.ReportMetric(float64(reducedRows)/elapsed.Seconds(), "logical_rows/s")
	}
	b.ReportMetric(float64(scannedRows)/float64(b.N), "scanned_rows/op")
	b.ReportMetric(float64(reducedRows)/float64(b.N), "reduced_rows/op")
	b.ReportMetric(float64(resultGroups)/float64(b.N), "result_groups/op")
	b.ReportMetric(float64(physicalBytes)/float64(b.N), "physical_bytes/op")
}
