package collections_test

import (
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
)

type timedCollectionBatch struct {
	ids  [][]byte
	docs [][]byte
}

func benchmarkDocumentBatches(start, total, targetBatchSize int, indexed bool) []timedCollectionBatch {
	if total <= 0 {
		return nil
	}
	batches := make([]timedCollectionBatch, 0, (total+targetBatchSize-1)/targetBatchSize)
	for generated := 0; generated < total; {
		batchSize := targetBatchSize
		if remaining := total - generated; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(start+generated, batchSize, indexed)
		batches = append(batches, timedCollectionBatch{ids: ids, docs: docs})
		generated += batchSize
	}
	return batches
}

func startTimedCollectionCPUProfile(b *testing.B) func() {
	b.Helper()

	profilePath := strings.TrimSpace(os.Getenv("TREEDB_COLLECTION_TIMED_CPU_PROFILE_PATH"))
	if profilePath == "" {
		return func() {}
	}
	if dir := filepath.Dir(profilePath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			b.Fatalf("create timed cpu profile dir: %v", err)
		}
	}
	file, err := os.Create(profilePath)
	if err != nil {
		b.Fatalf("create timed cpu profile: %v", err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		_ = file.Close()
		b.Fatalf("start timed cpu profile: %v; do not also pass go test -cpuprofile", err)
	}

	stopped := false
	return func() {
		if stopped {
			return
		}
		pprof.StopCPUProfile()
		stopped = true
		if err := file.Close(); err != nil {
			b.Errorf("close timed cpu profile: %v", err)
		}
	}
}

func benchmarkTimedProfileIndexedInsertBatch(b *testing.B, checkpoint bool) {
	backend, collection := openBenchmarkCollection(b, "bench_timed_profile_insert_batch_secondary", secondaryIndexes()...)
	targetBatchSize := benchmarkBatchSize(b)
	batches := benchmarkDocumentBatches(0, b.N, targetBatchSize, true)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)

	metricName := "target_docs/batch"
	if checkpoint {
		metricName = "target_docs/checkpoint"
	}
	b.ReportAllocs()
	b.ReportMetric(float64(targetBatchSize), metricName)
	b.ResetTimer()
	b.StopTimer()
	stopProfile := startTimedCollectionCPUProfile(b)
	profileActive := true
	defer func() {
		if profileActive {
			stopProfile()
		}
	}()

	b.StartTimer()
	for _, batch := range batches {
		if _, err := collection.InsertBatch(batch.ids, batch.docs); err != nil {
			b.Fatalf("timed profile insert batch with secondary indexes: %v", err)
		}
		if checkpoint {
			benchmarkSyncBoundary(b, backend)
		}
	}
	b.StopTimer()
	stopProfile()
	profileActive = false
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
}

func BenchmarkCollectionTimedProfileInsertBatchWithSecondaryIndexes(b *testing.B) {
	benchmarkTimedProfileIndexedInsertBatch(b, false)
}

func BenchmarkCollectionTimedProfileInsertBatchCheckpointWithSecondaryIndexes(b *testing.B) {
	benchmarkTimedProfileIndexedInsertBatch(b, true)
}
