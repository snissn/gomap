package collections_test

import (
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

const (
	timedCPUProfilePathEnv                = "TREEDB_COLLECTION_TIMED_CPU_PROFILE_PATH"
	maxUnprofiledTimedProfilePrebuiltDocs = 100_000
	maxProfiledTimedProfilePrebuiltDocs   = 1_000_000
)

type timedCollectionBatch struct {
	ids  [][]byte
	docs [][]byte
}

func benchmarkDocumentBatches(tb testing.TB, start, total, targetBatchSize int, indexed bool) []timedCollectionBatch {
	tb.Helper()
	if total <= 0 {
		return nil
	}
	batches := make([]timedCollectionBatch, 0, (total+targetBatchSize-1)/targetBatchSize)
	for generated := 0; generated < total; {
		batchSize := targetBatchSize
		if remaining := total - generated; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(tb, start+generated, batchSize, indexed)
		batches = append(batches, timedCollectionBatch{ids: ids, docs: docs})
		generated += batchSize
	}
	return batches
}

func startTimedCollectionCPUProfile(b *testing.B) func() {
	b.Helper()

	profilePath := strings.TrimSpace(os.Getenv(timedCPUProfilePathEnv))
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

func requireSafeTimedProfileBatchPrebuild(b *testing.B) {
	b.Helper()

	if strings.TrimSpace(os.Getenv(timedCPUProfilePathEnv)) != "" {
		if b.N <= maxProfiledTimedProfilePrebuiltDocs {
			return
		}
		b.Fatalf("timed-profile benchmark would prebuild %d documents outside the timed section; use a fixed-iteration -benchtime below %d docs", b.N, maxProfiledTimedProfilePrebuiltDocs)
	}
	if b.N <= maxUnprofiledTimedProfilePrebuiltDocs {
		return
	}
	b.Skipf("skipping timed-profile benchmark with no %s: prebuilding %d documents outside the timed section is only allowed for small unprofiled runs", timedCPUProfilePathEnv, b.N)
}

func benchmarkTimedProfileIndexedInsertBatch(b *testing.B, checkpoint bool) {
	targetBatchSize := benchmarkBatchSize(b)
	requireSafeTimedProfileBatchPrebuild(b)
	backend, collection := openBenchmarkCollection(b, "bench_timed_profile_insert_batch_secondary", secondaryIndexes()...)
	batches := benchmarkDocumentBatches(b, 0, b.N, targetBatchSize, true)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)
	var insertElapsed time.Duration
	var syncElapsed time.Duration

	metricName := "target_docs/batch"
	if checkpoint {
		metricName = "target_docs/checkpoint"
	}
	b.ReportAllocs()
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
		if checkpoint {
			insertStart := time.Now()
			if _, err := collection.InsertBatch(batch.ids, batch.docs); err != nil {
				b.Fatalf("timed profile insert batch with secondary indexes: %v", err)
			}
			insertElapsed += time.Since(insertStart)
			syncStart := time.Now()
			benchmarkSyncBoundary(b, backend)
			syncElapsed += time.Since(syncStart)
			continue
		}
		if _, err := collection.InsertBatch(batch.ids, batch.docs); err != nil {
			b.Fatalf("timed profile insert batch with secondary indexes: %v", err)
		}
	}
	b.StopTimer()
	stopProfile()
	profileActive = false
	b.ReportMetric(float64(targetBatchSize), metricName)
	b.ReportMetric(2, "indexes/doc")
	if checkpoint {
		benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	}
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
	benchmarkReportTreeDBDiskUsage(b, backend, b.N)
}

func BenchmarkCollectionTimedProfileInsertBatchWithSecondaryIndexes(b *testing.B) {
	benchmarkTimedProfileIndexedInsertBatch(b, false)
}

func BenchmarkCollectionTimedProfileInsertBatchCheckpointWithSecondaryIndexes(b *testing.B) {
	benchmarkTimedProfileIndexedInsertBatch(b, true)
}
