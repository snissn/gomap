package collections_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

// BenchmarkCollectionShapeInsertBatchNativeCOW uses the same fixture and
// collection options as the indexed-shape benchmark. The producer owns
// publication in InsertBatch. The explicit checkpoint and census are untimed.
func BenchmarkCollectionShapeInsertBatchNativeCOW(b *testing.B) {
	if !benchmarkBoolEnv(b, "TREEDB_COLLECTION_BENCH_NATIVE_COW_COMPANION", false) {
		b.Skip("native COW companion requires TREEDB_COLLECTION_BENCH_NATIVE_COW_COMPANION=true")
	}
	if profile := benchmarkTreeDBProfile(b); profile != treedb.ProfileCommandWALRelaxed {
		b.Fatalf("native COW companion requires command_wal_relaxed, got %q", profile)
	}
	format := benchmarkCollectionDocumentFormat(b)
	if format != collections.DocumentFormatBSON && format != collections.DocumentFormatJSON {
		b.Fatalf("native COW companion requires BSON or JSON, got %q", format)
	}
	b.Run("indexes_0", func(b *testing.B) {
		benchmarkNativeCOWShapeInsertBatch(b, 0)
	})
	if format == collections.DocumentFormatBSON {
		b.Run("indexes_2", func(b *testing.B) {
			benchmarkNativeCOWShapeInsertBatch(b, 2)
		})
	}
}

func benchmarkNativeCOWShapeInsertBatch(b *testing.B, indexCount int) {
	const publicationsKey = "treedb.cache.cow.publications_total"
	const acceptedKey = "treedb.command_wal.live_accepted_max_lsn"
	const appliedKey = "treedb.command_wal.applied_lsn"
	const checkpointCallsKey = "treedb.public.checkpoint.calls_total"
	const checkpointErrorsKey = "treedb.public.checkpoint.errors_total"
	opts := benchmarkCollectionDBOptions(b, b.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
	opts.BackgroundIndexVacuumInterval = -1
	database, err := treedb.Open(opts)
	if err != nil {
		b.Fatalf("open shared TreeDB COW owner: %v", err)
	}
	b.Cleanup(func() {
		if err := database.Close(); err != nil {
			b.Errorf("close shared TreeDB COW owner: %v", err)
		}
	})
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		b.Fatalf("activate borrowed native collection backend: %v", err)
	}
	manager := collections.NewCollectionManager(backend)
	b.Cleanup(func() {
		if err := manager.Close(); err != nil {
			b.Errorf("close borrowed native collection manager: %v", err)
		}
	})
	collection := createBenchmarkCollection(b, manager, fmt.Sprintf("bench_shape_insert_%d", indexCount), collectionShapeIndexes(indexCount))
	before := database.Stats()
	if database.ResolvedProfile() != treedb.ProfileCommandWALRelaxed ||
		before["treedb.cache.memtable_mode"] != "cow_btree" ||
		!backend.CommandWALEnabled() || !backend.HasNativeSemanticBridge() || !manager.UsesNativeCommandWALApply() {
		b.Fatalf("expected shared cow_btree native command-WAL owner: profile=%q memtable=%q wal=%t bridge=%t native_apply=%t",
			database.ResolvedProfile(), before["treedb.cache.memtable_mode"], backend.CommandWALEnabled(), backend.HasNativeSemanticBridge(), manager.UsesNativeCommandWALApply())
	}

	batchSize := benchmarkBatchSize(b)
	acknowledged, batches := 0, 0
	b.ReportAllocs()
	b.ResetTimer()
	for start := 0; start < b.N; {
		b.StopTimer()
		count := min(batchSize, b.N-start)
		ids, docs := benchmarkDocumentBatch(b, start, count, true)
		b.StartTimer()
		inserted, err := collection.InsertBatch(ids, docs)
		b.StopTimer()
		if err != nil {
			b.Fatalf("native InsertBatch indexes=%d start=%d size=%d: %v", indexCount, start, count, err)
		}
		if len(inserted) != count {
			b.Fatalf("native InsertBatch returned %d IDs for batch of %d", len(inserted), count)
		}
		acknowledged += len(inserted)
		batches++
		start += count
	}
	afterInsert := database.Stats()
	published := benchmarkStatUint64Delta(b, before, afterInsert, publicationsKey)
	acceptedBefore := benchmarkStatUint64(b, before, acceptedKey)
	acceptedAfter := benchmarkStatUint64(b, afterInsert, acceptedKey)
	checkpointCallsDuringInsert := benchmarkStatUint64Delta(b, before, afterInsert, checkpointCallsKey)
	if published != uint64(batches) || acceptedAfter <= acceptedBefore || checkpointCallsDuringInsert != 0 {
		b.Fatalf("native InsertBatch publication proof: COW publications=%d batches=%d accepted LSN=%d..%d", published, batches, acceptedBefore, acceptedAfter)
	}

	checkpointStart := time.Now()
	if err := database.Checkpoint(); err != nil {
		b.Fatalf("shared TreeDB post-timer checkpoint: %v", err)
	}
	checkpointElapsed := time.Since(checkpointStart)
	afterCheckpoint := database.Stats()
	checkpointCalls := benchmarkStatUint64Delta(b, afterInsert, afterCheckpoint, checkpointCallsKey)
	checkpointErrors := benchmarkStatUint64Delta(b, afterInsert, afterCheckpoint, checkpointErrorsKey)
	checkpointApplied := benchmarkStatUint64(b, afterCheckpoint, appliedKey)
	if checkpointCalls != 1 || checkpointErrors != 0 || checkpointApplied < acceptedAfter {
		b.Fatalf("native post-timer checkpoint proof: calls=%d errors=%d applied LSN=%d want coverage through %d", checkpointCalls, checkpointErrors, checkpointApplied, acceptedAfter)
	}

	scanned := benchmarkVerifyCollectionShapeIDs(b, collection)
	if benchmarkBoolEnv(b, "TREEDB_COLLECTION_REPORT_DISK_USAGE", true) {
		totalBytes, err := benchmarkTreeDBDiskUsageBytes(backend)
		if err != nil {
			b.Fatalf("native post-checkpoint disk usage: %v", err)
		}
		benchmarkReportDiskUsage(b, scanned, totalBytes)
	}

	b.ReportMetric(float64(indexCount), "indexes/doc")
	b.ReportMetric(float64(batchSize), "target_docs/batch")
	b.ReportMetric(1, "native_cow_owner")
	b.ReportMetric(float64(acknowledged), "native_acknowledged_docs")
	b.ReportMetric(float64(scanned), "verified_shape_docs")
	b.ReportMetric(float64(batches), "native_insert_batches")
	b.ReportMetric(float64(benchmarkStatUint64(b, before, publicationsKey)), "cow_publications_before_insert")
	b.ReportMetric(float64(benchmarkStatUint64(b, afterInsert, publicationsKey)), "cow_publications_after_insert")
	b.ReportMetric(float64(published), "cow_insert_publication_delta")
	b.ReportMetric(float64(acceptedBefore), "native_accepted_lsn_before_insert")
	b.ReportMetric(float64(acceptedAfter), "native_accepted_lsn_after_insert")
	b.ReportMetric(float64(checkpointCallsDuringInsert), "native_checkpoint_calls_during_insert")
	b.ReportMetric(float64(checkpointCalls), "native_checkpoint_calls")
	b.ReportMetric(float64(checkpointErrors), "native_checkpoint_errors")
	b.ReportMetric(float64(checkpointApplied), "native_checkpoint_applied_lsn")
	b.ReportMetric(float64(checkpointElapsed.Nanoseconds()), "checkpoint_ns/op")
}

func benchmarkVerifyCollectionShapeIDs(b *testing.B, collection *collections.Collection) int {
	b.Helper()

	scanned := 0
	truncated, err := collection.ScanDocumentsFunc(b.N, func(record collections.DocumentRecord) (bool, error) {
		if !bytes.Equal(record.ID, benchmarkDocumentID(scanned)) {
			return false, fmt.Errorf("unexpected document ID at position %d: %q", scanned, record.ID)
		}
		scanned++
		return true, nil
	})
	if err != nil || truncated || scanned != b.N {
		b.Fatalf("post-checkpoint collection census: scanned=%d want=%d truncated=%t err=%v", scanned, b.N, truncated, err)
	}
	return scanned
}
