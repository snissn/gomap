package collections_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	defaultCollectionBenchBatchSize = 8000
	collectionBenchSeedDocs         = 4096
	collectionBenchCities           = 64
	collectionBenchBackfill         = 1024
	collectionBenchIndexedPad       = "01234567890123456789"
)

var collectionBenchPayload = []byte(`{"name":"ada","city":"hnl","email":"ada@example.com","pad":"0123456789012345678901234567890123456789"}`)

func benchmarkBatchSize(b *testing.B) int {
	b.Helper()

	raw := strings.TrimSpace(os.Getenv("TREEDB_COLLECTION_BENCH_BATCH_SIZE"))
	if raw == "" {
		return defaultCollectionBenchBatchSize
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		b.Fatalf("unsupported TREEDB_COLLECTION_BENCH_BATCH_SIZE=%q", raw)
	}
	return n
}

func benchmarkTreeDBProfile(b *testing.B) treedb.Profile {
	b.Helper()

	switch strings.ToLower(strings.TrimSpace(os.Getenv("TREEDB_COLLECTION_BENCH_ENGINE"))) {
	case "", "production_fast", "backend_direct_fast", "backend_direct", "cached", "fast":
		return treedb.ProfileFast
	case "production_wal_on_fast", "backend_direct_wal_on_fast", "wal_on_fast":
		return treedb.ProfileWALOnFast
	case "durable":
		return treedb.ProfileDurable
	case "bench":
		return treedb.ProfileBench
	default:
		b.Fatalf("unsupported TREEDB_COLLECTION_BENCH_ENGINE=%q", os.Getenv("TREEDB_COLLECTION_BENCH_ENGINE"))
		return treedb.ProfileFast
	}
}

func benchmarkBoolEnv(tb testing.TB, name string, def bool) bool {
	tb.Helper()

	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		tb.Fatalf("unsupported %s=%q", name, raw)
	}
	return v
}

func benchmarkIntEnv(tb testing.TB, name string, def int) int {
	tb.Helper()

	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		tb.Fatalf("unsupported %s=%q", name, raw)
	}
	return v
}

func benchmarkInt64Env(tb testing.TB, name string, def int64) int64 {
	tb.Helper()

	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		tb.Fatalf("unsupported %s=%q", name, raw)
	}
	return v
}

func benchmarkRootStoragePolicy(outerLeavesInVLog bool) collections.RootStoragePolicy {
	if outerLeavesInVLog {
		return collections.RootStorageCompressed
	}
	return collections.RootStorageFast
}

func benchmarkCollectionStoragePolicy(tb testing.TB) (dataOuter, indexOuter bool) {
	tb.Helper()

	dataOuter = benchmarkBoolEnv(tb, "TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG", true)
	indexOuter = benchmarkBoolEnv(tb, "TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG", false)
	return dataOuter, indexOuter
}

func benchmarkCollectionDocumentFormat(tb testing.TB) collections.DocumentFormat {
	tb.Helper()

	raw := strings.TrimSpace(os.Getenv("TREEDB_COLLECTION_DOCUMENT_FORMAT"))
	switch strings.ToLower(raw) {
	case "", "json":
		return collections.DocumentFormatJSON
	case string(collections.DocumentFormatBSON):
		return collections.DocumentFormatBSON
	case string(collections.DocumentFormatTemplateV1):
		return collections.DocumentFormatTemplateV1
	default:
		tb.Fatalf("unsupported TREEDB_COLLECTION_DOCUMENT_FORMAT=%q", raw)
		return collections.DocumentFormatJSON
	}
}

func benchmarkStatUint64(tb testing.TB, stats map[string]string, key string) uint64 {
	tb.Helper()

	raw, ok := stats[key]
	if !ok {
		tb.Fatalf("missing benchmark stat %q", key)
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		tb.Fatalf("parse benchmark stat %q=%q: %v", key, raw, err)
	}
	return n
}

func benchmarkNativeProbeFallbackCounters(tb testing.TB, backend *backenddb.DB) (key, prefix uint64) {
	tb.Helper()

	stats := backend.Stats()
	return benchmarkStatUint64(tb, stats, "treedb.native_fastpath.per_item_key_probe_fallback_count"),
		benchmarkStatUint64(tb, stats, "treedb.native_fastpath.per_item_prefix_probe_fallback_count")
}

func benchmarkReportNativeProbeFallbackDeltas(b *testing.B, backend *backenddb.DB, startKey, startPrefix uint64) {
	b.Helper()

	endKey, endPrefix := benchmarkNativeProbeFallbackCounters(b, backend)
	if endKey < startKey {
		b.Fatalf("per_item_key_probe_fallback_count moved backwards: start=%d end=%d", startKey, endKey)
	}
	if endPrefix < startPrefix {
		b.Fatalf("per_item_prefix_probe_fallback_count moved backwards: start=%d end=%d", startPrefix, endPrefix)
	}
	keyDelta := endKey - startKey
	prefixDelta := endPrefix - startPrefix
	b.ReportMetric(float64(keyDelta), "per_item_key_probe_fallback_count")
	b.ReportMetric(float64(prefixDelta), "per_item_prefix_probe_fallback_count")
	if keyDelta != 0 {
		b.Fatalf("per_item_key_probe_fallback_count=%d want 0", keyDelta)
	}
	if prefixDelta != 0 {
		b.Fatalf("per_item_prefix_probe_fallback_count=%d want 0", prefixDelta)
	}
}

func benchmarkReportCheckpointSplit(b *testing.B, docs int, insertElapsed, syncElapsed time.Duration) {
	b.Helper()

	if docs <= 0 {
		return
	}
	b.ReportMetric(float64(insertElapsed.Nanoseconds())/float64(docs), "insert_ns/doc")
	b.ReportMetric(float64(syncElapsed.Nanoseconds())/float64(docs), "sync_ns/doc")
}

func benchmarkReportDiskUsage(b *testing.B, docs int, totalBytes uint64) {
	b.Helper()

	if docs <= 0 {
		return
	}
	b.ReportMetric(float64(docs), "stored_docs")
	b.ReportMetric(float64(totalBytes), "disk_total_bytes")
	b.ReportMetric(float64(totalBytes)/float64(docs), "disk_bytes/doc")
}

func benchmarkReportTreeDBDiskUsage(b *testing.B, backend *backenddb.DB, docs int) {
	b.Helper()

	if backend == nil {
		return
	}
	if docs <= 0 {
		return
	}
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint before TreeDB disk usage stats: %v", err)
	}
	totalBytes, err := benchmarkTreeDBDiskUsageBytes(backend)
	if err != nil {
		b.Fatalf("TreeDB disk usage stats: %v", err)
	}
	benchmarkReportDiskUsage(b, docs, totalBytes)
	currentBytes := benchmarkReportTreeDBValueLogRewrite(b, backend, docs, totalBytes)
	benchmarkReportTreeDBLeafGenerationPackGC(b, backend, docs, currentBytes)
}

func benchmarkReportTreeDBValueLogRewrite(b *testing.B, backend *backenddb.DB, docs int, beforeTotalBytes uint64) uint64 {
	b.Helper()

	if docs <= 0 || !benchmarkBoolEnv(b, "TREEDB_COLLECTION_REPORT_VLOG_REWRITE", false) {
		return beforeTotalBytes
	}
	rewriteStart := time.Now()
	rewriteStats, err := backend.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{})
	if err != nil {
		b.Fatalf("TreeDB value-log rewrite: %v", err)
	}
	rewriteElapsed := time.Since(rewriteStart)
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint after TreeDB value-log rewrite: %v", err)
	}
	afterRewriteBytes, err := benchmarkTreeDBDiskUsageBytes(backend)
	if err != nil {
		b.Fatalf("TreeDB disk usage after value-log rewrite: %v", err)
	}

	gcStart := time.Now()
	gcStats, err := backend.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{})
	if err != nil {
		b.Fatalf("TreeDB value-log GC after rewrite: %v", err)
	}
	gcElapsed := time.Since(gcStart)
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint after TreeDB value-log GC: %v", err)
	}
	afterGCBytes, err := benchmarkTreeDBDiskUsageBytes(backend)
	if err != nil {
		b.Fatalf("TreeDB disk usage after value-log GC: %v", err)
	}

	b.ReportMetric(float64(rewriteElapsed.Nanoseconds()), "vlog_rewrite_ns/op")
	b.ReportMetric(float64(beforeTotalBytes), "vlog_rewrite_disk_total_bytes_before")
	b.ReportMetric(float64(afterRewriteBytes), "vlog_rewrite_disk_total_bytes_after")
	b.ReportMetric(benchmarkSignedByteDelta(afterRewriteBytes, beforeTotalBytes), "vlog_rewrite_disk_total_bytes_delta")
	b.ReportMetric(float64(afterRewriteBytes)/float64(docs), "vlog_rewrite_disk_bytes/doc_after")
	b.ReportMetric(float64(rewriteStats.SegmentsBefore), "vlog_rewrite_segments_before")
	b.ReportMetric(float64(rewriteStats.SegmentsAfter), "vlog_rewrite_segments_after")
	b.ReportMetric(float64(rewriteStats.BytesBefore), "vlog_rewrite_value_bytes_before")
	b.ReportMetric(float64(rewriteStats.BytesAfter), "vlog_rewrite_value_bytes_after")
	b.ReportMetric(float64(rewriteStats.RecordsCopied), "vlog_rewrite_records_copied")
	b.ReportMetric(float64(rewriteStats.ValueRecordsCopied), "vlog_rewrite_value_records_copied")
	b.ReportMetric(float64(rewriteStats.ValueBytesCopied), "vlog_rewrite_value_bytes_copied")
	b.ReportMetric(float64(rewriteStats.TemplateRecordsAttempted), "vlog_rewrite_template_records_attempted")
	b.ReportMetric(float64(rewriteStats.TemplateRecordsKept), "vlog_rewrite_template_records_kept")
	b.ReportMetric(float64(rewriteStats.TemplateInputBytes), "vlog_rewrite_template_input_bytes")
	b.ReportMetric(float64(rewriteStats.TemplateOutputBytes), "vlog_rewrite_template_output_bytes")
	b.ReportMetric(float64(gcElapsed.Nanoseconds()), "vlog_gc_ns/op")
	b.ReportMetric(float64(afterGCBytes), "vlog_rewrite_gc_disk_total_bytes_after")
	b.ReportMetric(benchmarkSignedByteDelta(afterGCBytes, beforeTotalBytes), "vlog_rewrite_gc_disk_total_bytes_delta")
	b.ReportMetric(float64(afterGCBytes)/float64(docs), "vlog_rewrite_gc_disk_bytes/doc_after")
	b.ReportMetric(float64(gcStats.SegmentsDeleted), "vlog_gc_segments_deleted")
	b.ReportMetric(float64(gcStats.BytesDeleted), "vlog_gc_bytes_deleted")
	return benchmarkReportTreeDBPostMaintenanceIndexVacuum(b, backend, docs, beforeTotalBytes, afterGCBytes, "vlog_rewrite_gc")
}

func benchmarkReportTreeDBLeafGenerationPackGC(b *testing.B, backend *backenddb.DB, docs int, beforeTotalBytes uint64) {
	b.Helper()

	if docs <= 0 || !benchmarkBoolEnv(b, "TREEDB_COLLECTION_REPORT_LEAFGEN_PACK_GC", false) {
		return
	}
	ctx := context.Background()
	force := benchmarkBoolEnv(b, "TREEDB_COLLECTION_LEAFGEN_PACK_FORCE", false)
	maxGenerations := benchmarkIntEnv(b, "TREEDB_COLLECTION_LEAFGEN_PACK_MAX_GENERATIONS", 1)
	leafFrameK := benchmarkIntEnv(b, "TREEDB_COLLECTION_LEAFGEN_PACK_FRAME_K", 0)

	planStart := time.Now()
	plan, err := backend.LeafGenerationPlan(ctx, backenddb.LeafGenerationPlanOptions{Force: force})
	if err != nil {
		b.Fatalf("TreeDB leaf-generation plan: %v", err)
	}
	planElapsed := time.Since(planStart)
	b.ReportMetric(float64(planElapsed.Nanoseconds()), "leafgen_plan_ns/op")
	b.ReportMetric(float64(len(plan.CandidateGenerationIDs)), "leafgen_plan_candidate_generations")
	b.ReportMetric(float64(plan.CandidateBytesTotal), "leafgen_plan_candidate_bytes_total")
	b.ReportMetric(float64(plan.CandidateBytesLive), "leafgen_plan_candidate_bytes_live")
	b.ReportMetric(float64(plan.CandidateBytesDead), "leafgen_plan_candidate_bytes_dead")
	b.ReportMetric(float64(plan.CandidateBytesToCopy), "leafgen_plan_candidate_bytes_to_copy")
	b.ReportMetric(float64(plan.CandidateLivePages), "leafgen_plan_candidate_live_pages")
	b.ReportMetric(float64(plan.ExpectedReclaimBytes), "leafgen_plan_expected_reclaim_bytes")
	b.ReportMetric(float64(plan.ExpectedReclaimRatioPPM), "leafgen_plan_expected_reclaim_ratio_ppm")
	b.ReportMetric(float64(plan.ExpectedReclaimPerByteCopiedPPM), "leafgen_plan_expected_reclaim_per_copy_ppm")
	if len(plan.CandidateGenerationIDs) == 0 {
		gcStart := time.Now()
		gcStats, err := backend.LeafGenerationGC(ctx, backenddb.LeafGenerationGCOptions{})
		if err != nil {
			b.Fatalf("TreeDB leaf-generation GC without pack candidates: %v", err)
		}
		gcElapsed := time.Since(gcStart)
		if err := backend.Checkpoint(); err != nil {
			b.Fatalf("checkpoint after TreeDB leaf-generation GC without pack candidates: %v", err)
		}
		afterGCBytes, err := benchmarkTreeDBDiskUsageBytes(backend)
		if err != nil {
			b.Fatalf("TreeDB disk usage after leaf-generation GC without pack candidates: %v", err)
		}
		benchmarkReportTreeDBLeafGenerationPackGCNoop(b, docs, beforeTotalBytes, afterGCBytes, gcElapsed, gcStats)
		benchmarkReportTreeDBPostMaintenanceIndexVacuum(b, backend, docs, beforeTotalBytes, afterGCBytes, "leafgen_pack_gc")
		return
	}

	packStart := time.Now()
	packStats, err := backend.LeafGenerationPackFromPlan(ctx, backenddb.LeafGenerationPackFromPlanOptions{
		Force:          force,
		MaxGenerations: maxGenerations,
		Sync:           true,
		LeafFrameK:     leafFrameK,
	})
	if err != nil {
		b.Fatalf("TreeDB leaf-generation pack: %v", err)
	}
	packElapsed := time.Since(packStart)
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint after TreeDB leaf-generation pack: %v", err)
	}
	afterPackBytes, err := benchmarkTreeDBDiskUsageBytes(backend)
	if err != nil {
		b.Fatalf("TreeDB disk usage after leaf-generation pack: %v", err)
	}

	gcStart := time.Now()
	gcStats, err := backend.LeafGenerationGC(ctx, backenddb.LeafGenerationGCOptions{})
	if err != nil {
		b.Fatalf("TreeDB leaf-generation GC after pack: %v", err)
	}
	gcElapsed := time.Since(gcStart)
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint after TreeDB leaf-generation GC: %v", err)
	}
	afterGCBytes, err := benchmarkTreeDBDiskUsageBytes(backend)
	if err != nil {
		b.Fatalf("TreeDB disk usage after leaf-generation GC: %v", err)
	}

	b.ReportMetric(float64(packElapsed.Nanoseconds()), "leafgen_pack_ns/op")
	b.ReportMetric(float64(beforeTotalBytes), "leafgen_pack_disk_total_bytes_before")
	b.ReportMetric(float64(afterPackBytes), "leafgen_pack_disk_total_bytes_after")
	b.ReportMetric(benchmarkSignedByteDelta(afterPackBytes, beforeTotalBytes), "leafgen_pack_disk_total_bytes_delta")
	b.ReportMetric(float64(afterPackBytes)/float64(docs), "leafgen_pack_disk_bytes/doc_after")
	b.ReportMetric(float64(packStats.GenerationsMatched), "leafgen_pack_generations_matched")
	b.ReportMetric(float64(packStats.SourceBytesTotal), "leafgen_pack_source_bytes_total")
	b.ReportMetric(float64(packStats.SourceBytesLive), "leafgen_pack_source_bytes_live")
	b.ReportMetric(float64(packStats.SourceBytesDead), "leafgen_pack_source_bytes_dead")
	b.ReportMetric(float64(packStats.SourceBytesToCopy), "leafgen_pack_source_bytes_to_copy")
	b.ReportMetric(float64(packStats.ExpectedReclaimBytes), "leafgen_pack_expected_reclaim_bytes")
	b.ReportMetric(float64(packStats.ExpectedReclaimRatioPPM), "leafgen_pack_expected_reclaim_ratio_ppm")
	b.ReportMetric(float64(packStats.ExpectedReclaimPerByteCopiedPPM), "leafgen_pack_expected_reclaim_per_copy_ppm")
	b.ReportMetric(float64(packStats.LeafPagesCopied), "leafgen_pack_leaf_pages_copied")
	b.ReportMetric(float64(packStats.LeafFramesWritten), "leafgen_pack_leaf_frames_written")
	b.ReportMetric(float64(packStats.MaxLeafFrameK), "leafgen_pack_max_leaf_frame_k")
	b.ReportMetric(float64(packStats.BytesCopied), "leafgen_pack_bytes_copied")
	b.ReportMetric(float64(len(packStats.CreatedFileIDs)), "leafgen_pack_created_files")
	b.ReportMetric(float64(gcElapsed.Nanoseconds()), "leafgen_gc_ns/op")
	b.ReportMetric(float64(afterGCBytes), "leafgen_pack_gc_disk_total_bytes_after")
	b.ReportMetric(benchmarkSignedByteDelta(afterGCBytes, beforeTotalBytes), "leafgen_pack_gc_disk_total_bytes_delta")
	b.ReportMetric(float64(afterGCBytes)/float64(docs), "leafgen_pack_gc_disk_bytes/doc_after")
	b.ReportMetric(float64(gcStats.GenerationsDeleted), "leafgen_gc_generations_deleted")
	b.ReportMetric(float64(gcStats.FilesDeleted), "leafgen_gc_files_deleted")
	b.ReportMetric(float64(gcStats.BytesDeleted), "leafgen_gc_bytes_deleted")
	benchmarkReportTreeDBPostMaintenanceIndexVacuum(b, backend, docs, beforeTotalBytes, afterGCBytes, "leafgen_pack_gc")
}

func benchmarkReportTreeDBPostMaintenanceIndexVacuum(b *testing.B, backend *backenddb.DB, docs int, beforeTotalBytes, afterGCBytes uint64, metricPrefix string) uint64 {
	b.Helper()

	if backend == nil || docs <= 0 || strings.TrimSpace(metricPrefix) == "" ||
		!benchmarkBoolEnv(b, "TREEDB_COLLECTION_REPORT_POST_MAINTENANCE_INDEX_VACUUM", true) {
		return afterGCBytes
	}

	vacuumStart := time.Now()
	if err := backend.VacuumIndexOnline(context.Background()); err != nil {
		if errors.Is(err, backenddb.ErrVacuumUnsupported) {
			b.Logf("TreeDB index vacuum after %s unsupported on this platform; skipping post-maintenance vacuum metrics", metricPrefix)
			return afterGCBytes
		}
		b.Fatalf("TreeDB index vacuum after %s: %v", metricPrefix, err)
	}
	vacuumElapsed := time.Since(vacuumStart)
	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("checkpoint after TreeDB index vacuum after %s: %v", metricPrefix, err)
	}
	afterVacuumBytes, err := benchmarkTreeDBDiskUsageBytes(backend)
	if err != nil {
		b.Fatalf("TreeDB disk usage after index vacuum after %s: %v", metricPrefix, err)
	}

	b.ReportMetric(float64(vacuumElapsed.Nanoseconds()), metricPrefix+"_vacuum_ns/op")
	b.ReportMetric(float64(afterVacuumBytes), metricPrefix+"_vacuum_disk_total_bytes_after")
	b.ReportMetric(benchmarkSignedByteDelta(afterVacuumBytes, beforeTotalBytes), metricPrefix+"_vacuum_disk_total_bytes_delta")
	b.ReportMetric(float64(afterVacuumBytes)/float64(docs), metricPrefix+"_vacuum_disk_bytes/doc_after")
	return afterVacuumBytes
}

func benchmarkReportTreeDBLeafGenerationPackGCNoop(b *testing.B, docs int, beforeTotalBytes, afterGCBytes uint64, gcElapsed time.Duration, gcStats backenddb.LeafGenerationGCStats) {
	b.Helper()
	bytesPerDoc := float64(beforeTotalBytes) / float64(docs)
	gcBytesPerDoc := float64(afterGCBytes) / float64(docs)
	b.ReportMetric(float64(beforeTotalBytes), "leafgen_pack_disk_total_bytes_before")
	b.ReportMetric(float64(beforeTotalBytes), "leafgen_pack_disk_total_bytes_after")
	b.ReportMetric(0, "leafgen_pack_disk_total_bytes_delta")
	b.ReportMetric(bytesPerDoc, "leafgen_pack_disk_bytes/doc_after")
	b.ReportMetric(0, "leafgen_pack_ns/op")
	b.ReportMetric(0, "leafgen_pack_generations_matched")
	b.ReportMetric(0, "leafgen_pack_source_bytes_total")
	b.ReportMetric(0, "leafgen_pack_source_bytes_live")
	b.ReportMetric(0, "leafgen_pack_source_bytes_dead")
	b.ReportMetric(0, "leafgen_pack_source_bytes_to_copy")
	b.ReportMetric(0, "leafgen_pack_expected_reclaim_bytes")
	b.ReportMetric(0, "leafgen_pack_expected_reclaim_ratio_ppm")
	b.ReportMetric(0, "leafgen_pack_expected_reclaim_per_copy_ppm")
	b.ReportMetric(0, "leafgen_pack_leaf_pages_copied")
	b.ReportMetric(0, "leafgen_pack_leaf_frames_written")
	b.ReportMetric(0, "leafgen_pack_max_leaf_frame_k")
	b.ReportMetric(0, "leafgen_pack_bytes_copied")
	b.ReportMetric(0, "leafgen_pack_created_files")
	b.ReportMetric(float64(gcElapsed.Nanoseconds()), "leafgen_gc_ns/op")
	b.ReportMetric(float64(afterGCBytes), "leafgen_pack_gc_disk_total_bytes_after")
	b.ReportMetric(benchmarkSignedByteDelta(afterGCBytes, beforeTotalBytes), "leafgen_pack_gc_disk_total_bytes_delta")
	b.ReportMetric(gcBytesPerDoc, "leafgen_pack_gc_disk_bytes/doc_after")
	b.ReportMetric(float64(gcStats.GenerationsDeleted), "leafgen_gc_generations_deleted")
	b.ReportMetric(float64(gcStats.FilesDeleted), "leafgen_gc_files_deleted")
	b.ReportMetric(float64(gcStats.BytesDeleted), "leafgen_gc_bytes_deleted")
}

func benchmarkSignedByteDelta(after, before uint64) float64 {
	if after >= before {
		return float64(after - before)
	}
	return -float64(before - after)
}

func benchmarkTreeDBDiskUsageBytes(backend *backenddb.DB) (uint64, error) {
	dir := backend.Dir()
	if dir == "" {
		return 0, nil
	}
	indexPath := filepath.Clean(filepath.Join(dir, "index.db"))
	indexBytes, err := benchmarkFileUsageBytes(indexPath)
	if err != nil {
		return 0, err
	}
	otherBytes, err := benchmarkDirectoryUsageBytes(dir, indexPath)
	if err != nil {
		return 0, err
	}
	return indexBytes + otherBytes, nil
}

func benchmarkFileUsageBytes(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 {
		return 0, nil
	}
	return uint64(info.Size()), nil
}

func benchmarkDirectoryUsageBytes(root, skipPath string) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if skipPath != "" && filepath.Clean(path) == skipPath {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 {
			return nil
		}
		total += uint64(info.Size())
		return nil
	})
	return total, err
}

func TestBenchmarkCollectionStoragePolicyDefaultsProductionMainline(t *testing.T) {
	t.Setenv("TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG", "")
	t.Setenv("TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG", "")
	dataOuter, indexOuter := benchmarkCollectionStoragePolicy(t)
	if !dataOuter || indexOuter {
		t.Fatalf("benchmark storage defaults data_outer=%t index_outer=%t want production-mainline data_outer=true index_outer=false", dataOuter, indexOuter)
	}
}

func TestBenchmarkCollectionChunkSizeEnv(t *testing.T) {
	t.Setenv("TREEDB_COLLECTION_CHUNK_SIZE", "")
	if got := benchmarkInt64Env(t, "TREEDB_COLLECTION_CHUNK_SIZE", 0); got != 0 {
		t.Fatalf("default chunk size override=%d want 0", got)
	}
	t.Setenv("TREEDB_COLLECTION_CHUNK_SIZE", "65536")
	if got := benchmarkInt64Env(t, "TREEDB_COLLECTION_CHUNK_SIZE", 0); got != 65536 {
		t.Fatalf("chunk size override=%d want 65536", got)
	}
}

func openBenchmarkBackend(b *testing.B, dir string) (*backenddb.DB, func() error) {
	b.Helper()

	dataOuter, indexOuter := benchmarkCollectionStoragePolicy(b)
	opts := treedb.OptionsFor(benchmarkTreeDBProfile(b), dir)
	opts.IndexOuterLeavesInValueLog = dataOuter || indexOuter
	opts.IndexInternalBaseDelta = !opts.IndexOuterLeavesInValueLog
	if chunkSize := benchmarkInt64Env(b, "TREEDB_COLLECTION_CHUNK_SIZE", 0); chunkSize > 0 {
		opts.ChunkSize = chunkSize
	}
	if syncConcurrency := benchmarkIntEnv(b, "TREEDB_COLLECTION_PAGER_SYNC_CONCURRENCY", 0); syncConcurrency > 0 {
		opts.PagerSyncConcurrency = syncConcurrency
	}
	open := treedb.OpenBackend
	if opts.IndexOuterLeavesInValueLog {
		open = treedb.OpenBackendWithCachedLeafLog
	}
	backend, cleanup, err := open(opts)
	if err != nil {
		b.Fatalf("open backend: %v", err)
	}
	return backend, cleanup
}

func openBenchmarkCollection(b *testing.B, name string, indexes ...collections.IndexDefinition) (*backenddb.DB, *collections.Collection) {
	b.Helper()

	backend, cleanup := openBenchmarkBackend(b, b.TempDir())
	b.Cleanup(func() {
		if err := cleanup(); err != nil {
			b.Errorf("close backend: %v", err)
		}
	})

	manager := collections.NewCollectionManager(backend)
	dataOuter, indexOuter := benchmarkCollectionStoragePolicy(b)
	documentFormat := benchmarkCollectionDocumentFormat(b)
	bufferedIndexedWrites := benchmarkBoolEnv(b, "TREEDB_COLLECTION_BUFFERED_INDEXED_WRITES", true) && len(indexes) > 0
	bufferedIndexedWriteMaxDocuments := benchmarkIntEnv(b, "TREEDB_COLLECTION_BUFFERED_INDEXED_WRITE_MAX_DOCUMENTS", 0)
	bufferedIndexedWriteMaxBytes := benchmarkInt64Env(b, "TREEDB_COLLECTION_BUFFERED_INDEXED_WRITE_MAX_BYTES", 0)
	for i := range indexes {
		indexes[i].StoragePolicy = benchmarkRootStoragePolicy(indexOuter)
	}
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: name,
		Options: collections.CollectionOptions{
			DocumentFormat:                   documentFormat,
			DataRootStoragePolicy:            benchmarkRootStoragePolicy(dataOuter),
			IndexStateStoragePolicy:          benchmarkRootStoragePolicy(dataOuter),
			DisableIndexedWriteMemtables:     !bufferedIndexedWrites && len(indexes) > 0,
			BufferedIndexedWrites:            bufferedIndexedWrites,
			BufferedIndexedWriteMaxDocuments: bufferedIndexedWriteMaxDocuments,
			BufferedIndexedWriteMaxBytes:     bufferedIndexedWriteMaxBytes,
		},
		Indexes: indexes,
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection(name)
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	return backend, collection
}

func benchmarkSyncBoundary(b *testing.B, backend *backenddb.DB) {
	b.Helper()

	if err := backend.Checkpoint(); err != nil {
		b.Fatalf("sync boundary: %v", err)
	}
}

func appendZeroPaddedInt(dst []byte, n, width int) []byte {
	var scratch [20]byte
	pos := len(scratch)
	if n == 0 {
		pos--
		scratch[pos] = '0'
	} else {
		for n > 0 {
			pos--
			scratch[pos] = byte('0' + n%10)
			n /= 10
		}
	}
	for pad := width - (len(scratch) - pos); pad > 0; pad-- {
		dst = append(dst, '0')
	}
	return append(dst, scratch[pos:]...)
}

func benchmarkDocumentID(n int) []byte {
	out := make([]byte, 0, len("u-")+9)
	out = append(out, "u-"...)
	return appendZeroPaddedInt(out, n, 9)
}

func benchmarkIndexedDocument(n int) []byte {
	out := make([]byte, 0, 112)
	out = append(out, `{"name":"user-`...)
	out = appendZeroPaddedInt(out, n, 9)
	out = append(out, `","email":"user-`...)
	out = appendZeroPaddedInt(out, n, 9)
	out = append(out, `@example.com","city":"city-`...)
	out = appendZeroPaddedInt(out, n%collectionBenchCities, 2)
	out = append(out, `","pad":"`...)
	out = append(out, collectionBenchIndexedPad...)
	out = append(out, `"}`...)
	return out
}

func benchmarkTemplateDocument(tb testing.TB, encoder *collections.TemplateV1Encoder, n int, indexed bool) []byte {
	tb.Helper()
	if encoder == nil {
		encoder = &collections.TemplateV1Encoder{}
	}
	if indexed {
		doc, err := encoder.EncodeDocument(
			[]string{"name", "email", "city", "pad"},
			[]any{
				fmt.Sprintf("user-%09d", n),
				fmt.Sprintf("user-%09d@example.com", n),
				fmt.Sprintf("city-%02d", n%collectionBenchCities),
				collectionBenchIndexedPad,
			},
		)
		if err != nil {
			tb.Fatalf("encode template-v1 benchmark document: %v", err)
		}
		return doc
	}
	doc, err := encoder.EncodeDocument(
		[]string{"name", "city", "email", "pad"},
		[]any{"ada", "hnl", "ada@example.com", "0123456789012345678901234567890123456789"},
	)
	if err != nil {
		tb.Fatalf("encode template-v1 benchmark payload: %v", err)
	}
	return doc
}

func benchmarkBSONDocument(tb testing.TB, n int, indexed bool) []byte {
	tb.Helper()
	var doc bson.D
	if indexed {
		doc = bson.D{
			{Key: "name", Value: fmt.Sprintf("user-%09d", n)},
			{Key: "email", Value: fmt.Sprintf("user-%09d@example.com", n)},
			{Key: "city", Value: fmt.Sprintf("city-%02d", n%collectionBenchCities)},
			{Key: "pad", Value: collectionBenchIndexedPad},
		}
	} else {
		doc = bson.D{
			{Key: "name", Value: "ada"},
			{Key: "city", Value: "hnl"},
			{Key: "email", Value: "ada@example.com"},
			{Key: "pad", Value: "0123456789012345678901234567890123456789"},
		}
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		tb.Fatalf("encode BSON benchmark document: %v", err)
	}
	return raw
}

func benchmarkDocumentBatch(tb testing.TB, start, count int, indexed bool) ([][]byte, [][]byte) {
	tb.Helper()
	documentFormat := benchmarkCollectionDocumentFormat(tb)
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	var templateEncoder collections.TemplateV1Encoder
	for i := 0; i < count; i++ {
		docNum := start + i
		ids[i] = benchmarkDocumentID(docNum)
		if documentFormat == collections.DocumentFormatTemplateV1 {
			docs[i] = benchmarkTemplateDocument(tb, &templateEncoder, docNum, indexed)
		} else if documentFormat == collections.DocumentFormatBSON {
			docs[i] = benchmarkBSONDocument(tb, docNum, indexed)
		} else if indexed {
			docs[i] = benchmarkIndexedDocument(docNum)
		} else {
			docs[i] = collectionBenchPayload
		}
	}
	return ids, docs
}

func seedBenchmarkCollection(b *testing.B, collection *collections.Collection, start, count int, indexed bool) [][]byte {
	b.Helper()

	targetBatchSize := benchmarkBatchSize(b)
	allIDs := make([][]byte, 0, count)
	for inserted := 0; inserted < count; inserted += targetBatchSize {
		batchSize := targetBatchSize
		if remaining := count - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(b, start+inserted, batchSize, indexed)
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("seed insert batch: %v", err)
		}
		allIDs = append(allIDs, ids...)
	}
	return allIDs
}

func secondaryIndexes() []collections.IndexDefinition {
	return []collections.IndexDefinition{
		{Name: "email_idx", Field: "email", Unique: true},
		{Name: "city_idx", Field: "city"},
	}
}

func BenchmarkCollectionInsertProvidedID(b *testing.B) {
	_, collection := openBenchmarkCollection(b, "bench_insert_provided")
	ids, docs := benchmarkDocumentBatch(b, 0, b.N, false)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collection.Insert(ids[i], docs[i]); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func BenchmarkCollectionInsertBatchProvidedID(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_insert_batch_provided")
	targetBatchSize := benchmarkBatchSize(b)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(b, inserted, batchSize, false)
		b.StartTimer()

		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("insert batch: %v", err)
		}
		inserted += batchSize
	}
	b.StopTimer()
	b.ReportMetric(float64(targetBatchSize), "target_docs/batch")
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
}

func BenchmarkCollectionGetByID(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_get")
	ids := seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, false)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collection.Get(ids[i%len(ids)]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkCollectionGetByIDParallel(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_get_parallel")
	ids := seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, false)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := collection.Get(ids[i%len(ids)]); err != nil {
				b.Errorf("parallel get: %v", err)
			}
			i++
		}
	})
}

func BenchmarkCollectionDeleteByID(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_delete")
	ids := seedBenchmarkCollection(b, collection, 0, b.N, false)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := collection.Delete(ids[i]); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func BenchmarkCollectionInsertWithSecondaryIndexes(b *testing.B) {
	_, collection := openBenchmarkCollection(b, "bench_insert_secondary", secondaryIndexes()...)
	ids, docs := benchmarkDocumentBatch(b, 0, b.N, true)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collection.Insert(ids[i], docs[i]); err != nil {
			b.Fatalf("insert with secondary indexes: %v", err)
		}
	}
}

func BenchmarkCollectionInsertBatchWithSecondaryIndexes(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_insert_batch_secondary", secondaryIndexes()...)
	targetBatchSize := benchmarkBatchSize(b)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(b, inserted, batchSize, true)
		b.StartTimer()

		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("insert batch with secondary indexes: %v", err)
		}
		inserted += batchSize
	}
	b.StopTimer()
	b.ReportMetric(float64(targetBatchSize), "target_docs/batch")
	b.ReportMetric(2, "indexes/doc")
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
	benchmarkReportTreeDBDiskUsage(b, backend, b.N)
}

func BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_insert_batch_checkpoint_secondary", secondaryIndexes()...)
	targetBatchSize := benchmarkBatchSize(b)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)
	var insertElapsed time.Duration
	var syncElapsed time.Duration

	b.ReportAllocs()
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(b, inserted, batchSize, true)
		b.StartTimer()

		insertStart := time.Now()
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("insert batch with secondary indexes: %v", err)
		}
		insertElapsed += time.Since(insertStart)
		syncStart := time.Now()
		benchmarkSyncBoundary(b, backend)
		syncElapsed += time.Since(syncStart)
		inserted += batchSize
	}
	b.StopTimer()
	b.ReportMetric(float64(targetBatchSize), "target_docs/checkpoint")
	b.ReportMetric(2, "indexes/doc")
	benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
	benchmarkReportTreeDBDiskUsage(b, backend, b.N)
}

func BenchmarkCollectionDeleteWithSecondaryIndexes(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_delete_secondary", secondaryIndexes()...)
	ids := seedBenchmarkCollection(b, collection, 0, b.N, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := collection.Delete(ids[i]); err != nil {
			b.Fatalf("delete with secondary indexes: %v", err)
		}
	}
}

func BenchmarkSecondaryLookupUnique(b *testing.B) {
	backend, collection := openBenchmarkCollection(
		b,
		"bench_secondary_unique",
		collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true},
	)
	seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		email := fmt.Sprintf("user-%09d@example.com", i%collectionBenchSeedDocs)
		if _, err := collection.FindByIndex("email_idx", email); err != nil {
			b.Fatalf("lookup unique: %v", err)
		}
	}
}

func BenchmarkSecondaryLookupNonUnique(b *testing.B) {
	backend, collection := openBenchmarkCollection(
		b,
		"bench_secondary_non_unique",
		collections.IndexDefinition{Name: "city_idx", Field: "city"},
	)
	seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		city := fmt.Sprintf("city-%02d", i%collectionBenchCities)
		if _, err := collection.FindByIndex("city_idx", city); err != nil {
			b.Fatalf("lookup non-unique: %v", err)
		}
	}
}

func BenchmarkCollectionCreateIndexBackfillExistingDocs(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir, err := os.MkdirTemp("", "gomap_collections_backfill_*")
		if err != nil {
			b.Fatalf("create temp dir: %v", err)
		}
		backend, cleanup := openBenchmarkBackend(b, dir)
		manager := collections.NewCollectionManager(backend)
		dataOuter, indexOuter := benchmarkCollectionStoragePolicy(b)
		if _, err := manager.CreateCollection(&collections.CollectionMeta{
			Name: "bench_backfill",
			Options: collections.CollectionOptions{
				DataRootStoragePolicy:   benchmarkRootStoragePolicy(dataOuter),
				IndexStateStoragePolicy: benchmarkRootStoragePolicy(dataOuter),
			},
		}); err != nil {
			_ = cleanup()
			_ = os.RemoveAll(dir)
			b.Fatalf("create collection: %v", err)
		}
		collection, err := manager.OpenCollection("bench_backfill")
		if err != nil {
			_ = cleanup()
			_ = os.RemoveAll(dir)
			b.Fatalf("open collection: %v", err)
		}
		seedBenchmarkCollection(b, collection, 0, collectionBenchBackfill, true)
		benchmarkSyncBoundary(b, backend)
		b.StartTimer()

		if _, err := collection.CreateIndex(collections.IndexDefinition{
			Name:          "email_idx",
			Field:         "email",
			Unique:        true,
			StoragePolicy: benchmarkRootStoragePolicy(indexOuter),
		}); err != nil {
			b.Fatalf("create index with backfill: %v", err)
		}

		b.StopTimer()
		if err := cleanup(); err != nil {
			_ = os.RemoveAll(dir)
			b.Fatalf("close backend: %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatalf("remove temp dir: %v", err)
		}
		b.StartTimer()
	}
}
