package db

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
)

func BenchmarkCompactStorageSharedAudit(b *testing.B) {
	db := openCompactStorageAuditBenchmarkFixture(b, 4096, 256)
	opts := CompactStorageOptions{
		Mode:                           CompactStorageFull,
		DisableZeroByteValueLogCleanup: true,
	}
	var refScans, liveScans, leafScans atomic.Uint64
	unregisterRefs := registerScanValueLogRefCountsHook(func() { refScans.Add(1) })
	unregisterLive := registerRewritePlanLiveEstimateHook(func() { liveScans.Add(1) })
	unregisterLeaf := registerLeafGenerationLiveScanHook(func() { leafScans.Add(1) })
	b.Cleanup(unregisterRefs)
	b.Cleanup(unregisterLive)
	b.Cleanup(unregisterLeaf)

	var audit compactStorageAuditBenchmarkCounters
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.rewritePlanLiveBytesMu.Lock()
		db.rewritePlanLiveBytesCache = valueLogRewriteLiveBytesCache{}
		db.rewritePlanLiveBytesMu.Unlock()
		db.clearLeafGenerationReachabilityCaches()
		b.StartTimer()
		stats, err := db.CompactStoragePlan(context.Background(), opts)
		b.StopTimer()
		if err != nil {
			b.Fatalf("CompactStoragePlan: %v", err)
		}
		audit.add(stats)
	}
	if b.N > 0 {
		b.ReportMetric(float64(refScans.Load())/float64(b.N), "legacy_ref_scans/op")
		b.ReportMetric(float64(liveScans.Load())/float64(b.N), "legacy_live_scans/op")
		b.ReportMetric(float64(leafScans.Load())/float64(b.N), "legacy_leaf_scans/op")
		audit.report(b)
	}
}

type compactStorageAuditBenchmarkCounters struct {
	pagesVisited        uint64
	pointerProjections  uint64
	groupedDedupeHits   uint64
	physicalBytesRead   uint64
	sharedScans         uint64
	reuseHits           uint64
	reuseMisses         uint64
	revalidationRetries uint64
	auditUnavailable    uint64
	missReasons         map[string]uint64
}

func (c *compactStorageAuditBenchmarkCounters) add(stats any) {
	value := reflect.ValueOf(stats)
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Struct {
		c.auditUnavailable++
		return
	}
	audit := value.FieldByName("Audit")
	if !audit.IsValid() || audit.Kind() != reflect.Struct {
		c.auditUnavailable++
		return
	}
	c.pagesVisited += compactStorageAuditBenchmarkUint(audit, "PagesVisited")
	c.pointerProjections += compactStorageAuditBenchmarkUint(audit, "PointerProjections")
	c.groupedDedupeHits += compactStorageAuditBenchmarkUint(audit, "GroupedRecordDedupeHits")
	c.physicalBytesRead += compactStorageAuditBenchmarkUint(audit, "PhysicalBytesRead")
	c.sharedScans += compactStorageAuditBenchmarkUint(audit, "SharedScans")
	c.reuseHits += compactStorageAuditBenchmarkUint(audit, "StructuralReuseHits")
	c.reuseMisses += compactStorageAuditBenchmarkUint(audit, "StructuralReuseMisses")
	c.revalidationRetries += compactStorageAuditBenchmarkUint(audit, "RevalidationRetries")
	if reason := audit.FieldByName("LastStructuralReuseMissReason"); reason.IsValid() && reason.Kind() == reflect.String && reason.String() != "" {
		if c.missReasons == nil {
			c.missReasons = make(map[string]uint64)
		}
		c.missReasons[compactStorageAuditBenchmarkMetricToken(reason.String())]++
	}
}

func compactStorageAuditBenchmarkUint(value reflect.Value, field string) uint64 {
	metric := value.FieldByName(field)
	if !metric.IsValid() || metric.Kind() != reflect.Uint64 {
		return 0
	}
	return metric.Uint()
}

func compactStorageAuditBenchmarkMetricToken(value string) string {
	metric := []byte(value)
	for i, ch := range metric {
		switch {
		case ch >= 'A' && ch <= 'Z':
			metric[i] = ch + ('a' - 'A')
		case ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9':
		default:
			metric[i] = '_'
		}
	}
	return string(metric)
}

func (c *compactStorageAuditBenchmarkCounters) report(b *testing.B) {
	operations := float64(b.N)
	b.ReportMetric(float64(c.pagesVisited)/operations, "pages_visited/op")
	b.ReportMetric(float64(c.pointerProjections)/operations, "pointer_projections/op")
	b.ReportMetric(float64(c.groupedDedupeHits)/operations, "grouped_dedupe_hits/op")
	b.ReportMetric(float64(c.physicalBytesRead)/operations, "physical_bytes_read/op")
	b.ReportMetric(float64(c.sharedScans)/operations, "shared_scans/op")
	b.ReportMetric(float64(c.reuseHits)/operations, "reuse_hits/op")
	b.ReportMetric(float64(c.reuseMisses)/operations, "reuse_misses/op")
	b.ReportMetric(float64(c.revalidationRetries)/operations, "revalidation_retries/op")
	b.ReportMetric(float64(c.auditUnavailable)/operations, "audit_unavailable/op")
	for reason, count := range c.missReasons {
		b.ReportMetric(float64(count)/operations, "reuse_miss_reason_"+reason+"/op")
	}
}

func openCompactStorageAuditBenchmarkFixture(b *testing.B, records, valueSize int) *DB {
	b.Helper()
	dir := b.TempDir()
	ptrs := appendPointersInNewSegmentBench(b, dir, 0, 1, 1, records, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i%23)}, valueSize)
	})
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)
	b.Cleanup(func() { benchmarkCloseNoErr(b, leafLog) })
	b.Cleanup(func() { benchmarkCloseNoErr(b, db) })

	batch := db.NewBatch().(*Batch)
	for i := 0; i < records; i++ {
		key := []byte(fmt.Sprintf("audit-%06d", i))
		if err := batch.SetPointer(key, ptrs[i]); err != nil {
			b.Fatalf("SetPointer %d: %v", i, err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		b.Fatalf("WriteSync: %v", err)
	}
	benchmarkCloseNoErr(b, batch)
	if err := db.RefreshValueLogSet(); err != nil {
		b.Fatalf("RefreshValueLogSet: %v", err)
	}
	return db
}
