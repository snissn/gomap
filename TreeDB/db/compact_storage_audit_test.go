package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCompactStorageAudit_DryRunUsesOneSharedWalkAndNoLegacyScanners(t *testing.T) {
	db := openCompactStorageRewritePolicyBenchmarkFixture(t, 128, 16, 256)
	defer closeNoErr(t, db)

	var shared, refScans, liveScans, leafScans atomic.Uint64
	unregisterShared := registerCompactStorageSharedAuditScanHook(func(compactStorageAuditCounters) {
		shared.Add(1)
	})
	unregisterRefs := registerScanValueLogRefCountsHook(func() { refScans.Add(1) })
	unregisterLive := registerRewritePlanLiveEstimateHook(func() { liveScans.Add(1) })
	unregisterLeaf := registerLeafGenerationLiveScanHook(func() { leafScans.Add(1) })
	t.Cleanup(func() {
		unregisterLeaf()
		unregisterLive()
		unregisterRefs()
		unregisterShared()
	})

	stats, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{
		Mode:                           CompactStorageFull,
		DisableZeroByteValueLogCleanup: true,
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := shared.Load(); got != 1 {
		t.Fatalf("shared scans=%d want 1", got)
	}
	if got := refScans.Load(); got != 0 {
		t.Fatalf("scanValueLogRefCounts calls=%d want 0", got)
	}
	if got := liveScans.Load(); got != 0 {
		t.Fatalf("estimateValueLogLiveBytesBySegment calls=%d want 0", got)
	}
	if got := leafScans.Load(); got != 0 {
		t.Fatalf("legacy leaf-generation live scans=%d want 0", got)
	}
	if stats.Audit.SharedScans != 1 || stats.Audit.PagesVisited == 0 || stats.Audit.PointerProjections == 0 {
		t.Fatalf("unexpected audit counters: %+v", stats.Audit)
	}
}

func TestCompactStorageAuditKey_ExactInvalidation(t *testing.T) {
	setA := &valuelog.Set{}
	setB := &valuelog.Set{}
	base := compactStorageAuditKey{
		CommitSeq:                  1,
		RootPageID:                 2,
		SystemRootPageID:           3,
		LeafGenerationStateVersion: 4,
		ValueLogSetIdentity:        setA,
		ProtectedRootSetHash:       [32]byte{5},
		ProtectedPathSetHash:       [32]byte{6},
	}
	tests := map[string]func(*compactStorageAuditKey){
		"commit sequence":         func(k *compactStorageAuditKey) { k.CommitSeq++ },
		"root":                    func(k *compactStorageAuditKey) { k.RootPageID++ },
		"system root":             func(k *compactStorageAuditKey) { k.SystemRootPageID++ },
		"leaf generation version": func(k *compactStorageAuditKey) { k.LeafGenerationStateVersion++ },
		"value-log set identity":  func(k *compactStorageAuditKey) { k.ValueLogSetIdentity = setB },
		"protected roots":         func(k *compactStorageAuditKey) { k.ProtectedRootSetHash[0]++ },
		"protected paths":         func(k *compactStorageAuditKey) { k.ProtectedPathSetHash[0]++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			got := base
			mutate(&got)
			if got == base {
				t.Fatalf("mutation did not invalidate key: %+v", got)
			}
		})
	}
}

func TestCompactStorageAudit_ValueLogCollectorsMatchLegacyGroupedAliases(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 240_000, 2, func(int) []byte {
		return bytes.Repeat([]byte("grouped"), 64)
	})
	active := appendPointersInNewSegment(t, dir, 0, 2, 250_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("active"), 64)
	})[0]
	base := ptrs[0]
	recordLen := page.ValuePtrRecordLength(base)
	b := db.NewBatch().(*Batch)
	for i := 0; i < 3; i++ {
		ptr := base
		ptr.Length = page.ValuePtrMarkGrouped(recordLen, uint8(i))
		if err := b.SetPointer([]byte(fmt.Sprintf("grouped-%d", i)), ptr); err != nil {
			t.Fatalf("SetPointer grouped %d: %v", i, err)
		}
	}
	if err := b.SetPointer([]byte("active"), active); err != nil {
		t.Fatalf("SetPointer active: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	closeNoErr(t, b)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	wantRefs, _, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("legacy refs: %v", err)
	}
	wantLive, err := db.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("legacy live bytes: %v", err)
	}
	input, err := db.acquireCompactStorageAuditInput(CompactStorageOptions{})
	if err != nil {
		t.Fatalf("acquire audit input: %v", err)
	}
	defer input.close()
	got, err := db.scanCompactStorageAudit(context.Background(), input)
	if err != nil {
		t.Fatalf("shared audit: %v", err)
	}
	if !reflect.DeepEqual(got.valueLogRefCounts, wantRefs) {
		t.Fatalf("ref counts mismatch: shared=%v legacy=%v", got.valueLogRefCounts, wantRefs)
	}
	if !reflect.DeepEqual(got.valueLogLiveBytesBySegment, wantLive) {
		t.Fatalf("live bytes mismatch: shared=%v legacy=%v", got.valueLogLiveBytesBySegment, wantLive)
	}
	if got.valueLogRefCounts[base.FileID] != 3 {
		t.Fatalf("grouped alias refs=%d want 3", got.valueLogRefCounts[base.FileID])
	}
	if got.valueLogLiveBytesBySegment[base.FileID] != int64(recordLen) {
		t.Fatalf("grouped live bytes=%d want one record=%d", got.valueLogLiveBytesBySegment[base.FileID], recordLen)
	}
	if got.counters.GroupedRecordDedupeHits != 2 {
		t.Fatalf("grouped dedupe hits=%d want 2", got.counters.GroupedRecordDedupeHits)
	}
}

func TestCompactStorageAudit_CountsValuePointersInsideOuterLeafPages(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	const records = 512
	ptrs := appendPointersInNewSegment(t, db.dir, 0, 1, 260_000, records, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i%23)}, 256)
	})
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet before write: %v", err)
	}
	b := db.NewBatch().(*Batch)
	for i, ptr := range ptrs {
		if err := b.SetPointer([]byte(fmt.Sprintf("outer-%06d", i)), ptr); err != nil {
			t.Fatalf("SetPointer %d: %v", i, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	closeNoErr(t, b)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet after write: %v", err)
	}

	wantRefs, _, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("legacy refs: %v", err)
	}
	input, err := db.acquireCompactStorageAuditInput(CompactStorageOptions{})
	if err != nil {
		t.Fatalf("acquire audit input: %v", err)
	}
	defer input.close()
	got, err := db.scanCompactStorageAudit(context.Background(), input)
	if err != nil {
		t.Fatalf("shared audit: %v", err)
	}
	fileID := ptrs[0].FileID
	if wantRefs[fileID] != records {
		t.Fatalf("legacy refs for file %d=%d want %d", fileID, wantRefs[fileID], records)
	}
	if !reflect.DeepEqual(got.valueLogRefCounts, wantRefs) {
		t.Fatalf("outer-leaf ref counts mismatch: shared=%v legacy=%v", got.valueLogRefCounts, wantRefs)
	}
}

func TestCompactStorageAudit_PlannersMatchLegacyResults(t *testing.T) {
	db := openCompactStorageRewritePolicyBenchmarkFixture(t, 256, 32, 512)
	defer closeNoErr(t, db)

	opts := normalizeCompactStorageOptions(CompactStorageOptions{
		Mode:                           CompactStorageFull,
		DisableZeroByteValueLogCleanup: true,
	})
	protectedPaths := compactStorageFencedValueLogProtectedPaths(opts)
	wantRewrite, err := db.ValueLogRewritePlan(context.Background(), compactStorageRewritePlanOptions(opts, protectedPaths))
	if err != nil {
		t.Fatalf("legacy rewrite plan: %v", err)
	}
	wantGC, err := db.valueLogGC(context.Background(), ValueLogGCOptions{DryRun: true, ProtectedPaths: protectedPaths}, true)
	if err != nil {
		t.Fatalf("legacy value-log GC plan: %v", err)
	}

	got, err := db.CompactStoragePlan(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if !reflect.DeepEqual(got.ValueLogRewritePlan, wantRewrite) {
		t.Fatalf("rewrite plan mismatch:\nshared=%+v\nlegacy=%+v", got.ValueLogRewritePlan, wantRewrite)
	}
	if !reflect.DeepEqual(got.ValueLogGC, wantGC) {
		t.Fatalf("value-log GC plan mismatch:\nshared=%+v\nlegacy=%+v", got.ValueLogGC, wantGC)
	}
}

func TestCompactStorageAudit_ReusesStructureButRefreshesZeroByteDebt(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	session := &compactStorageAuditSession{}
	defer session.close()
	opts := normalizeCompactStorageOptions(CompactStorageOptions{})
	var first CompactStorageStats
	if _, err := db.populateCompactStorageAudit(context.Background(), opts, &first, true, nil, nil, session); err != nil {
		t.Fatalf("first audit: %v", err)
	}
	zeroPath := filepath.Join(ValueLogDirPath(db.dir), "value-l7-000777.log")
	if err := os.MkdirAll(filepath.Dir(zeroPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(zeroPath, nil, 0o644); err != nil {
		t.Fatalf("write zero-byte segment: %v", err)
	}
	var second CompactStorageStats
	debt, err := db.populateCompactStorageAudit(context.Background(), opts, &second, true, nil, nil, session)
	if err != nil {
		t.Fatalf("second audit: %v", err)
	}
	if second.Audit.SharedScans != 0 || second.Audit.StructuralReuseHits != 1 {
		t.Fatalf("expected structural reuse, counters=%+v", second.Audit)
	}
	if debt.ZeroByteValueLogFiles != 1 {
		t.Fatalf("zero-byte debt=%d want 1", debt.ZeroByteValueLogFiles)
	}
}

func TestCompactStorageAudit_StaleRevalidationRetriesOnce(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	var calls atomic.Uint64
	db.compactStorageAuditBeforeRevalidate = func(attempt int) {
		if calls.Add(1) != 1 {
			return
		}
		writeCompactStorageAuditInvalidation(t, db, "retry")
	}
	t.Cleanup(func() { db.compactStorageAuditBeforeRevalidate = nil })
	if _, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{}); err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("revalidation calls=%d want 2", got)
	}
	if _, ok := db.valueLogRefTracker.referencedSet(db.currentCommitSeq()); !ok {
		t.Fatal("successful retry did not replace tracker at current sequence")
	}
}

func TestCompactStorageAudit_ProtectedPathChangeRetriesOnce(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	protectedPath := "before"
	var calls atomic.Uint64
	db.compactStorageAuditBeforeRevalidate = func(attempt int) {
		if calls.Add(1) == 1 {
			protectedPath = "after"
		}
	}
	t.Cleanup(func() { db.compactStorageAuditBeforeRevalidate = nil })
	stats, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{
		ValueLogFencedProtectedPathsFunc: func() []string { return []string{protectedPath} },
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("revalidation calls=%d want 2", got)
	}
	if stats.Audit.RevalidationRetries != 1 || stats.Audit.SharedScans != 2 {
		t.Fatalf("unexpected retry counters: %+v", stats.Audit)
	}
}

func TestCompactStorageAudit_RepeatedInvalidationReturnsStaleError(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)
	db.valueLogRefTracker.invalidate()

	db.compactStorageAuditBeforeRevalidate = func(attempt int) {
		writeCompactStorageAuditInvalidation(t, db, fmt.Sprintf("stale-%d", attempt))
	}
	t.Cleanup(func() { db.compactStorageAuditBeforeRevalidate = nil })
	_, err = db.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if !errors.Is(err, ErrCompactStorageAuditStale) {
		t.Fatalf("CompactStoragePlan error=%v want ErrCompactStorageAuditStale", err)
	}
	if _, ok := db.valueLogRefTracker.referencedSet(db.currentCommitSeq()); ok {
		t.Fatal("repeated invalidation published a current tracker")
	}
}

func writeCompactStorageAuditInvalidation(t *testing.T, db *DB, key string) {
	t.Helper()
	b := db.NewBatch().(*Batch)
	if err := b.Set([]byte(key), []byte("value")); err != nil {
		closeNoErr(t, b)
		t.Fatalf("Set invalidation: %v", err)
	}
	if err := b.Write(); err != nil {
		closeNoErr(t, b)
		t.Fatalf("Write invalidation: %v", err)
	}
	closeNoErr(t, b)
}
