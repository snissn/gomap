package collections

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTextV2MaintenancePolicyTriggerSkipAndStats2732(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col, refundLive := createTextV2MaintenancePolicyFixture2732(t, d, 32)

	before, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats before: %v", err)
	}
	if before.V2DeletedDocs == 0 || before.V2MicroPostingBlocks == 0 || before.V2RewriteMergeState != TextIndexRewriteMergeStatePending {
		t.Fatalf("before stats=%+v want pending text-v2 rewrite debt", before)
	}

	skipped, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1_000, MinMicroPostingBlocks: 1_000, MinStalePostings: 1_000}})
	if err != nil {
		t.Fatalf("MaintainTextIndex skip: %v", err)
	}
	if skipped.IndexesRewritten != 0 || len(skipped.Indexes) != 1 || skipped.Indexes[0].Triggered || skipped.Indexes[0].SkippedReason != TextIndexMaintenanceSkipReasonBelowThresholds {
		t.Fatalf("skip stats=%+v want below-threshold no-op", skipped)
	}
	afterSkip, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats after skip: %v", err)
	}
	if afterSkip.V2RootGeneration != before.V2RootGeneration || afterSkip.V2DeletedDocs != before.V2DeletedDocs || afterSkip.V2MicroPostingBlocks != before.V2MicroPostingBlocks {
		t.Fatalf("skip mutated storage before=%+v after=%+v", before, afterSkip)
	}

	applied, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1}})
	if err != nil {
		t.Fatalf("MaintainTextIndex apply: %v", err)
	}
	if applied.IndexesRewritten != 1 || len(applied.Indexes) != 1 || !applied.Indexes[0].Triggered || !applied.Indexes[0].Applied || applied.Indexes[0].TriggerReason != TextIndexMaintenanceTriggerReasonDeletedDocs || applied.Indexes[0].SkippedReason != "" {
		t.Fatalf("apply stats=%+v want triggered applied rewrite without skipped reason", applied)
	}
	idx := applied.Indexes[0]
	if idx.Debt.DeletedDocuments == 0 || idx.Debt.MicroPostingBlocks == 0 || idx.Debt.StalePostings == 0 || idx.Rewrite.StalePostingsPurged == 0 {
		t.Fatalf("maintenance debt/rewrite=%+v/%+v want stale/tombstone metrics", idx.Debt, idx.Rewrite)
	}
	if idx.StorageAfter.V2DeletedDocs != 0 || idx.StorageAfter.V2MicroPostingBlocks != 0 || idx.StorageAfter.V2RewriteMergeState != TextIndexRewriteMergeStateCompacted {
		t.Fatalf("storage after=%+v want compacted logical text roots", idx.StorageAfter)
	}
	got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 64, ResultMode: TextSearchResultModeScoreOnly, CandidateLimit: 128, MaxPostingsScanned: 512})
	if err != nil || len(got.Results) != refundLive || got.Stats.DocumentsFetched != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("post-maintenance refund response=%+v err=%v want %d score-only zero-doc results", got, err, refundLive)
	}
}

func TestTextV2MaintenancePolicyBudgetAndTimeout2732(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col, _ := createTextV2MaintenancePolicyFixture2732(t, d, 48)

	budgeted, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
		Policy:   TextIndexMaintenancePolicy{MinDeletedDocuments: 1},
		MaxTerms: 1,
	})
	if err != nil {
		t.Fatalf("MaintainTextIndex budgeted: %v", err)
	}
	if !budgeted.BudgetExhausted || len(budgeted.Indexes) != 1 || budgeted.Indexes[0].Applied || budgeted.Indexes[0].BudgetExhaustedReason != TextIndexRewriteBudgetReasonMaxTerms {
		t.Fatalf("budgeted stats=%+v want max-terms no-op", budgeted)
	}
	afterBudget, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats after budget: %v", err)
	}
	if afterBudget.V2DeletedDocs == 0 || afterBudget.V2RewriteMergeState != TextIndexRewriteMergeStatePending {
		t.Fatalf("after budget stats=%+v want pending debt preserved", afterBudget)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := col.MaintainTextIndex(ctx, "lexical", TextIndexMaintenanceOptions{Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("MaintainTextIndex canceled err=%v want context.Canceled", err)
	}
}

func TestTextV2RewriteBudgetMaxDurationCheck2732(t *testing.T) {
	budget := newTextV2RewriteBudget(context.Background(), TextIndexRewriteOptions{MaxDuration: time.Hour})
	budget.started = time.Now().Add(-2 * time.Hour)
	if err := budget.check(); !errors.Is(err, errTextV2RewriteBudgetExhausted) || budget.reason != TextIndexRewriteBudgetReasonMaxDuration {
		t.Fatalf("budget err=%v reason=%q want max-duration exhaustion", err, budget.reason)
	}
}

func TestTextV2RewriteBudgetCapacityChecks2732(t *testing.T) {
	maxUint64 := ^uint64(0)

	blockBudget := newTextV2RewriteBudget(context.Background(), TextIndexRewriteOptions{MaxPostingBlocks: maxUint64})
	blockBudget.postingBlocks = maxUint64
	if err := blockBudget.reservePostingBlock(); !errors.Is(err, errTextV2RewriteBudgetExhausted) || blockBudget.reason != TextIndexRewriteBudgetReasonMaxPostingBlocks {
		t.Fatalf("block budget err=%v reason=%q want posting-block exhaustion", err, blockBudget.reason)
	}

	postingBudget := newTextV2RewriteBudget(context.Background(), TextIndexRewriteOptions{MaxPostings: maxUint64})
	postingBudget.postings = maxUint64 - 1
	if err := postingBudget.reservePostings(2); !errors.Is(err, errTextV2RewriteBudgetExhausted) || postingBudget.reason != TextIndexRewriteBudgetReasonMaxPostings {
		t.Fatalf("posting budget err=%v reason=%q want postings exhaustion", err, postingBudget.reason)
	}
}

func TestTextV2MaintenancePolicyReopenAfterMaintenance2732(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2TestDB(t, dir, false)
	col, refundLive := createTextV2MaintenancePolicyFixture2732(t, d, 40)
	stats, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1}})
	if err != nil || stats.IndexesRewritten != 1 {
		t.Fatalf("MaintainTextIndex stats=%+v err=%v want rewrite", stats, err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := openTextV2TestDB(t, dir, false)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	storage, err := reopenedCol.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("reopened TextIndexStorageStats: %v", err)
	}
	if storage.V2RewriteMergeState != TextIndexRewriteMergeStateCompacted || storage.V2DeletedDocs != 0 || storage.V2MicroPostingBlocks != 0 {
		t.Fatalf("reopened storage=%+v want compacted", storage)
	}
	got, err := reopenedCol.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 64, ResultMode: TextSearchResultModeScoreOnly, CandidateLimit: 128, MaxPostingsScanned: 512})
	if err != nil || len(got.Results) != refundLive || got.Stats.DocumentsFetched != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("reopened refund response=%+v err=%v want %d score-only zero-doc results", got, err, refundLive)
	}
}

func TestTextV2MaintenancePolicyConcurrentSearch2732(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col, refundLive := createTextV2MaintenancePolicyFixture2732(t, d, 96)

	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 16, ResultMode: TextSearchResultModeScoreOnly, CandidateLimit: 256, MaxPostingsScanned: 1024})
				if err != nil {
					errCh <- err
					return
				}
				if len(got.Results) == 0 || len(got.Results) > refundLive || got.Stats.DocumentsFetched != 0 || got.Stats.TextMatchDetailsBuilt != 0 || got.Stats.FailClosed != 0 {
					errCh <- fmt.Errorf("unexpected concurrent search response %+v", got)
					return
				}
			}
		}()
	}
	stats, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1}})
	if err != nil {
		t.Fatalf("MaintainTextIndex: %v", err)
	}
	if stats.IndexesRewritten != 1 {
		t.Fatalf("maintenance stats=%+v want rewrite", stats)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent search: %v", err)
		}
	}
}

func TestTextV2MaintenancePolicyMaxIndexesIgnoresTrailingV1Indexes2732(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col, _ := createTextV2MaintenancePolicyFixture2732(t, d, 24)
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "legacy", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex v1: %v", err)
	}

	stats, err := col.MaintainTextIndexes(context.Background(), TextIndexMaintenanceOptions{
		Policy:     TextIndexMaintenancePolicy{MinDeletedDocuments: 1_000},
		MaxIndexes: 1,
	})
	if err != nil {
		t.Fatalf("MaintainTextIndexes: %v", err)
	}
	if stats.BudgetExhausted || stats.BudgetExhaustedReason != "" || stats.IndexesScanned != 1 {
		t.Fatalf("stats=%+v want single v2 index processed without max-index exhaustion from trailing v1", stats)
	}
}

func TestTextV2MaintenancePolicyManagerMaxIndexesIgnoresCollectionsWithoutV22732(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	createTextV2MaintenancePolicyNamedFixture2732(t, d, "docs_v2", 24)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "legacy_only"}); err != nil {
		t.Fatalf("CreateCollection legacy_only: %v", err)
	}
	legacy, err := mgr.OpenCollection("legacy_only")
	if err != nil {
		t.Fatalf("OpenCollection legacy_only: %v", err)
	}
	if _, err := legacy.InsertBatch([][]byte{[]byte("doc-1")}, [][]byte{[]byte(`{"body":"legacy text"}`)}); err != nil {
		t.Fatalf("InsertBatch legacy_only: %v", err)
	}
	if _, _, err := legacy.CreateTextIndex(TextIndexDefinition{Name: "legacy", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex legacy_only: %v", err)
	}

	stats, err := mgr.MaintainTextIndexes(context.Background(), TextIndexMaintenanceOptions{
		Policy:     TextIndexMaintenancePolicy{MinDeletedDocuments: 1_000},
		MaxIndexes: 1,
	})
	if err != nil {
		t.Fatalf("manager MaintainTextIndexes: %v", err)
	}
	if stats.BudgetExhausted || stats.BudgetExhaustedReason != "" || stats.IndexesScanned != 1 {
		t.Fatalf("manager stats=%+v want no max-index exhaustion from trailing non-v2 collection", stats)
	}
}

func TestTextV2MaintenancePolicyManagerMaxIndexes2732(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	createTextV2MaintenancePolicyNamedFixture2732(t, d, "docs_a", 24)
	createTextV2MaintenancePolicyNamedFixture2732(t, d, "docs_b", 24)

	stats, err := NewCollectionManager(d).MaintainTextIndexes(context.Background(), TextIndexMaintenanceOptions{
		Policy:     TextIndexMaintenancePolicy{MinDeletedDocuments: 1},
		MaxIndexes: 1,
	})
	if err != nil {
		t.Fatalf("manager MaintainTextIndexes: %v", err)
	}
	if !stats.BudgetExhausted || stats.BudgetExhaustedReason != TextIndexMaintenanceSkipReasonMaxIndexes || stats.IndexesScanned != 1 || stats.IndexesSkipped == 0 {
		t.Fatalf("manager stats=%+v want max-index budget exhaustion after one index", stats)
	}
}

func TestTextV2MaintenancePolicyRejectsNegativeMaxIndexes2732(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col, _ := createTextV2MaintenancePolicyFixture2732(t, d, 24)

	opts := TextIndexMaintenanceOptions{MaxIndexes: -1}
	const want = "collections: text index maintenance MaxIndexes must be non-negative"
	if _, err := col.MaintainTextIndex(context.Background(), "lexical", opts); err == nil || err.Error() != want {
		t.Fatalf("MaintainTextIndex err=%v want %q", err, want)
	}
	if _, err := col.MaintainTextIndexes(context.Background(), opts); err == nil || err.Error() != want {
		t.Fatalf("MaintainTextIndexes err=%v want %q", err, want)
	}
	if _, err := NewCollectionManager(d).MaintainTextIndexes(context.Background(), opts); err == nil || err.Error() != want {
		t.Fatalf("manager MaintainTextIndexes err=%v want %q", err, want)
	}
}

func TestTextV2MaintenancePolicyStorageComposition2732(t *testing.T) {
	dir := t.TempDir()
	d, closeDB := openTextV2MaintenanceCompressedDB2732(t, dir)
	col, _ := createTextV2MaintenancePolicyFixture2732(t, d, 64)
	stats, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
		Policy:               TextIndexMaintenancePolicy{MinDeletedDocuments: 1},
		RunStorageCompaction: true,
		StorageCompactionOptions: backenddb.CompactStorageOptions{
			LeafPackMinExpectedReclaimBytes: 1,
			LeafPackMinReclaimPerCopyPPM:    1,
		},
	})
	if err != nil {
		t.Fatalf("MaintainTextIndex with storage compaction: %v", err)
	}
	if stats.IndexesRewritten != 1 || !stats.StorageCompacted || stats.StorageCompaction == nil {
		t.Fatalf("maintenance stats=%+v want rewrite plus CompactStorage composition", stats)
	}
	phaseNames := map[string]bool{}
	for _, phase := range stats.StorageCompaction.Storage.Phases {
		phaseNames[phase.Name] = true
	}
	for _, name := range []string{"value-log-rewrite", "value-log-gc", "leaf-generation-gc", "index-vacuum"} {
		if !phaseNames[name] {
			t.Fatalf("CompactStorage phases=%v missing %s", phaseNames, name)
		}
	}
	storage, err := col.TextIndexStorageStats("lexical")
	if err != nil || storage.V2RewriteMergeState != TextIndexRewriteMergeStateCompacted || storage.V2PostingBlocks == 0 || storage.V2NormBlocks == 0 || storage.V2DocMapBlocks == 0 {
		t.Fatalf("post-compaction storage=%+v err=%v want compacted reachable roots", storage, err)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func BenchmarkTextV2MaintenancePolicyRewriteMerge2732(b *testing.B) {
	for _, docsN := range []int{512, 10_000} {
		docsN := docsN
		b.Run(fmt.Sprintf("docs_%d", docsN), func(b *testing.B) {
			if docsN >= 10_000 && os.Getenv("TREEDB_TEXT_V2_MAINTENANCE_10K") != "1" {
				b.Skip("set TREEDB_TEXT_V2_MAINTENANCE_10K=1 for the 10k maintenance row")
			}
			b.ReportAllocs()
			b.ReportMetric(float64(docsN), "docs/op")
			var last TextIndexMaintenanceStats
			for i := 0; i < b.N; i++ {
				d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
				if err != nil {
					b.Fatalf("open db: %v", err)
				}
				col, _ := createTextV2MaintenancePolicyFixture2732(b, d, docsN)
				b.StartTimer()
				stats, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1}})
				b.StopTimer()
				if err != nil {
					b.Fatalf("MaintainTextIndex: %v", err)
				}
				last = stats
				_ = d.Close()
			}
			if len(last.Indexes) == 1 {
				idx := last.Indexes[0]
				b.ReportMetric(float64(idx.Debt.PostingBlocksRead), "posting_blocks_read/op")
				b.ReportMetric(float64(idx.Rewrite.PostingBlocksWritten), "posting_blocks_written/op")
				b.ReportMetric(float64(idx.Rewrite.PostingBlocksDeleted), "posting_blocks_deleted/op")
				b.ReportMetric(float64(idx.Debt.StalePostings), "stale_postings/op")
			}
		})
	}
}

func BenchmarkTextV2MaintenancePolicyWriteChurn2732(b *testing.B) {
	docsN := textV2MaintenanceBenchmarkDocs2732(512)
	b.ReportAllocs()
	b.ReportMetric(float64(docsN), "docs/op")
	var last TextIndexMaintenanceStats
	for i := 0; i < b.N; i++ {
		d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
		if err != nil {
			b.Fatalf("open db: %v", err)
		}
		col, _ := createTextV2MaintenancePolicyFixture2732(b, d, docsN)
		ids := make([][]byte, 16)
		for j := range ids {
			ids[j] = []byte(fmt.Sprintf("doc-%03d", docsN/2+j))
		}
		b.StartTimer()
		for _, id := range ids {
			id := append([]byte(nil), id...)
			if _, _, err := col.Update(id, func([]byte) ([]byte, bool, error) {
				return []byte(`{"body":"churn common policy refund"}`), true, nil
			}); err != nil {
				b.Fatalf("Update: %v", err)
			}
		}
		stats, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1, MinMicroPostingBlocks: 1}})
		b.StopTimer()
		if err != nil {
			b.Fatalf("MaintainTextIndex: %v", err)
		}
		last = stats
		_ = d.Close()
	}
	if len(last.Indexes) == 1 {
		b.ReportMetric(float64(last.Indexes[0].Debt.StalePostings), "stale_postings/op")
	}
}

func BenchmarkTextV2MaintenancePolicyStorageReclaim2732(b *testing.B) {
	docsN := textV2MaintenanceBenchmarkDocs2732(512)
	b.ReportAllocs()
	b.ReportMetric(float64(docsN), "docs/op")
	var last TextIndexMaintenanceStats
	for i := 0; i < b.N; i++ {
		d, closeDB := openTextV2MaintenanceCompressedDB2732(b, b.TempDir())
		col, _ := createTextV2MaintenancePolicyFixture2732(b, d, docsN)
		b.StartTimer()
		stats, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{
			Policy:               TextIndexMaintenancePolicy{MinDeletedDocuments: 1},
			RunStorageCompaction: true,
			StorageCompactionOptions: backenddb.CompactStorageOptions{
				LeafPackMinExpectedReclaimBytes: 1,
				LeafPackMinReclaimPerCopyPPM:    1,
			},
		})
		b.StopTimer()
		if err != nil {
			b.Fatalf("MaintainTextIndex: %v", err)
		}
		last = stats
		_ = closeDB()
	}
	if len(last.Indexes) == 1 {
		idx := last.Indexes[0]
		if idx.StorageBefore.EncodedBytes >= idx.StorageAfter.EncodedBytes {
			b.ReportMetric(float64(idx.StorageBefore.EncodedBytes-idx.StorageAfter.EncodedBytes), "text_encoded_bytes_reclaimed/op")
		}
		if idx.StorageBefore.V2PostingBlocks >= idx.StorageAfter.V2PostingBlocks {
			b.ReportMetric(float64(idx.StorageBefore.V2PostingBlocks-idx.StorageAfter.V2PostingBlocks), "posting_blocks_reclaimed/op")
		}
	}
	if last.StorageCompaction != nil {
		b.ReportMetric(float64(last.StorageCompaction.Storage.ValueLogGC.BytesDeleted), "value_log_gc_bytes_deleted/op")
		b.ReportMetric(float64(last.StorageCompaction.Storage.LeafGenerationGC.BytesDeleted), "leaf_gc_bytes_deleted/op")
	}
}

func BenchmarkTextV2MaintenancePolicySearchLatency2732(b *testing.B) {
	docsN := textV2MaintenanceBenchmarkDocs2732(512)
	b.Run("before", func(b *testing.B) {
		d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
		if err != nil {
			b.Fatalf("open db: %v", err)
		}
		defer func() { _ = d.Close() }()
		col, _ := createTextV2MaintenancePolicyFixture2732(b, d, docsN)
		benchTextV2MaintenanceSearch2732(b, col, docsN)
	})
	b.Run("during", func(b *testing.B) {
		d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
		if err != nil {
			b.Fatalf("open db: %v", err)
		}
		defer func() { _ = d.Close() }()
		col, _ := createTextV2MaintenancePolicyFixture2732(b, d, docsN)
		done := make(chan struct{})
		go func() {
			_, _ = col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1}})
			close(done)
		}()
		benchTextV2MaintenanceSearch2732(b, col, docsN)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			b.Fatal("maintenance did not finish")
		}
	})
	b.Run("after", func(b *testing.B) {
		d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
		if err != nil {
			b.Fatalf("open db: %v", err)
		}
		defer func() { _ = d.Close() }()
		col, _ := createTextV2MaintenancePolicyFixture2732(b, d, docsN)
		if _, err := col.MaintainTextIndex(context.Background(), "lexical", TextIndexMaintenanceOptions{Policy: TextIndexMaintenancePolicy{MinDeletedDocuments: 1}}); err != nil {
			b.Fatalf("MaintainTextIndex: %v", err)
		}
		benchTextV2MaintenanceSearch2732(b, col, docsN)
	})
}

func benchTextV2MaintenanceSearch2732(b *testing.B, col *Collection, docsN int) {
	b.Helper()
	b.ReportAllocs()
	opts := TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 16, ResultMode: TextSearchResultModeScoreOnly, CandidateLimit: docsN, MaxPostingsScanned: docsN * 4}
	warm, err := col.SearchText(opts)
	if err != nil || warm.Stats.DocumentsFetched != 0 || warm.Stats.TextMatchDetailsBuilt != 0 || warm.Stats.FailClosed != 0 {
		b.Fatalf("warm search=%+v err=%v", warm, err)
	}
	b.ResetTimer()
	var last TextSearchResponse
	for i := 0; i < b.N; i++ {
		got, err := col.SearchText(opts)
		if err != nil {
			b.Fatalf("SearchText: %v", err)
		}
		last = got
	}
	b.StopTimer()
	b.ReportMetric(float64(last.Stats.TextPostingBlocksVisited), "posting_blocks_visited/search")
	b.ReportMetric(float64(last.Stats.TextPostingBlocksSkipped), "posting_blocks_skipped/search")
	b.ReportMetric(float64(last.Stats.DocumentsFetched), "docs_fetched/search")
	b.ReportMetric(float64(last.Stats.TextMatchDetailsBuilt), "match_details/search")
}

func textV2MaintenanceBenchmarkDocs2732(fallback int) int {
	if raw := os.Getenv("TREEDB_TEXT_V2_MAINTENANCE_DOCS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func openTextV2MaintenanceCompressedDB2732(t testing.TB, dir string) (*backenddb.DB, func() error) {
	t.Helper()
	opts := treedb.Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	}
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open compressed db: %v", err)
	}
	return d, cleanup
}

func createTextV2MaintenancePolicyFixture2732(t testing.TB, d *backenddb.DB, docsN int) (*Collection, int) {
	t.Helper()
	return createTextV2MaintenancePolicyNamedFixture2732(t, d, "docs", docsN)
}

func createTextV2MaintenancePolicyNamedFixture2732(t testing.TB, d *backenddb.DB, name string, docsN int) (*Collection, int) {
	t.Helper()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: name}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection(name)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids := make([][]byte, docsN)
	docs := make([][]byte, docsN)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
		docs[i] = []byte(fmt.Sprintf(`{"body":"refund common policy token%d bucket%d"}`, i%17, i%5))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	updates := docsN / 4
	deletes := docsN / 8
	for i := 0; i < updates; i++ {
		id := append([]byte(nil), ids[i]...)
		if _, _, err := col.Update(id, func([]byte) ([]byte, bool, error) {
			return []byte(`{"body":"updated common policy replacement"}`), true, nil
		}); err != nil {
			t.Fatalf("Update %s: %v", id, err)
		}
	}
	deleteIDs := make([][]byte, deletes)
	for i := 0; i < deletes; i++ {
		deleteIDs[i] = ids[updates+i]
	}
	if deleted, err := col.DeleteBatch(deleteIDs); err != nil || deleted != deletes {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	return col, docsN - updates - deletes
}
