package collections

import (
	"fmt"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTextHybridScaleLargeFixtureSmoke2731(t *testing.T) {
	fixture := openHybridCloseoutFixture2506(t, 512, 8, 4)
	defer func() { _ = fixture.db.Close() }()

	storage, err := fixture.col.TextIndexStorageStats(hybridCloseoutTextIndexName2506)
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if storage.Version != TextIndexVersionV2 || storage.V2LiveDocuments != 512 || storage.V2PostingBlocks == 0 {
		t.Fatalf("storage=%+v want v2 live docs/posting blocks", storage)
	}

	text, err := fixture.col.SearchText(TextSearchOptions{IndexName: hybridCloseoutTextIndexName2506, Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly, CandidateLimit: 512, MaxPostingsScanned: 2048})
	if err != nil {
		t.Fatalf("SearchText common: %v", err)
	}
	assertTextScaleZeroDocStats2731(t, "text common", text.Stats)
	if len(text.Results) == 0 {
		t.Fatal("text common returned no results")
	}

	hybrid, err := fixture.col.SearchHybrid(HybridSearchOptions{
		TopK:       10,
		ResultMode: HybridResultModeScoreOnly,
		Text:       &HybridTextQuery{IndexName: hybridCloseoutTextIndexName2506, Query: "refund policy", CandidateLimit: 64},
		Vector:     &HybridVectorQuery{IndexName: fixture.def.Name, Query: hybridCloseoutQueryVector2506(8), CandidateLimit: 64, EfSearch: 64, QueryMode: VectorIndexQueryModeExact},
		ScalarFilter: &HybridScalarFilter{
			IndexName: hybridCloseoutTenantIndexName2506,
			Value:     "tenant-rare-06pct",
		},
	})
	if err != nil {
		t.Fatalf("SearchHybrid: %v", err)
	}
	assertHybridScaleZeroDocStats2731(t, "hybrid", hybrid.Stats)
	if len(hybrid.Results) == 0 {
		t.Fatal("hybrid returned no results")
	}
}

func TestTextHybridScaleReopenDurability2731(t *testing.T) {
	dir, d, _ := openTextHybridScaleTextOnlyFixture2731(t, 256)
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	stats, err := reopenedCol.TextIndexStorageStats(textV2ContractIndexName2623)
	if err != nil {
		t.Fatalf("TextIndexStorageStats reopened: %v", err)
	}
	if stats.Version != TextIndexVersionV2 || stats.V2LiveDocuments != 256 {
		t.Fatalf("reopened stats=%+v want v2 256 live docs", stats)
	}
	got, err := reopenedCol.SearchText(TextSearchOptions{IndexName: textV2ContractIndexName2623, Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly, CandidateLimit: 256, MaxPostingsScanned: 1024})
	if err != nil {
		t.Fatalf("SearchText reopened: %v", err)
	}
	assertTextScaleZeroDocStats2731(t, "reopened text", got.Stats)
	if len(got.Results) == 0 {
		t.Fatal("reopened text returned no results")
	}
}

func TestTextHybridScaleConcurrentSearchWriteSanity2731(t *testing.T) {
	_, d, col := openTextHybridScaleTextOnlyFixture2731(t, 256)
	defer func() { _ = d.Close() }()

	query := TextSearchOptions{IndexName: textV2ContractIndexName2623, Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly, CandidateLimit: 512, MaxPostingsScanned: 2048}
	var wg sync.WaitGroup
	errCh := make(chan error, 5)
	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				got, err := col.SearchText(query)
				if err != nil {
					errCh <- err
					return
				}
				if len(got.Results) == 0 {
					errCh <- fmt.Errorf("no search results")
					return
				}
				if guard := textScaleGuard2731(got.Stats); guard != "" {
					errCh <- fmt.Errorf("bad stats: %s", guard)
					return
				}
			}
		}()
	}
	ids, docs := textV2ContractDocuments2623(32, "scw")
	if _, err := col.InsertBatch(ids, docs); err != nil {
		errCh <- err
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent search/write: %v", err)
		}
	}
}

func TestTextHybridScaleMaintenanceRewritePostconditions2731(t *testing.T) {
	_, d, col := openTextHybridScaleTextOnlyFixture2731(t, 256)
	defer func() { _ = d.Close() }()

	for i := 0; i < 16; i++ {
		id := []byte(fmt.Sprintf("doc-scale-%06d", i))
		updated, changed, err := col.Update(id, func([]byte) ([]byte, bool, error) {
			return []byte(fmt.Sprintf(`{"title":"updated refund policy %d","body":"updated refund policy maintenance"}`, i)), true, nil
		})
		if err != nil {
			t.Fatalf("Update %s: %v", id, err)
		}
		if !updated || !changed {
			t.Fatalf("Update %s updated=%v changed=%v", id, updated, changed)
		}
	}
	deleteIDs := make([][]byte, 8)
	for i := range deleteIDs {
		deleteIDs[i] = []byte(fmt.Sprintf("doc-scale-%06d", 16+i))
	}
	if deleted, err := col.DeleteBatch(deleteIDs); err != nil || deleted != len(deleteIDs) {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	stats, err := col.RewriteTextIndex(textV2ContractIndexName2623, TextIndexRewriteOptions{})
	if err != nil {
		t.Fatalf("RewriteTextIndex: %v", err)
	}
	if stats.PostingBlocksRead == 0 || stats.PostingBlocksDeleted == 0 || stats.StalePostingsPurged == 0 {
		t.Fatalf("rewrite stats=%+v want stale posting purge work", stats)
	}
	after, err := col.TextIndexStorageStats(textV2ContractIndexName2623)
	if err != nil {
		t.Fatalf("TextIndexStorageStats after rewrite: %v", err)
	}
	if after.V2DeletedDocs != 0 || after.V2LiveDocuments != 248 {
		t.Fatalf("after stats=%+v want tombstones purged and 248 live docs", after)
	}
	got, err := col.SearchText(TextSearchOptions{IndexName: textV2ContractIndexName2623, Query: "refund", TopK: 10, ResultMode: TextSearchResultModeScoreOnly, CandidateLimit: 256, MaxPostingsScanned: 1024})
	if err != nil {
		t.Fatalf("post-rewrite SearchText: %v", err)
	}
	assertTextScaleZeroDocStats2731(t, "post rewrite", got.Stats)
}

func openTextHybridScaleTextOnlyFixture2731(t *testing.T, docs int) (string, *backenddb.DB, *Collection) {
	t.Helper()
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	meta := CollectionMeta{
		Name:    "docs",
		Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	ids, rawDocs := textV2ContractDocuments2623(docs, "scale")
	if _, err := col.InsertBatch(ids, rawDocs); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		t.Fatalf("Flush: %v", err)
	}
	if _, _, err := col.CreateTextIndex(textV2ContractV2IndexDefinition2626()); err != nil {
		_ = d.Close()
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		t.Fatalf("Flush text index: %v", err)
	}
	return dir, d, col
}

func assertTextScaleZeroDocStats2731(t *testing.T, label string, stats TextSearchStats) {
	t.Helper()
	if guard := textScaleGuard2731(stats); guard != "" {
		t.Fatalf("%s stats=%+v guard=%s", label, stats, guard)
	}
}

func textScaleGuard2731(stats TextSearchStats) string {
	return scaleZeroDocGuard2731(stats.DocumentsFetched, stats.FullDocumentScanFallbacks, stats.FailClosed, stats.TextStateLookups, stats.TextMatchDetailsBuilt)
}

func hybridScaleGuard2731(stats HybridSearchStats) string {
	return scaleZeroDocGuard2731(stats.DocumentsFetched, stats.FullDocumentScanFallbacks, stats.FailClosed, stats.TextStateLookups, stats.TextMatchDetailsBuilt)
}

func scaleZeroDocGuard2731(docs, fallbacks, fail, state, details uint64) string {
	if docs != 0 || fallbacks != 0 || fail != 0 || state != 0 || details != 0 {
		return fmt.Sprintf("docs=%d fallbacks=%d fail=%d state=%d details=%d", docs, fallbacks, fail, state, details)
	}
	return ""
}

func assertHybridScaleZeroDocStats2731(t *testing.T, label string, stats HybridSearchStats) {
	t.Helper()
	if guard := hybridScaleGuard2731(stats); guard != "" {
		t.Fatalf("%s stats=%+v guard=%s", label, stats, guard)
	}
}
