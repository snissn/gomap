package collections

import (
	"fmt"
	"sort"
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

	opts := HybridSearchOptions{
		TopK:       10,
		ResultMode: HybridResultModeScoreOnly,
		Text:       &HybridTextQuery{IndexName: hybridCloseoutTextIndexName2506, Query: "refund policy", CandidateLimit: 64},
		Vector:     &HybridVectorQuery{IndexName: fixture.def.Name, Query: hybridCloseoutQueryVector2506(8), CandidateLimit: 64, EfSearch: 64, QueryMode: VectorIndexQueryModeExact},
	}
	hybrid, err := fixture.col.SearchHybrid(opts)
	if err != nil {
		t.Fatalf("SearchHybrid: %v", err)
	}
	assertHybridScaleZeroDocStats2731(t, "hybrid", hybrid.Stats)
	if len(hybrid.Results) != opts.TopK || hybrid.Stats.TextCandidatesReturned != 64 || hybrid.Stats.VectorCandidatesReturned != 64 || hybrid.Stats.ScalarFilterLookups != 0 {
		t.Fatalf("hybrid results=%d stats=%+v want bounded unfiltered text/vector fusion", len(hybrid.Results), hybrid.Stats)
	}
	assertHybridScaleRRF2731(t, fixture.col, opts, hybrid.Results)

	// The same indexed fixture must refuse the retired ordinary filtered-vector
	// route before it executes either source or reads scalar postings.
	opts.ScalarFilter = &HybridScalarFilter{IndexName: hybridCloseoutTenantIndexName2506, Value: "tenant-rare-06pct"}
	unsupported, err := fixture.col.SearchHybrid(opts)
	requireFilteredVectorHybridUnsupported5275(t, unsupported, err)
}

func assertHybridScaleRRF2731(t *testing.T, col *Collection, opts HybridSearchOptions, got []HybridSearchResult) {
	t.Helper()
	text, err := col.SearchHybridTextCandidates(*opts.Text)
	if err != nil {
		t.Fatalf("independent text candidates: %v", err)
	}
	vector, err := col.SearchHybridVectorCandidates(*opts.Vector)
	if err != nil {
		t.Fatalf("independent vector candidates: %v", err)
	}
	if len(text.Candidates) != opts.Text.CandidateLimit || len(vector.Candidates) != opts.Vector.CandidateLimit {
		t.Fatalf("independent candidates: text=%d vector=%d want %d/%d", len(text.Candidates), len(vector.Candidates), opts.Text.CandidateLimit, opts.Vector.CandidateLimit)
	}
	// Build the default RRF oracle from independently executed source rankings,
	// without calling the executor's fusion or ordering helpers.
	type expectedResult struct {
		id         string
		score      float64
		bestRank   int
		bestSource int
		sources    int
	}
	byID := make(map[string]*expectedResult)
	for source, candidates := range [][]HybridSearchCandidate{text.Candidates, vector.Candidates} {
		for i, candidate := range candidates {
			id, rank := string(candidate.ID), i+1
			want := byID[id]
			if want == nil {
				want = &expectedResult{id: id, bestRank: rank, bestSource: source}
				byID[id] = want
			}
			want.score += 1 / float64(60+rank)
			want.sources++
			if rank < want.bestRank {
				want.bestRank, want.bestSource = rank, source
			}
		}
	}
	want := make([]*expectedResult, 0, len(byID))
	for _, result := range byID {
		want = append(want, result)
	}
	sort.Slice(want, func(i, j int) bool {
		a, b := want[i], want[j]
		if a.score != b.score {
			return a.score > b.score
		}
		if a.bestRank != b.bestRank {
			return a.bestRank < b.bestRank
		}
		if a.sources != b.sources {
			return a.sources > b.sources
		}
		if a.bestSource != b.bestSource {
			return a.bestSource < b.bestSource
		}
		return a.id < b.id
	})
	for i, result := range got {
		if string(result.ID) != want[i].id || result.FusedScore != want[i].score || result.Rank != i+1 {
			t.Fatalf("hybrid rank %d: got=%+v want=%+v", i+1, result, *want[i])
		}
		if result.DocumentFound || len(result.Document) != 0 || len(result.Sources) != 0 {
			t.Fatalf("score-only hybrid result carried document or attribution: %+v", result)
		}
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
