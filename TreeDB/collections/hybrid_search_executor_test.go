package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type hybridSearchExecutorFixtureRow2505 struct {
	id     string
	title  string
	body   string
	city   string
	score  int64
	vector []float32
}

func TestSearchHybridExecutorTextVectorOverlapAndBoundedFetch2505(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-shared", title: "refund", body: "refund refund", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-text", title: "refund", body: "refund customer policy", city: "sea", score: 20, vector: []float32{0.2, 0.8, 0}},
		{id: "doc-vector", title: "shipping", body: "shipping update", city: "sea", score: 30, vector: []float32{0.99, 0.01, 0}},
		{id: "doc-filtered", title: "refund", body: "refund", city: "sfo", score: 40, vector: []float32{0.4, 0.6, 0}},
		{id: "doc-background", title: "other", body: "other", city: "sea", score: 50, vector: []float32{0, 0, 1}},
	})
	defer func() { _ = d.Close() }()

	opts := HybridSearchOptions{
		TopK:             3,
		Text:             &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		Vector:           &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 5, QueryMode: VectorIndexQueryModeExact},
		ScalarFilter:     &HybridScalarFilter{IndexName: "city", Value: "sea"},
		IncludeDocuments: true,
		DocumentFetchOptions: DocumentFetchOptions{
			ExcludePaths: []string{"embedding"},
		},
		Debug: HybridSearchDebugOptions{IncludeCandidates: true},
	}
	got, err := col.SearchHybrid(opts)
	if err != nil {
		t.Fatalf("SearchHybrid: %v", err)
	}
	if got.Plan.FinalTopK != opts.TopK || got.Plan.TextCandidateLimit != 4 || got.Plan.VectorCandidateLimit != 4 || got.Plan.ScalarFilterStrategy != HybridScalarFilterStrategyPrefilter || got.Plan.ScalarFilterLookupCount != 1 || got.Plan.ScalarFilterLookupLimit != hybridScalarDefaultLookupLimit || got.Plan.ScalarFilterAggregateLimit != hybridScalarDefaultLookupLimit || got.Plan.FusionMethod != HybridFusionMethodRRF {
		t.Fatalf("plan=%+v want compatible one-lookup bounded prefilter RRF plan", got.Plan)
	}
	if got.Snapshot.Consistency != HybridConsistencyCurrentSnapshot || got.Snapshot.CommitSeq == 0 || got.Snapshot.SystemRootPageID == 0 {
		t.Fatalf("snapshot=%+v want current snapshot identity", got.Snapshot)
	}
	if len(got.Results) != 3 {
		t.Fatalf("results=%d want 3: %+v", len(got.Results), got.Results)
	}
	if gotID := string(got.Results[0].ID); gotID != "doc-shared" {
		t.Fatalf("top result=%q want overlapping doc-shared; results=%+v", gotID, got.Results)
	}
	if !hybridResultHasSource2505(got.Results[0], HybridCandidateSourceText) || !hybridResultHasSource2505(got.Results[0], HybridCandidateSourceVector) {
		t.Fatalf("top sources=%+v want text+vector overlap attribution", got.Results[0].Sources)
	}
	for _, result := range got.Results {
		if string(result.ID) == "doc-filtered" {
			t.Fatalf("scalar-filtered document reached results: %+v", got.Results)
		}
		if !result.DocumentFound || len(result.Document) == 0 || bytes.Contains(result.Document, []byte("embedding")) || !bytes.Contains(result.Document, []byte(`"city":"sea"`)) {
			t.Fatalf("result=%+v want bounded fetched sea document respecting projection", result)
		}
	}
	if len(got.Candidates) == 0 {
		t.Fatalf("debug candidates missing")
	}
	for _, candidate := range got.Candidates {
		if string(candidate.ID) == "doc-filtered" {
			t.Fatalf("prefiltered debug candidate escaped scalar allow-set: %+v", got.Candidates)
		}
	}
	if got.Stats.TextCandidatesReturned == 0 || got.Stats.VectorCandidatesReturned == 0 || got.Stats.CandidatesFused != uint64(len(got.Candidates)) || got.Stats.FusionBoth == 0 {
		t.Fatalf("stats=%+v want text/vector/fusion counters", got.Stats)
	}
	if got.Stats.ScalarPrefilterIDs != 4 || got.Stats.ScalarFilterRejected == 0 || got.Stats.DocumentsFetched != uint64(len(got.Results)) || got.Stats.DocumentsFetched > uint64(opts.TopK) || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 {
		t.Fatalf("stats=%+v want scalar filtering and bounded final fetch only", got.Stats)
	}

	noDocsOpts := opts
	noDocsOpts.IncludeDocuments = false
	noDocsOpts.DocumentFetchOptions = DocumentFetchOptions{}
	noDocsOpts.Debug.IncludeCandidates = false
	noDocs, err := col.SearchHybrid(noDocsOpts)
	if err != nil {
		t.Fatalf("SearchHybrid IncludeDocuments=false: %v", err)
	}
	if len(noDocs.Candidates) != 0 || noDocs.Stats.DocumentsFetched != 0 || noDocs.Stats.DocumentsMissing != 0 {
		t.Fatalf("no-doc response candidates=%d stats=%+v want no debug echo and zero document fetch", len(noDocs.Candidates), noDocs.Stats)
	}
	if gotIDs, noDocIDs := hybridResultIDs2505(got.Results), hybridResultIDs2505(noDocs.Results); !slicesEqualStrings(gotIDs, noDocIDs) {
		t.Fatalf("deterministic topK changed with IncludeDocuments=false: docs=%v no_docs=%v", gotIDs, noDocIDs)
	}
	for _, result := range noDocs.Results {
		if result.DocumentFound || len(result.Document) != 0 {
			t.Fatalf("IncludeDocuments=false result=%+v carried document", result)
		}
	}
}

func TestHybridSearchCandidatePreallocHintBoundsLimits2876(t *testing.T) {
	plan := hybridSearchExecutionPlan{
		text:   &HybridTextQuery{CandidateLimit: 64},
		vector: &HybridVectorQuery{CandidateLimit: 64},
	}
	if got := hybridSearchCandidatePreallocHint(plan); got != 128 {
		t.Fatalf("cap hint=%d want exact small combined limit", got)
	}

	plan.text.CandidateLimit = maxCollectionInt
	plan.vector.CandidateLimit = maxCollectionInt
	if got := hybridSearchCandidatePreallocHint(plan); got != hybridCandidatePreallocLimit {
		t.Fatalf("cap hint=%d want bounded limit %d", got, hybridCandidatePreallocLimit)
	}

	plan.vector = nil
	if got := hybridSearchCandidatePreallocHint(plan); got != 0 {
		t.Fatalf("single-source cap hint=%d want 0", got)
	}
}

func TestSearchHybridExecutorTextOnlyAndVectorOnly2505(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-a", title: "refund", body: "refund refund", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-b", title: "refund", body: "refund", city: "sea", score: 20, vector: []float32{0, 1, 0}},
		{id: "doc-c", title: "other", body: "other", city: "sea", score: 30, vector: []float32{0, 0, 1}},
	})
	defer func() { _ = d.Close() }()

	textOnly, err := col.SearchHybrid(HybridSearchOptions{TopK: 2, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2}})
	if err != nil {
		t.Fatalf("SearchHybrid text-only: %v", err)
	}
	if len(textOnly.Results) != 2 || textOnly.Plan.ScalarFilterStrategy != HybridScalarFilterStrategyTextFirst || textOnly.Stats.VectorCandidatesReturned != 0 || textOnly.Stats.FusionVectorOnly != 0 {
		t.Fatalf("text-only response=%+v stats=%+v", textOnly, textOnly.Stats)
	}
	for _, result := range textOnly.Results {
		if len(result.Sources) != 1 || result.Sources[0].Source != HybridCandidateSourceText {
			t.Fatalf("text-only result=%+v want text source only", result)
		}
	}

	vectorOnly, err := col.SearchHybrid(HybridSearchOptions{TopK: 2, Vector: &HybridVectorQuery{IndexName: def.Name, Query: []float32{0, 1, 0}, CandidateLimit: 2, EfSearch: 3}})
	if err != nil {
		t.Fatalf("SearchHybrid vector-only: %v", err)
	}
	if len(vectorOnly.Results) != 2 || vectorOnly.Plan.ScalarFilterStrategy != HybridScalarFilterStrategyVectorFirst || vectorOnly.Stats.TextCandidatesReturned != 0 || vectorOnly.Stats.FusionTextOnly != 0 {
		t.Fatalf("vector-only response=%+v stats=%+v", vectorOnly, vectorOnly.Stats)
	}
	for _, result := range vectorOnly.Results {
		if len(result.Sources) != 1 || result.Sources[0].Source != HybridCandidateSourceVector {
			t.Fatalf("vector-only result=%+v want vector source only", result)
		}
	}
}

func TestSearchHybridScalarPrefilterPushesIntoVectorCandidates2729(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-unallowed-best", title: "shipping", body: "shipping", city: "sfo", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-allowed-best", title: "shipping", body: "shipping", city: "sea", score: 20, vector: []float32{0.8, 0.2, 0}},
		{id: "doc-unallowed-worse", title: "shipping", body: "shipping", city: "sfo", score: 30, vector: []float32{0.2, 0.8, 0}},
	})
	defer func() { _ = d.Close() }()

	got, err := col.SearchHybrid(HybridSearchOptions{
		TopK:         1,
		Vector:       &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 1, EfSearch: 1, QueryMode: VectorIndexQueryModeExact},
		ScalarFilter: &HybridScalarFilter{IndexName: "city", Value: "sea"},
	})
	if err != nil {
		t.Fatalf("SearchHybrid vector scalar prefilter: %v", err)
	}
	if gotIDs := hybridResultIDs2505(got.Results); !slicesEqualStrings(gotIDs, []string{"doc-allowed-best"}) {
		t.Fatalf("ids=%v response=%+v want best allowed vector result despite unallowed global top-1", gotIDs, got)
	}
	if got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.VectorCandidatesReturned != 1 || got.Stats.VectorCandidatesExamined != 1 || got.Stats.Truncated != 0 {
		t.Fatalf("stats=%+v want no-doc vector allow-set candidate generation", got.Stats)
	}
	if got.Stats.ScalarPrefilterIDs != 1 || got.Stats.ScalarPostfilterChecks != 1 || got.Stats.ScalarFilterMatched != 1 || got.Stats.ScalarFilterRejected != 2 {
		t.Fatalf("stats=%+v want exact vector allow-set matched/rejected counters", got.Stats)
	}
}

func TestSearchHybridResultModes2505(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-a", title: "refund", body: "refund", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-b", title: "refund", body: "policy", city: "sea", score: 20, vector: []float32{0.9, 0.1, 0}},
	})
	defer func() { _ = d.Close() }()

	base := HybridSearchOptions{
		TopK:   1,
		Text:   &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2},
		Vector: &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 2, EfSearch: 2, QueryMode: VectorIndexQueryModeExact},
	}
	scoreOnly := base
	scoreOnly.ResultMode = HybridResultModeScoreOnly
	got, err := col.SearchHybrid(scoreOnly)
	if err != nil {
		t.Fatalf("SearchHybrid score-only: %v", err)
	}
	if got.Plan.ResultMode != HybridResultModeScoreOnly || len(got.Results) != 1 || len(got.Results[0].Sources) != 0 || got.Stats.DocumentsFetched != 0 {
		t.Fatalf("score-only response=%+v stats=%+v want no sources and no docs", got, got.Stats)
	}

	compact := base
	compact.ResultMode = HybridResultModeCompact
	got, err = col.SearchHybrid(compact)
	if err != nil {
		t.Fatalf("SearchHybrid compact: %v", err)
	}
	if got.Plan.ResultMode != HybridResultModeCompact || len(got.Results) != 1 || len(got.Results[0].Sources) == 0 || len(got.Results[0].Document) != 0 || got.Stats.DocumentsFetched != 0 {
		t.Fatalf("compact response=%+v stats=%+v want sources and no docs", got, got.Stats)
	}

	full := base
	full.ResultMode = HybridResultModeFull
	full.DocumentFetchOptions = DocumentFetchOptions{ExcludePaths: []string{"embedding"}}
	got, err = col.SearchHybrid(full)
	if err != nil {
		t.Fatalf("SearchHybrid full: %v", err)
	}
	if got.Plan.ResultMode != HybridResultModeFull || len(got.Results) != 1 || !got.Results[0].DocumentFound || got.Stats.DocumentsFetched != 1 {
		t.Fatalf("full response=%+v stats=%+v want bounded final doc", got, got.Stats)
	}

	conflict := base
	conflict.ResultMode = HybridResultModeScoreOnly
	conflict.IncludeDocuments = true
	failed, err := col.SearchHybrid(conflict)
	if !errors.Is(err, ErrHybridSearchUnsupported) || failed.Stats.FailClosed == 0 {
		t.Fatalf("conflicting mode response=%+v err=%v want fail-closed unsupported", failed, err)
	}

	projectionWithoutFull := base
	projectionWithoutFull.ResultMode = HybridResultModeCompact
	projectionWithoutFull.DocumentFetchOptions = DocumentFetchOptions{ExcludePaths: []string{"embedding"}}
	failed, err = col.SearchHybrid(projectionWithoutFull)
	if !errors.Is(err, ErrHybridSearchUnsupported) || failed.Stats.FailClosed == 0 {
		t.Fatalf("compact projection response=%+v err=%v want fail-closed unsupported", failed, err)
	}

	implicitCompactProjection := base
	implicitCompactProjection.DocumentFetchOptions = DocumentFetchOptions{ExcludePaths: []string{"embedding"}}
	failed, err = col.SearchHybrid(implicitCompactProjection)
	if !errors.Is(err, ErrHybridSearchUnsupported) || failed.Stats.FailClosed == 0 {
		t.Fatalf("implicit compact projection response=%+v err=%v want fail-closed unsupported", failed, err)
	}
}

func TestSearchHybridScalarFilterRangeAndFailClosed2505(t *testing.T) {
	_, d, col := openHybridScalarSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-10", title: "refund", body: "refund", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-20", title: "refund", body: "refund", city: "sea", score: 20, vector: []float32{0.9, 0.1, 0}},
		{id: "doc-30", title: "refund", body: "refund", city: "sea", score: 30, vector: []float32{0.8, 0.2, 0}},
		{id: "doc-40", title: "refund", body: "refund", city: "sea", score: 40, vector: []float32{0.7, 0.3, 0}},
	})
	defer func() { _ = d.Close() }()

	ranged, err := col.SearchHybrid(HybridSearchOptions{
		TopK: 2,
		Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		ScalarFilter: &HybridScalarFilter{IndexName: "score", Range: &IndexRangeOptions{
			Lower: IndexRangeBound{Value: int64(10), Inclusive: true},
			Upper: IndexRangeBound{Value: int64(20), Inclusive: true},
		}},
	})
	if err != nil {
		t.Fatalf("SearchHybrid range scalar: %v", err)
	}
	if gotIDs := hybridResultIDs2505(ranged.Results); !slicesEqualStrings(gotIDs, []string{"doc-10", "doc-20"}) {
		t.Fatalf("range result ids=%v want doc-10/doc-20 response=%+v", gotIDs, ranged)
	}
	if ranged.Stats.ScalarPrefilterIDs != 2 || ranged.Stats.ScalarFilterLookups != 1 || ranged.Stats.ScalarFilterInputIDs != 2 || ranged.Stats.ScalarFilterIntersectionSteps != 0 || ranged.Stats.ScalarFilterFinalIDs != 2 || ranged.Stats.ScalarFilterMatched != 2 || ranged.Stats.ScalarFilterRejected != 2 || ranged.Stats.FailClosed != 0 {
		t.Fatalf("range stats=%+v want compatible one-lookup bounded scalar include/exclude", ranged.Stats)
	}

	postfiltered, err := col.SearchHybrid(HybridSearchOptions{
		TopK:                 2,
		Text:                 &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		ScalarFilterStrategy: HybridScalarFilterStrategyPostfilter,
		ScalarFilter: &HybridScalarFilter{IndexName: "score", Range: &IndexRangeOptions{
			Lower: IndexRangeBound{Value: int64(10), Inclusive: true},
			Upper: IndexRangeBound{Value: int64(20), Inclusive: true},
		}},
	})
	if err != nil {
		t.Fatalf("SearchHybrid postfilter scalar: %v", err)
	}
	if gotIDs := hybridResultIDs2505(postfiltered.Results); !slicesEqualStrings(gotIDs, []string{"doc-10", "doc-20"}) || postfiltered.Plan.ScalarFilterStrategy != HybridScalarFilterStrategyPostfilter {
		t.Fatalf("postfilter ids=%v plan=%+v want doc-10/doc-20 postfilter", gotIDs, postfiltered.Plan)
	}
	if postfiltered.Stats.ScalarPrefilterIDs != 0 || postfiltered.Stats.ScalarPostfilterChecks != 4 || postfiltered.Stats.ScalarFilterMatched != 2 || postfiltered.Stats.ScalarFilterRejected != 2 {
		t.Fatalf("postfilter stats=%+v want bounded fused-result checks", postfiltered.Stats)
	}

	empty, err := col.SearchHybrid(HybridSearchOptions{
		TopK:         2,
		Text:         &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		ScalarFilter: &HybridScalarFilter{IndexName: "city", Value: "does-not-exist"},
	})
	if err != nil {
		t.Fatalf("SearchHybrid empty scalar match: %v", err)
	}
	if len(empty.Results) != 0 || empty.Stats.FailClosed != 0 || empty.Stats.FailClosedReason != "" || empty.Stats.ScalarPrefilterIDs != 0 || empty.Stats.ScalarFilterMatched != 0 || empty.Stats.ScalarFilterRejected != 0 || empty.Stats.TextPostingsScanned != 0 {
		t.Fatalf("empty scalar response=%+v want no results without fail-closed or source traversal", empty)
	}

	broad, err := col.SearchHybrid(HybridSearchOptions{
		TopK:         1,
		Text:         &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 1},
		ScalarFilter: &HybridScalarFilter{IndexName: "city", Value: "sea"},
	})
	if err != nil {
		t.Fatalf("SearchHybrid bounded broad scalar: %v", err)
	}
	if len(broad.Results) != 1 || broad.Stats.FailClosed != 0 || broad.Stats.ScalarPrefilterIDs != 4 || broad.Stats.TextCandidatesReturned != 1 {
		t.Fatalf("broad response=%+v want small bounded scalar allow-set to serve", broad)
	}

	forcedPlan := hybridSearchExecutionPlan{
		topK:                 1,
		scalarFilter:         &HybridScalarFilter{IndexName: "city", Value: "sea"},
		scalarFilterStrategy: HybridScalarFilterStrategyPrefilter,
		scalarLookupLimit:    1,
	}
	_, forcedStats, err := col.hybridScalarAllowSet(forcedPlan)
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) {
		t.Fatalf("forced tight scalar err=%v want ErrHybridSearchIndexUnavailable", err)
	}
	if forcedStats.Truncated == 0 {
		t.Fatalf("forced tight scalar stats=%+v want truncation", forcedStats)
	}

	missing, err := col.SearchHybrid(HybridSearchOptions{
		TopK:         2,
		Text:         &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2},
		ScalarFilter: &HybridScalarFilter{IndexName: "missing_city", Value: "sea"},
	})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) {
		t.Fatalf("missing scalar err=%v want ErrHybridSearchIndexUnavailable", err)
	}
	if missing.Stats.FailClosed != 1 || missing.Stats.FailClosedReason != HybridFailClosedReasonScalarFilterUnbounded || missing.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("missing scalar response=%+v want fail-closed scalar reason and no fallback", missing)
	}
}

func TestHybridScalarFilterDefaultLookupBudgetFailClosed2687(t *testing.T) {
	rows := make([]hybridSearchExecutorFixtureRow2505, hybridScalarDefaultLookupLimit+1)
	for i := range rows {
		rows[i] = hybridSearchExecutorFixtureRow2505{
			id:    fmt.Sprintf("doc-%05d", i),
			title: "refund",
			body:  "refund",
			city:  "sea",
			score: int64(i),
		}
	}
	_, d, col := openHybridScalarSearchExecutorFixture2505(t, rows)
	defer func() { _ = d.Close() }()

	got, err := col.SearchHybrid(HybridSearchOptions{
		TopK:         1,
		Text:         &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 1},
		ScalarFilter: &HybridScalarFilter{IndexName: "city", Value: "sea"},
	})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) {
		t.Fatalf("SearchHybrid over-budget scalar err=%v want ErrHybridSearchIndexUnavailable", err)
	}
	if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != HybridFailClosedReasonScalarFilterUnbounded || got.Stats.Truncated == 0 || got.Stats.TextCandidatesReturned != 0 {
		t.Fatalf("response=%+v want scalar unbounded before candidate generation", got)
	}
	if got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("stats=%+v want no document fetch/fallback on scalar fail-closed", got.Stats)
	}
}

func TestHybridCandidateErrorFailClosedReasonSourceAware2505(t *testing.T) {
	textErr := hybridCandidateSourceError{source: HybridCandidateSourceText, err: fmt.Errorf("%w: text unavailable without stats", ErrHybridSearchIndexUnavailable)}
	if got := hybridCandidateErrorFailClosedReason(textErr); got != HybridFailClosedReasonTextIndexUnavailable {
		t.Fatalf("text source fallback reason=%q want %q", got, HybridFailClosedReasonTextIndexUnavailable)
	}
	vectorErr := hybridCandidateSourceError{source: HybridCandidateSourceVector, err: fmt.Errorf("%w: vector unavailable without stats", ErrHybridSearchIndexUnavailable)}
	if got := hybridCandidateErrorFailClosedReason(vectorErr); got != HybridFailClosedReasonVectorIndexUnavailable {
		t.Fatalf("vector source fallback reason=%q want %q", got, HybridFailClosedReasonVectorIndexUnavailable)
	}
}

func TestSearchHybridMissingSourcesAndSnapshotModeFailClosed2505(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-a", title: "refund", body: "refund", city: "sea", score: 10, vector: []float32{1, 0, 0}},
	})
	defer func() { _ = d.Close() }()

	missingText, err := col.SearchHybrid(HybridSearchOptions{TopK: 1, Text: &HybridTextQuery{IndexName: "missing", Query: "refund", CandidateLimit: 1}})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) || missingText.Stats.FailClosedReason != HybridFailClosedReasonTextIndexUnavailable {
		t.Fatalf("missing text response=%+v err=%v", missingText, err)
	}

	missingVector, err := col.SearchHybrid(HybridSearchOptions{TopK: 1, Vector: &HybridVectorQuery{IndexName: "missing", Query: []float32{1, 0, 0}, CandidateLimit: 1, EfSearch: 1}})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) || missingVector.Stats.FailClosedReason != HybridFailClosedReasonVectorIndexUnavailable {
		t.Fatalf("missing vector response=%+v err=%v (valid def was %q)", missingVector, err, def.Name)
	}

	badFetch, err := col.SearchHybrid(HybridSearchOptions{
		TopK:             1,
		Text:             &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 1},
		IncludeDocuments: true,
		DocumentFetchOptions: DocumentFetchOptions{
			IncludePaths: []string{"body.nested"},
		},
	})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) || badFetch.Stats.FailClosedReason != HybridFailClosedReasonDocumentFetchUnavailable || badFetch.Stats.DocumentsFetched != 0 {
		t.Fatalf("bad fetch response=%+v err=%v want document-fetch unavailable without fetched docs", badFetch, err)
	}

	boundSnapshot, err := col.SearchHybrid(HybridSearchOptions{TopK: 1, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 1}, Consistency: HybridConsistencyOptions{Mode: HybridConsistencyBoundSnapshot}})
	if !errors.Is(err, ErrHybridSearchUnsupported) || boundSnapshot.Stats.FailClosedReason != HybridFailClosedReasonSnapshotMismatch {
		t.Fatalf("bound snapshot response=%+v err=%v want unsupported snapshot mismatch", boundSnapshot, err)
	}
}

func TestSearchHybridMultiFieldANDRoutesCountersAndFailures4292(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-10", title: "refund", body: "refund alpha", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-20", title: "refund", body: "refund beta", city: "sea", score: 20, vector: []float32{0.9, 0.1, 0}},
		{id: "doc-30", title: "refund", body: "refund gamma", city: "sfo", score: 30, vector: []float32{0.8, 0.2, 0}},
		{id: "doc-40", title: "shipping", body: "shipping", city: "sea", score: 40, vector: []float32{0, 1, 0}},
	})
	defer func() { _ = d.Close() }()

	eqEq := &HybridScalarFilter{And: []HybridScalarFilter{
		{IndexName: "city", Value: "sea"},
		{IndexName: "kind", Value: "hybrid"},
	}}
	got, err := col.SearchHybrid(HybridSearchOptions{
		TopK: 10, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 10}, ScalarFilter: eqEq,
	})
	if err != nil {
		t.Fatalf("eq+eq SearchHybrid: %v", err)
	}
	if got.Plan.ScalarFilterLookupCount != 2 || got.Stats.ScalarFilterLookups != 2 || got.Stats.ScalarFilterIntersectionSteps != 1 || got.Stats.ScalarFilterFinalIDs != 3 || got.Stats.ScalarPrefilterIDs != 3 {
		t.Fatalf("eq+eq plan=%+v stats=%+v", got.Plan, got.Stats)
	}
	eqRange, err := col.SearchHybrid(HybridSearchOptions{
		TopK: 4,
		Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		ScalarFilter: &HybridScalarFilter{And: []HybridScalarFilter{
			{IndexName: "city", Value: "sea"},
			{IndexName: "score", Range: &IndexRangeOptions{
				Lower: IndexRangeBound{Unbounded: true},
				Upper: IndexRangeBound{Value: int64(20), Inclusive: true},
			}},
		}},
	})
	if err != nil || !slicesEqualStrings(hybridResultIDs2505(eqRange.Results), []string{"doc-10", "doc-20"}) || eqRange.Stats.ScalarFilterInputIDs != 5 || eqRange.Stats.ScalarFilterFinalIDs != 2 {
		t.Fatalf("eq+range response=%+v err=%v", eqRange, err)
	}

	threeField := func(reverse bool) *HybridScalarFilter {
		leaves := []HybridScalarFilter{
			{IndexName: "city", Value: "sea"},
			{IndexName: "kind", Value: "hybrid"},
			{IndexName: "score", Range: &IndexRangeOptions{
				Lower: IndexRangeBound{Unbounded: true},
				Upper: IndexRangeBound{Value: int64(20), Inclusive: true},
			}},
		}
		if reverse {
			leaves[0], leaves[2] = leaves[2], leaves[0]
		}
		return &HybridScalarFilter{And: leaves}
	}
	routes := []struct {
		name   string
		text   *HybridTextQuery
		vector *HybridVectorQuery
	}{
		{name: "text", text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4}},
		{name: "vector", vector: &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 8, QueryMode: VectorIndexQueryModeExact}},
		{name: "hybrid", text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4}, vector: &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 8, QueryMode: VectorIndexQueryModeExact}},
	}
	var ordered []string
	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			response, err := col.SearchHybrid(HybridSearchOptions{TopK: 4, Text: route.text, Vector: route.vector, ScalarFilter: threeField(false)})
			if err != nil {
				t.Fatalf("SearchHybrid: %v", err)
			}
			ids := hybridResultIDs2505(response.Results)
			if !slicesEqualStrings(ids, []string{"doc-10", "doc-20"}) {
				t.Fatalf("ids=%v response=%+v", ids, response)
			}
			if response.Plan.ScalarFilterLookupCount != 3 || response.Plan.ScalarFilterLookupLimit != hybridScalarDefaultLookupLimit || response.Plan.ScalarFilterAggregateLimit != 3*hybridScalarDefaultLookupLimit {
				t.Fatalf("plan=%+v", response.Plan)
			}
			if response.Stats.ScalarFilterLookups != 3 || response.Stats.ScalarFilterInputIDs != 9 || response.Stats.ScalarFilterIntersectionSteps != 2 || response.Stats.ScalarFilterFinalIDs != 2 || response.Stats.FullDocumentScanFallbacks != 0 {
				t.Fatalf("stats=%+v", response.Stats)
			}
			if route.name == "hybrid" {
				ordered = ids
			}
		})
	}
	reversed, err := col.SearchHybrid(HybridSearchOptions{
		TopK:         4,
		Text:         &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		Vector:       &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 8, QueryMode: VectorIndexQueryModeExact},
		ScalarFilter: threeField(true),
	})
	if err != nil || !slicesEqualStrings(hybridResultIDs2505(reversed.Results), ordered) {
		t.Fatalf("reversed conjunction results=%v err=%v want %v", hybridResultIDs2505(reversed.Results), err, ordered)
	}

	empty, err := col.SearchHybrid(HybridSearchOptions{
		TopK:                 2,
		Text:                 &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		Vector:               &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 8, QueryMode: VectorIndexQueryModeExact},
		ScalarFilterStrategy: HybridScalarFilterStrategyPostfilter,
		ScalarFilter: &HybridScalarFilter{And: []HybridScalarFilter{
			{IndexName: "city", Value: "sfo"},
			{IndexName: "score", Range: &IndexRangeOptions{Lower: IndexRangeBound{Unbounded: true}, Upper: IndexRangeBound{Value: int64(20), Inclusive: true}}},
		}},
	})
	if err != nil || len(empty.Results) != 0 || empty.Stats.TextPostingsScanned != 0 || empty.Stats.VectorCandidatesExamined != 0 || empty.Stats.ScalarFilterLookups != 2 || empty.Stats.ScalarFilterFinalIDs != 0 {
		t.Fatalf("empty intersection response=%+v err=%v", empty, err)
	}

	missing, err := col.SearchHybrid(HybridSearchOptions{
		TopK: 2,
		Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		ScalarFilter: &HybridScalarFilter{And: []HybridScalarFilter{
			{IndexName: "city", Value: "does-not-exist"},
			{IndexName: "missing", Value: "x"},
		}},
	})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) || missing.Stats.FailClosed != 1 || missing.Stats.ScalarFilterLookups != 2 || len(missing.Results) != 0 || missing.Stats.TextPostingsScanned != 0 {
		t.Fatalf("missing-after-empty response=%+v err=%v", missing, err)
	}

	forcedPlan := hybridSearchExecutionPlan{
		topK: 1, scalarLookupLimit: 1, scalarAggregateLimit: 2,
		scalarFilterStrategy: HybridScalarFilterStrategyPrefilter,
		scalarFilter:         &HybridScalarFilter{And: []HybridScalarFilter{{IndexName: "city", Value: "sea"}, {IndexName: "kind", Value: "hybrid"}}},
	}
	if _, stats, err := col.hybridScalarAllowSet(forcedPlan); !errors.Is(err, ErrHybridSearchIndexUnavailable) || stats.Truncated != 1 || stats.ScalarFilterLookups != 1 {
		t.Fatalf("truncated conjunction stats=%+v err=%v", stats, err)
	}
}

func TestSearchHybridMultiFieldANDUnsupportedAndConcurrentSnapshot4292(t *testing.T) {
	if _, err := planHybridSearch(HybridSearchOptions{
		TopK: 1,
		Text: &HybridTextQuery{IndexName: "lexical", Query: "refund"},
		ScalarFilter: &HybridScalarFilter{And: []HybridScalarFilter{
			{IndexName: "city", Value: "sea"},
			{And: []HybridScalarFilter{{IndexName: "score", Value: int64(1)}, {IndexName: "kind", Value: "hybrid"}}},
		}},
	}); !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("nested conjunction err=%v want unsupported", err)
	}

	_, d, col := openHybridScalarSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "stable", title: "refund", body: "refund", city: "sea", score: 10},
		{id: "toggle", title: "refund", body: "refund", city: "sea", score: 99},
	})
	defer func() { _ = d.Close() }()
	filter := &HybridScalarFilter{And: []HybridScalarFilter{
		{IndexName: "city", Value: "sea"},
		{IndexName: "score", Range: &IndexRangeOptions{Lower: IndexRangeBound{Unbounded: true}, Upper: IndexRangeBound{Value: int64(10), Inclusive: true}}},
	}}
	oldDoc := mustHybridFixtureDocument2505(t, hybridSearchExecutorFixtureRow2505{id: "toggle", title: "refund", body: "refund", city: "sea", score: 99}, 2)
	newDoc := mustHybridFixtureDocument2505(t, hybridSearchExecutorFixtureRow2505{id: "toggle", title: "refund", body: "refund", city: "sfo", score: 10}, 2)
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 64 {
			next := oldDoc
			if i%2 == 0 {
				next = newDoc
			}
			if _, changed, err := col.Update([]byte("toggle"), func([]byte) ([]byte, bool, error) { return next, true, nil }); err != nil || !changed {
				errs <- fmt.Errorf("concurrent update %d changed=%v: %v", i, changed, err)
				return
			}
			if err := col.Flush(); err != nil {
				errs <- fmt.Errorf("concurrent flush %d: %w", i, err)
				return
			}
		}
	}()
	defer wg.Wait()
	successes := 0
	for i := range 64 {
		response, err := col.SearchHybrid(HybridSearchOptions{TopK: 4, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4}, ScalarFilter: filter})
		if err != nil {
			if response.Stats.FailClosed != 1 || len(response.Results) != 0 {
				t.Fatalf("iteration %d partial fail-closed response=%+v err=%v", i, response, err)
			}
			continue
		}
		if ids := hybridResultIDs2505(response.Results); !slicesEqualStrings(ids, []string{"stable"}) {
			t.Fatalf("iteration %d mixed-snapshot ids=%v response=%+v", i, ids, response)
		}
		successes++
	}
	wg.Wait()
	if successes == 0 {
		t.Fatal("all concurrent searches failed closed; want at least one coherent success")
	}
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
}

func TestSearchHybridInsertUpdateDeleteReopenConsistency2505(t *testing.T) {
	dir, d, col := openHybridScalarSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "mutable", title: "refund", body: "refund", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "deleted", title: "refund", body: "refund", city: "sea", score: 20, vector: []float32{0.9, 0.1, 0}},
		{id: "stable", title: "shipping", body: "shipping", city: "sea", score: 30, vector: []float32{0, 1, 0}},
	})
	closed := false
	defer func() {
		if !closed {
			_ = d.Close()
		}
	}()

	before, err := col.SearchHybrid(HybridSearchOptions{TopK: 10, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 10}, ScalarFilter: &HybridScalarFilter{IndexName: "city", Value: "sea"}})
	if err != nil {
		t.Fatalf("SearchHybrid before mutations: %v", err)
	}
	if gotIDs := hybridResultIDs2505(before.Results); !slicesEqualStrings(gotIDs, []string{"deleted", "mutable"}) {
		t.Fatalf("before ids=%v want deleted/mutable", gotIDs)
	}

	if gotIDs := searchHybridTextMultiScalarIDs4292(t, col); !slicesEqualStrings(gotIDs, []string{"deleted", "mutable"}) {
		t.Fatalf("before multi-field ids=%v want deleted/mutable", gotIDs)
	}
	updatedDoc := mustHybridFixtureDocument2505(t, hybridSearchExecutorFixtureRow2505{id: "mutable", title: "shipping", body: "shipping", city: "sfo", score: 11, vector: []float32{1, 0, 0}}, 10)
	if _, changed, err := col.Update([]byte("mutable"), func(current []byte) ([]byte, bool, error) {
		if len(current) == 0 {
			return nil, false, fmt.Errorf("missing mutable")
		}
		return updatedDoc, true, nil
	}); err != nil || !changed {
		t.Fatalf("Update mutable changed=%v err=%v", changed, err)
	}
	if _, err := col.Insert([]byte("new"), mustHybridFixtureDocument2505(t, hybridSearchExecutorFixtureRow2505{id: "new", title: "refund", body: "refund", city: "sea", score: 12, vector: []float32{0.8, 0.2, 0}}, 11)); err != nil {
		t.Fatalf("Insert new: %v", err)
	}
	if err := col.Delete([]byte("deleted")); err != nil {
		t.Fatalf("Delete deleted: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush mutations: %v", err)
	}

	wantAfter := []string{"new"}
	if gotIDs := searchHybridTextScalarIDs2505(t, col); !slicesEqualStrings(gotIDs, wantAfter) {
		t.Fatalf("after mutation ids=%v want %v", gotIDs, wantAfter)
	}
	if gotIDs := searchHybridTextMultiScalarIDs4292(t, col); !slicesEqualStrings(gotIDs, wantAfter) {
		t.Fatalf("after mutation multi-field ids=%v want %v", gotIDs, wantAfter)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	closed = true

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	if gotIDs := searchHybridTextScalarIDs2505(t, reopenedCol); !slicesEqualStrings(gotIDs, wantAfter) {
		t.Fatalf("after reopen ids=%v want %v", gotIDs, wantAfter)
	}
	if gotIDs := searchHybridTextMultiScalarIDs4292(t, reopenedCol); !slicesEqualStrings(gotIDs, wantAfter) {
		t.Fatalf("after reopen multi-field ids=%v want %v", gotIDs, wantAfter)
	}
}

var hybridSearchExecutorBenchmarkSink2505 HybridSearchResponse

func BenchmarkSearchHybridExecutor2505(b *testing.B) {
	rows := make([]hybridSearchExecutorFixtureRow2505, 128)
	for i := range rows {
		city := "city-rare"
		if i%4 != 0 {
			city = "city-common"
		}
		rows[i] = hybridSearchExecutorFixtureRow2505{
			id:     fmt.Sprintf("doc-%03d", i),
			title:  "refund policy",
			body:   fmt.Sprintf("refund policy shard %d customer %d", i%8, i%17),
			city:   city,
			score:  int64(i),
			vector: []float32{1 + float32(i%11)*0.01, float32((i*7)%17) * 0.01, float32((i*13)%19) * 0.01},
		}
	}
	_, d, col, def := openHybridSearchExecutorFixture2505(b, rows)
	defer func() { _ = d.Close() }()
	opts := HybridSearchOptions{
		TopK:             10,
		Text:             &HybridTextQuery{IndexName: "lexical", Query: "refund policy", CandidateLimit: 32},
		Vector:           &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0.1, 0.05}, CandidateLimit: 32, EfSearch: 64},
		ScalarFilter:     &HybridScalarFilter{IndexName: "city", Value: "city-rare"},
		IncludeDocuments: true,
		DocumentFetchOptions: DocumentFetchOptions{
			ExcludePaths: []string{"embedding"},
		},
	}
	warm, err := col.SearchHybrid(opts)
	if err != nil {
		b.Fatalf("warm SearchHybrid: %v", err)
	}
	if len(warm.Results) == 0 || warm.Stats.DocumentsFetched == 0 || warm.Stats.DocumentsFetched > uint64(opts.TopK) {
		b.Fatalf("warm response=%+v want bounded fetched hybrid results", warm)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var sink HybridSearchResponse
	for i := 0; i < b.N; i++ {
		got, err := col.SearchHybrid(opts)
		if err != nil {
			b.Fatalf("SearchHybrid: %v", err)
		}
		if len(got.Results) == 0 {
			b.Fatal("SearchHybrid returned no results")
		}
		sink = got
	}
	b.StopTimer()
	hybridSearchExecutorBenchmarkSink2505 = sink
	b.ReportMetric(float64(sink.Stats.TextCandidatesReturned), "text_candidates/search")
	b.ReportMetric(float64(sink.Stats.TextPostingsScanned), "text_postings/search")
	b.ReportMetric(float64(sink.Stats.TextStateLookups), "text_state_lookups/search")
	b.ReportMetric(float64(sink.Stats.TextNormLookups), "text_norm_lookups/search")
	b.ReportMetric(float64(sink.Stats.TextMatchDetailsBuilt), "text_match_details/search")
	b.ReportMetric(float64(sink.Stats.VectorCandidatesReturned), "vector_candidates/search")
	b.ReportMetric(float64(sink.Stats.CandidatesFused), "candidates_fused/search")
	b.ReportMetric(float64(sink.Stats.DocumentsFetched), "docs_fetched/search")
	b.ReportMetric(float64(sink.Stats.FusionBoth), "fusion_both/search")
	b.ReportMetric(float64(sink.Stats.ScalarFilterRejected), "scalar_rejected/search")
}

func openHybridSearchExecutorFixture2505(tb testing.TB, rows []hybridSearchExecutorFixtureRow2505) (string, *backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 4)
	cfg := columnGraphRebuildColumnStoreConfigV2A(3)
	cfg.RetainedPayload = ColumnRetainedPayloadFull
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingJSON
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "score", Field: "score", ValueType: IndexValueInt64},
			{Name: "kind", Field: "kind", ValueType: IndexValueString},
		},
		VectorIndexes: []VectorIndexDefinition{def},
		TextIndexes:   []TextIndexDefinition{{Name: "lexical", Fields: []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}, StorePositions: true}},
	}
	col := createHybridFixtureCollection2505(tb, d, meta, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	return dir, d, col, def
}

func openHybridScalarSearchExecutorFixture2505(tb testing.TB, rows []hybridSearchExecutorFixtureRow2505) (string, *backenddb.DB, *Collection) {
	tb.Helper()
	dir := tb.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	meta := CollectionMeta{
		Name:    "docs",
		Options: CollectionOptions{DocumentFormat: DocumentFormatJSON},
	}
	col := createHybridFixtureCollection2505(tb, d, meta, rows)
	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateIndex city: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "score", Field: "score", ValueType: IndexValueInt64}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateIndex score: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "kind", Field: "kind", ValueType: IndexValueString}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateIndex kind: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}, StorePositions: true}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateTextIndex: %v", err)
	}
	return dir, d, col
}

func createHybridFixtureCollection2505(tb testing.TB, d *backenddb.DB, meta CollectionMeta, rows []hybridSearchExecutorFixtureRow2505) *Collection {
	tb.Helper()
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	ids := make([][]byte, len(rows))
	docs := make([][]byte, len(rows))
	for i, row := range rows {
		ids[i] = []byte(row.id)
		docs[i] = mustHybridFixtureDocument2505(tb, row, i+1)
	}
	if len(rows) > 0 {
		if _, err := col.InsertBatch(ids, docs); err != nil {
			_ = d.Close()
			tb.Fatalf("InsertBatch: %v", err)
		}
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		tb.Fatalf("Flush: %v", err)
	}
	return col
}

func mustHybridFixtureDocument2505(tb testing.TB, row hybridSearchExecutorFixtureRow2505, timeUS int) []byte {
	tb.Helper()
	raw, err := json.Marshal(map[string]any{
		"time_us":   int64(timeUS),
		"kind":      "hybrid",
		"did":       row.id,
		"embedding": row.vector,
		"title":     row.title,
		"body":      row.body,
		"city":      row.city,
		"score":     row.score,
	})
	if err != nil {
		tb.Fatalf("json.Marshal row %q: %v", row.id, err)
	}
	return raw
}

func hybridResultHasSource2505(result HybridSearchResult, source HybridCandidateSource) bool {
	for _, contribution := range result.Sources {
		if contribution.Source == source {
			return true
		}
	}
	return false
}

func hybridResultIDs2505(results []HybridSearchResult) []string {
	ids := make([]string, len(results))
	for i := range results {
		ids[i] = string(results[i].ID)
	}
	return ids
}

func searchHybridTextScalarIDs2505(tb testing.TB, col *Collection) []string {
	tb.Helper()
	got, err := col.SearchHybrid(HybridSearchOptions{TopK: 10, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 10}, ScalarFilter: &HybridScalarFilter{IndexName: "city", Value: "sea"}})
	if err != nil {
		tb.Fatalf("SearchHybrid text+scalar: %v", err)
	}
	return hybridResultIDs2505(got.Results)
}

func searchHybridTextMultiScalarIDs4292(tb testing.TB, col *Collection) []string {
	tb.Helper()
	got, err := col.SearchHybrid(HybridSearchOptions{
		TopK: 10,
		Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 10},
		ScalarFilter: &HybridScalarFilter{And: []HybridScalarFilter{
			{IndexName: "city", Value: "sea"},
			{IndexName: "score", Range: &IndexRangeOptions{
				Lower: IndexRangeBound{Unbounded: true},
				Upper: IndexRangeBound{Value: int64(20), Inclusive: true},
			}},
		}},
	})
	if err != nil {
		tb.Fatalf("SearchHybrid text+multi-scalar: %v", err)
	}
	return hybridResultIDs2505(got.Results)
}
