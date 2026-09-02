package collections

import "math"

// TextSearchExplainServingPath identifies the text-v2 execution path chosen for
// a query explain. Values are intentionally low-cardinality so they can be used
// in docs, tests, and operational dashboards.
type TextSearchExplainServingPath string

const (
	TextSearchExplainPathUnset          TextSearchExplainServingPath = ""
	TextSearchExplainPathV1Postings     TextSearchExplainServingPath = "v1_postings"
	TextSearchExplainPathExactPostings  TextSearchExplainServingPath = "exact_postings_scan"
	TextSearchExplainPathBlockMaxSingle TextSearchExplainServingPath = "blockmax_single_term"
	TextSearchExplainPathBlockMaxAND    TextSearchExplainServingPath = "blockmax_and"
	TextSearchExplainPathBlockMaxORWAND TextSearchExplainServingPath = "blockmax_or_wand"
	TextSearchExplainPathPhrase         TextSearchExplainServingPath = "phrase_validation"
	TextSearchExplainPathFailClosed     TextSearchExplainServingPath = "fail_closed"
)

// TextSearchExplain is an opt-in diagnostic payload for SearchText. It is nil
// unless TextSearchOptions.Explain is true. The payload describes the analyzed
// query, text-v2 root/status snapshot, execution path, pruning counters, and
// BM25F score components for returned results.
type TextSearchExplain struct {
	Enabled            bool                      `json:"enabled"`
	CollectionName     string                    `json:"collection_name,omitempty"`
	IndexName          string                    `json:"index_name,omitempty"`
	IndexVersion       TextIndexVersion          `json:"index_version,omitempty"`
	Query              string                    `json:"query,omitempty"`
	Operator           TextSearchOperator        `json:"operator,omitempty"`
	Phrase             *TextSearchExplainPhrase  `json:"phrase,omitempty"`
	TopK               int                       `json:"top_k,omitempty"`
	CandidateLimit     int                       `json:"candidate_limit,omitempty"`
	MaxPostingsScanned int                       `json:"max_postings_scanned,omitempty"`
	ResultMode         TextSearchResultMode      `json:"result_mode,omitempty"`
	Terms              []TextSearchExplainTerm   `json:"terms,omitempty"`
	Fields             []TextSearchExplainField  `json:"fields,omitempty"`
	Snapshot           TextSearchExplainSnapshot `json:"snapshot,omitempty"`
	Serving            TextSearchExplainServing  `json:"serving,omitempty"`
	Counters           TextSearchExplainCounters `json:"counters,omitempty"`
	Results            []TextSearchExplainResult `json:"results,omitempty"`
	FailClosedReason   string                    `json:"fail_closed_reason,omitempty"`
	FallbackReasons    []string                  `json:"fallback_reasons,omitempty"`
}

// TextSearchExplainPhrase reports bounded structured phrase/proximity options
// after analysis. Gaps are analyzer-position gaps between adjacent phrase terms.
type TextSearchExplainPhrase struct {
	Query string   `json:"query,omitempty"`
	Terms []string `json:"terms,omitempty"`
	Gaps  []int    `json:"gaps,omitempty"`
	Slop  int      `json:"slop,omitempty"`
}

// TextSearchExplainTerm reports per-term collection statistics used by BM25F.
type TextSearchExplainTerm struct {
	Term               string `json:"term"`
	DocumentFrequency  uint64 `json:"document_frequency"`
	TotalTermFrequency uint64 `json:"total_term_frequency"`
	PostingBlockCount  uint64 `json:"posting_block_count"`
	StatsGeneration    uint64 `json:"stats_generation,omitempty"`
}

// TextSearchExplainField reports field weights and corpus accounting used by
// BM25F length normalization.
type TextSearchExplainField struct {
	Field           string  `json:"field"`
	Weight          float64 `json:"weight"`
	DocumentCount   uint64  `json:"document_count"`
	TotalTokenCount uint64  `json:"total_token_count"`
	AverageLength   float64 `json:"average_length,omitempty"`
	StatsGeneration uint64  `json:"stats_generation,omitempty"`
}

// TextSearchExplainSnapshot identifies the root/status snapshot that served the
// query. It lets reopen/snapshot diagnostics prove which text-v2 generation was
// consulted without scanning the full index.
type TextSearchExplainSnapshot struct {
	FormatVersion    uint32   `json:"format_version,omitempty"`
	RootGeneration   uint64   `json:"root_generation,omitempty"`
	StatsGeneration  uint64   `json:"stats_generation,omitempty"`
	DocMapGeneration uint64   `json:"doc_map_generation,omitempty"`
	NormGeneration   uint64   `json:"norm_generation,omitempty"`
	TermGeneration   uint64   `json:"term_generation,omitempty"`
	NextOrdinal      uint64   `json:"next_ordinal,omitempty"`
	LiveDocuments    uint64   `json:"live_documents,omitempty"`
	DeletedDocuments uint64   `json:"deleted_documents,omitempty"`
	CorpusDocuments  uint64   `json:"corpus_documents,omitempty"`
	ActiveRootNames  []string `json:"active_root_names,omitempty"`
}

// TextSearchExplainServing reports the selected serving path and the main
// low-cardinality decisions/counters that explain why work was visited, skipped,
// pruned, or failed closed.
type TextSearchExplainServing struct {
	Path                 TextSearchExplainServingPath `json:"path,omitempty"`
	BM25K1               float64                      `json:"bm25_k1,omitempty"`
	BM25B                float64                      `json:"bm25_b,omitempty"`
	BlockMaxEnabled      bool                         `json:"block_max_enabled,omitempty"`
	PostingBlocksVisited uint64                       `json:"posting_blocks_visited,omitempty"`
	PostingBlocksSkipped uint64                       `json:"posting_blocks_skipped,omitempty"`
	BlockMaxFallbacks    uint64                       `json:"block_max_fallbacks,omitempty"`
	BlockMaxThresholds   uint64                       `json:"block_max_thresholds,omitempty"`
	WANDPivots           uint64                       `json:"wand_pivots,omitempty"`
	ScalarPruning        TextSearchExplainScalar      `json:"scalar_pruning,omitempty"`
	PhraseValidation     TextSearchExplainPhraseStats `json:"phrase_validation,omitempty"`
	FailClosedReason     string                       `json:"fail_closed_reason,omitempty"`
	FallbackReasons      []string                     `json:"fallback_reasons,omitempty"`
}

// TextSearchExplainScalar reports scalar allow-set pruning when text-v2 search
// is invoked from hybrid text+scalar serving.
type TextSearchExplainScalar struct {
	Enabled              bool   `json:"enabled,omitempty"`
	AllowSetSize         uint64 `json:"allow_set_size,omitempty"`
	PostingBlocksSkipped uint64 `json:"posting_blocks_skipped,omitempty"`
	PostingsRejected     uint64 `json:"postings_rejected,omitempty"`
}

// TextSearchExplainPhraseStats reports phrase position validation work.
type TextSearchExplainPhraseStats struct {
	PositionLookups   uint64 `json:"position_lookups,omitempty"`
	CandidatesChecked uint64 `json:"candidates_checked,omitempty"`
	CandidatesMatched uint64 `json:"candidates_matched,omitempty"`
}

// TextSearchExplainCounters mirrors the hot text counters at the end of the
// query so an explain payload can be copied into runbooks without separately
// formatting TextSearchResponse.Stats.
type TextSearchExplainCounters struct {
	PostingsScanned            uint64 `json:"postings_scanned,omitempty"`
	CandidatesScored           uint64 `json:"candidates_scored,omitempty"`
	CandidatesReturned         uint64 `json:"candidates_returned,omitempty"`
	PostingBlocksVisited       uint64 `json:"posting_blocks_visited,omitempty"`
	PostingBlocksSkipped       uint64 `json:"posting_blocks_skipped,omitempty"`
	BlockMaxFallbacks          uint64 `json:"block_max_fallbacks,omitempty"`
	BlockMaxThresholds         uint64 `json:"block_max_thresholds,omitempty"`
	WANDPivots                 uint64 `json:"wand_pivots,omitempty"`
	ScalarPrefilterIDs         uint64 `json:"scalar_prefilter_ids,omitempty"`
	ScalarPostingBlocksSkipped uint64 `json:"scalar_posting_blocks_skipped,omitempty"`
	ScalarPostingsRejected     uint64 `json:"scalar_postings_rejected,omitempty"`
	NormLookups                uint64 `json:"norm_lookups,omitempty"`
	MatchDetailsBuilt          uint64 `json:"match_details_built,omitempty"`
	PositionLookups            uint64 `json:"position_lookups,omitempty"`
	PhraseCandidatesChecked    uint64 `json:"phrase_candidates_checked,omitempty"`
	PhraseCandidatesMatched    uint64 `json:"phrase_candidates_matched,omitempty"`
	DocumentsFetched           uint64 `json:"documents_fetched,omitempty"`
	FullDocumentScanFallbacks  uint64 `json:"full_document_scan_fallbacks,omitempty"`
	FailClosed                 uint64 `json:"fail_closed,omitempty"`
}

// TextSearchExplainResult reports BM25F score decomposition for one returned
// result. Components are populated only for returned top-K results.
type TextSearchExplainResult struct {
	DocumentID []byte                       `json:"document_id"`
	Rank       int                          `json:"rank"`
	Ordinal    uint64                       `json:"ordinal,omitempty"`
	Generation uint64                       `json:"generation,omitempty"`
	Score      float64                      `json:"score"`
	Terms      []TextSearchExplainScoreTerm `json:"terms,omitempty"`
}

// TextSearchExplainScoreTerm reports one term's contribution to a returned
// result's BM25F score.
type TextSearchExplainScoreTerm struct {
	Term              string                        `json:"term"`
	DocumentFrequency uint64                        `json:"document_frequency"`
	IDF               float64                       `json:"idf"`
	CombinedTF        float64                       `json:"combined_tf"`
	Score             float64                       `json:"score"`
	Fields            []TextSearchExplainScoreField `json:"fields,omitempty"`
}

// TextSearchExplainScoreField reports one field lane's contribution to a term's
// BM25F combined term frequency.
type TextSearchExplainScoreField struct {
	Field         string  `json:"field"`
	Weight        float64 `json:"weight"`
	TermFrequency uint32  `json:"term_frequency"`
	FieldLength   uint32  `json:"field_length"`
	AverageLength float64 `json:"average_length"`
	NormalizedTF  float64 `json:"normalized_tf"`
	WeightedTF    float64 `json:"weighted_tf"`
}

func newTextSearchExplain(opts TextSearchOptions, resultMode textSearchResultMode) *TextSearchExplain {
	mode := opts.ResultMode
	if mode == TextSearchResultModeDefault {
		switch resultMode {
		case textSearchResultScoreOnly:
			mode = TextSearchResultModeScoreOnly
		case textSearchResultTextMatchesOnly:
			mode = TextSearchResultModeCompact
		default:
			mode = TextSearchResultModeDetailed
		}
	}
	explain := &TextSearchExplain{
		Enabled:            true,
		IndexName:          opts.IndexName,
		Query:              opts.Query,
		TopK:               opts.TopK,
		CandidateLimit:     opts.CandidateLimit,
		MaxPostingsScanned: opts.MaxPostingsScanned,
		ResultMode:         mode,
	}
	if opts.Phrase != nil {
		explain.Phrase = &TextSearchExplainPhrase{Query: opts.Phrase.Query, Slop: opts.Phrase.Slop}
	}
	return explain
}

func textSearchExplainBindIndex(explain *TextSearchExplain, catalog *collectionCatalog, idx TextIndexDefinition, opts TextSearchOptions) {
	if explain == nil {
		return
	}
	if catalog != nil {
		explain.CollectionName = catalog.meta.Name
	}
	explain.IndexName = idx.Name
	explain.IndexVersion = idx.Version
	explain.TopK = opts.TopK
	if opts.Query != "" {
		explain.Query = opts.Query
	}
	if opts.Phrase != nil {
		explain.Phrase = &TextSearchExplainPhrase{Query: opts.Phrase.Query, Slop: opts.Phrase.Slop}
	}
}

func textSearchExplainBindParsed(explain *TextSearchExplain, terms []string, operator TextSearchOperator, candidateLimit, maxPostingsScanned int) {
	if explain == nil {
		return
	}
	explain.Operator = operator
	explain.CandidateLimit = candidateLimit
	explain.MaxPostingsScanned = maxPostingsScanned
	if len(terms) == 0 {
		explain.Terms = nil
		return
	}
	// Term stats are filled once text-v2 context is available. Preserve analyzed
	// term order for empty-corpus and early-exit explains.
	explain.Terms = make([]TextSearchExplainTerm, len(terms))
	for i, term := range terms {
		explain.Terms[i].Term = term
	}
}

func textSearchExplainBindPhrase(explain *TextSearchExplain, phrase textSearchParsedPhrase, slop int, candidateLimit, maxPostingsScanned int) {
	if explain == nil {
		return
	}
	explain.Operator = TextSearchOperatorAND
	explain.CandidateLimit = candidateLimit
	explain.MaxPostingsScanned = maxPostingsScanned
	if explain.Phrase == nil {
		explain.Phrase = &TextSearchExplainPhrase{}
	}
	explain.Phrase.Terms = append(explain.Phrase.Terms[:0], phrase.terms...)
	explain.Phrase.Gaps = append(explain.Phrase.Gaps[:0], phrase.gaps...)
	explain.Phrase.Slop = slop
	explain.Terms = make([]TextSearchExplainTerm, len(phrase.terms))
	for i, term := range phrase.terms {
		explain.Terms[i].Term = term
	}
}

func textSearchExplainBindV2Context(explain *TextSearchExplain, ctx *textV2SearchContext, terms []string, allowSet *textV2SearchOrdinalAllowSet) {
	if explain == nil || ctx == nil {
		return
	}
	explain.CollectionName = ctx.collectionName
	explain.IndexName = ctx.indexName
	explain.IndexVersion = TextIndexVersionV2
	explain.Snapshot = TextSearchExplainSnapshot{
		FormatVersion:    ctx.status.FormatVersion,
		RootGeneration:   ctx.status.RootGeneration,
		StatsGeneration:  ctx.status.StatsGeneration,
		DocMapGeneration: ctx.status.DocMapGeneration,
		NormGeneration:   ctx.status.NormGeneration,
		TermGeneration:   ctx.status.TermGeneration,
		NextOrdinal:      ctx.status.NextOrdinal,
		LiveDocuments:    ctx.status.LiveDocuments,
		DeletedDocuments: ctx.status.DeletedDocuments,
		CorpusDocuments:  ctx.corpus.DocumentCount,
		ActiveRootNames:  collectionTextV2RootNames(ctx.collectionName, ctx.indexName),
	}
	explain.Terms = make([]TextSearchExplainTerm, 0, len(terms))
	for _, term := range terms {
		stats := ctx.termStats[term]
		explain.Terms = append(explain.Terms, TextSearchExplainTerm{
			Term:               term,
			DocumentFrequency:  stats.DocumentFrequency,
			TotalTermFrequency: stats.TotalTermFrequency,
			PostingBlockCount:  stats.PostingBlockCount,
			StatsGeneration:    stats.StatsGeneration,
		})
	}
	explain.Fields = make([]TextSearchExplainField, 0, len(ctx.fieldNames))
	for i, field := range ctx.fieldNames {
		stats := ctx.fieldStats[i]
		avg := 0.0
		if stats.DocumentCount > 0 {
			avg = float64(stats.TotalTokenCount) / float64(stats.DocumentCount)
		}
		explain.Fields = append(explain.Fields, TextSearchExplainField{
			Field:           field,
			Weight:          ctx.fieldWeights[i],
			DocumentCount:   stats.DocumentCount,
			TotalTokenCount: stats.TotalTokenCount,
			AverageLength:   avg,
			StatsGeneration: stats.StatsGeneration,
		})
	}
	explain.Serving.BM25K1 = textSearchBM25K1
	explain.Serving.BM25B = textSearchBM25B
	if allowSet != nil {
		explain.Serving.ScalarPruning.Enabled = true
		explain.Serving.ScalarPruning.AllowSetSize = uint64(allowSet.size())
	}
}

func textSearchExplainSetServingPath(explain *TextSearchExplain, path TextSearchExplainServingPath, blockMaxEnabled bool) {
	if explain == nil {
		return
	}
	explain.Serving.Path = path
	explain.Serving.BlockMaxEnabled = blockMaxEnabled
}

func textSearchExplainAddFallback(explain *TextSearchExplain, reason string) {
	if explain == nil || reason == "" {
		return
	}
	explain.FallbackReasons = append(explain.FallbackReasons, reason)
	explain.Serving.FallbackReasons = append(explain.Serving.FallbackReasons, reason)
}

func textSearchExplainFailClosed(explain *TextSearchExplain, reason string) {
	if explain == nil {
		return
	}
	if reason == "" {
		reason = textSearchFailClosedStorageCorrupt
	}
	explain.FailClosedReason = reason
	explain.Serving.FailClosedReason = reason
	if explain.Serving.Path == TextSearchExplainPathUnset {
		explain.Serving.Path = TextSearchExplainPathFailClosed
	}
}

func textSearchExplainFinish(explain *TextSearchExplain, stats TextSearchStats) {
	if explain == nil {
		return
	}
	explain.Counters = TextSearchExplainCounters{
		PostingsScanned:            maxTextSearchExplainUint64(stats.TextPostingsScanned, stats.PostingsScanned),
		CandidatesScored:           maxTextSearchExplainUint64(stats.TextCandidatesScored, stats.CandidatesScored),
		CandidatesReturned:         stats.TextCandidatesReturned,
		PostingBlocksVisited:       stats.TextPostingBlocksVisited,
		PostingBlocksSkipped:       stats.TextPostingBlocksSkipped,
		BlockMaxFallbacks:          stats.TextBlockMaxFallbacks,
		BlockMaxThresholds:         stats.TextBlockMaxThresholds,
		WANDPivots:                 stats.TextWANDPivots,
		ScalarPrefilterIDs:         stats.TextScalarPrefilterIDs,
		ScalarPostingBlocksSkipped: stats.TextScalarPostingBlocksSkipped,
		ScalarPostingsRejected:     stats.TextScalarPostingsRejected,
		NormLookups:                stats.TextNormLookups,
		MatchDetailsBuilt:          stats.TextMatchDetailsBuilt,
		PositionLookups:            stats.TextPositionLookups,
		PhraseCandidatesChecked:    stats.TextPhraseCandidatesChecked,
		PhraseCandidatesMatched:    stats.TextPhraseCandidatesMatched,
		DocumentsFetched:           stats.DocumentsFetched,
		FullDocumentScanFallbacks:  stats.FullDocumentScanFallbacks,
		FailClosed:                 stats.FailClosed,
	}
	explain.Serving.PostingBlocksVisited = stats.TextPostingBlocksVisited
	explain.Serving.PostingBlocksSkipped = stats.TextPostingBlocksSkipped
	explain.Serving.BlockMaxFallbacks = stats.TextBlockMaxFallbacks
	explain.Serving.BlockMaxThresholds = stats.TextBlockMaxThresholds
	explain.Serving.WANDPivots = stats.TextWANDPivots
	explain.Serving.ScalarPruning.PostingBlocksSkipped = stats.TextScalarPostingBlocksSkipped
	explain.Serving.ScalarPruning.PostingsRejected = stats.TextScalarPostingsRejected
	if stats.TextScalarPrefilterIDs != 0 {
		explain.Serving.ScalarPruning.Enabled = true
		explain.Serving.ScalarPruning.AllowSetSize = stats.TextScalarPrefilterIDs
	}
	explain.Serving.PhraseValidation = TextSearchExplainPhraseStats{
		PositionLookups:   stats.TextPositionLookups,
		CandidatesChecked: stats.TextPhraseCandidatesChecked,
		CandidatesMatched: stats.TextPhraseCandidatesMatched,
	}
	if stats.FailClosedReason != "" {
		textSearchExplainFailClosed(explain, stats.FailClosedReason)
	}
}

func maxTextSearchExplainUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func textSearchExplainAppendV2CandidateScore(explain *TextSearchExplain, result TextSearchResult, candidate *textV2SearchCandidate, terms []string, ctx *textV2SearchContext, norm textV2SearchNormEntry) error {
	if explain == nil || candidate == nil || ctx == nil {
		return nil
	}
	entry := TextSearchExplainResult{
		DocumentID: append([]byte(nil), result.DocumentID...),
		Rank:       result.Rank,
		Ordinal:    candidate.ordinal,
		Generation: candidate.generation,
		Score:      result.Score,
	}
	for _, term := range terms {
		posting, ok := candidate.postingForTerm(term)
		if !ok || posting.generation != norm.Generation {
			continue
		}
		component, err := textSearchExplainScoreTerm(term, posting, ctx, norm)
		if err != nil {
			return err
		}
		if component.CombinedTF > 0 {
			entry.Terms = append(entry.Terms, component)
		}
	}
	explain.Results = append(explain.Results, entry)
	return nil
}

func textSearchExplainAppendV2TopCandidateScore(explain *TextSearchExplain, result TextSearchResult, candidate textV2SearchTopCandidate, ctx *textV2SearchContext, norm textV2SearchNormEntry) error {
	if explain == nil || ctx == nil || !candidate.hasPosting || candidate.posting.generation != norm.Generation {
		return nil
	}
	component, err := textSearchExplainScoreTerm(candidate.term, candidate.posting, ctx, norm)
	if err != nil {
		return err
	}
	explain.Results = append(explain.Results, TextSearchExplainResult{
		DocumentID: append([]byte(nil), result.DocumentID...),
		Rank:       result.Rank,
		Ordinal:    candidate.ordinal,
		Generation: candidate.generation,
		Score:      result.Score,
		Terms:      []TextSearchExplainScoreTerm{component},
	})
	return nil
}

func textSearchExplainScoreTerm(term string, posting textV2SearchPostingValue, ctx *textV2SearchContext, norm textV2SearchNormEntry) (TextSearchExplainScoreTerm, error) {
	if ctx.corpus.DocumentCount == 0 {
		return TextSearchExplainScoreTerm{}, errMalformedTextStorage("text-v2 corpus document count is zero with search explain score components")
	}
	if len(norm.FieldLengths) != len(ctx.fieldNames) {
		return TextSearchExplainScoreTerm{}, errMalformedTextStorage("text-v2 norm field count %d want %d", len(norm.FieldLengths), len(ctx.fieldNames))
	}
	stats := ctx.termStats[term]
	if stats.DocumentFrequency == 0 || stats.DocumentFrequency > ctx.corpus.DocumentCount {
		return TextSearchExplainScoreTerm{}, errMalformedTextStorage("text-v2 term %q document frequency %d outside corpus %d", term, stats.DocumentFrequency, ctx.corpus.DocumentCount)
	}
	corpusDocuments := float64(ctx.corpus.DocumentCount)
	df := float64(stats.DocumentFrequency)
	idf := math.Log(1 + (corpusDocuments-df+0.5)/(df+0.5))
	component := TextSearchExplainScoreTerm{
		Term:              term,
		DocumentFrequency: stats.DocumentFrequency,
		IDF:               idf,
	}
	for fieldIdx, field := range ctx.fieldNames {
		fieldTF := posting.fieldFrequency(fieldIdx)
		if fieldTF == 0 {
			continue
		}
		statsValue := ctx.fieldStats[fieldIdx]
		if statsValue.DocumentCount == 0 || statsValue.TotalTokenCount == 0 {
			return TextSearchExplainScoreTerm{}, errMalformedTextStorage("missing text-v2 field accounting for %q", field)
		}
		avgLength := float64(statsValue.TotalTokenCount) / float64(statsValue.DocumentCount)
		if avgLength <= 0 {
			return TextSearchExplainScoreTerm{}, errMalformedTextStorage("invalid average text-v2 field length for %q", field)
		}
		normalizedTF := float64(fieldTF) / (1 - textSearchBM25B + textSearchBM25B*(float64(norm.FieldLengths[fieldIdx])/avgLength))
		weightedTF := ctx.fieldWeights[fieldIdx] * normalizedTF
		component.CombinedTF += weightedTF
		component.Fields = append(component.Fields, TextSearchExplainScoreField{
			Field:         field,
			Weight:        ctx.fieldWeights[fieldIdx],
			TermFrequency: fieldTF,
			FieldLength:   norm.FieldLengths[fieldIdx],
			AverageLength: avgLength,
			NormalizedTF:  normalizedTF,
			WeightedTF:    weightedTF,
		})
	}
	if component.CombinedTF > 0 {
		component.Score = idf * ((component.CombinedTF * (textSearchBM25K1 + 1)) / (component.CombinedTF + textSearchBM25K1))
	}
	return component, nil
}
