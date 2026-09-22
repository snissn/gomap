package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// textV2PhrasePositionMatchBudget bounds recursive per-candidate phrase
// position validation for high-frequency terms that do not form a span.
const textV2PhrasePositionMatchBudget = 4096

func executeTextV2PhraseSearchAtSnapshot(
	c *Collection,
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	idx TextIndexDefinition,
	opts TextSearchOptions,
	phrase textSearchParsedPhrase,
	slop int,
	candidateLimit, maxPostingsScanned int,
	resultMode textSearchResultMode,
	response TextSearchResponse,
) (ret TextSearchResponse, err error) {
	if response.Explain != nil {
		defer func() { textSearchExplainFinish(ret.Explain, ret.Stats) }()
	}
	uniqueTerms := uniqueSortedTextSearchTerms(append([]string(nil), phrase.terms...))
	ctx, err := newTextV2SearchContext(snap, catalog, idx, uniqueTerms)
	if err != nil {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
	}
	textSearchExplainBindV2Context(response.Explain, ctx, uniqueTerms, nil)
	textSearchExplainSetServingPath(response.Explain, TextSearchExplainPathPhrase, false)
	if ctx.corpus.DocumentCount == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	allowSet, err := newTextV2SearchOrdinalAllowSet(snap, catalog, ctx, opts.textV2AllowedDocumentIDs)
	if err != nil {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
	}
	textSearchExplainBindV2Context(response.Explain, ctx, uniqueTerms, allowSet)
	if allowSet != nil {
		response.Stats.TextScalarPrefilterIDs = uint64(allowSet.size())
	}
	if allowSet.empty() {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	if len(uniqueTerms) == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	if len(uniqueTerms) == 1 && len(phrase.terms) == 1 {
		return executeTextV2SearchAtSnapshot(c, snap, catalog, idx, opts, uniqueTerms, TextSearchOperatorOR, candidateLimit, maxPostingsScanned, resultMode, response)
	}
	phraseTermIndexes := textV2PhraseTermIndexes(phrase.terms, uniqueTerms)

	candidates := make(map[uint64]*textV2SearchCandidate)
	cache := textV2SearchBlockCache{}
	scanTerms := orderTextV2SearchScanTerms(uniqueTerms, TextSearchOperatorAND, ctx.termStats)
	scanStart := time.Now()
	for i, term := range scanTerms {
		allowNewCandidates := i == 0
		truncated, err := scanTextV2SearchPostingBlocksTerm(snap, catalog, ctx, &cache, term, candidates, allowNewCandidates, candidateLimit, maxPostingsScanned, allowSet, &response.Stats)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if truncated {
			response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
			return textSearchFailClosed(response, response.Stats.FailClosedReason, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded phrase candidate generation", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
		}
		pruneTextV2SearchANDCandidates(candidates, term)
		if len(candidates) == 0 {
			break
		}
	}
	response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
	response.Stats.PostingsScanned = response.Stats.TextPostingsScanned
	if len(candidates) == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}

	scoreStart := time.Now()
	scored := make([]*textV2SearchCandidate, 0, len(candidates))
	positionScratch := make([]textV2PositionValue, len(uniqueTerms))
	for _, candidate := range candidates {
		norm, ok, err := cache.normEntry(snap, catalog, ctx, candidate.ordinal, &response.Stats)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if !ok || norm.tombstoned() {
			continue
		}
		docMap, ok, err := cache.docMapEntry(snap, catalog, ctx, candidate.ordinal)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if !ok {
			continue
		}
		if norm.Generation != docMap.Generation || norm.Flags != docMap.Flags {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 norm/docmap generation mismatch for ordinal %d", candidate.ordinal))
		}
		if docMap.tombstoned() {
			continue
		}
		if countTextV2CurrentPostings(candidate, uniqueTerms, norm.Generation) != len(uniqueTerms) {
			continue
		}
		response.Stats.TextPhraseCandidatesChecked++
		matched, err := textV2CandidateMatchesPhraseAtSnapshot(snap, catalog, ctx, idx, candidate, uniqueTerms, phraseTermIndexes, phrase.gaps, positionScratch, slop, norm.Generation, &response.Stats)
		if err != nil {
			if errors.Is(err, ErrTextIndexUnavailable) {
				reason := response.Stats.FailClosedReason
				if reason == "" {
					reason = textSearchFailClosedUnsupported
				}
				return textSearchFailClosed(response, reason, err)
			}
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if !matched {
			continue
		}
		response.Stats.TextPhraseCandidatesMatched++
		score, err := scoreTextV2SearchCandidate(candidate, uniqueTerms, ctx, norm)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		candidate.generation = norm.Generation
		candidate.documentID = append(candidate.documentID[:0], docMap.DocumentID...)
		candidate.score = score
		scored = append(scored, candidate)
	}
	response.Stats.CandidateScoreNanos += uint64(time.Since(scoreStart).Nanoseconds())
	response.Stats.TextCandidatesScored = uint64(len(scored))
	response.Stats.CandidatesScored = response.Stats.TextCandidatesScored
	if len(scored) == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return bytes.Compare(scored[i].documentID, scored[j].documentID) < 0
	})
	if len(scored) > opts.TopK {
		scored = scored[:opts.TopK]
	}
	response.Stats.TextCandidatesReturned = uint64(len(scored))
	response.Results = make([]TextSearchResult, len(scored))
	for i, candidate := range scored {
		result := TextSearchResult{
			DocumentID: append([]byte(nil), candidate.documentID...),
			IndexName:  idx.Name,
			Rank:       i + 1,
			Score:      candidate.score,
			ScoreKind:  HybridScoreKindBM25F,
		}
		if err := populateTextV2SearchResultMatchesFromCandidate(snap, catalog, ctx, idx, candidate, resultMode, &result, &response.Stats); err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if response.Explain != nil {
			norm, ok, err := cache.normEntry(snap, catalog, ctx, candidate.ordinal, &response.Stats)
			if err != nil || !ok {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if err := textSearchExplainAppendV2CandidateScore(response.Explain, result, candidate, uniqueTerms, ctx, norm); err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
		}
		response.Results[i] = result
	}
	if opts.IncludeDocuments && len(response.Results) > 0 {
		if err := fetchTextSearchResultDocuments(c, snap, catalog, opts, &response); err != nil {
			return textSearchFailClosed(response, textSearchFailClosedDocumentFetch, err)
		}
	}
	return response, nil
}

func textV2CandidateMatchesPhraseAtSnapshot(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	idx TextIndexDefinition,
	candidate *textV2SearchCandidate,
	uniqueTerms []string,
	phraseTermIndexes []int,
	phraseGaps []int,
	positions []textV2PositionValue,
	slop int,
	generation uint64,
	stats *TextSearchStats,
) (bool, error) {
	if candidate == nil || len(phraseTermIndexes) == 0 || len(positions) < len(uniqueTerms) {
		return false, nil
	}
	for i, term := range uniqueTerms {
		posting, ok := candidate.postingForTerm(term)
		if !ok || posting.generation != generation {
			return false, nil
		}
		value, err := readTextV2PhrasePositionValueAtSnapshot(snap, catalog, ctx, idx, candidate.ordinal, generation, term, posting, stats)
		if err != nil {
			return false, err
		}
		positions[i] = value
	}
	matched, err := textV2PhrasePositionValuesMatch(phraseTermIndexes, phraseGaps, positions, slop)
	if err != nil && errors.Is(err, ErrTextIndexUnavailable) && stats != nil {
		stats.FailClosedReason = textSearchFailClosedPhrasePosition
	}
	return matched, err
}

func readTextV2PhrasePositionValueAtSnapshot(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	idx TextIndexDefinition,
	ordinal, generation uint64,
	term string,
	posting textV2SearchPostingValue,
	stats *TextSearchStats,
) (textV2PositionValue, error) {
	if stats != nil {
		stats.TextPositionLookups++
	}
	raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, ctx.positionsRootName, encodeTextV2PositionKey(ordinal, term), nil)
	if err != nil {
		return textV2PositionValue{}, err
	}
	if !found {
		return textV2PositionValue{}, errMalformedTextStorage("missing text-v2 position entry for phrase ordinal %d term %q", ordinal, term)
	}
	value, err := decodeTextV2PositionValueForTerm(raw, term)
	if err != nil {
		return textV2PositionValue{}, err
	}
	if value.Ordinal != ordinal || value.Generation != generation || value.Term != term {
		return textV2PositionValue{}, errMalformedTextStorage("text-v2 phrase position key/value identity mismatch for ordinal %d term %q", ordinal, term)
	}
	if err := validateTextV2PositionValueMatchesPosting(value, idx, posting); err != nil {
		return textV2PositionValue{}, err
	}
	return value, nil
}

func textV2PhraseTermIndexes(phraseTerms, uniqueTerms []string) []int {
	indexes := make([]int, len(phraseTerms))
	for i, term := range phraseTerms {
		indexes[i] = -1
		for j, unique := range uniqueTerms {
			if term == unique {
				indexes[i] = j
				break
			}
		}
	}
	return indexes
}

func textV2PhrasePositionValuesMatch(phraseTermIndexes []int, phraseGaps []int, values []textV2PositionValue, slop int) (bool, error) {
	if len(phraseTermIndexes) == 0 || len(phraseGaps) != len(phraseTermIndexes)-1 || phraseTermIndexes[0] < 0 || phraseTermIndexes[0] >= len(values) {
		return false, nil
	}
	first := values[phraseTermIndexes[0]]
	budget := textV2PhrasePositionMatchBudget
	for _, field := range first.Fields {
		var inline [textSearchMaxPhraseTerms][]uint32
		lists := inline[:0]
		for _, valueIdx := range phraseTermIndexes {
			if valueIdx < 0 || valueIdx >= len(values) {
				return false, nil
			}
			positions, ok := textV2PositionFieldPositions(values[valueIdx], field.FieldIndex)
			if !ok || len(positions) == 0 {
				lists = nil
				break
			}
			lists = append(lists, positions)
		}
		if len(lists) == len(phraseTermIndexes) {
			matched, err := textV2OrderedPositionsMatchSlop(lists, phraseGaps, slop, &budget)
			if err != nil || matched {
				return matched, err
			}
		}
	}
	return false, nil
}

func textV2PositionFieldPositions(value textV2PositionValue, fieldIndex uint32) ([]uint32, bool) {
	for _, field := range value.Fields {
		if field.FieldIndex == fieldIndex {
			return field.Positions, true
		}
	}
	return nil, false
}

func textV2OrderedPositionsMatchSlop(lists [][]uint32, expectedGaps []int, slop int, budget *int) (bool, error) {
	if len(lists) == 0 || len(lists[0]) == 0 {
		return false, nil
	}
	if len(expectedGaps) != len(lists)-1 {
		return false, nil
	}
	if len(lists) == 1 {
		return true, nil
	}
	for _, start := range lists[0] {
		if !spendTextV2PhrasePositionMatchBudget(budget) {
			return false, fmt.Errorf("%w: text phrase position matching exceeded bounded work limit", ErrTextIndexUnavailable)
		}
		matched, err := textV2OrderedPositionsMatchFrom(lists, expectedGaps, 1, start, 0, slop, budget)
		if err != nil || matched {
			return matched, err
		}
	}
	return false, nil
}

func textV2OrderedPositionsMatchFrom(lists [][]uint32, expectedGaps []int, index int, previous uint32, usedSlop, maxSlop int, budget *int) (bool, error) {
	if index >= len(lists) {
		return true, nil
	}
	expectedGap := expectedGaps[index-1]
	for _, pos := range lists[index] {
		if !spendTextV2PhrasePositionMatchBudget(budget) {
			return false, fmt.Errorf("%w: text phrase position matching exceeded bounded work limit", ErrTextIndexUnavailable)
		}
		if pos <= previous {
			continue
		}
		documentGap := int(pos - previous - 1)
		if documentGap < expectedGap {
			continue
		}
		gapSlop := documentGap - expectedGap
		if usedSlop+gapSlop > maxSlop {
			// Positions are sorted ascending, so later positions can only
			// increase the gap and cannot fit the remaining slop budget.
			break
		}
		matched, err := textV2OrderedPositionsMatchFrom(lists, expectedGaps, index+1, pos, usedSlop+gapSlop, maxSlop, budget)
		if err != nil || matched {
			return matched, err
		}
	}
	return false, nil
}

func spendTextV2PhrasePositionMatchBudget(budget *int) bool {
	if budget == nil {
		return true
	}
	if *budget <= 0 {
		return false
	}
	*budget--
	return true
}
