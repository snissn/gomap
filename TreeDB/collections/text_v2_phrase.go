package collections

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func executeTextV2PhraseSearchAtSnapshot(
	c *Collection,
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	idx TextIndexDefinition,
	opts TextSearchOptions,
	phraseTerms []string,
	slop int,
	candidateLimit, maxPostingsScanned int,
	resultMode textSearchResultMode,
	response TextSearchResponse,
) (TextSearchResponse, error) {
	uniqueTerms := uniqueSortedTextSearchTerms(append([]string(nil), phraseTerms...))
	ctx, err := newTextV2SearchContext(snap, catalog, idx, uniqueTerms)
	if err != nil {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
	}
	if ctx.corpus.DocumentCount == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	allowSet, err := newTextV2SearchOrdinalAllowSet(snap, catalog, ctx, opts.textV2AllowedDocumentIDs)
	if err != nil {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
	}
	if allowSet.empty() {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	if len(uniqueTerms) == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	if len(uniqueTerms) == 1 && len(phraseTerms) == 1 {
		return executeTextV2SearchAtSnapshot(c, snap, catalog, idx, opts, uniqueTerms, TextSearchOperatorOR, candidateLimit, maxPostingsScanned, resultMode, response)
	}
	phraseTermIndexes := textV2PhraseTermIndexes(phraseTerms, uniqueTerms)

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
		matched, err := textV2CandidateMatchesPhraseAtSnapshot(snap, catalog, ctx, idx, candidate, uniqueTerms, phraseTermIndexes, positionScratch, slop, norm.Generation, &response.Stats)
		if err != nil {
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
	return textV2PhrasePositionValuesMatch(phraseTermIndexes, positions, slop), nil
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
	value, err := decodeTextV2PositionValue(raw)
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

func textV2PhrasePositionValuesMatch(phraseTermIndexes []int, values []textV2PositionValue, slop int) bool {
	if len(phraseTermIndexes) == 0 || phraseTermIndexes[0] < 0 || phraseTermIndexes[0] >= len(values) {
		return false
	}
	first := values[phraseTermIndexes[0]]
	for _, field := range first.Fields {
		var inline [textSearchMaxPhraseTerms][]uint32
		lists := inline[:0]
		for _, valueIdx := range phraseTermIndexes {
			if valueIdx < 0 || valueIdx >= len(values) {
				return false
			}
			positions, ok := textV2PositionFieldPositions(values[valueIdx], field.FieldIndex)
			if !ok || len(positions) == 0 {
				lists = nil
				break
			}
			lists = append(lists, positions)
		}
		if len(lists) == len(phraseTermIndexes) && textV2OrderedPositionsMatchSlop(lists, slop) {
			return true
		}
	}
	return false
}

func textV2PositionFieldPositions(value textV2PositionValue, fieldIndex uint32) ([]uint32, bool) {
	for _, field := range value.Fields {
		if field.FieldIndex == fieldIndex {
			return field.Positions, true
		}
	}
	return nil, false
}

func textV2OrderedPositionsMatchSlop(lists [][]uint32, slop int) bool {
	if len(lists) == 0 || len(lists[0]) == 0 {
		return false
	}
	if len(lists) == 1 {
		return true
	}
	for _, start := range lists[0] {
		if textV2OrderedPositionsMatchFrom(lists, 1, start, 0, slop) {
			return true
		}
	}
	return false
}

func textV2OrderedPositionsMatchFrom(lists [][]uint32, index int, previous uint32, usedSlop, maxSlop int) bool {
	if index >= len(lists) {
		return true
	}
	for _, pos := range lists[index] {
		if pos <= previous {
			continue
		}
		gap := int(pos - previous - 1)
		if usedSlop+gap > maxSlop {
			break
		}
		if textV2OrderedPositionsMatchFrom(lists, index+1, pos, usedSlop+gap, maxSlop) {
			return true
		}
	}
	return false
}
