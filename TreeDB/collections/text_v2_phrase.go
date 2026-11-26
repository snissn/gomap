package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
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
	allowSet, err := newTextV2SearchOrdinalAllowSet(snap, catalog, ctx, opts.scalarAllowedIDs())
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
	if !opts.textV2DisableBlockMax {
		return executeTextV2PhraseBlockMaxSearchAtSnapshot(c, snap, catalog, ctx, idx, opts, phrase, uniqueTerms, allowSet, slop, candidateLimit, maxPostingsScanned, resultMode, response)
	}
	return executeTextV2PhraseLegacySearchAtSnapshot(c, snap, catalog, ctx, idx, opts, phrase, uniqueTerms, allowSet, slop, candidateLimit, maxPostingsScanned, resultMode, response)
}

func executeTextV2PhraseBlockMaxSearchAtSnapshot(
	c *Collection,
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	idx TextIndexDefinition,
	opts TextSearchOptions,
	phrase textSearchParsedPhrase,
	uniqueTerms []string,
	allowSet *textV2SearchOrdinalAllowSet,
	slop int,
	candidateLimit, maxPostingsScanned int,
	resultMode textSearchResultMode,
	response TextSearchResponse,
) (TextSearchResponse, error) {
	textSearchExplainSetServingPath(response.Explain, TextSearchExplainPathPhrase, true)
	for _, term := range uniqueTerms {
		termStats := ctx.termStats[term]
		if termStats.DocumentFrequency == 0 {
			response.Results = []TextSearchResult{}
			return response, nil
		}
		if termStats.PostingBlockCount == 0 {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 term %q document frequency %d has no posting blocks", term, termStats.DocumentFrequency))
		}
	}

	fieldCount := len(ctx.fieldNames)
	scanTerms := orderTextV2SearchScanTerms(uniqueTerms, TextSearchOperatorAND, ctx.termStats)
	phraseTermIndexes := textV2PhraseTermIndexes(phrase.terms, scanTerms)
	states := make([]*textV2ANDBlockMaxTermState, 0, len(scanTerms))
	closeStates := func() {
		for _, state := range states {
			_ = state.close()
		}
		states = nil
	}
	defer closeStates()

	baseStats := response.Stats
	fallback := func() (TextSearchResponse, error) {
		closeStates()
		postingsScanned := response.Stats.TextPostingsScanned
		fallbackResponse := response
		fallbackResponse.Stats = baseStats
		fallbackResponse.Stats.TextPostingsScanned = postingsScanned
		fallbackResponse.Stats.PostingsScanned = postingsScanned
		fallbackResponse.Stats.TextBlockMaxFallbacks++
		textSearchExplainAddFallback(fallbackResponse.Explain, "blockmax_phrase_unsealed_stale_or_overlapping_block")
		fallbackOpts := opts
		fallbackOpts.textV2DisableBlockMax = true
		return executeTextV2PhraseLegacySearchAtSnapshot(c, snap, catalog, ctx, idx, fallbackOpts, phrase, uniqueTerms, allowSet, slop, candidateLimit, maxPostingsScanned, resultMode, fallbackResponse)
	}

	for _, term := range scanTerms {
		state, err := newTextV2ANDBlockMaxTermState(snap, catalog, ctx, term, fieldCount)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		states = append(states, state)
	}
	canonicalStates := make([]*textV2ANDBlockMaxTermState, len(uniqueTerms))
	for i, term := range uniqueTerms {
		for _, state := range states {
			if state.term == term {
				canonicalStates[i] = state
				break
			}
		}
	}

	cache := textV2SearchBlockCache{allowed: allowSet, maxBlocks: 2}
	top := textV2SearchTopK{limit: opts.TopK, candidates: make([]textV2SearchTopCandidate, 0, opts.TopK)}
	positionScratch := make([]textV2PositionValue, len(scanTerms))
	var posKeyScratch []byte
	var posValScratch []byte
	var hasLastOrdinal bool
	var lastOrdinal uint64
	retainDetail := resultMode != textSearchResultScoreOnly || response.Explain != nil

	scanStart := time.Now()
	for {
		if err := normalizeTextV2ANDBlockMaxStates(ctx, states, fieldCount); err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if textV2ANDBlockMaxAnyExhausted(states) {
			if err := exhaustTextV2ANDBlockMaxRemainingHeaders(ctx, states, fieldCount, &response.Stats); err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if textV2ANDBlockMaxNeedsFallback(states) {
				return fallback()
			}
			break
		}
		if textV2ANDBlockMaxNeedsFallback(states) {
			return fallback()
		}

		first, last := textV2ANDBlockMaxCurrentOverlap(states)
		if first > last {
			progressed, err := skipTextV2ANDBlockMaxNonOverlappingBlocks(ctx, states, fieldCount, first, &response.Stats)
			if err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if !progressed {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 phrase block-max failed to advance non-overlapping blocks"))
			}
			continue
		}

		cache.evictBefore(first)

		if allowSet != nil && !allowSet.intersects(first, last) {
			if err := skipTextV2ANDBlockMaxOverlapThrough(ctx, states, fieldCount, last, &response.Stats, true); err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			continue
		}

		combinedUpperBound := 0.0
		for _, state := range canonicalStates {
			combinedUpperBound += state.upperBound
		}
		if threshold, ok := top.threshold(); ok {
			canSkip := combinedUpperBound < threshold
			if !canSkip {
				tightCombinedUpperBound := 0.0
				for _, state := range canonicalStates {
					if err := tightenTextV2BlockMaxStateUpperBound(snap, catalog, ctx, &cache, allowSet, state, &response.Stats); err != nil {
						return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
					}
					tightCombinedUpperBound += state.upperBound
				}
				var err error
				canSkip, err = textV2SearchTightUpperBoundSkipsRange(snap, catalog, ctx, &cache, allowSet, tightCombinedUpperBound, first, last, &top)
				if err != nil {
					return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
				}
			}
			if canSkip {
				if err := skipTextV2ANDBlockMaxOverlapThrough(ctx, states, fieldCount, last, &response.Stats, false); err != nil {
					return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
				}
				continue
			}
		}

		truncated, stale, err := visitTextV2PhraseBlockMaxOverlap(
			snap, catalog, ctx, idx, states, canonicalStates, &cache, allowSet,
			phraseTermIndexes, phrase.gaps, positionScratch, &posKeyScratch, &posValScratch,
			slop, maxPostingsScanned, retainDetail, first, last, &top,
			&hasLastOrdinal, &lastOrdinal, &response.Stats,
		)
		if err != nil {
			response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
			response.Stats.PostingsScanned = response.Stats.TextPostingsScanned
			response.Stats.CandidatesScored = response.Stats.TextCandidatesScored
			if errors.Is(err, ErrTextIndexUnavailable) {
				reason := response.Stats.FailClosedReason
				if reason == "" {
					reason = textSearchFailClosedUnsupported
				}
				return textSearchFailClosed(response, reason, err)
			}
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if stale {
			return fallback()
		}
		if truncated {
			response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
			response.Stats.PostingsScanned = response.Stats.TextPostingsScanned
			response.Stats.CandidatesScored = response.Stats.TextCandidatesScored
			return textSearchFailClosed(response, response.Stats.FailClosedReason, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded phrase candidate generation", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
		}
	}
	for _, state := range states {
		if state.blocksSeen != state.termStats.PostingBlockCount {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 term %q posting block count %d does not match scanned blocks %d", state.term, state.termStats.PostingBlockCount, state.blocksSeen))
		}
	}
	response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
	response.Stats.PostingsScanned = response.Stats.TextPostingsScanned
	response.Stats.CandidatesScored = response.Stats.TextCandidatesScored
	if len(top.candidates) == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	response.Stats.TextCandidatesReturned = uint64(len(top.candidates))
	response.Results = make([]TextSearchResult, len(top.candidates))
	for i, candidate := range top.candidates {
		result := TextSearchResult{
			DocumentID: append([]byte(nil), candidate.documentID...),
			IndexName:  idx.Name,
			Rank:       i + 1,
			Score:      candidate.score,
			ScoreKind:  HybridScoreKindBM25F,
		}
		if resultMode != textSearchResultScoreOnly {
			if err := populateTextV2SearchResultMatchesFromCandidate(snap, catalog, ctx, idx, candidate.detail, resultMode, &result, &response.Stats); err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
		}
		if response.Explain != nil {
			norm, ok, err := cache.normEntry(snap, catalog, ctx, candidate.ordinal, &response.Stats)
			if err != nil || !ok {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if err := textSearchExplainAppendV2CandidateScore(response.Explain, result, candidate.detail, uniqueTerms, ctx, norm); err != nil {
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

func visitTextV2PhraseBlockMaxOverlap(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	idx TextIndexDefinition,
	states []*textV2ANDBlockMaxTermState,
	canonicalStates []*textV2ANDBlockMaxTermState,
	cache *textV2SearchBlockCache,
	allowSet *textV2SearchOrdinalAllowSet,
	phraseTermIndexes []int,
	phraseGaps []int,
	positionScratch []textV2PositionValue,
	posKeyScratch, posValScratch *[]byte,
	slop int,
	maxPostingsScanned int,
	retainDetail bool,
	first, last uint64,
	top *textV2SearchTopK,
	hasLastOrdinal *bool,
	lastOrdinal *uint64,
	stats *TextSearchStats,
) (bool, bool, error) {
	fieldCount := len(ctx.fieldNames)
	for _, state := range states {
		truncated, err := state.ensureDecoded(ctx, fieldCount, maxPostingsScanned, allowSet, stats)
		if truncated || err != nil {
			return truncated, false, err
		}
		for state.entryIdx < len(state.entries) && state.entries[state.entryIdx].ordinal < first {
			state.entryIdx++
		}
	}
	scoreStart := time.Now()
	defer func() {
		if stats != nil {
			stats.CandidateScoreNanos += uint64(time.Since(scoreStart).Nanoseconds())
		}
	}()
	var nextEvictOrdinal uint64
	for {
		var target uint64
		for _, state := range states {
			if state.entryIdx >= len(state.entries) || state.entries[state.entryIdx].ordinal > last {
				return false, false, nil
			}
			if state.entries[state.entryIdx].ordinal > target {
				target = state.entries[state.entryIdx].ordinal
			}
		}
		advanced := false
		for _, state := range states {
			for state.entryIdx < len(state.entries) && state.entries[state.entryIdx].ordinal < target {
				state.entryIdx++
				advanced = true
			}
			if state.entryIdx >= len(state.entries) || state.entries[state.entryIdx].ordinal > last {
				return false, false, nil
			}
			if state.entries[state.entryIdx].ordinal > target {
				target = state.entries[state.entryIdx].ordinal
				advanced = true
			}
		}
		if advanced {
			continue
		}
		if allowSet == nil || allowSet.contains(target) {
			if target >= nextEvictOrdinal {
				cache.evictBefore(target)
				normStart := textV2OrdinalBlockStart(target, textV2DefaultNormBlockSize)
				if normStart > 0 && normStart <= math.MaxUint64-uint64(textV2DefaultNormBlockSize) {
					nextEvictOrdinal = normStart + uint64(textV2DefaultNormBlockSize)
				} else {
					nextEvictOrdinal = math.MaxUint64
				}
			}
			norm, ok, err := cache.normEntry(snap, catalog, ctx, target, stats)
			if err != nil {
				return false, false, err
			}
			if ok && !norm.tombstoned() {
				current := true
				for _, state := range states {
					if state.entries[state.entryIdx].value.generation != norm.Generation {
						current = false
						break
					}
				}
				if !current {
					return false, true, nil
				}
				if *hasLastOrdinal && target <= *lastOrdinal {
					return false, false, errMalformedTextStorage("duplicate or non-monotonic text-v2 phrase candidate ordinal %d after %d", target, *lastOrdinal)
				}
				*hasLastOrdinal = true
				*lastOrdinal = target

				score, err := scoreTextV2ANDBlockMaxCandidate(canonicalStates, ctx, norm)
				if err != nil {
					return false, false, err
				}
				if top.needsDocumentIDForScore(score) {
					var docMap textV2SearchIdentity
					docMapLoaded := false
					skipByTie := false
					if worst, hasWorst := top.worst(); hasWorst && score == worst.score {
						loaded, docOK, err := cache.identityEntry(snap, catalog, ctx, target)
						if err != nil {
							return false, false, err
						}
						if !docOK {
							skipByTie = true
						} else {
							if norm.Generation != loaded.Generation || norm.Flags != loaded.Flags {
								return false, false, errMalformedTextStorage("text-v2 norm/docmap generation mismatch for ordinal %d", target)
							}
							if loaded.tombstoned() || loaded.compare(textV2SearchIdentity{inverse: worst.documentID}) >= 0 {
								skipByTie = true
							} else {
								docMap = loaded
								docMapLoaded = true
							}
						}
					}
					if !skipByTie {
						if stats != nil {
							stats.TextPhraseCandidatesChecked++
						}
						matched, err := textV2StatesMatchPhraseAtSnapshot(
							snap, catalog, ctx, idx, states,
							phraseTermIndexes, phraseGaps, positionScratch,
							posKeyScratch, posValScratch,
							slop, target, norm.Generation, stats,
						)
						if err != nil {
							return false, false, err
						}
						if matched {
							if !docMapLoaded {
								loaded, docOK, err := cache.identityEntry(snap, catalog, ctx, target)
								if err != nil {
									return false, false, err
								}
								if !docOK {
									goto advanceEntries
								}
								if norm.Generation != loaded.Generation || norm.Flags != loaded.Flags {
									return false, false, errMalformedTextStorage("text-v2 norm/docmap generation mismatch for ordinal %d", target)
								}
								if loaded.tombstoned() {
									goto advanceEntries
								}
								docMap = loaded
							}
							if stats != nil {
								stats.TextPhraseCandidatesMatched++
								stats.TextCandidatesScored++
							}
							topCandidate := textV2SearchTopCandidate{
								ordinal:    target,
								generation: norm.Generation,
								documentID: docMap.candidateID(),
								score:      score,
							}
							pos, admitted, thresholdChanged := top.addCandidate(topCandidate)
							if admitted {
								if docMap.known == "" {
									// Detach admitted ID from cached docMap block so evicted blocks can be GC'd.
									top.candidates[pos].documentID = docMap.appendTo(nil)
								}
								if retainDetail {
									detail := &textV2SearchCandidate{
										ordinal:    target,
										generation: norm.Generation,
										documentID: top.candidates[pos].documentID,
										score:      score,
									}
									for _, state := range states {
										if err := detail.addPostingValue(state.term, state.entries[state.entryIdx].value); err != nil {
											return false, false, err
										}
									}
									top.candidates[pos].detail = detail
								}
							}
							if stats != nil && thresholdChanged {
								stats.TextBlockMaxThresholds++
							}
						}
					}
				}
			}
		} else if stats != nil {
			stats.TextScalarPostingsRejected++
		}
	advanceEntries:
		for _, state := range states {
			state.entryIdx++
		}
	}
}

func executeTextV2PhraseLegacySearchAtSnapshot(
	c *Collection,
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	idx TextIndexDefinition,
	opts TextSearchOptions,
	phrase textSearchParsedPhrase,
	uniqueTerms []string,
	allowSet *textV2SearchOrdinalAllowSet,
	slop int,
	candidateLimit, maxPostingsScanned int,
	resultMode textSearchResultMode,
	response TextSearchResponse,
) (TextSearchResponse, error) {
	textSearchExplainSetServingPath(response.Explain, TextSearchExplainPathPhrase, false)
	phraseTermIndexes := textV2PhraseTermIndexes(phrase.terms, uniqueTerms)

	candidates := make(map[uint64]*textV2SearchCandidate)
	cache := textV2SearchBlockCache{allowed: allowSet}
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
			response.Stats.PostingsScanned = response.Stats.TextPostingsScanned
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
	var posKeyScratch []byte
	var posValScratch []byte
	for _, candidate := range candidates {
		norm, ok, err := cache.normEntry(snap, catalog, ctx, candidate.ordinal, &response.Stats)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if !ok || norm.tombstoned() {
			continue
		}
		docMap, ok, err := cache.identityEntry(snap, catalog, ctx, candidate.ordinal)
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
		matched, err := textV2CandidateMatchesPhraseAtSnapshot(snap, catalog, ctx, idx, candidate, uniqueTerms, phraseTermIndexes, phrase.gaps, positionScratch, &posKeyScratch, &posValScratch, slop, norm.Generation, &response.Stats)
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
		candidate.documentID = docMap.appendTo(candidate.documentID[:0])
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

func textV2StatesMatchPhraseAtSnapshot(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	idx TextIndexDefinition,
	states []*textV2ANDBlockMaxTermState,
	phraseTermIndexes []int,
	phraseGaps []int,
	positions []textV2PositionValue,
	keyScratch, valScratch *[]byte,
	slop int,
	ordinal, generation uint64,
	stats *TextSearchStats,
) (bool, error) {
	if len(states) == 0 || len(phraseTermIndexes) == 0 || len(positions) < len(states) {
		return false, nil
	}
	for i, state := range states {
		posting := state.entries[state.entryIdx].value
		if posting.generation != generation {
			return false, nil
		}
		value, err := readTextV2PhrasePositionValueAtSnapshot(snap, catalog, ctx, idx, ordinal, generation, state.term, posting, keyScratch, valScratch, stats)
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
	keyScratch, valScratch *[]byte,
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
		value, err := readTextV2PhrasePositionValueAtSnapshot(snap, catalog, ctx, idx, candidate.ordinal, generation, term, posting, keyScratch, valScratch, stats)
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

func appendTextV2PositionKey(dst []byte, ordinal uint64, term string) []byte {
	dst = append(dst[:0], textV2KeyVersion, textV2KeyKindPosition)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], ordinal)
	dst = append(dst, buf[:]...)
	return appendTextString(dst, term)
}

func readTextV2PhrasePositionValueAtSnapshot(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	idx TextIndexDefinition,
	ordinal, generation uint64,
	term string,
	posting textV2SearchPostingValue,
	keyScratch, valScratch *[]byte,
	stats *TextSearchStats,
) (textV2PositionValue, error) {
	if stats != nil {
		stats.TextPositionLookups++
	}
	var key []byte
	if keyScratch != nil {
		*keyScratch = appendTextV2PositionKey(*keyScratch, ordinal, term)
		key = *keyScratch
	} else {
		key = encodeTextV2PositionKey(ordinal, term)
	}
	var dst []byte
	if valScratch != nil {
		dst = (*valScratch)[:0]
	}
	raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, ctx.positionsRootName, key, dst)
	if valScratch != nil {
		*valScratch = raw
	}
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
