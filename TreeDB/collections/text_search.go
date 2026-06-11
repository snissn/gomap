package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// TextSearchOperator controls how analyzed query terms are combined. The zero
// value normalizes to OR.
type TextSearchOperator string

const (
	TextSearchOperatorOR  TextSearchOperator = "or"
	TextSearchOperatorAND TextSearchOperator = "and"
)

const (
	textSearchDefaultMinCandidateLimit = 1024
	textSearchDefaultMaxPostingsScan   = 1 << 20

	textSearchFailClosedCandidateLimit = "candidate_limit_exceeded"
	textSearchFailClosedPostingsLimit  = "postings_scan_limit_exceeded"
	textSearchFailClosedStorageCorrupt = "text_index_storage_corrupt"
	textSearchFailClosedDocumentFetch  = "document_fetch_unavailable"
)

const (
	textSearchBM25K1 = 1.2
	textSearchBM25B  = 0.75
)

// TextSearchOptions configures collection-native lexical search.
type TextSearchOptions struct {
	IndexName string
	Query     string
	Operator  TextSearchOperator
	TopK      int
	// CandidateLimit bounds the number of unique document candidates generated
	// from postings before scoring. The zero value uses an implementation default
	// derived from TopK. When the limit is exceeded, SearchText fails closed rather
	// than returning silently incomplete rankings.
	CandidateLimit int
	// MaxPostingsScanned bounds postings range-scan work. The zero value uses an
	// implementation default. When the limit is exceeded, SearchText fails closed.
	MaxPostingsScanned   int
	IncludeDocuments     bool
	DocumentFetchOptions DocumentFetchOptions
}

// TextSearchMatch carries matched field/term attribution for a lexical result.
type TextSearchMatch struct {
	Field string
	Terms []string
}

// TextSearchResult is one ranked lexical result. Rank is one-based in descending
// BM25F score order and DocumentID is response-owned.
type TextSearchResult struct {
	DocumentID    []byte
	IndexName     string
	Rank          int
	Score         float64
	ScoreKind     HybridScoreKind
	MatchedTerms  []string
	MatchedFields []string
	TextMatches   []TextSearchMatch
	Document      []byte
}

// TextSearchStats reports text-only search work. The Text* candidate counters
// intentionally mirror the hybrid-search counter vocabulary so #2503 can adapt
// text-only results without inventing new counter names. The shorter aliases are
// retained for text-only callers.
type TextSearchStats struct {
	QueryTerms                int
	TextCandidatesRequested   uint64
	TextCandidatesReturned    uint64
	TextPostingsScanned       uint64
	TextCandidatesScored      uint64
	PostingsScanned           uint64
	CandidatesScored          uint64
	DocumentsFetched          uint64
	DocumentsMissing          uint64
	FullDocumentScanFallbacks uint64
	PostingsScanNanos         uint64
	CandidateScoreNanos       uint64
	DocumentFetchNanos        uint64
	Truncated                 bool
	FailClosed                uint64
	FailClosedReason          string
	Unavailable               bool
	UnavailableReason         string
}

// TextSearchResponse contains ranked text results and diagnostics.
type TextSearchResponse struct {
	IndexName string
	Results   []TextSearchResult
	Stats     TextSearchStats
}

type textSearchCandidatePosting struct {
	term    string
	posting textSearchPostingValue
}

type textSearchCandidate struct {
	documentID string
	postings   [2]textSearchCandidatePosting
	overflow   []textSearchCandidatePosting
	postingsN  int
	score      float64
}

func (c *textSearchCandidate) postingCount() int {
	if c == nil {
		return 0
	}
	return c.postingsN
}

func (c *textSearchCandidate) postingForTerm(term string) (textSearchPostingValue, bool) {
	if c == nil {
		return textSearchPostingValue{}, false
	}
	for i := 0; i < c.postingsN; i++ {
		entry := c.postingAt(i)
		if entry.term == term {
			return entry.posting, true
		}
	}
	return textSearchPostingValue{}, false
}

func (c *textSearchCandidate) addPosting(term string, posting textSearchPostingValue) {
	for i := 0; i < c.postingsN; i++ {
		if c.postingAt(i).term == term {
			if i < len(c.postings) {
				c.postings[i].posting = posting
			} else {
				c.overflow[i-len(c.postings)].posting = posting
			}
			return
		}
	}
	if c.postingsN < len(c.postings) {
		c.postings[c.postingsN] = textSearchCandidatePosting{term: term, posting: posting}
	} else {
		c.overflow = append(c.overflow, textSearchCandidatePosting{term: term, posting: posting})
	}
	c.postingsN++
}

func (c *textSearchCandidate) postingAt(i int) textSearchCandidatePosting {
	if i < len(c.postings) {
		return c.postings[i]
	}
	return c.overflow[i-len(c.postings)]
}

type textSearchResultMode uint8

const (
	textSearchResultFull textSearchResultMode = iota
	textSearchResultTextMatchesOnly
)

// SearchText executes a bounded postings-backed lexical search for a declared
// collection text index. It never scans/ranks all collection documents.
func (c *Collection) SearchText(opts TextSearchOptions) (TextSearchResponse, error) {
	return c.searchText(opts, textSearchResultFull)
}

func (c *Collection) searchText(opts TextSearchOptions, resultMode textSearchResultMode) (TextSearchResponse, error) {
	var response TextSearchResponse
	if c == nil {
		return response, errCollectionNil
	}
	if c.db == nil {
		return response, errCollectionDBNil
	}
	if err := ValidateIndexName(opts.IndexName); err != nil {
		return response, err
	}
	if opts.TopK <= 0 {
		return response, errors.New("collections: text search TopK must be positive")
	}
	if opts.CandidateLimit < 0 {
		return response, errors.New("collections: text search CandidateLimit must be non-negative")
	}
	if opts.MaxPostingsScanned < 0 {
		return response, errors.New("collections: text search MaxPostingsScanned must be non-negative")
	}
	if _, err := normalizeTextSearchOperator(opts.Operator); err != nil {
		return response, err
	}
	if err := c.flushBufferedWrites(); err != nil {
		return response, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return response, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return response, err
	}
	if catalog == nil {
		return response, errCollectionNotFound
	}
	idx, ok := findTextIndex(catalog.meta.TextIndexes, opts.IndexName)
	if !ok {
		return response, ErrIndexNotFound
	}
	response.IndexName = idx.Name

	terms, operator, err := parseTextSearchQuery(idx.Analyzer, opts.Query, opts.Operator)
	if err != nil {
		return response, err
	}
	terms = uniqueTextSearchTerms(terms)
	response.Stats.QueryTerms = len(terms)
	if len(terms) == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	candidateLimit := normalizeTextSearchCandidateLimit(opts.TopK, opts.CandidateLimit)
	maxPostingsScanned := normalizeTextSearchMaxPostingsScanned(candidateLimit, len(terms), opts.MaxPostingsScanned)
	response.Stats.TextCandidatesRequested = uint64(candidateLimit)

	return executeTextSearchAtSnapshot(c, snap, catalog, idx, opts, terms, operator, candidateLimit, maxPostingsScanned, resultMode, response)
}

func executeTextSearchAtSnapshot(
	c *Collection,
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	idx TextIndexDefinition,
	opts TextSearchOptions,
	terms []string,
	operator TextSearchOperator,
	candidateLimit, maxPostingsScanned int,
	resultMode textSearchResultMode,
	response TextSearchResponse,
) (TextSearchResponse, error) {
	statsRootName := collectionTextStatsRootName(catalog.meta.Name, idx.Name)
	corpus, err := readTextStatsCorpusAtRoot(snap, catalog, statsRootName)
	if err != nil {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
	}
	termStats := make(map[string]textStatsTermValue, len(terms))
	for _, term := range terms {
		termValue, err := readTextStatsTermAtRoot(snap, catalog, statsRootName, term)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		termStats[term] = termValue
	}
	fieldStats := make(map[string]textStatsFieldValue, len(idx.Fields))
	fieldWeights := make(map[string]float64, len(idx.Fields))
	fieldNames := make([]string, 0, len(idx.Fields))
	for _, field := range idx.Fields {
		fieldNames = append(fieldNames, field.Field)
		statsValue, err := readTextStatsFieldAtRoot(snap, catalog, statsRootName, field.Field)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		fieldStats[field.Field] = statsValue
		weight := field.Weight
		if weight == 0 {
			weight = 1
		}
		fieldWeights[field.Field] = weight
	}

	candidates := make(map[string]*textSearchCandidate)
	scanTerms := orderTextSearchScanTerms(terms, operator, termStats)
	scanStart := time.Now()
	postingsRootName := collectionTextIndexRootName(catalog.meta.Name, idx.Name)
	for i, term := range scanTerms {
		allowNewCandidates := operator != TextSearchOperatorAND || i == 0
		truncated, err := scanTextSearchPostingsTerm(snap, catalog, postingsRootName, term, termStats[term], candidates, allowNewCandidates, candidateLimit, maxPostingsScanned, fieldNames, &response.Stats)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if truncated {
			response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
			return textSearchFailClosed(response, response.Stats.FailClosedReason, fmt.Errorf("%w: collection %q text index %q exceeded bounded candidate generation", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
		}
		if operator == TextSearchOperatorAND {
			pruneTextSearchANDCandidates(candidates, term)
			if len(candidates) == 0 {
				break
			}
		}
	}
	response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
	response.Stats.PostingsScanned = response.Stats.TextPostingsScanned

	if operator == TextSearchOperatorAND {
		for key, candidate := range candidates {
			if candidate.postingCount() != len(terms) {
				delete(candidates, key)
			}
		}
	}
	if len(candidates) == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}

	scoreStart := time.Now()
	scored := make([]*textSearchCandidate, 0, len(candidates))
	stateRootName := collectionTextStateRootName(catalog.meta.Name, idx.Name)
	var stateKeyScratch []byte
	var fieldLengthScratch []textDocumentFieldLength
	for _, candidate := range candidates {
		stateKeyScratch = appendTextStateKeyString(stateKeyScratch[:0], candidate.documentID)
		stateRaw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, stateRootName, stateKeyScratch, nil)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if !found {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("missing text-state for collection %q index %q document %q", catalog.meta.Name, idx.Name, candidate.documentID))
		}
		fieldLengthScratch, err = decodeTextDocumentStateFieldLengths(stateRaw, fieldLengthScratch[:0], fieldNames)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		score, err := scoreTextSearchCandidate(candidate, terms, corpus, termStats, fieldStats, fieldWeights, fieldLengthScratch)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		candidate.score = score
		scored = append(scored, candidate)
	}
	response.Stats.CandidateScoreNanos += uint64(time.Since(scoreStart).Nanoseconds())
	response.Stats.TextCandidatesScored = uint64(len(scored))
	response.Stats.CandidatesScored = response.Stats.TextCandidatesScored

	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].documentID < scored[j].documentID
	})
	if len(scored) > opts.TopK {
		scored = scored[:opts.TopK]
	}
	response.Stats.TextCandidatesReturned = uint64(len(scored))
	response.Results = make([]TextSearchResult, len(scored))
	for i, candidate := range scored {
		var matchedTerms, matchedFields []string
		var matches []TextSearchMatch
		if resultMode == textSearchResultTextMatchesOnly {
			matches = textSearchCandidateTextMatches(candidate)
		} else {
			matchedTerms, matchedFields, matches = textSearchCandidateMatches(candidate)
		}
		response.Results[i] = TextSearchResult{
			DocumentID:    []byte(candidate.documentID),
			IndexName:     idx.Name,
			Rank:          i + 1,
			Score:         candidate.score,
			ScoreKind:     HybridScoreKindBM25F,
			MatchedTerms:  matchedTerms,
			MatchedFields: matchedFields,
			TextMatches:   matches,
		}
	}

	if opts.IncludeDocuments && len(response.Results) > 0 {
		if err := fetchTextSearchResultDocuments(c, snap, catalog, opts, &response); err != nil {
			return textSearchFailClosed(response, textSearchFailClosedDocumentFetch, err)
		}
	}
	return response, nil
}

func pruneTextSearchANDCandidates(candidates map[string]*textSearchCandidate, term string) {
	for key, candidate := range candidates {
		if _, ok := candidate.postingForTerm(term); !ok {
			delete(candidates, key)
		}
	}
}

func scanTextSearchPostingsTerm(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	postingsRootName, term string,
	termStats textStatsTermValue,
	candidates map[string]*textSearchCandidate,
	allowNewCandidates bool,
	candidateLimit, maxPostingsScanned int,
	fieldNames []string,
	stats *TextSearchStats,
) (bool, error) {
	prefix := encodeTextPostingTermPrefix(term)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, postingsRootName, prefix, textSearchPrefixEnd(prefix), true)
	if err != nil {
		return false, err
	}
	if it == nil {
		if termStats.DocumentFrequency != 0 {
			return false, errMalformedTextStorage("text-stats term %q document frequency %d has no postings root", term, termStats.DocumentFrequency)
		}
		return false, nil
	}
	defer func() { _ = it.Close() }()
	var livePostingsForTerm uint64
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if maxPostingsScanned > 0 && stats.TextPostingsScanned >= uint64(maxPostingsScanned) {
			stats.Truncated = true
			stats.FailClosedReason = textSearchFailClosedPostingsLimit
			return true, nil
		}
		stats.TextPostingsScanned++
		if it.IsDeleted() {
			it.Next()
			continue
		}
		documentID, err := decodeTextPostingKeyDocumentIDForPrefix(key, prefix)
		if err != nil {
			return false, err
		}
		posting, err := decodeTextPostingValueForSearch(it.UnsafeValue(), fieldNames)
		if err != nil {
			return false, err
		}
		if termStats.DocumentFrequency == 0 {
			return false, errMalformedTextStorage("postings exist for term %q with zero document frequency", term)
		}
		livePostingsForTerm++
		keyString := string(documentID)
		candidate := candidates[keyString]
		if candidate == nil {
			if !allowNewCandidates {
				it.Next()
				continue
			}
			if candidateLimit > 0 && len(candidates) >= candidateLimit {
				stats.Truncated = true
				stats.FailClosedReason = textSearchFailClosedCandidateLimit
				return true, nil
			}
			candidate = &textSearchCandidate{documentID: keyString}
			candidates[keyString] = candidate
		}
		candidate.addPosting(term, posting)
		it.Next()
	}
	if err := it.Error(); err != nil {
		return false, err
	}
	if livePostingsForTerm != termStats.DocumentFrequency {
		return false, errMalformedTextStorage("text-stats term %q document frequency %d does not match live postings %d", term, termStats.DocumentFrequency, livePostingsForTerm)
	}
	return false, nil
}

func scoreTextSearchCandidate(candidate *textSearchCandidate, terms []string, corpus textStatsCorpusValue, termStats map[string]textStatsTermValue, fieldStats map[string]textStatsFieldValue, fieldWeights map[string]float64, fieldLengths []textDocumentFieldLength) (float64, error) {
	if corpus.DocumentCount == 0 {
		return 0, errMalformedTextStorage("text-stats corpus document count is zero with search candidates")
	}
	corpusDocuments := float64(corpus.DocumentCount)
	var score float64
	for _, term := range terms {
		posting, ok := candidate.postingForTerm(term)
		if !ok {
			continue
		}
		stats := termStats[term]
		if stats.DocumentFrequency == 0 || stats.DocumentFrequency > corpus.DocumentCount {
			return 0, errMalformedTextStorage("text-stats term %q document frequency %d outside corpus %d", term, stats.DocumentFrequency, corpus.DocumentCount)
		}
		df := float64(stats.DocumentFrequency)
		idf := math.Log(1 + (corpusDocuments-df+0.5)/(df+0.5))
		var combinedTF float64
		for fieldIdx := 0; fieldIdx < posting.fieldCount(); fieldIdx++ {
			fieldPosting := posting.fieldAt(fieldIdx)
			weight, ok := fieldWeights[fieldPosting.Field]
			if !ok {
				return 0, errMalformedTextStorage("posting references undeclared text field %q", fieldPosting.Field)
			}
			statsValue := fieldStats[fieldPosting.Field]
			if statsValue.DocumentCount == 0 || statsValue.TotalTokenCount == 0 {
				return 0, errMalformedTextStorage("missing text-stats field accounting for %q", fieldPosting.Field)
			}
			fieldLength, ok := textDocumentFieldLengthByName(fieldLengths, fieldPosting.Field)
			if !ok {
				return 0, errMalformedTextStorage("missing text-state field length for %q", fieldPosting.Field)
			}
			avgLength := float64(statsValue.TotalTokenCount) / float64(statsValue.DocumentCount)
			if avgLength <= 0 {
				return 0, errMalformedTextStorage("invalid average text field length for %q", fieldPosting.Field)
			}
			fieldTF := float64(fieldPosting.Frequency)
			if fieldTF <= 0 {
				continue
			}
			normalizedTF := fieldTF / (1 - textSearchBM25B + textSearchBM25B*(float64(fieldLength)/avgLength))
			combinedTF += weight * normalizedTF
		}
		if combinedTF > 0 {
			score += idf * ((combinedTF * (textSearchBM25K1 + 1)) / (combinedTF + textSearchBM25K1))
		}
	}
	return score, nil
}

func fetchTextSearchResultDocuments(c *Collection, snap *backenddb.Snapshot, catalog *collectionCatalog, opts TextSearchOptions, response *TextSearchResponse) error {
	ids := make([][]byte, len(response.Results))
	for i := range response.Results {
		ids[i] = response.Results[i].DocumentID
	}
	view := newCollectionReadViewAtSnapshot(c, snap, catalog, false, "")
	fetchStart := time.Now()
	fetch, err := view.FetchDocumentsByID(ids, opts.DocumentFetchOptions)
	response.Stats.DocumentFetchNanos += uint64(time.Since(fetchStart).Nanoseconds())
	closeErr := view.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	for i := range fetch.Results {
		if i >= len(response.Results) {
			break
		}
		if fetch.Results[i].Found {
			response.Results[i].Document = bytes.Clone(fetch.Results[i].Document)
			response.Stats.DocumentsFetched++
		} else {
			response.Stats.DocumentsMissing++
		}
	}
	return nil
}

type textSearchMatchPair struct {
	field string
	term  string
}

func textSearchCandidateTextMatches(candidate *textSearchCandidate) []TextSearchMatch {
	_, _, matches := textSearchCandidateMatchDetails(candidate, false)
	return matches
}

func textSearchCandidateMatches(candidate *textSearchCandidate) ([]string, []string, []TextSearchMatch) {
	return textSearchCandidateMatchDetails(candidate, true)
}

func textSearchCandidateMatchDetails(candidate *textSearchCandidate, includeLegacyLists bool) ([]string, []string, []TextSearchMatch) {
	var inline [8]textSearchMatchPair
	pairs := inline[:0]
	for postingIdx := 0; postingIdx < candidate.postingCount(); postingIdx++ {
		entry := candidate.postingAt(postingIdx)
		for fieldIdx := 0; fieldIdx < entry.posting.fieldCount(); fieldIdx++ {
			field := entry.posting.fieldAt(fieldIdx)
			if len(pairs) == cap(pairs) {
				grown := make([]textSearchMatchPair, len(pairs), len(pairs)*2)
				copy(grown, pairs)
				pairs = grown
			}
			pairs = append(pairs, textSearchMatchPair{field: field.Field, term: entry.term})
		}
	}
	if len(pairs) == 0 {
		return nil, nil, nil
	}
	sortTextSearchMatchPairs(pairs)
	unique := pairs[:0]
	for _, pair := range pairs {
		if len(unique) > 0 && unique[len(unique)-1] == pair {
			continue
		}
		unique = append(unique, pair)
	}

	matches := make([]TextSearchMatch, 0, len(unique))
	var matchedFields []string
	var matchedTerms []string
	if includeLegacyLists {
		matchedFields = make([]string, 0, len(unique))
		matchedTerms = make([]string, 0, len(unique))
	}
	for i := 0; i < len(unique); {
		field := unique[i].field
		j := i + 1
		for j < len(unique) && unique[j].field == field {
			j++
		}
		terms := make([]string, 0, j-i)
		for _, pair := range unique[i:j] {
			terms = append(terms, pair.term)
			if includeLegacyLists && !textSearchStringSliceContains(matchedTerms, pair.term) {
				matchedTerms = append(matchedTerms, pair.term)
			}
		}
		matches = append(matches, TextSearchMatch{Field: field, Terms: terms})
		if includeLegacyLists {
			matchedFields = append(matchedFields, field)
		}
		i = j
	}
	if includeLegacyLists {
		sort.Strings(matchedTerms)
	}
	return matchedTerms, matchedFields, matches
}

func sortTextSearchMatchPairs(pairs []textSearchMatchPair) {
	for i := 1; i < len(pairs); i++ {
		current := pairs[i]
		j := i - 1
		for j >= 0 && textSearchMatchPairLess(current, pairs[j]) {
			pairs[j+1] = pairs[j]
			j--
		}
		pairs[j+1] = current
	}
}

func textSearchMatchPairLess(a, b textSearchMatchPair) bool {
	if a.field != b.field {
		return a.field < b.field
	}
	return a.term < b.term
}

func textSearchStringSliceContains(values []string, value string) bool {
	for _, existing := range values {
		if existing == value {
			return true
		}
	}
	return false
}

func textSearchFailClosed(response TextSearchResponse, reason string, err error) (TextSearchResponse, error) {
	if reason == "" {
		reason = textSearchFailClosedStorageCorrupt
	}
	response.Stats.FailClosed = 1
	response.Stats.FailClosedReason = reason
	response.Stats.Unavailable = true
	response.Stats.UnavailableReason = reason
	if response.Stats.PostingsScanned == 0 {
		response.Stats.PostingsScanned = response.Stats.TextPostingsScanned
	}
	if response.Stats.CandidatesScored == 0 {
		response.Stats.CandidatesScored = response.Stats.TextCandidatesScored
	}
	if err == nil {
		return response, fmt.Errorf("%w: %s", ErrTextIndexUnavailable, reason)
	}
	if errors.Is(err, ErrTextIndexUnavailable) {
		return response, err
	}
	return response, fmt.Errorf("%w: %w", ErrTextIndexUnavailable, err)
}

func normalizeTextSearchCandidateLimit(topK, requested int) int {
	if requested > 0 {
		return requested
	}
	limit := topK * 64
	if limit < textSearchDefaultMinCandidateLimit {
		limit = textSearchDefaultMinCandidateLimit
	}
	return limit
}

func normalizeTextSearchMaxPostingsScanned(candidateLimit, termCount, requested int) int {
	if requested > 0 {
		return requested
	}
	limit := candidateLimit * maxIntForTextSearch(termCount, 1) * 4
	if limit < textSearchDefaultMinCandidateLimit {
		limit = textSearchDefaultMinCandidateLimit
	}
	if limit > textSearchDefaultMaxPostingsScan {
		limit = textSearchDefaultMaxPostingsScan
	}
	return limit
}

func maxIntForTextSearch(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func uniqueTextSearchTerms(terms []string) []string {
	if len(terms) <= 1 {
		return terms
	}
	seen := make(map[string]struct{}, len(terms))
	out := make([]string, 0, len(terms))
	for _, term := range terms {
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		out = append(out, term)
	}
	return out
}

func orderTextSearchScanTerms(terms []string, operator TextSearchOperator, stats map[string]textStatsTermValue) []string {
	out := append([]string(nil), terms...)
	if operator == TextSearchOperatorAND {
		sort.SliceStable(out, func(i, j int) bool {
			left := stats[out[i]].DocumentFrequency
			right := stats[out[j]].DocumentFrequency
			if left != right {
				return left < right
			}
			return out[i] < out[j]
		})
	}
	return out
}

func textSearchPrefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	out := bytes.Clone(prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func normalizeTextSearchOperator(op TextSearchOperator) (TextSearchOperator, error) {
	switch op {
	case "", TextSearchOperatorOR:
		return TextSearchOperatorOR, nil
	case TextSearchOperatorAND:
		return TextSearchOperatorAND, nil
	default:
		return "", fmt.Errorf("collections: unsupported text search operator %q", op)
	}
}

func parseTextSearchQuery(analyzer TextAnalyzer, query string, requested TextSearchOperator) ([]string, TextSearchOperator, error) {
	if strings.ContainsAny(query, "\"()") {
		return nil, "", errors.New("collections: unsupported text query syntax: phrase and grouped queries are not implemented")
	}
	operator := requested
	var explicit TextSearchOperator
	parts := strings.Fields(query)
	terms := make([]string, 0, len(parts))
	expectTerm := true
	for _, part := range parts {
		switch {
		case strings.EqualFold(part, "AND"):
			if expectTerm {
				return nil, "", errors.New("collections: malformed text query: dangling AND")
			}
			if explicit != "" && explicit != TextSearchOperatorAND {
				return nil, "", errors.New("collections: mixed AND/OR text queries are not supported")
			}
			explicit = TextSearchOperatorAND
			expectTerm = true
			continue
		case strings.EqualFold(part, "OR"):
			if expectTerm {
				return nil, "", errors.New("collections: malformed text query: dangling OR")
			}
			if explicit != "" && explicit != TextSearchOperatorOR {
				return nil, "", errors.New("collections: mixed AND/OR text queries are not supported")
			}
			explicit = TextSearchOperatorOR
			expectTerm = true
			continue
		}
		tokens, err := AnalyzeText(analyzer, part)
		if err != nil {
			return nil, "", err
		}
		if len(tokens) == 0 {
			continue
		}
		for _, token := range tokens {
			terms = append(terms, token.Term)
		}
		expectTerm = false
	}
	if expectTerm && len(terms) > 0 {
		return nil, "", errors.New("collections: malformed text query: dangling operator")
	}
	if explicit != "" {
		if requested != "" && requested != explicit {
			return nil, "", fmt.Errorf("collections: text query operator %q conflicts with requested operator %q", explicit, requested)
		}
		operator = explicit
	}
	if operator == "" {
		operator = TextSearchOperatorOR
	}
	return terms, operator, nil
}
