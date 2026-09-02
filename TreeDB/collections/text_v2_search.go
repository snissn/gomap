package collections

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

type textV2SearchContext struct {
	collectionName        string
	indexName             string
	docIDRootName         string
	termsRootName         string
	postingBlocksRootName string
	normBlocksRootName    string
	positionsRootName     string
	docMapRootName        string
	status                textV2IndexStatusValue
	corpus                textV2CorpusStatsValue
	termStats             map[string]textV2TermStatsValue
	fieldStats            []textV2FieldStatsValue
	fieldWeights          []float64
	fieldNames            []string
}

type textV2SearchCandidatePosting struct {
	term  string
	value textV2SearchPostingValue
}

type textV2SearchPostingValue struct {
	generation    uint64
	termFrequency uint32
	fieldCount    int
	inlineFields  [4]uint32
	fields        []uint32
}

func (p textV2SearchPostingValue) fieldFrequency(i int) uint32 {
	if i < 0 || i >= p.fieldCount {
		return 0
	}
	if p.fieldCount <= len(p.inlineFields) {
		return p.inlineFields[i]
	}
	return p.fields[i]
}

type textV2SearchCandidate struct {
	ordinal    uint64
	generation uint64
	documentID []byte
	postings   [2]textV2SearchCandidatePosting
	overflow   []textV2SearchCandidatePosting
	postingsN  int
	score      float64
}

func (c *textV2SearchCandidate) postingCount() int {
	if c == nil {
		return 0
	}
	return c.postingsN
}

func (c *textV2SearchCandidate) postingAt(i int) textV2SearchCandidatePosting {
	if i < len(c.postings) {
		return c.postings[i]
	}
	return c.overflow[i-len(c.postings)]
}

func (c *textV2SearchCandidate) postingForTerm(term string) (textV2SearchPostingValue, bool) {
	if c == nil {
		return textV2SearchPostingValue{}, false
	}
	for i := 0; i < c.postingsN; i++ {
		posting := c.postingAt(i)
		if posting.term == term {
			return posting.value, true
		}
	}
	return textV2SearchPostingValue{}, false
}

func (c *textV2SearchCandidate) addPosting(term string, entry textV2PostingBlockEntry, fieldCount int) error {
	value, err := textV2SearchPostingValueFromEntry(entry, fieldCount)
	if err != nil {
		return err
	}
	return c.addPostingValue(term, value)
}

func (c *textV2SearchCandidate) addPostingValue(term string, value textV2SearchPostingValue) error {
	if c == nil {
		return nil
	}
	for i := 0; i < c.postingsN; i++ {
		posting := c.postingAt(i)
		if posting.term != term {
			continue
		}
		if posting.value.generation == value.generation {
			return errMalformedTextStorage("duplicate text-v2 posting for term %q ordinal %d generation %d", term, c.ordinal, value.generation)
		}
		if value.generation > posting.value.generation {
			if i < len(c.postings) {
				c.postings[i].value = value
			} else {
				c.overflow[i-len(c.postings)].value = value
			}
		}
		return nil
	}
	posting := textV2SearchCandidatePosting{term: term, value: value}
	if c.postingsN < len(c.postings) {
		c.postings[c.postingsN] = posting
	} else {
		c.overflow = append(c.overflow, posting)
	}
	c.postingsN++
	return nil
}

type textV2SearchBlockCache struct {
	normBlocks   map[uint64]textV2SearchNormBlock
	docMapBlocks map[uint64]textV2SearchDocMapBlock
}

type textV2SearchNormBlock struct {
	BlockStart   uint64
	BlockSize    uint32
	FieldCount   uint32
	Entries      []textV2SearchNormEntry
	FieldLengths []uint32
}

type textV2SearchNormEntry struct {
	Ordinal      uint64
	Generation   uint64
	Flags        byte
	FieldLengths []uint32
}

func (e textV2SearchNormEntry) tombstoned() bool { return e.Flags&textV2DocFlagTombstone != 0 }

type textV2SearchDocMapBlock struct {
	BlockStart uint64
	BlockSize  uint32
	Entries    []textV2SearchDocMapEntry
}

type textV2SearchDocMapEntry struct {
	Ordinal    uint64
	Generation uint64
	Flags      byte
	DocumentID []byte
}

func (e textV2SearchDocMapEntry) tombstoned() bool { return e.Flags&textV2DocFlagTombstone != 0 }

type textV2SearchOrdinalAllowSet struct {
	ordinals map[uint64]struct{}
	sorted   []uint64
	count    int
	all      bool
}

func (s *textV2SearchOrdinalAllowSet) empty() bool {
	return s != nil && s.count == 0
}

func (s *textV2SearchOrdinalAllowSet) size() int {
	if s == nil {
		return 0
	}
	return s.count
}

func (s *textV2SearchOrdinalAllowSet) contains(ordinal uint64) bool {
	if s == nil || s.all {
		return true
	}
	_, ok := s.ordinals[ordinal]
	return ok
}

func (s *textV2SearchOrdinalAllowSet) intersects(first, last uint64) bool {
	_, _, ok := s.rangeBounds(first, last)
	return ok
}

func (s *textV2SearchOrdinalAllowSet) rangeBounds(first, last uint64) (int, int, bool) {
	if first > last {
		return 0, 0, false
	}
	if s == nil || s.all {
		return 0, 0, true
	}
	if len(s.sorted) == 0 {
		return 0, 0, false
	}
	start := sort.Search(len(s.sorted), func(i int) bool { return s.sorted[i] >= first })
	if start >= len(s.sorted) || s.sorted[start] > last {
		return 0, 0, false
	}
	end := sort.Search(len(s.sorted), func(i int) bool { return s.sorted[i] > last })
	return start, end, true
}

type textV2SearchTopCandidate struct {
	ordinal    uint64
	generation uint64
	documentID []byte
	score      float64
	term       string
	posting    textV2SearchPostingValue
	hasPosting bool
	detail     *textV2SearchCandidate
}

type textV2SearchTopK struct {
	limit      int
	candidates []textV2SearchTopCandidate
}

func (t *textV2SearchTopK) threshold() (float64, bool) {
	if t == nil || t.limit <= 0 || len(t.candidates) < t.limit {
		return 0, false
	}
	return t.candidates[len(t.candidates)-1].score, true
}

func (t *textV2SearchTopK) worst() (textV2SearchTopCandidate, bool) {
	if t == nil || t.limit <= 0 || len(t.candidates) < t.limit {
		return textV2SearchTopCandidate{}, false
	}
	return t.candidates[len(t.candidates)-1], true
}

func (t *textV2SearchTopK) needsDocumentIDForScore(score float64) bool {
	if t == nil || t.limit <= 0 {
		return false
	}
	if len(t.candidates) < t.limit {
		return true
	}
	worst := t.candidates[len(t.candidates)-1].score
	return score >= worst
}

func (t *textV2SearchTopK) add(candidate textV2SearchTopCandidate) bool {
	_, _, thresholdChanged := t.addCandidate(candidate)
	return thresholdChanged
}

func (t *textV2SearchTopK) addCandidate(candidate textV2SearchTopCandidate) (int, bool, bool) {
	if t == nil || t.limit <= 0 {
		return -1, false, false
	}
	beforeThreshold, beforeReady := t.threshold()
	if len(t.candidates) == t.limit && !textV2TopCandidateLess(candidate, t.candidates[len(t.candidates)-1]) {
		return -1, false, false
	}
	pos := sort.Search(len(t.candidates), func(i int) bool { return textV2TopCandidateLess(candidate, t.candidates[i]) })
	if len(t.candidates) < t.limit {
		t.candidates = append(t.candidates, textV2SearchTopCandidate{})
		copy(t.candidates[pos+1:], t.candidates[pos:])
		t.candidates[pos] = candidate
	} else {
		copy(t.candidates[pos+1:], t.candidates[pos:len(t.candidates)-1])
		t.candidates[pos] = candidate
	}
	afterThreshold, afterReady := t.threshold()
	return pos, true, afterReady && (!beforeReady || afterThreshold > beforeThreshold)
}

func textV2TopCandidateLess(a, b textV2SearchTopCandidate) bool {
	if a.score != b.score {
		return a.score > b.score
	}
	return bytes.Compare(a.documentID, b.documentID) < 0
}

func executeTextV2SearchAtSnapshot(
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
) (ret TextSearchResponse, err error) {
	if response.Explain != nil {
		defer func() { textSearchExplainFinish(ret.Explain, ret.Stats) }()
	}
	ctx, err := newTextV2SearchContext(snap, catalog, idx, terms)
	if err != nil {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
	}
	textSearchExplainBindV2Context(response.Explain, ctx, terms, nil)
	if ctx.corpus.DocumentCount == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	allowSet, err := newTextV2SearchOrdinalAllowSet(snap, catalog, ctx, opts.textV2AllowedDocumentIDs)
	if err != nil {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
	}
	textSearchExplainBindV2Context(response.Explain, ctx, terms, allowSet)
	if allowSet != nil {
		response.Stats.TextScalarPrefilterIDs = uint64(allowSet.size())
	}
	if allowSet.empty() {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	if !opts.textV2DisableBlockMax && operator == TextSearchOperatorOR && len(terms) == 1 {
		return executeTextV2BlockMaxSearchAtSnapshot(c, snap, catalog, ctx, idx, opts, terms[0], allowSet, candidateLimit, maxPostingsScanned, resultMode, response)
	}
	if !opts.textV2DisableBlockMax && operator == TextSearchOperatorOR && len(terms) > 1 {
		return executeTextV2ORBlockMaxSearchAtSnapshot(c, snap, catalog, ctx, idx, opts, terms, allowSet, candidateLimit, maxPostingsScanned, resultMode, response)
	}
	if !opts.textV2DisableBlockMax && operator == TextSearchOperatorAND && len(terms) > 1 {
		return executeTextV2ANDBlockMaxSearchAtSnapshot(c, snap, catalog, ctx, idx, opts, terms, allowSet, candidateLimit, maxPostingsScanned, resultMode, response)
	}
	if !opts.textV2DisableBlockMax {
		response.Stats.TextBlockMaxFallbacks++
		textSearchExplainAddFallback(response.Explain, "blockmax_unsupported_shape")
	}
	textSearchExplainSetServingPath(response.Explain, TextSearchExplainPathExactPostings, false)

	candidates := make(map[uint64]*textV2SearchCandidate)
	cache := textV2SearchBlockCache{}
	scanTerms := orderTextV2SearchScanTerms(terms, operator, ctx.termStats)
	scanStart := time.Now()
	for i, term := range scanTerms {
		allowNewCandidates := operator != TextSearchOperatorAND || i == 0
		truncated, err := scanTextV2SearchPostingBlocksTerm(snap, catalog, ctx, &cache, term, candidates, allowNewCandidates, candidateLimit, maxPostingsScanned, allowSet, &response.Stats)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if truncated {
			response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
			return textSearchFailClosed(response, response.Stats.FailClosedReason, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded candidate generation", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
		}
		if operator == TextSearchOperatorAND {
			pruneTextV2SearchANDCandidates(candidates, term)
			if len(candidates) == 0 {
				break
			}
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
	for _, candidate := range candidates {
		norm, ok, err := cache.normEntry(snap, catalog, ctx, candidate.ordinal, &response.Stats)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if !ok {
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
		if norm.tombstoned() || docMap.tombstoned() {
			continue
		}
		currentPostings := countTextV2CurrentPostings(candidate, terms, norm.Generation)
		if operator == TextSearchOperatorAND && currentPostings != len(terms) {
			continue
		}
		if currentPostings == 0 {
			continue
		}
		score, err := scoreTextV2SearchCandidate(candidate, terms, ctx, norm)
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
		id := append([]byte(nil), candidate.documentID...)
		result := TextSearchResult{
			DocumentID: id,
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
			if err := textSearchExplainAppendV2CandidateScore(response.Explain, result, candidate, terms, ctx, norm); err != nil {
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

func newTextV2SearchContext(snap *backenddb.Snapshot, catalog *collectionCatalog, idx TextIndexDefinition, terms []string) (*textV2SearchContext, error) {
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	fieldNames := textV2FieldNames(idx)
	for _, rootName := range collectionTextV2RootNames(catalog.meta.Name, idx.Name) {
		family, ok := textV2RootFamilyForName(catalog.meta.Name, idx.Name, rootName)
		if !ok {
			return nil, errMalformedTextStorage("unknown text-v2 root %q", rootName)
		}
		if err := validateTextV2SearchRootFormat(snap, catalog, rootName, family, fieldNames); err != nil {
			return nil, err
		}
	}
	generationsRootName := collectionTextV2GenerationsRootName(catalog.meta.Name, idx.Name)
	status, ok, err := readTextV2StatusAtRoot(snap, catalog, generationsRootName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errMalformedTextStorage("missing text-v2 status for collection %q index %q", catalog.meta.Name, idx.Name)
	}
	termsRootName := collectionTextV2TermsRootName(catalog.meta.Name, idx.Name)
	corpus, err := readTextV2CorpusStatsAtRoot(snap, catalog, termsRootName)
	if err != nil {
		return nil, err
	}
	if corpus.StatsGeneration != status.StatsGeneration || corpus.DocumentCount != status.LiveDocuments {
		return nil, errMalformedTextStorage("text-v2 corpus stats/status mismatch")
	}
	ctx := &textV2SearchContext{
		collectionName:        catalog.meta.Name,
		indexName:             idx.Name,
		docIDRootName:         collectionTextV2DocIDRootName(catalog.meta.Name, idx.Name),
		termsRootName:         termsRootName,
		postingBlocksRootName: collectionTextV2PostingBlocksRootName(catalog.meta.Name, idx.Name),
		normBlocksRootName:    collectionTextV2NormBlocksRootName(catalog.meta.Name, idx.Name),
		positionsRootName:     collectionTextV2PositionsRootName(catalog.meta.Name, idx.Name),
		docMapRootName:        collectionTextV2DocMapRootName(catalog.meta.Name, idx.Name),
		status:                status,
		corpus:                corpus,
		termStats:             make(map[string]textV2TermStatsValue, len(terms)),
		fieldStats:            make([]textV2FieldStatsValue, 0, len(idx.Fields)),
		fieldWeights:          make([]float64, 0, len(idx.Fields)),
		fieldNames:            fieldNames,
	}
	for _, field := range idx.Fields {
		stats, err := readTextV2FieldStatsAtRoot(snap, catalog, termsRootName, field.Field)
		if err != nil {
			return nil, err
		}
		if stats.StatsGeneration != status.StatsGeneration || stats.DocumentCount > status.LiveDocuments {
			return nil, errMalformedTextStorage("text-v2 field stats/status mismatch for %q", field.Field)
		}
		ctx.fieldStats = append(ctx.fieldStats, stats)
		weight := field.Weight
		if weight == 0 {
			weight = 1
		}
		ctx.fieldWeights = append(ctx.fieldWeights, weight)
	}
	for _, term := range terms {
		stats, err := readTextV2TermStatsAtRoot(snap, catalog, termsRootName, term)
		if err != nil {
			return nil, err
		}
		if stats.StatsGeneration > status.TermGeneration {
			return nil, errMalformedTextStorage("text-v2 term %q stats generation exceeds status", term)
		}
		if stats.DocumentFrequency > status.LiveDocuments || stats.DocumentFrequency > corpus.DocumentCount {
			return nil, errMalformedTextStorage("text-v2 term %q document frequency outside corpus", term)
		}
		if stats.DocumentFrequency == 0 && stats.TotalTermFrequency != 0 {
			return nil, errMalformedTextStorage("text-v2 term %q has zero df and non-zero tf", term)
		}
		if stats.DocumentFrequency != 0 && stats.TotalTermFrequency == 0 {
			return nil, errMalformedTextStorage("text-v2 term %q has non-zero df and zero tf", term)
		}
		ctx.termStats[term] = stats
	}
	return ctx, nil
}

func validateTextV2SearchRootFormat(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, family textV2RootFamily, fieldNames []string) error {
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextV2FormatKey(), nil)
	if err != nil {
		return err
	}
	if !ok {
		return errMalformedTextStorage("missing text-v2 root %q format record", rootName)
	}
	format, err := decodeTextV2RootFormatValue(raw)
	if err != nil {
		return err
	}
	if format.Family != family {
		return errMalformedTextStorage("text-v2 root %q format family=%s want %s", rootName, format.Family, family)
	}
	if format.DocMapBlockSize != textV2DefaultDocMapBlockSize || format.NormBlockSize != textV2DefaultNormBlockSize {
		return errMalformedTextStorage("text-v2 root %q format block sizes mismatch", rootName)
	}
	if !slices.Equal(format.Fields, fieldNames) {
		return errMalformedTextStorage("text-v2 root %q format fields mismatch", rootName)
	}
	return nil
}

func newTextV2SearchOrdinalAllowSet(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, allowedIDs hybridScalarAllowSet) (*textV2SearchOrdinalAllowSet, error) {
	if allowedIDs == nil {
		return nil, nil
	}
	ids := make([]string, 0, len(allowedIDs))
	for id := range allowedIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	set := &textV2SearchOrdinalAllowSet{ordinals: make(map[uint64]struct{}, len(ids)), sorted: make([]uint64, 0, len(ids))}
	for _, id := range ids {
		documentID := []byte(id)
		doc, ok, err := readTextV2DocIDAtRoot(snap, catalog, ctx.docIDRootName, documentID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errMalformedTextStorage("missing text-v2 docid mapping for scalar-filtered document %q", id)
		}
		if doc.Ordinal >= ctx.status.NextOrdinal || doc.Generation > ctx.status.DocMapGeneration || doc.tombstoned() {
			return nil, errMalformedTextStorage("text-v2 docid mapping for scalar-filtered document %q outside status snapshot", id)
		}
		if _, exists := set.ordinals[doc.Ordinal]; !exists {
			set.ordinals[doc.Ordinal] = struct{}{}
			set.sorted = append(set.sorted, doc.Ordinal)
		}
	}
	slices.Sort(set.sorted)
	set.count = len(set.sorted)
	set.all = uint64(set.count) == ctx.status.LiveDocuments
	return set, nil
}

func executeTextV2BlockMaxSearchAtSnapshot(
	c *Collection,
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	idx TextIndexDefinition,
	opts TextSearchOptions,
	term string,
	allowSet *textV2SearchOrdinalAllowSet,
	candidateLimit, maxPostingsScanned int,
	resultMode textSearchResultMode,
	response TextSearchResponse,
) (TextSearchResponse, error) {
	textSearchExplainSetServingPath(response.Explain, TextSearchExplainPathBlockMaxSingle, true)
	termStats := ctx.termStats[term]
	if termStats.DocumentFrequency == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	if termStats.PostingBlockCount == 0 {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 term %q document frequency %d has no posting blocks", term, termStats.DocumentFrequency))
	}
	prefix := encodeTextV2PostingBlockTermPrefix(term)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, ctx.postingBlocksRootName, prefix, textSearchPrefixEnd(prefix), true)
	if err != nil {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
	}
	if it == nil {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 term %q has posting block count %d but missing posting root", term, termStats.PostingBlockCount))
	}
	defer func() { _ = it.Close() }()

	baseStats := response.Stats
	fallbackToExactScan := func() (TextSearchResponse, error) {
		fallback := response
		fallback.Stats = baseStats
		fallback.Stats.TextBlockMaxFallbacks++
		textSearchExplainAddFallback(fallback.Explain, "blockmax_single_unsealed_or_overlapping_block")
		fallbackOpts := opts
		fallbackOpts.textV2DisableBlockMax = true
		return executeTextV2SearchAtSnapshot(c, snap, catalog, idx, fallbackOpts, []string{term}, TextSearchOperatorOR, candidateLimit, maxPostingsScanned, resultMode, fallback)
	}
	cache := textV2SearchBlockCache{}
	top := textV2SearchTopK{limit: opts.TopK, candidates: make([]textV2SearchTopCandidate, 0, opts.TopK)}
	fieldCount := len(ctx.fieldNames)
	var scratch []uint32
	var scannerStorage textV2PostingBlockEntryScanner
	var blocksSeen uint64
	var lastBlockLast uint64
	var hasLastBlock bool
	scanStart := time.Now()
	for it.Valid() {
		keyBytes := it.UnsafeKey()
		if !bytes.HasPrefix(keyBytes, prefix) {
			break
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		keyBlockStart, keyBlockID, err := decodeTextV2PostingBlockKeySuffixForPrefix(keyBytes, prefix)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		scanner, err := initTextV2PostingBlockEntryScanner(&scannerStorage, it.UnsafeValue(), scratch)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if scanner.block.BlockStart != keyBlockStart || scanner.block.BlockID != keyBlockID {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 posting block key/value identity mismatch"))
		}
		if len(scanner.block.Summary.MaxFieldTermFrequencies) != fieldCount {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 posting block field count %d want %d", len(scanner.block.Summary.MaxFieldTermFrequencies), fieldCount))
		}
		if !scanner.ChecksumVerified() || !textV2PostingBlockKindSupportsBlockMax(scanner.block.Kind) || (hasLastBlock && scanner.block.Summary.FirstOrdinal <= lastBlockLast) {
			return fallbackToExactScan()
		}
		hasLastBlock = true
		lastBlockLast = scanner.block.Summary.LastOrdinal
		blocksSeen++
		if allowSet != nil && !allowSet.intersects(scanner.block.Summary.FirstOrdinal, scanner.block.Summary.LastOrdinal) {
			response.Stats.TextPostingBlocksSkipped++
			response.Stats.TextScalarPostingBlocksSkipped++
			it.Next()
			continue
		}
		blockUpperBound, err := textV2SearchBlockUpperBound(term, scanner.block.Summary, ctx)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if threshold, ok := top.threshold(); ok {
			if blockUpperBound < threshold {
				response.Stats.TextPostingBlocksSkipped++
				it.Next()
				continue
			}
			tightUpperBound, err := textV2SearchTightBlockUpperBound(snap, catalog, ctx, &cache, allowSet, term, scanner.block.Summary, &response.Stats)
			if err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			canSkip, err := textV2SearchTightUpperBoundSkipsRange(snap, catalog, ctx, &cache, allowSet, tightUpperBound, scanner.block.Summary.FirstOrdinal, scanner.block.Summary.LastOrdinal, &top)
			if err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if canSkip {
				response.Stats.TextPostingBlocksSkipped++
				it.Next()
				continue
			}
		}
		response.Stats.TextPostingBlocksVisited++
		var entry textV2PostingBlockEntry
		for scanner.remaining > 0 {
			if maxPostingsScanned > 0 && response.Stats.TextPostingsScanned >= uint64(maxPostingsScanned) {
				response.Stats.Truncated = true
				response.Stats.FailClosedReason = textSearchFailClosedPostingsLimit
				response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
				return textSearchFailClosed(response, response.Stats.FailClosedReason, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded candidate generation", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
			}
			if !scanner.Next(&entry) {
				break
			}
			response.Stats.TextPostingsScanned++
			if entry.Ordinal >= ctx.status.NextOrdinal || entry.Generation > ctx.status.RootGeneration {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 posting entry outside status snapshot"))
			}
			if !allowSet.contains(entry.Ordinal) {
				response.Stats.TextScalarPostingsRejected++
				continue
			}
			norm, ok, err := cache.normEntry(snap, catalog, ctx, entry.Ordinal, &response.Stats)
			if err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if !ok || norm.tombstoned() || norm.Generation != entry.Generation {
				continue
			}
			if candidateLimit > 0 && response.Stats.TextCandidatesScored >= uint64(candidateLimit) {
				response.Stats.Truncated = true
				response.Stats.FailClosedReason = textSearchFailClosedCandidateLimit
				response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
				return textSearchFailClosed(response, response.Stats.FailClosedReason, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded candidate generation", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
			}
			posting, err := textV2SearchPostingValueFromEntry(entry, fieldCount)
			if err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			score, err := scoreTextV2SearchPostingValue(term, posting, ctx, norm)
			if err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			response.Stats.TextCandidatesScored++
			if !top.needsDocumentIDForScore(score) {
				continue
			}
			docMap, ok, err := cache.docMapEntry(snap, catalog, ctx, entry.Ordinal)
			if err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if !ok {
				continue
			}
			if norm.Generation != docMap.Generation || norm.Flags != docMap.Flags {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 norm/docmap generation mismatch for ordinal %d", entry.Ordinal))
			}
			if docMap.tombstoned() {
				continue
			}
			beforeThreshold, beforeReady := top.threshold()
			topCandidate := textV2SearchTopCandidate{ordinal: entry.Ordinal, generation: entry.Generation, documentID: docMap.DocumentID, score: score}
			if resultMode != textSearchResultScoreOnly || response.Explain != nil {
				topCandidate.term = term
				topCandidate.posting = posting
				topCandidate.hasPosting = true
			}
			top.add(topCandidate)
			afterThreshold, afterReady := top.threshold()
			if afterReady && (!beforeReady || afterThreshold > beforeThreshold) {
				response.Stats.TextBlockMaxThresholds++
			}
		}
		if err := scanner.Err(); err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		scratch = scanner.scratch
		it.Next()
	}
	if err := it.Error(); err != nil {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
	}
	if blocksSeen != termStats.PostingBlockCount {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 term %q posting block count %d does not match scanned blocks %d", term, termStats.PostingBlockCount, blocksSeen))
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
		if err := populateTextV2SearchResultMatchesFromTopCandidate(snap, catalog, ctx, idx, candidate, resultMode, &result, &response.Stats); err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if response.Explain != nil {
			norm, ok, err := cache.normEntry(snap, catalog, ctx, candidate.ordinal, &response.Stats)
			if err != nil || !ok {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if err := textSearchExplainAppendV2TopCandidateScore(response.Explain, result, candidate, ctx, norm); err != nil {
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

type textV2ANDBlockMaxTermState struct {
	term             string
	termStats        textV2TermStatsValue
	prefix           []byte
	it               iterator.UnsafeIterator
	scratch          []uint32
	block            textV2PostingBlockValue
	scanner          *textV2PostingBlockEntryScanner
	scannerStorage   textV2PostingBlockEntryScanner
	upperBound       float64
	upperBoundTight  bool
	entries          []textV2ANDBlockMaxPostingEntry
	entryIdx         int
	decoded          bool
	exhausted        bool
	requiresFallback bool
	blocksSeen       uint64
	lastBlockLast    uint64
	hasLastBlock     bool
}

type textV2ANDBlockMaxPostingEntry struct {
	ordinal uint64
	value   textV2SearchPostingValue
}

func executeTextV2ANDBlockMaxSearchAtSnapshot(
	c *Collection,
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	idx TextIndexDefinition,
	opts TextSearchOptions,
	terms []string,
	allowSet *textV2SearchOrdinalAllowSet,
	candidateLimit, maxPostingsScanned int,
	resultMode textSearchResultMode,
	response TextSearchResponse,
) (TextSearchResponse, error) {
	textSearchExplainSetServingPath(response.Explain, TextSearchExplainPathBlockMaxAND, true)
	for _, term := range terms {
		termStats := ctx.termStats[term]
		if termStats.DocumentFrequency == 0 {
			response.Results = []TextSearchResult{}
			return response, nil
		}
		if termStats.PostingBlockCount == 0 {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 term %q document frequency %d has no posting blocks", term, termStats.DocumentFrequency))
		}
	}

	baseStats := response.Stats
	fallback := func() (TextSearchResponse, error) {
		fallbackResponse := response
		fallbackResponse.Stats = baseStats
		fallbackResponse.Stats.TextBlockMaxFallbacks++
		textSearchExplainAddFallback(fallbackResponse.Explain, "blockmax_and_unsealed_stale_or_overlapping_block")
		fallbackOpts := opts
		fallbackOpts.textV2DisableBlockMax = true
		return executeTextV2SearchAtSnapshot(c, snap, catalog, idx, fallbackOpts, terms, TextSearchOperatorAND, candidateLimit, maxPostingsScanned, resultMode, fallbackResponse)
	}

	fieldCount := len(ctx.fieldNames)
	scanTerms := orderTextV2SearchScanTerms(terms, TextSearchOperatorAND, ctx.termStats)
	states := make([]*textV2ANDBlockMaxTermState, 0, len(scanTerms))
	for _, term := range scanTerms {
		state, err := newTextV2ANDBlockMaxTermState(snap, catalog, ctx, term, fieldCount)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		states = append(states, state)
	}
	defer func() {
		for _, state := range states {
			_ = state.close()
		}
	}()

	cache := textV2SearchBlockCache{}
	top := textV2SearchTopK{limit: opts.TopK, candidates: make([]textV2SearchTopCandidate, 0, opts.TopK)}
	seenCurrent := make(map[uint64]uint64)
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
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 AND block-max failed to advance non-overlapping blocks"))
			}
			continue
		}

		if allowSet != nil && !allowSet.intersects(first, last) {
			if err := skipTextV2ANDBlockMaxOverlapThrough(ctx, states, fieldCount, last, &response.Stats, true); err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			continue
		}

		combinedUpperBound := 0.0
		for _, state := range states {
			combinedUpperBound += state.upperBound
		}
		if threshold, ok := top.threshold(); ok {
			canSkip := combinedUpperBound < threshold
			if !canSkip {
				tightCombinedUpperBound := 0.0
				for _, state := range states {
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

		retainDetail := (resultMode != textSearchResultScoreOnly || response.Explain != nil) && len(terms) > 1
		truncated, stale, err := visitTextV2ANDBlockMaxOverlap(snap, catalog, ctx, states, &cache, allowSet, candidateLimit, maxPostingsScanned, retainDetail, first, last, &top, seenCurrent, &response.Stats)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if stale {
			return fallback()
		}
		if truncated {
			response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
			return textSearchFailClosed(response, response.Stats.FailClosedReason, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded candidate generation", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
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
		var detail *textV2SearchCandidate
		if candidate.detail != nil {
			detail = candidate.detail
		} else if (resultMode != textSearchResultScoreOnly || response.Explain != nil) && len(terms) > 1 {
			var truncated bool
			var err error
			detail, truncated, err = buildTextV2SearchCandidateForTopTerms(snap, catalog, ctx, terms, candidate.ordinal, candidate.generation, candidate.documentID, candidate.score, maxPostingsScanned, &response.Stats)
			if err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if truncated {
				response.Stats.Truncated = true
				response.Stats.FailClosedReason = textSearchFailClosedPostingsLimit
				return textSearchFailClosed(response, textSearchFailClosedPostingsLimit, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded explain posting scan", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
			}
		}
		if resultMode != textSearchResultScoreOnly && len(terms) > 1 {
			if err := populateTextV2SearchResultMatchesFromCandidate(snap, catalog, ctx, idx, detail, resultMode, &result, &response.Stats); err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
		} else if err := populateTextV2SearchResultMatchesFromTopCandidate(snap, catalog, ctx, idx, candidate, resultMode, &result, &response.Stats); err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if response.Explain != nil {
			norm, ok, err := cache.normEntry(snap, catalog, ctx, candidate.ordinal, &response.Stats)
			if err != nil || !ok {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if err := textSearchExplainAppendV2CandidateScore(response.Explain, result, detail, terms, ctx, norm); err != nil {
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

func newTextV2ANDBlockMaxTermState(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, term string, fieldCount int) (*textV2ANDBlockMaxTermState, error) {
	termStats := ctx.termStats[term]
	prefix := encodeTextV2PostingBlockTermPrefix(term)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, ctx.postingBlocksRootName, prefix, textSearchPrefixEnd(prefix), true)
	if err != nil {
		return nil, err
	}
	if it == nil {
		return nil, errMalformedTextStorage("text-v2 term %q has posting block count %d but missing posting root", term, termStats.PostingBlockCount)
	}
	state := &textV2ANDBlockMaxTermState{term: term, termStats: termStats, prefix: prefix, it: it}
	if err := state.loadNextBlock(ctx, fieldCount); err != nil {
		_ = state.close()
		return nil, err
	}
	return state, nil
}

func (s *textV2ANDBlockMaxTermState) close() error {
	if s == nil || s.it == nil {
		return nil
	}
	return s.it.Close()
}

func (s *textV2ANDBlockMaxTermState) loadNextBlock(ctx *textV2SearchContext, fieldCount int) error {
	if s == nil || s.it == nil {
		return errMalformedTextStorage("text-v2 AND block-max term state is nil")
	}
	s.block = textV2PostingBlockValue{}
	s.scanner = nil
	s.upperBound = 0
	s.upperBoundTight = false
	s.entries = s.entries[:0]
	s.entryIdx = 0
	s.decoded = false
	for s.it.Valid() {
		keyBytes := s.it.UnsafeKey()
		if !bytes.HasPrefix(keyBytes, s.prefix) {
			break
		}
		if s.it.IsDeleted() {
			s.it.Next()
			continue
		}
		keyBlockStart, keyBlockID, err := decodeTextV2PostingBlockKeySuffixForPrefix(keyBytes, s.prefix)
		if err != nil {
			return err
		}
		scanner, err := initTextV2PostingBlockEntryScanner(&s.scannerStorage, s.it.UnsafeValue(), s.scratch)
		if err != nil {
			return err
		}
		if scanner.block.BlockStart != keyBlockStart || scanner.block.BlockID != keyBlockID {
			return errMalformedTextStorage("text-v2 posting block key/value identity mismatch")
		}
		if len(scanner.block.Summary.MaxFieldTermFrequencies) != fieldCount {
			return errMalformedTextStorage("text-v2 posting block field count %d want %d", len(scanner.block.Summary.MaxFieldTermFrequencies), fieldCount)
		}
		upperBound, err := textV2SearchBlockUpperBound(s.term, scanner.block.Summary, ctx)
		if err != nil {
			return err
		}
		if !textV2PostingBlockKindSupportsBlockMax(scanner.block.Kind) || !scanner.ChecksumVerified() || (s.hasLastBlock && scanner.block.Summary.FirstOrdinal <= s.lastBlockLast) {
			s.requiresFallback = true
		}
		s.block = scanner.block
		s.scanner = scanner
		s.upperBound = upperBound
		s.blocksSeen++
		s.lastBlockLast = scanner.block.Summary.LastOrdinal
		s.hasLastBlock = true
		return nil
	}
	if err := s.it.Error(); err != nil {
		return err
	}
	s.exhausted = true
	return nil
}

func (s *textV2ANDBlockMaxTermState) advanceBlock(ctx *textV2SearchContext, fieldCount int) error {
	if s == nil || s.it == nil || s.exhausted {
		return nil
	}
	s.it.Next()
	return s.loadNextBlock(ctx, fieldCount)
}

func (s *textV2ANDBlockMaxTermState) currentFirst() uint64 {
	if s == nil || s.exhausted {
		return 0
	}
	if s.decoded && s.entryIdx < len(s.entries) {
		return s.entries[s.entryIdx].ordinal
	}
	return s.block.Summary.FirstOrdinal
}

func (s *textV2ANDBlockMaxTermState) currentLast() uint64 {
	if s == nil || s.exhausted {
		return 0
	}
	return s.block.Summary.LastOrdinal
}

func (s *textV2ANDBlockMaxTermState) ensureDecoded(ctx *textV2SearchContext, fieldCount, maxPostingsScanned int, allowSet *textV2SearchOrdinalAllowSet, stats *TextSearchStats) (bool, error) {
	if s == nil || s.exhausted || s.scanner == nil {
		return false, errMalformedTextStorage("text-v2 AND block-max missing current block")
	}
	if s.decoded {
		return false, nil
	}
	allowIdx, allowEnd, filterAllowed := 0, 0, false
	if allowSet != nil && !allowSet.all {
		allowIdx, allowEnd, filterAllowed = allowSet.rangeBounds(s.block.Summary.FirstOrdinal, s.block.Summary.LastOrdinal)
		if !filterAllowed {
			if stats != nil {
				stats.TextPostingBlocksSkipped++
				stats.TextScalarPostingBlocksSkipped++
			}
			s.scratch = s.scanner.fieldScratch
			s.decoded = true
			return false, nil
		}
	}
	if stats != nil {
		stats.TextPostingBlocksVisited++
	}
	var entry textV2PostingBlockEntry
	for s.scanner.remaining > 0 {
		if maxPostingsScanned > 0 && stats != nil && stats.TextPostingsScanned >= uint64(maxPostingsScanned) {
			stats.Truncated = true
			stats.FailClosedReason = textSearchFailClosedPostingsLimit
			return true, nil
		}
		if !s.scanner.Next(&entry) {
			break
		}
		if stats != nil {
			stats.TextPostingsScanned++
		}
		if entry.Ordinal >= ctx.status.NextOrdinal || entry.Generation > ctx.status.RootGeneration {
			return false, errMalformedTextStorage("text-v2 posting entry outside status snapshot")
		}
		if filterAllowed {
			for allowIdx < allowEnd && allowSet.sorted[allowIdx] < entry.Ordinal {
				allowIdx++
			}
			if allowIdx >= allowEnd || allowSet.sorted[allowIdx] != entry.Ordinal {
				if stats != nil {
					stats.TextScalarPostingsRejected++
				}
				continue
			}
		}
		posting, err := textV2SearchPostingValueFromEntry(entry, fieldCount)
		if err != nil {
			return false, err
		}
		s.entries = append(s.entries, textV2ANDBlockMaxPostingEntry{ordinal: entry.Ordinal, value: posting})
	}
	if err := s.scanner.Err(); err != nil {
		return false, err
	}
	s.scratch = s.scanner.scratch
	s.decoded = true
	return false, nil
}

func normalizeTextV2ANDBlockMaxStates(ctx *textV2SearchContext, states []*textV2ANDBlockMaxTermState, fieldCount int) error {
	for _, state := range states {
		for state != nil && !state.exhausted && state.decoded && state.entryIdx >= len(state.entries) {
			if err := state.advanceBlock(ctx, fieldCount); err != nil {
				return err
			}
		}
	}
	return nil
}

func textV2ANDBlockMaxAnyExhausted(states []*textV2ANDBlockMaxTermState) bool {
	for _, state := range states {
		if state == nil || state.exhausted {
			return true
		}
	}
	return false
}

func textV2ANDBlockMaxNeedsFallback(states []*textV2ANDBlockMaxTermState) bool {
	for _, state := range states {
		if state == nil || state.requiresFallback || (!state.exhausted && (state.scanner == nil || !state.scanner.ChecksumVerified() || !textV2PostingBlockKindSupportsBlockMax(state.scanner.block.Kind))) {
			return true
		}
	}
	return false
}

func textV2ANDBlockMaxCurrentOverlap(states []*textV2ANDBlockMaxTermState) (uint64, uint64) {
	var first uint64
	last := uint64(math.MaxUint64)
	for _, state := range states {
		if state == nil || state.exhausted {
			return 1, 0
		}
		if currentFirst := state.currentFirst(); currentFirst > first {
			first = currentFirst
		}
		if currentLast := state.currentLast(); currentLast < last {
			last = currentLast
		}
	}
	return first, last
}

func skipTextV2ANDBlockMaxNonOverlappingBlocks(ctx *textV2SearchContext, states []*textV2ANDBlockMaxTermState, fieldCount int, targetFirst uint64, stats *TextSearchStats) (bool, error) {
	progressed := false
	for _, state := range states {
		if state == nil || state.exhausted || state.currentLast() >= targetFirst {
			continue
		}
		if !state.decoded && stats != nil {
			stats.TextPostingBlocksSkipped++
		}
		if err := state.advanceBlock(ctx, fieldCount); err != nil {
			return progressed, err
		}
		progressed = true
	}
	return progressed, nil
}

func skipTextV2ANDBlockMaxOverlapThrough(ctx *textV2SearchContext, states []*textV2ANDBlockMaxTermState, fieldCount int, through uint64, stats *TextSearchStats, scalarPrune bool) error {
	for _, state := range states {
		if state == nil || state.exhausted {
			continue
		}
		if state.decoded {
			for state.entryIdx < len(state.entries) && state.entries[state.entryIdx].ordinal <= through {
				state.entryIdx++
			}
			if state.entryIdx >= len(state.entries) {
				if err := state.advanceBlock(ctx, fieldCount); err != nil {
					return err
				}
			}
			continue
		}
		if state.currentLast() <= through {
			if stats != nil {
				stats.TextPostingBlocksSkipped++
				if scalarPrune {
					stats.TextScalarPostingBlocksSkipped++
				}
			}
			if err := state.advanceBlock(ctx, fieldCount); err != nil {
				return err
			}
		}
	}
	return nil
}

func exhaustTextV2ANDBlockMaxRemainingHeaders(ctx *textV2SearchContext, states []*textV2ANDBlockMaxTermState, fieldCount int, stats *TextSearchStats) error {
	for _, state := range states {
		for state != nil && !state.exhausted {
			if state.decoded && state.entryIdx >= len(state.entries) {
				if err := state.advanceBlock(ctx, fieldCount); err != nil {
					return err
				}
				continue
			}
			if !state.decoded && stats != nil {
				stats.TextPostingBlocksSkipped++
			}
			if err := state.advanceBlock(ctx, fieldCount); err != nil {
				return err
			}
		}
	}
	return nil
}

func visitTextV2ANDBlockMaxOverlap(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	states []*textV2ANDBlockMaxTermState,
	cache *textV2SearchBlockCache,
	allowSet *textV2SearchOrdinalAllowSet,
	candidateLimit, maxPostingsScanned int,
	retainDetail bool,
	first, last uint64,
	top *textV2SearchTopK,
	seenCurrent map[uint64]uint64,
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
				if previousGeneration, seen := seenCurrent[target]; seen && previousGeneration == norm.Generation {
					return false, false, errMalformedTextStorage("duplicate current text-v2 AND candidate ordinal %d generation %d", target, norm.Generation)
				}
				if candidateLimit > 0 && stats != nil && stats.TextCandidatesScored >= uint64(candidateLimit) {
					stats.Truncated = true
					stats.FailClosedReason = textSearchFailClosedCandidateLimit
					return true, false, nil
				}
				score, err := scoreTextV2ANDBlockMaxCandidate(states, ctx, norm)
				if err != nil {
					return false, false, err
				}
				seenCurrent[target] = norm.Generation
				if stats != nil {
					stats.TextCandidatesScored++
				}
				if top.needsDocumentIDForScore(score) {
					docMap, ok, err := cache.docMapEntry(snap, catalog, ctx, target)
					if err != nil {
						return false, false, err
					}
					if ok {
						if norm.Generation != docMap.Generation || norm.Flags != docMap.Flags {
							return false, false, errMalformedTextStorage("text-v2 norm/docmap generation mismatch for ordinal %d", target)
						}
						if !docMap.tombstoned() {
							topCandidate := textV2SearchTopCandidate{ordinal: target, generation: norm.Generation, documentID: docMap.DocumentID, score: score}
							pos, admitted, thresholdChanged := top.addCandidate(topCandidate)
							if admitted && retainDetail {
								detail := &textV2SearchCandidate{ordinal: target, generation: norm.Generation, documentID: append([]byte(nil), docMap.DocumentID...), score: score}
								for _, state := range states {
									if err := detail.addPostingValue(state.term, state.entries[state.entryIdx].value); err != nil {
										return false, false, err
									}
								}
								top.candidates[pos].detail = detail
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
		for _, state := range states {
			state.entryIdx++
		}
	}
}

func executeTextV2ORBlockMaxSearchAtSnapshot(
	c *Collection,
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	idx TextIndexDefinition,
	opts TextSearchOptions,
	terms []string,
	allowSet *textV2SearchOrdinalAllowSet,
	candidateLimit, maxPostingsScanned int,
	resultMode textSearchResultMode,
	response TextSearchResponse,
) (TextSearchResponse, error) {
	textSearchExplainSetServingPath(response.Explain, TextSearchExplainPathBlockMaxORWAND, true)
	liveTerms := make([]string, 0, len(terms))
	for _, term := range terms {
		termStats := ctx.termStats[term]
		if termStats.DocumentFrequency == 0 {
			continue
		}
		if termStats.PostingBlockCount == 0 {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 term %q document frequency %d has no posting blocks", term, termStats.DocumentFrequency))
		}
		liveTerms = append(liveTerms, term)
	}
	if len(liveTerms) == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	if len(liveTerms) == 1 {
		return executeTextV2BlockMaxSearchAtSnapshot(c, snap, catalog, ctx, idx, opts, liveTerms[0], allowSet, candidateLimit, maxPostingsScanned, resultMode, response)
	}

	baseStats := response.Stats
	fallback := func() (TextSearchResponse, error) {
		fallbackResponse := response
		fallbackResponse.Stats = baseStats
		fallbackResponse.Stats.TextBlockMaxFallbacks++
		textSearchExplainAddFallback(fallbackResponse.Explain, "blockmax_or_unsealed_or_overlapping_block")
		fallbackOpts := opts
		fallbackOpts.textV2DisableBlockMax = true
		return executeTextV2SearchAtSnapshot(c, snap, catalog, idx, fallbackOpts, terms, TextSearchOperatorOR, candidateLimit, maxPostingsScanned, resultMode, fallbackResponse)
	}

	fieldCount := len(ctx.fieldNames)
	states := make([]*textV2ANDBlockMaxTermState, 0, len(liveTerms))
	for _, term := range liveTerms {
		state, err := newTextV2ANDBlockMaxTermState(snap, catalog, ctx, term, fieldCount)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		states = append(states, state)
	}
	defer func() {
		for _, state := range states {
			_ = state.close()
		}
	}()

	cache := textV2SearchBlockCache{}
	top := textV2SearchTopK{limit: opts.TopK, candidates: make([]textV2SearchTopCandidate, 0, opts.TopK)}
	activeScratch := make([]*textV2ANDBlockMaxTermState, 0, len(states))
	scanStart := time.Now()
	for {
		if err := normalizeTextV2ANDBlockMaxStates(ctx, states, fieldCount); err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if textV2ANDBlockMaxNeedsFallback(states) {
			return fallback()
		}
		if err := skipTextV2ORBlockMaxDisallowedBlocks(ctx, states, fieldCount, allowSet, &response.Stats); err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if textV2ANDBlockMaxNeedsFallback(states) {
			return fallback()
		}
		active := textV2ORBlockMaxActiveStates(states, activeScratch)
		if len(active) == 0 {
			break
		}
		textV2ORBlockMaxSortStates(active)

		if threshold, ok := top.threshold(); ok {
			truncated, progressed, err := skipTextV2ORBlockMaxLowUpperWindow(snap, catalog, ctx, &cache, allowSet, active, fieldCount, maxPostingsScanned, threshold, &top, &response.Stats)
			if err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if truncated {
				response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
				return textSearchFailClosed(response, response.Stats.FailClosedReason, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded candidate generation", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
			}
			if progressed {
				continue
			}
		}

		threshold, thresholdReady := top.threshold()
		pivot := 0
		if thresholdReady {
			upperSum := 0.0
			pivot = -1
			for i, state := range active {
				upperSum += state.upperBound
				if upperSum >= threshold {
					pivot = i
					break
				}
			}
			if pivot < 0 {
				truncated, err := advanceTextV2BlockMaxStatePastCurrent(ctx, active[0], fieldCount, maxPostingsScanned, allowSet, &response.Stats)
				if err != nil {
					return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
				}
				if truncated {
					response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
					return textSearchFailClosed(response, response.Stats.FailClosedReason, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded candidate generation", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
				}
				continue
			}
		}

		if thresholdReady {
			response.Stats.TextWANDPivots++
		}
		target := active[pivot].currentFirst()
		if target == 0 {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, errMalformedTextStorage("text-v2 OR block-max invalid target ordinal"))
		}
		if active[0].currentFirst() < target {
			truncated, err := advanceTextV2BlockMaxStateTo(ctx, active[0], fieldCount, maxPostingsScanned, allowSet, target, &response.Stats)
			if err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if truncated {
				response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
				return textSearchFailClosed(response, response.Stats.FailClosedReason, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded candidate generation", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
			}
			continue
		}

		retainDetail := resultMode != textSearchResultScoreOnly || response.Explain != nil
		truncated, err := visitTextV2ORBlockMaxCandidate(snap, catalog, ctx, states, active, &cache, allowSet, candidateLimit, maxPostingsScanned, retainDetail, target, &top, &response.Stats)
		if err != nil {
			return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
		}
		if truncated {
			response.Stats.PostingsScanNanos += uint64(time.Since(scanStart).Nanoseconds())
			return textSearchFailClosed(response, response.Stats.FailClosedReason, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded candidate generation", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
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
		var detail *textV2SearchCandidate
		if candidate.detail != nil {
			detail = candidate.detail
		} else if candidate.hasPosting {
			detail = &textV2SearchCandidate{ordinal: candidate.ordinal, generation: candidate.generation, documentID: append([]byte(nil), candidate.documentID...), score: candidate.score}
			if err := detail.addPostingValue(candidate.term, candidate.posting); err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
		} else if resultMode != textSearchResultScoreOnly || response.Explain != nil {
			var truncated bool
			var err error
			detail, truncated, err = buildTextV2SearchCandidateForTopMatchingTerms(snap, catalog, ctx, terms, candidate.ordinal, candidate.generation, candidate.documentID, candidate.score, maxPostingsScanned, &response.Stats)
			if err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if truncated {
				response.Stats.Truncated = true
				response.Stats.FailClosedReason = textSearchFailClosedPostingsLimit
				return textSearchFailClosed(response, textSearchFailClosedPostingsLimit, fmt.Errorf("%w: collection %q text-v2 index %q exceeded bounded explain posting scan", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name))
			}
		}
		if resultMode != textSearchResultScoreOnly {
			if err := populateTextV2SearchResultMatchesFromCandidate(snap, catalog, ctx, idx, detail, resultMode, &result, &response.Stats); err != nil {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
		}
		if response.Explain != nil {
			norm, ok, err := cache.normEntry(snap, catalog, ctx, candidate.ordinal, &response.Stats)
			if err != nil || !ok {
				return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
			}
			if err := textSearchExplainAppendV2CandidateScore(response.Explain, result, detail, terms, ctx, norm); err != nil {
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

func textV2PostingBlockKindSupportsBlockMax(kind textV2PostingBlockKind) bool {
	switch kind {
	case textV2PostingBlockKindSealed, textV2PostingBlockKindMicro:
		return true
	default:
		return false
	}
}

func textV2ORBlockMaxActiveStates(states, scratch []*textV2ANDBlockMaxTermState) []*textV2ANDBlockMaxTermState {
	active := scratch[:0]
	for _, state := range states {
		if state == nil || state.exhausted {
			continue
		}
		active = append(active, state)
	}
	return active
}

func textV2ORBlockMaxSortStates(states []*textV2ANDBlockMaxTermState) {
	slices.SortFunc(states, func(left, right *textV2ANDBlockMaxTermState) int {
		leftFirst := left.currentFirst()
		rightFirst := right.currentFirst()
		if leftFirst < rightFirst {
			return -1
		}
		if leftFirst > rightFirst {
			return 1
		}
		if left.upperBound > right.upperBound {
			return -1
		}
		if left.upperBound < right.upperBound {
			return 1
		}
		return strings.Compare(left.term, right.term)
	})
}

func tightenTextV2BlockMaxStateUpperBound(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, cache *textV2SearchBlockCache, allowSet *textV2SearchOrdinalAllowSet, state *textV2ANDBlockMaxTermState, stats *TextSearchStats) error {
	if state == nil || state.exhausted || state.upperBoundTight {
		return nil
	}
	upperBound, err := textV2SearchTightBlockUpperBound(snap, catalog, ctx, cache, allowSet, state.term, state.block.Summary, stats)
	if err != nil {
		return err
	}
	state.upperBound = upperBound
	state.upperBoundTight = true
	return nil
}

func skipTextV2ORBlockMaxDisallowedBlocks(ctx *textV2SearchContext, states []*textV2ANDBlockMaxTermState, fieldCount int, allowSet *textV2SearchOrdinalAllowSet, stats *TextSearchStats) error {
	if allowSet == nil {
		return nil
	}
	for _, state := range states {
		for state != nil && !state.exhausted && !state.decoded && !state.requiresFallback && !allowSet.intersects(state.block.Summary.FirstOrdinal, state.block.Summary.LastOrdinal) {
			if stats != nil {
				stats.TextPostingBlocksSkipped++
				stats.TextScalarPostingBlocksSkipped++
			}
			if err := state.advanceBlock(ctx, fieldCount); err != nil {
				return err
			}
		}
	}
	return nil
}

func skipTextV2ORBlockMaxLowUpperWindow(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, cache *textV2SearchBlockCache, allowSet *textV2SearchOrdinalAllowSet, states []*textV2ANDBlockMaxTermState, fieldCount, maxPostingsScanned int, threshold float64, top *textV2SearchTopK, stats *TextSearchStats) (bool, bool, error) {
	if len(states) == 0 {
		return false, false, nil
	}
	windowFirst := states[0].currentFirst()
	if windowFirst == 0 {
		return false, false, errMalformedTextStorage("text-v2 OR block-max invalid window ordinal")
	}
	windowLast := uint64(math.MaxUint64)
	upperSum := 0.0
	var overlappingInline [8]*textV2ANDBlockMaxTermState
	overlapping := overlappingInline[:0]
	if len(states) > len(overlappingInline) {
		overlapping = make([]*textV2ANDBlockMaxTermState, 0, len(states))
	}
	for _, state := range states {
		currentFirst := state.currentFirst()
		currentLast := state.currentLast()
		if currentFirst <= windowFirst && currentLast >= windowFirst {
			upperSum += state.upperBound
			overlapping = append(overlapping, state)
			if currentLast < windowLast {
				windowLast = currentLast
			}
			continue
		}
		if currentFirst > windowFirst && currentFirst-1 < windowLast {
			windowLast = currentFirst - 1
		}
	}
	if windowLast < windowFirst {
		return false, false, nil
	}
	canSkip := upperSum < threshold
	if !canSkip {
		tightUpperSum := 0.0
		for _, state := range overlapping {
			if err := tightenTextV2BlockMaxStateUpperBound(snap, catalog, ctx, cache, allowSet, state, stats); err != nil {
				return false, false, err
			}
			tightUpperSum += state.upperBound
		}
		var err error
		canSkip, err = textV2SearchTightUpperBoundSkipsRange(snap, catalog, ctx, cache, allowSet, tightUpperSum, windowFirst, windowLast, top)
		if err != nil {
			return false, false, err
		}
	}
	if !canSkip {
		return false, false, nil
	}
	target := windowLast + 1
	if target == 0 {
		return false, false, errMalformedTextStorage("text-v2 OR block-max window target overflow")
	}
	progressed := false
	for _, state := range states {
		if state.currentFirst() > windowLast || state.currentLast() < windowFirst {
			continue
		}
		if !state.decoded && state.currentLast() <= windowLast {
			if stats != nil {
				stats.TextPostingBlocksSkipped++
			}
			if err := state.advanceBlock(ctx, fieldCount); err != nil {
				return false, progressed, err
			}
			progressed = true
			continue
		}
		truncated, err := advanceTextV2BlockMaxStateTo(ctx, state, fieldCount, maxPostingsScanned, allowSet, target, stats)
		if truncated || err != nil {
			return truncated, progressed, err
		}
		progressed = true
	}
	return false, progressed, nil
}

func advanceTextV2BlockMaxStateTo(ctx *textV2SearchContext, state *textV2ANDBlockMaxTermState, fieldCount, maxPostingsScanned int, allowSet *textV2SearchOrdinalAllowSet, target uint64, stats *TextSearchStats) (bool, error) {
	if state == nil || state.exhausted {
		return false, nil
	}
	// Block-max bounds are valid for the current block only. Stop at a block
	// boundary and let the WAND loop recompute with the next block's upper bound
	// instead of skipping across a future block that may have a higher bound.
	if !state.decoded && state.currentLast() < target {
		if stats != nil {
			stats.TextPostingBlocksSkipped++
		}
		if err := state.advanceBlock(ctx, fieldCount); err != nil {
			return false, err
		}
		return false, nil
	}
	truncated, err := state.ensureDecoded(ctx, fieldCount, maxPostingsScanned, allowSet, stats)
	if truncated || err != nil {
		return truncated, err
	}
	for state.entryIdx < len(state.entries) && state.entries[state.entryIdx].ordinal < target {
		state.entryIdx++
	}
	if state.entryIdx >= len(state.entries) {
		if err := state.advanceBlock(ctx, fieldCount); err != nil {
			return false, err
		}
	}
	return false, nil
}

func advanceTextV2BlockMaxStatePastCurrent(ctx *textV2SearchContext, state *textV2ANDBlockMaxTermState, fieldCount, maxPostingsScanned int, allowSet *textV2SearchOrdinalAllowSet, stats *TextSearchStats) (bool, error) {
	if state == nil || state.exhausted {
		return false, nil
	}
	truncated, err := state.ensureDecoded(ctx, fieldCount, maxPostingsScanned, allowSet, stats)
	if truncated || err != nil {
		return truncated, err
	}
	if state.entryIdx < len(state.entries) {
		state.entryIdx++
	}
	for state != nil && !state.exhausted && state.decoded && state.entryIdx >= len(state.entries) {
		if err := state.advanceBlock(ctx, fieldCount); err != nil {
			return false, err
		}
	}
	return false, nil
}

func visitTextV2ORBlockMaxCandidate(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	scoreStates []*textV2ANDBlockMaxTermState,
	advanceStates []*textV2ANDBlockMaxTermState,
	cache *textV2SearchBlockCache,
	allowSet *textV2SearchOrdinalAllowSet,
	candidateLimit, maxPostingsScanned int,
	retainDetail bool,
	target uint64,
	top *textV2SearchTopK,
	stats *TextSearchStats,
) (bool, error) {
	fieldCount := len(ctx.fieldNames)
	if allowSet != nil && !allowSet.contains(target) {
		return decodeTextV2ORBlockMaxStatesAtTarget(ctx, advanceStates, fieldCount, maxPostingsScanned, allowSet, target, stats)
	}
	norm, ok, err := cache.normEntry(snap, catalog, ctx, target, stats)
	if err != nil {
		return false, err
	}
	currentOK := ok && !norm.tombstoned()

	score := 0.0
	matched := false
	if currentOK {
		// Accumulate in canonical query-term order (scoreStates), not the
		// WAND-active order (advanceStates), so floating-point ties match the
		// exhaustive scorer's deterministic ordering exactly.
		for _, state := range scoreStates {
			if state == nil || state.exhausted || state.currentFirst() != target {
				continue
			}
			truncated, err := state.ensureDecoded(ctx, fieldCount, maxPostingsScanned, allowSet, stats)
			if truncated || err != nil {
				return truncated, err
			}
			if state.entryIdx >= len(state.entries) || state.entries[state.entryIdx].ordinal != target {
				continue
			}
			posting := state.entries[state.entryIdx].value
			if posting.generation != norm.Generation {
				continue
			}
			if !matched {
				if candidateLimit > 0 && stats != nil && stats.TextCandidatesScored >= uint64(candidateLimit) {
					stats.Truncated = true
					stats.FailClosedReason = textSearchFailClosedCandidateLimit
					return true, nil
				}
				matched = true
			}
			termScore, err := scoreTextV2SearchPostingValue(state.term, posting, ctx, norm)
			if err != nil {
				return false, err
			}
			score += termScore
		}
	}
	if matched {
		if stats != nil {
			stats.TextCandidatesScored++
		}
		if top.needsDocumentIDForScore(score) {
			docMap, ok, err := cache.docMapEntry(snap, catalog, ctx, target)
			if err != nil {
				return false, err
			}
			if ok {
				if norm.Generation != docMap.Generation || norm.Flags != docMap.Flags {
					return false, errMalformedTextStorage("text-v2 norm/docmap generation mismatch for ordinal %d", target)
				}
				if !docMap.tombstoned() {
					topCandidate := textV2SearchTopCandidate{ordinal: target, generation: norm.Generation, documentID: docMap.DocumentID, score: score}
					pos, admitted, thresholdChanged := top.addCandidate(topCandidate)
					if admitted && retainDetail {
						detail := &textV2SearchCandidate{ordinal: target, generation: norm.Generation, documentID: append([]byte(nil), docMap.DocumentID...), score: score}
						for _, state := range scoreStates {
							if state == nil || state.exhausted || state.currentFirst() != target || state.entryIdx >= len(state.entries) || state.entries[state.entryIdx].ordinal != target {
								continue
							}
							posting := state.entries[state.entryIdx].value
							if posting.generation != norm.Generation {
								continue
							}
							if err := detail.addPostingValue(state.term, posting); err != nil {
								return false, err
							}
						}
						top.candidates[pos].detail = detail
					}
					if stats != nil && thresholdChanged {
						stats.TextBlockMaxThresholds++
					}
				}
			}
		}
	}
	return advanceTextV2ORBlockMaxStatesPastTarget(ctx, advanceStates, fieldCount, maxPostingsScanned, allowSet, target, stats)
}

func decodeTextV2ORBlockMaxStatesAtTarget(ctx *textV2SearchContext, states []*textV2ANDBlockMaxTermState, fieldCount, maxPostingsScanned int, allowSet *textV2SearchOrdinalAllowSet, target uint64, stats *TextSearchStats) (bool, error) {
	for _, state := range states {
		if state == nil || state.exhausted || state.currentFirst() != target {
			continue
		}
		truncated, err := state.ensureDecoded(ctx, fieldCount, maxPostingsScanned, allowSet, stats)
		if truncated || err != nil {
			return truncated, err
		}
		for state.entryIdx < len(state.entries) && state.entries[state.entryIdx].ordinal < target {
			state.entryIdx++
		}
		if state.entryIdx < len(state.entries) && state.entries[state.entryIdx].ordinal == target && allowSet != nil && !allowSet.contains(target) {
			state.entryIdx++
		}
		for state != nil && !state.exhausted && state.decoded && state.entryIdx >= len(state.entries) {
			if err := state.advanceBlock(ctx, fieldCount); err != nil {
				return false, err
			}
		}
	}
	return false, nil
}

func advanceTextV2ORBlockMaxStatesPastTarget(ctx *textV2SearchContext, states []*textV2ANDBlockMaxTermState, fieldCount, maxPostingsScanned int, allowSet *textV2SearchOrdinalAllowSet, target uint64, stats *TextSearchStats) (bool, error) {
	for _, state := range states {
		if state == nil || state.exhausted || state.currentFirst() != target {
			continue
		}
		truncated, err := advanceTextV2BlockMaxStatePastCurrent(ctx, state, fieldCount, maxPostingsScanned, allowSet, stats)
		if truncated || err != nil {
			return truncated, err
		}
	}
	return false, nil
}

func scoreTextV2ANDBlockMaxCandidate(states []*textV2ANDBlockMaxTermState, ctx *textV2SearchContext, norm textV2SearchNormEntry) (float64, error) {
	if ctx.corpus.DocumentCount == 0 {
		return 0, errMalformedTextStorage("text-v2 corpus document count is zero with search candidates")
	}
	if len(norm.FieldLengths) != len(ctx.fieldNames) {
		return 0, errMalformedTextStorage("text-v2 norm field count %d want %d", len(norm.FieldLengths), len(ctx.fieldNames))
	}
	corpusDocuments := float64(ctx.corpus.DocumentCount)
	var score float64
	for _, state := range states {
		posting := state.entries[state.entryIdx].value
		stats := ctx.termStats[state.term]
		if stats.DocumentFrequency == 0 || stats.DocumentFrequency > ctx.corpus.DocumentCount {
			return 0, errMalformedTextStorage("text-v2 term %q document frequency %d outside corpus %d", state.term, stats.DocumentFrequency, ctx.corpus.DocumentCount)
		}
		df := float64(stats.DocumentFrequency)
		idf := math.Log(1 + (corpusDocuments-df+0.5)/(df+0.5))
		var combinedTF float64
		for fieldIdx := range ctx.fieldNames {
			fieldTF := float64(posting.fieldFrequency(fieldIdx))
			if fieldTF <= 0 {
				continue
			}
			statsValue := ctx.fieldStats[fieldIdx]
			if statsValue.DocumentCount == 0 || statsValue.TotalTokenCount == 0 {
				return 0, errMalformedTextStorage("missing text-v2 field accounting for %q", ctx.fieldNames[fieldIdx])
			}
			avgLength := float64(statsValue.TotalTokenCount) / float64(statsValue.DocumentCount)
			if avgLength <= 0 {
				return 0, errMalformedTextStorage("invalid average text-v2 field length for %q", ctx.fieldNames[fieldIdx])
			}
			normalizedTF := fieldTF / (1 - textSearchBM25B + textSearchBM25B*(float64(norm.FieldLengths[fieldIdx])/avgLength))
			combinedTF += ctx.fieldWeights[fieldIdx] * normalizedTF
		}
		if combinedTF > 0 {
			score += idf * ((combinedTF * (textSearchBM25K1 + 1)) / (combinedTF + textSearchBM25K1))
		}
	}
	return score, nil
}

func buildTextV2SearchCandidateForTopTerms(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, terms []string, ordinal, generation uint64, documentID []byte, score float64, maxPostingsScanned int, stats *TextSearchStats) (*textV2SearchCandidate, bool, error) {
	candidate := &textV2SearchCandidate{ordinal: ordinal, generation: generation, documentID: append([]byte(nil), documentID...), score: score}
	fieldCount := len(ctx.fieldNames)
	for _, term := range terms {
		posting, found, scanned, err := readTextV2PositionPostingAtRootCounted(snap, catalog, ctx.postingBlocksRootName, term, ordinal, generation, fieldCount)
		if stats != nil && scanned > 0 {
			stats.TextPostingsScanned += uint64(scanned)
			stats.PostingsScanned = stats.TextPostingsScanned
		}
		if maxPostingsScanned > 0 && stats != nil && stats.TextPostingsScanned > uint64(maxPostingsScanned) {
			return nil, true, nil
		}
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, errMalformedTextStorage("missing text-v2 scoring posting for final candidate ordinal %d generation %d term %q", ordinal, generation, term)
		}
		if err := candidate.addPostingValue(term, posting); err != nil {
			return nil, false, err
		}
	}
	return candidate, false, nil
}

func buildTextV2SearchCandidateForTopMatchingTerms(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, terms []string, ordinal, generation uint64, documentID []byte, score float64, maxPostingsScanned int, stats *TextSearchStats) (*textV2SearchCandidate, bool, error) {
	candidate := &textV2SearchCandidate{ordinal: ordinal, generation: generation, documentID: append([]byte(nil), documentID...), score: score}
	fieldCount := len(ctx.fieldNames)
	for _, term := range terms {
		posting, found, scanned, err := readTextV2PositionPostingAtRootCounted(snap, catalog, ctx.postingBlocksRootName, term, ordinal, generation, fieldCount)
		if stats != nil && scanned > 0 {
			stats.TextPostingsScanned += uint64(scanned)
			stats.PostingsScanned = stats.TextPostingsScanned
		}
		if maxPostingsScanned > 0 && stats != nil && stats.TextPostingsScanned > uint64(maxPostingsScanned) {
			return nil, true, nil
		}
		if err != nil {
			return nil, false, err
		}
		if !found {
			continue
		}
		if err := candidate.addPostingValue(term, posting); err != nil {
			return nil, false, err
		}
	}
	if candidate.postingCount() == 0 {
		return nil, false, errMalformedTextStorage("missing text-v2 scoring postings for final OR candidate ordinal %d generation %d", ordinal, generation)
	}
	return candidate, false, nil
}

func scanTextV2SearchPostingBlocksTerm(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	cache *textV2SearchBlockCache,
	term string,
	candidates map[uint64]*textV2SearchCandidate,
	allowNewCandidates bool,
	candidateLimit, maxPostingsScanned int,
	allowSet *textV2SearchOrdinalAllowSet,
	stats *TextSearchStats,
) (bool, error) {
	termStats := ctx.termStats[term]
	if termStats.DocumentFrequency == 0 {
		// M3/M7 retain historical posting blocks after updates/deletes. Term stats
		// are the live search contract, so a zero-df term cannot produce live
		// candidates and stale blocks must not consume candidate or posting budget.
		return false, nil
	}
	if termStats.PostingBlockCount == 0 {
		return false, errMalformedTextStorage("text-v2 term %q document frequency %d has no posting blocks", term, termStats.DocumentFrequency)
	}
	prefix := encodeTextV2PostingBlockTermPrefix(term)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, ctx.postingBlocksRootName, prefix, textSearchPrefixEnd(prefix), true)
	if err != nil {
		return false, err
	}
	if it == nil {
		return false, errMalformedTextStorage("text-v2 term %q has posting block count %d but missing posting root", term, termStats.PostingBlockCount)
	}
	defer func() { _ = it.Close() }()
	var scratch []uint32
	var scannerStorage textV2PostingBlockEntryScanner
	var blocksSeen uint64
	fieldCount := len(ctx.fieldNames)
	for it.Valid() {
		keyBytes := it.UnsafeKey()
		if !bytes.HasPrefix(keyBytes, prefix) {
			break
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		keyBlockStart, keyBlockID, err := decodeTextV2PostingBlockKeySuffixForPrefix(keyBytes, prefix)
		if err != nil {
			return false, err
		}
		scanner, err := initTextV2PostingBlockEntryScanner(&scannerStorage, it.UnsafeValue(), scratch)
		if err != nil {
			return false, err
		}
		if scanner.block.BlockStart != keyBlockStart || scanner.block.BlockID != keyBlockID {
			return false, errMalformedTextStorage("text-v2 posting block key/value identity mismatch")
		}
		if len(scanner.block.Summary.MaxFieldTermFrequencies) != fieldCount {
			return false, errMalformedTextStorage("text-v2 posting block field count %d want %d", len(scanner.block.Summary.MaxFieldTermFrequencies), fieldCount)
		}
		blocksSeen++
		if allowSet != nil && scanner.ChecksumVerified() && !allowSet.intersects(scanner.block.Summary.FirstOrdinal, scanner.block.Summary.LastOrdinal) {
			stats.TextPostingBlocksSkipped++
			stats.TextScalarPostingBlocksSkipped++
			it.Next()
			continue
		}
		stats.TextPostingBlocksVisited++
		var entry textV2PostingBlockEntry
		for scanner.remaining > 0 {
			if maxPostingsScanned > 0 && stats.TextPostingsScanned >= uint64(maxPostingsScanned) {
				stats.Truncated = true
				stats.FailClosedReason = textSearchFailClosedPostingsLimit
				return true, nil
			}
			if !scanner.Next(&entry) {
				break
			}
			stats.TextPostingsScanned++
			if entry.Ordinal >= ctx.status.NextOrdinal || entry.Generation > ctx.status.RootGeneration {
				return false, errMalformedTextStorage("text-v2 posting entry outside status snapshot")
			}
			if !allowSet.contains(entry.Ordinal) {
				stats.TextScalarPostingsRejected++
				continue
			}
			candidate := candidates[entry.Ordinal]
			if candidate == nil && !allowNewCandidates {
				continue
			}
			current, err := textV2SearchPostingEntryCurrent(snap, catalog, ctx, cache, entry, stats)
			if err != nil {
				return false, err
			}
			if !current {
				continue
			}
			if candidate == nil {
				if candidateLimit > 0 && len(candidates) >= candidateLimit {
					stats.Truncated = true
					stats.FailClosedReason = textSearchFailClosedCandidateLimit
					return true, nil
				}
				candidate = &textV2SearchCandidate{ordinal: entry.Ordinal}
				candidates[entry.Ordinal] = candidate
			}
			if err := candidate.addPosting(term, entry, fieldCount); err != nil {
				return false, err
			}
		}
		if err := scanner.Err(); err != nil {
			return false, err
		}
		scratch = scanner.scratch
		it.Next()
	}
	if err := it.Error(); err != nil {
		return false, err
	}
	if blocksSeen != termStats.PostingBlockCount {
		return false, errMalformedTextStorage("text-v2 term %q posting block count %d does not match scanned blocks %d", term, termStats.PostingBlockCount, blocksSeen)
	}
	return false, nil
}

func textV2SearchPostingEntryCurrent(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, cache *textV2SearchBlockCache, entry textV2PostingBlockEntry, stats *TextSearchStats) (bool, error) {
	if cache == nil {
		return false, errMalformedTextStorage("text-v2 search cache is nil")
	}
	norm, ok, err := cache.normEntry(snap, catalog, ctx, entry.Ordinal, stats)
	if err != nil || !ok {
		return false, err
	}
	if norm.tombstoned() || norm.Generation != entry.Generation {
		return false, nil
	}
	return true, nil
}

func pruneTextV2SearchANDCandidates(candidates map[uint64]*textV2SearchCandidate, term string) {
	for ordinal, candidate := range candidates {
		if _, ok := candidate.postingForTerm(term); !ok {
			delete(candidates, ordinal)
		}
	}
}

func countTextV2CurrentPostings(candidate *textV2SearchCandidate, terms []string, generation uint64) int {
	count := 0
	for _, term := range terms {
		posting, ok := candidate.postingForTerm(term)
		if ok && posting.generation == generation {
			count++
		}
	}
	return count
}

func textV2SearchPostingValueFromEntry(entry textV2PostingBlockEntry, fieldCount int) (textV2SearchPostingValue, error) {
	value := textV2SearchPostingValue{generation: entry.Generation, termFrequency: entry.TermFrequency, fieldCount: fieldCount}
	if len(entry.FieldFrequencies) != fieldCount {
		return textV2SearchPostingValue{}, errMalformedTextStorage("text-v2 posting entry field count %d want %d", len(entry.FieldFrequencies), fieldCount)
	}
	if fieldCount <= len(value.inlineFields) {
		copy(value.inlineFields[:], entry.FieldFrequencies)
	} else {
		value.fields = append(value.fields, entry.FieldFrequencies...)
	}
	return value, nil
}

func textV2SearchBlockUpperBound(term string, summary textV2PostingBlockSummary, ctx *textV2SearchContext) (float64, error) {
	return textV2SearchBlockUpperBoundWithFieldLengthMinimums(term, summary, ctx, nil)
}

func textV2SearchTightBlockUpperBound(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, cache *textV2SearchBlockCache, allowSet *textV2SearchOrdinalAllowSet, term string, summary textV2PostingBlockSummary, stats *TextSearchStats) (float64, error) {
	if cache == nil {
		return 0, errMalformedTextStorage("text-v2 search cache is nil")
	}
	fieldCount := len(ctx.fieldNames)
	var inline [8]uint32
	mins := inline[:]
	if fieldCount > len(inline) {
		mins = make([]uint32, fieldCount)
	} else {
		mins = mins[:fieldCount]
	}
	hasLive, complete, err := cache.minFieldLengthsInAllowedRange(snap, catalog, ctx, allowSet, summary.FirstOrdinal, summary.LastOrdinal, mins, stats)
	if err != nil {
		return 0, err
	}
	if !complete {
		return textV2SearchBlockUpperBound(term, summary, ctx)
	}
	if !hasLive {
		return 0, nil
	}
	return textV2SearchBlockUpperBoundWithFieldLengthMinimums(term, summary, ctx, mins)
}

func textV2SearchBlockUpperBoundWithFieldLengthMinimums(term string, summary textV2PostingBlockSummary, ctx *textV2SearchContext, minFieldLengths []uint32) (float64, error) {
	if summary.UpperBoundKind != textV2PostingUpperBoundKindBM25FLaneMax {
		return 0, errMalformedTextStorage("text-v2 posting block unsupported upper-bound kind %d", summary.UpperBoundKind)
	}
	if len(summary.MaxFieldTermFrequencies) != len(ctx.fieldNames) {
		return 0, errMalformedTextStorage("text-v2 posting block upper-bound field count %d want %d", len(summary.MaxFieldTermFrequencies), len(ctx.fieldNames))
	}
	if minFieldLengths != nil && len(minFieldLengths) != len(ctx.fieldNames) {
		return 0, errMalformedTextStorage("text-v2 posting block upper-bound min field count %d want %d", len(minFieldLengths), len(ctx.fieldNames))
	}
	stats := ctx.termStats[term]
	if stats.DocumentFrequency == 0 || stats.DocumentFrequency > ctx.corpus.DocumentCount {
		return 0, errMalformedTextStorage("text-v2 term %q document frequency %d outside corpus %d", term, stats.DocumentFrequency, ctx.corpus.DocumentCount)
	}
	var combinedTFUpper float64
	for fieldIdx, maxTF := range summary.MaxFieldTermFrequencies {
		if maxTF == 0 {
			continue
		}
		statsValue := ctx.fieldStats[fieldIdx]
		if statsValue.DocumentCount == 0 || statsValue.TotalTokenCount == 0 {
			return 0, errMalformedTextStorage("missing text-v2 field accounting for %q", ctx.fieldNames[fieldIdx])
		}
		avgLength := float64(statsValue.TotalTokenCount) / float64(statsValue.DocumentCount)
		if avgLength <= 0 {
			return 0, errMalformedTextStorage("invalid average text-v2 field length for %q", ctx.fieldNames[fieldIdx])
		}
		denominator := 1 - textSearchBM25B
		if minFieldLengths != nil {
			denominator += textSearchBM25B * (float64(minFieldLengths[fieldIdx]) / avgLength)
		}
		if denominator <= 0 {
			return 0, errMalformedTextStorage("invalid BM25F normalization denominator")
		}
		combinedTFUpper += ctx.fieldWeights[fieldIdx] * (float64(maxTF) / denominator)
	}
	if combinedTFUpper <= 0 {
		return 0, nil
	}
	corpusDocuments := float64(ctx.corpus.DocumentCount)
	df := float64(stats.DocumentFrequency)
	idf := math.Log(1 + (corpusDocuments-df+0.5)/(df+0.5))
	return idf * ((combinedTFUpper * (textSearchBM25K1 + 1)) / (combinedTFUpper + textSearchBM25K1)), nil
}

func textV2SearchTightUpperBoundSkipsRange(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, cache *textV2SearchBlockCache, allowSet *textV2SearchOrdinalAllowSet, upperBound float64, first, last uint64, top *textV2SearchTopK) (bool, error) {
	worst, ok := top.worst()
	if !ok {
		return false, nil
	}
	if upperBound < worst.score {
		return true, nil
	}
	if upperBound > worst.score {
		return false, nil
	}
	minDocID, hasLive, complete, err := cache.minDocumentIDInAllowedRange(snap, catalog, ctx, allowSet, first, last)
	if err != nil {
		return false, err
	}
	if !complete {
		return false, nil
	}
	if !hasLive {
		return true, nil
	}
	return bytes.Compare(minDocID, worst.documentID) >= 0, nil
}

func scoreTextV2SearchPostingValue(term string, posting textV2SearchPostingValue, ctx *textV2SearchContext, norm textV2SearchNormEntry) (float64, error) {
	if ctx.corpus.DocumentCount == 0 {
		return 0, errMalformedTextStorage("text-v2 corpus document count is zero with search candidates")
	}
	if len(norm.FieldLengths) != len(ctx.fieldNames) {
		return 0, errMalformedTextStorage("text-v2 norm field count %d want %d", len(norm.FieldLengths), len(ctx.fieldNames))
	}
	stats := ctx.termStats[term]
	if stats.DocumentFrequency == 0 || stats.DocumentFrequency > ctx.corpus.DocumentCount {
		return 0, errMalformedTextStorage("text-v2 term %q document frequency %d outside corpus %d", term, stats.DocumentFrequency, ctx.corpus.DocumentCount)
	}
	corpusDocuments := float64(ctx.corpus.DocumentCount)
	df := float64(stats.DocumentFrequency)
	idf := math.Log(1 + (corpusDocuments-df+0.5)/(df+0.5))
	var combinedTF float64
	for fieldIdx := range ctx.fieldNames {
		fieldTF := float64(posting.fieldFrequency(fieldIdx))
		if fieldTF <= 0 {
			continue
		}
		statsValue := ctx.fieldStats[fieldIdx]
		if statsValue.DocumentCount == 0 || statsValue.TotalTokenCount == 0 {
			return 0, errMalformedTextStorage("missing text-v2 field accounting for %q", ctx.fieldNames[fieldIdx])
		}
		avgLength := float64(statsValue.TotalTokenCount) / float64(statsValue.DocumentCount)
		if avgLength <= 0 {
			return 0, errMalformedTextStorage("invalid average text-v2 field length for %q", ctx.fieldNames[fieldIdx])
		}
		normalizedTF := fieldTF / (1 - textSearchBM25B + textSearchBM25B*(float64(norm.FieldLengths[fieldIdx])/avgLength))
		combinedTF += ctx.fieldWeights[fieldIdx] * normalizedTF
	}
	if combinedTF <= 0 {
		return 0, nil
	}
	return idf * ((combinedTF * (textSearchBM25K1 + 1)) / (combinedTF + textSearchBM25K1)), nil
}

func scoreTextV2SearchCandidate(candidate *textV2SearchCandidate, terms []string, ctx *textV2SearchContext, norm textV2SearchNormEntry) (float64, error) {
	if ctx.corpus.DocumentCount == 0 {
		return 0, errMalformedTextStorage("text-v2 corpus document count is zero with search candidates")
	}
	if len(norm.FieldLengths) != len(ctx.fieldNames) {
		return 0, errMalformedTextStorage("text-v2 norm field count %d want %d", len(norm.FieldLengths), len(ctx.fieldNames))
	}
	corpusDocuments := float64(ctx.corpus.DocumentCount)
	var score float64
	for _, term := range terms {
		posting, ok := candidate.postingForTerm(term)
		if !ok || posting.generation != norm.Generation {
			continue
		}
		stats := ctx.termStats[term]
		if stats.DocumentFrequency == 0 || stats.DocumentFrequency > ctx.corpus.DocumentCount {
			return 0, errMalformedTextStorage("text-v2 term %q document frequency %d outside corpus %d", term, stats.DocumentFrequency, ctx.corpus.DocumentCount)
		}
		df := float64(stats.DocumentFrequency)
		idf := math.Log(1 + (corpusDocuments-df+0.5)/(df+0.5))
		var combinedTF float64
		for fieldIdx := range ctx.fieldNames {
			fieldTF := float64(posting.fieldFrequency(fieldIdx))
			if fieldTF <= 0 {
				continue
			}
			statsValue := ctx.fieldStats[fieldIdx]
			if statsValue.DocumentCount == 0 || statsValue.TotalTokenCount == 0 {
				return 0, errMalformedTextStorage("missing text-v2 field accounting for %q", ctx.fieldNames[fieldIdx])
			}
			avgLength := float64(statsValue.TotalTokenCount) / float64(statsValue.DocumentCount)
			if avgLength <= 0 {
				return 0, errMalformedTextStorage("invalid average text-v2 field length for %q", ctx.fieldNames[fieldIdx])
			}
			normalizedTF := fieldTF / (1 - textSearchBM25B + textSearchBM25B*(float64(norm.FieldLengths[fieldIdx])/avgLength))
			combinedTF += ctx.fieldWeights[fieldIdx] * normalizedTF
		}
		if combinedTF > 0 {
			score += idf * ((combinedTF * (textSearchBM25K1 + 1)) / (combinedTF + textSearchBM25K1))
		}
	}
	return score, nil
}

func populateTextV2SearchResultMatchesFromCandidate(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, idx TextIndexDefinition, candidate *textV2SearchCandidate, resultMode textSearchResultMode, result *TextSearchResult, stats *TextSearchStats) error {
	if resultMode == textSearchResultScoreOnly || candidate == nil || result == nil {
		return nil
	}
	includeLegacy := resultMode == textSearchResultFull
	matchedTerms, matchedFields, matches := textV2SearchCandidateMatchDetails(candidate, ctx, candidate.generation, includeLegacy)
	if resultMode == textSearchResultFull && idx.StorePositions {
		for postingIdx := 0; postingIdx < candidate.postingCount(); postingIdx++ {
			entry := candidate.postingAt(postingIdx)
			if entry.value.generation != candidate.generation {
				continue
			}
			if err := validateTextV2PositionPostingAtSnapshot(snap, catalog, ctx, idx, candidate.ordinal, candidate.generation, entry.term, entry.value); err != nil {
				return err
			}
		}
	}
	result.MatchedTerms = matchedTerms
	result.MatchedFields = matchedFields
	result.TextMatches = matches
	if stats != nil {
		stats.TextMatchDetailsBuilt++
	}
	return nil
}

func populateTextV2SearchResultMatchesFromTopCandidate(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, idx TextIndexDefinition, candidate textV2SearchTopCandidate, resultMode textSearchResultMode, result *TextSearchResult, stats *TextSearchStats) error {
	if resultMode == textSearchResultScoreOnly || result == nil {
		return nil
	}
	if !candidate.hasPosting || candidate.posting.generation != candidate.generation {
		return errMalformedTextStorage("text-v2 top candidate missing detail posting for ordinal %d", candidate.ordinal)
	}
	includeLegacy := resultMode == textSearchResultFull
	matchedTerms, matchedFields, matches := textV2SearchPostingMatchDetails(candidate.term, candidate.posting, ctx, includeLegacy)
	if resultMode == textSearchResultFull && idx.StorePositions {
		if err := validateTextV2PositionPostingAtSnapshot(snap, catalog, ctx, idx, candidate.ordinal, candidate.generation, candidate.term, candidate.posting); err != nil {
			return err
		}
	}
	result.MatchedTerms = matchedTerms
	result.MatchedFields = matchedFields
	result.TextMatches = matches
	if stats != nil {
		stats.TextMatchDetailsBuilt++
	}
	return nil
}

func textV2SearchCandidateMatchDetails(candidate *textV2SearchCandidate, ctx *textV2SearchContext, generation uint64, includeLegacyLists bool) ([]string, []string, []TextSearchMatch) {
	var inline [8]textSearchMatchPair
	pairs := inline[:0]
	if candidate == nil || ctx == nil {
		return nil, nil, nil
	}
	for postingIdx := 0; postingIdx < candidate.postingCount(); postingIdx++ {
		entry := candidate.postingAt(postingIdx)
		if entry.value.generation != generation {
			continue
		}
		pairs = appendTextV2PostingMatchPairs(pairs, entry.term, entry.value, ctx)
	}
	return textSearchMatchDetailsFromPairs(pairs, includeLegacyLists)
}

func textV2SearchPostingMatchDetails(term string, posting textV2SearchPostingValue, ctx *textV2SearchContext, includeLegacyLists bool) ([]string, []string, []TextSearchMatch) {
	var inline [8]textSearchMatchPair
	pairs := appendTextV2PostingMatchPairs(inline[:0], term, posting, ctx)
	return textSearchMatchDetailsFromPairs(pairs, includeLegacyLists)
}

func appendTextV2PostingMatchPairs(pairs []textSearchMatchPair, term string, posting textV2SearchPostingValue, ctx *textV2SearchContext) []textSearchMatchPair {
	if ctx == nil {
		return pairs
	}
	for fieldIdx, field := range ctx.fieldNames {
		if posting.fieldFrequency(fieldIdx) == 0 {
			continue
		}
		pairs = appendTextSearchMatchPair(pairs, textSearchMatchPair{field: field, term: term})
	}
	return pairs
}

func validateTextV2PositionPostingAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, idx TextIndexDefinition, ordinal, generation uint64, term string, posting textV2SearchPostingValue) error {
	if ctx == nil {
		return errMalformedTextStorage("text-v2 position validation missing search context")
	}
	raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, ctx.positionsRootName, encodeTextV2PositionKey(ordinal, term), nil)
	if err != nil {
		return err
	}
	if !found {
		return errMalformedTextStorage("missing text-v2 position entry for ordinal %d term %q", ordinal, term)
	}
	value, err := decodeTextV2PositionValueForTerm(raw, term)
	if err != nil {
		return err
	}
	if value.Ordinal != ordinal || value.Generation != generation || value.Term != term {
		return errMalformedTextStorage("text-v2 position key/value identity mismatch for ordinal %d term %q", ordinal, term)
	}
	return validateTextV2PositionValueMatchesPosting(value, idx, posting)
}

func decodeTextV2SearchNormBlock(raw []byte) (textV2SearchNormBlock, error) {
	cur, err := textV2ValueCursor(raw, textV2NormBlockValueVersion, "norm block")
	if err != nil {
		return textV2SearchNormBlock{}, err
	}
	blockStart, err := cur.readUvarint()
	if err != nil {
		return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm block start: %v", err)
	}
	blockSizeRaw, err := cur.readUvarint()
	if err != nil {
		return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm block size: %v", err)
	}
	fieldCountRaw, err := cur.readUvarint()
	if err != nil {
		return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm field count: %v", err)
	}
	entryCount, err := cur.readUvarint()
	if err != nil {
		return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm entry count: %v", err)
	}
	if entryCount > uint64(cur.remaining()+1) {
		return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm entry count too large")
	}
	blockSize := checkedTextUint32(blockSizeRaw)
	fieldCount := checkedTextUint32(fieldCountRaw)
	if err := validateTextV2BlockHeader(blockStart, blockSizeRaw, blockSize, "norm"); err != nil {
		return textV2SearchNormBlock{}, err
	}
	if uint64(fieldCount) != fieldCountRaw || fieldCount == 0 {
		return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm field count invalid")
	}
	if entryCount > 0 && fieldCountRaw > 0 && entryCount > uint64(cur.remaining())/fieldCountRaw {
		return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm field count exceeds remaining payload")
	}
	if entryCount > uint64(int(^uint(0)>>1)) || entryCount*fieldCountRaw > uint64(int(^uint(0)>>1)) {
		return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm block too large")
	}
	block := textV2SearchNormBlock{
		BlockStart:   blockStart,
		BlockSize:    blockSize,
		FieldCount:   fieldCount,
		Entries:      make([]textV2SearchNormEntry, 0, int(entryCount)),
		FieldLengths: make([]uint32, 0, int(entryCount*fieldCountRaw)),
	}
	var prev uint64
	for i := uint64(0); i < entryCount; i++ {
		ordinal, err := cur.readUvarint()
		if err != nil {
			return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm entry[%d] ordinal: %v", i, err)
		}
		generation, err := cur.readUvarint()
		if err != nil {
			return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm entry[%d] generation: %v", i, err)
		}
		if cur.remaining() == 0 {
			return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm entry[%d] missing flags", i)
		}
		flags := cur.buf[cur.pos]
		cur.pos++
		if err := validateTextV2BlockEntry(blockStart, blockSize, ordinal, generation, flags, prev, i, "norm"); err != nil {
			return textV2SearchNormBlock{}, err
		}
		fieldOffset := len(block.FieldLengths)
		for j := uint32(0); j < fieldCount; j++ {
			length, err := cur.readUvarint()
			if err != nil {
				return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm entry[%d] field[%d] length: %v", i, j, err)
			}
			if uint64(checkedTextUint32(length)) != length {
				return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm entry[%d] field[%d] length overflows uint32", i, j)
			}
			block.FieldLengths = append(block.FieldLengths, uint32(length))
		}
		block.Entries = append(block.Entries, textV2SearchNormEntry{Ordinal: ordinal, Generation: generation, Flags: flags, FieldLengths: block.FieldLengths[fieldOffset : fieldOffset+int(fieldCount)]})
		prev = ordinal
	}
	if cur.remaining() != 0 {
		return textV2SearchNormBlock{}, errMalformedTextStorage("text-v2 norm trailing bytes")
	}
	return block, nil
}

func (b textV2SearchNormBlock) find(ordinal uint64) (textV2SearchNormEntry, bool) {
	i := sort.Search(len(b.Entries), func(i int) bool { return b.Entries[i].Ordinal >= ordinal })
	if i < len(b.Entries) && b.Entries[i].Ordinal == ordinal {
		return b.Entries[i], true
	}
	return textV2SearchNormEntry{}, false
}

func decodeTextV2SearchDocMapBlock(raw []byte) (textV2SearchDocMapBlock, error) {
	cur, err := textV2ValueCursor(raw, textV2DocMapValueVersion, "docmap block")
	if err != nil {
		return textV2SearchDocMapBlock{}, err
	}
	blockStart, err := cur.readUvarint()
	if err != nil {
		return textV2SearchDocMapBlock{}, errMalformedTextStorage("text-v2 docmap block start: %v", err)
	}
	blockSizeRaw, err := cur.readUvarint()
	if err != nil {
		return textV2SearchDocMapBlock{}, errMalformedTextStorage("text-v2 docmap block size: %v", err)
	}
	entryCount, err := cur.readUvarint()
	if err != nil {
		return textV2SearchDocMapBlock{}, errMalformedTextStorage("text-v2 docmap entry count: %v", err)
	}
	if entryCount > uint64(cur.remaining()+1) {
		return textV2SearchDocMapBlock{}, errMalformedTextStorage("text-v2 docmap entry count too large")
	}
	blockSize := checkedTextUint32(blockSizeRaw)
	if err := validateTextV2BlockHeader(blockStart, blockSizeRaw, blockSize, "docmap"); err != nil {
		return textV2SearchDocMapBlock{}, err
	}
	if entryCount > uint64(int(^uint(0)>>1)) {
		return textV2SearchDocMapBlock{}, errMalformedTextStorage("text-v2 docmap block too large")
	}
	block := textV2SearchDocMapBlock{BlockStart: blockStart, BlockSize: blockSize, Entries: make([]textV2SearchDocMapEntry, 0, int(entryCount))}
	var prev uint64
	for i := uint64(0); i < entryCount; i++ {
		ordinal, err := cur.readUvarint()
		if err != nil {
			return textV2SearchDocMapBlock{}, errMalformedTextStorage("text-v2 docmap entry[%d] ordinal: %v", i, err)
		}
		generation, err := cur.readUvarint()
		if err != nil {
			return textV2SearchDocMapBlock{}, errMalformedTextStorage("text-v2 docmap entry[%d] generation: %v", i, err)
		}
		if cur.remaining() == 0 {
			return textV2SearchDocMapBlock{}, errMalformedTextStorage("text-v2 docmap entry[%d] missing flags", i)
		}
		flags := cur.buf[cur.pos]
		cur.pos++
		documentID, err := cur.readBytes()
		if err != nil {
			return textV2SearchDocMapBlock{}, errMalformedTextStorage("text-v2 docmap entry[%d] document id: %v", i, err)
		}
		if err := validateTextV2BlockEntry(blockStart, blockSize, ordinal, generation, flags, prev, i, "docmap"); err != nil {
			return textV2SearchDocMapBlock{}, err
		}
		if len(documentID) == 0 {
			return textV2SearchDocMapBlock{}, errMalformedTextStorage("text-v2 docmap entry[%d] missing document id", i)
		}
		block.Entries = append(block.Entries, textV2SearchDocMapEntry{Ordinal: ordinal, Generation: generation, Flags: flags, DocumentID: documentID})
		prev = ordinal
	}
	if cur.remaining() != 0 {
		return textV2SearchDocMapBlock{}, errMalformedTextStorage("text-v2 docmap trailing bytes")
	}
	return block, nil
}

func (b textV2SearchDocMapBlock) find(ordinal uint64) (textV2SearchDocMapEntry, bool) {
	i := sort.Search(len(b.Entries), func(i int) bool { return b.Entries[i].Ordinal >= ordinal })
	if i < len(b.Entries) && b.Entries[i].Ordinal == ordinal {
		return b.Entries[i], true
	}
	return textV2SearchDocMapEntry{}, false
}

func (cache *textV2SearchBlockCache) normBlockAtOptional(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, blockStart uint64, stats *TextSearchStats) (textV2SearchNormBlock, bool, error) {
	if blockStart == 0 {
		return textV2SearchNormBlock{}, false, errMalformedTextStorage("text-v2 invalid norm block start")
	}
	if cache.normBlocks == nil {
		cache.normBlocks = make(map[uint64]textV2SearchNormBlock)
	}
	block, ok := cache.normBlocks[blockStart]
	if ok {
		return block, true, nil
	}
	raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, ctx.normBlocksRootName, encodeTextV2BlockKey(blockStart), nil)
	if err != nil {
		return textV2SearchNormBlock{}, false, err
	}
	if stats != nil {
		stats.TextNormLookups++
	}
	if !found {
		return textV2SearchNormBlock{}, false, nil
	}
	decoded, err := decodeTextV2SearchNormBlock(raw)
	if err != nil {
		return textV2SearchNormBlock{}, false, err
	}
	if decoded.BlockStart != blockStart || decoded.BlockSize != textV2DefaultNormBlockSize || decoded.FieldCount != uint32(len(ctx.fieldNames)) {
		return textV2SearchNormBlock{}, false, errMalformedTextStorage("text-v2 norm block key/value mismatch")
	}
	cache.normBlocks[blockStart] = decoded
	return decoded, true, nil
}

func (cache *textV2SearchBlockCache) normBlockAt(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, blockStart uint64, stats *TextSearchStats) (textV2SearchNormBlock, error) {
	block, found, err := cache.normBlockAtOptional(snap, catalog, ctx, blockStart, stats)
	if err != nil {
		return textV2SearchNormBlock{}, err
	}
	if !found {
		return textV2SearchNormBlock{}, errMalformedTextStorage("missing text-v2 norm block %d", blockStart)
	}
	return block, nil
}

func (cache *textV2SearchBlockCache) normEntry(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, ordinal uint64, stats *TextSearchStats) (textV2SearchNormEntry, bool, error) {
	blockStart := textV2OrdinalBlockStart(ordinal, textV2DefaultNormBlockSize)
	if blockStart == 0 {
		return textV2SearchNormEntry{}, false, errMalformedTextStorage("text-v2 invalid norm ordinal %d", ordinal)
	}
	block, err := cache.normBlockAt(snap, catalog, ctx, blockStart, stats)
	if err != nil {
		return textV2SearchNormEntry{}, false, err
	}
	entry, found := block.find(ordinal)
	if !found {
		return textV2SearchNormEntry{}, false, errMalformedTextStorage("missing text-v2 norm entry for ordinal %d", ordinal)
	}
	if entry.Ordinal >= ctx.status.NextOrdinal || entry.Generation > ctx.status.NormGeneration {
		return textV2SearchNormEntry{}, false, errMalformedTextStorage("text-v2 norm entry outside status snapshot")
	}
	return entry, true, nil
}

func (cache *textV2SearchBlockCache) minFieldLengthsInRange(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, first, last uint64, mins []uint32, stats *TextSearchStats) (hasLive bool, complete bool, err error) {
	if first == 0 || last < first {
		return false, false, errMalformedTextStorage("text-v2 invalid norm range [%d,%d]", first, last)
	}
	if len(mins) != len(ctx.fieldNames) {
		return false, false, errMalformedTextStorage("text-v2 norm min field count %d want %d", len(mins), len(ctx.fieldNames))
	}
	for i := range mins {
		mins[i] = math.MaxUint32
	}
	blockStart := textV2OrdinalBlockStart(first, textV2DefaultNormBlockSize)
	if blockStart == 0 {
		return false, false, errMalformedTextStorage("text-v2 invalid norm range start %d", first)
	}
	for blockStart <= last {
		block, found, err := cache.normBlockAtOptional(snap, catalog, ctx, blockStart, stats)
		if err != nil {
			return false, false, err
		}
		if !found {
			// Missing norm blocks can be legitimate sparse/tombstone-purged ordinal gaps,
			// but they make the field-length minimum incomplete for this posting range.
			// The caller must fail closed to the loose summary-only bound instead of
			// treating the gap as proof that no live posting ordinal can score.
			return hasLive, false, nil
		}
		overlapFirst := max(first, blockStart)
		overlapLast := min(last, blockStart+uint64(textV2DefaultNormBlockSize)-1)
		idx := sort.Search(len(block.Entries), func(i int) bool { return block.Entries[i].Ordinal >= overlapFirst })
		expectedOrdinal := overlapFirst
		for idx < len(block.Entries) && block.Entries[idx].Ordinal <= overlapLast {
			entry := block.Entries[idx]
			if entry.Ordinal != expectedOrdinal {
				// A present sidecar block with a missing entry in the posting range
				// does not prove the range is empty or fully bounded: the posting block
				// may still reference a corrupt/missing norm entry that the exact scorer
				// must fail closed on.
				return hasLive, false, nil
			}
			if len(entry.FieldLengths) != len(ctx.fieldNames) {
				return false, false, errMalformedTextStorage("text-v2 norm entry field count %d want %d", len(entry.FieldLengths), len(ctx.fieldNames))
			}
			if entry.Ordinal >= ctx.status.NextOrdinal || entry.Generation > ctx.status.NormGeneration {
				return false, false, errMalformedTextStorage("text-v2 norm entry outside status snapshot")
			}
			if !entry.tombstoned() {
				for fieldIdx, length := range entry.FieldLengths {
					if length < mins[fieldIdx] {
						mins[fieldIdx] = length
					}
				}
				hasLive = true
			}
			expectedOrdinal++
			idx++
		}
		if expectedOrdinal <= overlapLast {
			return hasLive, false, nil
		}
		if blockStart > math.MaxUint64-uint64(textV2DefaultNormBlockSize) {
			break
		}
		blockStart += uint64(textV2DefaultNormBlockSize)
	}
	return hasLive, true, nil
}

func (cache *textV2SearchBlockCache) minFieldLengthsInAllowedRange(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, allowSet *textV2SearchOrdinalAllowSet, first, last uint64, mins []uint32, stats *TextSearchStats) (hasLive bool, complete bool, err error) {
	if allowSet == nil || allowSet.all {
		return cache.minFieldLengthsInRange(snap, catalog, ctx, first, last, mins, stats)
	}
	if first == 0 || last < first {
		return false, false, errMalformedTextStorage("text-v2 invalid scalar norm range [%d,%d]", first, last)
	}
	if len(mins) != len(ctx.fieldNames) {
		return false, false, errMalformedTextStorage("text-v2 scalar norm min field count %d want %d", len(mins), len(ctx.fieldNames))
	}
	for i := range mins {
		mins[i] = math.MaxUint32
	}
	start, end, ok := allowSet.rangeBounds(first, last)
	if !ok {
		return false, true, nil
	}
	for i := start; i < end; i++ {
		ordinal := allowSet.sorted[i]
		blockStart := textV2OrdinalBlockStart(ordinal, textV2DefaultNormBlockSize)
		if blockStart == 0 {
			return false, false, errMalformedTextStorage("text-v2 invalid scalar norm ordinal %d", ordinal)
		}
		block, found, err := cache.normBlockAtOptional(snap, catalog, ctx, blockStart, stats)
		if err != nil {
			return false, false, err
		}
		if !found {
			return hasLive, false, nil
		}
		entry, found := block.find(ordinal)
		if !found {
			return hasLive, false, nil
		}
		if len(entry.FieldLengths) != len(ctx.fieldNames) {
			return false, false, errMalformedTextStorage("text-v2 scalar norm entry field count %d want %d", len(entry.FieldLengths), len(ctx.fieldNames))
		}
		if entry.Ordinal >= ctx.status.NextOrdinal || entry.Generation > ctx.status.NormGeneration {
			return false, false, errMalformedTextStorage("text-v2 scalar norm entry outside status snapshot")
		}
		if entry.tombstoned() {
			continue
		}
		for fieldIdx, length := range entry.FieldLengths {
			if length < mins[fieldIdx] {
				mins[fieldIdx] = length
			}
		}
		hasLive = true
	}
	return hasLive, true, nil
}

func (cache *textV2SearchBlockCache) docMapBlockAtOptional(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, blockStart uint64) (textV2SearchDocMapBlock, bool, error) {
	if blockStart == 0 {
		return textV2SearchDocMapBlock{}, false, errMalformedTextStorage("text-v2 invalid docmap block start")
	}
	if cache.docMapBlocks == nil {
		cache.docMapBlocks = make(map[uint64]textV2SearchDocMapBlock)
	}
	block, ok := cache.docMapBlocks[blockStart]
	if ok {
		return block, true, nil
	}
	raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, ctx.docMapRootName, encodeTextV2BlockKey(blockStart), nil)
	if err != nil {
		return textV2SearchDocMapBlock{}, false, err
	}
	if !found {
		return textV2SearchDocMapBlock{}, false, nil
	}
	decoded, err := decodeTextV2SearchDocMapBlock(raw)
	if err != nil {
		return textV2SearchDocMapBlock{}, false, err
	}
	if decoded.BlockStart != blockStart || decoded.BlockSize != textV2DefaultDocMapBlockSize {
		return textV2SearchDocMapBlock{}, false, errMalformedTextStorage("text-v2 docmap block key/value mismatch")
	}
	cache.docMapBlocks[blockStart] = decoded
	return decoded, true, nil
}

func (cache *textV2SearchBlockCache) docMapBlockAt(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, blockStart uint64) (textV2SearchDocMapBlock, error) {
	block, found, err := cache.docMapBlockAtOptional(snap, catalog, ctx, blockStart)
	if err != nil {
		return textV2SearchDocMapBlock{}, err
	}
	if !found {
		return textV2SearchDocMapBlock{}, errMalformedTextStorage("missing text-v2 docmap block %d", blockStart)
	}
	return block, nil
}

func (cache *textV2SearchBlockCache) docMapEntry(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, ordinal uint64) (textV2SearchDocMapEntry, bool, error) {
	blockStart := textV2OrdinalBlockStart(ordinal, textV2DefaultDocMapBlockSize)
	if blockStart == 0 {
		return textV2SearchDocMapEntry{}, false, errMalformedTextStorage("text-v2 invalid docmap ordinal %d", ordinal)
	}
	block, err := cache.docMapBlockAt(snap, catalog, ctx, blockStart)
	if err != nil {
		return textV2SearchDocMapEntry{}, false, err
	}
	entry, found := block.find(ordinal)
	if !found {
		return textV2SearchDocMapEntry{}, false, errMalformedTextStorage("missing text-v2 docmap entry for ordinal %d", ordinal)
	}
	if entry.Ordinal >= ctx.status.NextOrdinal || entry.Generation > ctx.status.DocMapGeneration {
		return textV2SearchDocMapEntry{}, false, errMalformedTextStorage("text-v2 docmap entry outside status snapshot")
	}
	return entry, true, nil
}

func (cache *textV2SearchBlockCache) minDocumentIDInRange(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, first, last uint64) (minID []byte, hasLive bool, complete bool, err error) {
	if first == 0 || last < first {
		return nil, false, false, errMalformedTextStorage("text-v2 invalid docmap range [%d,%d]", first, last)
	}
	blockStart := textV2OrdinalBlockStart(first, textV2DefaultDocMapBlockSize)
	if blockStart == 0 {
		return nil, false, false, errMalformedTextStorage("text-v2 invalid docmap range start %d", first)
	}
	for blockStart <= last {
		block, found, err := cache.docMapBlockAtOptional(snap, catalog, ctx, blockStart)
		if err != nil {
			return nil, false, false, err
		}
		if !found {
			// Missing docmap blocks make the document-ID tie proof incomplete.
			// Do not treat the gap as empty; let the caller visit the range so a
			// referenced posting can fail closed through the required docmap lookup.
			return minID, minID != nil, false, nil
		}
		overlapFirst := max(first, blockStart)
		overlapLast := min(last, blockStart+uint64(textV2DefaultDocMapBlockSize)-1)
		idx := sort.Search(len(block.Entries), func(i int) bool { return block.Entries[i].Ordinal >= overlapFirst })
		expectedOrdinal := overlapFirst
		for idx < len(block.Entries) && block.Entries[idx].Ordinal <= overlapLast {
			entry := block.Entries[idx]
			if entry.Ordinal != expectedOrdinal {
				// A present sidecar block with a missing entry in the posting range
				// does not prove the range is empty or fully bounded: the posting block
				// may still reference a corrupt/missing docmap entry that the exact
				// scorer must fail closed on.
				return minID, minID != nil, false, nil
			}
			if entry.Ordinal >= ctx.status.NextOrdinal || entry.Generation > ctx.status.DocMapGeneration {
				return nil, false, false, errMalformedTextStorage("text-v2 docmap entry outside status snapshot")
			}
			if !entry.tombstoned() && (minID == nil || bytes.Compare(entry.DocumentID, minID) < 0) {
				minID = entry.DocumentID
			}
			expectedOrdinal++
			idx++
		}
		if expectedOrdinal <= overlapLast {
			return minID, minID != nil, false, nil
		}
		if blockStart > math.MaxUint64-uint64(textV2DefaultDocMapBlockSize) {
			break
		}
		blockStart += uint64(textV2DefaultDocMapBlockSize)
	}
	if minID == nil {
		// A range with only tombstoned doc-map entries is not a safe tie-prune
		// proof: corrupt tombstone flags can otherwise hide the required
		// norm/docmap consistency check for referenced postings.
		return nil, false, false, nil
	}
	return minID, true, true, nil
}

func (cache *textV2SearchBlockCache) minDocumentIDInAllowedRange(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, allowSet *textV2SearchOrdinalAllowSet, first, last uint64) (minID []byte, hasLive bool, complete bool, err error) {
	if allowSet == nil || allowSet.all {
		return cache.minDocumentIDInRange(snap, catalog, ctx, first, last)
	}
	if first == 0 || last < first {
		return nil, false, false, errMalformedTextStorage("text-v2 invalid scalar docmap range [%d,%d]", first, last)
	}
	start, end, ok := allowSet.rangeBounds(first, last)
	if !ok {
		return nil, false, true, nil
	}
	for i := start; i < end; i++ {
		ordinal := allowSet.sorted[i]
		blockStart := textV2OrdinalBlockStart(ordinal, textV2DefaultDocMapBlockSize)
		if blockStart == 0 {
			return nil, false, false, errMalformedTextStorage("text-v2 invalid scalar docmap ordinal %d", ordinal)
		}
		block, found, err := cache.docMapBlockAtOptional(snap, catalog, ctx, blockStart)
		if err != nil {
			return nil, false, false, err
		}
		if !found {
			return minID, minID != nil, false, nil
		}
		entry, found := block.find(ordinal)
		if !found {
			return minID, minID != nil, false, nil
		}
		if entry.Ordinal >= ctx.status.NextOrdinal || entry.Generation > ctx.status.DocMapGeneration {
			return nil, false, false, errMalformedTextStorage("text-v2 scalar docmap entry outside status snapshot")
		}
		if entry.tombstoned() {
			continue
		}
		if minID == nil || bytes.Compare(entry.DocumentID, minID) < 0 {
			minID = entry.DocumentID
		}
	}
	return minID, minID != nil, true, nil
}

func orderTextV2SearchScanTerms(terms []string, operator TextSearchOperator, stats map[string]textV2TermStatsValue) []string {
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
