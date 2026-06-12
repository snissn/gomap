package collections

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type textV2SearchContext struct {
	collectionName        string
	indexName             string
	termsRootName         string
	postingBlocksRootName string
	normBlocksRootName    string
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
	if c == nil {
		return nil
	}
	value := textV2SearchPostingValue{generation: entry.Generation, termFrequency: entry.TermFrequency, fieldCount: fieldCount}
	if len(entry.FieldFrequencies) != fieldCount {
		return errMalformedTextStorage("text-v2 posting entry field count %d want %d", len(entry.FieldFrequencies), fieldCount)
	}
	if fieldCount <= len(value.inlineFields) {
		copy(value.inlineFields[:], entry.FieldFrequencies)
	} else {
		value.fields = append(value.fields, entry.FieldFrequencies...)
	}
	for i := 0; i < c.postingsN; i++ {
		posting := c.postingAt(i)
		if posting.term != term {
			continue
		}
		if posting.value.generation == entry.Generation {
			return errMalformedTextStorage("duplicate text-v2 posting for term %q ordinal %d generation %d", term, c.ordinal, entry.Generation)
		}
		if entry.Generation > posting.value.generation {
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
) (TextSearchResponse, error) {
	ctx, err := newTextV2SearchContext(snap, catalog, idx, terms)
	if err != nil {
		return textSearchFailClosed(response, textSearchFailClosedStorageCorrupt, err)
	}
	if ctx.corpus.DocumentCount == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}

	candidates := make(map[uint64]*textV2SearchCandidate)
	cache := textV2SearchBlockCache{}
	scanTerms := orderTextV2SearchScanTerms(terms, operator, ctx.termStats)
	scanStart := time.Now()
	for i, term := range scanTerms {
		allowNewCandidates := operator != TextSearchOperatorAND || i == 0
		truncated, err := scanTextV2SearchPostingBlocksTerm(snap, catalog, ctx, &cache, term, candidates, allowNewCandidates, candidateLimit, maxPostingsScanned, &response.Stats)
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
	_ = resultMode // v2 detailed match materialization is deferred to #2629; M4 returns score-only rows.
	for i, candidate := range scored {
		id := append([]byte(nil), candidate.documentID...)
		response.Results[i] = TextSearchResult{
			DocumentID: id,
			IndexName:  idx.Name,
			Rank:       i + 1,
			Score:      candidate.score,
			ScoreKind:  HybridScoreKindBM25F,
		}
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
		termsRootName:         termsRootName,
		postingBlocksRootName: collectionTextV2PostingBlocksRootName(catalog.meta.Name, idx.Name),
		normBlocksRootName:    collectionTextV2NormBlocksRootName(catalog.meta.Name, idx.Name),
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

func scanTextV2SearchPostingBlocksTerm(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	ctx *textV2SearchContext,
	cache *textV2SearchBlockCache,
	term string,
	candidates map[uint64]*textV2SearchCandidate,
	allowNewCandidates bool,
	candidateLimit, maxPostingsScanned int,
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
	var blocksVisited uint64
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
		key, err := decodeTextV2PostingBlockKeyForPrefix(keyBytes, prefix)
		if err != nil {
			return false, err
		}
		scanner, err := newTextV2PostingBlockEntryScanner(it.UnsafeValue(), scratch)
		if err != nil {
			return false, err
		}
		if scanner.block.BlockStart != key.BlockStart || scanner.block.BlockID != key.BlockID {
			return false, errMalformedTextStorage("text-v2 posting block key/value identity mismatch")
		}
		if len(scanner.block.Summary.MaxFieldTermFrequencies) != fieldCount {
			return false, errMalformedTextStorage("text-v2 posting block field count %d want %d", len(scanner.block.Summary.MaxFieldTermFrequencies), fieldCount)
		}
		blocksVisited++
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
		scratch = scanner.fieldScratch
		it.Next()
	}
	if err := it.Error(); err != nil {
		return false, err
	}
	if blocksVisited != termStats.PostingBlockCount {
		return false, errMalformedTextStorage("text-v2 term %q posting block count %d does not match scanned blocks %d", term, termStats.PostingBlockCount, blocksVisited)
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

func (cache *textV2SearchBlockCache) normEntry(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, ordinal uint64, stats *TextSearchStats) (textV2SearchNormEntry, bool, error) {
	blockStart := textV2OrdinalBlockStart(ordinal, textV2DefaultNormBlockSize)
	if blockStart == 0 {
		return textV2SearchNormEntry{}, false, errMalformedTextStorage("text-v2 invalid norm ordinal %d", ordinal)
	}
	if cache.normBlocks == nil {
		cache.normBlocks = make(map[uint64]textV2SearchNormBlock)
	}
	block, ok := cache.normBlocks[blockStart]
	if !ok {
		raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, ctx.normBlocksRootName, encodeTextV2BlockKey(blockStart), nil)
		if err != nil {
			return textV2SearchNormEntry{}, false, err
		}
		stats.TextNormLookups++
		if !found {
			return textV2SearchNormEntry{}, false, errMalformedTextStorage("missing text-v2 norm block %d", blockStart)
		}
		decoded, err := decodeTextV2SearchNormBlock(raw)
		if err != nil {
			return textV2SearchNormEntry{}, false, err
		}
		if decoded.BlockStart != blockStart || decoded.BlockSize != textV2DefaultNormBlockSize || decoded.FieldCount != uint32(len(ctx.fieldNames)) {
			return textV2SearchNormEntry{}, false, errMalformedTextStorage("text-v2 norm block key/value mismatch")
		}
		cache.normBlocks[blockStart] = decoded
		block = decoded
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

func (cache *textV2SearchBlockCache) docMapEntry(snap *backenddb.Snapshot, catalog *collectionCatalog, ctx *textV2SearchContext, ordinal uint64) (textV2SearchDocMapEntry, bool, error) {
	blockStart := textV2OrdinalBlockStart(ordinal, textV2DefaultDocMapBlockSize)
	if blockStart == 0 {
		return textV2SearchDocMapEntry{}, false, errMalformedTextStorage("text-v2 invalid docmap ordinal %d", ordinal)
	}
	if cache.docMapBlocks == nil {
		cache.docMapBlocks = make(map[uint64]textV2SearchDocMapBlock)
	}
	block, ok := cache.docMapBlocks[blockStart]
	if !ok {
		raw, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, ctx.docMapRootName, encodeTextV2BlockKey(blockStart), nil)
		if err != nil {
			return textV2SearchDocMapEntry{}, false, err
		}
		if !found {
			return textV2SearchDocMapEntry{}, false, errMalformedTextStorage("missing text-v2 docmap block %d", blockStart)
		}
		decoded, err := decodeTextV2SearchDocMapBlock(raw)
		if err != nil {
			return textV2SearchDocMapEntry{}, false, err
		}
		if decoded.BlockStart != blockStart || decoded.BlockSize != textV2DefaultDocMapBlockSize {
			return textV2SearchDocMapEntry{}, false, errMalformedTextStorage("text-v2 docmap block key/value mismatch")
		}
		cache.docMapBlocks[blockStart] = decoded
		block = decoded
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
