package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TextIndexBackfillStats reports durable root entries produced by CreateTextIndex.
type TextIndexBackfillStats struct {
	DocumentsScanned int
	StateEntries     int
	PostingEntries   int
	StatsEntries     int
	EncodedBytes     uint64
}

// TextIndexStorageStats reports durable text-root contents after validation.
type TextIndexStorageStats struct {
	Documents      uint64
	StateEntries   uint64
	PostingEntries uint64
	StatsEntries   uint64
	EncodedBytes   uint64
}

type createTextIndexBackfillPlan struct {
	rootNames   []string
	baseRootIDs map[string]uint64
	tables      []memtable.Table
	policies    []backenddb.OrderedRootStoragePolicy
	stats       TextIndexBackfillStats
}

type textAnalyzedDocument struct {
	Fields []textAnalyzedField
}

type textAnalyzedField struct {
	Field  string
	Length uint32
	Terms  map[string]*textAnalyzedTerm
}

type textAnalyzedTerm struct {
	Term      string
	Frequency uint32
	Positions []uint32
	Offsets   []textTokenOffset
}

type textBackfillStatsAccumulator struct {
	Corpus textStatsCorpusValue
	Terms  map[string]*textStatsTermValue
	Fields map[string]*textStatsFieldValue
}

func buildCreateTextIndexBackfillPlan(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	def TextIndexDefinition,
	opts collectionOptions,
) (*createTextIndexBackfillPlan, error) {
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	postingsRootName := collectionTextIndexRootName(catalog.meta.Name, def.Name)
	stateRootName := collectionTextStateRootName(catalog.meta.Name, def.Name)
	statsRootName := collectionTextStatsRootName(catalog.meta.Name, def.Name)
	plan := &createTextIndexBackfillPlan{
		baseRootIDs: map[string]uint64{
			collectionPrimaryRootName(catalog.meta.Name): catalog.rootID(collectionPrimaryRootName(catalog.meta.Name)),
			postingsRootName: catalog.rootID(postingsRootName),
			stateRootName:    catalog.rootID(stateRootName),
			statsRootName:    catalog.rootID(statsRootName),
		},
	}

	postingsTable := newCollectionRunTable(0)
	stateTable := newCollectionRunTable(0)
	statsTable := newCollectionRunTable(0)
	acc := textBackfillStatsAccumulator{
		Terms:  make(map[string]*textStatsTermValue),
		Fields: make(map[string]*textStatsFieldValue),
	}

	primaryRootID := catalog.rootID(collectionPrimaryRootName(catalog.meta.Name))
	if primaryRootID != 0 {
		it, err := collectionIteratorAtCatalogRoot(snap, catalog, collectionPrimaryRootName(catalog.meta.Name), nil, nil, false)
		if err != nil {
			resetCollectionTables([]memtable.Table{postingsTable, stateTable, statsTable})
			return nil, err
		}
		if it != nil {
			defer func() { _ = it.Close() }()
			for it.Valid() {
				if it.IsDeleted() {
					it.Next()
					continue
				}
				documentID := bytes.Clone(it.UnsafeKey())
				document := it.ValueCopy(nil)
				jsonDocument, err := materializeTextBackfillDocumentJSON(document, opts)
				if err != nil {
					resetCollectionTables([]memtable.Table{postingsTable, stateTable, statsTable})
					return nil, err
				}
				analysis, err := analyzeTextIndexDocument(def, jsonDocument)
				if err != nil {
					resetCollectionTables([]memtable.Table{postingsTable, stateTable, statsTable})
					return nil, err
				}
				stateRaw := encodeTextDocumentStateValue(textDocumentStateValueFromAnalysis(analysis))
				stateKey := encodeTextStateKey(documentID)
				stateTable.SetSteal(stateKey, stateRaw)
				plan.stats.StateEntries++
				plan.stats.EncodedBytes += uint64(len(stateKey) + len(stateRaw))
				postingEntries, postingBytes := addTextPostingsForDocument(postingsTable, documentID, analysis)
				plan.stats.PostingEntries += postingEntries
				plan.stats.EncodedBytes += postingBytes
				acc.addDocument(analysis)
				plan.stats.DocumentsScanned++
				it.Next()
			}
			if err := it.Error(); err != nil {
				resetCollectionTables([]memtable.Table{postingsTable, stateTable, statsTable})
				return nil, err
			}
		}
	}

	statsEntries, statsBytes := addTextStatsEntries(statsTable, acc)
	plan.stats.StatsEntries = statsEntries
	plan.stats.EncodedBytes += statsBytes

	if postingsTable.Len() > 0 {
		postingsTable.Freeze()
		plan.rootNames = append(plan.rootNames, postingsRootName)
		plan.tables = append(plan.tables, postingsTable)
		plan.policies = append(plan.policies, mustBackendTextRootStoragePolicy(def.StoragePolicy))
	} else {
		resetCollectionTables([]memtable.Table{postingsTable})
	}
	if stateTable.Len() > 0 {
		stateTable.Freeze()
		plan.rootNames = append(plan.rootNames, stateRootName)
		plan.tables = append(plan.tables, stateTable)
		plan.policies = append(plan.policies, opts.indexStateStoragePolicy)
	} else {
		resetCollectionTables([]memtable.Table{stateTable})
	}
	if statsTable.Len() > 0 {
		statsTable.Freeze()
		plan.rootNames = append(plan.rootNames, statsRootName)
		plan.tables = append(plan.tables, statsTable)
		plan.policies = append(plan.policies, opts.indexStateStoragePolicy)
	} else {
		resetCollectionTables([]memtable.Table{statsTable})
	}
	return plan, nil
}

func materializeTextBackfillDocumentJSON(document []byte, opts collectionOptions) ([]byte, error) {
	switch normalizedDocumentFormat(opts.documentFormat) {
	case DocumentFormatJSON:
		return document, nil
	case DocumentFormatBSON:
		raw := bson.Raw(document)
		if err := raw.Validate(); err != nil {
			return nil, fmt.Errorf("collections: text index backfill BSON document: %w", err)
		}
		return bson.MarshalExtJSON(raw, true, false)
	case DocumentFormatTemplateV1:
		return templateV1StoredDocumentJSON(document, opts.templateResolver)
	default:
		return nil, fmt.Errorf("collections: unsupported text index backfill document format %q", opts.documentFormat)
	}
}

func analyzeTextIndexDocument(def TextIndexDefinition, jsonDocument []byte) (textAnalyzedDocument, error) {
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(jsonDocument))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return textAnalyzedDocument{}, fmt.Errorf("collections: text index extraction requires JSON document: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = errors.New("unexpected trailing JSON value")
		}
		return textAnalyzedDocument{}, fmt.Errorf("collections: text index extraction requires JSON document: %w", err)
	}
	obj, ok := decoded.(map[string]any)
	if !ok {
		return textAnalyzedDocument{}, errors.New("collections: text index extraction requires JSON object document")
	}
	out := textAnalyzedDocument{Fields: make([]textAnalyzedField, 0, len(def.Fields))}
	for _, fieldDef := range def.Fields {
		value, found := extractIndexPathValue(obj, splitIndexPath(fieldDef.Field))
		if !found || value == nil {
			continue
		}
		text, ok := textIndexFieldText(value)
		if !ok {
			continue
		}
		tokens, err := AnalyzeText(def.Analyzer, text)
		if err != nil {
			return textAnalyzedDocument{}, err
		}
		field := textAnalyzedField{Field: fieldDef.Field, Length: uint32(len(tokens)), Terms: make(map[string]*textAnalyzedTerm)}
		for _, token := range tokens {
			term := field.Terms[token.Term]
			if term == nil {
				term = &textAnalyzedTerm{Term: token.Term}
				field.Terms[token.Term] = term
			}
			term.Frequency++
			if def.StorePositions {
				term.Positions = append(term.Positions, uint32(token.Position))
			}
			if def.StoreOffsets {
				term.Offsets = append(term.Offsets, textTokenOffset{Start: uint32(token.StartOffset), End: uint32(token.EndOffset)})
			}
		}
		out.Fields = append(out.Fields, field)
	}
	return out, nil
}

func textIndexFieldText(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				parts = append(parts, s)
			}
		}
		if len(parts) == 0 {
			return "", false
		}
		return strings.Join(parts, " "), true
	default:
		return "", false
	}
}

func textDocumentStateValueFromAnalysis(analysis textAnalyzedDocument) textDocumentStateValue {
	state := textDocumentStateValue{Fields: make([]textDocumentFieldState, 0, len(analysis.Fields))}
	for _, analyzedField := range analysis.Fields {
		terms := make([]textDocumentTermState, 0, len(analyzedField.Terms))
		for _, analyzedTerm := range analyzedField.Terms {
			terms = append(terms, textDocumentTermState{
				Term:      analyzedTerm.Term,
				Frequency: analyzedTerm.Frequency,
				Positions: append([]uint32(nil), analyzedTerm.Positions...),
				Offsets:   append([]textTokenOffset(nil), analyzedTerm.Offsets...),
			})
		}
		sort.SliceStable(terms, func(i, j int) bool { return terms[i].Term < terms[j].Term })
		state.Fields = append(state.Fields, textDocumentFieldState{
			Field:  analyzedField.Field,
			Length: analyzedField.Length,
			Terms:  terms,
		})
	}
	sort.SliceStable(state.Fields, func(i, j int) bool { return state.Fields[i].Field < state.Fields[j].Field })
	return state
}

func addTextPostingsForDocument(table memtable.Table, documentID []byte, analysis textAnalyzedDocument) (int, uint64) {
	if table == nil {
		return 0, 0
	}
	byTerm := make(map[string]*textPostingValue)
	for _, field := range analysis.Fields {
		for _, term := range field.Terms {
			posting := byTerm[term.Term]
			if posting == nil {
				posting = &textPostingValue{}
				byTerm[term.Term] = posting
			}
			posting.TermFrequency += term.Frequency
			posting.Fields = append(posting.Fields, textPostingFieldValue{
				Field:     field.Field,
				Frequency: term.Frequency,
				Positions: append([]uint32(nil), term.Positions...),
				Offsets:   append([]textTokenOffset(nil), term.Offsets...),
			})
		}
	}
	terms := make([]string, 0, len(byTerm))
	for term := range byTerm {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	var bytesWritten uint64
	for _, term := range terms {
		key := encodeTextPostingKey(term, documentID)
		value := encodeTextPostingValue(*byTerm[term])
		table.SetSteal(key, value)
		bytesWritten += uint64(len(key) + len(value))
	}
	return len(terms), bytesWritten
}

func (a *textBackfillStatsAccumulator) addDocument(analysis textAnalyzedDocument) {
	if a.Terms == nil {
		a.Terms = make(map[string]*textStatsTermValue)
	}
	if a.Fields == nil {
		a.Fields = make(map[string]*textStatsFieldValue)
	}
	a.Corpus.DocumentCount++
	seenTerms := make(map[string]uint32)
	for _, field := range analysis.Fields {
		fieldStats := a.Fields[field.Field]
		if fieldStats == nil {
			fieldStats = &textStatsFieldValue{}
			a.Fields[field.Field] = fieldStats
		}
		fieldStats.DocumentCount++
		fieldStats.TotalTokenCount += uint64(field.Length)
		for _, term := range field.Terms {
			seenTerms[term.Term] += term.Frequency
		}
	}
	for term, freq := range seenTerms {
		stats := a.Terms[term]
		if stats == nil {
			stats = &textStatsTermValue{}
			a.Terms[term] = stats
		}
		stats.DocumentFrequency++
		stats.TotalTermFrequency += uint64(freq)
	}
}

func addTextStatsEntries(table memtable.Table, acc textBackfillStatsAccumulator) (int, uint64) {
	if table == nil {
		return 0, 0
	}
	entries := 0
	var bytesWritten uint64
	set := func(key, value []byte) {
		table.SetSteal(key, value)
		entries++
		bytesWritten += uint64(len(key) + len(value))
	}
	set(encodeTextStatsCorpusKey(), encodeTextStatsCorpusValue(acc.Corpus))
	terms := make([]string, 0, len(acc.Terms))
	for term := range acc.Terms {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	for _, term := range terms {
		set(encodeTextStatsTermKey(term), encodeTextStatsTermValue(*acc.Terms[term]))
	}
	fields := make([]string, 0, len(acc.Fields))
	for field := range acc.Fields {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	for _, field := range fields {
		set(encodeTextStatsFieldKey(field), encodeTextStatsFieldValue(*acc.Fields[field]))
	}
	return entries, bytesWritten
}

func mustBackendTextRootStoragePolicy(policy RootStoragePolicy) backenddb.OrderedRootStoragePolicy {
	out, err := backendRootStoragePolicy(policy)
	if err != nil {
		return backenddb.OrderedRootStorageDefault
	}
	return out
}

func inspectTextIndexStorage(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition) (TextIndexStorageStats, error) {
	var stats TextIndexStorageStats
	if snap == nil {
		return stats, backenddb.ErrClosed
	}
	if catalog == nil {
		return stats, errCollectionNotFound
	}
	postingsRootName := collectionTextIndexRootName(catalog.meta.Name, def.Name)
	if err := inspectTextPostingsRoot(snap, catalog, postingsRootName, &stats); err != nil {
		return stats, err
	}
	stateRootName := collectionTextStateRootName(catalog.meta.Name, def.Name)
	if err := inspectTextStateRoot(snap, catalog, stateRootName, &stats); err != nil {
		return stats, err
	}
	statsRootName := collectionTextStatsRootName(catalog.meta.Name, def.Name)
	if err := inspectTextStatsRoot(snap, catalog, statsRootName, &stats); err != nil {
		return stats, err
	}
	return stats, nil
}

func inspectTextPostingsRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, stats *TextIndexStorageStats) error {
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil
	}
	if err != nil || it == nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if !it.IsDeleted() {
			key := it.UnsafeKey()
			value := it.ValueCopy(nil)
			if _, _, err := decodeTextPostingKey(key); err != nil {
				return err
			}
			if _, err := decodeTextPostingValue(value); err != nil {
				return err
			}
			stats.PostingEntries++
			stats.EncodedBytes += uint64(len(key) + len(value))
		}
		it.Next()
	}
	return it.Error()
}

func inspectTextStateRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, stats *TextIndexStorageStats) error {
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil
	}
	if err != nil || it == nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if !it.IsDeleted() {
			key := it.UnsafeKey()
			value := it.ValueCopy(nil)
			if _, err := decodeTextStateKey(key); err != nil {
				return err
			}
			if _, err := decodeTextDocumentStateValue(value); err != nil {
				return err
			}
			stats.StateEntries++
			stats.EncodedBytes += uint64(len(key) + len(value))
		}
		it.Next()
	}
	return it.Error()
}

func inspectTextStatsRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, stats *TextIndexStorageStats) error {
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil
	}
	if err != nil || it == nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if !it.IsDeleted() {
			key := it.UnsafeKey()
			value := it.ValueCopy(nil)
			statsKey, err := decodeTextStatsKey(key)
			if err != nil {
				return err
			}
			switch statsKey.Kind {
			case textStatsKeyKindCorpus:
				corpus, err := decodeTextStatsCorpusValue(value)
				if err != nil {
					return err
				}
				stats.Documents = corpus.DocumentCount
			case textStatsKeyKindTerm:
				if _, err := decodeTextStatsTermValue(value); err != nil {
					return err
				}
			case textStatsKeyKindField:
				if _, err := decodeTextStatsFieldValue(value); err != nil {
					return err
				}
			default:
				return errMalformedTextStorage("unsupported text-stats key kind %d", statsKey.Kind)
			}
			stats.StatsEntries++
			stats.EncodedBytes += uint64(len(key) + len(value))
		}
		it.Next()
	}
	return it.Error()
}
