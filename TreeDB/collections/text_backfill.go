package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"unicode"

	"github.com/buger/jsonparser"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/tidwall/gjson"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TextIndexBackfillStats reports durable root entries produced by CreateTextIndex.
type TextIndexBackfillStats struct {
	DocumentsScanned int
	StateEntries     int
	PostingEntries   int
	StatsEntries     int
	EncodedBytes     uint64

	V2DocIDEntries    int
	V2DocMapBlocks    int
	V2PostingBlocks   int
	V2NormBlocks      int
	V2PositionEntries int
	V2TermStats       int
	V2FormatRecords   int
	V2StatusRecords   int
	V2NextOrdinal     uint64
	V2RootGeneration  uint64

	V2DocIDBytes        uint64
	V2DocMapBytes       uint64
	V2PostingBlockBytes uint64
	V2NormBlockBytes    uint64
	V2PositionBytes     uint64
	V2TermStatsBytes    uint64
	V2StatusFormatBytes uint64
}

// TextIndexStorageStats reports durable text-root contents after validation.
type TextIndexStorageStats struct {
	Documents      uint64
	StateEntries   uint64
	PostingEntries uint64
	StatsEntries   uint64
	EncodedBytes   uint64

	Version               TextIndexVersion
	V2DocIDEntries        uint64
	V2DocMapBlocks        uint64
	V2PostingBlocks       uint64
	V2NormBlocks          uint64
	V2PositionEntries     uint64
	V2TermStats           uint64
	V2FormatRecords       uint64
	V2StatusRecords       uint64
	V2NextOrdinal         uint64
	V2LiveDocuments       uint64
	V2DeletedDocs         uint64
	V2RootGeneration      uint64
	V2StatsGeneration     uint64
	V2SealedPostingBlocks uint64
	V2DeltaPostingBlocks  uint64
	V2MicroPostingBlocks  uint64
	V2RewriteMergeState   string

	V2DocIDBytes        uint64
	V2DocMapBytes       uint64
	V2PostingBlockBytes uint64
	V2NormBlockBytes    uint64
	V2PositionBytes     uint64
	V2TermStatsBytes    uint64
	V2StatusFormatBytes uint64
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

const textTermAccumulatorInlineTerms = 8

type textTermAccumulator struct {
	storePositions bool
	storeOffsets   bool
	length         uint32
	inline         [textTermAccumulatorInlineTerms]textAnalyzedTerm
	terms          []textAnalyzedTerm
	lookup         map[string]int
}

func newTextTermAccumulator(storePositions, storeOffsets bool) textTermAccumulator {
	acc := textTermAccumulator{storePositions: storePositions, storeOffsets: storeOffsets}
	acc.terms = acc.inline[:0]
	return acc
}

func (a *textTermAccumulator) AddTextToken(token TextToken) error {
	a.addToken(token)
	return nil
}

func (a *textTermAccumulator) addToken(token TextToken) {
	idx := -1
	if a.lookup != nil {
		if found, ok := a.lookup[token.Term]; ok {
			idx = found
		}
	} else {
		for i := range a.terms {
			if a.terms[i].Term == token.Term {
				idx = i
				break
			}
		}
	}
	if idx < 0 {
		idx = len(a.terms)
		a.terms = append(a.terms, textAnalyzedTerm{Term: token.Term})
		if a.lookup != nil {
			a.lookup[token.Term] = idx
		} else if len(a.terms) > textTermAccumulatorInlineTerms {
			a.lookup = make(map[string]int, len(a.terms)*2)
			for i := range a.terms {
				a.lookup[a.terms[i].Term] = i
			}
		}
	}
	term := &a.terms[idx]
	term.Frequency++
	if a.storePositions {
		term.Positions = append(term.Positions, uint32(token.Position))
	}
	if a.storeOffsets {
		term.Offsets = append(term.Offsets, textTokenOffset{Start: uint32(token.StartOffset), End: uint32(token.EndOffset)})
	}
	a.length++
}

func (a *textTermAccumulator) termsMap() map[string]*textAnalyzedTerm {
	out := make(map[string]*textAnalyzedTerm, len(a.terms))
	for i := range a.terms {
		out[a.terms[i].Term] = &a.terms[i]
	}
	return out
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
	if def.Version == TextIndexVersionV2 {
		return buildCreateTextV2IndexBackfillPlan(snap, catalog, def, opts)
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

func buildCreateTextV2IndexBackfillPlan(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	def TextIndexDefinition,
	opts collectionOptions,
) (*createTextIndexBackfillPlan, error) {
	rootNames := collectionTextV2RootNames(catalog.meta.Name, def.Name)
	plan := &createTextIndexBackfillPlan{
		rootNames:   make([]string, 0, len(rootNames)),
		baseRootIDs: make(map[string]uint64, len(rootNames)+1),
		tables:      make([]memtable.Table, 0, len(rootNames)),
		policies:    make([]backenddb.OrderedRootStoragePolicy, 0, len(rootNames)),
	}
	primaryRootName := collectionPrimaryRootName(catalog.meta.Name)
	plan.baseRootIDs[primaryRootName] = catalog.rootID(primaryRootName)
	for _, rootName := range rootNames {
		plan.baseRootIDs[rootName] = catalog.rootID(rootName)
	}

	tables := make(map[string]memtable.Table, len(rootNames))
	resetAll := func() {
		owned := make([]memtable.Table, 0, len(tables))
		for _, table := range tables {
			owned = append(owned, table)
		}
		resetCollectionTables(owned)
	}
	for _, rootName := range rootNames {
		tables[rootName] = newCollectionRunTable(0)
	}

	fieldNames := textV2FieldNames(def)
	for _, rootName := range rootNames {
		family, ok := textV2RootFamilyForName(catalog.meta.Name, def.Name, rootName)
		if !ok {
			resetAll()
			return nil, fmt.Errorf("collections: unknown text-v2 root %q", rootName)
		}
		key := encodeTextV2FormatKey()
		value := encodeTextV2RootFormatValue(textV2RootFormatValue{
			FormatVersion:   textV2FormatVersion,
			Family:          family,
			DocMapBlockSize: textV2DefaultDocMapBlockSize,
			NormBlockSize:   textV2DefaultNormBlockSize,
			Fields:          fieldNames,
		})
		tables[rootName].SetSteal(key, value)
		bytesWritten := uint64(len(key) + len(value))
		plan.stats.V2FormatRecords++
		plan.stats.EncodedBytes += bytesWritten
		plan.stats.V2StatusFormatBytes += bytesWritten
	}

	docIDRootName := collectionTextV2DocIDRootName(catalog.meta.Name, def.Name)
	docMapRootName := collectionTextV2DocMapRootName(catalog.meta.Name, def.Name)
	termsRootName := collectionTextV2TermsRootName(catalog.meta.Name, def.Name)
	postingBlocksRootName := collectionTextV2PostingBlocksRootName(catalog.meta.Name, def.Name)
	normRootName := collectionTextV2NormBlocksRootName(catalog.meta.Name, def.Name)
	positionsRootName := collectionTextV2PositionsRootName(catalog.meta.Name, def.Name)
	generationsRootName := collectionTextV2GenerationsRootName(catalog.meta.Name, def.Name)
	docMapBlocks := make(map[uint64]*textV2DocMapBlockValue)
	normBlocks := make(map[uint64]*textV2NormBlockValue)
	var postingBuilder textV2PostingBatchBuilder
	acc := textBackfillStatsAccumulator{
		Terms:  make(map[string]*textStatsTermValue),
		Fields: make(map[string]*textStatsFieldValue),
	}

	nextOrdinal := uint64(1)
	primaryRootID := catalog.rootID(primaryRootName)
	if primaryRootID != 0 {
		it, err := collectionIteratorAtCatalogRoot(snap, catalog, primaryRootName, nil, nil, false)
		if err != nil {
			resetAll()
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
					resetAll()
					return nil, err
				}
				analysis, err := analyzeTextIndexDocument(def, jsonDocument)
				if err != nil {
					resetAll()
					return nil, err
				}

				ordinal := nextOrdinal
				nextOrdinal++
				generation := uint64(1)
				docIDKey := encodeTextV2DocIDKey(documentID)
				docIDValue := encodeTextV2DocIDValue(textV2DocIDValue{Ordinal: ordinal, Generation: generation})
				tables[docIDRootName].SetSteal(docIDKey, docIDValue)
				docIDBytes := uint64(len(docIDKey) + len(docIDValue))
				plan.stats.V2DocIDEntries++
				plan.stats.EncodedBytes += docIDBytes
				plan.stats.V2DocIDBytes += docIDBytes

				docBlockStart := textV2OrdinalBlockStart(ordinal, textV2DefaultDocMapBlockSize)
				docBlock := docMapBlocks[docBlockStart]
				if docBlock == nil {
					docBlock = &textV2DocMapBlockValue{BlockStart: docBlockStart, BlockSize: textV2DefaultDocMapBlockSize}
					docMapBlocks[docBlockStart] = docBlock
				}
				docBlock.upsert(textV2DocMapEntry{Ordinal: ordinal, Generation: generation, DocumentID: documentID})

				fieldLengths := textV2FieldLengthsFromAnalysis(def, analysis)
				normBlockStart := textV2OrdinalBlockStart(ordinal, textV2DefaultNormBlockSize)
				normBlock := normBlocks[normBlockStart]
				if normBlock == nil {
					normBlock = &textV2NormBlockValue{BlockStart: normBlockStart, BlockSize: textV2DefaultNormBlockSize, FieldCount: uint32(len(fieldNames))}
					normBlocks[normBlockStart] = normBlock
				}
				normBlock.upsert(textV2NormBlockEntry{Ordinal: ordinal, Generation: generation, FieldLengths: fieldLengths})

				if err := postingBuilder.addDocument(def, ordinal, generation, analysis); err != nil {
					resetAll()
					return nil, err
				}
				positionEntries, positionBytes, err := addTextV2PositionEntriesForDocument(tables[positionsRootName], def, ordinal, generation, analysis)
				if err != nil {
					resetAll()
					return nil, err
				}
				plan.stats.V2PositionEntries += positionEntries
				plan.stats.EncodedBytes += positionBytes
				plan.stats.V2PositionBytes += positionBytes
				acc.addDocument(analysis)
				plan.stats.DocumentsScanned++
				it.Next()
			}
			if err := it.Error(); err != nil {
				resetAll()
				return nil, err
			}
		}
	}

	for _, blockStart := range sortedTextV2BlockStarts(docMapBlocks) {
		key := encodeTextV2BlockKey(blockStart)
		value := encodeTextV2DocMapBlockValue(*docMapBlocks[blockStart])
		tables[docMapRootName].SetSteal(key, value)
		docMapBytes := uint64(len(key) + len(value))
		plan.stats.V2DocMapBlocks++
		plan.stats.EncodedBytes += docMapBytes
		plan.stats.V2DocMapBytes += docMapBytes
	}
	for _, blockStart := range sortedTextV2BlockStarts(normBlocks) {
		key := encodeTextV2BlockKey(blockStart)
		value := encodeTextV2NormBlockValue(*normBlocks[blockStart])
		tables[normRootName].SetSteal(key, value)
		normBytes := uint64(len(key) + len(value))
		plan.stats.V2NormBlocks++
		plan.stats.EncodedBytes += normBytes
		plan.stats.V2NormBlockBytes += normBytes
	}

	postingBlocks, postingBytes, postingBlockCounts, err := buildTextV2PostingBatchTable(tables[postingBlocksRootName], &postingBuilder, textV2PostingBlockKindSealed, textV2PostingBlockTargetPostings, 1)
	if err != nil {
		resetAll()
		return nil, err
	}
	plan.stats.V2PostingBlocks = postingBlocks
	plan.stats.EncodedBytes += postingBytes
	plan.stats.V2PostingBlockBytes += postingBytes

	statsEntries, statsBytes := addTextV2StatsEntries(tables[termsRootName], acc, def, 1, postingBlockCounts)
	plan.stats.V2TermStats = statsEntries
	plan.stats.StatsEntries = statsEntries
	plan.stats.EncodedBytes += statsBytes
	plan.stats.V2TermStatsBytes += statsBytes

	statusKey := encodeTextV2StatusKey()
	statusValue := encodeTextV2IndexStatusValue(textV2IndexStatusValue{
		FormatVersion:    textV2FormatVersion,
		RootGeneration:   1,
		StatsGeneration:  1,
		DocMapGeneration: 1,
		NormGeneration:   1,
		TermGeneration:   1,
		NextOrdinal:      nextOrdinal,
		LiveDocuments:    uint64(plan.stats.DocumentsScanned),
		DeletedDocuments: 0,
	})
	tables[generationsRootName].SetSteal(statusKey, statusValue)
	statusBytes := uint64(len(statusKey) + len(statusValue))
	plan.stats.V2StatusRecords = 1
	plan.stats.V2NextOrdinal = nextOrdinal
	plan.stats.V2RootGeneration = 1
	plan.stats.EncodedBytes += statusBytes
	plan.stats.V2StatusFormatBytes += statusBytes

	for _, rootName := range rootNames {
		table := tables[rootName]
		table.Freeze()
		plan.rootNames = append(plan.rootNames, rootName)
		plan.tables = append(plan.tables, table)
		policy, err := textV2RootStoragePolicyForDefinition(catalog.meta, def, rootName)
		if err != nil {
			resetAll()
			return nil, err
		}
		plan.policies = append(plan.policies, policy)
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
	if out, ok, err := analyzeTextIndexDocumentJSONRootFastPath(def, jsonDocument); ok || err != nil {
		return out, err
	}
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
		field, ok, err := analyzeTextIndexField(def, fieldDef.Field, text)
		if err != nil {
			return textAnalyzedDocument{}, err
		}
		if ok {
			out.Fields = append(out.Fields, field)
		}
	}
	return out, nil
}

func analyzeTextIndexDocumentJSONRootFastPath(def TextIndexDefinition, jsonDocument []byte) (textAnalyzedDocument, bool, error) {
	if len(def.Fields) == 0 {
		return textAnalyzedDocument{}, true, nil
	}
	for _, fieldDef := range def.Fields {
		if fieldDef.Field == "" || strings.Contains(fieldDef.Field, ".") {
			return textAnalyzedDocument{}, false, nil
		}
	}
	if !gjson.ValidBytes(jsonDocument) {
		return textAnalyzedDocument{}, true, errors.New("collections: text index extraction requires JSON document: invalid JSON")
	}
	if !jsonDocumentLooksObject(jsonDocument) {
		return textAnalyzedDocument{}, true, errors.New("collections: text index extraction requires JSON object document")
	}

	var stackValues [8]jsonParserIndexValue
	values := stackValues[:]
	if len(def.Fields) > len(stackValues) {
		values = make([]jsonParserIndexValue, len(def.Fields))
	} else {
		values = values[:len(def.Fields)]
	}
	if err := jsonparser.ObjectEach(jsonDocument, func(key, value []byte, dataType jsonparser.ValueType, _ int) error {
		// jsonparser.ObjectEach already returns decoded object keys. Unescaping here
		// would treat literal backslashes as JSON escapes (for example `a\\q` ->
		// `a\q` -> invalid `\q`) and could reject unrelated top-level fields.
		for i, fieldDef := range def.Fields {
			if textBytesEqualString(key, fieldDef.Field) {
				values[i] = jsonParserIndexValue{raw: value, valueType: dataType}
			}
		}
		return nil
	}); err != nil {
		return textAnalyzedDocument{}, true, fmt.Errorf("collections: text index extraction requires JSON document: %w", err)
	}

	out := textAnalyzedDocument{Fields: make([]textAnalyzedField, 0, len(def.Fields))}
	var scratch []byte
	for i, fieldDef := range def.Fields {
		text, ok, err := textIndexFieldTextFromJSONParser(values[i], &scratch)
		if err != nil {
			return textAnalyzedDocument{}, true, err
		}
		if !ok {
			continue
		}
		field, ok, err := analyzeTextIndexField(def, fieldDef.Field, text)
		if err != nil {
			return textAnalyzedDocument{}, true, err
		}
		if ok {
			out.Fields = append(out.Fields, field)
		}
	}
	return out, true, nil
}

func textIndexFieldTextFromJSONParser(value jsonParserIndexValue, scratch *[]byte) (string, bool, error) {
	switch value.valueType {
	case jsonparser.NotExist, jsonparser.Null:
		return "", false, nil
	case jsonparser.String:
		unescaped, err := textJSONParserStringBytes(value.raw, scratch)
		if err != nil {
			return "", false, fmt.Errorf("collections: text index extraction requires JSON document: %w", err)
		}
		return string(unescaped), true, nil
	case jsonparser.Array:
		var builder strings.Builder
		found := false
		var parseErr error
		_, err := jsonparser.ArrayEach(value.raw, func(elem []byte, dataType jsonparser.ValueType, _ int, elemErr error) {
			if parseErr != nil {
				return
			}
			if elemErr != nil {
				parseErr = elemErr
				return
			}
			if dataType != jsonparser.String {
				return
			}
			unescaped, err := textJSONParserStringBytes(elem, scratch)
			if err != nil {
				parseErr = err
				return
			}
			if found {
				builder.WriteByte(' ')
			}
			builder.Write(unescaped)
			found = true
		})
		if err != nil {
			return "", false, fmt.Errorf("collections: text index extraction requires JSON document: %w", err)
		}
		if parseErr != nil {
			return "", false, fmt.Errorf("collections: text index extraction requires JSON document: %w", parseErr)
		}
		if !found {
			return "", false, nil
		}
		return builder.String(), true, nil
	default:
		return "", false, nil
	}
}

func textJSONParserStringBytes(raw []byte, scratch *[]byte) ([]byte, error) {
	if bytes.IndexByte(raw, '\\') == -1 {
		return raw, nil
	}
	unescaped, err := jsonparser.Unescape(raw, (*scratch)[:0])
	if err != nil {
		return nil, err
	}
	*scratch = unescaped[:0]
	return unescaped, nil
}

func analyzeTextIndexField(def TextIndexDefinition, fieldName, text string) (textAnalyzedField, bool, error) {
	acc := newTextTermAccumulator(def.StorePositions, def.StoreOffsets)
	if err := analyzeTextToTermAccumulator(def.Analyzer, def.AnalyzerOptions, text, &acc); err != nil {
		return textAnalyzedField{}, false, err
	}
	field := textAnalyzedField{Field: fieldName, Length: acc.length, Terms: acc.termsMap()}
	return field, true, nil
}

func analyzeTextToTermAccumulator(analyzer TextAnalyzer, options *TextAnalyzerOptions, text string, acc *textTermAccumulator) error {
	return AnalyzeTextToSinkWithOptions(analyzer, options, text, acc)
}

func analyzeSimpleTextToTermAccumulator(text string, acc *textTermAccumulator) {
	var builder strings.Builder
	start := -1
	position := 0
	flush := func(end int) {
		if start < 0 {
			return
		}
		term := builder.String()
		if term != "" {
			acc.addToken(TextToken{Term: term, Position: position, StartOffset: start, EndOffset: end})
			position++
		}
		builder.Reset()
		start = -1
	}
	for offset, r := range text {
		if simpleTextTokenRune(r) {
			if start < 0 {
				start = offset
			}
			builder.WriteRune(unicode.ToLower(r))
			continue
		}
		flush(offset)
	}
	flush(len(text))
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
				Positions: analyzedTerm.Positions,
				Offsets:   analyzedTerm.Offsets,
			})
		}
		slices.SortFunc(terms, func(a, b textDocumentTermState) int { return compareTextStrings(a.Term, b.Term) })
		state.Fields = append(state.Fields, textDocumentFieldState{
			Field:  analyzedField.Field,
			Length: analyzedField.Length,
			Terms:  terms,
		})
	}
	slices.SortFunc(state.Fields, func(a, b textDocumentFieldState) int { return compareTextStrings(a.Field, b.Field) })
	return state
}

func addTextPostingsForDocument(table memtable.Table, documentID []byte, analysis textAnalyzedDocument) (int, uint64) {
	if table == nil {
		return 0, 0
	}
	byTerm := make(map[string]textPostingValue)
	for _, field := range analysis.Fields {
		for _, term := range field.Terms {
			posting := byTerm[term.Term]
			posting.TermFrequency += term.Frequency
			posting.Fields = append(posting.Fields, textPostingFieldValue{
				Field:     field.Field,
				Frequency: term.Frequency,
				Positions: term.Positions,
				Offsets:   term.Offsets,
			})
			byTerm[term.Term] = posting
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
		value := encodeTextPostingValue(byTerm[term])
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

func addTextV2StatsEntries(table memtable.Table, acc textBackfillStatsAccumulator, def TextIndexDefinition, generation uint64, postingBlockCounts map[string]uint64) (int, uint64) {
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
	set(encodeTextV2CorpusStatsKey(), encodeTextV2CorpusStatsValue(textV2CorpusStatsValue{StatsGeneration: generation, DocumentCount: acc.Corpus.DocumentCount}))
	terms := make([]string, 0, len(acc.Terms))
	for term := range acc.Terms {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	for _, term := range terms {
		stats := acc.Terms[term]
		if stats == nil {
			continue
		}
		set(encodeTextV2TermStatsKey(term), encodeTextV2TermStatsValue(textV2TermStatsValue{
			StatsGeneration:    generation,
			DocumentFrequency:  stats.DocumentFrequency,
			TotalTermFrequency: stats.TotalTermFrequency,
			PostingBlockCount:  postingBlockCounts[term],
		}))
	}
	seenFields := make(map[string]struct{}, len(def.Fields))
	for _, fieldDef := range def.Fields {
		field := fieldDef.Field
		if _, ok := seenFields[field]; ok {
			continue
		}
		seenFields[field] = struct{}{}
		stats := acc.Fields[field]
		value := textV2FieldStatsValue{StatsGeneration: generation}
		if stats != nil {
			value.DocumentCount = stats.DocumentCount
			value.TotalTokenCount = stats.TotalTokenCount
		}
		set(encodeTextV2FieldStatsKey(field), encodeTextV2FieldStatsValue(value))
	}
	return entries, bytesWritten
}

func textV2FieldNames(def TextIndexDefinition) []string {
	fields := make([]string, 0, len(def.Fields))
	for _, field := range def.Fields {
		fields = append(fields, field.Field)
	}
	return fields
}

func textV2FieldLengthsFromAnalysis(def TextIndexDefinition, analysis textAnalyzedDocument) []uint32 {
	byField := make(map[string]uint32, len(analysis.Fields))
	for _, field := range analysis.Fields {
		byField[field.Field] = field.Length
	}
	lengths := make([]uint32, 0, len(def.Fields))
	for _, field := range def.Fields {
		lengths = append(lengths, byField[field.Field])
	}
	return lengths
}

func sortedTextV2BlockStarts[V any](blocks map[uint64]V) []uint64 {
	starts := make([]uint64, 0, len(blocks))
	for start := range blocks {
		starts = append(starts, start)
	}
	slices.Sort(starts)
	return starts
}

func textV2RootStoragePolicyForDefinition(meta CollectionMeta, def TextIndexDefinition, rootName string) (backenddb.OrderedRootStoragePolicy, error) {
	switch rootName {
	case collectionTextV2DocMapRootName(meta.Name, def.Name), collectionTextV2PostingBlocksRootName(meta.Name, def.Name), collectionTextV2NormBlocksRootName(meta.Name, def.Name), collectionTextV2PositionsRootName(meta.Name, def.Name):
		return backendRootStoragePolicy(def.StoragePolicy)
	case collectionTextV2DocIDRootName(meta.Name, def.Name), collectionTextV2TermsRootName(meta.Name, def.Name), collectionTextV2GenerationsRootName(meta.Name, def.Name):
		return backendRootStoragePolicy(meta.Options.IndexStateStoragePolicy)
	default:
		return backenddb.OrderedRootStorageDefault, fmt.Errorf("collections: unknown text-v2 root %q for %q", rootName, meta.Name)
	}
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
	if def.Version == TextIndexVersionV2 {
		return inspectTextV2IndexStorage(snap, catalog, def)
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

func inspectTextV2IndexStorage(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition) (TextIndexStorageStats, error) {
	return inspectTextV2IndexStorageWithBudget(snap, catalog, def, nil)
}

func inspectTextV2IndexStorageWithBudget(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, budget *textV2RewriteBudget) (TextIndexStorageStats, error) {
	stats := TextIndexStorageStats{Version: TextIndexVersionV2}
	var positionPostings *textV2PositionPostingValidation
	if def.StorePositions {
		positionPostings = newTextV2PositionPostingValidation()
	}
	generationsRootName := collectionTextV2GenerationsRootName(catalog.meta.Name, def.Name)
	status, ok, err := readTextV2StatusAtRoot(snap, catalog, generationsRootName)
	if err != nil {
		return stats, err
	}
	if !ok {
		return stats, errMalformedTextStorage("missing text-v2 status record for collection %q index %q", catalog.meta.Name, def.Name)
	}
	stats.Documents = status.LiveDocuments
	stats.V2NextOrdinal = status.NextOrdinal
	stats.V2LiveDocuments = status.LiveDocuments
	stats.V2DeletedDocs = status.DeletedDocuments
	stats.V2RootGeneration = status.RootGeneration
	stats.V2StatsGeneration = status.StatsGeneration
	if budget != nil {
		if err := budget.check(); err != nil {
			return stats, err
		}
	}

	for _, rootName := range collectionTextV2RootNames(catalog.meta.Name, def.Name) {
		family, ok := textV2RootFamilyForName(catalog.meta.Name, def.Name, rootName)
		if !ok {
			return stats, errMalformedTextStorage("unknown text-v2 root %q", rootName)
		}
		if err := inspectTextV2Root(snap, catalog, def, rootName, family, status, &stats, budget, positionPostings); err != nil {
			return stats, err
		}
	}
	stats.V2RewriteMergeState = textV2RewriteMergeStateFromStats(stats)
	return stats, nil
}

func addTextV2StorageLaneBytes(stats *TextIndexStorageStats, family textV2RootFamily, key, value []byte) {
	if stats == nil {
		return
	}
	bytesWritten := uint64(len(key) + len(value))
	stats.EncodedBytes += bytesWritten
	if textV2KeyIsKind(key, textV2KeyKindFormat) || textV2KeyIsKind(key, textV2KeyKindStatus) {
		stats.V2StatusFormatBytes += bytesWritten
		return
	}
	switch family {
	case textV2RootFamilyDocID:
		stats.V2DocIDBytes += bytesWritten
	case textV2RootFamilyDocMap:
		stats.V2DocMapBytes += bytesWritten
	case textV2RootFamilyTerms:
		stats.V2TermStatsBytes += bytesWritten
	case textV2RootFamilyPostingBlocks:
		stats.V2PostingBlockBytes += bytesWritten
	case textV2RootFamilyNormBlocks:
		stats.V2NormBlockBytes += bytesWritten
	case textV2RootFamilyPositions:
		stats.V2PositionBytes += bytesWritten
	}
}

func textV2KeyIsKind(key []byte, kind byte) bool {
	return len(key) == 2 && key[0] == textV2KeyVersion && key[1] == kind
}

func readTextV2StatusAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string) (textV2IndexStatusValue, bool, error) {
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextV2StatusKey(), nil)
	if err != nil || !ok {
		return textV2IndexStatusValue{}, ok, err
	}
	status, err := decodeTextV2IndexStatusValue(raw)
	return status, true, err
}

func inspectTextV2Root(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, rootName string, family textV2RootFamily, status textV2IndexStatusValue, stats *TextIndexStorageStats, budget *textV2RewriteBudget, positionPostings *textV2PositionPostingValidation) error {
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return errMalformedTextStorage("missing text-v2 root %q", rootName)
	}
	if err != nil {
		return err
	}
	if it == nil {
		return errMalformedTextStorage("missing text-v2 root %q", rootName)
	}
	defer func() { _ = it.Close() }()
	formatSeen := false
	statusSeen := false
	corpusSeen := false
	fieldNames := textV2FieldNames(def)
	fieldSet := make(map[string]struct{}, len(fieldNames))
	seenFieldStats := make(map[string]struct{}, len(fieldNames))
	for _, field := range fieldNames {
		fieldSet[field] = struct{}{}
	}
	var termStatsScanned uint64
	for it.Valid() {
		if budget != nil {
			if err := budget.check(); err != nil {
				return err
			}
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		key := it.UnsafeKey()
		if budget != nil {
			switch family {
			case textV2RootFamilyTerms:
				if !bytes.Equal(key, encodeTextV2FormatKey()) {
					statsKey, err := decodeTextV2StatsKey(key)
					if err != nil {
						return err
					}
					if statsKey.Kind == textV2KeyKindTermStats {
						termStatsScanned++
						if err := budget.checkTermCount(termStatsScanned); err != nil {
							return err
						}
					}
				}
			case textV2RootFamilyPostingBlocks:
				if !bytes.Equal(key, encodeTextV2FormatKey()) {
					if err := budget.reservePostingBlock(); err != nil {
						return err
					}
				}
			}
		}
		value := it.ValueCopy(nil)
		switch {
		case bytes.Equal(key, encodeTextV2FormatKey()):
			format, err := decodeTextV2RootFormatValue(value)
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
			formatSeen = true
			stats.V2FormatRecords++
		case bytes.Equal(key, encodeTextV2StatusKey()):
			if family != textV2RootFamilyGenerations {
				return errMalformedTextStorage("text-v2 status key found in root family %s", family)
			}
			got, err := decodeTextV2IndexStatusValue(value)
			if err != nil {
				return err
			}
			if got != status {
				return errMalformedTextStorage("text-v2 status record changed while scanning")
			}
			statusSeen = true
			stats.V2StatusRecords++
		case family == textV2RootFamilyDocID:
			if _, err := decodeTextV2DocIDKey(key); err != nil {
				return err
			}
			doc, err := decodeTextV2DocIDValue(value)
			if err != nil {
				return err
			}
			if doc.Ordinal >= status.NextOrdinal || doc.Generation > status.RootGeneration {
				return errMalformedTextStorage("text-v2 docid ordinal/generation outside status snapshot")
			}
			stats.V2DocIDEntries++
		case family == textV2RootFamilyDocMap:
			blockStart, err := decodeTextV2BlockKey(key)
			if err != nil {
				return err
			}
			block, err := decodeTextV2DocMapBlockValue(value)
			if err != nil {
				return err
			}
			if block.BlockStart != blockStart || block.BlockSize != textV2DefaultDocMapBlockSize {
				return errMalformedTextStorage("text-v2 docmap block key/value mismatch")
			}
			for _, entry := range block.Entries {
				if entry.Ordinal >= status.NextOrdinal || entry.Generation > status.DocMapGeneration {
					return errMalformedTextStorage("text-v2 docmap entry outside status snapshot")
				}
			}
			stats.V2DocMapBlocks++
		case family == textV2RootFamilyNormBlocks:
			blockStart, err := decodeTextV2BlockKey(key)
			if err != nil {
				return err
			}
			block, err := decodeTextV2NormBlockValue(value)
			if err != nil {
				return err
			}
			if block.BlockStart != blockStart || block.BlockSize != textV2DefaultNormBlockSize || block.FieldCount != uint32(len(def.Fields)) {
				return errMalformedTextStorage("text-v2 norm block key/value mismatch")
			}
			for _, entry := range block.Entries {
				if entry.Ordinal >= status.NextOrdinal || entry.Generation > status.NormGeneration {
					return errMalformedTextStorage("text-v2 norm entry outside status snapshot")
				}
			}
			stats.V2NormBlocks++
		case family == textV2RootFamilyPostingBlocks:
			postingKey, err := decodeTextV2PostingBlockKey(key)
			if err != nil {
				return err
			}
			block, err := decodeTextV2PostingBlockValue(value)
			if err != nil {
				return err
			}
			if block.BlockStart != postingKey.BlockStart || block.BlockID != postingKey.BlockID {
				return errMalformedTextStorage("text-v2 posting block key/value mismatch")
			}
			if len(block.Summary.MaxFieldTermFrequencies) != len(def.Fields) {
				return errMalformedTextStorage("text-v2 posting block field lanes mismatch")
			}
			if budget != nil {
				if err := budget.reservePostings(uint64(block.Summary.DocCount)); err != nil {
					return err
				}
			}
			for _, entry := range block.Entries {
				if entry.Ordinal >= status.NextOrdinal || entry.Generation > status.RootGeneration {
					return errMalformedTextStorage("text-v2 posting block entry outside status snapshot")
				}
				if err := positionPostings.add(postingKey.Term, entry, len(def.Fields)); err != nil {
					return err
				}
			}
			switch block.Kind {
			case textV2PostingBlockKindSealed:
				stats.V2SealedPostingBlocks++
			case textV2PostingBlockKindDelta:
				stats.V2DeltaPostingBlocks++
			case textV2PostingBlockKindMicro:
				stats.V2MicroPostingBlocks++
			}
			stats.V2PostingBlocks++
		case family == textV2RootFamilyTerms:
			statsKey, err := decodeTextV2StatsKey(key)
			if err != nil {
				return err
			}
			switch statsKey.Kind {
			case textV2KeyKindCorpusStats:
				corpus, err := decodeTextV2CorpusStatsValue(value)
				if err != nil {
					return err
				}
				if corpus.StatsGeneration != status.StatsGeneration || corpus.DocumentCount != status.LiveDocuments {
					return errMalformedTextStorage("text-v2 corpus stats/status mismatch")
				}
				corpusSeen = true
			case textV2KeyKindFieldStats:
				if _, ok := fieldSet[statsKey.Value]; !ok {
					return errMalformedTextStorage("text-v2 field stats for undeclared field %q", statsKey.Value)
				}
				if _, dup := seenFieldStats[statsKey.Value]; dup {
					return errMalformedTextStorage("duplicate text-v2 field stats for field %q", statsKey.Value)
				}
				seenFieldStats[statsKey.Value] = struct{}{}
				field, err := decodeTextV2FieldStatsValue(value)
				if err != nil {
					return err
				}
				if field.StatsGeneration != status.StatsGeneration {
					return errMalformedTextStorage("text-v2 field stats generation mismatch")
				}
				if field.DocumentCount > status.LiveDocuments {
					return errMalformedTextStorage("text-v2 field stats document count exceeds live documents")
				}
			case textV2KeyKindTermStats:
				term, err := decodeTextV2TermStatsValue(value)
				if err != nil {
					return err
				}
				if term.StatsGeneration > status.TermGeneration {
					return errMalformedTextStorage("text-v2 term stats generation exceeds status")
				}
			default:
				return errMalformedTextStorage("unsupported text-v2 terms key kind %d", statsKey.Kind)
			}
			stats.V2TermStats++
			stats.StatsEntries++
		case family == textV2RootFamilyPositions:
			_, term, err := decodeTextV2PositionKey(key)
			if err != nil {
				return err
			}
			position, err := decodeTextV2PositionValueForTerm(value, term)
			if err != nil {
				return err
			}
			if err := validateTextV2PositionEntryAtSnapshot(snap, catalog, def, key, position, status, positionPostings); err != nil {
				return err
			}
			stats.V2PositionEntries++
		case family == textV2RootFamilyGenerations:
			return errMalformedTextStorage("unsupported text-v2 root %q entry key %x", rootName, key)
		default:
			return errMalformedTextStorage("unsupported text-v2 root %q family %s", rootName, family)
		}
		addTextV2StorageLaneBytes(stats, family, key, value)
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	if !formatSeen {
		return errMalformedTextStorage("missing text-v2 root %q format record", rootName)
	}
	if family == textV2RootFamilyGenerations && !statusSeen {
		return errMalformedTextStorage("missing text-v2 status record in root %q", rootName)
	}
	if family == textV2RootFamilyTerms {
		if !corpusSeen {
			return errMalformedTextStorage("missing text-v2 corpus stats record in root %q", rootName)
		}
		for _, field := range fieldNames {
			if _, ok := seenFieldStats[field]; !ok {
				return errMalformedTextStorage("missing text-v2 field stats for field %q", field)
			}
		}
	}
	return nil
}
