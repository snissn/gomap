package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type textDocumentMutation struct {
	documentID  []byte
	oldDocument []byte
	deleteOld   bool
	newDocument []byte
	preparedNew *textDocumentStateValue
	setNew      bool
}

type preparedTextIndexInsert struct {
	indexName string
	states    []textDocumentStateValue
}

func appendPreparedTextIndexInserts(dst *[]preparedTextIndexInsert, src []preparedTextIndexInsert) {
	if len(src) == 0 {
		return
	}
	if len(*dst) == 0 {
		*dst = clonePreparedTextIndexInserts(src)
		return
	}
	for index := range src {
		(*dst)[index].states = append((*dst)[index].states, src[index].states...)
	}
}

func clonePreparedTextIndexInserts(in []preparedTextIndexInsert) []preparedTextIndexInsert {
	if len(in) == 0 {
		return nil
	}
	out := make([]preparedTextIndexInsert, len(in))
	for index := range in {
		out[index] = preparedTextIndexInsert{
			indexName: in[index].indexName,
			states:    append([]textDocumentStateValue(nil), in[index].states...),
		}
	}
	return out
}

type textStatsDelta struct {
	Documents     int64
	Terms         map[string]textStatsTermDelta
	Fields        map[string]textStatsFieldDelta
	PostingBlocks map[string]int64
}

type textStatsTermDelta struct {
	DocumentFrequency  int64
	TotalTermFrequency int64
}

type textStatsFieldDelta struct {
	DocumentCount   int64
	TotalTokenCount int64
}

func appendTextIndexMutationDeltas(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	opts collectionOptions,
	mutations []textDocumentMutation,
	rootNames *[]string,
	baseRootIDs map[string]uint64,
	policies *[]backenddb.OrderedRootStoragePolicy,
	deltaTables *[]memtable.Table,
) error {
	if snap == nil {
		return backenddb.ErrClosed
	}
	if catalog == nil {
		return errCollectionNotFound
	}
	if len(catalog.meta.TextIndexes) == 0 || len(mutations) == 0 {
		return nil
	}
	if rootNames == nil || policies == nil || deltaTables == nil {
		return errors.New("collections: text index maintenance missing root accumulator")
	}
	if baseRootIDs == nil {
		return errors.New("collections: text index maintenance missing base root map")
	}
	for _, def := range catalog.meta.TextIndexes {
		if err := appendSingleTextIndexMutationDeltas(snap, catalog, opts, def, mutations, rootNames, baseRootIDs, policies, deltaTables); err != nil {
			return err
		}
	}
	return nil
}

func appendSingleTextIndexMutationDeltas(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	opts collectionOptions,
	def TextIndexDefinition,
	mutations []textDocumentMutation,
	rootNames *[]string,
	baseRootIDs map[string]uint64,
	policies *[]backenddb.OrderedRootStoragePolicy,
	deltaTables *[]memtable.Table,
) error {
	if def.Version == TextIndexVersionV2 {
		return appendSingleTextV2IndexMutationDeltas(snap, catalog, opts, def, mutations, rootNames, baseRootIDs, policies, deltaTables)
	}
	postingsRootName := collectionTextIndexRootName(catalog.meta.Name, def.Name)
	stateRootName := collectionTextStateRootName(catalog.meta.Name, def.Name)
	statsRootName := collectionTextStatsRootName(catalog.meta.Name, def.Name)
	postingsTable := newCollectionRunTable(0)
	stateTable := newCollectionRunTable(len(mutations))
	statsDelta := textStatsDelta{}
	success := false
	defer func() {
		if success {
			return
		}
		resetCollectionRunTable(postingsTable)
		resetCollectionRunTable(stateTable)
	}()

	for _, mutation := range mutations {
		if len(mutation.documentID) == 0 {
			return errors.New("collections: text index maintenance document id cannot be empty")
		}
		if mutation.deleteOld {
			oldState, err := loadTextDocumentStateForMutation(snap, catalog, def, mutation.documentID, mutation.oldDocument, opts)
			if err != nil {
				return err
			}
			deleteTextPostingsForDocument(postingsTable, mutation.documentID, oldState)
			stateTable.DeleteSteal(encodeTextStateKey(mutation.documentID))
			statsDelta.addState(oldState, -1)
		}
		if mutation.setNew {
			newState, newAnalysis, err := textMutationNewStateAndAnalysis(def, mutation, opts)
			if err != nil {
				return err
			}
			addTextPostingsForDocument(postingsTable, mutation.documentID, newAnalysis)
			stateTable.SetSteal(encodeTextStateKey(mutation.documentID), encodeTextDocumentStateValue(newState))
			statsDelta.addState(newState, 1)
		}
	}

	statsTable := newCollectionRunTable(0)
	if err := buildTextStatsDeltaTable(snap, catalog, statsRootName, statsDelta, statsTable); err != nil {
		resetCollectionRunTable(statsTable)
		return err
	}
	if postingsTable.Len() > 0 {
		postingsTable.Freeze()
		appendTextRootDelta(rootNames, baseRootIDs, policies, deltaTables, postingsRootName, catalog.rootID(postingsRootName), mustBackendTextRootStoragePolicy(def.StoragePolicy), postingsTable)
		postingsTable = nil
	}
	if stateTable.Len() > 0 {
		stateTable.Freeze()
		appendTextRootDelta(rootNames, baseRootIDs, policies, deltaTables, stateRootName, catalog.rootID(stateRootName), opts.indexStateStoragePolicy, stateTable)
		stateTable = nil
	}
	if statsTable.Len() > 0 {
		statsTable.Freeze()
		appendTextRootDelta(rootNames, baseRootIDs, policies, deltaTables, statsRootName, catalog.rootID(statsRootName), opts.indexStateStoragePolicy, statsTable)
	} else {
		resetCollectionRunTable(statsTable)
	}
	success = true
	return nil
}

func appendTextRootDelta(rootNames *[]string, baseRootIDs map[string]uint64, policies *[]backenddb.OrderedRootStoragePolicy, deltaTables *[]memtable.Table, rootName string, baseRootID uint64, policy backenddb.OrderedRootStoragePolicy, table memtable.Table) {
	*rootNames = append(*rootNames, rootName)
	baseRootIDs[rootName] = baseRootID
	*policies = append(*policies, policy)
	*deltaTables = append(*deltaTables, table)
}

func appendSingleTextV2IndexMutationDeltas(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	opts collectionOptions,
	def TextIndexDefinition,
	mutations []textDocumentMutation,
	rootNames *[]string,
	baseRootIDs map[string]uint64,
	policies *[]backenddb.OrderedRootStoragePolicy,
	deltaTables *[]memtable.Table,
) error {
	generationsRootName := collectionTextV2GenerationsRootName(catalog.meta.Name, def.Name)
	status, ok, err := readTextV2StatusAtRoot(snap, catalog, generationsRootName)
	if err != nil {
		return err
	}
	if !ok {
		return errMalformedTextStorage("missing text-v2 status for collection %q index %q", catalog.meta.Name, def.Name)
	}
	orderedMutations := append([]textDocumentMutation(nil), mutations...)
	sort.Slice(orderedMutations, func(i, j int) bool {
		return bytes.Compare(orderedMutations[i].documentID, orderedMutations[j].documentID) < 0
	})

	docIDRootName := collectionTextV2DocIDRootName(catalog.meta.Name, def.Name)
	docMapRootName := collectionTextV2DocMapRootName(catalog.meta.Name, def.Name)
	termsRootName := collectionTextV2TermsRootName(catalog.meta.Name, def.Name)
	postingBlocksRootName := collectionTextV2PostingBlocksRootName(catalog.meta.Name, def.Name)
	normRootName := collectionTextV2NormBlocksRootName(catalog.meta.Name, def.Name)
	positionsRootName := collectionTextV2PositionsRootName(catalog.meta.Name, def.Name)
	docIDTable := newCollectionRunTable(len(orderedMutations))
	docMapTable := newCollectionRunTable(0)
	termsTable := newCollectionRunTable(0)
	postingBlocksTable := newCollectionRunTable(0)
	normTable := newCollectionRunTable(0)
	positionsTable := newCollectionRunTable(0)
	generationsTable := newCollectionRunTable(1)
	deltaOwned := []memtable.Table{docIDTable, docMapTable, termsTable, postingBlocksTable, normTable, positionsTable, generationsTable}
	success := false
	defer func() {
		if !success {
			resetCollectionTables(deltaOwned)
		}
	}()

	docMapBlocks := make(map[uint64]*textV2DocMapBlockValue)
	normBlocks := make(map[uint64]*textV2NormBlockValue)
	var postingBuilder textV2PostingBatchBuilder
	statsDelta := textStatsDelta{}
	nextOrdinal := status.NextOrdinal
	liveDocuments := status.LiveDocuments
	deletedDocuments := status.DeletedDocuments
	nextGeneration := status.RootGeneration + 1
	if nextGeneration == 0 {
		return errMalformedTextStorage("text-v2 root generation overflow")
	}

	for _, mutation := range orderedMutations {
		if len(mutation.documentID) == 0 {
			return errors.New("collections: text-v2 index maintenance document id cannot be empty")
		}
		current, hasCurrent, err := readTextV2DocIDAtRoot(snap, catalog, docIDRootName, mutation.documentID)
		if err != nil {
			return err
		}

		var oldState textDocumentStateValue
		var newState textDocumentStateValue
		var newAnalysis textAnalyzedDocument
		if mutation.deleteOld {
			if !hasCurrent || current.tombstoned() {
				return errMalformedTextStorage("missing live text-v2 ordinal for delete collection %q index %q document %q", catalog.meta.Name, def.Name, string(mutation.documentID))
			}
			oldDocument, err := loadTextV2StoredDocumentForMutation(snap, catalog, mutation.documentID, mutation.oldDocument)
			if err != nil {
				return err
			}
			oldState, err = analyzeTextIndexStoredDocument(def, oldDocument, opts)
			if err != nil {
				return err
			}
		}
		if mutation.setNew {
			if mutation.preparedNew != nil {
				newState = *mutation.preparedNew
			} else {
				newState, newAnalysis, err = analyzeTextIndexStoredDocumentWithAnalysis(def, mutation.newDocument, opts)
				if err != nil {
					return err
				}
			}
		}

		var nextDoc textV2DocIDValue
		switch {
		case mutation.deleteOld && mutation.setNew:
			nextDoc = textV2DocIDValue{Ordinal: current.Ordinal, Generation: current.Generation + 1}
			if nextDoc.Generation == 0 {
				return errMalformedTextStorage("text-v2 generation overflow for document %q", string(mutation.documentID))
			}
			statsDelta.addState(oldState, -1)
			statsDelta.addState(newState, 1)
		case mutation.deleteOld:
			nextDoc = textV2DocIDValue{Ordinal: current.Ordinal, Generation: current.Generation + 1, Flags: textV2DocFlagTombstone}
			if nextDoc.Generation == 0 {
				return errMalformedTextStorage("text-v2 generation overflow for document %q", string(mutation.documentID))
			}
			statsDelta.addState(oldState, -1)
			if liveDocuments == 0 {
				return errMalformedTextStorage("text-v2 live document underflow")
			}
			liveDocuments--
			deletedDocuments++
		case mutation.setNew:
			if hasCurrent && !current.tombstoned() {
				return errMalformedTextStorage("live text-v2 ordinal already exists for insert collection %q index %q document %q", catalog.meta.Name, def.Name, string(mutation.documentID))
			}
			nextDoc = textV2DocIDValue{Ordinal: nextOrdinal, Generation: 1}
			if nextDoc.Ordinal == 0 {
				return errMalformedTextStorage("text-v2 ordinal overflow")
			}
			nextOrdinal++
			if nextOrdinal == 0 {
				return errMalformedTextStorage("text-v2 next ordinal overflow")
			}
			statsDelta.addState(newState, 1)
			liveDocuments++
		default:
			continue
		}

		docIDTable.SetSteal(encodeTextV2DocIDKey(mutation.documentID), encodeTextV2DocIDValue(nextDoc))
		if err := upsertTextV2DocMapMutation(snap, catalog, docMapRootName, docMapBlocks, nextDoc, mutation.documentID); err != nil {
			return err
		}
		lengths := textV2FieldLengthsFromState(def, newState)
		if nextDoc.tombstoned() {
			lengths = textV2FieldLengthsFromState(def, oldState)
		}
		if err := upsertTextV2NormMutation(snap, catalog, normRootName, normBlocks, nextDoc, lengths); err != nil {
			return err
		}
		if mutation.deleteOld {
			deleteTextV2PositionEntriesForDocument(positionsTable, def, current.Ordinal, oldState)
		}
		if mutation.setNew {
			if mutation.preparedNew != nil {
				if err := postingBuilder.addDocumentState(def, nextDoc.Ordinal, nextDoc.Generation, newState); err != nil {
					return err
				}
				if _, _, err := addTextV2PositionEntriesForState(positionsTable, def, nextDoc.Ordinal, nextDoc.Generation, newState); err != nil {
					return err
				}
			} else {
				if err := postingBuilder.addDocument(def, nextDoc.Ordinal, nextDoc.Generation, newAnalysis); err != nil {
					return err
				}
				if _, _, err := addTextV2PositionEntriesForDocument(positionsTable, def, nextDoc.Ordinal, nextDoc.Generation, newAnalysis); err != nil {
					return err
				}
			}
		}
	}

	for _, blockStart := range sortedTextV2BlockStarts(docMapBlocks) {
		block := docMapBlocks[blockStart]
		docMapTable.SetSteal(encodeTextV2BlockKey(blockStart), encodeTextV2DocMapBlockValue(*block))
	}
	for _, blockStart := range sortedTextV2BlockStarts(normBlocks) {
		block := normBlocks[blockStart]
		normTable.SetSteal(encodeTextV2BlockKey(blockStart), encodeTextV2NormBlockValue(*block))
	}
	mutationBlockIDStart, err := textV2PostingBlockMutationBlockIDStart(nextGeneration)
	if err != nil {
		return err
	}
	_, _, postingBlockCounts, err := buildTextV2PostingBatchTable(postingBlocksTable, &postingBuilder, textV2PostingBlockKindMicro, textV2PostingBlockMicroPostings, mutationBlockIDStart)
	if err != nil {
		return err
	}
	statsDelta.addPostingBlocks(postingBlockCounts)
	if err := buildTextV2StatsDeltaTable(snap, catalog, def, termsRootName, statsDelta, nextGeneration, termsTable); err != nil {
		return err
	}
	nextStatus := textV2IndexStatusValue{
		FormatVersion:    textV2FormatVersion,
		RootGeneration:   nextGeneration,
		StatsGeneration:  nextGeneration,
		DocMapGeneration: nextGeneration,
		NormGeneration:   nextGeneration,
		TermGeneration:   nextGeneration,
		NextOrdinal:      nextOrdinal,
		LiveDocuments:    liveDocuments,
		DeletedDocuments: deletedDocuments,
	}
	generationsTable.SetSteal(encodeTextV2StatusKey(), encodeTextV2IndexStatusValue(nextStatus))

	appendIfNonEmpty := func(rootName string, table memtable.Table) error {
		if table == nil || table.Len() == 0 {
			return nil
		}
		table.Freeze()
		policy, err := collectionRootStoragePolicyForDB(nil, catalog.meta, rootName)
		if err != nil {
			return err
		}
		appendTextRootDelta(rootNames, baseRootIDs, policies, deltaTables, rootName, catalog.rootID(rootName), policy, table)
		return nil
	}
	if err := appendIfNonEmpty(docIDRootName, docIDTable); err != nil {
		return err
	}
	if err := appendIfNonEmpty(docMapRootName, docMapTable); err != nil {
		return err
	}
	if err := appendIfNonEmpty(termsRootName, termsTable); err != nil {
		return err
	}
	if err := appendIfNonEmpty(postingBlocksRootName, postingBlocksTable); err != nil {
		return err
	}
	if err := appendIfNonEmpty(normRootName, normTable); err != nil {
		return err
	}
	if err := appendIfNonEmpty(positionsRootName, positionsTable); err != nil {
		return err
	}
	if err := appendIfNonEmpty(generationsRootName, generationsTable); err != nil {
		return err
	}
	success = true
	return nil
}

func readTextV2DocIDAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, documentID []byte) (textV2DocIDValue, bool, error) {
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextV2DocIDKey(documentID), nil)
	if err != nil || !ok {
		return textV2DocIDValue{}, ok, err
	}
	value, err := decodeTextV2DocIDValue(raw)
	return value, true, err
}

func loadTextV2StoredDocumentForMutation(snap *backenddb.Snapshot, catalog *collectionCatalog, documentID, fallback []byte) ([]byte, error) {
	if fallback != nil {
		return fallback, nil
	}
	primaryRootName := collectionPrimaryRootName(catalog.meta.Name)
	if catalog.primaryRootName != "" {
		primaryRootName = catalog.primaryRootName
	}
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, primaryRootName, documentID, nil)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errMalformedTextStorage("missing primary document for text-v2 mutation document %q", string(documentID))
	}
	return raw, nil
}

func upsertTextV2DocMapMutation(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, blocks map[uint64]*textV2DocMapBlockValue, doc textV2DocIDValue, documentID []byte) error {
	blockStart := textV2OrdinalBlockStart(doc.Ordinal, textV2DefaultDocMapBlockSize)
	block, err := loadTextV2DocMapMutationBlock(snap, catalog, rootName, blocks, blockStart)
	if err != nil {
		return err
	}
	block.upsert(textV2DocMapEntry{Ordinal: doc.Ordinal, Generation: doc.Generation, Flags: doc.Flags, DocumentID: documentID})
	return nil
}

func loadTextV2DocMapMutationBlock(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, blocks map[uint64]*textV2DocMapBlockValue, blockStart uint64) (*textV2DocMapBlockValue, error) {
	if block := blocks[blockStart]; block != nil {
		return block, nil
	}
	block := &textV2DocMapBlockValue{BlockStart: blockStart, BlockSize: textV2DefaultDocMapBlockSize}
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextV2BlockKey(blockStart), nil)
	if err != nil {
		return nil, err
	}
	if ok {
		decoded, err := decodeTextV2DocMapBlockValue(raw)
		if err != nil {
			return nil, err
		}
		block = &decoded
	}
	blocks[blockStart] = block
	return block, nil
}

func upsertTextV2NormMutation(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, blocks map[uint64]*textV2NormBlockValue, doc textV2DocIDValue, fieldLengths []uint32) error {
	blockStart := textV2OrdinalBlockStart(doc.Ordinal, textV2DefaultNormBlockSize)
	block, err := loadTextV2NormMutationBlock(snap, catalog, rootName, blocks, blockStart, uint32(len(fieldLengths)))
	if err != nil {
		return err
	}
	block.upsert(textV2NormBlockEntry{Ordinal: doc.Ordinal, Generation: doc.Generation, Flags: doc.Flags, FieldLengths: fieldLengths})
	return nil
}

func loadTextV2NormMutationBlock(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, blocks map[uint64]*textV2NormBlockValue, blockStart uint64, fieldCount uint32) (*textV2NormBlockValue, error) {
	if block := blocks[blockStart]; block != nil {
		return block, nil
	}
	block := &textV2NormBlockValue{BlockStart: blockStart, BlockSize: textV2DefaultNormBlockSize, FieldCount: fieldCount}
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextV2BlockKey(blockStart), nil)
	if err != nil {
		return nil, err
	}
	if ok {
		decoded, err := decodeTextV2NormBlockValue(raw)
		if err != nil {
			return nil, err
		}
		block = &decoded
	}
	blocks[blockStart] = block
	return block, nil
}

func textV2FieldLengthsFromState(def TextIndexDefinition, state textDocumentStateValue) []uint32 {
	byField := make(map[string]uint32, len(state.Fields))
	for _, field := range state.Fields {
		byField[field.Field] = field.Length
	}
	lengths := make([]uint32, 0, len(def.Fields))
	for _, field := range def.Fields {
		lengths = append(lengths, byField[field.Field])
	}
	return lengths
}

func buildTextV2StatsDeltaTable(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, rootName string, delta textStatsDelta, generation uint64, table memtable.Table) error {
	if table == nil {
		return nil
	}
	currentCorpus, err := readTextV2CorpusStatsAtRoot(snap, catalog, rootName)
	if err != nil {
		return err
	}
	nextDocuments, err := applySignedTextDelta(currentCorpus.DocumentCount, delta.Documents, "text-v2 corpus document count")
	if err != nil {
		return err
	}
	table.SetSteal(encodeTextV2CorpusStatsKey(), encodeTextV2CorpusStatsValue(textV2CorpusStatsValue{StatsGeneration: generation, DocumentCount: nextDocuments}))

	for _, fieldDef := range def.Fields {
		field := fieldDef.Field
		current, err := readTextV2FieldStatsAtRoot(snap, catalog, rootName, field)
		if err != nil {
			return err
		}
		fieldDelta := delta.Fields[field]
		docs, err := applySignedTextDelta(current.DocumentCount, fieldDelta.DocumentCount, "text-v2 field document count")
		if err != nil {
			return err
		}
		tokens, err := applySignedTextDelta(current.TotalTokenCount, fieldDelta.TotalTokenCount, "text-v2 field token count")
		if err != nil {
			return err
		}
		table.SetSteal(encodeTextV2FieldStatsKey(field), encodeTextV2FieldStatsValue(textV2FieldStatsValue{StatsGeneration: generation, DocumentCount: docs, TotalTokenCount: tokens}))
	}

	termSet := make(map[string]struct{}, len(delta.Terms)+len(delta.PostingBlocks))
	for term, termDelta := range delta.Terms {
		if termDelta.DocumentFrequency == 0 && termDelta.TotalTermFrequency == 0 {
			continue
		}
		termSet[term] = struct{}{}
	}
	for term, blockDelta := range delta.PostingBlocks {
		if blockDelta == 0 {
			continue
		}
		termSet[term] = struct{}{}
	}
	terms := make([]string, 0, len(termSet))
	for term := range termSet {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	for _, term := range terms {
		termDelta := delta.Terms[term]
		current, err := readTextV2TermStatsAtRoot(snap, catalog, rootName, term)
		if err != nil {
			return err
		}
		df, err := applySignedTextDelta(current.DocumentFrequency, termDelta.DocumentFrequency, "text-v2 term document frequency")
		if err != nil {
			return err
		}
		tf, err := applySignedTextDelta(current.TotalTermFrequency, termDelta.TotalTermFrequency, "text-v2 term total frequency")
		if err != nil {
			return err
		}
		blocks, err := applySignedTextDelta(current.PostingBlockCount, delta.PostingBlocks[term], "text-v2 term posting block count")
		if err != nil {
			return err
		}
		key := encodeTextV2TermStatsKey(term)
		if df == 0 && tf == 0 && blocks == 0 {
			table.DeleteSteal(key)
		} else {
			table.SetSteal(key, encodeTextV2TermStatsValue(textV2TermStatsValue{StatsGeneration: generation, DocumentFrequency: df, TotalTermFrequency: tf, PostingBlockCount: blocks}))
		}
	}
	return nil
}

func readTextV2CorpusStatsAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string) (textV2CorpusStatsValue, error) {
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextV2CorpusStatsKey(), nil)
	if err != nil {
		return textV2CorpusStatsValue{}, err
	}
	if !ok {
		return textV2CorpusStatsValue{}, errMalformedTextStorage("missing text-v2 corpus stats in root %q", rootName)
	}
	return decodeTextV2CorpusStatsValue(raw)
}

func readTextV2TermStatsAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName, term string) (textV2TermStatsValue, error) {
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextV2TermStatsKey(term), nil)
	if err != nil {
		return textV2TermStatsValue{}, err
	}
	if !ok {
		return textV2TermStatsValue{}, nil
	}
	return decodeTextV2TermStatsValue(raw)
}

func readTextV2FieldStatsAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName, field string) (textV2FieldStatsValue, error) {
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextV2FieldStatsKey(field), nil)
	if err != nil {
		return textV2FieldStatsValue{}, err
	}
	if !ok {
		return textV2FieldStatsValue{}, errMalformedTextStorage("missing text-v2 field stats %q in root %q", field, rootName)
	}
	return decodeTextV2FieldStatsValue(raw)
}

func analyzeTextIndexStoredDocument(def TextIndexDefinition, document []byte, opts collectionOptions) (textDocumentStateValue, error) {
	state, _, err := analyzeTextIndexStoredDocumentWithAnalysis(def, document, opts)
	return state, err
}

func analyzeTextIndexStoredDocumentWithAnalysis(def TextIndexDefinition, document []byte, opts collectionOptions) (textDocumentStateValue, textAnalyzedDocument, error) {
	jsonDocument, err := materializeTextBackfillDocumentJSON(document, opts)
	if err != nil {
		return textDocumentStateValue{}, textAnalyzedDocument{}, err
	}
	analysis, err := analyzeTextIndexDocument(def, jsonDocument)
	if err != nil {
		return textDocumentStateValue{}, textAnalyzedDocument{}, err
	}
	return textDocumentStateValueFromAnalysis(analysis), analysis, nil
}

func textMutationNewStateAndAnalysis(def TextIndexDefinition, mutation textDocumentMutation, opts collectionOptions) (textDocumentStateValue, textAnalyzedDocument, error) {
	if mutation.preparedNew != nil {
		return *mutation.preparedNew, textAnalysisFromDocumentState(*mutation.preparedNew), nil
	}
	return analyzeTextIndexStoredDocumentWithAnalysis(def, mutation.newDocument, opts)
}

func loadTextDocumentStateForMutation(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, documentID, fallbackDocument []byte, opts collectionOptions) (textDocumentStateValue, error) {
	stateRootName := collectionTextStateRootName(catalog.meta.Name, def.Name)
	stateKey := encodeTextStateKey(documentID)
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, stateRootName, stateKey, nil)
	if err != nil {
		return textDocumentStateValue{}, err
	}
	if ok {
		return decodeTextDocumentStateValue(raw)
	}
	if fallbackDocument != nil {
		return analyzeTextIndexStoredDocument(def, fallbackDocument, opts)
	}
	return textDocumentStateValue{}, errMalformedTextStorage("missing text-state for collection %q index %q document %q", catalog.meta.Name, def.Name, string(documentID))
}

func deleteTextPostingsForDocument(table memtable.Table, documentID []byte, state textDocumentStateValue) int {
	if table == nil {
		return 0
	}
	terms := textDocumentStateTerms(state)
	for _, term := range terms {
		table.DeleteSteal(encodeTextPostingKey(term, documentID))
	}
	return len(terms)
}

func textDocumentStateTerms(state textDocumentStateValue) []string {
	seen := make(map[string]struct{})
	for _, field := range state.Fields {
		for _, term := range field.Terms {
			seen[term.Term] = struct{}{}
		}
	}
	terms := make([]string, 0, len(seen))
	for term := range seen {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	return terms
}

func textAnalysisFromDocumentState(state textDocumentStateValue) textAnalyzedDocument {
	analysis := textAnalyzedDocument{Fields: make([]textAnalyzedField, 0, len(state.Fields))}
	for _, fieldState := range state.Fields {
		field := textAnalyzedField{Field: fieldState.Field, Length: fieldState.Length, Terms: make(map[string]*textAnalyzedTerm, len(fieldState.Terms))}
		for _, termState := range fieldState.Terms {
			field.Terms[termState.Term] = &textAnalyzedTerm{
				Term:      termState.Term,
				Frequency: termState.Frequency,
				Positions: append([]uint32(nil), termState.Positions...),
				Offsets:   append([]textTokenOffset(nil), termState.Offsets...),
			}
		}
		analysis.Fields = append(analysis.Fields, field)
	}
	return analysis
}

func (d *textStatsDelta) addState(state textDocumentStateValue, sign int64) {
	if sign == 0 {
		return
	}
	d.Documents += sign
	if d.Terms == nil {
		d.Terms = make(map[string]textStatsTermDelta)
	}
	if d.Fields == nil {
		d.Fields = make(map[string]textStatsFieldDelta)
	}
	seenTerms := make(map[string]uint64)
	for _, field := range state.Fields {
		fieldDelta := d.Fields[field.Field]
		fieldDelta.DocumentCount += sign
		fieldDelta.TotalTokenCount += signedTextDelta(field.Length, sign)
		d.Fields[field.Field] = fieldDelta
		for _, term := range field.Terms {
			seenTerms[term.Term] += uint64(term.Frequency)
		}
	}
	for term, freq := range seenTerms {
		termDelta := d.Terms[term]
		termDelta.DocumentFrequency += sign
		termDelta.TotalTermFrequency += signedTextDeltaUint64(freq, sign)
		d.Terms[term] = termDelta
	}
}

func (d *textStatsDelta) addPostingBlocks(blockCounts map[string]uint64) {
	if len(blockCounts) == 0 {
		return
	}
	if d.PostingBlocks == nil {
		d.PostingBlocks = make(map[string]int64, len(blockCounts))
	}
	for term, count := range blockCounts {
		d.PostingBlocks[term] += signedTextDeltaUint64(count, 1)
	}
}

func signedTextDelta(value uint32, sign int64) int64 {
	return signedTextDeltaUint64(uint64(value), sign)
}

func signedTextDeltaUint64(value uint64, sign int64) int64 {
	if sign < 0 {
		if value > uint64(^uint64(0)>>1) {
			return -int64(^uint64(0) >> 1)
		}
		return -int64(value)
	}
	if value > uint64(^uint64(0)>>1) {
		return int64(^uint64(0) >> 1)
	}
	return int64(value)
}

func buildTextStatsDeltaTable(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, delta textStatsDelta, table memtable.Table) error {
	if table == nil {
		return nil
	}
	if delta.Documents != 0 {
		current, err := readTextStatsCorpusAtRoot(snap, catalog, rootName)
		if err != nil {
			return err
		}
		next, err := applySignedTextDelta(current.DocumentCount, delta.Documents, "text-stats corpus document count")
		if err != nil {
			return err
		}
		table.SetSteal(encodeTextStatsCorpusKey(), encodeTextStatsCorpusValue(textStatsCorpusValue{DocumentCount: next}))
	}
	terms := make([]string, 0, len(delta.Terms))
	for term, termDelta := range delta.Terms {
		if termDelta.DocumentFrequency == 0 && termDelta.TotalTermFrequency == 0 {
			continue
		}
		terms = append(terms, term)
	}
	sort.Strings(terms)
	for _, term := range terms {
		termDelta := delta.Terms[term]
		current, err := readTextStatsTermAtRoot(snap, catalog, rootName, term)
		if err != nil {
			return err
		}
		df, err := applySignedTextDelta(current.DocumentFrequency, termDelta.DocumentFrequency, "text-stats term document frequency")
		if err != nil {
			return err
		}
		tf, err := applySignedTextDelta(current.TotalTermFrequency, termDelta.TotalTermFrequency, "text-stats term total frequency")
		if err != nil {
			return err
		}
		key := encodeTextStatsTermKey(term)
		if df == 0 && tf == 0 {
			table.DeleteSteal(key)
		} else {
			table.SetSteal(key, encodeTextStatsTermValue(textStatsTermValue{DocumentFrequency: df, TotalTermFrequency: tf}))
		}
	}
	fields := make([]string, 0, len(delta.Fields))
	for field, fieldDelta := range delta.Fields {
		if fieldDelta.DocumentCount == 0 && fieldDelta.TotalTokenCount == 0 {
			continue
		}
		fields = append(fields, field)
	}
	sort.Strings(fields)
	for _, field := range fields {
		fieldDelta := delta.Fields[field]
		current, err := readTextStatsFieldAtRoot(snap, catalog, rootName, field)
		if err != nil {
			return err
		}
		docs, err := applySignedTextDelta(current.DocumentCount, fieldDelta.DocumentCount, "text-stats field document count")
		if err != nil {
			return err
		}
		tokens, err := applySignedTextDelta(current.TotalTokenCount, fieldDelta.TotalTokenCount, "text-stats field token count")
		if err != nil {
			return err
		}
		key := encodeTextStatsFieldKey(field)
		if docs == 0 && tokens == 0 {
			table.DeleteSteal(key)
		} else {
			table.SetSteal(key, encodeTextStatsFieldValue(textStatsFieldValue{DocumentCount: docs, TotalTokenCount: tokens}))
		}
	}
	return nil
}

func readTextStatsCorpusAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string) (textStatsCorpusValue, error) {
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextStatsCorpusKey(), nil)
	if err != nil {
		return textStatsCorpusValue{}, err
	}
	if !ok {
		return textStatsCorpusValue{}, nil
	}
	return decodeTextStatsCorpusValue(raw)
}

func readTextStatsTermAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName, term string) (textStatsTermValue, error) {
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextStatsTermKey(term), nil)
	if err != nil {
		return textStatsTermValue{}, err
	}
	if !ok {
		return textStatsTermValue{}, nil
	}
	return decodeTextStatsTermValue(raw)
}

func readTextStatsFieldAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName, field string) (textStatsFieldValue, error) {
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextStatsFieldKey(field), nil)
	if err != nil {
		return textStatsFieldValue{}, err
	}
	if !ok {
		return textStatsFieldValue{}, nil
	}
	return decodeTextStatsFieldValue(raw)
}

func applySignedTextDelta(current uint64, delta int64, label string) (uint64, error) {
	if delta < 0 {
		amount := uint64(-delta)
		if amount > current {
			return 0, errMalformedTextStorage("%s underflow applying delta %d to %d", label, delta, current)
		}
		return current - amount, nil
	}
	amount := uint64(delta)
	if amount > ^uint64(0)-current {
		return 0, errMalformedTextStorage("%s overflow applying delta %d to %d", label, delta, current)
	}
	return current + amount, nil
}

func appendTextIndexInsertPlanDeltas(snap *backenddb.Snapshot, catalog *collectionCatalog, opts collectionOptions, plan *insertBatchPlan) error {
	if plan == nil || len(catalog.meta.TextIndexes) == 0 {
		return nil
	}
	if plan.templateResolver != nil {
		opts.templateResolver = plan.templateResolver
	}
	primaryRootName := collectionPrimaryRootName(catalog.meta.Name)
	if catalog.primaryRootName != "" {
		primaryRootName = catalog.primaryRootName
	}
	var primaryRun *collectionRootRun
	for i := range plan.runs {
		if plan.runs[i].name == primaryRootName || plan.runs[i].kind == collectionRootPrimary {
			primaryRun = &plan.runs[i]
			break
		}
	}
	if primaryRun == nil || primaryRun.table == nil {
		return nil
	}
	it := primaryRun.table.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	mutations := make([]textDocumentMutation, 0, primaryRun.table.Len())
	for it.Valid() {
		if !it.IsDeleted() {
			mutations = append(mutations, textDocumentMutation{
				documentID:  bytes.Clone(it.UnsafeKey()),
				newDocument: it.ValueCopy(nil),
				setNew:      true,
			})
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	if len(mutations) == 0 {
		return nil
	}
	return appendTextIndexInsertMutationDeltas(snap, catalog, opts, mutations, plan)
}

func appendTextIndexInsertDocumentsDeltas(snap *backenddb.Snapshot, catalog *collectionCatalog, opts collectionOptions, documents []columnWriteDocument, plan *insertBatchPlan) error {
	mutations := make([]textDocumentMutation, 0, len(documents))
	for _, document := range documents {
		mutations = append(mutations, textDocumentMutation{
			documentID:  document.ID,
			newDocument: document.Document,
			setNew:      true,
		})
	}
	return appendTextIndexInsertMutationDeltas(snap, catalog, opts, mutations, plan)
}

func prepareTextIndexInsertDocuments(meta CollectionMeta, opts collectionOptions, documents []columnWriteDocument) ([]preparedTextIndexInsert, error) {
	if len(meta.TextIndexes) == 0 {
		return nil, nil
	}
	if len(documents) == 0 {
		return nil, errors.New("collections: buffered text-index insert missing source documents")
	}
	prepared := make([]preparedTextIndexInsert, len(meta.TextIndexes))
	for index := range meta.TextIndexes {
		def := meta.TextIndexes[index]
		prepared[index] = preparedTextIndexInsert{
			indexName: def.Name,
			states:    make([]textDocumentStateValue, len(documents)),
		}
		for row := range documents {
			state, err := analyzeTextIndexStoredDocument(def, documents[row].Document, opts)
			if err != nil {
				return nil, err
			}
			prepared[index].states[row] = state
		}
	}
	return prepared, nil
}

func validatePreparedTextIndexInserts(meta CollectionMeta, documents []columnWriteDocument, prepared []preparedTextIndexInsert) error {
	if len(meta.TextIndexes) != len(prepared) {
		return fmt.Errorf("collections: prepared text indexes=%d want %d", len(prepared), len(meta.TextIndexes))
	}
	for index := range prepared {
		if prepared[index].indexName != meta.TextIndexes[index].Name {
			return fmt.Errorf("collections: prepared text index %d name=%q want %q", index, prepared[index].indexName, meta.TextIndexes[index].Name)
		}
		if len(prepared[index].states) != len(documents) {
			return fmt.Errorf("collections: prepared text index %q documents=%d want %d", prepared[index].indexName, len(prepared[index].states), len(documents))
		}
	}
	return nil
}

func appendPreparedTextIndexInsertDeltas(snap *backenddb.Snapshot, catalog *collectionCatalog, opts collectionOptions, documents []columnWriteDocument, prepared []preparedTextIndexInsert, plan *insertBatchPlan) error {
	if plan == nil || len(prepared) == 0 {
		return nil
	}
	if err := validatePreparedTextIndexInserts(catalog.meta, documents, prepared); err != nil {
		return err
	}
	rootNames := make([]string, 0, len(prepared)*3)
	baseRootIDs := make(map[string]uint64, len(prepared)*3)
	policies := make([]backenddb.OrderedRootStoragePolicy, 0, len(prepared)*3)
	deltaTables := make([]memtable.Table, 0, len(prepared)*3)
	for index, def := range catalog.meta.TextIndexes {
		mutations := make([]textDocumentMutation, len(documents))
		for row := range documents {
			mutations[row] = textDocumentMutation{
				documentID:  documents[row].ID,
				preparedNew: &prepared[index].states[row],
				setNew:      true,
			}
		}
		if err := appendSingleTextIndexMutationDeltas(snap, catalog, opts, def, mutations, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
			resetCollectionTables(deltaTables)
			return err
		}
	}
	for index, rootName := range rootNames {
		plan.runs = append(plan.runs, collectionRootRun{
			name:          rootName,
			table:         deltaTables[index],
			storagePolicy: policies[index],
		})
	}
	plan.stats.Runs = len(plan.runs)
	return nil
}

func appendTextIndexInsertMutationDeltas(snap *backenddb.Snapshot, catalog *collectionCatalog, opts collectionOptions, mutations []textDocumentMutation, plan *insertBatchPlan) error {
	if plan == nil || len(mutations) == 0 {
		return nil
	}
	rootNames := make([]string, 0, len(catalog.meta.TextIndexes)*3)
	baseRootIDs := make(map[string]uint64, len(catalog.meta.TextIndexes)*3)
	policies := make([]backenddb.OrderedRootStoragePolicy, 0, len(catalog.meta.TextIndexes)*3)
	deltaTables := make([]memtable.Table, 0, len(catalog.meta.TextIndexes)*3)
	if err := appendTextIndexMutationDeltas(snap, catalog, opts, mutations, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
		resetCollectionTables(deltaTables)
		return err
	}
	for i, rootName := range rootNames {
		plan.runs = append(plan.runs, collectionRootRun{
			name:          rootName,
			table:         deltaTables[i],
			storagePolicy: policies[i],
		})
	}
	plan.stats.Runs = len(plan.runs)
	return nil
}

func appendTextIndexNoIndexInsertDeltas(snap *backenddb.Snapshot, catalog *collectionCatalog, opts collectionOptions, entries []noIndexBatchEntry, rootNames *[]string, baseRootIDs map[string]uint64, policies *[]backenddb.OrderedRootStoragePolicy, deltaTables *[]memtable.Table) error {
	if len(catalog.meta.TextIndexes) == 0 || len(entries) == 0 {
		return nil
	}
	mutations := make([]textDocumentMutation, 0, len(entries))
	for _, entry := range entries {
		mutations = append(mutations, textDocumentMutation{documentID: entry.id, newDocument: entry.document, setNew: true})
	}
	return appendTextIndexMutationDeltas(snap, catalog, opts, mutations, rootNames, baseRootIDs, policies, deltaTables)
}

func appendTextIndexDeleteDeltas(snap *backenddb.Snapshot, catalog *collectionCatalog, opts collectionOptions, documentIDs [][]byte, fallbackDocuments [][]byte, rootNames *[]string, baseRootIDs map[string]uint64, policies *[]backenddb.OrderedRootStoragePolicy, deltaTables *[]memtable.Table) error {
	if len(catalog.meta.TextIndexes) == 0 || len(documentIDs) == 0 {
		return nil
	}
	mutations := make([]textDocumentMutation, 0, len(documentIDs))
	for i, id := range documentIDs {
		var fallback []byte
		if i < len(fallbackDocuments) {
			fallback = fallbackDocuments[i]
		}
		mutations = append(mutations, textDocumentMutation{documentID: id, oldDocument: fallback, deleteOld: true})
	}
	return appendTextIndexMutationDeltas(snap, catalog, opts, mutations, rootNames, baseRootIDs, policies, deltaTables)
}

func appendTextIndexUpdateDeltas(snap *backenddb.Snapshot, catalog *collectionCatalog, opts collectionOptions, changed []preparedBatchUpdate, rootNames *[]string, baseRootIDs map[string]uint64, policies *[]backenddb.OrderedRootStoragePolicy, deltaTables *[]memtable.Table) error {
	if len(catalog.meta.TextIndexes) == 0 || len(changed) == 0 {
		return nil
	}
	mutations := make([]textDocumentMutation, 0, len(changed))
	for _, item := range changed {
		mutations = append(mutations, textDocumentMutation{documentID: item.documentID, deleteOld: true, newDocument: item.document, setNew: true})
	}
	return appendTextIndexMutationDeltas(snap, catalog, opts, mutations, rootNames, baseRootIDs, policies, deltaTables)
}
