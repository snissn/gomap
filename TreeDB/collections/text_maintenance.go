package collections

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sort"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type textDocumentMutation struct {
	documentID  []byte
	oldDocument []byte
	deleteOld   bool
	newDocument []byte
	preparedNew *textDocumentStateValue
	preparedOld *textDocumentStateValue
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
	Terms         textValues[textStatsTermDelta]
	Fields        textValues[textStatsFieldDelta]
	PostingBlocks textValues[int64]
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
	postingsTable := opts.newTextTable(0)
	stateTable := opts.newTextTable(len(mutations))
	statsDelta := textStatsDelta{Terms: textValues[textStatsTermDelta]{admit: opts.textMaterializationAdmit}, Fields: textValues[textStatsFieldDelta]{admit: opts.textMaterializationAdmit}, PostingBlocks: textValues[int64]{admit: opts.textMaterializationAdmit}}
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
			oldState, err := loadTextDocumentStateForMutation(snap, catalog, def, mutation.documentID, mutation.oldDocument, opts, mutation.preparedOld)
			if err != nil {
				return err
			}
			if _, err := deleteTextPostingsForDocument(postingsTable, mutation.documentID, oldState, opts.textMaterializationAdmit); err != nil {
				return err
			}
			stateTable.DeleteSteal(encodeTextStateKey(mutation.documentID))
			if err := statsDelta.addState(oldState, -1); err != nil {
				return err
			}
		}
		if mutation.setNew {
			newState, newAnalysis, err := textMutationNewStateAndAnalysis(def, mutation, opts)
			if err != nil {
				return err
			}
			if opts.textMaterializationAdmit != nil {
				if err := addNativeTextPostingsForState(postingsTable, mutation.documentID, newState, opts.textMaterializationAdmit); err != nil {
					return err
				}
			} else {
				addTextPostingsForDocument(postingsTable, mutation.documentID, newAnalysis)
			}
			stateTable.SetSteal(encodeTextStateKey(mutation.documentID), encodeTextDocumentStateValue(newState))
			if err := statsDelta.addState(newState, 1); err != nil {
				return err
			}
		}
	}

	statsTable := opts.newTextTable(0)
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
	if baseRootIDs != nil {
		baseRootIDs[rootName] = baseRootID
	}
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
	typedOldStates, err := loadTypedTextOldStates(snap, catalog, opts, def, orderedMutations)
	if err != nil {
		return err
	}

	docIDRootName := collectionTextV2DocIDRootName(catalog.meta.Name, def.Name)
	docMapRootName := collectionTextV2DocMapRootName(catalog.meta.Name, def.Name)
	termsRootName := collectionTextV2TermsRootName(catalog.meta.Name, def.Name)
	postingBlocksRootName := collectionTextV2PostingBlocksRootName(catalog.meta.Name, def.Name)
	normRootName := collectionTextV2NormBlocksRootName(catalog.meta.Name, def.Name)
	positionsRootName := collectionTextV2PositionsRootName(catalog.meta.Name, def.Name)
	docIDTable := opts.newTextTable(len(orderedMutations))
	docMapTable := opts.newTextTable(0)
	termsTable := opts.newTextTable(0)
	postingBlocksTable := opts.newTextTable(0)
	normTable := opts.newTextTable(0)
	positionsTable := opts.newTextTable(0)
	generationsTable := opts.newTextTable(1)
	deltaOwned := []memtable.Table{docIDTable, docMapTable, termsTable, postingBlocksTable, normTable, positionsTable, generationsTable}
	success := false
	defer func() {
		if !success {
			resetCollectionTables(deltaOwned)
		}
	}()

	var docMapBlocks map[uint64]*textV2DocMapBlockValue
	var normBlocks map[uint64]*textV2NormBlockValue
	var docCache *textNumberValues[*textV2DocMapBlockValue]
	var normCache *textNumberValues[*textV2NormBlockValue]
	if opts.textMaterializationAdmit != nil {
		docCache, err = newTextNumberValues[*textV2DocMapBlockValue](opts.textMaterializationAdmit)
		if err != nil {
			return err
		}
		normCache, err = newTextNumberValues[*textV2NormBlockValue](opts.textMaterializationAdmit)
		if err != nil {
			return err
		}
	} else {
		docMapBlocks = make(map[uint64]*textV2DocMapBlockValue)
		normBlocks = make(map[uint64]*textV2NormBlockValue)
	}
	postingBuilder := textV2PostingBatchBuilder{admit: opts.textMaterializationAdmit}
	statsDelta := textStatsDelta{Terms: textValues[textStatsTermDelta]{admit: opts.textMaterializationAdmit}, Fields: textValues[textStatsFieldDelta]{admit: opts.textMaterializationAdmit}, PostingBlocks: textValues[int64]{admit: opts.textMaterializationAdmit}}
	nextOrdinal := status.NextOrdinal
	liveDocuments := status.LiveDocuments
	deletedDocuments := status.DeletedDocuments
	nextGeneration := status.RootGeneration + 1
	if nextGeneration == 0 {
		return errMalformedTextStorage("text-v2 root generation overflow")
	}

	for mutationIndex, mutation := range orderedMutations {
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
			if typedOldStates != nil {
				oldState = typedOldStates[mutationIndex]
			} else {
				oldDocument, err := loadTextV2StoredDocumentForMutation(snap, catalog, mutation.documentID, mutation.oldDocument)
				if err != nil {
					return err
				}
				oldState, err = analyzeTextIndexStoredDocument(def, oldDocument, opts)
				if err != nil {
					return err
				}
			}
		}
		if mutation.setNew {
			if mutation.preparedNew != nil {
				newState = *mutation.preparedNew
			} else {
				newState, newAnalysis, err = textMutationNewStateAndAnalysis(def, mutation, opts)
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
			if err := statsDelta.addState(oldState, -1); err != nil {
				return err
			}
			if err := statsDelta.addState(newState, 1); err != nil {
				return err
			}
		case mutation.deleteOld:
			nextDoc = textV2DocIDValue{Ordinal: current.Ordinal, Generation: current.Generation + 1, Flags: textV2DocFlagTombstone}
			if nextDoc.Generation == 0 {
				return errMalformedTextStorage("text-v2 generation overflow for document %q", string(mutation.documentID))
			}
			if err := statsDelta.addState(oldState, -1); err != nil {
				return err
			}
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
			if err := statsDelta.addState(newState, 1); err != nil {
				return err
			}
			liveDocuments++
		default:
			continue
		}

		docIDTable.SetSteal(encodeTextV2DocIDKey(mutation.documentID), encodeTextV2DocIDValue(nextDoc))
		if err := upsertTextV2DocMapMutation(snap, catalog, docMapRootName, docMapBlocks, nextDoc, mutation.documentID, docCache); err != nil {
			return err
		}
		lengths := textV2FieldLengthsFromState(def, newState)
		if nextDoc.tombstoned() {
			lengths = textV2FieldLengthsFromState(def, oldState)
		}
		if err := upsertTextV2NormMutation(snap, catalog, normRootName, normBlocks, nextDoc, lengths, normCache); err != nil {
			return err
		}
		if mutation.deleteOld {
			if _, err := deleteTextV2PositionEntriesForDocumentAdmitted(positionsTable, def, current.Ordinal, oldState, opts.textMaterializationAdmit); err != nil {
				return err
			}
		}
		if mutation.setNew {
			if mutation.preparedNew != nil || opts.textMaterializationAdmit != nil {
				if err := postingBuilder.addDocumentState(def, nextDoc.Ordinal, nextDoc.Generation, newState); err != nil {
					return err
				}
				if _, _, err := addTextV2PositionEntriesForState(positionsTable, def, nextDoc.Ordinal, nextDoc.Generation, newState, opts.textMaterializationAdmit); err != nil {
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

	if docCache != nil {
		for _, entry := range docCache.entries {
			docMapTable.SetSteal(encodeTextV2BlockKey(entry.key), encodeTextV2DocMapBlockValue(*entry.value))
		}
	}
	if normCache != nil {
		for _, entry := range normCache.entries {
			normTable.SetSteal(encodeTextV2BlockKey(entry.key), encodeTextV2NormBlockValue(*entry.value))
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
	if err := statsDelta.addPostingBlocks(postingBlockCounts); err != nil {
		return err
	}
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

func upsertTextV2DocMapMutation(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, blocks map[uint64]*textV2DocMapBlockValue, doc textV2DocIDValue, documentID []byte, cache ...*textNumberValues[*textV2DocMapBlockValue]) error {
	blockStart := textV2OrdinalBlockStart(doc.Ordinal, textV2DefaultDocMapBlockSize)
	block, err := loadTextV2DocMapMutationBlock(snap, catalog, rootName, blocks, blockStart, cache...)
	if err != nil {
		return err
	}
	block.upsert(textV2DocMapEntry{Ordinal: doc.Ordinal, Generation: doc.Generation, Flags: doc.Flags, DocumentID: documentID})
	return nil
}

func loadTextV2DocMapMutationBlock(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, blocks map[uint64]*textV2DocMapBlockValue, blockStart uint64, cache ...*textNumberValues[*textV2DocMapBlockValue]) (*textV2DocMapBlockValue, error) {
	if len(cache) > 0 && cache[0] != nil {
		if block := cache[0].get(blockStart); block != nil {
			return block, nil
		}
	}
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
	if len(cache) > 0 && cache[0] != nil {
		if err := cache[0].set(blockStart, block); err != nil {
			return nil, err
		}
	} else {
		blocks[blockStart] = block
	}
	return block, nil
}

func upsertTextV2NormMutation(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, blocks map[uint64]*textV2NormBlockValue, doc textV2DocIDValue, fieldLengths []uint32, cache ...*textNumberValues[*textV2NormBlockValue]) error {
	blockStart := textV2OrdinalBlockStart(doc.Ordinal, textV2DefaultNormBlockSize)
	block, err := loadTextV2NormMutationBlock(snap, catalog, rootName, blocks, blockStart, uint32(len(fieldLengths)), cache...)
	if err != nil {
		return err
	}
	block.upsert(textV2NormBlockEntry{Ordinal: doc.Ordinal, Generation: doc.Generation, Flags: doc.Flags, FieldLengths: fieldLengths})
	return nil
}

func loadTextV2NormMutationBlock(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, blocks map[uint64]*textV2NormBlockValue, blockStart uint64, fieldCount uint32, cache ...*textNumberValues[*textV2NormBlockValue]) (*textV2NormBlockValue, error) {
	if len(cache) > 0 && cache[0] != nil {
		if block := cache[0].get(blockStart); block != nil {
			return block, nil
		}
	}
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
	if len(cache) > 0 && cache[0] != nil {
		if err := cache[0].set(blockStart, block); err != nil {
			return nil, err
		}
	} else {
		blocks[blockStart] = block
	}
	return block, nil
}

func textV2FieldLengthsFromState(def TextIndexDefinition, state textDocumentStateValue) []uint32 {
	lengths := make([]uint32, len(def.Fields))
	for i, wanted := range def.Fields {
		for _, field := range state.Fields {
			if field.Field == wanted.Field {
				lengths[i] = field.Length
			}
		}
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
		fieldDelta := delta.Fields.get(field)
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

	termSet := textValues[struct{}]{admit: delta.Terms.admit}
	for term, value := range delta.Terms.all {
		if value.DocumentFrequency != 0 || value.TotalTermFrequency != 0 {
			if err := termSet.set(term, struct{}{}); err != nil {
				return err
			}
		}
	}
	for term, count := range delta.PostingBlocks.all {
		if count != 0 {
			if err := termSet.set(term, struct{}{}); err != nil {
				return err
			}
		}
	}
	terms, err := termSet.keys()
	if err != nil {
		return err
	}

	sort.Strings(terms)
	for _, term := range terms {
		termDelta := delta.Terms.get(term)
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
		blocks, err := applySignedTextDelta(current.PostingBlockCount, delta.PostingBlocks.get(term), "text-v2 term posting block count")
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

// Only the native mutation phase opts into source-only record copies for the
// exact maintained codecs below. Fixed V1/V2 stats contain no decoded backing;
// V1 state separately admits every backing through its failable decoder.
// General reads retain the original complete record allowance, including their
// inspection/rewrite/query cohorts. No record or decoder credit is refunded.
func textMutationCodecRecordAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, key []byte) ([]byte, bool, error) {
	if catalog != nil && catalog.nativeText != nil && catalog.nativeText.mutationCodecs {
		return catalog.nativeText.getAdmitted(rootName, key, nil, catalog.nativeText.admit)
	}
	return collectionGetAppendAtCatalogRoot(snap, catalog, rootName, key, nil)
}

func readTextV2CorpusStatsAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string) (textV2CorpusStatsValue, error) {
	raw, ok, err := textMutationCodecRecordAtRoot(snap, catalog, rootName, encodeTextV2CorpusStatsKey())
	if err != nil {
		return textV2CorpusStatsValue{}, err
	}
	if !ok {
		return textV2CorpusStatsValue{}, errMalformedTextStorage("missing text-v2 corpus stats in root %q", rootName)
	}
	return decodeTextV2CorpusStatsValue(raw)
}

func readTextV2TermStatsAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName, term string) (textV2TermStatsValue, error) {
	raw, ok, err := textMutationCodecRecordAtRoot(snap, catalog, rootName, encodeTextV2TermStatsKey(term))
	if err != nil {
		return textV2TermStatsValue{}, err
	}
	if !ok {
		return textV2TermStatsValue{}, nil
	}
	return decodeTextV2TermStatsValue(raw)
}

func readTextV2FieldStatsAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName, field string) (textV2FieldStatsValue, error) {
	raw, ok, err := textMutationCodecRecordAtRoot(snap, catalog, rootName, encodeTextV2FieldStatsKey(field))
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
	if opts.textMaterializationAdmit != nil {
		state, err := nativeTextDocumentState(def, jsonDocument, opts)
		return state, textAnalyzedDocument{}, err
	}
	analysis, err := analyzeTextIndexDocument(def, jsonDocument)
	if err != nil {
		return textDocumentStateValue{}, textAnalyzedDocument{}, err
	}
	return textDocumentStateValueFromAnalysis(analysis), analysis, nil
}

func textMutationNewStateAndAnalysis(def TextIndexDefinition, mutation textDocumentMutation, opts collectionOptions) (textDocumentStateValue, textAnalyzedDocument, error) {
	if mutation.preparedNew != nil {
		if opts.textMaterializationAdmit != nil {
			return *mutation.preparedNew, textAnalyzedDocument{}, nil
		}
		return *mutation.preparedNew, textAnalysisFromDocumentState(*mutation.preparedNew), nil
	}
	if p := opts.typedProjection; p != nil && p.typedRows != nil && !p.legacyTyped {
		values, ok := p.typedRows[string(mutation.documentID)]
		if !ok {
			return textDocumentStateValue{}, textAnalyzedDocument{}, errors.New("collections: typed text row missing")
		}
		analysis, err := typedTextAnalysis(def, values, p.columns)
		return textDocumentStateValueFromAnalysis(analysis), analysis, err
	}
	return analyzeTextIndexStoredDocumentWithAnalysis(def, mutation.newDocument, opts)
}

func loadTextDocumentStateForMutation(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, documentID, fallbackDocument []byte, opts collectionOptions, preparedOld *textDocumentStateValue) (textDocumentStateValue, error) {
	stateRootName := collectionTextStateRootName(catalog.meta.Name, def.Name)
	stateKey := encodeTextStateKey(documentID)
	raw, ok, err := textMutationCodecRecordAtRoot(snap, catalog, stateRootName, stateKey)
	if err != nil {
		return textDocumentStateValue{}, err
	}
	if ok {
		if catalog.nativeText != nil && catalog.nativeText.mutationCodecs {
			state, err := decodeTextDocumentStateValueAdmitted(raw, catalog.nativeText.admit)
			if err != nil {
				return textDocumentStateValue{}, err
			}
			// The opening-derived downstream promise applies only to exactly
			// that state. Arbitrary parseable persisted state retains the
			// original general record envelope before any reducer key/table
			// effect. This does not reject mismatches or refund decode custody.
			if preparedOld == nil || !textDocumentStatesEqual(state, *preparedOld) {
				if err := catalog.nativeText.admitRecord(memtable.COWAllocationCharge(uint64(len(raw)))); err != nil {
					return textDocumentStateValue{}, err
				}
			}
			return state, nil
		}
		return decodeTextDocumentStateValue(raw)
	}
	if fallbackDocument != nil {
		return analyzeTextIndexStoredDocument(def, fallbackDocument, opts)
	}
	return textDocumentStateValue{}, errMalformedTextStorage("missing text-state for collection %q index %q document %q", catalog.meta.Name, def.Name, string(documentID))
}

// Equality borrows both owned states and allocates no lookup or sorting lane.
// Check every decoded member, including zero-term fields and zero-frequency
// terms. A different ordering simply keeps the conservative general promise.
func textDocumentStatesEqual(a, b textDocumentStateValue) bool {
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i, field := range a.Fields {
		other := b.Fields[i]
		if field.Field != other.Field || field.Length != other.Length || len(field.Terms) != len(other.Terms) {
			return false
		}
		for j, term := range field.Terms {
			compared := other.Terms[j]
			if term.Term != compared.Term || term.Frequency != compared.Frequency || !slices.Equal(term.Positions, compared.Positions) || !slices.Equal(term.Offsets, compared.Offsets) {
				return false
			}
		}
	}
	return true
}

func deleteTextPostingsForDocument(table memtable.Table, documentID []byte, state textDocumentStateValue, admit func(uint64) error) (int, error) {
	if table == nil {
		return 0, nil
	}
	terms, err := textDocumentStateTerms(state, admit)
	if err != nil {
		return 0, err
	}
	for _, term := range terms {
		table.DeleteSteal(encodeTextPostingKey(term, documentID))
	}
	return len(terms), nil
}
func textDocumentStateTerms(state textDocumentStateValue, admit func(uint64) error) ([]string, error) {
	seen := textValues[struct{}]{admit: admit}
	for _, field := range state.Fields {
		for _, term := range field.Terms {
			if err := seen.set(term.Term, struct{}{}); err != nil {
				return nil, err
			}
		}
	}
	terms, err := seen.keys()
	if err != nil {
		return nil, err
	}
	sort.Strings(terms)
	return terms, nil
}
func addNativeTextPostingsForState(table memtable.Table, id []byte, state textDocumentStateValue, admit func(uint64) error) error {
	byTerm := textValues[textPostingValue]{admit: admit}
	for _, field := range state.Fields {
		for _, term := range field.Terms {
			posting := byTerm.get(term.Term)
			posting.TermFrequency += term.Frequency
			var err error
			posting.Fields, err = appendTextConstructor(posting.Fields, textPostingFieldValue{Field: field.Field, Frequency: term.Frequency, Positions: term.Positions, Offsets: term.Offsets}, admit)
			if err != nil {
				return err
			}
			if err = byTerm.set(term.Term, posting); err != nil {
				return err
			}
		}
	}
	terms, err := byTerm.keys()
	if err != nil {
		return err
	}
	sort.Strings(terms)
	for _, term := range terms {
		table.SetSteal(encodeTextPostingKey(term, id), encodeTextPostingValue(byTerm.get(term)))
	}
	return nil
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

func (d *textStatsDelta) addState(state textDocumentStateValue, sign int64) error {
	if sign == 0 {
		return nil
	}
	seen := textValues[uint64]{admit: d.Terms.admit}
	for _, field := range state.Fields {
		value := d.Fields.get(field.Field)
		value.DocumentCount += sign
		value.TotalTokenCount += signedTextDelta(field.Length, sign)
		if err := d.Fields.set(field.Field, value); err != nil {
			return err
		}
		for _, term := range field.Terms {
			if err := seen.set(term.Term, seen.get(term.Term)+uint64(term.Frequency)); err != nil {
				return err
			}
		}
	}
	for term, frequency := range seen.all {
		value := d.Terms.get(term)
		value.DocumentFrequency += sign
		value.TotalTermFrequency += signedTextDeltaUint64(frequency, sign)
		if err := d.Terms.set(term, value); err != nil {
			return err
		}
	}
	d.Documents += sign
	return nil
}
func (d *textStatsDelta) addPostingBlocks(counts textValues[uint64]) error {
	for term, count := range counts.all {
		if err := d.PostingBlocks.set(term, d.PostingBlocks.get(term)+signedTextDeltaUint64(count, 1)); err != nil {
			return err
		}
	}
	return nil
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
	if err := admitLocalIDArray(delta.Terms.admit, delta.Terms.len(), uint64(unsafe.Sizeof(string("")))); err != nil {
		return err
	}
	terms := make([]string, 0, delta.Terms.len())
	for term, termDelta := range delta.Terms.all {
		if termDelta.DocumentFrequency == 0 && termDelta.TotalTermFrequency == 0 {
			continue
		}
		terms = append(terms, term)
	}
	sort.Strings(terms)
	for _, term := range terms {
		termDelta := delta.Terms.get(term)
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
	if err := admitLocalIDArray(delta.Fields.admit, delta.Fields.len(), uint64(unsafe.Sizeof(string("")))); err != nil {
		return err
	}
	fields := make([]string, 0, delta.Fields.len())
	for field, fieldDelta := range delta.Fields.all {
		if fieldDelta.DocumentCount == 0 && fieldDelta.TotalTokenCount == 0 {
			continue
		}
		fields = append(fields, field)
	}
	sort.Strings(fields)
	for _, field := range fields {
		fieldDelta := delta.Fields.get(field)
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
	raw, ok, err := textMutationCodecRecordAtRoot(snap, catalog, rootName, encodeTextStatsCorpusKey())
	if err != nil {
		return textStatsCorpusValue{}, err
	}
	if !ok {
		return textStatsCorpusValue{}, nil
	}
	return decodeTextStatsCorpusValue(raw)
}

func readTextStatsTermAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName, term string) (textStatsTermValue, error) {
	raw, ok, err := textMutationCodecRecordAtRoot(snap, catalog, rootName, encodeTextStatsTermKey(term))
	if err != nil {
		return textStatsTermValue{}, err
	}
	if !ok {
		return textStatsTermValue{}, nil
	}
	return decodeTextStatsTermValue(raw)
}

func readTextStatsFieldAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName, field string) (textStatsFieldValue, error) {
	raw, ok, err := textMutationCodecRecordAtRoot(snap, catalog, rootName, encodeTextStatsFieldKey(field))
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
			state, _, err := textMutationNewStateAndAnalysis(def, textDocumentMutation{documentID: documents[row].ID, newDocument: documents[row].Document}, opts)
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
