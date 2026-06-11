package collections

import (
	"bytes"
	"errors"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type textDocumentMutation struct {
	documentID  []byte
	oldDocument []byte
	deleteOld   bool
	newDocument []byte
	setNew      bool
}

type textStatsDelta struct {
	Documents int64
	Terms     map[string]textStatsTermDelta
	Fields    map[string]textStatsFieldDelta
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
			newState, newAnalysis, err := analyzeTextIndexStoredDocumentWithAnalysis(def, mutation.newDocument, opts)
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
