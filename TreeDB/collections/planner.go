package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const documentIndexStateVersion = 1

type collectionRootKind uint8

const (
	collectionRootPrimary collectionRootKind = iota + 1
	collectionRootIndexState
	collectionRootSecondary
)

type collectionOptions struct {
	allowArrayValuesInIndex bool
	dataStoragePolicy       backenddb.OrderedRootStoragePolicy
	indexStateStoragePolicy backenddb.OrderedRootStoragePolicy
}

type indexDefinition struct {
	name          string
	field         string
	unique        bool
	multiKey      bool
	storagePolicy backenddb.OrderedRootStoragePolicy
}

type insertBatchPlanner struct {
	collection      string
	primaryRoot     string
	indexStateRoot  string
	indexes         []indexDefinition
	options         collectionOptions
	buildPrimaryVal func(documentID, document []byte) ([]byte, error)
}

type insertBatchPlan struct {
	resultIDs       [][]byte
	runs            []collectionRootRun
	uniqueProbeRuns []collectionUniqueProbeRun
	stats           insertBatchPlanStats
}

type insertBatchPlanStats struct {
	payloadBuilds int
}

type collectionRootRun struct {
	name          string
	kind          collectionRootKind
	indexName     string
	table         memtable.Table
	storagePolicy backenddb.OrderedRootStoragePolicy
}

type collectionUniqueProbeRun struct {
	indexName string
	prefixes  [][]byte
}

type insertBatchItem struct {
	id       []byte
	document []byte
	state    orderedDocumentIndexState
}

type indexRuntime struct {
	def  indexDefinition
	path []string
}

type uniqueProbeCandidate struct {
	indexName    string
	encodedValue []byte
	documentID   []byte
}

type documentIndexState map[string][][]byte
type orderedDocumentIndexState [][][]byte

type indexEncodeArena struct {
	buf []byte
}

type groupedRootPublisher interface {
	PublishOrderedRootGroup(systemIter iterator.UnsafeIterator, ordered []backenddb.OrderedRootPublishInput) (uint64, []uint64, error)
}

type rootSnapshotProbe interface {
	HasAnySortedAtRoot(rootID uint64, keys [][]byte) (bool, error)
	HasPrefixesAtRoot(rootID uint64, prefixes [][]byte) ([]bool, error)
}

type insertBatchPreflight struct {
	snapshot           rootSnapshotProbe
	primaryRootID      uint64
	uniqueIndexRootIDs map[string]uint64
}

func (p insertBatchPlanner) planInsertBatch(ids, documents [][]byte) (*insertBatchPlan, error) {
	return p.planInsertBatchWithPreflight(ids, documents, insertBatchPreflight{})
}

func (p insertBatchPlanner) planInsertBatchWithPreflight(ids, documents [][]byte, preflight insertBatchPreflight) (*insertBatchPlan, error) {
	if len(documents) == 0 {
		return &insertBatchPlan{}, nil
	}
	if len(ids) != len(documents) {
		return nil, fmt.Errorf("collections: caller-provided batch ids length mismatch")
	}
	if p.collection == "" {
		return nil, errors.New("collections: collection name cannot be empty")
	}
	if p.primaryRoot == "" {
		p.primaryRoot = p.collection + "/primary"
	}
	if len(p.indexes) > 0 && p.indexStateRoot == "" {
		p.indexStateRoot = p.collection + "/index-state"
	}
	if p.buildPrimaryVal == nil {
		p.buildPrimaryVal = borrowPrimaryDocument
	}

	resultIDs, err := cloneBatchDocumentIDs(ids)
	if err != nil {
		return nil, err
	}
	items := make([]insertBatchItem, len(documents))
	for i := range documents {
		id := resultIDs[i]
		items[i] = insertBatchItem{
			id:       id,
			document: documents[i],
		}
	}

	primaryOrder := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	if err := rejectDuplicateDocumentIDs(items, primaryOrder); err != nil {
		return nil, err
	}

	runtimes, err := p.indexRuntimes()
	if err != nil {
		return nil, err
	}
	uniqueProbes, err := p.planIndexStateAndUniqueProbes(items, runtimes)
	if err != nil {
		return nil, err
	}
	sortUniqueProbeCandidates(uniqueProbes)
	if err := rejectDuplicateUniqueProbeCandidates(uniqueProbes); err != nil {
		return nil, err
	}
	if err := preflight.checkDocumentConflicts(items, primaryOrder, resultIDs); err != nil {
		return nil, err
	}
	uniqueProbeRuns, err := buildUniqueProbeRunsForPreflightFromSorted(uniqueProbes, preflight)
	if err != nil {
		return nil, err
	}
	if err := preflight.checkUniqueConflicts(uniqueProbeRuns); err != nil {
		return nil, err
	}

	plan := &insertBatchPlan{
		resultIDs:       resultIDs,
		uniqueProbeRuns: uniqueProbeRuns,
	}
	if err := p.emitPrimaryRun(plan, items, primaryOrder); err != nil {
		return nil, err
	}
	if len(runtimes) > 0 {
		if err := p.emitIndexStateRun(plan, items, runtimes); err != nil {
			return nil, err
		}
		if err := p.emitSecondaryRuns(plan, items, runtimes); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func cloneBatchDocumentIDs(ids [][]byte) ([][]byte, error) {
	total := 0
	maxInt := int(^uint(0) >> 1)
	for _, id := range ids {
		if len(id) == 0 {
			return nil, errors.New("collections: document id cannot be empty")
		}
		if len(id) > maxInt-total {
			return nil, errors.New("collections: total document id bytes exceed maximum supported size")
		}
		total += len(id)
	}
	out := make([][]byte, len(ids))
	arena := make([]byte, 0, total)
	for i, id := range ids {
		start := len(arena)
		arena = append(arena, id...)
		out[i] = arena[start:len(arena):len(arena)]
	}
	return out, nil
}

func (p insertBatchPreflight) checkDocumentConflicts(items []insertBatchItem, order []int, presortedIDs [][]byte) error {
	if p.snapshot == nil || p.primaryRootID == 0 {
		return nil
	}
	// sortedItemOrderByKey returns nil only when items are already sorted by ID,
	// so the caller-owned presortedIDs slice is safe to reuse only in that case.
	keys := presortedIDs
	if order != nil || len(keys) != len(items) {
		keys = make([][]byte, len(items))
		for i := range items {
			keys[i] = items[orderedItemIndex(order, i)].id
		}
	}
	exists, err := p.snapshot.HasAnySortedAtRoot(p.primaryRootID, keys)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("collections: document already exists")
	}
	return nil
}

func (p insertBatchPreflight) checkUniqueConflicts(runs []collectionUniqueProbeRun) error {
	if p.snapshot == nil || len(p.uniqueIndexRootIDs) == 0 {
		return nil
	}
	for _, run := range runs {
		rootID := p.uniqueIndexRootIDs[run.indexName]
		if rootID == 0 {
			continue
		}
		exists, err := p.snapshot.HasPrefixesAtRoot(rootID, run.prefixes)
		if err != nil {
			return err
		}
		for _, ok := range exists {
			if ok {
				return fmt.Errorf("collections: unique index %q conflict", run.indexName)
			}
		}
	}
	return nil
}

func borrowPrimaryDocument(_, document []byte) ([]byte, error) {
	return document, nil
}

func rejectDuplicateDocumentIDs(items []insertBatchItem, order []int) error {
	for i := 1; i < len(items); i++ {
		if bytes.Equal(items[orderedItemIndex(order, i-1)].id, items[orderedItemIndex(order, i)].id) {
			return errors.New("collections: duplicate document id in batch")
		}
	}
	return nil
}

func (p insertBatchPlanner) indexRuntimes() ([]indexRuntime, error) {
	runtimes := make([]indexRuntime, len(p.indexes))
	seen := make(map[string]struct{}, len(p.indexes))
	for i, idx := range p.indexes {
		if idx.name == "" {
			return nil, errors.New("collections: index name cannot be empty")
		}
		if idx.field == "" {
			return nil, fmt.Errorf("collections: index %q field cannot be empty", idx.name)
		}
		if _, exists := seen[idx.name]; exists {
			return nil, fmt.Errorf("collections: duplicate index %q", idx.name)
		}
		seen[idx.name] = struct{}{}
		runtimes[i] = indexRuntime{
			def:  idx,
			path: splitIndexPath(idx.field),
		}
	}
	return runtimes, nil
}

func (p insertBatchPlanner) planIndexStateAndUniqueProbes(items []insertBatchItem, runtimes []indexRuntime) ([]uniqueProbeCandidate, error) {
	if len(runtimes) == 0 {
		return nil, nil
	}
	uniqueProbes := make([]uniqueProbeCandidate, 0, len(items))
	encoder := indexEncodeArena{buf: make([]byte, 0, estimateBatchIndexEncodeArenaBytes(items, len(runtimes)))}
	for i := range items {
		state, err := orderedIndexStateForDocumentWithArena(items[i].document, runtimes, p.options, &encoder)
		if err != nil {
			return nil, err
		}
		items[i].state = state
		for runtimeIdx, runtime := range runtimes {
			if !runtime.def.unique {
				continue
			}
			for _, encoded := range state.valuesAt(runtimeIdx) {
				uniqueProbes = append(uniqueProbes, uniqueProbeCandidate{
					indexName:    runtime.def.name,
					encodedValue: encoded,
					documentID:   items[i].id,
				})
			}
		}
	}
	return uniqueProbes, nil
}

func sortUniqueProbeCandidates(candidates []uniqueProbeCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].indexName != candidates[j].indexName {
			return candidates[i].indexName < candidates[j].indexName
		}
		if cmp := compareIndexValuePrefixEncoded(candidates[i].encodedValue, candidates[j].encodedValue); cmp != 0 {
			return cmp < 0
		}
		return bytes.Compare(candidates[i].documentID, candidates[j].documentID) < 0
	})
}

func rejectDuplicateUniqueProbeCandidates(candidates []uniqueProbeCandidate) error {
	for i := 1; i < len(candidates); i++ {
		candidate := &candidates[i]
		prev := &candidates[i-1]
		if candidate.indexName == prev.indexName &&
			bytes.Equal(candidate.encodedValue, prev.encodedValue) &&
			!bytes.Equal(candidate.documentID, prev.documentID) {
			return fmt.Errorf("collections: unique index %q conflict", candidate.indexName)
		}
	}
	return nil
}

func buildUniqueProbeRuns(candidates []uniqueProbeCandidate) ([]collectionUniqueProbeRun, error) {
	sortUniqueProbeCandidates(candidates)
	if err := rejectDuplicateUniqueProbeCandidates(candidates); err != nil {
		return nil, err
	}
	return buildUniqueProbeRunsFromSorted(candidates, nil)
}

func estimateUniqueProbePrefixBytes(candidates []uniqueProbeCandidate) int {
	total := 0
	for _, candidate := range candidates {
		total += 2 + len(candidate.encodedValue)
	}
	return total
}

func buildUniqueProbeRunsForPreflightFromSorted(candidates []uniqueProbeCandidate, preflight insertBatchPreflight) ([]collectionUniqueProbeRun, error) {
	if preflight.snapshot == nil || len(preflight.uniqueIndexRootIDs) == 0 {
		return nil, nil
	}
	return buildUniqueProbeRunsFromSorted(candidates, func(indexName string) bool {
		return preflight.uniqueIndexRootIDs[indexName] != 0
	})
}

func buildUniqueProbeRunsFromSorted(candidates []uniqueProbeCandidate, includeIndex func(indexName string) bool) ([]collectionUniqueProbeRun, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	runs := make([]collectionUniqueProbeRun, 0)
	prefixArena := make([]byte, 0, estimateUniqueProbePrefixBytes(candidates))
	for start := 0; start < len(candidates); {
		indexName := candidates[start].indexName
		end := start + 1
		for end < len(candidates) && candidates[end].indexName == indexName {
			end++
		}
		if includeIndex != nil && !includeIndex(indexName) {
			start = end
			continue
		}
		run := collectionUniqueProbeRun{
			indexName: indexName,
			prefixes:  make([][]byte, 0, end-start),
		}
		for i := start; i < end; i++ {
			candidate := &candidates[i]
			if len(run.prefixes) == 0 || !bytes.Equal(candidate.encodedValue, candidates[i-1].encodedValue) {
				var prefix []byte
				var err error
				prefixArena, prefix, err = appendIndexValuePrefixSlice(prefixArena, candidate.encodedValue)
				if err != nil {
					return nil, err
				}
				run.prefixes = append(run.prefixes, prefix)
			}
		}
		runs = append(runs, run)
		start = end
	}
	return runs, nil
}

func (p insertBatchPlanner) emitPrimaryRun(plan *insertBatchPlan, items []insertBatchItem, order []int) error {
	table := newCollectionRunTable(len(items))
	if err := applyCollectionRunEntries(table, len(items), func(i int) (key, value []byte, err error) {
		idx := orderedItemIndex(order, i)
		value, err = p.buildPrimaryVal(items[idx].id, items[idx].document)
		if err != nil {
			return nil, nil, err
		}
		plan.stats.payloadBuilds++
		return items[idx].id, value, nil
	}); err != nil {
		return err
	}
	table.Freeze()
	plan.runs = append(plan.runs, collectionRootRun{
		name:          p.primaryRoot,
		kind:          collectionRootPrimary,
		table:         table,
		storagePolicy: p.options.dataStoragePolicy,
	})
	return nil
}

func setCollectionRunValue(table memtable.Table, key, value []byte) {
	table.SetEntrySteal(key, value, page.ValuePtr{}, node.FlagInline)
}

func applyCollectionRunEntries(table memtable.Table, count int, emit func(i int) (key, value []byte, err error)) error {
	if count <= 0 {
		return nil
	}
	if appender, ok := table.(memtable.StealEntryFuncApplier); ok {
		return appender.ApplyStealEntryFunc(count, func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error) {
			key, value, err = emit(i)
			if err != nil {
				return nil, nil, page.ValuePtr{}, 0, err
			}
			return key, value, page.ValuePtr{}, node.FlagInline, nil
		})
	}
	for i := 0; i < count; i++ {
		key, value, err := emit(i)
		if err != nil {
			return err
		}
		setCollectionRunValue(table, key, value)
	}
	return nil
}

func (p insertBatchPlanner) emitIndexStateRun(plan *insertBatchPlan, items []insertBatchItem, runtimes []indexRuntime) error {
	order := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	counts := make([]int, len(items))
	valueBytes := 0
	for i := range items {
		idx := orderedItemIndex(order, i)
		count, size, err := runtimeOrderedDocumentIndexStateStats(items[idx].state, runtimes)
		if err != nil {
			return err
		}
		counts[i] = count
		valueBytes += size
	}
	table := newCollectionRunTable(len(items))
	valueArena := make([]byte, 0, valueBytes)
	if err := applyCollectionRunEntries(table, len(items), func(i int) (key, value []byte, err error) {
		idx := orderedItemIndex(order, i)
		valueArena, value = appendRuntimeOrderedDocumentIndexState(valueArena, items[idx].state, runtimes, counts[i])
		return items[idx].id, value, nil
	}); err != nil {
		return err
	}
	table.Freeze()
	plan.runs = append(plan.runs, collectionRootRun{
		name:          p.indexStateRoot,
		kind:          collectionRootIndexState,
		table:         table,
		storagePolicy: p.options.indexStateStoragePolicy,
	})
	return nil
}

func (p insertBatchPlanner) emitSecondaryRuns(plan *insertBatchPlan, items []insertBatchItem, runtimes []indexRuntime) error {
	for runtimeIdx, runtime := range runtimes {
		entryCount, keyBytes, alreadySorted, err := secondaryEntryOrderStats(items, runtimeIdx)
		if err != nil {
			return err
		}
		if entryCount == 0 {
			continue
		}
		if alreadySorted {
			table := newCollectionRunTable(entryCount)
			keyArena := make([]byte, 0, keyBytes)
			itemPos := 0
			valuePos := 0
			if err := applyCollectionRunEntries(table, entryCount, func(_ int) (key, value []byte, err error) {
				for itemPos < len(items) {
					values := items[itemPos].state.valuesAt(runtimeIdx)
					if valuePos < len(values) {
						encoded := values[valuePos]
						valuePos++
						keyArena, key, err = appendIndexEntryKey(keyArena, encoded, items[itemPos].id)
						return key, nil, err
					}
					itemPos++
					valuePos = 0
				}
				return nil, nil, errors.New("collections: secondary index entry count mismatch")
			}); err != nil {
				return err
			}
			table.Freeze()
			plan.runs = append(plan.runs, collectionRootRun{
				name:          p.collection + "/index/" + runtime.def.name,
				kind:          collectionRootSecondary,
				indexName:     runtime.def.name,
				table:         table,
				storagePolicy: runtime.def.storagePolicy,
			})
			continue
		}

		keys := make([][]byte, 0, entryCount)
		keyArena := make([]byte, 0, keyBytes)
		for i := range items {
			for _, encoded := range items[i].state.valuesAt(runtimeIdx) {
				var key []byte
				keyArena, key, err = appendIndexEntryKey(keyArena, encoded, items[i].id)
				if err != nil {
					return err
				}
				keys = append(keys, key)
			}
		}
		if len(keys) == 0 {
			continue
		}
		sort.Slice(keys, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) < 0
		})
		table := newCollectionRunTable(len(keys))
		if err := applyCollectionRunEntries(table, len(keys), func(i int) (key, value []byte, err error) {
			return keys[i], nil, nil
		}); err != nil {
			return err
		}
		table.Freeze()
		plan.runs = append(plan.runs, collectionRootRun{
			name:          p.collection + "/index/" + runtime.def.name,
			kind:          collectionRootSecondary,
			indexName:     runtime.def.name,
			table:         table,
			storagePolicy: runtime.def.storagePolicy,
		})
	}
	return nil
}

func secondaryEntryOrderStats(items []insertBatchItem, runtimeIdx int) (entryCount int, keyBytes int, alreadySorted bool, err error) {
	alreadySorted = true
	var lastValue []byte
	var lastDocumentID []byte
	for i := range items {
		for _, encoded := range items[i].state.valuesAt(runtimeIdx) {
			if len(encoded) > 65535 {
				return 0, 0, false, errors.New("collections: index key too large")
			}
			if entryCount > 0 && compareIndexEntryKeyParts(lastValue, lastDocumentID, encoded, items[i].id) > 0 {
				alreadySorted = false
			}
			lastValue = encoded
			lastDocumentID = items[i].id
			entryCount++
			keyBytes += 2 + len(encoded) + len(items[i].id)
		}
	}
	return entryCount, keyBytes, alreadySorted, nil
}

func newCollectionRunTable(entries int) memtable.Table {
	if entries < 0 {
		entries = 0
	}
	return memtable.NewAppendOnlyWithEntryCapacity(entries)
}

func resetCollectionRunTables(runs []collectionRootRun) {
	for _, run := range runs {
		resetCollectionRunTable(run.table)
	}
}

func resetCollectionTables(tables []memtable.Table) {
	for _, table := range tables {
		resetCollectionRunTable(table)
	}
}

func resetCollectionRunTable(table memtable.Table) {
	type releaser interface {
		Release()
	}
	type resetter interface {
		Reset()
	}
	if table == nil {
		return
	}
	if r, ok := table.(releaser); ok {
		r.Release()
		return
	}
	if r, ok := table.(resetter); ok {
		r.Reset()
	}
}

func sortedItemOrderByKey(items []insertBatchItem, keyFn func(*insertBatchItem) []byte) []int {
	for i := 1; i < len(items); i++ {
		if bytes.Compare(keyFn(&items[i-1]), keyFn(&items[i])) > 0 {
			order := make([]int, len(items))
			for i := range order {
				order[i] = i
			}
			sort.Slice(order, func(i, j int) bool {
				return bytes.Compare(keyFn(&items[order[i]]), keyFn(&items[order[j]])) < 0
			})
			return order
		}
	}
	return nil
}

func orderedItemIndex(order []int, pos int) int {
	if order == nil {
		return pos
	}
	return order[pos]
}

func (s orderedDocumentIndexState) valuesAt(index int) [][]byte {
	if index < 0 || index >= len(s) {
		return nil
	}
	return s[index]
}

func indexStateForDocument(document []byte, runtimes []indexRuntime, opts collectionOptions) (documentIndexState, error) {
	ordered, err := orderedIndexStateForDocument(document, runtimes, opts)
	if err != nil {
		return nil, err
	}
	return documentIndexStateFromOrdered(ordered, runtimes), nil
}

func orderedIndexStateForDocument(document []byte, runtimes []indexRuntime, opts collectionOptions) (orderedDocumentIndexState, error) {
	encoder := indexEncodeArena{buf: make([]byte, 0, estimateDocumentIndexEncodeArenaBytes(len(runtimes)))}
	return orderedIndexStateForDocumentWithArena(document, runtimes, opts, &encoder)
}

func orderedIndexStateForDocumentWithArena(document []byte, runtimes []indexRuntime, opts collectionOptions, encoder *indexEncodeArena) (orderedDocumentIndexState, error) {
	if len(runtimes) == 0 {
		return nil, nil
	}
	if encoder == nil {
		encoder = &indexEncodeArena{buf: make([]byte, 0, estimateDocumentIndexEncodeArenaBytes(len(runtimes)))}
	}
	var decoded any
	if err := json.Unmarshal(document, &decoded); err != nil {
		return nil, fmt.Errorf("collections: index extraction requires JSON document: %w", err)
	}
	obj, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("collections: index extraction requires JSON object document")
	}
	state := make(orderedDocumentIndexState, len(runtimes))
	for runtimeIdx, runtime := range runtimes {
		value, found := extractIndexPathValue(obj, runtime.path)
		if !found || value == nil {
			continue
		}
		values, err := normalizeIndexValues(value, runtime.def.multiKey, opts.allowArrayValuesInIndex)
		if err != nil {
			return nil, err
		}
		encoded := make([][]byte, 0, len(values))
		for _, scalar := range values {
			var next []byte
			var err error
			encoder.buf, next, err = appendIndexScalar(encoder.buf, scalar)
			if err != nil {
				return nil, err
			}
			encoded = append(encoded, next)
		}
		encoded = normalizeOwnedEncodedIndexValues(encoded)
		if len(encoded) > 0 {
			state[runtimeIdx] = encoded
		}
	}
	return state, nil
}

func estimateDocumentIndexEncodeArenaBytes(runtimeCount int) int {
	if runtimeCount <= 0 {
		return 0
	}
	const encodedScalarGuess = 20
	return runtimeCount * encodedScalarGuess
}

func estimateBatchIndexEncodeArenaBytes(items []insertBatchItem, runtimeCount int) int {
	if len(items) == 0 || runtimeCount <= 0 {
		return 0
	}
	total := len(items) * estimateDocumentIndexEncodeArenaBytes(runtimeCount)
	const maxInitialArenaBytes = 4 << 20
	if total > maxInitialArenaBytes {
		return maxInitialArenaBytes
	}
	return total
}

func documentIndexStateFromOrdered(state orderedDocumentIndexState, runtimes []indexRuntime) documentIndexState {
	if len(state) == 0 {
		return nil
	}
	out := make(documentIndexState, len(state))
	for i, values := range state {
		if len(values) == 0 || i >= len(runtimes) {
			continue
		}
		out[runtimes[i].def.name] = values
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func encodeDocumentIndexState(state documentIndexState) ([]byte, error) {
	return encodeDocumentIndexStateWithOptions(state, true)
}

func encodeNormalizedDocumentIndexState(state documentIndexState) ([]byte, error) {
	return encodeDocumentIndexStateWithOptions(state, false)
}

func encodeRuntimeOrderedDocumentIndexState(state orderedDocumentIndexState, runtimes []indexRuntime) ([]byte, error) {
	count, size, err := runtimeOrderedDocumentIndexStateStats(state, runtimes)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, size)
	_, encoded := appendRuntimeOrderedDocumentIndexState(out, state, runtimes, count)
	return encoded, nil
}

func runtimeOrderedDocumentIndexStateStats(state orderedDocumentIndexState, runtimes []indexRuntime) (int, int, error) {
	count := 0
	size := 1 + 2
	for runtimeIdx, runtime := range runtimes {
		indexName := runtime.def.name
		values := filterEmptyEncodedIndexValues(state.valuesAt(runtimeIdx))
		if len(values) == 0 {
			continue
		}
		if len(indexName) > 65535 {
			return 0, 0, errors.New("collections: index state name too long")
		}
		size += 2 + len(indexName) + 2
		for _, value := range values {
			if len(value) > 65535 {
				return 0, 0, errors.New("collections: index state value too large")
			}
			size += 2 + len(value)
		}
		count++
	}
	return count, size, nil
}

func appendRuntimeOrderedDocumentIndexState(dst []byte, state orderedDocumentIndexState, runtimes []indexRuntime, count int) ([]byte, []byte) {
	start := len(dst)
	dst = append(dst, documentIndexStateVersion)
	dst = binary.BigEndian.AppendUint16(dst, uint16(count))
	for runtimeIdx, runtime := range runtimes {
		indexName := runtime.def.name
		values := filterEmptyEncodedIndexValues(state.valuesAt(runtimeIdx))
		if len(values) == 0 {
			continue
		}
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(indexName)))
		dst = append(dst, indexName...)
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(values)))
		for _, value := range values {
			dst = binary.BigEndian.AppendUint16(dst, uint16(len(value)))
			dst = append(dst, value...)
		}
	}
	return dst, dst[start:len(dst):len(dst)]
}

func encodeDocumentIndexStateWithOptions(state documentIndexState, normalizeValues bool) ([]byte, error) {
	if state == nil {
		state = make(documentIndexState)
	}
	names := make([]string, 0, len(state))
	for indexName, values := range state {
		if indexName == "" {
			return nil, errors.New("collections: index state name cannot be empty")
		}
		if normalizeValues {
			values = normalizeEncodedIndexValues(values)
		} else {
			values = filterEmptyEncodedIndexValues(values)
		}
		if len(values) == 0 {
			delete(state, indexName)
			continue
		}
		state[indexName] = values
		names = append(names, indexName)
	}
	sort.Strings(names)

	size := 1 + 2
	for _, indexName := range names {
		values := state[indexName]
		if len(indexName) > 65535 {
			return nil, errors.New("collections: index state name too long")
		}
		size += 2 + len(indexName) + 2
		for _, value := range values {
			if len(value) > 65535 {
				return nil, errors.New("collections: index state value too large")
			}
			size += 2 + len(value)
		}
	}

	out := make([]byte, 0, size)
	out = append(out, documentIndexStateVersion)
	out = binary.BigEndian.AppendUint16(out, uint16(len(names)))
	for _, indexName := range names {
		values := state[indexName]
		out = binary.BigEndian.AppendUint16(out, uint16(len(indexName)))
		out = append(out, indexName...)
		out = binary.BigEndian.AppendUint16(out, uint16(len(values)))
		for _, value := range values {
			out = binary.BigEndian.AppendUint16(out, uint16(len(value)))
			out = append(out, value...)
		}
	}
	return out, nil
}

func decodeDocumentIndexState(raw []byte) (documentIndexState, error) {
	if len(raw) == 0 {
		return nil, errors.New("collections: empty index state")
	}
	if raw[0] != documentIndexStateVersion {
		return nil, fmt.Errorf("collections: unsupported index state version %d", raw[0])
	}
	pos := 1
	if len(raw)-pos < 2 {
		return nil, errors.New("collections: malformed index state")
	}
	count := int(binary.BigEndian.Uint16(raw[pos:]))
	pos += 2
	state := make(documentIndexState, count)
	for i := 0; i < count; i++ {
		if len(raw)-pos < 2 {
			return nil, errors.New("collections: malformed index state name length")
		}
		nameLen := int(binary.BigEndian.Uint16(raw[pos:]))
		pos += 2
		if nameLen == 0 || len(raw)-pos < nameLen {
			return nil, errors.New("collections: malformed index state name")
		}
		name := string(raw[pos : pos+nameLen])
		pos += nameLen
		if len(raw)-pos < 2 {
			return nil, errors.New("collections: malformed index state value count")
		}
		valueCount := int(binary.BigEndian.Uint16(raw[pos:]))
		pos += 2
		values := make([][]byte, 0, valueCount)
		for j := 0; j < valueCount; j++ {
			if len(raw)-pos < 2 {
				return nil, errors.New("collections: malformed index state value length")
			}
			valueLen := int(binary.BigEndian.Uint16(raw[pos:]))
			pos += 2
			if valueLen == 0 || len(raw)-pos < valueLen {
				return nil, errors.New("collections: malformed index state value")
			}
			values = append(values, bytes.Clone(raw[pos:pos+valueLen]))
			pos += valueLen
		}
		state[name] = normalizeEncodedIndexValues(values)
	}
	if pos != len(raw) {
		return nil, errors.New("collections: trailing index state bytes")
	}
	return state, nil
}

func normalizeEncodedIndexValues(values [][]byte) [][]byte {
	if len(values) == 0 {
		return nil
	}
	sorted := make([][]byte, 0, len(values))
	for _, value := range values {
		if len(value) == 0 {
			continue
		}
		sorted = append(sorted, bytes.Clone(value))
	}
	if len(sorted) == 0 {
		return nil
	}
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i], sorted[j]) < 0
	})
	out := sorted[:1]
	for _, value := range sorted[1:] {
		if bytes.Equal(value, out[len(out)-1]) {
			continue
		}
		out = append(out, value)
	}
	return out
}

func normalizeOwnedEncodedIndexValues(values [][]byte) [][]byte {
	values = filterEmptyEncodedIndexValues(values)
	if len(values) == 0 {
		return nil
	}
	sort.Slice(values, func(i, j int) bool {
		return bytes.Compare(values[i], values[j]) < 0
	})
	out := values[:1]
	for _, value := range values[1:] {
		if bytes.Equal(value, out[len(out)-1]) {
			continue
		}
		out = append(out, value)
	}
	return out
}

func filterEmptyEncodedIndexValues(values [][]byte) [][]byte {
	if len(values) == 0 {
		return nil
	}
	out := values[:0]
	for _, value := range values {
		if len(value) == 0 {
			continue
		}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeIndexValues(value any, multiKey, allowArray bool) ([]any, error) {
	if arr, ok := value.([]any); ok {
		if !multiKey && !allowArray {
			return nil, errors.New("collections: array value not allowed for index")
		}
		if len(arr) == 0 {
			return nil, nil
		}
		return arr, nil
	}
	return []any{value}, nil
}

func splitIndexPath(path string) []string {
	if path == "" {
		return nil
	}
	if !strings.Contains(path, ".") {
		return []string{path}
	}
	return strings.Split(path, ".")
}

func extractIndexPathValue(document any, path []string) (any, bool) {
	if len(path) == 0 {
		return nil, false
	}
	current := document
	for _, segment := range path {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := obj[segment]
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func encodeIndexScalar(value any) ([]byte, error) {
	_, encoded, err := appendIndexScalar(nil, value)
	return encoded, err
}

func appendIndexScalar(dst []byte, value any) ([]byte, []byte, error) {
	start := len(dst)
	switch v := value.(type) {
	case string:
		dst = append(dst, "s:"...)
		dst = append(dst, v...)
	case bool:
		if v {
			dst = append(dst, "b:1"...)
		} else {
			dst = append(dst, "b:0"...)
		}
	case float64:
		dst = append(dst, "n:"...)
		dst = strconv.AppendFloat(dst, v, 'g', -1, 64)
	case nil:
		dst = append(dst, "z:"...)
	default:
		return dst, nil, fmt.Errorf("collections: unsupported indexed value type %T", value)
	}
	return dst, dst[start:len(dst):len(dst)], nil
}

func indexEntryKey(encodedValue, documentID []byte) ([]byte, error) {
	key := make([]byte, 0, 2+len(encodedValue)+len(documentID))
	key, out, err := appendIndexEntryKey(key, encodedValue, documentID)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func appendIndexEntryKey(dst, encodedValue, documentID []byte) ([]byte, []byte, error) {
	start := len(dst)
	dst, err := appendIndexValuePrefix(dst, encodedValue)
	if err != nil {
		return dst, nil, err
	}
	dst = append(dst, documentID...)
	return dst, dst[start:len(dst):len(dst)], nil
}

func indexValuePrefix(encodedValue []byte) ([]byte, error) {
	_, prefix, err := appendIndexValuePrefixSlice(make([]byte, 0, 2+len(encodedValue)), encodedValue)
	return prefix, err
}

func compareIndexValuePrefixEncoded(a, b []byte) int {
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return bytes.Compare(a, b)
}

func compareIndexEntryKeyParts(aValue, aDocumentID, bValue, bDocumentID []byte) int {
	if cmp := compareIndexValuePrefixEncoded(aValue, bValue); cmp != 0 {
		return cmp
	}
	return bytes.Compare(aDocumentID, bDocumentID)
}

func appendIndexValuePrefix(out []byte, encodedValue []byte) ([]byte, error) {
	if len(encodedValue) > 65535 {
		return nil, errors.New("collections: index key too large")
	}
	out = binary.BigEndian.AppendUint16(out, uint16(len(encodedValue)))
	out = append(out, encodedValue...)
	return out, nil
}

func appendIndexValuePrefixSlice(dst []byte, encodedValue []byte) ([]byte, []byte, error) {
	start := len(dst)
	next, err := appendIndexValuePrefix(dst, encodedValue)
	if err != nil {
		return next, nil, err
	}
	return next, next[start:len(next):len(next)], nil
}

func prefixEnd(prefix []byte) []byte {
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

func (plan *insertBatchPlan) publishRootRuns(publisher groupedRootPublisher, baseRootIDs map[string]uint64) ([]uint64, error) {
	if plan == nil {
		return nil, errors.New("collections: nil insert batch plan")
	}
	if publisher == nil {
		return nil, errors.New("collections: nil root publisher")
	}
	ordered := make([]backenddb.OrderedRootPublishInput, 0, len(plan.runs))
	iterators := make([]iterator.UnsafeIterator, 0, len(plan.runs))
	iteratorsOwned := true
	defer func() {
		if iteratorsOwned {
			for _, it := range iterators {
				_ = it.Close()
			}
		}
	}()
	for _, run := range plan.runs {
		if run.table == nil {
			return nil, fmt.Errorf("collections: root run %q has nil table", run.name)
		}
		iter := run.table.NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootPublishInput{
			BaseRoot:      baseRootIDs[run.name],
			Iter:          iter,
			StoragePolicy: run.storagePolicy,
		})
	}
	// PublishOrderedRootGroup owns and closes iterators once they are handed off.
	iteratorsOwned = false
	_, rootIDs, err := publisher.PublishOrderedRootGroup(nil, ordered)
	if err != nil {
		return nil, err
	}
	return append([]uint64(nil), rootIDs...), nil
}
