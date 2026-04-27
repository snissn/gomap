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
	state    documentIndexState
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

	items := make([]insertBatchItem, len(documents))
	resultIDs := make([][]byte, len(documents))
	for i := range documents {
		if len(ids[i]) == 0 {
			return nil, errors.New("collections: document id cannot be empty")
		}
		id := bytes.Clone(ids[i])
		items[i] = insertBatchItem{
			id:       id,
			document: documents[i],
		}
		resultIDs[i] = id
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
	uniqueProbeRuns, err := buildUniqueProbeRunsForPreflight(uniqueProbes, preflight)
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

func (p insertBatchPreflight) checkDocumentConflicts(items []insertBatchItem, order []int, sortedIDs [][]byte) error {
	if p.snapshot == nil || p.primaryRootID == 0 {
		return nil
	}
	keys := sortedIDs
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
	for i := range items {
		state, err := indexStateForDocument(items[i].document, runtimes, p.options)
		if err != nil {
			return nil, err
		}
		items[i].state = state
		for _, runtime := range runtimes {
			if !runtime.def.unique {
				continue
			}
			for _, encoded := range state[runtime.def.name] {
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

func buildUniqueProbeRunsForPreflight(candidates []uniqueProbeCandidate, preflight insertBatchPreflight) ([]collectionUniqueProbeRun, error) {
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
				prefix, err := indexValuePrefix(candidate.encodedValue)
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
	for i := range items {
		idx := orderedItemIndex(order, i)
		value, err := p.buildPrimaryVal(items[idx].id, items[idx].document)
		if err != nil {
			return err
		}
		plan.stats.payloadBuilds++
		setCollectionRunValue(table, items[idx].id, value)
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

func (p insertBatchPlanner) emitIndexStateRun(plan *insertBatchPlan, items []insertBatchItem, runtimes []indexRuntime) error {
	order := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	table := newCollectionRunTable(len(items))
	for i := range items {
		idx := orderedItemIndex(order, i)
		raw, err := encodeRuntimeOrderedDocumentIndexState(items[idx].state, runtimes)
		if err != nil {
			return err
		}
		table.SetSteal(items[idx].id, raw)
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
	for _, runtime := range runtimes {
		entryCount, alreadySorted, err := secondaryEntryOrderStats(items, runtime.def.name)
		if err != nil {
			return err
		}
		if entryCount == 0 {
			continue
		}
		if alreadySorted {
			table := newCollectionRunTable(entryCount)
			for i := range items {
				for _, encoded := range items[i].state[runtime.def.name] {
					key, err := indexEntryKey(encoded, items[i].id)
					if err != nil {
						return err
					}
					table.SetSteal(key, nil)
				}
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
		for i := range items {
			for _, encoded := range items[i].state[runtime.def.name] {
				key, err := indexEntryKey(encoded, items[i].id)
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
		for _, key := range keys {
			table.SetSteal(key, nil)
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

func secondaryEntryOrderStats(items []insertBatchItem, indexName string) (entryCount int, alreadySorted bool, err error) {
	alreadySorted = true
	var lastValue []byte
	var lastDocumentID []byte
	for i := range items {
		for _, encoded := range items[i].state[indexName] {
			if len(encoded) > 65535 {
				return 0, false, errors.New("collections: index key too large")
			}
			if entryCount > 0 && compareIndexEntryKeyParts(lastValue, lastDocumentID, encoded, items[i].id) > 0 {
				alreadySorted = false
			}
			lastValue = encoded
			lastDocumentID = items[i].id
			entryCount++
		}
	}
	return entryCount, alreadySorted, nil
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

func indexStateForDocument(document []byte, runtimes []indexRuntime, opts collectionOptions) (documentIndexState, error) {
	if len(runtimes) == 0 {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal(document, &decoded); err != nil {
		return nil, fmt.Errorf("collections: index extraction requires JSON document: %w", err)
	}
	obj, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("collections: index extraction requires JSON object document")
	}
	state := make(documentIndexState, len(runtimes))
	for _, runtime := range runtimes {
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
			next, err := encodeIndexScalar(scalar)
			if err != nil {
				return nil, err
			}
			encoded = append(encoded, next)
		}
		encoded = normalizeOwnedEncodedIndexValues(encoded)
		if len(encoded) > 0 {
			state[runtime.def.name] = encoded
		}
	}
	return state, nil
}

func encodeDocumentIndexState(state documentIndexState) ([]byte, error) {
	return encodeDocumentIndexStateWithOptions(state, true)
}

func encodeNormalizedDocumentIndexState(state documentIndexState) ([]byte, error) {
	return encodeDocumentIndexStateWithOptions(state, false)
}

func encodeRuntimeOrderedDocumentIndexState(state documentIndexState, runtimes []indexRuntime) ([]byte, error) {
	if state == nil {
		state = make(documentIndexState)
	}
	count := 0
	size := 1 + 2
	for _, runtime := range runtimes {
		indexName := runtime.def.name
		values := filterEmptyEncodedIndexValues(state[indexName])
		if len(values) == 0 {
			continue
		}
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
		count++
	}

	out := make([]byte, 0, size)
	out = append(out, documentIndexStateVersion)
	out = binary.BigEndian.AppendUint16(out, uint16(count))
	for _, runtime := range runtimes {
		indexName := runtime.def.name
		values := filterEmptyEncodedIndexValues(state[indexName])
		if len(values) == 0 {
			continue
		}
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
	switch v := value.(type) {
	case string:
		out := make([]byte, 0, 2+len(v))
		out = append(out, "s:"...)
		out = append(out, v...)
		return out, nil
	case bool:
		if v {
			return []byte("b:1"), nil
		}
		return []byte("b:0"), nil
	case float64:
		return []byte("n:" + strconv.FormatFloat(v, 'g', -1, 64)), nil
	case nil:
		return []byte("z:"), nil
	default:
		return nil, fmt.Errorf("collections: unsupported indexed value type %T", value)
	}
}

func indexEntryKey(encodedValue, documentID []byte) ([]byte, error) {
	key := make([]byte, 0, 2+len(encodedValue)+len(documentID))
	key, err := appendIndexValuePrefix(key, encodedValue)
	if err != nil {
		return nil, err
	}
	key = append(key, documentID...)
	return key, nil
}

func indexValuePrefix(encodedValue []byte) ([]byte, error) {
	return appendIndexValuePrefix(make([]byte, 0, 2+len(encodedValue)), encodedValue)
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
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
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
	_, rootIDs, err := publisher.PublishOrderedRootGroup(nil, ordered)
	if err != nil {
		return nil, err
	}
	return append([]uint64(nil), rootIDs...), nil
}
