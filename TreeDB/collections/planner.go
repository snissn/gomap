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
}

type indexDefinition struct {
	name     string
	field    string
	unique   bool
	multiKey bool
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
	name      string
	kind      collectionRootKind
	indexName string
	table     memtable.Table
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
	indexName  string
	prefix     []byte
	documentID []byte
}

type documentIndexState map[string][][]byte

type groupedRootPublisher interface {
	PublishOrderedRootGroup(systemIter iterator.UnsafeIterator, ordered []backenddb.OrderedRootPublishInput) (uint64, []uint64, error)
}

func (p insertBatchPlanner) planInsertBatch(ids, documents [][]byte) (*insertBatchPlan, error) {
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
		p.buildPrimaryVal = clonePrimaryDocument
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
		resultIDs[i] = bytes.Clone(id)
	}

	if err := rejectDuplicateDocumentIDs(items); err != nil {
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
	uniqueProbeRuns, err := buildUniqueProbeRuns(uniqueProbes)
	if err != nil {
		return nil, err
	}

	plan := &insertBatchPlan{
		resultIDs:       resultIDs,
		uniqueProbeRuns: uniqueProbeRuns,
	}
	if err := p.emitPrimaryRun(plan, items); err != nil {
		return nil, err
	}
	if len(runtimes) > 0 {
		if err := p.emitIndexStateRun(plan, items); err != nil {
			return nil, err
		}
		if err := p.emitSecondaryRuns(plan, items, runtimes); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func clonePrimaryDocument(_, document []byte) ([]byte, error) {
	return bytes.Clone(document), nil
}

func rejectDuplicateDocumentIDs(items []insertBatchItem) error {
	order := make([]int, len(items))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return bytes.Compare(items[order[i]].id, items[order[j]].id) < 0
	})
	for i := 1; i < len(order); i++ {
		if bytes.Equal(items[order[i-1]].id, items[order[i]].id) {
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
				prefix, err := indexValuePrefix(encoded)
				if err != nil {
					return nil, err
				}
				uniqueProbes = append(uniqueProbes, uniqueProbeCandidate{
					indexName:  runtime.def.name,
					prefix:     prefix,
					documentID: items[i].id,
				})
			}
		}
	}
	return uniqueProbes, nil
}

func buildUniqueProbeRuns(candidates []uniqueProbeCandidate) ([]collectionUniqueProbeRun, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].indexName != candidates[j].indexName {
			return candidates[i].indexName < candidates[j].indexName
		}
		if cmp := bytes.Compare(candidates[i].prefix, candidates[j].prefix); cmp != 0 {
			return cmp < 0
		}
		return bytes.Compare(candidates[i].documentID, candidates[j].documentID) < 0
	})

	runs := make([]collectionUniqueProbeRun, 0)
	var cur *collectionUniqueProbeRun
	for i := range candidates {
		candidate := &candidates[i]
		if i > 0 &&
			candidate.indexName == candidates[i-1].indexName &&
			bytes.Equal(candidate.prefix, candidates[i-1].prefix) &&
			!bytes.Equal(candidate.documentID, candidates[i-1].documentID) {
			return nil, fmt.Errorf("collections: unique index %q conflict", candidate.indexName)
		}
		if cur == nil || cur.indexName != candidate.indexName {
			runs = append(runs, collectionUniqueProbeRun{indexName: candidate.indexName})
			cur = &runs[len(runs)-1]
		}
		if len(cur.prefixes) == 0 || !bytes.Equal(cur.prefixes[len(cur.prefixes)-1], candidate.prefix) {
			cur.prefixes = append(cur.prefixes, bytes.Clone(candidate.prefix))
		}
	}
	return runs, nil
}

func (p insertBatchPlanner) emitPrimaryRun(plan *insertBatchPlan, items []insertBatchItem) error {
	order := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	table := newCollectionRunTable(len(items))
	for _, idx := range order {
		value, err := p.buildPrimaryVal(items[idx].id, items[idx].document)
		if err != nil {
			return err
		}
		plan.stats.payloadBuilds++
		table.SetSteal(bytes.Clone(items[idx].id), value)
	}
	table.Freeze()
	plan.runs = append(plan.runs, collectionRootRun{
		name:  p.primaryRoot,
		kind:  collectionRootPrimary,
		table: table,
	})
	return nil
}

func (p insertBatchPlanner) emitIndexStateRun(plan *insertBatchPlan, items []insertBatchItem) error {
	order := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	table := newCollectionRunTable(len(items))
	for _, idx := range order {
		raw, err := encodeDocumentIndexState(items[idx].state)
		if err != nil {
			return err
		}
		table.SetSteal(bytes.Clone(items[idx].id), raw)
	}
	table.Freeze()
	plan.runs = append(plan.runs, collectionRootRun{
		name:  p.indexStateRoot,
		kind:  collectionRootIndexState,
		table: table,
	})
	return nil
}

func (p insertBatchPlanner) emitSecondaryRuns(plan *insertBatchPlan, items []insertBatchItem, runtimes []indexRuntime) error {
	for _, runtime := range runtimes {
		keys := make([][]byte, 0, len(items))
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
			name:      p.collection + "/index/" + runtime.def.name,
			kind:      collectionRootSecondary,
			indexName: runtime.def.name,
			table:     table,
		})
	}
	return nil
}

func newCollectionRunTable(entries int) memtable.Table {
	if entries < 0 {
		entries = 0
	}
	return memtable.NewAppendOnlyWithCapacity(entries * 64)
}

func sortedItemOrderByKey(items []insertBatchItem, keyFn func(*insertBatchItem) []byte) []int {
	order := make([]int, len(items))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return bytes.Compare(keyFn(&items[order[i]]), keyFn(&items[order[j]])) < 0
	})
	return order
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
		encoded = normalizeEncodedIndexValues(encoded)
		if len(encoded) > 0 {
			state[runtime.def.name] = encoded
		}
	}
	return state, nil
}

func encodeDocumentIndexState(state documentIndexState) ([]byte, error) {
	if state == nil {
		state = make(documentIndexState)
	}
	names := make([]string, 0, len(state))
	for indexName, values := range state {
		if indexName == "" {
			return nil, errors.New("collections: index state name cannot be empty")
		}
		state[indexName] = normalizeEncodedIndexValues(values)
		names = append(names, indexName)
	}
	sort.Strings(names)

	out := make([]byte, 0, 32)
	out = append(out, documentIndexStateVersion)
	out = binary.BigEndian.AppendUint16(out, uint16(len(names)))
	for _, indexName := range names {
		values := state[indexName]
		if len(indexName) > 65535 {
			return nil, errors.New("collections: index state name too long")
		}
		out = binary.BigEndian.AppendUint16(out, uint16(len(indexName)))
		out = append(out, indexName...)
		out = binary.BigEndian.AppendUint16(out, uint16(len(values)))
		for _, value := range values {
			if len(value) > 65535 {
				return nil, errors.New("collections: index state value too large")
			}
			out = binary.BigEndian.AppendUint16(out, uint16(len(value)))
			out = append(out, value...)
		}
	}
	return out, nil
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
		return append([]byte("s:"), []byte(v)...), nil
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
	prefix, err := indexValuePrefix(encodedValue)
	if err != nil {
		return nil, err
	}
	key := make([]byte, 0, len(prefix)+len(documentID))
	key = append(key, prefix...)
	key = append(key, documentID...)
	return key, nil
}

func indexValuePrefix(encodedValue []byte) ([]byte, error) {
	if len(encodedValue) > 65535 {
		return nil, errors.New("collections: index key too large")
	}
	out := make([]byte, 0, 2+len(encodedValue))
	out = binary.BigEndian.AppendUint16(out, uint16(len(encodedValue)))
	out = append(out, encodedValue...)
	return out, nil
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
			BaseRoot: baseRootIDs[run.name],
			Iter:     iter,
		})
	}
	_, rootIDs, err := publisher.PublishOrderedRootGroup(nil, ordered)
	if err != nil {
		return nil, err
	}
	return append([]uint64(nil), rootIDs...), nil
}
