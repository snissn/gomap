package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/tidwall/gjson"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const documentIndexStateVersion = 2

const (
	// Most benchmarked scalar index values are short strings ("city-00",
	// emails, booleans, or numeric tags). This is only an initial arena hint;
	// larger values still append correctly and grow the arena as needed.
	indexEncodeArenaScalarGuessBytes = 20
	// Keep the speculative batch arena bounded. If a batch needs more encoded
	// value bytes, append growth preserves earlier slices and avoids a large
	// upfront allocation.
	indexEncodeArenaMaxInitialBytes = 4 << 20
	// One-value index states share a planner-owned [][]byte backing array.
	// 128K slice headers is enough for the current native benchmark batches
	// while keeping speculative header memory around 3 MiB on 64-bit platforms.
	indexEncodeArenaMaxInitialValueRefs = 128 << 10
	// Non-unique secondary indexes often have a small number of distinct values
	// in a large batch. Up to this point, grouping by value avoids sorting every
	// full secondary key; above it, the generic key sort avoids quadratic scans.
	secondaryGroupedRunMaxDistinctValues = 128
)

type collectionRootKind uint8

const (
	collectionRootPrimary collectionRootKind = iota + 1
	collectionRootTemplate
	collectionRootIndexState
	collectionRootSecondary
)

type collectionOptions struct {
	db                      *backenddb.DB
	typedProjection         *trustedFloat32Projection
	allowArrayValuesInIndex bool
	documentFormat          DocumentFormat
	trustedBSONDocuments    bool
	templateResolver        templateV1Resolver
	learnTemplateIDs        bool
	allowTemplateV1Stored   bool
	dataStoragePolicy       backenddb.OrderedRootStoragePolicy
	indexStateStoragePolicy backenddb.OrderedRootStoragePolicy
}

type indexDefinition struct {
	name          string
	field         string
	valueType     IndexValueType
	unique        bool
	multiKey      bool
	storagePolicy backenddb.OrderedRootStoragePolicy
	components    []IndexComponent
}

type insertBatchPlanner struct {
	collection             string
	primaryRoot            string
	templateRoot           string
	indexStateRoot         string
	indexes                []indexDefinition
	cachedIndexRuntimes    []indexRuntime
	cachedIndexRuntimesErr error
	options                collectionOptions
	buildPrimaryVal        func(documentID, document []byte) ([]byte, error)
	cloneTemplateRunValues bool
	directBufferedRuns     bool
	pointerizePrimary      bool
}

type insertBatchPlan struct {
	resultIDs                  [][]byte
	primaryKeys                [][]byte
	runs                       []collectionRootRun
	directBufferedInsert       *directBufferedInsertPlan
	uniqueProbeCandidates      []uniqueProbeCandidate
	uniqueProbeCandidatesBuilt bool
	uniqueProbeRuns            []collectionUniqueProbeRun
	allUniqueProbeRuns         []collectionUniqueProbeRun
	allUniqueProbeRunsBuilt    bool
	templateRecords            []templateV1Record
	templateLearned            []templateV1LearnedTemplate
	templateResolver           templateV1Resolver
	stats                      insertBatchPlanStats
}

type insertBatchPlanStats struct {
	CollectionInsertStats
	payloadBuilds int
}

type collectionRootRun struct {
	name           string
	kind           collectionRootKind
	indexName      string
	indexValueType IndexValueType
	table          memtable.Table
	storagePolicy  backenddb.OrderedRootStoragePolicy
}

type directBufferedInsertPlan struct {
	templateEntries      []directBufferedRootEntry
	primaryEntries       []directBufferedRootEntry
	indexStateEntries    []directBufferedRootEntry
	secondaryRootPlans   []directBufferedSecondaryRootPlan
	uniqueValueRootPlans []directBufferedUniqueValueRootPlan
	rootNames            []string
	policies             []backenddb.OrderedRootStoragePolicy
	templateRootName     string
	primaryRootName      string
	indexStateRootName   string
	stagedBytes          int64
	pointerizePrimary    bool
	pointerizedPtrs      []page.ValuePtr
}

type directBufferedUniqueValueRootPlan struct {
	indexName string
	valueType IndexValueType
	prefixes  [][]byte
	keyBytes  int
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
	def               indexDefinition
	path              []string
	componentPaths    [][]string
	secondaryRootName string
}

type uniqueProbeCandidate struct {
	indexName    string
	encodedValue []byte
	documentID   []byte
}

type documentIndexState map[string][][]byte
type orderedDocumentIndexState [][][]byte

type indexEncodeArena struct {
	buf       []byte
	scratch   []byte
	states    [][][]byte
	valueRefs [][]byte
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
	if p.templateRoot == "" {
		p.templateRoot = p.collection + "/templates"
	}
	persistIndexState := persistIndexStateForOptions(p.options)
	runtimes, err := p.indexRuntimes()
	if err != nil {
		return nil, err
	}
	if len(runtimes) > 0 && persistIndexState && p.indexStateRoot == "" {
		p.indexStateRoot = p.collection + "/index-state"
	}
	if p.buildPrimaryVal == nil {
		p.buildPrimaryVal = borrowPrimaryDocument
	}
	stats := CollectionInsertStats{
		Documents: len(documents),
		Indexes:   len(runtimes),
	}
	phaseStart := time.Now()
	preparedDocuments, templateRecords, templateLearned, templateResolver, err := prepareInsertDocuments(documents, p.options)
	stats.PrepareDocuments = time.Since(phaseStart)
	if err != nil {
		return nil, err
	}
	if templateResolver != nil {
		p.options.templateResolver = templateResolver
	}

	resultIDs, err := cloneBatchDocumentIDs(ids)
	if err != nil {
		return nil, err
	}
	if p.directBufferedRuns && len(preparedDocuments) == 1 {
		return p.planSingleDirectBufferedInsertWithPreflight(resultIDs[0], preparedDocuments[0], runtimes, persistIndexState, templateRecords, templateLearned, preflight, stats)
	}
	items := make([]insertBatchItem, len(preparedDocuments))
	for i := range preparedDocuments {
		id := resultIDs[i]
		items[i] = insertBatchItem{
			id:       id,
			document: preparedDocuments[i],
		}
	}

	primaryOrder := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	// Keep ID cloning, item assembly, and primary-order sorting outside this phase.
	phaseStart = time.Now()
	if err := rejectDuplicateDocumentIDs(items, primaryOrder); err != nil {
		return nil, err
	}
	primaryKeys := sortedDocumentIDKeys(items, primaryOrder, resultIDs)
	if err := preflight.checkDocumentConflictKeys(primaryKeys); err != nil {
		return nil, err
	}
	stats.DuplicateDocumentPreflight = time.Since(phaseStart)

	phaseStart = time.Now()
	uniqueProbes, err := p.planIndexStateAndUniqueProbes(items, runtimes)
	stats.IndexStateExtraction = time.Since(phaseStart)
	if err != nil {
		return nil, err
	}
	phaseStart = time.Now()
	sortUniqueProbeCandidates(uniqueProbes)
	if err := rejectDuplicateUniqueProbeCandidates(uniqueProbes); err != nil {
		return nil, err
	}
	var uniqueProbeRuns []collectionUniqueProbeRun
	var allUniqueProbeRuns []collectionUniqueProbeRun
	allUniqueProbeRunsBuilt := true
	if len(uniqueProbes) > 0 {
		allUniqueProbeRuns, err = buildUniqueProbeRunsFromSorted(uniqueProbes, nil)
		if err != nil {
			return nil, err
		}
	}
	if preflight.snapshot != nil && len(preflight.uniqueIndexRootIDs) > 0 {
		uniqueProbeRuns = uniqueProbeRunsForPreflight(allUniqueProbeRuns, preflight)
		if err := preflight.checkUniqueConflicts(uniqueProbeRuns); err != nil {
			return nil, err
		}
	}
	stats.UniqueIndexPreflight = time.Since(phaseStart)

	plan := &insertBatchPlan{
		resultIDs:                  resultIDs,
		primaryKeys:                primaryKeys,
		uniqueProbeCandidates:      uniqueProbes,
		uniqueProbeCandidatesBuilt: true,
		uniqueProbeRuns:            uniqueProbeRuns,
		allUniqueProbeRuns:         allUniqueProbeRuns,
		allUniqueProbeRunsBuilt:    allUniqueProbeRunsBuilt,
		templateRecords:            templateRecords,
		templateLearned:            templateLearned,
		templateResolver:           templateResolver,
		stats:                      insertBatchPlanStats{CollectionInsertStats: stats},
	}
	if p.directBufferedRuns {
		if err := p.buildDirectBufferedInsertPlan(plan, items, primaryOrder, runtimes, persistIndexState, templateRecords); err != nil {
			return nil, err
		}
		if plan.directBufferedInsert != nil {
			plan.stats.Runs = len(plan.directBufferedInsert.rootNames)
		}
		return plan, nil
	}
	if len(templateRecords) > 0 {
		phaseStart = time.Now()
		if err := p.emitTemplateRun(plan, templateRecords); err != nil {
			return nil, err
		}
		plan.stats.TemplateRunBuild = time.Since(phaseStart)
	}
	phaseStart = time.Now()
	if err := p.emitPrimaryRun(plan, items, primaryOrder); err != nil {
		return nil, err
	}
	plan.stats.PrimaryRunBuild = time.Since(phaseStart)
	if len(runtimes) > 0 {
		if persistIndexState {
			phaseStart = time.Now()
			if err := p.emitIndexStateRun(plan, items, runtimes); err != nil {
				return nil, err
			}
			plan.stats.IndexStateRunBuild = time.Since(phaseStart)
		}
		phaseStart = time.Now()
		if err := p.emitSecondaryRuns(plan, items, runtimes, primaryOrder); err != nil {
			return nil, err
		}
		plan.stats.SecondaryRunBuild = time.Since(phaseStart)
	}
	plan.stats.Runs = len(plan.runs)
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

func (p insertBatchPlanner) planSingleDirectBufferedInsertWithPreflight(resultID, preparedDocument []byte, runtimes []indexRuntime, persistIndexState bool, templateRecords []templateV1Record, templateLearned []templateV1LearnedTemplate, preflight insertBatchPreflight, stats CollectionInsertStats) (*insertBatchPlan, error) {
	primaryKeys := [][]byte{resultID}
	phaseStart := time.Now()
	if err := preflight.checkDocumentConflictKeys(primaryKeys); err != nil {
		return nil, err
	}
	stats.DuplicateDocumentPreflight = time.Since(phaseStart)

	item := insertBatchItem{id: resultID, document: preparedDocument}
	phaseStart = time.Now()
	allUniqueProbeRuns, uniqueProbeRuns, err := p.singleItemIndexStateAndUniqueProbeRuns(&item, runtimes, preflight)
	stats.IndexStateExtraction = time.Since(phaseStart)
	if err != nil {
		return nil, err
	}

	phaseStart = time.Now()
	if err := preflight.checkUniqueConflicts(uniqueProbeRuns); err != nil {
		return nil, err
	}
	stats.UniqueIndexPreflight = time.Since(phaseStart)

	plan := &insertBatchPlan{
		resultIDs:               primaryKeys,
		primaryKeys:             primaryKeys,
		uniqueProbeRuns:         uniqueProbeRuns,
		allUniqueProbeRuns:      allUniqueProbeRuns,
		allUniqueProbeRunsBuilt: true,
		templateRecords:         templateRecords,
		templateLearned:         templateLearned,
		stats:                   insertBatchPlanStats{CollectionInsertStats: stats},
	}
	if err := p.buildSingleDirectBufferedInsertPlan(plan, &item, runtimes, persistIndexState, templateRecords); err != nil {
		return nil, err
	}
	if plan.directBufferedInsert != nil {
		plan.stats.Runs = len(plan.directBufferedInsert.rootNames)
	}
	return plan, nil
}

func (p insertBatchPreflight) checkDocumentConflicts(items []insertBatchItem, order []int, presortedIDs [][]byte) error {
	return p.checkDocumentConflictKeys(sortedDocumentIDKeys(items, order, presortedIDs))
}

func (p insertBatchPreflight) checkDocumentConflictKeys(keys [][]byte) error {
	if p.snapshot == nil || p.primaryRootID == 0 {
		return nil
	}
	exists, err := p.snapshot.HasAnySortedAtRoot(p.primaryRootID, keys)
	if err != nil {
		return err
	}
	if exists {
		return ErrDocumentExists
	}
	return nil
}

func sortedDocumentIDKeys(items []insertBatchItem, order []int, presortedIDs [][]byte) [][]byte {
	// sortedItemOrderByKey returns nil only when items are already sorted by ID,
	// so the caller-owned presortedIDs slice is safe to reuse only in that case.
	if order == nil && len(presortedIDs) == len(items) {
		return presortedIDs
	}
	keys := make([][]byte, len(items))
	for i := range items {
		keys[i] = items[orderedItemIndex(order, i)].id
	}
	return keys
}

func (plan *insertBatchPlan) checkPersistedConflicts(snap *backenddb.Snapshot, catalog *collectionCatalog) error {
	if plan == nil {
		return errors.New("collections: insert conflict check missing plan")
	}
	if snap == nil {
		return errors.New("collections: insert conflict check missing snapshot")
	}
	if catalog == nil {
		return errors.New("collections: insert conflict check missing catalog")
	}
	if len(plan.primaryKeys) != len(plan.resultIDs) {
		return fmt.Errorf("collections: insert conflict check missing primary keys: got %d, want %d", len(plan.primaryKeys), len(plan.resultIDs))
	}
	if len(catalog.rootOverlays) != 0 {
		uniqueIndexNames := uniqueIndexNamesWithDataOrOverlays(catalog)
		uniqueProbeRuns, err := plan.uniqueProbeRunsForPersistedConflictIndexes(func(indexName string) bool {
			_, ok := uniqueIndexNames[indexName]
			return ok
		})
		if err != nil {
			return err
		}
		if err := plan.checkPersistedDocumentConflictsAtCatalogRoot(snap, catalog); err != nil {
			return err
		}
		return checkPersistedUniqueConflictsAtCatalogRoots(snap, catalog, uniqueProbeRuns)
	}
	uniqueRootIDs := uniqueIndexRootIDs(catalog)
	uniqueProbeRuns, err := plan.uniqueProbeRunsForPersistedConflicts(uniqueRootIDs)
	if err != nil {
		return err
	}
	preflight := insertBatchPreflight{
		snapshot:           snap,
		primaryRootID:      catalog.rootID(collectionPrimaryRootName(catalog.meta.Name)),
		uniqueIndexRootIDs: uniqueRootIDs,
	}
	if err := preflight.checkDocumentConflictKeys(plan.primaryKeys); err != nil {
		return err
	}
	return preflight.checkUniqueConflicts(uniqueProbeRuns)
}

func (plan *insertBatchPlan) checkPersistedDocumentConflictsAtCatalogRoot(snap *backenddb.Snapshot, catalog *collectionCatalog) error {
	rootName := collectionPrimaryRootName(catalog.meta.Name)
	for _, key := range plan.primaryKeys {
		entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, rootName, key)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if entry.Flags&node.FlagTombstone == 0 {
			return ErrDocumentExists
		}
	}
	return nil
}

func checkPersistedUniqueConflictsAtCatalogRoots(snap *backenddb.Snapshot, catalog *collectionCatalog, runs []collectionUniqueProbeRun) error {
	if snap == nil || catalog == nil || len(runs) == 0 {
		return nil
	}
	for _, run := range runs {
		rootName := collectionSecondaryRootName(catalog.meta.Name, run.indexName)
		for _, prefix := range run.prefixes {
			it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, prefix, prefixEnd(prefix), false)
			if err != nil {
				return err
			}
			if it == nil {
				continue
			}
			conflict := it.Valid()
			iterErr := it.Error()
			closeErr := it.Close()
			if iterErr != nil {
				return iterErr
			}
			if closeErr != nil {
				return closeErr
			}
			if conflict {
				return fmt.Errorf("%w %q", ErrUniqueIndexConflict, run.indexName)
			}
		}
	}
	return nil
}

func (plan *insertBatchPlan) uniqueProbeRunsForPersistedConflicts(uniqueRootIDs map[string]uint64) ([]collectionUniqueProbeRun, error) {
	if plan == nil || len(plan.resultIDs) == 0 || len(uniqueRootIDs) == 0 {
		return nil, nil
	}
	return plan.uniqueProbeRunsForPersistedConflictIndexes(func(indexName string) bool {
		return uniqueRootIDs[indexName] != 0
	})
}

func (plan *insertBatchPlan) uniqueProbeRunsForPersistedConflictIndexes(includeIndex func(indexName string) bool) ([]collectionUniqueProbeRun, error) {
	if plan == nil || len(plan.resultIDs) == 0 || includeIndex == nil {
		return nil, nil
	}
	if plan.allUniqueProbeRunsBuilt {
		out := make([]collectionUniqueProbeRun, 0, len(plan.allUniqueProbeRuns))
		for _, run := range plan.allUniqueProbeRuns {
			if includeIndex(run.indexName) {
				out = append(out, run)
			}
		}
		return out, nil
	}
	if !plan.uniqueProbeCandidatesBuilt {
		return nil, errors.New("collections: insert conflict check missing unique probe candidates")
	}
	return buildUniqueProbeRunsFromSorted(plan.uniqueProbeCandidates, includeIndex)
}

func (p insertBatchPreflight) checkUniqueConflicts(runs []collectionUniqueProbeRun) error {
	if p.snapshot == nil || len(p.uniqueIndexRootIDs) == 0 {
		return nil
	}
	for _, run := range runs {
		rootID, ok := p.uniqueIndexRootIDs[run.indexName]
		if !ok || rootID == 0 {
			continue
		}
		exists, err := p.snapshot.HasPrefixesAtRoot(rootID, run.prefixes)
		if err != nil {
			return err
		}
		for _, ok := range exists {
			if ok {
				return fmt.Errorf("%w %q", ErrUniqueIndexConflict, run.indexName)
			}
		}
	}
	return nil
}

func borrowPrimaryDocument(_, document []byte) ([]byte, error) {
	return document, nil
}

func clonePrimaryDocument(_, document []byte) ([]byte, error) {
	return bytes.Clone(document), nil
}

func rejectDuplicateDocumentIDs(items []insertBatchItem, order []int) error {
	for i := 1; i < len(items); i++ {
		if bytes.Equal(items[orderedItemIndex(order, i-1)].id, items[orderedItemIndex(order, i)].id) {
			return ErrDuplicateDocumentID
		}
	}
	return nil
}

func (p insertBatchPlanner) indexRuntimes() ([]indexRuntime, error) {
	if p.cachedIndexRuntimes != nil || p.cachedIndexRuntimesErr != nil {
		return p.cachedIndexRuntimes, p.cachedIndexRuntimesErr
	}
	runtimes := make([]indexRuntime, len(p.indexes))
	seen := make(map[string]struct{}, len(p.indexes))
	for i, idx := range p.indexes {
		if idx.name == "" {
			return nil, errors.New("collections: index name cannot be empty")
		}
		if idx.field == "" {
			return nil, fmt.Errorf("collections: index %q field cannot be empty", idx.name)
		}
		valueType, err := normalizeIndexValueType(idx.valueType)
		if err != nil {
			return nil, fmt.Errorf("collections: index %q value_type: %w", idx.name, err)
		}
		idx.valueType = valueType
		if _, exists := seen[idx.name]; exists {
			return nil, fmt.Errorf("collections: duplicate index %q", idx.name)
		}
		seen[idx.name] = struct{}{}
		secondaryRootName := ""
		if p.collection != "" {
			secondaryRootName = collectionSecondaryRootName(p.collection, idx.name)
		}
		runtimes[i] = indexRuntime{
			def:               idx,
			path:              splitIndexPath(idx.field),
			componentPaths:    indexComponentPaths(idx.components),
			secondaryRootName: secondaryRootName,
		}
	}
	return runtimes, nil
}

func indexComponentPaths(components []IndexComponent) [][]string {
	paths := make([][]string, len(components))
	for i := range components {
		paths[i] = splitIndexPath(components[i].Field)
	}
	return paths
}

func (p insertBatchPlanner) planIndexStateAndUniqueProbes(items []insertBatchItem, runtimes []indexRuntime) ([]uniqueProbeCandidate, error) {
	if len(runtimes) == 0 {
		return nil, nil
	}
	uniqueProbes := make([]uniqueProbeCandidate, 0, len(items))
	encoder := indexEncodeArena{
		buf:       make([]byte, 0, estimateBatchIndexEncodeArenaBytes(items, len(runtimes))),
		states:    make([][][]byte, 0, estimateBatchIndexValueRefCount(items, len(runtimes))),
		valueRefs: make([][]byte, 0, estimateBatchIndexValueRefCount(items, len(runtimes))),
	}
	for i := range items {
		state, err := p.itemIndexState(items[i].id, items[i].document, runtimes, &encoder)
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

func (p insertBatchPlanner) singleItemIndexStateAndUniqueProbeRuns(item *insertBatchItem, runtimes []indexRuntime, preflight insertBatchPreflight) ([]collectionUniqueProbeRun, []collectionUniqueProbeRun, error) {
	if item == nil || len(runtimes) == 0 {
		return nil, nil, nil
	}
	encoder := indexEncodeArena{
		buf:       make([]byte, 0, estimateDocumentIndexEncodeArenaBytes(len(runtimes))),
		states:    make([][][]byte, 0, len(runtimes)),
		valueRefs: make([][]byte, 0, len(runtimes)),
	}
	state, err := p.itemIndexState(item.id, item.document, runtimes, &encoder)
	if err != nil {
		return nil, nil, err
	}
	item.state = state

	uniqueRunCount := 0
	preflightRunCount := 0
	prefixBytes := 0
	hasUniquePreflight := preflight.snapshot != nil && len(preflight.uniqueIndexRootIDs) != 0
	for runtimeIdx, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		values := state.valuesAt(runtimeIdx)
		if len(values) == 0 {
			continue
		}
		uniqueRunCount++
		if hasUniquePreflight && preflight.uniqueIndexRootIDs[runtime.def.name] != 0 {
			preflightRunCount++
		}
		for _, value := range values {
			prefixBytes += len(value)
		}
	}
	if uniqueRunCount == 0 {
		return nil, nil, nil
	}

	allRuns := make([]collectionUniqueProbeRun, 0, uniqueRunCount)
	var preflightRuns []collectionUniqueProbeRun
	if preflightRunCount > 0 && preflightRunCount < uniqueRunCount {
		preflightRuns = make([]collectionUniqueProbeRun, 0, preflightRunCount)
	}
	prefixArena := make([]byte, 0, prefixBytes)
	for runtimeIdx, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		values := state.valuesAt(runtimeIdx)
		if len(values) == 0 {
			continue
		}
		run := collectionUniqueProbeRun{
			indexName: runtime.def.name,
			prefixes:  make([][]byte, 0, len(values)),
		}
		for _, value := range values {
			var prefix []byte
			prefixArena, prefix, err = appendIndexValuePrefixSlice(prefixArena, value)
			if err != nil {
				return nil, nil, err
			}
			run.prefixes = append(run.prefixes, prefix)
		}
		allRuns = append(allRuns, run)
		if preflightRuns != nil && hasUniquePreflight && preflight.uniqueIndexRootIDs[runtime.def.name] != 0 {
			preflightRuns = append(preflightRuns, run)
		}
	}
	if preflightRunCount == uniqueRunCount {
		preflightRuns = allRuns
	}
	return allRuns, preflightRuns, nil
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
			return fmt.Errorf("%w %q", ErrUniqueIndexConflict, candidate.indexName)
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
		add := len(candidate.encodedValue)
		if add > indexEncodeArenaMaxInitialBytes-total {
			return indexEncodeArenaMaxInitialBytes
		}
		total += add
	}
	return total
}

func uniqueProbeRunsForPreflight(runs []collectionUniqueProbeRun, preflight insertBatchPreflight) []collectionUniqueProbeRun {
	if preflight.snapshot == nil || len(preflight.uniqueIndexRootIDs) == 0 || len(runs) == 0 {
		return nil
	}
	out := make([]collectionUniqueProbeRun, 0, len(runs))
	for _, run := range runs {
		rootID, ok := preflight.uniqueIndexRootIDs[run.indexName]
		if !ok || rootID == 0 {
			continue
		}
		out = append(out, run)
	}
	return out
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

func (p insertBatchPlanner) emitTemplateRun(plan *insertBatchPlan, records []templateV1Record) error {
	if len(records) == 0 {
		return nil
	}
	sortTemplateV1Records(records)
	records, err := dedupeTemplateV1Records(records)
	if err != nil {
		return err
	}
	entryCount := len(records)*2 + 1
	var maxID uint64
	for _, record := range records {
		if record.id > maxID {
			maxID = record.id
		}
	}
	nextID := maxID + 1
	table := newCollectionRunTable(entryCount)
	if err := applyCollectionRunEntries(table, entryCount, func(i int) (key, value []byte, err error) {
		if i == 0 {
			return templateV1NextIDKey(), encodeTemplateV1ID(nextID), nil
		}
		record := records[(i-1)/2]
		if (i-1)%2 == 0 {
			return templateV1HashKey(record.hash), encodeTemplateV1ID(record.id), nil
		}
		raw := record.raw
		if p.cloneTemplateRunValues {
			raw = bytes.Clone(raw)
		}
		return templateV1RecordKey(record.id), raw, nil
	}); err != nil {
		return err
	}
	table.Freeze()
	plan.runs = append(plan.runs, collectionRootRun{
		name:          p.templateRoot,
		kind:          collectionRootTemplate,
		table:         table,
		storagePolicy: p.options.dataStoragePolicy,
	})
	return nil
}

func (p insertBatchPlanner) buildDirectBufferedInsertPlan(plan *insertBatchPlan, items []insertBatchItem, primaryOrder []int, runtimes []indexRuntime, persistIndexState bool, templateRecords []templateV1Record) error {
	rootCap := 1
	if len(templateRecords) > 0 {
		rootCap++
	}
	if len(runtimes) > 0 {
		if persistIndexState {
			rootCap++
		}
		rootCap += len(runtimes)
	}
	direct := &directBufferedInsertPlan{
		templateRootName:   p.templateRoot,
		primaryRootName:    p.primaryRoot,
		indexStateRootName: p.indexStateRoot,
		rootNames:          make([]string, 0, rootCap),
		policies:           make([]backenddb.OrderedRootStoragePolicy, 0, rootCap),
		pointerizePrimary:  p.pointerizePrimary,
	}
	if len(templateRecords) > 0 {
		phaseStart := time.Now()
		entries, err := p.directBufferedTemplateRootEntries(templateRecords)
		if err != nil {
			return err
		}
		direct.templateEntries = entries
		direct.addRoot(p.templateRoot, p.options.dataStoragePolicy)
		direct.stagedBytes = saturatingAddNonNegativeInt64(direct.stagedBytes, directBufferedRootEntriesSize(entries))
		plan.stats.TemplateRunBuild = time.Since(phaseStart)
	}

	phaseStart := time.Now()
	primaryEntries, err := p.directBufferedPrimaryRootEntries(plan, items, primaryOrder)
	if err != nil {
		return err
	}
	direct.primaryEntries = primaryEntries
	direct.addRoot(p.primaryRoot, p.options.dataStoragePolicy)
	direct.stagedBytes = saturatingAddNonNegativeInt64(direct.stagedBytes, directBufferedRootEntriesSize(primaryEntries))
	plan.stats.PrimaryRunBuild = time.Since(phaseStart)

	if len(runtimes) > 0 {
		if persistIndexState {
			phaseStart = time.Now()
			entries, err := p.directBufferedIndexStateRootEntries(items, runtimes)
			if err != nil {
				return err
			}
			direct.indexStateEntries = entries
			direct.addRoot(p.indexStateRoot, p.options.indexStateStoragePolicy)
			direct.stagedBytes = saturatingAddNonNegativeInt64(direct.stagedBytes, directBufferedRootEntriesSize(entries))
			plan.stats.IndexStateRunBuild = time.Since(phaseStart)
		}
		phaseStart = time.Now()
		secondaryPlans, secondaryBytes, err := p.directBufferedSecondaryRootPlans(items, runtimes, &plan.stats.CollectionInsertStats)
		if err != nil {
			return err
		}
		direct.secondaryRootPlans = secondaryPlans
		direct.stagedBytes = saturatingAddNonNegativeInt64(direct.stagedBytes, secondaryBytes)
		for _, secondaryPlan := range secondaryPlans {
			runtimeIdx := secondaryPlan.runtimeIdx
			if runtimeIdx < 0 || runtimeIdx >= len(runtimes) {
				return fmt.Errorf("collections: invalid direct buffered secondary runtime index %d", runtimeIdx)
			}
			direct.addRoot(secondaryPlan.rootName, runtimes[runtimeIdx].def.storagePolicy)
		}
		direct.uniqueValueRootPlans = directBufferedUniqueValueRootPlans(plan.allUniqueProbeRuns, runtimes)
		for _, uniquePlan := range direct.uniqueValueRootPlans {
			direct.stagedBytes = saturatingAddNonNegativeInt64(direct.stagedBytes, int64(uniquePlan.keyBytes))
		}
		plan.stats.SecondaryRunBuild = time.Since(phaseStart)
	}
	plan.directBufferedInsert = direct
	return nil
}

func (p insertBatchPlanner) buildSingleDirectBufferedInsertPlan(plan *insertBatchPlan, item *insertBatchItem, runtimes []indexRuntime, persistIndexState bool, templateRecords []templateV1Record) error {
	if item == nil {
		return nil
	}
	rootCap := 1
	if len(templateRecords) > 0 {
		rootCap++
	}
	if len(runtimes) > 0 {
		if persistIndexState {
			rootCap++
		}
		rootCap += len(runtimes)
	}
	direct := &directBufferedInsertPlan{
		templateRootName:   p.templateRoot,
		primaryRootName:    p.primaryRoot,
		indexStateRootName: p.indexStateRoot,
		rootNames:          make([]string, 0, rootCap),
		policies:           make([]backenddb.OrderedRootStoragePolicy, 0, rootCap),
		pointerizePrimary:  p.pointerizePrimary,
	}
	if len(templateRecords) > 0 {
		phaseStart := time.Now()
		entries, err := p.directBufferedTemplateRootEntries(templateRecords)
		if err != nil {
			return err
		}
		direct.templateEntries = entries
		direct.addRoot(p.templateRoot, p.options.dataStoragePolicy)
		direct.stagedBytes = saturatingAddNonNegativeInt64(direct.stagedBytes, directBufferedRootEntriesSize(entries))
		plan.stats.TemplateRunBuild = time.Since(phaseStart)
	}

	phaseStart := time.Now()
	value, err := p.buildPrimaryVal(item.id, item.document)
	if err != nil {
		return err
	}
	plan.stats.payloadBuilds++
	direct.primaryEntries = []directBufferedRootEntry{{
		key:   item.id,
		value: value,
		flags: node.FlagInline,
	}}
	direct.addRoot(p.primaryRoot, p.options.dataStoragePolicy)
	direct.stagedBytes = saturatingAddNonNegativeInt64(direct.stagedBytes, directBufferedRootEntriesSize(direct.primaryEntries))
	plan.stats.PrimaryRunBuild = time.Since(phaseStart)

	if len(runtimes) > 0 {
		if persistIndexState {
			phaseStart = time.Now()
			entry, size, err := p.singleDirectBufferedIndexStateRootEntry(item, runtimes)
			if err != nil {
				return err
			}
			direct.indexStateEntries = []directBufferedRootEntry{entry}
			direct.addRoot(p.indexStateRoot, p.options.indexStateStoragePolicy)
			direct.stagedBytes = saturatingAddNonNegativeInt64(direct.stagedBytes, int64(size))
			plan.stats.IndexStateRunBuild = time.Since(phaseStart)
		}
		phaseStart = time.Now()
		secondaryPlans, secondaryBytes, err := p.singleDirectBufferedSecondaryRootPlans(item, runtimes, &plan.stats.CollectionInsertStats)
		if err != nil {
			return err
		}
		direct.secondaryRootPlans = secondaryPlans
		direct.stagedBytes = saturatingAddNonNegativeInt64(direct.stagedBytes, secondaryBytes)
		for _, secondaryPlan := range secondaryPlans {
			runtimeIdx := secondaryPlan.runtimeIdx
			if runtimeIdx < 0 || runtimeIdx >= len(runtimes) {
				return fmt.Errorf("collections: invalid direct buffered secondary runtime index %d", runtimeIdx)
			}
			direct.addRoot(secondaryPlan.rootName, runtimes[runtimeIdx].def.storagePolicy)
		}
		direct.uniqueValueRootPlans = directBufferedUniqueValueRootPlans(plan.allUniqueProbeRuns, runtimes)
		for _, uniquePlan := range direct.uniqueValueRootPlans {
			direct.stagedBytes = saturatingAddNonNegativeInt64(direct.stagedBytes, int64(uniquePlan.keyBytes))
		}
		plan.stats.SecondaryRunBuild = time.Since(phaseStart)
	}
	plan.directBufferedInsert = direct
	return nil
}

func (p insertBatchPlanner) directBufferedTemplateRootEntries(records []templateV1Record) ([]directBufferedRootEntry, error) {
	if len(records) == 0 {
		return nil, nil
	}
	sortTemplateV1Records(records)
	records, err := dedupeTemplateV1Records(records)
	if err != nil {
		return nil, err
	}
	entryCount := len(records)*2 + 1
	entries := make([]directBufferedRootEntry, entryCount)
	var maxID uint64
	for _, record := range records {
		if record.id > maxID {
			maxID = record.id
		}
	}
	entries[0] = directBufferedRootEntry{
		key:   templateV1NextIDKey(),
		value: encodeTemplateV1ID(maxID + 1),
		flags: node.FlagInline,
	}
	for i, record := range records {
		raw := record.raw
		if p.cloneTemplateRunValues {
			raw = bytes.Clone(raw)
		}
		entryOffset := i*2 + 1
		entries[entryOffset] = directBufferedRootEntry{
			key:   templateV1HashKey(record.hash),
			value: encodeTemplateV1ID(record.id),
			flags: node.FlagInline,
		}
		entries[entryOffset+1] = directBufferedRootEntry{
			key:   templateV1RecordKey(record.id),
			value: raw,
			flags: node.FlagInline,
		}
	}
	return entries, nil
}

func (p insertBatchPlanner) directBufferedPrimaryRootEntries(plan *insertBatchPlan, items []insertBatchItem, order []int) ([]directBufferedRootEntry, error) {
	if len(items) == 0 {
		return nil, nil
	}
	entries := make([]directBufferedRootEntry, len(items))
	for i := range items {
		idx := orderedItemIndex(order, i)
		value, err := p.buildPrimaryVal(items[idx].id, items[idx].document)
		if err != nil {
			return nil, err
		}
		if plan != nil {
			plan.stats.payloadBuilds++
		}
		entries[i] = directBufferedRootEntry{
			key:   items[idx].id,
			value: value,
			flags: node.FlagInline,
		}
	}
	return entries, nil
}

func (p insertBatchPlanner) directBufferedIndexStateRootEntries(items []insertBatchItem, runtimes []indexRuntime) ([]directBufferedRootEntry, error) {
	if len(items) == 0 {
		return nil, nil
	}
	order := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	counts := make([]int, len(items))
	valueBytes := 0
	for i := range items {
		idx := orderedItemIndex(order, i)
		count, size, err := runtimeOrderedDocumentIndexStateStats(items[idx].state, runtimes)
		if err != nil {
			return nil, err
		}
		counts[i] = count
		valueBytes += size
	}
	entries := make([]directBufferedRootEntry, len(items))
	valueArena := make([]byte, 0, valueBytes)
	for i := range items {
		idx := orderedItemIndex(order, i)
		var value []byte
		valueArena, value = appendRuntimeOrderedDocumentIndexState(valueArena, items[idx].state, runtimes, counts[i])
		entries[i] = directBufferedRootEntry{
			key:   items[idx].id,
			value: value,
			flags: node.FlagInline,
		}
	}
	return entries, nil
}

func (p insertBatchPlanner) singleDirectBufferedIndexStateRootEntry(item *insertBatchItem, runtimes []indexRuntime) (directBufferedRootEntry, int, error) {
	if item == nil {
		return directBufferedRootEntry{}, 0, nil
	}
	count, size, err := runtimeOrderedDocumentIndexStateStats(item.state, runtimes)
	if err != nil {
		return directBufferedRootEntry{}, 0, err
	}
	valueArena := make([]byte, 0, size)
	_, value := appendRuntimeOrderedDocumentIndexState(valueArena, item.state, runtimes, count)
	return directBufferedRootEntry{
		key:   item.id,
		value: value,
		flags: node.FlagInline,
	}, len(item.id) + len(value), nil
}

func (p insertBatchPlanner) directBufferedSecondaryRootPlans(items []insertBatchItem, runtimes []indexRuntime, stats *CollectionInsertStats) ([]directBufferedSecondaryRootPlan, int64, error) {
	if len(items) == 0 || len(runtimes) == 0 {
		return nil, 0, nil
	}
	plans := make([]directBufferedSecondaryRootPlan, 0, len(runtimes))
	var stagedBytes int64
	for runtimeIdx, runtime := range runtimes {
		runStart := time.Now()
		entryCount, keyBytes, alreadySorted, err := secondaryEntryOrderStats(items, runtimeIdx, nil)
		if err != nil {
			return nil, 0, err
		}
		runStats := CollectionSecondaryRunStats{
			IndexName:     runtime.def.name,
			Entries:       entryCount,
			KeyBytes:      keyBytes,
			AlreadySorted: alreadySorted,
		}
		if entryCount == 0 {
			runStats.Build = time.Since(runStart)
			if stats != nil {
				stats.SecondaryRuns = append(stats.SecondaryRuns, runStats)
			}
			continue
		}
		rootName := runtime.secondaryRootName
		if rootName == "" {
			rootName = collectionSecondaryRootName(p.collection, runtime.def.name)
		}
		secondaryPlan := directBufferedSecondaryRootPlan{
			rootName:   rootName,
			entries:    make([]directBufferedSecondaryRootEntry, 0, entryCount),
			arena:      make([]byte, 0, keyBytes),
			indexName:  runtime.def.name,
			valueType:  runtime.def.valueType,
			unique:     runtime.def.unique,
			sets:       entryCount,
			keyBytes:   keyBytes,
			runtimeIdx: runtimeIdx,
		}
		for i := range items {
			for _, encoded := range items[i].state.valuesAt(runtimeIdx) {
				var key []byte
				secondaryPlan.arena, key, err = appendIndexEntryKeyForValueType(secondaryPlan.arena, runtime.def.valueType, encoded, items[i].id)
				if err != nil {
					return nil, 0, err
				}
				secondaryPlan.entries = append(secondaryPlan.entries, directBufferedSecondaryRootEntry{key: key})
			}
		}
		runStats.Build = time.Since(runStart)
		if stats != nil {
			stats.SecondaryRuns = append(stats.SecondaryRuns, runStats)
			stats.SecondaryEntries += entryCount
			stats.SecondaryKeyBytes += keyBytes
			if alreadySorted {
				stats.SecondarySortedRuns++
			} else {
				stats.SecondaryUnsortedRuns++
			}
		}
		stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, int64(keyBytes))
		plans = append(plans, secondaryPlan)
	}
	return plans, stagedBytes, nil
}

func (p insertBatchPlanner) singleDirectBufferedSecondaryRootPlans(item *insertBatchItem, runtimes []indexRuntime, stats *CollectionInsertStats) ([]directBufferedSecondaryRootPlan, int64, error) {
	if item == nil || len(runtimes) == 0 {
		return nil, 0, nil
	}
	plans := make([]directBufferedSecondaryRootPlan, 0, len(runtimes))
	var stagedBytes int64
	for runtimeIdx, runtime := range runtimes {
		runStart := time.Now()
		values := item.state.valuesAt(runtimeIdx)
		entryCount := len(values)
		keyBytes := 0
		for _, encoded := range values {
			if len(encoded) > 65535 {
				return nil, 0, errors.New("collections: index key too large")
			}
			keyBytes += len(encoded) + len(item.id)
		}
		runStats := CollectionSecondaryRunStats{
			IndexName:     runtime.def.name,
			Entries:       entryCount,
			KeyBytes:      keyBytes,
			AlreadySorted: true,
		}
		if entryCount == 0 {
			runStats.Build = time.Since(runStart)
			if stats != nil {
				stats.SecondaryRuns = append(stats.SecondaryRuns, runStats)
			}
			continue
		}
		rootName := runtime.secondaryRootName
		if rootName == "" {
			rootName = collectionSecondaryRootName(p.collection, runtime.def.name)
		}
		secondaryPlan := directBufferedSecondaryRootPlan{
			rootName:   rootName,
			entries:    make([]directBufferedSecondaryRootEntry, entryCount),
			arena:      make([]byte, 0, keyBytes),
			indexName:  runtime.def.name,
			valueType:  runtime.def.valueType,
			unique:     runtime.def.unique,
			sets:       entryCount,
			keyBytes:   keyBytes,
			runtimeIdx: runtimeIdx,
		}
		for i, encoded := range values {
			var key []byte
			var err error
			secondaryPlan.arena, key, err = appendIndexEntryKeyForValueType(secondaryPlan.arena, runtime.def.valueType, encoded, item.id)
			if err != nil {
				return nil, 0, err
			}
			secondaryPlan.entries[i] = directBufferedSecondaryRootEntry{key: key}
		}
		runStats.Build = time.Since(runStart)
		if stats != nil {
			stats.SecondaryRuns = append(stats.SecondaryRuns, runStats)
			stats.SecondaryEntries += entryCount
			stats.SecondaryKeyBytes += keyBytes
			stats.SecondarySortedRuns++
		}
		stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, int64(keyBytes))
		plans = append(plans, secondaryPlan)
	}
	return plans, stagedBytes, nil
}

func directBufferedUniqueValueRootPlans(runs []collectionUniqueProbeRun, runtimes []indexRuntime) []directBufferedUniqueValueRootPlan {
	if len(runs) == 0 || len(runtimes) == 0 {
		return nil
	}
	var valueTypes map[string]IndexValueType
	if len(runs) > 2 && len(runtimes) > 2 {
		valueTypes = make(map[string]IndexValueType, len(runtimes))
		for _, runtime := range runtimes {
			if runtime.def.unique {
				valueTypes[runtime.def.name] = runtime.def.valueType
			}
		}
	}
	plans := make([]directBufferedUniqueValueRootPlan, 0, len(runs))
	for _, run := range runs {
		if len(run.prefixes) == 0 {
			continue
		}
		valueType, ok := valueTypes[run.indexName]
		if valueTypes == nil {
			valueType, ok = uniqueIndexValueTypeForName(runtimes, run.indexName)
		}
		if !ok {
			continue
		}
		keyBytes := 0
		for _, prefix := range run.prefixes {
			keyBytes += len(prefix)
		}
		plans = append(plans, directBufferedUniqueValueRootPlan{
			indexName: run.indexName,
			valueType: valueType,
			prefixes:  run.prefixes,
			keyBytes:  keyBytes,
		})
	}
	return plans
}

func uniqueIndexValueTypeForName(runtimes []indexRuntime, indexName string) (IndexValueType, bool) {
	for _, runtime := range runtimes {
		if runtime.def.unique && runtime.def.name == indexName {
			return runtime.def.valueType, true
		}
	}
	return "", false
}

func (plan *directBufferedInsertPlan) addRoot(rootName string, policy backenddb.OrderedRootStoragePolicy) {
	if plan == nil || rootName == "" {
		return
	}
	for i, existing := range plan.rootNames {
		if existing == rootName {
			plan.policies[i] = policy
			return
		}
	}
	plan.rootNames = append(plan.rootNames, rootName)
	plan.policies = append(plan.policies, policy)
}

func directBufferedRootEntriesSize(entries []directBufferedRootEntry) int64 {
	var total int64
	for _, entry := range entries {
		valueBytes := len(entry.value)
		if entry.flags&node.FlagPointer != 0 {
			valueBytes = page.ValuePtrSize
		}
		total = saturatingAddNonNegativeInt64(total, int64(len(entry.key)+valueBytes))
	}
	return total
}

func setCollectionRunValue(table memtable.Table, key, value []byte) {
	table.SetEntrySteal(key, value, page.ValuePtr{}, node.FlagInline)
}

func setCollectionRunCopiedValue(table memtable.Table, key, value []byte) {
	table.SetEntry(key, value, page.ValuePtr{}, node.FlagInline)
}

func applyCollectionRunEntries(table memtable.Table, count int, emit func(i int) (key, value []byte, err error)) error {
	if count <= 0 {
		return nil
	}
	return applyCollectionRunEntriesWithFlags(table, count, func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error) {
		key, value, err = emit(i)
		if err != nil {
			return nil, nil, page.ValuePtr{}, 0, err
		}
		return key, value, page.ValuePtr{}, node.FlagInline, nil
	})
}

func applyCollectionRunEntriesWithFlags(table memtable.Table, count int, emit func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error)) error {
	if count <= 0 {
		return nil
	}
	if appender, ok := table.(memtable.StealEntryFuncApplier); ok {
		return appender.ApplyStealEntryFunc(count, emit)
	}
	for i := 0; i < count; i++ {
		key, value, ptr, flags, err := emit(i)
		if err != nil {
			return err
		}
		if flags&node.FlagTombstone != 0 {
			table.DeleteSteal(key)
			continue
		}
		table.SetEntrySteal(key, value, ptr, flags)
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

func (p insertBatchPlanner) emitSecondaryRuns(plan *insertBatchPlan, items []insertBatchItem, runtimes []indexRuntime, documentIDOrder []int) error {
	for runtimeIdx, runtime := range runtimes {
		runStart := time.Now()
		entryCount, keyBytes, alreadySorted, err := secondaryEntryOrderStats(items, runtimeIdx, nil)
		if err != nil {
			return err
		}
		emitOrder := []int(nil)
		if !alreadySorted && documentIDOrder != nil {
			entryCount, keyBytes, alreadySorted, err = secondaryEntryOrderStats(items, runtimeIdx, documentIDOrder)
			if err != nil {
				return err
			}
			emitOrder = documentIDOrder
		}
		runStats := CollectionSecondaryRunStats{
			IndexName:     runtime.def.name,
			Entries:       entryCount,
			KeyBytes:      keyBytes,
			AlreadySorted: alreadySorted,
		}
		if entryCount == 0 {
			runStats.Build = time.Since(runStart)
			plan.stats.SecondaryRuns = append(plan.stats.SecondaryRuns, runStats)
			continue
		}
		if alreadySorted {
			table := newCollectionRunTable(entryCount)
			keyArena := make([]byte, 0, keyBytes)
			itemPos := 0
			valuePos := 0
			if err := applyCollectionRunEntries(table, entryCount, func(_ int) (key, value []byte, err error) {
				for itemPos < len(items) {
					idx := orderedItemIndex(emitOrder, itemPos)
					values := items[idx].state.valuesAt(runtimeIdx)
					if valuePos < len(values) {
						encoded := values[valuePos]
						valuePos++
						keyArena, key, err = appendIndexEntryKeyForValueType(keyArena, runtime.def.valueType, encoded, items[idx].id)
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
				name:           p.collection + "/index/" + runtime.def.name,
				kind:           collectionRootSecondary,
				indexName:      runtime.def.name,
				indexValueType: runtime.def.valueType,
				table:          table,
				storagePolicy:  runtime.def.storagePolicy,
			})
			runStats.Build = time.Since(runStart)
			plan.stats.SecondaryRuns = append(plan.stats.SecondaryRuns, runStats)
			plan.stats.SecondaryEntries += entryCount
			plan.stats.SecondaryKeyBytes += keyBytes
			plan.stats.SecondarySortedRuns++
			continue
		}

		if table, ok, err := p.emitGroupedSecondaryRunTable(items, runtimeIdx, runtime.def.name, runtime.def.valueType, documentIDOrder, entryCount, keyBytes); err != nil {
			return err
		} else if ok {
			plan.runs = append(plan.runs, collectionRootRun{
				name:           p.collection + "/index/" + runtime.def.name,
				kind:           collectionRootSecondary,
				indexName:      runtime.def.name,
				indexValueType: runtime.def.valueType,
				table:          table,
				storagePolicy:  runtime.def.storagePolicy,
			})
			runStats.Build = time.Since(runStart)
			plan.stats.SecondaryRuns = append(plan.stats.SecondaryRuns, runStats)
			plan.stats.SecondaryEntries += entryCount
			plan.stats.SecondaryKeyBytes += keyBytes
			plan.stats.SecondaryUnsortedRuns++
			continue
		}

		keys := make([][]byte, 0, entryCount)
		keyArena := make([]byte, 0, keyBytes)
		for i := range items {
			idx := orderedItemIndex(documentIDOrder, i)
			for _, encoded := range items[idx].state.valuesAt(runtimeIdx) {
				var key []byte
				keyArena, key, err = appendIndexEntryKeyForValueType(keyArena, runtime.def.valueType, encoded, items[idx].id)
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
			name:           p.collection + "/index/" + runtime.def.name,
			kind:           collectionRootSecondary,
			indexName:      runtime.def.name,
			indexValueType: runtime.def.valueType,
			table:          table,
			storagePolicy:  runtime.def.storagePolicy,
		})
		runStats.Build = time.Since(runStart)
		plan.stats.SecondaryRuns = append(plan.stats.SecondaryRuns, runStats)
		plan.stats.SecondaryEntries += entryCount
		plan.stats.SecondaryKeyBytes += keyBytes
		plan.stats.SecondaryUnsortedRuns++
	}
	return nil
}

type secondaryValueGroup struct {
	encoded     []byte
	documentIDs [][]byte
}

func (p insertBatchPlanner) emitGroupedSecondaryRunTable(items []insertBatchItem, runtimeIdx int, indexName string, valueType IndexValueType, documentIDOrder []int, entryCount, keyBytes int) (memtable.Table, bool, error) {
	if entryCount <= 0 {
		return nil, false, nil
	}
	groups := make([]secondaryValueGroup, 0, min(entryCount, secondaryGroupedRunMaxDistinctValues))
	for i := range items {
		idx := orderedItemIndex(documentIDOrder, i)
		item := &items[idx]
		for _, encoded := range item.state.valuesAt(runtimeIdx) {
			groupIdx := -1
			for j := range groups {
				if bytes.Equal(groups[j].encoded, encoded) {
					groupIdx = j
					break
				}
			}
			if groupIdx < 0 {
				if len(groups) >= secondaryGroupedRunMaxDistinctValues {
					return nil, false, nil
				}
				groups = append(groups, secondaryValueGroup{encoded: encoded})
				groupIdx = len(groups) - 1
			}
			groups[groupIdx].documentIDs = append(groups[groupIdx].documentIDs, item.id)
		}
	}
	if len(groups) == 0 || len(groups) == entryCount {
		return nil, false, nil
	}
	sort.Slice(groups, func(i, j int) bool {
		return compareIndexValuePrefixEncoded(groups[i].encoded, groups[j].encoded) < 0
	})

	table := newCollectionRunTable(entryCount)
	keyArena := make([]byte, 0, keyBytes)
	groupPos := 0
	documentPos := 0
	if err := applyCollectionRunEntries(table, entryCount, func(_ int) (key, value []byte, err error) {
		for groupPos < len(groups) {
			group := &groups[groupPos]
			if documentPos < len(group.documentIDs) {
				documentID := group.documentIDs[documentPos]
				documentPos++
				keyArena, key, err = appendIndexEntryKeyForValueType(keyArena, valueType, group.encoded, documentID)
				return key, nil, err
			}
			groupPos++
			documentPos = 0
		}
		return nil, nil, fmt.Errorf(
			"collections: grouped secondary index entry count mismatch collection=%q index=%q runtimeIdx=%d entryCount=%d groups=%d groupPos=%d documentPos=%d",
			p.collection,
			indexName,
			runtimeIdx,
			entryCount,
			len(groups),
			groupPos,
			documentPos,
		)
	}); err != nil {
		return nil, false, err
	}
	table.Freeze()
	return table, true, nil
}

func secondaryEntryOrderStats(items []insertBatchItem, runtimeIdx int, documentIDOrder []int) (entryCount int, keyBytes int, alreadySorted bool, err error) {
	alreadySorted = true
	var lastValue []byte
	var lastDocumentID []byte
	for i := range items {
		idx := orderedItemIndex(documentIDOrder, i)
		for _, encoded := range items[idx].state.valuesAt(runtimeIdx) {
			if len(encoded) > 65535 {
				return 0, 0, false, errors.New("collections: index key too large")
			}
			if entryCount > 0 && compareIndexEntryKeyParts(lastValue, lastDocumentID, encoded, items[idx].id) > 0 {
				alreadySorted = false
			}
			lastValue = encoded
			lastDocumentID = items[idx].id
			entryCount++
			keyBytes += len(encoded) + len(items[idx].id)
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
	encoder := indexEncodeArena{
		buf:       make([]byte, 0, estimateDocumentIndexEncodeArenaBytes(len(runtimes))),
		valueRefs: make([][]byte, 0, len(runtimes)),
	}
	return orderedIndexStateForDocumentWithArena(document, runtimes, opts, &encoder)
}

func orderedIndexStateForDocumentWithArena(document []byte, runtimes []indexRuntime, opts collectionOptions, encoder *indexEncodeArena) (orderedDocumentIndexState, error) {
	if len(runtimes) == 0 {
		return nil, nil
	}
	if encoder == nil {
		encoder = &indexEncodeArena{
			buf:       make([]byte, 0, estimateDocumentIndexEncodeArenaBytes(len(runtimes))),
			valueRefs: make([][]byte, 0, len(runtimes)),
		}
	}
	switch normalizedDocumentFormat(opts.documentFormat) {
	case DocumentFormatJSON:
	case DocumentFormatBSON:
		return bsonOrderedIndexStateForDocumentWithArena(document, runtimes, opts, encoder)
	case DocumentFormatTemplateV1:
		return templateV1OrderedIndexStateForDocumentWithArena(document, runtimes, opts, encoder)
	default:
		return nil, fmt.Errorf("collections: unsupported document format %q", opts.documentFormat)
	}
	if state, ok, err := orderedJSONRootIndexStateForDocumentFastPath(document, runtimes, opts, encoder); ok || err != nil {
		return state, err
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("collections: index extraction requires JSON document: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = errors.New("unexpected trailing JSON value")
		}
		return nil, fmt.Errorf("collections: index extraction requires JSON document: %w", err)
	}
	obj, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("collections: index extraction requires JSON object document")
	}
	state := encoder.appendState(len(runtimes))
	for runtimeIdx, runtime := range runtimes {
		value, found := extractIndexPathValue(obj, runtime.path)
		if !found || value == nil {
			continue
		}

		if arr, ok := value.([]any); ok {
			if !runtime.def.multiKey && !opts.allowArrayValuesInIndex {
				return nil, errors.New("collections: array value not allowed for index")
			}
			encoded := make([][]byte, 0, len(arr))
			for _, scalar := range arr {
				if scalar == nil {
					continue
				}
				var next []byte
				var err error
				encoder.buf, next, err = appendIndexScalar(encoder.buf, runtime.def.valueType, scalar)
				if err != nil {
					return nil, err
				}
				encoded = append(encoded, next)
			}
			switch len(encoded) {
			case 0:
				continue
			case 1:
				state[runtimeIdx] = encoder.appendSingleValueRef(encoded[0])
				continue
			default:
				encoded = normalizeOwnedEncodedIndexValues(encoded)
				if len(encoded) > 0 {
					state[runtimeIdx] = encoded
				}
				continue
			}
		}

		var next []byte
		var err error
		encoder.buf, next, err = appendIndexScalar(encoder.buf, runtime.def.valueType, value)
		if err != nil {
			return nil, err
		}
		state[runtimeIdx] = encoder.appendSingleValueRef(next)
	}
	return state, nil
}

func orderedJSONRootIndexStateForDocumentFastPath(document []byte, runtimes []indexRuntime, opts collectionOptions, encoder *indexEncodeArena) (orderedDocumentIndexState, bool, error) {
	if len(runtimes) == 0 {
		return nil, true, nil
	}
	for _, runtime := range runtimes {
		if len(runtime.path) != 1 {
			return nil, false, nil
		}
	}
	if !gjson.ValidBytes(document) {
		return nil, true, errors.New("collections: index extraction requires JSON document: invalid JSON")
	}
	if !jsonDocumentLooksObject(document) {
		return nil, true, errors.New("collections: index extraction requires JSON object document")
	}
	var stackValues [8]jsonParserIndexValue
	var values []jsonParserIndexValue
	if len(runtimes) <= len(stackValues) {
		values = stackValues[:len(runtimes)]
	} else {
		values = make([]jsonParserIndexValue, len(runtimes))
	}
	if err := jsonparser.ObjectEach(document, func(key, value []byte, dataType jsonparser.ValueType, _ int) error {
		for runtimeIdx, runtime := range runtimes {
			if runtimeRootFieldEqual(runtime, key) {
				values[runtimeIdx] = jsonParserIndexValue{
					raw:       value,
					valueType: dataType,
				}
			}
		}
		return nil
	}); err != nil {
		return nil, true, fmt.Errorf("collections: index extraction requires JSON document: %w", err)
	}
	state := encoder.appendState(len(runtimes))
	for runtimeIdx, runtime := range runtimes {
		if err := appendJSONParserIndexValueToState(state, runtimeIdx, runtime, values[runtimeIdx], opts, encoder); err != nil {
			return nil, true, err
		}
	}
	return state, true, nil
}

type jsonParserIndexValue struct {
	raw       []byte
	valueType jsonparser.ValueType
}

func jsonDocumentLooksObject(document []byte) bool {
	for _, c := range document {
		switch c {
		case ' ', '\n', '\r', '\t':
			continue
		case '{':
			return true
		default:
			return false
		}
	}
	return false
}

func runtimeRootFieldEqual(runtime indexRuntime, key []byte) bool {
	return string(key) == runtime.path[0]
}

func appendJSONParserArrayIndexValues(value []byte, valueType IndexValueType, encoder *indexEncodeArena) ([][]byte, error) {
	var values [][]byte
	var first []byte
	count := 0
	var encodeErr error
	_, err := jsonparser.ArrayEach(value, func(elem []byte, dataType jsonparser.ValueType, _ int, elemErr error) {
		if encodeErr != nil {
			return
		}
		if elemErr != nil {
			encodeErr = elemErr
			return
		}
		if dataType == jsonparser.Null || dataType == jsonparser.NotExist {
			return
		}
		var next []byte
		next, encodeErr = encoder.appendJSONParserIndexScalar(valueType, elem, dataType)
		if encodeErr != nil {
			return
		}
		count++
		if count == 1 {
			first = next
			return
		}
		if count == 2 {
			values = make([][]byte, 0, 4)
			values = append(values, first)
		}
		values = append(values, next)
	})
	if err != nil {
		return nil, err
	}
	if encodeErr != nil {
		return nil, encodeErr
	}
	switch count {
	case 0:
		return nil, nil
	case 1:
		return [][]byte{first}, nil
	default:
		return values, nil
	}
}

func appendJSONParserIndexValueToState(state orderedDocumentIndexState, runtimeIdx int, runtime indexRuntime, value jsonParserIndexValue, opts collectionOptions, encoder *indexEncodeArena) error {
	switch value.valueType {
	case jsonparser.NotExist, jsonparser.Null:
		return nil
	case jsonparser.Array:
		if !runtime.def.multiKey && !opts.allowArrayValuesInIndex {
			return errors.New("collections: array value not allowed for index")
		}
		encoded, err := appendJSONParserArrayIndexValues(value.raw, runtime.def.valueType, encoder)
		if err != nil {
			return err
		}
		if len(encoded) == 1 {
			state[runtimeIdx] = encoder.appendSingleValueRef(encoded[0])
		} else if len(encoded) > 1 {
			state[runtimeIdx] = normalizeOwnedEncodedIndexValues(encoded)
		}
		return nil
	default:
		var next []byte
		var err error
		next, err = encoder.appendJSONParserIndexScalar(runtime.def.valueType, value.raw, value.valueType)
		if err != nil {
			return err
		}
		state[runtimeIdx] = encoder.appendSingleValueRef(next)
		return nil
	}
}

func (a *indexEncodeArena) appendJSONParserIndexScalar(indexValueType IndexValueType, raw []byte, valueType jsonparser.ValueType) ([]byte, error) {
	if encoded, ok, err := a.appendJSONParserExtendedJSONIndexScalar(indexValueType, raw, valueType); ok || err != nil {
		return encoded, err
	}
	dst := a.buf
	start := len(dst)
	switch indexValueType {
	case IndexValueString:
		if valueType != jsonparser.String {
			return nil, fmt.Errorf("collections: indexed JSON value for type %q must be string, got %s", indexValueType, valueType)
		}
		if bytes.IndexByte(raw, '\\') == -1 {
			dst = appendIndexStringComponent(dst, raw)
			break
		}
		unescaped, err := jsonparser.Unescape(raw, a.scratch[:0])
		if err != nil {
			return nil, err
		}
		a.scratch = unescaped[:0]
		dst = appendIndexStringComponent(dst, unescaped)
	case IndexValueBool:
		if valueType != jsonparser.Boolean {
			return nil, fmt.Errorf("collections: indexed JSON value for type %q must be bool, got %s", indexValueType, valueType)
		}
		if len(raw) == 4 && raw[0] == 't' && raw[1] == 'r' && raw[2] == 'u' && raw[3] == 'e' {
			dst = appendIndexBoolComponent(dst, true)
		} else if len(raw) == 5 && raw[0] == 'f' && raw[1] == 'a' && raw[2] == 'l' && raw[3] == 's' && raw[4] == 'e' {
			dst = appendIndexBoolComponent(dst, false)
		} else {
			return nil, fmt.Errorf("collections: unsupported indexed JSON boolean %q", raw)
		}
	case IndexValueInt64:
		if valueType != jsonparser.Number {
			return nil, fmt.Errorf("collections: indexed JSON value for type %q must be number, got %s", indexValueType, valueType)
		}
		v, err := parseJSONInt64IndexValue(string(raw))
		if err != nil {
			return nil, err
		}
		dst = appendIndexInt64Component(dst, v)
	case IndexValueDouble:
		if valueType != jsonparser.Number {
			return nil, fmt.Errorf("collections: indexed JSON value for type %q must be number, got %s", indexValueType, valueType)
		}
		v, err := parseJSONDoubleIndexValue(string(raw))
		if err != nil {
			return nil, err
		}
		dst = appendIndexDoubleComponent(dst, v)
	default:
		return nil, fmt.Errorf("collections: unsupported index value type %q", indexValueType)
	}
	a.buf = dst
	return dst[start:len(dst):len(dst)], nil
}

func (a *indexEncodeArena) appendJSONParserExtendedJSONIndexScalar(indexValueType IndexValueType, raw []byte, valueType jsonparser.ValueType) ([]byte, bool, error) {
	if valueType != jsonparser.Object {
		return nil, false, nil
	}
	field, value, ok, err := a.jsonParserExtendedJSONNumberString(raw)
	if err != nil || !ok {
		return nil, ok, err
	}
	dst := a.buf
	start := len(dst)
	switch indexValueType {
	case IndexValueInt64:
		switch field {
		case "$numberInt", "$numberLong":
			v, err := parseJSONInt64IndexValue(value)
			if err != nil {
				return nil, true, err
			}
			dst = appendIndexInt64Component(dst, v)
		default:
			return nil, true, fmt.Errorf("collections: indexed extended JSON value for type %q must be $numberInt or $numberLong, got %s", indexValueType, field)
		}
	case IndexValueDouble:
		switch field {
		case "$numberInt", "$numberLong", "$numberDouble":
			v, err := parseJSONDoubleIndexValue(value)
			if err != nil {
				return nil, true, err
			}
			dst = appendIndexDoubleComponent(dst, v)
		default:
			return nil, true, fmt.Errorf("collections: indexed extended JSON value for type %q must be numeric, got %s", indexValueType, field)
		}
	default:
		return nil, false, nil
	}
	a.buf = dst
	return dst[start:len(dst):len(dst)], true, nil
}

func (a *indexEncodeArena) jsonParserExtendedJSONNumberString(raw []byte) (string, string, bool, error) {
	count := 0
	var field string
	var value []byte
	var valueType jsonparser.ValueType
	err := jsonparser.ObjectEach(raw, func(key, rawValue []byte, dataType jsonparser.ValueType, _ int) error {
		count++
		if count == 1 {
			field = string(key)
			value = rawValue
			valueType = dataType
		}
		return nil
	})
	if err != nil {
		return "", "", false, err
	}
	if count != 1 || !isExtendedJSONNumberField(field) {
		return "", "", false, nil
	}
	if valueType != jsonparser.String {
		return "", "", true, fmt.Errorf("collections: extended JSON numeric wrapper %s must contain a string", field)
	}
	if bytes.IndexByte(value, '\\') >= 0 {
		unescaped, err := jsonparser.Unescape(value, a.scratch[:0])
		if err != nil {
			return "", "", true, err
		}
		a.scratch = unescaped[:0]
		return field, string(unescaped), true, nil
	}
	return field, string(value), true, nil
}

func isExtendedJSONNumberField(field string) bool {
	switch field {
	case "$numberInt", "$numberLong", "$numberDouble":
		return true
	default:
		return false
	}
}

func jsonNumberLooksInteger(raw string) bool {
	return !strings.ContainsAny(raw, ".eE")
}

func (a *indexEncodeArena) appendSingleValueRef(value []byte) [][]byte {
	start := len(a.valueRefs)
	a.valueRefs = append(a.valueRefs, value)
	return a.valueRefs[start:len(a.valueRefs):len(a.valueRefs)]
}

func (a *indexEncodeArena) appendState(runtimeCount int) orderedDocumentIndexState {
	if runtimeCount <= 0 {
		return nil
	}
	if a.states == nil {
		return make(orderedDocumentIndexState, runtimeCount)
	}
	start := len(a.states)
	end := start + runtimeCount
	if end > cap(a.states) {
		nextCap := cap(a.states) * 2
		if nextCap < end {
			nextCap = end
		}
		next := make([][][]byte, end, nextCap)
		copy(next, a.states)
		a.states = next
	} else {
		a.states = a.states[:end]
	}
	return a.states[start:end:end]
}

func estimateDocumentIndexEncodeArenaBytes(runtimeCount int) int {
	if runtimeCount <= 0 {
		return 0
	}
	if runtimeCount > indexEncodeArenaMaxInitialBytes/indexEncodeArenaScalarGuessBytes {
		return indexEncodeArenaMaxInitialBytes
	}
	return runtimeCount * indexEncodeArenaScalarGuessBytes
}

func estimateBatchIndexEncodeArenaBytes(items []insertBatchItem, runtimeCount int) int {
	if len(items) == 0 || runtimeCount <= 0 {
		return 0
	}
	return estimateIndexEncodeArenaBytesForCount(len(items), runtimeCount)
}

func estimateIndexEncodeArenaBytesForCount(count, runtimeCount int) int {
	if count == 0 || runtimeCount <= 0 {
		return 0
	}
	perDocument := estimateDocumentIndexEncodeArenaBytes(runtimeCount)
	if perDocument == 0 {
		return 0
	}
	if count > indexEncodeArenaMaxInitialBytes/perDocument {
		return indexEncodeArenaMaxInitialBytes
	}
	return count * perDocument
}

func estimateBatchIndexValueRefCount(items []insertBatchItem, runtimeCount int) int {
	if len(items) == 0 || runtimeCount <= 0 {
		return 0
	}
	return estimateIndexValueRefCountForCount(len(items), runtimeCount)
}

func estimateIndexValueRefCountForCount(count, runtimeCount int) int {
	if count == 0 || runtimeCount <= 0 {
		return 0
	}
	if runtimeCount > indexEncodeArenaMaxInitialValueRefs {
		return indexEncodeArenaMaxInitialValueRefs
	}
	if count > indexEncodeArenaMaxInitialValueRefs/runtimeCount {
		return indexEncodeArenaMaxInitialValueRefs
	}
	return count * runtimeCount
}

func estimateIndexStateSlotCountForCount(count, runtimeCount int) int {
	return estimateIndexValueRefCountForCount(count, runtimeCount)
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
	if len(values) <= 1 {
		return values
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

func encodeIndexScalar(valueType IndexValueType, value any) ([]byte, error) {
	_, encoded, err := appendIndexScalar(nil, valueType, value)
	return encoded, err
}

func appendIndexScalar(dst []byte, valueType IndexValueType, value any) ([]byte, []byte, error) {
	start := len(dst)
	switch valueType {
	case IndexValueBSONOrderedV2:
		raw, ok := value.(bson.RawValue)
		if !ok {
			return dst, nil, fmt.Errorf("collections: indexed value for type %q must be bson.RawValue, got %T", valueType, value)
		}
		var err error
		dst, _, err = appendBSONIndexKeyComponentV2(dst, raw)
		if err != nil {
			return dst, nil, err
		}
	case IndexValueString:
		v, ok := value.(string)
		if !ok {
			return dst, nil, fmt.Errorf("collections: indexed value for type %q must be string, got %T", valueType, value)
		}
		dst = appendIndexStringComponent(dst, []byte(v))
	case IndexValueBool:
		v, ok := value.(bool)
		if !ok {
			return dst, nil, fmt.Errorf("collections: indexed value for type %q must be bool, got %T", valueType, value)
		}
		dst = appendIndexBoolComponent(dst, v)
	case IndexValueInt64:
		v, err := indexInt64Value(value)
		if err != nil {
			return dst, nil, err
		}
		dst = appendIndexInt64Component(dst, v)
	case IndexValueDouble:
		v, err := indexDoubleValue(value)
		if err != nil {
			return dst, nil, err
		}
		dst = appendIndexDoubleComponent(dst, v)
	default:
		return dst, nil, fmt.Errorf("collections: unsupported index value type %q", valueType)
	}
	return dst, dst[start:len(dst):len(dst)], nil
}

func appendIndexStringComponent(dst []byte, value []byte) []byte {
	for _, c := range value {
		if c == 0x00 {
			dst = append(dst, 0x00, 0xff)
			continue
		}
		dst = append(dst, c)
	}
	return append(dst, 0x00, 0x00)
}

func appendIndexBoolComponent(dst []byte, value bool) []byte {
	if value {
		return append(dst, 0x01)
	}
	return append(dst, 0x00)
}

func appendIndexInt64Component(dst []byte, value int64) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(value)^0x8000000000000000)
}

func appendIndexDoubleComponent(dst []byte, value float64) []byte {
	switch {
	case math.IsNaN(value):
		return append(dst, 0x00)
	case math.IsInf(value, -1):
		return append(dst, 0x01)
	case math.IsInf(value, 1):
		return append(dst, 0x03)
	default:
		if value == 0 {
			value = 0
		}
		dst = append(dst, 0x02)
		return binary.BigEndian.AppendUint64(dst, sortableFloat64Bits(value))
	}
}

func sortableFloat64Bits(value float64) uint64 {
	bits := math.Float64bits(value)
	if bits&(1<<63) != 0 {
		return ^bits
	}
	return bits ^ (1 << 63)
}

func indexInt64Value(value any) (int64, error) {
	switch v := value.(type) {
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case json.Number:
		return parseJSONInt64IndexValue(v.String())
	default:
		return 0, fmt.Errorf("collections: indexed value for type %q must be int64-compatible, got %T", IndexValueInt64, value)
	}
}

func parseJSONInt64IndexValue(raw string) (int64, error) {
	if !jsonNumberLooksInteger(raw) {
		return 0, fmt.Errorf("collections: indexed JSON number %q is not an int64", raw)
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("collections: unsupported indexed JSON int64 %q: %w", raw, err)
	}
	return v, nil
}

func indexDoubleValue(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case int32:
		return float64(v), nil
	case int64:
		return int64IndexValueAsExactFloat64(v)
	case json.Number:
		return parseJSONDoubleIndexValue(v.String())
	default:
		return 0, fmt.Errorf("collections: indexed value for type %q must be double-compatible, got %T", IndexValueDouble, value)
	}
}

func int64IndexValueAsExactFloat64(value int64) (float64, error) {
	out := float64(value)
	roundTrip, err := exactFloat64AsInt64(out)
	if err != nil || roundTrip != value {
		return 0, fmt.Errorf("collections: indexed int64 %d cannot be represented exactly as double", value)
	}
	return out, nil
}

func exactFloat64AsInt64(value float64) (int64, error) {
	const int64UpperBoundAsFloat64 = 9223372036854775808.0
	if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value ||
		value < -int64UpperBoundAsFloat64 || value >= int64UpperBoundAsFloat64 {
		return 0, fmt.Errorf("collections: indexed number %v is not an int64", value)
	}
	out := int64(value)
	if float64(out) != value {
		return 0, fmt.Errorf("collections: indexed number %v is not exactly representable as int64", value)
	}
	return out, nil
}

func parseJSONDoubleIndexValue(raw string) (float64, error) {
	if jsonNumberLooksInteger(raw) {
		value, err := parseJSONInt64IndexValue(raw)
		if err != nil {
			return 0, err
		}
		return int64IndexValueAsExactFloat64(value)
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(value, 0) {
		if err != nil {
			return 0, fmt.Errorf("collections: unsupported indexed JSON double %q: %w", raw, err)
		}
		return 0, fmt.Errorf("collections: unsupported indexed JSON double %q", raw)
	}
	return value, nil
}

func indexEntryKey(encodedValue, documentID []byte) ([]byte, error) {
	key := make([]byte, 0, len(encodedValue)+len(documentID))
	key, out, err := appendIndexEntryKey(key, encodedValue, documentID)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func appendIndexEntryKey(dst, encodedValue, documentID []byte) ([]byte, []byte, error) {
	start := len(dst)
	dst = append(dst, encodedValue...)
	dst = append(dst, documentID...)
	return dst, dst[start:len(dst):len(dst)], nil
}

// appendIndexEntryKeyForValueType keeps legacy typed-v1 roots byte-for-byte
// stable while making BSON v2 roots use their mandatory, self-delimiting ID
// suffix. Callers must select this from index metadata, never from key bytes.
func appendIndexEntryKeyForValueType(dst []byte, valueType IndexValueType, encodedValue, documentID []byte) ([]byte, []byte, error) {
	if valueType != IndexValueBSONOrderedV2 {
		return appendIndexEntryKey(dst, encodedValue, documentID)
	}
	return appendBSONIndexEntryKeyV2(dst, encodedValue, documentID)
}

func setCollectionSecondaryIndexEntryForValueType(table memtable.Table, valueType IndexValueType, encodedValue, documentID []byte) (int, error) {
	if table == nil {
		return 0, nil
	}
	if valueType != IndexValueBSONOrderedV2 {
		if keyParts, ok := table.(memtable.KeyPartsWriter); ok {
			keyParts.SetInlineNilKeyParts(encodedValue, documentID)
			return len(encodedValue) + len(documentID), nil
		}
	}
	_, key, err := appendIndexEntryKeyForValueType(nil, valueType, encodedValue, documentID)
	if err != nil {
		return 0, err
	}
	table.SetSteal(key, nil)
	return len(key), nil
}

func setCollectionSecondaryIndexEntry(table memtable.Table, encodedValue, documentID []byte) (int, error) {
	return setCollectionSecondaryIndexEntryForValueType(table, IndexValueString, encodedValue, documentID)
}

func deleteCollectionSecondaryIndexEntryForValueType(table memtable.Table, valueType IndexValueType, encodedValue, documentID []byte) (int, error) {
	if table == nil {
		return 0, nil
	}
	if valueType != IndexValueBSONOrderedV2 {
		if keyParts, ok := table.(memtable.KeyPartsWriter); ok {
			keyParts.DeleteKeyParts(encodedValue, documentID)
			return len(encodedValue) + len(documentID), nil
		}
	}
	_, key, err := appendIndexEntryKeyForValueType(nil, valueType, encodedValue, documentID)
	if err != nil {
		return 0, err
	}
	table.DeleteSteal(key)
	return len(key), nil
}

func deleteCollectionSecondaryIndexEntry(table memtable.Table, encodedValue, documentID []byte) (int, error) {
	return deleteCollectionSecondaryIndexEntryForValueType(table, IndexValueString, encodedValue, documentID)
}

func indexValuePrefix(encodedValue []byte) ([]byte, error) {
	_, prefix, err := appendIndexValuePrefixSlice(make([]byte, 0, len(encodedValue)), encodedValue)
	return prefix, err
}

func compareIndexValuePrefixEncoded(a, b []byte) int {
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

func indexKeyDocumentID(valueType IndexValueType, key []byte) ([]byte, error) {
	if valueType == IndexValueBSONOrderedV2 {
		return bsonIndexKeyDocumentIDV2(key)
	}
	n, err := indexComponentLength(valueType, key)
	if err != nil {
		return nil, err
	}
	if len(key) == n {
		return nil, errors.New("collections: malformed secondary index key: missing document id")
	}
	return key[n:], nil
}

func indexComponentLength(valueType IndexValueType, key []byte) (int, error) {
	switch valueType {
	case IndexValueBSONOrderedV2:
		return bsonIndexKeyComponentV2Length(key)
	case IndexValueString:
		for i := 0; i < len(key); i++ {
			if key[i] != 0x00 {
				continue
			}
			if i+1 >= len(key) {
				return 0, errors.New("collections: malformed string index component")
			}
			switch key[i+1] {
			case 0x00:
				return i + 2, nil
			case 0xff:
				i++
			default:
				return 0, errors.New("collections: malformed string index component escape")
			}
		}
		return 0, errors.New("collections: unterminated string index component")
	case IndexValueBool:
		if len(key) < 1 {
			return 0, errors.New("collections: malformed bool index component")
		}
		if key[0] != 0x00 && key[0] != 0x01 {
			return 0, errors.New("collections: malformed bool index component")
		}
		return 1, nil
	case IndexValueInt64:
		if len(key) < 8 {
			return 0, errors.New("collections: malformed int64 index component")
		}
		return 8, nil
	case IndexValueDouble:
		if len(key) < 1 {
			return 0, errors.New("collections: malformed double index component")
		}
		switch key[0] {
		case 0x00, 0x01, 0x03:
			return 1, nil
		case 0x02:
			if len(key) < 9 {
				return 0, errors.New("collections: malformed finite double index component")
			}
			return 9, nil
		default:
			return 0, errors.New("collections: malformed double index component")
		}
	default:
		return 0, fmt.Errorf("collections: unsupported index value type %q", valueType)
	}
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
