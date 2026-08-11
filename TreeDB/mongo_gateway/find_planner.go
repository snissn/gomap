package mongogateway

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/buger/jsonparser"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// errMongoFindScanCapExceeded identifies bounded planner work exhaustion.
// Explain relies on the error identity, never the client-facing text.
var errMongoFindScanCapExceeded = errors.New("mongo gateway find scan cap exceeded")

type findPredicateOp uint8

const (
	findPredicateEq findPredicateOp = iota + 1
	findPredicateIn
	findPredicateGT
	findPredicateGTE
	findPredicateLT
	findPredicateLTE
	findPredicateNE
	findPredicateNIN
	findPredicateExists
)

type findPredicate struct {
	field                   string
	op                      findPredicateOp
	values                  []bson.RawValue
	compoundCanonicalValues []bson.RawValue
	compoundCanonicalized   bool
	// negate is used only for field-level $not.  It deliberately remains a
	// residual predicate: no complement/index-union claim is safe here.
	negate bool
}

type findSort struct {
	field string
	desc  bool
	terms []findSortTerm
}

type findSortTerm struct {
	field string
	desc  bool
}

type findHint struct {
	present    bool
	name       string
	components []collections.IndexComponent
}

type findPlan struct {
	predicates  []findPredicate
	orBranches  [][]findPredicate
	norBranches [][]findPredicate
	sort        findSort
	skip        int32
	limit       int32
	hint        findHint
	projection  compiledProjection
	stats       *findExecutionStats
}

// cloneFindPlanForCursor detaches predicate BSON values from the wire command
// buffer before a getMore may reuse that buffer on another request.
func cloneFindPlanForCursor(plan findPlan) findPlan {
	clonePredicates := func(in []findPredicate) []findPredicate {
		out := make([]findPredicate, len(in))
		for i := range in {
			out[i] = in[i]
			// Compound canonicalization is a request-local planning cache. It
			// points at command-buffer BSON and is neither needed nor safe to
			// retain in an ID cursor.
			out[i].compoundCanonicalValues = nil
			out[i].compoundCanonicalized = false
			out[i].values = make([]bson.RawValue, len(in[i].values))
			for j := range in[i].values {
				out[i].values[j] = in[i].values[j]
				out[i].values[j].Value = bytes.Clone(in[i].values[j].Value)
			}
		}
		return out
	}
	out := plan
	out.predicates = clonePredicates(plan.predicates)
	out.orBranches = make([][]findPredicate, len(plan.orBranches))
	for i := range plan.orBranches {
		out.orBranches[i] = clonePredicates(plan.orBranches[i])
	}
	out.norBranches = make([][]findPredicate, len(plan.norBranches))
	for i := range plan.norBranches {
		out.norBranches[i] = clonePredicates(plan.norBranches[i])
	}
	out.sort.terms = append([]findSortTerm(nil), plan.sort.terms...)
	out.hint.components = append([]collections.IndexComponent(nil), plan.hint.components...)
	return out
}

// findPlanCursorRetainedBytes counts all payloads retained by a cloned plan.
// Raw BSON values are cloned per occurrence. Command strings are only copied
// as headers, so aliases of the same backing bytes are charged once while
// equal text backed by separate command allocations is charged separately.
func findPlanCursorRetainedBytes(plan findPlan) int {
	bytes := 0
	bytes += len(plan.orBranches) * int(unsafe.Sizeof([]findPredicate{}))
	bytes += len(plan.norBranches) * int(unsafe.Sizeof([]findPredicate{}))
	bytes += len(plan.sort.terms) * int(unsafe.Sizeof(findSortTerm{}))
	bytes += len(plan.hint.components) * int(unsafe.Sizeof(collections.IndexComponent{}))
	// A cloned projection retains a Go map allocation and its groups, in
	// addition to the field bytes charged below. Runtime map internals vary by
	// Go release (including Swiss-map group layout), so cursor admission uses a
	// stable conservative owned-memory model rather than unsafe.Sizeof(map),
	// which measures only the map header. These figures intentionally exceed a
	// one-group map and leave room for per-entry string/header bookkeeping.
	const projectionMapBaseBytes = 256
	const projectionMapEntryBytes = 64
	if len(plan.projection.fields) != 0 {
		bytes += projectionMapBaseBytes
		bytes += len(plan.projection.fields) * projectionMapEntryBytes
	}
	type stringAllocation struct {
		data *byte
		len  int
	}
	seenStrings := make(map[stringAllocation]struct{})
	addString := func(value string) {
		allocation := stringAllocation{data: unsafe.StringData(value), len: len(value)}
		if _, duplicate := seenStrings[allocation]; duplicate {
			return
		}
		seenStrings[allocation] = struct{}{}
		bytes += len(value)
	}
	add := func(predicates []findPredicate) {
		bytes += len(predicates) * int(unsafe.Sizeof(findPredicate{}))
		for _, predicate := range predicates {
			bytes += len(predicate.values) * int(unsafe.Sizeof(bson.RawValue{}))
			addString(predicate.field)
			for _, value := range predicate.values {
				bytes += len(value.Value)
			}
		}
	}
	add(plan.predicates)
	for _, branch := range plan.orBranches {
		add(branch)
	}
	for _, branch := range plan.norBranches {
		add(branch)
	}
	for field := range plan.projection.fields {
		addString(field)
	}
	addString(plan.sort.field)
	for _, term := range plan.sort.terms {
		addString(term.field)
	}
	addString(plan.hint.name)
	for _, component := range plan.hint.components {
		addString(component.Field)
	}
	return bytes
}

type findResultSet struct {
	docs       []wire.Document
	projection compiledProjection
}

// findPlannerSelection is the metadata-only half of find planning. It is used
// by explain queryPlanner and by executionStats before any document is opened.
// It intentionally contains gateway vocabulary only, never storage topology.
type findPlannerSelection struct {
	stage           string
	indexName       string
	indexField      string
	residualFilters int
	sortSatisfied   bool
	equalityPrefix  int
	hasRange        bool
	reverse         bool
}

type findIndexProbe struct {
	idx   collections.IndexDefinition
	stage string
}

// findIndexProbes is the shared eligibility vocabulary for explain and the
// indexed executor: each entry is a concrete equality or range probe that can
// actually be issued for this index/value type combination.
func findIndexProbes(meta collections.CollectionMeta, plan findPlan) []findIndexProbe {
	// A strict compound hint is an execution contract, not a preference: legacy
	// scalar paths must neither compete with it nor leak into explain output.
	if len(plan.orBranches) != 0 || plan.hint.present {
		return nil
	}
	probes := make([]findIndexProbe, 0)
	for _, idx := range meta.Indexes {
		probes = append(probes, findIndexProbesForIndex(plan, idx)...)
	}
	return probes
}

// findIndexProbesForIndex identifies the probe paths the executor can issue
// for one index. Keep this stricter than a generic bounded scan: a lossy
// numeric coercion must not be presented as an indexed range probe.
func findIndexProbesForIndex(plan findPlan, idx collections.IndexDefinition) []findIndexProbe {
	if len(plan.orBranches) != 0 || !legacyFindPlannerIndexUsable(idx) {
		return nil
	}
	probes := make([]findIndexProbe, 0, 2)
	for _, pred := range plan.predicates {
		if pred.field != idx.Field || predicateContainsNull(pred) || (pred.op != findPredicateEq && pred.op != findPredicateIn) {
			continue
		}
		// Equality probes retain the executor's existing empty-result behavior
		// for an incompatible scalar (for example 37.5 against int64). They
		// are still a deterministic candidate path, unlike a lossy range.
		probes = append(probes, findIndexProbe{idx: idx, stage: "secondary_equality_lookup"})
		break
	}
	if indexedRangeProbeEligible(plan.predicates, idx) {
		probes = append(probes, findIndexProbe{idx: idx, stage: "secondary_range_lookup"})
	}
	return probes
}

func indexedRangeProbeEligible(predicates []findPredicate, idx collections.IndexDefinition) bool {
	// indexRangeOptionsForPredicates is also the executor's source of truth.
	// A known-empty range (NaN, a disjoint scalar, or contradictory bounds)
	// remains an executable zero-work path; it must not fall through to a
	// bounded collection scan merely because no index lookup is needed.
	_, ok, _, err := indexRangeOptionsForPredicates(predicates, idx)
	return err == nil && ok
}

func selectFindPlannerSelection(meta collections.CollectionMeta, plan findPlan) findPlannerSelection {
	if len(plan.orBranches) != 0 {
		return findPlannerSelection{stage: "bounded_scan"}
	}
	if !plan.hint.present {
		if _, ok := primaryCandidatePredicate(plan.predicates); ok {
			return findPlannerSelection{stage: "primary_lookup"}
		}
	}
	compound, compoundOK := compoundIndexPlanFor(meta, plan)
	if compoundOK && !compoundPlanDeferredToLegacyLookup(meta, plan) {
		return findPlannerSelection{stage: "compound_index_scan", indexName: compound.idx.Name, indexField: compound.idx.Field, residualFilters: compound.residualFilters, sortSatisfied: compound.sortSatisfied, equalityPrefix: compound.equalityPrefix, hasRange: compound.hasRange, reverse: compound.reverse}
	}
	if probes := findIndexProbes(meta, plan); len(probes) != 0 {
		return findPlannerSelection{stage: probes[0].stage, indexName: probes[0].idx.Name, indexField: probes[0].idx.Field}
	}
	if compoundOK {
		return findPlannerSelection{stage: "compound_index_scan", indexName: compound.idx.Name, indexField: compound.idx.Field, residualFilters: compound.residualFilters, sortSatisfied: compound.sortSatisfied, equalityPrefix: compound.equalityPrefix, hasRange: compound.hasRange, reverse: compound.reverse}
	}
	return findPlannerSelection{stage: "bounded_scan"}
}

func parseFindPlan(command wire.Document, filter wire.Document) (findPlan, error) {
	predicates, orBranches, norBranches, err := parseFindFilter(filter)
	if err != nil {
		return findPlan{}, err
	}
	sortSpec, err := parseFindSort(command)
	if err != nil {
		return findPlan{}, err
	}
	skip, limit, err := parseFindPagination(command)
	if err != nil {
		return findPlan{}, err
	}
	hint, err := parseFindHint(command)
	if err != nil {
		return findPlan{}, err
	}
	projectionDoc, err := commandOptionalDocument(command, "projection")
	if err != nil {
		return findPlan{}, err
	}
	projection, err := compileProjection(projectionDoc)
	if err != nil {
		return findPlan{}, err
	}
	return finalizeFindPlan(findPlan{
		predicates:  predicates,
		orBranches:  orBranches,
		norBranches: norBranches,
		sort:        sortSpec,
		skip:        skip,
		limit:       limit,
		hint:        hint,
		projection:  projection,
	}), nil
}

func parseFindHint(command wire.Document) (findHint, error) {
	value := bson.Raw(command).Lookup("hint")
	if value.IsZero() {
		return findHint{}, nil
	}
	if name, ok := value.StringValueOK(); ok {
		if strings.TrimSpace(name) == "" {
			return findHint{}, errors.New("Mongo gateway find hint name must be non-empty")
		}
		return findHint{present: true, name: name}, nil
	}
	doc, ok := value.DocumentOK()
	if !ok {
		return findHint{}, errors.New("Mongo gateway find hint must be an index name or exact key pattern")
	}
	elements, err := doc.Elements()
	if err != nil {
		return findHint{}, err
	}
	if len(elements) == 0 || len(elements) > 4 {
		return findHint{}, errors.New("Mongo gateway find hint key pattern must contain one through four fields")
	}
	components := make([]collections.IndexComponent, 0, len(elements))
	seen := make(map[string]struct{}, len(elements))
	for _, element := range elements {
		field, err := element.KeyErr()
		if err != nil {
			return findHint{}, err
		}
		if err := collections.ValidateIndexPath(field); err != nil {
			return findHint{}, fmt.Errorf("Mongo gateway find hint field %q: %w", field, err)
		}
		if _, duplicate := seen[field]; duplicate {
			return findHint{}, fmt.Errorf("Mongo gateway find hint repeats field %q", field)
		}
		seen[field] = struct{}{}
		direction := collections.IndexDirectionAscending
		if !isAscendingIndexKey(element.Value()) {
			desc, err := findSortDirectionValue(element.Value())
			if err != nil || !desc {
				return findHint{}, fmt.Errorf("Mongo gateway find hint direction for %q must be 1 or -1", field)
			}
			direction = collections.IndexDirectionDescending
		}
		components = append(components, collections.IndexComponent{Field: field, Direction: direction})
	}
	return findHint{present: true, components: components}, nil
}

func (s *Server) executeFind(col *collections.Collection, plan findPlan) (findResultSet, error) {
	plan = finalizeFindPlan(plan)
	// Selection metadata is diagnostic-only. Avoid allocating probe descriptors
	// on the ordinary find path; executionStats receives the same selector and
	// may later refine it to the materialized winner.
	if plan.stats != nil {
		selection := selectFindPlannerSelection(col.MetaView(), plan)
		plan.recordWinner(selection.stage, selection.indexName)
	}
	if err := validateCompoundHint(col.MetaView(), plan); err != nil {
		return findResultSet{}, err
	}
	materializer, err := storedDocumentMaterializerForCollection(col)
	if err != nil {
		return findResultSet{}, err
	}
	defer func() { _ = materializer.Close() }()

	// MaxFindScanDocuments is a candidate-work cap, not a page-size cap. It is
	// enforced while scanning candidates. Unsorted collection scans can stop
	// early once skip/limit is satisfied because later records cannot affect the
	// result set.
	if docs, compound, ok, err := s.documentsForCompoundIndexPlan(col, materializer, plan); ok || err != nil {
		if err != nil {
			return findResultSet{}, err
		}
		filtered := make([]wire.Document, 0, len(docs))
		for _, doc := range docs {
			match, err := documentMatchesPlan(doc, plan)
			if err != nil {
				return findResultSet{}, err
			}
			if match {
				filtered = append(filtered, doc)
			}
		}
		if plan.sort.field != "" && !compound.sortSatisfied {
			if err := validateFindSortDocuments(filtered, plan.sort); err != nil {
				return findResultSet{}, err
			}
			sort.SliceStable(filtered, func(i, j int) bool {
				return compareDocumentsForFindSort(filtered[i], filtered[j], plan.sort) < 0
			})
		}
		alreadyPaginated := compoundPlanPaginationSafe(compound, plan)
		if plan.skip > 0 && !alreadyPaginated {
			if int(plan.skip) >= len(filtered) {
				filtered = nil
			} else {
				filtered = filtered[plan.skip:]
			}
		}
		if plan.limit > 0 && !alreadyPaginated && int(plan.limit) < len(filtered) {
			filtered = filtered[:plan.limit]
		}
		plan.recordCompoundWinner(compound)
		plan.recordReturned(len(filtered))
		return findResultSet{docs: filtered, projection: plan.projection}, nil
	}
	if docs, ok, err := s.findUnsortedScanDocuments(col, materializer, plan); ok || err != nil {
		if err != nil {
			return findResultSet{}, err
		}
		plan.recordReturned(len(docs))
		return findResultSet{docs: docs, projection: plan.projection}, nil
	}
	if docs, ok, err := s.findPureIndexedRangeLimitDocuments(col, materializer, plan); ok || err != nil {
		if err != nil {
			return findResultSet{}, err
		}
		plan.recordReturned(len(docs))
		return findResultSet{docs: docs, projection: plan.projection}, nil
	}
	docs, err := s.findCandidateDocuments(col, materializer, plan)
	if err != nil {
		return findResultSet{}, err
	}
	filtered := make([]wire.Document, 0, len(docs))
	for _, doc := range docs {
		match, err := documentMatchesPlan(doc, plan)
		if err != nil {
			return findResultSet{}, err
		}
		if match {
			filtered = append(filtered, doc)
		}
	}
	docs = filtered

	if plan.sort.field != "" {
		if err := validateFindSortDocuments(docs, plan.sort); err != nil {
			return findResultSet{}, err
		}
		sort.SliceStable(docs, func(i, j int) bool { return compareDocumentsForFindSort(docs[i], docs[j], plan.sort) < 0 })
	}

	if plan.skip > 0 {
		if int(plan.skip) >= len(docs) {
			docs = nil
		} else {
			docs = docs[plan.skip:]
		}
	}
	if plan.limit > 0 && int(plan.limit) < len(docs) {
		docs = docs[:plan.limit]
	}
	if len(docs) > 0 {
		docs = append([]wire.Document(nil), docs...)
	}
	plan.recordReturned(len(docs))
	return findResultSet{docs: docs, projection: plan.projection}, nil
}

const (
	findBatchOverheadBytes                = 5 // BSON document length plus trailing NUL.
	findBatchResponseReserveBytes         = 4096
	mongoQueryMaxDecimal128Normalizations = 1024
	// Negative predicates are residual work. Bound both their choice lists and
	// boolean branch fan-out so a scan cap cannot multiply an unbounded filter.
	mongoQueryMaxNegativeChoices   = 256
	mongoQueryMaxBooleanBranches   = 64
	mongoQueryMaxBooleanPredicates = 256
)

func findBatchDocumentBytes(doc wire.Document, index int) int {
	return len(doc) + bsonArrayElementOverhead(index)
}

func bsonArrayElementOverhead(index int) int {
	return 1 + bsonArrayIndexDigitCount(index) + 1
}

func bsonArrayIndexDigitCount(index int) int {
	if index < 0 {
		return len(strconv.Itoa(index))
	}
	if index < 10 {
		return 1
	}
	digits := 1
	for index >= 10 {
		index /= 10
		digits++
	}
	return digits
}

func validateFindCommandOptions(command wire.Document, filter wire.Document) error {
	_, err := parseFindPlan(command, filter)
	if err != nil {
		return err
	}
	batchSize, batchSizeSet, err := optionalInt32FieldWithPresence(command, "batchSize")
	if err != nil {
		return err
	}
	if _, err := normalizeBatchSize(int(batchSize), batchSizeSet, defaultCursorBatchSize); err != nil {
		return err
	}
	if _, err := optionalBoolField(command, "singleBatch"); err != nil {
		return err
	}
	return nil
}

func (s *Server) maxFindBatchBytes() int {
	max := int(s.maxMessageLength()) - findBatchResponseReserveBytes
	if max < 0 {
		return 0
	}
	return max
}

func (s *Server) findPureIndexedRangeLimitDocuments(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, plan findPlan) ([]wire.Document, bool, error) {
	idx, opts, limit, ok, empty, err := pureIndexedRangeLimitPlan(col.MetaView(), plan, s.maxFindScanDocuments())
	if err != nil || !ok {
		return nil, ok, err
	}
	if empty || limit == 0 {
		plan.recordWinner("secondary_range_lookup", idx.Name)
		return nil, true, nil
	}
	candidateLimit := candidateLimitWithOverflowSlot(limit)
	docs, candidates, err := documentsForIndexedRange(col, materializer, idx, opts, candidateLimit, limit, false)
	plan.recordCandidates(candidates)
	plan.recordWinner("secondary_range_lookup", idx.Name)
	return docs, true, err
}

func pureIndexedRangeLimitPlan(meta collections.CollectionMeta, plan findPlan, maxDocuments int) (collections.IndexDefinition, collections.IndexRangeOptions, int, bool, bool, error) {
	if len(plan.orBranches) != 0 || len(plan.norBranches) != 0 || plan.limit <= 0 || plan.skip != 0 || plan.sort.field != "" || len(plan.predicates) != 1 {
		return collections.IndexDefinition{}, collections.IndexRangeOptions{}, 0, false, false, nil
	}
	if int64(plan.limit) > int64(maxInt) {
		return collections.IndexDefinition{}, collections.IndexRangeOptions{}, 0, false, false, nil
	}
	limit := int(plan.limit)
	if maxDocuments > 0 && limit > maxDocuments {
		return collections.IndexDefinition{}, collections.IndexRangeOptions{}, 0, false, false, nil
	}
	pred := plan.predicates[0]
	if !isRangePredicate(pred.op) {
		return collections.IndexDefinition{}, collections.IndexRangeOptions{}, 0, false, false, nil
	}
	for _, idx := range meta.Indexes {
		if !legacyFindPlannerIndexUsable(idx) || idx.Field != pred.field {
			continue
		}
		opts, ok, empty, err := indexRangeOptionsForPredicates(plan.predicates, idx)
		if err != nil {
			return collections.IndexDefinition{}, collections.IndexRangeOptions{}, 0, false, false, err
		}
		if !ok {
			continue
		}
		return idx, opts, limit, true, empty, nil
	}
	return collections.IndexDefinition{}, collections.IndexRangeOptions{}, 0, false, false, nil
}

func (s *Server) findCandidateDocuments(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, plan findPlan) ([]wire.Document, error) {
	meta := col.MetaView()
	maxDocuments := s.maxFindScanDocuments()
	if len(plan.orBranches) != 0 {
		plan.recordWinner("bounded_scan", "")
		records, truncated, err := col.ScanDocuments(maxDocuments)
		if err != nil {
			return nil, err
		}
		if truncated {
			for range records {
				plan.recordCandidate()
			}
			return nil, fmt.Errorf("%w: Mongo gateway find requires a bounded scan and exceeded %d documents", errMongoFindScanCapExceeded, maxDocuments)
		}
		out := make([]wire.Document, 0, len(records))
		for _, record := range records {
			doc, err := storedDocumentToBSON(col, materializer, record.Document)
			if err != nil {
				return nil, err
			}
			out = append(out, doc)
		}
		plan.recordCandidates(len(out))
		return out, nil
	}
	var primaryDocs []wire.Document
	primarySet := false
	predicates := plan.predicates
	if pred, ok := primaryCandidatePredicate(predicates); ok {
		docs, err := documentsForPrimaryPredicate(col, materializer, pred, maxDocuments)
		if err != nil {
			return nil, err
		}
		primaryDocs = docs
		primarySet = true
		// Primary candidates are materialized while comparing this path with
		// usable secondary indexes, so they are actual planner work even if a
		// secondary path eventually wins.
		plan.recordCandidates(len(docs))
	}
	if docs, indexedStage, indexedName, ok, err := s.bestIndexedCandidateDocuments(col, materializer, meta, plan, maxDocuments); ok || err != nil {
		if err != nil {
			return nil, err
		}
		if !primarySet || len(docs) < len(primaryDocs) {
			plan.recordWinner(indexedStage, indexedName)
			docs, limitErr := s.limitCandidateDocuments(docs)
			return docs, limitErr
		}
	}
	if primarySet {
		plan.recordWinner("primary_lookup", "")
		docs, limitErr := s.limitCandidateDocuments(primaryDocs)
		return docs, limitErr
	}
	records, truncated, err := col.ScanDocuments(maxDocuments)
	if err != nil {
		return nil, err
	}
	if truncated {
		for range records {
			plan.recordCandidate()
		}
		return nil, fmt.Errorf("%w: Mongo gateway find requires a bounded scan and exceeded %d documents", errMongoFindScanCapExceeded, maxDocuments)
	}
	plan.recordWinner("bounded_scan", "")
	out := make([]wire.Document, 0, len(records))
	for _, record := range records {
		doc, err := storedDocumentToBSON(col, materializer, record.Document)
		if err != nil {
			return nil, err
		}
		out = append(out, doc)
	}
	plan.recordCandidates(len(out))
	return out, nil
}

func (s *Server) findUnsortedScanDocuments(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, plan findPlan) ([]wire.Document, bool, error) {
	if plan.stats == nil {
		return s.findUnsortedScanDocumentsWithoutStats(col, materializer, plan)
	}
	return s.findUnsortedScanDocumentsWithStats(col, materializer, plan)
}

// findUnsortedScanDocumentsWithoutStats is deliberately separate from explain
// accounting. Ordinary find requests must not branch on nil diagnostics for
// every scanned record.
func (s *Server) findUnsortedScanDocumentsWithoutStats(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, plan findPlan) ([]wire.Document, bool, error) {
	if plan.sort.field != "" || (len(plan.orBranches) == 0 && len(plan.norBranches) == 0 && findPlanHasDirectCandidate(col.MetaView(), plan.predicates)) {
		return nil, false, nil
	}
	maxDocuments := s.maxFindScanDocuments()
	docs := make([]wire.Document, 0)
	matched := 0
	truncated, err := col.ScanDocumentsFunc(maxDocuments, func(record collections.DocumentRecord) (bool, error) {
		var match, ok bool
		var err error
		if len(plan.orBranches) == 0 && len(plan.norBranches) == 0 {
			match, ok, err = storedDocumentMatchesPredicatesForCollection(col, record.Document, plan.predicates)
			if err != nil {
				return false, err
			}
			if ok && !match {
				return true, nil
			}
		}
		var doc wire.Document
		if !ok || len(plan.orBranches) > 0 || len(plan.norBranches) > 0 {
			doc, err = storedDocumentToBSON(col, materializer, record.Document)
			if err != nil {
				return false, err
			}
			match, err = documentMatchesPlan(doc, plan)
			if err != nil {
				return false, err
			}
		}
		if !match {
			return true, nil
		}
		if matched < int(plan.skip) {
			matched++
			return true, nil
		}
		if ok && len(plan.orBranches) == 0 && len(plan.norBranches) == 0 {
			doc, err = storedDocumentToBSON(col, materializer, record.Document)
			if err != nil {
				return false, err
			}
		}
		docs = append(docs, doc)
		matched++
		if plan.limit > 0 && len(docs) >= int(plan.limit) {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return nil, true, err
	}
	if truncated {
		return nil, true, fmt.Errorf("%w: Mongo gateway find requires a bounded scan and exceeded %d documents", errMongoFindScanCapExceeded, maxDocuments)
	}
	return docs, true, nil
}

func (s *Server) findUnsortedScanDocumentsWithStats(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, plan findPlan) ([]wire.Document, bool, error) {
	if plan.sort.field != "" || (len(plan.orBranches) == 0 && len(plan.norBranches) == 0 && findPlanHasDirectCandidate(col.MetaView(), plan.predicates)) {
		return nil, false, nil
	}
	maxDocuments := s.maxFindScanDocuments()
	docs := make([]wire.Document, 0)
	matched := 0
	truncated, err := col.ScanDocumentsFunc(maxDocuments, func(record collections.DocumentRecord) (bool, error) {
		plan.recordCandidate()
		var match, ok bool
		var err error
		if len(plan.orBranches) == 0 && len(plan.norBranches) == 0 {
			match, ok, err = storedDocumentMatchesPredicatesForCollection(col, record.Document, plan.predicates)
			if err != nil {
				return false, err
			}
			if ok && !match {
				return true, nil
			}
		}
		var doc wire.Document
		if !ok || len(plan.orBranches) > 0 || len(plan.norBranches) > 0 {
			plan.recordMaterialized()
			doc, err = storedDocumentToBSON(col, materializer, record.Document)
			if err != nil {
				return false, err
			}
			match, err = documentMatchesPlan(doc, plan)
			if err != nil {
				return false, err
			}
		}
		if !match {
			return true, nil
		}
		if matched < int(plan.skip) {
			matched++
			return true, nil
		}
		if ok && len(plan.orBranches) == 0 && len(plan.norBranches) == 0 {
			plan.recordMaterialized()
			doc, err = storedDocumentToBSON(col, materializer, record.Document)
			if err != nil {
				return false, err
			}
		}
		docs = append(docs, doc)
		matched++
		if plan.limit > 0 && len(docs) >= int(plan.limit) {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return nil, true, err
	}
	if truncated {
		return nil, true, fmt.Errorf("%w: Mongo gateway find requires a bounded scan and exceeded %d documents", errMongoFindScanCapExceeded, maxDocuments)
	}
	plan.recordWinner("bounded_scan", "")
	return docs, true, nil
}

func storedDocumentMatchesPredicates(stored []byte, predicates []findPredicate) (bool, bool, error) {
	remainingDecimal128Normalizations := mongoQueryMaxDecimal128Normalizations
	for _, pred := range predicates {
		// The JSON bridge fast path cannot soundly distinguish missing from null
		// for these operators. Fall back to BSON materialization rather than
		// risking a false match or rejection.
		if pred.negate || pred.op == findPredicateNE || pred.op == findPredicateNIN || pred.op == findPredicateExists {
			return false, false, nil
		}
		if isRangePredicate(pred.op) {
			match, ok, err := storedDocumentMatchesInt64RangePredicate(stored, pred)
			if err != nil {
				return false, false, err
			}
			if ok {
				if !match {
					return false, true, nil
				}
				continue
			}
		}
		value, found, ok, err := storedDocumentPredicateValue(stored, pred.field)
		if err != nil {
			return false, false, err
		}
		if !ok {
			return false, false, nil
		}
		if !found {
			if missingValueMatchesPredicate(pred) {
				continue
			}
			return false, true, nil
		}
		match, err := valueMatchesPredicateWithBudget(value, pred, &remainingDecimal128Normalizations)
		if err != nil {
			return false, false, err
		}
		if !match {
			return false, true, nil
		}
	}
	return true, true, nil
}

func storedDocumentMatchesPredicatesForCollection(col *collections.Collection, stored []byte, predicates []findPredicate) (bool, bool, error) {
	if col == nil {
		return storedDocumentMatchesPredicates(stored, predicates)
	}
	switch col.MetaView().Options.DocumentFormat {
	case collections.DocumentFormatDefault, collections.DocumentFormatJSON:
		return storedDocumentMatchesPredicates(stored, predicates)
	case collections.DocumentFormatBSON:
		match, err := documentMatchesPredicates(wire.Document(stored), predicates)
		return match, true, err
	default:
		return false, false, nil
	}
}

func isRangePredicate(op findPredicateOp) bool {
	switch op {
	case findPredicateGT, findPredicateGTE, findPredicateLT, findPredicateLTE:
		return true
	default:
		return false
	}
}

func storedDocumentMatchesInt64RangePredicate(stored []byte, pred findPredicate) (bool, bool, error) {
	if field := pred.field; field == "" || strings.Contains(field, ".") {
		return false, false, nil
	}
	if len(pred.values) != 1 {
		return false, false, nil
	}
	threshold, ok := rawValueInt64(pred.values[0])
	if !ok {
		return false, false, nil
	}
	raw, valueType, _, err := jsonparser.Get(stored, pred.field)
	if err == jsonparser.KeyPathNotFoundError {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	value, ok, err := storedJSONInt64(raw, valueType)
	if err != nil || !ok {
		return false, ok, err
	}
	switch pred.op {
	case findPredicateGT:
		return value > threshold, true, nil
	case findPredicateGTE:
		return value >= threshold, true, nil
	case findPredicateLT:
		return value < threshold, true, nil
	case findPredicateLTE:
		return value <= threshold, true, nil
	default:
		return false, false, nil
	}
}

func rawValueInt64(value bson.RawValue) (int64, bool) {
	if v, ok := value.Int64OK(); ok {
		return v, true
	}
	if v, ok := value.Int32OK(); ok {
		return int64(v), true
	}
	return 0, false
}

func storedJSONInt64(raw []byte, valueType jsonparser.ValueType) (int64, bool, error) {
	switch valueType {
	case jsonparser.Number:
		value, err := jsonparser.ParseInt(raw)
		if err != nil {
			return 0, false, nil
		}
		return value, true, nil
	case jsonparser.Object:
		return storedExtendedJSONInt64(raw)
	default:
		return 0, false, nil
	}
}

func storedExtendedJSONInt64(raw []byte) (int64, bool, error) {
	var (
		key       string
		value     []byte
		valueType jsonparser.ValueType
		count     int
	)
	err := jsonparser.ObjectEach(raw, func(k, v []byte, t jsonparser.ValueType, _ int) error {
		count++
		key = string(k)
		value = append(value[:0], v...)
		valueType = t
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	if count != 1 || valueType != jsonparser.String {
		return 0, false, nil
	}
	text, err := jsonparser.ParseString(value)
	if err != nil {
		return 0, false, err
	}
	switch key {
	case "$numberInt":
		parsed, err := strconv.ParseInt(text, 10, 32)
		if err != nil {
			return 0, false, err
		}
		return parsed, true, nil
	case "$numberLong":
		parsed, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return 0, false, err
		}
		return parsed, true, nil
	default:
		return 0, false, nil
	}
}

func storedDocumentPredicateValue(stored []byte, field string) (bson.RawValue, bool, bool, error) {
	if field == "" || strings.Contains(field, ".") {
		return bson.RawValue{}, false, false, nil
	}
	raw, valueType, _, err := jsonparser.Get(stored, field)
	if err == jsonparser.KeyPathNotFoundError {
		return bson.RawValue{}, false, true, nil
	}
	if err != nil {
		return bson.RawValue{}, false, false, err
	}
	value, ok, err := bsonRawValueFromStoredJSON(raw, valueType)
	return value, true, ok, err
}

func bsonRawValueFromStoredJSON(raw []byte, valueType jsonparser.ValueType) (bson.RawValue, bool, error) {
	switch valueType {
	case jsonparser.String:
		value, err := jsonparser.ParseString(raw)
		if err != nil {
			return bson.RawValue{}, false, err
		}
		rawValue, err := bsonRawValueFromGoValue(value)
		return rawValue, true, err
	case jsonparser.Number:
		rawValue, err := bsonRawValueFromJSONNumber(raw)
		return rawValue, true, err
	case jsonparser.Boolean:
		switch string(raw) {
		case "true":
			rawValue, err := bsonRawValueFromGoValue(true)
			return rawValue, true, err
		case "false":
			rawValue, err := bsonRawValueFromGoValue(false)
			return rawValue, true, err
		default:
			return bson.RawValue{}, false, jsonparser.MalformedValueError
		}
	case jsonparser.Null:
		return bson.RawValue{Type: bson.TypeNull}, true, nil
	case jsonparser.Object:
		return bsonRawValueFromStoredExtendedJSON(raw)
	default:
		return bson.RawValue{}, false, nil
	}
}

func bsonRawValueFromJSONNumber(raw []byte) (bson.RawValue, error) {
	if !bytes.ContainsAny(raw, ".eE") {
		if value, err := jsonparser.ParseInt(raw); err == nil {
			return bsonRawValueFromGoValue(value)
		}
	}
	value, err := jsonparser.ParseFloat(raw)
	if err != nil {
		return bson.RawValue{}, err
	}
	return bsonRawValueFromGoValue(value)
}

func bsonRawValueFromStoredExtendedJSON(raw []byte) (bson.RawValue, bool, error) {
	var (
		key       string
		value     []byte
		valueType jsonparser.ValueType
		count     int
	)
	err := jsonparser.ObjectEach(raw, func(k, v []byte, t jsonparser.ValueType, _ int) error {
		count++
		key = string(k)
		value = append(value[:0], v...)
		valueType = t
		return nil
	})
	if err != nil {
		return bson.RawValue{}, false, err
	}
	if count != 1 || valueType != jsonparser.String {
		return bson.RawValue{}, false, nil
	}
	text, err := jsonparser.ParseString(value)
	if err != nil {
		return bson.RawValue{}, false, err
	}
	switch key {
	case "$numberInt":
		parsed, err := strconv.ParseInt(text, 10, 32)
		if err != nil {
			return bson.RawValue{}, false, err
		}
		rawValue, err := bsonRawValueFromGoValue(int32(parsed))
		return rawValue, true, err
	case "$numberLong":
		parsed, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return bson.RawValue{}, false, err
		}
		rawValue, err := bsonRawValueFromGoValue(parsed)
		return rawValue, true, err
	case "$numberDouble":
		parsed, err := parseExtendedJSONDouble(text)
		if err != nil {
			return bson.RawValue{}, false, err
		}
		rawValue, err := bsonRawValueFromGoValue(parsed)
		return rawValue, true, err
	default:
		return bson.RawValue{}, false, nil
	}
}

func parseExtendedJSONDouble(text string) (float64, error) {
	switch text {
	case "NaN":
		return math.NaN(), nil
	case "Infinity":
		return math.Inf(1), nil
	case "-Infinity":
		return math.Inf(-1), nil
	default:
		return strconv.ParseFloat(text, 64)
	}
}

func bsonRawValueFromGoValue(value any) (bson.RawValue, error) {
	valueType, raw, err := bson.MarshalValue(value)
	if err != nil {
		return bson.RawValue{}, err
	}
	return bson.RawValue{Type: valueType, Value: raw}, nil
}

func findPlanHasDirectCandidate(meta collections.CollectionMeta, predicates []findPredicate) bool {
	if _, ok := primaryCandidatePredicate(predicates); ok {
		return true
	}
	for _, pred := range predicates {
		if pred.op == findPredicateEq || pred.op == findPredicateIn {
			if predicateContainsNull(pred) {
				continue
			}
			for _, idx := range meta.Indexes {
				if legacyFindPlannerIndexUsable(idx) && idx.Field == pred.field {
					return true
				}
			}
			continue
		}
		if !isRangePredicate(pred.op) {
			continue
		}
		for _, idx := range meta.Indexes {
			if !legacyFindPlannerIndexUsable(idx) || idx.Field != pred.field {
				continue
			}
			_, ok, _, err := indexRangeOptionsForPredicates(predicates, idx)
			if err != nil || ok {
				return true
			}
		}
	}
	return false
}

// legacyFindPlannerIndexUsable limits the existing single-field Mongo find
// planner to definitions accepted by FindByIndexValue/FindByIndexRange.
// Ordered BSON compound and explicit descending indexes have direct collection
// APIs; automatic planner selection is deliberately deferred to #4065.
func legacyFindPlannerIndexUsable(idx collections.IndexDefinition) bool {
	if idx.ValueType != collections.IndexValueBSONOrderedV2 {
		return true
	}
	return len(idx.Components) == 0 || (len(idx.Components) == 1 && idx.Components[0].Direction != collections.IndexDirectionDescending)
}

func (s *Server) limitCandidateDocuments(docs []wire.Document) ([]wire.Document, error) {
	// This bound limits planner work and memory before predicate filtering,
	// sorting, projection, and skip/limit pagination are applied.
	if len(docs) > s.maxFindScanDocuments() {
		return nil, fmt.Errorf("%w: Mongo gateway find candidate set exceeded %d documents", errMongoFindScanCapExceeded, s.maxFindScanDocuments())
	}
	return docs, nil
}

func (s *Server) bestIndexedCandidateDocuments(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, meta collections.CollectionMeta, plan findPlan, maxDocuments int) ([]wire.Document, string, string, bool, error) {
	var best []wire.Document
	bestStage, bestName := "", ""
	bestSet := false
	for _, idx := range meta.Indexes {
		if !legacyFindPlannerIndexUsable(idx) {
			continue
		}
		docs, stage, ok, work, err := documentsForIndexedFieldPredicates(col, materializer, plan, idx, maxDocuments)
		if err != nil {
			return nil, "", "", false, err
		}
		if !ok {
			continue
		}
		// The selector has materialized every usable index candidate to make
		// its choice. Account for all of that work, not merely the winner.
		plan.recordCandidates(work)
		for _, doc := range docs {
			plan.recordMaterializedBytes(len(doc))
		}
		if !bestSet || len(docs) < len(best) {
			best = docs
			bestStage, bestName = stage, idx.Name
			bestSet = true
		}
	}
	return best, bestStage, bestName, bestSet, nil
}

func indexedFindStage(plan findPlan, idx collections.IndexDefinition) string {
	for _, pred := range plan.predicates {
		if pred.field != idx.Field {
			continue
		}
		if isRangePredicate(pred.op) {
			return "secondary_range_lookup"
		}
		if pred.op == findPredicateEq || pred.op == findPredicateIn {
			return "secondary_equality_lookup"
		}
	}
	return "secondary_equality_lookup"
}

func primaryCandidatePredicate(predicates []findPredicate) (findPredicate, bool) {
	for _, pred := range predicates {
		if pred.field == "_id" && (pred.op == findPredicateEq || pred.op == findPredicateIn) {
			return pred, true
		}
	}
	return findPredicate{}, false
}

func simplePrimaryEqualityFindValue(plan findPlan) (bson.RawValue, bool) {
	if len(plan.orBranches) != 0 || len(plan.norBranches) != 0 || plan.sort.field != "" || plan.skip != 0 || plan.limit > 1 {
		return bson.RawValue{}, false
	}
	if len(plan.predicates) != 1 {
		return bson.RawValue{}, false
	}
	pred := plan.predicates[0]
	if pred.field != "_id" || pred.op != findPredicateEq || len(pred.values) != 1 {
		return bson.RawValue{}, false
	}
	if pred.values[0].Type == bson.TypeRegex {
		return bson.RawValue{}, false
	}
	return pred.values[0], true
}

func documentsForPrimaryPredicate(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, pred findPredicate, maxDocuments int) ([]wire.Document, error) {
	out := make([]wire.Document, 0, len(pred.values))
	seen := make(map[string]struct{}, len(pred.values))
	for _, value := range pred.values {
		key, err := encodePrimaryKey(value)
		if err != nil {
			return nil, err
		}
		encodedKey := string(key)
		if _, ok := seen[encodedKey]; ok {
			continue
		}
		seen[encodedKey] = struct{}{}
		stored, err := col.Get(key)
		if err != nil {
			return nil, err
		}
		if len(stored) == 0 {
			continue
		}
		doc, err := storedDocumentToBSON(col, materializer, stored)
		if err != nil {
			return nil, err
		}
		out = append(out, doc)
		if len(out) > maxDocuments {
			return out, nil
		}
	}
	return out, nil
}

func documentsForIndexedPredicate(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, pred findPredicate, idx collections.IndexDefinition, maxDocuments, candidateLimit int) ([]wire.Document, error) {
	out := make([]wire.Document, 0)
	seen := make(map[string]struct{})
	for _, value := range pred.values {
		scalar, ok := indexScalarForBSONValue(value, idx.ValueType)
		if !ok {
			continue
		}
		ids, _, err := col.FindByIndexValueLimit(idx.Name, scalar, candidateLimit)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			if _, ok := seen[string(id)]; ok {
				continue
			}
			seen[string(id)] = struct{}{}
			stored, err := col.Get(id)
			if err != nil {
				return nil, err
			}
			if len(stored) == 0 {
				continue
			}
			doc, err := storedDocumentToBSON(col, materializer, stored)
			if err != nil {
				return nil, err
			}
			out = append(out, doc)
			if len(out) > maxDocuments {
				return out, nil
			}
		}
	}
	return out, nil
}

func documentsForIndexedFieldPredicates(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, plan findPlan, idx collections.IndexDefinition, maxDocuments int) ([]wire.Document, string, bool, int, error) {
	if !legacyFindPlannerIndexUsable(idx) {
		return nil, "", false, 0, nil
	}
	var best []wire.Document
	bestStage := ""
	work := 0
	bestSet := false
	consider := func(docs []wire.Document, stage string, examined int) {
		work += examined
		if !bestSet || len(docs) < len(best) {
			best = docs
			bestStage = stage
			bestSet = true
		}
	}
	probes := findIndexProbesForIndex(plan, idx)
	hasRange := false
	for _, probe := range probes {
		hasRange = hasRange || probe.stage == "secondary_range_lookup"
	}
	for _, pred := range plan.predicates {
		if pred.field != idx.Field || (pred.op != findPredicateEq && pred.op != findPredicateIn) {
			continue
		}
		if predicateContainsNull(pred) {
			continue
		}
		candidateLimit := candidateLimitWithOverflowSlot(maxDocuments)
		if limit, ok := indexedEqualityCandidateLimit(plan, idx, maxDocuments); ok {
			candidateLimit = limit
		}
		docs, err := documentsForIndexedPredicate(col, materializer, pred, idx, maxDocuments, candidateLimit)
		if err != nil {
			return nil, "", false, work, err
		}
		consider(docs, "secondary_equality_lookup", len(docs))
	}
	if !hasRange {
		return best, bestStage, bestSet, work, nil
	}
	opts, ok, empty, err := indexRangeOptionsForPredicates(plan.predicates, idx)
	if err != nil || !ok {
		if bestSet {
			return best, bestStage, true, work, err
		}
		return nil, "", false, work, err
	}
	if empty {
		consider(nil, "secondary_range_lookup", 0)
		return best, bestStage, true, work, nil
	}
	candidateLimit := candidateLimitWithOverflowSlot(maxDocuments)
	if limit, ok := indexedRangeCandidateLimit(plan, idx, maxDocuments); ok {
		candidateLimit = limit
	}
	docs, examined, err := documentsForIndexedRange(col, materializer, idx, opts, candidateLimit, maxDocuments, true)
	if err != nil {
		return nil, "", false, work, err
	}
	consider(docs, "secondary_range_lookup", examined)
	return best, bestStage, bestSet, work, nil
}

func indexedEqualityCandidateLimit(plan findPlan, idx collections.IndexDefinition, maxDocuments int) (int, bool) {
	if plan.limit <= 0 || len(plan.orBranches) != 0 || len(plan.norBranches) != 0 || plan.sort.field != "" || len(plan.predicates) != 1 {
		return 0, false
	}
	pred := plan.predicates[0]
	if pred.field != idx.Field || pred.op != findPredicateEq || len(pred.values) != 1 || predicateContainsNull(pred) {
		return 0, false
	}
	if _, ok := indexScalarForBSONValue(pred.values[0], idx.ValueType); !ok {
		return 0, false
	}
	limit := int64(plan.skip) + int64(plan.limit)
	if limit <= 0 {
		return 0, false
	}
	maxCandidateLimit := candidateLimitWithOverflowSlot(maxDocuments)
	if limit > int64(maxCandidateLimit) {
		return maxCandidateLimit, true
	}
	return int(limit), true
}

func indexedRangeCandidateLimit(plan findPlan, idx collections.IndexDefinition, maxDocuments int) (int, bool) {
	if plan.limit <= 0 || len(plan.orBranches) != 0 || len(plan.norBranches) != 0 {
		return 0, false
	}
	if plan.sort.field != "" && (len(findSortTerms(plan.sort)) != 1 || plan.sort.field != idx.Field || plan.sort.desc) {
		return 0, false
	}
	for _, pred := range plan.predicates {
		if pred.field != idx.Field || !isRangePredicate(pred.op) {
			return 0, false
		}
	}
	limit := int64(plan.skip) + int64(plan.limit)
	if limit <= 0 {
		return 0, false
	}
	maxCandidateLimit := candidateLimitWithOverflowSlot(maxDocuments)
	if limit > int64(maxCandidateLimit) {
		return maxCandidateLimit, true
	}
	return int(limit), true
}

func candidateLimitWithOverflowSlot(maxDocuments int) int {
	if maxDocuments <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if maxDocuments >= maxInt {
		return maxInt
	}
	return maxDocuments + 1
}

type indexRangeCandidateBound struct {
	value     any
	inclusive bool
	set       bool
}

func indexRangeOptionsForPredicates(predicates []findPredicate, idx collections.IndexDefinition) (collections.IndexRangeOptions, bool, bool, error) {
	var lower, upper indexRangeCandidateBound
	found := false
	for _, pred := range predicates {
		if pred.field != idx.Field || !isRangePredicate(pred.op) {
			continue
		}
		found = true
		if len(pred.values) != 1 || rawValueIsNaN(pred.values[0]) {
			return collections.IndexRangeOptions{}, true, true, nil
		}
		scalar, ok := indexScalarForBSONValue(pred.values[0], idx.ValueType)
		if !ok {
			if unindexedRangePredicateShouldScan(pred.values[0], idx.ValueType) {
				return collections.IndexRangeOptions{}, false, false, nil
			}
			return collections.IndexRangeOptions{}, true, true, nil
		}
		switch pred.op {
		case findPredicateGT:
			next := indexRangeCandidateBound{value: scalar, inclusive: false, set: true}
			var err error
			lower, err = stricterLowerIndexBound(idx.ValueType, lower, next)
			if err != nil {
				return collections.IndexRangeOptions{}, true, false, err
			}
		case findPredicateGTE:
			next := indexRangeCandidateBound{value: scalar, inclusive: true, set: true}
			var err error
			lower, err = stricterLowerIndexBound(idx.ValueType, lower, next)
			if err != nil {
				return collections.IndexRangeOptions{}, true, false, err
			}
		case findPredicateLT:
			next := indexRangeCandidateBound{value: scalar, inclusive: false, set: true}
			var err error
			upper, err = stricterUpperIndexBound(idx.ValueType, upper, next)
			if err != nil {
				return collections.IndexRangeOptions{}, true, false, err
			}
		case findPredicateLTE:
			next := indexRangeCandidateBound{value: scalar, inclusive: true, set: true}
			var err error
			upper, err = stricterUpperIndexBound(idx.ValueType, upper, next)
			if err != nil {
				return collections.IndexRangeOptions{}, true, false, err
			}
		}
	}
	if !found {
		return collections.IndexRangeOptions{}, false, false, nil
	}
	if lower.set && upper.set {
		cmp, err := compareIndexScalars(idx.ValueType, lower.value, upper.value)
		if err != nil {
			return collections.IndexRangeOptions{}, true, false, err
		}
		if cmp > 0 || (cmp == 0 && (!lower.inclusive || !upper.inclusive)) {
			return collections.IndexRangeOptions{}, true, true, nil
		}
	}
	opts := collections.IndexRangeOptions{}
	if lower.set {
		opts.Lower = collections.IndexRangeBound{Value: lower.value, Inclusive: lower.inclusive}
	} else {
		opts.Lower = collections.IndexRangeBound{Unbounded: true}
	}
	if upper.set {
		opts.Upper = collections.IndexRangeBound{Value: upper.value, Inclusive: upper.inclusive}
	} else {
		opts.Upper = collections.IndexRangeBound{Unbounded: true}
	}
	return opts, true, false, nil
}

func stricterLowerIndexBound(valueType collections.IndexValueType, current, next indexRangeCandidateBound) (indexRangeCandidateBound, error) {
	if !current.set {
		return next, nil
	}
	cmp, err := compareIndexScalars(valueType, next.value, current.value)
	if err != nil {
		return indexRangeCandidateBound{}, err
	}
	if cmp > 0 {
		return next, nil
	}
	if cmp == 0 {
		current.inclusive = current.inclusive && next.inclusive
	}
	return current, nil
}

func stricterUpperIndexBound(valueType collections.IndexValueType, current, next indexRangeCandidateBound) (indexRangeCandidateBound, error) {
	if !current.set {
		return next, nil
	}
	cmp, err := compareIndexScalars(valueType, next.value, current.value)
	if err != nil {
		return indexRangeCandidateBound{}, err
	}
	if cmp < 0 {
		return next, nil
	}
	if cmp == 0 {
		current.inclusive = current.inclusive && next.inclusive
	}
	return current, nil
}

func compareIndexScalars(valueType collections.IndexValueType, left, right any) (int, error) {
	switch valueType {
	case collections.IndexValueBSONOrderedV2:
		leftRaw, leftOK := left.(bson.RawValue)
		rightRaw, rightOK := right.(bson.RawValue)
		if !leftOK || !rightOK {
			return 0, fmt.Errorf("Mongo gateway internal BSON v2 index bound mismatch")
		}
		leftKey, err := collections.EncodeBSONIndexKeyComponentV2(leftRaw)
		if err != nil {
			return 0, err
		}
		rightKey, err := collections.EncodeBSONIndexKeyComponentV2(rightRaw)
		if err != nil {
			return 0, err
		}
		return bytes.Compare(leftKey, rightKey), nil
	case collections.IndexValueString:
		leftString, leftOK := left.(string)
		rightString, rightOK := right.(string)
		if !leftOK || !rightOK {
			return 0, fmt.Errorf("Mongo gateway internal string index bound mismatch")
		}
		return strings.Compare(leftString, rightString), nil
	case collections.IndexValueBool:
		leftBool, leftOK := left.(bool)
		rightBool, rightOK := right.(bool)
		if !leftOK || !rightOK {
			return 0, fmt.Errorf("Mongo gateway internal bool index bound mismatch")
		}
		switch {
		case leftBool == rightBool:
			return 0, nil
		case !leftBool && rightBool:
			return -1, nil
		default:
			return 1, nil
		}
	case collections.IndexValueInt64:
		leftInt, leftOK := left.(int64)
		rightInt, rightOK := right.(int64)
		if !leftOK || !rightOK {
			return 0, fmt.Errorf("Mongo gateway internal int64 index bound mismatch")
		}
		return compareInt64(leftInt, rightInt), nil
	case collections.IndexValueDouble:
		leftFloat, leftOK := left.(float64)
		rightFloat, rightOK := right.(float64)
		if !leftOK || !rightOK {
			return 0, fmt.Errorf("Mongo gateway internal double index bound mismatch")
		}
		return compareFloat64IndexValues(leftFloat, rightFloat), nil
	default:
		return 0, fmt.Errorf("Mongo gateway unsupported index value type %q", valueType)
	}
}

func compareInt64(left, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareFloat64IndexValues(left, right float64) int {
	switch {
	case math.IsNaN(left) && math.IsNaN(right):
		return 0
	case math.IsNaN(left):
		return -1
	case math.IsNaN(right):
		return 1
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func documentsForIndexedRange(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, idx collections.IndexDefinition, opts collections.IndexRangeOptions, candidateLimit, maxDocuments int, allowOverflow bool) ([]wire.Document, int, error) {
	if candidateLimit <= 0 {
		candidateLimit = candidateLimitWithOverflowSlot(maxDocuments)
	}
	opts.Limit = candidateLimit
	records, _, err := col.FindDocumentsByIndexRange(idx.Name, opts)
	if err != nil {
		return nil, 0, err
	}
	out := make([]wire.Document, 0, len(records))
	for _, record := range records {
		if len(record.Document) == 0 {
			continue
		}
		doc, err := storedDocumentToBSON(col, materializer, record.Document)
		if err != nil {
			return nil, len(out), err
		}
		out = append(out, doc)
		if maxDocuments > 0 && len(out) > maxDocuments {
			if allowOverflow {
				return out, len(out), nil
			}
			return out[:maxDocuments], len(out), nil
		}
	}
	return out, len(out), nil
}

func parseFindPredicates(filter wire.Document) ([]findPredicate, error) {
	predicates, _, _, err := parseFindFilter(filter)
	return predicates, err
}

func parseFindFilter(filter wire.Document) ([]findPredicate, [][]findPredicate, [][]findPredicate, error) {
	if filter == nil {
		return nil, nil, nil, nil
	}
	budget := findPredicateParseBudget{remaining: mongoQueryMaxBooleanPredicates}
	var predicates []findPredicate
	var orBranches [][]findPredicate
	var norBranches [][]findPredicate
	err := forEachBoundedBSONDocumentElement(bson.Raw(filter), mongoQueryMaxBooleanPredicates, findPredicateCapError(), func(elem bson.RawElement) error {
		key, err := elem.KeyErr()
		if err != nil {
			return err
		}
		if key == "$or" {
			if orBranches != nil {
				return errors.New("Mongo gateway find supports only one top-level $or")
			}
			orBranches, err = parseBooleanBranches(elem.Value(), "$or", &budget)
			if err != nil {
				return err
			}
			return nil
		}
		if key == "$nor" {
			if norBranches != nil {
				return errors.New("Mongo gateway find supports only one top-level $nor")
			}
			norBranches, err = parseBooleanBranches(elem.Value(), "$nor", &budget)
			if err != nil {
				return err
			}
			return nil
		}
		if key == "$and" {
			parsed, err := parseAndPredicates(elem.Value(), &budget)
			if err != nil {
				return err
			}
			predicates = append(predicates, parsed...)
			return nil
		}
		if strings.HasPrefix(key, "$") {
			return fmt.Errorf("Mongo gateway find does not support query operator %q", key)
		}
		parsed, err := parseFieldPredicate(key, elem.Value())
		if err != nil {
			return err
		}
		if err := budget.consume(len(parsed)); err != nil {
			return err
		}
		predicates = append(predicates, parsed...)
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if err := validateFindPredicateWork(predicates, orBranches, norBranches); err != nil {
		return nil, nil, nil, err
	}
	return predicates, orBranches, norBranches, nil
}

func validateFindPredicateWork(predicates []findPredicate, orBranches, norBranches [][]findPredicate) error {
	if len(orBranches) > mongoQueryMaxBooleanBranches || len(norBranches) > mongoQueryMaxBooleanBranches {
		return fmt.Errorf("Mongo gateway boolean query exceeds %d branches", mongoQueryMaxBooleanBranches)
	}
	total := len(predicates)
	check := func(branches [][]findPredicate) error {
		for _, branch := range branches {
			total += len(branch)
			for _, predicate := range branch {
				if (predicate.op == findPredicateNIN || predicate.negate) && len(predicate.values) > mongoQueryMaxNegativeChoices {
					return fmt.Errorf("Mongo gateway %s predicate exceeds %d choices", findPredicateName(predicate), mongoQueryMaxNegativeChoices)
				}
			}
		}
		return nil
	}
	for _, predicate := range predicates {
		if (predicate.op == findPredicateNIN || predicate.negate) && len(predicate.values) > mongoQueryMaxNegativeChoices {
			return fmt.Errorf("Mongo gateway %s predicate exceeds %d choices", findPredicateName(predicate), mongoQueryMaxNegativeChoices)
		}
	}
	if err := check(orBranches); err != nil {
		return err
	}
	if err := check(norBranches); err != nil {
		return err
	}
	if total > mongoQueryMaxBooleanPredicates {
		return fmt.Errorf("Mongo gateway boolean query exceeds %d predicates", mongoQueryMaxBooleanPredicates)
	}
	return nil
}

func parseFindPredicateDocument(doc wire.Document, budget *findPredicateParseBudget) ([]findPredicate, error) {
	var out []findPredicate
	err := forEachBoundedBSONDocumentElement(bson.Raw(doc), mongoQueryMaxBooleanPredicates, findPredicateCapError(), func(elem bson.RawElement) error {
		key, err := elem.KeyErr()
		if err != nil {
			return err
		}
		value := elem.Value()
		if key == "$and" {
			preds, err := parseAndPredicates(value, budget)
			if err != nil {
				return err
			}
			out = append(out, preds...)
			return nil
		}
		preds, err := parseFieldPredicate(key, value)
		if err != nil {
			return err
		}
		if err := budget.consume(len(preds)); err != nil {
			return err
		}
		out = append(out, preds...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func parseAndPredicates(value bson.RawValue, budget *findPredicateParseBudget) ([]findPredicate, error) {
	array, ok := value.ArrayOK()
	if !ok {
		return nil, errors.New("Mongo gateway $and must be an array")
	}
	values, err := boundedBSONArrayValues(array, mongoQueryMaxBooleanPredicates, "$and")
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, errors.New("Mongo gateway $and must contain at least one expression")
	}
	if len(values) > mongoQueryMaxBooleanPredicates {
		return nil, fmt.Errorf("Mongo gateway $and exceeds %d predicates", mongoQueryMaxBooleanPredicates)
	}
	var out []findPredicate
	for i, value := range values {
		doc, ok := value.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("Mongo gateway $and[%d] must be a document", i)
		}
		preds, err := parseFindPredicateDocument(wire.Document(doc), budget)
		if err != nil {
			return nil, err
		}
		out = append(out, preds...)
	}
	return out, nil
}

func parseOrPredicates(value bson.RawValue) ([][]findPredicate, error) {
	budget := findPredicateParseBudget{remaining: mongoQueryMaxBooleanPredicates}
	return parseBooleanBranches(value, "$or", &budget)
}

func parseBooleanBranches(value bson.RawValue, operator string, budget *findPredicateParseBudget) ([][]findPredicate, error) {
	array, ok := value.ArrayOK()
	if !ok {
		return nil, fmt.Errorf("Mongo gateway %s must be an array", operator)
	}
	values, err := boundedBSONArrayValues(array, mongoQueryMaxBooleanBranches, operator)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("Mongo gateway %s must contain at least one expression", operator)
	}
	branches := make([][]findPredicate, 0, len(values))
	for i, value := range values {
		doc, ok := value.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("Mongo gateway %s[%d] must be a document", operator, i)
		}
		predicates, err := parseFindPredicateDocument(wire.Document(doc), budget)
		if err != nil {
			return nil, err
		}
		branches = append(branches, predicates)
	}
	return branches, nil
}

// boundedBSONArrayValues walks the raw BSON array without first materializing
// every element. Query operands are client-controlled, so discovering the
// first over-cap element must reject before a max-wire array can allocate a
// proportional []RawValue backing slice.
func boundedBSONArrayValues(array bson.RawArray, max int, operator string) ([]bson.RawValue, error) {
	length, remaining, ok := bsoncore.ReadLength(array)
	if !ok || length < 5 || int(length) != len(array) || len(remaining) == 0 || remaining[len(remaining)-1] != 0 {
		return nil, errors.New("Mongo gateway invalid BSON array")
	}
	remaining = remaining[:len(remaining)-1]
	values := make([]bson.RawValue, 0, min(max, 8))
	for len(remaining) != 0 {
		element, next, ok := bsoncore.ReadElement(remaining)
		if !ok {
			return nil, errors.New("Mongo gateway invalid BSON array")
		}
		value, err := bson.RawElement(element).ValueErr()
		if err != nil {
			return nil, err
		}
		if len(values) == max {
			if operator == "$or" || operator == "$nor" {
				return nil, fmt.Errorf("Mongo gateway %s exceeds %d branches", operator, max)
			}
			if operator == "$and" {
				return nil, fmt.Errorf("Mongo gateway $and exceeds %d predicates", max)
			}
			return nil, fmt.Errorf("Mongo gateway %s exceeds %d choices", operator, max)
		}
		values = append(values, value)
		remaining = next
	}
	return values, nil
}

// findPredicateParseBudget is shared by the filter root and every allowed
// boolean branch. It prevents a valid-looking nesting shape from doing
// proportional parse work before the global predicate admission check.
type findPredicateParseBudget struct {
	remaining int
}

func (budget *findPredicateParseBudget) consume(count int) error {
	if count < 0 || count > budget.remaining {
		return findPredicateCapError()
	}
	budget.remaining -= count
	return nil
}

func findPredicateCapError() error {
	return fmt.Errorf("Mongo gateway boolean query exceeds %d predicates", mongoQueryMaxBooleanPredicates)
}

// forEachBoundedBSONDocumentElement walks a BSON document directly from its
// wire representation. In particular, it never calls Raw.Elements(), whose
// result backing slice scales with a client-controlled document width. The
// over-cap element is detected before its value is decoded or retained.
func forEachBoundedBSONDocumentElement(doc bson.Raw, max int, limitError error, visit func(bson.RawElement) error) error {
	length, remaining, ok := bsoncore.ReadLength(doc)
	if !ok || length < 5 || int(length) != len(doc) || len(remaining) == 0 || remaining[len(remaining)-1] != 0 {
		return errors.New("Mongo gateway invalid BSON document")
	}
	remaining = remaining[:len(remaining)-1]
	count := 0
	for len(remaining) != 0 {
		element, next, ok := bsoncore.ReadElement(remaining)
		if !ok {
			return errors.New("Mongo gateway invalid BSON document")
		}
		if count == max {
			return limitError
		}
		raw := bson.RawElement(element)
		if err := raw.Validate(); err != nil {
			return err
		}
		if err := visit(raw); err != nil {
			return err
		}
		count++
		remaining = next
	}
	return nil
}

func parseFieldPredicate(field string, value bson.RawValue) ([]findPredicate, error) {
	if err := validateMongoDocumentPath(field); err != nil {
		return nil, fmt.Errorf("Mongo gateway unsupported find predicate %q: %w", field, err)
	}
	if doc, ok := value.DocumentOK(); ok {
		isOperatorDoc, err := operatorDocument(doc)
		if err != nil {
			return nil, err
		}
		if !isOperatorDoc {
			return []findPredicate{{field: field, op: findPredicateEq, values: []bson.RawValue{value}}}, nil
		}
		out := make([]findPredicate, 0, min(mongoQueryMaxBooleanPredicates, 4))
		seen := make(map[string]struct{}, min(mongoQueryMaxBooleanPredicates, 4))
		err = forEachBoundedBSONDocumentElement(doc, mongoQueryMaxBooleanPredicates, findPredicateCapError(), func(elem bson.RawElement) error {
			op, err := elem.KeyErr()
			if err != nil {
				return err
			}
			if _, duplicate := seen[op]; duplicate {
				return fmt.Errorf("Mongo gateway find field %q repeats operator %q", field, op)
			}
			seen[op] = struct{}{}
			opValue := elem.Value()
			switch op {
			case "$in", "$nin":
				array, ok := opValue.ArrayOK()
				if !ok {
					return fmt.Errorf("Mongo gateway find field %q %s must be an array", field, op)
				}
				var values []bson.RawValue
				if op == "$nin" {
					values, err = boundedBSONArrayValues(array, mongoQueryMaxNegativeChoices, op)
				} else {
					values, err = array.Values()
				}
				if err != nil {
					return err
				}
				kind := findPredicateIn
				if op == "$nin" {
					kind = findPredicateNIN
				}
				out = append(out, findPredicate{field: field, op: kind, values: values})
			case "$ne":
				out = append(out, findPredicate{field: field, op: findPredicateNE, values: []bson.RawValue{opValue}})
			case "$exists":
				exists, ok := opValue.BooleanOK()
				if !ok {
					return fmt.Errorf("Mongo gateway find field %q $exists must be boolean", field)
				}
				valueType, raw, err := bson.MarshalValue(exists)
				if err != nil {
					return err
				}
				out = append(out, findPredicate{field: field, op: findPredicateExists, values: []bson.RawValue{{Type: valueType, Value: raw}}})
			case "$not":
				notDoc, ok := opValue.DocumentOK()
				if !ok {
					return fmt.Errorf("Mongo gateway find field %q $not must be an operator document", field)
				}
				if err := validateNotOperandWork(notDoc); err != nil {
					return err
				}
				inner, err := parseFieldPredicate(field, bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: notDoc})
				if err != nil || len(inner) != 1 || inner[0].negate || !findPredicateSupportsNot(inner[0].op) {
					if err != nil {
						return err
					}
					return fmt.Errorf("Mongo gateway find field %q $not requires one supported operator", field)
				}
				inner[0].negate = true
				out = append(out, inner[0])
			case "$gt", "$gte", "$lt", "$lte":
				out = append(out, findPredicate{field: field, op: rangeOperator(op), values: []bson.RawValue{opValue}})
			default:
				return fmt.Errorf("Mongo gateway unsupported find operator %q", op)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	return []findPredicate{{field: field, op: findPredicateEq, values: []bson.RawValue{value}}}, nil
}

// validateNotOperandWork admits the raw array before the recursive parser can
// call RawArray.Values for the only supported $not inner predicate.
func validateNotOperandWork(doc bson.Raw) error {
	var op string
	var value bson.RawValue
	count := 0
	err := forEachBoundedBSONDocumentElement(doc, 1, errors.New("Mongo gateway $not requires one supported operator"), func(elem bson.RawElement) error {
		var err error
		op, err = elem.KeyErr()
		if err != nil {
			return err
		}
		value = elem.Value()
		count++
		return nil
	})
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("Mongo gateway $not requires one supported operator")
	}
	if op != "$in" && op != "$nin" {
		return nil
	}
	array, ok := value.ArrayOK()
	if !ok {
		return errors.New("Mongo gateway $not array operand must be an array")
	}
	_, err = boundedBSONArrayValues(array, mongoQueryMaxNegativeChoices, op)
	return err
}

func findPredicateSupportsNot(op findPredicateOp) bool {
	switch op {
	case findPredicateIn, findPredicateNIN, findPredicateNE, findPredicateExists,
		findPredicateGT, findPredicateGTE, findPredicateLT, findPredicateLTE:
		return true
	default:
		return false
	}
}

// validateMongoDocumentPath is shared by query, sort, and projection parsing.
// Query paths deliberately remain document-only at evaluation time; parsing
// still permits numeric field names because BSON documents may use them.
func validateMongoDocumentPath(path string) error {
	if path == "" || strings.HasPrefix(path, ".") || strings.HasSuffix(path, ".") || strings.Contains(path, "..") {
		return errors.New("path must contain non-empty segments")
	}
	segments := strings.Split(path, ".")
	if len(segments) > mongoMutationMaxPathDepth {
		return fmt.Errorf("path exceeds %d components", mongoMutationMaxPathDepth)
	}
	for _, segment := range segments {
		if strings.HasPrefix(segment, "$") {
			return errors.New("path has unsupported segment")
		}
	}
	return nil
}

func operatorDocument(doc bson.Raw) (bool, error) {
	sawOperator := false
	sawNonOperator := false
	count := 0
	err := forEachBoundedBSONDocumentElement(doc, mongoQueryMaxBooleanPredicates, findPredicateCapError(), func(elem bson.RawElement) error {
		key, err := elem.KeyErr()
		if err != nil {
			return err
		}
		count++
		if strings.HasPrefix(key, "$") {
			sawOperator = true
		} else {
			sawNonOperator = true
		}
		if sawOperator && sawNonOperator {
			return errors.New("Mongo gateway find field predicate document cannot mix operator and non-operator keys")
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	return sawOperator, nil
}

func rangeOperator(op string) findPredicateOp {
	switch op {
	case "$gt":
		return findPredicateGT
	case "$gte":
		return findPredicateGTE
	case "$lt":
		return findPredicateLT
	default:
		return findPredicateLTE
	}
}

func documentMatchesPredicates(doc wire.Document, predicates []findPredicate) (bool, error) {
	remainingDecimal128Normalizations := mongoQueryMaxDecimal128Normalizations
	return documentMatchesPredicatesWithBudget(doc, predicates, &remainingDecimal128Normalizations)
}

func documentMatchesPredicatesWithBudget(doc wire.Document, predicates []findPredicate, remainingDecimal128Normalizations *int) (bool, error) {
	for _, pred := range predicates {
		if predicateRejectsArrayTraversal(pred) && documentPathContainsArray(doc, pred.field) {
			return false, fmt.Errorf("Mongo gateway %s predicate does not support array path %q", findPredicateName(pred), pred.field)
		}
		values, ok, err := lookupDocumentPredicateValues(doc, pred.field)
		if err != nil {
			return false, err
		}
		if !ok {
			if missingValueMatchesPredicate(pred) != pred.negate {
				continue
			}
			return false, nil
		}
		matched := false
		for _, value := range values {
			match, err := valueMatchesPredicateWithBudget(value, pred, remainingDecimal128Normalizations)
			if err != nil {
				return false, err
			}
			if match {
				matched = true
				break
			}
		}
		if matched == pred.negate {
			return false, nil
		}
	}
	return true, nil
}

func predicateRejectsArrayTraversal(pred findPredicate) bool {
	return pred.negate || pred.op == findPredicateNE || pred.op == findPredicateNIN || pred.op == findPredicateExists
}

func findPredicateName(pred findPredicate) string {
	if pred.negate {
		return "$not"
	}
	switch pred.op {
	case findPredicateNE:
		return "$ne"
	case findPredicateNIN:
		return "$nin"
	case findPredicateExists:
		return "$exists"
	default:
		return "query"
	}
}

// documentPathContainsArray is deliberately a structural check, not the
// matcher’s historical fan-out lookup. #4066 does not claim multikey/array
// semantics, so any array encountered at or below a requested path rejects.
func documentPathContainsArray(doc wire.Document, field string) bool {
	parts := strings.Split(field, ".")
	current := bson.Raw(doc)
	for i, part := range parts {
		value := current.Lookup(part)
		if value.IsZero() {
			return false
		}
		if _, array := value.ArrayOK(); array {
			return true
		}
		if i == len(parts)-1 {
			return false
		}
		next, ok := value.DocumentOK()
		if !ok {
			return false
		}
		current = next
	}
	return false
}

func validateFindSortDocuments(docs []wire.Document, sortSpec findSort) error {
	for _, term := range findSortTerms(sortSpec) {
		if !strings.Contains(term.field, ".") {
			continue
		}
		for _, doc := range docs {
			if documentPathContainsArray(doc, term.field) {
				return fmt.Errorf("Mongo gateway dotted sort does not support array path %q", term.field)
			}
		}
	}
	return nil
}

func documentMatchesPlan(doc wire.Document, plan findPlan) (bool, error) {
	remainingDecimal128Normalizations := mongoQueryMaxDecimal128Normalizations
	match, err := documentMatchesPredicatesWithBudget(doc, plan.predicates, &remainingDecimal128Normalizations)
	if err != nil || !match {
		return match, err
	}
	if len(plan.orBranches) != 0 {
		match = false
	}
	for _, branch := range plan.orBranches {
		branchMatch, err := documentMatchesPredicatesWithBudget(doc, branch, &remainingDecimal128Normalizations)
		if err != nil {
			return false, err
		}
		if branchMatch {
			match = true
			break
		}
	}
	if !match {
		return false, nil
	}
	for _, branch := range plan.norBranches {
		match, err := documentMatchesPredicatesWithBudget(doc, branch, &remainingDecimal128Normalizations)
		if err != nil {
			return false, err
		}
		if match {
			return false, nil
		}
	}
	return true, nil
}

func missingValueMatchesPredicate(pred findPredicate) bool {
	switch pred.op {
	case findPredicateEq, findPredicateIn:
		return predicateContainsNull(pred)
	case findPredicateNE:
		// Mongo's $ne:null is the non-null/existence form; unlike ordinary
		// $ne it does not include a missing path.
		return !predicateContainsNull(pred)
	case findPredicateNIN:
		return true
	case findPredicateExists:
		exists, _ := pred.values[0].BooleanOK()
		return !exists
	default:
		return false
	}
}

func predicateContainsNull(pred findPredicate) bool {
	for _, value := range pred.values {
		if rawValueIsNull(value) {
			return true
		}
	}
	return false
}

func rawValueIsNull(value bson.RawValue) bool {
	return value.Type == bson.TypeNull
}

func valueMatchesPredicate(value bson.RawValue, pred findPredicate) (bool, error) {
	remainingDecimal128Normalizations := mongoQueryMaxDecimal128Normalizations
	return valueMatchesPredicateWithBudget(value, pred, &remainingDecimal128Normalizations)
}

func valueMatchesPredicateWithBudget(value bson.RawValue, pred findPredicate, remainingDecimal128Normalizations *int) (bool, error) {
	switch pred.op {
	case findPredicateEq:
		return rawValuesEqualModeBudget(value, pred.values[0], false, remainingDecimal128Normalizations)
	case findPredicateIn:
		for _, candidate := range pred.values {
			equal, err := rawValuesEqualModeBudget(value, candidate, false, remainingDecimal128Normalizations)
			if err != nil {
				return false, err
			}
			if equal {
				return true, nil
			}
		}
		return false, nil
	case findPredicateNE:
		equal, err := rawValuesEqualModeBudget(value, pred.values[0], false, remainingDecimal128Normalizations)
		return !equal, err
	case findPredicateNIN:
		for _, candidate := range pred.values {
			equal, err := rawValuesEqualModeBudget(value, candidate, false, remainingDecimal128Normalizations)
			if err != nil {
				return false, err
			}
			if equal {
				return false, nil
			}
		}
		return true, nil
	case findPredicateExists:
		exists, _ := pred.values[0].BooleanOK()
		return exists, nil
	case findPredicateGT, findPredicateGTE, findPredicateLT, findPredicateLTE:
		if rawValueIsNaN(value) || rawValueIsNaN(pred.values[0]) {
			return false, nil
		}
		if !rangeValuesComparable(value, pred.values[0]) {
			return false, nil
		}
		cmp := compareRawValues(value, pred.values[0])
		switch pred.op {
		case findPredicateGT:
			return cmp > 0, nil
		case findPredicateGTE:
			return cmp >= 0, nil
		case findPredicateLT:
			return cmp < 0, nil
		default:
			return cmp <= 0, nil
		}
	default:
		return false, errors.New("Mongo gateway internal unknown predicate")
	}
}

func rangeValuesComparable(left, right bson.RawValue) bool {
	if left.IsNumber() && right.IsNumber() {
		return rawNumberComparable(left) && rawNumberComparable(right)
	}
	return left.Type == right.Type
}

func parseFindSort(command wire.Document) (findSort, error) {
	sortDoc, err := commandOptionalDocument(command, "sort")
	if err != nil || sortDoc == nil {
		return findSort{}, err
	}
	return parseFindSortDocument(sortDoc)
}

func parseFindSortDocument(sortDoc wire.Document) (findSort, error) {
	elements, err := bson.Raw(sortDoc).Elements()
	if err != nil {
		return findSort{}, err
	}
	if len(elements) == 0 {
		return findSort{}, nil
	}
	if len(elements) > 4 {
		return findSort{}, errors.New("Mongo gateway find sort supports at most four fields")
	}
	terms := make([]findSortTerm, 0, len(elements))
	seen := make(map[string]struct{}, len(elements))
	for _, element := range elements {
		field, err := element.KeyErr()
		if err != nil {
			return findSort{}, err
		}
		if err := validateMongoDocumentPath(field); err != nil {
			return findSort{}, fmt.Errorf("Mongo gateway find sort field %q is invalid: %w", field, err)
		}
		if _, duplicate := seen[field]; duplicate {
			return findSort{}, fmt.Errorf("Mongo gateway find sort repeats field %q", field)
		}
		seen[field] = struct{}{}
		desc, err := findSortDirectionValue(element.Value())
		if err != nil {
			return findSort{}, err
		}
		terms = append(terms, findSortTerm{field: field, desc: desc})
	}
	return findSort{field: terms[0].field, desc: terms[0].desc, terms: terms}, nil
}

func findSortDirectionValue(value bson.RawValue) (bool, error) {
	if isAscendingIndexKey(value) {
		return false, nil
	}
	if v, ok := value.Int32OK(); ok && v == -1 {
		return true, nil
	}
	if v, ok := value.Int64OK(); ok && v == -1 {
		return true, nil
	}
	if v, ok := value.DoubleOK(); ok && v == -1 {
		return true, nil
	}
	return false, errors.New("Mongo gateway find sort direction must be 1 or -1")
}

func findSortTerms(sort findSort) []findSortTerm {
	if len(sort.terms) != 0 {
		return sort.terms
	}
	if sort.field == "" {
		return nil
	}
	return []findSortTerm{{field: sort.field, desc: sort.desc}}
}

func compareDocumentsForFindSort(left, right wire.Document, sort findSort) int {
	for _, term := range findSortTerms(sort) {
		cmp := compareDocumentField(left, right, term.field)
		if cmp == 0 {
			continue
		}
		if term.desc {
			return -cmp
		}
		return cmp
	}
	return compareDocumentField(left, right, "_id")
}

func parseFindPagination(command wire.Document) (int32, int32, error) {
	skip, err := optionalInt32Field(command, "skip")
	if err != nil {
		return 0, 0, err
	}
	if skip < 0 {
		return 0, 0, errors.New("Mongo gateway find skip must be non-negative")
	}
	limit, err := optionalInt32Field(command, "limit")
	if err != nil {
		return 0, 0, err
	}
	if limit < 0 {
		return 0, 0, errors.New("Mongo gateway find limit must be non-negative")
	}
	return skip, limit, nil
}

func compareDocumentField(left, right wire.Document, field string) int {
	leftValue, leftOK := lookupDocumentValue(left, field)
	rightValue, rightOK := lookupDocumentValue(right, field)
	if !leftOK {
		leftValue = bson.RawValue{Type: bson.TypeNull}
	}
	if !rightOK {
		rightValue = bson.RawValue{Type: bson.TypeNull}
	}
	return compareRawValues(leftValue, rightValue)
}

type compiledProjection struct {
	present     bool
	mode        projectionMode
	fields      map[string]struct{}
	includeID   bool
	idSpecified bool
}

func compileProjection(projection wire.Document) (compiledProjection, error) {
	if projection == nil {
		return compiledProjection{}, nil
	}
	mode, fields, includeID, idSpecified, err := parseProjection(projection)
	if err != nil {
		return compiledProjection{}, err
	}
	if _, err := projectionTree(fields); err != nil {
		return compiledProjection{}, err
	}
	return compiledProjection{
		present:     true,
		mode:        mode,
		fields:      fields,
		includeID:   includeID,
		idSpecified: idSpecified,
	}, nil
}

func projectDocument(doc wire.Document, projection wire.Document) (wire.Document, error) {
	compiled, err := compileProjection(projection)
	if err != nil {
		return nil, err
	}
	return projectDocumentWithProjection(doc, compiled)
}

func projectDocumentWithProjection(doc wire.Document, projection compiledProjection) (wire.Document, error) {
	if !projection.present {
		return doc, nil
	}
	mode := projection.mode
	if mode == projectionNone {
		if !projection.idSpecified {
			return doc, nil
		}
		if projection.includeID {
			mode = projectionInclude
		} else {
			mode = projectionExclude
		}
	}
	return projectDocumentWithEffectiveProjection(doc, projection, mode)
}

func projectDocumentWithEffectiveProjection(doc wire.Document, projection compiledProjection, mode projectionMode) (wire.Document, error) {
	tree, err := projectionTree(projection.fields)
	if err != nil {
		return nil, err
	}
	if mode == projectionInclude {
		out, _, err := projectIncludedDocument(bson.Raw(doc), tree, projection.includeID, true)
		return wire.Document(out), err
	}
	return projectExcludedDocument(bson.Raw(doc), tree, projection.includeID)
}

// Projection intentionally supports only document traversal. A dotted path
// that would enter an array rejects instead of borrowing Mongo's positional or
// multikey projection semantics.
type projectionNode struct {
	terminal bool
	children map[string]*projectionNode
}

func projectionTree(fields map[string]struct{}) (*projectionNode, error) {
	root := &projectionNode{children: make(map[string]*projectionNode)}
	for field := range fields {
		if err := validateMongoDocumentPath(field); err != nil {
			return nil, fmt.Errorf("Mongo gateway projection has invalid path %q: %w", field, err)
		}
		parts := strings.Split(field, ".")
		node := root
		for _, part := range parts {
			if node.terminal {
				return nil, fmt.Errorf("Mongo gateway projection paths overlap at %q", field)
			}
			if node.children[part] == nil {
				node.children[part] = &projectionNode{children: make(map[string]*projectionNode)}
			}
			node = node.children[part]
		}
		if node.terminal || len(node.children) != 0 {
			return nil, fmt.Errorf("Mongo gateway projection paths overlap at %q", field)
		}
		node.terminal = true
	}
	return root, nil
}

func projectIncludedDocument(doc bson.Raw, node *projectionNode, includeID, root bool) ([]byte, bool, error) {
	elements, err := bson.Raw(doc).Elements()
	if err != nil {
		return nil, false, err
	}
	idx, out := bsoncore.AppendDocumentStart(make([]byte, 0, len(doc)))
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, false, err
		}
		if root && key == "_id" {
			if includeID {
				out = appendRawValueElement(out, key, elem.Value())
			}
			continue
		}
		child := node.children[key]
		if child == nil {
			continue
		}
		if child.terminal {
			out = appendRawValueElement(out, key, elem.Value())
			continue
		}
		nested, ok := elem.Value().DocumentOK()
		if !ok {
			if _, array := elem.Value().ArrayOK(); array {
				return nil, false, fmt.Errorf("Mongo gateway dotted projection does not traverse arrays")
			}
			continue
		}
		projected, present, err := projectIncludedDocument(nested, child, false, false)
		if err != nil {
			return nil, false, err
		}
		if present {
			out = appendRawValueElement(out, key, bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: projected})
		}
	}
	out, err = bsoncore.AppendDocumentEnd(out, idx)
	if err != nil {
		return nil, false, err
	}
	return out, len(out) > 5, nil
}

func projectExcludedDocument(doc bson.Raw, node *projectionNode, includeID bool) (wire.Document, error) {
	elements, err := bson.Raw(doc).Elements()
	if err != nil {
		return nil, err
	}
	idx, out := bsoncore.AppendDocumentStart(make([]byte, 0, len(doc)))
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return nil, err
		}
		if key == "_id" && !includeID {
			continue
		}
		child := node.children[key]
		if child == nil {
			out = appendRawValueElement(out, key, elem.Value())
			continue
		}
		if child.terminal {
			continue
		}
		nested, ok := elem.Value().DocumentOK()
		if !ok {
			if _, array := elem.Value().ArrayOK(); array {
				return nil, fmt.Errorf("Mongo gateway dotted projection does not traverse arrays")
			}
			out = appendRawValueElement(out, key, elem.Value())
			continue
		}
		projected, err := projectExcludedDocument(nested, child, true)
		if err != nil {
			return nil, err
		}
		out = appendRawValueElement(out, key, bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: projected})
	}
	out, err = bsoncore.AppendDocumentEnd(out, idx)
	if err != nil {
		return nil, err
	}
	return wire.Document(out), nil
}

func appendRawValueElement(dst []byte, key string, value bson.RawValue) []byte {
	return bsoncore.AppendValueElement(dst, key, bsoncore.Value{
		Type: bsoncore.Type(value.Type),
		Data: value.Value,
	})
}

type projectionMode uint8

const (
	projectionNone projectionMode = iota
	projectionInclude
	projectionExclude
)

func projectionHasDottedPath(projection compiledProjection) bool {
	for field := range projection.fields {
		if strings.Contains(field, ".") {
			return true
		}
	}
	return false
}

func parseProjection(projection wire.Document) (projectionMode, map[string]struct{}, bool, bool, error) {
	elements, err := bson.Raw(projection).Elements()
	if err != nil {
		return projectionNone, nil, true, false, err
	}
	fields := make(map[string]struct{}, len(elements))
	mode := projectionNone
	includeID := true
	idSpecified := false
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return projectionNone, nil, true, false, err
		}
		include, err := projectionValueIncluded(elem.Value())
		if err != nil {
			return projectionNone, nil, true, false, err
		}
		if key == "_id" {
			includeID = include
			idSpecified = true
			continue
		}
		if key == "" || strings.HasPrefix(key, "$") {
			return projectionNone, nil, true, false, fmt.Errorf("Mongo gateway projection has invalid path %q", key)
		}
		nextMode := projectionExclude
		if include {
			nextMode = projectionInclude
		}
		if mode != projectionNone && mode != nextMode {
			return projectionNone, nil, true, false, errors.New("Mongo gateway projection cannot mix include and exclude fields")
		}
		mode = nextMode
		fields[key] = struct{}{}
	}
	return mode, fields, includeID, idSpecified, nil
}

func projectionValueIncluded(value bson.RawValue) (bool, error) {
	if v, ok := value.BooleanOK(); ok {
		return v, nil
	}
	if v, ok := value.Int32OK(); ok {
		return v != 0, nil
	}
	if v, ok := value.Int64OK(); ok {
		return v != 0, nil
	}
	if v, ok := value.DoubleOK(); ok {
		return v != 0, nil
	}
	return false, errors.New("Mongo gateway projection values must be boolean or numeric")
}

func lookupDocumentValue(doc wire.Document, field string) (bson.RawValue, bool) {
	if field == "" {
		return bson.RawValue{}, false
	}
	if !strings.Contains(field, ".") {
		value := bson.Raw(doc).Lookup(field)
		if value.IsZero() {
			return bson.RawValue{}, false
		}
		return value, true
	}
	parts := strings.Split(field, ".")
	current := bson.Raw(doc)
	for i, part := range parts {
		value := current.Lookup(part)
		if value.IsZero() {
			return bson.RawValue{}, false
		}
		if i == len(parts)-1 {
			return value, true
		}
		next, ok := value.DocumentOK()
		if !ok {
			return bson.RawValue{}, false
		}
		current = next
	}
	return bson.RawValue{}, false
}

func lookupDocumentPredicateValues(doc wire.Document, field string) ([]bson.RawValue, bool, error) {
	if field == "" {
		return nil, false, nil
	}
	parts := strings.Split(field, ".")
	return lookupRawValuesForParts(bson.Raw(doc), parts)
}

func lookupRawValuesForParts(current bson.Raw, parts []string) ([]bson.RawValue, bool, error) {
	if len(parts) == 0 {
		return nil, false, nil
	}
	value := current.Lookup(parts[0])
	if value.IsZero() {
		return nil, false, nil
	}
	if len(parts) == 1 {
		array, ok := value.ArrayOK()
		if !ok {
			return []bson.RawValue{value}, true, nil
		}
		values, err := array.Values()
		if err != nil {
			return nil, false, err
		}
		out := make([]bson.RawValue, 0, len(values)+1)
		out = append(out, value)
		out = append(out, values...)
		return out, true, nil
	}
	return lookupRawValueDescendants(value, parts[1:])
}

func lookupRawValueDescendants(value bson.RawValue, parts []string) ([]bson.RawValue, bool, error) {
	if doc, ok := value.DocumentOK(); ok {
		return lookupRawValuesForParts(doc, parts)
	}
	array, ok := value.ArrayOK()
	if !ok {
		return nil, false, nil
	}
	values, err := array.Values()
	if err != nil {
		return nil, false, err
	}
	if index, ok := dottedArrayIndex(parts[0]); ok {
		if index >= len(values) {
			return nil, false, nil
		}
		if len(parts) == 1 {
			return []bson.RawValue{values[index]}, true, nil
		}
		return lookupRawValueDescendants(values[index], parts[1:])
	}
	var out []bson.RawValue
	for _, item := range values {
		doc, ok := item.DocumentOK()
		if !ok {
			continue
		}
		itemValues, itemOK, err := lookupRawValuesForParts(doc, parts)
		if err != nil {
			return nil, false, err
		}
		if itemOK {
			out = append(out, itemValues...)
		}
	}
	if len(out) == 0 {
		return nil, false, nil
	}
	return out, true, nil
}

func dottedArrayIndex(part string) (int, bool) {
	if part == "" {
		return 0, false
	}
	for _, r := range part {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	index, err := strconv.Atoi(part)
	if err != nil {
		return 0, false
	}
	return index, true
}

func rawValuesBothScalar(left, right bson.RawValue) bool {
	return left.Type != bson.TypeEmbeddedDocument && left.Type != bson.TypeArray &&
		right.Type != bson.TypeEmbeddedDocument && right.Type != bson.TypeArray
}

func rawValuesEqual(left, right bson.RawValue) bool {
	return rawValuesEqualMode(left, right, false)
}

func mongoMutationValuesEqual(left, right bson.RawValue) bool {
	return rawValuesEqualMode(left, right, true)
}

func rawValuesEqualMode(left, right bson.RawValue, equalNaN bool) bool {
	equal, _ := rawValuesEqualModeBudget(left, right, equalNaN, nil)
	return equal
}

func rawValuesEqualModeBudget(left, right bson.RawValue, equalNaN bool, remainingDecimal128Normalizations *int) (bool, error) {
	if left.Type != bson.TypeCodeWithScope && right.Type != bson.TypeCodeWithScope && rawValuesBothScalar(left, right) {
		return rawScalarValuesEqualModeBudget(left, right, equalNaN, remainingDecimal128Normalizations)
	}
	type frame struct {
		left, right []byte
		document    bool
	}
	currentLeft, currentRight := left, right
	frames := make([]frame, 0, 8)
	for {
		if !currentLeft.IsZero() || !currentRight.IsZero() {
			if currentLeft.Type == bson.TypeCodeWithScope || currentRight.Type == bson.TypeCodeWithScope {
				if currentLeft.Type != bson.TypeCodeWithScope || currentRight.Type != bson.TypeCodeWithScope {
					return false, nil
				}
				leftCode, leftScope, leftRemaining, leftOK := bsoncore.ReadCodeWithScope(currentLeft.Value)
				rightCode, rightScope, rightRemaining, rightOK := bsoncore.ReadCodeWithScope(currentRight.Value)
				if !leftOK || !rightOK || len(leftRemaining) != 0 || len(rightRemaining) != 0 || leftCode != rightCode {
					return false, nil
				}
				if len(frames) == mongoMutationMaxBSONNesting {
					return false, nil
				}
				leftContents, leftOK := rawBSONContainerContents(bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: leftScope})
				rightContents, rightOK := rawBSONContainerContents(bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: rightScope})
				if !leftOK || !rightOK {
					return false, nil
				}
				// The scope document contributes one container level, matching the
				// shared mutation nesting validator.
				frames = append(frames, frame{left: leftContents, right: rightContents, document: true})
				currentLeft, currentRight = bson.RawValue{}, bson.RawValue{}
				continue
			}
			if rawValuesBothScalar(currentLeft, currentRight) {
				equal, err := rawScalarValuesEqualModeBudget(currentLeft, currentRight, equalNaN, remainingDecimal128Normalizations)
				if err != nil || !equal {
					return false, err
				}
			} else {
				if currentLeft.Type != currentRight.Type {
					return false, nil
				}
				leftRemaining, leftOK := rawBSONContainerContents(currentLeft)
				rightRemaining, rightOK := rawBSONContainerContents(currentRight)
				if !leftOK || !rightOK {
					return false, nil
				}
				if len(frames) == mongoMutationMaxBSONNesting {
					return false, nil
				}
				frames = append(frames, frame{
					left:     leftRemaining,
					right:    rightRemaining,
					document: currentLeft.Type == bson.TypeEmbeddedDocument,
				})
			}
			currentLeft, currentRight = bson.RawValue{}, bson.RawValue{}
			continue
		}
		if len(frames) == 0 {
			return true, nil
		}
		last := len(frames) - 1
		current := &frames[last]
		if len(current.left) == 0 || len(current.right) == 0 {
			if len(current.left) != len(current.right) {
				return false, nil
			}
			frames = frames[:last]
			continue
		}
		leftElement, leftRemaining, leftOK := bsoncore.ReadElement(current.left)
		rightElement, rightRemaining, rightOK := bsoncore.ReadElement(current.right)
		if !leftOK || !rightOK || leftElement.Validate() != nil || rightElement.Validate() != nil {
			return false, nil
		}
		if current.document && !bytes.Equal(leftElement.KeyBytes(), rightElement.KeyBytes()) {
			return false, nil
		}
		leftValue, leftErr := leftElement.ValueErr()
		rightValue, rightErr := rightElement.ValueErr()
		if leftErr != nil || rightErr != nil {
			return false, nil
		}
		current.left, current.right = leftRemaining, rightRemaining
		currentLeft = bson.RawValue{Type: bson.Type(leftValue.Type), Value: leftValue.Data}
		currentRight = bson.RawValue{Type: bson.Type(rightValue.Type), Value: rightValue.Data}
	}
}

func rawBSONContainerContents(value bson.RawValue) ([]byte, bool) {
	if value.Type != bson.TypeEmbeddedDocument && value.Type != bson.TypeArray {
		return nil, false
	}
	length, remaining, ok := bsoncore.ReadLength(value.Value)
	if !ok || length < 5 || int(length) != len(value.Value) || len(remaining) == 0 || remaining[len(remaining)-1] != 0 {
		return nil, false
	}
	return remaining[:len(remaining)-1], true
}

func rawScalarValuesEqualModeBudget(left, right bson.RawValue, equalNaN bool, remainingDecimal128Normalizations *int) (bool, error) {
	if left.IsNumber() && right.IsNumber() {
		if rawValueIsNaN(left) || rawValueIsNaN(right) {
			return equalNaN && rawValueIsNaN(left) && rawValueIsNaN(right), nil
		}
		if left.Type == right.Type && bytes.Equal(left.Value, right.Value) {
			return true, nil
		}
		if remainingDecimal128Normalizations != nil {
			normalizations := decimal128NormalizationCount(left) + decimal128NormalizationCount(right)
			if normalizations > *remainingDecimal128Normalizations {
				return false, fmt.Errorf("Mongo gateway query equality exceeds %d Decimal128 normalizations", mongoQueryMaxDecimal128Normalizations)
			}
			*remainingDecimal128Normalizations -= normalizations
		}
		return compareRawValues(left, right) == 0, nil
	}
	return left.Type == right.Type && left.Equal(right), nil
}

func decimal128NormalizationCount(value bson.RawValue) int {
	if value.Type != bson.TypeDecimal128 {
		return 0
	}
	if _, nonFinite := rawNumberNonFiniteRank(value); nonFinite {
		return 0
	}
	return 1
}

func compareRawValues(left, right bson.RawValue) int {
	if left.IsNumber() && right.IsNumber() {
		return compareRawNumbers(left, right)
	}
	if left.Type != right.Type {
		return compareInt(bsonTypeSortRank(left.Type), bsonTypeSortRank(right.Type))
	}
	switch left.Type {
	case bson.TypeString:
		return strings.Compare(left.StringValue(), right.StringValue())
	case bson.TypeBoolean:
		leftBool := left.Boolean()
		rightBool := right.Boolean()
		switch {
		case leftBool == rightBool:
			return 0
		case !leftBool && rightBool:
			return -1
		default:
			return 1
		}
	case bson.TypeNull:
		return 0
	case bson.TypeObjectID:
		return bytes.Compare(left.Value, right.Value)
	case bson.TypeDateTime:
		leftMilliseconds, leftOK := left.DateTimeOK()
		rightMilliseconds, rightOK := right.DateTimeOK()
		if leftOK && rightOK {
			return compareInt64(leftMilliseconds, rightMilliseconds)
		}
	case bson.TypeTimestamp:
		leftTimestamp, leftOrdinal, leftOK := left.TimestampOK()
		rightTimestamp, rightOrdinal, rightOK := right.TimestampOK()
		if leftOK && rightOK {
			if timestampCompare := compareUint32(leftTimestamp, rightTimestamp); timestampCompare != 0 {
				return timestampCompare
			}
			return compareUint32(leftOrdinal, rightOrdinal)
		}
	default:
		return bytes.Compare(left.Value, right.Value)
	}
	// Invalid raw values are not admissible index components, but preserve the
	// existing deterministic fallback for defensive collection-scan sorting.
	return bytes.Compare(left.Value, right.Value)
}

func compareUint32(left, right uint32) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func compareRawNumbers(left, right bson.RawValue) int {
	leftRat, leftOK := rawNumberRat(left)
	rightRat, rightOK := rawNumberRat(right)
	if leftOK && rightOK {
		return leftRat.Cmp(rightRat)
	}
	leftRank := numberSortRank(left, leftOK)
	rightRank := numberSortRank(right, rightOK)
	if leftRank != rightRank {
		return compareInt(leftRank, rightRank)
	}
	if leftRank != 4 {
		return 0
	}
	if left.Type != right.Type {
		return compareInt(bsonTypeSortRank(left.Type), bsonTypeSortRank(right.Type))
	}
	return 0
}

func numberSortRank(value bson.RawValue, finite bool) int {
	if finite {
		return 2
	}
	if rank, ok := rawNumberNonFiniteRank(value); ok {
		return rank
	}
	return 4
}

func rawNumberNonFiniteRank(value bson.RawValue) (int, bool) {
	switch value.Type {
	case bson.TypeDouble:
		v, ok := value.DoubleOK()
		if ok {
			switch {
			case math.IsNaN(v):
				return 0, true
			case math.IsInf(v, -1):
				return 1, true
			case math.IsInf(v, 1):
				return 3, true
			}
		}
	case bson.TypeDecimal128:
		v, ok := value.Decimal128OK()
		if ok {
			if v.IsNaN() {
				return 0, true
			}
			switch v.IsInf() {
			case -1:
				return 1, true
			case 1:
				return 3, true
			}
		}
	}
	return 0, false
}

func rawValueIsNaN(value bson.RawValue) bool {
	rank, ok := rawNumberNonFiniteRank(value)
	return ok && rank == 0
}

func rawNumberRat(value bson.RawValue) (*big.Rat, bool) {
	switch value.Type {
	case bson.TypeInt32:
		v, ok := value.Int32OK()
		if !ok {
			return nil, false
		}
		return big.NewRat(int64(v), 1), true
	case bson.TypeInt64:
		v, ok := value.Int64OK()
		if !ok {
			return nil, false
		}
		return big.NewRat(v, 1), true
	case bson.TypeDouble:
		v, ok := value.DoubleOK()
		if !ok {
			return nil, false
		}
		rat := new(big.Rat)
		if rat.SetFloat64(v) == nil {
			return nil, false
		}
		return rat, true
	case bson.TypeDecimal128:
		v, ok := value.Decimal128OK()
		if !ok {
			return nil, false
		}
		return decimal128Rat(v)
	default:
		return nil, false
	}
}

func rawNumberComparable(value bson.RawValue) bool {
	if _, ok := rawNumberRat(value); ok {
		return true
	}
	_, ok := rawNumberNonFiniteRank(value)
	return ok && !rawValueIsNaN(value)
}

func decimal128Rat(value bson.Decimal128) (*big.Rat, bool) {
	significand, exponent, err := value.BigInt()
	if err != nil {
		return nil, false
	}
	rat := new(big.Rat).SetInt(significand)
	if exponent == 0 {
		return rat, true
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(absInt(exponent))), nil)
	if exponent > 0 {
		rat.Mul(rat, new(big.Rat).SetInt(scale))
	} else {
		rat.Quo(rat, new(big.Rat).SetInt(scale))
	}
	return rat, true
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func compareInt(left, right int) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func bsonTypeSortRank(t bson.Type) int {
	switch t {
	case bson.TypeMinKey:
		return 0
	case bson.TypeNull:
		return 1
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
		return 2
	case bson.TypeString:
		return 3
	case bson.TypeEmbeddedDocument:
		return 4
	case bson.TypeArray:
		return 5
	case bson.TypeBinary:
		return 6
	case bson.TypeObjectID:
		return 7
	case bson.TypeBoolean:
		return 8
	case bson.TypeDateTime:
		return 9
	case bson.TypeTimestamp:
		return 10
	case bson.TypeMaxKey:
		return 100
	default:
		return 50 + int(t)
	}
}

func indexScalarForBSONValue(value bson.RawValue, valueType collections.IndexValueType) (any, bool) {
	switch valueType {
	case collections.IndexValueBSONOrderedV2:
		if _, err := collections.EncodeBSONIndexKeyComponentV2(value); err != nil {
			return nil, false
		}
		return value, true
	case collections.IndexValueString:
		if value.Type != bson.TypeString {
			return nil, false
		}
		out, ok := value.StringValueOK()
		return out, ok
	case collections.IndexValueBool:
		if value.Type != bson.TypeBoolean {
			return nil, false
		}
		out, ok := value.BooleanOK()
		return out, ok
	case collections.IndexValueInt64:
		switch value.Type {
		case bson.TypeInt32:
			out, ok := value.Int32OK()
			if !ok {
				return nil, false
			}
			return int64(out), true
		case bson.TypeInt64:
			out, ok := value.Int64OK()
			return out, ok
		case bson.TypeDouble:
			out, ok := value.DoubleOK()
			if !ok {
				return nil, false
			}
			intValue, ok := exactInt64FromFloat64(out)
			if !ok {
				return nil, false
			}
			return intValue, true
		case bson.TypeDecimal128:
			out, ok := value.Decimal128OK()
			if !ok {
				return nil, false
			}
			intValue, ok := exactInt64FromDecimal128(out)
			if !ok {
				return nil, false
			}
			return intValue, true
		default:
			return nil, false
		}
	case collections.IndexValueDouble:
		switch value.Type {
		case bson.TypeDouble:
			out, ok := value.DoubleOK()
			if !ok || math.IsNaN(out) {
				return nil, false
			}
			return out, ok
		case bson.TypeInt32:
			out, ok := value.Int32OK()
			if !ok {
				return nil, false
			}
			return float64(out), true
		case bson.TypeInt64:
			out, ok := value.Int64OK()
			if !ok || !int64CanRepresentAsExactFloat64(out) {
				return nil, false
			}
			return float64(out), true
		case bson.TypeDecimal128:
			out, ok := value.Decimal128OK()
			if !ok {
				return nil, false
			}
			doubleValue, ok := exactFloat64FromDecimal128(out)
			if !ok {
				return nil, false
			}
			return doubleValue, true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

func uncoercibleNumericRangeShouldScan(value bson.RawValue, valueType collections.IndexValueType) bool {
	switch valueType {
	case collections.IndexValueInt64, collections.IndexValueDouble:
		return value.IsNumber() && rawNumberComparable(value)
	default:
		return false
	}
}

func unindexedRangePredicateShouldScan(value bson.RawValue, valueType collections.IndexValueType) bool {
	return rawValueIsNull(value) || uncoercibleNumericRangeShouldScan(value, valueType)
}

func exactInt64FromDecimal128(value bson.Decimal128) (int64, bool) {
	rat, ok := decimal128Rat(value)
	if !ok || rat.Denom().Cmp(big.NewInt(1)) != 0 || !rat.Num().IsInt64() {
		return 0, false
	}
	return rat.Num().Int64(), true
}

func exactFloat64FromDecimal128(value bson.Decimal128) (float64, bool) {
	rat, ok := decimal128Rat(value)
	if !ok {
		return 0, false
	}
	out, exact := rat.Float64()
	if !exact || math.IsNaN(out) || math.IsInf(out, 0) {
		return 0, false
	}
	return out, true
}

func exactInt64FromFloat64(value float64) (int64, bool) {
	const minInt64AsFloat64 = -9223372036854775808.0
	const maxInt64PlusOneAsFloat64 = 9223372036854775808.0
	if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value {
		return 0, false
	}
	if value < minInt64AsFloat64 || value >= maxInt64PlusOneAsFloat64 {
		return 0, false
	}
	return int64(value), true
}

func int64CanRepresentAsExactFloat64(value int64) bool {
	const int64UpperBoundAsFloat64 = 9223372036854775808.0
	out := float64(value)
	if out < -int64UpperBoundAsFloat64 || out >= int64UpperBoundAsFloat64 {
		return false
	}
	return int64(out) == value
}
