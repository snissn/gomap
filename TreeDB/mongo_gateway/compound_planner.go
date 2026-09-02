package mongogateway

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// compoundIndexPlan is the bounded, standalone-only bridge from Mongo's
// supported scalar predicates to #4063's direct BSON-v2 range primitive.  It
// deliberately does not share the legacy FindByIndexValue/Range path: those
// APIs cannot encode descending or multi-component BSON-v2 bounds correctly.
type compoundIndexPlan struct {
	idx             collections.IndexDefinition
	prefixChoices   [][]bson.RawValue
	lower           collections.IndexRangeBound
	upper           collections.IndexRangeBound
	hasRange        bool
	reverse         bool
	sortSatisfied   bool
	equalityPrefix  int
	residualFilters int
}

func compoundIndexComponents(idx collections.IndexDefinition) []collections.IndexComponent {
	if len(idx.Components) != 0 {
		return idx.Components
	}
	return []collections.IndexComponent{{Field: idx.Field, Direction: collections.IndexDirectionAscending}}
}

func compoundIndexPlans(meta collections.CollectionMeta, plan findPlan) []compoundIndexPlan {
	if len(plan.orBranches) != 0 || len(plan.norBranches) != 0 {
		return nil
	}
	out := make([]compoundIndexPlan, 0)
	for _, idx := range meta.Indexes {
		if plan.hint.present && !findHintMatchesIndex(plan.hint, idx) {
			continue
		}
		// BSON-v2 array expansion has index-entry semantics, not Mongo document
		// sort semantics: a scalar array produces several physical keys and an
		// empty array produces none.  A direct index walk therefore cannot prove
		// a complete, stable document order before skip/limit.  Keep the public
		// #4063 direct primitive available, but never select an array-capable
		// index automatically.  A strict hint consequently fails before opening
		// an index iterator rather than observing a partial ordering.
		if idx.ValueType == collections.IndexValueBSONOrderedV2 &&
			(idx.MultiKey || meta.Options.AllowArrayValuesInIndex) {
			continue
		}
		candidate, ok := buildCompoundIndexPlan(idx, plan)
		if ok {
			out = append(out, candidate)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].equalityPrefix != out[j].equalityPrefix {
			return out[i].equalityPrefix > out[j].equalityPrefix
		}
		if out[i].hasRange != out[j].hasRange {
			return out[i].hasRange
		}
		if out[i].sortSatisfied != out[j].sortSatisfied {
			return out[i].sortSatisfied
		}
		if out[i].residualFilters != out[j].residualFilters {
			return out[i].residualFilters < out[j].residualFilters
		}
		if out[i].idx.Unique != out[j].idx.Unique {
			return out[i].idx.Unique
		}
		return out[i].idx.Name < out[j].idx.Name
	})
	return out
}

// compoundPlanPaginationSafe reports whether an exact single-prefix walk may
// stop after skip+limit candidates. With no requested sort the visible order is
// intentionally unconstrained, so any prefix order is sufficient; with a
// requested sort, the physical walk must satisfy the complete gateway order.
func compoundPlanPaginationSafe(candidate compoundIndexPlan, plan findPlan) bool {
	return candidate.residualFilters == 0 && (plan.sort.field == "" || candidate.sortSatisfied)
}

// compoundPlanDeferredToLegacyLookup keeps the established selective legacy
// path available for unhinted queries only when it cannot discard a compound
// ordering advantage. A requested sort that a compound index satisfies is a
// real execution benefit: selecting a scalar equality probe instead would
// force an in-memory sort and make explain's selected plan disagree with the
// executor. No requested sort is deliberately order-irrelevant, so the
// legacy probe remains preferred there.
func compoundPlanDeferredToLegacyLookup(meta collections.CollectionMeta, plan findPlan) bool {
	if plan.hint.present || len(findIndexProbes(meta, plan)) == 0 {
		return false
	}
	candidate, ok := compoundIndexPlanFor(meta, plan)
	return !ok || plan.sort.field == "" || !candidate.sortSatisfied
}

// mongoPrimaryKeyLess compares canonical gateway primary keys by their BSON
// _id values. Storage byte order is intentionally not Mongo's public _id
// order (for example strings retain BSON's little-endian length).
func mongoPrimaryKeyLess(left, right []byte) bool {
	decode := func(key []byte) (bson.RawValue, bool) {
		if len(key) < 2 || key[0] != primaryKeyPrefixBSONValue {
			return bson.RawValue{}, false
		}
		value := bson.RawValue{Type: bson.Type(key[1]), Value: key[2:]}
		return value, value.Validate() == nil
	}
	leftValue, leftOK := decode(left)
	rightValue, rightOK := decode(right)
	if !leftOK || !rightOK {
		return bytes.Compare(left, right) < 0
	}
	return compareRawValues(leftValue, rightValue) < 0
}

func findHintMatchesIndex(hint findHint, idx collections.IndexDefinition) bool {
	if !hint.present {
		return true
	}
	if hint.name != "" {
		return idx.Name == hint.name
	}
	components := compoundIndexComponents(idx)
	if len(components) != len(hint.components) {
		return false
	}
	for i := range components {
		if components[i] != hint.components[i] {
			return false
		}
	}
	return true
}

// validateCompoundHint is deliberately metadata-only. Callers invoke it before
// opening a materializer or issuing an index/primary read, so an invalid hint
// cannot become an observation-dependent fallback.
func validateCompoundHint(meta collections.CollectionMeta, plan findPlan) error {
	if !plan.hint.present {
		return nil
	}
	found := false
	for _, idx := range meta.Indexes {
		if findHintMatchesIndex(plan.hint, idx) {
			found = true
			break
		}
	}
	if !found {
		return errors.New("Mongo gateway find hint does not name an existing index")
	}
	if _, ok := compoundIndexPlanFor(meta, plan); !ok {
		return errors.New("Mongo gateway find hint is incompatible with the supported compound-index query shape")
	}
	return nil
}

func buildCompoundIndexPlan(idx collections.IndexDefinition, plan findPlan) (compoundIndexPlan, bool) {
	if idx.ValueType != collections.IndexValueBSONOrderedV2 {
		return compoundIndexPlan{}, false
	}
	components := compoundIndexComponents(idx)
	if len(components) == 0 {
		return compoundIndexPlan{}, false
	}
	candidate := compoundIndexPlan{idx: idx}
	used := make(map[int]struct{})
	component := 0
	for component < len(components) {
		predicates := predicatesForField(plan.predicates, components[component].Field)
		eq, eqOK := oneCompoundEqualityPredicate(predicates)
		// Gateway null equality also matches a missing field. BSON-v2 stores
		// those as distinct keys, so an exact null prefix would silently omit
		// missing documents unless it probes both encodings.
		if eqOK && predicateContainsNull(eq) {
			return compoundIndexPlan{}, false
		}
		if eqOK {
			// `$in` fans out one bounded probe per prefix combination. Results are
			// concatenated rather than k-way merged, so the later sort check rejects
			// multi-value prefixes from supplying index order.
			if len(eq.values) == 0 {
				return compoundIndexPlan{}, false
			}
			values := compoundPredicatePrefixValues(eq)
			if len(values) == 0 || len(values) > maxCompoundPlannerPrefixChoices {
				return compoundIndexPlan{}, false
			}
			candidate.prefixChoices = append(candidate.prefixChoices, values)
			if compoundPrefixCombinationCount(candidate.prefixChoices) > maxCompoundPlannerPrefixChoices {
				return compoundIndexPlan{}, false
			}
			candidate.equalityPrefix++
			for i, pred := range plan.predicates {
				if !pred.negate && pred.field == eq.field && (pred.op == findPredicateEq || pred.op == findPredicateIn) {
					used[i] = struct{}{}
				}
			}
			component++
			continue
		}
		lower, upper, hasRange, valid := compoundRangeBounds(predicates)
		if !valid {
			return compoundIndexPlan{}, false
		}
		if hasRange {
			candidate.lower, candidate.upper, candidate.hasRange = lower, upper, true
			// BSON comparison ranges are type-bracketed, while BSON-v2 storage
			// orders missing and null before scalar values. Keep range predicates
			// residual until bounds encode that bracket exactly; otherwise a
			// skip/limit page could stop at a non-matching cross-type candidate.
			if compoundRangeIsExactlyTypeBracketed(lower, upper) {
				for i, pred := range plan.predicates {
					if !pred.negate && pred.field == components[component].Field && isRangePredicate(pred.op) {
						used[i] = struct{}{}
					}
				}
			}
		}
		break
	}
	if candidate.equalityPrefix == 0 && !candidate.hasRange {
		// An unbounded index traversal is admissible only when it supplies the
		// requested order.  It remains bounded by MaxFindScanDocuments below.
		if plan.sort.field == "" {
			return compoundIndexPlan{}, false
		}
	}
	if plan.sort.field != "" {
		if compoundPrefixCombinationCount(candidate.prefixChoices) != 1 {
			return compoundIndexPlan{}, false
		}
		terms := findSortTerms(plan.sort)
		// Every remaining unfixed component must participate in the requested
		// order. Otherwise it physically orders equal sort values before _id.
		if component+len(terms) != len(components) || components[component].Field != terms[0].field {
			return compoundIndexPlan{}, false
		}
		candidate.sortSatisfied = true
		candidate.reverse = components[component].Direction != findSortTermDirection(terms[0])
		for i, term := range terms {
			want := findSortTermDirection(term)
			if candidate.reverse {
				want = -want
			}
			if components[component+i].Field != term.field || components[component+i].Direction != want {
				return compoundIndexPlan{}, false
			}
		}
		// BSON comparison treats missing and null as equal. In a multi-field
		// sort, those physically distinct runs can interleave with the later
		// components, so one bounded adjacent tie buffer cannot preserve the
		// gateway comparator. Keep the compound candidate, but let the bounded
		// executor apply its in-memory comparator before pagination.
		if len(terms) > 1 {
			candidate.sortSatisfied = false
		}
	}
	candidate.residualFilters = len(plan.predicates) - len(used)
	return candidate, true
}

const maxCompoundPlannerPrefixChoices = 64

func compoundPrefixCombinationCount(choices [][]bson.RawValue) int {
	count := 1
	for _, values := range choices {
		if len(values) == 0 || count > maxCompoundPlannerPrefixChoices/len(values) {
			return maxCompoundPlannerPrefixChoices + 1
		}
		count *= len(values)
	}
	return count
}

// canonicalCompoundPrefixValues deduplicates by the actual BSON-v2 probe
// component before fanout eligibility is evaluated. In particular, numeric
// equality shares an index key across integer widths.
func canonicalCompoundPrefixValues(values []bson.RawValue) []bson.RawValue {
	capacity := len(values)
	if capacity > maxCompoundPlannerPrefixChoices+1 {
		capacity = maxCompoundPlannerPrefixChoices + 1
	}
	out := make([]bson.RawValue, 0, capacity)
	seen := make(map[string]struct{}, capacity)
	for _, value := range values {
		encoded, err := collections.EncodeBSONIndexKeyComponentV2(value)
		if err != nil {
			return nil
		}
		if _, duplicate := seen[string(encoded)]; duplicate {
			continue
		}
		seen[string(encoded)] = struct{}{}
		out = append(out, value)
		if len(out) > maxCompoundPlannerPrefixChoices {
			return out
		}
	}
	return out
}

// cacheCompoundCanonicalValues performs the potentially raw-input-sized BSON
// encoding pass once while parsing a request. It deliberately retains only a
// small (at most 65) representative slice; duplicate-heavy $in remains
// supported without multiplying work by the number of usable indexes.
func cacheCompoundCanonicalValues(predicates []findPredicate) {
	for i := range predicates {
		if predicates[i].compoundCanonicalized {
			continue
		}
		if predicates[i].negate || predicates[i].op != findPredicateIn {
			continue
		}
		predicates[i].compoundCanonicalValues = canonicalCompoundPrefixValues(predicates[i].values)
		predicates[i].compoundCanonicalized = true
	}
}

// finalizeFindPlan initializes request-local planner caches for every command
// adapter that constructs a findPlan. Keeping this here avoids a find-only
// initialization rule: count, distinct, aggregate and explain all share the
// same compound planner and must not repeat raw $in canonicalization.
func finalizeFindPlan(plan findPlan) findPlan {
	cacheCompoundCanonicalValues(plan.predicates)
	for i := range plan.orBranches {
		cacheCompoundCanonicalValues(plan.orBranches[i])
	}
	return plan
}

func compoundPredicatePrefixValues(predicate findPredicate) []bson.RawValue {
	if predicate.compoundCanonicalized {
		return predicate.compoundCanonicalValues
	}
	// Package-local callers and focused planner tests may construct a plan
	// directly instead of parsing a wire command. Preserve that API shape
	// without weakening production's request-local cache contract.
	return canonicalCompoundPrefixValues(predicate.values)
}

func compoundPrefixes(choices [][]bson.RawValue) [][]bson.RawValue {
	if len(choices) == 0 {
		return [][]bson.RawValue{{}}
	}
	out := make([][]bson.RawValue, 0, compoundPrefixCombinationCount(choices))
	var visit func(int, []bson.RawValue)
	visit = func(at int, prefix []bson.RawValue) {
		if at == len(choices) {
			out = append(out, append([]bson.RawValue(nil), prefix...))
			return
		}
		for _, value := range choices[at] {
			visit(at+1, append(prefix, value))
		}
	}
	visit(0, nil)
	return out
}

func findSortTermDirection(term findSortTerm) collections.IndexDirection {
	if term.desc {
		return collections.IndexDirectionDescending
	}
	return collections.IndexDirectionAscending
}

func predicatesForField(predicates []findPredicate, field string) []findPredicate {
	out := make([]findPredicate, 0, 2)
	for _, pred := range predicates {
		if pred.field == field {
			out = append(out, pred)
		}
	}
	return out
}

func oneCompoundEqualityPredicate(predicates []findPredicate) (findPredicate, bool) {
	var found findPredicate
	for _, pred := range predicates {
		if pred.negate || (pred.op != findPredicateEq && pred.op != findPredicateIn) {
			continue
		}
		if found.field != "" {
			return findPredicate{}, false
		}
		found = pred
	}
	return found, found.field != ""
}

func compoundRangeBounds(predicates []findPredicate) (collections.IndexRangeBound, collections.IndexRangeBound, bool, bool) {
	var lower, upper collections.IndexRangeBound
	hasRange := false
	for _, pred := range predicates {
		if pred.negate || !isRangePredicate(pred.op) {
			continue
		}
		if len(pred.values) != 1 || rawValueIsNaN(pred.values[0]) {
			return collections.IndexRangeBound{}, collections.IndexRangeBound{}, false, false
		}
		if _, err := collections.EncodeBSONIndexKeyComponentV2(pred.values[0]); err != nil {
			return collections.IndexRangeBound{}, collections.IndexRangeBound{}, false, false
		}
		hasRange = true
		bound := collections.IndexRangeBound{Value: pred.values[0], Inclusive: pred.op == findPredicateGTE || pred.op == findPredicateLTE}
		switch pred.op {
		case findPredicateGT, findPredicateGTE:
			cmp := 1
			if lower.Value != nil {
				cmp = compareRawValues(bound.Value.(bson.RawValue), lower.Value.(bson.RawValue))
			}
			if lower.Value == nil || cmp > 0 {
				lower = bound
			} else if cmp == 0 {
				lower.Inclusive = lower.Inclusive && bound.Inclusive
			}
		case findPredicateLT, findPredicateLTE:
			cmp := -1
			if upper.Value != nil {
				cmp = compareRawValues(bound.Value.(bson.RawValue), upper.Value.(bson.RawValue))
			}
			if upper.Value == nil || cmp < 0 {
				upper = bound
			} else if cmp == 0 {
				upper.Inclusive = upper.Inclusive && bound.Inclusive
			}
		}
	}
	if !hasRange {
		return collections.IndexRangeBound{Unbounded: true}, collections.IndexRangeBound{Unbounded: true}, false, true
	}
	if lower.Value == nil {
		lower.Unbounded = true
	}
	if upper.Value == nil {
		upper.Unbounded = true
	}
	return lower, upper, true, true
}

func (s *Server) documentsForCompoundIndexPlan(col *collections.Collection, materializer *collections.StoredDocumentJSONMaterializer, plan findPlan) ([]wire.Document, compoundIndexPlan, bool, error) {
	// Keep direct primary-key lookup as the established winner for supported
	// `_id` equality/$in shapes. Compound planning must be additive, never a
	// regression of that bounded path.
	if !plan.hint.present {
		if _, ok := primaryCandidatePredicate(plan.predicates); ok {
			return nil, compoundIndexPlan{}, false, nil
		}
		if compoundPlanDeferredToLegacyLookup(col.MetaView(), plan) {
			return nil, compoundIndexPlan{}, false, nil
		}
	}
	ids, candidate, ok, err := s.compoundIndexPlanIDs(col, plan)
	if err != nil && !plan.hint.present && len(findIndexProbes(col.MetaView(), plan)) != 0 && errors.Is(err, errMongoFindScanCapExceeded) {
		// A sort-satisfying compound walk is preferred when it completes, but
		// it is not entitled to turn a selective legacy alternative into an
		// error. Hand the command back to the established adaptive legacy
		// selector, which may materialize a smaller candidate set then sort it.
		return nil, candidate, false, nil
	}
	if !ok || err != nil {
		return nil, candidate, ok, err
	}
	if compoundPlanPaginationSafe(candidate, plan) {
		start := int(plan.skip)
		if start >= len(ids) {
			ids = nil
		} else {
			ids = ids[start:]
		}
		if plan.limit > 0 && int(plan.limit) < len(ids) {
			ids = ids[:plan.limit]
		}
	}
	maxDocuments := s.maxFindScanDocuments()
	docs := make([]wire.Document, 0, len(ids))
	materializedBytes := 0
	maxMaterializedBytes := s.maxCursorRetainedBytes()
	for _, id := range ids {
		stored, err := col.Get(id)
		if err != nil {
			return nil, candidate, true, err
		}
		if len(stored) == 0 {
			continue
		}
		plan.recordMaterialized()
		doc, err := storedDocumentToBSON(col, materializer, stored)
		if err != nil {
			return nil, candidate, true, err
		}
		if len(doc) > maxMaterializedBytes-materializedBytes {
			return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index materialization exceeded %d bytes", errMongoFindScanCapExceeded, maxMaterializedBytes)
		}
		materializedBytes += len(doc)
		plan.recordMaterializedBytes(len(doc))
		docs = append(docs, doc)
		if len(docs) > maxDocuments {
			return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index fetched more than %d documents", errMongoFindScanCapExceeded, maxDocuments)
		}
	}
	return docs, candidate, true, nil
}

// compoundIndexPlanIDs performs the bounded ordered index walk without
// decoding documents. Cursor execution retains these small primary keys and
// materializes BSON only as each batch is requested.
func (s *Server) compoundIndexPlanIDs(col *collections.Collection, plan findPlan, retainedIDCaps ...int) ([][]byte, compoundIndexPlan, bool, error) {
	if s.compoundIndexPlanScanHook != nil {
		s.compoundIndexPlanScanHook()
	}
	if !plan.hint.present {
		if _, ok := primaryCandidatePredicate(plan.predicates); ok {
			return nil, compoundIndexPlan{}, false, nil
		}
	}
	candidates := compoundIndexPlans(col.MetaView(), plan)
	if len(candidates) == 0 {
		return nil, compoundIndexPlan{}, false, nil
	}
	// A strict hint names the required index. Unhinted plans, by contrast, are
	// advertised as adaptive candidates: a broad leading-prefix candidate may
	// exhaust its bounded walk while a later fully usable candidate is selective.
	// Try each concrete candidate rather than reporting a syntactic alternative
	// which execution never considers. Non-cap failures remain fail-closed.
	var lastCapErr error
	for _, candidate := range candidates {
		ids, selected, ok, err := s.compoundIndexPlanIDsForCandidate(col, plan, candidate, retainedIDCaps...)
		if err == nil || !ok || plan.hint.present || !errors.Is(err, errMongoFindScanCapExceeded) {
			return ids, selected, ok, err
		}
		lastCapErr = err
	}
	return nil, candidates[0], true, lastCapErr
}

func (s *Server) compoundIndexPlanIDsForCandidate(col *collections.Collection, plan findPlan, candidate compoundIndexPlan, retainedIDCaps ...int) ([][]byte, compoundIndexPlan, bool, error) {
	prefixes := compoundPrefixes(candidate.prefixChoices)
	maxDocuments := s.maxFindScanDocuments()
	// Keep the physical inspection ceiling tied to the global planner budget,
	// not a subsequently reduced result page. Stable BSON-v2 ties must inspect
	// the entire first logical group before emitting its deterministic _id
	// prefix, even for limit:1.
	maxInspected := compoundPlannerInspectedCap(maxDocuments)
	paginationBudgetCoversResult := false
	// Pagination can bound the index walk only when its physical order is the
	// complete gateway order. With no requested sort, pagination is likewise
	// safe in the physical prefix order. A fallback in-memory sort must see every
	// bounded candidate before applying skip/limit; otherwise a later null or
	// missing value may precede an early physical entry under Mongo ordering.
	if compoundPlanPaginationSafe(candidate, plan) && plan.limit > 0 && len(prefixes) == 1 {
		// skip and limit are int32 wire values. Add them at a wider width so
		// a syntactically valid but unrepresentable page cannot wrap negative
		// and accidentally accept a scan truncated by the global work cap.
		needed := int64(plan.skip) + int64(plan.limit)
		if needed > 0 && needed <= int64(maxDocuments) {
			paginationBudgetCoversResult = true
			if needed < int64(maxDocuments) {
				maxDocuments = int(needed)
			}
		}
	}
	if maxDocuments <= 0 {
		return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index planner requires a positive work cap", errMongoFindScanCapExceeded)
	}
	// Every execution shape retains the candidate ID slice and cross-prefix
	// dedupe map before it either materializes documents or opens a cursor.
	// Bound that owned memory even for single-batch/count/distinct/aggregate
	// paths; cursor dispatch may pass a smaller residual admission budget.
	retainedIDCap := s.maxCursorRetainedBytes()
	if len(retainedIDCaps) != 0 {
		retainedIDCap = retainedIDCaps[0]
	}
	initialIDCapacity := maxDocuments
	if retainedIDCap > 0 {
		initialIDCapacity = 0
	}
	ids := make([][]byte, 0, initialIDCapacity)
	seenIDs := make(map[string]struct{})
	retainedIDBytes := 0
	retainID := func(id []byte) (bool, error) {
		// The returned ID slice and cross-prefix dedupe key own separate
		// payloads. Charge both plus conservative slice/map bookkeeping before
		// converting to string or retaining either one.
		const retainedIDOverhead = 64
		maxInt := int(^uint(0) >> 1)
		if len(id) > (maxInt-retainedIDOverhead)/2 {
			return false, fmt.Errorf("%w: Mongo gateway compound cursor IDs exceed retained-byte cap", errMongoFindScanCapExceeded)
		}
		charge := len(id)*2 + retainedIDOverhead
		if retainedIDCap > 0 && charge > retainedIDCap-retainedIDBytes {
			return false, fmt.Errorf("%w: Mongo gateway compound cursor IDs exceed retained-byte cap", errMongoFindScanCapExceeded)
		}
		idKey := string(id)
		if _, duplicate := seenIDs[idKey]; duplicate {
			return false, nil
		}
		seenIDs[idKey] = struct{}{}
		retainedIDBytes += charge
		ids = append(ids, id)
		return true, nil
	}
	remainingCandidates := maxDocuments
	remainingInspected := maxInspected
	for _, prefix := range prefixes {
		// Every prefix shares one candidate budget. Once it is exhausted, a
		// one-ID probe can only establish whether another candidate exists; it
		// never contributes an ID to the result. This avoids both the former
		// even split (which rejected a sparse later prefix) and unbounded work
		// across $in branches.
		probeLimit := remainingCandidates
		if probeLimit == 0 {
			probeLimit = 1
		}
		directRetainedCap := 0
		if retainedIDCap > 0 {
			remainingRetained := retainedIDCap - retainedIDBytes
			if remainingRetained <= 0 {
				return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound cursor IDs exceed retained-byte cap", errMongoFindScanCapExceeded)
			}
			// The direct primitive owns the ID payload while this planner retains
			// a second string copy for cross-prefix dedupe, so reserve no more
			// than half of the remaining cursor budget for one probe result.
			directRetainedCap = remainingRetained / 2
			if directRetainedCap == 0 {
				return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound cursor IDs exceed retained-byte cap", errMongoFindScanCapExceeded)
			}
		}
		if remainingInspected <= 0 {
			return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index inspection budget exceeded %d entries", errMongoFindScanCapExceeded, maxInspected)
		}
		inspected := 0
		found, truncated, err := col.FindByCompoundIndexRange(candidate.idx.Name, collections.CompoundIndexRangeOptions{
			Prefix: prefix, Lower: candidate.lower, Upper: candidate.upper, Limit: probeLimit, Desc: candidate.reverse,
			MaxInspected:       remainingInspected,
			MaxRetainedIDBytes: directRetainedCap,
			Inspected:          &inspected,
			// Mongo's compatible sort contract uses _id as the deterministic tie
			// breaker. BSON-v2 keeps missing and null physically distinct, while
			// Mongo compares them as equal, so stable grouping is required in both
			// physical directions.
			StableDocumentIDTies: candidate.sortSatisfied,
			DocumentIDLess:       mongoPrimaryKeyLess,
		})
		if inspected > remainingInspected {
			return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index inspection budget exceeded %d entries", errMongoFindScanCapExceeded, maxInspected)
		}
		remainingInspected -= inspected
		if err != nil {
			return nil, candidate, true, err
		}
		if len(found) > remainingCandidates {
			return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index candidate scan exceeded %d documents", errMongoFindScanCapExceeded, maxDocuments)
		}
		if truncated {
			// For one exact ordered probe with no residual filtering, skip+limit
			// already bounds all IDs execution can observe. The direct primitive
			// marks the next entry as truncated; that is sufficient, not a scan
			// cap failure, because later IDs cannot affect this result page.
			if paginationBudgetCoversResult && len(found) == probeLimit {
				for _, id := range found {
					added, err := retainID(id)
					if err != nil {
						return nil, candidate, true, err
					}
					if added {
						plan.recordCandidate()
					}
				}
				continue
			}
			// The direct primitive has already examined and returned these
			// candidates. Account for the observable work even though the global
			// cap rejects the command before any primary document is loaded.
			for range found {
				plan.recordCandidate()
			}
			return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index candidate scan exceeded %d documents", errMongoFindScanCapExceeded, maxDocuments)
		}
		for _, id := range found {
			remainingCandidates--
			added, err := retainID(id)
			if err != nil {
				return nil, candidate, true, err
			}
			if added {
				plan.recordCandidate()
			}
		}
	}
	return ids, candidate, true, nil
}

func compoundPlannerInspectedCap(maxDocuments int) int {
	const perResult = 64
	maxInt := int(^uint(0) >> 1)
	if maxDocuments > maxInt/perResult {
		return maxInt
	}
	return maxDocuments * perResult
}

// compoundIDCursorEligible is intentionally metadata-only: command dispatch
// must not walk an index merely to discover that a residual or multi-prefix
// plan needs the ordinary executor. The executor then performs its one bounded
// walk and owns the corresponding executionStats counters.
func compoundIDCursorEligible(meta collections.CollectionMeta, plan findPlan) bool {
	if !plan.hint.present {
		if _, ok := primaryCandidatePredicate(plan.predicates); ok {
			return false
		}
	}
	candidate, ok := compoundIndexPlanFor(meta, plan)
	return ok && candidate.residualFilters == 0 &&
		(plan.sort.field == "" || candidate.sortSatisfied) &&
		compoundPrefixCombinationCount(candidate.prefixChoices) == 1
}

func compoundRangeIsExactlyTypeBracketed(lower, upper collections.IndexRangeBound) bool {
	if lower.Unbounded || upper.Unbounded || lower.Value == nil || upper.Value == nil {
		return false
	}
	lowerValue, lowerOK := lower.Value.(bson.RawValue)
	upperValue, upperOK := upper.Value.(bson.RawValue)
	return lowerOK && upperOK && lowerValue.Type == upperValue.Type
}

func compoundIndexPlanFor(meta collections.CollectionMeta, plan findPlan) (compoundIndexPlan, bool) {
	plans := compoundIndexPlans(meta, plan)
	if len(plans) == 0 {
		return compoundIndexPlan{}, false
	}
	return plans[0], true
}
