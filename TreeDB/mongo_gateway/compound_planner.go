package mongogateway

import (
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
	if len(plan.orBranches) != 0 {
		return nil
	}
	out := make([]compoundIndexPlan, 0)
	for _, idx := range meta.Indexes {
		if plan.hint.present && !findHintMatchesIndex(plan.hint, idx) {
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
			for _, value := range eq.values {
				if _, err := collections.EncodeBSONIndexKeyComponentV2(value); err != nil {
					return compoundIndexPlan{}, false
				}
			}
			if len(eq.values) > maxCompoundPlannerPrefixChoices {
				return compoundIndexPlan{}, false
			}
			candidate.prefixChoices = append(candidate.prefixChoices, append([]bson.RawValue(nil), eq.values...))
			if compoundPrefixCombinationCount(candidate.prefixChoices) > maxCompoundPlannerPrefixChoices {
				return compoundIndexPlan{}, false
			}
			candidate.equalityPrefix++
			for i, pred := range plan.predicates {
				if pred.field == eq.field && (pred.op == findPredicateEq || pred.op == findPredicateIn) {
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
					if pred.field == components[component].Field && isRangePredicate(pred.op) {
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
		if component+len(terms) > len(components) || components[component].Field != terms[0].field {
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
		seen := make(map[string]struct{}, len(choices[at]))
		for _, value := range choices[at] {
			// BSON-v2 canonicalizes numeric equality (for example int32(1) and
			// int64(1)). Deduplicate by the actual probe component rather than
			// raw BSON type/payload so equivalent $in choices do not consume the
			// global candidate budget twice.
			encoded, err := collections.EncodeBSONIndexKeyComponentV2(value)
			if err != nil {
				continue // buildCompoundIndexPlan already validated every value.
			}
			key := string(encoded)
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
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
		if pred.op != findPredicateEq && pred.op != findPredicateIn {
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
		if !isRangePredicate(pred.op) {
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
	}
	ids, candidate, ok, err := s.compoundIndexPlanIDs(col, plan)
	if !ok || err != nil {
		return nil, candidate, ok, err
	}
	if candidate.residualFilters == 0 && candidate.sortSatisfied {
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
func (s *Server) compoundIndexPlanIDs(col *collections.Collection, plan findPlan) ([][]byte, compoundIndexPlan, bool, error) {
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
	candidate := candidates[0]
	prefixes := compoundPrefixes(candidate.prefixChoices)
	maxDocuments := s.maxFindScanDocuments()
	if candidate.residualFilters == 0 && plan.limit > 0 && len(prefixes) == 1 {
		needed := int(plan.skip + plan.limit)
		if needed > 0 && needed < maxDocuments {
			maxDocuments = needed
		}
	}
	if maxDocuments <= 0 {
		return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index planner requires a positive work cap", errMongoFindScanCapExceeded)
	}
	ids := make([][]byte, 0, maxDocuments)
	seenIDs := make(map[string]struct{})
	remainingCandidates := maxDocuments
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
		found, truncated, err := col.FindByCompoundIndexRange(candidate.idx.Name, collections.CompoundIndexRangeOptions{
			Prefix: prefix, Lower: candidate.lower, Upper: candidate.upper, Limit: probeLimit, Desc: candidate.reverse,
			// Mongo's compatible sort contract uses _id as the deterministic tie
			// breaker. BSON-v2 keeps missing and null physically distinct, while
			// Mongo compares them as equal, so stable grouping is required in both
			// physical directions.
			StableDocumentIDTies: candidate.sortSatisfied,
		})
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
			if candidate.residualFilters == 0 && plan.limit > 0 && len(prefixes) == 1 && len(found) == probeLimit {
				for _, id := range found {
					if _, duplicate := seenIDs[string(id)]; duplicate {
						continue
					}
					seenIDs[string(id)] = struct{}{}
					ids = append(ids, id)
					plan.recordCandidate()
				}
				continue
			}
			return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index candidate scan exceeded %d documents", errMongoFindScanCapExceeded, maxDocuments)
		}
		for _, id := range found {
			remainingCandidates--
			if _, duplicate := seenIDs[string(id)]; duplicate {
				continue
			}
			seenIDs[string(id)] = struct{}{}
			ids = append(ids, id)
			plan.recordCandidate()
		}
	}
	return ids, candidate, true, nil
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
