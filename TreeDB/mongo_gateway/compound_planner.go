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
		if eqOK {
			// `$in` is a supported query predicate, but a multi-value equality
			// prefix needs a k-way ordered merge.  Until that bounded merge is
			// available, retain it as a residual rather than returning a partial
			// index order.
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
			for i, pred := range plan.predicates {
				if pred.field == components[component].Field && isRangePredicate(pred.op) {
					used[i] = struct{}{}
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
			key := string(append([]byte{byte(value.Type)}, value.Value...))
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
	if _, ok := primaryCandidatePredicate(plan.predicates); ok {
		return nil, compoundIndexPlan{}, false, nil
	}
	candidates := compoundIndexPlans(col.MetaView(), plan)
	if len(candidates) == 0 {
		return nil, compoundIndexPlan{}, false, nil
	}
	candidate := candidates[0]
	maxDocuments := s.maxFindScanDocuments()
	if maxDocuments <= 0 {
		return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index planner requires a positive work cap", errMongoFindScanCapExceeded)
	}
	prefixes := compoundPrefixes(candidate.prefixChoices)
	if len(prefixes) > maxDocuments {
		return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index planner has %d prefix probes for a %d-document work cap", errMongoFindScanCapExceeded, len(prefixes), maxDocuments)
	}
	// A direct range invocation has a physical cap of Limit*64. Splitting the
	// document budget across all prefix probes makes that cap global for this
	// query rather than allowing each $in branch to consume a fresh budget.
	perPrefixLimit := maxDocuments / len(prefixes)
	if perPrefixLimit == 0 {
		perPrefixLimit = 1
	}
	ids := make([][]byte, 0, maxDocuments)
	seenIDs := make(map[string]struct{})
	for _, prefix := range prefixes {
		found, truncated, err := col.FindByCompoundIndexRange(candidate.idx.Name, collections.CompoundIndexRangeOptions{
			Prefix: prefix, Lower: candidate.lower, Upper: candidate.upper, Limit: perPrefixLimit, Desc: candidate.reverse,
		})
		if err != nil {
			return nil, candidate, true, err
		}
		if truncated {
			return nil, candidate, true, fmt.Errorf("%w: Mongo gateway compound index candidate scan exceeded %d documents", errMongoFindScanCapExceeded, maxDocuments)
		}
		for _, id := range found {
			if _, duplicate := seenIDs[string(id)]; duplicate {
				continue
			}
			seenIDs[string(id)] = struct{}{}
			ids = append(ids, id)
			plan.recordCandidate()
		}
	}
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

func compoundIndexPlanFor(meta collections.CollectionMeta, plan findPlan) (compoundIndexPlan, bool) {
	plans := compoundIndexPlans(meta, plan)
	if len(plans) == 0 {
		return compoundIndexPlan{}, false
	}
	return plans[0], true
}

func isCompoundPlanCapError(err error) bool { return errors.Is(err, errMongoFindScanCapExceeded) }
