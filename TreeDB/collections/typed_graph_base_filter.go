package collections

import (
	"math/bits"
	"reflect"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

type typedGraphBaseFilterLimits struct {
	typedGraphFilterLimits
	Clauses, PredicateBytes int
}

type typedGraphFilterBindLimits struct {
	Rows, IDBytes, ValueBytes, MappingWork, PredicateWork int
	RetainedBytes, ExactScanRows                          int
}

type typedGraphBaseFilter struct {
	plan           *typedGraphPreparedFilter
	predicates     []typedGraphScalarPredicate
	predicateBytes int
}

type typedGraphScalarPredicate struct {
	definition IndexDefinition
	column     int
	clause     nativeScalarClause
}

func prepareTypedGraphBaseFilter(base *VectorIndexSearcher, filter HybridScalarFilter, limits typedGraphBaseFilterLimits) (*typedGraphBaseFilter, error) {
	if base == nil || base.closed || base.snapshot == nil || base.catalog == nil || base.reader == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	if limits.Clauses <= 0 || limits.PredicateBytes <= 0 {
		return nil, errTypedGraphSearchBudget
	}
	if err := validateHybridScalarFilter(filter); err != nil {
		return nil, err
	}
	leaves := filter.And
	if len(leaves) == 0 {
		leaves = []HybridScalarFilter{filter}
	}
	if len(leaves) > limits.Clauses {
		return nil, errTypedGraphSearchBudget
	}
	cfg := base.catalog.meta.Options.ColumnStore
	if cfg == nil {
		return nil, ErrHybridSearchUnsupported
	}
	result := &typedGraphBaseFilter{predicates: make([]typedGraphScalarPredicate, 0, len(leaves))}
	// Charge the exact escaped string bytes before the existing compiler owns
	// them. Equalities own both bounds. No caller range/value slices survive.
	charge := func(value any) error {
		s, ok := value.(string)
		if !ok {
			return ErrHybridSearchUnsupported
		}
		remaining := limits.PredicateBytes - result.predicateBytes
		if remaining < 2 || len(s) > remaining-2 {
			return errTypedGraphSearchBudget
		}
		n := len(s) + 2
		for i := range len(s) {
			if s[i] == 0 {
				if n == remaining {
					return errTypedGraphSearchBudget
				}
				n++
			}
		}
		result.predicateBytes += n
		return nil
	}
	for _, leaf := range leaves {
		def, ok := findIndex(base.catalog.meta.Indexes, leaf.IndexName)
		if !ok {
			return nil, ErrHybridSearchIndexUnavailable
		}
		column := typedStringColumnIndex(cfg.Columns, def.Field)
		if def.ValueType != IndexValueString || len(def.Components) != 0 || shouldDedupeIndexDocumentIDs(def, base.catalog.meta.Options) || column < 0 || cfg.Columns[column].Nullable {
			return nil, ErrHybridSearchUnsupported
		}
		if leaf.Range == nil {
			if err := charge(leaf.Value); err != nil {
				return nil, err
			}
			if err := charge(leaf.Value); err != nil {
				return nil, err
			}
		} else {
			if !leaf.Range.Lower.Unbounded {
				if err := charge(leaf.Range.Lower.Value); err != nil {
					return nil, err
				}
			}
			if !leaf.Range.Upper.Unbounded {
				if err := charge(leaf.Range.Upper.Value); err != nil {
					return nil, err
				}
			}
		}
		clause, err := compileNativeScalarClause(def, leaf)
		if err != nil {
			return nil, err
		}
		result.predicates = append(result.predicates, typedGraphScalarPredicate{definition: def, column: column, clause: clause})
	}
	// Searchers are single-owner. Reuse their existing lazy materializer and
	// immutable pin; this does not introduce concurrent bind/Close semantics.
	if base.documentView == nil {
		base.documentView = newCollectionReadViewAtSnapshot(base.collection, base.snapshot, base.catalog, false, mappedresource.ScopePreparedSearch)
	}
	overlay, err := prepareTypedGraphOverlaySearch(base, base.documentView, typedGraphOverlayLimits{Rows: 1, Tombstones: 1, Bytes: 1})
	if err != nil {
		return nil, err
	}
	if len(overlay.rows) != 0 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	result.plan, err = prepareTypedGraphFilter(overlay, filter, limits.typedGraphFilterLimits)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func bindTypedGraphBaseFilter(base *typedGraphBaseFilter, overlay *typedGraphOverlaySearch, limits typedGraphFilterBindLimits) (*typedGraphPreparedFilter, error) {
	if base == nil || base.plan == nil || !base.plan.validFor(base.plan.overlay) || overlay == nil || overlay.base != base.plan.overlay.base || overlay.current == nil || overlay.current.closed {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	if limits.Rows <= 0 || limits.IDBytes <= 0 || limits.ValueBytes <= 0 || limits.MappingWork <= 0 || limits.PredicateWork <= 0 || limits.RetainedBytes <= 0 || limits.ExactScanRows <= 0 || len(overlay.rows) > limits.Rows {
		return nil, errTypedGraphSearchBudget
	}
	for _, predicate := range base.predicates {
		def, ok := findIndex(overlay.current.catalog.meta.Indexes, predicate.definition.Name)
		if !ok || !reflect.DeepEqual(def, predicate.definition) || shouldDedupeIndexDocumentIDs(def, overlay.current.catalog.meta.Options) {
			return nil, ErrVectorIndexSnapshotMismatch
		}
	}
	const word = bits.UintSize / 8
	d := len(overlay.rows)
	if d > limits.RetainedBytes/(2*word) {
		return nil, errTypedGraphSearchBudget
	}
	plan := &typedGraphPreparedFilter{overlay: overlay, base: base.plan.base, borrowedBaseFilter: base, retainedBytes: 2 * d * word}
	storage := make([]int, 2*d)
	excluded, matched := storage[:0:d], storage[d:d:2*d]
	view := base.plan.overlay.current
	end := view.beginForegroundRead()
	defer end()
	ids := make([][]byte, 0, min(512, d))
	// Include inverse lookup, selection membership and bounded exclusion-sort
	// comparisons. Exact enumeration is separately counted below.
	perID := bits.Len(uint(overlay.base.reader.rowRefSource.rows)) + bits.Len(uint(plan.base.Count())) + bits.Len(uint(d)) + 2
	for start := 0; start < d; {
		ids = ids[:0]
		for start < d && len(ids) < cap(ids) {
			id := overlay.rows[start].ID
			if len(id) > limits.IDBytes-plan.sourceBytes || perID > limits.MappingWork-plan.mappingWork {
				return nil, errTypedGraphSearchBudget
			}
			plan.sourceIDs++
			plan.sourceBytes += len(id)
			plan.mappingWork += perID
			ids = append(ids, id)
			start++
		}
		_, err := view.visitDocumentRowRefsByID(ids, func(_ []byte, ref DocumentRowRef, found bool) error {
			if !found {
				return nil
			}
			ordinal, ok := overlay.base.reader.rowRefSource.ordinalForPhysicalRow(ref)
			if !ok {
				return ErrVectorIndexSnapshotMismatch
			}
			if plan.base.Contains(ordinal) {
				excluded = append(excluded, ordinal)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	var scratch []byte
	for i, row := range overlay.rows {
		if row.Deleted {
			continue
		}
		matches := true
		for _, predicate := range base.predicates {
			if plan.predicateWork == limits.PredicateWork {
				return nil, errTypedGraphSearchBudget
			}
			plan.predicateWork++
			if predicate.column >= len(row.Values) {
				return nil, ErrVectorIndexSnapshotMismatch
			}
			value := row.Values[predicate.column]
			if !value.Present || value.Null || value.Type != ColumnStoreValueString {
				return nil, ErrVectorIndexSnapshotMismatch
			}
			// The checked overlay owns normalized string values. Worst-case
			// escaped length is charged before encoder scratch can grow.
			remaining := limits.ValueBytes - plan.predicateValueBytes
			if remaining < 2 || len(value.String) > (remaining-2)/2 {
				return nil, errTypedGraphSearchBudget
			}
			scratch = appendIndexStringComponent(scratch[:0], []byte(value.String))
			plan.predicateValueBytes += len(scratch)
			if !predicate.clause.matches(scratch, true) {
				matches = false
				break
			}
		}
		if matches {
			matched = append(matched, i)
		}
	}
	slices.Sort(excluded)
	if len(excluded) > 0 {
		plan.excludedBase = excluded
	}
	if len(matched) > 0 {
		plan.delta = matched
	}
	plan.count = plan.base.Count() - len(excluded) + len(matched)
	if len(excluded) == 0 && len(matched) == 0 {
		plan.retainedBytes = 0
	}
	plan.ordinalGrowthPeakBytes = 2 * d * word
	if plan.count <= typedGraphScalarExactLimit {
		if plan.base.Count() > limits.ExactScanRows || plan.base.Count()-len(excluded) > typedGraphScalarExactLimit {
			return nil, errTypedGraphSearchBudget
		}
		plan.exactScanRows = plan.base.Count()
		survivors := plan.base.Count() - len(excluded)
		if survivors > (limits.RetainedBytes-plan.retainedBytes)/word {
			return nil, errTypedGraphSearchBudget
		}
		ordinals := make([]int, 0, survivors)
		for position := 0; position < plan.base.Count(); position++ {
			ordinal, ok := typedGraphFilterOrdinalAt(plan.base, position)
			if !ok {
				return nil, ErrVectorIndexSnapshotMismatch
			}
			if !plan.excludesBaseOrdinal(ordinal) {
				ordinals = append(ordinals, ordinal)
			}
		}
		// This owned survivor slice becomes the exact rank itself, no clone.
		plan.exactBaseByID = ordinals
		plan.retainedBytes += cap(ordinals) * word
		plan.ordinalGrowthPeakBytes += cap(ordinals) * word
		if err := sortTypedGraphExactRanks(plan); err != nil {
			return nil, err
		}
	}
	return plan, nil
}
