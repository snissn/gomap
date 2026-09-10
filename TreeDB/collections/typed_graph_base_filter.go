package collections

import (
	"context"
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
	// Only the keeper installs detached plans. Its existing read lock protects
	// these immutable fields through scoring; each query supplies its own pin.
	holder     *columnVectorGraphSharedPreparedSearch
	schemaHash uint64
}

type typedGraphScalarPredicate struct {
	definition IndexDefinition
	column     int
	clause     nativeScalarClause
}

func prepareTypedGraphBaseFilter(base *VectorIndexSearcher, filter HybridScalarFilter, limits typedGraphBaseFilterLimits) (*typedGraphBaseFilter, error) {
	result, err := compileTypedGraphBaseFilter(base, filter, limits)
	if err != nil {
		return nil, err
	}
	err = result.prepare(context.Background(), base, filter, limits.typedGraphFilterLimits, nil)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func compileTypedGraphBaseFilter(base *VectorIndexSearcher, filter HybridScalarFilter, limits typedGraphBaseFilterLimits) (*typedGraphBaseFilter, error) {
	if base == nil || base.closed || base.snapshot == nil || base.catalog == nil || base.reader == nil || base.collection == nil || base.collection.db == nil || base.collection.db.IsClosing() {
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
	return result, nil
}

func (result *typedGraphBaseFilter) prepare(ctx context.Context, base *VectorIndexSearcher, filter HybridScalarFilter, limits typedGraphFilterLimits, workOut *ColumnGraphFilterWork) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Searchers are single-owner. Reuse their existing lazy materializer and
	// immutable pin; this does not introduce concurrent bind/Close semantics.
	if base.documentView == nil {
		base.documentView = newCollectionReadViewAtSnapshot(base.collection, base.snapshot, base.catalog, false, mappedresource.ScopePreparedSearch)
	}
	overlay, err := prepareTypedGraphOverlaySearch(base, base.documentView, typedGraphOverlayLimits{Rows: 1, Tombstones: 1, Bytes: 1})
	if err != nil {
		return err
	}
	if len(overlay.rows) != 0 {
		return ErrVectorIndexSnapshotMismatch
	}
	if workOut == nil {
		result.plan, err = prepareTypedGraphFilterWithContext(ctx, overlay, filter, limits, nil)
	} else {
		result.plan, err = prepareTypedGraphFilterUnmetered(ctx, overlay, filter, limits, workOut)
	}
	return err
}

func (b *typedGraphBaseFilter) validFor(overlay *typedGraphOverlaySearch) bool {
	if b == nil || b.plan == nil || !overlay.validOpen() {
		return false
	}
	if b.holder == nil {
		return b.plan.validFor(b.plan.overlay) && overlay.base == b.plan.overlay.base
	}
	ref := overlay.base.reader.sharedPreparedSearch
	cfg := overlay.base.catalog.meta.Options.ColumnStore
	return ref != nil && ref.holder == b.holder && cfg != nil && cfg.SchemaHash == b.schemaHash
}

func bindTypedGraphBaseFilter(base *typedGraphBaseFilter, overlay *typedGraphOverlaySearch, limits typedGraphFilterBindLimits) (*typedGraphPreparedFilter, error) {
	return bindTypedGraphBaseFilterWithContext(context.Background(), base, overlay, limits, nil)
}

func bindTypedGraphBaseFilterWithContext(ctx context.Context, base *typedGraphBaseFilter, overlay *typedGraphOverlaySearch, limits typedGraphFilterBindLimits, workOut *ColumnGraphFilterWork) (_ *typedGraphPreparedFilter, err error) {
	var plan *typedGraphPreparedFilter
	defer func() {
		if workOut != nil {
			*workOut = plan.work(err == nil)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !base.validFor(overlay) {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	// Remaining cold-plus-bind allowances may be exactly zero with no suffix.
	if limits.Rows < 0 || limits.IDBytes < 0 || limits.ValueBytes < 0 || limits.MappingWork < 0 || limits.PredicateWork < 0 || limits.RetainedBytes < 0 || limits.ExactScanRows <= 0 || len(overlay.rows) > limits.Rows {
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
	plan = &typedGraphPreparedFilter{overlay: overlay, base: base.plan.base, borrowedBaseFilter: base, retainedBytes: 2 * d * word, ordinalGrowthPeakBytes: 2 * d * word}
	if d == 0 {
		plan.count = plan.base.Count()
		plan.exactBaseByID = base.plan.exactBaseByID
		return plan, nil
	}
	storage := make([]int, 2*d)
	excluded, matched := storage[:0:d], storage[d:d:2*d]
	if overlay.base.documentView == nil {
		overlay.base.documentView = newCollectionReadViewAtSnapshot(overlay.base.collection, overlay.base.snapshot, overlay.base.catalog, false, mappedresource.ScopePreparedSearch)
	}
	view := overlay.base.documentView
	end := view.beginForegroundRead()
	defer end()
	ids := make([][]byte, 0, min(512, d))
	// Include inverse lookup, selection membership and bounded exclusion-sort
	// comparisons. Exact enumeration is separately counted below.
	perID := bits.Len(uint(overlay.base.reader.graph.RowCount)) + bits.Len(uint(plan.base.Count())) + bits.Len(uint(d)) + 2
	for start := 0; start < d; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ids = ids[:0]
		bytesInChunk := 0
		for start < d && len(ids) < cap(ids) {
			id := overlay.rows[start].ID
			if len(id) > limits.IDBytes-plan.sourceBytes || perID > limits.MappingWork-plan.mappingWork {
				return nil, errTypedGraphSearchBudget
			}
			plan.sourceIDs++
			plan.sourceBytes += len(id)
			bytesInChunk += len(id)
			plan.mappingWork += perID
			ids = append(ids, id)
			start++
		}
		plan.scratchRows = max(plan.scratchRows, len(ids))
		plan.scratchIDBytes = max(plan.scratchIDBytes, bytesInChunk)
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
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
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
			remainingMapping := limits.MappingWork - plan.mappingWork
			if remainingMapping < 3 || len(value.String) > (remainingMapping-3)/2 {
				return nil, errTypedGraphSearchBudget
			}
			scratch = appendIndexStringComponent(scratch[:0], []byte(value.String))
			plan.predicateValueBytes += len(scratch)
			plan.mappingWork += 1 + len(scratch)
			if !predicate.clause.matches(scratch, true) {
				matches = false
				break
			}
		}
		if matches {
			matched = append(matched, i)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	slices.Sort(excluded)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	if plan.count <= typedGraphScalarExactLimit && len(excluded) == 0 && base.plan.count <= typedGraphScalarExactLimit {
		plan.exactBaseByID = base.plan.exactBaseByID
	} else if plan.count <= typedGraphScalarExactLimit {
		if plan.base.Count() > limits.MappingWork-plan.mappingWork || plan.base.Count() > limits.ExactScanRows || plan.base.Count()-len(excluded) > typedGraphScalarExactLimit {
			return nil, errTypedGraphSearchBudget
		}
		plan.exactScanRows = plan.base.Count()
		plan.mappingWork += plan.exactScanRows
		survivors := plan.base.Count() - len(excluded)
		if survivors > (limits.RetainedBytes-plan.retainedBytes)/word {
			return nil, errTypedGraphSearchBudget
		}
		ordinals := make([]int, 0, survivors)
		plan.retainedBytes += cap(ordinals) * word
		plan.ordinalGrowthPeakBytes += cap(ordinals) * word
		for position := 0; position < plan.base.Count(); position++ {
			if position&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
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
		if err := sortTypedGraphExactRanksWithContext(ctx, plan); err != nil {
			return nil, err
		}
	}
	return plan, nil
}
