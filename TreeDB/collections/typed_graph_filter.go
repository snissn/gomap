package collections

import (
	"bytes"
	"context"
	"math/bits"
	"slices"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

type typedGraphFilterLimits struct {
	SourceIDs, SourceBytes, RetainedBytes, MappingWork int
	InspectedEntries                                   int
}

type typedGraphPreparedFilter struct {
	overlay                                            *typedGraphOverlaySearch
	base                                               typedcolumn.RowSelection
	delta                                              []int
	exactBaseByID                                      []int
	count                                              int
	sourceIDs, sourceBytes, retainedBytes, mappingWork int
	inspectedEntries                                   int
	scratchIDBytes, scratchRows                        int
	// Ordinal growth peak includes old and new backing arrays during copying
	// and any membership bitmap; it is not a total Go heap measurement.
	ordinalGrowthPeakBytes                            int
	borrowedBaseFilter                                *typedGraphBaseFilter
	excludedBase                                      []int
	predicateWork, predicateValueBytes, exactScanRows int
}

func prepareTypedGraphFilter(overlay *typedGraphOverlaySearch, filter HybridScalarFilter, limits typedGraphFilterLimits) (*typedGraphPreparedFilter, error) {
	return prepareTypedGraphFilterWithWork(overlay, filter, limits, nil)
}

func prepareTypedGraphFilterWithWork(overlay *typedGraphOverlaySearch, filter HybridScalarFilter, limits typedGraphFilterLimits, workOut *ColumnGraphFilterWork) (_ *typedGraphPreparedFilter, err error) {
	return prepareTypedGraphFilterWithContext(context.Background(), overlay, filter, limits, workOut)
}

func prepareTypedGraphFilterWithContext(ctx context.Context, overlay *typedGraphOverlaySearch, filter HybridScalarFilter, limits typedGraphFilterLimits, workOut *ColumnGraphFilterWork) (_ *typedGraphPreparedFilter, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var plan *typedGraphPreparedFilter
	workstats.Graph.Filters.Attempts.Add(1)
	defer func() {
		w := plan.work(err == nil)
		if workOut != nil {
			*workOut = w
		}
		workstats.Graph.Filters.Finish(err == nil)
		workstats.Graph.FilterSourceIDs.Add(w.SourceIDs)
		workstats.Graph.FilterSourceBytes.Add(w.SourceBytes)
		workstats.Graph.FilterInspectedEntries.Add(w.InspectedEntries)
		workstats.Graph.FilterMappingWorkCharged.Add(w.MappingWorkCharged)
	}()
	if !overlay.validOpen() {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	endForegroundRead := overlay.current.beginForegroundRead()
	defer endForegroundRead()
	if limits.SourceIDs <= 0 || limits.SourceBytes <= 0 || limits.RetainedBytes <= 0 || limits.MappingWork <= 0 || limits.InspectedEntries <= 0 {
		return nil, errTypedGraphSearchBudget
	}
	if err := validateHybridScalarFilter(filter); err != nil {
		return nil, err
	}
	if !overlay.baseInverseReady() {
		return nil, errTypedGraphInverseRequired
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	plan = &typedGraphPreparedFilter{overlay: overlay}
	leaves := filter.And
	if len(leaves) == 0 {
		leaves = []HybridScalarFilter{filter}
	}
	lookup := hybridScalarLookupView{context: ctx, snapshot: overlay.current.snapshot, catalog: overlay.current.catalog}
	if len(leaves) == 1 {
		return prepareTypedGraphSingleLeaf(ctx, plan, lookup, leaves[0], limits)
	}
	allowed, err := prepareTypedGraphAND(ctx, plan, lookup, leaves, limits)
	if err != nil {
		return nil, err
	}
	// Final intersection, not individual leaf cardinality, chooses exact/ANN.
	plan.count = len(allowed)
	bytesPerRow := bits.UintSize / 8
	if plan.count <= typedGraphScalarExactLimit {
		bytesPerRow *= 2
	}
	if plan.count > limits.RetainedBytes/bytesPerRow {
		return nil, errTypedGraphSearchBudget
	}
	perID := bits.Len(uint(overlay.base.reader.graph.RowCount)) + bits.Len(uint(len(overlay.rows))) + 2
	if plan.count > (limits.MappingWork-plan.mappingWork)/perID {
		return nil, errTypedGraphSearchBudget
	}
	plan.mappingWork += plan.count * perID
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ordinals := make([]int, plan.count)
	plan.retainedBytes = plan.count * (bits.UintSize / 8)
	baseCount, deltaCount := 0, 0
	// Only the bounded locator chunk owns temporary ID copies. The map's
	// cumulative string payload is bounded before copying by SourceBytes;
	// locator scratch has at most 512 rows. scratchIDBytes measures logical
	// chunk payload, not the locator's geometric arena/header allocation cost.
	ids := make([][]byte, 0, min(512, plan.count))
	var idArena []byte
	defer func() {
		plan.scratchRows = max(plan.scratchRows, len(ids))
		plan.scratchIDBytes = max(plan.scratchIDBytes, len(idArena))
	}()
	flush := func() error {
		if len(ids) == 0 {
			return nil
		}
		bytesInChunk := 0
		for _, id := range ids {
			bytesInChunk += len(id)
		}
		plan.scratchIDBytes = max(plan.scratchIDBytes, bytesInChunk)
		plan.scratchRows = max(plan.scratchRows, len(ids))
		_, err := overlay.current.visitDocumentRowRefsByID(ids, func(_ []byte, ref DocumentRowRef, found bool) error {
			if (baseCount+deltaCount)&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if !found {
				return ErrVectorIndexSnapshotMismatch
			}
			ordinal, delta, err := overlay.ordinalForCurrentRef(ref)
			if err != nil {
				return err
			}
			if delta {
				deltaCount++
				ordinals[len(ordinals)-deltaCount] = ordinal
			} else {
				ordinals[baseCount] = ordinal
				baseCount++
			}
			return nil
		})
		if err != nil {
			return err
		}
		clear(ids)
		ids = ids[:0]
		idArena = idArena[:0]
		return nil
	}
	copied := 0
	for id := range allowed {
		if copied&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		copied++
		start := len(idArena)
		idArena = append(idArena, id...)
		ids = append(ids, idArena[start:len(idArena):len(idArena)])
		if len(ids) == cap(ids) {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return finishTypedGraphFilter(ctx, plan, ordinals[:baseCount:baseCount], ordinals[len(ordinals)-deltaCount:], limits)
}

func finishTypedGraphFilter(ctx context.Context, plan *typedGraphPreparedFilter, baseOrdinals, deltaOrdinals []int, limits typedGraphFilterLimits) (*typedGraphPreparedFilter, error) {
	overlay := plan.overlay
	liveOrdinalBytes := plan.retainedBytes
	plan.ordinalGrowthPeakBytes = max(plan.ordinalGrowthPeakBytes, liveOrdinalBytes)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	slices.Sort(baseOrdinals)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var err error
	plan.base, err = typedcolumn.NewSparseRowSelectionNoCopy(overlay.base.reader.graph.RowCount, baseOrdinals)
	if err != nil {
		return nil, err
	}
	if len(deltaOrdinals) > 0 {
		plan.delta = deltaOrdinals
		slices.Sort(plan.delta)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	// RetainedBytes was checked against the worst-case ordinal arena above.
	// All/range selection with no delta drops that arena entirely.
	if len(plan.delta) == 0 && len(plan.base.SparseRows()) == 0 {
		plan.retainedBytes = 0
	}
	if plan.count > typedGraphScalarExactLimit && plan.base.Kind() == typedcolumn.RowSelectionSparse {
		plan.base, err = plan.base.WithBitmapMembership(ctx, limits.RetainedBytes-plan.retainedBytes)
		if err != nil {
			return nil, err
		}
		bitmapBytes := plan.base.Shape().BitmapWords * 8
		plan.retainedBytes += bitmapBytes
		plan.ordinalGrowthPeakBytes = max(plan.ordinalGrowthPeakBytes, liveOrdinalBytes+bitmapBytes)
	}
	if plan.count <= typedGraphScalarExactLimit {
		rankBytes := len(baseOrdinals) * (bits.UintSize / 8)
		if rankBytes > limits.RetainedBytes-plan.retainedBytes {
			return nil, errTypedGraphSearchBudget
		}
		plan.ordinalGrowthPeakBytes = max(plan.ordinalGrowthPeakBytes, liveOrdinalBytes+rankBytes)
		// Exact cutoff ties use document ID, not locality-ordered graph ordinal.
		// Read mapped IDs only during bounded preparation; query heaps compare
		// these ranks and translate back to graph ordinals for vector access.
		plan.exactBaseByID = make([]int, len(baseOrdinals))
		copy(plan.exactBaseByID, baseOrdinals)
		plan.retainedBytes += len(plan.exactBaseByID) * (bits.UintSize / 8)
		if err := sortTypedGraphExactRanksWithContext(ctx, plan); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return plan, nil
}

// One scalar leaf has unique posting IDs and needs no owning intersection map.
// Keep only a bounded ID chunk and checked ordinal capacities. Conjunctions
// use selective discovery or complete-set intersection before ordinal mapping.
func prepareTypedGraphSingleLeaf(ctx context.Context, plan *typedGraphPreparedFilter, lookup hybridScalarLookupView, leaf HybridScalarFilter, limits typedGraphFilterLimits) (*typedGraphPreparedFilter, error) {
	overlay := plan.overlay
	idx, ok := findIndex(overlay.current.catalog.meta.Indexes, leaf.IndexName)
	if !ok {
		return nil, ErrHybridSearchIndexUnavailable
	}
	if shouldDedupeIndexDocumentIDs(idx, overlay.current.catalog.meta.Options) {
		return nil, ErrHybridSearchUnsupported
	}
	perID := bits.Len(uint(overlay.base.reader.graph.RowCount)) + bits.Len(uint(len(overlay.rows))) + 2
	const word = bits.UintSize / 8
	var baseOrdinals, deltaOrdinals []int
	appendOrdinal := func(dst *[]int, ordinal int) error {
		if len(*dst) == cap(*dst) {
			remaining := (limits.RetainedBytes - plan.retainedBytes) / word
			if remaining == 0 {
				return errTypedGraphSearchBudget
			}
			// Explicit capacity avoids append's unspecified growth. Charge before
			// allocation; peak includes the still-live old buffer during copy.
			growth := min(max(64, cap(*dst)), remaining)
			newCapacity := cap(*dst) + growth
			plan.ordinalGrowthPeakBytes = max(plan.ordinalGrowthPeakBytes, plan.retainedBytes+newCapacity*word)
			grown := make([]int, len(*dst), newCapacity)
			copy(grown, *dst)
			*dst = grown
			plan.retainedBytes += growth * word
		}
		*dst = append(*dst, ordinal)
		return nil
	}
	ids := make([][]byte, 0, min(512, limits.SourceIDs))
	var arena []byte
	defer func() {
		// Admission can reject a partially copied chunk before it is flushed.
		plan.scratchRows = max(plan.scratchRows, len(ids))
		plan.scratchIDBytes = max(plan.scratchIDBytes, len(arena))
	}()
	flush := func() error {
		if len(ids) == 0 {
			return nil
		}
		plan.scratchRows = max(plan.scratchRows, len(ids))
		plan.scratchIDBytes = max(plan.scratchIDBytes, len(arena))
		_, err := overlay.current.visitDocumentRowRefsByID(ids, func(_ []byte, ref DocumentRowRef, found bool) error {
			if (len(baseOrdinals)+len(deltaOrdinals))&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if !found {
				return ErrVectorIndexSnapshotMismatch
			}
			ordinal, delta, err := overlay.ordinalForCurrentRef(ref)
			if err != nil {
				return err
			}
			if delta {
				return appendOrdinal(&deltaOrdinals, ordinal)
			}
			return appendOrdinal(&baseOrdinals, ordinal)
		})
		clear(ids)
		ids = ids[:0]
		arena = arena[:0]
		return err
	}
	var callbackErr error
	_, truncated, err := lookup.visitLeafIDs(leaf, limits.SourceIDs, limits.InspectedEntries, &plan.inspectedEntries, func(id []byte) error {
		if plan.sourceIDs&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if plan.sourceIDs == limits.SourceIDs || len(id) > limits.SourceBytes-plan.sourceBytes || perID > limits.MappingWork-plan.mappingWork {
			callbackErr = errTypedGraphSearchBudget
			return callbackErr
		}
		plan.sourceIDs++
		plan.sourceBytes += len(id)
		plan.mappingWork += perID
		start := len(arena)
		arena = append(arena, id...)
		ids = append(ids, arena[start:len(arena):len(arena)])
		if len(ids) == cap(ids) {
			callbackErr = flush()
		}
		return callbackErr
	})
	if callbackErr != nil {
		return nil, callbackErr
	}
	if truncated {
		return nil, errTypedGraphSearchBudget
	}
	if err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	plan.count = len(baseOrdinals) + len(deltaOrdinals)
	return finishTypedGraphFilter(ctx, plan, baseOrdinals, deltaOrdinals, limits)
}

func (overlay *typedGraphOverlaySearch) ordinalForCurrentRef(ref DocumentRowRef) (ordinal int, delta bool, err error) {
	i := sort.Search(len(overlay.rows), func(i int) bool { return bytes.Compare(overlay.rows[i].ID, ref.DocumentID) >= 0 })
	if i < len(overlay.rows) && bytes.Equal(overlay.rows[i].ID, ref.DocumentID) {
		row := overlay.rows[i]
		if row.Deleted || row.Generation != ref.Generation || row.PartID != ref.PartID || row.RowIndex != ref.RowIndex || row.AppliedCommandLSN != ref.AppliedCommandLSN {
			return 0, false, ErrVectorIndexSnapshotMismatch
		}
		return i, true, nil
	}
	ordinal, ok := overlay.base.reader.rowRefSource.ordinalForPhysicalRow(ref)
	if !ok {
		return 0, false, ErrVectorIndexSnapshotMismatch
	}
	return ordinal, false, nil
}

func (p *typedGraphPreparedFilter) validFor(overlay *typedGraphOverlaySearch) bool {
	return p != nil && p.overlay == overlay && overlay.validOpen() && overlay.baseInverseReady() && (p.borrowedBaseFilter == nil || p.borrowedBaseFilter.plan.validFor(p.borrowedBaseFilter.plan.overlay))
}

func (v *typedGraphOverlaySearch) baseInverseReady() bool {
	// A validated empty graph has no physical rows to invert. Never synthesize
	// a mapping or exempt a nonempty graph from its persisted inverse contract.
	return v.base.reader.graph.RowCount == 0 || v.base.reader.rowRefSource.inversePermutationActive()
}

func (p *typedGraphPreparedFilter) excludesBaseOrdinal(ordinal int) bool {
	_, found := slices.BinarySearch(p.excludedBase, ordinal)
	return found
}

func sortTypedGraphExactRanks(plan *typedGraphPreparedFilter) error {
	return sortTypedGraphExactRanksWithContext(context.Background(), plan)
}

func sortTypedGraphExactRanksWithContext(ctx context.Context, plan *typedGraphPreparedFilter) error {
	for i, ordinal := range plan.exactBaseByID {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if _, ok := plan.overlay.pack.documentIDForOrdinal(ordinal); !ok {
			return ErrVectorIndexSnapshotMismatch
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	invalidID := false
	slices.SortFunc(plan.exactBaseByID, func(a, b int) int {
		aID, aOK := plan.overlay.pack.documentIDForOrdinal(a)
		bID, bOK := plan.overlay.pack.documentIDForOrdinal(b)
		if !aOK || !bOK {
			invalidID = true
		}
		return bytes.Compare(aID, bID)
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	if invalidID {
		return ErrVectorIndexSnapshotMismatch
	}
	return nil
}
