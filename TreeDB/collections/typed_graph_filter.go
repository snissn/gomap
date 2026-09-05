package collections

import (
	"bytes"
	"math/bits"
	"slices"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
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
	// Ordinal growth peak includes old and new backing arrays during copying;
	// it is separate from retained capacity, not a total Go heap measurement.
	ordinalGrowthPeakBytes int
}

func prepareTypedGraphFilter(overlay *typedGraphOverlaySearch, filter HybridScalarFilter, limits typedGraphFilterLimits) (*typedGraphPreparedFilter, error) {
	if overlay == nil || overlay.base == nil || overlay.base.closed || overlay.current == nil || overlay.current.closed {
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
	inverse := overlay.base.reader.rowRefSource
	if !inverse.inversePermutationActive() {
		return nil, errTypedGraphInverseRequired
	}
	plan := &typedGraphPreparedFilter{overlay: overlay}
	leaves := filter.And
	if len(leaves) == 0 {
		leaves = []HybridScalarFilter{filter}
	}
	lookup := hybridScalarLookupView{snapshot: overlay.current.snapshot, catalog: overlay.current.catalog}
	if len(leaves) == 1 {
		return prepareTypedGraphSingleLeaf(plan, lookup, leaves[0], limits)
	}
	var allowed hybridScalarAllowSet
	for leafIndex, leaf := range leaves {
		idx, ok := findIndex(overlay.current.catalog.meta.Indexes, leaf.IndexName)
		if !ok {
			return nil, ErrHybridSearchIndexUnavailable
		}
		// Array/multikey dedupe owns IDs before the visitor. This internal typed
		// scalar seam deliberately rejects that different admission contract.
		if shouldDedupeIndexDocumentIDs(idx, overlay.current.catalog.meta.Options) {
			return nil, ErrHybridSearchUnsupported
		}
		if plan.inspectedEntries == limits.InspectedEntries {
			return nil, errTypedGraphSearchBudget
		}
		exhausted := false
		inspected := 0
		set, _, truncated, err := lookup.leafProbeBeforeCopy(leaf, limits.SourceIDs, limits.InspectedEntries-plan.inspectedEntries, &inspected, func(id []byte) error {
			if plan.sourceIDs == limits.SourceIDs || len(id) > limits.SourceBytes-plan.sourceBytes {
				exhausted = true
				return errTypedGraphSearchBudget
			}
			plan.sourceIDs++
			plan.sourceBytes += len(id)
			return nil
		})
		plan.inspectedEntries += inspected
		if exhausted || truncated {
			return nil, errTypedGraphSearchBudget
		}
		if err != nil {
			return nil, err
		}
		if leafIndex == 0 {
			allowed = set
		} else {
			for id := range allowed {
				if _, ok := set[id]; !ok {
					delete(allowed, id)
				}
			}
		}
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
	plan.retainedBytes = plan.count * (bits.UintSize / 8)
	perID := bits.Len(uint(inverse.rows)) + bits.Len(uint(len(overlay.rows))) + 2
	if plan.count > limits.MappingWork/perID {
		return nil, errTypedGraphSearchBudget
	}
	plan.mappingWork = plan.count * perID
	ordinals := make([]int, plan.count)
	baseCount, deltaCount := 0, 0
	// Only the bounded locator chunk owns temporary ID copies. The map's
	// cumulative string payload is bounded before copying by SourceBytes;
	// locator scratch has at most 512 rows. scratchIDBytes measures logical
	// chunk payload, not the locator's geometric arena/header allocation cost.
	ids := make([][]byte, 0, min(512, plan.count))
	var idArena []byte
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
	for id := range allowed {
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
	return finishTypedGraphFilter(plan, ordinals[:baseCount:baseCount], ordinals[len(ordinals)-deltaCount:], limits)
}

func finishTypedGraphFilter(plan *typedGraphPreparedFilter, baseOrdinals, deltaOrdinals []int, limits typedGraphFilterLimits) (*typedGraphPreparedFilter, error) {
	overlay := plan.overlay
	liveOrdinalBytes := plan.retainedBytes
	plan.ordinalGrowthPeakBytes = max(plan.ordinalGrowthPeakBytes, liveOrdinalBytes)
	slices.Sort(baseOrdinals)
	var err error
	plan.base, err = typedcolumn.NewSparseRowSelectionNoCopy(overlay.base.reader.rowRefSource.rows, baseOrdinals)
	if err != nil {
		return nil, err
	}
	if len(deltaOrdinals) > 0 {
		plan.delta = deltaOrdinals
		slices.Sort(plan.delta)
	}
	// RetainedBytes was checked against the worst-case ordinal arena above.
	// All/range selection with no delta drops that arena entirely.
	if len(plan.delta) == 0 && len(plan.base.SparseRows()) == 0 {
		plan.retainedBytes = 0
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
		for _, ordinal := range plan.exactBaseByID {
			if _, ok := overlay.pack.documentIDForOrdinal(ordinal); !ok {
				return nil, ErrVectorIndexSnapshotMismatch
			}
		}
		invalidID := false
		slices.SortFunc(plan.exactBaseByID, func(a, b int) int {
			aID, aOK := overlay.pack.documentIDForOrdinal(a)
			bID, bOK := overlay.pack.documentIDForOrdinal(b)
			if !aOK || !bOK {
				invalidID = true
			}
			return bytes.Compare(aID, bID)
		})
		if invalidID {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		plan.retainedBytes += len(plan.exactBaseByID) * (bits.UintSize / 8)
	}
	return plan, nil
}

// One scalar leaf has unique posting IDs and needs no owning intersection map.
// Keep only a bounded ID chunk and checked ordinal capacities. Conjunctions
// deliberately retain the existing complete-set intersection path above.
func prepareTypedGraphSingleLeaf(plan *typedGraphPreparedFilter, lookup hybridScalarLookupView, leaf HybridScalarFilter, limits typedGraphFilterLimits) (*typedGraphPreparedFilter, error) {
	overlay := plan.overlay
	idx, ok := findIndex(overlay.current.catalog.meta.Indexes, leaf.IndexName)
	if !ok {
		return nil, ErrHybridSearchIndexUnavailable
	}
	if shouldDedupeIndexDocumentIDs(idx, overlay.current.catalog.meta.Options) {
		return nil, ErrHybridSearchUnsupported
	}
	inverse := overlay.base.reader.rowRefSource
	perID := bits.Len(uint(inverse.rows)) + bits.Len(uint(len(overlay.rows))) + 2
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
	flush := func() error {
		if len(ids) == 0 {
			return nil
		}
		plan.scratchRows = max(plan.scratchRows, len(ids))
		plan.scratchIDBytes = max(plan.scratchIDBytes, len(arena))
		_, err := overlay.current.visitDocumentRowRefsByID(ids, func(_ []byte, ref DocumentRowRef, found bool) error {
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
	return finishTypedGraphFilter(plan, baseOrdinals, deltaOrdinals, limits)
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
	return p != nil && p.overlay == overlay && overlay != nil && overlay.base != nil && !overlay.base.closed && overlay.current != nil && !overlay.current.closed && overlay.base.reader.rowRefSource.inversePermutationActive()
}
