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
			i := sort.Search(len(overlay.rows), func(i int) bool { return bytes.Compare(overlay.rows[i].ID, ref.DocumentID) >= 0 })
			if i < len(overlay.rows) && bytes.Equal(overlay.rows[i].ID, ref.DocumentID) {
				row := overlay.rows[i]
				if row.Deleted || row.Generation != ref.Generation || row.PartID != ref.PartID || row.RowIndex != ref.RowIndex || row.AppliedCommandLSN != ref.AppliedCommandLSN {
					return ErrVectorIndexSnapshotMismatch
				}
				deltaCount++
				ordinals[len(ordinals)-deltaCount] = i
			} else {
				ordinal, ok := inverse.ordinalForPhysicalRow(ref)
				if !ok {
					return ErrVectorIndexSnapshotMismatch
				}
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
	slices.Sort(ordinals[:baseCount])
	var err error
	plan.base, err = typedcolumn.NewSparseRowSelectionNoCopy(inverse.rows, ordinals[:baseCount:baseCount])
	if err != nil {
		return nil, err
	}
	if deltaCount > 0 {
		plan.delta = ordinals[len(ordinals)-deltaCount:]
		slices.Sort(plan.delta)
	}
	// RetainedBytes was checked against the worst-case ordinal arena above.
	// All/range selection with no delta drops that arena entirely.
	if len(plan.delta) == 0 && len(plan.base.SparseRows()) == 0 {
		plan.retainedBytes = 0
	}
	if plan.count <= typedGraphScalarExactLimit {
		// Exact cutoff ties use document ID, not locality-ordered graph ordinal.
		// Read mapped IDs only during bounded preparation; query heaps compare
		// these ranks and translate back to graph ordinals for vector access.
		plan.exactBaseByID = slices.Clone(ordinals[:baseCount])
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

func (p *typedGraphPreparedFilter) validFor(overlay *typedGraphOverlaySearch) bool {
	return p != nil && p.overlay == overlay && overlay != nil && overlay.base != nil && !overlay.base.closed && overlay.current != nil && !overlay.current.closed && overlay.base.reader.rowRefSource.inversePermutationActive()
}
