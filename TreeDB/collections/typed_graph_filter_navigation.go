package collections

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
)

// ponytail: bound cold per-filter construction above the frozen 200K-row shape;
// raise only if a larger selective workload fits the retained-byte budget.
const typedGraphFilterNavigationMaxRows = 1 << 18

var errTypedGraphFilterNavigationDeclined = errors.New("collections: typed graph filter navigation declined")

// typedGraphFilterNavigation is derived, process-local navigation. The base
// pack remains the sole vector and document-ID owner.
type typedGraphFilterNavigation struct {
	view          columnHNSWSearchPackPreparedView
	baseOrdinals  []uint32
	retainedBytes int
}

func buildTypedGraphFilterNavigation(ctx context.Context, overlay *typedGraphOverlaySearch, plan *typedGraphPreparedFilter, maxBytes int) (*typedGraphFilterNavigation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if plan == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	selection := plan.base
	count := selection.Count()
	if overlay == nil || !overlay.validOpen() || overlay.pack == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	if count <= typedGraphScalarExactLimit || count > typedGraphFilterNavigationMaxRows || selection.IsAll() || maxBytes <= 0 {
		return nil, errTypedGraphFilterNavigationDeclined
	}
	levelIndex, err := newVectorIndex(nil, vectorIndexOptionsFromDefinition(overlay.base.reader.def))
	if err != nil {
		return nil, err
	}
	remaining := uint64(maxBytes)
	charge := func(items, bytes uint64) bool {
		if items != 0 && bytes > remaining/items {
			return false
		}
		remaining -= items * bytes
		return true
	}
	if !charge(1, uint64(reflect.TypeFor[typedGraphFilterNavigation]().Size())) || !charge(uint64(count), 6) {
		return nil, errTypedGraphFilterNavigationDeclined
	}
	maxLevel := 0
	for i := 0; i < count; i++ {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		ordinal, ok := typedGraphFilterOrdinalAt(selection, i)
		if !ok {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		id, ok := overlay.pack.documentIDForOrdinal(ordinal)
		if !ok {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		level := levelIndex.levelForDocumentID(id)
		maxLevel = max(maxLevel, level)
		neighbors := levelIndex.maxNeighborsForLayer(0) + level*levelIndex.maxNeighborsForLayer(1)
		if !charge(uint64(neighbors), 4) {
			return nil, errTypedGraphFilterNavigationDeclined
		}
	}
	layersCount := uint64(maxLevel + 1)
	if !charge(layersCount, uint64(reflect.TypeFor[columnHNSWSearchPackPreparedLayer]().Size())) || !charge(layersCount*uint64(count+1), 8) {
		return nil, errTypedGraphFilterNavigationDeclined
	}
	rows := make([]columnVectorGraphAssetRow, count)
	for i := range rows {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		ordinal, ok := typedGraphFilterOrdinalAt(selection, i)
		if !ok || uint64(ordinal) > math.MaxUint32 {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		id, ok := overlay.pack.documentIDForOrdinal(ordinal)
		if !ok {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		vector, _, _, ok := overlay.base.reader.typedVectorSource.vectorForOrdinal(ordinal)
		if !ok {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		invNorm, _, _, ok := overlay.base.reader.invNormForOrdinal(ordinal)
		if !ok {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		rows[i] = columnVectorGraphAssetRow{ID: id, Vector: vector, InvNorm: invNorm, BaseRowRef: DocumentRowRef{RowIndex: ordinal}}
	}
	// Base packs are locality-remapped after construction. Restore the primary
	// document-ID order used by rebuild before deriving another HNSW graph.
	if err := sortVectorPartitionSliceWithContextV1(ctx, rows, func(a, b columnVectorGraphAssetRow) bool { return bytes.Compare(a.ID, b.ID) < 0 }); err != nil {
		return nil, err
	}
	if err := buildColumnVectorGraphAdjacencyWithContext(ctx, rows, overlay.base.reader.def); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	levels, layers, maxLayer, err := buildColumnHNSWSearchPackLevelsAndAdjacencyWithContext(ctx, rows)
	if err != nil {
		return nil, err
	}
	baseOrdinals := make([]uint32, count)
	preparedLayers := make([]columnHNSWSearchPackPreparedLayer, len(layers))
	retained := int(reflect.TypeFor[typedGraphFilterNavigation]().Size()) + cap(baseOrdinals)*4 + cap(levels)*2 + cap(preparedLayers)*int(reflect.TypeFor[columnHNSWSearchPackPreparedLayer]().Size())
	for i := range rows {
		ordinal := rows[i].BaseRowRef.RowIndex
		if ordinal < 0 || uint64(ordinal) > math.MaxUint32 {
			return nil, ErrVectorIndexSnapshotMismatch
		}
		baseOrdinals[i] = uint32(ordinal)
	}
	for i := range layers {
		preparedLayers[i] = columnHNSWSearchPackPreparedLayer{Offsets: layers[i].Offsets, Neighbors: layers[i].Neighbors}
		retained += cap(layers[i].Offsets)*8 + cap(layers[i].Neighbors)*4
	}
	if retained > maxBytes {
		return nil, errTypedGraphFilterNavigationDeclined
	}
	return &typedGraphFilterNavigation{
		view: columnHNSWSearchPackPreparedView{
			Header: columnHNSWSearchPackHeader{
				Rows: count, Dimensions: overlay.pack.Header.Dimensions, VectorStride: overlay.pack.Header.VectorStride,
				M: overlay.pack.Header.M, EfConstruction: overlay.pack.Header.EfConstruction, EfSearch: overlay.pack.Header.EfSearch,
				EntryOrdinal: 0, MaxLayer: maxLayer, AdjacencyLayerCount: len(preparedLayers),
			},
			Levels: levels, AdjacencyLayers: preparedLayers, status: columnHNSWSearchPackPreparedStatusHeap, ephemeralHeap: true,
		},
		baseOrdinals: baseOrdinals, retainedBytes: retained,
	}, nil
}

type typedGraphFilterNavigationScorePlane struct {
	ctx      context.Context
	base     columnHNSWPreparedExactFP32ScorePlane
	ordinals []uint32
	limit    uint64
}

func (p *typedGraphFilterNavigationScorePlane) kind() columnHNSWPreparedTraversalScorePlaneKind {
	return columnHNSWPreparedTraversalScorePlaneKindExactFP32
}

func (p *typedGraphFilterNavigationScorePlane) prepareForHNSWPreparedTraversal(_ *columnHNSWSearchPackPreparedView, query []float32, opts columnHNSWPreparedTraversalOptions, scratch *columnVectorGraphNativeSearchScratch) error {
	if err := p.ctx.Err(); err != nil {
		return err
	}
	return p.base.prepareForHNSWPreparedTraversal(p.base.pack, query, opts, scratch)
}

func (p *typedGraphFilterNavigationScorePlane) scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if err := p.ctx.Err(); err != nil {
		return 0, err
	}
	if stats.PreparedScoreCalls >= p.limit {
		return 0, errTypedGraphSearchBudget
	}
	if ordinal < 0 || ordinal >= len(p.ordinals) {
		return 0, ErrVectorIndexSnapshotMismatch
	}
	return p.base.scoreOrdinal(int(p.ordinals[ordinal]), scratch, stats)
}

func (p *typedGraphFilterNavigationScorePlane) scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if err := p.ctx.Err(); err != nil {
		return dst[:0], err
	}
	if stats.PreparedScoreCalls > p.limit || uint64(len(ordinals)) > p.limit-stats.PreparedScoreCalls {
		return dst[:0], errTypedGraphSearchBudget
	}
	rows := p.mapOrdinals(scratch, len(ordinals))
	for i, ordinal := range ordinals {
		if ordinal < 0 || ordinal >= len(p.ordinals) {
			return dst[:0], ErrVectorIndexSnapshotMismatch
		}
		rows[i] = p.ordinals[ordinal]
	}
	return p.base.scoreRowIDsPrevalidated(rows, dst, scratch, stats)
}

func (p *typedGraphFilterNavigationScorePlane) scoreRowIDsPrevalidated(rowIDs []uint32, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
	if err := p.ctx.Err(); err != nil {
		return dst[:0], err
	}
	if stats.PreparedScoreCalls > p.limit || uint64(len(rowIDs)) > p.limit-stats.PreparedScoreCalls {
		return dst[:0], errTypedGraphSearchBudget
	}
	rows := p.mapOrdinals(scratch, len(rowIDs))
	for i, rowID := range rowIDs {
		if uint64(rowID) >= uint64(len(p.ordinals)) {
			return dst[:0], ErrVectorIndexSnapshotMismatch
		}
		rows[i] = p.ordinals[rowID]
	}
	return p.base.scoreRowIDsPrevalidated(rows, dst, scratch, stats)
}

func (p *typedGraphFilterNavigationScorePlane) mapOrdinals(scratch *columnVectorGraphNativeSearchScratch, count int) []uint32 {
	if cap(scratch.filterNavigationRowIDs) < count {
		scratch.filterNavigationRowIDs = make([]uint32, count)
	}
	return scratch.filterNavigationRowIDs[:count]
}

func (n *typedGraphFilterNavigation) search(ctx context.Context, query []float32, topK, efSearch, candidateLimit int, base *columnHNSWSearchPackPreparedView, scratch *columnVectorGraphNativeSearchScratch) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
	if n == nil || base == nil || candidateLimit <= 0 {
		return nil, columnVectorGraphNativeSearchStats{}, errTypedGraphFilterNavigationDeclined
	}
	if ctx == nil {
		ctx = context.Background()
	}
	plane := typedGraphFilterNavigationScorePlane{ctx: ctx, base: columnHNSWPreparedExactFP32ScorePlane{pack: base}, ordinals: n.baseOrdinals, limit: uint64(candidateLimit)}
	_, stats, err := n.view.searchCosinePreparedScorePlane(query, columnHNSWPreparedTraversalOptions{TopK: topK, EfSearch: efSearch, RetainedCandidateLimit: efSearch, ScoreBatchMode: columnVectorGraphScoreBatchModeDefault, StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics, OmitResultMaterialization: true, SuppressOmittedResultMaterialization: true}, scratch, &plane)
	if err != nil {
		return nil, stats, err
	}
	if stats.PreparedScoreCalls > uint64(candidateLimit) {
		return nil, stats, errTypedGraphSearchBudget
	}
	scratch.retainTopBestFirst(topK)
	results := scratch.results[:0]
	for i, candidate := range scratch.top {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, stats, err
			}
		}
		local := candidate.ordinal
		if local < 0 || local >= len(n.baseOrdinals) {
			return nil, stats, ErrVectorIndexSnapshotMismatch
		}
		ordinal := int(n.baseOrdinals[local])
		id, ok := base.documentIDForOrdinal(ordinal)
		if !ok {
			return nil, stats, ErrVectorIndexSnapshotMismatch
		}
		results = append(results, columnVectorGraphNativeSearchResult{Ordinal: ordinal, ID: id, Score: candidate.score})
	}
	scratch.results = results
	return results, stats, nil
}
