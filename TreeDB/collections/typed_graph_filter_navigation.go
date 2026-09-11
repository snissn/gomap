package collections

import (
	"context"
	"errors"
	"math"
	"reflect"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// ponytail: bound cold per-filter construction; raise only if larger selective
// filters show measured benefit within the existing retained-byte budget.
const typedGraphFilterNavigationMaxRows = 1 << 16

var errTypedGraphFilterNavigationDeclined = errors.New("collections: typed graph filter navigation declined")

// typedGraphFilterNavigation is derived, process-local navigation. The base
// pack remains the sole vector and document-ID owner.
type typedGraphFilterNavigation struct {
	view          columnHNSWSearchPackPreparedView
	baseOrdinals  []uint32
	retainedBytes int
	maxScoreCalls int
}

func buildTypedGraphFilterNavigation(ctx context.Context, overlay *typedGraphOverlaySearch, selection typedcolumn.RowSelection, maxBytes int) (*typedGraphFilterNavigation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	count := selection.Count()
	if overlay == nil || !overlay.validOpen() || overlay.pack == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	if count <= typedGraphScalarExactLimit || count > typedGraphFilterNavigationMaxRows || selection.IsAll() || maxBytes <= 0 {
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
	if err := buildColumnVectorGraphAdjacency(rows, overlay.base.reader.def); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	levels, layers, maxLayer, err := buildColumnHNSWSearchPackLevelsAndAdjacency(rows)
	if err != nil {
		return nil, err
	}
	baseOrdinals := make([]uint32, count)
	preparedLayers := make([]columnHNSWSearchPackPreparedLayer, len(layers))
	retained := cap(baseOrdinals)*4 + cap(levels)*2 + cap(preparedLayers)*int(reflect.TypeFor[columnHNSWSearchPackPreparedLayer]().Size())
	maxScoreCalls := count + maxLayer
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
		if i > 0 {
			maxScoreCalls += len(layers[i].Neighbors)
		}
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
		baseOrdinals: baseOrdinals, retainedBytes: retained, maxScoreCalls: maxScoreCalls,
	}, nil
}

type typedGraphFilterNavigationScorePlane struct {
	base     columnHNSWPreparedExactFP32ScorePlane
	ordinals []uint32
}

func (p *typedGraphFilterNavigationScorePlane) kind() columnHNSWPreparedTraversalScorePlaneKind {
	return columnHNSWPreparedTraversalScorePlaneKindExactFP32
}

func (p *typedGraphFilterNavigationScorePlane) prepareForHNSWPreparedTraversal(_ *columnHNSWSearchPackPreparedView, query []float32, opts columnHNSWPreparedTraversalOptions, scratch *columnVectorGraphNativeSearchScratch) error {
	return p.base.prepareForHNSWPreparedTraversal(p.base.pack, query, opts, scratch)
}

func (p *typedGraphFilterNavigationScorePlane) scoreOrdinal(ordinal int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) (float64, error) {
	if ordinal < 0 || ordinal >= len(p.ordinals) {
		return 0, ErrVectorIndexSnapshotMismatch
	}
	return p.base.scoreOrdinal(int(p.ordinals[ordinal]), scratch, stats)
}

func (p *typedGraphFilterNavigationScorePlane) scoreOrdinals(ordinals []int, dst []float64, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphNativeSearchStats) ([]float64, error) {
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

func (n *typedGraphFilterNavigation) search(query []float32, topK, efSearch, candidateLimit int, base *columnHNSWSearchPackPreparedView, scratch *columnVectorGraphNativeSearchScratch) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
	if n == nil || base == nil || candidateLimit < n.maxScoreCalls {
		return nil, columnVectorGraphNativeSearchStats{}, errTypedGraphFilterNavigationDeclined
	}
	plane := typedGraphFilterNavigationScorePlane{base: columnHNSWPreparedExactFP32ScorePlane{pack: base}, ordinals: n.baseOrdinals}
	_, stats, err := n.view.searchCosinePreparedScorePlane(query, columnHNSWPreparedTraversalOptions{TopK: topK, EfSearch: efSearch, RetainedCandidateLimit: efSearch, ScoreBatchMode: columnVectorGraphScoreBatchModeDefault, StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics, OmitResultMaterialization: true, SuppressOmittedResultMaterialization: true}, scratch, &plane)
	if err != nil {
		return nil, stats, err
	}
	if stats.PreparedScoreCalls > uint64(candidateLimit) {
		return nil, stats, errTypedGraphSearchBudget
	}
	scratch.retainTopBestFirst(topK)
	results := scratch.results[:0]
	for _, candidate := range scratch.top {
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
