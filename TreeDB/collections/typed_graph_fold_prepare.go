package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
)

// prepareTypedGraphCapturedAssets prepares only an unpublished physical
// candidate. It neither installs a manifest nor advances logical authority.
// The caller retains the validated captured snapshot and owns returned stable
// resources through publication or release. Rows are encoder-ready owned values.
func (c *Collection) prepareTypedGraphCapturedAssets(state columnStoreCompactionState, rows []columnDeclaredRow, admission *typedGraphFoldAssetAdmission) (ColumnPublishPreparedAssets, error) {
	return c.prepareTypedGraphCapturedAssetsFromSources(state, rows, nil, nil, admission)
}

func (c *Collection) prepareTypedGraphCapturedAssetsFromSources(state columnStoreCompactionState, rows []columnDeclaredRow, rowSource columnDeclaredRowSource, typedSource typedColumnAdapterRowSource, admission *typedGraphFoldAssetAdmission) (ColumnPublishPreparedAssets, error) {
	generation := state.manifest.Generation
	if generation == 0 || state.cfg.ActiveManifest == nil || state.cfg.ActiveManifest.Generation != generation || state.manifest.AppliedCommandLSN == 0 || state.manifest.AppliedCommandLSN != state.cfg.RecoveryAuthoritativeAppliedCommandLSN {
		return ColumnPublishPreparedAssets{}, errors.New("collections: captured typed assets require exact manifest frontier")
	}
	// Typed reconstruction selects part2 within each generation. Preserve that
	// existing convention; only the row part must be beyond captured coordinates.
	maxPart := uint64(typedColumnPartAssetPartID)
	for _, record := range state.records {
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		partGeneration, partID, err := decodeColumnManifestPartRecordKey(record.key)
		if err != nil {
			return ColumnPublishPreparedAssets{}, err
		}
		if partGeneration == generation && partID > maxPart {
			maxPart = partID
		}
	}
	if maxPart == math.MaxUint64 {
		return ColumnPublishPreparedAssets{}, errors.New("collections: captured typed row part identity exhausted")
	}
	rowCount := len(rows)
	if rowSource != nil {
		rowCount = rowSource.Len()
	}
	prepared := ColumnPublishPreparedAssets{RowCount: rowCount}
	if rowCount == 0 {
		return prepared, nil
	}
	input := columnWritePublishInput{meta: state.meta, operation: ColumnPublishOperationInsert, rows: rowCount, declaredRows: rows, declaredRowsReady: true}
	input.candidateAdmission = admission
	return c.prepareColumnPhysicalAssetRowsAtIdentityFromSources(prepared, input, ColumnPublishAssetPrepareInput{
		Collection: state.meta.Name, ColumnStore: state.cfg, Operation: ColumnPublishOperationInsert,
		AppliedCommandLSN: state.manifest.AppliedCommandLSN, CurrentManifest: state.cfg.ActiveManifest,
	}, rows, rowSource, typedSource, generation, maxPart+1, typedColumnPartAssetPartID)
}

func typedGraphFoldStreamedSourcesEligible(cfg ColumnStoreConfig, def VectorIndexDefinition) (bool, error) {
	fields := columnStoreTypedColumnPartFields(cfg)
	if len(fields) != 1 || fields[0].Path != def.Field || fields[0].ValueType != ColumnStoreValueFloat32Vector || fields[0].VectorDims != def.Dimensions {
		return false, nil
	}
	if len(columnStoreTypedColumnPartAggregateMetadata(cfg)) != 0 {
		return false, nil
	}
	rowCfg := columnStoreRowAssetConfig(cfg)
	_, fused, err := newColumnRowSidecarAggregateSpecs(rowCfg, rowCfg.AggregateMetadata)
	return fused, err
}

type typedGraphFoldRowSource struct {
	resolveScoringRefs bool // captured locator root contains metadata-only rows
	ctx                context.Context
	rows               []columnVectorGraphAssetRow
	readView           CollectionReadView
	view               columnPhysicalScanSnapshotView
	projection         columnPhysicalScanProjection
	scratch            columnPhysicalRowReaderScratch
}

var typedGraphFoldRowSourceCloseErrorForTest error

func newTypedGraphFoldRowSource(ctx context.Context, c *Collection, state columnStoreCompactionState, rows []columnVectorGraphAssetRow, resolveScoringRefs bool) (*typedGraphFoldRowSource, error) {
	view, err := c.prepareColumnPhysicalScanSnapshotViewAtSnapshot(state.snap, state.catalog, state.meta.Name, state.baseRoot, state.cfg, true)
	if err != nil {
		return nil, err
	}
	projection, err := newColumnPhysicalScanProjection(view.Config, nil)
	if err != nil {
		return nil, err
	}
	source := &typedGraphFoldRowSource{
		resolveScoringRefs: resolveScoringRefs,
		ctx:                ctx,
		rows:               rows,
		readView:           CollectionReadView{collection: c, snapshot: state.snap, catalog: state.catalog},
		view:               view,
		projection:         projection,
	}
	if err := source.readView.ensureAssetReadCaches(state.cfg, ""); err != nil {
		_ = source.Close()
		return nil, err
	}
	return source, nil
}

func (s *typedGraphFoldRowSource) Len() int { return len(s.rows) }

func (s *typedGraphFoldRowSource) Row(i int) (columnDeclaredRow, error) {
	if i < 0 || i >= len(s.rows) {
		return columnDeclaredRow{}, fmt.Errorf("row index=%d outside rows=%d", i, len(s.rows))
	}
	if err := s.ctx.Err(); err != nil {
		return columnDeclaredRow{}, err
	}
	graphRow := s.rows[i]
	ref := graphRow.BaseRowRef
	ref.DocumentID = graphRow.ID
	if s.resolveScoringRefs {
		var err error
		ref, err = s.readView.resolveDocumentRowRefLatest(ref, nil, true)
		if err != nil {
			return columnDeclaredRow{}, err
		}
	}
	row, err := s.readView.fetchDocumentPointRow(s.view, ref, s.projection, &s.scratch, nil)
	if err != nil {
		return columnDeclaredRow{}, err
	}
	for i := range row.Values {
		if row.Values[i].StringBytes != nil {
			row.Values[i].String = string(row.Values[i].StringBytes)
			row.Values[i].StringBytes = nil
		}
	}
	return columnDeclaredRow{ID: graphRow.ID, Values: row.Values}, nil
}

func (s *typedGraphFoldRowSource) Close() error {
	return errors.Join(s.readView.Close(), typedGraphFoldRowSourceCloseErrorForTest)
}

type typedGraphFoldVectorSource struct {
	ctx   context.Context
	rows  []columnVectorGraphAssetRow
	field TypedStorageField
}

func (s typedGraphFoldVectorSource) Len() int { return len(s.rows) }

func (s typedGraphFoldVectorSource) PrimaryID(rowIdx int) int64 { return int64(rowIdx) }

func (s typedGraphFoldVectorSource) Value(rowIdx int, column typedColumnAdapterColumn) (columnDeclaredValue, bool, error) {
	if err := s.ctx.Err(); err != nil {
		return columnDeclaredValue{}, false, err
	}
	if rowIdx < 0 || rowIdx >= len(s.rows) {
		return columnDeclaredValue{}, false, fmt.Errorf("row index=%d outside rows=%d", rowIdx, len(s.rows))
	}
	if column.Field.Path != s.field.Path || column.Field.ValueType != s.field.ValueType {
		return columnDeclaredValue{}, false, fmt.Errorf("typed field %q is outside streamed fold field %q", column.Field.Path, s.field.Path)
	}
	return columnDeclaredValue{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: s.rows[rowIdx].Vector}, true, nil
}
