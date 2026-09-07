package collections

import (
	"bytes"
	"errors"
	"math"
)

// prepareTypedGraphCapturedAssets prepares only an unpublished physical
// candidate. It neither installs a manifest nor advances logical authority.
// The caller retains the validated captured snapshot and owns returned stable
// resources through publication or release. Rows are encoder-ready owned values.
func (c *Collection) prepareTypedGraphCapturedAssets(state columnStoreCompactionState, rows []columnDeclaredRow, admission *typedGraphFoldAssetAdmission) (ColumnPublishPreparedAssets, error) {
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
	prepared := ColumnPublishPreparedAssets{RowCount: len(rows)}
	if len(rows) == 0 {
		return prepared, nil
	}
	input := columnWritePublishInput{meta: state.meta, operation: ColumnPublishOperationInsert, rows: len(rows), declaredRows: rows, declaredRowsReady: true}
	input.candidateAdmission = admission
	return c.prepareColumnPhysicalAssetRowsAtIdentity(prepared, input, ColumnPublishAssetPrepareInput{
		Collection: state.meta.Name, ColumnStore: state.cfg, Operation: ColumnPublishOperationInsert,
		AppliedCommandLSN: state.manifest.AppliedCommandLSN, CurrentManifest: state.cfg.ActiveManifest,
	}, rows, generation, maxPart+1, typedColumnPartAssetPartID)
}
