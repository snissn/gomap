package collections

// Admission consumes owning declared rows already produced by typed planning.
// No vector copies, value encoding, fake LSN, or retained JSON extraction.
func typedGraphTypedAssetsEncodedBound(input columnWritePublishInput) (int64, error) {
	cfg := input.meta.Options.ColumnStore
	if cfg == nil || len(cfg.AggregateMetadata) != 0 {
		return 0, ErrHybridSearchUnsupported
	}
	if err := validateTypedGraphOverlayVectorOwners(*cfg); err != nil {
		return 0, err
	}
	rowColumns := columnStoreRowAssetColumns(*cfg)
	indexes := make([]int, 0, len(rowColumns))
	for i, col := range cfg.Columns {
		if columnStoreColumnIsTypedRowAsset(col) {
			indexes = append(indexes, i)
		}
	}
	values := make([]columnDeclaredValue, len(indexes))
	var total int64
	rowBound := func(documents []columnWriteDocument, operation ColumnPublishOperation, declared bool) error {
		if len(documents) == 0 {
			return nil
		}
		in := columnPhysicalAssetEncodeInput{Collection: input.meta.Name, Namespace: cfg.AssetManager.Namespace, Operation: operation, Columns: rowColumns}
		header, err := columnPhysicalAssetEncodedUpperBound(in)
		if err != nil {
			return err
		}
		if err := addTypedGraphEncodedBytes(&total, header); err != nil {
			return err
		}
		one := [1]columnDeclaredRow{}
		for i, doc := range documents {
			one[0] = columnDeclaredRow{ID: doc.ID, Deleted: operation == ColumnPublishOperationDelete}
			if !one[0].Deleted {
				rowValues := doc.declaredValues
				ready := doc.declaredValuesReady
				if declared && input.declaredRowsReady && len(input.declaredRows) == len(documents) {
					rowValues, ready = input.declaredRows[i].Values, true
				}
				if !ready || len(rowValues) != len(cfg.Columns) {
					return ErrHybridSearchUnsupported
				}
				for j, index := range indexes {
					values[j] = rowValues[index]
					if values[j].StringBytes != nil {
						return ErrHybridSearchUnsupported
					}
				}
				one[0].Values = values
			}
			in.Rows = one[:]
			n, err := columnPhysicalAssetEncodedUpperBound(in)
			if err != nil {
				return err
			}
			if err := addTypedGraphEncodedBytes(&total, n-header); err != nil {
				return err
			}
		}
		return nil
	}
	if err := rowBound(input.sourceDeleteDocuments, ColumnPublishOperationDelete, false); err != nil {
		return 0, err
	}
	if err := rowBound(input.documents, input.operation, true); err != nil {
		return 0, err
	}
	if input.operation == ColumnPublishOperationDelete || len(input.documents) == 0 {
		return total, nil
	}
	encoded, err := typedColumnPublicationFP32EncodedBound(*cfg, len(input.documents))
	if err != nil {
		return 0, err
	}
	if err := addTypedGraphEncodedBytes(&total, encoded); err != nil {
		return 0, err
	}
	return total, nil
}
