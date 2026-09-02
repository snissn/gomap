package collections

type columnRowSidecarAssets struct {
	DictionaryCodes   []columnDictionaryCodesAsset
	Int64Values       []columnInt64ValuesAsset
	AggregateMetadata []columnAggregateMetadataAsset
}

func buildColumnRowSidecarAssets(cfg ColumnStoreConfig, rows []columnDeclaredRow, aggregates []ColumnAggregateMetadata, collection, namespace string, generation, partID, appliedLSN uint64) (columnRowSidecarAssets, bool, error) {
	dictionaryBuilders := make([]columnDictionaryCodesAssetBuilder, 0)
	int64Builders := make([]columnInt64ValuesAssetBuilder, 0)
	for colIdx, col := range cfg.Columns {
		if col.Dictionary && col.ValueType == ColumnStoreValueString {
			dictionaryBuilders = append(dictionaryBuilders, newColumnDictionaryCodesAssetBuilder(col, colIdx, len(rows)))
		}
		if col.ValueType == ColumnStoreValueInt64 && !col.Nullable {
			int64Builders = append(int64Builders, newColumnInt64ValuesAssetBuilder(col, colIdx, len(rows)))
		}
	}
	aggregateSpecs, ok, err := newColumnRowSidecarAggregateSpecs(cfg, aggregates)
	if err != nil || !ok {
		return columnRowSidecarAssets{}, ok, err
	}
	accumulators, specAccumulatorIdx := newColumnAggregateMetadataAccumulators(aggregateSpecs)
	for rowIdx, row := range rows {
		if row.Deleted {
			return columnRowSidecarAssets{}, false, nil
		}
		if len(row.Values) != len(cfg.Columns) {
			return columnRowSidecarAssets{}, false, nil
		}
		for idx := range dictionaryBuilders {
			if err := dictionaryBuilders[idx].appendValue(rowIdx, row.Values[dictionaryBuilders[idx].columnIndex]); err != nil {
				return columnRowSidecarAssets{}, true, err
			}
		}
		for idx := range int64Builders {
			if err := int64Builders[idx].appendValue(rowIdx, row.Values[int64Builders[idx].columnIndex]); err != nil {
				return columnRowSidecarAssets{}, true, err
			}
		}
		for idx := range accumulators {
			if err := accumulators[idx].spec.appendRow(accumulators[idx].entries, row); err != nil {
				return columnRowSidecarAssets{}, true, err
			}
		}
	}
	assets := columnRowSidecarAssets{
		DictionaryCodes:   make([]columnDictionaryCodesAsset, 0, len(dictionaryBuilders)),
		Int64Values:       make([]columnInt64ValuesAsset, 0, len(int64Builders)),
		AggregateMetadata: make([]columnAggregateMetadataAsset, 0, len(aggregateSpecs)),
	}
	for idx := range dictionaryBuilders {
		asset, ok := dictionaryBuilders[idx].asset(cfg.SchemaHash, collection, namespace, generation, partID, appliedLSN)
		if ok {
			assets.DictionaryCodes = append(assets.DictionaryCodes, asset)
		}
	}
	for idx := range int64Builders {
		asset, ok := int64Builders[idx].asset(cfg.SchemaHash, collection, namespace, generation, partID, appliedLSN)
		if ok {
			assets.Int64Values = append(assets.Int64Values, asset)
		}
	}
	for idx, spec := range aggregateSpecs {
		entries := sortedColumnAggregateMetadataEntries(accumulators[specAccumulatorIdx[idx]].entries)
		assets.AggregateMetadata = append(assets.AggregateMetadata, spec.asset(cfg.SchemaHash, collection, namespace, generation, partID, appliedLSN, len(rows), entries))
	}
	return assets, true, nil
}

func newColumnRowSidecarAggregateSpecs(cfg ColumnStoreConfig, aggregates []ColumnAggregateMetadata) ([]columnAggregateMetadataBuildSpec, bool, error) {
	specs := make([]columnAggregateMetadataBuildSpec, 0, len(aggregates))
	for _, aggregate := range aggregates {
		spec, ok, err := newColumnAggregateMetadataBuildSpec(cfg, aggregate)
		if err != nil {
			return nil, false, err
		}
		if !ok || len(spec.predicateSpecs) != 0 || columnAggregateMetadataUsesTypedColumnGranules(cfg, aggregate) {
			return nil, false, nil
		}
		specs = append(specs, spec)
	}
	return specs, true, nil
}
