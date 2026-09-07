package documentservice

import "github.com/snissn/gomap/TreeDB/collections"

// The schema, not a process-local flag, identifies selected typed input after
// reopen. Operational graph admission remains a separate lifecycle action.
func serviceTypedInputConfig(dimension int, fields []normalizedScalarField) (*collections.ColumnStoreConfig, error) {
	cfg := serviceColumnStoreConfig(dimension)
	cfg.Columns = append(cfg.Columns, collections.ColumnStoreColumn{
		Name: defaultTextField, Path: defaultTextField, ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset,
	})
	for _, field := range fields {
		if field.collectionTy != collections.IndexValueString {
			return nil, serviceError(CodeInvalidRequest, "typed input requires declared string scalar fields")
		}
		cfg.Columns = append(cfg.Columns, collections.ColumnStoreColumn{
			Name: field.field, Path: field.field, ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset,
		})
	}
	return cfg, nil
}

func serviceUsesTypedInput(meta collections.CollectionMeta) bool {
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.RetainedPayload != collections.ColumnRetainedPayloadNonColumn ||
		cfg.RetainedPayloadEncoding != collections.ColumnRetainedPayloadEncodingJSON ||
		len(cfg.Columns) != len(meta.Indexes)+2 || len(meta.VectorIndexes) != 1 ||
		meta.VectorIndexes[0].Strategy != collections.VectorIndexStrategyColumnGraph {
		return false
	}
	for _, column := range cfg.Columns {
		if column.Nullable {
			return false
		}
		if column.Path != defaultEmbeddingField && column.Owner != "" && column.Owner != collections.TypedStorageOwnerRowAsset {
			return false
		}
		switch column.Path {
		case defaultEmbeddingField:
			if column.Name != defaultEmbeddingField || column.Path != defaultEmbeddingField ||
				column.ValueType != collections.ColumnStoreValueFloat32Vector ||
				column.Owner != collections.TypedStorageOwnerColumnPart || column.VectorDims != meta.VectorIndexes[0].Dimensions {
				return false
			}
		case defaultTextField:
			if column.Name != defaultTextField || column.Path != defaultTextField || column.ValueType != collections.ColumnStoreValueString {
				return false
			}
		default:
			matched := false
			for _, index := range meta.Indexes {
				if column.Name == index.Field && column.Path == index.Field && column.ValueType == collections.ColumnStoreValueString && index.ValueType == collections.IndexValueString {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
	}
	return true
}
