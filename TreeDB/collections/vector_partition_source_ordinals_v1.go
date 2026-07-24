package collections

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

// VectorPartitionSourceOrdinalV1 binds a stable document ID to the ordinal
// used by one authoritative vector-index generation. Partition builders use
// this mapping instead of assuming that collection ingestion order and native
// HNSW row order are identical.
type VectorPartitionSourceOrdinalV1 struct {
	Ordinal  uint64
	StableID string
}

// VectorPartitionSourceOrdinalsV1 returns the authoritative native-ordinal
// mapping for a loaded vector-index generation. The identity and rows come from
// the same immutable reader snapshot.
func (c *Collection) VectorPartitionSourceOrdinalsV1(indexName string) (VectorPartitionSourceIdentityV1, []VectorPartitionSourceOrdinalV1, error) {
	if c == nil {
		return VectorPartitionSourceIdentityV1{}, nil, errCollectionNil
	}
	if c.db == nil {
		return VectorPartitionSourceIdentityV1{}, nil, errCollectionDBNil
	}
	reader, err := c.openColumnVectorGraphPhysicalRowReader(indexName, columnVectorGraphPhysicalRowReaderOptions{})
	if err != nil {
		return VectorPartitionSourceIdentityV1{}, nil, fmt.Errorf("collections: vector partition source ordinals: %w", err)
	}
	defer reader.Close()

	limits := DefaultVectorPartitionManifestLimits()
	if reader.graph.RowCount < 1 || reader.graph.RowCount > limits.sourceRowLimit() {
		return VectorPartitionSourceIdentityV1{}, nil, fmt.Errorf("%w: source ordinal count cap", ErrVectorPartitionManifestInvalid)
	}
	identity := VectorPartitionSourceIdentityV1{
		Generation: reader.graph.BaseManifestGeneration,
		Checksum:   reader.graph.BaseManifestChecksum,
		SchemaHash: reader.graph.BaseSchemaHash,
		RowCount:   uint64(reader.graph.RowCount),
	}
	rows := make([]VectorPartitionSourceOrdinalV1, reader.graph.RowCount)
	seen := make(map[string]struct{}, reader.graph.RowCount)
	var stableIDBytes int
	for ordinal := range rows {
		id, ok := reader.documentIDForOrdinal(ordinal)
		if !ok || len(id) == 0 || len(id) > limits.MaxStringBytes || !utf8.Valid(id) {
			return VectorPartitionSourceIdentityV1{}, nil, fmt.Errorf("%w: source stable ID at ordinal %d", ErrVectorPartitionManifestInvalid, ordinal)
		}
		if len(id) > limits.MaxBytes-stableIDBytes {
			return VectorPartitionSourceIdentityV1{}, nil, fmt.Errorf("%w: source stable ID byte cap", ErrVectorPartitionManifestInvalid)
		}
		stableIDBytes += len(id)
		stableID := string(id)
		if _, duplicate := seen[stableID]; duplicate {
			return VectorPartitionSourceIdentityV1{}, nil, fmt.Errorf("%w: duplicate source stable ID %q", ErrVectorPartitionManifestInvalid, stableID)
		}
		seen[stableID] = struct{}{}
		rows[ordinal] = VectorPartitionSourceOrdinalV1{Ordinal: uint64(ordinal), StableID: stableID}
	}
	if uint64(len(rows)) != identity.RowCount {
		return VectorPartitionSourceIdentityV1{}, nil, errors.New("collections: vector partition source ordinal accounting mismatch")
	}
	return identity, rows, nil
}
