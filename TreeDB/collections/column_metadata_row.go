package collections

import (
	"bytes"
	"errors"
	"strings"
	"unicode/utf8"
)

func validateColumnMetadataPublishInput(input columnWritePublishInput) error {
	if input.operation != ColumnPublishOperationUpdate || !input.declaredRowsReady || input.meta.Options.ColumnStore == nil || input.rows != len(input.documents) || input.rows != len(input.declaredRows) || len(input.sourceDeleteDocuments) != 0 {
		return errors.New("collections: invalid metadata publication input")
	}
	columns := input.meta.Options.ColumnStore.Columns
	for i, row := range input.declaredRows {
		doc := input.documents[i]
		if row.Preserved == nil || doc.preserved == nil || *row.Preserved != *doc.preserved || !bytes.Equal(row.ID, doc.ID) || row.Deleted || len(row.Values) != len(columns) {
			return errors.New("collections: invalid metadata publication row")
		}
		if err := validateDocumentRowRefForPointFetch(i, row.Preserved.ref(row.ID)); err != nil {
			return err
		}
		for j, col := range columns {
			value := row.Values[j]
			if columnMetadataStoredColumn(col) {
				if !columnStoreColumnIsTypedRowAsset(col) || value.Type != ColumnStoreValueString || !value.Present || value.Null || value.StringBytes != nil || !utf8.ValidString(value.String) {
					return errors.New("collections: metadata publication requires owned non-null row-asset strings")
				}
			} else if value.Type != "" || value.Present || value.Null || value.String != "" || value.StringBytes != nil || value.Float32Vector != nil || value.DenseNumericVector != nil || value.Bytes != nil || value.Uint32List != nil || value.AdjacencyList != nil {
				return errors.New("collections: metadata publication contains unchanged column payload")
			}
		}
	}
	return nil
}

// TCPA v9 retains the ordinary row schema in its header, but only stores the
// metadata string slots. All other slots come from the single preserved row.
func columnMetadataStoredColumn(col ColumnStoreColumn) bool {
	return strings.HasPrefix(col.Path, "meta.") && col.ValueType == ColumnStoreValueString
}

func validateColumnPreservedRow(id []byte, ref columnRowCoordinates, generation, lsn uint64) error {
	if err := validateDocumentRowRefForPointFetch(0, ref.ref(id)); err != nil {
		return err
	}
	if ref.Generation >= generation || ref.AppliedCommandLSN >= lsn {
		return errors.New("collections: metadata row must preserve an earlier full row")
	}
	return nil
}

func writeColumnPreservedRow(b *bytes.Buffer, ref columnRowCoordinates) {
	writeManifestUint64(b, ref.Generation)
	writeManifestUint64(b, ref.PartID)
	writeManifestUint64(b, uint64(ref.RowIndex))
	writeManifestUint64(b, ref.AppliedCommandLSN)
}

func readColumnPreservedRow(cur *manifestCursor, id []byte, generation, lsn uint64, operation ColumnPublishOperation, deleted bool) *columnRowCoordinates {
	ref := columnRowCoordinates{Generation: cur.u64(), PartID: cur.u64()}
	rowIndex := cur.u64()
	ref.AppliedCommandLSN = cur.u64()
	if cur.err != nil {
		return nil
	}
	if operation != ColumnPublishOperationUpdate || deleted || rowIndex > uint64(maxCollectionInt) {
		cur.err = errors.New("collections: invalid metadata row operation or coordinates")
		return nil
	}
	ref.RowIndex = int(rowIndex)
	cur.err = validateColumnPreservedRow(id, ref, generation, lsn)
	if cur.err != nil {
		return nil
	}
	return &ref
}
