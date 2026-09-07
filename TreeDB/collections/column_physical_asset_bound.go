package collections

import (
	"errors"
	"math"
)

// columnPhysicalAssetEncodedUpperBound is the selected typed publication row
// image bound: strings owned by row assets, vectors owned by separate TCIM parts.
// Input rows must be encoder-ready: String is the encoder's payload; borrowed
// reconstruction StringBytes must first pass the existing owning normalization.
// It includes empty images and the fixed/dense-ID alternative headers. It is not
// a retained-document, sidecar, container, manifest-tree or allocation bound.
func columnPhysicalAssetEncodedUpperBound(input columnPhysicalAssetEncodeInput) (int64, error) {
	if input.Collection == "" || input.Namespace == "" || !isSupportedColumnPhysicalAssetOperation(input.Operation) {
		return 0, errColumnPhysicalAssetBound
	}
	var n int64
	add := func(v int64) bool {
		if v < 0 || v > math.MaxInt64-n {
			return false
		}
		n += v
		return true
	}
	// Magic/version, six uint64 header fields, and possible V7/V8 ID header.
	if !add(4 + 2 + 6*8 + 8 + int64(len(columnPhysicalAssetRowEncodingDenseIDRange)) + 8) {
		return 0, errColumnPhysicalAssetBound
	}
	for _, s := range []string{input.Collection, input.Namespace, string(input.Operation)} {
		if !add(8 + int64(len(s))) {
			return 0, errColumnPhysicalAssetBound
		}
	}
	for _, col := range input.Columns {
		if col.ValueType != ColumnStoreValueString || col.Nullable || col.Dictionary || col.VectorDims != 0 || col.ElementsPerRow != 0 || col.FixedWidthEncoding != ColumnFixedWidthEncodingDefault {
			return 0, errColumnPhysicalAssetBound
		}
		for _, s := range []string{col.Name, col.Path, string(col.ValueType), string(col.FixedWidthEncoding)} {
			if !add(8 + int64(len(s))) {
				return 0, errColumnPhysicalAssetBound
			}
		}
		// Nullable/dictionary flags, vector dimensions and V6 element width.
		if !add(2 + 8 + 8) {
			return 0, errColumnPhysicalAssetBound
		}
	}
	for _, row := range input.Rows {
		if !add(8 + int64(len(row.ID)) + 1) {
			return 0, errColumnPhysicalAssetBound
		}
		if input.Operation == ColumnPublishOperationDelete {
			if !row.Deleted || len(row.Values) != 0 {
				return 0, errColumnPhysicalAssetBound
			}
			continue
		}
		if row.Deleted || len(row.Values) != len(input.Columns) {
			return 0, errColumnPhysicalAssetBound
		}
		for _, value := range row.Values {
			if value.Type != ColumnStoreValueString || value.Null || !value.Present {
				return 0, errColumnPhysicalAssetBound
			}
			if !add(8 + int64(len(value.Type)) + 2 + 8 + int64(len(value.String))) {
				return 0, errColumnPhysicalAssetBound
			}
		}
	}
	return n, nil
}

var errColumnPhysicalAssetBound = errors.New("collections: unsupported or overflowing typed string row-image bound")
