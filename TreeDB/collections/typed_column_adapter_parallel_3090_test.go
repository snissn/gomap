package collections

import (
	"runtime"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestTypedColumnDenseParallelDecodersMatchSerial3090(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	const (
		blocks       = typedColumnDenseParallelMinBlocks + 1
		rowsPerBlock = typedColumnDenseParallelMinRows/typedColumnDenseParallelMinBlocks + 1
		cardinality  = 257
	)
	rows := blocks * rowsPerBlock

	t.Run("uint32_codes", func(t *testing.T) {
		partColumn := buildTypedColumnUint32CodePart3090(t, blocks, rowsPerBlock, cardinality)
		want, wantBytes, wantBlocks, err := decodeTypedColumnUint32CodesForRowRange(partColumn, cardinality, 0, rows, "test serial codes", nil)
		if err != nil {
			t.Fatalf("decode serial codes: %v", err)
		}
		got, gotBytes, gotBlocks, err := decodeTypedColumnDenseUint32Codes(partColumn, cardinality, rows, "test parallel codes")
		if err != nil {
			t.Fatalf("decode parallel codes: %v", err)
		}
		if !slices.Equal(got, want) || gotBytes != wantBytes || gotBlocks != wantBlocks {
			t.Fatalf("parallel codes mismatch len=%d/%d bytes=%d/%d blocks=%d/%d", len(got), len(want), gotBytes, wantBytes, gotBlocks, wantBlocks)
		}
	})

	t.Run("nullable_uint32_codes", func(t *testing.T) {
		partColumn := buildTypedColumnNullableCodePart3090(t, blocks, rowsPerBlock, cardinality)
		wantCodes, wantValid, wantBytes, wantBlocks, err := decodeTypedColumnNullableUint32CodesForRowRange(partColumn, cardinality, 0, rows, "test serial nullable codes", nil, nil)
		if err != nil {
			t.Fatalf("decode serial nullable codes: %v", err)
		}
		gotCodes, gotValid, gotBytes, gotBlocks, err := decodeTypedColumnDenseNullableUint32Codes(partColumn, cardinality, rows, "test parallel nullable codes")
		if err != nil {
			t.Fatalf("decode parallel nullable codes: %v", err)
		}
		if !slices.Equal(gotCodes, wantCodes) || !slices.Equal(gotValid, wantValid) || gotBytes != wantBytes || gotBlocks != wantBlocks {
			t.Fatalf("parallel nullable codes mismatch len=%d/%d valid=%d/%d bytes=%d/%d blocks=%d/%d", len(gotCodes), len(wantCodes), len(gotValid), len(wantValid), gotBytes, wantBytes, gotBlocks, wantBlocks)
		}
	})

	t.Run("int64_values", func(t *testing.T) {
		partColumn := buildTypedColumnInt64Part3090(t, blocks, rowsPerBlock)
		want, wantBytes, wantBlocks, err := decodeTypedColumnInt64ValuesForRowRange(partColumn, 0, rows, "test serial int64", nil)
		if err != nil {
			t.Fatalf("decode serial int64: %v", err)
		}
		got, gotBytes, gotBlocks, err := decodeTypedColumnDenseInt64Values(partColumn, rows, "test parallel int64")
		if err != nil {
			t.Fatalf("decode parallel int64: %v", err)
		}
		if !slices.Equal(got, want) || gotBytes != wantBytes || gotBlocks != wantBlocks {
			t.Fatalf("parallel int64 mismatch len=%d/%d bytes=%d/%d blocks=%d/%d", len(got), len(want), gotBytes, wantBytes, gotBlocks, wantBlocks)
		}
	})
}

func buildTypedColumnUint32CodePart3090(t *testing.T, blocks, rowsPerBlock, cardinality int) typedcolumn.ColumnPartColumn {
	t.Helper()
	builder := typedcolumn.NewGranuleBuilder(typedcolumn.Config{Encoding: typedcolumn.EncodingLowCardinalityUint32, Compression: typedcolumn.CompressionLZ4})
	partColumn := typedcolumn.ColumnPartColumn{Definition: typedcolumn.ColumnDefinition{Name: "codes", Type: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingLowCardinalityUint32, Cardinality: uint32(cardinality)}}
	firstRow := 0
	for blockIdx := 0; blockIdx < blocks; blockIdx++ {
		values := make([]uint32, rowsPerBlock)
		for row := range values {
			values[row] = uint32((blockIdx*31 + row*7) % cardinality)
		}
		granule, err := builder.BuildUint32Codes(values, uint32(cardinality))
		if err != nil {
			t.Fatalf("BuildUint32Codes block %d: %v", blockIdx, err)
		}
		partColumn.Blocks = append(partColumn.Blocks, typedColumnBlockForTest3090(blockIdx, firstRow, rowsPerBlock, copyTypedColumnGranulePayload3090(granule)))
		firstRow += rowsPerBlock
	}
	return partColumn
}

func buildTypedColumnNullableCodePart3090(t *testing.T, blocks, rowsPerBlock, cardinality int) typedcolumn.ColumnPartColumn {
	t.Helper()
	builder := typedcolumn.NewGranuleBuilder(typedcolumn.Config{Encoding: typedcolumn.EncodingNullableInt64, Compression: typedcolumn.CompressionLZ4})
	partColumn := typedcolumn.ColumnPartColumn{Definition: typedcolumn.ColumnDefinition{Name: "nullable_codes", Type: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingNullableInt64, Cardinality: uint32(cardinality)}}
	firstRow := 0
	for blockIdx := 0; blockIdx < blocks; blockIdx++ {
		values := make([]int64, rowsPerBlock)
		nulls := make([]bool, rowsPerBlock)
		defaults := make([]bool, rowsPerBlock)
		for row := range values {
			absoluteRow := firstRow + row
			nulls[row] = absoluteRow%29 == 0
			defaults[row] = !nulls[row] && absoluteRow%31 == 0
			if !nulls[row] && !defaults[row] {
				values[row] = int64((blockIdx*37 + row*11) % cardinality)
			}
		}
		granule, err := builder.BuildNullableInt64(values, nulls, defaults, 0)
		if err != nil {
			t.Fatalf("BuildNullableInt64 block %d: %v", blockIdx, err)
		}
		partColumn.Blocks = append(partColumn.Blocks, typedColumnBlockForTest3090(blockIdx, firstRow, rowsPerBlock, copyTypedColumnGranulePayload3090(granule)))
		firstRow += rowsPerBlock
	}
	return partColumn
}

func buildTypedColumnInt64Part3090(t *testing.T, blocks, rowsPerBlock int) typedcolumn.ColumnPartColumn {
	t.Helper()
	builder := typedcolumn.NewGranuleBuilder(typedcolumn.Config{Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionLZ4})
	partColumn := typedcolumn.ColumnPartColumn{Definition: typedcolumn.ColumnDefinition{Name: "values", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint}}
	firstRow := 0
	for blockIdx := 0; blockIdx < blocks; blockIdx++ {
		values := make([]int64, rowsPerBlock)
		base := int64(blockIdx * rowsPerBlock)
		for row := range values {
			values[row] = base + int64(row*3) - int64(row%17)
		}
		granule, err := builder.BuildInt64(values)
		if err != nil {
			t.Fatalf("BuildInt64 block %d: %v", blockIdx, err)
		}
		partColumn.Blocks = append(partColumn.Blocks, typedColumnBlockForTest3090(blockIdx, firstRow, rowsPerBlock, copyTypedColumnGranulePayload3090(granule)))
		firstRow += rowsPerBlock
	}
	return partColumn
}

func typedColumnBlockForTest3090(blockIdx, firstRow, rowCount int, granule typedcolumn.EncodedGranule) typedcolumn.ColumnBlock {
	return typedcolumn.ColumnBlock{
		Descriptor: typedcolumn.ColumnBlockDescriptor{
			FirstRow:          firstRow,
			RowCount:          rowCount,
			FirstGranule:      blockIdx,
			LastGranule:       blockIdx,
			Encoding:          granule.Encoding,
			Compression:       granule.Compression,
			RawBytes:          granule.RawBytes,
			StoredBytes:       granule.StoredBytes,
			CodecBlockOrdinal: blockIdx,
		},
		Granule: granule,
	}
}

func copyTypedColumnGranulePayload3090(granule typedcolumn.EncodedGranule) typedcolumn.EncodedGranule {
	granule.Payload = append([]byte(nil), granule.Payload...)
	return granule
}
