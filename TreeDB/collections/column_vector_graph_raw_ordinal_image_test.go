package collections

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// Exercise the actual five collection entry points against the former general
// construction route, including their adapter dictionaries and logical types.
func TestColumnVectorGraphRawOrdinalAssetConstructorParity(t *testing.T) {
	base, err := normalizeColumnStoreConfig("docs", columnGraphRebuildColumnStoreConfigV2A(2))
	if err != nil {
		t.Fatal(err)
	}
	def := columnGraphRebuildVectorIndexDefinitionV2A(2, 2)
	for _, n := range []int{0, 1, 4096, typedcolumn.DefaultRowsPerGranule + 1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			rows := make([]columnVectorGraphAssetRow, n)
			list := typedcolumn.RawUint32OffsetsList{Rows: n, Offsets: make([]uint64, n+1)}
			for i := range rows {
				rows[i] = columnVectorGraphAssetRow{ID: []byte(fmt.Sprintf("doc-%d", i)), Vector: []float32{3, 4}, BaseRowRef: DocumentRowRef{Generation: 2, PartID: 7, RowIndex: i, AppliedCommandLSN: 3}}
				if n > 1 {
					list.Values = append(list.Values, uint32((i+1)%n))
				}
				list.Offsets[i+1] = uint64(len(list.Values))
			}
			check := func(label string, raw []byte, padding int64, cfg ColumnStoreConfig, adapter typedColumnAdapterColumn, values typedcolumn.RawOrdinalColumnValues, logical map[string]string) typedcolumn.ColumnPartByteAccounting {
				t.Helper()
				ids := make([]int64, n)
				for i := range ids {
					ids[i] = int64(i)
				}
				batch := typedcolumn.Batch{Rows: n, Columns: map[string][]int64{typedColumnAdapterPrimaryIDColumn: ids}}
				switch adapter.Definition.Type {
				case typedcolumn.ColumnTypeInt64:
					batch.Columns[adapter.Definition.Name] = values.Int64
				case typedcolumn.ColumnTypeFloat32:
					batch.Float32Columns = map[string][]float32{adapter.Definition.Name: values.Float32}
				case typedcolumn.ColumnTypeUint32List, typedcolumn.ColumnTypeAdjacencyList:
					batch.Uint32OffsetsLists = map[string]typedcolumn.RawUint32OffsetsList{adapter.Definition.Name: values.Uint32List}
				case typedcolumn.ColumnTypeBytes:
					batch.BytesColumns = map[string]typedcolumn.RawBytesOffsets{adapter.Definition.Name: values.Bytes}
				default:
					t.Fatalf("%s unsupported type %s", label, adapter.Definition.Type)
				}
				part, err := typedcolumn.BuildColumnPart(91, columnVectorGraphRawOrdinalOptions(cfg, adapter), batch)
				if err != nil {
					t.Fatalf("%s generic part: %v", label, err)
				}
				imageOpts := typedcolumn.ColumnPartImageOptions{Dictionaries: typedColumnAdapterDictionaries([]typedColumnAdapterColumn{adapter}), LayoutLogicalTypes: logical}
				want, err := typedcolumn.BuildColumnPartImage(part, imageOpts)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(raw, want.Bytes) {
					t.Fatalf("%s image differs: raw=%d generic=%d", label, len(raw), len(want.Bytes))
				}
				accounting := part.ByteAccountingFromImage(want)
				if padding != int64(accounting.SerializedPaddingBytes) {
					t.Fatalf("%s padding differs", label)
				}
				if _, err = typedcolumn.ColumnPartFromImage(want); err != nil {
					t.Fatal(err)
				}
				if _, err = typedcolumn.PlanRawOrdinalColumnPartImage(columnVectorGraphRawOrdinalOptions(cfg, adapter), values, imageOpts); err != nil {
					t.Fatalf("%s maintained shape did not activate: %v", label, err)
				}
				return accounting
			}
			state, err := prepareColumnVectorIndexStateAdjacencyPayloadFromList("unused", "docs", *base, def, 91, 0, list)
			if err != nil {
				t.Fatal(err)
			}
			_, stateAdapter, err := columnVectorIndexStateAdjacencyColumnStoreConfig("docs", *base, def, 0)
			if err != nil {
				t.Fatal(err)
			}
			logical, _ := columnStoreSemanticLogicalType(ColumnStoreValueUint32List)
			a := check("state adjacency", state.Payload, state.PaddingBytes, state.Config, stateAdapter, typedcolumn.RawOrdinalColumnValues{Rows: n, Uint32List: list}, map[string]string{stateAdapter.Definition.Name: string(logical)})
			if state.OffsetsBytes != int64(a.DeclaredColumnOffsetsBytes) || state.ValuesBytes != int64(a.DeclaredColumnValuesBytes) {
				t.Fatal("state adjacency accounting differs")
			}
			source, err := prepareColumnVectorGraphAdjacencySourcePayloadFromList("unused", "docs", *base, def, 91, 0, list)
			if err != nil {
				t.Fatal(err)
			}
			_, sourceAdapter, err := columnVectorGraphAdjacencySourceColumnStoreConfig("docs", *base, def, 0)
			if err != nil {
				t.Fatal(err)
			}
			logical, _ = columnStoreSemanticLogicalType(ColumnStoreValueAdjacencyList)
			a = check("adjacency source", source.Payload, source.PaddingBytes, source.Config, sourceAdapter, typedcolumn.RawOrdinalColumnValues{Rows: n, Uint32List: list}, map[string]string{sourceAdapter.Definition.Name: string(logical)})
			if source.OffsetsBytes != int64(a.DeclaredColumnOffsetsBytes) || source.ValuesBytes != int64(a.DeclaredColumnValuesBytes) {
				t.Fatal("adjacency source accounting differs")
			}
			docs, err := prepareColumnVectorGraphDocumentIDStatePayload("docs", *base, def, 91, rows)
			if err != nil {
				t.Fatal(err)
			}
			_, docsAdapter, err := columnVectorGraphDocumentIDStateColumnStoreConfig("docs", *base, def)
			if err != nil {
				t.Fatal(err)
			}
			ids, err := buildColumnVectorGraphDocumentIDStateBytes(rows)
			if err != nil {
				t.Fatal(err)
			}
			logical, _ = columnStoreSemanticLogicalType(ColumnStoreValueBytes)
			a = check("document IDs", docs.Payload, docs.PaddingBytes, docs.Config, docsAdapter, typedcolumn.RawOrdinalColumnValues{Rows: n, Bytes: ids}, map[string]string{docsAdapter.Definition.Name: string(logical)})
			if docs.OffsetsBytes != int64(a.DeclaredColumnOffsetsBytes) || docs.ValuesBytes != int64(a.DeclaredColumnValuesBytes) {
				t.Fatal("document-ID accounting differs")
			}
			norms, err := prepareColumnVectorGraphInvNormStatePayload("docs", *base, def, 91, rows)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				if len(norms.Payload) != 0 {
					t.Fatal("empty inverse norms unexpectedly emitted")
				}
				return
			}
			_, normAdapter, err := columnVectorGraphInvNormStateColumnStoreConfig("docs", *base, def)
			if err != nil {
				t.Fatal(err)
			}
			normValues := make([]float32, n)
			for i := range normValues {
				normValues[i], err = columnVectorGraphInvNorm(rows[i].Vector)
				if err != nil {
					t.Fatal(err)
				}
			}
			check("inverse norms", norms.Payload, norms.PaddingBytes, norms.Config, normAdapter, typedcolumn.RawOrdinalColumnValues{Rows: n, Float32: normValues}, map[string]string{typedColumnAdapterPrimaryIDColumn: string(typedcolumn.ColumnTypeInt64), normAdapter.Definition.Name: string(ColumnStoreValueFloat32)})
			for _, field := range columnVectorGraphRowRefStateFields {
				values, err := columnVectorGraphRowRefStateValues(field, rows, 2)
				if err != nil {
					t.Fatal(err)
				}
				ref, err := prepareColumnVectorGraphRowRefStatePayloadFromValues("docs", *base, def, 91, field, values)
				if err != nil {
					t.Fatal(err)
				}
				_, adapter, err := columnVectorGraphRowRefStateColumnStoreConfig("docs", *base, def, field)
				if err != nil {
					t.Fatal(err)
				}
				check(string(field), ref.Payload, ref.PaddingBytes, ref.Config, adapter, typedcolumn.RawOrdinalColumnValues{Rows: n, Int64: values}, map[string]string{typedColumnAdapterPrimaryIDColumn: string(typedcolumn.ColumnTypeInt64), adapter.Definition.Name: string(ColumnStoreValueInt64)})
			}
		})
	}
}
