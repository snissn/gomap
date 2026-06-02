package collections

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/columnlayout"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

type typedColumnDirectViewMatrixKey struct {
	valueType       ColumnStoreValueType
	owner           typedColumnDirectViewStorageOwner
	consumer        typedColumnDirectViewConsumerPath
	adjacencyLayout typedColumnDirectViewAdjacencyLayout
}

func TestTypedColumnDirectViewAllTypeInventoryCoversCurrentValueTypesExactlyOnce(t *testing.T) {
	fromSource := currentColumnStoreValueTypesFromSource(t)
	counts := make(map[ColumnStoreValueType]int, len(fromSource))
	for _, valueType := range typedColumnDirectViewAllTypeInventory() {
		counts[valueType]++
	}
	for _, valueType := range fromSource {
		if counts[valueType] != 1 {
			t.Fatalf("ColumnStoreValueType %s inventory count=%d want exactly 1", valueType, counts[valueType])
		}
		delete(counts, valueType)
	}
	for valueType, count := range counts {
		if count != 0 {
			t.Fatalf("inventory includes unknown ColumnStoreValueType %s count=%d", valueType, count)
		}
	}
}

func TestTypedColumnDirectViewConformanceMatrixRowsAreExplicit(t *testing.T) {
	expected := map[typedColumnDirectViewMatrixKey]typedColumnDirectViewSupport{}
	for _, valueType := range typedColumnDirectViewAllTypeInventory() {
		support := typedColumnDirectViewFallbackOnly
		switch valueType {
		case ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble, ColumnStoreValueFloat32Vector, ColumnStoreValueByteVector, ColumnStoreValuePackedBitVector, ColumnStoreValuePackedUint2Vector, ColumnStoreValuePackedUint4Vector, ColumnStoreValueUint32List, ColumnStoreValueBytes,
			ColumnStoreValueInt8, ColumnStoreValueUint8, ColumnStoreValueInt16, ColumnStoreValueUint16, ColumnStoreValueInt32, ColumnStoreValueUint32, ColumnStoreValueUint64, ColumnStoreValueFloat16, ColumnStoreValueBFloat16:
			support = typedColumnDirectViewActiveLittleEndianCandidate
		case ColumnStoreValueAdjacencyList:
			support = typedColumnDirectViewDeferredFallbackOnly
		}
		if columnStoreValueTypeIsDenseNumericVector(valueType) {
			support = typedColumnDirectViewActiveLittleEndianCandidate
		}
		layout := typedColumnDirectViewAdjacencyLayoutNone
		rowAssetLayout := typedColumnDirectViewAdjacencyLayoutNone
		if valueType == ColumnStoreValueAdjacencyList {
			layout = typedColumnDirectViewAdjacencyLayoutRawUint32Dense
			rowAssetLayout = typedColumnDirectViewAdjacencyLayoutPhysicalRowAsset
		}
		expected[typedColumnDirectViewMatrixKey{valueType: valueType, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, adjacencyLayout: layout}] = support
		expected[typedColumnDirectViewMatrixKey{valueType: valueType, owner: typedColumnDirectViewStoragePhysicalRowAsset, consumer: typedColumnDirectViewConsumerRowAssetGeneric, adjacencyLayout: rowAssetLayout}] = typedColumnDirectViewDeferredFallbackOnly
	}
	expected[typedColumnDirectViewMatrixKey{valueType: ColumnStoreValueFloat32Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerColumnGraphTypedVector}] = typedColumnDirectViewActiveLittleEndianCandidate
	expected[typedColumnDirectViewMatrixKey{valueType: ColumnStoreValueAdjacencyList, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets, adjacencyLayout: typedColumnDirectViewAdjacencyLayoutRawUint32Offsets}] = typedColumnDirectViewActiveLittleEndianCandidate
	expected[typedColumnDirectViewMatrixKey{valueType: ColumnStoreValueAdjacencyList, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerColumnGraphTypedAdjacency, adjacencyLayout: typedColumnDirectViewAdjacencyLayoutRawUint32Offsets}] = typedColumnDirectViewDeferredFallbackOnly
	expected[typedColumnDirectViewMatrixKey{valueType: ColumnStoreValueFloat32Vector, owner: typedColumnDirectViewStoragePhysicalRowAsset, consumer: typedColumnDirectViewConsumerRowAssetVector}] = typedColumnDirectViewDeferredFallbackOnly
	expected[typedColumnDirectViewMatrixKey{valueType: ColumnStoreValueAdjacencyList, owner: typedColumnDirectViewStoragePhysicalRowAsset, consumer: typedColumnDirectViewConsumerRowAssetAdjacency, adjacencyLayout: typedColumnDirectViewAdjacencyLayoutPhysicalRowAsset}] = typedColumnDirectViewDeferredFallbackOnly

	seen := make(map[typedColumnDirectViewMatrixKey]bool, len(expected))
	for _, row := range typedColumnDirectViewConformanceMatrix() {
		key := typedColumnDirectViewMatrixKey{valueType: row.ValueType, owner: row.StorageOwner, consumer: row.Consumer, adjacencyLayout: row.AdjacencyLayout}
		want, ok := expected[key]
		if !ok {
			t.Fatalf("unexpected broad/nonsensical direct-view matrix row: %+v", row)
		}
		if seen[key] {
			t.Fatalf("duplicate direct-view matrix row: %+v", row)
		}
		seen[key] = true
		if row.Support != want {
			t.Fatalf("matrix row %+v support=%s want %s", key, row.Support, want)
		}
	}
	for key := range expected {
		if !seen[key] {
			t.Fatalf("missing explicit direct-view matrix row: %+v", key)
		}
	}
}

func TestTypedColumnDirectViewOwnershipMatrix(t *testing.T) {
	tests := []struct {
		name      string
		valueType ColumnStoreValueType
		owner     typedColumnDirectViewStorageOwner
		consumer  typedColumnDirectViewConsumerPath
		support   typedColumnDirectViewSupport
		endian    string
		size      int
		align     int
		followUp  int
	}{
		{name: "typed int64", valueType: ColumnStoreValueInt64, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 8, align: 8},
		{name: "typed native float32", valueType: ColumnStoreValueFloat32, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 4, align: 4},
		{name: "typed native double", valueType: ColumnStoreValueDouble, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 8, align: 8},
		{name: "typed primitive int8", valueType: ColumnStoreValueInt8, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 1, align: 1},
		{name: "typed primitive uint8", valueType: ColumnStoreValueUint8, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 1, align: 1},
		{name: "typed primitive int16", valueType: ColumnStoreValueInt16, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 2, align: 2},
		{name: "typed primitive uint16", valueType: ColumnStoreValueUint16, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 2, align: 2},
		{name: "typed primitive int32", valueType: ColumnStoreValueInt32, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 4, align: 4},
		{name: "typed primitive uint32", valueType: ColumnStoreValueUint32, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 4, align: 4},
		{name: "typed primitive uint64", valueType: ColumnStoreValueUint64, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 8, align: 8},
		{name: "typed primitive float16 bits", valueType: ColumnStoreValueFloat16, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 2, align: 2},
		{name: "typed primitive bfloat16 bits", valueType: ColumnStoreValueBFloat16, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 2, align: 2},
		{name: "typed vector", valueType: ColumnStoreValueFloat32Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 4, align: 4},
		{name: "typed dense uint8 vector", valueType: ColumnStoreValueUint8Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 1, align: 1},
		{name: "typed dense bfloat16 vector", valueType: ColumnStoreValueBFloat16Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 2, align: 2},
		{name: "typed dense uint32 vector", valueType: ColumnStoreValueUint32Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 4, align: 4},
		{name: "typed dense float64 vector", valueType: ColumnStoreValueFloat64Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 8, align: 8},
		{name: "typed uint32 list", valueType: ColumnStoreValueUint32List, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 4, align: 4},
		{name: "typed bytes", valueType: ColumnStoreValueBytes, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 1, align: 1},
		{name: "typed byte vector", valueType: ColumnStoreValueByteVector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 1, align: 1},
		{name: "typed packed bit vector", valueType: ColumnStoreValuePackedBitVector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 1, align: 1},
		{name: "typed packed uint2 vector", valueType: ColumnStoreValuePackedUint2Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 1, align: 1},
		{name: "typed packed uint4 vector", valueType: ColumnStoreValuePackedUint4Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 1, align: 1},
		{name: "bytes is not column graph vector source", valueType: ColumnStoreValueBytes, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerColumnGraphTypedVector, support: typedColumnDirectViewFallbackOnly},
		{name: "bytes is not adjacency offsets source", valueType: ColumnStoreValueBytes, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets, support: typedColumnDirectViewFallbackOnly},
		{name: "uint32 list is not column graph vector source", valueType: ColumnStoreValueUint32List, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerColumnGraphTypedVector, support: typedColumnDirectViewFallbackOnly},
		{name: "uint32 list is not adjacency offsets source", valueType: ColumnStoreValueUint32List, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets, support: typedColumnDirectViewFallbackOnly},
		{name: "column graph typed vector source", valueType: ColumnStoreValueFloat32Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerColumnGraphTypedVector, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 4, align: 4},
		{name: "typed adapter offsets-list adjacency source", valueType: ColumnStoreValueAdjacencyList, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 4, align: 4},
		{name: "bool fallback", valueType: ColumnStoreValueBool, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewFallbackOnly},
		{name: "string fallback", valueType: ColumnStoreValueString, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewFallbackOnly},
		{name: "dense adjacency compatibility deferred", valueType: ColumnStoreValueAdjacencyList, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewDeferredFallbackOnly, endian: "little", size: 4, align: 4, followUp: 1901},
		{name: "row asset vector deferred", valueType: ColumnStoreValueFloat32Vector, owner: typedColumnDirectViewStoragePhysicalRowAsset, consumer: typedColumnDirectViewConsumerRowAssetVector, support: typedColumnDirectViewDeferredFallbackOnly, followUp: 1897},
		{name: "row asset adjacency deferred", valueType: ColumnStoreValueAdjacencyList, owner: typedColumnDirectViewStoragePhysicalRowAsset, consumer: typedColumnDirectViewConsumerRowAssetAdjacency, support: typedColumnDirectViewDeferredFallbackOnly, followUp: 1897},
		{name: "row asset generic deferred", valueType: ColumnStoreValueInt64, owner: typedColumnDirectViewStoragePhysicalRowAsset, consumer: typedColumnDirectViewConsumerRowAssetGeneric, support: typedColumnDirectViewDeferredFallbackOnly, followUp: 1897},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := typedColumnDirectViewClassificationFor(tc.valueType, tc.owner, tc.consumer)
			if got.Support != tc.support || got.PayloadEndian != tc.endian || got.ElementSize != tc.size || got.Alignment != tc.align || got.FollowUpIssue != tc.followUp {
				t.Fatalf("classification=%+v want support=%s endian=%q size=%d align=%d follow_up=%d", got, tc.support, tc.endian, tc.size, tc.align, tc.followUp)
			}
		})
	}
}

func TestTypedColumnLayoutDescriptorPackedOverflowZerosDerivedGeometry1931(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	column := typedColumnAdapterColumn{
		Field: TypedStorageField{
			Name:           "codes",
			Path:           "codes",
			Owner:          TypedStorageOwnerColumnPart,
			ValueType:      ColumnStoreValuePackedUint4Vector,
			ElementsPerRow: maxInt/4 + 1,
			BitsPerElement: 4,
		},
		Definition: typedcolumn.ColumnDefinition{
			Name:               "codes",
			Type:               typedcolumn.ColumnTypePackedUint4Vector,
			Encoding:           typedcolumn.EncodingRawPackedUint4Vector,
			Compression:        typedcolumn.CompressionNone,
			FixedWidthElements: maxInt/4 + 1,
			BitsPerElement:     4,
		},
	}
	desc := typedColumnLayoutDescriptorForAdapterColumn(column)
	if desc.BytesPerRow != 0 || desc.LogicalBitsPerRow != 0 {
		t.Fatalf("descriptor=%+v want overflow to leave derived row geometry zero", desc)
	}
	caps := columnlayout.CapabilitiesFor(desc)
	if cap := caps.Supports(columnlayout.OpDirectView); cap.Supported() || cap.Reason != columnlayout.ReasonLengthMultipleMismatch {
		t.Fatalf("overflow direct cap=%+v want %s", cap, columnlayout.ReasonLengthMultipleMismatch)
	}
}

func TestTypedColumnDirectViewFixedAndPackedRequireRowGeometry1931(t *testing.T) {
	byteVector := typedColumnDirectViewClassificationFor(ColumnStoreValueByteVector, typedColumnDirectViewStorageTypedColumnPart, typedColumnDirectViewConsumerTypedColumnPartGeneric)
	if byteVector.Support != typedColumnDirectViewActiveLittleEndianCandidate || !byteVector.RequiresElementsPerRow {
		t.Fatalf("byte_vector classification=%+v want active direct candidate requiring row geometry", byteVector)
	}
	packed := typedColumnDirectViewClassificationFor(ColumnStoreValuePackedUint4Vector, typedColumnDirectViewStorageTypedColumnPart, typedColumnDirectViewConsumerTypedColumnPartGeneric)
	if packed.Support != typedColumnDirectViewActiveLittleEndianCandidate || !packed.RequiresElementsPerRow {
		t.Fatalf("packed classification=%+v want active direct candidate requiring row geometry", packed)
	}
	bytes := typedColumnDirectViewClassificationFor(ColumnStoreValueBytes, typedColumnDirectViewStorageTypedColumnPart, typedColumnDirectViewConsumerTypedColumnPartGeneric)
	if bytes.RequiresElementsPerRow {
		t.Fatalf("bytes classification=%+v must remain offsets/value geometry, not fixed-row geometry", bytes)
	}
}

func TestTypedColumnDirectViewAdjacencyOffsetsListSpecClassification(t *testing.T) {
	offsets := typedColumnDirectViewClassificationForAdjacencyLayout(ColumnStoreValueAdjacencyList, typedColumnDirectViewStorageTypedColumnPart, typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets, typedColumnDirectViewAdjacencyLayoutRawUint32Offsets)
	if offsets.Support != typedColumnDirectViewActiveLittleEndianCandidate || offsets.AdjacencyLayout != typedColumnDirectViewAdjacencyLayoutRawUint32Offsets {
		t.Fatalf("offsets-list classification=%+v want explicit active raw_uint32_offsets_list adapter path", offsets)
	}
	if offsets.RequiresElementsPerRow {
		t.Fatalf("offsets-list classification=%+v must not use fixed padded row degree", offsets)
	}
	if offsets.OffsetsElementSize != 8 || offsets.OffsetsAlignment != 8 || offsets.ValuesElementSize != 4 || offsets.ValuesAlignment != 4 {
		t.Fatalf("offsets-list alignment table=%+v want offsets uint64/8-byte and values uint32/4-byte", offsets)
	}
	if !strings.Contains(offsets.Reason, "uint64 offsets") || !strings.Contains(offsets.Reason, "uint32 values") {
		t.Fatalf("offsets-list reason=%q must name uint64 offsets and uint32 values", offsets.Reason)
	}

	dense := typedColumnDirectViewClassificationForAdjacencyLayout(ColumnStoreValueAdjacencyList, typedColumnDirectViewStorageTypedColumnPart, typedColumnDirectViewConsumerTypedColumnPartGeneric, typedColumnDirectViewAdjacencyLayoutRawUint32Dense)
	if dense.Support != typedColumnDirectViewDeferredFallbackOnly || !dense.RequiresElementsPerRow || dense.AdjacencyLayout != typedColumnDirectViewAdjacencyLayoutRawUint32Dense {
		t.Fatalf("dense adjacency classification=%+v want fixed-degree dense fallback/compatibility", dense)
	}
	if strings.Contains(dense.Reason, "uint64 offsets") {
		t.Fatalf("dense adjacency reason=%q must not be silently reclassified as offsets-list", dense.Reason)
	}
}

func TestTypedColumnDirectViewAdjacencyOffsetsListUsesExplicitValueTypeExtension(t *testing.T) {
	if ColumnAdjacencyListLayoutUint32OffsetsList == ColumnAdjacencyListLayoutFixedDense {
		t.Fatal("offsets-list selector must be distinct from fixed dense default")
	}
	if _, err := normalizeColumnStoreValueType(ColumnStoreValueAdjacencyList); err != nil {
		t.Fatalf("ColumnStoreValueAdjacencyList must remain the value type: %v", err)
	}
	if _, err := normalizeColumnStoreValueType(ColumnStoreValueType("uint32_offsets_list")); err == nil {
		t.Fatal("uint32_offsets_list must not become a new ColumnStoreValueType")
	}

	field := TypedStorageField{Name: "neighbors", Path: "neighbors", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueAdjacencyList, AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList}
	layout, err := NormalizeTypedStorageLayout(TypedStorageLayout{Collection: "events", Fields: []TypedStorageField{field}})
	if err != nil {
		t.Fatalf("NormalizeTypedStorageLayout offsets-list selector: %v", err)
	}
	if got := layout.Fields[0].AdjacencyLayout; got != ColumnAdjacencyListLayoutUint32OffsetsList {
		t.Fatalf("adjacency_layout=%q want %q", got, ColumnAdjacencyListLayoutUint32OffsetsList)
	}
	if layout.Fields[0].AdjacencyDegree != 0 {
		t.Fatalf("offsets-list selector used adjacency_degree=%d; missing degree must not mean dense row width", layout.Fields[0].AdjacencyDegree)
	}
}

func TestTypedColumnDirectViewActiveRowsAreCertifiedSetOnly(t *testing.T) {
	expectedActive := map[typedColumnDirectViewMatrixKey]bool{
		{valueType: ColumnStoreValueInt64, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                         true,
		{valueType: ColumnStoreValueFloat32, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                       true,
		{valueType: ColumnStoreValueDouble, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                        true,
		{valueType: ColumnStoreValueFloat32Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                 true,
		{valueType: ColumnStoreValueUint8Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                   true,
		{valueType: ColumnStoreValueInt8Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                    true,
		{valueType: ColumnStoreValueUint16Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                  true,
		{valueType: ColumnStoreValueInt16Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                   true,
		{valueType: ColumnStoreValueUint32Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                  true,
		{valueType: ColumnStoreValueInt32Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                   true,
		{valueType: ColumnStoreValueUint64Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                  true,
		{valueType: ColumnStoreValueInt64Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                   true,
		{valueType: ColumnStoreValueFloat16Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                 true,
		{valueType: ColumnStoreValueBFloat16Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                true,
		{valueType: ColumnStoreValueFloat64Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                 true,
		{valueType: ColumnStoreValueUint32List, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                    true,
		{valueType: ColumnStoreValueBytes, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                         true,
		{valueType: ColumnStoreValueByteVector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                    true,
		{valueType: ColumnStoreValuePackedBitVector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                               true,
		{valueType: ColumnStoreValuePackedUint2Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                             true,
		{valueType: ColumnStoreValuePackedUint4Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                             true,
		{valueType: ColumnStoreValueInt8, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                          true,
		{valueType: ColumnStoreValueUint8, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                         true,
		{valueType: ColumnStoreValueInt16, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                         true,
		{valueType: ColumnStoreValueUint16, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                        true,
		{valueType: ColumnStoreValueInt32, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                         true,
		{valueType: ColumnStoreValueUint32, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                        true,
		{valueType: ColumnStoreValueUint64, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                        true,
		{valueType: ColumnStoreValueFloat16, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                       true,
		{valueType: ColumnStoreValueBFloat16, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric}:                                                                                      true,
		{valueType: ColumnStoreValueFloat32Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerColumnGraphTypedVector}:                                                                                 true,
		{valueType: ColumnStoreValueAdjacencyList, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets, adjacencyLayout: typedColumnDirectViewAdjacencyLayoutRawUint32Offsets}: true,
	}
	seen := make(map[typedColumnDirectViewMatrixKey]bool, len(expectedActive))
	for _, row := range typedColumnDirectViewConformanceMatrix() {
		key := typedColumnDirectViewMatrixKey{valueType: row.ValueType, owner: row.StorageOwner, consumer: row.Consumer, adjacencyLayout: row.AdjacencyLayout}
		if row.Support == typedColumnDirectViewActiveLittleEndianCandidate {
			if !expectedActive[key] {
				t.Fatalf("unexpected active direct-view row: %+v", row)
			}
			seen[key] = true
		}
	}
	for key := range expectedActive {
		if !seen[key] {
			t.Fatalf("missing active direct-view row: %+v", key)
		}
	}
}

func TestTypedColumnDirectViewRowAssetAndAdjacencyGuardrails(t *testing.T) {
	for _, row := range typedColumnDirectViewConformanceMatrix() {
		rowAsset := row.StorageOwner == typedColumnDirectViewStoragePhysicalRowAsset || row.Consumer == typedColumnDirectViewConsumerRowAssetGeneric || row.Consumer == typedColumnDirectViewConsumerRowAssetVector || row.Consumer == typedColumnDirectViewConsumerRowAssetAdjacency
		adjacency := row.ValueType == ColumnStoreValueAdjacencyList || row.Consumer == typedColumnDirectViewConsumerRowAssetAdjacency
		if rowAsset {
			if row.Support != typedColumnDirectViewDeferredFallbackOnly || !typedColumnDirectViewHasFollowUp(row, 1897) {
				t.Fatalf("row-asset row=%+v want deferred fallback linked to #1897", row)
			}
		}
		if adjacency {
			activeOffsetsAdapter := row.StorageOwner == typedColumnDirectViewStorageTypedColumnPart && row.Consumer == typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets && row.AdjacencyLayout == typedColumnDirectViewAdjacencyLayoutRawUint32Offsets
			if activeOffsetsAdapter {
				if row.Support != typedColumnDirectViewActiveLittleEndianCandidate {
					t.Fatalf("offsets-list adapter adjacency row=%+v want active direct-view candidate", row)
				}
			} else if row.Support != typedColumnDirectViewDeferredFallbackOnly || !typedColumnDirectViewHasFollowUp(row, 1901) {
				t.Fatalf("adjacency row=%+v want deferred fallback linked to #1901", row)
			}
			if row.StorageOwner == typedColumnDirectViewStorageTypedColumnPart && row.AdjacencyLayout == typedColumnDirectViewAdjacencyLayoutNone {
				t.Fatalf("typed-column adjacency row=%+v missing explicit adjacency layout selector", row)
			}
		}
	}
	if columnVectorGraphBlockViewAdjacencyDirectView(nil) {
		t.Fatal("column_graph physical row adjacency unexpectedly enabled direct-view certification before #1897/#1901")
	}

	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	field.AdjacencyDegree = 2
	field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{
		{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{1, 2}},
		{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{3, 4}},
	})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage adjacency: %v", err)
	}
	cert, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage adjacency: %v", err)
	}
	column, ok := cert.Column("neighbors")
	if !ok {
		t.Fatal("missing adjacency contract column")
	}
	if cert.DirectViewCertified != 0 || column.DirectViewCertified {
		t.Fatalf("adjacency direct-view certified=%d column=%+v want fallback-only/deferred until #1901", cert.DirectViewCertified, column)
	}
}

func TestTypedColumnDirectViewSafetyChecksAndCounterVocabulary(t *testing.T) {
	placements := map[typedColumnDirectViewCheckPlacement]bool{}
	checkNames := map[string]bool{}
	for _, check := range typedColumnDirectViewSafetyChecks() {
		placements[check.Placement] = true
		checkNames[check.Name] = true
	}
	for _, placement := range []typedColumnDirectViewCheckPlacement{typedColumnDirectViewReadTime, typedColumnDirectViewCertificationTime, typedColumnDirectViewFallbackPolicy, typedColumnDirectViewDeferredPolicy} {
		if !placements[placement] {
			t.Fatalf("missing direct-view safety-check placement %s", placement)
		}
	}
	for _, name := range []string{"mappedresource source support", "offsets-list offset count is exactly row_count+1", "offsets-list offsets start at zero and are monotonic", "offsets-list final offset exactly matches uint32 value count", "offsets-list offsets fit Go int slice ranges before indexing", "absolute asset+offsets-section storage offset alignment", "absolute asset+values-section storage offset alignment"} {
		if !checkNames[name] {
			t.Fatalf("missing offsets-list safety check %q", name)
		}
	}
	gotCounters := make(map[typeddecode.Counter]bool)
	for _, counter := range typeddecode.CounterVocabulary() {
		gotCounters[counter] = true
	}
	for _, counter := range []typeddecode.Counter{
		typeddecode.CounterMmapDirectView,
		typeddecode.CounterOffsetsMmapDirectView,
		typeddecode.CounterValuesMmapDirectView,
		typeddecode.CounterHeapCopyTypedView,
		typeddecode.CounterOffsetsHeapCopyTypedView,
		typeddecode.CounterValuesHeapCopyTypedView,
		typeddecode.CounterScratchDecode,
		typeddecode.CounterStreamingFallback,
		typeddecode.CounterSourceUnsupported,
		typeddecode.CounterCertificationFailure,
		typeddecode.CounterAbsoluteOffsetUnaligned,
		typeddecode.CounterActualPointerUnaligned,
		typeddecode.CounterStaleHandle,
	} {
		if !gotCounters[counter] {
			t.Fatalf("missing counter vocabulary token %s", counter)
		}
	}
	if !gotCounters[typeddecode.CounterOffsetsListValidation] {
		t.Fatalf("missing offsets-list validation counter vocabulary token")
	}
	gotReasons := make(map[typeddecode.Reason]bool)
	for _, reason := range typeddecode.ReasonVocabulary() {
		gotReasons[reason] = true
	}
	for _, reason := range []typeddecode.Reason{typeddecode.ReasonOffsetsCountMismatch, typeddecode.ReasonOffsetsStartMismatch, typeddecode.ReasonOffsetsNonMonotonic, typeddecode.ReasonOffsetsGoIntRange, typeddecode.ReasonValuesLengthMismatch} {
		if !gotReasons[reason] {
			t.Fatalf("missing offsets-list reason token %s", reason)
		}
	}
}

func TestTypedColumnDirectViewLittleEndianByteFixtures(t *testing.T) {
	int64Raw := make([]byte, 16)
	binary.LittleEndian.PutUint64(int64Raw[0:8], uint64(0x0102030405060708))
	binary.LittleEndian.PutUint64(int64Raw[8:16], 0xfffffffffffffffe)
	if want := []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}; !slices.Equal(int64Raw, want) {
		t.Fatalf("int64 little-endian bytes=%x want %x", int64Raw, want)
	}
	if big := make([]byte, 8); func() bool {
		binary.BigEndian.PutUint64(big, uint64(0x0102030405060708))
		return slices.Equal(big, int64Raw[:8])
	}() {
		t.Fatalf("big-endian int64 bytes unexpectedly matched little-endian fixture")
	}

	float32Bits := []uint32{0x00000000, 0x80000000, 0x7f800000, 0xff800000, 0x7f7fffff, 0xff7fffff, 0x7fc00001, 0x7fa12345}
	float32Raw := make([]byte, len(float32Bits)*4)
	for i, bits := range float32Bits {
		value := math.Float32frombits(bits)
		binary.LittleEndian.PutUint32(float32Raw[i*4:], math.Float32bits(value))
		if got := binary.LittleEndian.Uint32(float32Raw[i*4:]); got != bits {
			t.Fatalf("float32 fixture[%d] bits=%08x want %08x", i, got, bits)
		}
	}

	float64Bits := []uint64{0x0000000000000000, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7fefffffffffffff, 0xffefffffffffffff, 0x7ff8000000000001, 0x7ff123456789abcd}
	float64Raw := make([]byte, len(float64Bits)*8)
	for i, bits := range float64Bits {
		value := math.Float64frombits(bits)
		binary.LittleEndian.PutUint64(float64Raw[i*8:], math.Float64bits(value))
		if got := binary.LittleEndian.Uint64(float64Raw[i*8:]); got != bits {
			t.Fatalf("float64 fixture[%d] bits=%016x want %016x", i, got, bits)
		}
	}

	vectorValues := []float32{1, math.Float32frombits(0x80000000), math.Float32frombits(0x7fc01234), -2.5}
	vectorRaw := make([]byte, len(vectorValues)*4)
	for i, value := range vectorValues {
		binary.LittleEndian.PutUint32(vectorRaw[i*4:], math.Float32bits(value))
	}
	wantVector := []byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x80, 0x34, 0x12, 0xc0, 0x7f, 0x00, 0x00, 0x20, 0xc0}
	if !slices.Equal(vectorRaw, wantVector) {
		t.Fatalf("float32_vector little-endian bytes=%x want %x", vectorRaw, wantVector)
	}

	offsets := []uint64{0, 2, 2, 5}
	offsetsRaw := make([]byte, len(offsets)*8)
	for i, value := range offsets {
		binary.LittleEndian.PutUint64(offsetsRaw[i*8:], value)
	}
	wantOffsets := []byte{0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0}
	if !slices.Equal(offsetsRaw, wantOffsets) {
		t.Fatalf("uint32 offsets-list offsets little-endian bytes=%x want %x", offsetsRaw, wantOffsets)
	}
	values := []uint32{1, 2, 9, 10, 11}
	valuesRaw := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(valuesRaw[i*4:], value)
	}
	wantValues := []byte{1, 0, 0, 0, 2, 0, 0, 0, 9, 0, 0, 0, 10, 0, 0, 0, 11, 0, 0, 0}
	if !slices.Equal(valuesRaw, wantValues) {
		t.Fatalf("uint32 offsets-list values little-endian bytes=%x want %x", valuesRaw, wantValues)
	}
}

func TestTypedColumnRawInt64FloatCarriersAreNotNativeScalarDirectViews(t *testing.T) {
	for _, valueType := range []ColumnStoreValueType{ColumnStoreValueFloat32, ColumnStoreValueDouble} {
		column, err := typedColumnAdapterMapField(typedColumnAdapterField(string(valueType), valueType))
		if err != nil {
			t.Fatalf("typedColumnAdapterMapField(%s): %v", valueType, err)
		}
		if column.Definition.Type != typedcolumn.ColumnTypeInt64 || column.Definition.Encoding != typedcolumn.EncodingRawInt64 {
			t.Fatalf("%s current carrier type/encoding=(%s,%s), want raw int64 compatibility carrier", valueType, column.Definition.Type, column.Definition.Encoding)
		}
		caps := typedColumnLayoutCapabilitiesForAdapterColumn(column)
		if caps.DirectView.Eligible {
			t.Fatalf("%s raw-int64 carrier advertised direct view: %+v", valueType, caps.DirectView)
		}
		capability, err := typedColumnAdapterCapability(column, columnsemantics.OpDirectScalarValueCarrier)
		if err != nil {
			t.Fatalf("typedColumnAdapterCapability(%s): %v", valueType, err)
		}
		if capability.Status != columnsemantics.StatusUnsupported || capability.Reason != columnsemantics.ReasonFloatRawInt64BitPattern {
			t.Fatalf("%s direct scalar capability=%+v want raw-int64 rejection", valueType, capability)
		}
	}
}

func TestTypedColumnNativeScalarFloatFixedWidthCandidates(t *testing.T) {
	cases := []struct {
		valueType ColumnStoreValueType
		wantType  typedcolumn.ColumnType
		wantEnc   typedcolumn.Encoding
	}{
		{valueType: ColumnStoreValueFloat32, wantType: typedcolumn.ColumnTypeFloat32, wantEnc: typedcolumn.EncodingRawFloat32},
		{valueType: ColumnStoreValueDouble, wantType: typedcolumn.ColumnTypeFloat64, wantEnc: typedcolumn.EncodingRawFloat64},
	}
	for _, tc := range cases {
		field := typedColumnAdapterField(string(tc.valueType), tc.valueType)
		field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
		column, err := typedColumnAdapterMapField(field)
		if err != nil {
			t.Fatalf("typedColumnAdapterMapField(%s little_endian): %v", tc.valueType, err)
		}
		if column.Definition.Type != tc.wantType || column.Definition.Encoding != tc.wantEnc {
			t.Fatalf("%s native type/encoding=(%s,%s), want (%s,%s)", tc.valueType, column.Definition.Type, column.Definition.Encoding, tc.wantType, tc.wantEnc)
		}
		caps := typedColumnLayoutCapabilitiesForAdapterColumn(column)
		if !caps.DirectView.Eligible || caps.DirectView.Reason != columnlayout.ReasonSupported {
			t.Fatalf("%s native direct-view candidate caps=%+v", tc.valueType, caps.DirectView)
		}
	}
}

func TestTypedColumnDirectViewWriterStorageAccounting1895(t *testing.T) {
	cases := []struct {
		fixture      string
		field        TypedStorageField
		values       []columnDeclaredValue
		rows         int
		dims         int
		wantDirect   bool
		wantFallback bool
		wantDeferred bool
		note         string
	}{
		{
			fixture:      "bool_bitpack_rle",
			field:        typedColumnAdapterField("flag", ColumnStoreValueBool),
			values:       []columnDeclaredValue{{Type: ColumnStoreValueBool, Present: true, Bool: true}, {Type: ColumnStoreValueBool, Present: true, Bool: false}, {Type: ColumnStoreValueBool, Present: true, Bool: true}},
			rows:         3,
			wantFallback: true,
			note:         "fallback_only_bool_bitpack_rle",
		},
		{
			fixture: "int64_raw_fixed_width",
			field: func() TypedStorageField {
				field := typedColumnAdapterField("count", ColumnStoreValueInt64)
				field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
				return field
			}(),
			values:     []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 10}, {Type: ColumnStoreValueInt64, Present: true, Int64: 20}, {Type: ColumnStoreValueInt64, Present: true, Int64: 30}},
			rows:       3,
			wantDirect: true,
			note:       "raw_int64_direct_view_certified",
		},
		{
			fixture: "float32_native_raw_fixed_width",
			field: func() TypedStorageField {
				field := typedColumnAdapterField("score32", ColumnStoreValueFloat32)
				field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
				return field
			}(),
			values:     []columnDeclaredValue{{Type: ColumnStoreValueFloat32, Present: true, Float32: 1.25}, {Type: ColumnStoreValueFloat32, Present: true, Float32: -0}, {Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x7fc01234)}},
			rows:       3,
			wantDirect: true,
			note:       "native_raw_float32_direct_view_certified",
		},
		{
			fixture: "double_native_raw_fixed_width",
			field: func() TypedStorageField {
				field := typedColumnAdapterField("score64", ColumnStoreValueDouble)
				field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
				return field
			}(),
			values:     []columnDeclaredValue{{Type: ColumnStoreValueDouble, Present: true, Double: 1.25}, {Type: ColumnStoreValueDouble, Present: true, Double: -0}, {Type: ColumnStoreValueDouble, Present: true, Double: math.Float64frombits(0x7ff8000000001234)}},
			rows:       3,
			wantDirect: true,
			note:       "native_raw_float64_direct_view_certified",
		},
		{
			fixture:      "string_dictionary_codes",
			field:        typedColumnAdapterField("kind", ColumnStoreValueString),
			values:       []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: "alpha"}, {Type: ColumnStoreValueString, Present: true, String: "beta"}, {Type: ColumnStoreValueString, Present: true, String: "alpha"}},
			rows:         3,
			wantFallback: true,
			note:         "fallback_only_string_dictionary",
		},
		{
			fixture: "float32_vector_fixed_dim",
			field: func() TypedStorageField {
				field := typedColumnAdapterField("embedding", ColumnStoreValueFloat32Vector)
				field.VectorDims = 3
				return field
			}(),
			values:     []columnDeclaredValue{{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 0, 0}}, {Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{0, 1, 0}}, {Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{0, 0, 1}}},
			rows:       3,
			dims:       3,
			wantDirect: true,
			note:       "row_major_raw_float32_vector_direct_view_certified",
		},
		{
			fixture: "adjacency_deferred",
			field: func() TypedStorageField {
				field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
				field.AdjacencyDegree = 2
				return field
			}(),
			values:       []columnDeclaredValue{{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{1, 2}}, {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{2, 3}}, {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{3, 4}}},
			rows:         3,
			dims:         2,
			wantFallback: true,
			wantDeferred: true,
			note:         "legacy_dense_adjacency_deferred_no_direct_view_certification",
		},
		{
			fixture: "adjacency_offsets_list_variable",
			field: func() TypedStorageField {
				field := typedColumnAdapterField("neighbors_offsets", ColumnStoreValueAdjacencyList)
				field.AdjacencyLayout = ColumnAdjacencyListLayoutUint32OffsetsList
				return field
			}(),
			values:     []columnDeclaredValue{{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: nil}, {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{2, 3}}, {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{3, 4, 5}}},
			rows:       3,
			wantDirect: true,
			note:       "raw_uint32_offsets_list_variable_adjacency_direct_view_certified",
		},
	}
	for _, tc := range cases {
		t.Run(tc.fixture, func(t *testing.T) {
			part := typedColumnAdapterBuildPart(t, tc.field, tc.values)
			image, err := part.buildImage()
			if err != nil {
				t.Fatalf("buildImage: %v", err)
			}
			accounting := part.Part.ByteAccountingFromImage(image)
			cert, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
			if err != nil {
				t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
			}
			column, ok := cert.Column(tc.field.Name)
			if !ok {
				t.Fatalf("missing contract column %q", tc.field.Name)
			}
			consumer := typedColumnDirectViewConsumerTypedColumnPartGeneric
			adjacencyLayout := typedColumnDirectViewAdjacencyLayoutNone
			if tc.field.AdjacencyLayout == ColumnAdjacencyListLayoutUint32OffsetsList {
				consumer = typedColumnDirectViewConsumerTypedColumnPartAdjacencyOffsets
				adjacencyLayout = typedColumnDirectViewAdjacencyLayoutRawUint32Offsets
			}
			classification := typedColumnDirectViewClassificationForAdjacencyLayout(tc.field.ValueType, typedColumnDirectViewStorageTypedColumnPart, consumer, adjacencyLayout)
			if got := column.DirectViewCertified; got != tc.wantDirect {
				t.Fatalf("%s DirectViewCertified=%v want %v contract=%+v", tc.fixture, got, tc.wantDirect, column)
			}
			wantCertifiedColumns := 0
			if tc.wantDirect {
				wantCertifiedColumns = 1
			}
			if cert.DirectViewCertified != wantCertifiedColumns {
				t.Fatalf("%s certified columns=%d want %d (declared target only; internal primary-id must not certify)", tc.fixture, cert.DirectViewCertified, wantCertifiedColumns)
			}
			if tc.wantDeferred && classification.Support != typedColumnDirectViewDeferredFallbackOnly {
				t.Fatalf("%s classification=%+v want deferred fallback", tc.fixture, classification)
			}
			if tc.wantFallback && column.DirectViewCertified {
				t.Fatalf("%s fallback fixture unexpectedly direct-certified: %+v", tc.fixture, column)
			}
			if accounting.SerializedImageBytes != image.TotalBytes() || accounting.SerializedPaddingBytes != image.PaddingBytes() || accounting.LayoutContractBytes != image.CategoryBytes(typedcolumn.ColumnPartImageCategoryLayoutContract) || accounting.TotalStoredBytes != image.TotalBytes() {
				t.Fatalf("%s accounting=%+v image_bytes=%d padding=%d contract=%d", tc.fixture, accounting, image.TotalBytes(), image.PaddingBytes(), image.CategoryBytes(typedcolumn.ColumnPartImageCategoryLayoutContract))
			}
			segmentPrefixPadding := 0
			if tc.wantDirect {
				segmentPrefixPadding = columnAssetSegmentPrefixPadding(1, typedColumnPartDirectViewAssetAlignment)
			}
			t.Logf("storage_table fixture=%s type=%s rows=%d dims=%d image_bytes=%d contract_bytes=%d image_padding_bytes=%d segment_prefix_padding_bytes=%d total_padding_bytes=%d direct_view_certified=%t fallback_only=%t deferred=%t note=%s", tc.fixture, tc.field.ValueType, tc.rows, tc.dims, image.TotalBytes(), accounting.LayoutContractBytes, accounting.SerializedPaddingBytes, segmentPrefixPadding, accounting.SerializedPaddingBytes+segmentPrefixPadding, column.DirectViewCertified, tc.wantFallback, tc.wantDeferred, tc.note)
		})
	}
}

func TestTypedColumnDirectViewFailClosedFixtures(t *testing.T) {
	cert := directViewConformanceVectorCert(2, 3)
	plan := typeddecode.DenseFloat32VectorPlan(cert, 3)
	validReq := typeddecode.DirectViewColumnRequest{Plan: plan, Certification: cert, Rows: 2, PayloadBytes: 24, AssetOffset: 0, HasAssetOffset: true}
	if status := typeddecode.ValidateDirectViewColumn(validReq); !status.Direct() {
		t.Fatalf("valid direct-view fixture status=%+v", status)
	}

	cases := []struct {
		name string
		edit func(*typedcolumn.ColumnPartLayoutContractColumn, *typeddecode.DirectViewColumnRequest)
		want typeddecode.Reason
	}{
		{name: "wrong endian", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn, _ *typeddecode.DirectViewColumnRequest) {
			c.Endian = typedcolumn.ColumnPartLayoutEndianCodecDefined
		}, want: typeddecode.ReasonWrongEndian},
		{name: "wrong length", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn, r *typeddecode.DirectViewColumnRequest) {
			c.Section.Length = 23
			c.Blocks[0].PayloadLength = 23
			c.Blocks[0].RawBytes = 23
			c.Blocks[0].StoredBytes = 23
			r.PayloadBytes = 23
		}, want: typeddecode.ReasonLengthMultipleMismatch},
		{name: "wrong row count", edit: func(_ *typedcolumn.ColumnPartLayoutContractColumn, r *typeddecode.DirectViewColumnRequest) {
			r.Rows = 3
		}, want: typeddecode.ReasonRowCountMismatch},
		{name: "wrong dims", edit: func(_ *typedcolumn.ColumnPartLayoutContractColumn, r *typeddecode.DirectViewColumnRequest) {
			r.Plan = typeddecode.DenseFloat32VectorPlan(cert, 2)
		}, want: typeddecode.ReasonDimensionMismatch},
		{name: "absolute offset unaligned", edit: func(_ *typedcolumn.ColumnPartLayoutContractColumn, r *typeddecode.DirectViewColumnRequest) {
			r.AssetOffset = 1
		}, want: typeddecode.ReasonAbsoluteOffsetUnaligned},
		{name: "missing absolute offset", edit: func(_ *typedcolumn.ColumnPartLayoutContractColumn, r *typeddecode.DirectViewColumnRequest) {
			r.HasAssetOffset = false
		}, want: typeddecode.ReasonAbsoluteOffsetUnaligned},
		{name: "nullable wrapper", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn, _ *typeddecode.DirectViewColumnRequest) {
			c.NullMaskPresent = true
			c.NullCount = 1
		}, want: typeddecode.ReasonNullableWrapper},
		{name: "default wrapper", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn, _ *typeddecode.DirectViewColumnRequest) {
			c.DefaultMaskPresent = true
			c.DefaultCount = 1
		}, want: typeddecode.ReasonNullableWrapper},
		{name: "compressed", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn, _ *typeddecode.DirectViewColumnRequest) {
			c.Compression = typedcolumn.CompressionSnappy
		}, want: typeddecode.ReasonCompressed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := cloneConformanceCert(cert)
			req := validReq
			req.Certification = candidate
			tc.edit(&candidate, &req)
			req.Certification = candidate
			status := typeddecode.ValidateDirectViewColumn(req)
			if status.Reason != tc.want {
				t.Fatalf("status=%+v want reason %s", status, tc.want)
			}
		})
	}

	deltaCert := typedcolumn.ColumnPartLayoutContractColumn{Name: "v", LogicalType: "int64", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone, Rows: 2, StreamingCertified: true, Endian: typedcolumn.ColumnPartLayoutEndianCodecDefined}
	deltaPlan := typeddecode.Int64ReducerPlan(typedColumnLayoutCapabilitiesForAdapterColumn(typedColumnAdapterColumn{Field: typedColumnAdapterField("v", ColumnStoreValueInt64), Definition: typedcolumn.ColumnDefinition{Name: "v", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone}}), deltaCert)
	if deltaPlan.Path != typeddecode.PathStreaming || deltaPlan.Reason != typeddecode.ReasonVariableWidth {
		t.Fatalf("delta plan=%+v want streaming fallback", deltaPlan)
	}

	t.Run("multi asset second candidate requires segment padding", func(t *testing.T) {
		firstAsset := validReq
		firstAsset.AssetOffset = 0
		if status := typeddecode.ValidateDirectViewColumn(firstAsset); !status.Direct() {
			t.Fatalf("first asset status=%+v want direct", status)
		}
		secondAsset := validReq
		secondAsset.AssetOffset = 25 // preceding asset length without appender padding misaligns the next asset.
		if status := typeddecode.ValidateDirectViewColumn(secondAsset); status.Reason != typeddecode.ReasonAbsoluteOffsetUnaligned {
			t.Fatalf("second asset status=%+v want %s", status, typeddecode.ReasonAbsoluteOffsetUnaligned)
		}
	})

	if got := typedColumnDirectViewClassificationFor(ColumnStoreValueBool, typedColumnDirectViewStorageTypedColumnPart, typedColumnDirectViewConsumerTypedColumnPartGeneric); got.Support != typedColumnDirectViewFallbackOnly {
		t.Fatalf("bool classification=%+v want fallback-only", got)
	}
	if got := typedColumnDirectViewClassificationFor(ColumnStoreValueString, typedColumnDirectViewStorageTypedColumnPart, typedColumnDirectViewConsumerTypedColumnPartGeneric); got.Support != typedColumnDirectViewFallbackOnly {
		t.Fatalf("string classification=%+v want fallback-only", got)
	}
	if got := typedColumnDirectViewClassificationFor(ColumnStoreValueInt64, typedColumnDirectViewStoragePhysicalRowAsset, typedColumnDirectViewConsumerRowAssetGeneric); got.Support != typedColumnDirectViewDeferredFallbackOnly || got.FollowUpIssue != 1897 {
		t.Fatalf("row asset classification=%+v want #1897 deferred", got)
	}
	if got := typedColumnDirectViewClassificationFor(ColumnStoreValueAdjacencyList, typedColumnDirectViewStorageTypedColumnPart, typedColumnDirectViewConsumerTypedColumnPartGeneric); got.Support != typedColumnDirectViewDeferredFallbackOnly || got.FollowUpIssue != 1901 {
		t.Fatalf("adjacency classification=%+v want #1901 deferred", got)
	}
}

func TestTypedColumnScalarFloatDirectViewReaderValidation1896(t *testing.T) {
	cases := []struct {
		name      string
		cert      typedcolumn.ColumnPartLayoutContractColumn
		plan      func(typedcolumn.ColumnPartLayoutContractColumn) typeddecode.Plan
		elemBytes int
	}{
		{name: "float32", cert: directViewConformanceScalarCert("score32", "float32", typedcolumn.ColumnTypeFloat32, typedcolumn.EncodingRawFloat32, 3, 4), plan: typeddecode.Float32ScalarPlan, elemBytes: 4},
		{name: "double", cert: directViewConformanceScalarCert("score64", "double", typedcolumn.ColumnTypeFloat64, typedcolumn.EncodingRawFloat64, 3, 8), plan: typeddecode.Float64ScalarPlan, elemBytes: 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := tc.plan(tc.cert)
			valid := typeddecode.DirectViewColumnRequest{Plan: plan, Certification: tc.cert, Rows: 3, PayloadBytes: 3 * tc.elemBytes, AssetOffset: 0, HasAssetOffset: true}
			if status := typeddecode.ValidateDirectViewColumn(valid); !status.Direct() {
				t.Fatalf("valid %s status=%+v want direct", tc.name, status)
			}
			checks := []struct {
				name string
				edit func(*typedcolumn.ColumnPartLayoutContractColumn, *typeddecode.DirectViewColumnRequest)
				want typeddecode.Reason
			}{
				{name: "wrong endian", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn, _ *typeddecode.DirectViewColumnRequest) {
					c.Endian = typedcolumn.ColumnPartLayoutEndianCodecDefined
				}, want: typeddecode.ReasonWrongEndian},
				{name: "wrong length", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn, r *typeddecode.DirectViewColumnRequest) {
					c.Section.Length--
					c.Blocks[0].PayloadLength--
					c.Blocks[0].RawBytes--
					c.Blocks[0].StoredBytes--
					r.PayloadBytes--
				}, want: typeddecode.ReasonLengthMultipleMismatch},
				{name: "wrong row count", edit: func(_ *typedcolumn.ColumnPartLayoutContractColumn, r *typeddecode.DirectViewColumnRequest) {
					r.Rows = 2
				}, want: typeddecode.ReasonRowCountMismatch},
				{name: "absolute offset unaligned", edit: func(_ *typedcolumn.ColumnPartLayoutContractColumn, r *typeddecode.DirectViewColumnRequest) {
					r.AssetOffset = 1
				}, want: typeddecode.ReasonAbsoluteOffsetUnaligned},
				{name: "missing absolute offset", edit: func(_ *typedcolumn.ColumnPartLayoutContractColumn, r *typeddecode.DirectViewColumnRequest) {
					r.HasAssetOffset = false
				}, want: typeddecode.ReasonAbsoluteOffsetUnaligned},
				{name: "nullable", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn, _ *typeddecode.DirectViewColumnRequest) {
					c.NullMaskPresent = true
					c.NullCount = 1
				}, want: typeddecode.ReasonNullableWrapper},
				{name: "default", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn, _ *typeddecode.DirectViewColumnRequest) {
					c.DefaultMaskPresent = true
					c.DefaultCount = 1
				}, want: typeddecode.ReasonNullableWrapper},
				{name: "compressed", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn, _ *typeddecode.DirectViewColumnRequest) {
					c.Compression = typedcolumn.CompressionSnappy
				}, want: typeddecode.ReasonCompressed},
				{name: "not certified", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn, _ *typeddecode.DirectViewColumnRequest) {
					c.DirectViewCertified = false
				}, want: typeddecode.ReasonNotWriterCertified},
			}
			for _, check := range checks {
				t.Run(check.name, func(t *testing.T) {
					cert := cloneConformanceCert(tc.cert)
					req := valid
					req.Certification = cert
					check.edit(&cert, &req)
					req.Certification = cert
					req.Plan = tc.plan(cert)
					if status := typeddecode.ValidateDirectViewColumn(req); status.Reason != check.want {
						t.Fatalf("status=%+v want %s", status, check.want)
					}
				})
			}
		})
	}
	rawInt64FloatCarrier := typedcolumn.ColumnPartLayoutContractColumn{Name: "score", LogicalType: "float32", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, Rows: 1, Section: typedcolumn.ColumnPartLayoutContractSection{Length: 8}, ElementSize: 8, Alignment: 8, Endian: typedcolumn.ColumnPartLayoutEndianLittle, LengthMultiple: 8, DirectViewCertified: true, Blocks: []typedcolumn.ColumnPartLayoutContractBlock{{RowCount: 1, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, RawBytes: 8, StoredBytes: 8, PayloadLength: 8}}}
	if plan := typeddecode.Float32ScalarPlan(rawInt64FloatCarrier); plan.DirectCandidate() {
		t.Fatalf("raw-int64 logical float carrier plan=%+v want fallback/non-direct", plan)
	}
}

func TestTypedColumnDirectViewActualPointerStaleAndChecksumFailures(t *testing.T) {
	cert := directViewConformanceVectorCert(2, 3)
	plan := typeddecode.DenseFloat32VectorPlan(cert, 3)
	req := typeddecode.DirectViewColumnRequest{Plan: plan, Certification: cert, Rows: 2, PayloadBytes: 24, AssetOffset: 0, HasAssetOffset: true}
	mgr := mappedresource.NewManager()
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "direct-view-conformance", Namespace: "direct-view-conformance"}

	misalignedRaw := directViewConformanceAlignedBytes(4, 25)[1:25]
	misaligned, err := mgr.AcquireBytes(mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: scope.Namespace, Kind: "column", Generation: 1, PartID: 1, FileID: 1, Length: int64(len(misalignedRaw))}, scope, mappedresource.SourceMapped, misalignedRaw, mappedresource.AcquireOptions{Reason: "misaligned"})
	if err != nil {
		t.Fatalf("AcquireBytes misaligned: %v", err)
	}
	if _, status := typeddecode.DenseFloat32VectorView(mgr, misaligned, req, typeddecode.ResourceViewOptions{ExpectedElements: 6, RequireMapped: true}); status.Reason != typeddecode.ReasonActualPointerUnaligned {
		t.Fatalf("actual pointer status=%+v want %s", status, typeddecode.ReasonActualPointerUnaligned)
	}
	_ = misaligned.Release()

	alignedRaw := directViewConformanceAlignedBytes(4, 24)
	aligned, err := mgr.AcquireBytes(mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: scope.Namespace, Kind: "column", Generation: 1, PartID: 2, FileID: 1, Length: int64(len(alignedRaw))}, scope, mappedresource.SourceMapped, alignedRaw, mappedresource.AcquireOptions{Reason: "stale"})
	if err != nil {
		t.Fatalf("AcquireBytes aligned: %v", err)
	}
	if err := aligned.Release(); err != nil {
		t.Fatalf("Release aligned: %v", err)
	}
	if _, status := typeddecode.DenseFloat32VectorView(mgr, aligned, req, typeddecode.ResourceViewOptions{ExpectedElements: 6, RequireMapped: true}); status.Reason != typeddecode.ReasonStaleHandle {
		t.Fatalf("stale status=%+v want %s", status, typeddecode.ReasonStaleHandle)
	}

	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 1}, {Type: ColumnStoreValueInt64, Present: true, Int64: 2}})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	corrupt := image
	corrupt.Bytes = append([]byte(nil), image.Bytes...)
	section := typedColumnAdapterFindColumnSection(t, corrupt, "count")
	corrupt.Bytes[section.Offset] ^= 0xff
	if _, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(corrupt); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("checksum mismatch err=%v want checksum fail-closed", err)
	}
	old := image
	old.Sections = append([]typedcolumn.ColumnPartImageSection(nil), image.Sections...)
	for i, section := range old.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionLayoutContract {
			old.Sections = append(old.Sections[:i], old.Sections[i+1:]...)
			break
		}
	}
	if _, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(old); err == nil || !strings.Contains(err.Error(), "pre-alpha typed-column assets must be rebuilt") {
		t.Fatalf("missing contract err=%v want old/non-certified fail-closed", err)
	}
}

func typedColumnDirectViewHasFollowUp(row typedColumnDirectViewClassification, issue int) bool {
	if row.FollowUpIssue == issue {
		return true
	}
	return slices.Contains(row.FollowUpIssues, issue)
}

func currentColumnStoreValueTypesFromSource(t *testing.T) []ColumnStoreValueType {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(file), "column_store.go"))
	if err != nil {
		t.Fatalf("read column_store.go: %v", err)
	}
	re := regexp.MustCompile(`(?m)^\s*(ColumnStoreValue[A-Za-z0-9]+)\s+ColumnStoreValueType\s*=\s*"([^"]+)"`)
	matches := re.FindAllSubmatch(raw, -1)
	if len(matches) == 0 {
		t.Fatalf("no ColumnStoreValueType constants found")
	}
	out := make([]ColumnStoreValueType, 0, len(matches))
	for _, match := range matches {
		out = append(out, ColumnStoreValueType(match[2]))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func directViewConformanceScalarCert(name string, logical string, columnType typedcolumn.ColumnType, encoding typedcolumn.Encoding, rows int, elemBytes int) typedcolumn.ColumnPartLayoutContractColumn {
	bytes := rows * elemBytes
	return typedcolumn.ColumnPartLayoutContractColumn{
		Name: name, LogicalType: logical, Type: columnType, Encoding: encoding, Compression: typedcolumn.CompressionNone,
		Rows: rows, Section: typedcolumn.ColumnPartLayoutContractSection{Offset: 8, Length: bytes}, ElementSize: elemBytes, Alignment: elemBytes, Endian: typedcolumn.ColumnPartLayoutEndianLittle, LengthMultiple: elemBytes, DirectViewCertified: true,
		Blocks: []typedcolumn.ColumnPartLayoutContractBlock{{FirstRow: 0, RowCount: rows, Encoding: encoding, Compression: typedcolumn.CompressionNone, RawBytes: bytes, StoredBytes: bytes, PayloadOffset: 8, PayloadLength: bytes}},
	}
}

func directViewConformanceVectorCert(rows, dims int) typedcolumn.ColumnPartLayoutContractColumn {
	bytes := rows * dims * 4
	return typedcolumn.ColumnPartLayoutContractColumn{
		Name: "embedding", LogicalType: "float32_vector", Type: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector, Compression: typedcolumn.CompressionNone,
		Rows: rows, Section: typedcolumn.ColumnPartLayoutContractSection{Offset: 8, Length: bytes}, FixedWidthElements: dims, ElementSize: 4, Alignment: 4, Endian: typedcolumn.ColumnPartLayoutEndianLittle, LengthMultiple: 4, DirectViewCertified: true,
		Blocks: []typedcolumn.ColumnPartLayoutContractBlock{{FirstRow: 0, RowCount: rows, Encoding: typedcolumn.EncodingRawFloat32Vector, Compression: typedcolumn.CompressionNone, RawBytes: bytes, StoredBytes: bytes, PayloadOffset: 8, PayloadLength: bytes}},
	}
}

func cloneConformanceCert(cert typedcolumn.ColumnPartLayoutContractColumn) typedcolumn.ColumnPartLayoutContractColumn {
	cert.Blocks = append([]typedcolumn.ColumnPartLayoutContractBlock(nil), cert.Blocks...)
	return cert
}

func directViewConformanceAlignedBytes(align int, n int) []byte {
	buf := make([]byte, n+align)
	base := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
	off := int((uintptr(align) - base%uintptr(align)) % uintptr(align))
	return buf[off : off+n]
}
