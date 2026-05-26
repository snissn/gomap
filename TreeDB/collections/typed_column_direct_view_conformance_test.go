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

func TestTypedColumnDirectViewConformanceMatrixCoversCurrentValueTypes(t *testing.T) {
	fromSource := currentColumnStoreValueTypesFromSource(t)
	seen := make(map[ColumnStoreValueType]bool, len(fromSource))
	for _, row := range typedColumnDirectViewConformanceMatrix() {
		if row.StorageOwner == typedColumnDirectViewStorageTypedColumnPart && row.Consumer == typedColumnDirectViewConsumerTypedColumnPartGeneric {
			seen[row.ValueType] = true
		}
	}
	for _, valueType := range fromSource {
		if !seen[valueType] {
			t.Fatalf("ColumnStoreValueType %s missing explicit direct-view classification", valueType)
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
		{name: "typed vector", valueType: ColumnStoreValueFloat32Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 4, align: 4},
		{name: "column graph typed vector source", valueType: ColumnStoreValueFloat32Vector, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerColumnGraphTypedVector, support: typedColumnDirectViewActiveLittleEndianCandidate, endian: "little", size: 4, align: 4},
		{name: "bool fallback", valueType: ColumnStoreValueBool, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewFallbackOnly},
		{name: "string fallback", valueType: ColumnStoreValueString, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewFallbackOnly},
		{name: "adjacency deferred", valueType: ColumnStoreValueAdjacencyList, owner: typedColumnDirectViewStorageTypedColumnPart, consumer: typedColumnDirectViewConsumerTypedColumnPartGeneric, support: typedColumnDirectViewDeferredFallbackOnly, endian: "little", size: 4, align: 4, followUp: 1901},
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

func TestTypedColumnDirectViewSafetyChecksAndCounterVocabulary(t *testing.T) {
	placements := map[typedColumnDirectViewCheckPlacement]bool{}
	for _, check := range typedColumnDirectViewSafetyChecks() {
		placements[check.Placement] = true
	}
	for _, placement := range []typedColumnDirectViewCheckPlacement{typedColumnDirectViewReadTime, typedColumnDirectViewCertificationTime, typedColumnDirectViewFallbackPolicy, typedColumnDirectViewDeferredPolicy} {
		if !placements[placement] {
			t.Fatalf("missing direct-view safety-check placement %s", placement)
		}
	}
	gotCounters := make(map[typeddecode.Counter]bool)
	for _, counter := range typeddecode.CounterVocabulary() {
		gotCounters[counter] = true
	}
	for _, counter := range []typeddecode.Counter{
		typeddecode.CounterMmapDirectView,
		typeddecode.CounterHeapCopyTypedView,
		typeddecode.CounterScratchDecode,
		typeddecode.CounterStreamingFallback,
		typeddecode.CounterCertificationFailure,
		typeddecode.CounterAbsoluteOffsetUnaligned,
		typeddecode.CounterActualPointerUnaligned,
		typeddecode.CounterStaleHandle,
	} {
		if !gotCounters[counter] {
			t.Fatalf("missing counter vocabulary token %s", counter)
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
