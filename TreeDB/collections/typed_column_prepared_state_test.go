package collections

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

func TestTypedColumnPreparedStateNonInt64DependencyDescriptions(t *testing.T) {
	span, err := typedcolumn.NewRowSpan(0, 128)
	if err != nil {
		t.Fatalf("NewRowSpan: %v", err)
	}

	stringField := TypedStorageField{Name: "kind", Path: "kind", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueString}
	stringPlan, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:               stringField,
		Role:                typedcolumn.ColumnRolePredicate,
		Operation:           columnsemantics.OpDictionaryEquality,
		IncludeDictionaries: true,
		IncludePruning:      true,
	}, span)
	if err != nil {
		t.Fatalf("describe string dictionary column: %v", err)
	}
	if !stringPlan.Capability.Supported() || stringPlan.Logical != columnsemantics.LogicalString || stringPlan.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode {
		t.Fatalf("string plan=%+v want supported low-cardinality string dictionary plan", stringPlan)
	}
	assertPreparedPlanDependency(t, stringPlan, typedcolumn.SectionDependencyValues)
	assertPreparedPlanDependency(t, stringPlan, typedcolumn.SectionDependencyDictionaries)
	assertPreparedPlanDependency(t, stringPlan, typedcolumn.SectionDependencyPruningMetadata)

	fallbackStringRange, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:     stringField,
		Role:      typedcolumn.ColumnRolePredicate,
		Operation: columnsemantics.OpOrderedRange,
	}, span)
	if err != nil {
		t.Fatalf("describe fallback string range: %v", err)
	}
	if fallbackStringRange.Capability.Status != columnsemantics.StatusFallback || fallbackStringRange.Capability.Reason != columnsemantics.ReasonDictionaryOrderUnproven {
		t.Fatalf("fallback string range capability=%+v want #1846 dictionary-order fallback status", fallbackStringRange.Capability)
	}
	if len(fallbackStringRange.Dependencies) != 0 {
		t.Fatalf("fallback string range deps=%+v want no hot-path dependencies", fallbackStringRange.Dependencies)
	}

	nullableInt := TypedStorageField{Name: "maybe_count", Path: "maybe_count", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64, Nullable: true}
	nullPlan, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:     nullableInt,
		Role:      typedcolumn.ColumnRolePredicate,
		Operation: columnsemantics.OpIsNull,
	}, span)
	if err != nil {
		t.Fatalf("describe nullable is-null: %v", err)
	}
	if !nullPlan.Capability.Supported() {
		t.Fatalf("nullable is-null capability=%+v want supported mask operation", nullPlan.Capability)
	}
	assertPreparedPlanDependency(t, nullPlan, typedcolumn.SectionDependencyNullMask)
	assertPreparedPlanDependency(t, nullPlan, typedcolumn.SectionDependencyDefaultMask)
	nullableSum, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:     nullableInt,
		Role:      typedcolumn.ColumnRoleMeasure,
		Operation: columnsemantics.OpSum,
	}, span)
	if err != nil {
		t.Fatalf("describe nullable sum: %v", err)
	}
	if nullableSum.Capability.Status != columnsemantics.StatusFallback || nullableSum.Capability.Reason != columnsemantics.ReasonNullableCarrierAggregateSemantics || len(nullableSum.Dependencies) != 0 {
		t.Fatalf("nullable sum plan=%+v want #1843 fallback before treating carriers as values", nullableSum)
	}

	boolPlan, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:          TypedStorageField{Name: "flag", Path: "flag", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueBool},
		Role:           typedcolumn.ColumnRolePredicate,
		Operation:      columnsemantics.OpEquality,
		IncludePruning: true,
	}, span)
	if err != nil {
		t.Fatalf("describe bool equality: %v", err)
	}
	if !boolPlan.Capability.Supported() || boolPlan.Logical != columnsemantics.LogicalBool || boolPlan.Definition.Type != typedcolumn.ColumnTypeBool {
		t.Fatalf("bool plan=%+v want supported bool predicate plan", boolPlan)
	}

	primitiveCases := []struct {
		valueType ColumnStoreValueType
		logical   columnsemantics.LogicalType
		physical  typedcolumn.ColumnType
	}{
		{ColumnStoreValueInt8, columnsemantics.LogicalInt8, typedcolumn.ColumnTypeInt8},
		{ColumnStoreValueUint8, columnsemantics.LogicalUint8, typedcolumn.ColumnTypeUint8},
		{ColumnStoreValueInt16, columnsemantics.LogicalInt16, typedcolumn.ColumnTypeInt16},
		{ColumnStoreValueUint16, columnsemantics.LogicalUint16, typedcolumn.ColumnTypeUint16},
		{ColumnStoreValueInt32, columnsemantics.LogicalInt32, typedcolumn.ColumnTypeInt32},
		{ColumnStoreValueUint32, columnsemantics.LogicalUint32, typedcolumn.ColumnTypeUint32},
		{ColumnStoreValueUint64, columnsemantics.LogicalUint64, typedcolumn.ColumnTypeUint64},
		{ColumnStoreValueFloat16, columnsemantics.LogicalFloat16, typedcolumn.ColumnTypeFloat16},
		{ColumnStoreValueBFloat16, columnsemantics.LogicalBFloat16, typedcolumn.ColumnTypeBFloat16},
	}
	for _, tc := range primitiveCases {
		primitivePlan, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
			Field:     TypedStorageField{Name: "v", Path: "v", Owner: TypedStorageOwnerColumnPart, ValueType: tc.valueType},
			Role:      typedcolumn.ColumnRoleProjection,
			Operation: columnsemantics.OpDirectScalarValueCarrier,
		}, span)
		if err != nil {
			t.Fatalf("describe primitive %s direct scalar: %v", tc.valueType, err)
		}
		if !primitivePlan.Capability.Supported() || !primitivePlan.LayoutCapability.Supported() || primitivePlan.Logical != tc.logical || primitivePlan.Definition.Type != tc.physical {
			t.Fatalf("primitive %s plan=%+v want supported prepared direct scalar plan", tc.valueType, primitivePlan)
		}
		assertPreparedPlanDependency(t, primitivePlan, typedcolumn.SectionDependencyValues)
	}

	vectorPlan, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:                TypedStorageField{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: 4},
		Role:                 typedcolumn.ColumnRoleProjection,
		Operation:            columnsemantics.OpAllRows,
		IncludeVectorPayload: true,
	}, span)
	if err != nil {
		t.Fatalf("describe vector payload: %v", err)
	}
	if !vectorPlan.Capability.Supported() || vectorPlan.Definition.Type != typedcolumn.ColumnTypeFloat32Vector {
		t.Fatalf("vector plan=%+v want supported all-rows vector payload descriptor", vectorPlan)
	}
	assertPreparedPlanDependency(t, vectorPlan, typedcolumn.SectionDependencyVectorPayload)
	assertPreparedPlanNoDependency(t, vectorPlan, typedcolumn.SectionDependencyValues)

	adjacencyPlan, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:                   TypedStorageField{Name: "neighbors", Path: "neighbors", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueAdjacencyList, AdjacencyDegree: 8},
		Role:                    typedcolumn.ColumnRoleProjection,
		Operation:               columnsemantics.OpAllRows,
		IncludeAdjacencyPayload: true,
	}, span)
	if err != nil {
		t.Fatalf("describe adjacency payload: %v", err)
	}
	if !adjacencyPlan.Capability.Supported() || adjacencyPlan.Definition.Type != typedcolumn.ColumnTypeAdjacencyList {
		t.Fatalf("adjacency plan=%+v want supported all-rows adjacency payload descriptor", adjacencyPlan)
	}
	assertPreparedPlanDependency(t, adjacencyPlan, typedcolumn.SectionDependencyAdjacencyPayload)
	assertPreparedPlanNoDependency(t, adjacencyPlan, typedcolumn.SectionDependencyValues)

	uint32ListPlan, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:     TypedStorageField{Name: "tags", Path: "tags", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueUint32List},
		Role:      typedcolumn.ColumnRoleProjection,
		Operation: columnsemantics.OpUint32ListDirectPayload,
	}, span)
	if err != nil {
		t.Fatalf("describe uint32_list prepared payload: %v", err)
	}
	if uint32ListPlan.Capability.Status != columnsemantics.StatusUnsupported || uint32ListPlan.Capability.Reason != columnsemantics.ReasonUnknownLogicalType || len(uint32ListPlan.Dependencies) != 0 {
		t.Fatalf("uint32_list prepared plan=%+v want fail-closed until split-section prepared dependency support exists", uint32ListPlan)
	}

	vectorScalar, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:     TypedStorageField{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: 4},
		Role:      typedcolumn.ColumnRoleMeasure,
		Operation: columnsemantics.OpSum,
	}, span)
	if err != nil {
		t.Fatalf("describe vector scalar sum: %v", err)
	}
	if vectorScalar.Capability.Status != columnsemantics.StatusUnsupported || vectorScalar.Capability.Reason != columnsemantics.ReasonVectorScalarOperationUnsupported {
		t.Fatalf("vector scalar capability=%+v want #1843 unsupported scalar operation", vectorScalar.Capability)
	}
}

func TestTypedColumnPreparedFastDecodeSourceCounters1896(t *testing.T) {
	var diag TypedColumnInt64PredicateScanDiagnostics
	recordTypedColumnInt64FastDecodePlan(&diag, typeddecode.Plan{Path: typeddecode.PathStreaming, Reason: typeddecode.ReasonNotWriterCertified})
	if diag.FastDecodeStreamingPlans != 1 || diag.FastDecodeCertificationFailure != 1 || diag.FastDecodeFallbackReason != string(typeddecode.ReasonNotWriterCertified) {
		t.Fatalf("streaming plan counters=%+v want certification streaming plan", diag)
	}
	recordTypedColumnInt64ScratchDecode(&diag, typeddecode.ReasonNotWriterCertified)
	if diag.FastDecodeScratchDecodes != 1 || diag.FastDecodeStreamingFallbacks != 1 || diag.FastDecodeFallbackReason != string(typeddecode.ReasonNotWriterCertified) {
		t.Fatalf("scratch counters=%+v want certification streaming fallback", diag)
	}
	recordTypedColumnInt64DirectViewStatus(&diag, typeddecode.StreamingStatus(typeddecode.ReasonAbsoluteOffsetUnaligned, "absolute"))
	if diag.DirectViewFailures != 1 || diag.FastDecodeAbsoluteUnaligned != 1 || diag.FastDecodeStreamingFallbacks != 1 {
		t.Fatalf("absolute counters=%+v want direct-view failure without scratch fallback", diag)
	}
	recordTypedColumnInt64ScratchDecode(&diag, typeddecode.ReasonAbsoluteOffsetUnaligned)
	if diag.FastDecodeScratchDecodes != 2 || diag.FastDecodeStreamingFallbacks != 2 {
		t.Fatalf("absolute scratch counters=%+v want streaming fallback", diag)
	}
	recordTypedColumnInt64DirectViewStatus(&diag, typeddecode.StreamingStatus(typeddecode.ReasonActualPointerUnaligned, "actual"))
	if diag.DirectViewFailures != 2 || diag.FastDecodeActualUnaligned != 1 {
		t.Fatalf("actual counters=%+v want actual pointer counter", diag)
	}
	recordTypedColumnInt64DirectViewStatus(&diag, typeddecode.UnsupportedStatus(typeddecode.ReasonStaleHandle, "stale"))
	if diag.DirectViewFailures != 3 || diag.FastDecodeStaleHandles != 1 || diag.FastDecodeStreamingFallbacks != 2 {
		t.Fatalf("stale counters=%+v want stale counter without fallback", diag)
	}

	mgr := mappedresource.NewManager()
	h, err := mgr.AcquireBytes(mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: "test", Kind: "counter", Generation: 1, PartID: 1, FileID: 1, Length: 8}, mappedresource.Scope{Kind: mappedresource.ScopePreparedQuery, ID: "counter", Namespace: "test"}, mappedresource.SourceMapped, make([]byte, 8), mappedresource.AcquireOptions{Reason: "counter"})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	defer func() { _ = h.Release() }()
	recordTypedColumnInt64DirectViewStatusWithHandle(&diag, typeddecode.DirectStatus(), h)
	if diag.DirectViewSuccesses != 1 || diag.FastDecodeMmapDirectViews != 1 || diag.FastDecodeHeapCopyTypedViews != 0 {
		t.Fatalf("direct counters=%+v want one mmap direct view", diag)
	}
	heap, err := mgr.AcquireBytes(mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: "test", Kind: "counter", Generation: 1, PartID: 2, FileID: 1, Length: 8}, mappedresource.Scope{Kind: mappedresource.ScopePreparedQuery, ID: "counter", Namespace: "test"}, mappedresource.SourceHeapCopy, make([]byte, 8), mappedresource.AcquireOptions{Reason: "counter heap"})
	if err != nil {
		t.Fatalf("AcquireBytes heap: %v", err)
	}
	defer func() { _ = heap.Release() }()
	recordTypedColumnInt64DirectViewStatusWithHandle(&diag, typeddecode.DirectStatus(), heap)
	if diag.DirectViewSuccesses != 2 || diag.FastDecodeMmapDirectViews != 1 || diag.FastDecodeHeapCopyTypedViews != 1 {
		t.Fatalf("heap counters=%+v want separate heap-copy typed view", diag)
	}
}

func TestTypedColumnPreparedVectorAndAdjacencyPayloadDependenciesC3(t *testing.T) {
	span, err := typedcolumn.NewRowSpan(0, 32)
	if err != nil {
		t.Fatalf("NewRowSpan: %v", err)
	}
	vectorPlan, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:                TypedStorageField{Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: 4},
		Role:                 typedcolumn.ColumnRoleProjection,
		Operation:            columnsemantics.OpAllRows,
		IncludeVectorPayload: true,
	}, span)
	if err != nil {
		t.Fatalf("describe vector payload: %v", err)
	}
	assertPreparedPlanDependency(t, vectorPlan, typedcolumn.SectionDependencyVectorPayload)
	assertPreparedPlanNoDependency(t, vectorPlan, typedcolumn.SectionDependencyValues)

	adjacencyPlan, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:                   TypedStorageField{Name: "neighbors", Path: "neighbors", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueAdjacencyList, AdjacencyDegree: 8},
		Role:                    typedcolumn.ColumnRoleProjection,
		Operation:               columnsemantics.OpAllRows,
		IncludeAdjacencyPayload: true,
	}, span)
	if err != nil {
		t.Fatalf("describe adjacency payload: %v", err)
	}
	assertPreparedPlanDependency(t, adjacencyPlan, typedcolumn.SectionDependencyAdjacencyPayload)
	assertPreparedPlanNoDependency(t, adjacencyPlan, typedcolumn.SectionDependencyValues)
}

func TestTypedColumnPreparedColumnStateAllowsZeroLengthEmptyBlock(t *testing.T) {
	plan := typedColumnPreparedColumnPlan{Definition: typedcolumn.ColumnDefinition{Name: "count", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone}}
	column := typedcolumn.ColumnPartColumn{
		Definition: plan.Definition,
		Blocks: []typedcolumn.ColumnBlock{{
			Descriptor: typedcolumn.ColumnBlockDescriptor{FirstRow: 0, RowCount: 0, StoredBytes: 0},
			Granule:    typedcolumn.EncodedGranule{RawBytes: 0},
		}},
	}
	section := typedcolumn.ColumnPartImageSection{Kind: typedcolumn.ColumnPartImageSectionColumnData, Column: "count", Offset: 128, Length: 0}
	state, diag, err := buildTypedColumnPreparedColumnState(plan, column, section, typedcolumn.ColumnPartLayoutContractColumn{}, nil)
	if err != nil {
		t.Fatalf("buildTypedColumnPreparedColumnState zero-length empty block: %v", err)
	}
	if len(state.BlockPlans) != 1 || state.BlockPlans[0].PayloadLength != 0 || !state.BlockPlans[0].CandidateSelection.IsEmpty() {
		t.Fatalf("zero-length block plan=%+v want one empty zero-payload block", state.BlockPlans)
	}
	if diag.BlocksPrepared != 1 || diag.PrunedBlocks != 1 || diag.CandidateBlocks != 0 || diag.CandidateRangeBytes != 0 {
		t.Fatalf("zero-length block diagnostics=%+v want one prepared/pruned block with no candidate bytes", diag)
	}

	column.Blocks[0].Descriptor.RowCount = 1
	_, _, err = buildTypedColumnPreparedColumnState(plan, column, section, typedcolumn.ColumnPartLayoutContractColumn{}, nil)
	if err == nil || !strings.Contains(err.Error(), "zero-length payload") {
		t.Fatalf("buildTypedColumnPreparedColumnState zero-length non-empty block err=%v want fail-closed zero-length payload", err)
	}
}

func TestTypedColumnPreparedStateMultiColumnMismatchFailsClosed(t *testing.T) {
	countField := typedColumnAdapterField("count", ColumnStoreValueInt64)
	kindField := typedColumnAdapterField("kind", ColumnStoreValueString)
	fields := []TypedStorageField{countField, kindField}
	rows := []typedColumnAdapterRow{
		{PrimaryID: 1, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 10}, "kind": {Type: ColumnStoreValueString, Present: true, String: "alpha"}}},
		{PrimaryID: 2, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 20}, "kind": {Type: ColumnStoreValueString, Present: true, String: "beta"}}},
	}
	adapterPart, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 91, RowsPerGranule: 2, Fields: fields}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := adapterPart.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	ref := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "test", Generation: 1, PartID: image.PartID, FileID: 1, Length: int64(len(image.Bytes))}
	physical := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: "test", Generation: 1, PartID: columnPhysicalRowAssetPartID, FileID: 1, Length: int64(len(image.Bytes))}
	readRange := func(offset int, length int, section bool) ([]byte, error) {
		if offset < 0 || length <= 0 || offset+length > len(image.Bytes) {
			t.Fatalf("range offset=%d length=%d outside image bytes=%d", offset, length, len(image.Bytes))
		}
		return image.Bytes[offset : offset+length], nil
	}
	duplicateRequests := []typedColumnPreparedColumnRequest{
		{Field: countField, Role: typedcolumn.ColumnRolePredicate, Operation: columnsemantics.OpAllRows, IncludePruning: true},
		{Field: countField, Role: typedcolumn.ColumnRoleMeasure, Operation: columnsemantics.OpSum},
	}
	part, _, err := typedColumnPreparePartStateFromRanges(ref, physical, image.Rows, image.Rows, fields, uint64(adapterPart.Part.Descriptor.SchemaVersion), duplicateRequests, readRange, nil)
	if err != nil {
		t.Fatalf("prepare duplicate predicate/measure column: %v", err)
	}
	if got := part.Columns["count"].Plan.Operation; got != columnsemantics.OpSum {
		t.Fatalf("duplicate column prepared operation=%s want measure operation %s", got, columnsemantics.OpSum)
	}
	request := []typedColumnPreparedColumnRequest{{Field: countField, Role: typedcolumn.ColumnRoleMeasure, Operation: columnsemantics.OpSum}}
	_, _, err = typedColumnPreparePartStateFromRanges(ref, physical, 0, image.Rows, fields, uint64(adapterPart.Part.Descriptor.SchemaVersion), request, readRange, nil)
	if err == nil || !strings.Contains(err.Error(), "image/ref mismatch") {
		t.Fatalf("prepare with typed manifest row mismatch err=%v want fail-closed row mismatch", err)
	}
	_, _, err = typedColumnPreparePartStateFromRanges(ref, physical, image.Rows, image.Rows+1, fields, uint64(adapterPart.Part.Descriptor.SchemaVersion), request, readRange, nil)
	if err == nil || !strings.Contains(err.Error(), "image/physical row mismatch") {
		t.Fatalf("prepare with physical row mismatch err=%v want fail-closed row mismatch", err)
	}

	parsedImage, desc, columns, manifestBytes, descriptorRaw, contractRaw, err := typedColumnPreparedReadImageAndDescriptor(ref, readRange)
	if err != nil {
		t.Fatalf("typedColumnPreparedReadImageAndDescriptor: %v", err)
	}
	missingKindSection := parsedImage
	missingKindSection.Sections = make([]typedcolumn.ColumnPartImageSection, 0, len(parsedImage.Sections))
	for _, section := range parsedImage.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Column == "kind" {
			continue
		}
		missingKindSection.Sections = append(missingKindSection.Sections, section)
	}
	_, _, err = typedColumnPreparePartStateFromParsed(ref, physical, image.Rows, image.Rows, fields, uint64(adapterPart.Part.Descriptor.SchemaVersion), missingKindSection, desc, columns, manifestBytes, descriptorRaw, contractRaw, request, nil)
	if err == nil || !strings.Contains(err.Error(), "missing column data section \"kind\"") {
		t.Fatalf("prepare missing unrelated string column section err=%v want fail-closed multi-column metadata mismatch", err)
	}
}

func TestTypedColumnPreparedRangeReadCacheBounds3417(t *testing.T) {
	raw := make([]byte, maxTypedColumnPreparedRangeCacheEntryBytes+4096)
	for i := range raw {
		raw[i] = byte(i)
	}
	calls := 0
	readRange := func(offset int, length int, section bool) ([]byte, error) {
		calls++
		if !section {
			t.Fatalf("section=%v want true", section)
		}
		if offset < 0 || length <= 0 || offset+length > len(raw) {
			t.Fatalf("range offset=%d length=%d outside bytes=%d", offset, length, len(raw))
		}
		return raw[offset : offset+length], nil
	}
	cache := newTypedColumnPreparedRangeReadCache(readRange)
	for i := 0; i < maxTypedColumnPreparedRangeCacheEntries+4; i++ {
		offset := i*16 + 1
		got, err := cache.read(offset, 4, true)
		if err != nil {
			t.Fatalf("cache read %d: %v", i, err)
		}
		if string(got) != string(raw[offset:offset+4]) {
			t.Fatalf("cache read %d returned wrong bytes", i)
		}
	}
	if cache.entryN != maxTypedColumnPreparedRangeCacheEntries {
		t.Fatalf("cache entries=%d want hard cap %d", cache.entryN, maxTypedColumnPreparedRangeCacheEntries)
	}
	callsBefore := calls
	if _, err := cache.read(1, 4, true); err != nil {
		t.Fatalf("cached first range: %v", err)
	}
	if calls != callsBefore {
		t.Fatalf("cached range caused read calls=%d want %d", calls, callsBefore)
	}
	uncachedOffset := (maxTypedColumnPreparedRangeCacheEntries+1)*16 + 1
	if _, err := cache.read(uncachedOffset, 4, true); err != nil {
		t.Fatalf("uncached overflow range: %v", err)
	}
	if calls != callsBefore+1 {
		t.Fatalf("uncached overflow calls=%d want %d", calls, callsBefore+1)
	}

	largeCalls := 0
	largeCache := newTypedColumnPreparedRangeReadCache(func(offset int, length int, section bool) ([]byte, error) {
		largeCalls++
		if offset < 0 || length <= 0 || offset+length > len(raw) {
			t.Fatalf("large range offset=%d length=%d outside bytes=%d", offset, length, len(raw))
		}
		return raw[offset : offset+length], nil
	})
	largeLen := maxTypedColumnPreparedRangeCacheEntryBytes + 1
	if _, err := largeCache.read(0, largeLen, true); err != nil {
		t.Fatalf("large first read: %v", err)
	}
	if largeCache.entryN != 0 {
		t.Fatalf("large read cached entries=%d want 0", largeCache.entryN)
	}
	if _, err := largeCache.read(0, largeLen, true); err != nil {
		t.Fatalf("large second read: %v", err)
	}
	if largeCalls != 2 {
		t.Fatalf("large read calls=%d want 2 uncached reads", largeCalls)
	}
}

func TestTypedColumnPreparedMetadataPrefetchCoalescesSmallDictionary3417(t *testing.T) {
	raw := make([]byte, 512)
	for i := range raw {
		raw[i] = byte(i)
	}
	image := typedcolumn.ColumnPartImage{Sections: []typedcolumn.ColumnPartImageSection{
		{Kind: typedcolumn.ColumnPartImageSectionDescriptor, Offset: 100, Length: 32},
		{Kind: typedcolumn.ColumnPartImageSectionDictionaries, Offset: 140, Length: 16},
	}}
	type readCall struct {
		offset int
		length int
	}
	var calls []readCall
	cache := newTypedColumnPreparedRangeReadCache(func(offset int, length int, section bool) ([]byte, error) {
		if !section {
			t.Fatalf("section=%v want true", section)
		}
		if offset < 0 || length <= 0 || offset+length > len(raw) {
			t.Fatalf("range offset=%d length=%d outside bytes=%d", offset, length, len(raw))
		}
		calls = append(calls, readCall{offset: offset, length: length})
		return raw[offset : offset+length], nil
	})
	requests := []typedColumnPreparedColumnRequest{{IncludeDictionaries: true}}
	if err := typedColumnPreparedPrefetchMetadataSections(image, requests, cache); err != nil {
		t.Fatalf("prefetch metadata: %v", err)
	}
	if len(calls) != 1 || calls[0] != (readCall{offset: 100, length: 56}) {
		t.Fatalf("prefetch calls=%+v want one coalesced descriptor+dictionary read", calls)
	}
	callsBefore := len(calls)
	if _, err := cache.read(100, 32, true); err != nil {
		t.Fatalf("cached descriptor read: %v", err)
	}
	if _, err := cache.read(140, 16, true); err != nil {
		t.Fatalf("cached dictionary read: %v", err)
	}
	if len(calls) != callsBefore {
		t.Fatalf("cached sections caused calls=%d want %d", len(calls), callsBefore)
	}
}

func TestTypedColumnPreparedMetadataPrefetchUsesFullSectionBudget3417(t *testing.T) {
	raw := make([]byte, 512)
	for i := range raw {
		raw[i] = byte(i)
	}
	image := typedcolumn.ColumnPartImage{Sections: []typedcolumn.ColumnPartImageSection{
		{Kind: typedcolumn.ColumnPartImageSectionDescriptor, Offset: 100, Length: 8},
		{Kind: typedcolumn.ColumnPartImageSectionSortKeyMetadata, Offset: 108, Length: 8},
		{Kind: typedcolumn.ColumnPartImageSectionSortKeyMarks, Offset: 116, Length: 8},
		{Kind: typedcolumn.ColumnPartImageSectionColumnStats, Offset: 124, Length: 8},
		{Kind: typedcolumn.ColumnPartImageSectionPruningMetadata, Offset: 132, Length: 8},
		{Kind: typedcolumn.ColumnPartImageSectionDictionaries, Offset: 140, Length: 8},
	}}
	type readCall struct {
		offset int
		length int
	}
	var calls []readCall
	cache := newTypedColumnPreparedRangeReadCache(func(offset int, length int, section bool) ([]byte, error) {
		if !section {
			t.Fatalf("section=%v want true", section)
		}
		if offset < 0 || length <= 0 || offset+length > len(raw) {
			t.Fatalf("range offset=%d length=%d outside bytes=%d", offset, length, len(raw))
		}
		calls = append(calls, readCall{offset: offset, length: length})
		return raw[offset : offset+length], nil
	})
	requests := []typedColumnPreparedColumnRequest{{
		IncludeDictionaries:    true,
		IncludeStats:           true,
		IncludePruning:         true,
		IncludeSortKeyMetadata: true,
		IncludeSortKeyMarks:    true,
	}}
	if err := typedColumnPreparedPrefetchMetadataSections(image, requests, cache); err != nil {
		t.Fatalf("prefetch metadata: %v", err)
	}
	if len(calls) != 1 || calls[0] != (readCall{offset: 100, length: 48}) {
		t.Fatalf("prefetch calls=%+v want one full-budget coalesced metadata read", calls)
	}
}

func TestTypedColumnPreparedMetadataPrefetchSkipsLargeDictionary3417(t *testing.T) {
	dictLen := maxTypedColumnPreparedRangeCacheEntryBytes + 1
	raw := make([]byte, 140+dictLen)
	for i := range raw {
		raw[i] = byte(i)
	}
	image := typedcolumn.ColumnPartImage{Sections: []typedcolumn.ColumnPartImageSection{
		{Kind: typedcolumn.ColumnPartImageSectionDescriptor, Offset: 100, Length: 32},
		{Kind: typedcolumn.ColumnPartImageSectionDictionaries, Offset: 140, Length: dictLen},
	}}
	type readCall struct {
		offset int
		length int
	}
	var calls []readCall
	cache := newTypedColumnPreparedRangeReadCache(func(offset int, length int, section bool) ([]byte, error) {
		if offset < 0 || length <= 0 || offset+length > len(raw) {
			t.Fatalf("range offset=%d length=%d outside bytes=%d", offset, length, len(raw))
		}
		calls = append(calls, readCall{offset: offset, length: length})
		return raw[offset : offset+length], nil
	})
	requests := []typedColumnPreparedColumnRequest{{IncludeDictionaries: true}}
	if err := typedColumnPreparedPrefetchMetadataSections(image, requests, cache); err != nil {
		t.Fatalf("prefetch metadata: %v", err)
	}
	if len(calls) != 0 {
		t.Fatalf("prefetch calls=%+v want no prefetch when dictionary exceeds cache entry cap", calls)
	}
	if _, err := cache.read(100, 32, true); err != nil {
		t.Fatalf("descriptor read: %v", err)
	}
	if cache.entryN != 1 {
		t.Fatalf("cache entries after descriptor=%d want 1", cache.entryN)
	}
	if _, err := cache.read(140, dictLen, true); err != nil {
		t.Fatalf("large dictionary read: %v", err)
	}
	if cache.entryN != 1 {
		t.Fatalf("cache entries after large dictionary=%d want descriptor only", cache.entryN)
	}
	if _, err := cache.read(140, dictLen, true); err != nil {
		t.Fatalf("large dictionary second read: %v", err)
	}
	if len(calls) != 3 {
		t.Fatalf("calls=%+v want descriptor plus two uncached large dictionary reads", calls)
	}
}

func TestTypedColumnPreparedMetadataPrefetchSkipsUncacheableSortMarks3417(t *testing.T) {
	marksLen := maxTypedColumnPreparedRangeCacheEntryBytes + 1
	raw := make([]byte, 156+marksLen)
	for i := range raw {
		raw[i] = byte(i)
	}
	image := typedcolumn.ColumnPartImage{Sections: []typedcolumn.ColumnPartImageSection{
		{Kind: typedcolumn.ColumnPartImageSectionDescriptor, Offset: 100, Length: 32},
		{Kind: typedcolumn.ColumnPartImageSectionSortKeyMetadata, Offset: 140, Length: 16},
		{Kind: typedcolumn.ColumnPartImageSectionSortKeyMarks, Offset: 156, Length: marksLen},
	}}
	type readCall struct {
		offset int
		length int
	}
	var calls []readCall
	cache := newTypedColumnPreparedRangeReadCache(func(offset int, length int, section bool) ([]byte, error) {
		if offset < 0 || length <= 0 || offset+length > len(raw) {
			t.Fatalf("range offset=%d length=%d outside bytes=%d", offset, length, len(raw))
		}
		calls = append(calls, readCall{offset: offset, length: length})
		return raw[offset : offset+length], nil
	})
	requests := []typedColumnPreparedColumnRequest{{IncludeSortKeyMarks: true}}
	if err := typedColumnPreparedPrefetchMetadataSections(image, requests, cache); err != nil {
		t.Fatalf("prefetch metadata: %v", err)
	}
	if len(calls) != 1 || calls[0] != (readCall{offset: 100, length: 56}) {
		t.Fatalf("prefetch calls=%+v want descriptor+sort metadata only", calls)
	}
	callsBefore := len(calls)
	if _, err := cache.read(156, marksLen, true); err != nil {
		t.Fatalf("large sort marks read: %v", err)
	}
	if _, err := cache.read(156, marksLen, true); err != nil {
		t.Fatalf("large sort marks second read: %v", err)
	}
	if len(calls) != callsBefore+2 {
		t.Fatalf("large sort marks calls=%d want %d uncached reads", len(calls), callsBefore+2)
	}
}

func TestTypedColumnPreparedPruningComposedSelectionsDoNotAliasScratch(t *testing.T) {
	field := TypedStorageField{Name: "score", Path: "score", Owner: TypedStorageOwnerColumnPart, ValueType: "int64"}
	values := []int64{
		0, 1, 1, 0, 0, 1, 1, 0, 1, 1,
		1, 0, 0, 1, 1, 0, 0, 1, 1, 0,
	}
	rows := make([]typedColumnAdapterRow, len(values))
	for i, value := range values {
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{
			"score": {Type: field.ValueType, Present: true, Int64: value},
		}}
	}
	adapterPart, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1841, RowsPerGranule: 10, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := adapterPart.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	ref := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "test", Generation: 1, PartID: image.PartID, FileID: 1, Length: int64(len(image.Bytes))}
	physical := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: "test", Generation: 1, PartID: columnPhysicalRowAssetPartID, FileID: 1, Length: int64(len(image.Bytes))}
	readRange := func(offset int, length int, section bool) ([]byte, error) {
		if offset < 0 || length <= 0 || offset+length > len(image.Bytes) {
			t.Fatalf("range offset=%d length=%d outside image bytes=%d", offset, length, len(image.Bytes))
		}
		return image.Bytes[offset : offset+length], nil
	}
	blockSelection := func(g typedcolumn.EncodedGranule, rows int) (typedcolumn.RowSelection, bool, error) {
		selection, err := typedcolumn.NewRangesRowSelection(rows, []typedcolumn.RowRange{{Start: 0, End: 2}, {Start: 5, End: 6}, {Start: 8, End: 9}})
		return selection, true, err
	}
	requests := []typedColumnPreparedColumnRequest{{
		Field:                    field,
		Role:                     typedcolumn.ColumnRolePredicate,
		Operation:                columnsemantics.OpEquality,
		IncludePruning:           true,
		HasInt64PruningPredicate: true,
		Int64PruningPredicate:    typedcolumn.Int64PruningPredicate{Kind: typedcolumn.Int64PruningPredicateEqual, Value: 1},
	}}
	part, diag, err := typedColumnPreparePartStateFromRanges(ref, physical, image.Rows, image.Rows, []TypedStorageField{field}, uint64(adapterPart.Part.Descriptor.SchemaVersion), requests, readRange, blockSelection)
	if err != nil {
		t.Fatalf("typedColumnPreparePartStateFromRanges: %v", err)
	}
	if diag.PruningValidationFailures != 0 || diag.PruningFallbackBlocks != 0 {
		t.Fatalf("pruning diagnostics=%+v want no validation/fallback", diag)
	}
	if diag.PruningBlocks != 2 || diag.PruningRows != 5 {
		t.Fatalf("pruning diagnostics=%+v want two narrowed blocks and five candidate rows", diag)
	}
	column := part.Columns["score"]
	if column == nil || !column.Int64PruningReady {
		t.Fatalf("prepared column=%+v want pruning ready", column)
	}
	if len(column.BlockPlans) != 2 {
		t.Fatalf("block plans=%d want 2", len(column.BlockPlans))
	}
	assertPreparedSelectionRows(t, column.BlockPlans[0].CandidateSelection, []int{1, 5, 8})
	assertPreparedSelectionRows(t, column.BlockPlans[1].CandidateSelection, []int{0, 8})
	if !column.BlockPlans[0].NeedsPredicate || !column.BlockPlans[1].NeedsPredicate {
		t.Fatalf("block plans must keep predicate verification after pruning: %+v", column.BlockPlans)
	}
}

func TestTypedColumnPreparedPruningFallbackDoesNotInflateEmptyBlockPlans(t *testing.T) {
	var diag typedColumnPreparedStateDiagnostics
	column := &typedColumnPreparedColumnState{}
	typedColumnPreparedPruningFallback(column, &diag, "missing")
	if diag.PruningFallbackBlocks != 0 || diag.PruningFallbackReason != "missing" || column.PruningFallbackReason != "missing" {
		t.Fatalf("fallback diag=%+v column_reason=%q want zero blocks and reason", diag, column.PruningFallbackReason)
	}
	column.BlockPlans = []typedColumnPreparedBlockPlan{{}, {}}
	typedColumnPreparedPruningFallback(column, &diag, "unsupported")
	if diag.PruningFallbackBlocks != 2 || diag.PruningFallbackReason != "unsupported" || column.PruningFallbackReason != "unsupported" {
		t.Fatalf("fallback diag=%+v column_reason=%q want two blocks and updated reason", diag, column.PruningFallbackReason)
	}
}

func TestTypedColumnPreparedDictionarySelectiveRawDecode3175(t *testing.T) {
	raw := encodePreparedRawDictionarySectionForTest([]preparedDictionaryForTest{
		{Name: typedColumnAdapterMetadataDictionary, Entries: []preparedDictionaryEntryForTest{{Code: 0, Value: "metadata"}}},
		{Name: "event", Entries: []preparedDictionaryEntryForTest{{Code: 0, Value: "app.bsky.feed.post"}, {Code: 1, Value: "app.bsky.graph.follow"}}},
		{Name: "unused", Entries: []preparedDictionaryEntryForTest{{Code: 0, Value: strings.Repeat("skip", 128)}}},
	})
	got, err := decodeTypedColumnPreparedDictionariesSectionForEncoding(0, raw, map[string]struct{}{
		typedColumnAdapterMetadataDictionary: {},
		"event":                              {},
	})
	if err != nil {
		t.Fatalf("selective raw decode: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("decoded dictionaries=%v want metadata+event only", mapKeysForTest(got))
	}
	if got["event"]["app.bsky.feed.post"] != 0 || got["event"]["app.bsky.graph.follow"] != 1 {
		t.Fatalf("event dictionary=%v want dense event codes", got["event"])
	}
	if _, ok := got["unused"]; ok {
		t.Fatalf("unused dictionary decoded despite selective request: %v", got["unused"])
	}
}

func TestTypedColumnPreparedDictionarySelectiveDenseDecode3175(t *testing.T) {
	raw := encodePreparedDenseDictionarySectionForTest([]preparedDenseDictionaryForTest{
		{Name: typedColumnAdapterMetadataDictionary, Values: []string{"metadata"}},
		{Name: "event", Values: []string{"app.bsky.feed.post", "app.bsky.graph.follow"}},
		{Name: "unused", Values: []string{strings.Repeat("skip", 128)}},
	})
	got, err := decodeTypedColumnPreparedDictionariesSectionForEncoding(typedcolumn.EncodingDictionaryDense, raw, map[string]struct{}{
		"event": {},
	})
	if err != nil {
		t.Fatalf("selective dense decode: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("decoded dictionaries=%v want event only", mapKeysForTest(got))
	}
	if got["event"]["app.bsky.feed.post"] != 0 || got["event"]["app.bsky.graph.follow"] != 1 {
		t.Fatalf("event dictionary=%v want dense event codes", got["event"])
	}
	if _, ok := got[typedColumnAdapterMetadataDictionary]; ok {
		t.Fatalf("metadata dictionary decoded despite selective request: %v", got[typedColumnAdapterMetadataDictionary])
	}
}

func TestTypedColumnPreparedDictionaryReverseOnlyDenseDecode3175(t *testing.T) {
	raw := encodePreparedDenseDictionarySectionForTest([]preparedDenseDictionaryForTest{
		{Name: "kind", Values: []string{"app.bsky.feed.post", "app.bsky.graph.follow"}},
	})
	got, err := decodeTypedColumnPreparedDictionariesForModes(typedcolumn.EncodingDictionaryDense, raw, map[string]typedColumnPreparedDictionaryRequestMode{
		"kind": {Reverse: true},
	})
	if err != nil {
		t.Fatalf("reverse-only dense decode: %v", err)
	}
	if len(got.Forward) != 0 {
		t.Fatalf("forward dictionaries=%v want none", mapKeysForTest(got.Forward))
	}
	if got.Reverse["kind"][0] != "app.bsky.feed.post" || got.Reverse["kind"][1] != "app.bsky.graph.follow" {
		t.Fatalf("reverse kind dictionary=%v want code-ordered values", got.Reverse["kind"])
	}
}

func TestTypedColumnPreparedDictionaryRequestNamesIncludeMetadata3175(t *testing.T) {
	skippedField := typedColumnAdapterField("unused", "string")
	names, err := typedColumnPreparedDictionaryRequestNames([]typedColumnPreparedColumnRequest{
		{Field: typedColumnAdapterField("kind", "string"), Role: typedcolumn.ColumnRolePredicate, IncludeDictionaries: true},
		{Field: skippedField, IncludeDictionaries: false},
	})
	if err != nil {
		t.Fatalf("request names: %v", err)
	}
	if _, ok := names[typedColumnAdapterMetadataDictionary]; !ok {
		t.Fatalf("request names=%v missing adapter metadata dictionary", names)
	}
	adapterColumn, err := typedColumnAdapterMapField(typedColumnAdapterField("kind", "string"))
	if err != nil {
		t.Fatalf("map kind field: %v", err)
	}
	if _, ok := names[adapterColumn.Definition.Name]; !ok {
		t.Fatalf("request names=%v missing requested kind dictionary %q", names, adapterColumn.Definition.Name)
	}
	skippedColumn, err := typedColumnAdapterMapField(skippedField)
	if err != nil {
		t.Fatalf("map skipped field: %v", err)
	}
	if _, ok := names[skippedColumn.Definition.Name]; ok {
		t.Fatalf("request names=%v included non-dictionary request %q", names, skippedColumn.Definition.Name)
	}
	modes, err := typedColumnPreparedDictionaryRequestModes([]typedColumnPreparedColumnRequest{
		{Field: typedColumnAdapterField("kind", "string"), Role: typedcolumn.ColumnRolePredicate, IncludeDictionaries: true},
		{Field: typedColumnAdapterField("type", "string"), Role: typedcolumn.ColumnRoleProjection, IncludeDictionaries: true},
		{Field: typedColumnAdapterField("type", "string"), Role: typedcolumn.ColumnRolePredicate, IncludeDictionaries: true},
	})
	if err != nil {
		t.Fatalf("request modes: %v", err)
	}
	kindColumn, err := typedColumnAdapterMapField(typedColumnAdapterField("kind", "string"))
	if err != nil {
		t.Fatalf("map kind field: %v", err)
	}
	typeColumn, err := typedColumnAdapterMapField(typedColumnAdapterField("type", "string"))
	if err != nil {
		t.Fatalf("map type field: %v", err)
	}
	if got := modes[kindColumn.Definition.Name]; !got.Forward || got.Reverse {
		t.Fatalf("kind predicate mode=%+v want forward-only", got)
	}
	if got := modes[typeColumn.Definition.Name]; !got.Forward || !got.Reverse {
		t.Fatalf("type mixed mode=%+v want forward+reverse", got)
	}
	if got := modes[typedColumnAdapterMetadataDictionary]; !got.Forward || got.Reverse {
		t.Fatalf("metadata mode=%+v want forward-only", got)
	}
}

type preparedDictionaryEntryForTest struct {
	Code  int64
	Value string
}

type preparedDictionaryForTest struct {
	Name    string
	Entries []preparedDictionaryEntryForTest
}

type preparedDenseDictionaryForTest struct {
	Name   string
	Values []string
}

func encodePreparedRawDictionarySectionForTest(dictionaries []preparedDictionaryForTest) []byte {
	var out []byte
	out = appendPreparedU32ForTest(out, uint32(len(dictionaries)))
	for _, dict := range dictionaries {
		out = appendPreparedStringForTest(out, dict.Name)
		out = appendPreparedU32ForTest(out, uint32(len(dict.Entries)))
		for _, entry := range dict.Entries {
			out = appendPreparedI64ForTest(out, entry.Code)
			out = appendPreparedStringForTest(out, entry.Value)
		}
	}
	return out
}

func encodePreparedDenseDictionarySectionForTest(dictionaries []preparedDenseDictionaryForTest) []byte {
	var out []byte
	out = appendPreparedU32ForTest(out, 0x54434944)
	out = appendPreparedU16ForTest(out, 1)
	out = appendPreparedU16ForTest(out, 0)
	out = appendPreparedU32ForTest(out, uint32(len(dictionaries)))
	for _, dict := range dictionaries {
		out = appendPreparedStringForTest(out, dict.Name)
		out = appendPreparedU32ForTest(out, uint32(len(dict.Values)))
		for _, value := range dict.Values {
			out = appendPreparedStringForTest(out, value)
		}
	}
	return out
}

func appendPreparedU16ForTest(out []byte, v uint16) []byte {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	return append(out, buf[:]...)
}

func appendPreparedU32ForTest(out []byte, v uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	return append(out, buf[:]...)
}

func appendPreparedI64ForTest(out []byte, v int64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	return append(out, buf[:]...)
}

func appendPreparedStringForTest(out []byte, value string) []byte {
	out = appendPreparedU32ForTest(out, uint32(len(value)))
	return append(out, value...)
}

func mapKeysForTest[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func assertPreparedSelectionRows(t *testing.T, selection typedcolumn.RowSelection, want []int) {
	t.Helper()
	got := selection.AppendRows(nil)
	if len(got) != len(want) {
		t.Fatalf("selection rows=%v want %v shape=%+v", got, want, selection.Shape())
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selection rows=%v want %v shape=%+v", got, want, selection.Shape())
		}
	}
}

func assertPreparedPlanDependency(t *testing.T, plan typedColumnPreparedColumnPlan, kind typedcolumn.SectionDependencyKind) {
	t.Helper()
	for _, dep := range plan.Dependencies {
		if dep.Kind == kind {
			return
		}
	}
	t.Fatalf("plan dependencies=%+v missing kind %s", plan.Dependencies, kind)
}

func assertPreparedPlanNoDependency(t *testing.T, plan typedColumnPreparedColumnPlan, kind typedcolumn.SectionDependencyKind) {
	t.Helper()
	for _, dep := range plan.Dependencies {
		if dep.Kind == kind {
			t.Fatalf("plan dependencies=%+v unexpectedly included kind %s", plan.Dependencies, kind)
		}
	}
}
