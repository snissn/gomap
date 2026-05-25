package collections

import (
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
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

	unsupportedStringRange, err := typedColumnDescribePreparedColumn(typedColumnPreparedColumnRequest{
		Field:     stringField,
		Role:      typedcolumn.ColumnRolePredicate,
		Operation: columnsemantics.OpOrderedRange,
	}, span)
	if err != nil {
		t.Fatalf("describe unsupported string range: %v", err)
	}
	if unsupportedStringRange.Capability.Status != columnsemantics.StatusUnsupported || unsupportedStringRange.Capability.Reason != columnsemantics.ReasonDictionaryOrderUnproven {
		t.Fatalf("unsupported string range capability=%+v want #1843 dictionary-order status", unsupportedStringRange.Capability)
	}
	if len(unsupportedStringRange.Dependencies) != 0 {
		t.Fatalf("unsupported string range deps=%+v want no hot-path dependencies", unsupportedStringRange.Dependencies)
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
	state, diag, err := buildTypedColumnPreparedColumnState(plan, column, section, nil)
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
	_, _, err = buildTypedColumnPreparedColumnState(plan, column, section, nil)
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
	request := []typedColumnPreparedColumnRequest{{Field: countField, Role: typedcolumn.ColumnRoleMeasure, Operation: columnsemantics.OpSum}}
	_, _, err = typedColumnPreparePartStateFromRanges(ref, physical, 0, image.Rows, fields, uint64(adapterPart.Part.Descriptor.SchemaVersion), request, readRange, nil)
	if err == nil || !strings.Contains(err.Error(), "image/ref mismatch") {
		t.Fatalf("prepare with typed manifest row mismatch err=%v want fail-closed row mismatch", err)
	}
	_, _, err = typedColumnPreparePartStateFromRanges(ref, physical, image.Rows, image.Rows+1, fields, uint64(adapterPart.Part.Descriptor.SchemaVersion), request, readRange, nil)
	if err == nil || !strings.Contains(err.Error(), "image/physical row mismatch") {
		t.Fatalf("prepare with physical row mismatch err=%v want fail-closed row mismatch", err)
	}

	parsedImage, desc, columns, manifestBytes, descriptorRaw, err := typedColumnPreparedReadImageAndDescriptor(ref, readRange)
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
	_, _, err = typedColumnPreparePartStateFromParsed(ref, physical, image.Rows, image.Rows, fields, uint64(adapterPart.Part.Descriptor.SchemaVersion), missingKindSection, desc, columns, manifestBytes, len(descriptorRaw), request, nil)
	if err == nil || !strings.Contains(err.Error(), "missing column data section \"kind\"") {
		t.Fatalf("prepare missing unrelated string column section err=%v want fail-closed multi-column metadata mismatch", err)
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
