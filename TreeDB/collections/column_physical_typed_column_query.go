package collections

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"
)

type columnTypedColumnPhysicalQueryPlan struct {
	Fields               []TypedStorageField
	Selected             []bool
	ProjectedColumns     []string
	PredicateDiagnostics columnPhysicalQueryPredicateDiagnosticPlan
	GroupColumn          string
	ValueColumn          string
	DistinctColumn       string
	PredicateSpecs       []columnPhysicalQueryPredicateSpec
	SortKey              []ColumnSortKey
	SortKeyPrefix        columnTypedColumnSortKeyPrefixPlan
	DenseGroupCount      bool
	DenseGroupHourCount  bool
	DenseInt64Span       bool
}

type columnTypedColumnDenseGroupCountPart struct {
	Cardinality      int
	DictionaryByCode map[int64]string
	Codes            []uint32
}

type columnTypedColumnDensePredicatePart struct {
	Codes      []uint32
	Allowed    []uint64
	RejectsAll bool
}

type columnTypedColumnDenseGroupHourCountPart struct {
	Cardinality      int
	DictionaryByCode map[int64]string
	GroupCodes       []uint32
	Values           []int64
	Predicates       []columnTypedColumnDensePredicatePart
}

type columnTypedColumnDenseInt64SpanPart struct {
	Cardinality      int
	DictionaryByCode map[int64]string
	GroupCodes       []uint32
	Values           []int64
	Predicates       []columnTypedColumnDensePredicatePart
}

type columnTypedColumnPhysicalQueryPart struct {
	Ref                       columnManifestAssetRefForScan
	PhysicalRef               columnManifestAssetRefForScan
	Values                    map[string][]columnDeclaredValue
	RowIndexes                []int
	Rows                      int
	Bytes                     int64
	Sections                  int
	SectionBytes              uint64
	GranulesConsidered        int
	GranulesDecoded           int
	GranulesSkipped           int
	DecodedBlocks             int
	DecodedPayloadBytes       uint64
	SortKeyMarkChecks         int
	SortKeyMarkMatches        int
	SortKeyMarkSkips          int
	SortKeyMarkFallbackReason string
	DenseGroupCount           *columnTypedColumnDenseGroupCountPart
	DenseGroupHourCount       *columnTypedColumnDenseGroupHourCountPart
	DenseInt64Span            *columnTypedColumnDenseInt64SpanPart
}

type columnTypedColumnPhysicalQueryRunner struct {
	plan                      columnTypedColumnPhysicalQueryPlan
	parts                     []columnTypedColumnPhysicalQueryPart
	assetBytes                int64
	sections                  int
	sectionBytes              uint64
	granulesConsidered        int
	granulesDecoded           int
	granulesSkipped           int
	decodedBlocks             int
	decodedPayloadBytes       uint64
	sortKeyMarkChecks         int
	sortKeyMarkMatches        int
	sortKeyMarkSkips          int
	sortKeyMarkFallbackReason string
	segmentFileCacheHits      uint64
	segmentFileCacheMisses    uint64
	denseGroupCounts          map[string]int
	denseLocalCounts          []int
	denseGroupHourCounts      map[string][24]int
	denseLocalHourCounts      []int
	denseSpanValues           map[string]columnPhysicalQuerySpan
	denseLocalSpans           []columnPhysicalQuerySpan
	denseLocalSpanSeen        []bool
	resultGroups              []ColumnPhysicalQueryGroup
}

func (c *Collection) runColumnPhysicalQueryTypedColumnPartInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, bool, error) {
	if !columnTypedColumnPhysicalQueryTouchesTypedColumnPart(view.FullConfig, req) {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if _, candidate, err := planColumnTypedColumnPhysicalQuery(view.FullConfig, req); err != nil || !candidate {
		return ColumnPhysicalQueryResult{}, candidate, err
	}
	start := time.Now()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		result := ColumnPhysicalQueryResult{}
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()
	runner, candidate, err := prepareColumnTypedColumnPhysicalQueryRunner(view, req, &readCache)
	if err != nil || !candidate {
		result := ColumnPhysicalQueryResult{}
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, candidate, err
	}
	result, err := runner.run(view, req)
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	return result, true, err
}

func prepareColumnTypedColumnPhysicalQueryRunner(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache) (*columnTypedColumnPhysicalQueryRunner, bool, error) {
	if !columnTypedColumnPhysicalQueryTouchesTypedColumnPart(view.FullConfig, req) {
		return nil, false, nil
	}
	plan, candidate, err := planColumnTypedColumnPhysicalQuery(view.FullConfig, req)
	if err != nil || !candidate {
		return nil, candidate, err
	}
	if view.MutationParts != 0 {
		return nil, true, fmt.Errorf("%w: typed-column part physical query requires insert-only manifest", ErrColumnQueryPlanUnsupported)
	}
	if readCache == nil {
		return nil, true, errors.New("collections: typed-column part physical query missing read cache")
	}
	refsByGeneration, err := typedColumnPhysicalQueryRefsByGeneration(view)
	if err != nil {
		return nil, true, err
	}
	runner := &columnTypedColumnPhysicalQueryRunner{plan: plan, parts: make([]columnTypedColumnPhysicalQueryPart, 0, len(view.AssetRefs))}
	if len(refsByGeneration) == 0 {
		if len(view.AssetRefs) == 0 {
			return runner, true, nil
		}
		return nil, true, errors.New("collections: missing typed_column_part assets for typed-column part physical query")
	}
	if _, err := validateTypedColumnPhysicalAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
		return nil, true, typedColumnPhysicalQueryPairingError(err)
	}

	// Disable returnViews during decode to avoid pinning mmaps/handles for the runner lifetime,
	// since we fully decode to Go types and don't retain references to raw bytes.
	savedReturnViews := readCache.returnViews
	readCache.returnViews = false
	defer func() { readCache.returnViews = savedReturnViews }()

	var rawScratch []byte
	for _, physical := range view.AssetRefs {
		if physical.Role == ColumnManifestPartRoleTombstone || physical.Reason == ColumnPublishOperationDelete {
			continue
		}
		typedRef, ok := refsByGeneration[physical.Ref.Generation]
		if !ok {
			return nil, true, fmt.Errorf("collections: missing typed_column_part asset for generation=%d", physical.Ref.Generation)
		}
		raw, err := readCache.read(typedRef.Ref, rawScratch)
		runner.segmentFileCacheHits = readCache.hits
		runner.segmentFileCacheMisses = readCache.misses
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, true, fmt.Errorf("collections: typed-column part physical query read generation=%d part_id=%d short read: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
			}
			return nil, true, fmt.Errorf("collections: typed-column part physical query read generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		rawScratch = raw
		var part columnTypedColumnPhysicalQueryPart
		switch {
		case columnTypedColumnPhysicalQueryUseDenseGroupCount(plan, req):
			part, err = decodeTypedColumnPhysicalQueryDenseGroupCountPart(plan, view.FullConfig.SchemaHash, typedRef, physical, raw)
		case columnTypedColumnPhysicalQueryUseDenseGroupHourCount(plan, req):
			part, err = decodeTypedColumnPhysicalQueryDenseGroupHourCountPart(plan, view.FullConfig.SchemaHash, typedRef, physical, raw)
		case columnTypedColumnPhysicalQueryUseDenseInt64Span(plan, req):
			part, err = decodeTypedColumnPhysicalQueryDenseInt64SpanPart(plan, view.FullConfig.SchemaHash, typedRef, physical, raw)
		default:
			part, err = decodeTypedColumnPhysicalQueryPart(plan, view.FullConfig.SchemaHash, typedRef, physical, raw)
		}
		if err != nil {
			return nil, true, fmt.Errorf("collections: typed-column part physical query decode generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		runner.assetBytes += part.Bytes
		runner.sections += part.Sections
		runner.sectionBytes += part.SectionBytes
		runner.granulesConsidered += part.GranulesConsidered
		runner.granulesDecoded += part.GranulesDecoded
		runner.granulesSkipped += part.GranulesSkipped
		runner.decodedBlocks += part.DecodedBlocks
		runner.decodedPayloadBytes += part.DecodedPayloadBytes
		runner.sortKeyMarkChecks += part.SortKeyMarkChecks
		runner.sortKeyMarkMatches += part.SortKeyMarkMatches
		runner.sortKeyMarkSkips += part.SortKeyMarkSkips
		runner.sortKeyMarkFallbackReason = mergeColumnPhysicalSortKeyFallbackReason(runner.sortKeyMarkFallbackReason, part.SortKeyMarkFallbackReason)
		runner.parts = append(runner.parts, part)
	}
	if len(runner.parts) == 0 && len(view.AssetRefs) != 0 {
		return nil, true, errors.New("collections: typed-column part physical query has no live typed_column_part assets")
	}
	return runner, true, nil
}

func columnTypedColumnPhysicalQueryTouchesTypedColumnPart(cfg ColumnStoreConfig, req ColumnPhysicalQueryRequest) bool {
	check := func(name string) bool {
		if name == "" {
			return false
		}
		col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, name)
		return ok && columnStoreColumnIsTypedColumnPart(col)
	}
	if check(req.GroupColumn) || check(req.ValueColumn) || check(req.DistinctColumn) {
		return true
	}
	for _, predicate := range req.Predicates {
		if check(predicate.Column) {
			return true
		}
	}
	return false
}

func planColumnTypedColumnPhysicalQuery(cfg ColumnStoreConfig, req ColumnPhysicalQueryRequest) (columnTypedColumnPhysicalQueryPlan, bool, error) {
	if req.AggregateMetadataName != "" {
		return columnTypedColumnPhysicalQueryPlan{}, false, nil
	}
	plan := columnTypedColumnPhysicalQueryPlan{PredicateDiagnostics: newColumnPhysicalQueryPredicateDiagnosticPlan(req)}
	requiredTypes, err := columnTypedColumnPhysicalQueryRequiredTypes(req)
	if err != nil {
		return columnTypedColumnPhysicalQueryPlan{}, true, err
	}
	if len(requiredTypes) == 0 {
		return columnTypedColumnPhysicalQueryPlan{}, false, nil
	}
	touchesTyped := columnTypedColumnPhysicalQueryTouchesTypedColumnPart(cfg, req)
	anyTyped := false
	allTyped := true
	for column, wantType := range requiredTypes {
		col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, column)
		if !ok {
			if touchesTyped {
				return columnTypedColumnPhysicalQueryPlan{}, true, fmt.Errorf("%w: typed-column part physical query requested undeclared column %q", ErrColumnQueryPlanUnsupported, column)
			}
			return columnTypedColumnPhysicalQueryPlan{}, false, nil
		}
		if col.ValueType != wantType {
			return columnTypedColumnPhysicalQueryPlan{}, true, fmt.Errorf("%w: typed-column part physical query column %q has type %q, want %q", ErrColumnQueryPlanUnsupported, column, col.ValueType, wantType)
		}
		if col.Nullable {
			return columnTypedColumnPhysicalQueryPlan{}, true, fmt.Errorf("%w: typed-column part physical query column %q does not support nullable values", ErrColumnQueryPlanUnsupported, column)
		}
		if columnStoreColumnIsTypedColumnPart(col) {
			anyTyped = true
		} else {
			allTyped = false
		}
	}
	if !anyTyped {
		return columnTypedColumnPhysicalQueryPlan{}, false, nil
	}
	if !allTyped {
		return columnTypedColumnPhysicalQueryPlan{}, true, fmt.Errorf("%w: typed-column part physical query cannot mix typed_column_part and compatibility-owned columns", ErrColumnQueryPlanUnsupported)
	}
	predicateSpecs, err := columnTypedColumnPhysicalQueryPredicateSpecs(cfg, req)
	if err != nil {
		return columnTypedColumnPhysicalQueryPlan{}, true, err
	}
	fields := columnStoreTypedColumnPartFields(cfg)
	selected := make([]bool, len(fields))
	fieldIndexByName := make(map[string]int, len(fields))
	for idx, field := range fields {
		if field.Name != "" {
			fieldIndexByName[field.Name] = idx
		}
		if field.Path != "" {
			fieldIndexByName[field.Path] = idx
		}
	}
	for column := range requiredTypes {
		idx, ok := fieldIndexByName[column]
		if !ok {
			return columnTypedColumnPhysicalQueryPlan{}, true, fmt.Errorf("collections: typed-column part physical query column %q is not in typed_column_part fields", column)
		}
		selected[idx] = true
	}
	sortKey, err := typedColumnPartPublicationSortKey(cfg, fields)
	if err != nil {
		return columnTypedColumnPhysicalQueryPlan{}, true, err
	}
	plan.SortKeyPrefix = planColumnTypedColumnSortKeyPrefix(cfg, sortKey, req)
	plan.DenseGroupCount = columnTypedColumnPhysicalQueryShapeCanUseDenseGroupCount(req)
	plan.DenseGroupHourCount = columnTypedColumnPhysicalQueryShapeCanUseDenseGroupHourCount(req)
	plan.DenseInt64Span = columnTypedColumnPhysicalQueryShapeCanUseDenseInt64Span(req)
	plan.Fields = fields
	plan.Selected = selected
	plan.PredicateSpecs = predicateSpecs
	plan.GroupColumn = req.GroupColumn
	plan.ValueColumn = req.ValueColumn
	plan.DistinctColumn = req.DistinctColumn
	plan.SortKey = sortKey
	plan.ProjectedColumns = make([]string, 0, len(requiredTypes))
	for column := range requiredTypes {
		plan.ProjectedColumns = append(plan.ProjectedColumns, column)
	}
	sort.Strings(plan.ProjectedColumns)
	return plan, true, nil
}

func columnTypedColumnPhysicalQueryRequiredTypes(req ColumnPhysicalQueryRequest) (map[string]ColumnStoreValueType, error) {
	required := make(map[string]ColumnStoreValueType, 3+len(req.Predicates))
	add := func(name string, valueType ColumnStoreValueType, role string) error {
		if name == "" {
			return fmt.Errorf("%w: typed-column part physical query %s column is required", ErrColumnQueryPlanUnsupported, role)
		}
		if existing, ok := required[name]; ok && existing != valueType {
			return fmt.Errorf("%w: typed-column part physical query column %q used as both %s and %s", ErrColumnQueryPlanUnsupported, name, existing, valueType)
		}
		required[name] = valueType
		return nil
	}
	for idx, predicate := range req.Predicates {
		if predicate.Column == "" {
			return nil, fmt.Errorf("%w: typed-column part physical predicate[%d] column is required", ErrColumnQueryPlanUnsupported, idx)
		}
		if err := add(predicate.Column, ColumnStoreValueString, "predicate"); err != nil {
			return nil, err
		}
	}
	switch req.Kind {
	case ColumnPhysicalQueryGroupCount:
		if err := add(req.GroupColumn, ColumnStoreValueString, "group"); err != nil {
			return nil, err
		}
	case ColumnPhysicalQueryGroupCountDistinct, ColumnPhysicalQueryGroupCountAndDistinct:
		if err := add(req.GroupColumn, ColumnStoreValueString, "group"); err != nil {
			return nil, err
		}
		if err := add(req.DistinctColumn, ColumnStoreValueString, "distinct"); err != nil {
			return nil, err
		}
		if req.Kind == ColumnPhysicalQueryGroupCountAndDistinct && req.GroupColumn == req.DistinctColumn {
			return nil, fmt.Errorf("%w: typed-column part physical query group and distinct columns must differ", ErrColumnQueryPlanUnsupported)
		}
	case ColumnPhysicalQueryHourCount:
		if err := add(req.ValueColumn, ColumnStoreValueInt64, "value"); err != nil {
			return nil, err
		}
	case ColumnPhysicalQueryGroupHourCount:
		if err := add(req.GroupColumn, ColumnStoreValueString, "group"); err != nil {
			return nil, err
		}
		if err := add(req.ValueColumn, ColumnStoreValueInt64, "value"); err != nil {
			return nil, err
		}
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64, ColumnPhysicalQueryGroupInt64Span:
		if err := add(req.GroupColumn, ColumnStoreValueString, "group"); err != nil {
			return nil, err
		}
		if err := add(req.ValueColumn, ColumnStoreValueInt64, "value"); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: unsupported typed-column part physical query kind %q", ErrColumnQueryPlanUnsupported, req.Kind)
	}
	return required, nil
}

func columnTypedColumnPhysicalQueryPredicateSpecs(cfg ColumnStoreConfig, req ColumnPhysicalQueryRequest) ([]columnPhysicalQueryPredicateSpec, error) {
	if len(req.Predicates) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(req.Predicates))
	specs := make([]columnPhysicalQueryPredicateSpec, 0, len(req.Predicates))
	for idx, predicate := range req.Predicates {
		if predicate.Column == "" {
			return nil, fmt.Errorf("%w: typed-column part physical predicate[%d] column is required", ErrColumnQueryPlanUnsupported, idx)
		}
		if _, ok := seen[predicate.Column]; ok {
			return nil, fmt.Errorf("%w: multiple typed-column part physical predicates on column %q are not supported", ErrColumnQueryPlanUnsupported, predicate.Column)
		}
		seen[predicate.Column] = struct{}{}
		col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, predicate.Column)
		if !ok {
			return nil, fmt.Errorf("%w: typed-column part physical predicate requested undeclared column %q", ErrColumnQueryPlanUnsupported, predicate.Column)
		}
		if col.ValueType != ColumnStoreValueString {
			return nil, fmt.Errorf("%w: typed-column part physical predicate column %q has type %q, want %q", ErrColumnQueryPlanUnsupported, predicate.Column, col.ValueType, ColumnStoreValueString)
		}
		if col.Nullable {
			return nil, fmt.Errorf("%w: typed-column part physical predicate column %q does not support nullable values", ErrColumnQueryPlanUnsupported, predicate.Column)
		}
		if !columnStoreColumnIsTypedColumnPart(col) {
			return nil, fmt.Errorf("%w: typed-column part physical predicate column %q is not owned by typed_column_part", ErrColumnQueryPlanUnsupported, predicate.Column)
		}
		kind := columnPhysicalQueryPredicateKindOrDefault(predicate.Kind)
		var values []string
		switch kind {
		case ColumnPhysicalQueryPredicateEqual:
			if len(predicate.Values) != 0 {
				return nil, fmt.Errorf("%w: typed-column part physical predicate column %q equal uses Value, not Values", ErrColumnQueryPlanUnsupported, predicate.Column)
			}
			values = []string{predicate.Value}
		case ColumnPhysicalQueryPredicateInList:
			if predicate.Value != "" {
				return nil, fmt.Errorf("%w: typed-column part physical predicate column %q in-list uses Values, not Value", ErrColumnQueryPlanUnsupported, predicate.Column)
			}
			if len(predicate.Values) == 0 {
				return nil, fmt.Errorf("%w: typed-column part physical predicate column %q in-list requires at least one value", ErrColumnQueryPlanUnsupported, predicate.Column)
			}
			if len(predicate.Values) > columnPhysicalQueryMaxPredicateValues {
				return nil, fmt.Errorf("%w: typed-column part physical predicate column %q in-list values=%d exceeds limit=%d", ErrColumnQueryPlanUnsupported, predicate.Column, len(predicate.Values), columnPhysicalQueryMaxPredicateValues)
			}
			values = append([]string(nil), predicate.Values...)
		default:
			return nil, fmt.Errorf("%w: unsupported typed-column part physical predicate kind %q for column %q", ErrColumnQueryPlanUnsupported, predicate.Kind, predicate.Column)
		}
		specs = append(specs, columnPhysicalQueryPredicateSpec{column: predicate.Column, kind: kind, values: values, valueBytes: columnPhysicalQueryPredicateValueBytes(values)})
	}
	return specs, nil
}

func typedColumnPhysicalQueryRefsByGeneration(view columnPhysicalScanSnapshotView) (map[uint64]columnManifestAssetRefForScan, error) {
	refsByGeneration := make(map[uint64]columnManifestAssetRefForScan, len(view.TypedColumnPartRefs))
	for _, ref := range view.TypedColumnPartRefs {
		if ref.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
			continue
		}
		if ref.Ref.PartID != typedColumnPartAssetPartID {
			return nil, fmt.Errorf("collections: typed-column part physical query generation=%d part_id=%d has multipart/non-primary typed_column_part ref; multipart/non-primary typed_column_part refs are unsupported by this physical query path", ref.Ref.Generation, ref.Ref.PartID)
		}
		if ref.Role == ColumnManifestPartRoleTombstone || ref.Reason == ColumnPublishOperationDelete {
			return nil, fmt.Errorf("collections: typed-column part physical query got tombstone typed ref generation=%d", ref.Ref.Generation)
		}
		if _, exists := refsByGeneration[ref.Ref.Generation]; exists {
			return nil, fmt.Errorf("collections: duplicate typed_column_part ref for generation=%d", ref.Ref.Generation)
		}
		refsByGeneration[ref.Ref.Generation] = ref
	}
	return refsByGeneration, nil
}

func typedColumnPhysicalQueryPairingError(err error) error {
	var reasonErr typedColumnPhysicalAssetPairingReasonError
	if errors.As(err, &reasonErr) {
		return fmt.Errorf("collections: typed-column part physical query requires insert-only physical refs, got %s", reasonErr.reason)
	}
	return err
}

func validateTypedColumnPhysicalQuerySortMetadata(expected, manifest, image []ColumnSortKey) error {
	if !columnSortKeysEqual(expected, manifest) {
		return fmt.Errorf("collections: typed-column part physical query sort metadata mismatch: manifest=%v want %v", manifest, expected)
	}
	if !columnSortKeysEqual(expected, image) {
		return fmt.Errorf("collections: typed-column part physical query sort metadata mismatch: image=%v want %v", image, expected)
	}
	return nil
}

func columnSortKeysEqual(left, right []ColumnSortKey) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func decodeTypedColumnPhysicalQueryPart(plan columnTypedColumnPhysicalQueryPlan, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, raw []byte) (columnTypedColumnPhysicalQueryPart, error) {
	adapterPart, summary, err := typedColumnAdapterPartFromBytesForReconstructionWithSummary(typedColumnAdapterOptions{Fields: plan.Fields, SchemaVersion: uint32(schemaHash)}, raw)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	if summary.PartID != typedRef.Ref.PartID || summary.Rows != typedRef.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part image/ref mismatch image_part=%d ref_part=%d image_rows=%d manifest_rows=%d", summary.PartID, typedRef.Ref.PartID, summary.Rows, typedRef.Rows)
	}
	if physical.Rows != 0 && summary.Rows != physical.Rows {
		return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part rows=%d do not match physical rows=%d", summary.Rows, physical.Rows)
	}
	if err := validateTypedColumnPhysicalQuerySortMetadata(plan.SortKey, typedRef.SortKey, summary.SortKey); err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	pruned, err := plan.SortKeyPrefix.prunePartRows(adapterPart)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	selectedRows := pruned.Rows
	if pruned.AllRows {
		selectedRows = nil
	} else if selectedRows == nil {
		selectedRows = []int{}
	}
	decoded, scanDiag, err := adapterPart.scanDecodedValuesSelectedRows(plan.Selected, selectedRows)
	if err != nil {
		return columnTypedColumnPhysicalQueryPart{}, err
	}
	values := make(map[string][]columnDeclaredValue, len(plan.ProjectedColumns))
	for idx, field := range plan.Fields {
		if !plan.Selected[idx] {
			continue
		}
		name := field.Name
		if name == "" {
			name = field.Path
		}
		columnValues := decoded.Values[idx]
		wantRows := len(pruned.Rows)
		if pruned.AllRows {
			wantRows = summary.Rows
		}
		if len(columnValues) != wantRows {
			return columnTypedColumnPhysicalQueryPart{}, fmt.Errorf("typed_column_part column %q decoded rows=%d want selected rows=%d", name, len(columnValues), wantRows)
		}
		values[name] = columnValues
		if field.Path != "" && field.Path != name {
			values[field.Path] = columnValues
		}
	}
	return columnTypedColumnPhysicalQueryPart{
		Ref:                       typedRef,
		PhysicalRef:               physical,
		Values:                    values,
		RowIndexes:                selectedRows,
		Rows:                      summary.Rows,
		Bytes:                     int64(len(raw)),
		Sections:                  summary.Sections,
		SectionBytes:              summary.SectionBytes,
		GranulesConsidered:        pruned.Considered,
		GranulesDecoded:           scanDiag.GranulesDecoded,
		GranulesSkipped:           pruned.Skips,
		DecodedBlocks:             scanDiag.BlocksDecoded,
		DecodedPayloadBytes:       uint64(scanDiag.BytesDecoded),
		SortKeyMarkChecks:         pruned.Checks,
		SortKeyMarkMatches:        pruned.Matches,
		SortKeyMarkSkips:          pruned.Skips,
		SortKeyMarkFallbackReason: pruned.FallbackReason,
	}, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) run(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	if columnTypedColumnPhysicalQueryUseDenseGroupCount(r.plan, req) {
		return r.runDenseGroupCount(view, req)
	}
	if columnTypedColumnPhysicalQueryUseDenseGroupHourCount(r.plan, req) {
		return r.runDenseGroupHourCount(view, req)
	}
	if columnTypedColumnPhysicalQueryUseDenseInt64Span(r.plan, req) {
		return r.runDenseInt64Span(view, req)
	}
	if columnTypedColumnPhysicalQueryUseSortedGroupedDistinct(r.plan, req) {
		result, err := r.runSortedGroupedDistinct(view, req)
		if err == nil {
			finalizeColumnPhysicalQueryResultGroups(req, &result)
			r.resultGroups = result.Groups
		}
		return result, err
	}

	start := time.Now()
	acc := newColumnTypedColumnPhysicalQueryAccumulator(req.Kind)
	rowsScanned := 0
	matchedRows := 0
	for _, part := range r.parts {
		partRows := len(part.RowIndexes)
		if part.RowIndexes == nil {
			partRows = part.Rows
		}
		for rowIdx := 0; rowIdx < partRows; rowIdx++ {
			rowsScanned++
			matched, err := typedColumnPhysicalQueryPredicatesMatch(part.Values, r.plan.PredicateSpecs, rowIdx)
			if err != nil {
				return ColumnPhysicalQueryResult{Diagnostics: r.diagnostics(view, req, rowsScanned, matchedRows, acc.reduceRows, time.Since(start).Nanoseconds())}, err
			}
			if !matched {
				continue
			}
			if len(r.plan.PredicateSpecs) != 0 {
				matchedRows++
			}
			if err := acc.visit(req, part.Values, rowIdx); err != nil {
				result := ColumnPhysicalQueryResult{Diagnostics: r.diagnostics(view, req, rowsScanned, matchedRows, acc.reduceRows, time.Since(start).Nanoseconds())}
				return result, err
			}
		}
	}
	groups := acc.groups(req, r.resultGroups)
	r.resultGroups = groups
	diag := r.diagnostics(view, req, rowsScanned, matchedRows, acc.reduceRows, time.Since(start).Nanoseconds())
	result := ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

func columnTypedColumnPhysicalQueryShapeCanUseDenseGroupCount(req ColumnPhysicalQueryRequest) bool {
	return req.Kind == ColumnPhysicalQueryGroupCount && req.GroupColumn != "" && !columnPhysicalQueryHasPredicates(req) && req.AggregateMetadataName == "" && req.ValueColumn == "" && req.DistinctColumn == ""
}

func columnTypedColumnPhysicalQueryUseDenseGroupCount(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest) bool {
	return plan.DenseGroupCount && columnTypedColumnPhysicalQueryShapeCanUseDenseGroupCount(req)
}

func columnTypedColumnPhysicalQueryShapeCanUseDenseGroupHourCount(req ColumnPhysicalQueryRequest) bool {
	return req.Kind == ColumnPhysicalQueryGroupHourCount && req.GroupColumn != "" && req.ValueColumn != "" && req.AggregateMetadataName == "" && req.DistinctColumn == ""
}

func columnTypedColumnPhysicalQueryUseDenseGroupHourCount(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest) bool {
	return plan.DenseGroupHourCount && columnTypedColumnPhysicalQueryShapeCanUseDenseGroupHourCount(req)
}

func columnTypedColumnPhysicalQueryShapeCanUseDenseInt64Span(req ColumnPhysicalQueryRequest) bool {
	return req.Kind == ColumnPhysicalQueryGroupInt64Span && req.GroupColumn != "" && req.ValueColumn != "" && req.AggregateMetadataName == "" && req.DistinctColumn == ""
}

func columnTypedColumnPhysicalQueryUseDenseInt64Span(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest) bool {
	return plan.DenseInt64Span && columnTypedColumnPhysicalQueryShapeCanUseDenseInt64Span(req)
}

func (r *columnTypedColumnPhysicalQueryRunner) runDenseGroupCount(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	if r.denseGroupCounts == nil {
		r.denseGroupCounts = make(map[string]int, 16)
	} else {
		clear(r.denseGroupCounts)
	}
	rowsScanned := 0
	for partIdx := range r.parts {
		dense := r.parts[partIdx].DenseGroupCount
		if dense == nil {
			diag := r.diagnostics(view, req, rowsScanned, 0, rowsScanned, time.Since(start).Nanoseconds())
			diag.DenseGroupCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-count missing prepared part %d", partIdx)
		}
		if dense.Cardinality == 0 && len(dense.Codes) != 0 {
			diag := r.diagnostics(view, req, rowsScanned, 0, rowsScanned, time.Since(start).Nanoseconds())
			diag.DenseGroupCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-count part %d has empty dictionary", partIdx)
		}
		if cap(r.denseLocalCounts) < dense.Cardinality {
			r.denseLocalCounts = make([]int, dense.Cardinality)
		} else {
			r.denseLocalCounts = r.denseLocalCounts[:dense.Cardinality]
			clear(r.denseLocalCounts)
		}
		for rowIdx, code := range dense.Codes {
			localIdx, ok := columnDictionaryCodeIndex(code, len(r.denseLocalCounts))
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, 0, rowsScanned, time.Since(start).Nanoseconds())
				diag.DenseGroupCountUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-count part %d code[%d]=%d outside cardinality=%d", partIdx, rowIdx, code, len(r.denseLocalCounts))
			}
			r.denseLocalCounts[localIdx]++
		}
		rowsScanned += len(dense.Codes)
		for localCode, count := range r.denseLocalCounts {
			if count == 0 {
				continue
			}
			key, ok := dense.DictionaryByCode[int64(localCode)]
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, 0, rowsScanned, time.Since(start).Nanoseconds())
				diag.DenseGroupCountUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-count part %d dictionary missing local code %d", partIdx, localCode)
			}
			r.denseGroupCounts[key] += count
		}
	}
	r.resultGroups = r.resultGroups[:0]
	for key, count := range r.denseGroupCounts {
		if count == 0 {
			continue
		}
		r.resultGroups = append(r.resultGroups, ColumnPhysicalQueryGroup{Key: key, Count: count})
	}
	sortColumnPhysicalQueryGroupsByKey(r.resultGroups)
	diag := r.diagnostics(view, req, rowsScanned, 0, rowsScanned, time.Since(start).Nanoseconds())
	diag.DenseGroupCountUsed = true
	diag.ResultGroups = len(r.resultGroups)
	result := ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) runDenseGroupHourCount(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	if r.denseGroupHourCounts == nil {
		r.denseGroupHourCounts = make(map[string][24]int, 16)
	} else {
		clear(r.denseGroupHourCounts)
	}
	rowsScanned := 0
	matchedRows := 0
	reduceRows := 0
	for partIdx := range r.parts {
		dense := r.parts[partIdx].DenseGroupHourCount
		if dense == nil {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupHourCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-hour missing prepared part %d", partIdx)
		}
		if len(dense.GroupCodes) != len(dense.Values) {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupHourCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-hour part %d group/value rows=%d/%d", partIdx, len(dense.GroupCodes), len(dense.Values))
		}
		needLocal := dense.Cardinality * 24
		if cap(r.denseLocalHourCounts) < needLocal {
			r.denseLocalHourCounts = make([]int, needLocal)
		} else {
			r.denseLocalHourCounts = r.denseLocalHourCounts[:needLocal]
			clear(r.denseLocalHourCounts)
		}
		if columnTypedColumnDensePredicatesRejectAll(dense.Predicates) {
			rowsScanned += len(dense.GroupCodes)
			continue
		}
		for rowIdx, code := range dense.GroupCodes {
			rowsScanned++
			if !columnTypedColumnDensePredicatesMatch(dense.Predicates, rowIdx) {
				continue
			}
			if len(dense.Predicates) != 0 {
				matchedRows++
			}
			reduceRows++
			localIdx, ok := columnDictionaryCodeIndex(code, dense.Cardinality)
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseGroupHourCountUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-hour part %d code[%d]=%d outside cardinality=%d", partIdx, rowIdx, code, dense.Cardinality)
			}
			hour := columnPhysicalQueryUTCHour(dense.Values[rowIdx])
			r.denseLocalHourCounts[localIdx*24+hour]++
		}
		for localCode := 0; localCode < dense.Cardinality; localCode++ {
			key := ""
			var byHour [24]int
			seen := false
			base := localCode * 24
			for hour := 0; hour < 24; hour++ {
				count := r.denseLocalHourCounts[base+hour]
				if count == 0 {
					continue
				}
				if !seen {
					var ok bool
					key, ok = dense.DictionaryByCode[int64(localCode)]
					if !ok {
						diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
						diag.DenseGroupHourCountUsed = true
						return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-hour part %d dictionary missing local code %d", partIdx, localCode)
					}
					byHour = r.denseGroupHourCounts[key]
					seen = true
				}
				byHour[hour] += count
			}
			if seen {
				r.denseGroupHourCounts[key] = byHour
			}
		}
	}
	r.resultGroups = r.resultGroups[:0]
	for key, byHour := range r.denseGroupHourCounts {
		for hour, count := range byHour {
			if count == 0 {
				continue
			}
			r.resultGroups = append(r.resultGroups, ColumnPhysicalQueryGroup{Key: key, Hour: hour, Count: count})
		}
	}
	sortColumnPhysicalQueryGroupsByKeyHour(r.resultGroups)
	diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
	diag.DenseGroupHourCountUsed = true
	result := ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) runDenseInt64Span(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	if r.denseSpanValues == nil {
		r.denseSpanValues = make(map[string]columnPhysicalQuerySpan, 16)
	} else {
		clear(r.denseSpanValues)
	}
	rowsScanned := 0
	matchedRows := 0
	reduceRows := 0
	for partIdx := range r.parts {
		dense := r.parts[partIdx].DenseInt64Span
		if dense == nil {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseInt64SpanUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span missing prepared part %d", partIdx)
		}
		if len(dense.GroupCodes) != len(dense.Values) {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseInt64SpanUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span part %d group/value rows=%d/%d", partIdx, len(dense.GroupCodes), len(dense.Values))
		}
		if cap(r.denseLocalSpans) < dense.Cardinality {
			r.denseLocalSpans = make([]columnPhysicalQuerySpan, dense.Cardinality)
			r.denseLocalSpanSeen = make([]bool, dense.Cardinality)
		} else {
			r.denseLocalSpans = r.denseLocalSpans[:dense.Cardinality]
			clear(r.denseLocalSpans)
			r.denseLocalSpanSeen = r.denseLocalSpanSeen[:dense.Cardinality]
			clear(r.denseLocalSpanSeen)
		}
		if columnTypedColumnDensePredicatesRejectAll(dense.Predicates) {
			rowsScanned += len(dense.GroupCodes)
			continue
		}
		for rowIdx, code := range dense.GroupCodes {
			rowsScanned++
			if !columnTypedColumnDensePredicatesMatch(dense.Predicates, rowIdx) {
				continue
			}
			if len(dense.Predicates) != 0 {
				matchedRows++
			}
			reduceRows++
			localIdx, ok := columnDictionaryCodeIndex(code, len(r.denseLocalSpans))
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseInt64SpanUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span part %d code[%d]=%d outside cardinality=%d", partIdx, rowIdx, code, len(r.denseLocalSpans))
			}
			value := dense.Values[rowIdx]
			if !r.denseLocalSpanSeen[localIdx] {
				r.denseLocalSpans[localIdx] = columnPhysicalQuerySpan{min: value, max: value}
				r.denseLocalSpanSeen[localIdx] = true
				continue
			}
			span := r.denseLocalSpans[localIdx]
			if value < span.min {
				span.min = value
			}
			if value > span.max {
				span.max = value
			}
			r.denseLocalSpans[localIdx] = span
		}
		for localCode, seen := range r.denseLocalSpanSeen {
			if !seen {
				continue
			}
			key, ok := dense.DictionaryByCode[int64(localCode)]
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseInt64SpanUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span part %d dictionary missing local code %d", partIdx, localCode)
			}
			partSpan := r.denseLocalSpans[localCode]
			cur, ok := r.denseSpanValues[key]
			if !ok {
				r.denseSpanValues[key] = partSpan
				continue
			}
			if partSpan.min < cur.min {
				cur.min = partSpan.min
			}
			if partSpan.max > cur.max {
				cur.max = partSpan.max
			}
			r.denseSpanValues[key] = cur
		}
	}
	r.resultGroups = r.resultGroups[:0]
	for key, span := range r.denseSpanValues {
		r.resultGroups = append(r.resultGroups, ColumnPhysicalQueryGroup{Key: key, Int64: span.max - span.min})
	}
	if req.TopK == 0 {
		sortColumnPhysicalQueryGroupsByKey(r.resultGroups)
	}
	diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
	diag.DenseInt64SpanUsed = true
	result := ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

func columnTypedColumnDensePredicatesRejectAll(predicates []columnTypedColumnDensePredicatePart) bool {
	for _, predicate := range predicates {
		if predicate.RejectsAll {
			return true
		}
	}
	return false
}

func columnTypedColumnDensePredicatesMatch(predicates []columnTypedColumnDensePredicatePart, rowIdx int) bool {
	for _, predicate := range predicates {
		if predicate.RejectsAll {
			return false
		}
		if rowIdx < 0 || rowIdx >= len(predicate.Codes) {
			return false
		}
		code := predicate.Codes[rowIdx]
		word := int(code / 64)
		bit := uint(code % 64)
		if word >= len(predicate.Allowed) || (predicate.Allowed[word]&(uint64(1)<<bit)) == 0 {
			return false
		}
	}
	return true
}

func columnTypedColumnPhysicalQueryUseSortedGroupedDistinct(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest) bool {
	return req.Kind == ColumnPhysicalQueryGroupCountAndDistinct && plan.SortKeyPrefix.SortedGroupedDistinctReady
}

func (r *columnTypedColumnPhysicalQueryRunner) runSortedGroupedDistinct(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	iterators := make([]*columnTypedColumnSortedGroupedDistinctIterator, 0, len(r.parts))
	heap := columnTypedColumnSortedGroupedDistinctHeap{}
	for partIdx := range r.parts {
		iterator, err := newColumnTypedColumnSortedGroupedDistinctIterator(&r.parts[partIdx], r.plan, req, partIdx)
		if err != nil {
			rowsScanned, matchedRows := columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators)
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, 0, time.Since(start).Nanoseconds())
			diag.SortedGroupedDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		iterators = append(iterators, iterator)
		iteratorIdx := len(iterators) - 1
		if err := iterator.advance(); err != nil {
			rowsScanned, matchedRows := columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators)
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, 0, time.Since(start).Nanoseconds())
			diag.SortedGroupedDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		if !iterator.done {
			heap.push(iteratorIdx, iterators)
		}
	}

	groups := r.resultGroups[:0]
	firstGroup := true
	currentGroup := ""
	currentDistinct := ""
	groupRows := 0
	distinctRows := 0
	reduceRows := 0
	emitGroup := func() {
		if firstGroup {
			return
		}
		groups = append(groups, ColumnPhysicalQueryGroup{Key: currentGroup, Count: groupRows, DistinctCount: distinctRows})
	}
	for heap.len() != 0 {
		iteratorIdx := heap.pop(iterators)
		iterator := iterators[iteratorIdx]
		group := iterator.currentGroup
		distinct := iterator.currentDistinct
		if firstGroup {
			firstGroup = false
			currentGroup = group
			currentDistinct = distinct
			groupRows = 1
			distinctRows = 1
		} else if group != currentGroup {
			emitGroup()
			currentGroup = group
			currentDistinct = distinct
			groupRows = 1
			distinctRows = 1
		} else {
			groupRows++
			if distinct != currentDistinct {
				currentDistinct = distinct
				distinctRows++
			}
		}
		reduceRows++
		if err := iterator.advance(); err != nil {
			rowsScanned, matchedRows := columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators)
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.SortedGroupedDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		if !iterator.done {
			heap.push(iteratorIdx, iterators)
		}
	}
	emitGroup()
	r.resultGroups = groups
	rowsScanned, matchedRows := columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators)
	diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
	diag.SortedGroupedDistinctUsed = true
	diag.ResultGroups = len(groups)
	return ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}, nil
}

type columnTypedColumnSortedGroupedDistinctIterator struct {
	partIndex        int
	row              int
	rows             int
	groupValues      []columnDeclaredValue
	distinctValues   []columnDeclaredValue
	predicateSpecs   []columnPhysicalQueryPredicateSpec
	predicateColumns [][]columnDeclaredValue
	currentGroup     string
	currentDistinct  string
	done             bool
	rowsScanned      int
	matchedRows      int
}

func newColumnTypedColumnSortedGroupedDistinctIterator(part *columnTypedColumnPhysicalQueryPart, plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest, partIndex int) (*columnTypedColumnSortedGroupedDistinctIterator, error) {
	partRows := len(part.RowIndexes)
	if part.RowIndexes == nil {
		partRows = part.Rows
	}
	groupValues, err := typedColumnPhysicalQueryStringColumnValues(part.Values, req.GroupColumn, partRows)
	if err != nil {
		return nil, err
	}
	distinctValues, err := typedColumnPhysicalQueryStringColumnValues(part.Values, req.DistinctColumn, partRows)
	if err != nil {
		return nil, err
	}
	predicateColumns := make([][]columnDeclaredValue, len(plan.PredicateSpecs))
	for idx, spec := range plan.PredicateSpecs {
		values, err := typedColumnPhysicalQueryStringColumnValues(part.Values, spec.column, partRows)
		if err != nil {
			return nil, err
		}
		predicateColumns[idx] = values
	}
	return &columnTypedColumnSortedGroupedDistinctIterator{
		partIndex:        partIndex,
		rows:             partRows,
		groupValues:      groupValues,
		distinctValues:   distinctValues,
		predicateSpecs:   plan.PredicateSpecs,
		predicateColumns: predicateColumns,
	}, nil
}

func (it *columnTypedColumnSortedGroupedDistinctIterator) advance() error {
	// These slices come from typed-column adapter decoding, which materializes
	// owned String values (not physical-row StringBytes views). Keep the hot loop
	// on direct string header comparisons and avoid per-row map lookups or string
	// conversions; decode/shape validation already happened before the runner was
	// built.
	for it.row < it.rows {
		rowIdx := it.row
		it.row++
		it.rowsScanned++
		matched := true
		for idx, spec := range it.predicateSpecs {
			if !typedColumnPhysicalQueryPredicateStringMatches(it.predicateColumns[idx][rowIdx].String, spec) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		if len(it.predicateSpecs) != 0 {
			it.matchedRows++
		}
		it.currentGroup = it.groupValues[rowIdx].String
		it.currentDistinct = it.distinctValues[rowIdx].String
		return nil
	}
	it.done = true
	it.currentGroup = ""
	it.currentDistinct = ""
	return nil
}

type columnTypedColumnSortedGroupedDistinctHeap struct {
	items []int
}

func (h *columnTypedColumnSortedGroupedDistinctHeap) len() int { return len(h.items) }

func (h *columnTypedColumnSortedGroupedDistinctHeap) push(iteratorIdx int, iterators []*columnTypedColumnSortedGroupedDistinctIterator) {
	h.items = append(h.items, iteratorIdx)
	for idx := len(h.items) - 1; idx > 0; {
		parent := (idx - 1) / 2
		if !columnTypedColumnSortedGroupedDistinctIteratorLess(iterators[h.items[idx]], iterators[h.items[parent]]) {
			break
		}
		h.items[idx], h.items[parent] = h.items[parent], h.items[idx]
		idx = parent
	}
}

func (h *columnTypedColumnSortedGroupedDistinctHeap) pop(iterators []*columnTypedColumnSortedGroupedDistinctIterator) int {
	out := h.items[0]
	last := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	if len(h.items) == 0 {
		return out
	}
	h.items[0] = last
	for idx := 0; ; {
		left := idx*2 + 1
		if left >= len(h.items) {
			break
		}
		smallest := left
		right := left + 1
		if right < len(h.items) && columnTypedColumnSortedGroupedDistinctIteratorLess(iterators[h.items[right]], iterators[h.items[left]]) {
			smallest = right
		}
		if !columnTypedColumnSortedGroupedDistinctIteratorLess(iterators[h.items[smallest]], iterators[h.items[idx]]) {
			break
		}
		h.items[idx], h.items[smallest] = h.items[smallest], h.items[idx]
		idx = smallest
	}
	return out
}

func columnTypedColumnSortedGroupedDistinctIteratorLess(left, right *columnTypedColumnSortedGroupedDistinctIterator) bool {
	if left.currentGroup != right.currentGroup {
		return left.currentGroup < right.currentGroup
	}
	if left.currentDistinct != right.currentDistinct {
		return left.currentDistinct < right.currentDistinct
	}
	return left.partIndex < right.partIndex
}

func columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators []*columnTypedColumnSortedGroupedDistinctIterator) (int, int) {
	rowsScanned := 0
	matchedRows := 0
	for _, iterator := range iterators {
		rowsScanned += iterator.rowsScanned
		matchedRows += iterator.matchedRows
	}
	return rowsScanned, matchedRows
}

func typedColumnPhysicalQueryStringColumnValues(values map[string][]columnDeclaredValue, column string, wantRows int) ([]columnDeclaredValue, error) {
	columnValues, ok := values[column]
	if !ok {
		return nil, fmt.Errorf("collections: typed-column part physical query missing string column %q", column)
	}
	if len(columnValues) != wantRows {
		return nil, fmt.Errorf("collections: typed-column part physical query column %q rows=%d want %d", column, len(columnValues), wantRows)
	}
	return columnValues, nil
}

func typedColumnPhysicalQueryPredicateStringMatches(value string, spec columnPhysicalQueryPredicateSpec) bool {
	for _, target := range spec.values {
		if value == target {
			return true
		}
	}
	return false
}

func (r *columnTypedColumnPhysicalQueryRunner) diagnostics(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, rowsScanned, matchedRows, reduceRows int, scanNanos int64) ColumnPhysicalQueryDiagnostics {
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ProjectedColumns = len(r.plan.ProjectedColumns)
	diag.ScheduledGranules = r.granulesConsidered
	diag.SkippedGranules = r.granulesSkipped
	diag.DecodedGranules = r.granulesDecoded
	diag.DecodedBlocks = r.decodedBlocks
	diag.DirectReduceBlocks = r.decodedBlocks
	diag.TypedColumnPartSections = r.sections
	diag.TypedColumnPartSectionBytes = r.sectionBytes
	diag.RowsScanned = rowsScanned
	diag.PhysicalBytesScanned = r.assetBytes
	diag.DecodedPayloadBytes = r.decodedPayloadBytes
	diag.ReduceRows = reduceRows
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	diag.StorageSource = ColumnPhysicalQueryStorageSourceTypedColumnPartSection
	diag.FallbackReason = ColumnPhysicalQueryFallbackNone
	diag.SegmentFileCacheHits = r.segmentFileCacheHits
	diag.SegmentFileCacheMisses = r.segmentFileCacheMisses
	diag.SortKeyPrefixPlanned = r.plan.SortKeyPrefix.Planned
	if r.plan.SortKeyPrefix.Planned {
		diag.SortKeyPrefixColumns = r.plan.SortKeyPrefix.prefixColumns()
		diag.SortKeyPrefixLiterals = r.plan.SortKeyPrefix.PrefixLen
	}
	diag.SortKeyMarkChecks = r.sortKeyMarkChecks
	diag.SortKeyMarkMatches = r.sortKeyMarkMatches
	diag.SortKeyMarkSkips = r.sortKeyMarkSkips
	diag.SortKeyMarkFallbackReason = mergeColumnPhysicalSortKeyFallbackReason(r.plan.SortKeyPrefix.FallbackReason, r.sortKeyMarkFallbackReason)
	diag.SortedGroupedDistinctReady = r.plan.SortKeyPrefix.SortedGroupedDistinctReady
	diag.SortedGroupedDistinctUsed = false
	diag.SortedGroupedDistinctFallbackReason = r.plan.SortKeyPrefix.SortedGroupedDistinctFallbackReason
	applyColumnPhysicalQueryPredicateDiagnostics(&diag, r.plan.PredicateDiagnostics, matchedRows, 0)
	diag.ScanNanos = scanNanos
	return diag
}

type columnTypedColumnPhysicalQueryAccumulator struct {
	kind        ColumnPhysicalQueryKind
	counts      map[string]int
	distinct    map[string]map[string]struct{}
	hourCounts  [24]int
	groupHours  map[string]map[int]int
	int64Values map[string]int64
	int64Spans  map[string]columnPhysicalQuerySpan
	reduceRows  int
}

func newColumnTypedColumnPhysicalQueryAccumulator(kind ColumnPhysicalQueryKind) *columnTypedColumnPhysicalQueryAccumulator {
	acc := &columnTypedColumnPhysicalQueryAccumulator{kind: kind}
	switch kind {
	case ColumnPhysicalQueryGroupCount:
		acc.counts = make(map[string]int)
	case ColumnPhysicalQueryGroupCountDistinct, ColumnPhysicalQueryGroupCountAndDistinct:
		acc.counts = make(map[string]int)
		acc.distinct = make(map[string]map[string]struct{})
	case ColumnPhysicalQueryGroupHourCount:
		acc.groupHours = make(map[string]map[int]int)
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64:
		acc.int64Values = make(map[string]int64)
	case ColumnPhysicalQueryGroupInt64Span:
		acc.int64Spans = make(map[string]columnPhysicalQuerySpan)
	}
	return acc
}

func (a *columnTypedColumnPhysicalQueryAccumulator) visit(req ColumnPhysicalQueryRequest, values map[string][]columnDeclaredValue, rowIdx int) error {
	a.reduceRows++
	switch a.kind {
	case ColumnPhysicalQueryGroupCount:
		key, err := typedColumnPhysicalQueryStringAt(values, req.GroupColumn, rowIdx)
		if err != nil {
			return err
		}
		a.counts[key]++
	case ColumnPhysicalQueryGroupCountDistinct, ColumnPhysicalQueryGroupCountAndDistinct:
		key, err := typedColumnPhysicalQueryStringAt(values, req.GroupColumn, rowIdx)
		if err != nil {
			return err
		}
		distinct, err := typedColumnPhysicalQueryStringAt(values, req.DistinctColumn, rowIdx)
		if err != nil {
			return err
		}
		if a.kind == ColumnPhysicalQueryGroupCountAndDistinct {
			a.counts[key]++
		}
		set := a.distinct[key]
		if set == nil {
			set = make(map[string]struct{})
			a.distinct[key] = set
		}
		set[distinct] = struct{}{}
	case ColumnPhysicalQueryHourCount:
		value, err := typedColumnPhysicalQueryInt64At(values, req.ValueColumn, rowIdx)
		if err != nil {
			return err
		}
		a.hourCounts[columnPhysicalQueryUTCHour(value)]++
	case ColumnPhysicalQueryGroupHourCount:
		key, err := typedColumnPhysicalQueryStringAt(values, req.GroupColumn, rowIdx)
		if err != nil {
			return err
		}
		value, err := typedColumnPhysicalQueryInt64At(values, req.ValueColumn, rowIdx)
		if err != nil {
			return err
		}
		hour := columnPhysicalQueryUTCHour(value)
		byHour := a.groupHours[key]
		if byHour == nil {
			byHour = make(map[int]int)
			a.groupHours[key] = byHour
		}
		byHour[hour]++
	case ColumnPhysicalQueryGroupMinInt64:
		key, value, err := typedColumnPhysicalQueryStringInt64At(values, req.GroupColumn, req.ValueColumn, rowIdx)
		if err != nil {
			return err
		}
		if cur, ok := a.int64Values[key]; !ok || value < cur {
			a.int64Values[key] = value
		}
	case ColumnPhysicalQueryGroupMaxInt64:
		key, value, err := typedColumnPhysicalQueryStringInt64At(values, req.GroupColumn, req.ValueColumn, rowIdx)
		if err != nil {
			return err
		}
		if cur, ok := a.int64Values[key]; !ok || value > cur {
			a.int64Values[key] = value
		}
	case ColumnPhysicalQueryGroupInt64Span:
		key, value, err := typedColumnPhysicalQueryStringInt64At(values, req.GroupColumn, req.ValueColumn, rowIdx)
		if err != nil {
			return err
		}
		cur, ok := a.int64Spans[key]
		if !ok {
			a.int64Spans[key] = columnPhysicalQuerySpan{min: value, max: value}
			return nil
		}
		if value < cur.min {
			cur.min = value
		}
		if value > cur.max {
			cur.max = value
		}
		a.int64Spans[key] = cur
	default:
		return fmt.Errorf("%w: unsupported typed-column part physical query kind %q", ErrColumnQueryPlanUnsupported, a.kind)
	}
	return nil
}

func (a *columnTypedColumnPhysicalQueryAccumulator) groups(req ColumnPhysicalQueryRequest, dst []ColumnPhysicalQueryGroup) []ColumnPhysicalQueryGroup {
	out := dst[:0]
	switch a.kind {
	case ColumnPhysicalQueryGroupCount:
		for key, count := range a.counts {
			out = append(out, ColumnPhysicalQueryGroup{Key: key, Count: count})
		}
	case ColumnPhysicalQueryGroupCountDistinct, ColumnPhysicalQueryGroupCountAndDistinct:
		for key, set := range a.distinct {
			group := ColumnPhysicalQueryGroup{Key: key, Count: len(set)}
			if a.kind == ColumnPhysicalQueryGroupCountAndDistinct {
				group.Count = a.counts[key]
				group.DistinctCount = len(set)
			}
			out = append(out, group)
		}
	case ColumnPhysicalQueryHourCount:
		for hour, count := range a.hourCounts {
			if count == 0 {
				continue
			}
			out = append(out, ColumnPhysicalQueryGroup{Key: columnPhysicalQueryHourKey(hour), Hour: hour, Count: count})
		}
	case ColumnPhysicalQueryGroupHourCount:
		for key, byHour := range a.groupHours {
			for hour, count := range byHour {
				out = append(out, ColumnPhysicalQueryGroup{Key: key, Hour: hour, Count: count})
			}
		}
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64:
		for key, value := range a.int64Values {
			out = append(out, ColumnPhysicalQueryGroup{Key: key, Int64: value})
		}
	case ColumnPhysicalQueryGroupInt64Span:
		for key, span := range a.int64Spans {
			out = append(out, ColumnPhysicalQueryGroup{Key: key, Int64: span.max - span.min})
		}
	}
	if req.TopK == 0 {
		if a.kind == ColumnPhysicalQueryGroupHourCount {
			sortColumnPhysicalQueryGroupsByKeyHour(out)
		} else {
			sortColumnPhysicalQueryGroupsByKey(out)
		}
	}
	return out
}

func typedColumnPhysicalQueryPredicatesMatch(values map[string][]columnDeclaredValue, specs []columnPhysicalQueryPredicateSpec, rowIdx int) (bool, error) {
	for _, spec := range specs {
		value, err := typedColumnPhysicalQueryStringValueAt(values, spec.column, rowIdx)
		if err != nil {
			return false, err
		}
		if !typedColumnPhysicalQueryPredicateValueMatches(value, spec) {
			return false, nil
		}
	}
	return true, nil
}

func typedColumnPhysicalQueryPredicateValueMatches(value columnDeclaredValue, spec columnPhysicalQueryPredicateSpec) bool {
	if value.StringBytes != nil {
		for _, target := range spec.valueBytes {
			if bytes.Equal(value.StringBytes, target) {
				return true
			}
		}
		return false
	}
	for _, target := range spec.values {
		if value.String == target {
			return true
		}
	}
	return false
}

func typedColumnPhysicalQueryStringInt64At(values map[string][]columnDeclaredValue, groupColumn, valueColumn string, rowIdx int) (string, int64, error) {
	key, err := typedColumnPhysicalQueryStringAt(values, groupColumn, rowIdx)
	if err != nil {
		return "", 0, err
	}
	value, err := typedColumnPhysicalQueryInt64At(values, valueColumn, rowIdx)
	if err != nil {
		return "", 0, err
	}
	return key, value, nil
}

func typedColumnPhysicalQueryStringAt(values map[string][]columnDeclaredValue, column string, rowIdx int) (string, error) {
	value, err := typedColumnPhysicalQueryStringValueAt(values, column, rowIdx)
	if err != nil {
		return "", err
	}
	if value.StringBytes != nil {
		return string(value.StringBytes), nil
	}
	return value.String, nil
}

func typedColumnPhysicalQueryStringValueAt(values map[string][]columnDeclaredValue, column string, rowIdx int) (columnDeclaredValue, error) {
	columnValues, ok := values[column]
	if !ok {
		return columnDeclaredValue{}, fmt.Errorf("collections: typed-column part physical query missing string column %q", column)
	}
	if rowIdx < 0 || rowIdx >= len(columnValues) {
		return columnDeclaredValue{}, fmt.Errorf("collections: typed-column part physical query row_index=%d outside column %q rows=%d", rowIdx, column, len(columnValues))
	}
	value := columnValues[rowIdx]
	if value.Type != ColumnStoreValueString {
		return columnDeclaredValue{}, fmt.Errorf("%w: typed-column part physical query expected string column %q, got %q", ErrColumnQueryPlanUnsupported, column, value.Type)
	}
	if !value.Present || value.Null {
		return columnDeclaredValue{}, fmt.Errorf("%w: typed-column part physical query column %q has null/missing string value", ErrColumnQueryPlanUnsupported, column)
	}
	return value, nil
}

func typedColumnPhysicalQueryInt64At(values map[string][]columnDeclaredValue, column string, rowIdx int) (int64, error) {
	columnValues, ok := values[column]
	if !ok {
		return 0, fmt.Errorf("collections: typed-column part physical query missing int64 column %q", column)
	}
	if rowIdx < 0 || rowIdx >= len(columnValues) {
		return 0, fmt.Errorf("collections: typed-column part physical query row_index=%d outside column %q rows=%d", rowIdx, column, len(columnValues))
	}
	value := columnValues[rowIdx]
	if value.Type != ColumnStoreValueInt64 {
		return 0, fmt.Errorf("%w: typed-column part physical query expected int64 column %q, got %q", ErrColumnQueryPlanUnsupported, column, value.Type)
	}
	if !value.Present || value.Null {
		return 0, fmt.Errorf("%w: typed-column part physical query column %q has null/missing int64 value", ErrColumnQueryPlanUnsupported, column)
	}
	return value.Int64, nil
}
