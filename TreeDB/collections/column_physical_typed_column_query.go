package collections

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"slices"
	"sort"
	"time"
)

const columnTypedColumnDenseGroupCountDistinctMaxBitsetWords = 2 << 20

const (
	columnTypedColumnDenseGroupCountDistinctReducerPairBitset   = "pair_bitset"
	columnTypedColumnDenseGroupCountDistinctReducerLocalBitset  = "local_dictionary_pair_bitset"
	columnTypedColumnDenseGroupCountDistinctReducerActiveBitset = "active_group_bitset"
	columnTypedColumnDenseGroupCountDistinctReducerSortedPairs  = "sorted_packed_pairs"
	columnTypedColumnDenseInt64SpanReducerGlobalCodes           = "global_codes"
	columnTypedColumnDenseInt64SpanReducerLocalMap              = "local_map"
)

type columnTypedColumnPhysicalQueryPlan struct {
	Fields                  []TypedStorageField
	Selected                []bool
	ProjectedColumns        []string
	PredicateDiagnostics    columnPhysicalQueryPredicateDiagnosticPlan
	GroupColumn             string
	ValueColumn             string
	DistinctColumn          string
	PredicateSpecs          []columnPhysicalQueryPredicateSpec
	SortKey                 []ColumnSortKey
	SortKeyPrefix           columnTypedColumnSortKeyPrefixPlan
	DenseGroupCount         bool
	DenseGroupCountDistinct bool
	DenseGroupHourCount     bool
	DenseInt64Span          bool
	TimeOrderTopK           bool
	NullableStringValues    bool
}

type columnTypedColumnDenseGroupCountPart struct {
	Cardinality int
	Dictionary  []string
	Codes       []uint32
	Valid       []bool
}

type columnTypedColumnDensePredicatePart struct {
	Codes               []uint32
	Valid               []bool
	Allowed             []uint64
	SingleCode          uint32
	SingleCodeAllowed   bool
	MissingMatchesEmpty bool
	RejectsAll          bool
}

type columnTypedColumnDenseStringCodeColumn struct {
	Codes            []uint32
	Valid            []bool
	Dictionary       []string
	GlobalCodes      []uint32
	GlobalDictionary []string
}

type columnTypedColumnDenseGroupCountDistinctPart struct {
	Rows       int
	Group      columnTypedColumnDenseStringCodeColumn
	Distinct   columnTypedColumnDenseStringCodeColumn
	Predicates []columnTypedColumnDensePredicatePart
}

type columnTypedColumnDenseGroupHourCountPart struct {
	Cardinality      int
	DictionaryByCode map[int64]string
	GroupCodes       []uint32
	GroupValid       []bool
	Values           []int64
	Predicates       []columnTypedColumnDensePredicatePart
}

type columnTypedColumnDenseInt64SpanPart struct {
	Cardinality      int
	DictionaryByCode map[int64]string
	GroupCodes       []uint32
	GroupValid       []bool
	GlobalRanks      []uint32
	GlobalDictionary []string
	Values           []int64
	Predicates       []columnTypedColumnDensePredicatePart
}

type columnTypedColumnSortedGroupedDistinctCodeColumn struct {
	Codes            []int64
	Dictionary       []string
	GlobalCodes      []uint32
	GlobalDictionary []string
}

type columnTypedColumnSortedGroupedDistinctPredicate struct {
	Codes      []int64
	Allowed    []uint64
	RejectsAll bool
}

type columnTypedColumnSortedGroupedDistinctPart struct {
	Rows         int
	RowIndexes   []int
	PhysicalRows []int
	Group        columnTypedColumnSortedGroupedDistinctCodeColumn
	Distinct     columnTypedColumnSortedGroupedDistinctCodeColumn
	Predicates   []columnTypedColumnSortedGroupedDistinctPredicate
}

type columnTypedColumnPhysicalQueryPart struct {
	Ref                       columnManifestAssetRefForScan
	PhysicalRef               columnManifestAssetRefForScan
	Values                    map[string][]columnDeclaredValue
	RowIndexes                []int
	PhysicalRowIndexes        []int
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
	DenseGroupCountDistinct   *columnTypedColumnDenseGroupCountDistinctPart
	DenseGroupHourCount       *columnTypedColumnDenseGroupHourCountPart
	DenseInt64Span            *columnTypedColumnDenseInt64SpanPart
	SortedGroupedDistinct     *columnTypedColumnSortedGroupedDistinctPart
	TimeOrderTopK             *columnTypedColumnTimeOrderTopKPart
}

type columnTypedColumnPhysicalQueryRunner struct {
	plan                                  columnTypedColumnPhysicalQueryPlan
	parts                                 []columnTypedColumnPhysicalQueryPart
	aggregateSummary                      *columnTypedColumnPhysicalAggregateSummary
	assetBytes                            int64
	sections                              int
	sectionBytes                          uint64
	granulesConsidered                    int
	granulesDecoded                       int
	granulesSkipped                       int
	decodedBlocks                         int
	decodedPayloadBytes                   uint64
	sortKeyMarkChecks                     int
	sortKeyMarkMatches                    int
	sortKeyMarkSkips                      int
	sortKeyMarkFallbackReason             string
	segmentFileCacheHits                  uint64
	segmentFileCacheMisses                uint64
	denseGroupCounts                      map[string]int
	denseLocalCounts                      []int
	denseGroupCountDistinctCounts         []int
	denseGroupCountDistinctDistinctCounts []int
	denseGroupCountDistinctPairBits       []uint64
	denseGroupCountDistinctGroupActive    []bool
	denseGroupCountDistinctGroupOffsets   []int
	denseGroupCountDistinctActiveGroups   []int
	denseGroupCountDistinctPairList       []uint64
	denseGroupHourCounts                  map[string][24]int
	denseLocalHourCounts                  []int
	denseSpanValues                       map[string]columnPhysicalQuerySpan
	denseLocalSpans                       []columnPhysicalQuerySpan
	denseLocalSpanSeen                    []bool
	timeOrderMinValues                    map[string]int64
	timeOrderHeap                         columnTypedColumnTimeOrderTopKHeap
	timeOrderTopKScratch                  []ColumnPhysicalQueryGroup
	resultGroups                          []ColumnPhysicalQueryGroup
}

type columnTypedColumnPhysicalAggregateSummary struct {
	groups              []ColumnPhysicalQueryGroup
	matchedRows         int
	reduceRows          int
	denseGroupCount     bool
	denseGroupHourCount bool
}

func (c *Collection) runColumnPhysicalQueryTypedColumnPartInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, bool, error) {
	if !columnTypedColumnPhysicalQueryTouchesTypedColumnPart(view.FullConfig, req) {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	start := time.Now()
	plan, candidate, err := planColumnTypedColumnPhysicalQuery(view.FullConfig, req)
	if err != nil {
		if view.MutationParts != 0 && candidate {
			result := ColumnPhysicalQueryResult{Diagnostics: columnTypedColumnPhysicalMutationDiagnostics(view, req)}
			annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedColumnPartSection, ColumnPhysicalQueryFallbackMutationVisibilityUnsupported)
			applyColumnPhysicalQueryPredicateDiagnostics(&result.Diagnostics, newColumnPhysicalQueryPredicateDiagnosticPlan(req), 0, 0)
			result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
			return result, candidate, err
		}
		return ColumnPhysicalQueryResult{}, candidate, err
	}
	if !candidate {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if view.MutationParts != 0 {
		return runColumnPhysicalQueryTypedColumnPartLatestVisibleInSnapshotView(view, req, plan, start)
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		result := ColumnPhysicalQueryResult{}
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()
	runner, candidate, err := prepareColumnTypedColumnPhysicalQueryRunner(view, req, &readCache, false)
	if err != nil || !candidate {
		result := ColumnPhysicalQueryResult{}
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, candidate, err
	}
	result, err := runner.run(view, req)
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	return result, true, err
}

func columnTypedColumnPhysicalMutationDiagnostics(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) ColumnPhysicalQueryDiagnostics {
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	return diag
}

func runColumnPhysicalQueryTypedColumnPartMutationUnsupported(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, start time.Time) (ColumnPhysicalQueryResult, bool, error) {
	result := ColumnPhysicalQueryResult{Diagnostics: columnTypedColumnPhysicalMutationDiagnostics(view, req)}
	annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedColumnPartSection, ColumnPhysicalQueryFallbackMutationVisibilityUnsupported)
	predicateDiagnostics := newColumnPhysicalQueryPredicateDiagnosticPlan(req)
	applyColumnPhysicalQueryPredicateDiagnostics(&result.Diagnostics, predicateDiagnostics, 0, 0)
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	defer func() { _ = readCache.close() }()
	refsByGeneration, err := typedColumnPhysicalQueryRefsByGeneration(view)
	if err != nil {
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	if err := validateTypedColumnMultipartAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	var visibilityDiag TypedColumnInt64PredicateScanDiagnostics
	state, err := buildTypedColumnLatestVisibilityState(view, &readCache, &visibilityDiag)
	result.Diagnostics.PhysicalBytesScanned += visibilityDiag.PhysicalBytesScanned
	result.Diagnostics.SegmentFileCacheHits = visibilityDiag.SegmentFileCacheHits
	result.Diagnostics.SegmentFileCacheMisses = visibilityDiag.SegmentFileCacheMisses
	if state != nil {
		result.Diagnostics.RowsScanned = state.CandidateRows
		result.Diagnostics.DeletedRows = state.DeletedRows
		result.Diagnostics.VisibilityRows = state.VisibleRows
	}
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	result.Diagnostics.VisibilityNanos = result.Diagnostics.ScanNanos
	if err != nil {
		return result, true, err
	}
	if typedColumnRefsHaveSortKey(refsByGeneration) {
		return result, true, typedColumnSortedMutationVisibilityUnsupported("typed-column part physical query")
	}
	return result, true, fmt.Errorf("%w: typed-column part physical query with mutation visibility is deferred to multipart reducers", ErrColumnQueryPlanUnsupported)
}

func runColumnPhysicalQueryTypedColumnPartLatestVisibleInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, plan columnTypedColumnPhysicalQueryPlan, start time.Time) (ColumnPhysicalQueryResult, bool, error) {
	if !columnTypedColumnPhysicalQueryUseDenseLatestVisible(plan, req) && columnPhysicalQueryHasPredicates(req) && !columnTypedColumnPhysicalQueryUseSortedLatestVisible(plan, req) {
		return runColumnPhysicalQueryTypedColumnPartMutationUnsupported(view, req, start)
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		result := ColumnPhysicalQueryResult{}
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	defer func() { _ = readCache.close() }()
	refsByGeneration, err := typedColumnPhysicalQueryRefsByGeneration(view)
	if err != nil {
		result := ColumnPhysicalQueryResult{Diagnostics: columnTypedColumnPhysicalMutationDiagnostics(view, req)}
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedColumnPartSection, ColumnPhysicalQueryFallbackMutationVisibilityUnsupported)
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	if err := validateTypedColumnMultipartAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
		result := ColumnPhysicalQueryResult{Diagnostics: columnTypedColumnPhysicalMutationDiagnostics(view, req)}
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedColumnPartSection, ColumnPhysicalQueryFallbackMutationVisibilityUnsupported)
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}

	visibilityStart := time.Now()
	var visibilityDiag TypedColumnInt64PredicateScanDiagnostics
	state, err := buildTypedColumnLatestVisibilityState(view, &readCache, &visibilityDiag)
	visibilityNanos := time.Since(visibilityStart).Nanoseconds()
	if err != nil {
		result := ColumnPhysicalQueryResult{Diagnostics: columnTypedColumnPhysicalMutationDiagnostics(view, req)}
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedColumnPartSection, ColumnPhysicalQueryFallbackMutationVisibilityUnsupported)
		result.Diagnostics.PhysicalBytesScanned += visibilityDiag.PhysicalBytesScanned
		result.Diagnostics.SegmentFileCacheHits = visibilityDiag.SegmentFileCacheHits
		result.Diagnostics.SegmentFileCacheMisses = visibilityDiag.SegmentFileCacheMisses
		result.Diagnostics.VisibilityNanos = visibilityNanos
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	sortedRefs := typedColumnRefsHaveSortKey(refsByGeneration)
	if sortedRefs && !columnTypedColumnPhysicalQueryUseSortedLatestVisible(plan, req) {
		result := ColumnPhysicalQueryResult{Diagnostics: columnTypedColumnPhysicalMutationDiagnostics(view, req)}
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedColumnPartSection, ColumnPhysicalQueryFallbackMutationVisibilityUnsupported)
		applyColumnPhysicalQueryPredicateDiagnostics(&result.Diagnostics, plan.PredicateDiagnostics, 0, 0)
		applyTypedColumnLatestVisibilityDiagnostics(&result.Diagnostics, state, visibilityDiag.PhysicalBytesScanned, visibilityNanos)
		result.Diagnostics.SegmentFileCacheHits = visibilityDiag.SegmentFileCacheHits
		result.Diagnostics.SegmentFileCacheMisses = visibilityDiag.SegmentFileCacheMisses
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, typedColumnSortedMutationVisibilityUnsupported("typed-column part physical query")
	}
	if columnTypedColumnPhysicalQueryUseSortedLatestVisible(plan, req) && !sortedRefs {
		result := ColumnPhysicalQueryResult{Diagnostics: columnTypedColumnPhysicalMutationDiagnostics(view, req)}
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedColumnPartSection, ColumnPhysicalQueryFallbackMutationVisibilityUnsupported)
		applyColumnPhysicalQueryPredicateDiagnostics(&result.Diagnostics, plan.PredicateDiagnostics, 0, 0)
		applyTypedColumnLatestVisibilityDiagnostics(&result.Diagnostics, state, visibilityDiag.PhysicalBytesScanned, visibilityNanos)
		result.Diagnostics.SegmentFileCacheHits = visibilityDiag.SegmentFileCacheHits
		result.Diagnostics.SegmentFileCacheMisses = visibilityDiag.SegmentFileCacheMisses
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, fmt.Errorf("%w: typed-column sorted latest-visible physical query requires sorted typed_column_part assets", ErrColumnQueryPlanUnsupported)
	}

	runner, err := decodeColumnTypedColumnPhysicalQueryRunnerParts(view, req, plan, refsByGeneration, &readCache, columnTypedColumnPhysicalQueryUseSortedLatestVisible(plan, req), false, false)
	if err != nil {
		result := ColumnPhysicalQueryResult{Diagnostics: columnTypedColumnPhysicalMutationDiagnostics(view, req)}
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedColumnPartSection, ColumnPhysicalQueryFallbackMutationVisibilityUnsupported)
		applyColumnPhysicalQueryPredicateDiagnostics(&result.Diagnostics, plan.PredicateDiagnostics, 0, 0)
		applyTypedColumnLatestVisibilityDiagnostics(&result.Diagnostics, state, visibilityDiag.PhysicalBytesScanned, visibilityNanos)
		result.Diagnostics.SegmentFileCacheHits = readCache.hits
		result.Diagnostics.SegmentFileCacheMisses = readCache.misses
		result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
		return result, true, err
	}
	result, err := runner.runLatestVisible(view, req, state, visibilityDiag.PhysicalBytesScanned, visibilityNanos)
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	result.Diagnostics.SegmentFileCacheHits = readCache.hits
	result.Diagnostics.SegmentFileCacheMisses = readCache.misses
	if err != nil {
		return result, true, err
	}
	return result, true, nil
}

func columnTypedColumnPhysicalQueryUseDenseLatestVisible(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest) bool {
	return columnTypedColumnPhysicalQueryUseDenseGroupCount(plan, req) ||
		columnTypedColumnPhysicalQueryUseDenseGroupHourCount(plan, req) ||
		columnTypedColumnPhysicalQueryUseDenseInt64Span(plan, req)
}

func columnTypedColumnPhysicalQueryUseSortedLatestVisible(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest) bool {
	return columnTypedColumnPhysicalQueryUseSortedGroupedDistinct(plan, req) ||
		columnTypedColumnPhysicalQueryUseSortedMarkPrunedTopK(plan, req) ||
		columnTypedColumnPhysicalQueryUseTimeOrderTopK(plan, req)
}

func columnTypedColumnPhysicalQueryUseSortedMarkPrunedTopK(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest) bool {
	return !plan.NullableStringValues &&
		req.Kind == ColumnPhysicalQueryGroupMinInt64 &&
		req.GroupColumn != "" && req.ValueColumn != "" && req.DistinctColumn == "" &&
		req.AggregateMetadataName == "" && req.TopK > 0 && req.TopKOrder == ColumnPhysicalQueryTopKInt64Asc &&
		len(plan.PredicateSpecs) != 0 && plan.SortKeyPrefix.Planned && !columnTypedColumnPhysicalQueryUseTimeOrderTopK(plan, req)
}

func applyTypedColumnLatestVisibilityDiagnostics(diag *ColumnPhysicalQueryDiagnostics, state *typedColumnLatestVisibilityState, physicalBytes int64, visibilityNanos int64) {
	if diag == nil {
		return
	}
	if state != nil {
		diag.VisibilityRows = state.VisibleRows
		diag.DeletedRows = state.DeletedRows
	}
	diag.PhysicalBytesScanned += physicalBytes
	diag.VisibilityNanos = visibilityNanos
}

func prepareColumnTypedColumnPhysicalQueryRunner(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, readCache *columnPhysicalAssetReadCache, prepareSummaries bool) (*columnTypedColumnPhysicalQueryRunner, bool, error) {
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
	if len(refsByGeneration) == 0 {
		runner := &columnTypedColumnPhysicalQueryRunner{plan: plan, parts: make([]columnTypedColumnPhysicalQueryPart, 0, len(view.AssetRefs))}
		if len(view.AssetRefs) == 0 {
			return runner, true, nil
		}
		return nil, true, errors.New("collections: missing typed_column_part assets for typed-column part physical query")
	}
	if _, err := validateTypedColumnPhysicalAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
		return nil, true, typedColumnPhysicalQueryPairingError(err)
	}
	runner, err := decodeColumnTypedColumnPhysicalQueryRunnerParts(view, req, plan, refsByGeneration, readCache, false, prepareSummaries, prepareSummaries)
	if err != nil {
		return nil, true, err
	}
	if prepareSummaries {
		if err := prepareColumnTypedColumnPhysicalAggregateSummary(runner, req); err != nil {
			return nil, true, err
		}
	}
	return runner, true, nil
}

func decodeColumnTypedColumnPhysicalQueryRunnerParts(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, plan columnTypedColumnPhysicalQueryPlan, refsByGeneration map[uint64]columnManifestAssetRefForScan, readCache *columnPhysicalAssetReadCache, includePhysicalRows bool, prepareDenseInt64SpanGlobalCodes bool, prepareDenseGroupCountDistinctGlobalCodes bool) (*columnTypedColumnPhysicalQueryRunner, error) {
	if readCache == nil {
		return nil, errors.New("collections: typed-column part physical query missing read cache")
	}
	runner := &columnTypedColumnPhysicalQueryRunner{plan: plan, parts: make([]columnTypedColumnPhysicalQueryPart, 0, len(view.AssetRefs))}
	if len(refsByGeneration) == 0 {
		if len(view.AssetRefs) == 0 {
			return runner, nil
		}
		return nil, errors.New("collections: missing typed_column_part assets for typed-column part physical query")
	}

	// Disable returnViews during decode to avoid pinning mmaps/handles for the runner lifetime,
	// since we fully decode to Go types and don't retain references to raw bytes.
	savedReturnViews := readCache.returnViews
	readCache.returnViews = false
	defer func() { readCache.returnViews = savedReturnViews }()

	var rawScratch []byte
	allowDenseGroupCountDistinct := view.MutationParts == 0
	for _, physical := range view.AssetRefs {
		if physical.Role == ColumnManifestPartRoleTombstone || physical.Reason == ColumnPublishOperationDelete {
			continue
		}
		typedRef, ok := refsByGeneration[physical.Ref.Generation]
		if !ok {
			return nil, fmt.Errorf("collections: missing typed_column_part asset for generation=%d", physical.Ref.Generation)
		}
		var part columnTypedColumnPhysicalQueryPart
		part, rangeOK, err := decodeTypedColumnPhysicalQueryDensePartFromRanges(plan, req, view.FullConfig.SchemaHash, typedRef, physical, readCache, allowDenseGroupCountDistinct)
		runner.segmentFileCacheHits = readCache.hits
		runner.segmentFileCacheMisses = readCache.misses
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column part physical query range decode generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		if !rangeOK {
			raw, err := readCache.read(typedRef.Ref, rawScratch)
			runner.segmentFileCacheHits = readCache.hits
			runner.segmentFileCacheMisses = readCache.misses
			if err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) {
					return nil, fmt.Errorf("collections: typed-column part physical query read generation=%d part_id=%d short read: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
				}
				return nil, fmt.Errorf("collections: typed-column part physical query read generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
			}
			rawScratch = raw
			switch {
			case columnTypedColumnPhysicalQueryUseDenseGroupCount(plan, req):
				part, err = decodeTypedColumnPhysicalQueryDenseGroupCountPart(plan, view.FullConfig.SchemaHash, typedRef, physical, raw)
			case columnTypedColumnPhysicalQueryUseDenseGroupHourCount(plan, req):
				part, err = decodeTypedColumnPhysicalQueryDenseGroupHourCountPart(plan, view.FullConfig.SchemaHash, typedRef, physical, raw)
			case columnTypedColumnPhysicalQueryUseDenseInt64Span(plan, req):
				part, err = decodeTypedColumnPhysicalQueryDenseInt64SpanPart(plan, view.FullConfig.SchemaHash, typedRef, physical, raw)
			case columnTypedColumnPhysicalQueryUseTimeOrderTopK(plan, req):
				part, err = decodeTypedColumnPhysicalQueryTimeOrderTopKPart(plan, view.FullConfig.SchemaHash, typedRef, physical, raw, includePhysicalRows)
			case columnTypedColumnPhysicalQueryUseSortedGroupedDistinct(plan, req):
				part, err = decodeTypedColumnPhysicalQuerySortedGroupedDistinctPart(plan, view.FullConfig.SchemaHash, typedRef, physical, raw, includePhysicalRows)
			case allowDenseGroupCountDistinct && columnTypedColumnPhysicalQueryUseDenseGroupCountDistinct(plan, req):
				part, err = decodeTypedColumnPhysicalQueryDenseGroupCountDistinctPart(plan, view.FullConfig.SchemaHash, typedRef, physical, raw)
			default:
				part, err = decodeTypedColumnPhysicalQueryPart(plan, view.FullConfig.SchemaHash, typedRef, physical, raw, includePhysicalRows)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("collections: typed-column part physical query decode generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
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
		return nil, errors.New("collections: typed-column part physical query has no live typed_column_part assets")
	}
	if columnTypedColumnPhysicalQueryUseSortedGroupedDistinct(plan, req) {
		if err := prepareColumnTypedColumnSortedGroupedDistinctGlobalCodes(runner.parts); err != nil {
			return nil, err
		}
	} else if prepareDenseGroupCountDistinctGlobalCodes && allowDenseGroupCountDistinct && columnTypedColumnPhysicalQueryUseDenseGroupCountDistinct(plan, req) {
		if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalCodes(runner.parts); err != nil {
			return nil, err
		}
	} else if prepareDenseInt64SpanGlobalCodes && columnTypedColumnPhysicalQueryUseDenseInt64Span(plan, req) {
		if err := prepareColumnTypedColumnDenseInt64SpanGlobalCodes(runner.parts); err != nil {
			return nil, err
		}
	}
	return runner, nil
}

func prepareColumnTypedColumnPhysicalAggregateSummary(runner *columnTypedColumnPhysicalQueryRunner, req ColumnPhysicalQueryRequest) error {
	if runner == nil {
		return nil
	}
	var (
		summary *columnTypedColumnPhysicalAggregateSummary
		err     error
	)
	switch {
	case columnTypedColumnPhysicalQueryUseDenseGroupCount(runner.plan, req):
		summary, err = buildColumnTypedColumnDenseGroupCountSummary(runner.parts)
	case columnTypedColumnPhysicalQueryUseDenseGroupHourCount(runner.plan, req):
		summary, err = buildColumnTypedColumnDenseGroupHourCountSummary(runner.parts)
	}
	if err != nil {
		return err
	}
	runner.aggregateSummary = summary
	return nil
}

func buildColumnTypedColumnDenseGroupCountSummary(parts []columnTypedColumnPhysicalQueryPart) (*columnTypedColumnPhysicalAggregateSummary, error) {
	counts := make(map[string]int, 16)
	reduceRows := 0
	var localCounts []int
	for partIdx := range parts {
		dense := parts[partIdx].DenseGroupCount
		if dense == nil {
			return nil, fmt.Errorf("collections: dense typed-column group-count missing prepared part %d", partIdx)
		}
		if err := validateColumnTypedColumnDenseGroupCountPart(dense, partIdx); err != nil {
			return nil, err
		}
		if cap(localCounts) < dense.Cardinality {
			localCounts = make([]int, dense.Cardinality)
		} else {
			localCounts = localCounts[:dense.Cardinality]
			clear(localCounts)
		}
		missingCount := 0
		for rowIdx, code := range dense.Codes {
			if !columnTypedColumnDenseCodeValid(dense.Valid, rowIdx) {
				missingCount++
				continue
			}
			localIdx, ok := columnDictionaryCodeIndex(code, len(localCounts))
			if !ok {
				return nil, fmt.Errorf("collections: dense typed-column group-count part %d code[%d]=%d outside cardinality=%d", partIdx, rowIdx, code, len(localCounts))
			}
			localCounts[localIdx]++
		}
		reduceRows += len(dense.Codes)
		if missingCount != 0 {
			counts[""] += missingCount
		}
		for localCode, count := range localCounts {
			if count == 0 {
				continue
			}
			key, ok := columnTypedColumnDenseGroupCountDictionaryValue(dense, localCode)
			if !ok {
				return nil, fmt.Errorf("collections: dense typed-column group-count part %d dictionary missing local code %d", partIdx, localCode)
			}
			counts[key] += count
		}
	}
	groups := make([]ColumnPhysicalQueryGroup, 0, len(counts))
	for key, count := range counts {
		if count == 0 {
			continue
		}
		groups = append(groups, ColumnPhysicalQueryGroup{Key: key, Count: count})
	}
	sortColumnPhysicalQueryGroupsByKey(groups)
	return &columnTypedColumnPhysicalAggregateSummary{
		groups:          groups,
		reduceRows:      reduceRows,
		denseGroupCount: true,
	}, nil
}

func buildColumnTypedColumnDenseGroupHourCountSummary(parts []columnTypedColumnPhysicalQueryPart) (*columnTypedColumnPhysicalAggregateSummary, error) {
	counts := make(map[string][24]int, 16)
	matchedRows := 0
	reduceRows := 0
	var localHourCounts []int
	for partIdx := range parts {
		dense := parts[partIdx].DenseGroupHourCount
		if dense == nil {
			return nil, fmt.Errorf("collections: dense typed-column group-hour missing prepared part %d", partIdx)
		}
		if err := validateColumnTypedColumnDenseGroupHourCountPart(dense, partIdx); err != nil {
			return nil, err
		}
		needLocal := dense.Cardinality * 24
		if cap(localHourCounts) < needLocal {
			localHourCounts = make([]int, needLocal)
		} else {
			localHourCounts = localHourCounts[:needLocal]
			clear(localHourCounts)
		}
		if columnTypedColumnDensePredicatesRejectAll(dense.Predicates) {
			continue
		}
		var missingHourCounts [24]int
		for rowIdx, code := range dense.GroupCodes {
			if !columnTypedColumnDensePredicatesMatch(dense.Predicates, rowIdx) {
				continue
			}
			if len(dense.Predicates) != 0 {
				matchedRows++
			}
			reduceRows++
			hour := columnPhysicalQueryUTCHour(dense.Values[rowIdx])
			if !columnTypedColumnDenseCodeValid(dense.GroupValid, rowIdx) {
				missingHourCounts[hour]++
				continue
			}
			localIdx, ok := columnDictionaryCodeIndex(code, dense.Cardinality)
			if !ok {
				return nil, fmt.Errorf("collections: dense typed-column group-hour part %d code[%d]=%d outside cardinality=%d", partIdx, rowIdx, code, dense.Cardinality)
			}
			localHourCounts[localIdx*24+hour]++
		}
		var missingByHour [24]int
		missingSeen := false
		for hour, count := range missingHourCounts {
			if count == 0 {
				continue
			}
			if !missingSeen {
				missingByHour = counts[""]
				missingSeen = true
			}
			missingByHour[hour] += count
		}
		if missingSeen {
			counts[""] = missingByHour
		}
		for localCode := 0; localCode < dense.Cardinality; localCode++ {
			key := ""
			var byHour [24]int
			seen := false
			base := localCode * 24
			for hour := 0; hour < 24; hour++ {
				count := localHourCounts[base+hour]
				if count == 0 {
					continue
				}
				if !seen {
					var ok bool
					key, ok = dense.DictionaryByCode[int64(localCode)]
					if !ok {
						return nil, fmt.Errorf("collections: dense typed-column group-hour part %d dictionary missing local code %d", partIdx, localCode)
					}
					byHour = counts[key]
					seen = true
				}
				byHour[hour] += count
			}
			if seen {
				counts[key] = byHour
			}
		}
	}
	groups := make([]ColumnPhysicalQueryGroup, 0, len(counts))
	for key, byHour := range counts {
		for hour, count := range byHour {
			if count == 0 {
				continue
			}
			groups = append(groups, ColumnPhysicalQueryGroup{Key: key, Hour: hour, Count: count})
		}
	}
	sortColumnPhysicalQueryGroupsByKeyHour(groups)
	return &columnTypedColumnPhysicalAggregateSummary{
		groups:              groups,
		matchedRows:         matchedRows,
		reduceRows:          reduceRows,
		denseGroupHourCount: true,
	}, nil
}

func prepareColumnTypedColumnSortedGroupedDistinctGlobalCodes(parts []columnTypedColumnPhysicalQueryPart) error {
	groupDict, groupRanks, err := columnTypedColumnSortedGroupedDistinctGlobalDictionary(parts, func(part *columnTypedColumnSortedGroupedDistinctPart) *columnTypedColumnSortedGroupedDistinctCodeColumn {
		return &part.Group
	})
	if err != nil {
		return err
	}
	distinctDict, distinctRanks, err := columnTypedColumnSortedGroupedDistinctGlobalDictionary(parts, func(part *columnTypedColumnSortedGroupedDistinctPart) *columnTypedColumnSortedGroupedDistinctCodeColumn {
		return &part.Distinct
	})
	if err != nil {
		return err
	}
	for partIdx := range parts {
		part := parts[partIdx].SortedGroupedDistinct
		if part == nil {
			return fmt.Errorf("collections: sorted grouped-distinct missing prepared part %d", partIdx)
		}
		if err := prepareColumnTypedColumnSortedGroupedDistinctGlobalColumnCodes(&part.Group, groupDict, groupRanks); err != nil {
			return fmt.Errorf("collections: sorted grouped-distinct group part %d: %w", partIdx, err)
		}
		if err := prepareColumnTypedColumnSortedGroupedDistinctGlobalColumnCodes(&part.Distinct, distinctDict, distinctRanks); err != nil {
			return fmt.Errorf("collections: sorted grouped-distinct distinct part %d: %w", partIdx, err)
		}
	}
	return nil
}

func columnTypedColumnSortedGroupedDistinctGlobalDictionary(parts []columnTypedColumnPhysicalQueryPart, selectColumn func(*columnTypedColumnSortedGroupedDistinctPart) *columnTypedColumnSortedGroupedDistinctCodeColumn) ([]string, map[string]uint32, error) {
	values := make(map[string]struct{})
	for partIdx := range parts {
		part := parts[partIdx].SortedGroupedDistinct
		if part == nil {
			return nil, nil, fmt.Errorf("collections: sorted grouped-distinct missing prepared part %d", partIdx)
		}
		column := selectColumn(part)
		for _, value := range column.Dictionary {
			values[value] = struct{}{}
		}
	}
	dictionary := make([]string, 0, len(values))
	for value := range values {
		dictionary = append(dictionary, value)
	}
	sort.Strings(dictionary)
	ranks := make(map[string]uint32, len(dictionary))
	for rank, value := range dictionary {
		if uint64(rank) > uint64(^uint32(0)) {
			return nil, nil, fmt.Errorf("collections: sorted grouped-distinct global dictionary cardinality=%d exceeds uint32", len(dictionary))
		}
		ranks[value] = uint32(rank)
	}
	return dictionary, ranks, nil
}

func prepareColumnTypedColumnSortedGroupedDistinctGlobalColumnCodes(column *columnTypedColumnSortedGroupedDistinctCodeColumn, globalDictionary []string, ranks map[string]uint32) error {
	localRanks := make([]uint32, len(column.Dictionary))
	for localCode, value := range column.Dictionary {
		rank, ok := ranks[value]
		if !ok {
			return fmt.Errorf("local dictionary value %q missing from global dictionary", value)
		}
		localRanks[localCode] = rank
	}
	globalCodes := make([]uint32, len(column.Codes))
	for row, localCode := range column.Codes {
		if localCode < 0 || localCode >= int64(len(localRanks)) {
			return fmt.Errorf("row=%d code=%d outside cardinality=%d", row, localCode, len(localRanks))
		}
		globalCodes[row] = localRanks[localCode]
	}
	column.GlobalCodes = globalCodes
	column.GlobalDictionary = globalDictionary
	return nil
}

func prepareColumnTypedColumnDenseGroupCountDistinctGlobalCodes(parts []columnTypedColumnPhysicalQueryPart) error {
	groupDict, groupRanks, err := columnTypedColumnDenseGroupCountDistinctGlobalDictionary(parts, func(part *columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn {
		return &part.Group
	})
	if err != nil {
		return err
	}
	distinctDict, distinctRanks, err := columnTypedColumnDenseGroupCountDistinctGlobalDictionary(parts, func(part *columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn {
		return &part.Distinct
	})
	if err != nil {
		return err
	}
	for partIdx := range parts {
		part := parts[partIdx].DenseGroupCountDistinct
		if part == nil {
			return fmt.Errorf("collections: dense grouped count-distinct missing prepared part %d", partIdx)
		}
		if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalColumnCodes(&part.Group, groupDict, groupRanks); err != nil {
			return fmt.Errorf("collections: dense grouped count-distinct group part %d: %w", partIdx, err)
		}
		if err := prepareColumnTypedColumnDenseGroupCountDistinctGlobalColumnCodes(&part.Distinct, distinctDict, distinctRanks); err != nil {
			return fmt.Errorf("collections: dense grouped count-distinct distinct part %d: %w", partIdx, err)
		}
	}
	return nil
}

func columnTypedColumnDenseGroupCountDistinctGlobalDictionary(parts []columnTypedColumnPhysicalQueryPart, selectColumn func(*columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn) ([]string, map[string]uint32, error) {
	values := make(map[string]struct{})
	for partIdx := range parts {
		part := parts[partIdx].DenseGroupCountDistinct
		if part == nil {
			return nil, nil, fmt.Errorf("collections: dense grouped count-distinct missing prepared part %d", partIdx)
		}
		column := selectColumn(part)
		for _, value := range column.Dictionary {
			values[value] = struct{}{}
		}
		for _, valid := range column.Valid {
			if !valid {
				values[""] = struct{}{}
				break
			}
		}
	}
	dictionary := make([]string, 0, len(values))
	for value := range values {
		dictionary = append(dictionary, value)
	}
	sort.Strings(dictionary)
	ranks := make(map[string]uint32, len(dictionary))
	for rank, value := range dictionary {
		if uint64(rank) > uint64(^uint32(0)) {
			return nil, nil, fmt.Errorf("collections: dense grouped count-distinct global dictionary cardinality=%d exceeds uint32", len(dictionary))
		}
		ranks[value] = uint32(rank)
	}
	return dictionary, ranks, nil
}

func columnTypedColumnDenseGroupCountDistinctGlobalRanks(parts []columnTypedColumnPhysicalQueryPart, selectColumn func(*columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn) (map[string]uint32, int, error) {
	ranks := make(map[string]uint32)
	addValue := func(value string) error {
		if _, ok := ranks[value]; ok {
			return nil
		}
		if uint64(len(ranks)) > uint64(^uint32(0)) {
			return fmt.Errorf("collections: dense grouped count-distinct global dictionary cardinality exceeds uint32")
		}
		ranks[value] = uint32(len(ranks))
		return nil
	}
	for partIdx := range parts {
		part := parts[partIdx].DenseGroupCountDistinct
		if part == nil {
			return nil, 0, fmt.Errorf("collections: dense grouped count-distinct missing prepared part %d", partIdx)
		}
		column := selectColumn(part)
		for _, value := range column.Dictionary {
			if err := addValue(value); err != nil {
				return nil, 0, err
			}
		}
		for _, valid := range column.Valid {
			if !valid {
				if err := addValue(""); err != nil {
					return nil, 0, err
				}
				break
			}
		}
	}
	return ranks, len(ranks), nil
}

func prepareColumnTypedColumnDenseGroupCountDistinctGlobalColumnCodes(column *columnTypedColumnDenseStringCodeColumn, globalDictionary []string, ranks map[string]uint32) error {
	localRanks := make([]uint32, len(column.Dictionary))
	for localCode, value := range column.Dictionary {
		rank, ok := ranks[value]
		if !ok {
			return fmt.Errorf("local dictionary value %q missing from global dictionary", value)
		}
		localRanks[localCode] = rank
	}
	emptyRank, emptyOK := ranks[""]
	globalCodes := make([]uint32, len(column.Codes))
	for row, localCode := range column.Codes {
		if !columnTypedColumnDenseCodeValid(column.Valid, row) {
			if !emptyOK {
				return fmt.Errorf("row=%d nullable missing value has no empty-string global rank", row)
			}
			globalCodes[row] = emptyRank
			continue
		}
		if uint64(localCode) >= uint64(len(localRanks)) {
			return fmt.Errorf("row=%d code=%d outside cardinality=%d", row, localCode, len(localRanks))
		}
		globalCodes[row] = localRanks[localCode]
	}
	column.GlobalCodes = globalCodes
	column.GlobalDictionary = globalDictionary
	return nil
}

func columnTypedColumnDenseGroupCountDistinctLocalRanks(column *columnTypedColumnDenseStringCodeColumn, ranks map[string]uint32) ([]uint32, uint32, bool, error) {
	localRanks := make([]uint32, len(column.Dictionary))
	for localCode, value := range column.Dictionary {
		rank, ok := ranks[value]
		if !ok {
			return nil, 0, false, fmt.Errorf("local dictionary value %q missing from global dictionary", value)
		}
		localRanks[localCode] = rank
	}
	emptyRank, emptyOK := ranks[""]
	return localRanks, emptyRank, emptyOK, nil
}

func columnTypedColumnDenseGroupCountDistinctRowRank(column *columnTypedColumnDenseStringCodeColumn, localRanks []uint32, emptyRank uint32, emptyOK bool, rowIdx int) (uint32, error) {
	if !columnTypedColumnDenseCodeValid(column.Valid, rowIdx) {
		if !emptyOK {
			return 0, fmt.Errorf("row=%d nullable missing value has no empty-string global rank", rowIdx)
		}
		return emptyRank, nil
	}
	if rowIdx < 0 || rowIdx >= len(column.Codes) {
		return 0, fmt.Errorf("row=%d outside codes length=%d", rowIdx, len(column.Codes))
	}
	localCode := column.Codes[rowIdx]
	if uint64(localCode) >= uint64(len(localRanks)) {
		return 0, fmt.Errorf("row=%d code=%d outside cardinality=%d", rowIdx, localCode, len(localRanks))
	}
	return localRanks[localCode], nil
}

func prepareColumnTypedColumnDenseInt64SpanGlobalCodes(parts []columnTypedColumnPhysicalQueryPart) error {
	dictionary, ranks, err := columnTypedColumnDenseInt64SpanGlobalDictionary(parts)
	if err != nil {
		return err
	}
	for partIdx := range parts {
		dense := parts[partIdx].DenseInt64Span
		if dense == nil {
			return fmt.Errorf("collections: dense typed-column int64-span missing prepared part %d", partIdx)
		}
		globalRanks := make([]uint32, dense.Cardinality)
		for localCode := 0; localCode < dense.Cardinality; localCode++ {
			value, ok := dense.DictionaryByCode[int64(localCode)]
			if !ok {
				return fmt.Errorf("collections: dense typed-column int64-span part %d dictionary missing local code %d", partIdx, localCode)
			}
			rank, ok := ranks[value]
			if !ok {
				return fmt.Errorf("collections: dense typed-column int64-span part %d local value %q missing from global dictionary", partIdx, value)
			}
			globalRanks[localCode] = rank
		}
		dense.GlobalRanks = globalRanks
		dense.GlobalDictionary = dictionary
	}
	return nil
}

func columnTypedColumnDenseInt64SpanGlobalDictionary(parts []columnTypedColumnPhysicalQueryPart) ([]string, map[string]uint32, error) {
	values := make(map[string]struct{})
	for partIdx := range parts {
		dense := parts[partIdx].DenseInt64Span
		if dense == nil {
			return nil, nil, fmt.Errorf("collections: dense typed-column int64-span missing prepared part %d", partIdx)
		}
		for localCode := 0; localCode < dense.Cardinality; localCode++ {
			value, ok := dense.DictionaryByCode[int64(localCode)]
			if !ok {
				return nil, nil, fmt.Errorf("collections: dense typed-column int64-span part %d dictionary missing local code %d", partIdx, localCode)
			}
			values[value] = struct{}{}
		}
	}
	dictionary := make([]string, 0, len(values))
	for value := range values {
		dictionary = append(dictionary, value)
	}
	sort.Strings(dictionary)
	ranks := make(map[string]uint32, len(dictionary))
	for rank, value := range dictionary {
		if uint64(rank) > uint64(^uint32(0)) {
			return nil, nil, fmt.Errorf("collections: dense typed-column int64-span global dictionary cardinality=%d exceeds uint32", len(dictionary))
		}
		ranks[value] = uint32(rank)
	}
	return dictionary, ranks, nil
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
	hasNullableStringValues := false
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
			if wantType != ColumnStoreValueString {
				return columnTypedColumnPhysicalQueryPlan{}, true, fmt.Errorf("%w: typed-column part physical query column %q does not support nullable %s values", ErrColumnQueryPlanUnsupported, column, wantType)
			}
			hasNullableStringValues = true
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
	if hasNullableStringValues {
		plan.SortKeyPrefix = columnTypedColumnSortKeyPrefixPlan{}
	}
	plan.DenseGroupCount = columnTypedColumnPhysicalQueryShapeCanUseDenseGroupCount(req)
	plan.DenseGroupCountDistinct = columnTypedColumnPhysicalQueryShapeCanUseDenseGroupCountDistinct(req)
	plan.DenseGroupHourCount = columnTypedColumnPhysicalQueryShapeCanUseDenseGroupHourCount(req)
	plan.DenseInt64Span = columnTypedColumnPhysicalQueryShapeCanUseDenseInt64Span(req)
	plan.TimeOrderTopK = columnTypedColumnPhysicalQueryShapeCanUseTimeOrderTopK(req) && columnTypedColumnPhysicalQuerySortKeyCanUseTimeOrderTopK(sortKey, req)
	plan.NullableStringValues = hasNullableStringValues
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

func decodeTypedColumnPhysicalQueryPart(plan columnTypedColumnPhysicalQueryPlan, schemaHash uint64, typedRef, physical columnManifestAssetRefForScan, raw []byte, includePhysicalRows bool) (columnTypedColumnPhysicalQueryPart, error) {
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
	var physicalRowIndexes []int
	if includePhysicalRows {
		decodedRows := len(selectedRows)
		if selectedRows == nil {
			decodedRows = summary.Rows
		}
		var primaryDiag columnTypedColumnPhysicalRowIndexDiagnostics
		physicalRowIndexes, primaryDiag, err = typedColumnPhysicalQueryPhysicalRows(adapterPart, selectedRows, decodedRows)
		if err != nil {
			return columnTypedColumnPhysicalQueryPart{}, err
		}
		if primaryDiag.GranulesDecoded > scanDiag.GranulesDecoded {
			scanDiag.GranulesDecoded = primaryDiag.GranulesDecoded
		}
		scanDiag.BlocksDecoded += primaryDiag.BlocksDecoded
		scanDiag.BytesDecoded += primaryDiag.BytesDecoded
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
		PhysicalRowIndexes:        physicalRowIndexes,
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

func (s *columnTypedColumnPhysicalAggregateSummary) run(r *columnTypedColumnPhysicalQueryRunner, view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	r.resultGroups = append(r.resultGroups[:0], s.groups...)
	diag := r.diagnostics(view, req, 0, s.matchedRows, s.reduceRows, time.Since(start).Nanoseconds())
	diag.DenseGroupCountUsed = s.denseGroupCount
	diag.DenseGroupHourCountUsed = s.denseGroupHourCount
	result := ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) run(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	if r.aggregateSummary != nil {
		return r.aggregateSummary.run(r, view, req)
	}
	if columnTypedColumnPhysicalQueryUseTimeOrderTopK(r.plan, req) {
		return r.runTimeOrderTopK(view, req)
	}
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
	if columnTypedColumnPhysicalQueryUseDenseGroupCountDistinct(r.plan, req) {
		return r.runDenseGroupCountDistinct(view, req)
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

func (r *columnTypedColumnPhysicalQueryRunner) runLatestVisible(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, state *typedColumnLatestVisibilityState, visibilityBytes int64, visibilityNanos int64) (ColumnPhysicalQueryResult, error) {
	if columnTypedColumnPhysicalQueryUseDenseGroupCount(r.plan, req) {
		return r.runLatestVisibleDenseGroupCount(view, req, state, visibilityBytes, visibilityNanos)
	}
	if columnTypedColumnPhysicalQueryUseDenseGroupHourCount(r.plan, req) {
		return r.runLatestVisibleDenseGroupHourCount(view, req, state, visibilityBytes, visibilityNanos)
	}
	if columnTypedColumnPhysicalQueryUseDenseInt64Span(r.plan, req) {
		return r.runLatestVisibleDenseInt64Span(view, req, state, visibilityBytes, visibilityNanos)
	}
	if columnTypedColumnPhysicalQueryUseTimeOrderTopK(r.plan, req) {
		return r.runLatestVisibleTimeOrderTopK(view, req, state, visibilityBytes, visibilityNanos)
	}
	if columnTypedColumnPhysicalQueryUseSortedGroupedDistinct(r.plan, req) {
		return r.runLatestVisibleSortedGroupedDistinct(view, req, state, visibilityBytes, visibilityNanos)
	}
	if !columnPhysicalQueryHasPredicates(req) || columnTypedColumnPhysicalQueryUseSortedMarkPrunedTopK(r.plan, req) {
		return r.runLatestVisibleGeneric(view, req, state, visibilityBytes, visibilityNanos)
	}
	result := ColumnPhysicalQueryResult{Diagnostics: r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, 0, 0, 0, 0)}
	annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedColumnPartSection, ColumnPhysicalQueryFallbackMutationVisibilityUnsupported)
	return result, fmt.Errorf("%w: typed-column part physical query with mutation visibility is deferred to multipart reducers", ErrColumnQueryPlanUnsupported)
}

func (r *columnTypedColumnPhysicalQueryRunner) latestVisiblePartForRunnerPart(part columnTypedColumnPhysicalQueryPart, state *typedColumnLatestVisibilityState) (*typedColumnLatestPhysicalPart, error) {
	if state == nil || state.resolver == nil {
		return nil, errors.New("collections: typed-column latest-visible physical state is missing")
	}
	visibility, ok := state.resolver.partForGeneration(part.PhysicalRef.Ref.Generation)
	if !ok {
		return nil, fmt.Errorf("collections: typed-column latest-visible physical state missing generation=%d", part.PhysicalRef.Ref.Generation)
	}
	if visibility.Rows != part.Rows {
		return nil, fmt.Errorf("collections: typed-column latest-visible rows=%d do not match typed-column part rows=%d for generation=%d", visibility.Rows, part.Rows, part.PhysicalRef.Ref.Generation)
	}
	return visibility, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) latestVisibleDiagnostics(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, state *typedColumnLatestVisibilityState, visibilityBytes int64, visibilityNanos int64, rowsScanned, matchedRows, reduceRows int, scanNanos int64) ColumnPhysicalQueryDiagnostics {
	diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, scanNanos)
	applyTypedColumnLatestVisibilityDiagnostics(&diag, state, visibilityBytes, visibilityNanos)
	return diag
}

func (r *columnTypedColumnPhysicalQueryRunner) runLatestVisibleGeneric(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, state *typedColumnLatestVisibilityState, visibilityBytes int64, visibilityNanos int64) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	acc := newColumnTypedColumnPhysicalQueryAccumulator(req.Kind)
	rowsScanned := 0
	matchedRows := 0
	for partIdx := range r.parts {
		part := r.parts[partIdx]
		visibility, err := r.latestVisiblePartForRunnerPart(part, state)
		if err != nil {
			return ColumnPhysicalQueryResult{Diagnostics: r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, acc.reduceRows, time.Since(start).Nanoseconds())}, err
		}
		partRows := len(part.RowIndexes)
		if part.RowIndexes == nil {
			partRows = part.Rows
		}
		for rowIdx := 0; rowIdx < partRows; rowIdx++ {
			physicalRow := rowIdx
			if part.PhysicalRowIndexes != nil {
				physicalRow = part.PhysicalRowIndexes[rowIdx]
			} else if part.RowIndexes != nil {
				physicalRow = part.RowIndexes[rowIdx]
			}
			rowsScanned++
			if !visibility.rowVisible(physicalRow) {
				continue
			}
			matched, err := typedColumnPhysicalQueryPredicatesMatch(part.Values, r.plan.PredicateSpecs, rowIdx)
			if err != nil {
				return ColumnPhysicalQueryResult{Diagnostics: r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, acc.reduceRows, time.Since(start).Nanoseconds())}, err
			}
			if !matched {
				continue
			}
			if len(r.plan.PredicateSpecs) != 0 {
				matchedRows++
			}
			if err := acc.visit(req, part.Values, rowIdx); err != nil {
				return ColumnPhysicalQueryResult{Diagnostics: r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, acc.reduceRows, time.Since(start).Nanoseconds())}, err
			}
		}
	}
	groups := acc.groups(req, r.resultGroups)
	r.resultGroups = groups
	diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, acc.reduceRows, time.Since(start).Nanoseconds())
	result := ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) runLatestVisibleDenseGroupCount(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, state *typedColumnLatestVisibilityState, visibilityBytes int64, visibilityNanos int64) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	if r.denseGroupCounts == nil {
		r.denseGroupCounts = make(map[string]int, 16)
	} else {
		clear(r.denseGroupCounts)
	}
	rowsScanned := 0
	reduceRows := 0
	for partIdx := range r.parts {
		part := r.parts[partIdx]
		visibility, err := r.latestVisiblePartForRunnerPart(part, state)
		if err != nil {
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, 0, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		dense := part.DenseGroupCount
		if dense == nil {
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, 0, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-count missing prepared part %d", partIdx)
		}
		if err := validateColumnTypedColumnDenseGroupCountPart(dense, partIdx); err != nil {
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, 0, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		if cap(r.denseLocalCounts) < dense.Cardinality {
			r.denseLocalCounts = make([]int, dense.Cardinality)
		} else {
			r.denseLocalCounts = r.denseLocalCounts[:dense.Cardinality]
			clear(r.denseLocalCounts)
		}
		missingCount := 0
		for rowIdx, code := range dense.Codes {
			rowsScanned++
			if !visibility.rowVisible(rowIdx) {
				continue
			}
			if !columnTypedColumnDenseCodeValid(dense.Valid, rowIdx) {
				missingCount++
				reduceRows++
				continue
			}
			localIdx, ok := columnDictionaryCodeIndex(code, len(r.denseLocalCounts))
			if !ok {
				diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, 0, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseGroupCountUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-count part %d code[%d]=%d outside cardinality=%d", partIdx, rowIdx, code, len(r.denseLocalCounts))
			}
			r.denseLocalCounts[localIdx]++
			reduceRows++
		}
		if missingCount != 0 {
			r.denseGroupCounts[""] += missingCount
		}
		for localCode, count := range r.denseLocalCounts {
			if count == 0 {
				continue
			}
			key, ok := columnTypedColumnDenseGroupCountDictionaryValue(dense, localCode)
			if !ok {
				diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, 0, reduceRows, time.Since(start).Nanoseconds())
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
	diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, 0, reduceRows, time.Since(start).Nanoseconds())
	diag.DenseGroupCountUsed = true
	result := ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) runLatestVisibleDenseGroupHourCount(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, state *typedColumnLatestVisibilityState, visibilityBytes int64, visibilityNanos int64) (ColumnPhysicalQueryResult, error) {
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
		part := r.parts[partIdx]
		visibility, err := r.latestVisiblePartForRunnerPart(part, state)
		if err != nil {
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupHourCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		dense := part.DenseGroupHourCount
		if dense == nil {
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupHourCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-hour missing prepared part %d", partIdx)
		}
		if err := validateColumnTypedColumnDenseGroupHourCountPart(dense, partIdx); err != nil {
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupHourCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
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
		var missingHourCounts [24]int
		for rowIdx, code := range dense.GroupCodes {
			rowsScanned++
			if !visibility.rowVisible(rowIdx) {
				continue
			}
			if !columnTypedColumnDensePredicatesMatch(dense.Predicates, rowIdx) {
				continue
			}
			if len(dense.Predicates) != 0 {
				matchedRows++
			}
			reduceRows++
			hour := columnPhysicalQueryUTCHour(dense.Values[rowIdx])
			if !columnTypedColumnDenseCodeValid(dense.GroupValid, rowIdx) {
				missingHourCounts[hour]++
				continue
			}
			localIdx, ok := columnDictionaryCodeIndex(code, dense.Cardinality)
			if !ok {
				diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseGroupHourCountUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-hour part %d code[%d]=%d outside cardinality=%d", partIdx, rowIdx, code, dense.Cardinality)
			}
			r.denseLocalHourCounts[localIdx*24+hour]++
		}
		var missingByHour [24]int
		missingSeen := false
		for hour, count := range missingHourCounts {
			if count == 0 {
				continue
			}
			if !missingSeen {
				missingByHour = r.denseGroupHourCounts[""]
				missingSeen = true
			}
			missingByHour[hour] += count
		}
		if missingSeen {
			r.denseGroupHourCounts[""] = missingByHour
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
						diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
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
	diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
	diag.DenseGroupHourCountUsed = true
	result := ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) runLatestVisibleDenseInt64Span(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, state *typedColumnLatestVisibilityState, visibilityBytes int64, visibilityNanos int64) (ColumnPhysicalQueryResult, error) {
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
		part := r.parts[partIdx]
		visibility, err := r.latestVisiblePartForRunnerPart(part, state)
		if err != nil {
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseInt64SpanUsed = true
			diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		dense := part.DenseInt64Span
		if dense == nil {
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseInt64SpanUsed = true
			diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span missing prepared part %d", partIdx)
		}
		if len(dense.GroupCodes) != len(dense.Values) {
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseInt64SpanUsed = true
			diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span part %d group/value rows=%d/%d", partIdx, len(dense.GroupCodes), len(dense.Values))
		}
		if cap(r.denseLocalSpans) < dense.Cardinality || cap(r.denseLocalSpanSeen) < dense.Cardinality {
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
			if !visibility.rowVisible(rowIdx) {
				continue
			}
			if !columnTypedColumnDensePredicatesMatch(dense.Predicates, rowIdx) {
				continue
			}
			if len(dense.Predicates) != 0 {
				matchedRows++
			}
			reduceRows++
			if !columnTypedColumnDenseCodeValid(dense.GroupValid, rowIdx) {
				if req.SkipEmptyGroupKey {
					continue
				}
				diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseInt64SpanUsed = true
				diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("%w: dense typed-column int64-span nullable group column requires skip-empty group key", ErrColumnQueryPlanUnsupported)
			}
			localIdx, ok := columnDictionaryCodeIndex(code, len(r.denseLocalSpans))
			if !ok {
				diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseInt64SpanUsed = true
				diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
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
				diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseInt64SpanUsed = true
				diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
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
	diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
	diag.DenseInt64SpanUsed = true
	diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
	result := ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
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

func columnTypedColumnPhysicalQueryShapeCanUseDenseGroupCountDistinct(req ColumnPhysicalQueryRequest) bool {
	return req.Kind == ColumnPhysicalQueryGroupCountAndDistinct && req.GroupColumn != "" && req.DistinctColumn != "" && req.GroupColumn != req.DistinctColumn && req.AggregateMetadataName == "" && req.ValueColumn == "" && req.TopK == 0 && req.TopKOrder == ""
}

func columnTypedColumnPhysicalQueryUseDenseGroupCountDistinct(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest) bool {
	return plan.DenseGroupCountDistinct && columnTypedColumnPhysicalQueryShapeCanUseDenseGroupCountDistinct(req)
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
	return plan.DenseInt64Span && columnTypedColumnPhysicalQueryShapeCanUseDenseInt64Span(req) && columnTypedColumnPhysicalQueryNullableStringsAllowedForTopK(plan, req)
}

func columnTypedColumnPhysicalQueryShapeCanUseTimeOrderTopK(req ColumnPhysicalQueryRequest) bool {
	return req.Kind == ColumnPhysicalQueryGroupMinInt64 && req.GroupColumn != "" && req.ValueColumn != "" && req.AggregateMetadataName == "" && req.DistinctColumn == "" && req.TopK > 0 && req.TopKOrder == ColumnPhysicalQueryTopKInt64Asc
}

func columnTypedColumnPhysicalQuerySortKeyCanUseTimeOrderTopK(sortKey []ColumnSortKey, req ColumnPhysicalQueryRequest) bool {
	if len(sortKey) == 0 || req.ValueColumn == "" {
		return false
	}
	first := sortKey[0]
	return first.Column == req.ValueColumn && first.Direction == ColumnSortAscending
}

func columnTypedColumnPhysicalQueryUseTimeOrderTopK(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest) bool {
	return plan.TimeOrderTopK && columnTypedColumnPhysicalQueryShapeCanUseTimeOrderTopK(req) && columnTypedColumnPhysicalQueryNullableStringsAllowedForTopK(plan, req)
}

func columnTypedColumnPhysicalQueryNullableStringsAllowedForTopK(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest) bool {
	if !plan.NullableStringValues {
		return true
	}
	return req.SkipEmptyGroupKey
}

func (r *columnTypedColumnPhysicalQueryRunner) runTimeOrderTopK(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	if r.timeOrderMinValues == nil {
		r.timeOrderMinValues = make(map[string]int64, req.TopK+1)
	} else {
		clear(r.timeOrderMinValues)
	}
	r.timeOrderHeap.items = r.timeOrderHeap.items[:0]
	iterators := make([]columnTypedColumnTimeOrderTopKIterator, 0, len(r.parts))
	for partIdx := range r.parts {
		part := r.parts[partIdx].TimeOrderTopK
		if part == nil {
			diag := r.diagnostics(view, req, 0, 0, 0, time.Since(start).Nanoseconds())
			diag.TimeOrderTopKUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: time-order topK missing prepared part %d", partIdx)
		}
		iterator, err := newColumnTypedColumnTimeOrderTopKIterator(partIdx, part, nil)
		if err != nil {
			rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes := columnTypedColumnTimeOrderTopKIteratorTotals(iterators)
			diag := r.timeOrderTopKDiagnostics(view, req, rowsScanned, matchedRows, matchedRows, decodedGranules, decodedBlocks, decodedBytes, time.Since(start).Nanoseconds())
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		iterators = append(iterators, iterator)
		if !iterator.done {
			r.timeOrderHeap.push(len(iterators)-1, iterators)
		}
	}

	haveKth := false
	kthTime := int64(0)
	for r.timeOrderHeap.len() != 0 {
		iteratorIdx := r.timeOrderHeap.peek()
		if iteratorIdx < 0 {
			break
		}
		if haveKth && iterators[iteratorIdx].currentTime > kthTime {
			break
		}
		iteratorIdx = r.timeOrderHeap.pop(iterators)
		iterator := &iterators[iteratorIdx]
		group, matched, err := iterator.evaluateCurrent(len(r.plan.PredicateSpecs) != 0)
		if err != nil {
			rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes := columnTypedColumnTimeOrderTopKIteratorTotals(iterators)
			reduceRows := matchedRows
			if len(r.plan.PredicateSpecs) == 0 {
				reduceRows = rowsScanned
			}
			diag := r.timeOrderTopKDiagnostics(view, req, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks, decodedBytes, time.Since(start).Nanoseconds())
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		if matched {
			if !(req.SkipEmptyGroupKey && group == "") {
				if _, exists := r.timeOrderMinValues[group]; !exists {
					r.timeOrderMinValues[group] = iterator.currentTime
					var ok bool
					kthTime, ok, r.timeOrderTopKScratch = columnTypedColumnTimeOrderTopKKthTime(r.timeOrderMinValues, req, r.timeOrderTopKScratch)
					haveKth = ok
				}
			}
		}
		if err := iterator.advance(); err != nil {
			rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes := columnTypedColumnTimeOrderTopKIteratorTotals(iterators)
			reduceRows := matchedRows
			if len(r.plan.PredicateSpecs) == 0 {
				reduceRows = rowsScanned
			}
			diag := r.timeOrderTopKDiagnostics(view, req, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks, decodedBytes, time.Since(start).Nanoseconds())
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		if !iterator.done {
			r.timeOrderHeap.push(iteratorIdx, iterators)
		}
	}

	groups := r.resultGroups[:0]
	for key, value := range r.timeOrderMinValues {
		groups = append(groups, ColumnPhysicalQueryGroup{Key: key, Int64: value})
	}
	r.resultGroups = groups
	rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes := columnTypedColumnTimeOrderTopKIteratorTotals(iterators)
	reduceRows := matchedRows
	if len(r.plan.PredicateSpecs) == 0 {
		reduceRows = rowsScanned
	}
	diag := r.timeOrderTopKDiagnostics(view, req, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks, decodedBytes, time.Since(start).Nanoseconds())
	result := ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	return result, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) runLatestVisibleTimeOrderTopK(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, state *typedColumnLatestVisibilityState, visibilityBytes int64, visibilityNanos int64) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	if r.timeOrderMinValues == nil {
		r.timeOrderMinValues = make(map[string]int64, req.TopK+1)
	} else {
		clear(r.timeOrderMinValues)
	}
	r.timeOrderHeap.items = r.timeOrderHeap.items[:0]
	iterators := make([]columnTypedColumnTimeOrderTopKIterator, 0, len(r.parts))
	reduceRows := 0
	for partIdx := range r.parts {
		runnerPart := r.parts[partIdx]
		part := runnerPart.TimeOrderTopK
		if part == nil {
			diag := r.latestVisibleTimeOrderTopKDiagnostics(view, req, state, visibilityBytes, visibilityNanos, 0, 0, reduceRows, 0, 0, 0, time.Since(start).Nanoseconds())
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: time-order topK missing prepared part %d", partIdx)
		}
		visibility, err := r.latestVisiblePartForRunnerPart(runnerPart, state)
		if err != nil {
			rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes := columnTypedColumnTimeOrderTopKIteratorTotals(iterators)
			diag := r.latestVisibleTimeOrderTopKDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks, decodedBytes, time.Since(start).Nanoseconds())
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		iterator, err := newColumnTypedColumnTimeOrderTopKIterator(partIdx, part, visibility)
		if err != nil {
			rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes := columnTypedColumnTimeOrderTopKIteratorTotals(iterators)
			diag := r.latestVisibleTimeOrderTopKDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks, decodedBytes, time.Since(start).Nanoseconds())
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		iterators = append(iterators, iterator)
		if !iterator.done {
			r.timeOrderHeap.push(len(iterators)-1, iterators)
		}
	}

	// Mutation-bearing sorted parts cannot use the insert-only time-threshold
	// early stop: a newer visible version may live in any later/delta part. Keep
	// the logical time-order merge, but scan every non-tombstone candidate and
	// apply latest-visible physical row identity before predicate/reduce emission.
	for r.timeOrderHeap.len() != 0 {
		iteratorIdx := r.timeOrderHeap.pop(iterators)
		iterator := &iterators[iteratorIdx]
		visible, err := iterator.currentVisible()
		if err != nil {
			rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes := columnTypedColumnTimeOrderTopKIteratorTotals(iterators)
			diag := r.latestVisibleTimeOrderTopKDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks, decodedBytes, time.Since(start).Nanoseconds())
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		if !visible {
			if err := iterator.skipCurrent(); err != nil {
				rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes := columnTypedColumnTimeOrderTopKIteratorTotals(iterators)
				diag := r.latestVisibleTimeOrderTopKDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks, decodedBytes, time.Since(start).Nanoseconds())
				return ColumnPhysicalQueryResult{Diagnostics: diag}, err
			}
		} else {
			group, matched, err := iterator.evaluateCurrent(len(r.plan.PredicateSpecs) != 0)
			if err != nil {
				rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes := columnTypedColumnTimeOrderTopKIteratorTotals(iterators)
				diag := r.latestVisibleTimeOrderTopKDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks, decodedBytes, time.Since(start).Nanoseconds())
				return ColumnPhysicalQueryResult{Diagnostics: diag}, err
			}
			if matched {
				reduceRows++
				if !(req.SkipEmptyGroupKey && group == "") {
					if _, exists := r.timeOrderMinValues[group]; !exists {
						r.timeOrderMinValues[group] = iterator.currentTime
					}
				}
			}
		}
		if err := iterator.advance(); err != nil {
			rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes := columnTypedColumnTimeOrderTopKIteratorTotals(iterators)
			diag := r.latestVisibleTimeOrderTopKDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks, decodedBytes, time.Since(start).Nanoseconds())
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		if !iterator.done {
			r.timeOrderHeap.push(iteratorIdx, iterators)
		}
	}

	groups := r.resultGroups[:0]
	for key, value := range r.timeOrderMinValues {
		groups = append(groups, ColumnPhysicalQueryGroup{Key: key, Int64: value})
	}
	r.resultGroups = groups
	rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes := columnTypedColumnTimeOrderTopKIteratorTotals(iterators)
	diag := r.latestVisibleTimeOrderTopKDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks, decodedBytes, time.Since(start).Nanoseconds())
	result := ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) timeOrderTopKDiagnostics(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks int, decodedBytes uint64, scanNanos int64) ColumnPhysicalQueryDiagnostics {
	diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, scanNanos)
	diag.TimeOrderTopKUsed = true
	decodedGranules += r.granulesDecoded
	decodedBlocks += r.decodedBlocks
	decodedBytes += r.decodedPayloadBytes
	diag.DecodedGranules = decodedGranules
	diag.DecodedBlocks = decodedBlocks
	diag.DirectReduceBlocks = decodedBlocks
	diag.DecodedPayloadBytes = decodedBytes
	if diag.ScheduledGranules >= decodedGranules {
		diag.SkippedGranules = diag.ScheduledGranules - decodedGranules
	} else {
		diag.SkippedGranules = 0
	}
	return diag
}

func (r *columnTypedColumnPhysicalQueryRunner) latestVisibleTimeOrderTopKDiagnostics(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, state *typedColumnLatestVisibilityState, visibilityBytes int64, visibilityNanos int64, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks int, decodedBytes uint64, scanNanos int64) ColumnPhysicalQueryDiagnostics {
	diag := r.timeOrderTopKDiagnostics(view, req, rowsScanned, matchedRows, reduceRows, decodedGranules, decodedBlocks, decodedBytes, scanNanos)
	applyTypedColumnLatestVisibilityDiagnostics(&diag, state, visibilityBytes, visibilityNanos)
	return diag
}

type columnTypedColumnTimeOrderTopKIterator struct {
	partIndex           int
	part                *columnTypedColumnTimeOrderTopKPart
	visibility          *typedColumnLatestPhysicalPart
	granule             int
	rowInGranule        int
	currentTime         int64
	done                bool
	rowsScanned         int
	matchedRows         int
	decodedGranules     int
	decodedBlocks       int
	decodedPayloadBytes uint64
}

func newColumnTypedColumnTimeOrderTopKIterator(partIndex int, part *columnTypedColumnTimeOrderTopKPart, visibility *typedColumnLatestPhysicalPart) (columnTypedColumnTimeOrderTopKIterator, error) {
	part.resetTimeOrderTopKScan()
	currentTime, ok, err := part.firstTimeOrderTopKTime()
	if err != nil {
		return columnTypedColumnTimeOrderTopKIterator{}, err
	}
	return columnTypedColumnTimeOrderTopKIterator{partIndex: partIndex, part: part, visibility: visibility, currentTime: currentTime, done: !ok}, nil
}

func (it *columnTypedColumnTimeOrderTopKIterator) currentPhysicalRow() (int, error) {
	if it == nil || it.part == nil {
		return 0, errors.New("collections: nil time-order topK iterator")
	}
	if it.granule < 0 || it.granule >= len(it.part.Granules) {
		return 0, fmt.Errorf("collections: time-order topK granule %d outside %d", it.granule, len(it.part.Granules))
	}
	granule := it.part.Granules[it.granule]
	if it.rowInGranule < 0 || it.rowInGranule >= granule.RowCount {
		return 0, fmt.Errorf("collections: time-order topK row_in_granule=%d outside granule rows=%d", it.rowInGranule, granule.RowCount)
	}
	sortedRow := granule.FirstRow + it.rowInGranule
	if sortedRow < 0 || sortedRow >= it.part.Rows {
		return 0, fmt.Errorf("collections: time-order topK sorted row=%d outside rows=%d", sortedRow, it.part.Rows)
	}
	if it.part.PhysicalRows != nil {
		if sortedRow >= len(it.part.PhysicalRows) {
			return 0, fmt.Errorf("collections: time-order topK physical row map sorted row=%d outside rows=%d", sortedRow, len(it.part.PhysicalRows))
		}
		return it.part.PhysicalRows[sortedRow], nil
	}
	return sortedRow, nil
}

func (it *columnTypedColumnTimeOrderTopKIterator) currentVisible() (bool, error) {
	if it.visibility == nil {
		return true, nil
	}
	physicalRow, err := it.currentPhysicalRow()
	if err != nil {
		return false, err
	}
	return it.visibility.rowVisible(physicalRow), nil
}

func (it *columnTypedColumnTimeOrderTopKIterator) skipCurrent() error {
	decoded, blocks, decodedBytes, err := it.part.ensureTimeOrderTopKGranuleDecoded(it.granule)
	if err != nil {
		return err
	}
	if it.rowInGranule < 0 || it.rowInGranule >= len(it.part.timeValues) {
		return fmt.Errorf("collections: time-order topK row_in_granule=%d outside decoded time rows=%d", it.rowInGranule, len(it.part.timeValues))
	}
	it.rowsScanned++
	if decoded {
		it.decodedGranules++
		it.decodedBlocks += blocks
		it.decodedPayloadBytes += decodedBytes
	}
	return nil
}

func (it *columnTypedColumnTimeOrderTopKIterator) evaluateCurrent(countMatchedRows bool) (string, bool, error) {
	group, matched, decoded, blocks, decodedBytes, err := it.part.evaluateTimeOrderTopKRow(it.granule, it.rowInGranule)
	if err != nil {
		return "", false, err
	}
	it.rowsScanned++
	if matched && countMatchedRows {
		it.matchedRows++
	}
	if decoded {
		it.decodedGranules++
		it.decodedBlocks += blocks
		it.decodedPayloadBytes += decodedBytes
	}
	return group, matched, nil
}

func (it *columnTypedColumnTimeOrderTopKIterator) advance() error {
	granule, rowInGranule, currentTime, done, err := it.part.nextTimeOrderTopKPosition(it.granule, it.rowInGranule)
	if err != nil {
		return err
	}
	it.granule = granule
	it.rowInGranule = rowInGranule
	it.currentTime = currentTime
	it.done = done
	return nil
}

type columnTypedColumnTimeOrderTopKHeap struct {
	items []int
}

func (h *columnTypedColumnTimeOrderTopKHeap) len() int { return len(h.items) }

func (h *columnTypedColumnTimeOrderTopKHeap) peek() int {
	if len(h.items) == 0 {
		return -1
	}
	return h.items[0]
}

func (h *columnTypedColumnTimeOrderTopKHeap) push(iteratorIdx int, iterators []columnTypedColumnTimeOrderTopKIterator) {
	h.items = append(h.items, iteratorIdx)
	for idx := len(h.items) - 1; idx > 0; {
		parent := (idx - 1) / 2
		if !columnTypedColumnTimeOrderTopKIteratorLess(iterators[h.items[idx]], iterators[h.items[parent]]) {
			break
		}
		h.items[idx], h.items[parent] = h.items[parent], h.items[idx]
		idx = parent
	}
}

func (h *columnTypedColumnTimeOrderTopKHeap) pop(iterators []columnTypedColumnTimeOrderTopKIterator) int {
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
		if right < len(h.items) && columnTypedColumnTimeOrderTopKIteratorLess(iterators[h.items[right]], iterators[h.items[left]]) {
			smallest = right
		}
		if !columnTypedColumnTimeOrderTopKIteratorLess(iterators[h.items[smallest]], iterators[h.items[idx]]) {
			break
		}
		h.items[idx], h.items[smallest] = h.items[smallest], h.items[idx]
		idx = smallest
	}
	return out
}

func columnTypedColumnTimeOrderTopKIteratorLess(left, right columnTypedColumnTimeOrderTopKIterator) bool {
	if left.currentTime != right.currentTime {
		return left.currentTime < right.currentTime
	}
	return left.partIndex < right.partIndex
}

func columnTypedColumnTimeOrderTopKIteratorTotals(iterators []columnTypedColumnTimeOrderTopKIterator) (int, int, int, int, uint64) {
	rowsScanned := 0
	matchedRows := 0
	decodedGranules := 0
	decodedBlocks := 0
	decodedBytes := uint64(0)
	for _, iterator := range iterators {
		rowsScanned += iterator.rowsScanned
		matchedRows += iterator.matchedRows
		decodedGranules += iterator.decodedGranules
		decodedBlocks += iterator.decodedBlocks
		decodedBytes += iterator.decodedPayloadBytes
	}
	return rowsScanned, matchedRows, decodedGranules, decodedBlocks, decodedBytes
}

func columnTypedColumnTimeOrderTopKKthTime(mins map[string]int64, req ColumnPhysicalQueryRequest, scratch []ColumnPhysicalQueryGroup) (int64, bool, []ColumnPhysicalQueryGroup) {
	scratch = scratch[:0]
	for key, value := range mins {
		insertColumnPhysicalTopKGroup(&scratch, ColumnPhysicalQueryGroup{Key: key, Int64: value}, req.TopK, req.TopKOrder)
	}
	if len(scratch) < req.TopK {
		return 0, false, scratch
	}
	return scratch[len(scratch)-1].Int64, true, scratch
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
		if err := validateColumnTypedColumnDenseGroupCountPart(dense, partIdx); err != nil {
			diag := r.diagnostics(view, req, rowsScanned, 0, rowsScanned, time.Since(start).Nanoseconds())
			diag.DenseGroupCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		if cap(r.denseLocalCounts) < dense.Cardinality {
			r.denseLocalCounts = make([]int, dense.Cardinality)
		} else {
			r.denseLocalCounts = r.denseLocalCounts[:dense.Cardinality]
			clear(r.denseLocalCounts)
		}
		missingCount := 0
		for rowIdx, code := range dense.Codes {
			if !columnTypedColumnDenseCodeValid(dense.Valid, rowIdx) {
				missingCount++
				continue
			}
			localIdx, ok := columnDictionaryCodeIndex(code, len(r.denseLocalCounts))
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, 0, rowsScanned, time.Since(start).Nanoseconds())
				diag.DenseGroupCountUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-count part %d code[%d]=%d outside cardinality=%d", partIdx, rowIdx, code, len(r.denseLocalCounts))
			}
			r.denseLocalCounts[localIdx]++
		}
		rowsScanned += len(dense.Codes)
		if missingCount != 0 {
			r.denseGroupCounts[""] += missingCount
		}
		for localCode, count := range r.denseLocalCounts {
			if count == 0 {
				continue
			}
			key, ok := columnTypedColumnDenseGroupCountDictionaryValue(dense, localCode)
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

func validateColumnTypedColumnDenseGroupCountPart(dense *columnTypedColumnDenseGroupCountPart, partIdx int) error {
	if dense.Cardinality != len(dense.Dictionary) {
		return fmt.Errorf("collections: dense typed-column group-count part %d dictionary cardinality=%d want %d", partIdx, len(dense.Dictionary), dense.Cardinality)
	}
	if dense.Valid != nil && len(dense.Valid) != len(dense.Codes) {
		return fmt.Errorf("collections: dense typed-column group-count part %d valid rows=%d want codes rows=%d", partIdx, len(dense.Valid), len(dense.Codes))
	}
	if dense.Cardinality == 0 && dense.Valid == nil && len(dense.Codes) != 0 {
		return fmt.Errorf("collections: dense typed-column group-count part %d has empty dictionary", partIdx)
	}
	return nil
}

func validateColumnTypedColumnDenseGroupHourCountPart(dense *columnTypedColumnDenseGroupHourCountPart, partIdx int) error {
	if dense.GroupValid != nil && len(dense.GroupValid) != len(dense.GroupCodes) {
		return fmt.Errorf("collections: dense typed-column group-hour part %d valid rows=%d want codes rows=%d", partIdx, len(dense.GroupValid), len(dense.GroupCodes))
	}
	if dense.Cardinality == 0 && dense.GroupValid == nil && len(dense.GroupCodes) != 0 {
		return fmt.Errorf("collections: dense typed-column group-hour part %d has empty dictionary", partIdx)
	}
	if len(dense.GroupCodes) != len(dense.Values) {
		return fmt.Errorf("collections: dense typed-column group-hour part %d group/value rows=%d/%d", partIdx, len(dense.GroupCodes), len(dense.Values))
	}
	return nil
}

func columnTypedColumnDenseGroupCountDictionaryValue(dense *columnTypedColumnDenseGroupCountPart, localCode int) (string, bool) {
	if localCode < 0 || localCode >= len(dense.Dictionary) {
		return "", false
	}
	return dense.Dictionary[localCode], true
}

func columnTypedColumnDenseGroupCountDistinctBitsetLayout(groupCardinality, distinctCardinality int) (int, int, bool) {
	if groupCardinality < 0 || distinctCardinality < 0 {
		return 0, 0, false
	}
	if groupCardinality == 0 || distinctCardinality == 0 {
		return 0, 0, true
	}
	wordsPerGroup := (distinctCardinality + 63) / 64
	if wordsPerGroup <= 0 {
		return 0, 0, false
	}
	totalWords, ok := columnTypedColumnDenseGroupCountDistinctBitsetWords(groupCardinality, wordsPerGroup)
	if !ok {
		return wordsPerGroup, 0, false
	}
	return wordsPerGroup, totalWords, true
}

func columnTypedColumnDenseGroupCountDistinctBitsetWords(groups, wordsPerGroup int) (int, bool) {
	if groups < 0 || wordsPerGroup < 0 {
		return 0, false
	}
	if groups == 0 || wordsPerGroup == 0 {
		return 0, true
	}
	maxInt := int(^uint(0) >> 1)
	if groups > maxInt/wordsPerGroup {
		return 0, false
	}
	totalWords := groups * wordsPerGroup
	if totalWords > columnTypedColumnDenseGroupCountDistinctMaxBitsetWords {
		return 0, false
	}
	return totalWords, true
}

func (r *columnTypedColumnPhysicalQueryRunner) runDenseGroupCountDistinct(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	if columnTypedColumnDenseGroupCountDistinctPartsHaveGlobalCodes(r.parts) {
		return r.runDenseGroupCountDistinctGlobalCodes(view, req)
	}
	return r.runDenseGroupCountDistinctLocalCodes(view, req)
}

func columnTypedColumnDenseGroupCountDistinctPartsHaveGlobalCodes(parts []columnTypedColumnPhysicalQueryPart) bool {
	for partIdx := range parts {
		dense := parts[partIdx].DenseGroupCountDistinct
		if dense == nil {
			return false
		}
		if dense.Group.GlobalDictionary == nil || dense.Distinct.GlobalDictionary == nil {
			return false
		}
		if len(dense.Group.GlobalCodes) != dense.Rows || len(dense.Distinct.GlobalCodes) != dense.Rows {
			return false
		}
	}
	return true
}

func (r *columnTypedColumnPhysicalQueryRunner) runDenseGroupCountDistinctGlobalCodes(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	groupCardinality := 0
	distinctCardinality := 0
	haveCardinality := false
	for partIdx := range r.parts {
		dense := r.parts[partIdx].DenseGroupCountDistinct
		if dense == nil {
			diag := r.diagnostics(view, req, 0, 0, 0, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct missing prepared part %d", partIdx)
		}
		if !haveCardinality {
			groupCardinality = len(dense.Group.GlobalDictionary)
			distinctCardinality = len(dense.Distinct.GlobalDictionary)
			haveCardinality = true
		} else if len(dense.Group.GlobalDictionary) != groupCardinality {
			diag := r.diagnostics(view, req, 0, 0, 0, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d group global cardinality=%d want %d", partIdx, len(dense.Group.GlobalDictionary), groupCardinality)
		} else if len(dense.Distinct.GlobalDictionary) != distinctCardinality {
			diag := r.diagnostics(view, req, 0, 0, 0, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d distinct global cardinality=%d want %d", partIdx, len(dense.Distinct.GlobalDictionary), distinctCardinality)
		}
	}
	if cap(r.denseGroupCountDistinctCounts) < groupCardinality {
		r.denseGroupCountDistinctCounts = make([]int, groupCardinality)
	} else {
		r.denseGroupCountDistinctCounts = r.denseGroupCountDistinctCounts[:groupCardinality]
		clear(r.denseGroupCountDistinctCounts)
	}
	if cap(r.denseGroupCountDistinctDistinctCounts) < groupCardinality {
		r.denseGroupCountDistinctDistinctCounts = make([]int, groupCardinality)
	} else {
		r.denseGroupCountDistinctDistinctCounts = r.denseGroupCountDistinctDistinctCounts[:groupCardinality]
		clear(r.denseGroupCountDistinctDistinctCounts)
	}
	wordsPerGroup, pairBitWords, usePairBitset := columnTypedColumnDenseGroupCountDistinctBitsetLayout(groupCardinality, distinctCardinality)
	reducer := columnTypedColumnDenseGroupCountDistinctReducerPairBitset
	useActivePairBitset := false
	useSortedPairs := false
	if usePairBitset {
		if cap(r.denseGroupCountDistinctPairBits) < pairBitWords {
			r.denseGroupCountDistinctPairBits = make([]uint64, pairBitWords)
		} else {
			r.denseGroupCountDistinctPairBits = r.denseGroupCountDistinctPairBits[:pairBitWords]
			clear(r.denseGroupCountDistinctPairBits)
		}
	} else if wordsPerGroup > 0 && wordsPerGroup <= columnTypedColumnDenseGroupCountDistinctMaxBitsetWords {
		reducer = columnTypedColumnDenseGroupCountDistinctReducerActiveBitset
		useActivePairBitset = true
		pairBitWords = 0
		r.denseGroupCountDistinctPairBits = r.denseGroupCountDistinctPairBits[:0]
		if cap(r.denseGroupCountDistinctGroupActive) < groupCardinality {
			r.denseGroupCountDistinctGroupActive = make([]bool, groupCardinality)
		} else {
			r.denseGroupCountDistinctGroupActive = r.denseGroupCountDistinctGroupActive[:groupCardinality]
			clear(r.denseGroupCountDistinctGroupActive)
		}
		if cap(r.denseGroupCountDistinctGroupOffsets) < groupCardinality {
			r.denseGroupCountDistinctGroupOffsets = make([]int, groupCardinality)
		} else {
			r.denseGroupCountDistinctGroupOffsets = r.denseGroupCountDistinctGroupOffsets[:groupCardinality]
		}
		r.denseGroupCountDistinctActiveGroups = r.denseGroupCountDistinctActiveGroups[:0]
	} else {
		reducer = columnTypedColumnDenseGroupCountDistinctReducerSortedPairs
		useSortedPairs = true
		pairBitWords = 0
		r.denseGroupCountDistinctPairList = r.denseGroupCountDistinctPairList[:0]
	}

	rowsScanned := 0
	matchedRows := 0
	reduceRows := 0
	var groupDictionary []string
	for partIdx := range r.parts {
		dense := r.parts[partIdx].DenseGroupCountDistinct
		if dense == nil {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct missing prepared part %d", partIdx)
		}
		if groupDictionary == nil {
			groupDictionary = dense.Group.GlobalDictionary
		}
		if dense.Rows != len(dense.Group.GlobalCodes) || dense.Rows != len(dense.Distinct.GlobalCodes) || dense.Rows != len(dense.Group.Codes) || dense.Rows != len(dense.Distinct.Codes) {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d rows=%d group_codes=%d distinct_codes=%d group_local=%d distinct_local=%d", partIdx, dense.Rows, len(dense.Group.GlobalCodes), len(dense.Distinct.GlobalCodes), len(dense.Group.Codes), len(dense.Distinct.Codes))
		}
		if len(dense.Group.GlobalDictionary) != groupCardinality {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d group global cardinality=%d want %d", partIdx, len(dense.Group.GlobalDictionary), groupCardinality)
		}
		if len(dense.Distinct.GlobalDictionary) != distinctCardinality {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d distinct global cardinality=%d want %d", partIdx, len(dense.Distinct.GlobalDictionary), distinctCardinality)
		}
		if columnTypedColumnDensePredicatesRejectAll(dense.Predicates) {
			rowsScanned += dense.Rows
			continue
		}
		for rowIdx := 0; rowIdx < dense.Rows; rowIdx++ {
			rowsScanned++
			if !columnTypedColumnDensePredicatesMatch(dense.Predicates, rowIdx) {
				continue
			}
			if len(dense.Predicates) != 0 {
				matchedRows++
			}
			reduceRows++
			groupCode := dense.Group.GlobalCodes[rowIdx]
			distinctCode := dense.Distinct.GlobalCodes[rowIdx]
			groupIdx, ok := columnDictionaryCodeIndex(groupCode, len(r.denseGroupCountDistinctCounts))
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseGroupCountDistinctUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d group code[%d]=%d outside cardinality=%d", partIdx, rowIdx, groupCode, len(r.denseGroupCountDistinctCounts))
			}
			r.denseGroupCountDistinctCounts[groupIdx]++
			distinctIdx, ok := columnDictionaryCodeIndex(distinctCode, distinctCardinality)
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseGroupCountDistinctUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d distinct code[%d]=%d outside cardinality=%d", partIdx, rowIdx, distinctCode, distinctCardinality)
			}
			if usePairBitset {
				wordIdx := groupIdx*wordsPerGroup + distinctIdx/64
				mask := uint64(1) << uint(distinctIdx&63)
				if r.denseGroupCountDistinctPairBits[wordIdx]&mask == 0 {
					r.denseGroupCountDistinctPairBits[wordIdx] |= mask
					r.denseGroupCountDistinctDistinctCounts[groupIdx]++
				}
			} else if useActivePairBitset {
				if !r.denseGroupCountDistinctGroupActive[groupIdx] {
					nextPairBitWords, ok := columnTypedColumnDenseGroupCountDistinctBitsetWords(len(r.denseGroupCountDistinctActiveGroups)+1, wordsPerGroup)
					if !ok {
						r.convertDenseGroupCountDistinctActiveBitsToPairs(wordsPerGroup, distinctCardinality)
						clear(r.denseGroupCountDistinctDistinctCounts)
						reducer = columnTypedColumnDenseGroupCountDistinctReducerSortedPairs
						useActivePairBitset = false
						useSortedPairs = true
						pairBitWords = 0
					} else {
						offset := len(r.denseGroupCountDistinctPairBits)
						if cap(r.denseGroupCountDistinctPairBits) < nextPairBitWords {
							nextBits := make([]uint64, nextPairBitWords)
							copy(nextBits, r.denseGroupCountDistinctPairBits)
							r.denseGroupCountDistinctPairBits = nextBits
						} else {
							r.denseGroupCountDistinctPairBits = r.denseGroupCountDistinctPairBits[:nextPairBitWords]
							clear(r.denseGroupCountDistinctPairBits[offset:nextPairBitWords])
						}
						r.denseGroupCountDistinctGroupActive[groupIdx] = true
						r.denseGroupCountDistinctGroupOffsets[groupIdx] = offset
						r.denseGroupCountDistinctActiveGroups = append(r.denseGroupCountDistinctActiveGroups, groupIdx)
						pairBitWords = nextPairBitWords
					}
				}
				if useActivePairBitset {
					wordIdx := r.denseGroupCountDistinctGroupOffsets[groupIdx] + distinctIdx/64
					mask := uint64(1) << uint(distinctIdx&63)
					if r.denseGroupCountDistinctPairBits[wordIdx]&mask == 0 {
						r.denseGroupCountDistinctPairBits[wordIdx] |= mask
						r.denseGroupCountDistinctDistinctCounts[groupIdx]++
					}
				} else {
					r.denseGroupCountDistinctPairList = append(r.denseGroupCountDistinctPairList, uint64(groupIdx)<<32|uint64(distinctIdx))
				}
			} else {
				r.denseGroupCountDistinctPairList = append(r.denseGroupCountDistinctPairList, uint64(groupIdx)<<32|uint64(distinctIdx))
			}
		}
	}
	if useSortedPairs {
		slices.Sort(r.denseGroupCountDistinctPairList)
		var previous uint64
		havePrevious := false
		for _, pair := range r.denseGroupCountDistinctPairList {
			if havePrevious && pair == previous {
				continue
			}
			groupIdx := int(pair >> 32)
			r.denseGroupCountDistinctDistinctCounts[groupIdx]++
			previous = pair
			havePrevious = true
		}
	}
	r.resultGroups = r.resultGroups[:0]
	for groupIdx, count := range r.denseGroupCountDistinctCounts {
		distinct := r.denseGroupCountDistinctDistinctCounts[groupIdx]
		if count == 0 && distinct == 0 {
			continue
		}
		if groupIdx >= len(groupDictionary) {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct group index=%d outside dictionary=%d", groupIdx, len(groupDictionary))
		}
		r.resultGroups = append(r.resultGroups, ColumnPhysicalQueryGroup{Key: groupDictionary[groupIdx], Count: count, DistinctCount: distinct})
	}
	sortColumnPhysicalQueryGroupsByKey(r.resultGroups)
	diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
	diag.DenseGroupCountDistinctUsed = true
	annotateColumnTypedColumnDenseGroupCountDistinctDiagnostics(&diag, groupCardinality, distinctCardinality, pairBitWords, reducer)
	diag.ResultGroups = len(r.resultGroups)
	result := ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) runDenseGroupCountDistinctLocalCodes(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	groupDictionary, groupRanks, err := columnTypedColumnDenseGroupCountDistinctGlobalDictionary(r.parts, func(part *columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn {
		return &part.Group
	})
	if err != nil {
		diag := r.diagnostics(view, req, 0, 0, 0, time.Since(start).Nanoseconds())
		diag.DenseGroupCountDistinctUsed = true
		return ColumnPhysicalQueryResult{Diagnostics: diag}, err
	}
	distinctRanks, distinctCardinality, err := columnTypedColumnDenseGroupCountDistinctGlobalRanks(r.parts, func(part *columnTypedColumnDenseGroupCountDistinctPart) *columnTypedColumnDenseStringCodeColumn {
		return &part.Distinct
	})
	if err != nil {
		diag := r.diagnostics(view, req, 0, 0, 0, time.Since(start).Nanoseconds())
		diag.DenseGroupCountDistinctUsed = true
		return ColumnPhysicalQueryResult{Diagnostics: diag}, err
	}
	groupCardinality := len(groupDictionary)
	if cap(r.denseGroupCountDistinctCounts) < groupCardinality {
		r.denseGroupCountDistinctCounts = make([]int, groupCardinality)
	} else {
		r.denseGroupCountDistinctCounts = r.denseGroupCountDistinctCounts[:groupCardinality]
		clear(r.denseGroupCountDistinctCounts)
	}
	if cap(r.denseGroupCountDistinctDistinctCounts) < groupCardinality {
		r.denseGroupCountDistinctDistinctCounts = make([]int, groupCardinality)
	} else {
		r.denseGroupCountDistinctDistinctCounts = r.denseGroupCountDistinctDistinctCounts[:groupCardinality]
		clear(r.denseGroupCountDistinctDistinctCounts)
	}
	wordsPerGroup, pairBitWords, usePairBitset := columnTypedColumnDenseGroupCountDistinctBitsetLayout(groupCardinality, distinctCardinality)
	reducer := columnTypedColumnDenseGroupCountDistinctReducerLocalBitset
	useActivePairBitset := false
	useSortedPairs := false
	if usePairBitset {
		if cap(r.denseGroupCountDistinctPairBits) < pairBitWords {
			r.denseGroupCountDistinctPairBits = make([]uint64, pairBitWords)
		} else {
			r.denseGroupCountDistinctPairBits = r.denseGroupCountDistinctPairBits[:pairBitWords]
			clear(r.denseGroupCountDistinctPairBits)
		}
	} else if wordsPerGroup > 0 && wordsPerGroup <= columnTypedColumnDenseGroupCountDistinctMaxBitsetWords {
		reducer = columnTypedColumnDenseGroupCountDistinctReducerActiveBitset
		useActivePairBitset = true
		pairBitWords = 0
		r.denseGroupCountDistinctPairBits = r.denseGroupCountDistinctPairBits[:0]
		if cap(r.denseGroupCountDistinctGroupActive) < groupCardinality {
			r.denseGroupCountDistinctGroupActive = make([]bool, groupCardinality)
		} else {
			r.denseGroupCountDistinctGroupActive = r.denseGroupCountDistinctGroupActive[:groupCardinality]
			clear(r.denseGroupCountDistinctGroupActive)
		}
		if cap(r.denseGroupCountDistinctGroupOffsets) < groupCardinality {
			r.denseGroupCountDistinctGroupOffsets = make([]int, groupCardinality)
		} else {
			r.denseGroupCountDistinctGroupOffsets = r.denseGroupCountDistinctGroupOffsets[:groupCardinality]
		}
		r.denseGroupCountDistinctActiveGroups = r.denseGroupCountDistinctActiveGroups[:0]
	} else {
		reducer = columnTypedColumnDenseGroupCountDistinctReducerSortedPairs
		useSortedPairs = true
		pairBitWords = 0
		r.denseGroupCountDistinctPairList = r.denseGroupCountDistinctPairList[:0]
	}

	rowsScanned := 0
	matchedRows := 0
	reduceRows := 0
	for partIdx := range r.parts {
		dense := r.parts[partIdx].DenseGroupCountDistinct
		if dense == nil {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct missing prepared part %d", partIdx)
		}
		if dense.Rows != len(dense.Group.Codes) || dense.Rows != len(dense.Distinct.Codes) {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d rows=%d group_local=%d distinct_local=%d", partIdx, dense.Rows, len(dense.Group.Codes), len(dense.Distinct.Codes))
		}
		groupLocalRanks, groupEmptyRank, groupEmptyOK, err := columnTypedColumnDenseGroupCountDistinctLocalRanks(&dense.Group, groupRanks)
		if err != nil {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct group part %d: %w", partIdx, err)
		}
		distinctLocalRanks, distinctEmptyRank, distinctEmptyOK, err := columnTypedColumnDenseGroupCountDistinctLocalRanks(&dense.Distinct, distinctRanks)
		if err != nil {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct distinct part %d: %w", partIdx, err)
		}
		if columnTypedColumnDensePredicatesRejectAll(dense.Predicates) {
			rowsScanned += dense.Rows
			continue
		}
		for rowIdx := 0; rowIdx < dense.Rows; rowIdx++ {
			rowsScanned++
			if !columnTypedColumnDensePredicatesMatch(dense.Predicates, rowIdx) {
				continue
			}
			if len(dense.Predicates) != 0 {
				matchedRows++
			}
			reduceRows++
			groupCode, err := columnTypedColumnDenseGroupCountDistinctRowRank(&dense.Group, groupLocalRanks, groupEmptyRank, groupEmptyOK, rowIdx)
			if err != nil {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseGroupCountDistinctUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d group: %w", partIdx, err)
			}
			groupIdx, ok := columnDictionaryCodeIndex(groupCode, len(r.denseGroupCountDistinctCounts))
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseGroupCountDistinctUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d group rank[%d]=%d outside cardinality=%d", partIdx, rowIdx, groupCode, len(r.denseGroupCountDistinctCounts))
			}
			r.denseGroupCountDistinctCounts[groupIdx]++
			distinctCode, err := columnTypedColumnDenseGroupCountDistinctRowRank(&dense.Distinct, distinctLocalRanks, distinctEmptyRank, distinctEmptyOK, rowIdx)
			if err != nil {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseGroupCountDistinctUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d distinct: %w", partIdx, err)
			}
			distinctIdx, ok := columnDictionaryCodeIndex(distinctCode, distinctCardinality)
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseGroupCountDistinctUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct part %d distinct rank[%d]=%d outside cardinality=%d", partIdx, rowIdx, distinctCode, distinctCardinality)
			}
			if usePairBitset {
				wordIdx := groupIdx*wordsPerGroup + distinctIdx/64
				mask := uint64(1) << uint(distinctIdx&63)
				if r.denseGroupCountDistinctPairBits[wordIdx]&mask == 0 {
					r.denseGroupCountDistinctPairBits[wordIdx] |= mask
					r.denseGroupCountDistinctDistinctCounts[groupIdx]++
				}
			} else if useActivePairBitset {
				if !r.denseGroupCountDistinctGroupActive[groupIdx] {
					nextPairBitWords, ok := columnTypedColumnDenseGroupCountDistinctBitsetWords(len(r.denseGroupCountDistinctActiveGroups)+1, wordsPerGroup)
					if !ok {
						r.convertDenseGroupCountDistinctActiveBitsToPairs(wordsPerGroup, distinctCardinality)
						clear(r.denseGroupCountDistinctDistinctCounts)
						reducer = columnTypedColumnDenseGroupCountDistinctReducerSortedPairs
						useActivePairBitset = false
						useSortedPairs = true
						pairBitWords = 0
					} else {
						offset := len(r.denseGroupCountDistinctPairBits)
						if cap(r.denseGroupCountDistinctPairBits) < nextPairBitWords {
							nextBits := make([]uint64, nextPairBitWords)
							copy(nextBits, r.denseGroupCountDistinctPairBits)
							r.denseGroupCountDistinctPairBits = nextBits
						} else {
							r.denseGroupCountDistinctPairBits = r.denseGroupCountDistinctPairBits[:nextPairBitWords]
							clear(r.denseGroupCountDistinctPairBits[offset:nextPairBitWords])
						}
						r.denseGroupCountDistinctGroupActive[groupIdx] = true
						r.denseGroupCountDistinctGroupOffsets[groupIdx] = offset
						r.denseGroupCountDistinctActiveGroups = append(r.denseGroupCountDistinctActiveGroups, groupIdx)
						pairBitWords = nextPairBitWords
					}
				}
				if useActivePairBitset {
					wordIdx := r.denseGroupCountDistinctGroupOffsets[groupIdx] + distinctIdx/64
					mask := uint64(1) << uint(distinctIdx&63)
					if r.denseGroupCountDistinctPairBits[wordIdx]&mask == 0 {
						r.denseGroupCountDistinctPairBits[wordIdx] |= mask
						r.denseGroupCountDistinctDistinctCounts[groupIdx]++
					}
				} else {
					r.denseGroupCountDistinctPairList = append(r.denseGroupCountDistinctPairList, uint64(groupIdx)<<32|uint64(distinctIdx))
				}
			} else {
				r.denseGroupCountDistinctPairList = append(r.denseGroupCountDistinctPairList, uint64(groupIdx)<<32|uint64(distinctIdx))
			}
		}
	}
	if useSortedPairs {
		slices.Sort(r.denseGroupCountDistinctPairList)
		var previous uint64
		havePrevious := false
		for _, pair := range r.denseGroupCountDistinctPairList {
			if havePrevious && pair == previous {
				continue
			}
			groupIdx := int(pair >> 32)
			r.denseGroupCountDistinctDistinctCounts[groupIdx]++
			previous = pair
			havePrevious = true
		}
	}
	r.resultGroups = r.resultGroups[:0]
	for groupIdx, count := range r.denseGroupCountDistinctCounts {
		distinct := r.denseGroupCountDistinctDistinctCounts[groupIdx]
		if count == 0 && distinct == 0 {
			continue
		}
		if groupIdx >= len(groupDictionary) {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupCountDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column grouped count-distinct group index=%d outside dictionary=%d", groupIdx, len(groupDictionary))
		}
		r.resultGroups = append(r.resultGroups, ColumnPhysicalQueryGroup{Key: groupDictionary[groupIdx], Count: count, DistinctCount: distinct})
	}
	sortColumnPhysicalQueryGroupsByKey(r.resultGroups)
	diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
	diag.DenseGroupCountDistinctUsed = true
	annotateColumnTypedColumnDenseGroupCountDistinctDiagnostics(&diag, groupCardinality, distinctCardinality, pairBitWords, reducer)
	diag.ResultGroups = len(r.resultGroups)
	result := ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) convertDenseGroupCountDistinctActiveBitsToPairs(wordsPerGroup, distinctCardinality int) {
	r.denseGroupCountDistinctPairList = r.denseGroupCountDistinctPairList[:0]
	for _, groupIdx := range r.denseGroupCountDistinctActiveGroups {
		offset := r.denseGroupCountDistinctGroupOffsets[groupIdx]
		groupBits := r.denseGroupCountDistinctPairBits[offset : offset+wordsPerGroup]
		for wordOffset, word := range groupBits {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				distinctIdx := wordOffset*64 + bit
				if distinctIdx < distinctCardinality {
					r.denseGroupCountDistinctPairList = append(r.denseGroupCountDistinctPairList, uint64(groupIdx)<<32|uint64(distinctIdx))
				}
				word &= word - 1
			}
		}
	}
}

func annotateColumnTypedColumnDenseGroupCountDistinctDiagnostics(diag *ColumnPhysicalQueryDiagnostics, groupCardinality, distinctCardinality, pairBitWords int, reducer string) {
	if diag == nil {
		return
	}
	diag.DenseGroupCountDistinctGroups = groupCardinality
	diag.DenseGroupCountDistinctValues = distinctCardinality
	diag.DenseGroupCountDistinctPairBitWords = pairBitWords
	diag.DenseGroupCountDistinctReducer = reducer
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
		if err := validateColumnTypedColumnDenseGroupHourCountPart(dense, partIdx); err != nil {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseGroupHourCountUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
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
		var missingHourCounts [24]int
		for rowIdx, code := range dense.GroupCodes {
			rowsScanned++
			if !columnTypedColumnDensePredicatesMatch(dense.Predicates, rowIdx) {
				continue
			}
			if len(dense.Predicates) != 0 {
				matchedRows++
			}
			reduceRows++
			hour := columnPhysicalQueryUTCHour(dense.Values[rowIdx])
			if !columnTypedColumnDenseCodeValid(dense.GroupValid, rowIdx) {
				missingHourCounts[hour]++
				continue
			}
			localIdx, ok := columnDictionaryCodeIndex(code, dense.Cardinality)
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseGroupHourCountUsed = true
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column group-hour part %d code[%d]=%d outside cardinality=%d", partIdx, rowIdx, code, dense.Cardinality)
			}
			r.denseLocalHourCounts[localIdx*24+hour]++
		}
		var missingByHour [24]int
		missingSeen := false
		for hour, count := range missingHourCounts {
			if count == 0 {
				continue
			}
			if !missingSeen {
				missingByHour = r.denseGroupHourCounts[""]
				missingSeen = true
			}
			missingByHour[hour] += count
		}
		if missingSeen {
			r.denseGroupHourCounts[""] = missingByHour
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
	if columnTypedColumnDenseInt64SpanPartsHaveGlobalCodes(r.parts) {
		return r.runDenseInt64SpanGlobalCodes(view, req)
	}
	return r.runDenseInt64SpanLocalMap(view, req)
}

func columnTypedColumnDenseInt64SpanPartsHaveGlobalCodes(parts []columnTypedColumnPhysicalQueryPart) bool {
	if len(parts) == 0 {
		return false
	}
	cardinality := -1
	for partIdx := range parts {
		dense := parts[partIdx].DenseInt64Span
		if dense == nil || dense.GlobalDictionary == nil || len(dense.GlobalRanks) != dense.Cardinality {
			return false
		}
		if cardinality < 0 {
			cardinality = len(dense.GlobalDictionary)
			continue
		}
		if len(dense.GlobalDictionary) != cardinality {
			return false
		}
	}
	return true
}

func (r *columnTypedColumnPhysicalQueryRunner) runDenseInt64SpanGlobalCodes(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	var groupDictionary []string
	for partIdx := range r.parts {
		dense := r.parts[partIdx].DenseInt64Span
		if dense == nil {
			diag := r.diagnostics(view, req, 0, 0, 0, time.Since(start).Nanoseconds())
			diag.DenseInt64SpanUsed = true
			diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerGlobalCodes
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span missing prepared part %d", partIdx)
		}
		if len(dense.GroupCodes) != len(dense.Values) || len(dense.GlobalRanks) != dense.Cardinality {
			diag := r.diagnostics(view, req, 0, 0, 0, time.Since(start).Nanoseconds())
			diag.DenseInt64SpanUsed = true
			diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerGlobalCodes
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span part %d group/value/global-rank rows=%d/%d cardinality=%d/%d", partIdx, len(dense.GroupCodes), len(dense.Values), len(dense.GlobalRanks), dense.Cardinality)
		}
		if groupDictionary == nil {
			groupDictionary = dense.GlobalDictionary
			continue
		}
		if len(dense.GlobalDictionary) != len(groupDictionary) {
			diag := r.diagnostics(view, req, 0, 0, 0, time.Since(start).Nanoseconds())
			diag.DenseInt64SpanUsed = true
			diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerGlobalCodes
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span part %d global cardinality=%d want %d", partIdx, len(dense.GlobalDictionary), len(groupDictionary))
		}
	}
	groupCardinality := len(groupDictionary)
	if cap(r.denseLocalSpans) < groupCardinality || cap(r.denseLocalSpanSeen) < groupCardinality {
		r.denseLocalSpans = make([]columnPhysicalQuerySpan, groupCardinality)
		r.denseLocalSpanSeen = make([]bool, groupCardinality)
	} else {
		r.denseLocalSpans = r.denseLocalSpans[:groupCardinality]
		clear(r.denseLocalSpans)
		r.denseLocalSpanSeen = r.denseLocalSpanSeen[:groupCardinality]
		clear(r.denseLocalSpanSeen)
	}
	rowsScanned := 0
	matchedRows := 0
	reduceRows := 0
	for partIdx := range r.parts {
		dense := r.parts[partIdx].DenseInt64Span
		if columnTypedColumnDensePredicatesRejectAll(dense.Predicates) {
			rowsScanned += len(dense.GroupCodes)
			continue
		}
		for rowIdx := range dense.GroupCodes {
			rowsScanned++
			if !columnTypedColumnDensePredicatesMatch(dense.Predicates, rowIdx) {
				continue
			}
			if len(dense.Predicates) != 0 {
				matchedRows++
			}
			reduceRows++
			if !columnTypedColumnDenseCodeValid(dense.GroupValid, rowIdx) {
				if req.SkipEmptyGroupKey {
					continue
				}
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseInt64SpanUsed = true
				diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerGlobalCodes
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("%w: dense typed-column int64-span nullable group column requires skip-empty group key", ErrColumnQueryPlanUnsupported)
			}
			localIdx, ok := columnDictionaryCodeIndex(dense.GroupCodes[rowIdx], len(dense.GlobalRanks))
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseInt64SpanUsed = true
				diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerGlobalCodes
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span part %d code[%d]=%d outside cardinality=%d", partIdx, rowIdx, dense.GroupCodes[rowIdx], len(dense.GlobalRanks))
			}
			globalCode := dense.GlobalRanks[localIdx]
			globalIdx, ok := columnDictionaryCodeIndex(globalCode, len(r.denseLocalSpans))
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseInt64SpanUsed = true
				diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerGlobalCodes
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span part %d global code[%d]=%d outside cardinality=%d", partIdx, rowIdx, globalCode, len(r.denseLocalSpans))
			}
			value := dense.Values[rowIdx]
			if !r.denseLocalSpanSeen[globalIdx] {
				r.denseLocalSpans[globalIdx] = columnPhysicalQuerySpan{min: value, max: value}
				r.denseLocalSpanSeen[globalIdx] = true
				continue
			}
			span := r.denseLocalSpans[globalIdx]
			if value < span.min {
				span.min = value
			}
			if value > span.max {
				span.max = value
			}
			r.denseLocalSpans[globalIdx] = span
		}
	}
	scanNanos := time.Since(start).Nanoseconds()
	r.resultGroups = r.resultGroups[:0]
	diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, scanNanos)
	diag.DenseInt64SpanUsed = true
	diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerGlobalCodes
	if req.TopK > 0 {
		shapeStart := time.Now()
		candidates := 0
		for globalIdx, seen := range r.denseLocalSpanSeen {
			if !seen {
				continue
			}
			key := groupDictionary[globalIdx]
			if req.SkipEmptyGroupKey && key == "" {
				continue
			}
			span := r.denseLocalSpans[globalIdx]
			candidates++
			insertColumnPhysicalTopKGroup(&r.resultGroups, ColumnPhysicalQueryGroup{Key: key, Int64: span.max - span.min}, req.TopK, req.TopKOrder)
		}
		diag.ResultShapeNanos += time.Since(shapeStart).Nanoseconds()
		diag.TopKLimit = req.TopK
		diag.TopKOrder = string(req.TopKOrder)
		diag.TopKCandidates = candidates
		diag.ResultGroups = len(r.resultGroups)
		return ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}, nil
	}
	for globalIdx, seen := range r.denseLocalSpanSeen {
		if !seen {
			continue
		}
		key := groupDictionary[globalIdx]
		if req.SkipEmptyGroupKey && key == "" {
			continue
		}
		span := r.denseLocalSpans[globalIdx]
		r.resultGroups = append(r.resultGroups, ColumnPhysicalQueryGroup{Key: key, Int64: span.max - span.min})
	}
	sortColumnPhysicalQueryGroupsByKey(r.resultGroups)
	diag.ResultGroups = len(r.resultGroups)
	return ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) runDenseInt64SpanLocalMap(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
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
			diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span missing prepared part %d", partIdx)
		}
		if len(dense.GroupCodes) != len(dense.Values) {
			diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
			diag.DenseInt64SpanUsed = true
			diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: dense typed-column int64-span part %d group/value rows=%d/%d", partIdx, len(dense.GroupCodes), len(dense.Values))
		}
		if cap(r.denseLocalSpans) < dense.Cardinality || cap(r.denseLocalSpanSeen) < dense.Cardinality {
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
			if !columnTypedColumnDenseCodeValid(dense.GroupValid, rowIdx) {
				if req.SkipEmptyGroupKey {
					continue
				}
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseInt64SpanUsed = true
				diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
				return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("%w: dense typed-column int64-span nullable group column requires skip-empty group key", ErrColumnQueryPlanUnsupported)
			}
			localIdx, ok := columnDictionaryCodeIndex(code, len(r.denseLocalSpans))
			if !ok {
				diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
				diag.DenseInt64SpanUsed = true
				diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
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
				diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
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
	scanNanos := time.Since(start).Nanoseconds()
	r.resultGroups = r.resultGroups[:0]
	diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, scanNanos)
	diag.DenseInt64SpanUsed = true
	diag.DenseInt64SpanReducer = columnTypedColumnDenseInt64SpanReducerLocalMap
	if req.TopK > 0 {
		shapeStart := time.Now()
		candidates := 0
		for key, span := range r.denseSpanValues {
			if req.SkipEmptyGroupKey && key == "" {
				continue
			}
			candidates++
			insertColumnPhysicalTopKGroup(&r.resultGroups, ColumnPhysicalQueryGroup{Key: key, Int64: span.max - span.min}, req.TopK, req.TopKOrder)
		}
		diag.ResultShapeNanos += time.Since(shapeStart).Nanoseconds()
		diag.TopKLimit = req.TopK
		diag.TopKOrder = string(req.TopKOrder)
		diag.TopKCandidates = candidates
		diag.ResultGroups = len(r.resultGroups)
		return ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}, nil
	}
	for key, span := range r.denseSpanValues {
		r.resultGroups = append(r.resultGroups, ColumnPhysicalQueryGroup{Key: key, Int64: span.max - span.min})
	}
	sortColumnPhysicalQueryGroupsByKey(r.resultGroups)
	diag.ResultGroups = len(r.resultGroups)
	return ColumnPhysicalQueryResult{Groups: r.resultGroups, Diagnostics: diag}, nil
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
	switch len(predicates) {
	case 0:
		return true
	case 1:
		predicate := &predicates[0]
		if predicate.RejectsAll || rowIdx < 0 || rowIdx >= len(predicate.Codes) {
			return false
		}
		if predicate.SingleCodeAllowed {
			return columnTypedColumnDensePredicateSingleCodeMatchesAfterBounds(predicate, rowIdx)
		}
		return columnTypedColumnDensePredicateMatchesAfterBounds(predicate, rowIdx)
	case 2:
		left := &predicates[0]
		right := &predicates[1]
		if left.RejectsAll || right.RejectsAll || rowIdx < 0 || rowIdx >= len(left.Codes) || rowIdx >= len(right.Codes) {
			return false
		}
		if left.SingleCodeAllowed && right.SingleCodeAllowed {
			return columnTypedColumnDensePredicateSingleCodeMatchesAfterBounds(left, rowIdx) && columnTypedColumnDensePredicateSingleCodeMatchesAfterBounds(right, rowIdx)
		}
		if left.Valid == nil && right.Valid == nil {
			if !columnTypedColumnCodeAllowed(left.Allowed, left.Codes[rowIdx]) {
				return false
			}
			return columnTypedColumnCodeAllowed(right.Allowed, right.Codes[rowIdx])
		}
		return columnTypedColumnDensePredicateMatchesAfterBounds(left, rowIdx) && columnTypedColumnDensePredicateMatchesAfterBounds(right, rowIdx)
	case 3:
		left := &predicates[0]
		mid := &predicates[1]
		right := &predicates[2]
		if left.RejectsAll || mid.RejectsAll || right.RejectsAll || rowIdx < 0 || rowIdx >= len(left.Codes) || rowIdx >= len(mid.Codes) || rowIdx >= len(right.Codes) {
			return false
		}
		if left.SingleCodeAllowed && mid.SingleCodeAllowed && right.SingleCodeAllowed {
			return columnTypedColumnDensePredicateSingleCodeMatchesAfterBounds(left, rowIdx) &&
				columnTypedColumnDensePredicateSingleCodeMatchesAfterBounds(mid, rowIdx) &&
				columnTypedColumnDensePredicateSingleCodeMatchesAfterBounds(right, rowIdx)
		}
	}
	for _, predicate := range predicates {
		if predicate.RejectsAll {
			return false
		}
		if rowIdx < 0 || rowIdx >= len(predicate.Codes) {
			return false
		}
		if !columnTypedColumnDenseCodeValid(predicate.Valid, rowIdx) {
			if !predicate.MissingMatchesEmpty {
				return false
			}
			continue
		}
		if !columnTypedColumnCodeAllowed(predicate.Allowed, predicate.Codes[rowIdx]) {
			return false
		}
	}
	return true
}

func columnTypedColumnDensePredicateMatchesAfterBounds(predicate *columnTypedColumnDensePredicatePart, rowIdx int) bool {
	if !columnTypedColumnDenseCodeValid(predicate.Valid, rowIdx) {
		return predicate.MissingMatchesEmpty
	}
	return columnTypedColumnCodeAllowed(predicate.Allowed, predicate.Codes[rowIdx])
}

func columnTypedColumnDensePredicateSingleCodeMatchesAfterBounds(predicate *columnTypedColumnDensePredicatePart, rowIdx int) bool {
	if !columnTypedColumnDenseCodeValid(predicate.Valid, rowIdx) {
		return false
	}
	return predicate.Codes[rowIdx] == predicate.SingleCode
}

func columnTypedColumnDenseCodeValid(valid []bool, rowIdx int) bool {
	return valid == nil || (rowIdx >= 0 && rowIdx < len(valid) && valid[rowIdx])
}

func columnTypedColumnCodeAllowed(allowed []uint64, code uint32) bool {
	word := int(code / 64)
	bit := uint(code % 64)
	return word < len(allowed) && (allowed[word]&(uint64(1)<<bit)) != 0
}

func columnTypedColumnPhysicalQueryUseSortedGroupedDistinct(plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest) bool {
	return !plan.NullableStringValues && req.Kind == ColumnPhysicalQueryGroupCountAndDistinct && plan.SortKeyPrefix.SortedGroupedDistinctReady
}

func (r *columnTypedColumnPhysicalQueryRunner) runSortedGroupedDistinct(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	iterators := make([]*columnTypedColumnSortedGroupedDistinctIterator, 0, len(r.parts))
	heap := columnTypedColumnSortedGroupedDistinctHeap{}
	for partIdx := range r.parts {
		iterator, err := newColumnTypedColumnSortedGroupedDistinctIterator(&r.parts[partIdx], r.plan, req, partIdx, nil)
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

	groups, reduceRows, err := r.reduceSortedGroupedDistinct(iterators, &heap)
	if err != nil {
		rowsScanned, matchedRows := columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators)
		diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
		diag.SortedGroupedDistinctUsed = true
		return ColumnPhysicalQueryResult{Diagnostics: diag}, err
	}
	r.resultGroups = groups
	rowsScanned, matchedRows := columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators)
	diag := r.diagnostics(view, req, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
	diag.SortedGroupedDistinctUsed = true
	diag.ResultGroups = len(groups)
	return ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) runLatestVisibleSortedGroupedDistinct(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, state *typedColumnLatestVisibilityState, visibilityBytes int64, visibilityNanos int64) (ColumnPhysicalQueryResult, error) {
	start := time.Now()
	iterators := make([]*columnTypedColumnSortedGroupedDistinctIterator, 0, len(r.parts))
	heap := columnTypedColumnSortedGroupedDistinctHeap{}
	for partIdx := range r.parts {
		part := &r.parts[partIdx]
		visibility, err := r.latestVisiblePartForRunnerPart(*part, state)
		if err != nil {
			rowsScanned, matchedRows := columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators)
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, 0, time.Since(start).Nanoseconds())
			diag.SortedGroupedDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		iterator, err := newColumnTypedColumnSortedGroupedDistinctIterator(part, r.plan, req, partIdx, visibility)
		if err != nil {
			rowsScanned, matchedRows := columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators)
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, 0, time.Since(start).Nanoseconds())
			diag.SortedGroupedDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		iterators = append(iterators, iterator)
		iteratorIdx := len(iterators) - 1
		if err := iterator.advance(); err != nil {
			rowsScanned, matchedRows := columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators)
			diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, 0, time.Since(start).Nanoseconds())
			diag.SortedGroupedDistinctUsed = true
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		if !iterator.done {
			heap.push(iteratorIdx, iterators)
		}
	}

	groups, reduceRows, err := r.reduceSortedGroupedDistinct(iterators, &heap)
	if err != nil {
		rowsScanned, matchedRows := columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators)
		diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
		diag.SortedGroupedDistinctUsed = true
		return ColumnPhysicalQueryResult{Diagnostics: diag}, err
	}
	r.resultGroups = groups
	rowsScanned, matchedRows := columnTypedColumnSortedGroupedDistinctIteratorTotals(iterators)
	diag := r.latestVisibleDiagnostics(view, req, state, visibilityBytes, visibilityNanos, rowsScanned, matchedRows, reduceRows, time.Since(start).Nanoseconds())
	diag.SortedGroupedDistinctUsed = true
	result := ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	r.resultGroups = result.Groups
	return result, nil
}

type columnTypedColumnSortedGroupedDistinctIterator struct {
	partIndex           int
	row                 int
	rows                int
	rowIndexes          []int
	physicalRows        []int
	visibility          *typedColumnLatestPhysicalPart
	codePart            *columnTypedColumnSortedGroupedDistinctPart
	groupValues         []columnDeclaredValue
	distinctValues      []columnDeclaredValue
	predicateSpecs      []columnPhysicalQueryPredicateSpec
	predicateColumns    [][]columnDeclaredValue
	currentGroup        string
	currentDistinct     string
	currentGroupCode    uint32
	currentDistinctCode uint32
	globalCodes         bool
	done                bool
	rowsScanned         int
	matchedRows         int
}

func newColumnTypedColumnSortedGroupedDistinctIterator(part *columnTypedColumnPhysicalQueryPart, plan columnTypedColumnPhysicalQueryPlan, req ColumnPhysicalQueryRequest, partIndex int, visibility *typedColumnLatestPhysicalPart) (*columnTypedColumnSortedGroupedDistinctIterator, error) {
	if part.SortedGroupedDistinct != nil {
		return &columnTypedColumnSortedGroupedDistinctIterator{
			partIndex:    partIndex,
			rows:         part.SortedGroupedDistinct.Rows,
			rowIndexes:   part.SortedGroupedDistinct.RowIndexes,
			physicalRows: part.SortedGroupedDistinct.PhysicalRows,
			visibility:   visibility,
			codePart:     part.SortedGroupedDistinct,
			globalCodes:  columnTypedColumnSortedGroupedDistinctPartHasGlobalCodes(part.SortedGroupedDistinct),
		}, nil
	}
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
		rowIndexes:       part.RowIndexes,
		physicalRows:     part.PhysicalRowIndexes,
		visibility:       visibility,
		groupValues:      groupValues,
		distinctValues:   distinctValues,
		predicateSpecs:   plan.PredicateSpecs,
		predicateColumns: predicateColumns,
	}, nil
}

func (it *columnTypedColumnSortedGroupedDistinctIterator) advance() error {
	if it.codePart != nil {
		return it.advanceCodes()
	}
	// These slices come from typed-column adapter decoding, which materializes
	// owned String values (not physical-row StringBytes views). Keep the hot loop
	// on direct string header comparisons and avoid per-row map lookups or string
	// conversions; decode/shape validation already happened before the runner was
	// built.
	for it.row < it.rows {
		rowIdx := it.row
		it.row++
		it.rowsScanned++
		if it.visibility != nil {
			physicalRow := rowIdx
			if it.physicalRows != nil {
				physicalRow = it.physicalRows[rowIdx]
			} else if it.rowIndexes != nil {
				physicalRow = it.rowIndexes[rowIdx]
			}
			if !it.visibility.rowVisible(physicalRow) {
				continue
			}
		}
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

func (it *columnTypedColumnSortedGroupedDistinctIterator) advanceCodes() error {
	for it.row < it.rows {
		rowIdx := it.row
		it.row++
		it.rowsScanned++
		if it.visibility != nil {
			physicalRow := rowIdx
			if it.physicalRows != nil {
				physicalRow = it.physicalRows[rowIdx]
			} else if it.rowIndexes != nil {
				physicalRow = it.rowIndexes[rowIdx]
			}
			if !it.visibility.rowVisible(physicalRow) {
				continue
			}
		}
		matched := true
		for _, predicate := range it.codePart.Predicates {
			if predicate.RejectsAll || rowIdx < 0 || rowIdx >= len(predicate.Codes) {
				matched = false
				break
			}
			code := predicate.Codes[rowIdx]
			word := int(code / 64)
			bit := uint(code % 64)
			if word < 0 || word >= len(predicate.Allowed) || (predicate.Allowed[word]&(uint64(1)<<bit)) == 0 {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		if len(it.codePart.Predicates) != 0 {
			it.matchedRows++
		}
		if it.globalCodes {
			if rowIdx >= len(it.codePart.Group.GlobalCodes) || rowIdx >= len(it.codePart.Distinct.GlobalCodes) {
				return fmt.Errorf("collections: sorted grouped-distinct row=%d outside global code rows group=%d distinct=%d", rowIdx, len(it.codePart.Group.GlobalCodes), len(it.codePart.Distinct.GlobalCodes))
			}
			it.currentGroupCode = it.codePart.Group.GlobalCodes[rowIdx]
			it.currentDistinctCode = it.codePart.Distinct.GlobalCodes[rowIdx]
			if int(it.currentGroupCode) >= len(it.codePart.Group.GlobalDictionary) {
				return fmt.Errorf("collections: sorted grouped-distinct global group code=%d outside cardinality=%d", it.currentGroupCode, len(it.codePart.Group.GlobalDictionary))
			}
			if int(it.currentDistinctCode) >= len(it.codePart.Distinct.GlobalDictionary) {
				return fmt.Errorf("collections: sorted grouped-distinct global distinct code=%d outside cardinality=%d", it.currentDistinctCode, len(it.codePart.Distinct.GlobalDictionary))
			}
			return nil
		}
		groupCode := it.codePart.Group.Codes[rowIdx]
		distinctCode := it.codePart.Distinct.Codes[rowIdx]
		if groupCode < 0 || groupCode >= int64(len(it.codePart.Group.Dictionary)) {
			return fmt.Errorf("collections: sorted grouped-distinct group code=%d outside cardinality=%d", groupCode, len(it.codePart.Group.Dictionary))
		}
		if distinctCode < 0 || distinctCode >= int64(len(it.codePart.Distinct.Dictionary)) {
			return fmt.Errorf("collections: sorted grouped-distinct distinct code=%d outside cardinality=%d", distinctCode, len(it.codePart.Distinct.Dictionary))
		}
		it.currentGroup = it.codePart.Group.Dictionary[groupCode]
		it.currentDistinct = it.codePart.Distinct.Dictionary[distinctCode]
		return nil
	}
	it.done = true
	it.currentGroup = ""
	it.currentDistinct = ""
	it.currentGroupCode = 0
	it.currentDistinctCode = 0
	return nil
}

func columnTypedColumnSortedGroupedDistinctPartHasGlobalCodes(part *columnTypedColumnSortedGroupedDistinctPart) bool {
	return part != nil &&
		len(part.Group.GlobalCodes) == part.Rows &&
		len(part.Distinct.GlobalCodes) == part.Rows &&
		part.Group.GlobalDictionary != nil &&
		part.Distinct.GlobalDictionary != nil
}

func (r *columnTypedColumnPhysicalQueryRunner) reduceSortedGroupedDistinct(iterators []*columnTypedColumnSortedGroupedDistinctIterator, heap *columnTypedColumnSortedGroupedDistinctHeap) ([]ColumnPhysicalQueryGroup, int, error) {
	if columnTypedColumnSortedGroupedDistinctHeapUsesGlobalCodes(iterators, heap) {
		return r.reduceSortedGroupedDistinctGlobalCodes(iterators, heap)
	}
	return r.reduceSortedGroupedDistinctStrings(iterators, heap)
}

func (r *columnTypedColumnPhysicalQueryRunner) reduceSortedGroupedDistinctGlobalCodes(iterators []*columnTypedColumnSortedGroupedDistinctIterator, heap *columnTypedColumnSortedGroupedDistinctHeap) ([]ColumnPhysicalQueryGroup, int, error) {
	groups := r.resultGroups[:0]
	firstGroup := true
	var currentGroupCode uint32
	var currentDistinctCode uint32
	var groupDictionary []string
	groupRows := 0
	distinctRows := 0
	reduceRows := 0
	emitGroup := func() {
		if firstGroup {
			return
		}
		groups = append(groups, ColumnPhysicalQueryGroup{Key: groupDictionary[currentGroupCode], Count: groupRows, DistinctCount: distinctRows})
	}
	for heap.len() != 0 {
		iteratorIdx := heap.pop(iterators)
		iterator := iterators[iteratorIdx]
		groupCode := iterator.currentGroupCode
		distinctCode := iterator.currentDistinctCode
		if firstGroup {
			firstGroup = false
			currentGroupCode = groupCode
			currentDistinctCode = distinctCode
			groupDictionary = iterator.codePart.Group.GlobalDictionary
			groupRows = 1
			distinctRows = 1
		} else if groupCode != currentGroupCode {
			emitGroup()
			currentGroupCode = groupCode
			currentDistinctCode = distinctCode
			groupDictionary = iterator.codePart.Group.GlobalDictionary
			groupRows = 1
			distinctRows = 1
		} else {
			groupRows++
			if distinctCode != currentDistinctCode {
				currentDistinctCode = distinctCode
				distinctRows++
			}
		}
		reduceRows++
		if err := iterator.advance(); err != nil {
			return groups, reduceRows, err
		}
		if !iterator.done {
			heap.push(iteratorIdx, iterators)
		}
	}
	emitGroup()
	return groups, reduceRows, nil
}

func (r *columnTypedColumnPhysicalQueryRunner) reduceSortedGroupedDistinctStrings(iterators []*columnTypedColumnSortedGroupedDistinctIterator, heap *columnTypedColumnSortedGroupedDistinctHeap) ([]ColumnPhysicalQueryGroup, int, error) {
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
			return groups, reduceRows, err
		}
		if !iterator.done {
			heap.push(iteratorIdx, iterators)
		}
	}
	emitGroup()
	return groups, reduceRows, nil
}

func columnTypedColumnSortedGroupedDistinctHeapUsesGlobalCodes(iterators []*columnTypedColumnSortedGroupedDistinctIterator, heap *columnTypedColumnSortedGroupedDistinctHeap) bool {
	if heap.len() == 0 {
		return false
	}
	for _, iteratorIdx := range heap.items {
		if iteratorIdx < 0 || iteratorIdx >= len(iterators) || !iterators[iteratorIdx].globalCodes {
			return false
		}
	}
	return true
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
	if left.globalCodes && right.globalCodes {
		if left.currentGroupCode != right.currentGroupCode {
			return left.currentGroupCode < right.currentGroupCode
		}
		if left.currentDistinctCode != right.currentDistinctCode {
			return left.currentDistinctCode < right.currentDistinctCode
		}
		return left.partIndex < right.partIndex
	}
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

type columnTypedColumnPhysicalRowIndexDiagnostics struct {
	GranulesDecoded int
	BlocksDecoded   int
	BytesDecoded    int
}

func typedColumnPhysicalQueryPhysicalRows(part *typedColumnAdapterPart, selectedRows []int, wantRows int) ([]int, columnTypedColumnPhysicalRowIndexDiagnostics, error) {
	if part == nil || part.Part == nil || len(part.Part.Descriptor.SortKey) == 0 || typedColumnSortKeyIsSyntheticPrimaryID(part.Part.Descriptor.SortKey) {
		return nil, columnTypedColumnPhysicalRowIndexDiagnostics{}, nil
	}
	if selectedRows == nil {
		scan, err := part.Part.NewScanner().ScanProjected([]string{typedColumnAdapterPrimaryIDColumn})
		diag := columnTypedColumnPhysicalRowIndexDiagnostics{
			GranulesDecoded: scan.Diagnostics.GranulesDecoded,
			BlocksDecoded:   scan.Diagnostics.BlocksDecoded,
			BytesDecoded:    scan.Diagnostics.BytesDecoded,
		}
		if err != nil {
			return nil, diag, err
		}
		return typedColumnPhysicalQueryPhysicalRowsFromPrimaryIDs(part, nil, wantRows, scan.Columns[typedColumnAdapterPrimaryIDColumn], diag)
	}
	scan, err := part.Part.NewScanner().ScanProjectedRows([]string{typedColumnAdapterPrimaryIDColumn}, selectedRows)
	diag := columnTypedColumnPhysicalRowIndexDiagnostics{
		GranulesDecoded: scan.Diagnostics.GranulesDecoded,
		BlocksDecoded:   scan.Diagnostics.BlocksDecoded,
		BytesDecoded:    scan.Diagnostics.BytesDecoded,
	}
	if err != nil {
		return nil, diag, err
	}
	return typedColumnPhysicalQueryPhysicalRowsFromPrimaryIDs(part, selectedRows, wantRows, scan.Columns[typedColumnAdapterPrimaryIDColumn], diag)
}

func typedColumnPhysicalQueryPhysicalRowsFromPrimaryIDs(part *typedColumnAdapterPart, selectedRows []int, wantRows int, primaryValues []int64, diag columnTypedColumnPhysicalRowIndexDiagnostics) ([]int, columnTypedColumnPhysicalRowIndexDiagnostics, error) {
	if len(primaryValues) != wantRows {
		return nil, diag, fmt.Errorf("collections: typed-column part physical query primary-id rows=%d want %d", len(primaryValues), wantRows)
	}
	physicalRows := make([]int, len(primaryValues))
	identity := true
	for rowIdx, primaryID := range primaryValues {
		physicalRow, err := typedColumnPhysicalRowIndexFromPrimaryID(primaryID, part.Part.Descriptor.RowCount)
		if err != nil {
			return nil, diag, err
		}
		physicalRows[rowIdx] = physicalRow
		wantPhysicalRow := rowIdx
		if selectedRows != nil {
			wantPhysicalRow = selectedRows[rowIdx]
		}
		if physicalRow != wantPhysicalRow {
			identity = false
		}
	}
	if identity {
		return nil, diag, nil
	}
	return physicalRows, diag, nil
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
		return columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: ""}, nil
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
