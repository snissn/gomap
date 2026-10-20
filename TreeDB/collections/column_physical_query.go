package collections

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// ColumnPhysicalQueryKind names the small M13B physical aggregate/projection
// shapes supported directly by the column asset scanner.
type ColumnPhysicalQueryKind string

// ColumnPhysicalQueryTopKOrder selects ordering for optional Top-K int64
// physical query result shaping.
type ColumnPhysicalQueryTopKOrder string

const (
	// ColumnPhysicalQueryTopKInt64Asc keeps the smallest Int64 groups first,
	// using Key as the deterministic tie-break.
	ColumnPhysicalQueryTopKInt64Asc ColumnPhysicalQueryTopKOrder = "int64_asc"
	// ColumnPhysicalQueryTopKInt64Desc keeps the largest Int64 groups first,
	// using Key as the deterministic tie-break.
	ColumnPhysicalQueryTopKInt64Desc ColumnPhysicalQueryTopKOrder = "int64_desc"
)

const (
	// ColumnPhysicalQueryGroupCount counts rows by a string group column.
	ColumnPhysicalQueryGroupCount            ColumnPhysicalQueryKind = "group_count"
	ColumnPhysicalQueryGroupCountDistinct    ColumnPhysicalQueryKind = "group_count_distinct"
	ColumnPhysicalQueryGroupCountAndDistinct ColumnPhysicalQueryKind = "group_count_and_distinct"
	ColumnPhysicalQueryHourCount             ColumnPhysicalQueryKind = "hour_count"
	ColumnPhysicalQueryGroupHourCount        ColumnPhysicalQueryKind = "group_hour_count"
	ColumnPhysicalQueryGroupMinInt64         ColumnPhysicalQueryKind = "group_min_int64"
	ColumnPhysicalQueryGroupMaxInt64         ColumnPhysicalQueryKind = "group_max_int64"
	ColumnPhysicalQueryGroupInt64Span        ColumnPhysicalQueryKind = "group_int64_span"
	ColumnPhysicalQuerySumSecondOfDaySquare  ColumnPhysicalQueryKind = "sum_second_of_day_square"
)

// ColumnPhysicalQueryStorageSource names the physical storage family that
// satisfied a query/report row. It is diagnostic vocabulary for JSONBench
// baselines and must not be used as a route selector.
type ColumnPhysicalQueryStorageSource string

const (
	ColumnPhysicalQueryStorageSourceRowScan                               ColumnPhysicalQueryStorageSource = "row_scan"
	ColumnPhysicalQueryStorageSourceTypedRowAsset                         ColumnPhysicalQueryStorageSource = "typed_row_asset"
	ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset ColumnPhysicalQueryStorageSource = "compatibility_dictionary_code_int64_asset"
	ColumnPhysicalQueryStorageSourceTypedColumnPartSection                ColumnPhysicalQueryStorageSource = "typed_column_part_section"
	ColumnPhysicalQueryStorageSourceAggregateMetadata                     ColumnPhysicalQueryStorageSource = "aggregate_metadata"
	ColumnPhysicalQueryStorageSourceQueryReadyBaseDelta                   ColumnPhysicalQueryStorageSource = "query_ready_base_delta"
	ColumnPhysicalQueryStorageSourceFallback                              ColumnPhysicalQueryStorageSource = "fallback"
	ColumnPhysicalQueryStorageSourceMixed                                 ColumnPhysicalQueryStorageSource = "mixed"
)

// ColumnPhysicalQueryFallbackReason explains why a benchmark/report row did not
// use its most direct physical source. The "none" value is intentionally
// explicit so baseline tables can carry a value for every row.
type ColumnPhysicalQueryFallbackReason string

const (
	ColumnPhysicalQueryFallbackNone                          ColumnPhysicalQueryFallbackReason = "none"
	ColumnPhysicalQueryFallbackDirectAssetReduceUnsupported  ColumnPhysicalQueryFallbackReason = "direct_asset_reduce_unsupported"
	ColumnPhysicalQueryFallbackAggregateMetadataUnsupported  ColumnPhysicalQueryFallbackReason = "aggregate_metadata_unsupported"
	ColumnPhysicalQueryFallbackMutationVisibilityOverlay     ColumnPhysicalQueryFallbackReason = "mutation_visibility_overlay"
	ColumnPhysicalQueryFallbackMutationVisibilityUnsupported ColumnPhysicalQueryFallbackReason = "mutation_visibility_unsupported"
	ColumnPhysicalQueryFallbackDocumentRootRowScan           ColumnPhysicalQueryFallbackReason = "document_root_row_scan"
	ColumnPhysicalQueryFallbackMixed                         ColumnPhysicalQueryFallbackReason = "mixed"
)

const (
	columnPhysicalQueryHourUS             = int64(3_600_000_000)
	columnPhysicalQueryMaxParallelWorkers = 256
)

// ColumnPhysicalQueryRequest describes one explicit physical column query. It
// does not invoke planner routing; M14 owns forced/automatic route selection.
// Predicates are an AND-list of dictionary string equality/IN filters for the
// insert-only physical sidecar path; unsupported predicate shapes fail closed.
// TopK is optional for int64 aggregate reducers. When TopK is set,
// TopKOrder chooses value order and Key is the tie-break; SkipEmptyGroupKey
// omits empty-string group keys before applying the limit.
type ColumnPhysicalQueryRequest struct {
	Kind                     ColumnPhysicalQueryKind
	GroupColumn              string
	ValueColumn              string
	DistinctColumn           string
	AggregateMetadataName    string
	TopK                     int
	TopKOrder                ColumnPhysicalQueryTopKOrder
	SkipEmptyGroupKey        bool
	Predicates               []ColumnPhysicalQueryPredicate
	ColumnAssetReadIntegrity ColumnAssetReadIntegrity
}

// ColumnPhysicalQueryGroup is one reduced result row. Key is the group key.
// Count is populated for count-style queries and remains the distinct count for
// group_count_distinct. Hour is populated by group_hour_count. DistinctCount is
// populated by group_count_and_distinct; Int64 is populated for min/max/span/sum expression queries.
type ColumnPhysicalQueryGroup struct {
	Key           string
	Hour          int
	Count         int
	DistinctCount int
	Int64         int64
}

// ColumnPhysicalQueryDiagnostics reports scan and reduce work for a physical
// query without counting full-document row materialization.
type ColumnPhysicalQueryDiagnostics struct {
	ManifestRoot                          uint64
	ManifestRootName                      string
	ManifestGeneration                    uint64
	ActiveManifestChecksum                uint64
	RecoveryManifestGeneration            uint64
	RecoveryManifestChecksum              uint64
	AppliedCommandLSN                     uint64
	ManifestRecords                       int
	AssetRefs                             int
	MutationParts                         int
	DecodedBlocks                         int
	DirectReduceBlocks                    int
	TypedColumnPartSections               int
	TypedColumnPartSectionBytes           uint64
	MetadataHits                          int
	MetadataEntries                       int
	MetadataMisses                        int
	DictionaryCodeHits                    int
	PredicateDictionaryCodeHits           int
	Int64ValueHits                        int
	ScheduledGranules                     int
	SkippedGranules                       int
	DecodedGranules                       int
	RowsScanned                           int
	RowsMatched                           int
	DeletedRows                           int
	ProjectedColumns                      int
	PredicateCount                        int
	PredicateColumns                      []string
	PredicateKinds                        []string
	PredicateLiterals                     int
	SortKeyPrefixPlanned                  bool
	SortKeyPrefixColumns                  []string
	SortKeyPrefixLiterals                 int
	SortKeyMarkChecks                     int
	SortKeyMarkMatches                    int
	SortKeyMarkSkips                      int
	SortKeyMarkFallbackReason             string
	SortedGroupedDistinctReady            bool
	SortedGroupedDistinctUsed             bool
	SortedGroupedDistinctFallbackReason   string
	DenseGroupCountUsed                   bool
	DenseGroupCountDistinctUsed           bool
	DenseGroupCountDistinctReducer        string
	DenseGroupCountDistinctGroups         int
	DenseGroupCountDistinctValues         int
	DenseGroupCountDistinctPairBitWords   int
	DenseGroupHourCountUsed               bool
	DenseInt64SpanUsed                    bool
	DenseInt64SpanReducer                 string
	DenseInt64SpanPredicateBlocksSkipped  int
	TimeOrderTopKUsed                     bool
	DecodedPayloadBytes                   uint64
	FallbackReads                         int
	RowMaterializations                   int
	DocumentMaterializations              int
	PhysicalBytesScanned                  int64
	DecodedMetadataBytes                  uint64
	MappedBytes                           uint64
	HeapCopyBytes                         uint64
	ReduceRows                            int
	TopKLimit                             int
	TopKCandidates                        int
	TopKOrder                             string
	VisibilityRows                        int
	ReconstructionRows                    int
	ResultGroups                          int
	WorkerCount                           int
	SegmentFileCacheHits                  uint64
	SegmentFileCacheMisses                uint64
	TypedColumnOneShotCacheHit            bool
	TypedColumnOneShotCacheMiss           bool
	TypedColumnOneShotCacheBuild          bool
	TypedColumnOneShotBuildNanos          int64
	TypedColumnPrepareWorkerCount         int
	TypedColumnPreparePlanNanos           int64
	TypedColumnPrepareRefsNanos           int64
	TypedColumnPreparePairingNanos        int64
	TypedColumnPreparePartDecodeNanos     int64
	TypedColumnPreparePostPrepareNanos    int64
	TypedColumnPrepareSummaryNanos        int64
	TypedColumnOneShotCacheStoreNanos     int64
	TypedColumnPrepareReadImageNanos      int64
	TypedColumnPrepareStateBuildNanos     int64
	TypedColumnPrepareDictionaryNanos     int64
	TypedColumnPreparePruningNanos        int64
	TypedColumnPrepareSortKeyNanos        int64
	TypedColumnPrepareStatsNanos          int64
	TypedColumnPrepareRangeReadNanos      int64
	TypedColumnPrepareRangeReadBytes      int64
	TypedColumnPrepareAdapterNanos        int64
	TypedColumnPrepareDenseGroupNanos     int64
	TypedColumnPrepareDenseValueNanos     int64
	TypedColumnPrepareDensePredicateNanos int64
	TypedColumnPrepareDensePreapplyNanos  int64

	TypedColumnPrepareQ2GroupRankNanos                    int64
	TypedColumnPrepareQ2DistinctRankNanos                 int64
	TypedColumnPrepareQ2LocalRankNanos                    int64
	TypedColumnPrepareQ2DenseGroupGlobalRankNanos         int64
	TypedColumnPrepareQ2DenseDistinctGlobalRankNanos      int64
	TypedColumnPrepareQ2DensePartLocalRankNanos           int64
	TypedColumnPrepareQ2DenseDistinctRankPlanNanos        int64
	TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos int64
	TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos int64
	TypedColumnPrepareQ2DenseDistinctRankShardCount       int
	TypedColumnPrepareQ2DenseDistinctRankRefs             int
	TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs     int
	TypedColumnPrepareQ2DenseDistinctGlobalRanks          int

	TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos    int64
	TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos int64
	TypedColumnPrepareQ2GroupGlobalCodeRemapNanos         int64
	TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos      int64

	ColumnAssetReadIntegrity                    string
	StorageSource                               ColumnPhysicalQueryStorageSource
	FallbackReason                              ColumnPhysicalQueryFallbackReason
	ScanNanos                                   int64
	VisibilityNanos                             int64
	ReduceNanos                                 int64
	ResultShapeNanos                            int64
	ReconstructionNanos                         int64
	QueryReadyEncodedExecutions                 int
	QueryReadyPreparedParts                     int
	QueryReadyBaseParts                         int
	QueryReadyDeltaParts                        int
	QueryReadyRowsCandidate                     int
	QueryReadyRowsVisible                       int
	QueryReadyRowsSuperseded                    int
	QueryReadyCodeTranslations                  int
	QueryReadyDictionaryDomains                 int
	QueryReadyScratchBytes                      int64
	QueryReadyPreparationNanos                  int64
	QueryReadyBaseScanNanos                     int64
	QueryReadyDeltaMergeNanos                   int64
	QueryReadyPredicateNanos                    int64
	QueryReadyReductionNanos                    int64
	QueryReadyFusedPredicateReductionExecutions int
	QueryReadyFusedPredicateReductionWorkers    int
	QueryReadyFusedPredicateReductionNanos      int64
	QueryReadyGroupingNanos                     int64
	QueryReadyOrderingTopKNanos                 int64
	QueryReadyLegacyFallbacks                   int
	QueryReadyPrecomputedAnswers                int
}

// ColumnPhysicalQueryResult is the reduced result and diagnostics from an
// explicit physical column query.
type ColumnPhysicalQueryResult struct {
	Groups      []ColumnPhysicalQueryGroup
	Diagnostics ColumnPhysicalQueryDiagnostics
}

// ColumnPhysicalQueryRunner pins one immutable snapshot and reuses direct-query
// execution state across repeated scans. It is intentionally limited to
// insert-only direct physical reducers; mutation visibility and planner routing
// remain owned by RunColumnPhysicalQuery.
//
// The runner is not safe for concurrent use; callers must externally
// synchronize Run and Close. Result groups returned by Run alias runner-owned
// storage and are valid only until the next Run or Close.
type ColumnPhysicalQueryRunner struct {
	collection   *Collection
	view         columnPhysicalScanSnapshotView
	closeView    func()
	req          ColumnPhysicalQueryRequest
	exec         *columnPhysicalQueryExecutor
	readCache    columnPhysicalAssetReadCache
	dictCount    *columnDictionaryCodeGroupCountRunner
	dictDistinct *columnDictionaryCodeGroupCountDistinctRunner
	int64Hour    *columnInt64ValueHourCountRunner
	dictInt64    *columnDictionaryInt64GroupRunner
	dictHour     *columnDictionaryHourCountRunner
	typedColumn  *columnTypedColumnPhysicalQueryRunner
	metadata     *columnAggregateMetadataRunner
	lifecyclePin *ColumnAssetLifecyclePinSet
	closed       bool
}

var errColumnPhysicalScanCancelled = errors.New("collections: physical column scan cancelled")

func validateColumnPhysicalQueryRequest(req ColumnPhysicalQueryRequest) error {
	if err := validateColumnPhysicalGroupCountAndDistinctRequest(req); err != nil {
		return err
	}
	if err := validateColumnPhysicalGroupHourRequest(req); err != nil {
		return err
	}
	return validateColumnPhysicalTopKRequest(req)
}

func validateColumnPhysicalGroupHourRequest(req ColumnPhysicalQueryRequest) error {
	if req.Kind != ColumnPhysicalQueryGroupHourCount {
		return nil
	}
	if req.GroupColumn == "" {
		return fmt.Errorf("%w: grouped-hour physical query group column is required", ErrColumnQueryPlanUnsupported)
	}
	if req.ValueColumn == "" {
		return fmt.Errorf("%w: grouped-hour physical query value column is required", ErrColumnQueryPlanUnsupported)
	}
	return nil
}

func validateColumnPhysicalGroupCountAndDistinctRequest(req ColumnPhysicalQueryRequest) error {
	if req.Kind != ColumnPhysicalQueryGroupCountAndDistinct {
		return nil
	}
	if req.GroupColumn == "" {
		return fmt.Errorf("%w: physical column query group column is required", ErrColumnQueryPlanUnsupported)
	}
	if req.DistinctColumn == "" {
		return fmt.Errorf("%w: physical column query distinct column is required", ErrColumnQueryPlanUnsupported)
	}
	if req.GroupColumn == req.DistinctColumn {
		return fmt.Errorf("%w: physical column query group and distinct columns must differ", ErrColumnQueryPlanUnsupported)
	}
	return nil
}

func validateColumnPhysicalTopKRequest(req ColumnPhysicalQueryRequest) error {
	if req.TopK < 0 {
		return fmt.Errorf("%w: physical column query top-K limit must be non-negative", ErrColumnQueryPlanUnsupported)
	}
	if req.TopK == 0 {
		if req.TopKOrder != "" {
			return fmt.Errorf("%w: physical column query top-K order requires a positive limit", ErrColumnQueryPlanUnsupported)
		}
		if req.SkipEmptyGroupKey {
			return fmt.Errorf("%w: physical column query skip-empty group key requires a positive top-K limit", ErrColumnQueryPlanUnsupported)
		}
		return nil
	}
	switch req.Kind {
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64, ColumnPhysicalQueryGroupInt64Span:
	default:
		return fmt.Errorf("%w: physical column query top-K requires an int64 aggregate kind", ErrColumnQueryPlanUnsupported)
	}
	switch req.TopKOrder {
	case ColumnPhysicalQueryTopKInt64Asc, ColumnPhysicalQueryTopKInt64Desc:
		return nil
	case "":
		return fmt.Errorf("%w: physical column query top-K order is required", ErrColumnQueryPlanUnsupported)
	default:
		return fmt.Errorf("%w: unsupported physical column query top-K order %q", ErrColumnQueryPlanUnsupported, req.TopKOrder)
	}
}

// RunColumnPhysicalQuery executes an explicit serial physical column query over
// the recovery-authoritative manifest. Insert-only manifests use direct physical
// reducers. Supported dense typed-column mutation queries use latest-visible
// typed-column reducers; unsupported mutation-bearing shapes fail closed or use
// the legacy typed-row visibility overlay only where explicitly routed.
func (c *Collection) RunColumnPhysicalQuery(req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	if err := validateColumnPhysicalQueryRequest(req); err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	mutationParts, err := c.columnPhysicalQueryMutationPartsHint()
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	if mutationParts > 0 {
		if req.AggregateMetadataName != "" {
			return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: aggregate metadata physical query requires insert-only manifest", ErrColumnQueryPlanUnsupported)
		}
		cfg, err := c.columnPhysicalQueryColumnStoreConfig()
		if err != nil {
			return ColumnPhysicalQueryResult{}, err
		}
		if columnTypedColumnPhysicalQueryTouchesTypedColumnPart(cfg, req) {
			view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanSidecarsForPhysicalQuery(req))
			if closeView != nil {
				defer closeView()
			}
			if err != nil {
				return ColumnPhysicalQueryResult{}, err
			}
			if result, ok, err := c.runColumnPhysicalQueryTypedColumnPartInSnapshotView(view, req); ok {
				return result, err
			}
		}
		if columnPhysicalQueryHasPredicates(req) {
			return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: physical predicates require insert-only manifest", ErrColumnQueryPlanUnsupported)
		}
		return c.runColumnPhysicalQueryWithVisibility(cfg, req)
	}
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanSidecarsForPhysicalQuery(req))
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	if view.MutationParts > 0 {
		if req.AggregateMetadataName != "" {
			return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: aggregate metadata physical query requires insert-only manifest", ErrColumnQueryPlanUnsupported)
		}
		if result, ok, err := c.runColumnPhysicalQueryTypedColumnPartInSnapshotView(view, req); ok {
			return result, err
		}
		if columnPhysicalQueryHasPredicates(req) {
			return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: physical predicates require insert-only manifest", ErrColumnQueryPlanUnsupported)
		}
		return c.runColumnPhysicalQueryWithVisibility(view.Config, req)
	}
	if req.AggregateMetadataName != "" {
		return c.runColumnPhysicalQueryAggregateMetadataInSnapshotView(view, req)
	}
	return c.runColumnPhysicalQueryInSnapshotView(view, req)
}

// PrepareColumnPhysicalQuery prepares a reusable direct physical query runner
// over the current recovery-authoritative manifest. The runner pins a snapshot
// until Close and fail-closes for mutation-bearing manifests or unsupported
// query shapes rather than silently changing semantics.
func (c *Collection) PrepareColumnPhysicalQuery(req ColumnPhysicalQueryRequest) (*ColumnPhysicalQueryRunner, error) {
	if err := validateColumnPhysicalQueryRequest(req); err != nil {
		return nil, err
	}
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanSidecarsForPhysicalQuery(req))
	if err != nil {
		if closeView != nil {
			closeView()
		}
		return nil, err
	}
	release := true
	defer func() {
		if release && closeView != nil {
			closeView()
		}
	}()
	if view.MutationParts > 0 {
		return nil, fmt.Errorf("%w: prepared physical column query requires insert-only manifest until typed-column asset lifecycle/pinning is available", ErrColumnQueryPlanUnsupported)
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		return nil, err
	}
	readCache.returnViews = true
	var metadata *columnAggregateMetadataRunner
	var exec *columnPhysicalQueryExecutor
	typedColumn, typedColumnCandidate, err := prepareColumnTypedColumnPhysicalQueryRunner(view, req, &readCache, true)
	if err != nil {
		_ = readCache.close()
		return nil, err
	}
	if typedColumn == nil && req.Kind == ColumnPhysicalQueryHourCount && columnPhysicalQueryHasPredicates(req) {
		_ = readCache.close()
		return nil, fmt.Errorf("%w: hour-count physical predicates require a fused grouped-hour reducer", ErrColumnQueryPlanUnsupported)
	}
	if typedColumn != nil {
		// The typed-column part runner is prepared before compatibility sidecar
		// runners so JSONBench-style typed-owned fields use the sectioned
		// tcs1_typed_column_part image as their production substrate.
	} else if req.AggregateMetadataName != "" {
		metadata, err = prepareColumnAggregateMetadataRunner(view, req, &readCache)
		if err != nil {
			_ = readCache.close()
			return nil, err
		}
		if metadata == nil {
			_ = readCache.close()
			return nil, fmt.Errorf("%w: prepared aggregate metadata query requires metadata assets", ErrColumnQueryPlanUnsupported)
		}
	} else if !columnPhysicalQueryHasPredicates(req) && req.Kind != ColumnPhysicalQueryGroupCountAndDistinct && req.Kind != ColumnPhysicalQueryGroupHourCount {
		// Fused dictionary reducers intentionally skip the
		// columnPhysicalQueryHasPredicates/no-predicate direct executor path:
		// newColumnPhysicalQueryExecutor does not implement these semantics, so
		// prepared execution must use dictionary sidecar runners unless direct
		// fused support is added and validated here.
		exec, err = newColumnPhysicalQueryExecutor(view.Config, req)
		if err != nil {
			_ = readCache.close()
			return nil, err
		}
		if !exec.supportsDirectAssetReduce() {
			_ = readCache.close()
			return nil, fmt.Errorf("%w: prepared physical column query requires direct asset reducer", ErrColumnQueryPlanUnsupported)
		}
	}
	var dictCount *columnDictionaryCodeGroupCountRunner
	var dictDistinct *columnDictionaryCodeGroupCountDistinctRunner
	var int64Hour *columnInt64ValueHourCountRunner
	var dictInt64 *columnDictionaryInt64GroupRunner
	var dictHour *columnDictionaryHourCountRunner
	if typedColumnCandidate && typedColumn == nil {
		_ = readCache.close()
		return nil, fmt.Errorf("%w: typed-column part physical query was selected but no runner was prepared", ErrColumnQueryPlanUnsupported)
	}
	if typedColumn == nil && metadata == nil {
		dictCount, err = prepareColumnDictionaryCodeGroupCountRunner(view, req, &readCache)
		if err != nil {
			_ = readCache.close()
			return nil, err
		}
		dictDistinct, err = prepareColumnDictionaryCodeGroupCountDistinctRunner(view, req, &readCache)
		if err != nil {
			_ = readCache.close()
			return nil, err
		}
		if !columnPhysicalQueryHasPredicates(req) {
			int64Hour, err = prepareColumnInt64ValueHourCountRunner(view, req, &readCache)
			if err != nil {
				_ = readCache.close()
				return nil, err
			}
		}
		dictInt64, err = prepareColumnDictionaryInt64GroupRunner(view, req, &readCache)
		if err != nil {
			_ = readCache.close()
			return nil, err
		}
		dictHour, err = prepareColumnDictionaryHourCountRunner(view, req, &readCache)
		if err != nil {
			_ = readCache.close()
			return nil, err
		}
		if req.Kind == ColumnPhysicalQueryGroupCountAndDistinct && dictDistinct == nil {
			_ = readCache.close()
			return nil, fmt.Errorf("%w: fused count-distinct physical query requires dictionary sidecar reducers", ErrColumnQueryPlanUnsupported)
		}
		if req.Kind == ColumnPhysicalQueryGroupHourCount && dictHour == nil {
			_ = readCache.close()
			return nil, fmt.Errorf("%w: grouped-hour physical query requires dictionary/int64 sidecar reducers", ErrColumnQueryPlanUnsupported)
		}
		if columnPhysicalQueryHasPredicates(req) && dictCount == nil && dictDistinct == nil && dictInt64 == nil && dictHour == nil {
			_ = readCache.close()
			return nil, fmt.Errorf("%w: physical predicates require supported dictionary sidecar reducers", ErrColumnQueryPlanUnsupported)
		}
	}
	lifecyclePin, err := c.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{
		Source: ColumnAssetLifecyclePinSourcePreparedQuery,
		Owner:  "column_physical_query_runner",
		Reason: fmt.Sprintf("prepared physical query kind=%s", req.Kind),
		Refs:   columnPhysicalScanSnapshotViewAssetRefs(view),
	})
	if err != nil {
		return nil, errors.Join(err, readCache.close())
	}
	if view.snapshot != nil {
		view.snapshot.DetachForegroundRead()
		view.snapshot = nil
	}
	release = false
	return &ColumnPhysicalQueryRunner{
		collection:   c,
		view:         view,
		closeView:    closeView,
		req:          req,
		exec:         exec,
		readCache:    readCache,
		dictCount:    dictCount,
		dictDistinct: dictDistinct,
		int64Hour:    int64Hour,
		dictInt64:    dictInt64,
		dictHour:     dictHour,
		typedColumn:  typedColumn,
		metadata:     metadata,
		lifecyclePin: lifecyclePin,
	}, nil
}

// Close releases the pinned snapshot, lifecycle pin set, and typed column asset
// readers owned by the prepared physical query runner.
func (r *ColumnPhysicalQueryRunner) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	var closeErr error
	if err := r.readCache.close(); err != nil {
		closeErr = errors.Join(closeErr, err)
	}
	if r.lifecyclePin != nil {
		closeErr = errors.Join(closeErr, r.lifecyclePin.Close())
		r.lifecyclePin = nil
	}
	if r.closeView != nil {
		r.closeView()
		r.closeView = nil
	}
	return closeErr
}

// PrepareDiagnostics reports setup work captured while constructing this
// prepared runner. It is intentionally sparse for runner types without
// instrumented setup subphases.
func (r *ColumnPhysicalQueryRunner) PrepareDiagnostics() ColumnPhysicalQueryDiagnostics {
	var diag ColumnPhysicalQueryDiagnostics
	if r == nil || r.typedColumn == nil {
		return diag
	}
	r.typedColumn.prepareDiagnostics.applyTo(&diag)
	return diag
}

// Run executes the prepared direct physical query against the pinned snapshot.
// The returned Groups slice aliases runner-owned storage to keep hot loops
// allocation-free; copy Groups before calling Run again or Close if the result
// must be retained.
func (r *ColumnPhysicalQueryRunner) Run() (ColumnPhysicalQueryResult, error) {
	if r == nil || r.closed {
		return ColumnPhysicalQueryResult{}, errors.New("collections: prepared physical column query runner is closed")
	}
	endForegroundRead := noCollectionForegroundReadEnd
	if r.collection != nil && r.collection.db != nil {
		endForegroundRead = r.collection.db.BeginForegroundRead()
	}
	defer endForegroundRead()
	if r.typedColumn != nil {
		return r.typedColumn.run(r.view, r.req)
	}
	if r.dictCount != nil {
		result := r.dictCount.run(r.view, r.req)
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
		return result, nil
	}
	if r.dictDistinct != nil {
		result := r.dictDistinct.run(r.view, r.req)
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
		return result, nil
	}
	if r.int64Hour != nil {
		result := r.int64Hour.run(r.view, r.req)
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
		return result, nil
	}
	if r.dictInt64 != nil {
		result := r.dictInt64.run(r.view, r.req)
		finalizeColumnPhysicalQueryResultGroups(r.req, &result)
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
		return result, nil
	}
	if r.dictHour != nil {
		result := r.dictHour.run(r.view, r.req)
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
		return result, nil
	}
	if r.metadata != nil {
		result := r.metadata.run(r.view, r.req)
		annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceAggregateMetadata, ColumnPhysicalQueryFallbackNone)
		return result, nil
	}
	r.exec.resetForRun()
	beforeHits := r.readCache.hits
	beforeMisses := r.readCache.misses
	scanStart := time.Now()
	diag, err := r.collection.scanColumnPhysicalQueryDirectInSnapshotViewWithReadCache(r.view, r.exec, &r.readCache, 0, 0, nil)
	scanNanos := time.Since(scanStart).Nanoseconds()
	diag.SegmentFileCacheHits -= beforeHits
	diag.SegmentFileCacheMisses -= beforeMisses
	result := ColumnPhysicalQueryResult{
		Diagnostics: columnPhysicalQueryDiagnosticsFromScan(diag),
	}
	annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedRowAsset, ColumnPhysicalQueryFallbackNone)
	result.Diagnostics.DirectReduceBlocks = diag.DecodedBlocks
	result.Diagnostics.WorkerCount = 1
	result.Diagnostics.ScanNanos = scanNanos
	result.Diagnostics.ReduceRows = r.exec.reduceRows
	if err != nil {
		return result, err
	}
	result.Groups = r.exec.groups()
	finalizeColumnPhysicalQueryResultGroups(r.req, &result)
	return result, nil
}

func (c *Collection) runColumnPhysicalQueryAggregateMetadataInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	if view.MutationParts != 0 {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: aggregate metadata physical query requires insert-only manifest", ErrColumnQueryPlanUnsupported)
	}
	aggregate, ok := columnPhysicalQueryAggregateMetadataConfig(view.FullConfig, req)
	if !ok {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: aggregate metadata %q does not match physical query shape", ErrColumnQueryPlanUnsupported, req.AggregateMetadataName)
	}
	refs := columnPhysicalQueryAggregateMetadataRefs(view.AggregateMetadata, aggregate.Name)
	if len(refs) == 0 {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: aggregate metadata %q has no physical asset refs", ErrColumnQueryPlanUnsupported, aggregate.Name)
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()
	acc := newColumnPhysicalQueryMetadataAccumulator(req.Kind)
	predicateDiagnostics := newColumnPhysicalQueryPredicateDiagnosticPlan(req)
	var rawScratch []byte
	start := time.Now()
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.StorageSource = ColumnPhysicalQueryStorageSourceAggregateMetadata
	diag.FallbackReason = ColumnPhysicalQueryFallbackNone
	diag.WorkerCount = 1
	projectedColumns := 2
	if req.Kind == ColumnPhysicalQueryGroupCount {
		projectedColumns = 1
	}
	diag.ProjectedColumns = columnPhysicalQueryDiagnosticProjectedColumns(predicateDiagnostics, projectedColumns)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	diag.ScheduledGranules = len(refs)
	for _, metadataRef := range refs {
		raw, err := readCache.read(metadataRef.AssetRef, rawScratch)
		diag.SegmentFileCacheHits = readCache.hits
		diag.SegmentFileCacheMisses = readCache.misses
		if err != nil {
			diag.MetadataMisses++
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: aggregate metadata read %q generation=%d part_id=%d: %w", metadataRef.Name, metadataRef.AssetRef.Generation, metadataRef.AssetRef.PartID, err)
		}
		rawScratch = raw
		diag.PhysicalBytesScanned += int64(len(raw))
		diag.DecodedMetadataBytes += uint64(len(raw))
		if readCache.lastView {
			diag.MappedBytes += uint64(len(raw))
		} else {
			diag.HeapCopyBytes += uint64(len(raw))
		}
		asset, err := decodeColumnAggregateMetadataAsset(raw, metadataRef.AssetRef, view.FullConfig, view.CollectionName, aggregate.Name)
		if err != nil {
			diag.MetadataMisses++
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		if asset.GroupColumn != req.GroupColumn || asset.ValueColumn != req.ValueColumn {
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("%w: aggregate metadata %q columns %s/%s do not match query %s/%s", ErrColumnQueryPlanUnsupported, aggregate.Name, asset.GroupColumn, asset.ValueColumn, req.GroupColumn, req.ValueColumn)
		}
		if !columnPhysicalQueryPredicatesExactEqual(asset.Predicates, aggregate.Predicates) {
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("%w: aggregate metadata %q predicate coverage does not match declared metadata", ErrColumnQueryPlanUnsupported, aggregate.Name)
		}
		acc.add(asset.Entries)
		diag.MetadataHits++
	}
	diag.ScanNanos = time.Since(start).Nanoseconds()
	diag.ReduceRows = acc.rows
	diag.MetadataEntries = acc.entries
	applyColumnPhysicalQueryPredicateDiagnostics(&diag, predicateDiagnostics, acc.rows, 0)
	shapeStart := time.Now()
	groups := acc.groups(req)
	diag.ResultShapeNanos = time.Since(shapeStart).Nanoseconds()
	diag.ResultGroups = len(groups)
	diag.TopKLimit = req.TopK
	diag.TopKOrder = string(req.TopKOrder)
	diag.TopKCandidates = acc.topKCandidates(req)
	diag.SegmentFileCacheHits = readCache.hits
	diag.SegmentFileCacheMisses = readCache.misses
	return ColumnPhysicalQueryResult{Groups: groups, Diagnostics: diag}, nil
}

func (c *Collection) runColumnPhysicalQueryDirectInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest, refModulo, refRemainder int, shouldCancel func() bool) (ColumnPhysicalQueryResult, bool, error) {
	exec, err := newColumnPhysicalQueryExecutor(view.Config, req)
	if err != nil {
		return ColumnPhysicalQueryResult{}, false, err
	}
	if !exec.supportsDirectAssetReduce() {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	scanStart := time.Now()
	diag, err := c.scanColumnPhysicalQueryDirectInSnapshotView(view, exec, refModulo, refRemainder, shouldCancel)
	scanNanos := time.Since(scanStart).Nanoseconds()
	result := ColumnPhysicalQueryResult{
		Diagnostics: columnPhysicalQueryDiagnosticsFromScan(diag),
	}
	annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedRowAsset, ColumnPhysicalQueryFallbackNone)
	result.Diagnostics.DirectReduceBlocks = diag.DecodedBlocks
	result.Diagnostics.ScanNanos = scanNanos
	result.Diagnostics.ReduceRows = exec.reduceRows
	if err != nil {
		return result, true, err
	}
	result.Groups = exec.groups()
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	return result, true, nil
}

func (c *Collection) scanColumnPhysicalQueryDirectInSnapshotView(
	view columnPhysicalScanSnapshotView,
	exec *columnPhysicalQueryExecutor,
	refModulo int,
	refRemainder int,
	shouldCancel func() bool,
) (columnPhysicalScanDiagnostics, error) {
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, exec.readIntegrity)
	if err != nil {
		return view.Diagnostics, err
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()
	return c.scanColumnPhysicalQueryDirectInSnapshotViewWithReadCache(view, exec, &readCache, refModulo, refRemainder, shouldCancel)
}

func (c *Collection) scanColumnPhysicalQueryDirectInSnapshotViewWithReadCache(
	view columnPhysicalScanSnapshotView,
	exec *columnPhysicalQueryExecutor,
	readCache *columnPhysicalAssetReadCache,
	refModulo int,
	refRemainder int,
	shouldCancel func() bool,
) (columnPhysicalScanDiagnostics, error) {
	cfg := view.Config
	diag := view.Diagnostics
	if c == nil {
		return diag, errCollectionNil
	}
	if c.db == nil {
		return diag, errCollectionDBNil
	}
	if !view.ColumnStoreEnabled || !cfg.Enabled {
		return diag, errors.New("collections: physical column scan requires enabled column_store")
	}
	if cfg.ActiveManifest == nil {
		return diag, errors.New("collections: physical column scan requires active column manifest")
	}
	if view.MutationParts != 0 {
		return diag, errColumnPhysicalQueryNeedsVisibility
	}
	if refModulo < 0 {
		return diag, errors.New("collections: physical column scan ref ordinal modulo cannot be negative")
	}
	if refModulo == 0 && refRemainder != 0 {
		return diag, fmt.Errorf("collections: physical column scan ref ordinal remainder=%d requires non-zero modulo", refRemainder)
	}
	if refModulo > 0 && (refRemainder < 0 || refRemainder >= refModulo) {
		return diag, fmt.Errorf("collections: physical column scan ref ordinal remainder=%d outside modulo=%d", refRemainder, refModulo)
	}
	diag.ProjectedColumns = len(exec.projected)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(exec.readIntegrity)
	if readCache == nil {
		return diag, errors.New("collections: prepared physical column query missing read cache")
	}
	readCache.returnViews = true
	var rawScratch []byte
	start, step := columnPhysicalScanRefOrdinalPartition(columnPhysicalScanRequest{RefOrdinalModulo: refModulo, RefOrdinalRemainder: refRemainder})
	for ordinal := start; ordinal < len(view.AssetRefs); ordinal += step {
		assetRef := view.AssetRefs[ordinal]
		if shouldCancel != nil && shouldCancel() {
			return diag, errColumnPhysicalScanCancelled
		}
		diag.ScheduledGranules++
		ref := assetRef.Ref
		raw, err := readCache.read(ref, rawScratch)
		diag.SegmentFileCacheHits = readCache.hits
		diag.SegmentFileCacheMisses = readCache.misses
		if err != nil {
			return diag, fmt.Errorf("collections: column physical scan read generation=%d part_id=%d: %w", ref.Generation, ref.PartID, err)
		}
		// rawScratch may alias readCache.scratch; the direct reducer consumes raw synchronously and does not retain it.
		rawScratch = raw
		diag.PhysicalBytesScanned += int64(len(raw))
		summary, err := reduceColumnPhysicalAssetDirect(raw, ref, view.CollectionName, &cfg, assetRef.Reason, exec)
		if err != nil {
			if errors.Is(err, errColumnPhysicalAssetManifestOperationMismatch) {
				return diag, errColumnPhysicalQueryNeedsVisibility
			}
			return diag, fmt.Errorf("collections: column physical direct reduce generation=%d part_id=%d: %w", ref.Generation, ref.PartID, err)
		}
		diag.DecodedBlocks++
		diag.RowsScanned += summary.rows
		diag.DeletedRows += summary.deleted
	}
	diag.SegmentFileCacheHits = readCache.hits
	diag.SegmentFileCacheMisses = readCache.misses
	return diag, nil
}

// RunColumnPhysicalQueryParallel executes an insert-only physical query by
// partitioning immutable manifest refs across worker-local serial scanners.
// Mutation-bearing manifests stay fail-closed until partitioned visibility
// reconstruction is available.
func (c *Collection) RunColumnPhysicalQueryParallel(req ColumnPhysicalQueryRequest, maxWorkers int) (ColumnPhysicalQueryResult, error) {
	if err := validateColumnPhysicalQueryRequest(req); err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	if req.AggregateMetadataName != "" {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: parallel aggregate metadata physical queries are not supported", ErrColumnQueryPlanUnsupported)
	}
	if columnPhysicalQueryHasPredicates(req) {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: parallel physical predicates require partitioned dictionary sidecar execution", ErrColumnQueryPlanUnsupported)
	}
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	if view.MutationParts > 0 {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: parallel physical column query requires insert-only manifest until partitioned visibility execution lands", ErrColumnQueryPlanUnsupported)
	}
	if maxWorkers <= 1 {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: parallel physical column query requires at least two workers", ErrColumnQueryPlanUnsupported)
	}
	if len(view.AssetRefs) <= 1 {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: parallel physical column query requires more than one asset ref", ErrColumnQueryPlanUnsupported)
	}
	workers := maxWorkers
	workers = columnPhysicalQueryParallelWorkerCount(workers, len(view.AssetRefs))
	cfg := view.Config
	merged, err := newColumnPhysicalQueryExecutor(cfg, req)
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	direct := merged.supportsDirectAssetReduce()

	type workerResult struct {
		exec         *columnPhysicalQueryExecutor
		diag         columnPhysicalScanDiagnostics
		directBlocks int
		err          error
	}
	results := make([]workerResult, workers)
	start := time.Now()
	var cancel atomic.Bool
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			exec, err := newColumnPhysicalQueryExecutor(cfg, req)
			if err != nil {
				cancel.Store(true)
				results[worker].err = err
				return
			}
			var diag columnPhysicalScanDiagnostics
			var directBlocks int
			if direct {
				diag, err = c.scanColumnPhysicalQueryDirectInSnapshotView(view, exec, workers, worker, cancel.Load)
				directBlocks = diag.DecodedBlocks
			} else {
				diag, err = c.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{
					ProjectedColumns:    exec.projected,
					Visitor:             exec.visit,
					RequireInsertOnly:   true,
					RefOrdinalModulo:    workers,
					RefOrdinalRemainder: worker,
					ShouldCancel:        cancel.Load,
					ReadIntegrity:       req.ColumnAssetReadIntegrity,
				})
			}
			if err != nil {
				cancel.Store(true)
			}
			results[worker] = workerResult{exec: exec, diag: diag, directBlocks: directBlocks, err: err}
		}()
	}
	wg.Wait()

	result := ColumnPhysicalQueryResult{}
	fallbackReason := ColumnPhysicalQueryFallbackNone
	if !direct {
		fallbackReason = ColumnPhysicalQueryFallbackDirectAssetReduceUnsupported
	}
	var firstErr error
	for worker := range results {
		workerResult := results[worker]
		result.Diagnostics = mergeColumnPhysicalQueryDiagnostics(result.Diagnostics, columnPhysicalQueryDiagnosticsFromScan(workerResult.diag))
		result.Diagnostics.DirectReduceBlocks += workerResult.directBlocks
		if workerResult.err != nil {
			if firstErr == nil || errors.Is(firstErr, errColumnPhysicalScanCancelled) {
				firstErr = workerResult.err
			}
			continue
		}
		if err := merged.mergeFrom(workerResult.exec); err != nil {
			result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
			result.Diagnostics.ReduceRows = merged.reduceRows
			return result, err
		}
	}
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	result.Diagnostics.WorkerCount = workers
	annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedRowAsset, fallbackReason)
	if firstErr != nil {
		result.Diagnostics.ReduceRows = merged.reduceRows
		return result, firstErr
	}
	result.Groups = merged.groups()
	result.Diagnostics.ReduceRows = merged.reduceRows
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	return result, nil
}

func columnPhysicalQueryParallelWorkerCount(maxWorkers, assetRefs int) int {
	workers := maxWorkers
	if workers > assetRefs {
		workers = assetRefs
	}
	if workers > columnPhysicalQueryMaxParallelWorkers {
		workers = columnPhysicalQueryMaxParallelWorkers
	}
	return workers
}

func columnManifestScanSidecarsForPhysicalQuery(req ColumnPhysicalQueryRequest) columnManifestScanSidecarFilter {
	if req.AggregateMetadataName != "" {
		return columnManifestScanSidecarFilter{AggregateMetadata: true, AggregateMetadataName: req.AggregateMetadataName}
	}
	if !columnPhysicalQueryHasPredicates(req) {
		switch req.Kind {
		case ColumnPhysicalQueryGroupCount:
			return columnManifestScanSidecarFilter{DictionaryCodes: true, DictionaryColumn: req.GroupColumn}
		case ColumnPhysicalQueryGroupCountDistinct, ColumnPhysicalQueryGroupCountAndDistinct:
			return columnManifestScanSidecarFilter{DictionaryCodes: true, DictionaryColumn: req.GroupColumn, DictionaryColumn2: req.DistinctColumn}
		case ColumnPhysicalQueryHourCount:
			return columnManifestScanSidecarFilter{Int64Values: true, Int64Column: req.ValueColumn}
		case ColumnPhysicalQueryGroupHourCount:
			return columnManifestScanSidecarFilter{DictionaryCodes: true, DictionaryColumn: req.GroupColumn, Int64Values: true, Int64Column: req.ValueColumn}
		case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64, ColumnPhysicalQueryGroupInt64Span:
			return columnManifestScanSidecarFilter{DictionaryCodes: true, DictionaryColumn: req.GroupColumn, Int64Values: true, Int64Column: req.ValueColumn}
		default:
			return columnManifestScanNoSidecars()
		}
	}
	filter := columnManifestScanSidecarFilter{DictionaryColumns: make([]string, 0, len(req.Predicates)+2)}
	addDictionaryColumn := func(name string) {
		if name == "" {
			return
		}
		filter.DictionaryCodes = true
		for _, existing := range filter.DictionaryColumns {
			if existing == name {
				return
			}
		}
		filter.DictionaryColumns = append(filter.DictionaryColumns, name)
	}
	for _, predicate := range req.Predicates {
		addDictionaryColumn(predicate.Column)
	}
	switch req.Kind {
	case ColumnPhysicalQueryGroupCount:
		addDictionaryColumn(req.GroupColumn)
	case ColumnPhysicalQueryGroupCountDistinct, ColumnPhysicalQueryGroupCountAndDistinct:
		addDictionaryColumn(req.GroupColumn)
		addDictionaryColumn(req.DistinctColumn)
	case ColumnPhysicalQueryHourCount:
		filter.Int64Values = true
		filter.Int64Column = req.ValueColumn
	case ColumnPhysicalQueryGroupHourCount:
		addDictionaryColumn(req.GroupColumn)
		filter.Int64Values = true
		filter.Int64Column = req.ValueColumn
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64, ColumnPhysicalQueryGroupInt64Span:
		addDictionaryColumn(req.GroupColumn)
		filter.Int64Values = true
		filter.Int64Column = req.ValueColumn
	default:
		return columnManifestScanNoSidecars()
	}
	return filter
}

func (c *Collection) runColumnPhysicalQueryInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	if result, ok, err := c.runColumnPhysicalQueryTypedColumnOneShotInSnapshotView(view, req); ok {
		return result, err
	}
	if result, ok, err := c.runColumnPhysicalQueryTypedColumnPartInSnapshotView(view, req); ok {
		return result, err
	}
	if result, ok, err := c.runColumnPhysicalQueryDictionaryCodesInSnapshotView(view, req); ok {
		return result, err
	}
	if result, ok, err := c.runColumnPhysicalQueryInt64ValuesInSnapshotView(view, req); ok {
		return result, err
	}
	if result, ok, err := c.runColumnPhysicalQueryDictionaryInt64InSnapshotView(view, req); ok {
		return result, err
	}
	if result, ok, err := c.runColumnPhysicalQueryDictionaryHourInSnapshotView(view, req); ok {
		return result, err
	}
	if columnPhysicalQueryHasPredicates(req) {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: physical predicates require supported dictionary sidecar reducers", ErrColumnQueryPlanUnsupported)
	}
	if result, ok, err := c.runColumnPhysicalQueryDirectInSnapshotView(view, req, 0, 0, nil); ok {
		if err != nil {
			if errors.Is(err, errColumnPhysicalQueryNeedsVisibility) {
				return c.runColumnPhysicalQueryWithVisibility(view.Config, req)
			}
			return result, err
		}
		result.Diagnostics.WorkerCount = 1
		return result, nil
	}
	exec, err := newColumnPhysicalQueryExecutor(view.Config, req)
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	scanStart := time.Now()
	diag, err := c.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{
		ProjectedColumns:  exec.projected,
		Visitor:           exec.visit,
		RequireInsertOnly: true,
		ReadIntegrity:     req.ColumnAssetReadIntegrity,
	})
	scanNanos := time.Since(scanStart).Nanoseconds()
	result := ColumnPhysicalQueryResult{
		Diagnostics: columnPhysicalQueryDiagnosticsFromScan(diag),
	}
	annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedRowAsset, ColumnPhysicalQueryFallbackDirectAssetReduceUnsupported)
	result.Diagnostics.WorkerCount = 1
	result.Diagnostics.ScanNanos = scanNanos
	result.Diagnostics.ReduceRows = exec.reduceRows
	if err != nil {
		return result, err
	}
	result.Groups = exec.groups()
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	return result, nil
}

func (c *Collection) runColumnPhysicalQueryDictionaryCodesInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, bool, error) {
	if req.AggregateMetadataName != "" || view.MutationParts != 0 ||
		(req.Kind != ColumnPhysicalQueryGroupCount && req.Kind != ColumnPhysicalQueryGroupCountDistinct && req.Kind != ColumnPhysicalQueryGroupCountAndDistinct) {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		return ColumnPhysicalQueryResult{}, true, err
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()

	if !columnPhysicalQueryHasPredicates(req) {
		if result, ok, err := runColumnDictionaryCodeGroupCountOneShot(view, req, &readCache); ok {
			result.Diagnostics.SegmentFileCacheHits = readCache.hits
			result.Diagnostics.SegmentFileCacheMisses = readCache.misses
			annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
			return result, true, err
		}
		if result, ok, err := runColumnDictionaryCodeGroupCountDistinctOneShot(view, req, &readCache); ok {
			result.Diagnostics.SegmentFileCacheHits = readCache.hits
			result.Diagnostics.SegmentFileCacheMisses = readCache.misses
			annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
			return result, true, err
		}
	}

	dictCount, err := prepareColumnDictionaryCodeGroupCountRunner(view, req, &readCache)
	if err != nil {
		return ColumnPhysicalQueryResult{}, true, err
	}
	dictDistinct, err := prepareColumnDictionaryCodeGroupCountDistinctRunner(view, req, &readCache)
	if err != nil {
		return ColumnPhysicalQueryResult{}, true, err
	}
	if dictCount == nil && dictDistinct == nil {
		if columnPhysicalQueryHasPredicates(req) {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("%w: physical predicates require dictionary sidecar group reducers", ErrColumnQueryPlanUnsupported)
		}
		return ColumnPhysicalQueryResult{}, false, nil
	}

	var result ColumnPhysicalQueryResult
	if dictCount != nil {
		result = dictCount.run(view, req)
	} else {
		result = dictDistinct.run(view, req)
	}
	result.Diagnostics.SegmentFileCacheHits = readCache.hits
	result.Diagnostics.SegmentFileCacheMisses = readCache.misses
	if !columnPhysicalQueryHasPredicates(req) {
		result.Diagnostics.DictionaryCodeHits = result.Diagnostics.DecodedBlocks
	}
	annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
	return result, true, nil
}

func (c *Collection) runColumnPhysicalQueryInt64ValuesInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, bool, error) {
	if req.AggregateMetadataName != "" || view.MutationParts != 0 ||
		req.Kind != ColumnPhysicalQueryHourCount || req.ValueColumn == "" {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	if columnPhysicalQueryHasPredicates(req) {
		return ColumnPhysicalQueryResult{}, true, fmt.Errorf("%w: hour-count physical predicates require a fused grouped-hour reducer", ErrColumnQueryPlanUnsupported)
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		return ColumnPhysicalQueryResult{}, true, err
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()

	result, ok, err := runColumnInt64ValueHourCountOneShot(view, req, &readCache)
	if err != nil {
		return ColumnPhysicalQueryResult{}, true, err
	}
	if !ok {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	result.Diagnostics.SegmentFileCacheHits = readCache.hits
	result.Diagnostics.SegmentFileCacheMisses = readCache.misses
	annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
	return result, true, nil
}

func (c *Collection) runColumnPhysicalQueryDictionaryInt64InSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, bool, error) {
	if req.AggregateMetadataName != "" || view.MutationParts != 0 ||
		!columnDictionaryInt64GroupQueryKind(req.Kind) || req.GroupColumn == "" || req.ValueColumn == "" {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		return ColumnPhysicalQueryResult{}, true, err
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()

	if !columnPhysicalQueryHasPredicates(req) {
		result, ok, err := runColumnDictionaryInt64GroupOneShot(view, req, &readCache)
		if err != nil {
			return ColumnPhysicalQueryResult{}, true, err
		}
		if ok {
			result.Diagnostics.SegmentFileCacheHits = readCache.hits
			result.Diagnostics.SegmentFileCacheMisses = readCache.misses
			finalizeColumnPhysicalQueryResultGroups(req, &result)
			annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
			return result, true, nil
		}
	}

	runner, err := prepareColumnDictionaryInt64GroupRunner(view, req, &readCache)
	if err != nil {
		return ColumnPhysicalQueryResult{}, true, err
	}
	if runner == nil {
		if columnPhysicalQueryHasPredicates(req) {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("%w: physical predicates require dictionary/int64 sidecar reducers", ErrColumnQueryPlanUnsupported)
		}
		return ColumnPhysicalQueryResult{}, false, nil
	}
	result := runner.run(view, req)
	result.Diagnostics.SegmentFileCacheHits = readCache.hits
	result.Diagnostics.SegmentFileCacheMisses = readCache.misses
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
	return result, true, nil
}

func (c *Collection) runColumnPhysicalQueryDictionaryHourInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, bool, error) {
	if req.AggregateMetadataName != "" || view.MutationParts != 0 || req.Kind != ColumnPhysicalQueryGroupHourCount || req.GroupColumn == "" || req.ValueColumn == "" {
		return ColumnPhysicalQueryResult{}, false, nil
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		return ColumnPhysicalQueryResult{}, true, err
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()

	runner, err := prepareColumnDictionaryHourCountRunner(view, req, &readCache)
	if err != nil {
		return ColumnPhysicalQueryResult{}, true, err
	}
	if runner == nil {
		if columnPhysicalQueryHasPredicates(req) {
			return ColumnPhysicalQueryResult{}, true, fmt.Errorf("%w: grouped-hour physical predicates require dictionary/int64 sidecar reducers", ErrColumnQueryPlanUnsupported)
		}
		return ColumnPhysicalQueryResult{}, false, nil
	}
	result := runner.run(view, req)
	result.Diagnostics.SegmentFileCacheHits = readCache.hits
	result.Diagnostics.SegmentFileCacheMisses = readCache.misses
	annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceCompatibilityDictionaryCodeInt64Asset, ColumnPhysicalQueryFallbackNone)
	return result, true, nil
}

var errColumnPhysicalQueryNeedsVisibility = errors.New("collections: physical column query requires mutation visibility overlay")

func (c *Collection) runColumnPhysicalQueryWithVisibility(cfg ColumnStoreConfig, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	exec, err := newColumnPhysicalQueryExecutor(cfg, req)
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	visibilityStart := time.Now()
	visible, err := c.scanColumnPhysicalVisibleRowsWithReadIntegrity(exec.projected, req.ColumnAssetReadIntegrity)
	visibilityNanos := time.Since(visibilityStart).Nanoseconds()
	result := ColumnPhysicalQueryResult{
		Diagnostics: columnPhysicalQueryDiagnosticsFromScan(visible.Diagnostics),
	}
	annotateColumnPhysicalQueryResult(&result, ColumnPhysicalQueryStorageSourceTypedRowAsset, ColumnPhysicalQueryFallbackMutationVisibilityOverlay)
	result.Diagnostics.WorkerCount = 1
	result.Diagnostics.VisibilityRows = len(visible.Rows)
	result.Diagnostics.ScanNanos = visibilityNanos
	result.Diagnostics.VisibilityNanos = visibilityNanos
	if err != nil {
		return result, err
	}
	reduceStart := time.Now()
	for _, row := range visible.Rows {
		if row.Deleted {
			continue
		}
		if err := exec.visitValues(row.Values); err != nil {
			return result, err
		}
	}
	result.Diagnostics.ReduceNanos = time.Since(reduceStart).Nanoseconds()
	result.Groups = exec.groups()
	result.Diagnostics.ReduceRows = exec.reduceRows
	finalizeColumnPhysicalQueryResultGroups(req, &result)
	return result, nil
}

func (c *Collection) columnPhysicalQueryColumnStoreConfig() (ColumnStoreConfig, error) {
	if c == nil {
		return ColumnStoreConfig{}, errCollectionNil
	}
	c.catalogMu.RLock()
	defer c.catalogMu.RUnlock()
	if c.catalog == nil {
		return ColumnStoreConfig{}, errCollectionNotFound
	}
	cfg := c.catalog.meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled {
		return ColumnStoreConfig{}, fmt.Errorf("%w: physical column query requires enabled column_store", ErrColumnQueryPlanUnsupported)
	}
	return cfg.copy(), nil
}

func (c *Collection) columnPhysicalQueryMutationPartsHint() (uint64, error) {
	if c == nil {
		return 0, errCollectionNil
	}
	c.catalogMu.RLock()
	defer c.catalogMu.RUnlock()
	if c.catalog == nil {
		return 0, errCollectionNotFound
	}
	cfg := c.catalog.meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled {
		return 0, fmt.Errorf("%w: physical column query requires enabled column_store", ErrColumnQueryPlanUnsupported)
	}
	return cfg.PhysicalMutationParts, nil
}

func columnPhysicalQueryDiagnosticsFromScan(diag columnPhysicalScanDiagnostics) ColumnPhysicalQueryDiagnostics {
	return ColumnPhysicalQueryDiagnostics{
		ManifestRoot:               diag.ManifestRoot,
		ManifestRootName:           diag.ManifestRootName,
		ManifestGeneration:         diag.ManifestGeneration,
		ActiveManifestChecksum:     diag.ActiveManifestChecksum,
		RecoveryManifestGeneration: diag.RecoveryManifestGeneration,
		RecoveryManifestChecksum:   diag.RecoveryManifestChecksum,
		AppliedCommandLSN:          diag.AppliedCommandLSN,
		ManifestRecords:            diag.ManifestRecords,
		AssetRefs:                  diag.AssetRefs,
		MutationParts:              diag.MutationParts,
		DecodedBlocks:              diag.DecodedBlocks,
		ScheduledGranules:          diag.ScheduledGranules,
		SkippedGranules:            diag.SkippedGranules,
		RowsScanned:                diag.RowsScanned,
		DeletedRows:                diag.DeletedRows,
		ProjectedColumns:           diag.ProjectedColumns,
		RowMaterializations:        diag.RowMaterializations,
		PhysicalBytesScanned:       diag.PhysicalBytesScanned,
		SegmentFileCacheHits:       diag.SegmentFileCacheHits,
		SegmentFileCacheMisses:     diag.SegmentFileCacheMisses,
		ColumnAssetReadIntegrity:   diag.ColumnAssetReadIntegrity,
		FallbackReason:             ColumnPhysicalQueryFallbackNone,
	}
}

func annotateColumnPhysicalQueryResult(result *ColumnPhysicalQueryResult, source ColumnPhysicalQueryStorageSource, fallback ColumnPhysicalQueryFallbackReason) {
	if result == nil {
		return
	}
	if source != "" {
		result.Diagnostics.StorageSource = source
	}
	if fallback == "" {
		fallback = ColumnPhysicalQueryFallbackNone
	}
	result.Diagnostics.FallbackReason = fallback
}

func mergeColumnPhysicalQueryDiagnostics(left, right ColumnPhysicalQueryDiagnostics) ColumnPhysicalQueryDiagnostics {
	if left.ManifestRoot == 0 {
		left.ManifestRoot = right.ManifestRoot
		left.ManifestRootName = right.ManifestRootName
		left.ManifestGeneration = right.ManifestGeneration
		left.ActiveManifestChecksum = right.ActiveManifestChecksum
		left.RecoveryManifestGeneration = right.RecoveryManifestGeneration
		left.RecoveryManifestChecksum = right.RecoveryManifestChecksum
		left.AppliedCommandLSN = right.AppliedCommandLSN
		left.ManifestRecords = right.ManifestRecords
		left.AssetRefs = right.AssetRefs
		left.ProjectedColumns = right.ProjectedColumns
		left.ColumnAssetReadIntegrity = right.ColumnAssetReadIntegrity
		left.StorageSource = right.StorageSource
		left.FallbackReason = right.FallbackReason
	}
	if left.ManifestRootName == "" {
		left.ManifestRootName = right.ManifestRootName
	}
	if left.ActiveManifestChecksum == 0 {
		left.ActiveManifestChecksum = right.ActiveManifestChecksum
	}
	if left.RecoveryManifestChecksum == 0 {
		left.RecoveryManifestChecksum = right.RecoveryManifestChecksum
	}
	if left.ColumnAssetReadIntegrity == "" {
		left.ColumnAssetReadIntegrity = right.ColumnAssetReadIntegrity
	}
	left.StorageSource = mergeColumnPhysicalQueryStorageSource(left.StorageSource, right.StorageSource)
	left.FallbackReason = mergeColumnPhysicalQueryFallbackReason(left.FallbackReason, right.FallbackReason)
	if right.ManifestRecords > left.ManifestRecords {
		left.ManifestRecords = right.ManifestRecords
	}
	if right.AssetRefs > left.AssetRefs {
		left.AssetRefs = right.AssetRefs
	}
	if right.ProjectedColumns > left.ProjectedColumns {
		left.ProjectedColumns = right.ProjectedColumns
	}
	if right.MutationParts > left.MutationParts {
		left.MutationParts = right.MutationParts
	}
	left.DecodedBlocks += right.DecodedBlocks
	left.DirectReduceBlocks += right.DirectReduceBlocks
	left.TypedColumnPartSections += right.TypedColumnPartSections
	left.TypedColumnPartSectionBytes += right.TypedColumnPartSectionBytes
	left.MetadataHits += right.MetadataHits
	left.MetadataEntries += right.MetadataEntries
	left.MetadataMisses += right.MetadataMisses
	left.DictionaryCodeHits += right.DictionaryCodeHits
	left.PredicateDictionaryCodeHits += right.PredicateDictionaryCodeHits
	left.Int64ValueHits += right.Int64ValueHits
	left.ScheduledGranules += right.ScheduledGranules
	left.SkippedGranules += right.SkippedGranules
	left.DecodedGranules += right.DecodedGranules
	left.RowsScanned += right.RowsScanned
	left.RowsMatched += right.RowsMatched
	left.DeletedRows += right.DeletedRows
	left.FallbackReads += right.FallbackReads
	left.RowMaterializations += right.RowMaterializations
	left.DocumentMaterializations += right.DocumentMaterializations
	if left.PredicateCount == 0 && right.PredicateCount > 0 {
		left.PredicateCount = right.PredicateCount
		left.PredicateColumns = append([]string(nil), right.PredicateColumns...)
		left.PredicateKinds = append([]string(nil), right.PredicateKinds...)
		left.PredicateLiterals = right.PredicateLiterals
	}
	if !left.SortKeyPrefixPlanned && right.SortKeyPrefixPlanned {
		left.SortKeyPrefixPlanned = true
		left.SortKeyPrefixColumns = append([]string(nil), right.SortKeyPrefixColumns...)
		left.SortKeyPrefixLiterals = right.SortKeyPrefixLiterals
	}
	left.SortKeyMarkChecks += right.SortKeyMarkChecks
	left.SortKeyMarkMatches += right.SortKeyMarkMatches
	left.SortKeyMarkSkips += right.SortKeyMarkSkips
	left.SortKeyMarkFallbackReason = mergeColumnPhysicalSortKeyFallbackReason(left.SortKeyMarkFallbackReason, right.SortKeyMarkFallbackReason)
	if right.SortedGroupedDistinctReady {
		left.SortedGroupedDistinctReady = true
	}
	if right.SortedGroupedDistinctUsed {
		left.SortedGroupedDistinctUsed = true
	}
	left.SortedGroupedDistinctFallbackReason = mergeColumnPhysicalSortKeyFallbackReason(left.SortedGroupedDistinctFallbackReason, right.SortedGroupedDistinctFallbackReason)
	left.DenseGroupCountUsed = left.DenseGroupCountUsed || right.DenseGroupCountUsed
	left.DenseGroupCountDistinctUsed = left.DenseGroupCountDistinctUsed || right.DenseGroupCountDistinctUsed
	left.DenseGroupCountDistinctReducer = mergeColumnPhysicalQueryReducerLabel(left.DenseGroupCountDistinctReducer, right.DenseGroupCountDistinctReducer)
	if right.DenseGroupCountDistinctGroups > left.DenseGroupCountDistinctGroups {
		left.DenseGroupCountDistinctGroups = right.DenseGroupCountDistinctGroups
	}
	if right.DenseGroupCountDistinctValues > left.DenseGroupCountDistinctValues {
		left.DenseGroupCountDistinctValues = right.DenseGroupCountDistinctValues
	}
	if right.DenseGroupCountDistinctPairBitWords > left.DenseGroupCountDistinctPairBitWords {
		left.DenseGroupCountDistinctPairBitWords = right.DenseGroupCountDistinctPairBitWords
	}
	left.DenseGroupHourCountUsed = left.DenseGroupHourCountUsed || right.DenseGroupHourCountUsed
	left.DenseInt64SpanUsed = left.DenseInt64SpanUsed || right.DenseInt64SpanUsed
	left.DenseInt64SpanReducer = mergeColumnPhysicalQueryReducerLabel(left.DenseInt64SpanReducer, right.DenseInt64SpanReducer)
	left.DenseInt64SpanPredicateBlocksSkipped += right.DenseInt64SpanPredicateBlocksSkipped
	left.TimeOrderTopKUsed = left.TimeOrderTopKUsed || right.TimeOrderTopKUsed
	left.DecodedPayloadBytes += right.DecodedPayloadBytes
	left.PhysicalBytesScanned += right.PhysicalBytesScanned
	left.DecodedMetadataBytes += right.DecodedMetadataBytes
	left.MappedBytes += right.MappedBytes
	left.HeapCopyBytes += right.HeapCopyBytes
	left.ReduceRows += right.ReduceRows
	left.TopKCandidates += right.TopKCandidates
	if right.TopKLimit > left.TopKLimit {
		left.TopKLimit = right.TopKLimit
	}
	if left.TopKOrder == "" {
		left.TopKOrder = right.TopKOrder
	}
	left.ResultShapeNanos += right.ResultShapeNanos
	left.VisibilityRows += right.VisibilityRows
	left.ReconstructionRows += right.ReconstructionRows
	left.SegmentFileCacheHits += right.SegmentFileCacheHits
	left.SegmentFileCacheMisses += right.SegmentFileCacheMisses
	left.TypedColumnPreparePlanNanos += right.TypedColumnPreparePlanNanos
	left.TypedColumnPrepareRefsNanos += right.TypedColumnPrepareRefsNanos
	left.TypedColumnPreparePairingNanos += right.TypedColumnPreparePairingNanos
	left.TypedColumnPreparePartDecodeNanos += right.TypedColumnPreparePartDecodeNanos
	left.TypedColumnPreparePostPrepareNanos += right.TypedColumnPreparePostPrepareNanos
	left.TypedColumnPrepareSummaryNanos += right.TypedColumnPrepareSummaryNanos
	left.TypedColumnOneShotCacheStoreNanos += right.TypedColumnOneShotCacheStoreNanos
	left.TypedColumnPrepareReadImageNanos += right.TypedColumnPrepareReadImageNanos
	left.TypedColumnPrepareStateBuildNanos += right.TypedColumnPrepareStateBuildNanos
	left.TypedColumnPrepareDictionaryNanos += right.TypedColumnPrepareDictionaryNanos
	left.TypedColumnPreparePruningNanos += right.TypedColumnPreparePruningNanos
	left.TypedColumnPrepareSortKeyNanos += right.TypedColumnPrepareSortKeyNanos
	left.TypedColumnPrepareStatsNanos += right.TypedColumnPrepareStatsNanos
	left.TypedColumnPrepareRangeReadNanos += right.TypedColumnPrepareRangeReadNanos
	left.TypedColumnPrepareRangeReadBytes += right.TypedColumnPrepareRangeReadBytes
	left.TypedColumnPrepareAdapterNanos += right.TypedColumnPrepareAdapterNanos
	left.TypedColumnPrepareDenseGroupNanos += right.TypedColumnPrepareDenseGroupNanos
	left.TypedColumnPrepareDenseValueNanos += right.TypedColumnPrepareDenseValueNanos
	left.TypedColumnPrepareDensePredicateNanos += right.TypedColumnPrepareDensePredicateNanos
	left.TypedColumnPrepareDensePreapplyNanos += right.TypedColumnPrepareDensePreapplyNanos
	left.TypedColumnPrepareQ2GroupRankNanos += right.TypedColumnPrepareQ2GroupRankNanos
	left.TypedColumnPrepareQ2DistinctRankNanos += right.TypedColumnPrepareQ2DistinctRankNanos
	left.TypedColumnPrepareQ2LocalRankNanos += right.TypedColumnPrepareQ2LocalRankNanos
	left.TypedColumnPrepareQ2DenseGroupGlobalRankNanos += right.TypedColumnPrepareQ2DenseGroupGlobalRankNanos
	left.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos += right.TypedColumnPrepareQ2DenseDistinctGlobalRankNanos
	left.TypedColumnPrepareQ2DensePartLocalRankNanos += right.TypedColumnPrepareQ2DensePartLocalRankNanos
	left.TypedColumnPrepareQ2DenseDistinctRankPlanNanos += right.TypedColumnPrepareQ2DenseDistinctRankPlanNanos
	left.TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos += right.TypedColumnPrepareQ2DenseDistinctRankCollectRefsNanos
	left.TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos += right.TypedColumnPrepareQ2DenseDistinctRankBuildShardsNanos
	if right.TypedColumnPrepareQ2DenseDistinctRankShardCount > left.TypedColumnPrepareQ2DenseDistinctRankShardCount {
		left.TypedColumnPrepareQ2DenseDistinctRankShardCount = right.TypedColumnPrepareQ2DenseDistinctRankShardCount
	}
	left.TypedColumnPrepareQ2DenseDistinctRankRefs += right.TypedColumnPrepareQ2DenseDistinctRankRefs
	if right.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs > left.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs {
		left.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs = right.TypedColumnPrepareQ2DenseDistinctRankMaxShardRefs
	}
	if right.TypedColumnPrepareQ2DenseDistinctGlobalRanks > left.TypedColumnPrepareQ2DenseDistinctGlobalRanks {
		left.TypedColumnPrepareQ2DenseDistinctGlobalRanks = right.TypedColumnPrepareQ2DenseDistinctGlobalRanks
	}
	left.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos += right.TypedColumnPrepareQ2GroupGlobalDictionaryRankNanos
	left.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos += right.TypedColumnPrepareQ2DistinctGlobalDictionaryRankNanos
	left.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos += right.TypedColumnPrepareQ2GroupGlobalCodeRemapNanos
	left.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos += right.TypedColumnPrepareQ2DistinctGlobalCodeRemapNanos
	left.QueryReadyEncodedExecutions += right.QueryReadyEncodedExecutions
	if right.QueryReadyPreparedParts > left.QueryReadyPreparedParts {
		left.QueryReadyPreparedParts = right.QueryReadyPreparedParts
	}
	if right.QueryReadyBaseParts > left.QueryReadyBaseParts {
		left.QueryReadyBaseParts = right.QueryReadyBaseParts
	}
	if right.QueryReadyDeltaParts > left.QueryReadyDeltaParts {
		left.QueryReadyDeltaParts = right.QueryReadyDeltaParts
	}
	left.QueryReadyRowsCandidate += right.QueryReadyRowsCandidate
	left.QueryReadyRowsVisible += right.QueryReadyRowsVisible
	left.QueryReadyRowsSuperseded += right.QueryReadyRowsSuperseded
	left.QueryReadyCodeTranslations += right.QueryReadyCodeTranslations
	if right.QueryReadyDictionaryDomains > left.QueryReadyDictionaryDomains {
		left.QueryReadyDictionaryDomains = right.QueryReadyDictionaryDomains
	}
	if right.QueryReadyScratchBytes > left.QueryReadyScratchBytes {
		left.QueryReadyScratchBytes = right.QueryReadyScratchBytes
	}
	left.QueryReadyPreparationNanos += right.QueryReadyPreparationNanos
	left.QueryReadyBaseScanNanos += right.QueryReadyBaseScanNanos
	left.QueryReadyDeltaMergeNanos += right.QueryReadyDeltaMergeNanos
	left.QueryReadyPredicateNanos += right.QueryReadyPredicateNanos
	left.QueryReadyReductionNanos += right.QueryReadyReductionNanos
	left.QueryReadyFusedPredicateReductionExecutions += right.QueryReadyFusedPredicateReductionExecutions
	left.QueryReadyFusedPredicateReductionWorkers = max(left.QueryReadyFusedPredicateReductionWorkers, right.QueryReadyFusedPredicateReductionWorkers)
	left.QueryReadyFusedPredicateReductionNanos += right.QueryReadyFusedPredicateReductionNanos
	left.QueryReadyGroupingNanos += right.QueryReadyGroupingNanos
	left.QueryReadyOrderingTopKNanos += right.QueryReadyOrderingTopKNanos
	left.QueryReadyLegacyFallbacks += right.QueryReadyLegacyFallbacks
	left.QueryReadyPrecomputedAnswers += right.QueryReadyPrecomputedAnswers
	return left
}

func mergeColumnPhysicalQueryStorageSource(left, right ColumnPhysicalQueryStorageSource) ColumnPhysicalQueryStorageSource {
	if left == "" {
		return right
	}
	if right == "" || right == left {
		return left
	}
	return ColumnPhysicalQueryStorageSourceMixed
}

func mergeColumnPhysicalQueryReducerLabel(left, right string) string {
	if left == "" {
		return right
	}
	if right == "" || right == left {
		return left
	}
	return "mixed"
}

func mergeColumnPhysicalQueryFallbackReason(left, right ColumnPhysicalQueryFallbackReason) ColumnPhysicalQueryFallbackReason {
	if left == "" {
		if right == "" {
			return ColumnPhysicalQueryFallbackNone
		}
		return right
	}
	if right == "" || right == left || right == ColumnPhysicalQueryFallbackNone {
		return left
	}
	if left == ColumnPhysicalQueryFallbackNone {
		return right
	}
	return ColumnPhysicalQueryFallbackMixed
}

type columnPhysicalQueryExecutor struct {
	kind              ColumnPhysicalQueryKind
	readIntegrity     ColumnAssetReadIntegrity
	topK              int
	valueColumn       string
	projected         []string
	groupIdx          int
	valueIdx          int
	distinctIdx       int
	groupColumnIdx    int
	valueColumnIdx    int
	distinctColumnIdx int
	interner          columnPhysicalQueryStringInterner

	counts       map[string]int
	distinct     map[string]map[string]struct{}
	distinctPool []map[string]struct{}
	hourCounts   [24]int
	int64Values  map[string]int64
	int64Spans   map[string]columnPhysicalQuerySpan
	int64Sum     int64
	int64SumRows int
	reduceRows   int
	resultGroups []ColumnPhysicalQueryGroup
}

type columnPhysicalQuerySpan struct {
	min int64
	max int64
}

func newColumnPhysicalQueryExecutor(cfg ColumnStoreConfig, req ColumnPhysicalQueryRequest) (*columnPhysicalQueryExecutor, error) {
	if columnPhysicalQueryHasPredicates(req) {
		return nil, fmt.Errorf("%w: physical predicates require dictionary sidecar execution", ErrColumnQueryPlanUnsupported)
	}
	exec := &columnPhysicalQueryExecutor{
		kind:              req.Kind,
		readIntegrity:     req.ColumnAssetReadIntegrity,
		topK:              req.TopK,
		valueColumn:       req.ValueColumn,
		groupIdx:          -1,
		valueIdx:          -1,
		distinctIdx:       -1,
		groupColumnIdx:    -1,
		valueColumnIdx:    -1,
		distinctColumnIdx: -1,
	}
	addProjection := func(name string, wantType ColumnStoreValueType, role string) (int, int, error) {
		if name == "" {
			return -1, -1, fmt.Errorf("%w: physical column query %s column is required", ErrColumnQueryPlanUnsupported, role)
		}
		col, columnIdx, ok := columnPhysicalQueryDeclaredColumn(cfg, name)
		if !ok {
			return -1, -1, fmt.Errorf("%w: physical column query requested undeclared column %q", ErrColumnQueryPlanUnsupported, name)
		}
		if col.ValueType != wantType {
			return -1, -1, fmt.Errorf("%w: physical column query %s column %q has type %q, want %q", ErrColumnQueryPlanUnsupported, role, name, col.ValueType, wantType)
		}
		for idx, existing := range exec.projected {
			if existing == name {
				return idx, columnIdx, nil
			}
		}
		exec.projected = append(exec.projected, name)
		return len(exec.projected) - 1, columnIdx, nil
	}

	var err error
	switch req.Kind {
	case ColumnPhysicalQueryGroupCount:
		exec.groupIdx, exec.groupColumnIdx, err = addProjection(req.GroupColumn, ColumnStoreValueString, "group")
		exec.counts = make(map[string]int)
	case ColumnPhysicalQueryGroupCountDistinct:
		exec.groupIdx, exec.groupColumnIdx, err = addProjection(req.GroupColumn, ColumnStoreValueString, "group")
		if err == nil {
			exec.distinctIdx, exec.distinctColumnIdx, err = addProjection(req.DistinctColumn, ColumnStoreValueString, "distinct")
		}
		exec.distinct = make(map[string]map[string]struct{})
	case ColumnPhysicalQueryHourCount:
		exec.valueIdx, exec.valueColumnIdx, err = addProjection(req.ValueColumn, ColumnStoreValueInt64, "value")
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64:
		exec.groupIdx, exec.groupColumnIdx, err = addProjection(req.GroupColumn, ColumnStoreValueString, "group")
		if err == nil {
			exec.valueIdx, exec.valueColumnIdx, err = addProjection(req.ValueColumn, ColumnStoreValueInt64, "value")
		}
		exec.int64Values = make(map[string]int64)
	case ColumnPhysicalQueryGroupInt64Span:
		exec.groupIdx, exec.groupColumnIdx, err = addProjection(req.GroupColumn, ColumnStoreValueString, "group")
		if err == nil {
			exec.valueIdx, exec.valueColumnIdx, err = addProjection(req.ValueColumn, ColumnStoreValueInt64, "value")
		}
		exec.int64Spans = make(map[string]columnPhysicalQuerySpan)
	case ColumnPhysicalQuerySumSecondOfDaySquare:
		exec.valueIdx, exec.valueColumnIdx, err = addProjection(req.ValueColumn, ColumnStoreValueInt64, "value")
	default:
		err = fmt.Errorf("%w: unsupported physical column query kind %q", ErrColumnQueryPlanUnsupported, req.Kind)
	}
	if err != nil {
		return nil, err
	}
	return exec, nil
}

func columnPhysicalQueryDeclaredColumn(cfg ColumnStoreConfig, name string) (ColumnStoreColumn, int, bool) {
	for idx, col := range cfg.Columns {
		if col.Name == name {
			return col, idx, true
		}
	}
	return ColumnStoreColumn{}, -1, false
}

func columnPhysicalQueryAggregateMetadataConfig(cfg ColumnStoreConfig, req ColumnPhysicalQueryRequest) (ColumnAggregateMetadata, bool) {
	name := req.AggregateMetadataName
	if name == "" {
		return ColumnAggregateMetadata{}, false
	}
	for _, aggregate := range cfg.AggregateMetadata {
		if aggregate.Name != name {
			continue
		}
		if aggregate.GroupColumn != req.GroupColumn || aggregate.Column != req.ValueColumn {
			return ColumnAggregateMetadata{}, false
		}
		predicateCoverage, err := columnAggregateMetadataCanonicalPredicates(cfg, aggregate.Predicates)
		if err != nil {
			return ColumnAggregateMetadata{}, false
		}
		reqPredicateCoverage, err := columnAggregateMetadataCanonicalPredicates(cfg, req.Predicates)
		if err != nil || !columnPhysicalQueryPredicatesExactEqual(predicateCoverage, reqPredicateCoverage) {
			return ColumnAggregateMetadata{}, false
		}
		aggregate.Predicates = predicateCoverage
		switch req.Kind {
		case ColumnPhysicalQueryGroupCount:
			return aggregate, aggregate.Kind == ColumnAggregateCount
		case ColumnPhysicalQueryGroupHourCount:
			return aggregate, aggregate.Kind == ColumnAggregateGroupHourCount
		case ColumnPhysicalQueryGroupMinInt64:
			return aggregate, aggregate.Kind == ColumnAggregateMin
		case ColumnPhysicalQueryGroupMaxInt64:
			return aggregate, aggregate.Kind == ColumnAggregateMax
		case ColumnPhysicalQueryGroupInt64Span:
			return aggregate, aggregate.Kind == ColumnAggregateMin || aggregate.Kind == ColumnAggregateMax
		default:
			return ColumnAggregateMetadata{}, false
		}
	}
	return ColumnAggregateMetadata{}, false
}

func columnPhysicalQueryAggregateMetadataRefs(refs []columnManifestAggregateMetadataSnapshot, name string) []columnManifestAggregateMetadataSnapshot {
	var out []columnManifestAggregateMetadataSnapshot
	for _, ref := range refs {
		if ref.Name == name {
			out = append(out, ref)
		}
	}
	return out
}

func (e *columnPhysicalQueryExecutor) supportsDirectAssetReduce() bool {
	if e == nil {
		return false
	}
	switch e.kind {
	case ColumnPhysicalQueryGroupCount:
		return e.groupColumnIdx >= 0
	case ColumnPhysicalQueryGroupCountDistinct:
		return e.groupColumnIdx >= 0 && e.distinctColumnIdx >= 0
	case ColumnPhysicalQueryHourCount:
		return e.valueColumnIdx >= 0
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64, ColumnPhysicalQueryGroupInt64Span:
		return e.groupColumnIdx >= 0 && e.valueColumnIdx >= 0
	case ColumnPhysicalQuerySumSecondOfDaySquare:
		return e.valueColumnIdx >= 0
	default:
		return false
	}
}

func (e *columnPhysicalQueryExecutor) resetForRun() {
	if e == nil {
		return
	}
	e.reduceRows = 0
	e.resultGroups = e.resultGroups[:0]
	for idx := range e.hourCounts {
		e.hourCounts[idx] = 0
	}
	for key := range e.counts {
		delete(e.counts, key)
	}
	e.distinctPool = e.distinctPool[:0]
	for key, set := range e.distinct {
		for value := range set {
			delete(set, value)
		}
		e.distinctPool = append(e.distinctPool, set)
		delete(e.distinct, key)
	}
	for key := range e.int64Values {
		delete(e.int64Values, key)
	}
	for key := range e.int64Spans {
		delete(e.int64Spans, key)
	}
	e.int64Sum = 0
	e.int64SumRows = 0
}

func (e *columnPhysicalQueryExecutor) visit(row columnPhysicalScanRowView) error {
	if row.Deleted || row.Operation != ColumnPublishOperationInsert {
		return fmt.Errorf("%w: operation=%s deleted=%v", errColumnPhysicalQueryNeedsVisibility, row.Operation, row.Deleted)
	}
	return e.visitValues(row.Values)
}

func (e *columnPhysicalQueryExecutor) visitValues(values []columnDeclaredValue) error {
	e.reduceRows++
	switch e.kind {
	case ColumnPhysicalQueryGroupCount:
		key, err := e.stringKey(values[e.groupIdx])
		if err != nil {
			return err
		}
		e.counts[key]++
	case ColumnPhysicalQueryGroupCountDistinct:
		key, err := e.stringKey(values[e.groupIdx])
		if err != nil {
			return err
		}
		distinct, err := e.stringKey(values[e.distinctIdx])
		if err != nil {
			return err
		}
		set := e.distinctSetForGroup(key)
		set[distinct] = struct{}{}
	case ColumnPhysicalQueryHourCount:
		value, err := columnPhysicalQueryInt64Value(values[e.valueIdx])
		if err != nil {
			return err
		}
		e.hourCounts[columnPhysicalQueryUTCHour(value)]++
	case ColumnPhysicalQueryGroupMinInt64:
		key, value, err := e.stringInt64Values(values)
		if err != nil {
			return err
		}
		if cur, ok := e.int64Values[key]; !ok || value < cur {
			e.int64Values[key] = value
		}
	case ColumnPhysicalQueryGroupMaxInt64:
		key, value, err := e.stringInt64Values(values)
		if err != nil {
			return err
		}
		if cur, ok := e.int64Values[key]; !ok || value > cur {
			e.int64Values[key] = value
		}
	case ColumnPhysicalQueryGroupInt64Span:
		key, value, err := e.stringInt64Values(values)
		if err != nil {
			return err
		}
		cur, ok := e.int64Spans[key]
		if !ok {
			e.int64Spans[key] = columnPhysicalQuerySpan{min: value, max: value}
			return nil
		}
		if value < cur.min {
			cur.min = value
		}
		if value > cur.max {
			cur.max = value
		}
		e.int64Spans[key] = cur
	case ColumnPhysicalQuerySumSecondOfDaySquare:
		value, err := columnPhysicalQueryInt64Value(values[e.valueIdx])
		if err != nil {
			return err
		}
		return e.addSumSecondOfDaySquareValue(value)
	}
	return nil
}

func (e *columnPhysicalQueryExecutor) distinctSetForGroup(key string) map[string]struct{} {
	set := e.distinct[key]
	if set != nil {
		return set
	}
	if n := len(e.distinctPool); n > 0 {
		set = e.distinctPool[n-1]
		e.distinctPool = e.distinctPool[:n-1]
	} else {
		set = make(map[string]struct{})
	}
	e.distinct[key] = set
	return set
}

func reduceColumnPhysicalAssetDirect(raw []byte, ref ColumnAssetRef, expectedCollection string, cfg *ColumnStoreConfig, expectedOperation ColumnPublishOperation, exec *columnPhysicalQueryExecutor) (columnPhysicalAssetScanSummary, error) {
	header, version, rowsOffset, err := parseColumnPhysicalAssetScanHeader(raw, ref, expectedCollection, cfg, expectedOperation)
	if err != nil {
		return columnPhysicalAssetScanSummary{}, err
	}
	if header.Operation != ColumnPublishOperationInsert {
		return columnPhysicalAssetScanSummary{}, errColumnPhysicalQueryNeedsVisibility
	}
	cur := manifestCursor{raw: raw, pos: rowsOffset}
	var summary columnPhysicalAssetScanSummary
	for rowIdx := 0; rowIdx < header.RowCount; rowIdx++ {
		_ = cur.bytesView()
		deleted := false
		if version >= columnPhysicalAssetVersionV2 {
			deleted = cur.bool()
		}
		if cur.err != nil {
			return columnPhysicalAssetScanSummary{}, cur.err
		}
		if deleted {
			return columnPhysicalAssetScanSummary{}, fmt.Errorf("%w: operation=%s deleted=%v", errColumnPhysicalQueryNeedsVisibility, header.Operation, deleted)
		}
		group, groupOK, distinct, distinctOK, value, valueOK, err := scanColumnPhysicalDirectQueryRowValues(&cur, version, cfg, exec)
		if err != nil {
			return columnPhysicalAssetScanSummary{}, fmt.Errorf("row[%d]: %w", rowIdx, err)
		}
		switch exec.kind {
		case ColumnPhysicalQueryGroupCount:
			if err := exec.visitDirectGroupCount(group, groupOK); err != nil {
				return columnPhysicalAssetScanSummary{}, err
			}
		case ColumnPhysicalQueryGroupCountDistinct:
			if err := exec.visitDirectGroupCountDistinct(group, groupOK, distinct, distinctOK); err != nil {
				return columnPhysicalAssetScanSummary{}, err
			}
		case ColumnPhysicalQueryHourCount:
			if err := exec.visitDirectHourCount(value, valueOK); err != nil {
				return columnPhysicalAssetScanSummary{}, err
			}
		case ColumnPhysicalQueryGroupMinInt64:
			if err := exec.visitDirectGroupMinInt64(group, groupOK, value, valueOK); err != nil {
				return columnPhysicalAssetScanSummary{}, err
			}
		case ColumnPhysicalQueryGroupMaxInt64:
			if err := exec.visitDirectGroupMaxInt64(group, groupOK, value, valueOK); err != nil {
				return columnPhysicalAssetScanSummary{}, err
			}
		case ColumnPhysicalQueryGroupInt64Span:
			if err := exec.visitDirectGroupInt64Span(group, groupOK, value, valueOK); err != nil {
				return columnPhysicalAssetScanSummary{}, err
			}
		case ColumnPhysicalQuerySumSecondOfDaySquare:
			if err := exec.visitDirectSumSecondOfDaySquare(value, valueOK); err != nil {
				return columnPhysicalAssetScanSummary{}, err
			}
		default:
			return columnPhysicalAssetScanSummary{}, fmt.Errorf("%w: unsupported direct physical column query kind %q", ErrColumnQueryPlanUnsupported, exec.kind)
		}
		summary.rows++
	}
	if cur.err != nil {
		return columnPhysicalAssetScanSummary{}, cur.err
	}
	if cur.pos != len(raw) {
		return columnPhysicalAssetScanSummary{}, errors.New("trailing bytes in column physical asset")
	}
	return summary, nil
}

func scanColumnPhysicalDirectQueryRowValues(cur *manifestCursor, version uint16, cfg *ColumnStoreConfig, exec *columnPhysicalQueryExecutor) ([]byte, bool, []byte, bool, int64, bool, error) {
	var group []byte
	var groupOK bool
	var distinct []byte
	var distinctOK bool
	var value int64
	var valueOK bool
	for colIdx, col := range cfg.Columns {
		selectedGroup := colIdx == exec.groupColumnIdx
		selectedValue := colIdx == exec.valueColumnIdx
		selectedDistinct := colIdx == exec.distinctColumnIdx
		if exec.readIntegrity != ColumnAssetReadIntegritySkipChecksums {
			typeBytes := cur.stringBytes()
			if cur.err != nil {
				return nil, false, nil, false, 0, false, cur.err
			}
			if !columnPhysicalBytesEqualString(typeBytes, string(col.ValueType)) {
				return nil, false, nil, false, 0, false, fmt.Errorf("column[%d] type=%q want %q", colIdx, string(typeBytes), col.ValueType)
			}
		} else {
			// Unsafe checksum-skipping reads also skip redundant per-value type
			// tags and rely on the already-validated asset header/schema.
			cur.skipStringBytes()
			if cur.err != nil {
				return nil, false, nil, false, 0, false, cur.err
			}
		}
		null := cur.bool()
		if cur.err != nil {
			return nil, false, nil, false, 0, false, cur.err
		}
		present := true
		if version >= columnPhysicalAssetVersionV3 {
			present = cur.bool()
			if cur.err != nil {
				return nil, false, nil, false, 0, false, cur.err
			}
		}
		if !present {
			if !null {
				return nil, false, nil, false, 0, false, fmt.Errorf("column[%d] absent value is not null", colIdx)
			}
			if !col.Nullable {
				return nil, false, nil, false, 0, false, fmt.Errorf("column[%d] is absent but column is not nullable", colIdx)
			}
			if selectedGroup || selectedValue || selectedDistinct {
				return nil, false, nil, false, 0, false, columnPhysicalQueryNullDirectError(col.ValueType)
			}
			continue
		}
		if null {
			if !col.Nullable {
				return nil, false, nil, false, 0, false, fmt.Errorf("column[%d] is null but column is not nullable", colIdx)
			}
			if selectedGroup || selectedValue || selectedDistinct {
				return nil, false, nil, false, 0, false, columnPhysicalQueryNullDirectError(col.ValueType)
			}
			continue
		}
		switch col.ValueType {
		case ColumnStoreValueBool:
			// Bool is not a supported direct query group/value/distinct type.
			_ = cur.bool()
		case ColumnStoreValueInt64:
			if selectedValue {
				v := int64(cur.u64())
				value = v
				valueOK = true
			} else {
				cur.skip(8)
			}
		case ColumnStoreValueDouble:
			cur.skip(8)
		case ColumnStoreValueFloat32:
			cur.skip(4)
		case ColumnStoreValueInt8, ColumnStoreValueUint8, ColumnStoreValueInt16, ColumnStoreValueUint16, ColumnStoreValueInt32, ColumnStoreValueUint32, ColumnStoreValueUint64, ColumnStoreValueFloat16, ColumnStoreValueBFloat16:
			if selectedGroup || selectedValue || selectedDistinct {
				return nil, false, nil, false, 0, false, fmt.Errorf("unsupported column physical value type %q", col.ValueType)
			}
			width, ok := columnStorePrimitiveScalarWidth(col.ValueType)
			if !ok {
				return nil, false, nil, false, 0, false, fmt.Errorf("unsupported column physical value type %q", col.ValueType)
			}
			cur.skip(uint64(width))
		case ColumnStoreValueString:
			if selectedGroup || selectedDistinct {
				v := cur.stringBytes()
				if selectedGroup {
					group = v
					groupOK = true
				}
				if selectedDistinct {
					distinct = v
					distinctOK = true
				}
			} else {
				cur.skipStringBytes()
			}
		case ColumnStoreValueFloat32Vector:
			n := cur.skipUint32Slice()
			if cur.err != nil {
				return nil, false, nil, false, 0, false, cur.err
			}
			dims := columnStoreFloat32VectorElementsPerRow(col)
			if n != uint64(dims) {
				return nil, false, nil, false, 0, false, fmt.Errorf("column[%d] float32_vector length=%d want vector_dims=%d", colIdx, n, dims)
			}
		case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
			cur.skipDenseNumericVectorBytesWithExpectedLength(col)
		case ColumnStoreValueUint32List:
			cur.skipUint32Slice()
		case ColumnStoreValueAdjacencyList:
			cur.skipUint32Slice()
		default:
			return nil, false, nil, false, 0, false, fmt.Errorf("unsupported column physical value type %q", col.ValueType)
		}
		if cur.err != nil {
			return nil, false, nil, false, 0, false, cur.err
		}
	}
	return group, groupOK, distinct, distinctOK, value, valueOK, nil
}

func columnPhysicalQueryNullDirectError(valueType ColumnStoreValueType) error {
	switch valueType {
	case ColumnStoreValueString:
		return fmt.Errorf("%w: physical column query does not support null string values yet", ErrColumnQueryPlanUnsupported)
	case ColumnStoreValueInt64:
		return fmt.Errorf("%w: physical column query does not support null int64 values yet", ErrColumnQueryPlanUnsupported)
	default:
		return fmt.Errorf("%w: physical column query does not support null %s values yet", ErrColumnQueryPlanUnsupported, valueType)
	}
}

func (e *columnPhysicalQueryExecutor) visitDirectGroupCount(group []byte, groupOK bool) error {
	if !groupOK {
		return fmt.Errorf("%w: physical column query missing string group value", ErrColumnQueryPlanUnsupported)
	}
	key := e.interner.internBytes(group)
	e.counts[key]++
	e.reduceRows++
	return nil
}

func (e *columnPhysicalQueryExecutor) visitDirectGroupCountDistinct(group []byte, groupOK bool, distinct []byte, distinctOK bool) error {
	if !groupOK {
		return fmt.Errorf("%w: physical column query missing string group value", ErrColumnQueryPlanUnsupported)
	}
	if !distinctOK {
		return fmt.Errorf("%w: physical column query missing string distinct value", ErrColumnQueryPlanUnsupported)
	}
	key := e.interner.internBytes(group)
	distinctKey := e.interner.internBytes(distinct)
	set := e.distinctSetForGroup(key)
	set[distinctKey] = struct{}{}
	e.reduceRows++
	return nil
}

func (e *columnPhysicalQueryExecutor) visitDirectHourCount(value int64, valueOK bool) error {
	if !valueOK {
		return fmt.Errorf("%w: physical column query missing int64 value", ErrColumnQueryPlanUnsupported)
	}
	e.hourCounts[columnPhysicalQueryUTCHour(value)]++
	e.reduceRows++
	return nil
}

func (e *columnPhysicalQueryExecutor) visitDirectGroupMinInt64(group []byte, groupOK bool, value int64, valueOK bool) error {
	if !groupOK {
		return fmt.Errorf("%w: physical column query missing string group value", ErrColumnQueryPlanUnsupported)
	}
	if !valueOK {
		return fmt.Errorf("%w: physical column query missing int64 value", ErrColumnQueryPlanUnsupported)
	}
	key := e.interner.internBytes(group)
	if cur, ok := e.int64Values[key]; !ok || value < cur {
		e.int64Values[key] = value
	}
	e.reduceRows++
	return nil
}

func (e *columnPhysicalQueryExecutor) visitDirectGroupMaxInt64(group []byte, groupOK bool, value int64, valueOK bool) error {
	if !groupOK {
		return fmt.Errorf("%w: physical column query missing string group value", ErrColumnQueryPlanUnsupported)
	}
	if !valueOK {
		return fmt.Errorf("%w: physical column query missing int64 value", ErrColumnQueryPlanUnsupported)
	}
	key := e.interner.internBytes(group)
	if cur, ok := e.int64Values[key]; !ok || value > cur {
		e.int64Values[key] = value
	}
	e.reduceRows++
	return nil
}

func (e *columnPhysicalQueryExecutor) visitDirectGroupInt64Span(group []byte, groupOK bool, value int64, valueOK bool) error {
	if !groupOK {
		return fmt.Errorf("%w: physical column query missing string group value", ErrColumnQueryPlanUnsupported)
	}
	if !valueOK {
		return fmt.Errorf("%w: physical column query missing int64 value", ErrColumnQueryPlanUnsupported)
	}
	key := e.interner.internBytes(group)
	cur, ok := e.int64Spans[key]
	if !ok {
		e.int64Spans[key] = columnPhysicalQuerySpan{min: value, max: value}
		e.reduceRows++
		return nil
	}
	if value < cur.min {
		cur.min = value
	}
	if value > cur.max {
		cur.max = value
	}
	e.int64Spans[key] = cur
	e.reduceRows++
	return nil
}

func (e *columnPhysicalQueryExecutor) visitDirectSumSecondOfDaySquare(value int64, valueOK bool) error {
	if !valueOK {
		return fmt.Errorf("%w: physical column query missing int64 value", ErrColumnQueryPlanUnsupported)
	}
	if err := e.addSumSecondOfDaySquareValue(value); err != nil {
		return err
	}
	e.reduceRows++
	return nil
}

func (e *columnPhysicalQueryExecutor) addSumSecondOfDaySquareValue(value int64) error {
	result := TypedColumnInt64PredicateAggregateResult{Sum: e.int64Sum}
	if err := addTypedColumnInt64PredicateAggregateSecondOfDaySquareValue(&result, value); err != nil {
		return err
	}
	e.int64Sum = result.Sum
	e.int64SumRows++
	return nil
}

func (e *columnPhysicalQueryExecutor) addTransformedInt64Sum(value int64) error {
	result := TypedColumnInt64PredicateAggregateResult{Sum: e.int64Sum}
	if err := addTypedColumnInt64PredicateAggregateValue(&result, value); err != nil {
		return err
	}
	e.int64Sum = result.Sum
	return nil
}

func (e *columnPhysicalQueryExecutor) stringInt64Values(values []columnDeclaredValue) (string, int64, error) {
	key, err := e.stringKey(values[e.groupIdx])
	if err != nil {
		return "", 0, err
	}
	value, err := columnPhysicalQueryInt64Value(values[e.valueIdx])
	if err != nil {
		return "", 0, err
	}
	return key, value, nil
}

func (e *columnPhysicalQueryExecutor) stringKey(value columnDeclaredValue) (string, error) {
	if value.Type != ColumnStoreValueString {
		return "", fmt.Errorf("%w: physical column query expected string value, got %q", ErrColumnQueryPlanUnsupported, value.Type)
	}
	if value.Null {
		return "", fmt.Errorf("%w: physical column query does not support null string group values yet", ErrColumnQueryPlanUnsupported)
	}
	if value.StringBytes != nil {
		return e.interner.internBytes(value.StringBytes), nil
	}
	return e.interner.internString(value.String), nil
}

func (e *columnPhysicalQueryExecutor) groups() []ColumnPhysicalQueryGroup {
	e.resultGroups = e.resultGroups[:0]
	switch e.kind {
	case ColumnPhysicalQueryGroupCount:
		for key, count := range e.counts {
			e.resultGroups = append(e.resultGroups, ColumnPhysicalQueryGroup{Key: key, Count: count})
		}
	case ColumnPhysicalQueryGroupCountDistinct:
		for key, set := range e.distinct {
			e.resultGroups = append(e.resultGroups, ColumnPhysicalQueryGroup{Key: key, Count: len(set)})
		}
	case ColumnPhysicalQueryHourCount:
		for hour, count := range e.hourCounts {
			if count == 0 {
				continue
			}
			e.resultGroups = append(e.resultGroups, ColumnPhysicalQueryGroup{Key: columnPhysicalQueryHourKey(hour), Count: count})
		}
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64:
		for key, value := range e.int64Values {
			e.resultGroups = append(e.resultGroups, ColumnPhysicalQueryGroup{Key: key, Int64: value})
		}
	case ColumnPhysicalQueryGroupInt64Span:
		for key, span := range e.int64Spans {
			e.resultGroups = append(e.resultGroups, ColumnPhysicalQueryGroup{Key: key, Int64: span.max - span.min})
		}
	case ColumnPhysicalQuerySumSecondOfDaySquare:
		if e.int64SumRows > 0 {
			e.resultGroups = append(e.resultGroups, ColumnPhysicalQueryGroup{Key: columnPhysicalQuerySumSecondOfDaySquareKey(e.valueColumn), Count: e.int64SumRows, Int64: e.int64Sum})
		}
	}
	if e.topK == 0 {
		sortColumnPhysicalQueryGroupsByKey(e.resultGroups)
	}
	return e.resultGroups
}

func columnPhysicalQuerySumSecondOfDaySquareKey(valueColumn string) string {
	return valueColumn + "_second_of_day_square"
}

func finalizeColumnPhysicalQueryResultGroups(req ColumnPhysicalQueryRequest, result *ColumnPhysicalQueryResult) {
	if result == nil {
		return
	}
	if req.TopK > 0 {
		shapeStart := time.Now()
		candidates := columnPhysicalQueryTopKCandidateGroups(req, result.Groups)
		result.Groups = applyColumnPhysicalQueryTopKGroups(req, result.Groups)
		result.Diagnostics.ResultShapeNanos += time.Since(shapeStart).Nanoseconds()
		result.Diagnostics.TopKLimit = req.TopK
		result.Diagnostics.TopKOrder = string(req.TopKOrder)
		result.Diagnostics.TopKCandidates = candidates
	}
	result.Diagnostics.ResultGroups = len(result.Groups)
}

func columnPhysicalQueryTopKCandidateGroups(req ColumnPhysicalQueryRequest, groups []ColumnPhysicalQueryGroup) int {
	if req.TopK <= 0 {
		return 0
	}
	if !req.SkipEmptyGroupKey {
		return len(groups)
	}
	candidates := 0
	for _, group := range groups {
		if group.Key != "" {
			candidates++
		}
	}
	return candidates
}

func applyColumnPhysicalQueryTopKGroups(req ColumnPhysicalQueryRequest, groups []ColumnPhysicalQueryGroup) []ColumnPhysicalQueryGroup {
	if req.TopK <= 0 {
		return groups
	}
	out := groups[:0]
	for idx := 0; idx < len(groups); idx++ {
		group := groups[idx]
		if req.SkipEmptyGroupKey && group.Key == "" {
			continue
		}
		insertColumnPhysicalTopKGroup(&out, group, req.TopK, req.TopKOrder)
	}
	return out
}

func sortColumnPhysicalQueryGroupsByKey(groups []ColumnPhysicalQueryGroup) {
	const insertionSortLimit = 64
	if len(groups) > insertionSortLimit {
		slices.SortFunc(groups, func(a, b ColumnPhysicalQueryGroup) int {
			return cmp.Compare(a.Key, b.Key)
		})
		return
	}
	for i := 1; i < len(groups); i++ {
		group := groups[i]
		j := i - 1
		for ; j >= 0 && group.Key < groups[j].Key; j-- {
			groups[j+1] = groups[j]
		}
		groups[j+1] = group
	}
}

type columnPhysicalQueryMetadataAccumulator struct {
	kind        ColumnPhysicalQueryKind
	counts      map[string]int
	hourCounts  map[columnPhysicalQueryMetadataHourKey]int
	int64Values map[string]int64
	spans       map[string]columnPhysicalQuerySpan
	rows        int
	entries     int
}

type columnPhysicalQueryMetadataHourKey struct {
	group string
	hour  int
}

func newColumnPhysicalQueryMetadataAccumulator(kind ColumnPhysicalQueryKind) *columnPhysicalQueryMetadataAccumulator {
	acc := &columnPhysicalQueryMetadataAccumulator{kind: kind}
	switch kind {
	case ColumnPhysicalQueryGroupCount:
		acc.counts = make(map[string]int)
	case ColumnPhysicalQueryGroupHourCount:
		acc.hourCounts = make(map[columnPhysicalQueryMetadataHourKey]int)
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64:
		acc.int64Values = make(map[string]int64)
	case ColumnPhysicalQueryGroupInt64Span:
		acc.spans = make(map[string]columnPhysicalQuerySpan)
	}
	return acc
}

func (a *columnPhysicalQueryMetadataAccumulator) add(entries []columnAggregateMetadataEntry) {
	for _, entry := range entries {
		a.entries++
		a.rows += entry.Count
		switch a.kind {
		case ColumnPhysicalQueryGroupCount:
			a.counts[entry.Group] += entry.Count
		case ColumnPhysicalQueryGroupHourCount:
			a.hourCounts[columnPhysicalQueryMetadataHourKey{group: entry.Group, hour: entry.Hour}] += entry.Count
		case ColumnPhysicalQueryGroupMinInt64:
			if cur, ok := a.int64Values[entry.Group]; !ok || entry.Min < cur {
				a.int64Values[entry.Group] = entry.Min
			}
		case ColumnPhysicalQueryGroupMaxInt64:
			if cur, ok := a.int64Values[entry.Group]; !ok || entry.Max > cur {
				a.int64Values[entry.Group] = entry.Max
			}
		case ColumnPhysicalQueryGroupInt64Span:
			cur, ok := a.spans[entry.Group]
			if !ok {
				a.spans[entry.Group] = columnPhysicalQuerySpan{min: entry.Min, max: entry.Max}
				continue
			}
			if entry.Min < cur.min {
				cur.min = entry.Min
			}
			if entry.Max > cur.max {
				cur.max = entry.Max
			}
			a.spans[entry.Group] = cur
		}
	}
}

func (a *columnPhysicalQueryMetadataAccumulator) groupCount() int {
	if a == nil {
		return 0
	}
	if a.spans != nil {
		return len(a.spans)
	}
	if a.hourCounts != nil {
		return len(a.hourCounts)
	}
	if a.counts != nil {
		return len(a.counts)
	}
	return len(a.int64Values)
}

func (a *columnPhysicalQueryMetadataAccumulator) groups(req ColumnPhysicalQueryRequest) []ColumnPhysicalQueryGroup {
	capacity := a.groupCount()
	if req.TopK > 0 && req.TopK < capacity {
		capacity = req.TopK
	}
	out := make([]ColumnPhysicalQueryGroup, 0, capacity)
	add := func(group ColumnPhysicalQueryGroup) {
		if req.SkipEmptyGroupKey && group.Key == "" {
			return
		}
		if req.TopK > 0 {
			insertColumnPhysicalTopKGroup(&out, group, req.TopK, req.TopKOrder)
			return
		}
		out = append(out, group)
	}
	switch a.kind {
	case ColumnPhysicalQueryGroupCount:
		for key, count := range a.counts {
			add(ColumnPhysicalQueryGroup{Key: key, Count: count})
		}
	case ColumnPhysicalQueryGroupHourCount:
		for key, count := range a.hourCounts {
			add(ColumnPhysicalQueryGroup{Key: key.group, Hour: key.hour, Count: count})
		}
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64:
		for key, value := range a.int64Values {
			add(ColumnPhysicalQueryGroup{Key: key, Int64: value})
		}
	case ColumnPhysicalQueryGroupInt64Span:
		for key, span := range a.spans {
			add(ColumnPhysicalQueryGroup{Key: key, Int64: span.max - span.min})
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

func (a *columnPhysicalQueryMetadataAccumulator) topKCandidates(req ColumnPhysicalQueryRequest) int {
	if req.TopK <= 0 {
		return 0
	}
	if !req.SkipEmptyGroupKey {
		return a.groupCount()
	}
	candidates := 0
	if a.spans != nil {
		for key := range a.spans {
			if key != "" {
				candidates++
			}
		}
		return candidates
	}
	if a.hourCounts != nil {
		for key := range a.hourCounts {
			if key.group != "" {
				candidates++
			}
		}
		return candidates
	}
	if a.counts != nil {
		for key := range a.counts {
			if key != "" {
				candidates++
			}
		}
		return candidates
	}
	for key := range a.int64Values {
		if key != "" {
			candidates++
		}
	}
	return candidates
}

func (e *columnPhysicalQueryExecutor) mergeFrom(other *columnPhysicalQueryExecutor) error {
	if e == nil || other == nil {
		return errors.New("collections: cannot merge nil physical column query executor")
	}
	if e.kind != other.kind {
		return fmt.Errorf("collections: cannot merge physical column query kind %q into %q", other.kind, e.kind)
	}
	switch e.kind {
	case ColumnPhysicalQueryGroupCount:
		for key, count := range other.counts {
			e.counts[key] += count
		}
	case ColumnPhysicalQueryGroupCountDistinct:
		for key, otherSet := range other.distinct {
			set := e.distinct[key]
			if set == nil {
				set = make(map[string]struct{}, len(otherSet))
				e.distinct[key] = set
			}
			for value := range otherSet {
				set[value] = struct{}{}
			}
		}
	case ColumnPhysicalQueryHourCount:
		for hour, count := range other.hourCounts {
			e.hourCounts[hour] += count
		}
	case ColumnPhysicalQueryGroupMinInt64:
		for key, value := range other.int64Values {
			if cur, ok := e.int64Values[key]; !ok || value < cur {
				e.int64Values[key] = value
			}
		}
	case ColumnPhysicalQueryGroupMaxInt64:
		for key, value := range other.int64Values {
			if cur, ok := e.int64Values[key]; !ok || value > cur {
				e.int64Values[key] = value
			}
		}
	case ColumnPhysicalQueryGroupInt64Span:
		for key, span := range other.int64Spans {
			cur, ok := e.int64Spans[key]
			if !ok {
				e.int64Spans[key] = span
				continue
			}
			if span.min < cur.min {
				cur.min = span.min
			}
			if span.max > cur.max {
				cur.max = span.max
			}
			e.int64Spans[key] = cur
		}
	case ColumnPhysicalQuerySumSecondOfDaySquare:
		if other.int64SumRows > 0 {
			if err := e.addTransformedInt64Sum(other.int64Sum); err != nil {
				return err
			}
		}
		e.int64SumRows += other.int64SumRows
	default:
		return fmt.Errorf("%w: unsupported physical column query kind %q", ErrColumnQueryPlanUnsupported, e.kind)
	}
	e.reduceRows += other.reduceRows
	return nil
}

func columnPhysicalQueryInt64Value(value columnDeclaredValue) (int64, error) {
	if value.Type != ColumnStoreValueInt64 {
		return 0, fmt.Errorf("%w: physical column query expected int64 value, got %q", ErrColumnQueryPlanUnsupported, value.Type)
	}
	if value.Null {
		return 0, fmt.Errorf("%w: physical column query does not support null int64 values yet", ErrColumnQueryPlanUnsupported)
	}
	return value.Int64, nil
}

func columnPhysicalQueryUTCHour(timeUS int64) int {
	hours := timeUS / columnPhysicalQueryHourUS
	if timeUS < 0 && timeUS%columnPhysicalQueryHourUS != 0 {
		hours--
	}
	hour := int(hours % 24)
	if hour < 0 {
		hour += 24
	}
	return hour
}

func columnPhysicalQueryHourKey(hour int) string {
	if hour < 0 || hour >= 24 {
		return "hour_invalid"
	}
	return [...]string{
		"hour_00", "hour_01", "hour_02", "hour_03", "hour_04", "hour_05",
		"hour_06", "hour_07", "hour_08", "hour_09", "hour_10", "hour_11",
		"hour_12", "hour_13", "hour_14", "hour_15", "hour_16", "hour_17",
		"hour_18", "hour_19", "hour_20", "hour_21", "hour_22", "hour_23",
	}[hour]
}

type columnPhysicalQueryStringInterner struct {
	buckets map[uint64][]columnPhysicalQueryStringEntry
}

type columnPhysicalQueryStringEntry struct {
	key string
}

func (i *columnPhysicalQueryStringInterner) internBytes(raw []byte) string {
	if i.buckets == nil {
		i.buckets = make(map[uint64][]columnPhysicalQueryStringEntry, 16)
	}
	hash := columnPhysicalQueryHashBytes(raw)
	bucket := i.buckets[hash]
	for _, entry := range bucket {
		if columnPhysicalQueryStringEqualBytes(entry.key, raw) {
			return entry.key
		}
	}
	key := string(raw)
	i.buckets[hash] = append(bucket, columnPhysicalQueryStringEntry{key: key})
	return key
}

func (i *columnPhysicalQueryStringInterner) internString(raw string) string {
	if i.buckets == nil {
		i.buckets = make(map[uint64][]columnPhysicalQueryStringEntry, 16)
	}
	hash := columnPhysicalQueryHashString(raw)
	bucket := i.buckets[hash]
	for _, entry := range bucket {
		if entry.key == raw {
			return entry.key
		}
	}
	i.buckets[hash] = append(bucket, columnPhysicalQueryStringEntry{key: raw})
	return raw
}

func columnPhysicalQueryHashBytes(raw []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, b := range raw {
		hash ^= uint64(b)
		hash *= prime64
	}
	return hash
}

func columnPhysicalQueryHashString(raw string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for i := 0; i < len(raw); i++ {
		hash ^= uint64(raw[i])
		hash *= prime64
	}
	return hash
}

func columnPhysicalQueryStringEqualBytes(s string, raw []byte) bool {
	if len(s) != len(raw) {
		return false
	}
	for i := range raw {
		if s[i] != raw[i] {
			return false
		}
	}
	return true
}
