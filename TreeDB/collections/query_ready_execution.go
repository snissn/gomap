package collections

import (
	"errors"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// QueryReadyColumnGenerationKind is the public collection-facing vocabulary
// for already-selected immutable QRBG/QRDG assets. Selection/publication stays
// with the caller's existing manifest and asset inventory.
type QueryReadyColumnGenerationKind string

const (
	QueryReadyColumnGenerationBase             QueryReadyColumnGenerationKind = "base"
	QueryReadyColumnGenerationDelta            QueryReadyColumnGenerationKind = "delta"
	QueryReadyColumnGenerationConsolidatedBase QueryReadyColumnGenerationKind = "consolidated_base"
)

type QueryReadyColumnGenerationFile struct {
	Path       string
	Offset     int64
	Length     int64
	Generation uint64
	Kind       QueryReadyColumnGenerationKind
}

// QueryReadyColumnGenerationFiles is a caller-selected physical snapshot.
// Zero bounds use the M2 defaults. The collection derives schema/manifest
// identity from its recovery-authoritative ColumnStoreCacheIdentity.
type QueryReadyColumnGenerationFiles struct {
	Base                     QueryReadyColumnGenerationFile
	Deltas                   []QueryReadyColumnGenerationFile
	MaxVisibleGenerations    int
	MaxAccumulatedDeltaParts int
	MaxRows                  int64
	MaxBytes                 int64
	lifetime                 *queryReadyColumnPreparedLifetime
}

// QueryReadyColumnPhysicalQueryRunner owns a collection generation lease and
// bounded operator scratch. It is not safe for concurrent use.
type QueryReadyColumnPhysicalQueryRunner struct {
	lease     *collectionQueryReadyGenerationLease
	operator  *typedcolumn.QueryReadyOperator
	request   ColumnPhysicalQueryRequest
	groups    []ColumnPhysicalQueryGroup
	closeOnce sync.Once
	closeErr  error
	closed    bool
	lifetime  *queryReadyColumnPreparedLifetime
}

// PrepareQueryReadyColumnPhysicalQuery prepares q1-q5/qexpr-compatible shared
// encoded execution over one query-ready base plus visible bounded deltas.
func (c *Collection) PrepareQueryReadyColumnPhysicalQuery(files QueryReadyColumnGenerationFiles, request ColumnPhysicalQueryRequest) (*QueryReadyColumnPhysicalQueryRunner, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	identity, ok := c.ColumnStoreCacheIdentity()
	if !ok {
		return nil, errors.New("collections: query-ready execution has no column-store identity")
	}
	return c.prepareQueryReadyColumnPhysicalQueryForIdentity(identity, files, request)
}

func (c *Collection) prepareQueryReadyColumnPhysicalQueryForIdentity(identity ColumnStoreCacheIdentity, files QueryReadyColumnGenerationFiles, request ColumnPhysicalQueryRequest) (*QueryReadyColumnPhysicalQueryRunner, error) {
	if files.lifetime != nil {
		if err := files.lifetime.acquire(); err != nil {
			return nil, err
		}
	}
	releaseLifetime := true
	defer func() {
		if releaseLifetime && files.lifetime != nil {
			_ = files.lifetime.release()
		}
	}()
	if err := validateColumnPhysicalQueryRequest(request); err != nil {
		return nil, err
	}
	if request.AggregateMetadataName != "" {
		return nil, fmt.Errorf("%w: query-ready execution does not use query-specific aggregate metadata", ErrColumnQueryPlanUnsupported)
	}
	opened, err := queryReadyColumnOpenFiles(identity, files)
	if err != nil {
		return nil, err
	}
	lease, err := c.openCollectionQueryReadyGenerationForIdentity(identity, opened)
	if err != nil {
		return nil, err
	}
	operatorRequest, err := queryReadyColumnOperatorRequest(request)
	if err != nil {
		return nil, errors.Join(err, lease.Close())
	}
	operator, err := lease.Prepared().PrepareOperator(operatorRequest)
	if err != nil {
		return nil, errors.Join(err, lease.Close())
	}
	releaseLifetime = false
	return &QueryReadyColumnPhysicalQueryRunner{lease: lease, operator: operator, request: request, lifetime: files.lifetime}, nil
}

// RunQueryReadyColumnPhysicalQuery is the one-shot production adapter. Hot
// benchmark cells should prepare once and call runner.Run for each attempt.
func (c *Collection) RunQueryReadyColumnPhysicalQuery(files QueryReadyColumnGenerationFiles, request ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	runner, err := c.PrepareQueryReadyColumnPhysicalQuery(files, request)
	if err != nil {
		return ColumnPhysicalQueryResult{}, err
	}
	result, runErr := runner.Run()
	return result, errors.Join(runErr, runner.Close())
}

// Run returns runner-owned groups. Callers must consume them before the next
// Run or Close.
func (r *QueryReadyColumnPhysicalQueryRunner) Run() (ColumnPhysicalQueryResult, error) {
	if r == nil || r.closed || r.operator == nil {
		return ColumnPhysicalQueryResult{}, errors.New("collections: query-ready physical query runner is closed")
	}
	result, err := r.operator.Run()
	if cap(r.groups) < len(result.Groups) {
		r.groups = make([]ColumnPhysicalQueryGroup, len(result.Groups))
	} else {
		r.groups = r.groups[:len(result.Groups)]
	}
	out := ColumnPhysicalQueryResult{Groups: r.groups}
	for i, group := range result.Groups {
		out.Groups[i] = ColumnPhysicalQueryGroup{Key: group.Key, Hour: group.Hour, Count: group.Count, DistinctCount: group.DistinctCount, Int64: group.Int64}
		if r.request.Kind == ColumnPhysicalQueryHourCount {
			out.Groups[i].Key = columnPhysicalQueryHourKey(group.Hour)
			out.Groups[i].Hour = 0
		} else if r.request.Kind == ColumnPhysicalQuerySumSecondOfDaySquare {
			out.Groups[i].Key = columnPhysicalQuerySumSecondOfDaySquareKey(r.request.ValueColumn)
		}
	}
	applyQueryReadyExecutionDiagnostics(&out.Diagnostics, r.request, result.Stats)
	return out, err
}

func (r *QueryReadyColumnPhysicalQueryRunner) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.closed = true
		r.operator = nil
		r.groups = nil
		if r.lease != nil {
			r.closeErr = r.lease.Close()
			r.lease = nil
		}
		if r.lifetime != nil {
			r.closeErr = errors.Join(r.closeErr, r.lifetime.release())
			r.lifetime = nil
		}
	})
	return r.closeErr
}

func queryReadyColumnOpenFiles(identity ColumnStoreCacheIdentity, files QueryReadyColumnGenerationFiles) (typedcolumn.QueryReadyGenerationOpenFiles, error) {
	key, ok := collectionQueryReadyGenerationOpenKey(identity)
	if !ok {
		return typedcolumn.QueryReadyGenerationOpenFiles{}, errors.New("collections: incomplete query-ready column-store identity")
	}
	convert := func(file QueryReadyColumnGenerationFile, wantBase bool) (typedcolumn.QueryReadyGenerationFile, error) {
		kind := typedcolumn.QueryReadyGenerationDelta
		switch file.Kind {
		case QueryReadyColumnGenerationBase:
			kind = typedcolumn.QueryReadyGenerationBase
		case QueryReadyColumnGenerationDelta:
			kind = typedcolumn.QueryReadyGenerationDelta
		case QueryReadyColumnGenerationConsolidatedBase:
			kind = typedcolumn.QueryReadyGenerationConsolidatedBase
		default:
			return typedcolumn.QueryReadyGenerationFile{}, fmt.Errorf("collections: unsupported query-ready generation kind %q", file.Kind)
		}
		if wantBase && kind != typedcolumn.QueryReadyGenerationBase && kind != typedcolumn.QueryReadyGenerationConsolidatedBase {
			return typedcolumn.QueryReadyGenerationFile{}, errors.New("collections: query-ready base descriptor is not a base")
		}
		if !wantBase && kind != typedcolumn.QueryReadyGenerationDelta {
			return typedcolumn.QueryReadyGenerationFile{}, errors.New("collections: query-ready delta descriptor is not a delta")
		}
		if file.Generation == 0 {
			return typedcolumn.QueryReadyGenerationFile{}, errors.New("collections: query-ready generation is zero")
		}
		return typedcolumn.QueryReadyGenerationFile{Path: file.Path, Offset: file.Offset, Length: file.Length, Identity: typedcolumn.QueryReadyBaseIdentity{Generation: file.Generation, SchemaHash: key.Identity.SchemaHash}, Kind: kind}, nil
	}
	base, err := convert(files.Base, true)
	if err != nil {
		return typedcolumn.QueryReadyGenerationOpenFiles{}, err
	}
	deltas := make([]typedcolumn.QueryReadyGenerationFile, len(files.Deltas))
	for i, file := range files.Deltas {
		deltas[i], err = convert(file, false)
		if err != nil {
			return typedcolumn.QueryReadyGenerationOpenFiles{}, fmt.Errorf("collections: query-ready delta[%d]: %w", i, err)
		}
	}
	bound := typedcolumn.QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: files.MaxVisibleGenerations, MaxAccumulatedDeltaParts: files.MaxAccumulatedDeltaParts, MaxRows: files.MaxRows, MaxBytes: files.MaxBytes}
	defaults := typedcolumn.DefaultQueryReadyDeltaBoundPolicy()
	if bound.MaxVisibleGenerations == 0 {
		bound.MaxVisibleGenerations = defaults.MaxVisibleGenerations
	}
	if bound.MaxAccumulatedDeltaParts == 0 {
		bound.MaxAccumulatedDeltaParts = defaults.MaxAccumulatedDeltaParts
	}
	return typedcolumn.QueryReadyGenerationOpenFiles{Key: key, Base: base, Deltas: deltas, SnapshotGeneration: key.Identity.Generation, Bound: bound}, nil
}

func queryReadyColumnOperatorRequest(request ColumnPhysicalQueryRequest) (typedcolumn.QueryReadyOperatorRequest, error) {
	kind := typedcolumn.QueryReadyOperatorKind(request.Kind)
	switch request.Kind {
	case ColumnPhysicalQueryGroupCount, ColumnPhysicalQueryGroupCountDistinct, ColumnPhysicalQueryGroupCountAndDistinct, ColumnPhysicalQueryHourCount, ColumnPhysicalQueryGroupHourCount, ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64, ColumnPhysicalQueryGroupInt64Span, ColumnPhysicalQuerySumSecondOfDaySquare:
	default:
		return typedcolumn.QueryReadyOperatorRequest{}, fmt.Errorf("%w: query-ready physical query kind %q", ErrColumnQueryPlanUnsupported, request.Kind)
	}
	predicates := make([]typedcolumn.QueryReadyStringPredicate, 0, len(request.Predicates))
	for index, predicate := range request.Predicates {
		var values []string
		switch columnPhysicalQueryPredicateKindOrDefault(predicate.Kind) {
		case ColumnPhysicalQueryPredicateEqual:
			if len(predicate.Values) != 0 {
				return typedcolumn.QueryReadyOperatorRequest{}, fmt.Errorf("%w: query-ready predicate[%d] equal uses Value", ErrColumnQueryPlanUnsupported, index)
			}
			values = []string{predicate.Value}
		case ColumnPhysicalQueryPredicateInList:
			if predicate.Value != "" || len(predicate.Values) == 0 || len(predicate.Values) > columnPhysicalQueryMaxPredicateValues {
				return typedcolumn.QueryReadyOperatorRequest{}, fmt.Errorf("%w: invalid query-ready IN predicate[%d]", ErrColumnQueryPlanUnsupported, index)
			}
			values = append([]string(nil), predicate.Values...)
		default:
			return typedcolumn.QueryReadyOperatorRequest{}, fmt.Errorf("%w: unsupported query-ready predicate kind %q", ErrColumnQueryPlanUnsupported, predicate.Kind)
		}
		predicates = append(predicates, typedcolumn.QueryReadyStringPredicate{Column: predicate.Column, Values: values})
	}
	order := typedcolumn.QueryReadyOperatorOrder(request.TopKOrder)
	return typedcolumn.QueryReadyOperatorRequest{Kind: kind, GroupColumn: request.GroupColumn, ValueColumn: request.ValueColumn, DistinctColumn: request.DistinctColumn, Predicates: predicates, TopK: request.TopK, Order: order, SkipEmptyGroupKey: request.SkipEmptyGroupKey}, nil
}

func applyQueryReadyExecutionDiagnostics(diag *ColumnPhysicalQueryDiagnostics, request ColumnPhysicalQueryRequest, stats typedcolumn.QueryReadyExecutionStats) {
	if diag == nil {
		return
	}
	diag.StorageSource = ColumnPhysicalQueryStorageSourceQueryReadyBaseDelta
	diag.FallbackReason = ColumnPhysicalQueryFallbackNone
	diag.RowsScanned, diag.RowsMatched, diag.DeletedRows = stats.RowsScanned, stats.RowsMatched, stats.RowsDeleted
	diag.MutationParts = stats.DeltaParts
	diag.DecodedBlocks, diag.DecodedPayloadBytes = stats.DecodedBlocks, uint64(stats.DecodedBytes)
	diag.PhysicalBytesScanned = stats.DecodedBytes
	diag.ReduceRows, diag.ResultGroups = stats.RowsMatched, stats.GroupsReturned
	diag.PredicateCount, diag.PredicateLiterals = len(request.Predicates), 0
	for _, predicate := range request.Predicates {
		diag.PredicateColumns = append(diag.PredicateColumns, predicate.Column)
		diag.PredicateKinds = append(diag.PredicateKinds, string(columnPhysicalQueryPredicateKindOrDefault(predicate.Kind)))
		if predicate.Kind == ColumnPhysicalQueryPredicateInList {
			diag.PredicateLiterals += len(predicate.Values)
		} else {
			diag.PredicateLiterals++
		}
	}
	diag.TopKLimit, diag.TopKOrder, diag.TopKCandidates = request.TopK, string(request.TopKOrder), 0
	if request.TopK > 0 {
		diag.TopKCandidates = stats.GroupsConsidered
	}
	diag.ScanNanos = stats.BaseScanNanos + stats.DeltaMergeNanos + stats.PredicateNanos
	diag.ReduceNanos, diag.ResultShapeNanos = stats.ReductionNanos, stats.GroupingNanos+stats.OrderingTopKNanos
	diag.QueryReadyEncodedExecutions, diag.QueryReadyPreparedParts = stats.EncodedBaseDeltaExecutions, stats.PreparedParts
	diag.QueryReadyBaseParts, diag.QueryReadyDeltaParts = stats.BaseParts, stats.DeltaParts
	diag.QueryReadyRowsCandidate, diag.QueryReadyRowsVisible, diag.QueryReadyRowsSuperseded = stats.RowsCandidate, stats.RowsVisible, stats.RowsSuperseded
	diag.QueryReadyCodeTranslations, diag.QueryReadyDictionaryDomains, diag.QueryReadyScratchBytes = stats.CodeTranslations, stats.DictionaryDomains, stats.ScratchBytes
	diag.QueryReadyPreparationNanos, diag.QueryReadyBaseScanNanos, diag.QueryReadyDeltaMergeNanos = stats.PreparationNanos, stats.BaseScanNanos, stats.DeltaMergeNanos
	diag.QueryReadyPredicateNanos, diag.QueryReadyReductionNanos = stats.PredicateNanos, stats.ReductionNanos
	diag.QueryReadyFusedPredicateReductionExecutions = stats.FusedPredicateReductionExecutions
	diag.QueryReadyFusedPredicateReductionWorkers = stats.FusedPredicateReductionWorkers
	diag.QueryReadyFusedPredicateReductionNanos = stats.FusedPredicateReductionNanos
	diag.QueryReadyGroupingNanos, diag.QueryReadyOrderingTopKNanos = stats.GroupingNanos, stats.OrderingTopKNanos
	diag.QueryReadyLegacyFallbacks, diag.QueryReadyPrecomputedAnswers = stats.LegacyScanFallbacks, stats.PrecomputedAnswers
	diag.DocumentMaterializations, diag.FallbackReads = stats.DocumentMaterializations, stats.Fallbacks
}
