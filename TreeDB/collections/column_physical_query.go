package collections

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ColumnPhysicalQueryKind names the small M13B physical aggregate/projection
// shapes supported directly by the column asset scanner.
type ColumnPhysicalQueryKind string

const (
	// ColumnPhysicalQueryGroupCount counts rows by a string group column.
	ColumnPhysicalQueryGroupCount         ColumnPhysicalQueryKind = "group_count"
	ColumnPhysicalQueryGroupCountDistinct ColumnPhysicalQueryKind = "group_count_distinct"
	ColumnPhysicalQueryHourCount          ColumnPhysicalQueryKind = "hour_count"
	ColumnPhysicalQueryGroupMinInt64      ColumnPhysicalQueryKind = "group_min_int64"
	ColumnPhysicalQueryGroupMaxInt64      ColumnPhysicalQueryKind = "group_max_int64"
	ColumnPhysicalQueryGroupInt64Span     ColumnPhysicalQueryKind = "group_int64_span"
)

const columnPhysicalQueryHourUS = int64(3_600_000_000)

// ColumnPhysicalQueryRequest describes one explicit physical column query. It
// does not invoke planner routing; M14 owns forced/automatic route selection.
type ColumnPhysicalQueryRequest struct {
	Kind                     ColumnPhysicalQueryKind
	GroupColumn              string
	ValueColumn              string
	DistinctColumn           string
	AggregateMetadataName    string
	ColumnAssetReadIntegrity ColumnAssetReadIntegrity
}

// ColumnPhysicalQueryGroup is one reduced result row. Count is populated for
// count-style queries; Int64 is populated for min/max/span-style queries.
type ColumnPhysicalQueryGroup struct {
	Key   string
	Count int
	Int64 int64
}

// ColumnPhysicalQueryDiagnostics reports scan and reduce work for a physical
// query without counting full-document row materialization.
type ColumnPhysicalQueryDiagnostics struct {
	ManifestRoot               uint64
	ManifestGeneration         uint64
	RecoveryManifestGeneration uint64
	AppliedCommandLSN          uint64
	ManifestRecords            int
	AssetRefs                  int
	MutationParts              int
	DecodedBlocks              int
	DirectReduceBlocks         int
	MetadataHits               int
	ScheduledGranules          int
	SkippedGranules            int
	RowsScanned                int
	DeletedRows                int
	ProjectedColumns           int
	RowMaterializations        int
	PhysicalBytesScanned       int64
	ReduceRows                 int
	VisibilityRows             int
	ReconstructionRows         int
	ResultGroups               int
	WorkerCount                int
	SegmentFileCacheHits       uint64
	SegmentFileCacheMisses     uint64
	ColumnAssetReadIntegrity   string
	ScanNanos                  int64
	VisibilityNanos            int64
	ReduceNanos                int64
	ReconstructionNanos        int64
}

// ColumnPhysicalQueryResult is the reduced result and diagnostics from an
// explicit physical column query.
type ColumnPhysicalQueryResult struct {
	Groups      []ColumnPhysicalQueryGroup
	Diagnostics ColumnPhysicalQueryDiagnostics
}

var errColumnPhysicalScanCancelled = errors.New("collections: physical column scan cancelled")

// RunColumnPhysicalQuery executes an explicit serial physical column query over
// the recovery-authoritative manifest. Insert-only manifests use the direct
// scanner path; mutation-bearing manifests fall back to the M13C visibility
// overlay before reducing.
func (c *Collection) RunColumnPhysicalQuery(req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
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
		return c.runColumnPhysicalQueryWithVisibility(cfg, req)
	}
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotView()
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
		return c.runColumnPhysicalQueryWithVisibility(view.Config, req)
	}
	if req.AggregateMetadataName != "" {
		return c.runColumnPhysicalQueryAggregateMetadataInSnapshotView(view, req)
	}
	return c.runColumnPhysicalQueryInSnapshotView(view, req)
}

func (c *Collection) runColumnPhysicalQueryAggregateMetadataInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
	if view.MutationParts != 0 {
		return ColumnPhysicalQueryResult{}, fmt.Errorf("%w: aggregate metadata physical query requires insert-only manifest", ErrColumnQueryPlanUnsupported)
	}
	aggregate, ok := columnPhysicalQueryAggregateMetadataConfig(view.Config, req)
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
	var rawScratch []byte
	start := time.Now()
	diag := columnPhysicalQueryDiagnosticsFromScan(view.Diagnostics)
	diag.WorkerCount = 1
	diag.ProjectedColumns = 2
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	diag.ScheduledGranules = len(refs)
	for _, metadataRef := range refs {
		raw, err := readCache.read(metadataRef.AssetRef, rawScratch)
		diag.SegmentFileCacheHits = readCache.hits
		diag.SegmentFileCacheMisses = readCache.misses
		if err != nil {
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("collections: aggregate metadata read %q generation=%d part_id=%d: %w", metadataRef.Name, metadataRef.AssetRef.Generation, metadataRef.AssetRef.PartID, err)
		}
		rawScratch = raw
		diag.PhysicalBytesScanned += int64(len(raw))
		asset, err := decodeColumnAggregateMetadataAsset(raw, metadataRef.AssetRef, view.Config, view.CollectionName, aggregate.Name)
		if err != nil {
			return ColumnPhysicalQueryResult{Diagnostics: diag}, err
		}
		if asset.GroupColumn != req.GroupColumn || asset.ValueColumn != req.ValueColumn {
			return ColumnPhysicalQueryResult{Diagnostics: diag}, fmt.Errorf("%w: aggregate metadata %q columns %s/%s do not match query %s/%s", ErrColumnQueryPlanUnsupported, aggregate.Name, asset.GroupColumn, asset.ValueColumn, req.GroupColumn, req.ValueColumn)
		}
		acc.add(asset.Entries)
		diag.MetadataHits++
	}
	diag.ScanNanos = time.Since(start).Nanoseconds()
	diag.ReduceRows = acc.rows
	diag.ResultGroups = acc.groupCount()
	diag.SegmentFileCacheHits = readCache.hits
	diag.SegmentFileCacheMisses = readCache.misses
	return ColumnPhysicalQueryResult{Groups: acc.groups(), Diagnostics: diag}, nil
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
	result.Diagnostics.DirectReduceBlocks = diag.DecodedBlocks
	result.Diagnostics.ScanNanos = scanNanos
	result.Diagnostics.ReduceRows = exec.reduceRows
	if err != nil {
		return result, true, err
	}
	result.Groups = exec.groups()
	result.Diagnostics.ResultGroups = len(result.Groups)
	return result, true, nil
}

func (c *Collection) scanColumnPhysicalQueryDirectInSnapshotView(
	view columnPhysicalScanSnapshotView,
	exec *columnPhysicalQueryExecutor,
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
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, exec.readIntegrity)
	if err != nil {
		return diag, err
	}
	readCache.returnViews = true
	defer func() { _ = readCache.close() }()
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
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotView()
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
	if refs := len(view.AssetRefs); workers > refs {
		workers = refs
	}
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
	if firstErr != nil {
		result.Diagnostics.ReduceRows = merged.reduceRows
		return result, firstErr
	}
	result.Groups = merged.groups()
	result.Diagnostics.ReduceRows = merged.reduceRows
	result.Diagnostics.ResultGroups = len(result.Groups)
	return result, nil
}

func (c *Collection) runColumnPhysicalQueryInSnapshotView(view columnPhysicalScanSnapshotView, req ColumnPhysicalQueryRequest) (ColumnPhysicalQueryResult, error) {
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
	result.Diagnostics.WorkerCount = 1
	result.Diagnostics.ScanNanos = scanNanos
	result.Diagnostics.ReduceRows = exec.reduceRows
	if err != nil {
		return result, err
	}
	result.Groups = exec.groups()
	result.Diagnostics.ResultGroups = len(result.Groups)
	return result, nil
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
	result.Diagnostics.ResultGroups = len(result.Groups)
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
		ManifestGeneration:         diag.ManifestGeneration,
		RecoveryManifestGeneration: diag.RecoveryManifestGeneration,
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
	}
}

func mergeColumnPhysicalQueryDiagnostics(left, right ColumnPhysicalQueryDiagnostics) ColumnPhysicalQueryDiagnostics {
	if left.ManifestRoot == 0 {
		left.ManifestRoot = right.ManifestRoot
		left.ManifestGeneration = right.ManifestGeneration
		left.RecoveryManifestGeneration = right.RecoveryManifestGeneration
		left.AppliedCommandLSN = right.AppliedCommandLSN
		left.ManifestRecords = right.ManifestRecords
		left.AssetRefs = right.AssetRefs
		left.ProjectedColumns = right.ProjectedColumns
		left.ColumnAssetReadIntegrity = right.ColumnAssetReadIntegrity
	}
	if left.ColumnAssetReadIntegrity == "" {
		left.ColumnAssetReadIntegrity = right.ColumnAssetReadIntegrity
	}
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
	left.MetadataHits += right.MetadataHits
	left.ScheduledGranules += right.ScheduledGranules
	left.SkippedGranules += right.SkippedGranules
	left.RowsScanned += right.RowsScanned
	left.DeletedRows += right.DeletedRows
	left.RowMaterializations += right.RowMaterializations
	left.PhysicalBytesScanned += right.PhysicalBytesScanned
	left.ReduceRows += right.ReduceRows
	left.VisibilityRows += right.VisibilityRows
	left.ReconstructionRows += right.ReconstructionRows
	left.SegmentFileCacheHits += right.SegmentFileCacheHits
	left.SegmentFileCacheMisses += right.SegmentFileCacheMisses
	return left
}

type columnPhysicalQueryExecutor struct {
	kind              ColumnPhysicalQueryKind
	readIntegrity     ColumnAssetReadIntegrity
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
	hourCounts   [24]int
	int64Values  map[string]int64
	int64Spans   map[string]columnPhysicalQuerySpan
	reduceRows   int
	resultGroups []ColumnPhysicalQueryGroup
}

type columnPhysicalQuerySpan struct {
	min int64
	max int64
}

func newColumnPhysicalQueryExecutor(cfg ColumnStoreConfig, req ColumnPhysicalQueryRequest) (*columnPhysicalQueryExecutor, error) {
	exec := &columnPhysicalQueryExecutor{
		kind:              req.Kind,
		readIntegrity:     req.ColumnAssetReadIntegrity,
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
		switch req.Kind {
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
	default:
		return false
	}
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
		set := e.distinct[key]
		if set == nil {
			set = make(map[string]struct{})
			e.distinct[key] = set
		}
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
	}
	return nil
}

func reduceColumnPhysicalAssetDirect(raw []byte, ref ColumnAssetRef, expectedCollection string, cfg *ColumnStoreConfig, expectedOperation ColumnPublishOperation, exec *columnPhysicalQueryExecutor) (columnPhysicalAssetScanSummary, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnPhysicalAssetMagic {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("bad column physical asset magic=0x%08x", magic)
	}
	version := cur.u16()
	if !isSupportedColumnPhysicalAssetVersion(version) {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("unsupported column physical asset version=%d", version)
	}
	collection := cur.stringBytes()
	namespace := cur.stringBytes()
	generation := cur.u64()
	partID := cur.u64()
	appliedCommandLSN := cur.u64()
	operationBytes := cur.stringBytes()
	operation, operationOK := columnPhysicalScanOperationFromBytes(operationBytes)
	schemaHash := cur.u64()
	columnCount := cur.u64()
	rowCount := cur.u64()
	if err := cur.err; err != nil {
		return columnPhysicalAssetScanSummary{}, err
	}
	if columnCount > uint64(maxCollectionInt) {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset column_count=%d overflows int max=%d", columnCount, maxCollectionInt)
	}
	if rowCount > uint64(maxCollectionInt) {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset row_count=%d overflows int max=%d", rowCount, maxCollectionInt)
	}
	header := columnPhysicalAssetScanHeader{
		Collection:        collection,
		Namespace:         namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: appliedCommandLSN,
		Operation:         operation,
		SchemaHash:        schemaHash,
		ColumnCount:       int(columnCount),
		RowCount:          int(rowCount),
	}
	if !operationOK {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("unsupported column physical asset operation %q", operationBytes)
	}
	if version == columnPhysicalAssetVersionV1 && header.Operation == ColumnPublishOperationDelete {
		return columnPhysicalAssetScanSummary{}, errors.New("legacy v1 column physical asset delete operation unsupported")
	}
	if err := validateColumnPhysicalAssetScanHeader(header, ref, expectedCollection, cfg); err != nil {
		return columnPhysicalAssetScanSummary{}, err
	}
	if expectedOperation != "" && header.Operation != expectedOperation {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("%w: manifest reason=%q asset operation=%q", errColumnPhysicalAssetManifestOperationMismatch, expectedOperation, header.Operation)
	}
	if header.Operation != ColumnPublishOperationInsert {
		return columnPhysicalAssetScanSummary{}, errColumnPhysicalQueryNeedsVisibility
	}
	if header.ColumnCount != len(cfg.Columns) {
		return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset columns=%d want %d", header.ColumnCount, len(cfg.Columns))
	}
	for colIdx := 0; colIdx < header.ColumnCount; colIdx++ {
		name := cur.stringBytes()
		path := cur.stringBytes()
		valueType := cur.stringBytes()
		nullable := cur.bool()
		dictionary := cur.bool()
		if cur.err != nil {
			return columnPhysicalAssetScanSummary{}, cur.err
		}
		want := cfg.Columns[colIdx]
		if !columnPhysicalBytesEqualString(name, want.Name) ||
			!columnPhysicalBytesEqualString(path, want.Path) ||
			!columnPhysicalBytesEqualString(valueType, string(want.ValueType)) ||
			nullable != want.Nullable ||
			dictionary != want.Dictionary {
			return columnPhysicalAssetScanSummary{}, fmt.Errorf("column physical asset column[%d]={Name:%q Path:%q ValueType:%q Nullable:%t Dictionary:%t} want %+v",
				colIdx, string(name), string(path), string(valueType), nullable, dictionary, want)
		}
	}
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
		selected := selectedGroup || selectedValue || selectedDistinct
		if selected && exec.readIntegrity != ColumnAssetReadIntegritySkipChecksums {
			typeBytes := cur.stringBytes()
			if cur.err != nil {
				return nil, false, nil, false, 0, false, cur.err
			}
			if !columnPhysicalBytesEqualString(typeBytes, string(col.ValueType)) {
				return nil, false, nil, false, 0, false, fmt.Errorf("column[%d] type=%q want %q", colIdx, string(typeBytes), col.ValueType)
			}
		} else {
			// Header/schema validation and asset checksums already cover the redundant
			// per-value type tag. Unsafe checksum-skipping reads also skip selected
			// type tags and rely on the already-validated asset header/schema.
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
		if version >= columnPhysicalAssetVersion {
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
			cur.skip(1)
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
	set := e.distinct[key]
	if set == nil {
		set = make(map[string]struct{})
		e.distinct[key] = set
	}
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
	}
	sort.Slice(e.resultGroups, func(i, j int) bool {
		return e.resultGroups[i].Key < e.resultGroups[j].Key
	})
	return e.resultGroups
}

type columnPhysicalQueryMetadataAccumulator struct {
	kind        ColumnPhysicalQueryKind
	int64Values map[string]int64
	spans       map[string]columnPhysicalQuerySpan
	rows        int
}

func newColumnPhysicalQueryMetadataAccumulator(kind ColumnPhysicalQueryKind) *columnPhysicalQueryMetadataAccumulator {
	acc := &columnPhysicalQueryMetadataAccumulator{kind: kind}
	switch kind {
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64:
		acc.int64Values = make(map[string]int64)
	case ColumnPhysicalQueryGroupInt64Span:
		acc.spans = make(map[string]columnPhysicalQuerySpan)
	}
	return acc
}

func (a *columnPhysicalQueryMetadataAccumulator) add(entries []columnAggregateMetadataEntry) {
	for _, entry := range entries {
		a.rows += entry.Count
		switch a.kind {
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
	return len(a.int64Values)
}

func (a *columnPhysicalQueryMetadataAccumulator) groups() []ColumnPhysicalQueryGroup {
	out := make([]ColumnPhysicalQueryGroup, 0, a.groupCount())
	switch a.kind {
	case ColumnPhysicalQueryGroupMinInt64, ColumnPhysicalQueryGroupMaxInt64:
		for key, value := range a.int64Values {
			out = append(out, ColumnPhysicalQueryGroup{Key: key, Int64: value})
		}
	case ColumnPhysicalQueryGroupInt64Span:
		for key, span := range a.spans {
			out = append(out, ColumnPhysicalQueryGroup{Key: key, Int64: span.max - span.min})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
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
