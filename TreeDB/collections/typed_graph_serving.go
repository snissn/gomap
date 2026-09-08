package collections

import (
	"context"
	"errors"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// ColumnGraphServingOptions is explicit process-local admission for the selected
// durable typed column_graph route. Reapply it after opening a DB. Limits are
// immutable across collection handles until that DB closes; zero is not a default.
type ColumnGraphServingOptions struct {
	Publication                ColumnGraphPublicationLimits
	Owners                     ColumnGraphReadOwnerLimits
	CandidateOutput            ColumnGraphCandidateOutputLimits
	Maintenance                ColumnGraphMaintenanceLimits
	Filter                     ColumnGraphFilterLimits
	FoldRows, SearchCandidates int
}

type ColumnGraphPublicationLimits = typedGraphPublicationLimits
type ColumnGraphReadOwnerLimits = typedGraphReadOwnerLimits
type ColumnGraphColdLimits = typedGraphColdLimits
type ColumnGraphCandidateOutputLimits = typedGraphFoldAssetLimits
type ColumnGraphMaintenanceLimits = typedGraphWorkEpochLimits

// ColumnGraphFilterLimits bounds cumulative preparation work, including selective
// discovery and any full fallback. MappingWork admits ordinal-mapping bounds,
// secondary point requests and temporary encoded-prefix/key payload byte bounds;
// it is not a comparison count or a total Go heap bound.
type ColumnGraphFilterLimits = typedGraphFilterLimits
type ColumnGraphMaintenanceStats = typedGraphWorkEpochStats

// Backpressure sentinels preserve distinct caller actions: fold/maintenance,
// release held readers, or reduce query work. Use errors.Is, not error text.
var (
	ErrColumnGraphFoldNeeded   = errTypedGraphOverlayFoldNeeded
	ErrColumnGraphOwnerBudget  = errTypedGraphOwnerBudget
	ErrColumnGraphSearchBudget = errTypedGraphSearchBudget
)

type typedGraphServingPolicy struct {
	index   string
	options ColumnGraphServingOptions
}

func (c *Collection) typedGraphServingPolicy() *typedGraphServingPolicy {
	if c == nil || c.db == nil {
		return nil
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nil
	}
	return coord.typedGraphServing.Load()
}

// EnsureColumnGraphServing performs bounded cold reconciliation and admission.
// The declared typed graph must have been built explicitly. Repeated ensure on
// unchanged authority is idempotent; searches never invoke this operation.
func (c *Collection) EnsureColumnGraphServing(ctx context.Context, index string, opts ColumnGraphServingOptions) error {
	if ctx == nil || c == nil || c.db == nil || !c.db.CommandWALEnabled() {
		return ErrHybridSearchUnsupported
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateIndexName(index); err != nil {
		return err
	}
	p, o, f, m := opts.Publication, opts.Owners, opts.Filter, opts.Maintenance
	if p.Rows <= 0 || p.Tombstones <= 0 || p.ValueSlots <= 0 || p.OwnedBytes <= 0 || p.EncodedOutputBytes <= 0 || o.Owners <= 0 || o.States <= 0 || o.StateBytes <= 0 || o.AssetBytes <= 0 || o.Cold.ManifestRecords <= 0 || o.Cold.ManifestBytes <= 0 || o.Cold.AssetBytes <= 0 || o.Cold.DecodedTermBytes <= 0 || opts.CandidateOutput.Bytes <= 0 || opts.CandidateOutput.AppenderAttempts <= 0 || opts.FoldRows <= 0 || opts.SearchCandidates <= 0 || f.SourceIDs <= 0 || f.SourceBytes <= 0 || f.RetainedBytes <= 0 || f.MappingWork <= 0 || f.InspectedEntries <= 0 || m.NativeEntries <= 0 || m.ColumnSegments <= 0 || m.ManifestRecords <= 0 || m.LifecycleEntries <= 0 || m.NativeBytes <= 0 || m.ColumnBytes <= 0 || m.ManifestBytes <= 0 || m.RetainedBytes <= 0 || m.PagerPages == 0 {
		return errTypedGraphSearchBudget
	}
	coord := c.collectionSchemaCoordinator()
	policy := &typedGraphServingPolicy{index: index, options: opts}
	if old := coord.typedGraphServing.Load(); old != nil && *old != *policy {
		return ErrConcurrentMutation
	}
	def, err := c.declaredVectorIndexDefinition(index)
	if err != nil {
		return err
	}
	if def.Strategy != VectorIndexStrategyColumnGraph {
		return ErrHybridSearchUnsupported
	}
	if !coord.typedGraphServing.CompareAndSwap(nil, policy) {
		if old := coord.typedGraphServing.Load(); old == nil || *old != *policy {
			return ErrConcurrentMutation
		}
	}
	before := coord.typedPublication.Load()
	wasReady := before != nil && before.servingAdmitted && !before.invalid && before.servingBase != nil
	if err := c.reconcileTypedGraphPublicationWithContext(ctx, p, o.Cold); err != nil {
		return err
	}
	if _, err := coord.bindTypedGraphFoldAssetAdmission(opts.CandidateOutput); err != nil {
		return err
	}
	if err := c.prepareTypedGraphServingMetadata(ctx, o.Cold); err != nil {
		return err
	}
	// Metadata preparation just validated exact catalog authority under the
	// existing exclusion. A concurrent admitted suffix/fold may advance the
	// immutable pointer without requiring another cold activation or debt reset.
	if state := coord.typedPublication.Load(); wasReady && state != nil && !state.invalid && state.servingAdmitted && state.servingBase != nil {
		// Admission belongs to the shared authority, but its optional prepared
		// keeper belongs to this handle. Explicit Ensure warms new handles too.
		if state.servingBase.graph.RowCount == 0 {
			c.invalidateTypedGraphEmptyBaseKeeper(index)
			return nil
		}
		_, err := c.acquireTypedGraphCapturedBaseCacheWithContext(ctx, index, o)
		if err == nil {
			// A concurrent empty fold may have passed cleanup during the warm.
			c.invalidateTypedGraphEmptyBaseKeeper(index)
		}
		return err
	}
	prepared := coord.typedPublication.Load()
	if prepared == nil || prepared.invalid || prepared.servingBase == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	if _, err := c.renewTypedGraphWorkEpoch(ctx, m); err != nil {
		return err
	}
	// An exact empty base has no shared prepared search holder to warm. Its
	// coherent owner still serves the mutable suffix; the CAS below binds this
	// decision to the validated immutable state rather than a cache result.
	var warmed *collectionVectorIndexPreparedSearch
	if prepared.servingBase.graph.RowCount != 0 {
		warmed, err = c.acquireTypedGraphCapturedBaseCacheWithContext(ctx, index, o)
		if err != nil {
			return err
		}
	}
	ready := *prepared
	ready.servingAdmitted = true
	if !coord.typedPublication.CompareAndSwap(prepared, &ready) {
		// A late captured-base build is only an accelerator, never authority.
		// Release this rejected keeper without invalidating a newer cache entry
		// or any independently retained read-view owner.
		if warmed != nil {
			c.invalidateCollectionVectorIndexPreparedSearch(collectionVectorIndexPreparedSearchCacheSlot{family: collectionVectorIndexPreparedSearchFamilyCapturedBase, indexName: index}, warmed)
		}
		return ErrConcurrentMutation
	}
	if ready.servingBase.graph.RowCount == 0 {
		c.invalidateTypedGraphEmptyBaseKeeper(index)
	}
	return nil
}

// FoldColumnGraphServing performs explicit bounded rebuild and maintenance.
// Errors preserve fail-closed state and are never hidden by a retry loop.
func (c *Collection) FoldColumnGraphServing(ctx context.Context, index string) (err error) {
	workstats.Fold.Public.Attempts.Add(1)
	defer func() { workstats.Fold.Public.Finish(err == nil) }()
	p := c.typedGraphServingPolicy()
	if p == nil || p.index != index || ctx == nil {
		return ErrVectorIndexSearchUnavailable
	}
	if err := c.foldTypedGraph(ctx, p.options.Owners.Cold, p.options.FoldRows, p.options.CandidateOutput, nil); err != nil {
		return err
	}
	// Publication already installed its ready immutable state. Checkpoint and
	// work-epoch reclamation remain outside install admission; neither is a
	// reason to expose an invalid serving frontier.
	if _, err := c.renewTypedGraphWorkEpoch(ctx, p.options.Maintenance); err != nil {
		return err
	}
	if state := c.collectionSchemaCoordinator().typedPublication.Load(); state != nil && !state.invalid && state.servingAdmitted && state.servingBase != nil && state.servingBase.graph.RowCount == 0 {
		// Empty publication needs no optional shared prepared search holder.
		c.invalidateTypedGraphEmptyBaseKeeper(index)
		return nil
	}
	_, err = c.acquireTypedGraphCapturedBaseCacheWithContext(ctx, index, p.options.Owners)
	if err == nil {
		// A concurrent empty fold may have passed cleanup while this build
		// was in flight. Recheck after installing our optional keeper.
		c.invalidateTypedGraphEmptyBaseKeeper(index)
	}
	return err
}

// Without a replacement warm, an empty base must release its previous keeper.
// Observe the exact cache object only while the installed state is healthy and
// empty. A later nonempty publication/cache replacement cannot be invalidated
// by this exact-object cleanup; independent read-owner refs remain alive.
func (c *Collection) invalidateTypedGraphEmptyBaseKeeper(index string) {
	slot := collectionVectorIndexPreparedSearchCacheSlot{family: collectionVectorIndexPreparedSearchFamilyCapturedBase, indexName: index}
	coord := c.collectionSchemaCoordinator()
	var old *collectionVectorIndexPreparedSearch
	c.vectorBufferedSearchMu.Lock()
	if state := coord.typedPublication.Load(); state != nil && !state.invalid && state.servingAdmitted && state.servingBase != nil && state.servingBase.graph.RowCount == 0 {
		if entry := c.vectorBufferedSearch[slot]; entry != nil && !entry.building {
			old = entry.prepared
		}
	}
	c.vectorBufferedSearchMu.Unlock()
	if old != nil {
		c.invalidateCollectionVectorIndexPreparedSearch(slot, old)
	}
}

// RenewColumnGraphServing renews attempted-work allowance only after successful
// bounded maintenance. It does not fold a suffix or release caller-held views.
func (c *Collection) RenewColumnGraphServing(ctx context.Context, index string) (ColumnGraphMaintenanceStats, error) {
	p := c.typedGraphServingPolicy()
	if p == nil || p.index != index || ctx == nil {
		return ColumnGraphMaintenanceStats{}, ErrVectorIndexSearchUnavailable
	}
	return c.renewTypedGraphWorkEpoch(ctx, p.options.Maintenance)
}

func (c *Collection) searchTypedGraphServing(opts VectorIndexSearchOptions, buffer *VectorIndexSearchBuffer) (response VectorIndexSearchResponse, view *CollectionReadView, err error) {
	workstats.Graph.Requests.Attempts.Add(1)
	var stats typedGraphOverlaySearchStats
	var filterWork ColumnGraphFilterWork
	var snapshot ColumnGraphQuerySnapshot
	defer func() {
		workstats.Graph.Requests.Finish(err == nil)
		response.Stats.ColumnGraphWork = stats.work()
		response.Stats.ColumnGraphWork.Completed = err == nil
		response.Stats.ColumnGraphWork.Filter = filterWork
		response.Stats.ColumnGraphWork.Snapshot = snapshot
	}()
	if err = validateCollectionVectorIndexSearchWithBufferOptions(opts, buffer); err != nil {
		return
	}
	buffer.Reset()
	p := c.typedGraphServingPolicy()
	if p == nil || p.index != opts.IndexName {
		return response, nil, ErrVectorIndexSearchUnavailable
	}
	if opts.QueryMode != "" && opts.QueryMode != VectorIndexQueryModeExact || opts.StatsMode != VectorIndexSearchStatsModeMinimal && opts.StatsMode != VectorIndexSearchStatsModeProduction || opts.MaxDecodedBlocks != 0 {
		return response, nil, ErrHybridSearchUnsupported
	}
	if opts.Context != nil {
		if err = opts.Context.Err(); err != nil {
			return
		}
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	started := time.Now()
	owner, err := c.openTypedGraphReadOwnerWithContext(ctx, p.options.Owners)
	acquired := time.Since(started)
	if err != nil {
		return response, nil, err
	}
	snapshot = owner.querySnapshot()
	defer func() {
		if err != nil {
			err = errors.Join(err, owner.Close())
			buffer.Reset()
			response.Results = nil
		}
	}()
	if opts.DeclaredScalarFilter == nil {
		response.Results, stats, err = owner.overlay.searchWithContext(ctx, opts.Query, opts.TopK, opts.EfSearch, p.options.SearchCandidates, buffer)
	} else {
		var filter *typedGraphPreparedFilter
		filter, err = prepareTypedGraphFilterWithContext(ctx, owner.overlay, *opts.DeclaredScalarFilter, p.options.Filter, &filterWork)
		if err == nil {
			response.Results, stats, err = owner.overlay.searchPreparedFilterWithContext(ctx, filter, opts.Query, opts.TopK, opts.EfSearch, p.options.SearchCandidates, buffer)
		}
	}
	if err != nil {
		return response, nil, err
	}
	if opts.Context != nil {
		if err = opts.Context.Err(); err != nil {
			return response, nil, err
		}
	}
	response.IndexName, response.Strategy, response.Path = p.index, VectorIndexStrategyColumnGraph, VectorIndexSearchPathColumnGraphNativeReader
	response.Stats = vectorIndexSearchStatsFromInternal(stats.Base, owner.overlay.base.reader.Stats())
	response.Stats.ColumnGraphOwnerAcquireNanos = acquired.Nanoseconds()
	response.Stats.ColumnGraphDeltaScored = uint64(stats.DeltaScored)
	response.Stats.SearchRouteColumnGraphPrepared = 1
	if stats.PackMmapDirect {
		response.Stats.HNSWSearchPackMmapDirect = 1
	}
	if stats.Route == "typed_hnsw" {
		response.Stats.SearchRouteHNSWSearchPack = 1
	}
	response.Status = VectorIndexStatus{Name: p.index, Definition: owner.overlay.base.reader.def, Strategy: response.Strategy, Loaded: true, State: VectorIndexStateColumnGraphLoaded}
	view = owner.overlay.current
	view.typedGraphOwner = owner
	return response, view, nil
}
