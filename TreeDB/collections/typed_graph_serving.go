package collections

import (
	"context"
	"errors"
	"time"
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
	if wasReady && coord.typedPublication.Load() == before {
		return nil
	}
	prepared := coord.typedPublication.Load()
	if _, err := c.renewTypedGraphWorkEpoch(ctx, m); err != nil {
		return err
	}
	if _, err := c.acquireTypedGraphCapturedBaseCache(index, o); err != nil {
		return err
	}
	ready := *prepared
	ready.servingAdmitted = true
	if !coord.typedPublication.CompareAndSwap(prepared, &ready) {
		return ErrConcurrentMutation
	}
	return nil
}

// FoldColumnGraphServing performs explicit bounded rebuild and maintenance.
// Errors preserve fail-closed state and are never hidden by a retry loop.
func (c *Collection) FoldColumnGraphServing(ctx context.Context, index string) error {
	p := c.typedGraphServingPolicy()
	if p == nil || p.index != index || ctx == nil {
		return ErrVectorIndexSearchUnavailable
	}
	if err := c.foldTypedGraph(ctx, p.options.Owners.Cold, p.options.FoldRows, p.options.CandidateOutput, nil); err != nil {
		return err
	}
	return c.EnsureColumnGraphServing(ctx, index, p.options)
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
	defer func() {
		if err != nil {
			err = errors.Join(err, owner.Close())
			buffer.Reset()
		}
	}()
	var stats typedGraphOverlaySearchStats
	if opts.DeclaredScalarFilter == nil {
		response.Results, stats, err = owner.overlay.search(opts.Query, opts.TopK, opts.EfSearch, p.options.SearchCandidates, buffer)
	} else {
		var filter *typedGraphPreparedFilter
		filter, err = prepareTypedGraphFilter(owner.overlay, *opts.DeclaredScalarFilter, p.options.Filter)
		if err == nil {
			response.Results, stats, err = owner.overlay.searchPreparedFilter(filter, opts.Query, opts.TopK, opts.EfSearch, p.options.SearchCandidates, buffer)
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
	if !stats.FilteredExact {
		response.Stats.SearchRouteHNSWSearchPack = 1
	}
	response.Status = VectorIndexStatus{Name: p.index, Definition: owner.overlay.base.reader.def, Strategy: response.Strategy, Loaded: true, State: VectorIndexStateColumnGraphLoaded}
	view = owner.overlay.current
	view.typedGraphOwner = owner
	return response, view, nil
}
