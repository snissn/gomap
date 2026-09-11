package collections

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

type typedGraphCachedFilter struct {
	filter *typedGraphBaseFilter
	limits typedGraphFilterLimits
	ready  chan struct{}
}

// Borrow only an already-warmed keeper matching this independently pinned owner.
// Never hold the collection cache mutex while taking the prepared-object lock.
// TryRLock lets retirement bypass new readers; Close waits for active queries.
func (c *Collection) borrowTypedGraphFilterKeeper(owner *typedGraphReadOwner, index string) *collectionVectorIndexPreparedSearch {
	slot := collectionVectorIndexPreparedSearchCacheSlot{family: collectionVectorIndexPreparedSearchFamilyCapturedBase, indexName: index}
	c.vectorBufferedSearchMu.Lock()
	var p *collectionVectorIndexPreparedSearch
	if entry := c.vectorBufferedSearch[slot]; entry != nil && !entry.building {
		p = entry.prepared
	}
	c.vectorBufferedSearchMu.Unlock()
	if p == nil || !p.mu.TryRLock() {
		return nil
	}
	ref := owner.overlay.base.reader.sharedPreparedSearch
	if p.closed || p.capturedBase == nil || p.capturedBase.ref == nil || ref == nil || ref.holder != p.capturedBase.ref.holder || owner.accounting != p.capturedBase.accounting {
		p.mu.RUnlock()
		return nil
	}
	return p
}

// The caller holds keeper.mu.RLock through scoring and ID materialization.
func prepareTypedGraphServingFilter(ctx context.Context, keeper *collectionVectorIndexPreparedSearch, overlay *typedGraphOverlaySearch, filter HybridScalarFilter, limits typedGraphFilterLimits, workOut *ColumnGraphFilterWork) (_ *typedGraphPreparedFilter, err error) {
	work := ColumnGraphFilterWork{Attempted: true}
	workstats.Graph.Filters.Attempts.Add(1)
	defer func() {
		work.Completed = err == nil
		*workOut = work
		recordTypedGraphFilterWork(work)
	}()
	if keeper == nil {
		return prepareTypedGraphFilterUnmetered(ctx, overlay, filter, limits, &work)
	}
	candidate, compileErr := compileTypedGraphBaseFilter(overlay.base, filter, typedGraphBaseFilterLimits{typedGraphFilterLimits: limits, Clauses: hybridScalarMaxConjuncts, PredicateBytes: min(limits.SourceBytes, limits.MappingWork)})
	if compileErr != nil {
		// Optional reuse supports the existing typed string predicate subset.
		// Unsupported leaves keep the established current-snapshot preparation.
		return prepareTypedGraphFilterUnmetered(ctx, overlay, filter, limits, &work)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	candidate.schemaHash = overlay.base.catalog.meta.Options.ColumnStore.SchemaHash
	r := keeper.capturedBase
	var entry *typedGraphCachedFilter
	for {
		r.filtersMu.Lock()
		var empty *typedGraphCachedFilter
		for i := range r.filters {
			e := &r.filters[i]
			if e.filter == nil {
				if empty == nil {
					empty = e
				}
			} else if e.limits == limits && e.filter.schemaHash == candidate.schemaHash && reflect.DeepEqual(e.filter.predicates, candidate.predicates) {
				entry = e
				break
			}
		}
		if entry != nil {
			ready, base := entry.ready, entry.filter
			r.filtersMu.Unlock()
			if ready != nil {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-ready:
				}
				entry = nil
				continue
			}
			// ponytail: source-ID counts estimate preparation cost; use measured
			// weighted costs only if similarly sized plans need finer selection.
			if d := len(overlay.rows); d > 0 && d >= base.plan.sourceIDs {
				return prepareTypedGraphFilterUnmetered(ctx, overlay, filter, limits, &work)
			}
			return bindTypedGraphServingFilter(ctx, base, overlay, limits, &work)
		}
		if empty == nil {
			r.filtersMu.Unlock()
			return prepareTypedGraphFilterUnmetered(ctx, overlay, filter, limits, &work)
		}
		entry = empty
		*entry = typedGraphCachedFilter{filter: candidate, limits: limits, ready: make(chan struct{})}
		r.filtersMu.Unlock()
		break
	}
	installed := false
	defer func() {
		r.filtersMu.Lock()
		ready := entry.ready
		if installed {
			entry.ready = nil
		} else {
			*entry = typedGraphCachedFilter{}
		}
		close(ready)
		r.filtersMu.Unlock()
	}()
	if len(overlay.rows) == 0 {
		// The current owner already validated this base-only overlay. Reuse it
		// instead of rebuilding and comparing the same manifest views again.
		candidate.plan, err = prepareTypedGraphFilterUnmetered(ctx, overlay, filter, limits, &work)
	} else {
		err = candidate.prepare(ctx, overlay.base, filter, limits, &work)
	}
	if err != nil {
		return nil, err
	}
	// Detached plans contain only owned immutable selections and predicates, never
	// a request searcher, snapshot, catalog, suffix or current materializer.
	candidate.holder = r.ref.holder
	candidate.plan.overlay = nil
	plan, err := bindTypedGraphServingFilter(ctx, candidate, overlay, limits, &work)
	if err != nil {
		return nil, err
	}
	navigation, navigationErr := buildTypedGraphFilterNavigation(ctx, overlay, candidate.plan, limits.RetainedBytes-int(work.RetainedBytes))
	if navigationErr == nil {
		candidate.navigation = navigation
		work.RetainedBytes += uint64(navigation.retainedBytes)
	} else if !errors.Is(navigationErr, errTypedGraphFilterNavigationDeclined) {
		return nil, navigationErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// This backing becomes keeper-owned only after successful preparation/bind.
	// Until admission it remains bounded temporary work owned by this request.
	n := int64(candidate.plan.retainedBytes) + int64(reflect.TypeFor[typedGraphBaseFilter]().Size()+reflect.TypeFor[typedGraphPreparedFilter]().Size())
	if candidate.navigation != nil {
		n += int64(candidate.navigation.retainedBytes)
	}
	n += int64(cap(candidate.predicates)) * int64(reflect.TypeFor[typedGraphScalarPredicate]().Size())
	for _, p := range candidate.predicates {
		n += int64(cap(p.clause.lower) + cap(p.clause.upper) + len(p.definition.Name) + len(p.definition.Field) + len(p.definition.ValueType) + len(p.definition.StoragePolicy))
	}
	a := r.accounting
	a.Lock()
	remaining := a.limits.StateBytes - a.stateBytes - a.baseDescriptorBytes - a.baseBackingBytes
	if n > remaining && candidate.navigation != nil {
		n -= int64(candidate.navigation.retainedBytes)
		work.RetainedBytes -= uint64(candidate.navigation.retainedBytes)
		candidate.navigation = nil
	}
	if n <= remaining {
		a.baseBackingBytes += n
		r.backingBytes += n
		installed = true
	}
	a.Unlock()
	if installed {
		r.filtersMu.Lock()
		for i := range candidate.predicates {
			p := &candidate.predicates[i]
			p.definition.Name = strings.Clone(p.definition.Name)
			p.definition.Field = strings.Clone(p.definition.Field)
			p.definition.ValueType = IndexValueType(strings.Clone(string(p.definition.ValueType)))
			p.definition.StoragePolicy = RootStoragePolicy(strings.Clone(string(p.definition.StoragePolicy)))
			p.clause.indexName = p.definition.Name
			p.clause.valueType = p.definition.ValueType
			if p.definition.Components != nil {
				p.definition.Components = make([]IndexComponent, 0)
			}
		}
		r.filtersMu.Unlock()
	}
	return plan, nil
}

func bindTypedGraphServingFilter(ctx context.Context, base *typedGraphBaseFilter, overlay *typedGraphOverlaySearch, limits typedGraphFilterLimits, work *ColumnGraphFilterWork) (*typedGraphPreparedFilter, error) {
	// Cold work and suffix binding share one request budget. Hits do not replay
	// historical posting counters or recharge keeper-owned selection capacity.
	bind := typedGraphFilterBindLimits{Rows: limits.SourceIDs - int(work.SourceIDs), IDBytes: limits.SourceBytes - int(work.SourceBytes), ValueBytes: limits.MappingWork - int(work.MappingWorkCharged), MappingWork: limits.MappingWork - int(work.MappingWorkCharged), PredicateWork: limits.MappingWork - int(work.MappingWorkCharged), RetainedBytes: limits.RetainedBytes - int(work.RetainedBytes), ExactScanRows: limits.SourceIDs}
	var current ColumnGraphFilterWork
	plan, err := bindTypedGraphBaseFilterWithContext(ctx, base, overlay, bind, &current)
	work.EligibleRows = current.EligibleRows
	work.SourceIDs += current.SourceIDs
	work.SourceBytes += current.SourceBytes
	work.InspectedEntries += current.InspectedEntries
	work.MappingWorkCharged += current.MappingWorkCharged
	work.OrdinalGrowthPeakBytes = max(work.OrdinalGrowthPeakBytes, work.RetainedBytes+current.OrdinalGrowthPeakBytes)
	work.RetainedBytes += current.RetainedBytes
	work.ScratchIDBytes = max(work.ScratchIDBytes, current.ScratchIDBytes)
	work.ScratchRows = max(work.ScratchRows, current.ScratchRows)
	return plan, err
}
