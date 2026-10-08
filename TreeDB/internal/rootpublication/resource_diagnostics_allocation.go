package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"math"
	"slices"
	"sync/atomic"
	"time"
	"unsafe"
)

// ResourceDiagnostics owns admitted diagnostic arrays and the exact immutable
// descriptor edge. Values, their strings, fields, namespace and RID/frontier
// backing are read-only borrows until Close. They must not escape that lifetime;
// constructors retaining them must independently admit and copy their backing.
// Generic/nil construction keeps existing detached diagnostic semantics.
type ResourceDiagnostics struct {
	closed         atomic.Bool
	allocation     *resourceAllocation
	borrow         resourceKindBorrow
	emptyDirectory *DependencyDirectoryV2
	physical       []StableResourcePhysicalDescriptor
	stats          []ResourceKindStats
}

func (d *ResourceDiagnostics) Physical() []StableResourcePhysicalDescriptor {
	if d == nil || d.closed.Load() {
		return nil
	}
	return d.physical
}
func (d *ResourceDiagnostics) Stats() []ResourceKindStats {
	if d == nil || d.closed.Load() {
		return nil
	}
	return d.stats
}
func (d *ResourceDiagnostics) Close() {
	if d == nil || !d.closed.CompareAndSwap(false, true) {
		return
	}
	for i := range d.physical {
		clear(d.physical[i].reachability)
		d.physical[i] = StableResourcePhysicalDescriptor{}
	}
	clear(d.physical)
	clear(d.stats)
	d.physical = nil
	d.stats = nil
	d.borrow.release()
	d.borrow = resourceKindBorrow{}
	if d.emptyDirectory != nil {
		d.emptyDirectory.Release()
		d.emptyDirectory = nil
	}
	allocation := d.allocation
	d.allocation = nil
	if allocation != nil && allocation.drop() {
		allocation.refund()
	}
}
func diagnosticChargeAdd(total *uint64, count, width uint64) error {
	if count > math.MaxUint64/width {
		return retainedalloc.ErrCapacity
	}
	next := retainedalloc.AllocationCharge(count * width)
	if next > math.MaxUint64-*total {
		return retainedalloc.ErrCapacity
	}
	*total += next
	return nil
}

// An empty selected directory owns a real directory lease but has no kind
// backing. Admit its diagnostic wrapper and retain that exact lease under the
// set guard; the absence of physical descriptors is not an ownership failure.
func (set *StableResourceSet) acquireEmptyDirectoryDiagnostics() (*ResourceDiagnostics, bool, error) {
	set.mu.Lock()
	defer set.mu.Unlock()
	if !set.selectedKindViewsAccessibleLocked() {
		return nil, true, ErrResourceOwnership
	}
	directory := set.emptyDependencyDirectoryLocked()
	if directory == nil {
		return nil, false, nil
	}
	if set.metadata == nil {
		return nil, true, ErrResourceOwnership
	}
	allocation, err := newResourceAllocation(set.metadata.owner, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ResourceDiagnostics{}))))
	if err != nil {
		return nil, true, err
	}
	if err = directory.Retain(); err != nil {
		if allocation.drop() {
			allocation.refund()
		}
		return nil, true, err
	}
	return &ResourceDiagnostics{allocation: allocation, emptyDirectory: directory}, true, nil
}

// AcquirePhysicalDiagnostics performs no logical directory reads. Full actual
// scratch is admitted before allocation; refusal leaves the source unchanged.
func (set *StableResourceSet) AcquirePhysicalDiagnostics() (*ResourceDiagnostics, error) {
	if set == nil {
		return nil, nil
	}
	if !set.selected {
		return &ResourceDiagnostics{physical: set.PhysicalDescriptors()}, nil
	}
	if d, handled, err := set.acquireEmptyDirectoryDiagnostics(); handled {
		return d, err
	}
	borrow, err := set.borrowSelectedKindViews()
	if err != nil {
		return nil, err
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ResourceDiagnostics{})))
	count := stableResourceKindViewCount(borrow.backing)
	if err = diagnosticChargeAdd(&charge, uint64(count), uint64(unsafe.Sizeof(StableResourcePhysicalDescriptor{}))); err != nil {
		borrow.release()
		return nil, err
	}
	for _, view := range borrow.backing.all() {
		rangeStableResourceLogicalIndex(view.logical, func(entry *stableResourceEntry) bool {
			if err != nil {
				return false
			}
			err = diagnosticChargeAdd(&charge, uint64(entry.reachability.reachableCount()), uint64(unsafe.Sizeof(ReachabilityField(""))))
			if err == nil && entry.token.namespace != nil {
				err = diagnosticChargeAdd(&charge, 1, uint64(unsafe.Sizeof(StableNamespaceDescriptor{})))
			}
			return err == nil
		})
	}
	if err != nil {
		borrow.release()
		return nil, err
	}
	allocation, err := newResourceAllocation(borrow.backing.allocation.owner, charge)
	if err != nil {
		borrow.release()
		return nil, err
	}
	d := &ResourceDiagnostics{allocation: allocation, borrow: borrow, physical: make([]StableResourcePhysicalDescriptor, 0, count)}
	for _, view := range borrow.backing.all() {
		rangeStableResourceLogicalIndex(view.logical, func(entry *stableResourceEntry) bool {
			fields := make([]ReachabilityField, 0, entry.reachability.reachableCount())
			for field := range entry.reachability.reachableFields() {
				fields = append(fields, field)
			}
			slices.Sort(fields)
			value := StableResourcePhysicalDescriptor{scoped: true, Kind: entry.token.kind, Generation: entry.token.generation, LogicalObligationCount: uint64(entry.logicalObligations.count), LogicalObligationCountAvailable: !set.physicalOnly, logicalLane: entry.logicalLane, resourceID: entry.resourceID, diagnosticPath: entry.diagnosticPath, identity: entry.token.identity, digest: entry.token.digest, frontier: entry.frontier, reachability: fields}
			if n := entry.token.namespace; n != nil {
				value.namespace = &StableNamespaceDescriptor{ParentIdentity: n.parentIdentity, Operation: n.operation, OldName: n.oldName, NewName: n.newName, DiagnosticPath: n.diagnosticPath}
			}
			d.physical = append(d.physical, value)
			return true
		})
	}
	return d, nil
}

// AcquireStats retains the same real descriptor/operation custody through the
// returned diagnostic owner. Selected aggregation uses admitted fixed arrays,
// not hidden maps or detached borrowed kind strings.
func (set *StableResourceSet) AcquireStats(now time.Time) (*ResourceDiagnostics, error) {
	if set == nil {
		return nil, nil
	}
	if !set.selected {
		return &ResourceDiagnostics{stats: set.Stats(now)}, nil
	}
	if d, handled, err := set.acquireEmptyDirectoryDiagnostics(); handled {
		return d, err
	}
	borrow, err := set.borrowSelectedKindViews()
	if err != nil {
		return nil, err
	}
	count := stableResourceKindViewCount(borrow.backing)
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ResourceDiagnostics{})))
	if err = diagnosticChargeAdd(&charge, uint64(borrow.backing.len()), uint64(unsafe.Sizeof(ResourceKindStats{}))); err != nil {
		borrow.release()
		return nil, err
	}
	var scratch uint64
	if err = diagnosticChargeAdd(&scratch, uint64(count), uint64(unsafe.Sizeof((*StableNamespaceToken)(nil)))); err != nil {
		borrow.release()
		return nil, err
	}
	owner := borrow.backing.allocation.owner
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		borrow.release()
		return nil, err
	}
	if err = owner.AddPending(scratch); err != nil {
		borrow.release()
		if allocation.drop() {
			allocation.refund()
		}
		return nil, err
	}
	namespaces := make([]*StableNamespaceToken, 0, count)
	defer func() { clear(namespaces); namespaces = nil; owner.RemovePending(scratch) }()
	d := &ResourceDiagnostics{allocation: allocation, borrow: borrow, stats: make([]ResourceKindStats, 0, borrow.backing.len())}
	for kind, view := range borrow.backing.all() {
		stats := ResourceKindStats{Kind: kind, LogicalObligationCountAvailable: !set.physicalOnly, PinHighWater: uint64(view.count)}
		rangeStableResourceLogicalIndex(view.logical, func(entry *stableResourceEntry) bool {
			token := activeEntryToken(*entry)
			if token == nil {
				return true
			}
			stats.PendingCount++
			stats.PendingBytes = saturatingAdd(stats.PendingBytes, entry.frontier.Bytes)
			stats.LogicalObligationCount = saturatingAdd(stats.LogicalObligationCount, uint64(entry.logicalObligations.count))
			age := now.Sub(time.Unix(0, token.metrics.registeredNanos))
			if age > stats.PendingAge {
				stats.PendingAge = age
			}
			stats.Flushes += token.metrics.flushes.Load()
			stats.FlushDuration += time.Duration(token.metrics.flushNanos.Load())
			stats.Syncs += token.metrics.syncs.Load()
			stats.SyncDuration += time.Duration(token.metrics.syncNanos.Load())
			stats.PhysicalFileSyncs += token.metrics.physicalFileSyncs.Load()
			stats.PhysicalFileSyncDuration += time.Duration(token.metrics.physicalFileSyncNanos.Load())
			if token.namespace != nil {
				seen := false
				for _, n := range namespaces {
					if n == token.namespace {
						seen = true
						break
					}
				}
				if !seen {
					namespaces = append(namespaces, token.namespace)
					syncs, duration := token.namespace.physicalSyncStats()
					stats.NamespaceSyncs += syncs
					stats.NamespaceSyncDuration += duration
				}
			}
			active := false
			if len(entry.pins) == 0 {
				active = entry.token != nil && !entry.token.released.Load()
			} else {
				for _, pin := range entry.pins {
					if pin != nil && !pin.released.Load() {
						active = true
						break
					}
				}
			}
			if active {
				stats.ActivePins++
			}
			return true
		})
		d.stats = append(d.stats, stats)
	}
	return d, nil
}

// HasKind queries existing immutable entries without exporting diagnostic
// backing or allocating a temporary descriptor collection.
func (set *StableResourceSet) HasKind(kind ResourceKind) bool {
	if set == nil {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if !set.selectedKindViewsAccessibleLocked() {
		return false
	}
	if set.kindViews != nil {
		view, found := set.kindViews.lookup(kind)
		return found && view.count > 0
	}
	for i := range set.entries {
		if set.entries[i].token.kind == kind {
			return true
		}
	}
	return false
}

// StableResourceTelemetryStats copies scalar metrics into the existing detached
// telemetry API. Selected construction and aggregation retain admitted metadata
// until the copy completes. Only canonical built-in kind names may escape;
// no selected descriptor, string backing, token or proof is returned.
func StableResourceTelemetryStats(now time.Time, physicalOnly bool, sets ...*StableResourceSet) ([]ResourceKindStats, error) {
	var owner *retainedalloc.Owner
	for _, set := range sets {
		if set == nil {
			continue
		}
		set.mu.Lock()
		if set.selected && !set.selectedKindViewsAccessibleLocked() {
			set.mu.Unlock()
			return nil, ErrResourceOwnership
		}
		if set.selected && set.metadata != nil && owner == nil {
			owner = set.metadata.owner
		}
		set.mu.Unlock()
	}
	if owner == nil {
		mode := stableResourceViewPinned
		if physicalOnly {
			mode = stableResourceViewPhysicalPinned
		}
		view, err := unionStableResourceSets(mode, sets...)
		if err != nil {
			return nil, err
		}
		return view.Stats(now), nil
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(owner)
	if err != nil {
		return nil, err
	}
	defer builder.Abandon()
	for _, set := range sets {
		if set == nil {
			continue
		}
		imported, importErr := ImportStableResourceSetMetadata(owner, set)
		if importErr != nil {
			return nil, importErr
		}
		mergeErr := builder.Merge(imported)
		imported.Release()
		if mergeErr != nil {
			return nil, mergeErr
		}
	}
	view, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	defer view.Release()
	diagnostics, err := view.AcquireStats(now)
	if err != nil {
		return nil, err
	}
	defer diagnostics.Close()
	// The result is the pre-existing detached scalar telemetry allocation, not
	// a selected diagnostic borrow. Validate every kind before allocating it.
	for _, value := range diagnostics.Stats() {
		if _, ok := canonicalTelemetryKind(value.Kind); !ok {
			return nil, ErrResourceOwnership
		}
	}
	result := make([]ResourceKindStats, len(diagnostics.Stats()))
	for i, value := range diagnostics.Stats() {
		value.Kind, _ = canonicalTelemetryKind(value.Kind)
		if physicalOnly {
			value.LogicalObligationCount = 0
			value.LogicalObligationCountAvailable = false
		}
		result[i] = value
	}
	slices.SortFunc(result, func(a, b ResourceKindStats) int {
		if a.Kind < b.Kind {
			return -1
		}
		if a.Kind > b.Kind {
			return 1
		}
		return 0
	})
	return result, nil
}

func canonicalTelemetryKind(kind ResourceKind) (ResourceKind, bool) {
	switch kind {
	case ResourceIndex:
		return ResourceIndex, true
	case ResourceValueLog:
		return ResourceValueLog, true
	case ResourceOuterLeafLog:
		return ResourceOuterLeafLog, true
	case ResourceOuterLeafManifest:
		return ResourceOuterLeafManifest, true
	case ResourceOuterLeafPack:
		return ResourceOuterLeafPack, true
	case ResourceDictionary:
		return ResourceDictionary, true
	case ResourceTemplate:
		return ResourceTemplate, true
	case ResourceColumnAsset:
		return ResourceColumnAsset, true
	case ResourceTypedColumnAsset:
		return ResourceTypedColumnAsset, true
	case ResourceVectorGraphPack:
		return ResourceVectorGraphPack, true
	case ResourceLegacyVectorSnapshot:
		return ResourceLegacyVectorSnapshot, true
	case ResourceCommandWAL:
		return ResourceCommandWAL, true
	case ResourceCommandWALExternalRID:
		return ResourceCommandWALExternalRID, true
	case ResourceQueryReadyAsset:
		return ResourceQueryReadyAsset, true
	case ResourceSeparateDurability:
		return ResourceSeparateDurability, true
	case ResourceLegacyTreeDBField:
		return ResourceLegacyTreeDBField, true
	default:
		return kind, false
	}
}
