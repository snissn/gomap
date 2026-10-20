package rootpublication

import (
	"errors"
	"slices"
	"strings"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// Complete requirements deliberately traverse the live closure. Two passes
// admit then copy only the unwanted directory records, including their borrowed
// strings. Each pass uses the maintained stream (one scan per distinct pinned
// directory), not a directory scan per physical entry or a flattened history.
// The original kind borrow owns every entry pointer through plan.close.
func prepareOwnedDirectoryRequirementsV2(source *StableResourceSet, views *resourceKindBacking, r StableLogicalObligationRequirements, excluded []ResourceKind, selectedKind ResourceKind, selected []StableIdentity, work *StableResourceClosureWork) (plan ownedDirectoryRemovalPlanV2, retErr error) {
	owner := views.allocation.owner
	rangeStableResourceKindViews(views, func(entry *stableResourceEntry) bool {
		if entry.logicalObligations.directory != nil && plan.directory == nil {
			plan.directory = entry.logicalObligations.directory
		}
		return true
	})
	if len(r.ScopedFields)+len(r.ScopedNamespaces) == 0 {
		return plan, nil // Physical-only selection does not inspect logical history.
	}
	diagnostics, err := source.AcquirePhysicalDiagnostics()
	if err != nil {
		return plan, err
	}
	defer func() {
		retErr = errors.Join(retErr, diagnostics.CloseWithError())
		if retErr != nil {
			plan.close()
		}
	}()
	if diagnostics.allocation == nil || diagnostics.allocation.owner != owner {
		return plan, ErrResourceOwnership
	}
	// Kind and physical selection precede even the unwanted-record admission.
	// These synchronous values remain owned by the original diagnostics/borrow.
	unwanted := func(physical StableResourcePhysicalDescriptor, o StableLogicalObligation) (bool, error) {
		if resourceKindExcluded(physical.Kind, excluded) || !requirementContainsScope(r, o) {
			return false, nil
		}
		if physical.Kind == selectedKind && !slices.Contains(selected, physical.identity) {
			return false, nil
		}
		keep, err := requirementHasValue(r, o)
		return !keep, err
	}
	entryFor := func(physical StableResourcePhysicalDescriptor) *stableResourceEntry {
		view, ok := views.lookup(physical.Kind)
		if !ok {
			return nil
		}
		return findStableResourceLogical(view.logical, stableLogicalResourceKey{kind: physical.Kind, lane: physical.logicalLane, resourceID: physical.resourceID, generation: physical.Generation})
	}
	count, charge := 0, uint64(0)
	if err := diagnostics.WalkLogicalObligations(func(physical StableResourcePhysicalDescriptor, o StableLogicalObligation) error {
		work.SourceObligationsInspected++
		drop, err := unwanted(physical, o)
		if err != nil || !drop {
			return err
		}
		entry := entryFor(physical)
		if entry == nil || entry.outgoing == nil || entry.outgoing.allocation.owner != owner {
			return ErrResourceOwnership
		}
		if entry.logicalObligations.directory == nil {
			return nil // The unchanged flat filtering path handles this entry.
		}
		if count == int(^uint(0)>>1)/int(unsafe.Sizeof(ownedDirectoryDeltaIdentityV2{})) {
			return retainedalloc.ErrCapacity
		}
		count++
		for _, text := range [...]string{o.Class, o.Kind, o.Namespace, string(o.Reachability)} {
			if err := diagnosticChargeAdd(&charge, uint64(len(text)), 1); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return plan, err
	}
	if err := diagnosticChargeAdd(&charge, uint64(count), uint64(unsafe.Sizeof(ownedDirectoryDeltaIdentityV2{}))); err != nil {
		return plan, err
	}
	plan.allocation, err = newResourceAllocation(owner, charge)
	if err != nil {
		return plan, err
	}
	plan.values = make([]ownedDirectoryDeltaIdentityV2, 0, count)
	if err := diagnostics.WalkLogicalObligations(func(physical StableResourcePhysicalDescriptor, o StableLogicalObligation) error {
		work.SourceObligationsInspected++
		drop, err := unwanted(physical, o)
		if err != nil || !drop {
			return err
		}
		entry := entryFor(physical)
		if entry == nil || entry.outgoing == nil || entry.outgoing.allocation.owner != owner {
			return ErrResourceOwnership
		}
		if entry.logicalObligations.directory == nil {
			return nil
		}
		if len(plan.values) == cap(plan.values) {
			return ErrResourceOwnership
		}
		o.Class, o.Kind = strings.Clone(o.Class), strings.Clone(o.Kind)
		o.Namespace = strings.Clone(o.Namespace)
		o.Reachability = ReachabilityField(strings.Clone(string(o.Reachability)))
		plan.values = append(plan.values, ownedDirectoryDeltaIdentityV2{entry: entry, obligation: o})
		return nil
	}); err != nil {
		return plan, err
	}
	if len(plan.values) != count {
		return plan, ErrResourceOwnership
	}
	slices.SortFunc(plan.values, func(a, b ownedDirectoryDeltaIdentityV2) int {
		if c := compareRemovalEntryV2(a.entry, b.entry); c != 0 {
			return c
		}
		return compareResourceObligation(a.obligation, b.obligation)
	})
	return plan, nil
}
