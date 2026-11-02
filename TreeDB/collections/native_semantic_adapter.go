package collections

import (
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var _ backenddb.NativeSemanticState = (*nativeEngineVersion)(nil)

func (e *nativeEngineVersion) SparseProjections() []rootpublication.ParticipantProjection { return nil }
func (e *nativeEngineVersion) PrepareRollover() (backenddb.NativeSemanticState, error) {
	next, err := prepareNativeEngineRollover(e)
	if err != nil {
		err = fmt.Errorf("collections: native rollover: %w", err)
	}
	if next == nil {
		return nil, err
	}
	return next, err
}
func (e *nativeEngineVersion) PrepareBuild(group *backenddb.RootPublicationBuildGroup, basis *backenddb.Snapshot, lease *backenddb.StableResourceCaptureLease) error {
	if basis == nil || lease == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	build, err := prepareNativeEngineBuildAt(e, basis, lease, group)
	if err != nil {
		if refusal, ok := err.(backenddb.NativeSemanticBuildRetryableRefusal); ok {
			refusal.Cause = fmt.Errorf("collections: native build prepare: %w", refusal.Cause)
			return refusal
		}
		return fmt.Errorf("collections: native build prepare: %w", err)
	}
	if err = build.materializeDomains(group, e.names); err != nil {
		return fmt.Errorf("collections: native build materialize: %w", err)
	}
	return nil
}

// The two captured-flush callers are installPreparedCOWBasis in caching/cow_flush.go.
// Both receive basis.snapshot from the SAME group's preaccepted h.basis adopter;
// its accepted-cut receipt and retained opening cut establish exact physical
// identity. This adapter never recaptures a Snapshot or creates another authority.
// Type/non-nil checks here and the original complete/two-call/domain checks in
// build.prepareHandoff preserve that lower caller contract.
func (e *nativeEngineVersion) PrepareHandoff(latest backenddb.NativeSemanticState, accepted *backenddb.Snapshot) (backenddb.NativeSemanticState, error) {
	current, ok := latest.(*nativeEngineVersion)
	if !ok || accepted == nil || e.build == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	next, err := e.build.prepareHandoff(current)
	if err != nil {
		if refusal, ok := err.(backenddb.NativeSemanticHandoffRetryableRefusal); ok {
			refusal.Cause = fmt.Errorf("collections: native handoff: %w", refusal.Cause)
			return nil, refusal
		}
		return nil, fmt.Errorf("collections: native handoff: %w", err)
	}
	return next, nil
}

// PrepareBasisRefresh is independent of the single-use captured flush ledger.
// Maintenance supplies the exact paired Snapshots and consumes no source prefix.
func (e *nativeEngineVersion) PrepareBasisRefresh(before, after *backenddb.Snapshot) (backenddb.NativeSemanticState, error) {
	next, err := prepareNativeEngineBasisRefresh(e, before, after)
	if err != nil {
		err = fmt.Errorf("collections: native basis refresh: %w", err)
	}
	if next == nil {
		return nil, err
	}
	return next, err
}

var _ backenddb.NativeSemanticBasisRefresher = (*nativeEngineVersion)(nil)

// Leaf packing preserves decoded entries and the exact original asset refs and
// codecs. Only its producer-qualified before/after receipt authorizes rebinding
// accepted graph markers; equal LSNs or a generic physical refresh do not.
func (e *nativeEngineVersion) PrepareLeafPackBasisRefresh(before, after *backenddb.Snapshot, transition backenddb.LeafGenerationPackTransition) (backenddb.NativeSemanticState, error) {
	if err := transition.Validate(before, after); err != nil {
		return nil, err
	}
	next, err := prepareNativeEnginePhysicalBasisRefresh(e, before, after, true)
	if err != nil {
		err = fmt.Errorf("collections: native leaf-pack basis refresh: %w", err)
	}
	if next == nil {
		return nil, err
	}
	return next, err
}

var _ backenddb.NativeSemanticLeafPackBasisRefresher = (*nativeEngineVersion)(nil)
