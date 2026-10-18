package collections

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var _ backenddb.NativeSemanticState = (*nativeEngineVersion)(nil)

func (e *nativeEngineVersion) SparseProjections() []rootpublication.ParticipantProjection { return nil }
func (e *nativeEngineVersion) PrepareRollover() (backenddb.NativeSemanticState, error) {
	next, err := prepareNativeEngineRollover(e)
	if next == nil {
		return nil, err
	}
	return next, err
}
func (e *nativeEngineVersion) PrepareBuild(group *backenddb.RootPublicationBuildGroup, basis *backenddb.Snapshot, lease *backenddb.StableResourceCaptureLease) error {
	if basis == nil || lease == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	build, err := prepareNativeEngineBuild(e)
	if err != nil {
		return err
	}
	return build.materializeDomains(group)
}

// The two production callers are installPreparedCOWBasis in caching/cow_flush.go.
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
		return nil, err
	}
	return next, nil
}
