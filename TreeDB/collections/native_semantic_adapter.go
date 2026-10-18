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
