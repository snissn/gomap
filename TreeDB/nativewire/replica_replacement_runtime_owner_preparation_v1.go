package nativewire

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func (r *FixedPeerTCPRuntimeV1) validateImmutableOwnerPreparationV1(command raftplacement.ReplicaReplacementBeginV1) error {
	vector := r.config.Vector
	if vector == nil || r.config.Credentials == nil || command.OwnerPreparation == nil || *command.OwnerPreparation != vector.Identity || vector.Identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) || !slices.Contains(fixedPeerVectorOwnerGroupsV1(vector.Placement), command.GroupID) {
		return raftcluster.ErrUnsupportedFeature
	}
	if command.OldNodeID == vector.RouterNodeID || command.NewPeer.ID == vector.RouterNodeID {
		return raftcluster.ErrUnsupportedFeature
	}
	for _, group := range vector.Catalog.Placements {
		if group.GroupID == command.GroupID {
			return raftcluster.ErrUnsupportedFeature
		}
	}
	for _, peer := range r.config.Catalog.Peers {
		if peer.ID == command.NewPeer.ID || peer.ID == command.OldNodeID {
			return raftcluster.ErrUnsupportedFeature
		}
	}
	for _, group := range r.config.Groups {
		for _, peer := range group.Peers {
			if peer.ID == command.NewPeer.ID {
				return raftcluster.ErrUnsupportedFeature
			}
		}
	}
	return nil
}

// This is an inspection of actual installed bytes, not copied catalog READY.
// The reader follows the FSM's current DB across restore under its storage lock.
// No manager, topology, listener, warm cache, or readiness grant is created.
func (r *FixedPeerTCPRuntimeV1) verifyReplacementHostedOwnerV1(ctx context.Context, state raftplacement.ReplicaReplacementStateV1, minimumIndex uint64) error {
	if state.Begin.OwnerPreparation == nil {
		return nil
	}
	if err := r.validateImmutableOwnerPreparationV1(state.Begin); err != nil {
		return err
	}
	if r.config.NodeID != state.Begin.NewPeer.ID || state.Seed == nil {
		return raftcluster.ErrAdmissionUnavailable
	}
	reply, err := r.catalogConsumerCall(ctx, "vector-catalog-read", fixedPeerRequestV1{VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: fixedPeerVectorCatalogActiveV1}})
	if err != nil {
		return err
	}
	if reply.VectorCatalog == nil {
		return ErrFixedPeerVectorProofStaleV1
	}
	if err := r.validateImmutableVectorCatalogDecisionV1(reply.Catalog, *reply.VectorCatalog, fixedPeerVectorCatalogActiveV1); err != nil {
		return err
	}
	d := r.localDataV1(state.Begin.GroupID)
	if d == nil || d.fsm == nil {
		return raftcluster.ErrRouteTargetUnknown
	}
	identity := *state.Begin.OwnerPreparation
	meta, manifest, scope, err := d.fsm.PreparedVectorPartitionScopedManifestFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: r.config.NodeID, GroupID: state.Begin.GroupID, MinAppliedIndex: minimumIndex}, identity.Index.Collection.Collection, identity.Index.IndexName, identity.Generation)
	if err != nil {
		return err
	}
	if err := fixedPeerVectorImmutableDefinitionV1(meta, identity); err != nil {
		return err
	}
	raw, err := collections.EncodeVectorPartitionManifestV1(manifest)
	if err != nil {
		return err
	}
	placement, err := collections.VectorPartitionPlacementDigestV1(manifest)
	if err != nil {
		return err
	}
	if scope.Router || scope.HostedGroup != string(state.Begin.GroupID) || scope.ManifestDigest != identity.Immutable.ManifestDigest || scope.PlacementDigest != identity.Immutable.PlacementDigest || fmt.Sprintf("%x", sha256.Sum256(raw)) != identity.Immutable.ManifestDigest || placement != identity.Immutable.PlacementDigest || manifest.Collection != identity.Index.Collection.Collection || manifest.IndexName != identity.Index.IndexName || manifest.IndexDefinitionDigest != identity.Index.IndexDefinitionDigest || manifest.Generation != identity.Generation || manifest.SourceGeneration != identity.Source.Generation || manifest.SourceChecksum != identity.Source.Checksum || manifest.SourceSchemaHash != identity.Source.SchemaHash || manifest.SourceRowCount != identity.Source.RowCount {
		return ErrFixedPeerVectorProofStaleV1
	}
	return nil
}
