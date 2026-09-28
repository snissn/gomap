package nativewire

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// NewVectorPartitionImmutableSourceHolderPreparationV1 supplies the trusted
// BeginBuild preparation callback for a full-local source holder colocated
// with the meta-Raft leader and the collection data-group leader. It first
// proves that the local data group has applied a quorum read index, then
// derives both immutable digests and the complete owner set from verified
// source bytes and a fenced catalog record. A follower has no transferable
// leader proof and fails closed; this does not grant
// owner-local serving authority. The V1 prepared manifest and catalog record
// do not carry CollectionIncarnation or IndexEpoch, so this callback cannot
// certify those two caller-supplied fields. A later serving path must bind
// them to separate durable authority before admitting the generation. The
// source proof is valid at the read instant; the coordinator's catalog
// mutation-epoch checks must fence source changes through the BUILD commit.
// Direct local writes outside that catalog protocol are not covered.
func NewVectorPartitionImmutableSourceHolderPreparationV1(
	runtime *FixedPeerTCPRuntimeV1,
	collectionRef raftplacement.CollectionRefV1,
) (func(context.Context, raftplacement.VectorPartitionLifecycleIdentityV1) (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1, []raftcluster.GroupID, error), error) {
	if runtime == nil || runtime.authority == nil || runtime.meta == nil || runtime.config.NodeID == "" ||
		runtime.config.Vector == nil || runtime.config.Vector.Collection != collectionRef {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	return newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(collectionRef, runtime.authority, runtime.meta,
		func(ctx context.Context, group raftcluster.GroupID, index string, generation uint64) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, collections.VectorPartitionManifestV1, error) {
			var zero collections.VectorPartitionManifestV1
			if runtime.closed.Load() {
				return raftcluster.ReadIndexProof{}, raftcluster.AppliedProgress{}, zero, ErrFixedPeerVectorUnavailableV1
			}
			data := runtime.data[group]
			if data == nil || data.provider == nil || data.fsm == nil {
				return raftcluster.ReadIndexProof{}, raftcluster.AppliedProgress{}, zero, ErrFixedPeerVectorUnavailableV1
			}
			target := raftcluster.ReadIndexBarrier{NodeID: runtime.config.NodeID, GroupID: group}
			proof, err := data.provider.ReadIndex(ctx, target)
			if err != nil {
				return proof, raftcluster.AppliedProgress{}, zero, err
			}
			if err := target.Check(proof); err != nil {
				return proof, raftcluster.AppliedProgress{}, zero, err
			}
			progress, manifest, err := data.fsm.PreparedVectorPartitionManifestFromCurrentDBV1(ctx, proof.AppliedIndexBarrier(), collectionRef.Collection, index, generation)
			return proof, progress, manifest, err
		})
}

// The capture seam is private so callers cannot pair an unrelated Collection
// with a valid data-group read proof. Production capture derives both from the
// same fixed-peer group and its snapshot-replaceable FSM.
func newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(
	collectionRef raftplacement.CollectionRefV1,
	authority *raftplacement.CatalogMetaAuthorityV1,
	provider *raftcluster.CatalogMetaRaftProviderV1,
	capture func(context.Context, raftcluster.GroupID, string, uint64) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, collections.VectorPartitionManifestV1, error),
) (func(context.Context, raftplacement.VectorPartitionLifecycleIdentityV1) (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1, []raftcluster.GroupID, error), error) {
	if collectionRef.Database == "" || collectionRef.Catalog == "" || collectionRef.Collection == "" || authority == nil || provider == nil || capture == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	return func(ctx context.Context, identity raftplacement.VectorPartitionLifecycleIdentityV1) (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1, []raftcluster.GroupID, error) {
		var zero raftplacement.VectorPartitionLifecycleImmutableAuthorityV1
		if ctx == nil || identity.SourceFormat != 0 || identity.Index.Collection != collectionRef {
			return zero, nil, ErrFixedPeerVectorProofStaleV1
		}
		// Decode the local applied catalog before source capture, then prove
		// that this same node has applied the collection owner's read index.
		// A final catalog proof below rejects any change to this snapshot.
		snapshot, err := authority.ExportCatalogMetaSnapshotV1()
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		record, err := raftplacement.DecodeCatalogMetaRecordV1(snapshot.Record)
		if err != nil || record.Epoch != identity.Index.CatalogEpoch || record.Digest != identity.Index.CatalogDigest {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		resolved, err := raftplacement.Validate(record.Catalog)
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		placementMode, ok := resolved.Placement(identity.Index.Collection)
		if !ok || placementMode.Mode != raftplacement.PlacementModeCollectionV1 {
			return zero, nil, ErrFixedPeerVectorProofStaleV1
		}
		collectionGroup, ok := resolved.Group(placementMode.GroupID)
		if !ok {
			return zero, nil, ErrFixedPeerVectorProofStaleV1
		}
		// Collections in a local data-group DB are addressed by leaf name.
		// Only references that can use this data group share its leaf-name
		// namespace. A same-named collection in another group has separate
		// storage and cannot be confused with this source.
		for _, placed := range record.Catalog.Placements {
			if placed.Collection == collectionRef || placed.Collection.Collection != collectionRef.Collection {
				continue
			}
			if placed.GroupID == placementMode.GroupID {
				return zero, nil, ErrFixedPeerVectorProofStaleV1
			}
			for _, partition := range placed.TokenPartitions {
				if partition.GroupID == placementMode.GroupID {
					return zero, nil, ErrFixedPeerVectorProofStaleV1
				}
			}
		}
		dataProof, progress, manifest, err := capture(ctx, placementMode.GroupID, identity.Index.IndexName, identity.Generation)
		if err != nil || (raftcluster.ReadIndexBarrier{GroupID: placementMode.GroupID}).Check(dataProof) != nil ||
			(dataProof.AppliedIndexBarrier()).Check(progress) != nil || !slices.Contains(collectionGroup.Members, dataProof.NodeID) {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		if manifest.Collection != identity.Index.Collection.Collection || manifest.IndexName != identity.Index.IndexName ||
			manifest.IndexDefinitionDigest != identity.Index.IndexDefinitionDigest || manifest.Generation != identity.Generation ||
			manifest.SourceGeneration != identity.Source.Generation || manifest.SourceChecksum != identity.Source.Checksum ||
			manifest.SourceSchemaHash != identity.Source.SchemaHash || manifest.SourceRowCount != identity.Source.RowCount {
			return zero, nil, ErrFixedPeerVectorProofStaleV1
		}
		manifestBytes, err := collections.EncodeVectorPartitionManifestWithContextV1(ctx, manifest)
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		placementDigest, err := collections.VectorPartitionPlacementDigestWithContextV1(ctx, manifest)
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		placement := raftplacement.VectorPartitionPlacementRecordV1{
			Collection: identity.Index.Collection, IndexName: manifest.IndexName,
			IndexDefinitionDigest: manifest.IndexDefinitionDigest,
			SourceGeneration:      manifest.SourceGeneration, SourceChecksum: manifest.SourceChecksum,
			SourceSchemaHash: manifest.SourceSchemaHash, SourceRowCount: manifest.SourceRowCount,
			PartitionGeneration: manifest.Generation, PartitionCount: manifest.PartitionCount,
			Partitions: make([]raftplacement.VectorPartitionGroupV1, len(manifest.Placements)),
		}
		groups := make([]raftcluster.GroupID, 0, len(manifest.Placements))
		for i, part := range manifest.Placements {
			placement.Partitions[i] = raftplacement.VectorPartitionGroupV1{PartitionID: part.PartitionID, GroupID: raftcluster.GroupID(part.GroupID)}
			groups = append(groups, raftcluster.GroupID(part.GroupID))
		}
		if err := resolved.ValidateVectorPartitionPlacementV1(placement); err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		slices.Sort(groups)
		groups = slices.Compact(groups)
		manifestDigest := fmt.Sprintf("%x", sha256.Sum256(manifestBytes))
		// All source, placement, and owner-set work precedes the short leader
		// lease. Require the same applied catalog and same local data leader.
		proof, err := provider.LinearizableCatalogMetaReadProofV1(ctx)
		if err != nil || snapshot.AppliedIndex != proof.CatalogAppliedIndex || proof.NodeID != dataProof.NodeID {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		if err := provider.ValidateCatalogMetaReadProofLeaseV1(proof); err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		if err := ctx.Err(); err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		return raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{
			ManifestDigest: manifestDigest, PlacementDigest: placementDigest,
		}, groups, nil
	}, nil
}
