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
// with the meta-Raft leader. It derives both immutable digests and the complete
// owner set from verified source bytes and a fenced catalog record. A follower
// has no transferable leader proof and fails closed; this does not grant
// owner-local serving authority. The V1 prepared manifest and catalog record
// do not carry CollectionIncarnation or IndexEpoch, so this callback cannot
// certify those two caller-supplied fields. A later serving path must bind
// them to separate durable authority before admitting the generation. The
// source proof is valid at the read instant; the coordinator's catalog
// mutation-epoch checks must fence source changes through the BUILD commit.
// Direct local writes outside that catalog protocol are not covered.
func NewVectorPartitionImmutableSourceHolderPreparationV1(
	collection *collections.Collection,
	collectionRef raftplacement.CollectionRefV1,
	authority *raftplacement.CatalogMetaAuthorityV1,
	provider *raftcluster.CatalogMetaRaftProviderV1,
) (func(context.Context, raftplacement.VectorPartitionLifecycleIdentityV1) (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1, []raftcluster.GroupID, error), error) {
	if collection == nil || collectionRef.Database == "" || collectionRef.Catalog == "" ||
		collectionRef.Collection == "" || collection.Name() != collectionRef.Collection || authority == nil || provider == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	return func(ctx context.Context, identity raftplacement.VectorPartitionLifecycleIdentityV1) (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1, []raftcluster.GroupID, error) {
		var zero raftplacement.VectorPartitionLifecycleImmutableAuthorityV1
		if ctx == nil || identity.SourceFormat != 0 || identity.Index.Collection != collectionRef {
			return zero, nil, ErrFixedPeerVectorProofStaleV1
		}
		manifest, err := collection.PreparedVectorPartitionManifestWithContextV1(ctx, identity.Index.IndexName, identity.Generation)
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		if manifest.Collection != identity.Index.Collection.Collection || manifest.IndexName != identity.Index.IndexName ||
			manifest.IndexDefinitionDigest != identity.Index.IndexDefinitionDigest || manifest.Generation != identity.Generation ||
			manifest.SourceGeneration != identity.Source.Generation || manifest.SourceChecksum != identity.Source.Checksum ||
			manifest.SourceSchemaHash != identity.Source.SchemaHash || manifest.SourceRowCount != identity.Source.RowCount {
			return zero, nil, ErrFixedPeerVectorProofStaleV1
		}
		manifestBytes, err := collections.EncodeVectorPartitionManifestV1(manifest)
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		placementDigest, err := collections.VectorPartitionPlacementDigestV1(manifest)
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		// Source validation and manifest encoding can exceed the short Raft
		// leader lease. Acquire the catalog proof only after that work, then
		// pair it with an exact applied-index snapshot before returning.
		proof, err := provider.LinearizableCatalogMetaReadProofV1(ctx)
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		snapshot, err := authority.ExportCatalogMetaSnapshotV1()
		if err != nil || snapshot.AppliedIndex != proof.CatalogAppliedIndex {
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
		if _, ok := resolved.Placement(identity.Index.Collection); !ok {
			return zero, nil, ErrFixedPeerVectorProofStaleV1
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
		if err := provider.ValidateCatalogMetaReadProofLeaseV1(proof); err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		if err := ctx.Err(); err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		slices.Sort(groups)
		groups = slices.Compact(groups)
		return raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{
			ManifestDigest: fmt.Sprintf("%x", sha256.Sum256(manifestBytes)), PlacementDigest: placementDigest,
		}, groups, nil
	}, nil
}
