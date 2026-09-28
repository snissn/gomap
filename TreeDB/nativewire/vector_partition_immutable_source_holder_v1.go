package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/json"
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
		captureVectorPartitionImmutableSourceV1(runtime, collectionRef))
}

func captureVectorPartitionImmutableSourceV1(runtime *FixedPeerTCPRuntimeV1, collectionRef raftplacement.CollectionRefV1) func(context.Context, raftcluster.GroupID, string, uint64) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, collections.VectorPartitionManifestV1, error) {
	return func(ctx context.Context, group raftcluster.GroupID, index string, generation uint64) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, collections.VectorPartitionManifestV1, error) {
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
	}
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
		evidence, err := deriveVectorPartitionImmutableSourceEvidenceV1(ctx, collectionRef, authority, capture, identity)
		if err != nil {
			return zero, nil, err
		}
		// All source, placement, and owner-set work precedes the short leader
		// lease. Require the same applied catalog and same local data leader.
		proof, err := provider.LinearizableCatalogMetaReadProofV1(ctx)
		if err != nil || evidence.catalogAppliedIndex != proof.CatalogAppliedIndex || proof.NodeID != evidence.dataProof.NodeID {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		if err := provider.ValidateCatalogMetaReadProofLeaseV1(proof); err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		if err := ctx.Err(); err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		return evidence.immutable, evidence.groups, nil
	}, nil
}

type vectorPartitionImmutableSourceEvidenceV1 struct {
	immutable           raftplacement.VectorPartitionLifecycleImmutableAuthorityV1
	groups              []raftcluster.GroupID
	catalogAppliedIndex uint64
	dataProof           raftcluster.ReadIndexProof
	sourceGroup         raftcluster.GroupID
}

// The wire proof has fixed size even when the manifest contains many
// partitions or owner members. The full mapping is bound by ManifestDigest;
// the meta leader independently validates its configured manifest against
// the current catalog before using this proof.
type fixedPeerVectorSourceAttestationV1 struct {
	Immutable           raftplacement.VectorPartitionLifecycleImmutableAuthorityV1 `json:"immutable"`
	OwnerSetDigest      string                                                     `json:"owner_set_digest"`
	CatalogAppliedIndex uint64                                                     `json:"catalog_applied_index"`
	DataAppliedIndex    uint64                                                     `json:"data_applied_index"`
	SourceGroup         raftcluster.GroupID                                        `json:"source_group"`
}

const fixedPeerVectorSourceAttestationMaxBytesV1 = 1024

func boundFixedPeerVectorSourceAttestationV1(attestation *fixedPeerVectorSourceAttestationV1) error {
	if attestation == nil {
		return ErrFixedPeerVectorProofStaleV1
	}
	raw, err := json.Marshal(attestation)
	if err != nil || len(raw) > fixedPeerVectorSourceAttestationMaxBytesV1 {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	return nil
}

func vectorPartitionImmutableOwnerSetDigestV1(groups []raftcluster.GroupID) (string, error) {
	encoded, err := json.Marshal(groups)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}

// Only the authenticated current catalog leader may ask the source-data
// leader to attest its own prepared bytes. No manifest or readiness vote is
// accepted from the caller, and no manifest bytes cross this RPC.
func (r *FixedPeerTCPRuntimeV1) captureImmutableVectorSourceForMetaLeaderV1(ctx context.Context, caller raftcluster.NodeID) (*fixedPeerVectorSourceAttestationV1, error) {
	if r == nil || r.closed.Load() || r.draining.Load() || r.client == nil || r.client.security == nil || caller == "" ||
		r.authority == nil || r.vector == nil || r.config.Vector == nil || r.config.Vector.Identity.Immutable == (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	metaLeader, err := r.client.leader(ctx, r.config.Catalog)
	if err != nil || metaLeader != caller {
		return nil, errors.Join(errPeerAuthenticationV1, err)
	}
	vector := r.config.Vector
	if err := r.waitForImmutableCatalogStatusV1(ctx); err != nil {
		return nil, err
	}
	if _, err := r.catalogFence(ctx); err != nil {
		return nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if err := r.vector.requireCurrentImmutableDBV1(); err != nil {
		return nil, err
	}
	collection, err := r.vector.manager.OpenCollection(vector.Collection.Collection)
	if err != nil {
		return nil, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	if err := fixedPeerVectorImmutableDefinitionV1(collection.MetaView(), vector.Identity); err != nil {
		return nil, err
	}
	evidence, err := deriveVectorPartitionImmutableSourceEvidenceV1(ctx, vector.Collection, r.authority,
		captureVectorPartitionImmutableSourceV1(r, vector.Collection), vector.Identity)
	if err != nil {
		return nil, err
	}
	if evidence.dataProof.NodeID != r.config.NodeID || evidence.dataProof.Index == 0 || r.data[evidence.sourceGroup] == nil {
		return nil, ErrFixedPeerVectorProofStaleV1
	}
	// ReadIndex must come from the current source-group leader. A former leader
	// with copied prepared bytes cannot attest after an election.
	resolved, err := raftplacement.Validate(vector.Catalog)
	if err != nil {
		return nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	realLeader, err := r.immutableVectorOwnerLeaderV1(ctx, resolved, evidence.sourceGroup)
	if err != nil || realLeader != r.config.NodeID {
		return nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	status, err := r.catalogFence(ctx)
	if err != nil || status.AppliedIndex != evidence.catalogAppliedIndex ||
		status.Epoch != vector.Identity.Index.CatalogEpoch || status.Digest != vector.Identity.Index.CatalogDigest {
		return nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if err := r.vector.requireCurrentImmutableDBV1(); err != nil {
		return nil, err
	}
	ownerDigest, err := vectorPartitionImmutableOwnerSetDigestV1(evidence.groups)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	attestation := &fixedPeerVectorSourceAttestationV1{
		Immutable: evidence.immutable, OwnerSetDigest: ownerDigest,
		CatalogAppliedIndex: evidence.catalogAppliedIndex,
		DataAppliedIndex:    evidence.dataProof.Index,
		SourceGroup:         evidence.sourceGroup,
	}
	if err := boundFixedPeerVectorSourceAttestationV1(attestation); err != nil {
		return nil, err
	}
	return attestation, nil
}

// The meta leader uses a credentialed source-data leader when it has no
// source DB itself. The response is a bounded digest proof; the configured
// full manifest and its partition mapping are independently checked here.
func newVectorPartitionRemoteSourceHolderPreparationV1(r *FixedPeerTCPRuntimeV1, collectionRef raftplacement.CollectionRefV1) (func(context.Context, raftplacement.VectorPartitionLifecycleIdentityV1) (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1, []raftcluster.GroupID, error), error) {
	if r == nil || r.client == nil || r.client.security == nil || r.meta == nil || r.authority == nil ||
		r.config.Vector == nil || r.config.Vector.Collection != collectionRef {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	return func(ctx context.Context, identity raftplacement.VectorPartitionLifecycleIdentityV1) (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1, []raftcluster.GroupID, error) {
		var zero raftplacement.VectorPartitionLifecycleImmutableAuthorityV1
		if ctx == nil || identity != r.config.Vector.Identity {
			return zero, nil, ErrFixedPeerVectorProofStaleV1
		}
		vector := r.config.Vector
		// This atomic record/index snapshot is compared with the short lease
		// only after source capture and all manifest work have completed.
		recordBytes, appliedIndex, err := r.authority.AppliedCatalogMetaRecordV1(ctx)
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		record, err := raftplacement.DecodeCatalogMetaRecordV1(recordBytes)
		if err != nil || record.Epoch != identity.Index.CatalogEpoch || record.Digest != identity.Index.CatalogDigest {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		resolved, err := raftplacement.Validate(record.Catalog)
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		placement, ok := resolved.Placement(collectionRef)
		if !ok || placement.Mode != raftplacement.PlacementModeCollectionV1 ||
			resolved.ValidateVectorPartitionPlacementV1(vector.Placement) != nil {
			return zero, nil, ErrFixedPeerVectorProofStaleV1
		}
		// The full encoded manifest binds every partition to a group, not just
		// the set of groups in the compact source response.
		encoded, err := collections.EncodeVectorPartitionManifestWithContextV1(ctx, vector.Manifest)
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		placementDigest, err := collections.VectorPartitionPlacementDigestWithContextV1(ctx, vector.Manifest)
		if err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		configured := raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{
			ManifestDigest: fmt.Sprintf("%x", sha256.Sum256(encoded)), PlacementDigest: placementDigest,
		}
		if configured != identity.Immutable {
			return zero, nil, ErrFixedPeerVectorProofStaleV1
		}
		groups := fixedPeerVectorOwnerGroupsV1(vector.Placement)
		ownerDigest, err := vectorPartitionImmutableOwnerSetDigestV1(groups)
		if err != nil {
			return zero, nil, err
		}
		sourceLeader, err := r.immutableVectorOwnerLeaderV1(ctx, resolved, placement.GroupID)
		if err != nil || sourceLeader == r.config.NodeID {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		reply, err := r.client.call(ctx, sourceLeader, "vector-lifecycle", fixedPeerRequestV1{
			VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: fixedPeerVectorLifecycleCaptureSourceV1},
		}, false)
		if err == nil {
			err = boundFixedPeerVectorSourceAttestationV1(reply.VectorSource)
		}
		if err != nil || reply.VectorSource == nil || reply.VectorSource.SourceGroup != placement.GroupID ||
			reply.VectorSource.CatalogAppliedIndex != appliedIndex || reply.VectorSource.DataAppliedIndex == 0 ||
			reply.VectorSource.Immutable != configured || reply.VectorSource.OwnerSetDigest != ownerDigest {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		proof, err := r.meta.LinearizableCatalogMetaReadProofV1(ctx)
		if err != nil || proof.NodeID != r.config.NodeID || proof.CatalogAppliedIndex != appliedIndex {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		if err := r.meta.ValidateCatalogMetaReadProofLeaseV1(proof); err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		if err := ctx.Err(); err != nil {
			return zero, nil, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		return configured, groups, nil
	}, nil
}

// Derivation is shared by colocated and authenticated remote capture. It
// binds the entire source manifest and partition mapping to the applied
// catalog; callers separately fence leadership and the final catalog lease.
func deriveVectorPartitionImmutableSourceEvidenceV1(
	ctx context.Context,
	collectionRef raftplacement.CollectionRefV1,
	authority *raftplacement.CatalogMetaAuthorityV1,
	capture func(context.Context, raftcluster.GroupID, string, uint64) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, collections.VectorPartitionManifestV1, error),
	identity raftplacement.VectorPartitionLifecycleIdentityV1,
) (vectorPartitionImmutableSourceEvidenceV1, error) {
	var zero vectorPartitionImmutableSourceEvidenceV1
	if authority == nil || capture == nil {
		return zero, ErrFixedPeerVectorUnavailableV1
	}
	if ctx == nil || identity.SourceFormat != 0 || identity.Index.Collection != collectionRef {
		return zero, ErrFixedPeerVectorProofStaleV1
	}
	// Decode the local applied catalog before source capture, then prove
	// that this same node has applied the collection owner's read index.
	// A final catalog proof below rejects any change to this snapshot.
	recordBytes, appliedIndex, err := authority.AppliedCatalogMetaRecordV1(ctx)
	if err != nil {
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	record, err := raftplacement.DecodeCatalogMetaRecordV1(recordBytes)
	if err != nil || record.Epoch != identity.Index.CatalogEpoch || record.Digest != identity.Index.CatalogDigest {
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if err := ctx.Err(); err != nil {
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	resolved, err := raftplacement.Validate(record.Catalog)
	if err != nil {
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if err := ctx.Err(); err != nil {
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	placementMode, ok := resolved.Placement(identity.Index.Collection)
	if !ok || placementMode.Mode != raftplacement.PlacementModeCollectionV1 {
		return zero, ErrFixedPeerVectorProofStaleV1
	}
	collectionGroup, ok := resolved.Group(placementMode.GroupID)
	if !ok {
		return zero, ErrFixedPeerVectorProofStaleV1
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
			return zero, ErrFixedPeerVectorProofStaleV1
		}
		for _, partition := range placed.TokenPartitions {
			if partition.GroupID == placementMode.GroupID {
				return zero, ErrFixedPeerVectorProofStaleV1
			}
		}
	}
	dataProof, progress, manifest, err := capture(ctx, placementMode.GroupID, identity.Index.IndexName, identity.Generation)
	if err != nil || (raftcluster.ReadIndexBarrier{GroupID: placementMode.GroupID}).Check(dataProof) != nil ||
		(dataProof.AppliedIndexBarrier()).Check(progress) != nil || !slices.Contains(collectionGroup.Members, dataProof.NodeID) {
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if manifest.Collection != identity.Index.Collection.Collection || manifest.IndexName != identity.Index.IndexName ||
		manifest.IndexDefinitionDigest != identity.Index.IndexDefinitionDigest || manifest.Generation != identity.Generation ||
		manifest.SourceGeneration != identity.Source.Generation || manifest.SourceChecksum != identity.Source.Checksum ||
		manifest.SourceSchemaHash != identity.Source.SchemaHash || manifest.SourceRowCount != identity.Source.RowCount {
		return zero, ErrFixedPeerVectorProofStaleV1
	}
	manifestBytes, err := collections.EncodeVectorPartitionManifestWithContextV1(ctx, manifest)
	if err != nil {
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	placementDigest, err := collections.VectorPartitionPlacementDigestWithContextV1(ctx, manifest)
	if err != nil {
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
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
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	slices.Sort(groups)
	groups = slices.Compact(groups)
	manifestDigest := fmt.Sprintf("%x", sha256.Sum256(manifestBytes))
	if err := ctx.Err(); err != nil {
		return zero, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	return vectorPartitionImmutableSourceEvidenceV1{
		immutable: raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{
			ManifestDigest: manifestDigest, PlacementDigest: placementDigest,
		}, groups: groups, catalogAppliedIndex: appliedIndex,
		dataProof: dataProof, sourceGroup: placementMode.GroupID,
	}, nil
}
