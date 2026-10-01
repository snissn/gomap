package nativewire

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// ReplicaReplacementOwnerQualificationV1 is private ANN evidence only. Its
// leader-issued proof and target application are separate; it grants no route,
// READY, vote or ordinary M5 response authority.
type ReplicaReplacementOwnerQualificationV1 struct {
	Search VectorPartitionShardSearchResponseV1
}

// QualifyReplicaReplacementOwnerV1 searches an explicitly prepared private
// endpoint. Requests require StatsBasic and immutable, ANN-only results; each
// obtains fresh leader quorum and target applied evidence.
func (c *FixedPeerTCPClientV1) QualifyReplicaReplacementOwnerV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, request VectorPartitionShardSearchRequestV1) (ReplicaReplacementOwnerQualificationV1, error) {
	var result ReplicaReplacementOwnerQualificationV1
	if c == nil || c.config.Vector == nil || c.peerTransport == nil || command.OwnerPreparation == nil ||
		request.TargetNodeID != command.NewPeer.ID || request.TargetGroupID != command.GroupID || request.StrictCapability != nil ||
		request.StatsMode != VectorPartitionShardSearchStatsBasicV1 || request.LiveRevision != 0 || request.LiveCoverage != 0 || len(request.LiveDomainIDs) != 0 {
		return result, raftcluster.ErrUnsupportedFeature
	}
	limits := DefaultVectorPartitionShardSearchLimitsV1()
	if err := (&VectorPartitionShardSearchServiceV1{limits: limits}).validateRequest(request); err != nil {
		return result, err
	}
	requestBytes, err := vectorPartitionCoordinatorShardRequestBytesV1(request)
	if err != nil || requestBytes > uint64(limits.MaxRequestBytes) {
		return result, errors.Join(raftcluster.ErrRouteTargetUnsupported, err)
	}
	begin, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return result, err
	}
	requestBytes += uint64(4 + len(begin))
	if requestBytes > uint64(limits.MaxRequestBytes) || requestBytes > request.RequestBytesLimit {
		return result, ErrVectorPartitionShardSearchInvalidRequest
	}
	configuredBound, err := vectorPartitionShardSearchTCPResponseFrameBoundV1(limits)
	if err != nil {
		return result, err
	}
	bound, err := peerShardResponseFrameV1(request, configuredBound)
	if err != nil {
		return result, err
	}
	endpoint := c.config.Vector.ShardAddresses[command.GroupID][command.NewPeer.ID]
	if endpoint == "" {
		return result, errPeerAuthenticationV1
	}
	ctx, cancel, err := vectorPartitionShardSearchContextV1(ctx, request.DeadlineUnixNano)
	if err != nil {
		return result, err
	}
	defer cancel()
	// Reserve input/decoder expansion and actual fanout/top-k response before
	// dial, wire encoding or response allocation. Reuse request cancellation
	// and drain lifetime instead of creating an unaccounted private caller.
	work, err := c.peerTransport.admission.request(ctx, "shard:"+string(request.TargetGroupID), int64(requestBytes)*8+int64(bound)*2, peerRequestDescendantV1)
	if err != nil {
		return result, err
	}
	defer work.release()
	ctx = work.ctx
	group, err := c.replacementOwnerQualificationGroupV1(ctx, command, begin)
	if err != nil {
		return result, err
	}
	conn, err := c.peerTransport.dialScope(ctx, endpoint, command.NewPeer.ID, "shard:"+string(command.GroupID))
	if err != nil {
		return result, err
	}
	defer conn.Close()
	stop := vectorPartitionShardSearchTCPInterruptOnCancelV1(ctx, conn)
	defer stop()
	if err := vectorPartitionShardSearchTCPDeadlineV1(ctx, request.DeadlineUnixNano, conn); err != nil {
		return result, err
	}
	if err := writeVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPFrameV1{PrivateRequest: &request, PrivateBegin: begin}, uint32(limits.MaxRequestBytes)); err != nil {
		return result, err
	}
	frame, err := readVectorPartitionShardSearchTCPResponseFrameV1(conn, bound, request)
	if err != nil {
		return result, err
	}
	if frame.Error != nil {
		return result, frame.Error.toError()
	}
	if frame.Response != nil {
		return result, ErrVectorPartitionShardSearchRouteMismatch
	}
	return c.validateReplacementOwnerQualificationResponseV1(ctx, command, group, request, frame.PrivateResponse)
}

// Resolve the exact pending operation through the existing authenticated,
// quorum-fenced catalog read. Startup peers are not the committed roster after
// a completed replacement; this request-scoped decision is never cached.
func (c *FixedPeerTCPClientV1) replacementOwnerQualificationGroupV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, begin []byte) (FixedPeerTCPGroupV1, error) {
	var group FixedPeerTCPGroupV1
	if c.security == nil || c.peerTransport == nil || c.peerTransport.security == nil {
		return group, errPeerAuthenticationV1
	}
	if command.ConfigDigest != c.digest || c.config.Vector == nil || command.OwnerPreparation == nil ||
		*command.OwnerPreparation != c.config.Vector.Identity {
		return group, raftcluster.ErrInvalidConfig
	}
	leader, err := c.leader(ctx, c.config.Catalog)
	if err != nil {
		return group, err
	}
	reply, err := c.call(ctx, leader, "replacement-read", fixedPeerRequestV1{Entry: begin}, false)
	if err != nil {
		return group, err
	}
	if reply.ReplacementState == nil {
		return group, raftplacement.ErrCatalogMetaUnavailable
	}
	state := *reply.ReplacementState
	actual, err := raftplacement.EncodeReplicaReplacementBeginV1(state.Begin)
	if err != nil || string(actual) != string(begin) {
		return group, errors.Join(raftplacement.ErrCatalogMetaConflict, err)
	}
	if state.Phase != raftplacement.ReplicaReplacementAddIntentV1 || state.Seed == nil {
		return group, raftcluster.ErrAdmissionUnavailable
	}
	for _, candidate := range c.config.Groups {
		if candidate.ID == command.GroupID {
			group = candidate
			break
		}
	}
	if group.ID == "" {
		return group, raftcluster.ErrRouteTargetUnknown
	}
	group, err = replacementGroupPeersV1(group, state, false)
	if err != nil {
		return FixedPeerTCPGroupV1{}, err
	}
	for _, peer := range group.Peers {
		if c.addresses[peer.ID] == "" {
			return FixedPeerTCPGroupV1{}, raftcluster.ErrInvalidConfig
		}
	}
	return group, nil
}

func (c *FixedPeerTCPClientV1) validateReplacementOwnerQualificationResponseV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, group FixedPeerTCPGroupV1, request VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) (ReplicaReplacementOwnerQualificationV1, error) {
	var result ReplicaReplacementOwnerQualificationV1
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if response == nil {
		return result, ErrVectorPartitionShardSearchRouteMismatch
	}
	proof := response.Proof
	issuerMember := false
	for _, peer := range group.Peers {
		issuerMember = issuerMember || peer.ID == proof.LeaderNode
	}
	if response.Version != VectorPartitionShardSearchVersionV1 || response.RequestID != request.RequestID ||
		group.ID != command.GroupID || proof.Kind != vectorPartitionShardSearchProofPrivateOwnerV1 || proof.ServingNode != command.NewPeer.ID ||
		proof.LeaderNode == command.NewPeer.ID || !issuerMember ||
		proof.GroupID != command.GroupID || proof.ReadIndex == 0 || proof.ReadTerm == 0 || proof.AppliedTerm == 0 ||
		proof.AppliedIndex < proof.ReadIndex || proof.ReadySetDigest != request.ReadySetDigest ||
		proof.SourceGeneration != request.SourceGeneration || proof.SourceChecksum != request.SourceChecksum ||
		proof.SourceSchemaHash != request.SourceSchemaHash || proof.SourceRowCount != request.SourceRowCount ||
		proof.PartitionGeneration != request.PartitionGeneration || proof.RouterGeneration != request.RouterGeneration ||
		proof.LiveRevision != 0 || proof.LiveCoverage != 0 || proof.ServingIdentityDigest != "" ||
		proof.CatalogAppliedIndex != 0 || proof.GroupAppliedIndex != 0 ||
		response.Partitions != uint64(len(request.PartitionIDs)) || len(response.Partials) != len(request.PartitionIDs) ||
		response.ReadProofs != 1 || response.GenerationPins != 1 || response.PartitionOpens != response.Partitions ||
		response.ScoreCalls > request.ScoreCallsLimit ||
		response.BaseCandidates != response.Candidates || response.DeltaCandidates != 0 || response.DeltaResults != 0 ||
		response.LiveDomainsSearched != 0 || response.LiveMutatedIDs != 0 || response.LiveIDs != 0 ||
		response.Cutovers != 0 || response.RequestPathFullRebuilds != 0 {
		return result, ErrVectorPartitionShardSearchRouteMismatch
	}
	var results uint64
	for _, partial := range response.Partials {
		var ok bool
		results, ok = addUint64V1(results, uint64(len(partial.Neighbors)))
		if !ok || partial.SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 ||
			partial.Candidates > request.SourceRowCount || partial.Candidates > partial.ScoreCalls ||
			partial.RequiredChunks != partial.OpenedChunks || partial.RequiredChunks != partial.AccessedChunks {
			return result, ErrVectorPartitionShardSearchRouteMismatch
		}
	}
	if response.BaseResults != results {
		return result, ErrVectorPartitionShardSearchRouteMismatch
	}
	coordinator := VectorPartitionCoordinatorV1{limits: DefaultVectorPartitionCoordinatorLimitsV1()}
	task := vectorPartitionCoordinatorTaskV1{partitionIDs: request.PartitionIDs}
	if err := coordinator.validateShardResponsePayloadV1(ctx, task, request, *response); err != nil {
		return result, err
	}
	result.Search = *response
	return result, nil
}

// The original member's authenticated group-read-proof issuer is unchanged.
// Never substitute the replacement node into the received proof.
func (r *FixedPeerTCPRuntimeV1) replacementOwnerReadBarrierV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, d *fixedPeerDataV1) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, error) {
	var proof raftcluster.ReadIndexProof
	var progress raftcluster.AppliedProgress
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return proof, progress, err
	}
	if state.Phase != raftplacement.ReplicaReplacementAddIntentV1 || state.Begin.OwnerPreparation == nil || state.Seed == nil ||
		d == nil || d.fsm == nil || d.provider == nil || command.NewPeer.ID != r.config.NodeID {
		return proof, progress, raftcluster.ErrAdmissionUnavailable
	}
	group, err := r.replacementGroupV1(state, false)
	if err != nil {
		return proof, progress, err
	}
	leader, err := r.client.leader(ctx, group)
	if err != nil {
		return proof, progress, err
	}
	if leader == command.NewPeer.ID {
		return proof, progress, raftcluster.ErrReadBarrierTargetMismatch
	}
	reply, err := r.client.call(ctx, leader, "group-read-proof", fixedPeerRequestV1{Metadata: raftentry.RequestMetadataV1{ClusterRouteGroupID: string(command.GroupID)}}, false)
	if err != nil || reply.ReadProof == nil {
		return proof, progress, errors.Join(err, raftcluster.ErrReadBarrierNotSatisfied)
	}
	proof = *reply.ReadProof
	if proof.Term == 0 {
		return proof, progress, raftcluster.ErrReadBarrierNotSatisfied
	}
	if err := (raftcluster.ReadIndexBarrier{NodeID: leader, GroupID: command.GroupID}).Check(proof); err != nil {
		return proof, progress, err
	}
	barrier := raftcluster.AppliedIndexReadBarrier{NodeID: r.config.NodeID, GroupID: command.GroupID, MinAppliedIndex: proof.Index}
	progress, err = d.fsm.WaitAppliedIndex(ctx, barrier)
	if err != nil {
		return proof, progress, err
	}
	status, err := d.provider.RuntimeStatusV1(ctx)
	if err != nil {
		return proof, progress, err
	}
	if err := barrier.Check(progress); err != nil {
		return proof, progress, err
	}
	if status.NodeID != r.config.NodeID || status.GroupID != command.GroupID || status.RaftAppliedIndex < proof.Index {
		return proof, progress, raftcluster.ErrReadBarrierNotSatisfied
	}
	return proof, progress, nil
}
