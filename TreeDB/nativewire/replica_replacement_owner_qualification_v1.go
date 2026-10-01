package nativewire

import (
	"context"
	"errors"

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
// endpoint. Each request obtains fresh leader quorum and target applied evidence.
func (c *FixedPeerTCPClientV1) QualifyReplicaReplacementOwnerV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, request VectorPartitionShardSearchRequestV1) (ReplicaReplacementOwnerQualificationV1, error) {
	var result ReplicaReplacementOwnerQualificationV1
	if c == nil || c.config.Vector == nil || c.peerTransport == nil || command.OwnerPreparation == nil ||
		request.TargetNodeID != command.NewPeer.ID || request.TargetGroupID != command.GroupID || request.StrictCapability != nil {
		return result, raftcluster.ErrUnsupportedFeature
	}
	begin, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
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
	limits := DefaultVectorPartitionShardSearchLimitsV1()
	if err := writeVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPFrameV1{PrivateRequest: &request, PrivateBegin: begin}, uint32(limits.MaxRequestBytes)); err != nil {
		return result, err
	}
	bound, err := vectorPartitionShardSearchTCPResponseFrameBoundV1(limits)
	if err != nil {
		return result, err
	}
	frame, err := readVectorPartitionShardSearchTCPResponseFrameV1(conn, bound, request)
	if err != nil {
		return result, err
	}
	if frame.Error != nil {
		return result, frame.Error.toError()
	}
	response := frame.PrivateResponse
	if response == nil || frame.Response != nil || response.Version != VectorPartitionShardSearchVersionV1 ||
		response.RequestID != request.RequestID || response.Proof.Kind != vectorPartitionShardSearchProofPrivateOwnerV1 ||
		response.Proof.ServingNode != command.NewPeer.ID || response.Proof.LeaderNode == "" || response.Proof.LeaderNode == command.NewPeer.ID ||
		response.Proof.GroupID != command.GroupID || response.Proof.ReadIndex == 0 || response.Proof.ReadTerm == 0 ||
		response.Proof.AppliedIndex < response.Proof.ReadIndex || response.Proof.ReadySetDigest != request.ReadySetDigest ||
		response.Proof.SourceGeneration != request.SourceGeneration || response.Proof.SourceChecksum != request.SourceChecksum ||
		response.Proof.SourceSchemaHash != request.SourceSchemaHash || response.Proof.SourceRowCount != request.SourceRowCount ||
		response.Proof.PartitionGeneration != request.PartitionGeneration || response.Proof.RouterGeneration != request.RouterGeneration {
		return result, ErrVectorPartitionShardSearchRouteMismatch
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
