package nativewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// This control-only envelope uses the existing bounded Entry transport, leaving
// ordinary request encoding and admission unchanged.
type replacementOwnerQualificationCommitRequestV1 struct {
	Begin  []byte
	Search VectorPartitionShardSearchRequestV1
}

// CommitReplicaReplacementOwnerQualificationV1 asks the catalog coordinator to
// execute private ANN and record one historical receipt. It accepts no receipt
// bytes and grants no readiness, membership or route. Exact logical-query retries
// return owned historical bytes; they do not re-execute ANN or certify freshness.
func (c *FixedPeerTCPClientV1) CommitReplicaReplacementOwnerQualificationV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, request VectorPartitionShardSearchRequestV1) (raftplacement.ReplicaReplacementStateV1, error) {
	var zero raftplacement.ReplicaReplacementStateV1
	if c == nil || command.OwnerPreparation == nil || c.config.Vector == nil || *command.OwnerPreparation != c.config.Vector.Identity ||
		command.ConfigDigest != c.digest || !replacementOwnerQualificationRequestSupportedV1(request) {
		return zero, raftcluster.ErrInvalidConfig
	}
	limits := DefaultVectorPartitionShardSearchLimitsV1()
	if err := (&VectorPartitionShardSearchServiceV1{limits: limits}).validateRequest(request); err != nil {
		return zero, err
	}
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return zero, err
	}
	requestBytes, err := replacementOwnerQualificationRequestBytesV1(request, len(raw), limits.MaxRequestBytes)
	if err != nil {
		return zero, err
	}
	if c.security == nil || c.peerTransport == nil || c.peerTransport.security == nil || c.peerTransport.admission == nil {
		return zero, errPeerAuthenticationV1
	}
	ctx, cancel, err := vectorPartitionShardSearchContextV1(ctx, request.DeadlineUnixNano)
	if err != nil {
		return zero, err
	}
	defer cancel()
	// JSON escaping and its encoder/output copies are bounded before allocation.
	// The nested control call retains its own transport admission; neither lease waits.
	work, err := c.peerTransport.admission.request(ctx, "control-write", int64(requestBytes)*12+(64<<10), peerRequestDescendantV1)
	if err != nil {
		return zero, err
	}
	defer work.release()
	ctx = work.ctx
	leader, err := c.leader(ctx, c.config.Catalog)
	if err != nil {
		return zero, err
	}
	entry, err := json.Marshal(replacementOwnerQualificationCommitRequestV1{Begin: raw, Search: request})
	if err != nil {
		return zero, err
	}
	reply, err := c.call(ctx, leader, "replacement-owner-qualification-commit", fixedPeerRequestV1{Entry: entry}, true)
	if err != nil {
		return zero, err
	}
	if reply.ReplacementState == nil || reply.ReplacementState.OwnerQualification == nil {
		return zero, raftcluster.ErrReadBarrierNotSatisfied
	}
	state := *reply.ReplacementState
	actual, err := raftplacement.EncodeReplicaReplacementBeginV1(state.Begin)
	if err != nil || string(actual) != string(raw) || state.Phase != raftplacement.ReplicaReplacementAddIntentV1 {
		return zero, raftplacement.ErrCatalogMetaConflict
	}
	if _, err := raftplacement.EncodeReplicaReplacementStateV1(state); err != nil {
		return zero, err
	}
	logical := request
	logical.RequestID, logical.CancellationID, logical.DeadlineUnixNano = "", "", 0
	logical.LiveDomainIDs = nil // Immutable qualification accepts both nil and empty live domains.
	digest, err := replacementOwnerQualificationDigestV1(logical)
	if err != nil || state.OwnerQualification.QueryDigest != digest {
		return zero, raftplacement.ErrCatalogMetaConflict
	}
	return state, nil
}

func replacementOwnerQualificationDigestV1(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// Telemetry is not immutable result identity. Preserve only the validated
// partition/neighbor order, stable IDs and scores; empty neighbors have one encoding.
func replacementOwnerQualificationResultDigestV1(partials []VectorPartitionShardSearchPartialV1) (string, error) {
	results := make([]struct {
		PartitionID uint32
		Neighbors   []VectorPartitionShardSearchNeighborV1
	}, len(partials))
	for i, partial := range partials {
		results[i].PartitionID = partial.PartitionID
		if len(partial.Neighbors) != 0 {
			results[i].Neighbors = partial.Neighbors
		}
	}
	return replacementOwnerQualificationDigestV1(results)
}

// ponytail: one catalog producer's receipt gate conservatively serializes all
// receipt operations. Upgrade to per-operation gates only if measured contention
// warrants it. Waiters already hold bounded control request/byte leases; no worker
// or result lives beyond its caller deadline, and ordinary serving never uses it.
func (r *FixedPeerTCPRuntimeV1) acquireOwnerReceiptV1(ctx context.Context) (func(), error) {
	if r == nil || r.ownerReceipt == nil {
		return nil, raftcluster.ErrAdmissionUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case r.ownerReceipt <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-r.ownerReceipt
			return nil, err
		}
		return func() { <-r.ownerReceipt }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Bound result/hash storage by the same actual fanout/top-k frame ceiling as
// qualification. Capacity refusal precedes ANN; no raw results escape admission.
func replacementOwnerQualificationRetainedBytesV1(request VectorPartitionShardSearchRequestV1, limits VectorPartitionShardSearchLimitsV1) (int64, error) {
	configured, err := vectorPartitionShardSearchTCPResponseFrameBoundV1(limits)
	if err != nil {
		return 0, err
	}
	bound, err := peerShardResponseFrameV1(request, configured)
	if err != nil {
		return 0, err
	}
	return int64(bound) * 14, nil
}

func (r *FixedPeerTCPRuntimeV1) commitReplacementOwnerQualificationV1(ctx context.Context, entry []byte, reply *fixedPeerReplyV1) error {
	if len(entry) == 0 || len(entry) > fixedPeerMaxRPCBytesV1 {
		return raftcluster.ErrAdmissionUnavailable
	}
	var body replacementOwnerQualificationCommitRequestV1
	decoder := json.NewDecoder(bytes.NewReader(entry))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return raftcluster.ErrInvalidConfig
	}
	raw, request := body.Begin, &body.Search
	command, err := raftplacement.DecodeReplicaReplacementBeginV1(raw)
	if err != nil {
		return err
	}
	if request == nil || command.OwnerPreparation == nil || r.meta == nil || r.authority == nil || r.client == nil || r.client.security == nil || r.client.peerTransport == nil || r.client.peerTransport.admission == nil || r.config.Vector == nil ||
		*command.OwnerPreparation != r.config.Vector.Identity || request.TargetGroupID != command.GroupID || request.TargetNodeID != command.NewPeer.ID ||
		!replacementOwnerQualificationRequestSupportedV1(*request) {
		return raftcluster.ErrUnsupportedFeature
	}
	limits := DefaultVectorPartitionShardSearchLimitsV1()
	if err := (&VectorPartitionShardSearchServiceV1{limits: limits}).validateRequest(*request); err != nil {
		return err
	}
	if _, err := replacementOwnerQualificationRequestBytesV1(*request, len(raw), limits.MaxRequestBytes); err != nil {
		return err
	}
	ctx, cancel, err := vectorPartitionShardSearchContextV1(ctx, request.DeadlineUnixNano)
	if err != nil {
		return err
	}
	defer cancel()
	release, err := r.acquireOwnerReceiptV1(ctx)
	if err != nil {
		return err
	}
	defer release()
	if _, err := r.replacementReadV1(ctx, command); err != nil {
		return err
	}
	state, err := r.authority.ReplicaReplacementStateV1(command.GroupID)
	if err != nil {
		return err
	}
	if state.Phase != raftplacement.ReplicaReplacementAddIntentV1 || state.Seed == nil {
		return raftcluster.ErrAdmissionUnavailable
	}
	_, active, err := r.localImmutableVectorCatalogV1(ctx, fixedPeerVectorCatalogActiveV1)
	if err != nil {
		return err
	}
	if request.ReadySetDigest != active.ReadySetDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	// Correlation/deadline fields are transport lifetime, not logical query identity.
	logical := *request
	logical.RequestID, logical.CancellationID, logical.DeadlineUnixNano = "", "", 0
	logical.LiveDomainIDs = nil // Immutable qualification accepts both nil and empty live domains.
	queryDigest, err := replacementOwnerQualificationDigestV1(logical)
	if err != nil {
		return err
	}
	if state.OwnerQualification != nil {
		if state.OwnerQualification.QueryDigest != queryDigest {
			return raftplacement.ErrCatalogMetaConflict
		}
		reply.ReplacementState = &state
		return nil
	}
	// The qualification transport releases its transient response lease on return.
	// Retain decoded results plus both JSON buffers through fences/hash/commit:
	// two sixfold escaping buffers and two binary bounds of decoded storage.
	retainedBytes, err := replacementOwnerQualificationRetainedBytesV1(*request, limits)
	if err != nil {
		return err
	}
	retained, err := r.client.peerTransport.admission.acquire("shard:"+string(command.GroupID), peerBytesV1, retainedBytes)
	if err != nil {
		return err
	}
	defer retained.release()
	qualified, err := r.client.QualifyReplicaReplacementOwnerV1(ctx, command, *request)
	if err != nil {
		return err
	}
	// Re-read operation, ACTIVE and semantic target assets/tail after ANN; the
	// target's private search already checked its captured current FSM DB twice.
	if _, err := r.replacementReadV1(ctx, command); err != nil {
		return err
	}
	current, err := r.authority.ReplicaReplacementStateV1(command.GroupID)
	if err != nil {
		return err
	}
	if current.Phase != raftplacement.ReplicaReplacementAddIntentV1 || current.Seed == nil ||
		!raftcluster.SameReplacementSnapshotSeedV1(*current.Seed, *state.Seed) {
		return raftplacement.ErrCatalogMetaConflict
	}
	_, active, err = r.localImmutableVectorCatalogV1(ctx, fixedPeerVectorCatalogActiveV1)
	if err != nil {
		return err
	}
	if active.ReadySetDigest != qualified.Search.Proof.ReadySetDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	if current.OwnerQualification != nil {
		if current.OwnerQualification.QueryDigest != queryDigest {
			return raftplacement.ErrCatalogMetaConflict
		}
		reply.ReplacementState = &current
		return nil
	}
	group, err := r.replacementGroupV1(current, false)
	if err != nil {
		return err
	}
	issuerMember := false
	for _, peer := range group.Peers {
		issuerMember = issuerMember || peer.ID == qualified.Search.Proof.LeaderNode
	}
	if !issuerMember {
		return raftcluster.ErrInvalidConfig
	}
	leader, err := r.client.leader(ctx, group)
	if err != nil {
		return err
	}
	observed, err := r.client.call(ctx, leader, "replacement-tail", fixedPeerRequestV1{Entry: raw}, true)
	if err != nil {
		return err
	}
	if observed.ReplacementTail == nil || observed.ReplacementTail.LeaderID != leader || observed.ReplacementTail.CommitIndex < qualified.Search.Proof.ReadIndex {
		return raftcluster.ErrReadBarrierNotSatisfied
	}
	// Store a digest of immutable neighbors/partials, not transport/proof/timing.
	resultDigest, err := replacementOwnerQualificationResultDigestV1(qualified.Search.Partials)
	if err != nil {
		return err
	}
	proof := qualified.Search.Proof
	current.OwnerQualification = &raftplacement.ReplicaReplacementOwnerQualificationReceiptV1{
		QueryDigest: queryDigest, ResultDigest: resultDigest, ReadySetDigest: proof.ReadySetDigest,
		IssuerNode: proof.LeaderNode, ReadTerm: proof.ReadTerm, ReadIndex: proof.ReadIndex,
		TargetAppliedIndex: proof.AppliedIndex, Tail: *observed.ReplacementTail,
	}
	encoded, err := raftplacement.EncodeReplicaReplacementOwnerQualificationCommandV1(raftplacement.ReplicaReplacementOwnerQualificationCommandV1{State: current})
	if err != nil {
		return err
	}
	submitErr := r.submitCatalogCommandV1(ctx, encoded)
	if submitErr != nil && !errors.Is(submitErr, raftplacement.ErrCatalogMetaConflict) {
		return submitErr
	}
	// Another catalog leader may have committed the same query during this
	// producer's ANN. Freshly fence that owned receipt rather than publishing
	// this execution's different proof indexes. Other submission errors survive.
	if _, err := r.replacementReadV1(ctx, command); err != nil {
		return err
	}
	committed, err := r.authority.ReplicaReplacementStateV1(command.GroupID)
	if err != nil {
		return err
	}
	actualBegin, err := raftplacement.EncodeReplicaReplacementBeginV1(committed.Begin)
	if err != nil || !bytes.Equal(actualBegin, raw) || committed.Phase != raftplacement.ReplicaReplacementAddIntentV1 || committed.Seed == nil ||
		!raftcluster.SameReplacementSnapshotSeedV1(*committed.Seed, *state.Seed) || committed.OwnerQualification == nil || committed.OwnerQualification.QueryDigest != queryDigest {
		return raftplacement.ErrCatalogMetaConflict
	}
	reply.ReplacementState = &committed
	return nil
}
