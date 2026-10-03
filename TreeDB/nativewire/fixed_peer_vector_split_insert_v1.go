package nativewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// Split delivery is a production authenticated surface. Configuration digests
// and loopback addresses are not substitutes for a certificate-bound node.
func (r *FixedPeerTCPRuntimeV1) splitVectorGroupsV1(ctx context.Context) (raftcluster.GroupID, raftcluster.GroupID, error) {
	if r == nil || r.vector == nil || r.servingVectorConfigV1() == nil || r.config.Credentials == nil ||
		r.client == nil || r.client.security == nil || r.client.peerTransport == nil ||
		r.authority == nil || r.servingVectorConfigV1().Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) ||
		r.servingVectorConfigV1().Identity.SourceFormat != 0 || r.draining.Load() {
		return "", "", ErrFixedPeerVectorUnavailableV1
	}
	vector := r.servingVectorConfigV1()
	resolved, err := raftplacement.Validate(vector.Catalog)
	if err != nil {
		return "", "", err
	}
	placement, ok := resolved.Placement(vector.Collection)
	owners := fixedPeerVectorOwnerGroupsV1(vector.Placement)
	if !ok || placement.Mode != raftplacement.PlacementModeCollectionV1 || placement.GroupID == "" ||
		len(owners) != 1 || owners[0] == placement.GroupID {
		return "", "", ErrFixedPeerVectorUnavailableV1
	}
	if _, err := r.catalogFence(ctx); err != nil {
		return "", "", err
	}
	return placement.GroupID, owners[0], nil
}

func (r *FixedPeerTCPRuntimeV1) splitVectorGroupV1(group raftcluster.GroupID) (FixedPeerTCPGroupV1, error) {
	for _, fixed := range r.config.Groups {
		if fixed.ID == group {
			return fixed, nil
		}
	}
	return FixedPeerTCPGroupV1{}, ErrFixedPeerVectorWrongOwnerV1
}

func (r *FixedPeerTCPRuntimeV1) validateSplitVectorInsertV1(ctx context.Context, v commitlog.SplitVectorInsertV1, localGroup raftcluster.GroupID) error {
	if err := v.ValidateV1(); err != nil {
		return errors.Join(ErrFixedPeerVectorDocumentV1, err)
	}
	source, target, err := r.splitVectorGroupsV1(ctx)
	if err != nil {
		return err
	}
	vector := r.servingVectorConfigV1()
	if source != raftcluster.GroupID(v.SourceGroup) || target != raftcluster.GroupID(v.TargetGroup) ||
		v.Collection != vector.Collection.Collection || v.Index != vector.Manifest.IndexName ||
		v.Generation != vector.Identity.Generation || v.CatalogEpoch != vector.Identity.Index.CatalogEpoch ||
		v.CatalogDigest != vector.Identity.Index.CatalogDigest || localGroup != r.vector.dataGroup {
		return ErrFixedPeerVectorProofStaleV1
	}
	data := r.localDataV1(localGroup)
	if data == nil || data.fsm == nil || data.provider == nil || !data.fsm.HasCurrentDBV1(data.db) {
		return ErrFixedPeerVectorUnavailableV1
	}
	status, err := data.provider.RuntimeStatusV1(ctx)
	if err != nil || status.State != "Leader" || status.LeaderID != r.config.NodeID {
		return errors.Join(ErrFixedPeerVectorUnavailableV1, raftcluster.ErrNotLeader, err)
	}
	catalog, err := r.catalogFence(ctx)
	if err != nil || catalog.Epoch != v.CatalogEpoch || catalog.Digest != v.CatalogDigest {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	routed, err := r.authority.RouteDocumentToken(ctx, raftplacement.CatalogProofV1{Epoch: v.CatalogEpoch, Digest: v.CatalogDigest}, vector.Collection, raftplacement.DocumentIDTokenV1(v.ID))
	if err != nil || routed.GroupID() != source {
		return errors.Join(ErrFixedPeerVectorWrongOwnerV1, err)
	}
	record, exists := r.authority.VectorPartitionLifecycleRecordV1(vector.Identity)
	owners, ready, specErr := fixedPeerVectorLifecycleSpecV1(vector)
	if !exists || specErr != nil || len(owners) != 1 || len(record.RequiredGroups) != 1 ||
		record.RequiredGroups[0] != target || len(record.ReadyGroups) != 1 || record.ReadyGroups[0] != ready ||
		record.ReadySetDigest != v.ReadySetDigest || record.Identity != vector.Identity {
		return ErrFixedPeerVectorProofStaleV1
	}
	if err := record.CanSearch(raftplacement.VectorPartitionLifecycleSearchProofV1{Identity: vector.Identity, ReadySetDigest: v.ReadySetDigest}); err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	backend, err := r.vector.ensureBackendV1(ctx)
	if err != nil {
		return err
	}
	coordinator := backend.opts.Topology.Coordinator()
	lease, err := coordinator.acquireRouterSessionV1(ctx, v.Index, v.Generation)
	if err != nil {
		return err
	}
	defer lease.Close()
	router := lease.session.router.Status()
	readyDigest, err := coordinator.validateReplicatedLifecycle(ctx, router)
	if err != nil || readyDigest != v.ReadySetDigest || router.ModelDigest != v.ModelDigest {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	selection, err := lease.session.router.SearchWithContextV1(ctx, v.Vector, collections.VectorPartitionRouterSearchOptionsV3{
		Mode: collections.VectorPartitionRouterModeExactV1, ScoreBudget: int(router.Representatives), PartitionProbes: 1,
	})
	if err != nil || len(selection.Partitions) != 1 {
		return errors.Join(ErrFixedPeerVectorWrongOwnerV1, err)
	}
	partition, owner, err := vectorPartitionMutationOwnerV1(router.Manifest, coordinator.placement, selection.Partitions[0].PartitionID)
	if err != nil || owner != target || partition != v.PartitionID {
		return ErrFixedPeerVectorWrongOwnerV1
	}
	if !data.fsm.HasCurrentDBV1(data.db) {
		return ErrFixedPeerVectorUnavailableV1
	}
	return ctx.Err()
}

func splitVectorCommandKeyV1(v commitlog.SplitVectorInsertV1) []byte {
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/split-insert/v1/" + v.Operation + "/"))
	_, _ = h.Write(v.Attempt)
	return h.Sum(nil)
}

func (r *FixedPeerTCPRuntimeV1) submitSplitVectorCommandV1(ctx context.Context, v commitlog.SplitVectorInsertV1, group raftcluster.GroupID) (raftcluster.SubmitResultV1, error) {
	var zero raftcluster.SubmitResultV1
	if err := r.validateSplitVectorInsertV1(ctx, v, group); err != nil {
		return zero, err
	}
	if err := r.vector.collection.PreflightVectorPartitionSplitInsertV1(ctx, v); err != nil {
		return zero, err
	}
	data := r.localDataV1(group)
	guard, present, err := data.fsm.CurrentCatalogVersion(ctx)
	if err != nil || !present {
		return zero, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	key := splitVectorCommandKeyV1(v)
	originalGuard, originalDigest, known, err := data.fsm.AppliedIdempotencyGuardV1(ctx, key)
	if err != nil {
		return zero, err
	}
	if known {
		guard = originalGuard
	}
	payload, err := commitlog.EncodeSplitVectorInsertPayloadV1(v)
	if err != nil {
		return zero, err
	}
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandSplitVectorInsertV1, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: key},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, guard)},
		collectionNameRef(v.Collection),
		{ID: iwire.SectionSplitVectorInsertV1, Bytes: payload},
	}
	validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		return zero, err
	}
	entry, err := iwire.AppendDeterministicEntry(nil, validated)
	if err != nil {
		return zero, err
	}
	if known && raftentry.CommandDigestV1ForBytes(entry, raftentry.DecodeOptions{}) != originalDigest {
		return zero, &public.ErrorV1{Code: public.ErrorInvalidRequestV1, Err: collections.ErrVectorPartitionSplitInsertConflictV1}
	}
	base, ok := r.localRegistry.Lookup(group)
	if !ok {
		return zero, ErrFixedPeerVectorWrongOwnerV1
	}
	submitter, ok := base.(raftcluster.CommandSubmitterWithPreCommitV1)
	if !ok {
		return zero, ErrFixedPeerVectorUnavailableV1
	}
	metadata := raftentry.RequestMetadataV1{RequestID: binary.BigEndian.Uint64(key[:8]), AckPolicy: iwire.AckRaftCommitted,
		ClusterRouteKnown: true, ClusterRouteDatabase: r.servingVectorConfigV1().Collection.Database, ClusterRouteCatalog: r.servingVectorConfigV1().Collection.Catalog,
		ClusterRouteCollection: v.Collection, ClusterRouteShape: "split_source_projection_insert", ClusterRouteGroupID: string(group),
		CatalogMetaEpoch: v.CatalogEpoch, CatalogMetaDigest: v.CatalogDigest}
	result, err := submitter.SubmitCommandEntryWithPreCommitV1(ctx, entry, metadata, func(commitCtx context.Context) error {
		if err := r.validateSplitVectorInsertV1(commitCtx, v, group); err != nil {
			return err
		}
		return r.vector.collection.PreflightVectorPartitionSplitInsertV1(commitCtx, v)
	})
	if err = fixedPeerVectorSubmitErrorV1(result, err); err != nil {
		return result, err
	}
	if result.ActualAck != iwire.AckRaftCommitted || !result.CommittedRecoverable || !result.CommittedApplied ||
		!result.Evidence.ProvesProductionConsensus() || result.Evidence.GroupID != group ||
		result.Evidence.NodeID != r.config.NodeID || result.Evidence.LeaderID != r.config.NodeID ||
		result.Evidence.Term != result.CommittedEntry.Term || result.Evidence.Index != result.CommittedEntry.Index ||
		result.CommittedEntry.Term == 0 || result.CommittedEntry.Index == 0 ||
		(result.ApplyResult.Status != raftentry.ApplyStatusApplied && result.ApplyResult.Status != raftentry.ApplyStatusAlreadyApplied) {
		return result, fixedPeerVectorPostCommitAmbiguousV1(raftcluster.ErrCommitNotProven)
	}
	if err := r.validateSplitVectorInsertV1(ctx, v, group); err != nil {
		return result, fixedPeerVectorPostCommitAmbiguousV1(err)
	}
	return result, nil
}

func (r *FixedPeerTCPRuntimeV1) splitVectorReadFenceV1(ctx context.Context, group raftcluster.GroupID, minimum uint64) (raftcluster.ReadIndexProof, error) {
	data := r.localDataV1(group)
	if data == nil || data.fsm == nil || !data.fsm.HasCurrentDBV1(data.db) {
		return raftcluster.ReadIndexProof{}, ErrFixedPeerVectorUnavailableV1
	}
	proof, err := data.provider.ReadIndex(ctx, raftcluster.ReadIndexBarrier{NodeID: r.config.NodeID, GroupID: group})
	if err != nil {
		return proof, err
	}
	if err := (raftcluster.ReadIndexBarrier{NodeID: r.config.NodeID, GroupID: group}).Check(proof); err != nil {
		return proof, err
	}
	if proof.Index < minimum {
		return proof, raftcluster.ErrReadBarrierNotSatisfied
	}
	if _, err := data.fsm.WaitAppliedIndex(ctx, proof.AppliedIndexBarrier()); err != nil {
		return proof, err
	}
	if !data.fsm.HasCurrentDBV1(data.db) {
		return proof, ErrFixedPeerVectorUnavailableV1
	}
	return proof, ctx.Err()
}

// The source proof is a fresh quorum/applied read of its bounded durable slot,
// not a caller-declared ReadIndex interpreted as a semantic source EntryID.
func (r *FixedPeerTCPRuntimeV1) proveSplitVectorSourceV1(ctx context.Context, v commitlog.SplitVectorInsertV1) (raftcluster.ReadIndexProof, error) {
	group := raftcluster.GroupID(v.SourceGroup)
	if err := r.validateSplitVectorInsertV1(ctx, v, group); err != nil {
		return raftcluster.ReadIndexProof{}, err
	}
	proof, err := r.splitVectorReadFenceV1(ctx, group, v.SourceIndex)
	if err != nil {
		return proof, err
	}
	pending, receipt, known, err := r.vector.collection.VectorPartitionSplitInsertStateV1(v)
	if err != nil {
		return proof, err
	}
	if known {
		if receipt.SourceTerm != v.SourceTerm || receipt.SourceIndex != v.SourceIndex {
			return proof, ErrFixedPeerVectorProofStaleV1
		}
	} else {
		if pending == nil {
			return proof, ErrFixedPeerVectorProofMissingV1
		}
		expected, e := pending.DigestV1()
		actual, a := v.DigestV1()
		if e != nil || a != nil || expected != actual || pending.SourceTerm != v.SourceTerm || pending.SourceIndex != v.SourceIndex {
			return proof, ErrFixedPeerVectorProofStaleV1
		}
	}
	if err := r.validateSplitVectorInsertV1(ctx, v, group); err != nil {
		return proof, err
	}
	return proof, nil
}

func (r *FixedPeerTCPRuntimeV1) callSplitVectorGroupV1(ctx context.Context, group raftcluster.GroupID, operation string, v commitlog.SplitVectorInsertV1) (fixedPeerReplyV1, error) {
	fixed, err := r.splitVectorGroupV1(group)
	if err != nil {
		return fixedPeerReplyV1{}, err
	}
	leader, err := r.client.leader(ctx, fixed)
	if err != nil {
		return fixedPeerReplyV1{}, err
	}
	raw, err := commitlog.EncodeSplitVectorInsertPayloadV1(v)
	if err != nil {
		return fixedPeerReplyV1{}, err
	}
	return r.client.call(ctx, leader, operation, fixedPeerRequestV1{Entry: raw}, operation == "vector-split-project")
}

func (r *FixedPeerTCPRuntimeV1) handleSplitVectorControlV1(ctx context.Context, operation string, caller raftcluster.NodeID, raw []byte, reply *fixedPeerReplyV1) error {
	v, err := commitlog.DecodeSplitVectorInsertPayloadV1(raw)
	if err != nil {
		return errors.Join(ErrFixedPeerVectorDocumentV1, err)
	}
	if r.config.Credentials == nil || r.client.security == nil || caller == "" {
		return errPeerAuthenticationV1
	}
	group := raftcluster.GroupID(v.TargetGroup)
	callerGroup := raftcluster.GroupID(v.SourceGroup)
	if operation == "vector-split-source-proof" {
		group, callerGroup = callerGroup, group
	}
	fixed, err := r.splitVectorGroupV1(callerGroup)
	if err != nil {
		return err
	}
	allowed := false
	for _, peer := range fixed.Peers {
		allowed = allowed || peer.ID == caller
	}
	if operation == "vector-split-receipt" {
		targetGroup, e := r.splitVectorGroupV1(raftcluster.GroupID(v.TargetGroup))
		if e != nil {
			return e
		}
		for _, peer := range targetGroup.Peers {
			allowed = allowed || peer.ID == caller
		}
	}
	if !allowed {
		return errPeerAuthenticationV1
	}
	if err := r.validateSplitVectorInsertV1(ctx, v, group); err != nil {
		return err
	}
	if operation == "vector-split-source-proof" {
		proof, err := r.proveSplitVectorSourceV1(ctx, v)
		if err == nil {
			reply.ReadProof = &proof
		}
		return err
	}
	if operation == "vector-split-receipt" {
		return r.proveSplitVectorTargetV1(ctx, v, reply)
	}
	if operation != "vector-split-project" || v.Operation != "project" {
		return ErrFixedPeerVectorProofMissingV1
	}
	leader, err := r.client.leader(ctx, fixed)
	if err != nil || leader != caller {
		return errors.Join(errPeerAuthenticationV1, err)
	}
	r.vector.mutationMu.Lock()
	defer r.vector.mutationMu.Unlock()
	sourceReply, err := r.callSplitVectorGroupV1(ctx, callerGroup, "vector-split-source-proof", v)
	if err != nil || sourceReply.ReadProof == nil {
		return errors.Join(ErrFixedPeerVectorProofMissingV1, err)
	}
	if err := (raftcluster.ReadIndexBarrier{NodeID: caller, GroupID: callerGroup}).Check(*sourceReply.ReadProof); err != nil {
		return err
	}
	if sourceReply.ReadProof.Index < v.SourceIndex {
		return ErrFixedPeerVectorProofStaleV1
	}
	command := v
	command.TargetTerm, command.TargetIndex = 0, 0
	if _, err := r.submitSplitVectorCommandV1(ctx, command, group); err != nil {
		return err
	}
	return r.proveSplitVectorTargetV1(ctx, v, reply)
}

func (r *FixedPeerTCPRuntimeV1) proveSplitVectorTargetV1(ctx context.Context, v commitlog.SplitVectorInsertV1, reply *fixedPeerReplyV1) error {
	group := raftcluster.GroupID(v.TargetGroup)
	if err := r.validateSplitVectorInsertV1(ctx, v, group); err != nil {
		return err
	}
	pending, receipt, known, err := r.vector.collection.VectorPartitionSplitInsertStateV1(v)
	if err != nil || !known || pending != nil || receipt.SourceTerm != v.SourceTerm || receipt.SourceIndex != v.SourceIndex {
		return errors.Join(ErrFixedPeerVectorProofMissingV1, err)
	}
	if v.Operation == "clear" && (receipt.TargetTerm != v.TargetTerm || receipt.TargetIndex != v.TargetIndex || receipt.LiveRevision != v.LiveRevision) {
		return ErrFixedPeerVectorProofStaleV1
	}
	proof, err := r.splitVectorReadFenceV1(ctx, group, receipt.TargetIndex)
	if err != nil {
		return err
	}
	pin, err := r.vector.collection.AcquireVectorPartitionLiveSearchPinV1(r.servingVectorConfigV1().Manifest)
	if err != nil {
		return err
	}
	status := pin.StatusV1()
	present := pin.ContainsLiveIDV1(string(v.ID))
	pin.Release()
	if !present || status.Generation != v.Generation || status.Revision < receipt.LiveRevision {
		return ErrFixedPeerVectorProofStaleV1
	}
	if err := r.validateSplitVectorInsertV1(ctx, v, group); err != nil {
		return err
	}
	complete := v
	complete.Operation = "clear"
	complete.Document = nil
	complete.TargetTerm, complete.TargetIndex, complete.LiveRevision = receipt.TargetTerm, receipt.TargetIndex, receipt.LiveRevision
	token, err := commitlog.EncodeSplitVectorInsertPayloadV1(complete)
	if err != nil {
		return err
	}
	reply.ReadProof = &proof
	reply.VectorInsert = &public.InsertResponseV1{Generation: public.GenerationIDV1{Index: v.Index, Generation: v.Generation},
		PartitionID: v.PartitionID, OwnerGroup: v.TargetGroup, CommitTerm: receipt.TargetTerm, CommitIndex: receipt.TargetIndex,
		AppliedIndex: proof.Index, ProductionConsensus: true, LiveRevision: receipt.LiveRevision,
		VisibilityGeneration: public.GenerationIDV1{Index: v.Index, Generation: v.Generation}, VisibleID: string(v.ID), VisibilityToken: token}
	return nil
}

func (r *FixedPeerTCPRuntimeV1) applySplitVectorSourceInsertV1(ctx context.Context, request VectorPartitionRoutedInsertV1) (public.InsertResponseV1, error) {
	var zero public.InsertResponseV1
	source, target, err := r.splitVectorGroupsV1(ctx)
	if err != nil {
		return zero, err
	}
	if request.SourceGroup != source || request.OwnerGroup != target || request.Identity != r.servingVectorConfigV1().Identity {
		return zero, ErrFixedPeerVectorProofStaleV1
	}
	if err := public.ValidateInsertRequestV1(ctx, request.Request); err != nil {
		return zero, err
	}
	work, err := r.client.peerTransport.admission.request(ctx, "control-write", 2<<20, peerRequestDescendantV1)
	if err != nil {
		return zero, err
	}
	defer work.release()
	ctx = work.ctx
	docDigest := sha256.Sum256(request.Request.Document)
	v := commitlog.SplitVectorInsertV1{Version: 1, Operation: "source", Collection: r.servingVectorConfigV1().Collection.Collection,
		Index: request.Request.Generation.Index, Generation: request.Request.Generation.Generation,
		SourceGroup: string(source), TargetGroup: string(target), CatalogEpoch: request.CatalogProof.Epoch, CatalogDigest: request.CatalogProof.Digest,
		ReadySetDigest: request.ReadySetDigest, ModelDigest: request.RouterModelDigest, PartitionID: request.PartitionID,
		Attempt: bytes.Clone(request.Request.IdempotencyKey), ID: bytes.Clone(request.Request.ID), Vector: slices.Clone(request.Request.Vector),
		Document: bytes.Clone(request.Request.Document), DocumentDigest: hex.EncodeToString(docDigest[:])}
	r.vector.mutationMu.Lock()
	defer r.vector.mutationMu.Unlock()
	if _, err := r.submitSplitVectorCommandV1(ctx, v, source); err != nil {
		return zero, err
	}
	return r.finishSplitVectorPendingV1(ctx, v, request.Forwarded, 1)
}

func (r *FixedPeerTCPRuntimeV1) finishSplitVectorPendingV1(ctx context.Context, identity commitlog.SplitVectorInsertV1, forwarded bool, submissions uint64) (public.InsertResponseV1, error) {
	var zero public.InsertResponseV1
	pending, receipt, known, err := r.vector.collection.VectorPartitionSplitInsertStateV1(identity)
	if err != nil {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(err)
	}
	v := identity
	if known {
		v.Operation = "clear"
		v.Document = nil
		v.SourceTerm, v.SourceIndex = receipt.SourceTerm, receipt.SourceIndex
		v.TargetTerm, v.TargetIndex, v.LiveRevision = receipt.TargetTerm, receipt.TargetIndex, receipt.LiveRevision
	} else {
		if pending == nil {
			return zero, fixedPeerVectorPostCommitAmbiguousV1(ErrFixedPeerVectorProofMissingV1)
		}
		v = *pending
		v.Operation = "project"
		v.Document = nil
	}
	target := raftcluster.GroupID(v.TargetGroup)
	operation := "vector-split-project"
	if known {
		operation = "vector-split-receipt"
	}
	reply, err := r.callSplitVectorGroupV1(ctx, target, operation, v)
	if err != nil || reply.VectorInsert == nil {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, err))
	}
	complete, err := commitlog.DecodeSplitVectorInsertPayloadV1(reply.VectorInsert.VisibilityToken)
	if err != nil {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(err)
	}
	expected, e := v.DigestV1()
	actual, a := complete.DigestV1()
	if e != nil || a != nil || expected != actual || complete.SourceTerm != v.SourceTerm || complete.SourceIndex != v.SourceIndex {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(ErrFixedPeerVectorProofStaleV1)
	}
	if !known {
		submissions += 2 // target project and source clear submissions in this invocation
		if _, err := r.submitSplitVectorCommandV1(ctx, complete, raftcluster.GroupID(v.SourceGroup)); err != nil {
			return zero, fixedPeerVectorPostCommitAmbiguousV1(err)
		}
	}
	if _, err := r.proveSplitVectorSourceV1(ctx, complete); err != nil {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(err)
	}
	// Repeat the target receipt fence after source retirement before exposing a
	// token. A held response cannot survive fresh ACTIVE or current-DB refusal.
	final, err := r.callSplitVectorGroupV1(ctx, target, "vector-split-receipt", complete)
	if err != nil || final.VectorInsert == nil {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, err))
	}
	response := *final.VectorInsert
	forwards := uint64(0)
	if forwarded {
		forwards = 1
	}
	response.Counters = public.MutationCountersV1{Routes: 1, Forwards: forwards, Commits: submissions, Replications: submissions, Applies: submissions, VisibilityProofs: 2}
	return response, nil
}

// Visibility tokens require a durable exact receipt and real target quorum
// watermark. Ordinary strict searches with no token do not call this path.
func (r *FixedPeerTCPRuntimeV1) requireSplitVectorVisibilityV1(ctx context.Context, request public.SearchRequestV1) error {
	if len(request.VisibilityToken) == 0 {
		return nil
	}
	v, err := commitlog.DecodeSplitVectorInsertPayloadV1(request.VisibilityToken)
	if err != nil || v.Operation != "clear" || v.Index != request.Generation.Index || v.Generation != request.Generation.Generation {
		return &public.ErrorV1{Code: public.ErrorInvalidRequestV1, Err: errors.Join(ErrFixedPeerVectorProofStaleV1, err)}
	}
	source, target, err := r.splitVectorGroupsV1(ctx)
	if err != nil || v.SourceGroup != string(source) || v.TargetGroup != string(target) {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	reply, err := r.callSplitVectorGroupV1(ctx, target, "vector-split-receipt", v)
	if err != nil || reply.VectorInsert == nil || !bytes.Equal(reply.VectorInsert.VisibilityToken, request.VisibilityToken) {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	return nil
}
