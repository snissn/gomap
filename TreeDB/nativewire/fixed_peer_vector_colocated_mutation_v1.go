package nativewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

const colocatedVectorVisibilityMagicV1 = "CVM1"

type colocatedVectorVisibilityV1 struct {
	Scope   commitlog.ColocatedVectorMutationScopeV1
	Attempt []byte
	Outcome commitlog.ColocatedVectorMutationOutcomeV1
}

func colocatedVectorMutationScopeV1(request VectorPartitionRoutedMutationV1) (commitlog.ColocatedVectorMutationScopeV1, error) {
	raw, err := json.Marshal(struct {
		Identity      raftplacement.VectorPartitionLifecycleIdentityV1
		Catalog       raftplacement.CatalogProofV1
		Ready, Router string
		Owner         raftcluster.GroupID
	}{request.Identity, request.CatalogProof, request.ReadySetDigest, request.RouterModelDigest, request.OwnerGroup})
	if err != nil {
		return commitlog.ColocatedVectorMutationScopeV1{}, err
	}
	return commitlog.ColocatedVectorMutationScopeV1{Version: 1, Index: request.Request.Generation.Index, Generation: request.Request.Generation.Generation, OwnerGroup: string(request.OwnerGroup), Digest: sha256.Sum256(raw)}, nil
}

func (r *FixedPeerTCPRuntimeV1) SubmitVectorPartitionColocatedMutationV1(ctx context.Context, request VectorPartitionRoutedMutationV1) (public.MutationResponseV1, error) {
	if r == nil || r.vector == nil {
		return public.MutationResponseV1{}, ErrFixedPeerVectorUnavailableV1
	}
	vector := r.servingVectorConfigV1()
	if vector == nil || r.authority == nil || vector.Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) || vector.Identity.SourceFormat == 2 {
		return public.MutationResponseV1{}, ErrFixedPeerVectorUnavailableV1
	}
	owner, err := vectorPartitionColocatedOwnerV1(vector.Manifest, vector.Placement)
	if err != nil || owner != request.OwnerGroup {
		return public.MutationResponseV1{}, errors.Join(ErrFixedPeerVectorWrongOwnerV1, err)
	}
	source, err := r.authority.RouteDocumentToken(ctx, request.CatalogProof, vector.Collection, raftplacement.DocumentIDTokenV1(request.Request.ID))
	if err != nil || source.GroupID() != owner {
		return public.MutationResponseV1{}, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	group, err := r.colocatedVectorGroupV1(owner)
	if err != nil {
		return public.MutationResponseV1{}, err
	}
	leader, err := r.client.leader(ctx, *group)
	if err != nil {
		return public.MutationResponseV1{}, err
	}
	request.Forwarded = leader != r.config.NodeID
	reply, err := r.client.call(ctx, leader, "vector-forward", fixedPeerRequestV1{VectorMutation: &request}, true)
	if err != nil {
		return public.MutationResponseV1{}, err
	}
	if reply.VectorMutation == nil {
		return public.MutationResponseV1{}, ErrFixedPeerVectorUnavailableV1
	}
	return *reply.VectorMutation, nil
}

func (r *FixedPeerTCPRuntimeV1) colocatedVectorGroupV1(owner raftcluster.GroupID) (*FixedPeerTCPGroupV1, error) {
	if r == nil || r.vector == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	for i := range r.config.Groups {
		if r.config.Groups[i].ID == owner {
			return &r.config.Groups[i], nil
		}
	}
	return nil, ErrFixedPeerVectorWrongOwnerV1
}

func (r *FixedPeerTCPRuntimeV1) validateVectorColocatedOwnerV1(ctx context.Context, request VectorPartitionRoutedMutationV1) error {
	if err := r.requirePreparedVectorCurrentDBV1(); err != nil {
		return err
	}
	vector := r.servingVectorConfigV1()
	if vector == nil || r.vector == nil || r.authority == nil || vector.Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) || vector.Identity.SourceFormat == 2 || request.SourceGroup != "" {
		return ErrFixedPeerVectorUnavailableV1
	}
	if len(request.Request.ID) > commitlog.ColocatedVectorMutationMaxIdentityBytesV1 || len(request.Request.IdempotencyKey) > commitlog.ColocatedVectorMutationMaxIdentityBytesV1 || len(request.Request.Document) > commitlog.ColocatedVectorMutationMaxDocumentBytesV1 {
		return ErrFixedPeerVectorDocumentV1
	}
	owner, err := vectorPartitionColocatedOwnerV1(vector.Manifest, vector.Placement)
	if err != nil || owner != request.OwnerGroup || owner != r.vector.dataGroup {
		return errors.Join(ErrFixedPeerVectorWrongOwnerV1, err)
	}
	if !request.Delete {
		return r.validateVectorInsertOwnerV1(ctx, request.VectorPartitionRoutedInsertV1)
	}
	if len(request.Request.Document) != 0 || len(request.Request.Vector) != 0 {
		return ErrFixedPeerVectorDocumentV1
	}
	if err := public.ValidateDeleteRequestV1(ctx, public.DeleteRequestV1{Version: request.Request.Version, Generation: request.Request.Generation, ID: request.Request.ID, IdempotencyKey: request.Request.IdempotencyKey, Deadline: request.Request.Deadline}); err != nil {
		return err
	}
	if request.Identity != vector.Identity || request.Request.Generation.Index != vector.Identity.Index.IndexName || request.Request.Generation.Generation != vector.Identity.Generation || request.CatalogProof.Epoch != vector.Identity.Index.CatalogEpoch || request.CatalogProof.Digest != vector.Identity.Index.CatalogDigest || request.ReadySetDigest == "" || request.RouterModelDigest == "" {
		return ErrFixedPeerVectorProofStaleV1
	}
	data := r.data[owner]
	if data == nil || data.fsm == nil {
		return ErrFixedPeerVectorWrongOwnerV1
	}
	status, err := data.provider.RuntimeStatusV1(ctx)
	if err != nil || status.State != "Leader" || status.LeaderID != r.config.NodeID {
		return errors.Join(raftcluster.ErrNotLeader, err)
	}
	catalog, err := r.catalogFence(ctx)
	if err != nil || catalog.Epoch != request.CatalogProof.Epoch || catalog.Digest != request.CatalogProof.Digest {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	source, err := r.authority.RouteDocumentToken(ctx, request.CatalogProof, vector.Collection, raftplacement.DocumentIDTokenV1(request.Request.ID))
	if err != nil || source.GroupID() != owner {
		return errors.Join(ErrFixedPeerVectorWrongOwnerV1, err)
	}
	record, exists := r.authority.VectorPartitionLifecycleRecordV1(request.Identity)
	if !exists || record.ReadySetDigest != request.ReadySetDigest {
		return ErrFixedPeerVectorProofStaleV1
	}
	if err := record.CanSearch(raftplacement.VectorPartitionLifecycleSearchProofV1{Identity: request.Identity, ReadySetDigest: request.ReadySetDigest}); err != nil {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	backend, err := r.vector.ensureBackendV1(ctx)
	if err != nil {
		return err
	}
	coordinator := backend.opts.Topology.Coordinator()
	lease, err := coordinator.acquireRouterSessionV1(ctx, request.Request.Generation.Index, request.Request.Generation.Generation)
	if err != nil {
		return err
	}
	defer lease.Close()
	statusRouter := lease.session.router.Status()
	ready, err := coordinator.validateReplicatedLifecycle(ctx, statusRouter)
	if err != nil || ready != request.ReadySetDigest || statusRouter.ModelDigest != request.RouterModelDigest {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	partition, group, err := vectorPartitionMutationOwnerV1(statusRouter.Manifest, coordinator.placement, 0)
	if err != nil || partition != request.PartitionID || group != owner {
		return errors.Join(ErrFixedPeerVectorWrongOwnerV1, err)
	}
	return r.requirePreparedVectorCurrentDBV1()
}

func fixedPeerVectorColocatedEntryV1(collection string, catalog uint64, request VectorPartitionRoutedMutationV1, scope commitlog.ColocatedVectorMutationScopeV1) ([]byte, error) {
	raw, err := commitlog.EncodeColocatedVectorMutationScopeV1(scope)
	if err != nil {
		return nil, err
	}
	command := iwire.CommandReplaceBatch
	if request.Delete {
		command = iwire.CommandDeleteBatch
	}
	sections := []iwire.Section{{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: command, Version: 1})}, {ID: iwire.SectionIdempotencyKey, Bytes: slices.Clone(request.Request.IdempotencyKey)}, {ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalog)}, collectionNameRef(collection), {ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, request.Request.ID)}, {ID: iwire.SectionColocatedVectorMutationScopeV1, Bytes: raw}}
	if !request.Delete {
		sections = append(sections, documentFormatSection(collections.DocumentFormatJSON), iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, request.Request.Document)}, iwire.Section{ID: iwire.SectionReplacementMode, Bytes: binary.AppendUvarint(nil, 1)})
	}
	validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		return nil, err
	}
	return iwire.AppendDeterministicEntry(nil, validated)
}

func (r *FixedPeerTCPRuntimeV1) applyVectorColocatedMutationV1(ctx context.Context, request VectorPartitionRoutedMutationV1) (public.MutationResponseV1, error) {
	var zero public.MutationResponseV1
	if r == nil || r.vector == nil {
		return zero, ErrFixedPeerVectorUnavailableV1
	}
	if err := r.vector.lockMutationAdmissionV1(ctx); err != nil {
		return zero, err
	}
	defer r.vector.mutationMu.Unlock()
	if err := r.validateVectorColocatedOwnerV1(ctx, request); err != nil {
		return zero, err
	}
	scope, err := colocatedVectorMutationScopeV1(request)
	if err != nil {
		return zero, err
	}
	data := r.data[request.OwnerGroup]
	guard, ok, err := data.fsm.CurrentCatalogVersion(ctx)
	if err != nil || !ok {
		return zero, errors.Join(ErrFixedPeerVectorUnavailableV1, err)
	}
	original, known, err := r.vector.collection.ReadVectorPartitionColocatedOutcomeV1(scope, request.Request.IdempotencyKey, [32]byte{})
	if err != nil {
		return zero, err
	}
	if known {
		guard = original.ExpectedCatalogVersion
	}
	resultGuard, resultDigest, resultKnown, err := data.fsm.AppliedIdempotencyGuardV1(ctx, request.Request.IdempotencyKey)
	if err != nil {
		return zero, err
	}
	if resultKnown && !known {
		return zero, ErrFixedPeerVectorProofStaleV1
	}
	if resultKnown && resultGuard != guard {
		return zero, ErrFixedPeerVectorProofStaleV1
	}
	entry, err := fixedPeerVectorColocatedEntryV1(r.servingVectorConfigV1().Collection.Collection, guard, request, scope)
	if err != nil {
		return zero, err
	}
	digest := raftentry.CommandDigestV1ForBytes(entry, raftentry.DecodeOptions{})
	if (known && original.CommandDigest != [32]byte(digest)) || (resultKnown && resultDigest != digest) {
		return zero, &public.ErrorV1{Code: public.ErrorInvalidRequestV1, Err: errors.New("idempotency key conflicts with original colocated mutation")}
	}
	base, ok := r.localRegistry.Lookup(request.OwnerGroup)
	if !ok {
		return zero, ErrFixedPeerVectorWrongOwnerV1
	}
	submitter, ok := base.(raftcluster.CommandSubmitterWithPreCommitV1)
	if !ok {
		return zero, ErrFixedPeerVectorUnavailableV1
	}
	resolved, err := raftplacement.Validate(r.servingVectorConfigV1().Catalog)
	if err != nil {
		return zero, err
	}
	group, ok := resolved.Group(request.OwnerGroup)
	if !ok {
		return zero, ErrFixedPeerVectorWrongOwnerV1
	}
	members := make([]string, len(group.Members))
	for i := range group.Members {
		members[i] = string(group.Members[i])
	}
	key := sha256.Sum256(request.Request.IdempotencyKey)
	metadata := raftentry.RequestMetadataV1{RequestID: binary.BigEndian.Uint64(key[:8]), AckPolicy: iwire.AckRaftCommitted, ClusterRouteKnown: true, ClusterRouteDatabase: r.servingVectorConfigV1().Collection.Database, ClusterRouteCatalog: r.servingVectorConfigV1().Collection.Catalog, ClusterRouteCollection: r.servingVectorConfigV1().Collection.Collection, ClusterRouteShape: "vector_partition_exact_id", ClusterRouteGroupID: string(request.OwnerGroup), ClusterRouteMembers: members, ClusterRouteLeaderHint: string(group.LeaderHint), ClusterRoutePlacementMode: "vector_partition", ClusterRouteKey: "_id", CatalogMetaEpoch: request.CatalogProof.Epoch, CatalogMetaDigest: request.CatalogProof.Digest}
	if !request.Request.Deadline.IsZero() {
		metadata.DeadlineUnixNanos = request.Request.Deadline.UnixNano()
	}
	result, err := submitter.SubmitCommandEntryWithPreCommitV1(ctx, entry, metadata, func(commitCtx context.Context) error { return r.validateVectorColocatedOwnerV1(commitCtx, request) })
	if err = fixedPeerVectorSubmitErrorV1(result, err); err != nil {
		return zero, err
	}
	if result.ActualAck != iwire.AckRaftCommitted || !result.CommittedRecoverable || !result.CommittedApplied || !result.Evidence.ProvesProductionConsensus() || result.Evidence.GroupID != request.OwnerGroup || result.Evidence.NodeID != r.config.NodeID || result.Evidence.LeaderID != r.config.NodeID || result.Evidence.Term != result.CommittedEntry.Term || result.Evidence.Index != result.CommittedEntry.Index || result.CommittedEntry.Term == 0 || result.CommittedEntry.Index == 0 || (result.ApplyResult.Status != raftentry.ApplyStatusApplied && result.ApplyResult.Status != raftentry.ApplyStatusAlreadyApplied) {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(raftcluster.ErrCommitNotProven)
	}
	status, err := data.provider.RuntimeStatusV1(ctx)
	if err != nil || status.State != "Leader" || status.LeaderID != r.config.NodeID || status.RaftAppliedIndex < result.CommittedEntry.Index {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorUnavailableV1, err))
	}
	outcome, known, err := r.vector.collection.ReadVectorPartitionColocatedOutcomeV1(scope, request.Request.IdempotencyKey, [32]byte(digest))
	if err != nil || !known || outcome.Index > status.RaftAppliedIndex {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(errors.Join(ErrFixedPeerVectorProofStaleV1, err))
	}
	if err := r.colocatedVectorLiveFloorV1(ctx, scope, outcome); err != nil {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(err)
	}
	// Tokens omit the replica-local LSN. Every reader validates its own coverage.
	outcome.AppliedCommandLSN = 0
	raw, err := json.Marshal(colocatedVectorVisibilityV1{Scope: scope, Attempt: request.Request.IdempotencyKey, Outcome: outcome})
	if err != nil {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(err)
	}
	response := public.MutationResponseV1{Generation: request.Request.Generation, OwnerGroup: string(request.OwnerGroup), CommitTerm: outcome.Term, CommitIndex: outcome.Index, AppliedIndex: status.RaftAppliedIndex, ProductionConsensus: true, Coverage: outcome.Coverage, LiveRevision: outcome.Revision, Matched: outcome.Matched, VisibilityToken: append([]byte(colocatedVectorVisibilityMagicV1), raw...), Counters: public.MutationCountersV1{Routes: 1, Commits: 1, Replications: 1, Applies: 1, VisibilityProofs: 1}}
	if request.Delete {
		response.Deleted = outcome.Affected
	} else {
		response.Modified = outcome.Affected
	}
	if request.Forwarded {
		response.Counters.Forwards = 1
	}
	if err := r.validateVectorColocatedOwnerV1(ctx, request); err != nil {
		return zero, fixedPeerVectorPostCommitAmbiguousV1(err)
	}
	return response, nil
}

func (r *FixedPeerTCPRuntimeV1) colocatedVectorLiveFloorV1(ctx context.Context, scope commitlog.ColocatedVectorMutationScopeV1, outcome commitlog.ColocatedVectorMutationOutcomeV1) error {
	if err := r.requirePreparedVectorCurrentDBV1(); err != nil {
		return err
	}
	vector := r.servingVectorConfigV1()
	if vector == nil || scope.Index != vector.Identity.Index.IndexName || scope.Generation != vector.Identity.Generation {
		return ErrFixedPeerVectorProofStaleV1
	}
	owner, err := vectorPartitionColocatedOwnerV1(vector.Manifest, vector.Placement)
	if err != nil || string(owner) != scope.OwnerGroup {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if err := r.vector.collection.EnsureVectorPartitionLiveBindingV1(ctx, vector.Manifest); err != nil {
		return err
	}
	pin, err := r.vector.collection.AcquireVectorPartitionLiveSearchPinV1(vector.Manifest)
	if err != nil {
		return err
	}
	defer pin.Release()
	status := pin.StatusV1()
	if status.Generation != scope.Generation || status.Coverage < outcome.Coverage || status.Revision < outcome.Revision {
		return ErrFixedPeerVectorProofStaleV1
	}
	return r.requirePreparedVectorCurrentDBV1()
}

func (r *FixedPeerTCPRuntimeV1) requireColocatedVectorVisibilityV1(ctx context.Context, request public.SearchRequestV1) error {
	raw := request.VisibilityToken
	if !bytes.HasPrefix(raw, []byte(colocatedVectorVisibilityMagicV1)) || len(raw) > 8192 {
		return ErrFixedPeerVectorProofStaleV1
	}
	token, err := decodeColocatedVectorVisibilityV1(raw)
	if err != nil || token.Scope.Index != request.Generation.Index || token.Scope.Generation != request.Generation.Generation {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	group, err := r.colocatedVectorGroupV1(raftcluster.GroupID(token.Scope.OwnerGroup))
	if err != nil {
		return err
	}
	leader, err := r.client.leader(ctx, *group)
	if err != nil {
		return err
	}
	if leader != r.config.NodeID {
		_, err := r.client.call(ctx, leader, "vector-mutation-visibility", fixedPeerRequestV1{VectorMutationVisibility: raw}, false)
		return err
	}
	return r.proveColocatedVectorVisibilityV1(ctx, token)
}

func decodeColocatedVectorVisibilityV1(raw []byte) (colocatedVectorVisibilityV1, error) {
	var token colocatedVectorVisibilityV1
	if !bytes.HasPrefix(raw, []byte(colocatedVectorVisibilityMagicV1)) || len(raw) > 8192 {
		return token, ErrFixedPeerVectorProofStaleV1
	}
	decoder := json.NewDecoder(bytes.NewReader(raw[len(colocatedVectorVisibilityMagicV1):]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&token); err != nil {
		return token, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return token, ErrFixedPeerVectorProofStaleV1
	}
	if token.Scope.ValidateV1() != nil || len(token.Attempt) == 0 || len(token.Attempt) > commitlog.ColocatedVectorMutationMaxIdentityBytesV1 {
		return token, ErrFixedPeerVectorProofStaleV1
	}
	return token, nil
}

func (r *FixedPeerTCPRuntimeV1) proveColocatedVectorVisibilityV1(ctx context.Context, token colocatedVectorVisibilityV1) error {
	if err := r.requirePreparedVectorCurrentDBV1(); err != nil {
		return err
	}
	// A fresh ACTIVE/catalog/router scope must reproduce the token's digest.
	backend, err := r.vector.ensureBackendV1(ctx)
	if err != nil {
		return err
	}
	coordinator := backend.opts.Topology.Coordinator()
	lease, err := coordinator.acquireRouterSessionV1(ctx, token.Scope.Index, token.Scope.Generation)
	if err != nil {
		return err
	}
	defer lease.Close()
	status := lease.session.router.Status()
	ready, err := coordinator.validateReplicatedLifecycle(ctx, status)
	if err != nil {
		return err
	}
	fresh, err := colocatedVectorMutationScopeV1(VectorPartitionRoutedMutationV1{VectorPartitionRoutedInsertV1: VectorPartitionRoutedInsertV1{Request: public.InsertRequestV1{Generation: public.GenerationIDV1{Index: token.Scope.Index, Generation: token.Scope.Generation}}, Identity: backend.opts.Identity, CatalogProof: raftplacement.CatalogProofV1{Epoch: backend.opts.Identity.Index.CatalogEpoch, Digest: backend.opts.Identity.Index.CatalogDigest}, ReadySetDigest: ready, RouterModelDigest: status.ModelDigest, OwnerGroup: raftcluster.GroupID(token.Scope.OwnerGroup)}})
	if err != nil || fresh != token.Scope {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	outcome, known, err := r.vector.collection.ReadVectorPartitionColocatedOutcomeV1(token.Scope, token.Attempt, token.Outcome.CommandDigest)
	if err != nil || !known {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	outcome.AppliedCommandLSN = 0
	if outcome != token.Outcome {
		return ErrFixedPeerVectorProofStaleV1
	}
	if _, err := r.splitVectorReadFenceV1(ctx, raftcluster.GroupID(token.Scope.OwnerGroup), outcome.Index); err != nil {
		return err
	}
	return r.colocatedVectorLiveFloorV1(ctx, token.Scope, outcome)
}
