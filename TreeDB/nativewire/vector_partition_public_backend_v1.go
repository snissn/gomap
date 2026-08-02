package nativewire

// This adapter is the sole bridge from the supported vectorpartition service
// to the production topology and replicated lifecycle workflow. Its options
// are node-construction inputs; callers of vectorpartition.ServiceV1 never see
// these nativewire or raftplacement values.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type VectorPartitionPublicBackendOptionsV1 struct {
	Topology       *VectorPartitionProductionTopologyV1
	RequestBase    VectorPartitionCoordinatorRequestV1
	Lifecycle      raftplacement.VectorPartitionLifecycleCoordinatorV1
	Identity       raftplacement.VectorPartitionLifecycleIdentityV1
	RequiredGroups []raftcluster.GroupID
	Builder        raftplacement.VectorPartitionLifecycleGroupBuilderV1
	MutationEpoch  uint64
	// RebuildRequest is an optional node-owned enqueue hook. It deliberately
	// does not run a rebuild inline; scheduling and recovery policy are #4018.
	RebuildRequest func(context.Context) error
}

type VectorPartitionPublicBackendV1 struct {
	opts VectorPartitionPublicBackendOptionsV1
}

func NewVectorPartitionPublicBackendV1(opts VectorPartitionPublicBackendOptionsV1) (*VectorPartitionPublicBackendV1, error) {
	if opts.Topology == nil || opts.Topology.Coordinator() == nil || opts.Identity.Generation == 0 || opts.Identity.Index.IndexName == "" || len(opts.RequiredGroups) == 0 {
		return nil, errors.New("nativewire: public vector partition backend is incomplete")
	}
	if opts.Builder == nil || opts.Lifecycle.Authority == nil || opts.Lifecycle.Committer == nil {
		return nil, errors.New("nativewire: public vector partition backend requires lifecycle authority and group builder")
	}
	return &VectorPartitionPublicBackendV1{opts: opts}, nil
}

func (b *VectorPartitionPublicBackendV1) SearchVectorPartitionV1(ctx context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
	if b == nil || b.opts.Topology.Status().Closed {
		return public.SearchResponseV1{}, errors.New("production topology is unavailable")
	}
	if err := b.checkID(request.Generation); err != nil {
		return public.SearchResponseV1{}, err
	}
	r := b.opts.RequestBase
	r.Version, r.Query, r.IndexName, r.Metric, r.TopK, r.PartitionProbes, r.EfSearch, r.Consistency = request.Version, request.Query, request.Generation.Index, VectorPartitionShardSearchMetricV1(request.Metric), request.TopK, request.Probes, request.EfSearch, VectorPartitionShardSearchConsistencyV1(request.Consistency)
	r.RequestBytesLimit, r.CandidateBytesLimit, r.ResponseBytesLimit, r.MergeEntriesLimit = request.Limits.RequestBytes, request.Limits.CandidateBytes, request.Limits.ResponseBytes, request.Limits.MergeEntries
	if !request.Deadline.IsZero() {
		r.DeadlineUnixNano = request.Deadline.UnixNano()
	}
	response, err := b.opts.Topology.Coordinator().Search(ctx, r)
	if err != nil {
		return public.SearchResponseV1{}, publicBackendErrorV1(err)
	}
	result := public.SearchResponseV1{Generation: request.Generation, Counters: public.SearchCountersV1{Requests: response.Counters.Requests, RPCs: response.Counters.RPCs, Retries: response.Counters.Retries, Redirects: response.Counters.Redirects, Candidates: response.Counters.Candidates, Edges: response.Counters.Edges}, Timing: public.SearchTimingV1{Total: time.Duration(response.Timing.TotalNanos)}}
	result.Neighbors = make([]public.NeighborV1, len(response.Neighbors))
	for i, n := range response.Neighbors {
		result.Neighbors[i] = public.NeighborV1{ID: n.ID, Score: n.Score}
	}
	return result, nil
}

func (b *VectorPartitionPublicBackendV1) RegisterVectorPartitionV1(ctx context.Context, registration public.GenerationRegistrationV1) (public.GenerationStatusV1, error) {
	if err := b.checkID(registration.GenerationIDV1); err != nil {
		return public.GenerationStatusV1{}, err
	}
	i := b.opts.Identity
	if registration.SourceGeneration == 0 || registration.SourceChecksum == 0 || registration.SourceSchemaHash == 0 || registration.SourceRowCount == 0 || registration.SourceGeneration != i.Source.Generation || registration.SourceChecksum != i.Source.Checksum || registration.SourceSchemaHash != i.Source.SchemaHash || registration.SourceRowCount != i.Source.RowCount {
		return public.GenerationStatusV1{}, &public.ErrorV1{Code: public.ErrorGenerationMismatchV1, Err: errors.New("generation source does not match bound topology")}
	}
	record, err := b.opts.Lifecycle.BeginBuildV1(ctx, i, b.opts.RequiredGroups, 0, b.opts.MutationEpoch)
	if err != nil {
		return public.GenerationStatusV1{}, err
	}
	for _, group := range b.opts.RequiredGroups {
		record, err = b.opts.Lifecycle.BuildAndRecordGroupReadyV1(ctx, b.opts.Builder, i, group)
		if err != nil {
			return public.GenerationStatusV1{}, err
		}
	}
	return publicStatusV1(record, false), nil
}
func (b *VectorPartitionPublicBackendV1) GenerationStatusV1(_ context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.checkID(id); err != nil {
		return public.GenerationStatusV1{}, err
	}
	r, ok := b.opts.Lifecycle.Authority.VectorPartitionLifecycleRecordV1(b.opts.Identity)
	if !ok {
		return public.GenerationStatusV1{Generation: id, State: public.GenerationAbsentV1}, nil
	}
	return publicStatusV1(r, false), nil
}
func (b *VectorPartitionPublicBackendV1) PrepareVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.checkID(id); err != nil {
		return public.GenerationStatusV1{}, err
	}
	r, err := b.opts.Lifecycle.PrepareV1(ctx, b.opts.Identity)
	return publicStatusV1(r, false), err
}
func (b *VectorPartitionPublicBackendV1) ActivateVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.checkID(id); err != nil {
		return public.GenerationStatusV1{}, err
	}
	r, err := b.opts.Lifecycle.ActivateV1(ctx, b.opts.Identity)
	return publicStatusV1(r, err == nil && r.State == raftplacement.VectorPartitionLifecycleActiveV1), err
}
func (b *VectorPartitionPublicBackendV1) InvalidateVectorPartitionV1(ctx context.Context, id public.GenerationIDV1, reason string) (public.GenerationStatusV1, error) {
	if err := b.checkID(id); err != nil {
		return public.GenerationStatusV1{}, err
	}
	_, err := b.opts.Lifecycle.InvalidateBeforeRelevantMutationV1(ctx, b.opts.Identity.Index, reason)
	if err != nil {
		return public.GenerationStatusV1{}, err
	}
	return b.GenerationStatusV1(ctx, id)
}
func (b *VectorPartitionPublicBackendV1) RetireVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.checkID(id); err != nil {
		return public.GenerationStatusV1{}, err
	}
	r, err := b.opts.Lifecycle.RetireV1(ctx, b.opts.Identity)
	return publicStatusV1(r, false), err
}
func (b *VectorPartitionPublicBackendV1) RequestVectorPartitionRebuildV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.checkID(id); err != nil {
		return public.GenerationStatusV1{}, err
	}
	if b.opts.RebuildRequest == nil {
		return public.GenerationStatusV1{}, errors.New("rebuild request is not configured")
	}
	if err := b.opts.RebuildRequest(ctx); err != nil {
		return public.GenerationStatusV1{}, err
	}
	return b.GenerationStatusV1(ctx, id)
}
func (b *VectorPartitionPublicBackendV1) VectorPartitionCleanupEligibilityV1(ctx context.Context, id public.GenerationIDV1) (public.CleanupEligibilityV1, error) {
	status, err := b.GenerationStatusV1(ctx, id)
	if err != nil {
		return public.CleanupEligibilityV1{}, err
	}
	return public.CleanupEligibilityV1{Eligible: status.State == public.GenerationCleanableV1, Status: status}, nil
}

// OperationsHealthV1 derives operator readiness from the live catalog and
// lifecycle authority; it intentionally does not trust a cached frontend
// label. It is passed directly to vectorpartition.OperationsV1 at node setup.
func (b *VectorPartitionPublicBackendV1) OperationsHealthV1(ctx context.Context) (public.OperationsHealthV1, error) {
	if b == nil || b.opts.Topology == nil || b.opts.Lifecycle.Authority == nil {
		return public.OperationsHealthV1{Reason: "authority_unavailable"}, errors.New("production vector topology or lifecycle authority is unavailable")
	}
	id := public.GenerationIDV1{Index: b.opts.Identity.Index.IndexName, Generation: b.opts.Identity.Generation}
	topology := b.opts.Topology.Status()
	if topology.Closed || !topology.Ready {
		return public.OperationsHealthV1{Generation: id, Reason: "topology_unavailable"}, nil
	}
	proof, err := b.opts.Lifecycle.Authority.CurrentCatalogProof(ctx)
	if err != nil {
		return public.OperationsHealthV1{Generation: id, Reason: "catalog_unavailable"}, err
	}
	if proof.Epoch != b.opts.Identity.Index.CatalogEpoch || proof.Digest != b.opts.Identity.Index.CatalogDigest {
		return public.OperationsHealthV1{Generation: id, Reason: "catalog_mismatch"}, nil
	}
	record, ok := b.opts.Lifecycle.Authority.VectorPartitionLifecycleRecordV1(b.opts.Identity)
	if !ok {
		return public.OperationsHealthV1{Generation: id, State: public.GenerationAbsentV1, Reason: "generation_absent"}, nil
	}
	status := publicStatusV1(record, false)
	if record.Identity.Source != b.opts.Identity.Source {
		return public.OperationsHealthV1{Generation: id, State: status.State, Reason: "source_mismatch"}, nil
	}
	if !status.Active || !status.Ready {
		return public.OperationsHealthV1{Generation: id, State: status.State, Reason: "lifecycle_not_active"}, nil
	}
	if len(record.RequiredGroups) != len(b.opts.RequiredGroups) || len(record.ReadyGroups) != len(record.RequiredGroups) {
		return public.OperationsHealthV1{Generation: id, State: status.State, Reason: "group_assets_unavailable"}, nil
	}
	return public.OperationsHealthV1{Ready: true, Generation: id, State: status.State, Reason: "ready"}, nil
}

func (b *VectorPartitionPublicBackendV1) checkID(id public.GenerationIDV1) error {
	if b == nil || id.Index != b.opts.Identity.Index.IndexName || id.Generation != b.opts.Identity.Generation {
		return &public.ErrorV1{Code: public.ErrorGenerationMismatchV1, Err: fmt.Errorf("generation does not match bound topology")}
	}
	return nil
}
func publicStatusV1(r raftplacement.VectorPartitionLifecycleRecordV1, _ bool) public.GenerationStatusV1 {
	return public.GenerationStatusV1{Generation: public.GenerationIDV1{Index: r.Identity.Index.IndexName, Generation: r.Identity.Generation}, State: public.GenerationStateV1(r.State), Active: r.State == raftplacement.VectorPartitionLifecycleActiveV1, Ready: len(r.RequiredGroups) != 0 && len(r.ReadyGroups) == len(r.RequiredGroups), Revision: r.Revision}
}

func publicBackendErrorV1(err error) error {
	if errors.Is(err, context.Canceled) {
		return &public.ErrorV1{Code: public.ErrorCanceledV1, Err: err}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &public.ErrorV1{Code: public.ErrorDeadlineExceededV1, Err: err}
	}
	var coordinatorErr *VectorPartitionCoordinatorErrorV1
	if errors.As(err, &coordinatorErr) {
		switch coordinatorErr.Code {
		case VectorPartitionCoordinatorErrorInvalidRequestV1, VectorPartitionCoordinatorErrorRouteMismatchV1, VectorPartitionCoordinatorErrorBudgetExceededV1, VectorPartitionCoordinatorErrorMalformedResponseV1:
			return &public.ErrorV1{Code: public.ErrorInvalidRequestV1, Err: err}
		case VectorPartitionCoordinatorErrorGenerationMismatchV1:
			return &public.ErrorV1{Code: public.ErrorGenerationMismatchV1, Err: err}
		case VectorPartitionCoordinatorErrorCanceledV1:
			return &public.ErrorV1{Code: public.ErrorCanceledV1, Err: err}
		case VectorPartitionCoordinatorErrorDeadlineV1:
			return &public.ErrorV1{Code: public.ErrorDeadlineExceededV1, Err: err}
		}
	}
	return &public.ErrorV1{Code: public.ErrorUnavailableV1, Err: err}
}
