package nativewire

// This adapter is the sole bridge from the supported vectorpartition service
// to the production topology and replicated lifecycle workflow. Its options
// are node-construction inputs; callers of vectorpartition.ServiceV1 never see
// these nativewire or raftplacement values.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type VectorPartitionPublicBackendOptionsV1 struct {
	Topology       *VectorPartitionProductionTopologyV1
	RequestBase    VectorPartitionCoordinatorRequestV1
	Lifecycle      raftplacement.VectorPartitionLifecycleCoordinatorV1
	ReadFence      CatalogMetaLinearizableAppliedIndexProviderV1
	Identity       raftplacement.VectorPartitionLifecycleIdentityV1
	RequiredGroups []raftcluster.GroupID
	Builder        raftplacement.VectorPartitionLifecycleGroupBuilderV1
	MutationEpoch  uint64
	// RebuildRequest is an optional node-owned enqueue hook. It deliberately
	// does not run a rebuild inline; scheduling and recovery policy are #4018.
	RebuildRequest func(context.Context) error
}

type VectorPartitionPublicBackendV1 struct {
	opts     VectorPartitionPublicBackendOptionsV1
	sequence atomic.Uint64
}

const vectorPartitionPublicRequestSuffixBytesV1 = 1 + 16

func NewVectorPartitionPublicBackendV1(opts VectorPartitionPublicBackendOptionsV1) (*VectorPartitionPublicBackendV1, error) {
	if opts.Topology == nil || opts.Topology.Coordinator() == nil || opts.Identity.Generation == 0 || opts.Identity.Index.IndexName == "" || len(opts.RequiredGroups) == 0 || opts.RequestBase.RequestID == "" || opts.RequestBase.CancellationID == "" {
		return nil, errors.New("nativewire: public vector partition backend is incomplete")
	}
	maxIdentityBytes := opts.Topology.Coordinator().limits.MaxIdentityBytes
	if len(opts.RequestBase.RequestID)+vectorPartitionPublicRequestSuffixBytesV1 > maxIdentityBytes || len(opts.RequestBase.CancellationID)+vectorPartitionPublicRequestSuffixBytesV1 > maxIdentityBytes {
		return nil, errors.New("nativewire: public vector partition request identity exceeds coordinator limit after suffix")
	}
	if opts.Builder == nil || opts.Lifecycle.Authority == nil || opts.Lifecycle.Committer == nil || opts.ReadFence == nil {
		return nil, errors.New("nativewire: public vector partition backend requires lifecycle authority, linearizable read fence, and group builder")
	}
	return &VectorPartitionPublicBackendV1{opts: opts}, nil
}

func (b *VectorPartitionPublicBackendV1) SearchVectorPartitionV1(ctx context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
	started := time.Now()
	r, err := b.coordinatorRequestV1(request)
	if err != nil {
		return public.SearchResponseV1{}, err
	}
	adapterNanos := time.Since(started)
	response, err := b.opts.Topology.searchStrictV1(ctx, r)
	return b.publicSearchResponseV1(request, response, err, started, adapterNanos)
}

func (b *VectorPartitionPublicBackendV1) SearchVectorPartitionFastV1(ctx context.Context, request public.SearchRequestV1, options public.FastSearchOptionsV1) (public.SearchResponseV1, public.FastSearchEvidenceV1, error) {
	started := time.Now()
	r, err := b.coordinatorRequestV1(request)
	if err != nil {
		return public.SearchResponseV1{}, public.FastSearchEvidenceV1{}, err
	}
	adapterNanos := time.Since(started)
	response, evidence, err := b.opts.Topology.searchFastV1(ctx, r, options.MaxIndexAge, options.MinIndexedThrough)
	result, err := b.publicSearchResponseV1(request, response, err, started, adapterNanos)
	if err != nil {
		return public.SearchResponseV1{}, public.FastSearchEvidenceV1{}, err
	}
	return result, publicFastSearchEvidenceV1(request.Generation, evidence), nil
}

type vectorPartitionPublicPinnedSearchV1 struct {
	backend *VectorPartitionPublicBackendV1
	pinned  *vectorPartitionPinnedTopologySearchV1
}

func (b *VectorPartitionPublicBackendV1) PinVectorPartitionSearchSnapshotV1(ctx context.Context, options public.PinSearchSnapshotOptionsV1) (public.SearchSnapshotBackendV1, public.FastSearchEvidenceV1, error) {
	if b == nil || b.opts.Topology == nil || b.opts.Topology.Status().Closed {
		return nil, public.FastSearchEvidenceV1{}, errors.New("production topology is unavailable")
	}
	pinned, evidence, err := b.opts.Topology.pinSearchSnapshotV1(ctx, options.MaxIndexAge, options.MinIndexedThrough, options.MaxSessionAge)
	if err != nil {
		return nil, public.FastSearchEvidenceV1{}, publicBackendErrorV1(err)
	}
	generation := public.GenerationIDV1{Index: b.opts.Identity.Index.IndexName, Generation: b.opts.Identity.Generation}
	return &vectorPartitionPublicPinnedSearchV1{backend: b, pinned: pinned}, publicFastSearchEvidenceV1(generation, evidence), nil
}

func (p *vectorPartitionPublicPinnedSearchV1) SearchVectorPartitionV1(ctx context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
	if p == nil || p.backend == nil || p.pinned == nil {
		return public.SearchResponseV1{}, errors.New("pinned vector search is unavailable")
	}
	started := time.Now()
	r, err := p.backend.coordinatorRequestV1(request)
	if err != nil {
		return public.SearchResponseV1{}, err
	}
	adapterNanos := time.Since(started)
	response, err := p.pinned.searchV1(ctx, r)
	return p.backend.publicSearchResponseV1(request, response, err, started, adapterNanos)
}

func (p *vectorPartitionPublicPinnedSearchV1) Close() error {
	if p == nil || p.pinned == nil {
		return nil
	}
	return p.pinned.Close()
}

func (b *VectorPartitionPublicBackendV1) coordinatorRequestV1(request public.SearchRequestV1) (VectorPartitionCoordinatorRequestV1, error) {
	if b == nil || b.opts.Topology == nil || b.opts.Topology.Status().Closed {
		return VectorPartitionCoordinatorRequestV1{}, errors.New("production topology is unavailable")
	}
	if err := b.checkID(request.Generation); err != nil {
		return VectorPartitionCoordinatorRequestV1{}, err
	}
	r := b.opts.RequestBase
	sequence := b.sequence.Add(1)
	r.RequestID = fmt.Sprintf("%s/%016x", r.RequestID, sequence)
	r.CancellationID = fmt.Sprintf("%s/%016x", r.CancellationID, sequence)
	r.Version, r.Query, r.IndexName, r.Metric, r.TopK, r.PartitionProbes, r.EfSearch, r.Consistency = request.Version, request.Query, request.Generation.Index, VectorPartitionShardSearchMetricV1(request.Metric), request.TopK, request.Probes, request.EfSearch, VectorPartitionShardSearchConsistencyV1(request.Consistency)
	r.RequestBytesLimit, r.CandidateBytesLimit, r.ResponseBytesLimit, r.MergeEntriesLimit = request.Limits.RequestBytes, request.Limits.CandidateBytes, request.Limits.ResponseBytes, request.Limits.MergeEntries
	r.DeadlineUnixNano = 0
	if !request.Deadline.IsZero() {
		r.DeadlineUnixNano = request.Deadline.UnixNano()
	}
	return r, nil
}

func (b *VectorPartitionPublicBackendV1) publicSearchResponseV1(request public.SearchRequestV1, response VectorPartitionCoordinatorResponseV1, searchErr error, started time.Time, adapterNanos time.Duration) (public.SearchResponseV1, error) {
	if searchErr != nil {
		return public.SearchResponseV1{}, publicBackendErrorV1(searchErr)
	}
	if response.PartitionGeneration != request.Generation.Generation {
		return public.SearchResponseV1{}, &public.ErrorV1{Code: public.ErrorGenerationMismatchV1, Err: errors.New("serving topology returned another generation")}
	}
	adapterStarted := time.Now()
	result := public.SearchResponseV1{Generation: request.Generation, Counters: public.SearchCountersV1{
		SelectedPartitions: response.Counters.SelectedPartitions, SelectedGroups: response.Counters.SelectedGroups,
		HNSWServedPartitions: response.Counters.HNSWServedPartitions, ExactScanPartitions: response.Counters.ExactScanPartitions,
		Requests: response.Counters.Requests, RPCs: response.Counters.RPCs, Retries: response.Counters.Retries, Redirects: response.Counters.Redirects,
		Candidates: response.Counters.Candidates, Edges: response.Counters.Edges,
		SnapshotPins: response.Counters.SnapshotPins, ReadProofs: response.Counters.ReadProofs, GenerationPins: response.Counters.GenerationPins, PartitionOpens: response.Counters.PartitionOpens,
		QueryBytes: response.Counters.QueryBytes, RequestBytes: response.Counters.RequestBytes, CandidateBytes: response.Counters.CandidateBytes, ResponseBytes: response.Counters.ResponseBytes,
	}, Timing: public.SearchTimingV1{
		PublicAdapter: adapterNanos, RouterOpen: time.Duration(response.Timing.RouterOpenNanos), RouterSearch: time.Duration(response.Timing.RouterSearchNanos),
		Placement: time.Duration(response.Timing.PlacementNanos), CoordinatorLifecycle: time.Duration(response.Timing.LifecycleNanos), Dispatch: time.Duration(response.Timing.DispatchNanos),
		Queue: time.Duration(response.Timing.QueueNanos), RPC: time.Duration(response.Timing.RPCNanos), Network: time.Duration(response.Timing.NetworkNanos),
		ReadIndexApply: time.Duration(response.Timing.ReadIndexApplyNanos), GenerationOpen: time.Duration(response.Timing.GenerationOpenNanos), ShardSearch: time.Duration(response.Timing.ShardSearchNanos),
		Response: time.Duration(response.Timing.ResponseNanos), Dedupe: time.Duration(response.Timing.DedupeNanos), Merge: time.Duration(response.Timing.MergeNanos), CoordinatorTotal: time.Duration(response.Timing.TotalNanos),
	}}
	result.Neighbors = make([]public.NeighborV1, len(response.Neighbors))
	for i, n := range response.Neighbors {
		result.Neighbors[i] = public.NeighborV1{ID: n.ID, Score: n.Score}
	}
	result.Timing.PublicAdapter += time.Since(adapterStarted)
	result.Timing.Total = max(time.Since(started), result.Timing.PublicAdapter+result.Timing.CoordinatorTotal)
	return result, nil
}

func publicFastSearchEvidenceV1(generation public.GenerationIDV1, evidence vectorPartitionFastSearchEvidenceV1) public.FastSearchEvidenceV1 {
	return public.FastSearchEvidenceV1{
		Generation: generation, IndexedThrough: evidence.Identity.IndexedThrough,
		PublishedAt: time.Unix(0, evidence.Identity.PublishedAtUnixNano), IndexAge: evidence.Age,
		TopologyDigest: evidence.Identity.TopologyDigest, AuthorizationOverlayDigest: evidence.Identity.AuthorizationOverlayDigest,
	}
}

func (b *VectorPartitionPublicBackendV1) RegisterVectorPartitionV1(ctx context.Context, registration public.GenerationRegistrationV1) (public.GenerationStatusV1, error) {
	if err := b.checkID(registration.GenerationIDV1); err != nil {
		return public.GenerationStatusV1{}, err
	}
	i := b.opts.Identity
	if registration.SourceGeneration == 0 || registration.SourceChecksum == 0 || registration.SourceSchemaHash == 0 || registration.SourceRowCount == 0 || registration.SourceGeneration != i.Source.Generation || registration.SourceChecksum != i.Source.Checksum || registration.SourceSchemaHash != i.Source.SchemaHash || registration.SourceRowCount != i.Source.RowCount {
		return public.GenerationStatusV1{}, &public.ErrorV1{Code: public.ErrorGenerationMismatchV1, Err: errors.New("generation source does not match bound topology")}
	}
	previousGeneration := uint64(0)
	if existing, ok := b.opts.Lifecycle.Authority.VectorPartitionLifecycleRecordV1(i); ok {
		previousGeneration = existing.PreviousActiveGeneration
	} else {
		for _, status := range b.opts.Lifecycle.Authority.VectorPartitionLifecycleStatusesV1() {
			if status.Active && status.Identity.Index == i.Index {
				previousGeneration = status.Identity.Generation
				break
			}
		}
	}
	record, err := b.opts.Lifecycle.BeginBuildV1(ctx, i, b.opts.RequiredGroups, previousGeneration, b.opts.MutationEpoch)
	if err != nil {
		return public.GenerationStatusV1{}, err
	}
	for _, group := range b.opts.RequiredGroups {
		alreadyReady := false
		for _, ready := range record.ReadyGroups {
			if ready.GroupID == group {
				alreadyReady = true
				break
			}
		}
		if alreadyReady {
			continue
		}
		record, err = b.opts.Lifecycle.BuildAndRecordGroupReadyV1(ctx, b.opts.Builder, i, group)
		if err != nil {
			return public.GenerationStatusV1{}, err
		}
	}
	return publicStatusV1(record), nil
}
func (b *VectorPartitionPublicBackendV1) GenerationStatusV1(_ context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.checkID(id); err != nil {
		return public.GenerationStatusV1{}, err
	}
	r, ok := b.opts.Lifecycle.Authority.VectorPartitionLifecycleRecordV1(b.opts.Identity)
	if !ok {
		return public.GenerationStatusV1{Generation: id, State: public.GenerationAbsentV1}, nil
	}
	return publicStatusV1(r), nil
}
func (b *VectorPartitionPublicBackendV1) PrepareVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.checkID(id); err != nil {
		return public.GenerationStatusV1{}, err
	}
	r, err := b.opts.Lifecycle.PrepareV1(ctx, b.opts.Identity)
	return publicStatusV1(r), err
}
func (b *VectorPartitionPublicBackendV1) ActivateVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.checkID(id); err != nil {
		return public.GenerationStatusV1{}, err
	}
	r, err := b.opts.Lifecycle.ActivateV1(ctx, b.opts.Identity)
	if err == nil {
		err = b.opts.Topology.PublishServingSnapshotV1(ctx)
	}
	return publicStatusV1(r), err
}
func (b *VectorPartitionPublicBackendV1) InvalidateVectorPartitionV1(ctx context.Context, id public.GenerationIDV1, reason string) (public.GenerationStatusV1, error) {
	if err := b.checkID(id); err != nil {
		return public.GenerationStatusV1{}, err
	}
	if err := b.opts.Topology.InvalidateServingSnapshotV1(); err != nil {
		return public.GenerationStatusV1{}, err
	}
	_, err := b.opts.Lifecycle.InvalidateGenerationBeforeRelevantMutationV1(ctx, b.opts.Identity, reason)
	if err != nil {
		if errors.Is(err, raftplacement.ErrVectorPartitionLifecycleIdentity) {
			return public.GenerationStatusV1{}, &public.ErrorV1{Code: public.ErrorGenerationMismatchV1, Err: err}
		}
		return public.GenerationStatusV1{}, err
	}
	return b.GenerationStatusV1(ctx, id)
}
func (b *VectorPartitionPublicBackendV1) RetireVectorPartitionV1(ctx context.Context, id public.GenerationIDV1) (public.GenerationStatusV1, error) {
	if err := b.checkID(id); err != nil {
		return public.GenerationStatusV1{}, err
	}
	if err := b.opts.Topology.InvalidateServingSnapshotV1(); err != nil {
		return public.GenerationStatusV1{}, err
	}
	r, err := b.opts.Lifecycle.RetireV1(ctx, b.opts.Identity)
	return publicStatusV1(r), err
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
	if b == nil || b.opts.Topology == nil || b.opts.Lifecycle.Authority == nil || b.opts.ReadFence == nil {
		return public.OperationsHealthV1{Reason: "authority_unavailable"}, errors.New("production vector topology or lifecycle authority is unavailable")
	}
	id := public.GenerationIDV1{Index: b.opts.Identity.Index.IndexName, Generation: b.opts.Identity.Generation}
	topology := b.opts.Topology.Status()
	if topology.Closed || !topology.Ready {
		return public.OperationsHealthV1{Generation: id, Reason: "topology_unavailable"}, nil
	}
	requiredGroups := slices.Clone(b.opts.RequiredGroups)
	slices.Sort(requiredGroups)
	ownerGroups := make([]raftcluster.GroupID, 0, len(topology.Endpoints))
	for group := range topology.Endpoints {
		ownerGroups = append(ownerGroups, group)
	}
	slices.Sort(ownerGroups)
	if !slices.Equal(ownerGroups, requiredGroups) {
		return public.OperationsHealthV1{Generation: id, Reason: "topology_unavailable"}, nil
	}
	requiredAppliedIndex, err := b.opts.ReadFence.LinearizableCatalogMetaAppliedIndexV1(
		raftcluster.WithCatalogMetaReadSourceV1(ctx, raftcluster.CatalogMetaReadSourceOperationsHealthV1),
	)
	if err != nil {
		return public.OperationsHealthV1{Generation: id, Reason: "catalog_unavailable"}, err
	}
	catalogStatus, ok := b.opts.Lifecycle.Authority.Status()
	if !ok || requiredAppliedIndex == 0 || catalogStatus.AppliedIndex < requiredAppliedIndex {
		return public.OperationsHealthV1{Generation: id, Reason: "catalog_unavailable"}, raftplacement.ErrCatalogMetaUnavailable
	}
	proof := raftplacement.CatalogProofV1{Epoch: catalogStatus.Epoch, Digest: catalogStatus.Digest}
	if proof.Epoch != b.opts.Identity.Index.CatalogEpoch || proof.Digest != b.opts.Identity.Index.CatalogDigest {
		return public.OperationsHealthV1{Generation: id, Reason: "catalog_mismatch"}, nil
	}
	record, ok := b.opts.Lifecycle.Authority.VectorPartitionLifecycleRecordV1(b.opts.Identity)
	if !ok {
		for _, status := range b.opts.Lifecycle.Authority.VectorPartitionLifecycleStatusesV1() {
			if status.Identity.Index == b.opts.Identity.Index && status.Identity.Generation == b.opts.Identity.Generation {
				return public.OperationsHealthV1{Generation: id, State: public.GenerationStateV1(status.State), Reason: "source_mismatch"}, nil
			}
		}
		return public.OperationsHealthV1{Generation: id, State: public.GenerationAbsentV1, Reason: "generation_absent"}, nil
	}
	status := publicStatusV1(record)
	if !status.Active || !status.Ready {
		return public.OperationsHealthV1{Generation: id, State: status.State, Reason: "lifecycle_not_active"}, nil
	}
	readySetDigest, err := raftplacement.VectorPartitionLifecycleReadySetDigestV1(b.opts.Identity, b.opts.RequiredGroups, record.ReadyGroups)
	if err != nil || readySetDigest != record.ReadySetDigest {
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
func publicStatusV1(r raftplacement.VectorPartitionLifecycleRecordV1) public.GenerationStatusV1 {
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
		case VectorPartitionCoordinatorErrorInvalidRequestV1, VectorPartitionCoordinatorErrorRouteMismatchV1, VectorPartitionCoordinatorErrorBudgetExceededV1:
			return &public.ErrorV1{Code: public.ErrorInvalidRequestV1, Err: err}
		case VectorPartitionCoordinatorErrorMalformedResponseV1:
			return &public.ErrorV1{Code: public.ErrorFailedV1, Err: err}
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
