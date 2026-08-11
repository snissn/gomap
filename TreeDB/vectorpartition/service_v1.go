package vectorpartition

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

// ErrorCodeV1 is the stable error classification returned by ServiceV1.
// Backend details, including transport and Raft state, are deliberately not
// part of this contract.
type ErrorCodeV1 string

const (
	ErrorInvalidRequestV1     ErrorCodeV1 = "invalid_request"
	ErrorGenerationMismatchV1 ErrorCodeV1 = "generation_mismatch"
	ErrorUnavailableV1        ErrorCodeV1 = "unavailable"
	ErrorCanceledV1           ErrorCodeV1 = "canceled"
	ErrorDeadlineExceededV1   ErrorCodeV1 = "deadline_exceeded"
	ErrorFailedV1             ErrorCodeV1 = "failed"
)

type ErrorV1 struct {
	Code ErrorCodeV1
	Err  error
}

func (e *ErrorV1) Error() string {
	if e.Err == nil {
		return "vectorpartition: " + string(e.Code)
	}
	return "vectorpartition: " + string(e.Code) + ": " + e.Err.Error()
}
func (e *ErrorV1) Unwrap() error { return e.Err }

type GenerationIDV1 struct {
	Index      string
	Generation uint64
}

type SearchRequestV1 struct {
	Version     uint32
	Generation  GenerationIDV1
	Query       []float32
	Metric      MetricV1
	TopK        int
	Probes      int
	EfSearch    int
	Consistency ConsistencyV1
	Limits      SearchLimitsV1
	Deadline    time.Time
}

type MetricV1 string

const MetricCosineV1 MetricV1 = "cosine"

type ConsistencyV1 string

const ConsistencyGenerationSnapshotV1 ConsistencyV1 = "linearizable_generation_snapshot"

type SearchLimitsV1 struct {
	RequestBytes, CandidateBytes, ResponseBytes uint64
	MergeEntries                                int
}

type NeighborV1 struct {
	ID    string
	Score float32
}

type SearchCountersV1 struct {
	SelectedPartitions, SelectedGroups                       uint64
	Requests, RPCs, Retries, Redirects, Candidates, Edges    uint64
	SnapshotPins, ReadProofs, GenerationPins, PartitionOpens uint64
	QueryBytes, RequestBytes, CandidateBytes, ResponseBytes  uint64
}

// SearchTimingV1 retains exclusive public/coordinator stages plus per-shard
// sums for system qualification. Per-shard fields may exceed Total when shards
// run concurrently; CoordinatorTotal is nested inside Total.
type SearchTimingV1 struct {
	Admission, OperationsHealth, ServiceAdapter, PublicAdapter time.Duration
	RouterOpen, RouterSearch, Placement                        time.Duration
	CoordinatorLifecycle, Dispatch                             time.Duration
	Queue, RPC, Network                                        time.Duration
	ReadIndexApply, GenerationOpen                             time.Duration
	ShardSearch, Response                                      time.Duration
	Dedupe, Merge                                              time.Duration
	CoordinatorTotal, Total                                    time.Duration
}

type SearchResponseV1 struct {
	Generation GenerationIDV1
	Neighbors  []NeighborV1
	Counters   SearchCountersV1
	Timing     SearchTimingV1
}

// GenerationRegistrationV1 identifies an immutable derived generation without
// exposing catalog records, group IDs, or lifecycle encodings.
type GenerationRegistrationV1 struct {
	GenerationIDV1
	SourceGeneration uint64
	SourceChecksum   uint64
	SourceSchemaHash uint64
	SourceRowCount   uint64
}

type GenerationStateV1 string

const (
	GenerationAbsentV1    GenerationStateV1 = "absent"
	GenerationBuildingV1  GenerationStateV1 = "building"
	GenerationStagedV1    GenerationStateV1 = "staged"
	GenerationPreparedV1  GenerationStateV1 = "prepared"
	GenerationActiveV1    GenerationStateV1 = "active"
	GenerationInvalidV1   GenerationStateV1 = "invalidated"
	GenerationRetiredV1   GenerationStateV1 = "retired"
	GenerationCleanableV1 GenerationStateV1 = "cleanable"
)

type GenerationStatusV1 struct {
	Generation GenerationIDV1
	State      GenerationStateV1
	Active     bool
	Ready      bool
	Revision   uint64
}

type CleanupEligibilityV1 struct {
	Eligible bool
	Status   GenerationStatusV1
}

// BackendV1 is the one construction-time seam for a node's already assembled
// production topology and lifecycle authority. Request methods carry only
// stable public values; nativewire, Raft, listeners, and records stay behind it.
type BackendV1 interface {
	SearchVectorPartitionV1(context.Context, SearchRequestV1) (SearchResponseV1, error)
	SearchVectorPartitionFastV1(context.Context, SearchRequestV1, FastSearchOptionsV1) (SearchResponseV1, FastSearchEvidenceV1, error)
	PinVectorPartitionSearchSnapshotV1(context.Context, PinSearchSnapshotOptionsV1) (SearchSnapshotBackendV1, FastSearchEvidenceV1, error)
	RegisterVectorPartitionV1(context.Context, GenerationRegistrationV1) (GenerationStatusV1, error)
	GenerationStatusV1(context.Context, GenerationIDV1) (GenerationStatusV1, error)
	PrepareVectorPartitionV1(context.Context, GenerationIDV1) (GenerationStatusV1, error)
	ActivateVectorPartitionV1(context.Context, GenerationIDV1) (GenerationStatusV1, error)
	InvalidateVectorPartitionV1(context.Context, GenerationIDV1, string) (GenerationStatusV1, error)
	RetireVectorPartitionV1(context.Context, GenerationIDV1) (GenerationStatusV1, error)
	RequestVectorPartitionRebuildV1(context.Context, GenerationIDV1) (GenerationStatusV1, error)
	VectorPartitionCleanupEligibilityV1(context.Context, GenerationIDV1) (CleanupEligibilityV1, error)
}

type ServiceV1 struct{ backend BackendV1 }

func NewServiceV1(backend BackendV1) (*ServiceV1, error) {
	if backend == nil {
		return nil, errors.New("vectorpartition: backend is required")
	}
	return &ServiceV1{backend: backend}, nil
}

func (s *ServiceV1) Search(ctx context.Context, request SearchRequestV1) (SearchResponseV1, error) {
	if err := validateSearchRequestV1(ctx, request); err != nil {
		return SearchResponseV1{}, err
	}
	response, err := s.backend.SearchVectorPartitionV1(ctx, cloneSearchRequestV1(request))
	if err != nil {
		return SearchResponseV1{}, classifyErrorV1(ctx, err)
	}
	if err := validateSearchResponseV1(request, response); err != nil {
		return SearchResponseV1{}, err
	}
	response.Neighbors = slices.Clone(response.Neighbors)
	return response, nil
}

func validateSearchResponseV1(request SearchRequestV1, response SearchResponseV1) error {
	if response.Generation != request.Generation || len(response.Neighbors) > request.TopK {
		return &ErrorV1{Code: ErrorFailedV1, Err: errors.New("backend returned invalid search response")}
	}
	return nil
}

func (s *ServiceV1) Register(ctx context.Context, registration GenerationRegistrationV1) (GenerationStatusV1, error) {
	if err := validateGenerationV1(ctx, registration.GenerationIDV1); err != nil {
		return GenerationStatusV1{}, err
	}
	if registration.SourceGeneration == 0 || registration.SourceChecksum == 0 || registration.SourceSchemaHash == 0 || registration.SourceRowCount == 0 {
		return GenerationStatusV1{}, invalidV1("complete source identity is required")
	}
	status, err := s.backend.RegisterVectorPartitionV1(ctx, registration)
	return statusResultV1(ctx, registration.GenerationIDV1, status, err)
}
func (s *ServiceV1) Status(ctx context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	if err := validateGenerationV1(ctx, id); err != nil {
		return GenerationStatusV1{}, err
	}
	status, err := s.backend.GenerationStatusV1(ctx, id)
	return statusResultV1(ctx, id, status, err)
}
func (s *ServiceV1) Prepare(ctx context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	if err := validateGenerationV1(ctx, id); err != nil {
		return GenerationStatusV1{}, err
	}
	status, err := s.backend.PrepareVectorPartitionV1(ctx, id)
	return statusResultV1(ctx, id, status, err)
}
func (s *ServiceV1) Activate(ctx context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	if err := validateGenerationV1(ctx, id); err != nil {
		return GenerationStatusV1{}, err
	}
	status, err := s.backend.ActivateVectorPartitionV1(ctx, id)
	return statusResultV1(ctx, id, status, err)
}
func (s *ServiceV1) Invalidate(ctx context.Context, id GenerationIDV1, reason string) (GenerationStatusV1, error) {
	if err := validateGenerationV1(ctx, id); err != nil {
		return GenerationStatusV1{}, err
	}
	if reason == "" {
		return GenerationStatusV1{}, invalidV1("invalidation reason is required")
	}
	status, err := s.backend.InvalidateVectorPartitionV1(ctx, id, reason)
	return statusResultV1(ctx, id, status, err)
}
func (s *ServiceV1) Retire(ctx context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	if err := validateGenerationV1(ctx, id); err != nil {
		return GenerationStatusV1{}, err
	}
	status, err := s.backend.RetireVectorPartitionV1(ctx, id)
	return statusResultV1(ctx, id, status, err)
}
func (s *ServiceV1) RequestRebuild(ctx context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	if err := validateGenerationV1(ctx, id); err != nil {
		return GenerationStatusV1{}, err
	}
	status, err := s.backend.RequestVectorPartitionRebuildV1(ctx, id)
	return statusResultV1(ctx, id, status, err)
}
func (s *ServiceV1) CleanupEligibility(ctx context.Context, id GenerationIDV1) (CleanupEligibilityV1, error) {
	if err := validateGenerationV1(ctx, id); err != nil {
		return CleanupEligibilityV1{}, err
	}
	eligibility, err := s.backend.VectorPartitionCleanupEligibilityV1(ctx, id)
	if err != nil {
		return CleanupEligibilityV1{}, classifyErrorV1(ctx, err)
	}
	if err := validateStatusIdentityV1(id, eligibility.Status); err != nil {
		return CleanupEligibilityV1{}, err
	}
	return eligibility, nil
}

func statusResultV1(ctx context.Context, id GenerationIDV1, status GenerationStatusV1, err error) (GenerationStatusV1, error) {
	if err != nil {
		return GenerationStatusV1{}, classifyErrorV1(ctx, err)
	}
	if err := validateStatusIdentityV1(id, status); err != nil {
		return GenerationStatusV1{}, err
	}
	return status, nil
}
func validateStatusIdentityV1(id GenerationIDV1, status GenerationStatusV1) error {
	if status.Generation != id {
		return &ErrorV1{Code: ErrorFailedV1, Err: errors.New("backend returned status for another generation")}
	}
	return nil
}

func validateGenerationV1(ctx context.Context, id GenerationIDV1) error {
	if err := ctx.Err(); err != nil {
		return classifyErrorV1(ctx, err)
	}
	if id.Index == "" || id.Generation == 0 {
		return invalidV1("index and generation are required")
	}
	return nil
}
func validateSearchRequestV1(ctx context.Context, r SearchRequestV1) error {
	if err := validateGenerationV1(ctx, r.Generation); err != nil {
		return err
	}
	if r.Version != 1 || len(r.Query) == 0 || r.TopK <= 0 || r.Probes <= 0 || r.EfSearch < r.TopK || r.Metric != MetricCosineV1 || r.Consistency != ConsistencyGenerationSnapshotV1 || r.Limits.RequestBytes == 0 || r.Limits.CandidateBytes == 0 || r.Limits.ResponseBytes == 0 || r.Limits.MergeEntries <= 0 {
		return invalidV1("version, query, metric, consistency, limits, top_k, probes, and ef_search are required")
	}
	if uint64(len(r.Query)) > r.Limits.RequestBytes/4 {
		return invalidV1("query exceeds request byte limit")
	}
	if !r.Deadline.IsZero() && !time.Now().Before(r.Deadline) {
		return &ErrorV1{Code: ErrorDeadlineExceededV1, Err: context.DeadlineExceeded}
	}
	return nil
}
func cloneSearchRequestV1(r SearchRequestV1) SearchRequestV1 {
	r.Query = slices.Clone(r.Query)
	return r
}
func invalidV1(message string) error {
	return &ErrorV1{Code: ErrorInvalidRequestV1, Err: errors.New(message)}
}
func classifyErrorV1(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return &ErrorV1{Code: ErrorCanceledV1, Err: err}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &ErrorV1{Code: ErrorDeadlineExceededV1, Err: err}
	}
	var existing *ErrorV1
	if errors.As(err, &existing) {
		return err
	}
	return &ErrorV1{Code: ErrorUnavailableV1, Err: fmt.Errorf("%w", err)}
}
