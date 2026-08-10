package vectorpartition

import (
	"context"
	"errors"
	"sync"
	"time"
)

// OperationsConfigV1 is explicit and default-off. Every bound is checked
// before ServiceV1 can clone a query or dispatch it to the production backend.
type OperationsConfigV1 struct {
	Enabled                                           bool
	MaxQueryBytes, MaxRequestBytes, MaxCandidateBytes uint64
	MaxResponseBytes                                  uint64
	MaxTopK, MaxProbes, MaxEfSearch, MaxMergeEntries  int
}

func ConservativeOperationsConfigV1() OperationsConfigV1 {
	return OperationsConfigV1{MaxQueryBytes: 1 << 20, MaxRequestBytes: 4 << 20, MaxCandidateBytes: 64 << 20, MaxResponseBytes: 16 << 20, MaxTopK: 10_000, MaxProbes: 256, MaxEfSearch: 10_000, MaxMergeEntries: 2_560_000}
}

// OperationsHealthV1 is a live authority-derived health snapshot. It carries
// no catalog records, transport handles, or group identifiers.
type OperationsHealthV1 struct {
	Ready      bool
	State      GenerationStateV1
	Generation GenerationIDV1
	Reason     string
}

type OperationsCountersV1 struct {
	Disabled, ReadyChecks, Searches, CapQueryBytes, CapRequestBytes uint64
	CapCandidateBytes, CapResponseBytes, CapTopK, CapProbes         uint64
	CapEfSearch, CapMergeEntries                                    uint64
	Failures, Requests, RPCs, Retries, Redirects, Candidates, Edges uint64
	SnapshotPins, ReadProofs, GenerationPins, PartitionOpens        uint64
	SelectedPartitions, SelectedGroups                              uint64
	QueryBytes, RequestBytes, CandidateBytes, ResponseBytes         uint64
}

// OperationsV1 is the explicit operator boundary over an already assembled
// ServiceV1. health is node-owned and must read live authority state.
type OperationsV1 struct {
	service *ServiceV1
	config  OperationsConfigV1
	health  func(context.Context) (OperationsHealthV1, error)
	mu      sync.Mutex
	counts  OperationsCountersV1
}

func NewOperationsV1(service *ServiceV1, config OperationsConfigV1, health func(context.Context) (OperationsHealthV1, error)) (*OperationsV1, error) {
	if !config.Enabled {
		return &OperationsV1{config: config}, nil
	}
	if service == nil || health == nil || !validOperationsConfigV1(config) {
		return nil, errors.New("vectorpartition: enabled operations require service, live health, and bounded limits")
	}
	return &OperationsV1{service: service, config: config, health: health}, nil
}

func (o *OperationsV1) Enabled() bool {
	return o != nil && o.config.Enabled && o.service != nil && o.health != nil
}

func validOperationsConfigV1(c OperationsConfigV1) bool {
	return c.MaxQueryBytes != 0 && c.MaxRequestBytes != 0 && c.MaxCandidateBytes != 0 && c.MaxResponseBytes != 0 && c.MaxTopK > 0 && c.MaxProbes > 0 && c.MaxEfSearch > 0 && c.MaxMergeEntries > 0
}

func (o *OperationsV1) Status(ctx context.Context) (OperationsHealthV1, error) {
	if o == nil || !o.config.Enabled {
		return OperationsHealthV1{Reason: "disabled"}, nil
	}
	if err := ctx.Err(); err != nil {
		return OperationsHealthV1{}, classifyErrorV1(ctx, err)
	}
	o.mu.Lock()
	o.counts.ReadyChecks++
	o.mu.Unlock()
	health, err := o.health(ctx)
	if err != nil {
		if health.Reason == "" {
			health.Reason = "authority_unavailable"
		}
		return health, classifyErrorV1(ctx, err)
	}
	if !health.Ready && health.Reason == "" {
		health.Reason = "unavailable"
	}
	return health, nil
}

func (o *OperationsV1) Search(ctx context.Context, request SearchRequestV1) (SearchResponseV1, error) {
	started := time.Now()
	admissionStarted := time.Now()
	if err := o.admit(ctx, request); err != nil {
		return SearchResponseV1{}, err
	}
	admissionElapsed := time.Since(admissionStarted)
	serviceStarted := time.Now()
	response, err := o.service.Search(ctx, request)
	serviceElapsed := time.Since(serviceStarted)
	if err == nil {
		response.Timing.Admission = admissionElapsed
		nested := response.Timing.PublicAdapter + response.Timing.CoordinatorTotal
		if serviceElapsed >= nested {
			response.Timing.ServiceAdapter = serviceElapsed - nested
		}
		response.Timing.Total = time.Since(started)
	}
	o.mu.Lock()
	o.counts.Searches++
	if err != nil {
		o.counts.Failures++
	} else {
		o.counts.Requests += response.Counters.Requests
		o.counts.RPCs += response.Counters.RPCs
		o.counts.Retries += response.Counters.Retries
		o.counts.Redirects += response.Counters.Redirects
		o.counts.Candidates += response.Counters.Candidates
		o.counts.Edges += response.Counters.Edges
		o.counts.SnapshotPins += response.Counters.SnapshotPins
		o.counts.ReadProofs += response.Counters.ReadProofs
		o.counts.GenerationPins += response.Counters.GenerationPins
		o.counts.PartitionOpens += response.Counters.PartitionOpens
		o.counts.SelectedPartitions += response.Counters.SelectedPartitions
		o.counts.SelectedGroups += response.Counters.SelectedGroups
		o.counts.QueryBytes += response.Counters.QueryBytes
		o.counts.RequestBytes += response.Counters.RequestBytes
		o.counts.CandidateBytes += response.Counters.CandidateBytes
		o.counts.ResponseBytes += response.Counters.ResponseBytes
	}
	o.mu.Unlock()
	return response, err
}

func (o *OperationsV1) Inventory(ctx context.Context, id GenerationIDV1) ([]GenerationStatusV1, error) {
	if err := o.enabled(); err != nil {
		return nil, err
	}
	status, err := o.service.Status(ctx, id)
	if err != nil {
		return nil, err
	}
	return []GenerationStatusV1{status}, nil
}
func (o *OperationsV1) Register(ctx context.Context, registration GenerationRegistrationV1) (GenerationStatusV1, error) {
	if err := o.enabled(); err != nil {
		return GenerationStatusV1{}, err
	}
	return o.service.Register(ctx, registration)
}
func (o *OperationsV1) Prepare(ctx context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	if err := o.enabled(); err != nil {
		return GenerationStatusV1{}, err
	}
	return o.service.Prepare(ctx, id)
}
func (o *OperationsV1) Activate(ctx context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	if err := o.enabled(); err != nil {
		return GenerationStatusV1{}, err
	}
	return o.service.Activate(ctx, id)
}
func (o *OperationsV1) Invalidate(ctx context.Context, id GenerationIDV1, reason string) (GenerationStatusV1, error) {
	if err := o.enabled(); err != nil {
		return GenerationStatusV1{}, err
	}
	return o.service.Invalidate(ctx, id, reason)
}
func (o *OperationsV1) RequestRebuild(ctx context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	if err := o.enabled(); err != nil {
		return GenerationStatusV1{}, err
	}
	return o.service.RequestRebuild(ctx, id)
}
func (o *OperationsV1) Retire(ctx context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	if err := o.enabled(); err != nil {
		return GenerationStatusV1{}, err
	}
	return o.service.Retire(ctx, id)
}
func (o *OperationsV1) CleanupEligibility(ctx context.Context, id GenerationIDV1) (CleanupEligibilityV1, error) {
	if err := o.enabled(); err != nil {
		return CleanupEligibilityV1{}, err
	}
	return o.service.CleanupEligibility(ctx, id)
}
func (o *OperationsV1) Counters() OperationsCountersV1 {
	if o == nil {
		return OperationsCountersV1{}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.counts
}

func (o *OperationsV1) enabled() error {
	if o == nil || !o.config.Enabled || o.service == nil {
		if o != nil {
			o.mu.Lock()
			o.counts.Disabled++
			o.mu.Unlock()
		}
		return &ErrorV1{Code: ErrorUnavailableV1, Err: errors.New("vector partition operations are disabled")}
	}
	return nil
}
func (o *OperationsV1) admit(ctx context.Context, r SearchRequestV1) error {
	if err := o.enabled(); err != nil {
		return err
	}
	if err := validateSearchRequestV1(ctx, r); err != nil {
		return err
	}
	var reason *uint64
	switch {
	case uint64(len(r.Query)) > o.config.MaxQueryBytes/4:
		reason = &o.counts.CapQueryBytes
	case r.Limits.RequestBytes > o.config.MaxRequestBytes:
		reason = &o.counts.CapRequestBytes
	case r.Limits.CandidateBytes > o.config.MaxCandidateBytes:
		reason = &o.counts.CapCandidateBytes
	case r.Limits.ResponseBytes > o.config.MaxResponseBytes:
		reason = &o.counts.CapResponseBytes
	case r.TopK > o.config.MaxTopK:
		reason = &o.counts.CapTopK
	case r.Probes > o.config.MaxProbes:
		reason = &o.counts.CapProbes
	case r.EfSearch > o.config.MaxEfSearch:
		reason = &o.counts.CapEfSearch
	case r.Limits.MergeEntries > o.config.MaxMergeEntries:
		reason = &o.counts.CapMergeEntries
	}
	if reason == nil {
		return nil
	}
	o.mu.Lock()
	*reason++
	o.mu.Unlock()
	return &ErrorV1{Code: ErrorInvalidRequestV1, Err: errors.New("vector partition request exceeds configured operation limit")}
}
