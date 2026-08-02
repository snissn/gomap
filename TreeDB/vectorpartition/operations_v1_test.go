package vectorpartition

import (
	"context"
	"errors"
	"testing"
)

func operationsRequestV1() SearchRequestV1 {
	return SearchRequestV1{Version: 1, Generation: GenerationIDV1{Index: "embedding", Generation: 1}, Query: []float32{1}, Metric: MetricCosineV1, TopK: 1, Probes: 1, EfSearch: 1, Consistency: ConsistencyGenerationSnapshotV1, Limits: SearchLimitsV1{RequestBytes: 4, CandidateBytes: 1, ResponseBytes: 1, MergeEntries: 1}}
}

func TestOperationsV1DefaultsOffAndCapsBeforeServiceV1(t *testing.T) {
	calls := 0
	backend := &serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{}, search: func(_ context.Context, r SearchRequestV1) (SearchResponseV1, error) {
		calls++
		return SearchResponseV1{Generation: r.Generation}, nil
	}}
	service, err := NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	off, err := NewOperationsV1(service, OperationsConfigV1{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if health, err := off.Status(t.Context()); err != nil || health.Reason != "disabled" {
		t.Fatalf("off status=%+v err=%v", health, err)
	}
	if _, err := off.Search(t.Context(), operationsRequestV1()); !hasOperationErrorCodeV1(err, ErrorUnavailableV1) {
		t.Fatalf("off search err=%v", err)
	}
	config := ConservativeOperationsConfigV1()
	config.Enabled = true
	config.MaxTopK = 1
	ops, err := NewOperationsV1(service, config, func(context.Context) (OperationsHealthV1, error) {
		return OperationsHealthV1{Ready: true, State: GenerationActiveV1, Generation: GenerationIDV1{Index: "embedding", Generation: 1}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	request := operationsRequestV1()
	request.TopK = 2
	if _, err := ops.Search(t.Context(), request); !hasOperationErrorCodeV1(err, ErrorInvalidRequestV1) {
		t.Fatalf("cap search err=%v", err)
	}
	if calls != 0 {
		t.Fatalf("cap reached backend %d times", calls)
	}
	if got := ops.Counters().CapTopK; got != 1 {
		t.Fatalf("cap topk=%d", got)
	}
}

func hasOperationErrorCodeV1(err error, want ErrorCodeV1) bool {
	var apiErr *ErrorV1
	return errors.As(err, &apiErr) && apiErr.Code == want
}

func TestOperationsV1UsesLiveHealthAndDelegatesLifecycleV1(t *testing.T) {
	id := GenerationIDV1{Index: "embedding", Generation: 1}
	backend := &serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{id: {Generation: id, State: GenerationActiveV1, Active: true, Ready: true}}}
	service, err := NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	config := ConservativeOperationsConfigV1()
	config.Enabled = true
	available := true
	ops, err := NewOperationsV1(service, config, func(context.Context) (OperationsHealthV1, error) {
		return OperationsHealthV1{Ready: available, Generation: id, State: GenerationActiveV1, Reason: "catalog_source_generation_groups"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if health, err := ops.Status(t.Context()); err != nil || !health.Ready {
		t.Fatalf("health=%+v err=%v", health, err)
	}
	available = false
	if health, err := ops.Status(t.Context()); err != nil || health.Ready {
		t.Fatalf("live health=%+v err=%v", health, err)
	}
	if inventory, err := ops.Inventory(t.Context(), id); err != nil || len(inventory) != 1 || inventory[0].Generation != id {
		t.Fatalf("inventory=%+v err=%v", inventory, err)
	}
}

func TestOperationsV1EveryConfiguredCapStopsBeforeDispatchV1(t *testing.T) {
	calls := 0
	backend := &serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{}, search: func(_ context.Context, r SearchRequestV1) (SearchResponseV1, error) {
		calls++
		return SearchResponseV1{Generation: r.Generation}, nil
	}}
	service, err := NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	config := ConservativeOperationsConfigV1()
	config.Enabled, config.MaxQueryBytes, config.MaxRequestBytes, config.MaxCandidateBytes, config.MaxResponseBytes = true, 4, 4, 1, 1
	config.MaxTopK, config.MaxProbes, config.MaxEfSearch, config.MaxMergeEntries = 1, 1, 1, 1
	ops, err := NewOperationsV1(service, config, func(context.Context) (OperationsHealthV1, error) { return OperationsHealthV1{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*SearchRequestV1){
		func(r *SearchRequestV1) { r.Query = []float32{1, 2} }, func(r *SearchRequestV1) { r.Limits.RequestBytes = 5 }, func(r *SearchRequestV1) { r.Limits.CandidateBytes = 2 }, func(r *SearchRequestV1) { r.Limits.ResponseBytes = 2 },
		func(r *SearchRequestV1) { r.TopK = 2 }, func(r *SearchRequestV1) { r.Probes = 2 }, func(r *SearchRequestV1) { r.EfSearch = 2 }, func(r *SearchRequestV1) { r.Limits.MergeEntries = 2 },
	} {
		request := operationsRequestV1()
		mutate(&request)
		if _, err := ops.Search(t.Context(), request); !hasOperationErrorCodeV1(err, ErrorInvalidRequestV1) {
			t.Fatalf("cap err=%v", err)
		}
	}
	if calls != 0 {
		t.Fatalf("cap checks dispatched %d searches", calls)
	}
}
