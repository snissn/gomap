package vectorpartition

import (
	"context"
	"errors"
	"testing"
	"time"
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
	request.EfSearch = 2
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := ops.Search(canceled, request); !hasOperationErrorCodeV1(err, ErrorCanceledV1) {
		t.Fatalf("canceled cap search err=%v", err)
	}
	request.Deadline = time.Now().Add(-time.Second)
	if _, err := ops.Search(t.Context(), request); !hasOperationErrorCodeV1(err, ErrorDeadlineExceededV1) {
		t.Fatalf("expired cap search err=%v", err)
	}
	request.Deadline = time.Time{}
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
	searches := 0
	backend := &serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{id: {Generation: id, State: GenerationActiveV1, Active: true, Ready: true}}, search: func(_ context.Context, r SearchRequestV1) (SearchResponseV1, error) {
		searches++
		return SearchResponseV1{Generation: r.Generation}, nil
	}}
	service, err := NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	config := ConservativeOperationsConfigV1()
	config.Enabled = true
	available := true
	healthChecks := 0
	ops, err := NewOperationsV1(service, config, func(context.Context) (OperationsHealthV1, error) {
		healthChecks++
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
	if _, err := ops.Search(t.Context(), operationsRequestV1()); err != nil {
		t.Fatalf("strict backend search err=%v", err)
	}
	if searches != 1 || healthChecks != 2 {
		t.Fatalf("searches=%d health_checks=%d", searches, healthChecks)
	}
	if counters := ops.Counters(); counters.Searches != 1 || counters.Failures != 0 || counters.ReadyChecks != 2 {
		t.Fatalf("strict search counters=%+v", counters)
	}
	if inventory, err := ops.Inventory(t.Context(), id); err != nil || len(inventory) != 1 || inventory[0].Generation != id {
		t.Fatalf("inventory=%+v err=%v", inventory, err)
	}
}

func TestOperationsV1StatusPreservesCancellationCodesV1(t *testing.T) {
	service, err := NewServiceV1(&serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{}})
	if err != nil {
		t.Fatal(err)
	}
	config := ConservativeOperationsConfigV1()
	config.Enabled = true
	for _, tc := range []struct {
		err  error
		code ErrorCodeV1
	}{{context.Canceled, ErrorCanceledV1}, {context.DeadlineExceeded, ErrorDeadlineExceededV1}} {
		ops, err := NewOperationsV1(service, config, func(context.Context) (OperationsHealthV1, error) {
			return OperationsHealthV1{Reason: "catalog_unavailable"}, tc.err
		})
		if err != nil {
			t.Fatal(err)
		}
		health, err := ops.Status(t.Context())
		if health.Ready || health.Reason != "catalog_unavailable" || !errors.Is(err, tc.err) || !hasOperationErrorCodeV1(err, tc.code) {
			t.Fatalf("want=%v health=%+v err=%v", tc.err, health, err)
		}
	}
	calls := 0
	ops, err := NewOperationsV1(service, config, func(context.Context) (OperationsHealthV1, error) {
		calls++
		return OperationsHealthV1{Ready: true}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, stop := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer stop()
	for _, tc := range []struct {
		ctx  context.Context
		code ErrorCodeV1
	}{{canceled, ErrorCanceledV1}, {expired, ErrorDeadlineExceededV1}} {
		if health, err := ops.Status(tc.ctx); health.Ready || !hasOperationErrorCodeV1(err, tc.code) {
			t.Fatalf("health=%+v err=%v", health, err)
		}
	}
	if calls != 0 || ops.Counters().ReadyChecks != 0 {
		t.Fatalf("health calls=%d ready checks=%d", calls, ops.Counters().ReadyChecks)
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

func TestOperationsV1LifecycleMatrixDelegatesServiceV1(t *testing.T) {
	id := GenerationIDV1{Index: "embedding", Generation: 1}
	backend := &serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{}, search: func(context.Context, SearchRequestV1) (SearchResponseV1, error) { return SearchResponseV1{}, nil }}
	service, err := NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	config := ConservativeOperationsConfigV1()
	config.Enabled = true
	ops, err := NewOperationsV1(service, config, func(context.Context) (OperationsHealthV1, error) { return OperationsHealthV1{Generation: id}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if status, err := ops.Register(t.Context(), GenerationRegistrationV1{GenerationIDV1: id, SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4}); err != nil || status.State != GenerationBuildingV1 {
		t.Fatalf("register=%+v err=%v", status, err)
	}
	if status, err := ops.Prepare(t.Context(), id); err != nil || status.State != GenerationPreparedV1 {
		t.Fatalf("prepare=%+v err=%v", status, err)
	}
	if status, err := ops.Activate(t.Context(), id); err != nil || status.State != GenerationActiveV1 {
		t.Fatalf("activate=%+v err=%v", status, err)
	}
	if status, err := ops.Invalidate(t.Context(), id, "mutation"); err != nil || status.State != GenerationInvalidV1 {
		t.Fatalf("invalidate=%+v err=%v", status, err)
	}
	if status, err := ops.RequestRebuild(t.Context(), id); err != nil || status.Generation != id {
		t.Fatalf("rebuild=%+v err=%v", status, err)
	}
	if status, err := ops.Retire(t.Context(), id); err != nil || status.State != GenerationRetiredV1 {
		t.Fatalf("retire=%+v err=%v", status, err)
	}
	if cleanup, err := ops.CleanupEligibility(t.Context(), id); err != nil || cleanup.Status.Generation != id {
		t.Fatalf("cleanup=%+v err=%v", cleanup, err)
	}
}

func TestOperationsV1SnapshotsSearchObservabilityV1(t *testing.T) {
	id := GenerationIDV1{Index: "embedding", Generation: 1}
	backend := &serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{}, search: func(_ context.Context, r SearchRequestV1) (SearchResponseV1, error) {
		return SearchResponseV1{Generation: r.Generation, Counters: SearchCountersV1{Requests: 2, RPCs: 2, Retries: 1, Redirects: 1, Candidates: 3, Edges: 4, SelectedPartitions: 5, SelectedGroups: 2, QueryBytes: 4, RequestBytes: 5, CandidateBytes: 6, ResponseBytes: 7}}, nil
	}}
	service, err := NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	config := ConservativeOperationsConfigV1()
	config.Enabled = true
	ops, err := NewOperationsV1(service, config, func(context.Context) (OperationsHealthV1, error) {
		return OperationsHealthV1{Ready: true, Generation: id}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ops.Search(t.Context(), operationsRequestV1()); err != nil {
		t.Fatal(err)
	}
	got := ops.Counters()
	if got.Searches != 1 || got.Requests != 2 || got.RPCs != 2 || got.Retries != 1 || got.Redirects != 1 || got.Candidates != 3 || got.Edges != 4 || got.SelectedPartitions != 5 || got.SelectedGroups != 2 || got.QueryBytes != 4 || got.RequestBytes != 5 || got.CandidateBytes != 6 || got.ResponseBytes != 7 {
		t.Fatalf("counters=%+v", got)
	}
}
