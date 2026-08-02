package vectorpartition

import (
	"context"
	"errors"
	"testing"
	"time"
)

type serviceBackendV1 struct {
	search func(context.Context, SearchRequestV1) (SearchResponseV1, error)
	states map[GenerationIDV1]GenerationStatusV1
}

func (b *serviceBackendV1) SearchVectorPartitionV1(ctx context.Context, r SearchRequestV1) (SearchResponseV1, error) {
	return b.search(ctx, r)
}
func (b *serviceBackendV1) RegisterVectorPartitionV1(_ context.Context, r GenerationRegistrationV1) (GenerationStatusV1, error) {
	s := GenerationStatusV1{Generation: r.GenerationIDV1, State: GenerationBuildingV1}
	b.states[r.GenerationIDV1] = s
	return s, nil
}
func (b *serviceBackendV1) GenerationStatusV1(_ context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	return b.states[id], nil
}
func (b *serviceBackendV1) PrepareVectorPartitionV1(_ context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	s := b.states[id]
	s.State, s.Ready = GenerationPreparedV1, true
	b.states[id] = s
	return s, nil
}
func (b *serviceBackendV1) ActivateVectorPartitionV1(_ context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	s := b.states[id]
	s.State, s.Active = GenerationActiveV1, true
	b.states[id] = s
	return s, nil
}
func (b *serviceBackendV1) InvalidateVectorPartitionV1(_ context.Context, id GenerationIDV1, _ string) (GenerationStatusV1, error) {
	s := b.states[id]
	s.State, s.Active = GenerationInvalidV1, false
	b.states[id] = s
	return s, nil
}
func (b *serviceBackendV1) RetireVectorPartitionV1(_ context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	s := b.states[id]
	s.State = GenerationRetiredV1
	b.states[id] = s
	return s, nil
}
func (b *serviceBackendV1) RequestVectorPartitionRebuildV1(_ context.Context, id GenerationIDV1) (GenerationStatusV1, error) {
	return b.states[id], nil
}
func (b *serviceBackendV1) VectorPartitionCleanupEligibilityV1(_ context.Context, id GenerationIDV1) (CleanupEligibilityV1, error) {
	return CleanupEligibilityV1{Status: b.states[id]}, nil
}

func TestServiceV1PublicContract(t *testing.T) {
	id := GenerationIDV1{Index: "embedding", Generation: 7}
	backend := &serviceBackendV1{states: make(map[GenerationIDV1]GenerationStatusV1)}
	backend.search = func(_ context.Context, r SearchRequestV1) (SearchResponseV1, error) {
		if got, want := r.Query[0], float32(1); got != want {
			t.Fatalf("query = %v, want %v", got, want)
		}
		return SearchResponseV1{Generation: r.Generation, Neighbors: []NeighborV1{{ID: "a", Score: .9}}}, nil
	}
	svc, err := NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Register(context.Background(), GenerationRegistrationV1{GenerationIDV1: id}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Prepare(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if status, err := svc.Activate(context.Background(), id); err != nil || !status.Active {
		t.Fatalf("activate = %#v, %v", status, err)
	}
	response, err := svc.Search(context.Background(), SearchRequestV1{Generation: id, Query: []float32{1}, TopK: 1, Probes: 1, EfSearch: 1})
	if err != nil || len(response.Neighbors) != 1 || response.Neighbors[0].ID != "a" {
		t.Fatalf("search = %#v, %v", response, err)
	}
	if _, err := svc.RequestRebuild(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Invalidate(context.Background(), id, "mutation"); err != nil {
		t.Fatal(err)
	}
	if status, err := svc.Status(context.Background(), id); err != nil || status.State != GenerationInvalidV1 {
		t.Fatalf("status = %#v, %v", status, err)
	}
	if _, err := svc.Retire(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if eligibility, err := svc.CleanupEligibility(context.Background(), id); err != nil || eligibility.Status.State != GenerationRetiredV1 {
		t.Fatalf("cleanup eligibility = %#v, %v", eligibility, err)
	}
}

func TestServiceV1FailsClosedWithoutPartialResults(t *testing.T) {
	id := GenerationIDV1{Index: "embedding", Generation: 7}
	svc, err := NewServiceV1(&serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{}, search: func(context.Context, SearchRequestV1) (SearchResponseV1, error) {
		return SearchResponseV1{Generation: id, Neighbors: []NeighborV1{{ID: "partial"}}}, errors.New("remote owner unavailable")
	}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := svc.Search(context.Background(), SearchRequestV1{Generation: id, Query: []float32{1}, TopK: 1, Probes: 1, EfSearch: 1})
	if response.Generation != (GenerationIDV1{}) || len(response.Neighbors) != 0 {
		t.Fatalf("partial response escaped: %#v", response)
	}
	var apiErr *ErrorV1
	if !errors.As(err, &apiErr) || apiErr.Code != ErrorUnavailableV1 {
		t.Fatalf("error = %v", err)
	}
}

func TestServiceV1ValidatesCancellationAndDeadlineBeforeBackend(t *testing.T) {
	id := GenerationIDV1{Index: "embedding", Generation: 7}
	called := false
	svc, _ := NewServiceV1(&serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{}, search: func(context.Context, SearchRequestV1) (SearchResponseV1, error) {
		called = true
		return SearchResponseV1{}, nil
	}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.Search(ctx, SearchRequestV1{Generation: id, Query: []float32{1}, TopK: 1, Probes: 1, EfSearch: 1}); !hasCodeV1(err, ErrorCanceledV1) {
		t.Fatalf("cancel = %v", err)
	}
	if _, err := svc.Search(context.Background(), SearchRequestV1{Generation: id, Query: []float32{1}, TopK: 1, Probes: 1, EfSearch: 1, Deadline: time.Now().Add(-time.Second)}); !hasCodeV1(err, ErrorDeadlineExceededV1) {
		t.Fatalf("deadline = %v", err)
	}
	if called {
		t.Fatal("backend called after rejected request")
	}
}

func hasCodeV1(err error, code ErrorCodeV1) bool {
	var apiErr *ErrorV1
	return errors.As(err, &apiErr) && apiErr.Code == code
}
