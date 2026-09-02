package vectorpartition

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type serviceBackendV1 struct {
	search   func(context.Context, SearchRequestV1) (SearchResponseV1, error)
	states   map[GenerationIDV1]GenerationStatusV1
	snapshot *serviceBackendSnapshotV1
}

func (b *serviceBackendV1) SearchVectorPartitionV1(ctx context.Context, r SearchRequestV1) (SearchResponseV1, error) {
	return b.search(ctx, r)
}
func (b *serviceBackendV1) SearchVectorPartitionFastV1(ctx context.Context, r SearchRequestV1, options FastSearchOptionsV1) (SearchResponseV1, FastSearchEvidenceV1, error) {
	response, err := b.search(ctx, r)
	return response, FastSearchEvidenceV1{Generation: r.Generation, IndexedThrough: options.MinIndexedThrough, PublishedAt: time.Now(), IndexAge: time.Nanosecond, TopologyDigest: "topology", AuthorizationOverlayDigest: "overlay"}, err
}
func (b *serviceBackendV1) PinVectorPartitionSearchSnapshotV1(_ context.Context, options PinSearchSnapshotOptionsV1) (SearchSnapshotBackendV1, FastSearchEvidenceV1, error) {
	id := GenerationIDV1{Index: "embedding", Generation: 7}
	b.snapshot = &serviceBackendSnapshotV1{backend: b}
	return b.snapshot, FastSearchEvidenceV1{Generation: id, IndexedThrough: options.MinIndexedThrough, PublishedAt: time.Now(), IndexAge: time.Nanosecond, TopologyDigest: "topology", AuthorizationOverlayDigest: "overlay"}, nil
}

type serviceBackendSnapshotV1 struct {
	backend *serviceBackendV1
	closed  bool
}

func (s *serviceBackendSnapshotV1) SearchVectorPartitionV1(ctx context.Context, request SearchRequestV1) (SearchResponseV1, error) {
	if s.closed {
		return SearchResponseV1{}, errors.New("closed")
	}
	return s.backend.search(ctx, request)
}
func (s *serviceBackendSnapshotV1) Close() error { s.closed = true; return nil }

type blockingServiceSearchSnapshotBackendV1 struct {
	started chan struct{}
	release chan struct{}
	closed  chan struct{}
}

func (b *blockingServiceSearchSnapshotBackendV1) SearchVectorPartitionV1(_ context.Context, request SearchRequestV1) (SearchResponseV1, error) {
	close(b.started)
	<-b.release
	return SearchResponseV1{Generation: request.Generation, Neighbors: []NeighborV1{{ID: "a", Score: .9}}}, nil
}

func (b *blockingServiceSearchSnapshotBackendV1) Close() error {
	close(b.closed)
	return nil
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
	if _, err := svc.Register(context.Background(), GenerationRegistrationV1{GenerationIDV1: id, SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Prepare(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if status, err := svc.Activate(context.Background(), id); err != nil || !status.Active {
		t.Fatalf("activate = %#v, %v", status, err)
	}
	response, err := svc.Search(context.Background(), validSearchRequestV1(id))
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

func TestServiceV1FastAndPinnedContract(t *testing.T) {
	id := GenerationIDV1{Index: "embedding", Generation: 7}
	backend := &serviceBackendV1{states: make(map[GenerationIDV1]GenerationStatusV1)}
	backend.search = func(_ context.Context, request SearchRequestV1) (SearchResponseV1, error) {
		return SearchResponseV1{Generation: request.Generation, Neighbors: []NeighborV1{{ID: "a", Score: .9}}}, nil
	}
	service, err := NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	request := validSearchRequestV1(id)
	if _, _, err := service.SearchFast(t.Context(), request, FastSearchOptionsV1{}); !hasCodeV1(err, ErrorInvalidRequestV1) {
		t.Fatalf("unbounded fast search error=%v", err)
	}
	response, evidence, err := service.SearchFast(t.Context(), request, FastSearchOptionsV1{MaxIndexAge: time.Second})
	if err != nil || len(response.Neighbors) != 1 || evidence.Generation != id || evidence.IndexedThrough != 0 {
		t.Fatalf("fast response=%+v evidence=%+v err=%v", response, evidence, err)
	}
	pinned, err := service.pinSearchSnapshotV1(t.Context(), PinSearchSnapshotOptionsV1{FastSearchOptionsV1: FastSearchOptionsV1{MaxIndexAge: time.Second}, MaxSessionAge: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if response, err := pinned.Search(t.Context(), request); err != nil || len(response.Neighbors) != 1 {
		t.Fatalf("pinned response=%+v err=%v", response, err)
	}
	other := request
	other.Generation.Generation++
	if _, err := pinned.Search(t.Context(), other); !hasCodeV1(err, ErrorGenerationMismatchV1) {
		t.Fatalf("pinned generation mismatch error=%v", err)
	}
	if err := pinned.Close(); err != nil || !backend.snapshot.closed {
		t.Fatalf("pinned close=%v backend_closed=%v", err, backend.snapshot.closed)
	}
	if _, err := pinned.Search(t.Context(), request); !hasCodeV1(err, ErrorUnavailableV1) {
		t.Fatalf("closed pinned search error=%v", err)
	}
}

func TestServiceV1SearchPathsApplyRequestDeadlineBeforeBackendV1(t *testing.T) {
	for _, path := range []string{"strict", "fast", "pinned"} {
		t.Run(path, func(t *testing.T) {
			id := GenerationIDV1{Index: "embedding", Generation: 7}
			backend := &serviceBackendV1{states: make(map[GenerationIDV1]GenerationStatusV1)}
			backend.search = func(ctx context.Context, request SearchRequestV1) (SearchResponseV1, error) {
				select {
				case <-ctx.Done():
					return SearchResponseV1{}, ctx.Err()
				case <-time.After(time.Second):
					t.Fatalf("%s backend outlived request deadline", path)
					return SearchResponseV1{}, nil
				}
			}
			service, err := NewServiceV1(backend)
			if err != nil {
				t.Fatal(err)
			}
			request := validSearchRequestV1(id)
			request.Deadline = time.Now().Add(20 * time.Millisecond)
			switch path {
			case "strict":
				_, err = service.Search(context.Background(), request)
			case "fast":
				_, _, err = service.SearchFast(context.Background(), request, FastSearchOptionsV1{MaxIndexAge: time.Second, MinIndexedThrough: 1})
			case "pinned":
				var pinned *serviceSearchSnapshotV1
				pinned, err = service.pinSearchSnapshotV1(context.Background(), PinSearchSnapshotOptionsV1{FastSearchOptionsV1: FastSearchOptionsV1{MaxIndexAge: time.Second}, MaxSessionAge: time.Second})
				if err == nil {
					defer pinned.Close()
					_, err = pinned.Search(context.Background(), request)
				}
			}
			if !hasCodeV1(err, ErrorDeadlineExceededV1) {
				t.Fatalf("%s deadline error=%v", path, err)
			}
		})
	}
}

func TestServiceSearchSnapshotCloseWaitsForSearchV1(t *testing.T) {
	id := GenerationIDV1{Index: "embedding", Generation: 7}
	backend := &blockingServiceSearchSnapshotBackendV1{started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	snapshot := &serviceSearchSnapshotV1{backend: backend, evidence: FastSearchEvidenceV1{Generation: id}}
	searchDone := make(chan error, 1)
	go func() {
		_, err := snapshot.Search(t.Context(), validSearchRequestV1(id))
		searchDone <- err
	}()
	<-backend.started
	closeDone := make(chan error, 1)
	go func() { closeDone <- snapshot.Close() }()
	select {
	case <-backend.closed:
		t.Fatal("Close released the backend during an in-flight search")
	case <-time.After(20 * time.Millisecond):
	}
	close(backend.release)
	if err := <-searchDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-backend.closed:
	default:
		t.Fatal("Close did not release the backend after search completion")
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
	response, err := svc.Search(context.Background(), validSearchRequestV1(id))
	if response.Generation != (GenerationIDV1{}) || len(response.Neighbors) != 0 {
		t.Fatalf("partial response escaped: %#v", response)
	}
	var apiErr *ErrorV1
	if !errors.As(err, &apiErr) || apiErr.Code != ErrorUnavailableV1 {
		t.Fatalf("error = %v", err)
	}
}

func TestServiceV1OwnsRequestAndOrderedResponseV1(t *testing.T) {
	id := GenerationIDV1{Index: "embedding", Generation: 7}
	backendNeighbors := []NeighborV1{{ID: "alpha", Score: .9}, {ID: "beta", Score: .9}, {ID: "gamma", Score: .8}}
	svc, err := NewServiceV1(&serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{}, search: func(_ context.Context, request SearchRequestV1) (SearchResponseV1, error) {
		request.Query[0] = 99
		return SearchResponseV1{Generation: request.Generation, Neighbors: backendNeighbors}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	request := validSearchRequestV1(id)
	request.TopK = len(backendNeighbors)
	request.EfSearch = request.TopK
	response, err := svc.Search(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if request.Query[0] != 1 {
		t.Fatalf("backend mutated caller query: %v", request.Query)
	}
	for i, want := range []string{"alpha", "beta", "gamma"} {
		if response.Neighbors[i].ID != want {
			t.Fatalf("response order = %+v", response.Neighbors)
		}
	}
	backendNeighbors[0].ID = "backend-mutated"
	if response.Neighbors[0].ID != "alpha" {
		t.Fatalf("backend retained response ownership: %+v", response.Neighbors)
	}
	response.Neighbors[1].ID = "caller-mutated"
	if backendNeighbors[1].ID != "beta" {
		t.Fatalf("caller mutated backend response: %+v", backendNeighbors)
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
	if _, err := svc.Search(ctx, validSearchRequestV1(id)); !hasCodeV1(err, ErrorCanceledV1) {
		t.Fatalf("cancel = %v", err)
	}
	request := validSearchRequestV1(id)
	request.Deadline = time.Now().Add(-time.Second)
	if _, err := svc.Search(context.Background(), request); !hasCodeV1(err, ErrorDeadlineExceededV1) {
		t.Fatalf("deadline = %v", err)
	}
	if called {
		t.Fatal("backend called after rejected request")
	}
}

func TestServiceV1RejectsUnsupportedRequestContractBeforeBackend(t *testing.T) {
	id := GenerationIDV1{Index: "embedding", Generation: 7}
	called := false
	svc, _ := NewServiceV1(&serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{}, search: func(context.Context, SearchRequestV1) (SearchResponseV1, error) {
		called = true
		return SearchResponseV1{}, nil
	}})
	for _, mutate := range []func(*SearchRequestV1){func(r *SearchRequestV1) { r.Version = 2 }, func(r *SearchRequestV1) { r.Metric = "dot" }, func(r *SearchRequestV1) { r.Consistency = "eventual" }, func(r *SearchRequestV1) { r.EfSearch = r.TopK - 1 }, func(r *SearchRequestV1) { r.Limits.ResponseBytes = 0 }, func(r *SearchRequestV1) { r.Limits.MergeEntries = -1 }} {
		r := validSearchRequestV1(id)
		mutate(&r)
		if _, err := svc.Search(context.Background(), r); !hasCodeV1(err, ErrorInvalidRequestV1) {
			t.Fatalf("error = %v", err)
		}
	}
	r := validSearchRequestV1(id)
	r.Query = []float32{1, 2}
	r.Limits.RequestBytes = 4
	if _, err := svc.Search(context.Background(), r); !hasCodeV1(err, ErrorInvalidRequestV1) {
		t.Fatalf("query byte limit = %v", err)
	}
	if _, err := svc.Register(context.Background(), GenerationRegistrationV1{GenerationIDV1: id}); !hasCodeV1(err, ErrorInvalidRequestV1) {
		t.Fatalf("registration = %v", err)
	}
	if called {
		t.Fatal("backend called after rejected request")
	}
}

func TestServiceV1RejectsBackendStatusForAnotherGeneration(t *testing.T) {
	id := GenerationIDV1{Index: "embedding", Generation: 7}
	backend := &serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{id: {Generation: GenerationIDV1{Index: "embedding", Generation: 8}}}}
	svc, err := NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := svc.Status(context.Background(), id); status != (GenerationStatusV1{}) || !hasCodeV1(err, ErrorFailedV1) {
		t.Fatalf("status = %#v, %v", status, err)
	}
	if eligibility, err := svc.CleanupEligibility(context.Background(), id); eligibility != (CleanupEligibilityV1{}) || !hasCodeV1(err, ErrorFailedV1) {
		t.Fatalf("cleanup = %#v, %v", eligibility, err)
	}
}

func hasCodeV1(err error, code ErrorCodeV1) bool {
	var apiErr *ErrorV1
	return errors.As(err, &apiErr) && apiErr.Code == code
}

func TestErrorV1WithoutCause(t *testing.T) {
	if got := (&ErrorV1{Code: ErrorFailedV1}).Error(); got != "vectorpartition: failed" {
		t.Fatalf("error = %q", got)
	}
}

func validSearchRequestV1(id GenerationIDV1) SearchRequestV1 {
	return SearchRequestV1{Version: 1, Generation: id, Query: []float32{1}, Metric: MetricCosineV1, TopK: 1, Probes: 1, EfSearch: 1, Consistency: ConsistencyGenerationSnapshotV1, Limits: SearchLimitsV1{RequestBytes: 4, CandidateBytes: 1, ResponseBytes: 1, MergeEntries: 1}}
}

func ExampleServiceV1_Search() {
	id := GenerationIDV1{Index: "embedding", Generation: 7}
	backend := &serviceBackendV1{states: map[GenerationIDV1]GenerationStatusV1{}, search: func(_ context.Context, request SearchRequestV1) (SearchResponseV1, error) {
		return SearchResponseV1{Generation: request.Generation, Neighbors: []NeighborV1{{ID: "alpha", Score: 0.9}}}, nil
	}}
	service, _ := NewServiceV1(backend)
	response, _ := service.Search(context.Background(), SearchRequestV1{
		Version: 1, Generation: id, Query: []float32{1}, Metric: MetricCosineV1,
		TopK: 1, Probes: 1, EfSearch: 64, Consistency: ConsistencyGenerationSnapshotV1,
		Limits: SearchLimitsV1{RequestBytes: 4, CandidateBytes: 1 << 20, ResponseBytes: 1 << 20, MergeEntries: 16},
	})
	fmt.Println(response.Neighbors[0].ID)
	// Output: alpha
}
