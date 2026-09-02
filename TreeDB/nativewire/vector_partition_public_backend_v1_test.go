package nativewire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestVectorPartitionPublicBackendMapsCoordinatorErrorsV1(t *testing.T) {
	cases := []struct {
		code VectorPartitionCoordinatorErrorCodeV1
		want public.ErrorCodeV1
	}{
		{VectorPartitionCoordinatorErrorInvalidRequestV1, public.ErrorInvalidRequestV1},
		{VectorPartitionCoordinatorErrorMalformedResponseV1, public.ErrorFailedV1},
		{VectorPartitionCoordinatorErrorGenerationMismatchV1, public.ErrorGenerationMismatchV1},
		{VectorPartitionCoordinatorErrorCanceledV1, public.ErrorCanceledV1},
		{VectorPartitionCoordinatorErrorDeadlineV1, public.ErrorDeadlineExceededV1},
		{VectorPartitionCoordinatorErrorUnavailableV1, public.ErrorUnavailableV1},
	}
	for _, tc := range cases {
		err := publicBackendErrorV1(&VectorPartitionCoordinatorErrorV1{Code: tc.code})
		var got *public.ErrorV1
		if !errors.As(err, &got) || got.Code != tc.want {
			t.Fatalf("code=%q got=%v", tc.code, err)
		}
	}
	if got := publicBackendErrorV1(context.Canceled); !errors.Is(got, context.Canceled) {
		t.Fatalf("canceled = %v", got)
	}
}

func TestVectorPartitionPublicBackendMapsBoundGenerationMismatchV1(t *testing.T) {
	backend := &VectorPartitionPublicBackendV1{opts: VectorPartitionPublicBackendOptionsV1{Identity: raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{IndexName: "embedding"}, Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{Generation: 1, Checksum: 2, SchemaHash: 3, RowCount: 4}, Generation: 7}}}
	service, err := public.NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RequestRebuild(context.Background(), public.GenerationIDV1{Index: "embedding", Generation: 8}); !hasPublicErrorCodeV1(err, public.ErrorGenerationMismatchV1) {
		t.Fatalf("id mismatch = %v", err)
	}
	if _, err := service.Register(context.Background(), public.GenerationRegistrationV1{GenerationIDV1: public.GenerationIDV1{Index: "embedding", Generation: 7}, SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 5}); !hasPublicErrorCodeV1(err, public.ErrorGenerationMismatchV1) {
		t.Fatalf("source mismatch = %v", err)
	}
}

func hasPublicErrorCodeV1(err error, want public.ErrorCodeV1) bool {
	var apiErr *public.ErrorV1
	return errors.As(err, &apiErr) && apiErr.Code == want
}

type publicBackendLifecycleBuilderV1 struct{ calls int }

func (b *publicBackendLifecycleBuilderV1) BuildAndStageVectorPartitionGroupV1(_ context.Context, _ raftplacement.VectorPartitionLifecycleIdentityV1, group raftcluster.GroupID) (raftplacement.VectorPartitionLifecycleGroupReadyV1, error) {
	b.calls++
	return raftplacement.VectorPartitionLifecycleGroupReadyV1{GroupID: group, AppliedIndex: 9, AssetSetDigest: fmt.Sprintf("%064x", len(group))}, nil
}

func TestVectorPartitionPublicBackendLifecycleOverCatalogMetaRaftV1(t *testing.T) {
	ctx := t.Context()
	catalog := raftplacement.CatalogV1{Features: raftplacement.DefaultFeatureSet(), Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}}, {ID: "group-b", Members: []raftcluster.NodeID{"node-b"}}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: raftplacement.CollectionRefV1{Database: "db", Catalog: "default", Collection: "docs"}, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}}
	catalog.Features.Required = append(catalog.Features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]})
	harness, err := raftplacement.OpenCatalogMetaLifecycleHarnessV1(ctx, raftplacement.CatalogMetaLifecycleHarnessOptionsV1{Catalog: catalog, Prefix: "public-vector"})
	if err != nil {
		t.Fatal(err)
	}
	defer harness.Close()
	meta, ok := harness.LeaderAuthority().Status()
	if !ok {
		t.Fatal("leader authority unavailable")
	}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{Collection: raftplacement.CollectionRefV1{Database: "db", Catalog: "default", Collection: "docs"}, CollectionIncarnation: 1, IndexName: "embedding", IndexDefinitionDigest: vectorPartitionShardSearchDigestTestV1, IndexEpoch: 1, CatalogEpoch: meta.Epoch, CatalogDigest: meta.Digest}, Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{Generation: 11, Checksum: 22, SchemaHash: 33, RowCount: 2}, Generation: 7}
	requiredGroups := []raftcluster.GroupID{"group-a", "group-b"}
	readyGroups := []raftplacement.VectorPartitionLifecycleGroupReadyV1{{GroupID: "group-a", AppliedIndex: 9, AssetSetDigest: fmt.Sprintf("%064x", len("group-a"))}, {GroupID: "group-b", AppliedIndex: 9, AssetSetDigest: fmt.Sprintf("%064x", len("group-b"))}}
	openService := func(boundIdentity raftplacement.VectorPartitionLifecycleIdentityV1) (*public.OperationsV1, *VectorPartitionPublicBackendV1, *VectorPartitionProductionTopologyV1, VectorPartitionCoordinatorRequestV1, map[raftcluster.GroupID]*fakeVectorPartitionReadCoordinatorV1, *publicBackendLifecycleBuilderV1) {
		t.Helper()
		readySetDigest, err := raftplacement.VectorPartitionLifecycleReadySetDigestV1(boundIdentity, requiredGroups, readyGroups)
		if err != nil {
			t.Fatal(err)
		}
		servingAuthority, err := NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1(harness.LeaderAuthority(), harness.LeaderFence())
		if err != nil {
			t.Fatal(err)
		}
		topology, base, reads := newVectorPartitionProductionTopologyTwoGroupWithLifecycleReadySetTestV1(t, servingAuthority, readySetDigest)
		builder := &publicBackendLifecycleBuilderV1{}
		backend, err := NewVectorPartitionPublicBackendV1(VectorPartitionPublicBackendOptionsV1{Topology: topology, RequestBase: base, Lifecycle: harness.LifecycleCoordinator(), ReadFence: harness.LeaderFence(), Identity: boundIdentity, RequiredGroups: requiredGroups, Builder: builder, MutationEpoch: 1, RebuildRequest: func(context.Context) error { return nil }})
		if err != nil {
			t.Fatal(err)
		}
		service, err := public.NewServiceV1(backend)
		if err != nil {
			t.Fatal(err)
		}
		operationsConfig := public.ConservativeOperationsConfigV1()
		operationsConfig.Enabled = true
		// This lifecycle fixture intentionally exercises the coordinator's larger
		// production bounds; the operator cap remains explicit in deployment.
		operationsConfig.MaxCandidateBytes, operationsConfig.MaxResponseBytes = 1<<40, 1<<40
		operationsConfig.MaxTopK, operationsConfig.MaxProbes, operationsConfig.MaxEfSearch, operationsConfig.MaxMergeEntries = 1<<20, 1<<20, 1<<20, 1<<30
		operations, err := public.NewOperationsV1(service, operationsConfig, backend.OperationsHealthV1)
		if err != nil {
			t.Fatal(err)
		}
		return operations, backend, topology, base, reads, builder
	}
	operations, backend, topology, base, reads, _ := openService(identity)
	nearLimit := backend.opts
	maxIdentityBytes := topology.Coordinator().limits.MaxIdentityBytes
	nearLimit.RequestBase.RequestID = strings.Repeat("r", maxIdentityBytes-vectorPartitionPublicRequestSuffixBytesV1)
	if _, err := NewVectorPartitionPublicBackendV1(nearLimit); err != nil {
		t.Fatalf("exact identity limit = %v", err)
	}
	nearLimit.RequestBase.RequestID += "r"
	if _, err := NewVectorPartitionPublicBackendV1(nearLimit); err == nil || !strings.Contains(err.Error(), "exceeds coordinator limit after suffix") {
		t.Fatalf("oversized suffixed identity = %v", err)
	}
	id := public.GenerationIDV1{Index: base.IndexName, Generation: 7}
	if _, err := operations.Register(ctx, public.GenerationRegistrationV1{GenerationIDV1: id, SourceGeneration: 11, SourceChecksum: 22, SourceSchemaHash: 33, SourceRowCount: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := operations.Prepare(ctx, id); err != nil {
		t.Fatal(err)
	}
	if status, err := operations.Activate(ctx, id); err != nil || !status.Active {
		t.Fatalf("activate = %#v, %v", status, err)
	}
	if inventory, err := operations.Inventory(ctx, id); err != nil || len(inventory) != 1 || !inventory[0].Active {
		t.Fatalf("inventory = %#v, %v", inventory, err)
	}
	if health, err := backend.OperationsHealthV1(ctx); err != nil || !health.Ready || health.Reason != "ready" {
		t.Fatalf("operations health = %#v, %v", health, err)
	}
	topology.mu.Lock()
	listeners, serving := topology.listeners, topology.serving
	topology.listeners, topology.serving = nil, nil
	topology.mu.Unlock()
	if health, err := backend.OperationsHealthV1(ctx); err != nil || !health.Ready || health.Reason != "ready" {
		t.Fatalf("coordinator-only health = %#v, %v", health, err)
	}
	topology.mu.Lock()
	topology.listeners, topology.serving = listeners, serving
	topology.mu.Unlock()
	backend.opts.Identity.Source.Checksum++
	if health, err := backend.OperationsHealthV1(ctx); err != nil || health.Ready || health.Reason != "source_mismatch" {
		t.Fatalf("source mismatch health = %#v, %v", health, err)
	}
	backend.opts.Identity.Source.Checksum--
	backend.opts.ReadFence = &catalogMetaLinearizableAppliedIndexProviderTestV1{err: raftcluster.ErrNotLeader}
	if health, err := backend.OperationsHealthV1(ctx); !errors.Is(err, raftcluster.ErrNotLeader) || health.Ready || health.Reason != "catalog_unavailable" {
		t.Fatalf("unfenced operations health = %#v, %v", health, err)
	}
	backend.opts.ReadFence = harness.LeaderFence()
	backend.opts.RequiredGroups = []raftcluster.GroupID{"group-a", "group-c"}
	if health, err := backend.OperationsHealthV1(ctx); err != nil || health.Ready || health.Reason != "topology_unavailable" {
		t.Fatalf("mismatched group health = %#v, %v", health, err)
	}
	backend.opts.RequiredGroups = requiredGroups
	request := public.SearchRequestV1{Version: 1, Generation: id, Query: []float32{1, 0}, Metric: public.MetricCosineV1, TopK: base.TopK, Probes: base.PartitionProbes, EfSearch: base.EfSearch, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: base.RequestBytesLimit, CandidateBytes: base.CandidateBytesLimit, ResponseBytes: base.ResponseBytesLimit, MergeEntries: base.MergeEntriesLimit}}
	response, err := operations.Search(ctx, request)
	if err != nil || len(response.Neighbors) == 0 {
		t.Fatalf("search = %#v, %v", response, err)
	}
	for group, read := range reads {
		if read.callCount() == 0 {
			t.Fatalf("group %q did not produce read evidence", group)
		}
	}
	if _, err := harness.LeaderFence().LinearizableCatalogMetaAppliedIndexV1(ctx); err != nil {
		t.Fatal(err)
	}
	if err := topology.Close(); err != nil {
		t.Fatal(err)
	}
	if err := harness.RestartV1(ctx); err != nil {
		t.Fatal(err)
	}
	operations, backend, topology, base, reads, _ = openService(identity)
	if inventory, err := operations.Inventory(ctx, id); err != nil || len(inventory) != 1 || !inventory[0].Active {
		t.Fatalf("inventory after restart = %#v, %v", inventory, err)
	}
	request = public.SearchRequestV1{Version: 1, Generation: id, Query: []float32{1, 0}, Metric: public.MetricCosineV1, TopK: base.TopK, Probes: base.PartitionProbes, EfSearch: base.EfSearch, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: base.RequestBytesLimit, CandidateBytes: base.CandidateBytesLimit, ResponseBytes: base.ResponseBytesLimit, MergeEntries: base.MergeEntriesLimit}}
	if response, err := operations.Search(ctx, request); err != nil || len(response.Neighbors) == 0 {
		t.Fatalf("search after restart = %#v, %v", response, err)
	}
	for group, read := range reads {
		if read.callCount() == 0 {
			t.Fatalf("group %q did not produce post-restart read evidence", group)
		}
	}
	predecessorOperations, predecessorID := operations, id
	if err := topology.Close(); err != nil {
		t.Fatal(err)
	}
	successor := identity
	successor.Generation++
	operations, backend, topology, _, _, builder := openService(successor)
	defer topology.Close()
	id = public.GenerationIDV1{Index: base.IndexName, Generation: successor.Generation}
	if _, err := operations.Register(ctx, public.GenerationRegistrationV1{GenerationIDV1: id, SourceGeneration: 11, SourceChecksum: 22, SourceSchemaHash: 33, SourceRowCount: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := operations.Prepare(ctx, id); err != nil {
		t.Fatal(err)
	}
	if status, err := operations.Activate(ctx, id); err != nil || !status.Active {
		t.Fatalf("activate successor = %#v, %v", status, err)
	}
	buildCalls := builder.calls
	if _, err := operations.Register(ctx, public.GenerationRegistrationV1{GenerationIDV1: id, SourceGeneration: 11, SourceChecksum: 22, SourceSchemaHash: 33, SourceRowCount: 2}); err != nil || builder.calls != buildCalls {
		t.Fatalf("register retry rebuilt ready groups: calls=%d want=%d err=%v", builder.calls, buildCalls, err)
	}
	if _, err := predecessorOperations.Invalidate(ctx, predecessorID, "stale mutation"); !hasPublicErrorCodeV1(err, public.ErrorGenerationMismatchV1) {
		t.Fatalf("stale predecessor invalidation = %v", err)
	}
	if inventory, err := operations.Inventory(ctx, id); err != nil || len(inventory) != 1 || !inventory[0].Active {
		t.Fatalf("successor after stale invalidation = %#v, %v", inventory, err)
	}
	if previous, ok := harness.LeaderAuthority().VectorPartitionLifecycleRecordV1(identity); !ok || previous.State != raftplacement.VectorPartitionLifecycleRetiredV1 || previous.SupersededByGeneration != successor.Generation {
		t.Fatalf("predecessor after cutover = %#v, ok=%v", previous, ok)
	}
	if _, err := operations.Invalidate(ctx, id, "mutation"); err != nil {
		t.Fatal(err)
	}
	if health, err := backend.OperationsHealthV1(ctx); err != nil || health.Ready || health.Reason != "lifecycle_not_active" {
		t.Fatalf("invalidated operations health = %#v, %v", health, err)
	}
	proof, active, err := harness.LeaderAuthority().MutationProofV1(successor.Index)
	if err != nil || active {
		t.Fatalf("invalidation proof = %#v active=%v err=%v", proof, active, err)
	}
	if err := harness.LifecycleCoordinator().ConfirmRelevantMutationV1(ctx, proof); err != nil {
		t.Fatal(err)
	}
	if status, err := operations.Retire(ctx, id); err != nil || status.State != public.GenerationRetiredV1 {
		t.Fatalf("retire = %#v, %v", status, err)
	}
	if err := topology.Close(); err != nil {
		t.Fatal(err)
	}
	if health, err := backend.OperationsHealthV1(ctx); err != nil || health.Ready || health.Reason != "topology_unavailable" {
		t.Fatalf("closed operations health = %#v, %v", health, err)
	}
}

func TestVectorPartitionPublicBackendSearchesProductionTopologyV1(t *testing.T) {
	topology, base, reads := newVectorPartitionProductionTopologyTwoGroupTestV1(t)
	defer topology.Close()
	direct, err := topology.Coordinator().Search(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	base.DeadlineUnixNano = time.Now().Add(-time.Second).UnixNano()
	backend := &VectorPartitionPublicBackendV1{opts: VectorPartitionPublicBackendOptionsV1{
		Topology: topology, RequestBase: base,
		Identity: raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{IndexName: base.IndexName}, Generation: 7},
	}}
	service, err := public.NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	request := publicSearchRequestTestV1(base)
	response, err := service.Search(context.Background(), request)
	if err != nil || len(response.Neighbors) == 0 {
		t.Fatalf("search = %#v, %v", response, err)
	}
	wantNeighbors := publicNeighborsFromCoordinatorTestV1(direct.Neighbors)
	if !slices.Equal(response.Neighbors, wantNeighbors) {
		t.Fatalf("public neighbors = %+v, direct = %+v", response.Neighbors, wantNeighbors)
	}
	if want := publicCountersFromCoordinatorTestV1(direct.Counters); !publicCountersMatchWithRequestIdentityV1(response.Counters, want) {
		t.Fatalf("public counters = %+v, direct = %+v", response.Counters, want)
	}
	if response.Timing.Total < response.Timing.PublicAdapter+response.Timing.CoordinatorTotal || response.Timing.RouterSearch <= 0 || response.Timing.RPC <= 0 || response.Timing.ReadIndexApply <= 0 || response.Timing.ShardSearch <= 0 {
		t.Fatalf("public timing attribution = %+v", response.Timing)
	}
	response.Neighbors[0].ID = "caller-mutated"
	again, err := service.Search(context.Background(), request)
	if err != nil || !slices.Equal(again.Neighbors, wantNeighbors) {
		t.Fatalf("owned ordered response after caller mutation = %+v, %v", again.Neighbors, err)
	}
	for group, read := range reads {
		if read.callCount() == 0 {
			t.Fatalf("group %q did not produce read evidence", group)
		}
	}
	backend.opts.Identity.Generation++
	request.Generation.Generation++
	if response, err := service.Search(context.Background(), request); !hasPublicErrorCodeV1(err, public.ErrorGenerationMismatchV1) || response.Generation != (public.GenerationIDV1{}) || len(response.Neighbors) != 0 {
		t.Fatalf("stale topology response = %#v, %v", response, err)
	}
}

func BenchmarkVectorPartitionPublicServiceOverheadV1(b *testing.B) {
	parityTopology, parityBase, _ := newVectorPartitionProductionTopologyTwoGroupTestV1(b)
	parityBackend := &VectorPartitionPublicBackendV1{opts: VectorPartitionPublicBackendOptionsV1{
		Topology: parityTopology, RequestBase: parityBase,
		Identity: raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{IndexName: parityBase.IndexName}, Generation: 7},
	}}
	parityService, err := public.NewServiceV1(parityBackend)
	if err != nil {
		b.Fatal(err)
	}
	direct, err := parityTopology.Coordinator().Search(context.Background(), parityBase)
	if err != nil {
		b.Fatal(err)
	}
	viaPublic, err := parityService.Search(context.Background(), publicSearchRequestTestV1(parityBase))
	if err != nil {
		b.Fatal(err)
	}
	if !slices.Equal(viaPublic.Neighbors, publicNeighborsFromCoordinatorTestV1(direct.Neighbors)) || !publicCountersMatchWithRequestIdentityV1(viaPublic.Counters, publicCountersFromCoordinatorTestV1(direct.Counters)) {
		b.Fatalf("public/direct parity failed: public=%+v direct=%+v", viaPublic, direct)
	}
	if err := parityTopology.Close(); err != nil {
		b.Fatal(err)
	}
	b.Run("direct", func(b *testing.B) {
		topology, base, _ := newVectorPartitionProductionTopologyTwoGroupTestV1(b)
		defer topology.Close()
		for i := 0; i < 16; i++ {
			if _, err := topology.Coordinator().Search(context.Background(), base); err != nil {
				b.Fatal(err)
			}
		}
		runVectorPartitionPublicBenchmarkV1(b, func() (public.SearchCountersV1, error) {
			response, err := topology.Coordinator().Search(context.Background(), base)
			return publicCountersFromCoordinatorTestV1(response.Counters), err
		})
	})
	b.Run("public", func(b *testing.B) {
		topology, base, _ := newVectorPartitionProductionTopologyTwoGroupTestV1(b)
		defer topology.Close()
		backend := &VectorPartitionPublicBackendV1{opts: VectorPartitionPublicBackendOptionsV1{
			Topology: topology, RequestBase: base,
			Identity: raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{IndexName: base.IndexName}, Generation: 7},
		}}
		service, err := public.NewServiceV1(backend)
		if err != nil {
			b.Fatal(err)
		}
		request := publicSearchRequestTestV1(base)
		for i := 0; i < 16; i++ {
			if _, err := service.Search(context.Background(), request); err != nil {
				b.Fatal(err)
			}
		}
		runVectorPartitionPublicBenchmarkV1(b, func() (public.SearchCountersV1, error) {
			response, err := service.Search(context.Background(), request)
			return response.Counters, err
		})
	})
	b.Run("operations", func(b *testing.B) {
		topology, base, _ := newVectorPartitionProductionTopologyTwoGroupTestV1(b)
		defer topology.Close()
		backend := &VectorPartitionPublicBackendV1{opts: VectorPartitionPublicBackendOptionsV1{
			Topology: topology, RequestBase: base,
			Identity: raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{IndexName: base.IndexName}, Generation: 7},
		}}
		service, err := public.NewServiceV1(backend)
		if err != nil {
			b.Fatal(err)
		}
		config := public.ConservativeOperationsConfigV1()
		config.Enabled = true
		config.MaxRequestBytes, config.MaxCandidateBytes, config.MaxResponseBytes = base.RequestBytesLimit, base.CandidateBytesLimit, base.ResponseBytesLimit
		config.MaxTopK, config.MaxProbes, config.MaxEfSearch, config.MaxMergeEntries = base.TopK, base.PartitionProbes, base.EfSearch, base.MergeEntriesLimit
		operations, err := public.NewOperationsV1(service, config, func(context.Context) (public.OperationsHealthV1, error) {
			return public.OperationsHealthV1{Ready: true}, nil
		})
		if err != nil {
			b.Fatal(err)
		}
		request := publicSearchRequestTestV1(base)
		response, err := operations.Search(context.Background(), request)
		if err != nil || !slices.Equal(response.Neighbors, publicNeighborsFromCoordinatorTestV1(direct.Neighbors)) || !publicCountersMatchWithRequestIdentityV1(response.Counters, publicCountersFromCoordinatorTestV1(direct.Counters)) {
			b.Fatalf("operations/direct parity failed: operations=%+v direct=%+v err=%v", response, direct, err)
		}
		for i := 0; i < 16; i++ {
			if _, err := operations.Search(context.Background(), request); err != nil {
				b.Fatal(err)
			}
		}
		runVectorPartitionPublicBenchmarkV1(b, func() (public.SearchCountersV1, error) {
			response, err := operations.Search(context.Background(), request)
			return response.Counters, err
		})
	})
}

func runVectorPartitionPublicBenchmarkV1(b *testing.B, search func() (public.SearchCountersV1, error)) {
	latencies := make([]uint64, b.N)
	var counters public.SearchCountersV1
	b.ReportAllocs()
	b.ResetTimer()
	started := time.Now()
	for i := 0; i < b.N; i++ {
		callStarted := time.Now()
		var err error
		counters, err = search()
		latencies[i] = uint64(time.Since(callStarted))
		if err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(started)
	b.StopTimer()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "qps")
	b.ReportMetric(float64(vectorPartitionShardPercentileTestV1(latencies, 50)), "p50-ns")
	b.ReportMetric(float64(vectorPartitionShardPercentileTestV1(latencies, 95)), "p95-ns")
	b.ReportMetric(float64(vectorPartitionShardPercentileTestV1(latencies, 99)), "p99-ns")
	b.ReportMetric(float64(counters.QueryBytes), "query-B/op")
	b.ReportMetric(float64(counters.RequestBytes), "request-B/op")
	b.ReportMetric(float64(counters.CandidateBytes), "candidate-B/op")
	b.ReportMetric(float64(counters.ResponseBytes), "response-B/op")
	b.ReportMetric(float64(counters.Candidates), "candidates/op")
	b.ReportMetric(float64(counters.Edges), "edges/op")
}

func publicSearchRequestTestV1(base VectorPartitionCoordinatorRequestV1) public.SearchRequestV1 {
	return public.SearchRequestV1{Version: 1, Generation: public.GenerationIDV1{Index: base.IndexName, Generation: 7}, Query: slices.Clone(base.Query), Metric: public.MetricCosineV1, TopK: base.TopK, Probes: base.PartitionProbes, EfSearch: base.EfSearch, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: base.RequestBytesLimit, CandidateBytes: base.CandidateBytesLimit, ResponseBytes: base.ResponseBytesLimit, MergeEntries: base.MergeEntriesLimit}}
}

func publicNeighborsFromCoordinatorTestV1(neighbors []VectorPartitionCoordinatorNeighborV1) []public.NeighborV1 {
	out := make([]public.NeighborV1, len(neighbors))
	for i, neighbor := range neighbors {
		out[i] = public.NeighborV1{ID: neighbor.ID, Score: neighbor.Score}
	}
	return out
}

func publicCountersFromCoordinatorTestV1(counters VectorPartitionCoordinatorCountersV1) public.SearchCountersV1 {
	return public.SearchCountersV1{
		SelectedPartitions: counters.SelectedPartitions, SelectedGroups: counters.SelectedGroups,
		HNSWServedPartitions: counters.HNSWServedPartitions, ExactScanPartitions: counters.ExactScanPartitions,
		Requests: counters.Requests, RPCs: counters.RPCs, Retries: counters.Retries, Redirects: counters.Redirects,
		Candidates: counters.Candidates, Edges: counters.Edges,
		QueryBytes: counters.QueryBytes, RequestBytes: counters.RequestBytes, CandidateBytes: counters.CandidateBytes, ResponseBytes: counters.ResponseBytes,
	}
}

func publicCountersMatchWithRequestIdentityV1(got, want public.SearchCountersV1) bool {
	if got.RequestBytes < want.RequestBytes {
		return false
	}
	got.RequestBytes = want.RequestBytes
	return got == want
}
