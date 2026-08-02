package nativewire

import (
	"context"
	"errors"
	"fmt"
	"testing"

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

type publicBackendLifecycleBuilderV1 struct{}

func (publicBackendLifecycleBuilderV1) BuildAndStageVectorPartitionGroupV1(_ context.Context, _ raftplacement.VectorPartitionLifecycleIdentityV1, group raftcluster.GroupID) (raftplacement.VectorPartitionLifecycleGroupReadyV1, error) {
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
	servingAuthority, err := NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1(harness.LeaderAuthority(), harness.LeaderFence())
	if err != nil {
		t.Fatal(err)
	}
	meta, ok := harness.LeaderAuthority().Status()
	if !ok {
		t.Fatal("leader authority unavailable")
	}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{Collection: raftplacement.CollectionRefV1{Database: "db", Catalog: "default", Collection: "docs"}, CollectionIncarnation: 1, IndexName: "embedding", IndexDefinitionDigest: vectorPartitionShardSearchDigestTestV1, IndexEpoch: 1, CatalogEpoch: meta.Epoch, CatalogDigest: meta.Digest}, Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{Generation: 11, Checksum: 22, SchemaHash: 33, RowCount: 2}, Generation: 7}
	requiredGroups := []raftcluster.GroupID{"group-a", "group-b"}
	readySetDigest, err := raftplacement.VectorPartitionLifecycleReadySetDigestV1(identity, requiredGroups, []raftplacement.VectorPartitionLifecycleGroupReadyV1{{GroupID: "group-a", AppliedIndex: 9, AssetSetDigest: fmt.Sprintf("%064x", len("group-a"))}, {GroupID: "group-b", AppliedIndex: 9, AssetSetDigest: fmt.Sprintf("%064x", len("group-b"))}})
	if err != nil {
		t.Fatal(err)
	}
	topology, base, reads := newVectorPartitionProductionTopologyTwoGroupWithLifecycleReadySetTestV1(t, servingAuthority, readySetDigest)
	defer topology.Close()
	backend, err := NewVectorPartitionPublicBackendV1(VectorPartitionPublicBackendOptionsV1{Topology: topology, RequestBase: base, Lifecycle: harness.LifecycleCoordinator(), Identity: identity, RequiredGroups: requiredGroups, Builder: publicBackendLifecycleBuilderV1{}, MutationEpoch: 1, RebuildRequest: func(context.Context) error { return nil }})
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
	if _, err := operations.Invalidate(ctx, id, "mutation"); err != nil {
		t.Fatal(err)
	}
	if health, err := backend.OperationsHealthV1(ctx); err != nil || health.Ready || health.Reason != "lifecycle_not_active" {
		t.Fatalf("invalidated operations health = %#v, %v", health, err)
	}
	proof, active, err := harness.LeaderAuthority().MutationProofV1(identity.Index)
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
	backend := &VectorPartitionPublicBackendV1{opts: VectorPartitionPublicBackendOptionsV1{
		Topology: topology, RequestBase: base,
		Identity: raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{IndexName: base.IndexName}, Generation: 7},
	}}
	request := public.SearchRequestV1{Version: 1, Generation: public.GenerationIDV1{Index: base.IndexName, Generation: 7}, Query: []float32{1, 0}, Metric: public.MetricCosineV1, TopK: base.TopK, Probes: base.PartitionProbes, EfSearch: base.EfSearch, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: base.RequestBytesLimit, CandidateBytes: base.CandidateBytesLimit, ResponseBytes: base.ResponseBytesLimit, MergeEntries: base.MergeEntriesLimit}}
	response, err := backend.SearchVectorPartitionV1(context.Background(), request)
	if err != nil || len(response.Neighbors) == 0 {
		t.Fatalf("search = %#v, %v", response, err)
	}
	for group, read := range reads {
		if read.callCount() == 0 {
			t.Fatalf("group %q did not produce read evidence", group)
		}
	}
}
