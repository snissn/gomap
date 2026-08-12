package nativewire

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestTopVectorPartitionCoordinatorNeighborsV1DetachesIDs(t *testing.T) {
	backing := strings.Repeat("x", 4096) + "doc"
	id := backing[len(backing)-3:]
	got, err := topVectorPartitionCoordinatorNeighborsV1(context.Background(), map[string]float32{id: 1}, 1)
	if err != nil || len(got) != 1 || got[0].ID != id {
		t.Fatalf("neighbors=%+v err=%v", got, err)
	}
	start := uintptr(unsafe.Pointer(unsafe.StringData(backing)))
	result := uintptr(unsafe.Pointer(unsafe.StringData(got[0].ID)))
	if result >= start && result < start+uintptr(len(backing)) {
		t.Fatal("returned ID retains the decoded response backing frame")
	}
}

type testVectorPartitionCoordinatorRouterV1 struct {
	status     collections.VectorPartitionRouterRuntimeStatusV1
	partitions []collections.VectorPartitionRouterPartitionScoreV1
	closeMu    sync.Mutex
	closeCount int
	closeErr   error
	closeBlock <-chan struct{}
	closeStart chan<- struct{}
}

var vectorPartitionCoordinatorCandidateRowsBenchmarkSinkV1 struct {
	rows  []uint64
	total uint64
}

func (r *testVectorPartitionCoordinatorRouterV1) SearchWithContextV1(ctx context.Context, _ []float32, opts collections.VectorPartitionRouterSearchOptionsV1) (collections.VectorPartitionRouterSearchResultV1, error) {
	if err := ctx.Err(); err != nil {
		return collections.VectorPartitionRouterSearchResultV1{}, err
	}
	if opts.PartitionProbes < 1 || opts.PartitionProbes > len(r.partitions) {
		return collections.VectorPartitionRouterSearchResultV1{}, errors.New("bad probes")
	}
	return collections.VectorPartitionRouterSearchResultV1{
		Partitions: slices.Clone(r.partitions[:opts.PartitionProbes]),
		Status: collections.VectorPartitionRouterSearchStatusV1{
			Mode: opts.Mode, CandidateBudget: uint64(opts.CandidateBudget),
			PartitionProbes: uint64(opts.PartitionProbes), Candidates: uint64(opts.CandidateBudget),
			Selected: uint64(opts.PartitionProbes), SearchNanos: 1,
		},
	}, nil
}

func (r *testVectorPartitionCoordinatorRouterV1) Status() collections.VectorPartitionRouterRuntimeStatusV1 {
	return r.status
}

func (r *testVectorPartitionCoordinatorRouterV1) Close() error {
	r.closeMu.Lock()
	r.closeCount++
	closeErr, closeBlock, closeStart := r.closeErr, r.closeBlock, r.closeStart
	r.closeMu.Unlock()
	if closeStart != nil {
		closeStart <- struct{}{}
	}
	if closeBlock != nil {
		<-closeBlock
	}
	return closeErr
}

type testVectorPartitionCoordinatorRouterSourceV1 struct {
	mu          sync.Mutex
	router      *testVectorPartitionCoordinatorRouterV1
	opens       int
	generations []uint64
	openStarted chan<- struct{}
	openBlock   <-chan struct{}
	openErr     error
	openRouters []VectorPartitionCoordinatorRouterV1
	nextRouter  int
}

func (s *testVectorPartitionCoordinatorRouterSourceV1) OpenVectorPartitionCoordinatorRouterV1(ctx context.Context, _ string, generation uint64) (VectorPartitionCoordinatorRouterV1, error) {
	s.mu.Lock()
	s.opens++
	s.generations = append(s.generations, generation)
	started, block, openErr, router := s.openStarted, s.openBlock, s.openErr, VectorPartitionCoordinatorRouterV1(s.router)
	if len(s.openRouters) > 0 {
		router = s.openRouters[min(s.nextRouter, len(s.openRouters)-1)]
		s.nextRouter++
	}
	s.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if block != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-block:
		}
	}
	if openErr != nil {
		return nil, openErr
	}
	return router, nil
}

func (s *testVectorPartitionCoordinatorRouterSourceV1) openCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens
}

type testVectorPartitionCoordinatorDispatcherV1 struct {
	mu              sync.Mutex
	neighbors       map[uint32][]VectorPartitionShardSearchNeighborV1
	calls           []VectorPartitionShardSearchRequestV1
	active, maximum int
	blockByGroup    map[raftcluster.GroupID]<-chan struct{}
	blocked         chan<- raftcluster.GroupID
	failGroup       raftcluster.GroupID
	failErr         error
	failAfter       <-chan struct{}
	notLeaderOnce   map[raftcluster.GroupID]raftcluster.NodeID
	editResponse    func(VectorPartitionShardSearchRequestV1, *VectorPartitionShardSearchResponseV1)
}

func (d *testVectorPartitionCoordinatorDispatcherV1) DispatchVectorPartitionShardSearchV1(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
	d.mu.Lock()
	d.calls = append(d.calls, request)
	d.active++
	if d.active > d.maximum {
		d.maximum = d.active
	}
	defer func() {
		d.mu.Lock()
		d.active--
		d.mu.Unlock()
	}()
	if hint, ok := d.notLeaderOnce[request.TargetGroupID]; ok {
		delete(d.notLeaderOnce, request.TargetGroupID)
		d.mu.Unlock()
		return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{
			Code: VectorPartitionShardSearchErrorNotLeaderV1, GroupID: request.TargetGroupID,
			LeaderHint: hint, Err: raftcluster.ErrNotLeader,
		}
	}
	fail := request.TargetGroupID == d.failGroup
	failErr := d.failErr
	failAfter := d.failAfter
	block := d.blockByGroup[request.TargetGroupID]
	blocked := d.blocked
	editResponse := d.editResponse
	d.mu.Unlock()

	if fail && failAfter != nil {
		select {
		case <-ctx.Done():
			return VectorPartitionShardSearchResponseV1{}, ctx.Err()
		case <-failAfter:
		}
	}
	if block != nil {
		if blocked != nil {
			select {
			case blocked <- request.TargetGroupID:
			case <-ctx.Done():
				return VectorPartitionShardSearchResponseV1{}, ctx.Err()
			}
		}
		select {
		case <-ctx.Done():
			return VectorPartitionShardSearchResponseV1{}, ctx.Err()
		case <-block:
		}
	}
	if fail {
		return VectorPartitionShardSearchResponseV1{}, failErr
	}
	partials := make([]VectorPartitionShardSearchPartialV1, len(request.PartitionIDs))
	var candidates, responseBytes uint64 = 0, vectorPartitionShardSearchResponseEnvelopeBytesV1
	responseBytes += uint64(len(partials)) * vectorPartitionShardSearchPartialEnvelopeBytesV1
	for i, partitionID := range request.PartitionIDs {
		neighbors := slices.Clone(d.neighbors[partitionID])
		partials[i] = VectorPartitionShardSearchPartialV1{
			PartitionID: partitionID, Neighbors: neighbors,
			Candidates: uint64(len(neighbors)), SearchRoute: collections.VectorPartitionSearchRouteExactFP32ScanV1,
		}
		candidates += uint64(len(neighbors))
		for _, neighbor := range neighbors {
			responseBytes += uint64(len(neighbor.ID) + 16)
		}
	}
	response := VectorPartitionShardSearchResponseV1{
		Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID,
		Proof: VectorPartitionShardSearchProofV1{
			ServingNode: request.TargetNodeID, LeaderNode: request.TargetNodeID, GroupID: request.TargetGroupID,
			ReadySetDigest: request.ReadySetDigest,
			ReadTerm:       1, ReadIndex: 2, AppliedTerm: 1, AppliedIndex: 2,
			SourceGeneration: request.SourceGeneration, SourceChecksum: request.SourceChecksum,
			SourceSchemaHash: request.SourceSchemaHash, SourceRowCount: request.SourceRowCount,
			PartitionGeneration: request.PartitionGeneration, RouterGeneration: request.RouterGeneration,
		},
		Partials: partials, Partitions: uint64(len(partials)), Candidates: candidates,
		ResponseBytes: responseBytes,
		Timing: VectorPartitionShardSearchTimingV1{
			ReadIndexApplyNanos: 1, SearchNanos: 1, ResponseCopyNanos: 1, TotalNanos: 3,
		},
	}
	if editResponse != nil {
		editResponse(request, &response)
	}
	return response, nil
}

func testVectorPartitionCoordinatorV1(t *testing.T, groups []raftplacement.GroupV1, owners []raftcluster.GroupID, neighbors map[uint32][]VectorPartitionShardSearchNeighborV1, limits VectorPartitionCoordinatorLimitsV1) (*VectorPartitionCoordinatorV1, *testVectorPartitionCoordinatorRouterSourceV1, *testVectorPartitionCoordinatorDispatcherV1) {
	return testVectorPartitionCoordinatorWithShardLimitsV1(t, groups, owners, neighbors, limits, VectorPartitionShardSearchLimitsV1{})
}

func testVectorPartitionCoordinatorWithShardLimitsV1(t *testing.T, groups []raftplacement.GroupV1, owners []raftcluster.GroupID, neighbors map[uint32][]VectorPartitionShardSearchNeighborV1, limits VectorPartitionCoordinatorLimitsV1, shardLimits VectorPartitionShardSearchLimitsV1) (*VectorPartitionCoordinatorV1, *testVectorPartitionCoordinatorRouterSourceV1, *testVectorPartitionCoordinatorDispatcherV1) {
	t.Helper()
	ref := raftplacement.CollectionRefV1{Database: "db", Catalog: "default", Collection: "docs"}
	catalogInput := raftplacement.CatalogV1{
		Groups: groups,
		Placements: []raftplacement.CollectionPlacementV1{{
			Collection: ref, GroupID: groups[0].ID, Mode: raftplacement.PlacementModeCollectionV1,
		}},
	}
	catalog, err := raftplacement.Validate(catalogInput)
	if err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%064x", 17)
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection: ref, IndexName: "embedding", IndexDefinitionDigest: digest,
		SourceGeneration: 11, SourceChecksum: 12, SourceSchemaHash: 13, SourceRowCount: 100,
		PartitionGeneration: 7, PartitionCount: uint32(len(owners)),
		Partitions: make([]raftplacement.VectorPartitionGroupV1, len(owners)),
	}
	manifest := collections.VectorPartitionManifestV1{
		Format: collections.VectorPartitionManifestFormatV1, State: "ready", Collection: ref.Collection,
		IndexName: placement.IndexName, IndexDefinitionDigest: digest,
		SourceGeneration: placement.SourceGeneration, SourceChecksum: placement.SourceChecksum,
		SourceSchemaHash: placement.SourceSchemaHash, SourceRowCount: placement.SourceRowCount,
		Generation: placement.PartitionGeneration, RouterGeneration: placement.PartitionGeneration,
		PartitionCount: placement.PartitionCount, ReadySetDigest: fmt.Sprintf("%064x", 23),
		Placements: make([]collections.VectorPartitionPlacementV1, len(owners)),
	}
	partitionScores := make([]collections.VectorPartitionRouterPartitionScoreV1, len(owners))
	for partition, groupID := range owners {
		placement.Partitions[partition] = raftplacement.VectorPartitionGroupV1{PartitionID: uint32(partition), GroupID: groupID}
		manifest.Placements[partition] = collections.VectorPartitionPlacementV1{PartitionID: uint32(partition), GroupID: string(groupID)}
		partitionScores[partition] = collections.VectorPartitionRouterPartitionScoreV1{
			PartitionID: uint32(partition), Distance: float64(partition) / 100,
		}
		for range neighbors[uint32(partition)] {
			manifest.Memberships = append(manifest.Memberships, collections.VectorPartitionMembershipV1{
				VectorOrdinal: uint64(len(manifest.Memberships)), PartitionID: uint32(partition),
			})
		}
	}
	router := &testVectorPartitionCoordinatorRouterV1{
		status: collections.VectorPartitionRouterRuntimeStatusV1{
			Manifest: manifest, ModelDigest: fmt.Sprintf("%064x", 29),
			Representatives: uint64(len(owners)), Partitions: uint64(len(owners)),
		},
		partitions: partitionScores,
	}
	source := &testVectorPartitionCoordinatorRouterSourceV1{router: router}
	dispatcher := &testVectorPartitionCoordinatorDispatcherV1{
		neighbors: neighbors, blockByGroup: make(map[raftcluster.GroupID]<-chan struct{}),
		notLeaderOnce: make(map[raftcluster.GroupID]raftcluster.NodeID),
	}
	coordinator, err := NewVectorPartitionCoordinatorV1(VectorPartitionCoordinatorOptionsV1{
		Catalog: catalog, Placement: placement, RouterSource: source, Dispatcher: dispatcher, Limits: limits, ShardLimits: shardLimits,
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, source, dispatcher
}

func testVectorPartitionCoordinatorRequestV1(partitions int) VectorPartitionCoordinatorRequestV1 {
	return VectorPartitionCoordinatorRequestV1{
		Version: VectorPartitionCoordinatorVersionV1, RequestID: "request-1", CancellationID: "cancel-1",
		Database: "db", Catalog: "default", Collection: "docs", IndexName: "embedding",
		IndexDefinitionDigest: fmt.Sprintf("%064x", 17),
		Query:                 []float32{1, 0}, Metric: VectorPartitionShardSearchMetricCosineV1,
		RouterMode:            collections.VectorPartitionRouterModeExactV1,
		RouterCandidateBudget: partitions, PartitionProbes: partitions,
		Consistency: VectorPartitionShardSearchConsistencySnapshotV1,
		TopK:        3, EfSearch: 8, StatsMode: VectorPartitionShardSearchStatsBasicV1,
		RequestBytesLimit: 4 << 20, CandidateBytesLimit: 64 << 20,
		ResponseBytesLimit: 64 << 20, MergeEntriesLimit: partitions * 3,
	}
}

func testVectorPartitionCoordinatorUseM5PreflightV1(
	coordinator *VectorPartitionCoordinatorV1,
	dispatcher *testVectorPartitionCoordinatorDispatcherV1,
) {
	service := &VectorPartitionShardSearchServiceV1{
		limits: DefaultVectorPartitionShardSearchLimitsV1(),
	}
	coordinator.dispatcher = VectorPartitionShardSearchDispatcherFuncV1(
		func(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
			if err := service.validateRequest(request); err != nil {
				return VectorPartitionShardSearchResponseV1{}, err
			}
			return dispatcher.DispatchVectorPartitionShardSearchV1(ctx, request)
		},
	)
}

func TestVectorPartitionCoordinatorCoalescesChunksDedupesAndMergesV1(t *testing.T) {
	const partitions = 33
	owners := make([]raftcluster.GroupID, partitions)
	neighbors := make(map[uint32][]VectorPartitionShardSearchNeighborV1, partitions)
	for i := range owners {
		owners[i] = "group-a"
		neighbors[uint32(i)] = []VectorPartitionShardSearchNeighborV1{
			{ID: fmt.Sprintf("doc-%02d", i), Score: .5},
			{ID: "shared", Score: float32(i) / 100},
		}
	}
	coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		owners, neighbors, VectorPartitionCoordinatorLimitsV1{},
	)
	response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(partitions))
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	if source.opens != 1 || source.router.closeCount != 1 {
		t.Fatalf("router lifecycle opens=%d closes=%d", source.opens, source.router.closeCount)
	}
	if response.Counters.SelectedPartitions != partitions || response.Counters.SelectedGroups != 1 ||
		response.Counters.Requests != 2 || response.Counters.RPCs != 2 ||
		response.Counters.Duplicates != partitions-1 || response.Counters.ScoreDisagreements != partitions-1 {
		t.Fatalf("counters=%+v", response.Counters)
	}
	sort.Slice(dispatcher.calls, func(i, j int) bool {
		return dispatcher.calls[i].RequestID < dispatcher.calls[j].RequestID
	})
	if len(dispatcher.calls) != 2 || len(dispatcher.calls[0].PartitionIDs) != 32 ||
		len(dispatcher.calls[1].PartitionIDs) != 1 {
		t.Fatalf("calls=%+v", dispatcher.calls)
	}
	var maxRequestBytes uint64
	for _, call := range dispatcher.calls {
		requestBytes, err := vectorPartitionCoordinatorShardRequestBytesV1(call)
		if err != nil {
			t.Fatal(err)
		}
		maxRequestBytes = max(maxRequestBytes, requestBytes)
	}
	if response.Counters.MaxShardRequestBytes != maxRequestBytes {
		t.Fatalf("max shard request bytes=%d want=%d", response.Counters.MaxShardRequestBytes, maxRequestBytes)
	}
	if response.Counters.MaxShardPartitions != 32 {
		t.Fatalf("max shard partitions=%d want=32", response.Counters.MaxShardPartitions)
	}
	if got := response.Neighbors; len(got) != 3 || got[0].ID != "doc-00" ||
		got[1].ID != "doc-01" || got[2].ID != "doc-02" {
		t.Fatalf("stable top-k=%+v", got)
	}
}

func TestVectorPartitionCoordinatorPlansWithConfiguredShardLimitsV1(t *testing.T) {
	shardLimits := DefaultVectorPartitionShardSearchLimitsV1()
	shardLimits.MaxPartitions = 1
	coordinator, _, dispatcher := testVectorPartitionCoordinatorWithShardLimitsV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a", "group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{
			0: {{ID: "doc-0", Score: 1}},
			1: {{ID: "doc-1", Score: .9}},
		},
		VectorPartitionCoordinatorLimitsV1{}, shardLimits,
	)
	response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(2))
	if err != nil {
		t.Fatal(err)
	}
	if coordinator.limits.MaxPartitionsPerRequest != 1 || response.Counters.MaxShardPartitions != 1 || len(dispatcher.calls) != 2 {
		t.Fatalf("effective coordinator limit=%d counters=%+v calls=%+v", coordinator.limits.MaxPartitionsPerRequest, response.Counters, dispatcher.calls)
	}
	for _, call := range dispatcher.calls {
		if len(call.PartitionIDs) != 1 || call.CandidateBytesLimit > shardLimits.MaxCandidateBytes || call.ResponseBytesLimit > shardLimits.MaxResponseBytes || call.RequestBytesLimit > shardLimits.MaxRequestBytes {
			t.Fatalf("custom shard-limit call=%+v", call)
		}
	}
}

func TestVectorPartitionCoordinatorAcceptsLimitsAboveShardDefaultsWhenConfiguredV1(t *testing.T) {
	shardLimits := DefaultVectorPartitionShardSearchLimitsV1()
	shardLimits.MaxPartitions = 64
	shardLimits.MaxTopK = 512
	shardLimits.MaxEfSearch = 8192
	shardLimits.MaxIdentityBytes = 8192
	shardLimits.MaxStableIDBytes = 8192
	shardLimits.MaxResponseBytes = 128 << 20
	limits := DefaultVectorPartitionCoordinatorLimitsV1()
	limits.MaxPartitionsPerRequest = shardLimits.MaxPartitions
	limits.MaxTopK = shardLimits.MaxTopK
	limits.MaxEfSearch = shardLimits.MaxEfSearch
	limits.MaxIdentityBytes = shardLimits.MaxIdentityBytes
	limits.MaxStableIDBytes = shardLimits.MaxStableIDBytes
	limits.MaxResponseBytes = shardLimits.MaxResponseBytes

	coordinator, _, _ := testVectorPartitionCoordinatorWithShardLimitsV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}},
		limits, shardLimits,
	)
	if coordinator.limits.MaxPartitionsPerRequest != shardLimits.MaxPartitions ||
		coordinator.limits.MaxTopK != shardLimits.MaxTopK ||
		coordinator.limits.MaxEfSearch != shardLimits.MaxEfSearch ||
		coordinator.limits.MaxIdentityBytes != shardLimits.MaxIdentityBytes ||
		coordinator.limits.MaxStableIDBytes != shardLimits.MaxStableIDBytes ||
		coordinator.limits.MaxResponseBytes != shardLimits.MaxResponseBytes {
		t.Fatalf("effective coordinator limits=%+v want custom shard limits=%+v", coordinator.limits, shardLimits)
	}
}

func TestVectorPartitionCoordinatorRejectsQueryAboveConfiguredShardDimensionsV1(t *testing.T) {
	shardLimits := DefaultVectorPartitionShardSearchLimitsV1()
	shardLimits.MaxDimensions = 1
	coordinator, _, dispatcher := testVectorPartitionCoordinatorWithShardLimitsV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}},
		VectorPartitionCoordinatorLimitsV1{}, shardLimits,
	)
	if coordinator.limits.MaxQueryBytes != 4 {
		t.Fatalf("effective query bytes=%d want=4", coordinator.limits.MaxQueryBytes)
	}
	if _, err := coordinator.Search(t.Context(), testVectorPartitionCoordinatorRequestV1(1)); !errors.Is(err, ErrVectorPartitionCoordinatorInvalidRequest) {
		t.Fatalf("two-dimensional query err=%v want invalid request", err)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("invalid query dispatched shard calls=%+v", dispatcher.calls)
	}
}

func TestVectorPartitionCoordinatorAllowsBoundedAggregateCandidateLimitAboveShardDefaultV1(t *testing.T) {
	limits := DefaultVectorPartitionCoordinatorLimitsV1()
	limits.MaxCandidateBytes = 128 << 20
	got, err := normalizeVectorPartitionCoordinatorLimitsV1(limits)
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxCandidateBytes != 128<<20 {
		t.Fatalf("aggregate candidate limit=%d", got.MaxCandidateBytes)
	}
}

func TestVectorPartitionCoordinatorUsesReplicatedLifecycleAdmissionV1(t *testing.T) {
	owners := []raftcluster.GroupID{"group-a"}
	coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		owners, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	replicatedReadySetDigest := strings.Repeat("c", 64)
	authority := &recordingVectorPartitionReplicatedLifecycleAuthorityV1{readySetDigest: replicatedReadySetDigest}
	coordinator.replicatedLifecycle = authority
	response, err := coordinator.Search(t.Context(), testVectorPartitionCoordinatorRequestV1(len(owners)))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if authority.calls != 1 {
		t.Fatalf("replicated lifecycle calls=%d want 1", authority.calls)
	}
	if len(dispatcher.calls) == 0 {
		t.Fatal("admitted search did not dispatch shard request")
	}
	if !reflect.DeepEqual(source.generations, []uint64{coordinator.placement.PartitionGeneration}) {
		t.Fatalf("router generations=%v want exact placement generation %d", source.generations, coordinator.placement.PartitionGeneration)
	}
	if response.ReadySetDigest != replicatedReadySetDigest || dispatcher.calls[0].ReadySetDigest != replicatedReadySetDigest {
		t.Fatalf("replicated ready-set response/request=%q/%q want %q", response.ReadySetDigest, dispatcher.calls[0].ReadySetDigest, replicatedReadySetDigest)
	}

	authority.err = errors.Join(raftplacement.ErrVectorPartitionLifecycleGuard, errors.New("generation invalidated"))
	before := len(dispatcher.calls)
	if _, err := coordinator.Search(t.Context(), testVectorPartitionCoordinatorRequestV1(len(owners))); !errors.Is(err, authority.err) {
		t.Fatalf("invalidated search err=%v", err)
	}
	if authority.calls != 2 || source.openCount() != 1 {
		t.Fatalf("per-request lifecycle calls=%d router opens=%d", authority.calls, source.openCount())
	}
	if len(dispatcher.calls) != before {
		t.Fatal("replicated lifecycle rejection dispatched shard request")
	}
	if source.router.closeCount != 1 {
		t.Fatalf("invalidated router closes=%d want one", source.router.closeCount)
	}
	stats := coordinator.Stats()
	if len(stats.RouterSessions) != 1 || stats.RouterSessions[0].Invalidations != 1 ||
		stats.RouterSessions[0].ReaderPins != 1 || stats.RouterSessions[0].ReaderReleases != 1 ||
		stats.RouterSessions[0].Hits != 1 {
		t.Fatalf("invalidated session stats=%+v", stats.RouterSessions)
	}
	if err := coordinator.Close(); err != nil || source.router.closeCount != 1 {
		t.Fatalf("post-invalidation close err=%v router closes=%d", err, source.router.closeCount)
	}
}

func TestVectorPartitionCoordinatorReplicatedLifecycleCancellationPreservesWarmSessionV1(t *testing.T) {
	for _, lifecycleErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(lifecycleErr.Error(), func(t *testing.T) {
			owners := []raftcluster.GroupID{"group-a"}
			coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
				[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
				owners, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
			)
			authority := &recordingVectorPartitionReplicatedLifecycleAuthorityV1{readySetDigest: strings.Repeat("c", 64)}
			coordinator.replicatedLifecycle = authority
			if _, err := coordinator.Search(t.Context(), testVectorPartitionCoordinatorRequestV1(len(owners))); err != nil {
				t.Fatalf("warm Search: %v", err)
			}

			authority.err = lifecycleErr
			before := len(dispatcher.calls)
			if _, err := coordinator.Search(t.Context(), testVectorPartitionCoordinatorRequestV1(len(owners))); !errors.Is(err, lifecycleErr) {
				t.Fatalf("lifecycle Search err=%v want %v", err, lifecycleErr)
			}
			if len(dispatcher.calls) != before || source.openCount() != 1 || source.router.closeCount != 0 {
				t.Fatalf("lifecycle rejection dispatches=%d opens=%d closes=%d", len(dispatcher.calls)-before, source.openCount(), source.router.closeCount)
			}
			stats := coordinator.Stats()
			if len(stats.RouterSessions) != 1 || stats.RouterSessions[0].Invalidations != 0 ||
				stats.RouterSessions[0].ReaderPins != 1 || stats.RouterSessions[0].ReaderReleases != 0 || stats.RouterSessions[0].Hits != 1 {
				t.Fatalf("preserved session stats=%+v", stats.RouterSessions)
			}

			authority.err = nil
			if _, err := coordinator.Search(t.Context(), testVectorPartitionCoordinatorRequestV1(len(owners))); err != nil {
				t.Fatalf("healthy reuse Search: %v", err)
			}
			stats = coordinator.Stats()
			if source.openCount() != 1 || source.router.closeCount != 0 || len(stats.RouterSessions) != 1 || stats.RouterSessions[0].Hits != 2 {
				t.Fatalf("healthy reuse opens=%d closes=%d sessions=%+v", source.openCount(), source.router.closeCount, stats.RouterSessions)
			}
			if err := coordinator.Close(); err != nil || source.router.closeCount != 1 {
				t.Fatalf("Close err=%v closes=%d", err, source.router.closeCount)
			}
		})
	}
}

func TestVectorPartitionCoordinatorRouterSessionConcurrentReuseAccountingV1(t *testing.T) {
	coordinator, source, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	const requests = 16
	start := make(chan struct{})
	errs := make(chan error, requests)
	for i := 0; i < requests; i++ {
		go func(i int) {
			<-start
			request := testVectorPartitionCoordinatorRequestV1(1)
			request.RequestID = fmt.Sprintf("concurrent-%d", i)
			request.CancellationID = request.RequestID + "-cancel"
			_, err := coordinator.Search(t.Context(), request)
			errs <- err
		}(i)
	}
	close(start)
	for range requests {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if source.openCount() != 1 {
		t.Fatalf("cold opens=%d want one", source.openCount())
	}
	stats := coordinator.Stats()
	if len(stats.RouterSessions) != 1 {
		t.Fatalf("session stats=%+v", stats.RouterSessions)
	}
	session := stats.RouterSessions[0]
	if session.ColdOpens != 1 || session.ManifestOpenAttempts != 1 || session.Misses != 1 || session.Hits != requests-1 ||
		session.ReaderPins != 1 || session.ReaderReleases != 0 ||
		session.LeasePins != requests || session.LeaseReleases != requests || session.Invalidations != 0 {
		t.Fatalf("session=%+v", session)
	}
	identity := session.Identity
	if identity.Database != "db" || identity.Catalog != "default" || identity.Collection != "docs" ||
		identity.IndexName != "embedding" || identity.IndexDefinitionDigest == "" ||
		identity.SourceGeneration != coordinator.placement.SourceGeneration || identity.SourceChecksum != coordinator.placement.SourceChecksum ||
		identity.SourceSchemaHash != coordinator.placement.SourceSchemaHash || identity.SourceRowCount != coordinator.placement.SourceRowCount ||
		identity.PartitionGeneration != coordinator.placement.PartitionGeneration || identity.ReadySetDigest == "" || identity.RouterModelDigest == "" {
		t.Fatalf("identity=%+v", identity)
	}
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	session = coordinator.Stats().RouterSessions[0]
	if source.router.closeCount != 1 || session.Closes != 1 || session.ReaderReleases != 1 {
		t.Fatalf("post-close router closes=%d session=%+v", source.router.closeCount, session)
	}
}

func TestVectorPartitionCoordinatorReplacementDrainsOldEpochV1(t *testing.T) {
	old, oldSource, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "old", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	if _, err := old.Search(t.Context(), testVectorPartitionCoordinatorRequestV1(1)); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if oldSource.openCount() != 1 || oldSource.router.closeCount != 1 {
		t.Fatalf("old epoch opens=%d closes=%d", oldSource.openCount(), oldSource.router.closeCount)
	}

	replacement, replacementSource, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "new", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	if _, err := replacement.Search(t.Context(), testVectorPartitionCoordinatorRequestV1(1)); err != nil {
		t.Fatal(err)
	}
	if replacementSource.openCount() != 1 || oldSource.openCount() != 1 {
		t.Fatalf("replacement reused old epoch: replacement=%d old=%d", replacementSource.openCount(), oldSource.openCount())
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	if replacementSource.router.closeCount != 1 {
		t.Fatalf("replacement closes=%d", replacementSource.router.closeCount)
	}
}

func TestVectorPartitionCoordinatorRouterOpenCorruptionFailsClosedV1(t *testing.T) {
	coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	source.openErr = errors.New("router asset checksum corrupt")
	response, err := coordinator.Search(t.Context(), testVectorPartitionCoordinatorRequestV1(1))
	if err == nil || !vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if len(dispatcher.calls) != 0 || source.router.closeCount != 0 {
		t.Fatalf("dispatch=%d closes=%d", len(dispatcher.calls), source.router.closeCount)
	}
	stats := coordinator.Stats()
	if len(stats.RouterSessions) != 1 || stats.RouterSessions[0].ColdOpens != 1 || stats.RouterSessions[0].ManifestOpenAttempts != 1 || stats.RouterSessions[0].Misses != 1 || stats.RouterSessions[0].OpenFailures != 1 || stats.RouterSessions[0].ReaderPins != 0 {
		t.Fatalf("session stats=%+v", stats.RouterSessions)
	}
}

func TestVectorPartitionCoordinatorRouterBudgetRejectionKeepsHealthySessionV1(t *testing.T) {
	coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a", "group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{
			0: {{ID: "a", Score: 1}},
			1: {{ID: "b", Score: .5}},
		},
		VectorPartitionCoordinatorLimitsV1{},
	)
	underBudget := testVectorPartitionCoordinatorRequestV1(1)
	underBudget.RouterCandidateBudget = 1
	for i := range 2 {
		underBudget.RequestID = fmt.Sprintf("under-budget-%d", i)
		underBudget.CancellationID = underBudget.RequestID + "-cancel"
		response, err := coordinator.Search(context.Background(), underBudget)
		if !errors.Is(err, ErrVectorPartitionCoordinatorBudgetExceeded) ||
			!vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
			t.Fatalf("under-budget response=%+v err=%v", response, err)
		}
	}
	if source.openCount() != 1 || source.router.closeCount != 0 || len(dispatcher.calls) != 0 {
		t.Fatalf("under-budget opens=%d closes=%d dispatches=%d", source.openCount(), source.router.closeCount, len(dispatcher.calls))
	}
	sessions := coordinator.Stats().RouterSessions
	if len(sessions) != 1 || sessions[0].ColdOpens != 1 || sessions[0].Hits != 1 ||
		sessions[0].Invalidations != 0 || sessions[0].ReaderPins != 1 || sessions[0].ReaderReleases != 0 ||
		sessions[0].LeasePins != 2 || sessions[0].LeaseReleases != 2 || sessions[0].Closes != 0 {
		t.Fatalf("under-budget session stats=%+v", sessions)
	}

	healthy := testVectorPartitionCoordinatorRequestV1(2)
	healthy.RequestID = "healthy-after-budget-rejection"
	healthy.CancellationID = healthy.RequestID + "-cancel"
	if _, err := coordinator.Search(context.Background(), healthy); err != nil {
		t.Fatalf("healthy reuse: %v", err)
	}
	if source.openCount() != 1 || source.router.closeCount != 0 || len(dispatcher.calls) != 1 {
		t.Fatalf("healthy reuse opens=%d closes=%d dispatches=%d", source.openCount(), source.router.closeCount, len(dispatcher.calls))
	}
	sessions = coordinator.Stats().RouterSessions
	if len(sessions) != 1 || sessions[0].Hits != 2 || sessions[0].LeasePins != 3 ||
		sessions[0].LeaseReleases != 3 || sessions[0].Invalidations != 0 {
		t.Fatalf("healthy reuse session stats=%+v", sessions)
	}
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	sessions = coordinator.Stats().RouterSessions
	if source.router.closeCount != 1 || sessions[0].ReaderReleases != 1 || sessions[0].Closes != 1 {
		t.Fatalf("post-close router closes=%d sessions=%+v", source.router.closeCount, sessions)
	}
}

func TestVectorPartitionCoordinatorRouterBudgetRejectionStillRunsLifecycleAdmissionV1(t *testing.T) {
	coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a", "group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{
			0: {{ID: "a", Score: 1}},
			1: {{ID: "b", Score: .5}},
		},
		VectorPartitionCoordinatorLimitsV1{},
	)
	invalidated := errors.Join(raftplacement.ErrVectorPartitionLifecycleGuard, errors.New("generation invalidated"))
	authority := &recordingVectorPartitionReplicatedLifecycleAuthorityV1{
		readySetDigest: strings.Repeat("c", 64),
		err:            invalidated,
	}
	coordinator.replicatedLifecycle = authority
	underBudget := testVectorPartitionCoordinatorRequestV1(1)
	underBudget.RouterCandidateBudget = 1

	response, err := coordinator.Search(t.Context(), underBudget)
	if !errors.Is(err, invalidated) || !vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
		t.Fatalf("under-budget invalidated response=%+v err=%v", response, err)
	}
	if authority.calls != 1 || source.openCount() != 1 || source.router.closeCount != 1 || len(dispatcher.calls) != 0 {
		t.Fatalf("lifecycle calls=%d opens=%d closes=%d dispatches=%d", authority.calls, source.openCount(), source.router.closeCount, len(dispatcher.calls))
	}
	sessions := coordinator.Stats().RouterSessions
	if len(sessions) != 1 || sessions[0].ColdOpens != 1 || sessions[0].Invalidations != 1 ||
		sessions[0].ReaderPins != 1 || sessions[0].ReaderReleases != 1 ||
		sessions[0].LeasePins != 1 || sessions[0].LeaseReleases != 1 || sessions[0].Closes != 1 {
		t.Fatalf("invalidated session stats=%+v", sessions)
	}
	if err := coordinator.Close(); err != nil || source.router.closeCount != 1 {
		t.Fatalf("Close err=%v closes=%d", err, source.router.closeCount)
	}
}

func TestVectorPartitionCoordinatorColdOpenRouterCloseErrorIsRetainedV1(t *testing.T) {
	coordinator, source, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	sentinel := errors.New("retired router close failed")
	source.router.closeErr = sentinel
	source.router.status.Manifest.SourceChecksum++ // fail cold-open status validation
	response, err := coordinator.Search(t.Context(), testVectorPartitionCoordinatorRequestV1(1))
	if !errors.Is(err, sentinel) || !errors.Is(err, ErrVectorPartitionCoordinatorGenerationMismatch) || !vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if err := coordinator.Close(); !errors.Is(err, sentinel) {
		t.Fatalf("Coordinator.Close err=%v want retained %v", err, sentinel)
	}
}

func TestVectorPartitionCoordinatorRetainsFirstRouterCloseErrorV1(t *testing.T) {
	coordinator, _, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	first, second := errors.New("first router close failed"), errors.New("second router close failed")
	coordinator.sessionMu.Lock()
	coordinator.closing = 2
	coordinator.sessionMu.Unlock()
	if err := coordinator.closeRouterSessionV1(nil, &testVectorPartitionCoordinatorRouterV1{closeErr: first}); !errors.Is(err, first) {
		t.Fatalf("first close err=%v", err)
	}
	if err := coordinator.closeRouterSessionV1(nil, &testVectorPartitionCoordinatorRouterV1{closeErr: second}); !errors.Is(err, second) {
		t.Fatalf("second close err=%v", err)
	}
	if !errors.Is(coordinator.routerCloseErr, first) || errors.Is(coordinator.routerCloseErr, second) {
		t.Fatalf("retained close err=%v", coordinator.routerCloseErr)
	}
}

func TestVectorPartitionCoordinatorRetirementAllowsDistinctReplacementOpenV1(t *testing.T) {
	coordinator, source, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	oldRouter := source.router
	newRouter := &testVectorPartitionCoordinatorRouterV1{status: oldRouter.status, partitions: slices.Clone(oldRouter.partitions)}
	newRouter.status.Manifest.Memberships = slices.Clone(oldRouter.status.Manifest.Memberships)
	newRouter.status.Manifest.Memberships = append(newRouter.status.Manifest.Memberships, collections.VectorPartitionMembershipV1{PartitionID: 0})
	closeStarted, closeBlock := make(chan struct{}, 1), make(chan struct{})
	oldRouter.closeStart, oldRouter.closeBlock = closeStarted, closeBlock
	source.openRouters = []VectorPartitionCoordinatorRouterV1{oldRouter, newRouter}

	oldLease, err := coordinator.acquireRouterSessionV1(t.Context(), "embedding", coordinator.placement.PartitionGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.recordRouterSessionIdentityV1(oldLease, oldRouter.status, "ready-old"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(oldLease.session.partitionRows, []uint64{1}) {
		t.Fatalf("old partition rows=%v", oldLease.session.partitionRows)
	}
	coordinator.retireRouterSessionV1(oldLease)
	oldClosed := make(chan error, 1)
	go func() { oldClosed <- oldLease.Close() }()
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("old router close did not begin")
	}

	newLease, err := coordinator.acquireRouterSessionV1(t.Context(), "embedding", coordinator.placement.PartitionGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if newLease.session.router != newRouter || source.openCount() != 2 {
		t.Fatalf("replacement router=%p opens=%d", newLease.session.router, source.openCount())
	}
	if !slices.Equal(newLease.session.partitionRows, []uint64{2}) {
		t.Fatalf("replacement partition rows=%v", newLease.session.partitionRows)
	}
	newStatus := newRouter.status
	newStatus.ModelDigest = "new-model"
	if err := coordinator.recordRouterSessionIdentityV1(newLease, newStatus, "ready-new"); !errors.Is(err, ErrVectorPartitionCoordinatorGenerationMismatch) {
		t.Fatalf("changed identity err=%v", err)
	}
	if err := coordinator.recordRouterSessionIdentityV1(newLease, newRouter.status, "ready-old"); err != nil {
		t.Fatal(err)
	}
	close(closeBlock)
	if err := <-oldClosed; err != nil {
		t.Fatal(err)
	}
	if err := newLease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	if oldRouter.closeCount != 1 || newRouter.closeCount != 1 {
		t.Fatalf("old closes=%d new closes=%d", oldRouter.closeCount, newRouter.closeCount)
	}
	sessions := coordinator.Stats().RouterSessions
	if len(sessions) != 1 || sessions[0].Identity.ReadySetDigest != "ready-old" || sessions[0].Identity.RouterModelDigest != oldRouter.status.ModelDigest ||
		sessions[0].ColdOpens != 2 || sessions[0].ManifestOpenAttempts != 2 || sessions[0].Misses != 2 ||
		sessions[0].ReaderPins != 2 || sessions[0].ReaderReleases != 2 || sessions[0].LeasePins != 2 || sessions[0].LeaseReleases != 2 ||
		sessions[0].Invalidations != 1 || sessions[0].Closes != 2 {
		t.Fatalf("session stats=%+v", sessions)
	}
}

func TestVectorPartitionCoordinatorRouterSessionSingleflightAndCancellationV1(t *testing.T) {
	coordinator, source, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	started := make(chan struct{}, 1)
	block := make(chan struct{})
	source.openStarted, source.openBlock = started, block
	first := make(chan *vectorPartitionCoordinatorRouterLeaseV1, 1)
	firstErr := make(chan error, 1)
	go func() {
		lease, err := coordinator.acquireRouterSessionV1(context.Background(), "embedding", coordinator.placement.PartitionGeneration)
		if err != nil {
			firstErr <- err
			return
		}
		first <- lease
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("cold router open did not start")
	}
	canceled, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := coordinator.acquireRouterSessionV1(canceled, "embedding", coordinator.placement.PartitionGeneration); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting acquire err=%v want deadline exceeded", err)
	}
	if source.openCount() != 1 {
		t.Fatalf("cold opens=%d want one", source.openCount())
	}
	close(block)
	var lease *vectorPartitionCoordinatorRouterLeaseV1
	select {
	case err := <-firstErr:
		t.Fatal(err)
	case lease = <-first:
	case <-time.After(time.Second):
		t.Fatal("cold router open did not complete")
	}
	lease.Close()
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	if source.router.closeCount != 1 {
		t.Fatalf("router closes=%d want one", source.router.closeCount)
	}
}

func TestVectorPartitionCoordinatorRouterSessionSharesColdOpenFailureV1(t *testing.T) {
	coordinator, source, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	started := make(chan struct{}, 1)
	block := make(chan struct{})
	sentinel := errors.New("router asset checksum corrupt")
	source.openStarted, source.openBlock, source.openErr = started, block, sentinel

	const requests = 16
	start := make(chan struct{})
	errs := make(chan error, requests)
	for range requests {
		go func() {
			<-start
			_, err := coordinator.acquireRouterSessionV1(context.Background(), "embedding", coordinator.placement.PartitionGeneration)
			errs <- err
		}()
	}
	close(start)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("cold router open did not start")
	}
	select {
	case err := <-errs:
		t.Fatalf("acquire returned before cold open completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(block)
	for range requests {
		if err := <-errs; !errors.Is(err, sentinel) {
			t.Fatalf("shared cold-open err=%v want %v", err, sentinel)
		}
	}
	if source.openCount() != 1 {
		t.Fatalf("cold opens=%d want one shared failure", source.openCount())
	}
	sessions := coordinator.Stats().RouterSessions
	if len(sessions) != 1 || sessions[0].ColdOpens != 1 || sessions[0].ManifestOpenAttempts != 1 ||
		sessions[0].Misses != 1 || sessions[0].OpenFailures != 1 || sessions[0].ReaderPins != 0 {
		t.Fatalf("shared failure stats=%+v", sessions)
	}
}

func TestVectorPartitionCoordinatorRouterSessionRetriesCanceledColdOpenV1(t *testing.T) {
	coordinator, source, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	started := make(chan struct{}, 2)
	block := make(chan struct{})
	source.openStarted, source.openBlock = started, block

	initiatorCtx, cancelInitiator := context.WithCancel(context.Background())
	initiatorErr := make(chan error, 1)
	go func() {
		_, err := coordinator.acquireRouterSessionV1(initiatorCtx, "embedding", coordinator.placement.PartitionGeneration)
		initiatorErr <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("initiating cold router open did not start")
	}
	waiter := make(chan *vectorPartitionCoordinatorRouterLeaseV1, 1)
	waiterErr := make(chan error, 1)
	go func() {
		lease, err := coordinator.acquireRouterSessionV1(context.Background(), "embedding", coordinator.placement.PartitionGeneration)
		if err != nil {
			waiterErr <- err
			return
		}
		waiter <- lease
	}()
	select {
	case err := <-waiterErr:
		t.Fatalf("waiter returned before initiating cancellation: %v", err)
	case <-waiter:
		t.Fatal("waiter acquired before initiating cancellation")
	case <-time.After(20 * time.Millisecond):
	}

	cancelInitiator()
	select {
	case err := <-initiatorErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("initiating cold-open err=%v want canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("initiating cold open did not cancel")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("waiter did not retry canceled cold open")
	}
	close(block)
	var lease *vectorPartitionCoordinatorRouterLeaseV1
	select {
	case err := <-waiterErr:
		t.Fatal(err)
	case lease = <-waiter:
	case <-time.After(time.Second):
		t.Fatal("waiter retry did not complete")
	}
	lease.Close()
	if source.openCount() != 2 {
		t.Fatalf("cold opens=%d want canceled attempt plus waiter retry", source.openCount())
	}
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVectorPartitionCoordinatorCloseDrainsRouterLeasesAndRejectsNewSearchesV1(t *testing.T) {
	coordinator, source, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	lease, err := coordinator.acquireRouterSessionV1(context.Background(), "embedding", coordinator.placement.PartitionGeneration)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- coordinator.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before lease drain: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	lease.Close()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not drain router lease")
	}
	if source.router.closeCount != 1 {
		t.Fatalf("router closes=%d want one", source.router.closeCount)
	}
	if _, err := coordinator.acquireRouterSessionV1(context.Background(), "embedding", coordinator.placement.PartitionGeneration); !errors.Is(err, ErrVectorPartitionCoordinatorUnavailable) {
		t.Fatalf("acquire after Close err=%v", err)
	}
}

func TestVectorPartitionCoordinatorCloseDrainsRetiredRouterLeaseV1(t *testing.T) {
	coordinator, source, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	lease, err := coordinator.acquireRouterSessionV1(context.Background(), "embedding", coordinator.placement.PartitionGeneration)
	if err != nil {
		t.Fatal(err)
	}
	coordinator.retireRouterSessionV1(lease)
	closed := make(chan error, 1)
	go func() { closed <- coordinator.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before retired lease drain: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	lease.Close()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not drain retired lease")
	}
	if source.router.closeCount != 1 {
		t.Fatalf("router closes=%d want one", source.router.closeCount)
	}
}

func TestVectorPartitionCoordinatorCloseDrainsColdOpenV1(t *testing.T) {
	coordinator, source, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "doc", Score: 1}}}, VectorPartitionCoordinatorLimitsV1{},
	)
	started := make(chan struct{}, 1)
	block := make(chan struct{})
	source.openStarted, source.openBlock = started, block
	closeErr := errors.New("cold-open router close failed")
	source.router.closeErr = closeErr
	opened := make(chan error, 1)
	go func() {
		_, err := coordinator.acquireRouterSessionV1(context.Background(), "embedding", coordinator.placement.PartitionGeneration)
		opened <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("cold router open did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- coordinator.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before cold open drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(block)
	select {
	case err := <-opened:
		if !errors.Is(err, ErrVectorPartitionCoordinatorUnavailable) || !errors.Is(err, closeErr) {
			t.Fatalf("cold acquire after Close err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cold acquire did not return")
	}
	select {
	case err := <-closed:
		if !errors.Is(err, closeErr) {
			t.Fatalf("Close err=%v want retained %v", err, closeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not drain cold open")
	}
	if source.router.closeCount != 1 {
		t.Fatalf("router closes=%d want one", source.router.closeCount)
	}
	sessions := coordinator.Stats().RouterSessions
	if len(sessions) != 1 || sessions[0].OpenFailures != 1 || sessions[0].ReaderPins != 1 ||
		sessions[0].ReaderReleases != 1 || sessions[0].Closes != 1 {
		t.Fatalf("cold-open close stats=%+v", sessions)
	}
}

func TestVectorPartitionCoordinatorRequiresReplicatedLifecycleV1(t *testing.T) {
	_, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"}, map[uint32][]VectorPartitionShardSearchNeighborV1{0: nil}, VectorPartitionCoordinatorLimitsV1{},
	)
	ref := raftplacement.CollectionRefV1{Database: "db", Catalog: "default", Collection: "docs"}
	catalog, err := raftplacement.Validate(raftplacement.CatalogV1{Groups: []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}}, Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewVectorPartitionCoordinatorV1(VectorPartitionCoordinatorOptionsV1{Catalog: catalog, Placement: raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: "embedding", IndexDefinitionDigest: fmt.Sprintf("%064x", 17), SourceGeneration: 11, SourceChecksum: 12, SourceSchemaHash: 13, SourceRowCount: 100, PartitionGeneration: 7, PartitionCount: 1, Partitions: []raftplacement.VectorPartitionGroupV1{{GroupID: "group-a"}}}, RouterSource: source, Dispatcher: dispatcher, RequireReplicatedLifecycle: true})
	if !errors.Is(err, ErrVectorPartitionCoordinatorUnavailable) {
		t.Fatalf("missing M7 authority err=%v", err)
	}
}

func TestVectorPartitionCoordinatorAllPartitionParityAndStableTiesV1(t *testing.T) {
	owners := []raftcluster.GroupID{"group-b", "group-a", "group-b", "group-a"}
	neighbors := map[uint32][]VectorPartitionShardSearchNeighborV1{
		0: {{ID: "b", Score: .9}, {ID: "z", Score: .8}, {ID: "dup", Score: .7}},
		1: {{ID: "dup", Score: .95}, {ID: "a", Score: .9}},
		2: {},
		3: {{ID: "c", Score: .9}, {ID: "tail", Score: .1}},
	}
	coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"},
		},
		owners, neighbors, VectorPartitionCoordinatorLimitsV1{MaxConcurrentRequests: 1},
	)
	response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(len(owners)))
	if err != nil {
		t.Fatal(err)
	}
	want := []VectorPartitionCoordinatorNeighborV1{{ID: "dup", Score: .95}, {ID: "a", Score: .9}, {ID: "b", Score: .9}}
	if !slices.Equal(response.Neighbors, want) {
		t.Fatalf("neighbors=%+v want=%+v", response.Neighbors, want)
	}
	if dispatcher.maximum != 1 || len(dispatcher.calls) != 2 ||
		dispatcher.calls[0].TargetGroupID != "group-a" || dispatcher.calls[1].TargetGroupID != "group-b" {
		t.Fatalf("fanout max=%d calls=%+v", dispatcher.maximum, dispatcher.calls)
	}
	if !slices.Equal(response.ProbedPartitions, []uint32{0, 1, 2, 3}) ||
		!slices.Equal(response.ProbedGroups, []raftcluster.GroupID{"group-a", "group-b"}) {
		t.Fatalf("probed partitions=%v groups=%v", response.ProbedPartitions, response.ProbedGroups)
	}
}

func TestVectorPartitionCoordinatorTopKLargerThanLiveCorpusV1(t *testing.T) {
	coordinator, _, _ := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"},
		},
		[]raftcluster.GroupID{"group-a", "group-b"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{
			0: {{ID: "only-live-document", Score: .75}},
			1: {},
		},
		VectorPartitionCoordinatorLimitsV1{},
	)
	request := testVectorPartitionCoordinatorRequestV1(2)
	request.TopK = 5
	request.MergeEntriesLimit = 10
	response, err := coordinator.Search(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(response.Neighbors, []VectorPartitionCoordinatorNeighborV1{
		{ID: "only-live-document", Score: .75},
	}) {
		t.Fatalf("neighbors=%+v", response.Neighbors)
	}
}

func TestVectorPartitionCoordinatorRejectsImpossibleRouterFanoutBeforeOpenV1(t *testing.T) {
	tests := []struct {
		name string
		edit func(*VectorPartitionCoordinatorRequestV1)
	}{
		{
			name: "partition_probes_above_topology",
			edit: func(request *VectorPartitionCoordinatorRequestV1) {
				request.PartitionProbes = 3
				request.RouterCandidateBudget = 3
				request.MergeEntriesLimit = 9
			},
		},
		{
			name: "approximate_candidates_below_probes",
			edit: func(request *VectorPartitionCoordinatorRequestV1) {
				request.RouterMode = collections.VectorPartitionRouterModeApproxV1
				request.RouterCandidateBudget = 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
				[]raftplacement.GroupV1{
					{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"},
					{ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"},
				},
				[]raftcluster.GroupID{"group-a", "group-b"},
				map[uint32][]VectorPartitionShardSearchNeighborV1{
					0: {{ID: "a", Score: 1}},
					1: {{ID: "b", Score: .5}},
				},
				VectorPartitionCoordinatorLimitsV1{},
			)
			request := testVectorPartitionCoordinatorRequestV1(2)
			test.edit(&request)

			response, err := coordinator.Search(context.Background(), request)
			var coordinatorErr *VectorPartitionCoordinatorErrorV1
			if !errors.Is(err, ErrVectorPartitionCoordinatorInvalidRequest) ||
				!errors.As(err, &coordinatorErr) ||
				coordinatorErr.Code != VectorPartitionCoordinatorErrorInvalidRequestV1 ||
				!vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
				t.Fatalf("response=%+v err=%+v", response, err)
			}
			if source.opens != 0 || source.router.closeCount != 0 || len(dispatcher.calls) != 0 {
				t.Fatalf("router opens=%d closes=%d dispatches=%d", source.opens, source.router.closeCount, len(dispatcher.calls))
			}
		})
	}
}

func TestVectorPartitionCoordinatorRejectsUnrepresentableQueryNormBeforeOpenV1(t *testing.T) {
	tests := []struct {
		name  string
		query []float32
	}{
		{name: "inverse_norm_above_float32", query: []float32{math.SmallestNonzeroFloat32}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
				[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
				[]raftcluster.GroupID{"group-a"},
				map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
				VectorPartitionCoordinatorLimitsV1{},
			)
			request := testVectorPartitionCoordinatorRequestV1(1)
			request.Query = test.query

			response, err := coordinator.Search(context.Background(), request)
			var coordinatorErr *VectorPartitionCoordinatorErrorV1
			if !errors.Is(err, ErrVectorPartitionCoordinatorInvalidRequest) ||
				!errors.As(err, &coordinatorErr) ||
				coordinatorErr.Code != VectorPartitionCoordinatorErrorInvalidRequestV1 ||
				!vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
				t.Fatalf("response=%+v err=%+v", response, err)
			}
			if source.opens != 0 || source.router.closeCount != 0 || len(dispatcher.calls) != 0 {
				t.Fatalf("router opens=%d closes=%d dispatches=%d", source.opens, source.router.closeCount, len(dispatcher.calls))
			}
		})
	}
}

func TestVectorPartitionCoordinatorWeightsCandidateBudgetByMembershipV1(t *testing.T) {
	coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"},
		},
		[]raftcluster.GroupID{"group-a", "group-b"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{
			0: {{ID: "a", Score: 1}},
			1: {{ID: "b", Score: .5}},
		},
		VectorPartitionCoordinatorLimitsV1{},
	)
	source.router.status.Manifest.Memberships = nil
	dispatcher.editResponse = func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
		for i := range response.Partials {
			response.Partials[i].SearchRoute = collections.VectorPartitionSearchRouteHNSWSearchPackV1
		}
	}
	for i := range 100 {
		partitionID := uint32(0)
		if i >= 90 {
			partitionID = 1
		}
		source.router.status.Manifest.Memberships = append(
			source.router.status.Manifest.Memberships,
			collections.VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: partitionID},
		)
	}
	request := testVectorPartitionCoordinatorRequestV1(2)
	floors, floorTotal, err := vectorPartitionCoordinatorCandidateFloorsV1(
		[]uint64{90, 10}, []uint32{0, 1}, len(request.Query), request.TopK, request.EfSearch,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.CandidateBytesLimit = floorTotal + 100*64

	response, err := coordinator.Search(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Neighbors) != 2 || len(dispatcher.calls) != 2 {
		t.Fatalf("response=%+v calls=%+v", response, dispatcher.calls)
	}
	budgets := make(map[uint32]uint64, len(dispatcher.calls))
	var total uint64
	for _, call := range dispatcher.calls {
		if len(call.PartitionIDs) != 1 {
			t.Fatalf("partition ids=%v", call.PartitionIDs)
		}
		budgets[call.PartitionIDs[0]] = call.CandidateBytesLimit
		total += call.CandidateBytesLimit
	}
	if budgets[0] != floors[0]+90*64 || budgets[1] != floors[1]+10*64 || total != request.CandidateBytesLimit {
		t.Fatalf("candidate budgets=%v total=%d", budgets, total)
	}
}

func TestVectorPartitionCoordinatorReservesCandidateBaselineBeforeUnevenSurplusV1(t *testing.T) {
	coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"},
		},
		[]raftcluster.GroupID{"group-a", "group-b"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{
			0: {{ID: "a", Score: 1}},
			1: {{ID: "b", Score: .5}},
		},
		VectorPartitionCoordinatorLimitsV1{},
	)
	source.router.status.Manifest.Memberships = nil
	dispatcher.editResponse = func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
		for i := range response.Partials {
			response.Partials[i].SearchRoute = collections.VectorPartitionSearchRouteHNSWSearchPackV1
		}
	}
	for i := range 1001 {
		partitionID := uint32(0)
		if i == 1000 {
			partitionID = 1
		}
		source.router.status.Manifest.Memberships = append(
			source.router.status.Manifest.Memberships,
			collections.VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: partitionID},
		)
	}
	request := testVectorPartitionCoordinatorRequestV1(2)
	request.Query = make([]float32, 4096)
	request.Query[0] = 1
	floors, floorTotal, err := vectorPartitionCoordinatorCandidateFloorsV1(
		[]uint64{1000, 1}, []uint32{0, 1}, len(request.Query), request.TopK, request.EfSearch,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.CandidateBytesLimit = floorTotal + 1000*8

	response, err := coordinator.Search(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Neighbors) != 2 || len(dispatcher.calls) != 2 {
		t.Fatalf("response=%+v calls=%+v", response, dispatcher.calls)
	}
	budgets := make(map[uint32]uint64, len(dispatcher.calls))
	var total uint64
	for _, call := range dispatcher.calls {
		budgets[call.PartitionIDs[0]] = call.CandidateBytesLimit
		total += call.CandidateBytesLimit
	}
	largeSurplus := vectorPartitionCoordinatorWeightedBudgetShareV1(1000*8, 1001, 0, 1000)
	tinySurplus := vectorPartitionCoordinatorWeightedBudgetShareV1(1000*8, 1001, 1000, 1)
	if budgets[0] != floors[0]+largeSurplus || budgets[1] != floors[1]+tinySurplus || total != request.CandidateBytesLimit {
		t.Fatalf("candidate budgets=%v total=%d want large=%d tiny=%d total=%d",
			budgets, total, floors[0]+largeSurplus, floors[1]+tinySurplus, request.CandidateBytesLimit)
	}
}

func TestVectorPartitionCoordinatorPartitionRowsObserveContextV1(t *testing.T) {
	manifest := collections.VectorPartitionManifestV1{
		PartitionCount: 1,
		Memberships:    make([]collections.VectorPartitionMembershipV1, 4096),
	}
	ctx := &vectorPartitionCoordinatorCancelAfterErrContextV1{cancelAt: 3}
	if _, err := vectorPartitionCoordinatorPartitionRowsV1(
		ctx, manifest,
	); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("partition rows err=%v want deadline exceeded", err)
	}
	ctx = &vectorPartitionCoordinatorCancelAfterErrContextV1{cancelAt: 2}
	if _, err := vectorPartitionCoordinatorPartitionRowsV1(ctx, collections.VectorPartitionManifestV1{PartitionCount: 1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("final cancellation err=%v want deadline exceeded", err)
	}
}

func TestVectorPartitionCoordinatorPartitionRowsIncludeOverlapAndFailClosedV1(t *testing.T) {
	rows, err := vectorPartitionCoordinatorPartitionRowsV1(t.Context(), collections.VectorPartitionManifestV1{
		PartitionCount: 3,
		Memberships: []collections.VectorPartitionMembershipV1{
			{PartitionID: 0}, {PartitionID: 1}, {PartitionID: 1},
		},
		OverlapMemberships: []collections.VectorPartitionMembershipV1{
			{PartitionID: 1}, {PartitionID: 2},
		},
	})
	if err != nil || !slices.Equal(rows, []uint64{1, 3, 1}) {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	for _, test := range []struct {
		name     string
		selected []uint32
		total    uint64
	}{
		{name: "empty"},
		{name: "sparse", selected: []uint32{2}, total: 1},
		{name: "duplicate", selected: []uint32{1, 1}, total: 6},
		{name: "all", selected: []uint32{0, 1, 2}, total: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotRows, total, err := vectorPartitionCoordinatorCandidateRowsV1(t.Context(), rows, test.selected)
			if err != nil || !slices.Equal(gotRows, rows) || total != test.total {
				t.Fatalf("rows=%v total=%d err=%v", gotRows, total, err)
			}
		})
	}
	if _, err := vectorPartitionCoordinatorPartitionRowsV1(t.Context(), collections.VectorPartitionManifestV1{
		PartitionCount: 1, Memberships: []collections.VectorPartitionMembershipV1{{PartitionID: 1}},
	}); !errors.Is(err, ErrVectorPartitionCoordinatorGenerationMismatch) {
		t.Fatalf("invalid partition err=%v", err)
	}
	rows = []uint64{^uint64(0)}
	if err := vectorPartitionCoordinatorCountMembershipsV1(t.Context(), rows, []collections.VectorPartitionMembershipV1{{PartitionID: 0}}); !errors.Is(err, ErrVectorPartitionCoordinatorBudgetExceeded) {
		t.Fatalf("overflow err=%v", err)
	}
	if _, _, err := vectorPartitionCoordinatorCandidateRowsV1(t.Context(), []uint64{1}, []uint32{1}); !errors.Is(err, ErrVectorPartitionCoordinatorGenerationMismatch) {
		t.Fatalf("selected partition err=%v", err)
	}
}

func BenchmarkVectorPartitionCoordinatorCandidateRowsV1(b *testing.B) {
	manifest := collections.VectorPartitionManifestV1{PartitionCount: 16}
	manifest.Memberships = make([]collections.VectorPartitionMembershipV1, 250_000)
	manifest.OverlapMemberships = make([]collections.VectorPartitionMembershipV1, 50_000)
	for i := range manifest.Memberships {
		manifest.Memberships[i].PartitionID = uint32(i % int(manifest.PartitionCount))
	}
	for i := range manifest.OverlapMemberships {
		manifest.OverlapMemberships[i].PartitionID = uint32(i % int(manifest.PartitionCount))
	}
	partitionRows, err := vectorPartitionCoordinatorPartitionRowsV1(context.Background(), manifest)
	if err != nil {
		b.Fatal(err)
	}
	selected := []uint32{2, 13}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, total, err := vectorPartitionCoordinatorCandidateRowsV1(ctx, partitionRows, selected)
		if err != nil {
			b.Fatal(err)
		}
		vectorPartitionCoordinatorCandidateRowsBenchmarkSinkV1.rows = rows
		vectorPartitionCoordinatorCandidateRowsBenchmarkSinkV1.total = total
	}
	b.StopTimer()
	if vectorPartitionCoordinatorCandidateRowsBenchmarkSinkV1.total != 37_500 ||
		len(vectorPartitionCoordinatorCandidateRowsBenchmarkSinkV1.rows) != len(partitionRows) {
		b.Fatalf("rows=%d total=%d", len(vectorPartitionCoordinatorCandidateRowsBenchmarkSinkV1.rows), vectorPartitionCoordinatorCandidateRowsBenchmarkSinkV1.total)
	}
}

func TestVectorPartitionCoordinatorRejectsMixedRouterGenerationBeforeDispatchV1(t *testing.T) {
	tests := []struct {
		name string
		edit func(*collections.VectorPartitionRouterRuntimeStatusV1)
	}{
		{name: "source_generation", edit: func(status *collections.VectorPartitionRouterRuntimeStatusV1) {
			status.Manifest.SourceGeneration++
		}},
		{name: "ready_set_digest", edit: func(status *collections.VectorPartitionRouterRuntimeStatusV1) {
			status.Manifest.ReadySetDigest = "invalid"
		}},
		{name: "model_digest", edit: func(status *collections.VectorPartitionRouterRuntimeStatusV1) {
			status.ModelDigest = "invalid"
		}},
		{name: "placement", edit: func(status *collections.VectorPartitionRouterRuntimeStatusV1) {
			status.Manifest.Placements[0].GroupID = "wrong-group"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
				[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
				[]raftcluster.GroupID{"group-a"},
				map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
				VectorPartitionCoordinatorLimitsV1{},
			)
			test.edit(&source.router.status)
			response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(1))
			if err == nil || !vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
				t.Fatalf("response=%+v err=%v", response, err)
			}
			if err := coordinator.Close(); err != nil {
				t.Fatal(err)
			}
			if len(dispatcher.calls) != 0 || source.router.closeCount != 1 {
				t.Fatalf("dispatch=%d closes=%d", len(dispatcher.calls), source.router.closeCount)
			}
		})
	}
}

func TestVectorPartitionCoordinatorRejectsCorruptShardProofsAndPartialsV1(t *testing.T) {
	tests := []struct {
		name string
		edit func(VectorPartitionShardSearchRequestV1, *VectorPartitionShardSearchResponseV1)
	}{
		{name: "generation", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Proof.SourceGeneration++
		}},
		{name: "leader_serving_disagree", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Proof.LeaderNode = "node-b"
		}},
		{name: "missing_read_term", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Proof.ReadTerm = 0
		}},
		{name: "missing_read_index", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Proof.ReadIndex = 0
		}},
		{name: "missing_applied_term", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Proof.AppliedTerm = 0
		}},
		{name: "missing_applied_index", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Proof.AppliedIndex = 0
		}},
		{name: "applied_index_precedes_read", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Proof.ReadIndex = response.Proof.AppliedIndex + 1
		}},
		{name: "ready_set_digest", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Proof.ReadySetDigest = strings.Repeat("f", 64)
		}},
		{name: "underreported_candidates", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Partials[0].Candidates = 0
			response.Candidates = 0
		}},
		{name: "underreported_exact_candidates_and_neighbors", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Partials[0].Neighbors = nil
			response.Partials[0].Candidates = 0
			response.Candidates = 0
			response.ResponseBytes = vectorPartitionShardSearchResponseEnvelopeBytesV1 + vectorPartitionShardSearchPartialEnvelopeBytesV1
		}},
		{name: "dropped_neighbors", edit: func(request VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Partials[0].Candidates = uint64(request.TopK)
			response.Candidates = uint64(request.TopK)
		}},
		{name: "unrequested_partition", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Partials[0].PartitionID = 99
		}},
		{name: "oversized_neighbor_slice", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Partials[0].Neighbors = append(response.Partials[0].Neighbors,
				VectorPartitionShardSearchNeighborV1{ID: "b", Score: .75},
				VectorPartitionShardSearchNeighborV1{ID: "c", Score: .5},
				VectorPartitionShardSearchNeighborV1{ID: "d", Score: .25},
			)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
				[]raftplacement.GroupV1{{
					ID: "group-a", Members: []raftcluster.NodeID{"node-a", "node-b"}, LeaderHint: "node-a",
				}},
				[]raftcluster.GroupID{"group-a"},
				map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
				VectorPartitionCoordinatorLimitsV1{},
			)
			dispatcher.editResponse = test.edit
			response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(1))
			if !errors.Is(err, ErrVectorPartitionCoordinatorMalformedResponse) ||
				!vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
				t.Fatalf("response=%+v err=%v", response, err)
			}
		})
	}
}

func TestVectorPartitionCoordinatorAcceptsCommandFreeCurrentTermGapV1(t *testing.T) {
	coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
		VectorPartitionCoordinatorLimitsV1{},
	)
	dispatcher.editResponse = func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
		// A newly elected HashiCorp leader can prove a current-term read while
		// TreeDB's latest applied command remains in the prior term. The command-
		// free gap is safe once applied progress covers the proof's read index.
		response.Proof.ReadTerm = 7
		response.Proof.ReadIndex = 42
		response.Proof.AppliedTerm = 6
		response.Proof.AppliedIndex = 42
	}

	response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Neighbors) != 1 || response.Neighbors[0].ID != "a" || len(dispatcher.calls) != 1 {
		t.Fatalf("response=%+v calls=%+v", response, dispatcher.calls)
	}
}

func TestVectorPartitionCoordinatorClassifiesConsistentShardBudgetOverflowV1(t *testing.T) {
	coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
		VectorPartitionCoordinatorLimitsV1{},
	)
	dispatcher.editResponse = func(request VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
		candidates := request.CandidateBytesLimit/64 + 1
		response.Partials[0].Neighbors = []VectorPartitionShardSearchNeighborV1{
			{ID: "a", Score: 1},
			{ID: "b", Score: .75},
			{ID: "c", Score: .5},
		}
		response.Partials[0].SearchRoute = collections.VectorPartitionSearchRouteHNSWSearchPackV1
		response.Partials[0].Candidates = candidates
		response.Candidates = candidates
		responseBytes, err := MeasureVectorPartitionShardSearchResponseBytesV1(response.Partials)
		if err != nil {
			t.Fatal(err)
		}
		response.ResponseBytes = responseBytes
	}

	response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(1))
	if !errors.Is(err, ErrVectorPartitionCoordinatorBudgetExceeded) ||
		errors.Is(err, ErrVectorPartitionCoordinatorMalformedResponse) ||
		!vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	var serviceErr *VectorPartitionCoordinatorErrorV1
	if !errors.As(err, &serviceErr) || serviceErr.Code != VectorPartitionCoordinatorErrorBudgetExceededV1 {
		t.Fatalf("classified error=%+v", serviceErr)
	}

	dispatcher.editResponse = nil
	shardRequest := dispatcher.calls[0]
	shardResponse, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), shardRequest)
	if err != nil {
		t.Fatal(err)
	}
	shardRequest.ResponseBytesLimit = shardResponse.ResponseBytes - 1
	task := vectorPartitionCoordinatorTaskV1{
		group:         coordinator.groups[shardRequest.TargetGroupID],
		partitionIDs:  slices.Clone(shardRequest.PartitionIDs),
		candidateRows: []uint64{shardResponse.Partials[0].Candidates},
	}
	err = coordinator.wrapError(
		coordinator.validateShardResponse(context.Background(), task, shardRequest, shardResponse),
		shardRequest.TargetGroupID,
	)
	serviceErr = nil
	if !errors.Is(err, ErrVectorPartitionCoordinatorBudgetExceeded) ||
		errors.Is(err, ErrVectorPartitionCoordinatorMalformedResponse) ||
		!errors.As(err, &serviceErr) ||
		serviceErr.Code != VectorPartitionCoordinatorErrorBudgetExceededV1 {
		t.Fatalf("response-byte classified error=%+v", err)
	}
}

func TestVectorPartitionCoordinatorNotLeaderRedirectIsBoundedV1(t *testing.T) {
	coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-b", "node-a"}}},
		[]raftcluster.GroupID{"group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
		VectorPartitionCoordinatorLimitsV1{},
	)
	dispatcher.notLeaderOnce["group-a"] = "node-b"
	response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(1))
	if err != nil {
		t.Fatal(err)
	}
	if response.Counters.Retries != 1 || response.Counters.Redirects != 1 || response.Counters.RPCs != 2 ||
		len(dispatcher.calls) != 2 || dispatcher.calls[0].TargetNodeID != "node-a" ||
		dispatcher.calls[1].TargetNodeID != "node-b" {
		t.Fatalf("redirect response=%+v calls=%+v", response.Counters, dispatcher.calls)
	}
}

func TestVectorPartitionCoordinatorRedirectReservesLongestGroupMemberV1(t *testing.T) {
	const (
		shortNode = raftcluster.NodeID("n")
		longNode  = raftcluster.NodeID("node-with-a-substantially-longer-identity")
	)
	coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{
			ID: "group-a", Members: []raftcluster.NodeID{shortNode, longNode}, LeaderHint: shortNode,
		}},
		[]raftcluster.GroupID{"group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
		VectorPartitionCoordinatorLimitsV1{},
	)
	testVectorPartitionCoordinatorUseM5PreflightV1(coordinator, dispatcher)
	dispatcher.notLeaderOnce["group-a"] = longNode

	response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(dispatcher.calls) != 2 ||
		dispatcher.calls[0].TargetNodeID != shortNode ||
		dispatcher.calls[1].TargetNodeID != longNode {
		t.Fatalf("redirect calls=%+v", dispatcher.calls)
	}
	wantBytes, err := vectorPartitionCoordinatorShardRequestBytesV1(dispatcher.calls[1])
	if err != nil {
		t.Fatal(err)
	}
	if dispatcher.calls[0].RequestBytesLimit != wantBytes ||
		dispatcher.calls[1].RequestBytesLimit != wantBytes ||
		response.Counters.RequestBytes != wantBytes ||
		response.Counters.MaxShardRequestBytes != wantBytes {
		t.Fatalf("request limits=(%d,%d) counter=%d want longest-target reservation=%d",
			dispatcher.calls[0].RequestBytesLimit,
			dispatcher.calls[1].RequestBytesLimit,
			response.Counters.RequestBytes,
			wantBytes,
		)
	}
	dispatcher.mu.Lock()
	dispatcher.calls = nil
	dispatcher.mu.Unlock()
	underBudget := testVectorPartitionCoordinatorRequestV1(1)
	underBudget.RequestBytesLimit = wantBytes - 1
	underBudgetResponse, err := coordinator.Search(context.Background(), underBudget)
	if !errors.Is(err, ErrVectorPartitionCoordinatorBudgetExceeded) ||
		!vectorPartitionCoordinatorResponseIsZeroTestV1(underBudgetResponse) {
		t.Fatalf("under-budget response=%+v err=%v", underBudgetResponse, err)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("under-budget redirect reservation dispatched calls=%+v", dispatcher.calls)
	}
}

func TestVectorPartitionCoordinatorTerminalFailureCancelsAndReturnsNoPartialV1(t *testing.T) {
	block := make(chan struct{})
	blocked := make(chan raftcluster.GroupID, 1)
	failAfter := make(chan struct{})
	coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"},
		},
		[]raftcluster.GroupID{"group-a", "group-b"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{
			0: {{ID: "a", Score: 1}}, 1: {{ID: "b", Score: 1}},
		},
		VectorPartitionCoordinatorLimitsV1{MaxConcurrentRequests: 2},
	)
	dispatcher.blockByGroup["group-b"] = block
	dispatcher.blocked = blocked
	dispatcher.failGroup = "group-a"
	dispatcher.failErr = errors.New("connection lost")
	dispatcher.failAfter = failAfter
	type searchResult struct {
		response VectorPartitionCoordinatorResponseV1
		err      error
	}
	done := make(chan searchResult, 1)
	go func() {
		response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(2))
		done <- searchResult{response: response, err: err}
	}()
	select {
	case groupID := <-blocked:
		if groupID != "group-b" {
			t.Fatalf("blocked group=%q", groupID)
		}
	case <-time.After(time.Second):
		t.Fatal("sibling request did not block")
	}
	close(failAfter)
	var result searchResult
	select {
	case result = <-done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not cancel and join blocked sibling")
	}
	response, err := result.response, result.err
	if err == nil {
		t.Fatal("terminal dispatch error accepted")
	}
	if !vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
		t.Fatalf("partial response=%+v", response)
	}
	var coordinatorErr *VectorPartitionCoordinatorErrorV1
	if !errors.As(err, &coordinatorErr) {
		t.Fatalf("terminal error type=%T want coordinator error", err)
	}
	if coordinatorErr.Counters.SelectedPartitions != 2 || coordinatorErr.Counters.SelectedGroups != 2 ||
		coordinatorErr.Counters.Requests != 2 || coordinatorErr.Counters.RPCs == 0 ||
		coordinatorErr.Counters.RequestBytes == 0 || coordinatorErr.Counters.MaxShardPartitions != 1 ||
		coordinatorErr.Counters.MaxShardRequestBytes == 0 || coordinatorErr.Timing.TotalNanos == 0 {
		t.Fatalf("terminal resource evidence counters=%+v timing=%+v", coordinatorErr.Counters, coordinatorErr.Timing)
	}
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	if source.router.closeCount != 1 || dispatcher.active != 0 {
		t.Fatalf("lifecycle closes=%d active=%d", source.router.closeCount, dispatcher.active)
	}
	stats := coordinator.Stats()
	if stats.Requests != 1 || stats.Errors != 1 || stats.Successes != 0 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestVectorPartitionCoordinatorRejectsCorruptAndOverBudgetResponsesV1(t *testing.T) {
	tests := []struct {
		name    string
		wantErr error
		edit    func(*testVectorPartitionCoordinatorDispatcherV1, *VectorPartitionCoordinatorRequestV1)
	}{
		{
			name:    "merge_entries",
			wantErr: ErrVectorPartitionCoordinatorBudgetExceeded,
			edit: func(_ *testVectorPartitionCoordinatorDispatcherV1, request *VectorPartitionCoordinatorRequestV1) {
				request.MergeEntriesLimit = 1
			},
		},
		{
			name:    "response_bytes",
			wantErr: ErrVectorPartitionCoordinatorBudgetExceeded,
			edit: func(_ *testVectorPartitionCoordinatorDispatcherV1, request *VectorPartitionCoordinatorRequestV1) {
				request.ResponseBytesLimit = 1
			},
		},
		{
			name:    "duplicate_partial_id",
			wantErr: ErrVectorPartitionCoordinatorMalformedResponse,
			edit: func(dispatcher *testVectorPartitionCoordinatorDispatcherV1, _ *VectorPartitionCoordinatorRequestV1) {
				dispatcher.neighbors[0] = []VectorPartitionShardSearchNeighborV1{{ID: "a", Score: 1}, {ID: "a", Score: .5}}
			},
		},
		{
			name:    "nonfinite_score",
			wantErr: ErrVectorPartitionCoordinatorMalformedResponse,
			edit: func(dispatcher *testVectorPartitionCoordinatorDispatcherV1, _ *VectorPartitionCoordinatorRequestV1) {
				dispatcher.neighbors[0] = []VectorPartitionShardSearchNeighborV1{{ID: "a", Score: float32(math.NaN())}}
			},
		},
		{
			name:    "timing_component_exceeds_total",
			wantErr: ErrVectorPartitionCoordinatorMalformedResponse,
			edit: func(dispatcher *testVectorPartitionCoordinatorDispatcherV1, _ *VectorPartitionCoordinatorRequestV1) {
				dispatcher.editResponse = func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
					response.Timing.ReadIndexApplyNanos = response.Timing.TotalNanos + 1
				}
			},
		},
		{
			name:    "timing_component_subtotal_exceeds_total",
			wantErr: ErrVectorPartitionCoordinatorMalformedResponse,
			edit: func(dispatcher *testVectorPartitionCoordinatorDispatcherV1, _ *VectorPartitionCoordinatorRequestV1) {
				dispatcher.editResponse = func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
					response.Timing.ReadIndexApplyNanos = 2
					response.Timing.SearchNanos = 2
					response.Timing.ResponseCopyNanos = 0
				}
			},
		},
		{
			name:    "timing_component_subtotal_overflow",
			wantErr: ErrVectorPartitionCoordinatorMalformedResponse,
			edit: func(dispatcher *testVectorPartitionCoordinatorDispatcherV1, _ *VectorPartitionCoordinatorRequestV1) {
				dispatcher.editResponse = func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
					response.Timing.ReadIndexApplyNanos = math.MaxUint64
					response.Timing.SearchNanos = 1
					response.Timing.ResponseCopyNanos = 0
					response.Timing.TotalNanos = math.MaxUint64
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
				[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
				[]raftcluster.GroupID{"group-a"},
				map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
				VectorPartitionCoordinatorLimitsV1{},
			)
			request := testVectorPartitionCoordinatorRequestV1(1)
			test.edit(dispatcher, &request)
			response, err := coordinator.Search(context.Background(), request)
			if !errors.Is(err, test.wantErr) || !vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
				t.Fatalf("response=%+v err=%v", response, err)
			}
		})
	}
}

func TestVectorPartitionCoordinatorRejectsCrossTaskEdgeCounterOverflowV1(t *testing.T) {
	coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"},
		},
		[]raftcluster.GroupID{"group-a", "group-b"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{
			0: {{ID: "a", Score: 1}},
			1: {{ID: "b", Score: 1}},
		},
		VectorPartitionCoordinatorLimitsV1{},
	)
	dispatcher.editResponse = func(request VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
		edges := uint64(1)
		if request.PartitionIDs[0] == 0 {
			edges = math.MaxUint64
		}
		response.Partials[0].Edges = edges
		response.Edges = edges
	}

	response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(2))
	if !errors.Is(err, ErrVectorPartitionCoordinatorBudgetExceeded) ||
		!vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
		t.Fatalf("response=%+v err=%v", response, err)
	}
}

func TestVectorPartitionCoordinatorRejectsCrossTaskTimingOverflowV1(t *testing.T) {
	coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"},
		},
		[]raftcluster.GroupID{"group-a", "group-b"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{
			0: {{ID: "a", Score: 1}},
			1: {{ID: "b", Score: 1}},
		},
		VectorPartitionCoordinatorLimitsV1{},
	)
	dispatcher.editResponse = func(request VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
		response.Timing.ReadIndexApplyNanos = 1
		if request.PartitionIDs[0] == 0 {
			response.Timing.ReadIndexApplyNanos = math.MaxUint64
		}
	}

	response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(2))
	if !errors.Is(err, ErrVectorPartitionCoordinatorMalformedResponse) ||
		!vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
		t.Fatalf("response=%+v err=%v", response, err)
	}
}

func TestVectorPartitionCoordinatorResponseCounterAggregationRejectsOverflowV1(t *testing.T) {
	tests := []struct {
		name     string
		counters VectorPartitionCoordinatorCountersV1
		response VectorPartitionShardSearchResponseV1
	}{
		{
			name:     "response_bytes",
			counters: VectorPartitionCoordinatorCountersV1{ResponseBytes: math.MaxUint64},
			response: VectorPartitionShardSearchResponseV1{ResponseBytes: 1},
		},
		{
			name:     "candidates",
			counters: VectorPartitionCoordinatorCountersV1{Candidates: math.MaxUint64},
			response: VectorPartitionShardSearchResponseV1{Candidates: 1},
		},
		{
			name:     "candidate_bytes_multiply",
			response: VectorPartitionShardSearchResponseV1{Candidates: math.MaxUint64},
		},
		{
			name:     "candidate_bytes_sum",
			counters: VectorPartitionCoordinatorCountersV1{CandidateBytes: math.MaxUint64},
			response: VectorPartitionShardSearchResponseV1{Candidates: 1},
		},
		{
			name:     "edges",
			counters: VectorPartitionCoordinatorCountersV1{Edges: math.MaxUint64},
			response: VectorPartitionShardSearchResponseV1{Edges: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := test.counters
			if accumulateVectorPartitionCoordinatorResponseCountersV1(&test.counters, test.response) {
				t.Fatal("overflow accepted")
			}
			if test.counters != before {
				t.Fatalf("partial counters published: got=%+v want=%+v", test.counters, before)
			}
		})
	}
}

func TestVectorPartitionCoordinatorResponseCountersTrackShardMaximaV1(t *testing.T) {
	counters := VectorPartitionCoordinatorCountersV1{MaxShardCandidateBytes: 640, MaxShardResponseBytes: 200}
	if !accumulateVectorPartitionCoordinatorResponseCountersV1(&counters, VectorPartitionShardSearchResponseV1{Candidates: 20, ResponseBytes: 150}) {
		t.Fatal("valid shard counters rejected")
	}
	if counters.MaxShardCandidateBytes != 1280 || counters.MaxShardResponseBytes != 200 {
		t.Fatalf("shard maxima=%+v", counters)
	}
}

func TestVectorPartitionCoordinatorTimingAggregationRejectsOverflowV1(t *testing.T) {
	tests := []struct {
		name   string
		timing VectorPartitionCoordinatorTimingV1
		result vectorPartitionCoordinatorTaskResultV1
	}{
		{name: "queue", timing: VectorPartitionCoordinatorTimingV1{QueueNanos: math.MaxUint64}, result: vectorPartitionCoordinatorTaskResultV1{queueNanos: 1}},
		{name: "rpc", timing: VectorPartitionCoordinatorTimingV1{RPCNanos: math.MaxUint64}, result: vectorPartitionCoordinatorTaskResultV1{rpcNanos: 1}},
		{name: "network", timing: VectorPartitionCoordinatorTimingV1{NetworkNanos: math.MaxUint64}, result: vectorPartitionCoordinatorTaskResultV1{networkNanos: 1}},
		{name: "read_index_apply", timing: VectorPartitionCoordinatorTimingV1{ReadIndexApplyNanos: math.MaxUint64}, result: vectorPartitionCoordinatorTaskResultV1{response: VectorPartitionShardSearchResponseV1{Timing: VectorPartitionShardSearchTimingV1{ReadIndexApplyNanos: 1}}}},
		{name: "generation_open", timing: VectorPartitionCoordinatorTimingV1{GenerationOpenNanos: math.MaxUint64}, result: vectorPartitionCoordinatorTaskResultV1{response: VectorPartitionShardSearchResponseV1{Timing: VectorPartitionShardSearchTimingV1{GenerationOpenNanos: 1}}}},
		{name: "shard_search", timing: VectorPartitionCoordinatorTimingV1{ShardSearchNanos: math.MaxUint64}, result: vectorPartitionCoordinatorTaskResultV1{response: VectorPartitionShardSearchResponseV1{Timing: VectorPartitionShardSearchTimingV1{SearchNanos: 1}}}},
		{name: "response", timing: VectorPartitionCoordinatorTimingV1{ResponseNanos: math.MaxUint64}, result: vectorPartitionCoordinatorTaskResultV1{response: VectorPartitionShardSearchResponseV1{Timing: VectorPartitionShardSearchTimingV1{ResponseCopyNanos: 1}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := test.timing
			if accumulateVectorPartitionCoordinatorTimingV1(&test.timing, test.result) {
				t.Fatal("overflow accepted")
			}
			if test.timing != before {
				t.Fatalf("partial timing published: got=%+v want=%+v", test.timing, before)
			}
		})
	}
}

func TestVectorPartitionCoordinatorLowerStableIDCapUsesM5ResponseReservationV1(t *testing.T) {
	coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "short", Score: 1}}},
		VectorPartitionCoordinatorLimitsV1{MaxStableIDBytes: 8},
	)
	testVectorPartitionCoordinatorUseM5PreflightV1(coordinator, dispatcher)

	request := testVectorPartitionCoordinatorRequestV1(1)
	shardLimits := DefaultVectorPartitionShardSearchLimitsV1()
	wantReservation, err := vectorPartitionCoordinatorShardResponseReservationV1(
		1, request.TopK, shardLimits.MaxStableIDBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.ResponseBytesLimit = wantReservation
	response, err := coordinator.Search(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Neighbors) != 1 || response.Neighbors[0].ID != "short" ||
		len(dispatcher.calls) != 1 {
		t.Fatalf("response=%+v calls=%+v", response, dispatcher.calls)
	}
	coordinatorOnlyReservation, err := vectorPartitionCoordinatorShardResponseReservationV1(
		1, request.TopK, 8,
	)
	if err != nil {
		t.Fatal(err)
	}
	if dispatcher.calls[0].ResponseBytesLimit != wantReservation ||
		wantReservation <= coordinatorOnlyReservation {
		t.Fatalf("response reservation=%d want M5=%d coordinator-only=%d",
			dispatcher.calls[0].ResponseBytesLimit,
			wantReservation,
			coordinatorOnlyReservation,
		)
	}
}

func TestVectorPartitionCoordinatorDeadlineBeforeDispatchV1(t *testing.T) {
	coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
		VectorPartitionCoordinatorLimitsV1{},
	)
	request := testVectorPartitionCoordinatorRequestV1(1)
	request.DeadlineUnixNano = time.Now().Add(-time.Second).UnixNano()
	if response, err := coordinator.Search(context.Background(), request); !errors.Is(err, context.DeadlineExceeded) ||
		!vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if source.opens != 0 || len(dispatcher.calls) != 0 {
		t.Fatalf("expired request opened router or dispatched: opens=%d calls=%d", source.opens, len(dispatcher.calls))
	}
}

func TestVectorPartitionCoordinatorRejectsStatsNoneBeforeRouterOpenV1(t *testing.T) {
	coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
		VectorPartitionCoordinatorLimitsV1{},
	)
	request := testVectorPartitionCoordinatorRequestV1(1)
	request.StatsMode = VectorPartitionShardSearchStatsNoneV1
	response, err := coordinator.Search(context.Background(), request)
	if !errors.Is(err, ErrVectorPartitionCoordinatorInvalidRequest) ||
		!vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if source.opens != 0 || len(dispatcher.calls) != 0 {
		t.Fatalf("stats=none opened router or dispatched: opens=%d calls=%d", source.opens, len(dispatcher.calls))
	}
}

func TestVectorPartitionCoordinatorPropagatesEffectiveDeadlineV1(t *testing.T) {
	coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
		VectorPartitionCoordinatorLimitsV1{},
	)
	parentDeadline := time.Now().Add(5 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), parentDeadline)
	defer cancel()
	request := testVectorPartitionCoordinatorRequestV1(1)
	request.DeadlineUnixNano = parentDeadline.Add(5 * time.Second).UnixNano()
	if _, err := coordinator.Search(ctx, request); err != nil {
		t.Fatal(err)
	}
	if len(dispatcher.calls) != 1 ||
		dispatcher.calls[0].DeadlineUnixNano != parentDeadline.UnixNano() {
		t.Fatalf("shard deadline=%d want=%d", dispatcher.calls[0].DeadlineUnixNano, parentDeadline.UnixNano())
	}
}

func TestVectorPartitionCoordinatorClampsDefaultAndExplicitDeadlineV1(t *testing.T) {
	tests := []struct {
		name            string
		limits          VectorPartitionCoordinatorLimitsV1
		requestDeadline time.Duration
		wantWallClock   time.Duration
	}{
		{
			name:          "default_without_request_deadline",
			wantWallClock: DefaultVectorPartitionCoordinatorLimitsV1().MaxWallClock,
		},
		{
			name:            "explicit_deadline_clamped_to_nonzero_limit",
			limits:          VectorPartitionCoordinatorLimitsV1{MaxWallClock: 2 * time.Second},
			requestDeadline: time.Hour,
			wantWallClock:   2 * time.Second,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(t,
				[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
				[]raftcluster.GroupID{"group-a"},
				map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
				test.limits,
			)
			request := testVectorPartitionCoordinatorRequestV1(1)
			before := time.Now()
			if test.requestDeadline > 0 {
				request.DeadlineUnixNano = before.Add(test.requestDeadline).UnixNano()
			}
			if _, err := coordinator.Search(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			after := time.Now()
			if len(dispatcher.calls) != 1 {
				t.Fatalf("calls=%d", len(dispatcher.calls))
			}
			got := time.Unix(0, dispatcher.calls[0].DeadlineUnixNano)
			earliest := before.Add(test.wantWallClock)
			latest := after.Add(test.wantWallClock)
			if got.Before(earliest) || got.After(latest) {
				t.Fatalf("effective deadline=%v want range [%v,%v]", got, earliest, latest)
			}
		})
	}
}

func TestVectorPartitionCoordinatorWallClockTimeoutCancelsJoinsAndReturnsNoPartialV1(t *testing.T) {
	block := make(chan struct{})
	blocked := make(chan raftcluster.GroupID, 1)
	coordinator, source, dispatcher := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"}},
		[]raftcluster.GroupID{"group-a"},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: 1}}},
		VectorPartitionCoordinatorLimitsV1{MaxWallClock: 25 * time.Millisecond},
	)
	dispatcher.blockByGroup["group-a"] = block
	dispatcher.blocked = blocked
	type searchResult struct {
		response VectorPartitionCoordinatorResponseV1
		err      error
	}
	done := make(chan searchResult, 1)
	go func() {
		response, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(1))
		done <- searchResult{response: response, err: err}
	}()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not enter blocked call")
	}
	var result searchResult
	select {
	case result = <-done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not time out, cancel, and join dispatch")
	}
	if !errors.Is(result.err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", result.err)
	}
	if !vectorPartitionCoordinatorResponseIsZeroTestV1(result.response) {
		t.Fatalf("partial response=%+v", result.response)
	}
	dispatcher.mu.Lock()
	active := dispatcher.active
	dispatcher.mu.Unlock()
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	if active != 0 || source.router.closeCount != 1 {
		t.Fatalf("active=%d router closes=%d", active, source.router.closeCount)
	}
	stats := coordinator.Stats()
	if stats.Requests != 1 || stats.Errors != 1 || stats.TimedOut != 1 || stats.Successes != 0 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestVectorPartitionCoordinatorEnforcesConcurrentRequestCapV1(t *testing.T) {
	const taskCount = 6
	groups := make([]raftplacement.GroupV1, taskCount)
	owners := make([]raftcluster.GroupID, taskCount)
	neighbors := make(map[uint32][]VectorPartitionShardSearchNeighborV1, taskCount)
	block := make(chan struct{})
	blocked := make(chan raftcluster.GroupID, taskCount)
	for i := range taskCount {
		groupID := raftcluster.GroupID(fmt.Sprintf("group-%02d", i))
		nodeID := raftcluster.NodeID(fmt.Sprintf("node-%02d", i))
		groups[i] = raftplacement.GroupV1{ID: groupID, Members: []raftcluster.NodeID{nodeID}, LeaderHint: nodeID}
		owners[i] = groupID
		neighbors[uint32(i)] = []VectorPartitionShardSearchNeighborV1{{ID: fmt.Sprintf("doc-%02d", i), Score: 1}}
	}
	coordinator, _, dispatcher := testVectorPartitionCoordinatorV1(
		t, groups, owners, neighbors,
		VectorPartitionCoordinatorLimitsV1{MaxConcurrentRequests: 2},
	)
	for _, group := range groups {
		dispatcher.blockByGroup[group.ID] = block
	}
	dispatcher.blocked = blocked
	done := make(chan error, 1)
	go func() {
		_, err := coordinator.Search(context.Background(), testVectorPartitionCoordinatorRequestV1(taskCount))
		done <- err
	}()
	for range 2 {
		select {
		case <-blocked:
		case <-time.After(time.Second):
			t.Fatal("worker did not enter blocked dispatch")
		}
	}
	dispatcher.mu.Lock()
	active, maximum := dispatcher.active, dispatcher.maximum
	dispatcher.mu.Unlock()
	if active != 2 || maximum != 2 {
		t.Fatalf("active=%d maximum=%d want=2", active, maximum)
	}
	close(block)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("coordinator did not join all bounded workers")
	}
	dispatcher.mu.Lock()
	active, maximum, calls := dispatcher.active, dispatcher.maximum, len(dispatcher.calls)
	dispatcher.mu.Unlock()
	if active != 0 || maximum != 2 || calls != taskCount {
		t.Fatalf("active=%d maximum=%d calls=%d", active, maximum, calls)
	}
}

func vectorPartitionCoordinatorResponseIsZeroTestV1(response VectorPartitionCoordinatorResponseV1) bool {
	if len(response.Neighbors) != 0 || len(response.ProbedPartitions) != 0 || len(response.ProbedGroups) != 0 {
		return false
	}
	response.Neighbors = nil
	response.ProbedPartitions = nil
	response.ProbedGroups = nil
	return reflect.DeepEqual(response, VectorPartitionCoordinatorResponseV1{})
}

type vectorPartitionCoordinatorCancelAfterErrContextV1 struct {
	calls    int
	cancelAt int
}

func (c *vectorPartitionCoordinatorCancelAfterErrContextV1) Deadline() (time.Time, bool) {
	return time.Time{}, false
}
func (c *vectorPartitionCoordinatorCancelAfterErrContextV1) Done() <-chan struct{} { return nil }
func (c *vectorPartitionCoordinatorCancelAfterErrContextV1) Value(any) any         { return nil }
func (c *vectorPartitionCoordinatorCancelAfterErrContextV1) Err() error {
	c.calls++
	if c.cancelAt > 0 && c.calls >= c.cancelAt {
		return context.DeadlineExceeded
	}
	return nil
}
