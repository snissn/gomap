package nativewire

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

type testVectorPartitionCoordinatorRouterV1 struct {
	status     collections.VectorPartitionRouterRuntimeStatusV1
	partitions []collections.VectorPartitionRouterPartitionScoreV1
	closeCount int
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
	r.closeCount++
	return nil
}

type testVectorPartitionCoordinatorRouterSourceV1 struct {
	router *testVectorPartitionCoordinatorRouterV1
	opens  int
}

func (s *testVectorPartitionCoordinatorRouterSourceV1) OpenVectorPartitionCoordinatorRouterV1(context.Context, string) (VectorPartitionCoordinatorRouterV1, error) {
	s.opens++
	return s.router, nil
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
			ReadTerm: 1, ReadIndex: 2, AppliedTerm: 1, AppliedIndex: 2,
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
		Catalog: catalog, Placement: placement, RouterSource: source, Dispatcher: dispatcher, Limits: limits,
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
	if got := response.Neighbors; len(got) != 3 || got[0].ID != "doc-00" ||
		got[1].ID != "doc-01" || got[2].ID != "doc-02" {
		t.Fatalf("stable top-k=%+v", got)
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
		{name: "applied_term_precedes_read", edit: func(_ VectorPartitionShardSearchRequestV1, response *VectorPartitionShardSearchResponseV1) {
			response.Proof.ReadTerm = 2
			response.Proof.AppliedTerm = 1
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
			if err == nil || !vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
				t.Fatalf("response=%+v err=%v", response, err)
			}
		})
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
		name string
		edit func(*testVectorPartitionCoordinatorDispatcherV1, *VectorPartitionCoordinatorRequestV1)
	}{
		{
			name: "merge_entries",
			edit: func(_ *testVectorPartitionCoordinatorDispatcherV1, request *VectorPartitionCoordinatorRequestV1) {
				request.MergeEntriesLimit = 1
			},
		},
		{
			name: "response_bytes",
			edit: func(_ *testVectorPartitionCoordinatorDispatcherV1, request *VectorPartitionCoordinatorRequestV1) {
				request.ResponseBytesLimit = 1
			},
		},
		{
			name: "duplicate_partial_id",
			edit: func(dispatcher *testVectorPartitionCoordinatorDispatcherV1, _ *VectorPartitionCoordinatorRequestV1) {
				dispatcher.neighbors[0] = []VectorPartitionShardSearchNeighborV1{{ID: "a", Score: 1}, {ID: "a", Score: .5}}
			},
		},
		{
			name: "nonfinite_score",
			edit: func(dispatcher *testVectorPartitionCoordinatorDispatcherV1, _ *VectorPartitionCoordinatorRequestV1) {
				dispatcher.neighbors[0] = []VectorPartitionShardSearchNeighborV1{{ID: "a", Score: float32(math.NaN())}}
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
			if err == nil || !vectorPartitionCoordinatorResponseIsZeroTestV1(response) {
				t.Fatalf("response=%+v err=%v", response, err)
			}
		})
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

func vectorPartitionCoordinatorResponseIsZeroTestV1(response VectorPartitionCoordinatorResponseV1) bool {
	return response.Version == 0 &&
		response.RequestID == "" &&
		response.SourceGeneration == 0 &&
		response.PartitionGeneration == 0 &&
		response.RouterGeneration == 0 &&
		response.RouterModelDigest == "" &&
		response.ReadySetDigest == "" &&
		len(response.Neighbors) == 0 &&
		len(response.ProbedPartitions) == 0 &&
		len(response.ProbedGroups) == 0 &&
		response.Counters == (VectorPartitionCoordinatorCountersV1{}) &&
		response.Timing == (VectorPartitionCoordinatorTimingV1{})
}
