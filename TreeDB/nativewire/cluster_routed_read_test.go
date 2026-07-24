package nativewire

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

type routedReadApplyWaiter struct {
	progress raftcluster.AppliedProgress
	events   *[]string
	replace  func() error
	calls    []raftcluster.AppliedIndexReadBarrier
}

func (w *routedReadApplyWaiter) WaitAppliedIndex(_ context.Context, barrier raftcluster.AppliedIndexReadBarrier) (raftcluster.AppliedProgress, error) {
	w.calls = append(w.calls, barrier)
	if w.events != nil {
		*w.events = append(*w.events, "owner:wait-applied")
	}
	if w.replace != nil {
		if err := w.replace(); err != nil {
			return raftcluster.AppliedProgress{}, err
		}
	}
	return w.progress, nil
}

type routedReadIndexProvider struct {
	proof  raftcluster.ReadIndexProof
	events *[]string
	calls  []raftcluster.ReadIndexBarrier
}

func (p *routedReadIndexProvider) ReadIndex(_ context.Context, target raftcluster.ReadIndexBarrier) (raftcluster.ReadIndexProof, error) {
	p.calls = append(p.calls, target)
	if p.events != nil {
		*p.events = append(*p.events, "owner:read-index")
	}
	return p.proof, nil
}

type benchmarkRoutedReadIndexProvider struct {
	proof raftcluster.ReadIndexProof
}

func (p benchmarkRoutedReadIndexProvider) ReadIndex(context.Context, raftcluster.ReadIndexBarrier) (raftcluster.ReadIndexProof, error) {
	return p.proof, nil
}

type benchmarkRoutedReadApplyWaiter struct {
	progress raftcluster.AppliedProgress
}

func (w benchmarkRoutedReadApplyWaiter) WaitAppliedIndex(context.Context, raftcluster.AppliedIndexReadBarrier) (raftcluster.AppliedProgress, error) {
	return w.progress, nil
}

type benchmarkRoutedReadSubmitter struct {
	provider CatalogClusterRouteProvider
}

func (s benchmarkRoutedReadSubmitter) SubmitCommandEntryV1(context.Context, []byte, ClusterRequestMetadata) (ClusterSubmitResult, error) {
	return ClusterSubmitResult{}, fmt.Errorf("benchmark routed read unexpectedly submitted a mutation")
}

func (s benchmarkRoutedReadSubmitter) ClusterRoute(ctx context.Context, request ClusterRouteRequest) (ClusterRouteTarget, error) {
	return s.provider.ClusterRoute(ctx, request)
}

func TestProductionRouteSingleIDReadSelectsOwnerAndWaitsForApplyBeforeLocalObservation(t *testing.T) {
	id := []byte("u1")
	token := raftplacement.DocumentIDTokenV1(id)
	routeProvider := &placementRouteClusterSubmitter{
		fakeClusterSubmitter: &fakeClusterSubmitter{},
		provider: NewCatalogClusterRouteProvider(
			mustNativewireTokenGroupRouteCatalog(t, raftplacement.PlacementModeRingV1, token),
		),
	}
	var events []string
	wrongProvider := &routedReadIndexProvider{proof: raftcluster.ReadIndexProof{
		NodeID:       "node-a",
		GroupID:      "group-a",
		Index:        9,
		HasQuorum:    true,
		EvidenceKind: raftcluster.ReadIndexEvidenceProduction,
	}}
	wrongWaiter := &routedReadApplyWaiter{progress: raftcluster.AppliedProgress{
		NodeID:     "node-a",
		GroupID:    "group-a",
		Index:      9,
		HasApplied: true,
	}}
	ownerProvider := &routedReadIndexProvider{
		events: &events,
		proof: raftcluster.ReadIndexProof{
			NodeID:       "node-c",
			GroupID:      "group-b",
			Term:         7,
			Index:        42,
			HasQuorum:    true,
			EvidenceKind: raftcluster.ReadIndexEvidenceProduction,
		},
	}
	ownerWaiter := &routedReadApplyWaiter{
		events: &events,
		progress: raftcluster.AppliedProgress{
			NodeID:     "node-c",
			GroupID:    "group-b",
			Term:       7,
			Index:      42,
			HasApplied: true,
		},
	}
	routedCoordinator, err := raftcluster.NewGroupRoutedReadIndexCoordinator([]raftcluster.GroupReadIndexCoordinatorV1{
		{GroupID: "group-a", NodeID: "node-a", ReadIndexProvider: wrongProvider, AppliedIndexWaiter: wrongWaiter},
		{GroupID: "group-b", NodeID: "node-c", ReadIndexProvider: ownerProvider, AppliedIndexWaiter: ownerWaiter},
	})
	if err != nil {
		t.Fatalf("NewGroupRoutedReadIndexCoordinator: %v", err)
	}
	client, server, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{
		ClusterSubmitter: routeProvider,
		ClusterReadCoordinator: AppliedIndexReadCoordinator{
			RoutedReadIndexCoordinator: routedCoordinator,
		},
	})
	col := seedReadCollection(t, mgr)
	wrongLocal := []byte(`{"email":"ada@example.com","city":"hnl","name":"wrong-local"}`)
	if matched, err := col.Replace(id, wrongLocal); err != nil || !matched {
		t.Fatalf("seed wrong-local result matched=%t err=%v", matched, err)
	}
	ownerApplied := []byte(`{"email":"ada@example.com","city":"hnl","name":"owner-applied"}`)
	ownerWaiter.replace = func() error {
		matched, err := col.Replace(id, ownerApplied)
		if err == nil && !matched {
			return fmt.Errorf("owner apply replacement did not match u1")
		}
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	result, err := client.GetManyWithOptions(ctx, "users", [][]byte{id}, ReadOptions{
		ConsistencyPolicy: ConsistencyLinearizable,
	})
	if err != nil {
		t.Fatalf("GetManyWithOptions routed owner: %v", err)
	}
	if len(result.Docs) != 1 || !result.Present[0] {
		t.Fatalf("result docs/present=%q/%v want one present document", result.Docs, result.Present)
	}
	if !bytes.Equal(result.Docs[0], ownerApplied) || bytes.Equal(result.Docs[0], wrongLocal) {
		t.Fatalf("result=%q want owner-applied and never wrong-local", result.Docs[0])
	}
	if len(wrongProvider.calls) != 0 || len(wrongWaiter.calls) != 0 {
		t.Fatalf("wrong local group read calls provider=%v waiter=%v want none", wrongProvider.calls, wrongWaiter.calls)
	}
	if len(ownerProvider.calls) != 1 || len(ownerWaiter.calls) != 1 {
		t.Fatalf("owner calls provider=%v waiter=%v want one each", ownerProvider.calls, ownerWaiter.calls)
	}
	if len(events) != 2 || events[0] != "owner:read-index" || events[1] != "owner:wait-applied" {
		t.Fatalf("events=%v want owner read-index then apply wait", events)
	}
	if !result.ReadMeta.Valid ||
		result.ReadMeta.ActualConsistency != ConsistencyLinearizable ||
		result.ReadMeta.ServingNode != "node-c" ||
		result.ReadMeta.LeaderNode != "node-c" ||
		!result.ReadMeta.HasAppliedIndex ||
		result.ReadMeta.AppliedIndex != 42 {
		t.Fatalf("read metadata=%+v want owner node-c index 42", result.ReadMeta)
	}
	routes := routeProvider.snapshotRoutes()
	if len(routes) != 1 ||
		routes[0].Shape != ClusterRouteShapeToken ||
		!routes[0].TokenKnown ||
		routes[0].Token != token {
		t.Fatalf("route requests=%+v want one token route for u1", routes)
	}
	stats := server.Stats()
	for key, want := range map[string]string{
		"treedb.native_wire.cluster_read_route.requests_total":              "1",
		"treedb.native_wire.cluster_read_route.success_total":               "1",
		"treedb.native_wire.cluster_read_route.group.group-b.success_total": "1",
		"treedb.native_wire.cluster_read_route.partition.p1.success_total":  "1",
		"treedb.native_wire.cluster_read_route.linearizable_success_total":  "1",
		"treedb.native_wire.cluster_read_route.read_index_success_total":    "1",
		"treedb.native_wire.cluster_read_route.leader_success_total":        "1",
		"treedb.native_wire.cluster_read_route.follower_success_total":      "0",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("stats[%q]=%q want %q stats=%v", key, got, want, stats)
		}
	}
	if got := stats["treedb.native_wire.cluster_read_route.group.group-a.success_total"]; got != "" {
		t.Fatalf("wrong local group success counter=%q want absent", got)
	}
}

func TestProductionRouteSingleIDReadFailsClosedForStaleLeaderAndUnsupportedShapes(t *testing.T) {
	id := []byte("u1")
	token := raftplacement.DocumentIDTokenV1(id)
	newSubmitter := func() *placementRouteClusterSubmitter {
		return &placementRouteClusterSubmitter{
			fakeClusterSubmitter: &fakeClusterSubmitter{},
			provider: NewCatalogClusterRouteProvider(
				mustNativewireTokenGroupRouteCatalog(t, raftplacement.PlacementModeRingV1, token),
			),
		}
	}

	t.Run("stale_leader_hint", func(t *testing.T) {
		submitter := newSubmitter()
		ownerProvider := &routedReadIndexProvider{proof: raftcluster.ReadIndexProof{
			NodeID:       "node-d",
			GroupID:      "group-b",
			Index:        42,
			HasQuorum:    true,
			EvidenceKind: raftcluster.ReadIndexEvidenceProduction,
		}}
		ownerWaiter := &routedReadApplyWaiter{progress: raftcluster.AppliedProgress{
			NodeID:     "node-d",
			GroupID:    "group-b",
			Index:      42,
			HasApplied: true,
		}}
		routedCoordinator, err := raftcluster.NewGroupRoutedReadIndexCoordinator([]raftcluster.GroupReadIndexCoordinatorV1{
			{GroupID: "group-b", NodeID: "node-d", ReadIndexProvider: ownerProvider, AppliedIndexWaiter: ownerWaiter},
		})
		if err != nil {
			t.Fatalf("NewGroupRoutedReadIndexCoordinator: %v", err)
		}
		client, server, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{
			ClusterSubmitter: submitter,
			ClusterReadCoordinator: AppliedIndexReadCoordinator{
				RoutedReadIndexCoordinator: routedCoordinator,
			},
		})
		seedReadCollection(t, mgr)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Hello(ctx); err != nil {
			t.Fatalf("Hello: %v", err)
		}
		_, err = client.GetManyWithOptions(ctx, "users", [][]byte{id}, ReadOptions{
			ConsistencyPolicy: ConsistencyLinearizable,
		})
		if !isRemoteError(err, iwire.ErrConsistencyUnavailable) || !strings.Contains(err.Error(), "target mismatch") {
			t.Fatalf("stale leader routed read err=%v want consistency unavailable target mismatch", err)
		}
		if len(ownerProvider.calls) != 0 || len(ownerWaiter.calls) != 0 {
			t.Fatalf("stale leader reached owner provider/waiter: %v/%v", ownerProvider.calls, ownerWaiter.calls)
		}
		if got := server.Stats()["treedb.native_wire.cluster_read_route.errors_total"]; got != "1" {
			t.Fatalf("routed read errors=%q want 1", got)
		}
	})

	t.Run("local_stale", func(t *testing.T) {
		submitter := newSubmitter()
		client, server, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{
			ClusterSubmitter: submitter,
		})
		seedReadCollection(t, mgr)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Hello(ctx); err != nil {
			t.Fatalf("Hello: %v", err)
		}
		_, _, err := client.GetMany(ctx, "users", [][]byte{id})
		if !isRemoteError(err, iwire.ErrConsistencyUnavailable) || !strings.Contains(err.Error(), "requires linearizable consistency") {
			t.Fatalf("local-stale routed read err=%v want linearizable consistency rejection", err)
		}
		if got := server.Stats()["treedb.native_wire.cluster_read_route.stale_rejected_total"]; got != "1" {
			t.Fatalf("stale rejected counter=%q want 1", got)
		}
	})

	t.Run("generic_coordinator_cannot_authorize_local_observation", func(t *testing.T) {
		submitter := newSubmitter()
		generic := &fakeClusterReadCoordinator{result: ClusterReadResult{
			ActualConsistency: ConsistencyLinearizable,
			ServingNode:       "node-a",
			LeaderNode:        "node-a",
			AppliedIndex:      9,
			HasAppliedIndex:   true,
		}}
		client, _, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{
			ClusterSubmitter:       submitter,
			ClusterReadCoordinator: generic,
		})
		seedReadCollection(t, mgr)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Hello(ctx); err != nil {
			t.Fatalf("Hello: %v", err)
		}
		_, err := client.GetManyWithOptions(ctx, "users", [][]byte{id}, ReadOptions{
			ConsistencyPolicy: ConsistencyLinearizable,
		})
		if !isRemoteError(err, iwire.ErrConsistencyUnavailable) || !strings.Contains(err.Error(), "does not support routed owner reads") {
			t.Fatalf("generic coordinator routed read err=%v want fail-closed routed-owner rejection", err)
		}
		if len(generic.calls) != 0 {
			t.Fatalf("generic coordinator calls=%+v want none for routed read", generic.calls)
		}
	})

	t.Run("multi_id_and_secondary_index_remain_query_rejected", func(t *testing.T) {
		submitter := newSubmitter()
		client, server, mgr, _ := serveCollectionPipeWithServerAndOptions(t, ServerOptions{
			ClusterSubmitter: submitter,
		})
		seedReadCollection(t, mgr)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Hello(ctx); err != nil {
			t.Fatalf("Hello: %v", err)
		}
		_, err := client.GetManyWithOptions(ctx, "users", [][]byte{id, []byte("u2")}, ReadOptions{
			ConsistencyPolicy: ConsistencyLinearizable,
		})
		if !isRemoteError(err, iwire.ErrReadOnly) || !strings.Contains(err.Error(), "query route shape is not supported") {
			t.Fatalf("multi-id routed read err=%v want query rejection", err)
		}
		_, _, err = client.IndexLookup(ctx, "users", "email", "ada@example.com", CursorLimits{})
		if !isRemoteError(err, iwire.ErrReadOnly) || !strings.Contains(err.Error(), "query route shape is not supported") {
			t.Fatalf("secondary-index routed read err=%v want query rejection", err)
		}
		if routes := submitter.snapshotRoutes(); len(routes) != 0 {
			t.Fatalf("unsupported query route provider calls=%+v want none", routes)
		}
		if got := server.Stats()["treedb.native_wire.cluster_read_route.unsupported_total"]; got != "2" {
			t.Fatalf("unsupported routed read counter=%q want 2", got)
		}
	})
}

func BenchmarkRoutedSingleIDReadStaticInProcess(b *testing.B) {
	id := []byte("u1")
	token := raftplacement.DocumentIDTokenV1(id)
	submitter := benchmarkRoutedReadSubmitter{
		provider: NewCatalogClusterRouteProvider(
			mustNativewireTokenGroupRouteCatalog(b, raftplacement.PlacementModeRingV1, token),
		),
	}
	routedCoordinator, err := raftcluster.NewGroupRoutedReadIndexCoordinator([]raftcluster.GroupReadIndexCoordinatorV1{{
		GroupID: "group-b",
		NodeID:  "node-c",
		ReadIndexProvider: benchmarkRoutedReadIndexProvider{proof: raftcluster.ReadIndexProof{
			NodeID:       "node-c",
			GroupID:      "group-b",
			Term:         7,
			Index:        42,
			HasQuorum:    true,
			EvidenceKind: raftcluster.ReadIndexEvidenceProduction,
		}},
		AppliedIndexWaiter: benchmarkRoutedReadApplyWaiter{progress: raftcluster.AppliedProgress{
			NodeID:     "node-c",
			GroupID:    "group-b",
			Term:       7,
			Index:      42,
			HasApplied: true,
		}},
	}})
	if err != nil {
		b.Fatalf("NewGroupRoutedReadIndexCoordinator: %v", err)
	}
	client, _, mgr, _ := serveCollectionPipeWithServerAndOptions(b, ServerOptions{
		ClusterSubmitter: submitter,
		ClusterReadCoordinator: AppliedIndexReadCoordinator{
			RoutedReadIndexCoordinator: routedCoordinator,
		},
	})
	seedReadCollection(b, mgr)
	ctx := context.Background()
	if err := client.Hello(ctx); err != nil {
		b.Fatalf("Hello: %v", err)
	}
	options := ReadOptions{ConsistencyPolicy: ConsistencyLinearizable}
	const maxLatencySamples = 100_000
	sampleCount := b.N
	if sampleCount > maxLatencySamples {
		sampleCount = maxLatencySamples
	}
	samples := make([]int64, sampleCount)
	sampleEvery := 1
	if b.N > sampleCount {
		sampleEvery = b.N / sampleCount
	}
	sampleAt := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		result, err := client.GetManyWithOptions(ctx, "users", [][]byte{id}, options)
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("GetManyWithOptions: %v", err)
		}
		if len(result.Docs) != 1 || len(result.Present) != 1 || !result.Present[0] {
			b.Fatalf("routed result docs=%d present=%v", len(result.Docs), result.Present)
		}
		if sampleAt < sampleCount && i%sampleEvery == 0 {
			samples[sampleAt] = elapsed.Nanoseconds()
			sampleAt++
		}
	}
	b.StopTimer()
	samples = samples[:sampleAt]
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	reportRoutedReadLatencyPercentile(b, samples, 50, "p50-ns")
	reportRoutedReadLatencyPercentile(b, samples, 95, "p95-ns")
	reportRoutedReadLatencyPercentile(b, samples, 99, "p99-ns")
	b.ReportMetric(float64(sampleAt), "latency-samples")
}

func reportRoutedReadLatencyPercentile(b *testing.B, samples []int64, percentile int, unit string) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	index := (len(samples)*percentile + 99) / 100
	if index > 0 {
		index--
	}
	b.ReportMetric(float64(samples[index]), unit)
}
