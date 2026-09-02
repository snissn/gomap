package nativewire

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/raftharness"
)

type fakeAppliedIndexBarrierProvider struct {
	barrier raftcluster.AppliedIndexReadBarrier
	err     error
	calls   []ClusterReadRequest
}

func (f *fakeAppliedIndexBarrierProvider) LeaderReadBarrier(ctx context.Context, request ClusterReadRequest) (raftcluster.AppliedIndexReadBarrier, error) {
	f.calls = append(f.calls, request)
	return f.barrier, f.err
}

type fakeAppliedIndexWaiter struct {
	progress raftcluster.AppliedProgress
	err      error
	calls    []raftcluster.AppliedIndexReadBarrier
}

func (f *fakeAppliedIndexWaiter) WaitAppliedIndex(ctx context.Context, barrier raftcluster.AppliedIndexReadBarrier) (raftcluster.AppliedProgress, error) {
	f.calls = append(f.calls, barrier)
	return f.progress, f.err
}

type fakeReadIndexProvider struct {
	proof raftcluster.ReadIndexProof
	err   error
	calls []raftcluster.ReadIndexBarrier
}

func (f *fakeReadIndexProvider) ReadIndex(ctx context.Context, barrier raftcluster.ReadIndexBarrier) (raftcluster.ReadIndexProof, error) {
	f.calls = append(f.calls, barrier)
	return f.proof, f.err
}

func TestAppliedIndexReadCoordinatorProvesLeaderReadMetadata(t *testing.T) {
	provider := &fakeAppliedIndexBarrierProvider{
		barrier: raftcluster.AppliedIndexReadBarrier{
			NodeID:          "node-a",
			GroupID:         "group-a",
			MinAppliedIndex: 42,
		},
	}
	waiter := &fakeAppliedIndexWaiter{
		progress: raftcluster.AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       7,
			Index:      44,
			HasApplied: true,
		},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: AppliedIndexReadCoordinator{
			BarrierProvider: provider,
			Waiter:          waiter,
		},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	result, err := client.GetManyWithOptions(ctx, "users", [][]byte{[]byte("u1")}, ReadOptions{ConsistencyPolicy: ConsistencyLeaderRead})
	if err != nil {
		t.Fatalf("GetManyWithOptions: %v", err)
	}
	if got, want := result.Present, []bool{true}; !boolSlicesEqual(got, want) {
		t.Fatalf("present=%v want %v", got, want)
	}
	if !result.ReadMeta.Valid ||
		result.ReadMeta.ActualConsistency != ConsistencyLeaderRead ||
		result.ReadMeta.ServingNode != "node-a" ||
		result.ReadMeta.LeaderNode != "node-a" ||
		!result.ReadMeta.HasAppliedIndex ||
		result.ReadMeta.AppliedIndex != 44 {
		t.Fatalf("read meta=%+v", result.ReadMeta)
	}
	if len(provider.calls) != 1 {
		t.Fatalf("provider calls=%d want 1", len(provider.calls))
	}
	call := provider.calls[0]
	if call.Policy != ConsistencyLeaderRead || call.CommandID != iwire.CommandGetMany || call.CommandName != "get_many" {
		t.Fatalf("provider call=%+v", call)
	}
	if len(waiter.calls) != 1 || waiter.calls[0] != provider.barrier {
		t.Fatalf("waiter calls=%+v want %+v", waiter.calls, provider.barrier)
	}
}

func TestAppliedIndexReadCoordinatorFailsClosedForUnsatisfiedBarrier(t *testing.T) {
	provider := &fakeAppliedIndexBarrierProvider{
		barrier: raftcluster.AppliedIndexReadBarrier{
			NodeID:          "node-a",
			GroupID:         "group-a",
			MinAppliedIndex: 42,
		},
	}
	waiter := &fakeAppliedIndexWaiter{
		progress: raftcluster.AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       7,
			Index:      41,
			HasApplied: true,
		},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: AppliedIndexReadCoordinator{
			BarrierProvider: provider,
			Waiter:          waiter,
		},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	_, err := client.IndexLookupWithOptions(ctx, "users", "email", "ada@example.com", CursorLimits{}, ReadOptions{ConsistencyPolicy: ConsistencyLeaderRead})
	if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
		t.Fatalf("IndexLookupWithOptions err=%v want consistency_unavailable", err)
	}
	if len(provider.calls) != 1 || len(waiter.calls) != 1 {
		t.Fatalf("provider calls=%d waiter calls=%d want one each", len(provider.calls), len(waiter.calls))
	}
}

func TestAppliedIndexReadCoordinatorFailsClosedForTargetMismatch(t *testing.T) {
	tests := []struct {
		name     string
		progress raftcluster.AppliedProgress
	}{
		{
			name: "node",
			progress: raftcluster.AppliedProgress{
				NodeID:     "node-b",
				GroupID:    "group-a",
				Term:       7,
				Index:      44,
				HasApplied: true,
			},
		},
		{
			name: "group",
			progress: raftcluster.AppliedProgress{
				NodeID:     "node-a",
				GroupID:    "group-b",
				Term:       7,
				Index:      44,
				HasApplied: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &fakeAppliedIndexBarrierProvider{
				barrier: raftcluster.AppliedIndexReadBarrier{
					NodeID:          "node-a",
					GroupID:         "group-a",
					MinAppliedIndex: 42,
				},
			}
			waiter := &fakeAppliedIndexWaiter{progress: tt.progress}
			client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
				ClusterReadCoordinator: AppliedIndexReadCoordinator{
					BarrierProvider: provider,
					Waiter:          waiter,
				},
			})
			seedReadCollection(t, mgr)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatalf("Hello: %v", err)
			}

			_, err := client.GetManyWithOptions(ctx, "users", [][]byte{[]byte("u1")}, ReadOptions{ConsistencyPolicy: ConsistencyLeaderRead})
			if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
				t.Fatalf("GetManyWithOptions err=%v want consistency_unavailable", err)
			}
		})
	}
}

func TestAppliedIndexReadCoordinatorProvesLinearizableReadMetadataWithProductionProof(t *testing.T) {
	readIndex := &fakeReadIndexProvider{
		proof: raftcluster.ReadIndexProof{
			NodeID:       "node-a",
			GroupID:      "group-a",
			Term:         7,
			Index:        42,
			HasQuorum:    true,
			EvidenceKind: raftcluster.ReadIndexEvidenceProduction,
		},
	}
	waiter := &fakeAppliedIndexWaiter{
		progress: raftcluster.AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       7,
			Index:      44,
			HasApplied: true,
		},
	}
	target := raftcluster.ReadIndexBarrier{NodeID: "node-a", GroupID: "group-a"}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: AppliedIndexReadCoordinator{
			Waiter:            waiter,
			ReadIndexTarget:   target,
			ReadIndexProvider: readIndex,
		},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	result, err := client.GetManyWithOptions(ctx, "users", [][]byte{[]byte("u1")}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if err != nil {
		t.Fatalf("GetManyWithOptions: %v", err)
	}
	if got, want := result.Present, []bool{true}; !boolSlicesEqual(got, want) {
		t.Fatalf("present=%v want %v", got, want)
	}
	if !result.ReadMeta.Valid ||
		result.ReadMeta.ActualConsistency != ConsistencyLinearizable ||
		result.ReadMeta.ServingNode != "node-a" ||
		result.ReadMeta.LeaderNode != "node-a" ||
		!result.ReadMeta.HasAppliedIndex ||
		result.ReadMeta.AppliedIndex != 44 {
		t.Fatalf("read meta=%+v", result.ReadMeta)
	}
	if len(readIndex.calls) != 1 || readIndex.calls[0] != target {
		t.Fatalf("read index calls=%+v want %+v", readIndex.calls, target)
	}
	wantBarrier := raftcluster.AppliedIndexReadBarrier{
		NodeID:          "node-a",
		GroupID:         "group-a",
		MinAppliedIndex: 42,
	}
	if len(waiter.calls) != 1 || waiter.calls[0] != wantBarrier {
		t.Fatalf("waiter calls=%+v want %+v", waiter.calls, wantBarrier)
	}
}

func TestAppliedIndexReadCoordinatorLinearizableRejectsHarnessReadIndexEvidence(t *testing.T) {
	h, err := raftharness.Open(raftharness.Options{
		RootDir: t.TempDir(),
		GroupID: "group-a",
		Nodes: []raftharness.NodeConfig{
			{ID: "node-a"},
			{ID: "node-b"},
			{ID: "node-c"},
		},
	})
	if err != nil {
		t.Fatalf("Open harness: %v", err)
	}
	defer func() { _ = h.Close() }()

	entry := nativewireHarnessCommittedCreateCollectionEntry(t, 41, 1, "users", "nativewire:harness-read-index:users")
	evidence, err := h.Commit(entry)
	if err != nil {
		t.Fatalf("Commit harness entry: %v evidence=%+v", err, evidence)
	}
	node, ok := h.Node("node-b")
	if !ok {
		t.Fatal("node-b not found")
	}
	if last, ok := node.LastApplied(); ok {
		t.Fatalf("node-b last applied before read=%+v, want none", last)
	}

	target := raftcluster.ReadIndexBarrier{NodeID: "node-b", GroupID: "group-a"}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: AppliedIndexReadCoordinator{
			Waiter:            h.ReadBarrier("node-b"),
			ReadIndexTarget:   target,
			ReadIndexProvider: h.ReadIndexProvider("node-b"),
		},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	_, err = client.GetManyWithOptions(ctx, "users", [][]byte{[]byte("u1")}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
		t.Fatalf("GetManyWithOptions err=%v want consistency_unavailable", err)
	}
	if last, ok := node.LastApplied(); ok {
		t.Fatalf("node-b last applied after failed read=%+v, want none", last)
	}
}

func TestAppliedIndexReadCoordinatorLinearizableRejectsFollowerReadIndexWithoutWaiting(t *testing.T) {
	readIndex := &fakeReadIndexProvider{err: raftcluster.ErrNotLeader}
	waiter := &fakeAppliedIndexWaiter{
		progress: raftcluster.AppliedProgress{
			NodeID:     "node-b",
			GroupID:    "group-a",
			Term:       7,
			Index:      44,
			HasApplied: true,
		},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: AppliedIndexReadCoordinator{
			Waiter:            waiter,
			ReadIndexTarget:   raftcluster.ReadIndexBarrier{NodeID: "node-b", GroupID: "group-a"},
			ReadIndexProvider: readIndex,
		},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	_, err := client.GetManyWithOptions(ctx, "users", [][]byte{[]byte("u1")}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
		t.Fatalf("GetManyWithOptions err=%v want consistency_unavailable", err)
	}
	if len(readIndex.calls) != 1 {
		t.Fatalf("read index calls=%d want 1", len(readIndex.calls))
	}
	if len(waiter.calls) != 0 {
		t.Fatalf("waiter calls=%d want none after follower read-index rejection", len(waiter.calls))
	}
}

func TestAppliedIndexReadCoordinatorLocalStaleReadIsLabeledWithoutReadIndex(t *testing.T) {
	readIndex := &fakeReadIndexProvider{err: raftcluster.ErrNotLeader}
	waiter := &fakeAppliedIndexWaiter{}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: AppliedIndexReadCoordinator{
			Waiter:            waiter,
			ReadIndexProvider: readIndex,
		},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	result, err := client.GetManyWithOptions(ctx, "users", [][]byte{[]byte("u1")}, ReadOptions{ConsistencyPolicy: ConsistencyLocalStale})
	if err != nil {
		t.Fatalf("GetManyWithOptions local stale: %v", err)
	}
	if !result.ReadMeta.Valid || result.ReadMeta.ActualConsistency != ConsistencyLocalStale {
		t.Fatalf("read meta=%+v want local-stale", result.ReadMeta)
	}
	if result.ReadMeta.ServingNode != "" || result.ReadMeta.LeaderNode != "" || result.ReadMeta.HasAppliedIndex {
		t.Fatalf("local stale read meta unexpectedly reported strong-read fields: %+v", result.ReadMeta)
	}
	if len(readIndex.calls) != 0 || len(waiter.calls) != 0 {
		t.Fatalf("read index calls=%d waiter calls=%d want none for local-stale", len(readIndex.calls), len(waiter.calls))
	}
}

func TestAppliedIndexReadCoordinatorLinearizableFailsClosedWithoutReadIndexProvider(t *testing.T) {
	waiter := &fakeAppliedIndexWaiter{
		progress: raftcluster.AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       7,
			Index:      44,
			HasApplied: true,
		},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: AppliedIndexReadCoordinator{Waiter: waiter},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	_, err := client.IndexLookupWithOptions(ctx, "users", "email", "ada@example.com", CursorLimits{}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
		t.Fatalf("IndexLookupWithOptions err=%v want consistency_unavailable", err)
	}
	if len(waiter.calls) != 0 {
		t.Fatalf("waiter calls=%d want none", len(waiter.calls))
	}
}

func TestAppliedIndexReadCoordinatorLinearizableFailsClosedWithoutQuorumProof(t *testing.T) {
	readIndex := &fakeReadIndexProvider{
		proof: raftcluster.ReadIndexProof{
			NodeID:       "node-a",
			GroupID:      "group-a",
			Term:         7,
			Index:        42,
			EvidenceKind: raftcluster.ReadIndexEvidenceProduction,
		},
	}
	waiter := &fakeAppliedIndexWaiter{}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: AppliedIndexReadCoordinator{
			Waiter:            waiter,
			ReadIndexProvider: readIndex,
		},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	_, err := client.GetManyWithOptions(ctx, "users", [][]byte{[]byte("u1")}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
		t.Fatalf("GetManyWithOptions err=%v want consistency_unavailable", err)
	}
	if len(readIndex.calls) != 1 {
		t.Fatalf("read index calls=%d want 1", len(readIndex.calls))
	}
	if len(waiter.calls) != 0 {
		t.Fatalf("waiter calls=%d want none", len(waiter.calls))
	}
}

func TestAppliedIndexReadCoordinatorLinearizableFailsClosedForTargetMismatch(t *testing.T) {
	tests := []struct {
		name     string
		progress raftcluster.AppliedProgress
	}{
		{
			name: "node",
			progress: raftcluster.AppliedProgress{
				NodeID:     "node-b",
				GroupID:    "group-a",
				Term:       7,
				Index:      44,
				HasApplied: true,
			},
		},
		{
			name: "group",
			progress: raftcluster.AppliedProgress{
				NodeID:     "node-a",
				GroupID:    "group-b",
				Term:       7,
				Index:      44,
				HasApplied: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readIndex := &fakeReadIndexProvider{
				proof: raftcluster.ReadIndexProof{
					NodeID:       "node-a",
					GroupID:      "group-a",
					Term:         7,
					Index:        42,
					HasQuorum:    true,
					EvidenceKind: raftcluster.ReadIndexEvidenceProduction,
				},
			}
			waiter := &fakeAppliedIndexWaiter{progress: tt.progress}
			client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
				ClusterReadCoordinator: AppliedIndexReadCoordinator{
					Waiter:            waiter,
					ReadIndexProvider: readIndex,
				},
			})
			seedReadCollection(t, mgr)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatalf("Hello: %v", err)
			}

			_, err := client.GetManyWithOptions(ctx, "users", [][]byte{[]byte("u1")}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
			if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
				t.Fatalf("GetManyWithOptions err=%v want consistency_unavailable", err)
			}
			if len(readIndex.calls) != 1 || len(waiter.calls) != 1 {
				t.Fatalf("read index calls=%d waiter calls=%d want one each", len(readIndex.calls), len(waiter.calls))
			}
		})
	}
}

func TestAppliedIndexReadCoordinatorLinearizableFailsClosedForLocalApplyLag(t *testing.T) {
	readIndex := &fakeReadIndexProvider{
		proof: raftcluster.ReadIndexProof{
			NodeID:       "node-a",
			GroupID:      "group-a",
			Term:         7,
			Index:        42,
			HasQuorum:    true,
			EvidenceKind: raftcluster.ReadIndexEvidenceProduction,
		},
	}
	waiter := &fakeAppliedIndexWaiter{
		progress: raftcluster.AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       7,
			Index:      41,
			HasApplied: true,
		},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: AppliedIndexReadCoordinator{
			Waiter:            waiter,
			ReadIndexProvider: readIndex,
		},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	_, err := client.IndexLookupWithOptions(ctx, "users", "email", "ada@example.com", CursorLimits{}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
		t.Fatalf("IndexLookupWithOptions err=%v want consistency_unavailable", err)
	}
	if len(readIndex.calls) != 1 || len(waiter.calls) != 1 {
		t.Fatalf("read index calls=%d waiter calls=%d want one each", len(readIndex.calls), len(waiter.calls))
	}
}

func TestAppliedIndexReadCoordinatorKeepsLeaseReadFailClosed(t *testing.T) {
	readIndex := &fakeReadIndexProvider{
		proof: raftcluster.ReadIndexProof{
			NodeID:       "node-a",
			GroupID:      "group-a",
			Term:         7,
			Index:        42,
			HasQuorum:    true,
			EvidenceKind: raftcluster.ReadIndexEvidenceProduction,
		},
	}
	waiter := &fakeAppliedIndexWaiter{
		progress: raftcluster.AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Term:       7,
			Index:      44,
			HasApplied: true,
		},
	}
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		ClusterReadCoordinator: AppliedIndexReadCoordinator{
			Waiter:            waiter,
			ReadIndexProvider: readIndex,
		},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	_, err := client.IndexLookupWithOptions(ctx, "users", "email", "ada@example.com", CursorLimits{}, ReadOptions{ConsistencyPolicy: ConsistencyLeaseRead})
	if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
		t.Fatalf("IndexLookupWithOptions err=%v want consistency_unavailable", err)
	}
	if len(readIndex.calls) != 0 || len(waiter.calls) != 0 {
		t.Fatalf("read index calls=%d waiter calls=%d want none", len(readIndex.calls), len(waiter.calls))
	}
}

func TestAppliedIndexReadCoordinatorRejectsIncompleteLeaderReadBarrier(t *testing.T) {
	tests := []struct {
		name    string
		barrier raftcluster.AppliedIndexReadBarrier
	}{
		{
			name: "missing-node",
			barrier: raftcluster.AppliedIndexReadBarrier{
				GroupID:         "group-a",
				MinAppliedIndex: 42,
			},
		},
		{
			name: "missing-group",
			barrier: raftcluster.AppliedIndexReadBarrier{
				NodeID:          "node-a",
				MinAppliedIndex: 42,
			},
		},
		{
			name: "missing-min-index",
			barrier: raftcluster.AppliedIndexReadBarrier{
				NodeID:  "node-a",
				GroupID: "group-a",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &fakeAppliedIndexBarrierProvider{barrier: tt.barrier}
			waiter := &fakeAppliedIndexWaiter{
				progress: raftcluster.AppliedProgress{
					NodeID:     "node-a",
					GroupID:    "group-a",
					Term:       7,
					Index:      44,
					HasApplied: true,
				},
			}
			_, err := (AppliedIndexReadCoordinator{
				BarrierProvider: provider,
				Waiter:          waiter,
			}).CoordinateRead(context.Background(), ClusterReadRequest{Policy: ConsistencyLeaderRead})
			if err == nil {
				t.Fatal("CoordinateRead err=<nil> want fail-closed error")
			}
			if len(waiter.calls) != 0 {
				t.Fatalf("waiter calls=%d want none", len(waiter.calls))
			}
		})
	}
}

func TestAppliedIndexReadCoordinatorPropagatesWaiterFailure(t *testing.T) {
	provider := &fakeAppliedIndexBarrierProvider{
		barrier: raftcluster.AppliedIndexReadBarrier{
			NodeID:          "node-a",
			GroupID:         "group-a",
			MinAppliedIndex: 42,
		},
	}
	waiterErr := errors.New("wait unavailable")
	waiter := &fakeAppliedIndexWaiter{err: waiterErr}
	_, err := (AppliedIndexReadCoordinator{
		BarrierProvider: provider,
		Waiter:          waiter,
	}).CoordinateRead(context.Background(), ClusterReadRequest{Policy: ConsistencyLeaderRead})
	if !errors.Is(err, waiterErr) {
		t.Fatalf("CoordinateRead err=%v want waiter error", err)
	}
}

const nativewireHarnessCatalogVersionStart = 7

func nativewireHarnessCommittedCreateCollectionEntry(t *testing.T, term, index uint64, collection, idempotency string) raftfsm.CommittedEntryV1 {
	t.Helper()
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMeta(collections.CollectionMeta{
			Name: collection,
			Options: collections.CollectionOptions{
				DocumentFormat: collections.DocumentFormatJSON,
			},
		})},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, nativewireHarnessCatalogVersionStart)},
	}
	cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	raw, err := iwire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return raftfsm.CommittedEntryV1{
		Type:                     raftfsm.EntryTypeCommandEntryV1,
		Term:                     term,
		Index:                    index,
		Bytes:                    raw,
		CurrentCatalogVersion:    nativewireHarnessCatalogVersionStart,
		HasCurrentCatalogVersion: true,
	}
}
