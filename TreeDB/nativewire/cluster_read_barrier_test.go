package nativewire

import (
	"context"
	"errors"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
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

func TestAppliedIndexReadCoordinatorKeepsLinearizableFailClosed(t *testing.T) {
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

	_, err := client.IndexLookupWithOptions(ctx, "users", "email", "ada@example.com", CursorLimits{}, ReadOptions{ConsistencyPolicy: ConsistencyLinearizable})
	if !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
		t.Fatalf("IndexLookupWithOptions err=%v want consistency_unavailable", err)
	}
	if len(provider.calls) != 0 || len(waiter.calls) != 0 {
		t.Fatalf("provider calls=%d waiter calls=%d want none", len(provider.calls), len(waiter.calls))
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
