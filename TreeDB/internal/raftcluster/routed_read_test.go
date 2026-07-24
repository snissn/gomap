package raftcluster

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type recordingRoutedReadIndexProvider struct {
	name   string
	events *[]string
	proof  ReadIndexProof
	calls  []ReadIndexBarrier
}

func (p *recordingRoutedReadIndexProvider) ReadIndex(_ context.Context, target ReadIndexBarrier) (ReadIndexProof, error) {
	p.calls = append(p.calls, target)
	if p.events != nil {
		*p.events = append(*p.events, p.name+":read-index")
	}
	return p.proof, nil
}

type recordingRoutedReadWaiter struct {
	name     string
	events   *[]string
	progress AppliedProgress
	calls    []AppliedIndexReadBarrier
}

type benchmarkRoutedReadIndexProvider struct {
	proof ReadIndexProof
}

func (p benchmarkRoutedReadIndexProvider) ReadIndex(context.Context, ReadIndexBarrier) (ReadIndexProof, error) {
	return p.proof, nil
}

type benchmarkRoutedReadWaiter struct {
	progress AppliedProgress
}

func (w benchmarkRoutedReadWaiter) WaitAppliedIndex(context.Context, AppliedIndexReadBarrier) (AppliedProgress, error) {
	return w.progress, nil
}

func (w *recordingRoutedReadWaiter) WaitAppliedIndex(_ context.Context, barrier AppliedIndexReadBarrier) (AppliedProgress, error) {
	w.calls = append(w.calls, barrier)
	if w.events != nil {
		*w.events = append(*w.events, w.name+":wait-applied")
	}
	return w.progress, nil
}

func TestGroupRoutedReadIndexCoordinatorSelectsOwnerAndOrdersReadIndexBeforeApply(t *testing.T) {
	var events []string
	groupAProvider := &recordingRoutedReadIndexProvider{
		name:   "group-a",
		events: &events,
		proof: ReadIndexProof{
			NodeID:       "node-a",
			GroupID:      "group-a",
			Index:        11,
			HasQuorum:    true,
			EvidenceKind: ReadIndexEvidenceProduction,
		},
	}
	groupAWaiter := &recordingRoutedReadWaiter{
		name:   "group-a",
		events: &events,
		progress: AppliedProgress{
			NodeID:     "node-a",
			GroupID:    "group-a",
			Index:      11,
			HasApplied: true,
		},
	}
	groupBProvider := &recordingRoutedReadIndexProvider{
		name:   "group-b",
		events: &events,
		proof: ReadIndexProof{
			NodeID:       "node-c",
			GroupID:      "group-b",
			Term:         7,
			Index:        42,
			HasQuorum:    true,
			EvidenceKind: ReadIndexEvidenceProduction,
		},
	}
	groupBWaiter := &recordingRoutedReadWaiter{
		name:   "group-b",
		events: &events,
		progress: AppliedProgress{
			NodeID:     "node-c",
			GroupID:    "group-b",
			Term:       7,
			Index:      44,
			HasApplied: true,
		},
	}
	coordinator, err := NewGroupRoutedReadIndexCoordinator([]GroupReadIndexCoordinatorV1{
		{GroupID: "group-a", NodeID: "node-a", ReadIndexProvider: groupAProvider, AppliedIndexWaiter: groupAWaiter},
		{GroupID: "group-b", NodeID: "node-c", ReadIndexProvider: groupBProvider, AppliedIndexWaiter: groupBWaiter},
	})
	if err != nil {
		t.Fatalf("NewGroupRoutedReadIndexCoordinator: %v", err)
	}

	proof, progress, err := coordinator.CoordinateRoutedReadIndex(context.Background(), ReadIndexBarrier{
		NodeID:  "node-c",
		GroupID: "group-b",
	})
	if err != nil {
		t.Fatalf("CoordinateRoutedReadIndex: %v", err)
	}
	if proof.NodeID != "node-c" || proof.GroupID != "group-b" || proof.Index != 42 {
		t.Fatalf("proof=%+v want group-b/node-c index 42", proof)
	}
	if progress.NodeID != "node-c" || progress.GroupID != "group-b" || progress.Index != 44 {
		t.Fatalf("progress=%+v want group-b/node-c index 44", progress)
	}
	if len(groupAProvider.calls) != 0 || len(groupAWaiter.calls) != 0 {
		t.Fatalf("wrong owner calls provider=%v waiter=%v want none", groupAProvider.calls, groupAWaiter.calls)
	}
	if !reflect.DeepEqual(events, []string{"group-b:read-index", "group-b:wait-applied"}) {
		t.Fatalf("events=%v want owner read-index then apply wait", events)
	}
}

func TestGroupRoutedReadIndexCoordinatorFailsClosedForStaleOrUnsupportedOwner(t *testing.T) {
	provider := &recordingRoutedReadIndexProvider{
		proof: ReadIndexProof{
			NodeID:       "node-c",
			GroupID:      "group-b",
			Index:        42,
			HasQuorum:    true,
			EvidenceKind: ReadIndexEvidenceProduction,
		},
	}
	waiter := &recordingRoutedReadWaiter{
		progress: AppliedProgress{
			NodeID:     "node-c",
			GroupID:    "group-b",
			Index:      42,
			HasApplied: true,
		},
	}
	coordinator, err := NewGroupRoutedReadIndexCoordinator([]GroupReadIndexCoordinatorV1{
		{GroupID: "group-b", NodeID: "node-c", ReadIndexProvider: provider, AppliedIndexWaiter: waiter},
	})
	if err != nil {
		t.Fatalf("NewGroupRoutedReadIndexCoordinator: %v", err)
	}

	tests := []struct {
		name   string
		target ReadIndexBarrier
		want   error
	}{
		{name: "missing_group", target: ReadIndexBarrier{NodeID: "node-c"}, want: ErrRouteTargetMissing},
		{name: "unknown_group", target: ReadIndexBarrier{NodeID: "node-z", GroupID: "group-z"}, want: ErrRouteTargetUnknown},
		{name: "stale_leader", target: ReadIndexBarrier{NodeID: "node-old", GroupID: "group-b"}, want: ErrReadBarrierTargetMismatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			beforeProvider := len(provider.calls)
			beforeWaiter := len(waiter.calls)
			_, _, err := coordinator.CoordinateRoutedReadIndex(context.Background(), tc.target)
			if !errors.Is(err, tc.want) {
				t.Fatalf("CoordinateRoutedReadIndex err=%v want %v", err, tc.want)
			}
			if len(provider.calls) != beforeProvider || len(waiter.calls) != beforeWaiter {
				t.Fatalf("rejected target reached owner provider/waiter: provider=%v waiter=%v", provider.calls, waiter.calls)
			}
		})
	}
}

// BenchmarkInternalGroupRoutedReadIndexCoordinatorScaffold measures only the
// internal static dispatcher and synthetic proof/apply validation. It is not an
// enabled nativewire read-path, storage observation, network, or quorum benchmark.
func BenchmarkInternalGroupRoutedReadIndexCoordinatorScaffold(b *testing.B) {
	coordinator, err := NewGroupRoutedReadIndexCoordinator([]GroupReadIndexCoordinatorV1{{
		GroupID: "group-b",
		NodeID:  "node-c",
		ReadIndexProvider: benchmarkRoutedReadIndexProvider{proof: ReadIndexProof{
			NodeID:       "node-c",
			GroupID:      "group-b",
			Term:         7,
			Index:        42,
			HasQuorum:    true,
			EvidenceKind: ReadIndexEvidenceProduction,
		}},
		AppliedIndexWaiter: benchmarkRoutedReadWaiter{progress: AppliedProgress{
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
	ctx := context.Background()
	target := ReadIndexBarrier{NodeID: "node-c", GroupID: "group-b"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := coordinator.CoordinateRoutedReadIndex(ctx, target); err != nil {
			b.Fatalf("CoordinateRoutedReadIndex: %v", err)
		}
	}
}
