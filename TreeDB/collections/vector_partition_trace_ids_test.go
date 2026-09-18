package collections

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestVectorPartitionOfflineTraceIDsObserveCancellationAndReleasePin(t *testing.T) {
	const rows = 1024
	input := testVectorPartitionLocalSearcherDisconnectedHNSWInputV1(rows)
	raw, err := encodeColumnHNSWSearchPack(input)
	if err != nil {
		t.Fatal(err)
	}
	view, handle := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, input.BaseIdentity)
	s := &VectorPartitionLocalSearcherV1{asset: VectorPartitionSearchAssetV1{Generation: 11, PartitionID: 2, Dimensions: 1}, prepared: view, opened: 1}
	t.Cleanup(func() { _ = s.Close() })
	for _, poll := range []int{1, 3, 4} {
		ctx := &vectorPartitionLocalSearcherDeadlineAfterErrContextV1{Context: context.Background(), deadlineAfter: poll}
		ids, err := s.PackDocumentIDsForOfflineTraceWithContextV1(ctx)
		if !errors.Is(err, context.DeadlineExceeded) || ids != nil || ctx.calls != poll {
			t.Fatalf("poll=%d calls=%d ids=%d err=%v", poll, ctx.calls, len(ids), err)
		}
		if s.Status().ActivePins != 0 {
			t.Fatal("cancellation leaked a reader pin")
		}
	}
	ids, err := s.PackDocumentIDsForOfflineTraceWithContextV1(nil)
	if err != nil || len(ids) != rows {
		t.Fatalf("retry IDs=%d err=%v", len(ids), err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !handle.Released() || ids[rows-1] != "x" {
		t.Fatal("returned IDs were not owned across Close")
	}
}

func TestVectorPartitionOfflineTraceIDsCancellationBeforeAllocation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A huge shape with no reader is intentional: cancellation must be checked
	// before inspecting a pack or allocating its entire ordinal map.
	var s *VectorPartitionLocalSearcherV1
	ids, err := s.PackDocumentIDsForOfflineTraceWithContextV1(ctx)
	if !errors.Is(err, context.Canceled) || ids != nil {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
}
