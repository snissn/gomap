package nativewire

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

type vectorPartitionShardSearchHandlerFuncV1 func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error)

func (f vectorPartitionShardSearchHandlerFuncV1) Search(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
	return f(ctx, request)
}

func TestVectorPartitionShardSearchTCPDispatcherRoundTripV1(t *testing.T) {
	listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		if request.RequestID != "m8-round-trip" || request.TargetGroupID != "group-a" || request.TargetNodeID != "node-a" || len(request.PartitionIDs) != 1 || request.PartitionIDs[0] != 3 {
			t.Fatalf("unexpected serialized request: %+v", request)
		}
		return VectorPartitionShardSearchResponseV1{
			Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID,
			Proof:      VectorPartitionShardSearchProofV1{ServingNode: "node-a", LeaderNode: "node-a", GroupID: "group-a", ReadySetDigest: request.ReadySetDigest, ReadTerm: 2, ReadIndex: 9, AppliedTerm: 2, AppliedIndex: 9, SourceGeneration: request.SourceGeneration, SourceChecksum: request.SourceChecksum, SourceSchemaHash: request.SourceSchemaHash, SourceRowCount: request.SourceRowCount, PartitionGeneration: request.PartitionGeneration, RouterGeneration: request.RouterGeneration},
			Partitions: 1,
		}, nil
	}))
	defer listener.Close()
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	response, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{
		Version: VectorPartitionShardSearchVersionV1, RequestID: "m8-round-trip", CancellationID: "cancel", Database: "default", Catalog: "default", Collection: "users", IndexName: "embedding", IndexDefinitionDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", ReadySetDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 5, RouterGeneration: 6, TargetGroupID: "group-a", TargetNodeID: "node-a", PartitionIDs: []uint32{3}, Query: []float32{1, 0}, Metric: VectorPartitionShardSearchMetricCosineV1, Mode: VectorPartitionShardSearchModeNoDocumentV1, Consistency: VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: VectorPartitionShardSearchStatsBasicV1, TopK: 1, EfSearch: 1, RequestBytesLimit: 1024, CandidateBytesLimit: 1024, ResponseBytesLimit: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.RequestID != "m8-round-trip" || response.Proof.GroupID != "group-a" || response.Proof.AppliedIndex != 9 {
		t.Fatalf("response=%+v", response)
	}
}

func TestVectorPartitionShardSearchTCPDispatcherPreservesTypedServiceErrorV1(t *testing.T) {
	listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorNotLeaderV1, GroupID: "group-a", LeaderHint: "node-b", Err: errors.New("leader moved")}
	}))
	defer listener.Close()
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	response, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a"})
	if response.Version != 0 || response.RequestID != "" || len(response.Partials) != 0 || response.Partitions != 0 {
		t.Fatalf("error returned partial response: %+v", response)
	}
	var shardErr *VectorPartitionShardSearchErrorV1
	if !errors.As(err, &shardErr) || shardErr.Code != VectorPartitionShardSearchErrorNotLeaderV1 || shardErr.LeaderHint != "node-b" {
		t.Fatalf("error=%v does not preserve M5 class and redirect", err)
	}
}

func newVectorPartitionShardSearchTCPListenerV1(t *testing.T, handler VectorPartitionShardSearchHandlerV1) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go (VectorPartitionShardSearchTCPServerV1{Service: handler}).ServeConn(context.Background(), conn)
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func TestVectorPartitionShardSearchTCPFrameRejectsOversizeV1(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	done := make(chan error, 1)
	go func() {
		var prefix [4]byte
		prefix[3] = 5
		_, err := right.Write(prefix[:])
		done <- err
	}()
	_, err := readVectorPartitionShardSearchTCPFrameV1(left, 4)
	if err == nil {
		t.Fatal("oversized frame was accepted")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer did not finish")
	}
}
