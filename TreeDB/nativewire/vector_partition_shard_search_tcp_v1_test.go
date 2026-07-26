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

func TestVectorPartitionShardSearchTCPDispatcherCancellationWithoutDeadlineV1(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		close(started)
		<-release
		return VectorPartitionShardSearchResponseV1{}, nil
	}))
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan struct {
		response VectorPartitionShardSearchResponseV1
		err      error
	}, 1)
	go func() {
		response, dispatchErr := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a"})
		result <- struct {
			response VectorPartitionShardSearchResponseV1
			err      error
		}{response, dispatchErr}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server handler did not start")
	}
	cancel()
	select {
	case got := <-result:
		if got.response.Version != 0 || got.response.RequestID != "" || len(got.response.Partials) != 0 || got.response.Partitions != 0 {
			t.Fatalf("cancellation returned partial response: %+v", got.response)
		}
		var shardErr *VectorPartitionShardSearchErrorV1
		if !errors.As(got.err, &shardErr) || shardErr.Code != VectorPartitionShardSearchErrorCanceledV1 || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("error=%v, want typed cancellation", got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not interrupt TCP read")
	}
}

func TestVectorPartitionShardSearchTCPServerUsesRequestDeadlineV1(t *testing.T) {
	observed := make(chan error, 1)
	listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(ctx context.Context, _ VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		<-ctx.Done()
		observed <- ctx.Err()
		return VectorPartitionShardSearchResponseV1{}, ctx.Err()
	}))
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(25 * time.Millisecond)
	_, err = dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", DeadlineUnixNano: deadline.UnixNano()})
	var shardErr *VectorPartitionShardSearchErrorV1
	if !errors.As(err, &shardErr) || shardErr.Code != VectorPartitionShardSearchErrorDeadlineV1 {
		t.Fatalf("error=%v, want typed deadline", err)
	}
	select {
	case err := <-observed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("handler context error=%v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not observe request deadline")
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
