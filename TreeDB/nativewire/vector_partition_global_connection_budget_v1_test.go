package nativewire

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestManyIdleEndpointsRespectGlobalConnectionBudgetV1(t *testing.T) {
	// A default dispatcher retains at most 64 connections across every
	// endpoint. Visiting more owners must evict idle connections and keep
	// making progress; a per-endpoint cap alone cannot meet this contract.
	const connectionBudget = 64
	const endpointCount = connectionBudget + 32
	endpoints := make(map[raftcluster.GroupID]string, endpointCount)
	for i := 0; i < endpointCount; i++ {
		group := raftcluster.GroupID(fmt.Sprintf("group-%03d", i))
		listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
			return VectorPartitionShardSearchResponseV1{Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID}, nil
		}))
		endpoints[group] = listener.Addr().String()
	}
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < endpointCount; i++ {
		request := VectorPartitionShardSearchRequestV1{TargetGroupID: raftcluster.GroupID(fmt.Sprintf("group-%03d", i)), RequestID: fmt.Sprintf("request-%03d", i)}
		response, err := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, request)
		if err != nil || response.RequestID != request.RequestID {
			t.Fatalf("owner %d did not make progress: response=%+v error=%v", i, response, err)
		}
	}
	dispatcher.mu.Lock()
	retained := 0
	for _, pool := range dispatcher.pools {
		pool.mu.Lock()
		retained += len(pool.all)
		pool.mu.Unlock()
	}
	dispatcher.mu.Unlock()
	if retained > connectionBudget {
		t.Fatalf("retained %d connections after visiting %d owners; global budget is %d", retained, endpointCount, connectionBudget)
	}
}
