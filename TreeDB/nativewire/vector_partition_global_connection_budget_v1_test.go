package nativewire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
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

func TestPeerShardBudgetRefusalPrecedesDialV1(t *testing.T) {
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"owner": "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	var dials atomic.Int64
	dispatcher.dial = func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("unexpected dial")
	}
	// Model all tokens held by active sockets. No idle eviction is possible.
	for i := 0; i < cap(dispatcher.connectionSlots); i++ {
		dispatcher.connectionSlots <- struct{}{}
	}
	request := VectorPartitionShardSearchRequestV1{TargetGroupID: "owner", RequestID: "over-budget"}
	_, err = dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), request)
	if !errors.Is(err, raftcluster.ErrAdmissionUnavailable) || dials.Load() != 0 {
		t.Fatalf("budget refusal must precede dial and reconnect: dials=%d error=%v", dials.Load(), err)
	}
	for i := 0; i < cap(dispatcher.connectionSlots); i++ {
		<-dispatcher.connectionSlots
	}
	if len(dispatcher.requestSlots) != 0 {
		t.Fatal("refused request retained admission")
	}
}

func TestPeerShardCloseCancelsPendingDialV1(t *testing.T) {
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"owner": "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	started := make(chan struct{})
	dispatcher.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		_, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "owner"})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dial did not begin")
	}
	if err := dispatcher.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed dispatcher completed a search")
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher close did not interrupt pending dial")
	}
	if len(dispatcher.connectionSlots) != 0 || len(dispatcher.requestSlots) != 0 {
		t.Fatal("closed pending dial retained admission")
	}
}

func TestHotGroupCannotExhaustUnrelatedGroupAdmissionV1(t *testing.T) {
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	hot := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		select {
		case <-release:
			return VectorPartitionShardSearchResponseV1{Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID}, nil
		case <-ctx.Done():
			return VectorPartitionShardSearchResponseV1{}, ctx.Err()
		}
	}))
	cold := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		return VectorPartitionShardSearchResponseV1{Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID}, nil
	}))
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"hot": hot.Addr().String(), "cold": cold.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	count := cap(dispatcher.groupRequests["hot"])
	done := make(chan error, count)
	for i := 0; i < count; i++ {
		go func(i int) {
			_, err := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, VectorPartitionShardSearchRequestV1{TargetGroupID: "hot", RequestID: fmt.Sprintf("hot-%d", i)})
			done <- err
		}(i)
	}
	fixedPeerWaitV1(t, ctx, func() bool { return len(dispatcher.groupRequests["hot"]) == count })
	if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, VectorPartitionShardSearchRequestV1{TargetGroupID: "hot", RequestID: "overflow"}); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("hot group overflow=%v", err)
	}
	response, err := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, VectorPartitionShardSearchRequestV1{TargetGroupID: "cold", RequestID: "independent"})
	if err != nil || response.RequestID != "independent" {
		t.Fatalf("hot group starved unrelated owner: %+v error=%v", response, err)
	}
	close(release)
	for i := 0; i < count; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
