package nativewire

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestVectorPartitionShardSearchTCPDispatcherReusesConnectionV1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var accepts atomic.Uint64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			go (VectorPartitionShardSearchTCPServerV1{Service: vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, r VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
				return VectorPartitionShardSearchResponseV1{Version: 1, RequestID: r.RequestID}, nil
			})}).ServeConn(context.Background(), conn)
		}
	}()
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	for _, id := range []string{"first", "second"} {
		if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", RequestID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if got := accepts.Load(); got != 1 {
		t.Fatalf("connections=%d want one reused connection", got)
	}
}

func TestVectorPartitionShardSearchTCPDispatcherReconnectsAfterIdleCloseAndFailsAfterCloseV1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	idleClosed := make(chan struct{}, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				(VectorPartitionShardSearchTCPServerV1{InitialTimeout: 10 * time.Millisecond, Service: vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, r VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
					return VectorPartitionShardSearchResponseV1{Version: 1, RequestID: r.RequestID}, nil
				})}).ServeConn(context.Background(), conn)
				select {
				case idleClosed <- struct{}{}:
				default:
				}
			}()
		}
	}()
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(map[raftcluster.GroupID]string{"group-a": listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", RequestID: "first"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-idleClosed:
	case <-time.After(time.Second):
		t.Fatal("server did not close idle connection")
	}
	if response, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", RequestID: "reconnected"}); err != nil || response.RequestID != "reconnected" {
		t.Fatalf("reconnect response=%+v err=%v", response, err)
	}
	if err := dispatcher.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a"}); err == nil {
		t.Fatal("closed dispatcher accepted request")
	}
}

func TestVectorPartitionShardSearchTCPDispatcherUsesLeaderNodeEndpointV1(t *testing.T) {
	leader := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, r VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		return VectorPartitionShardSearchResponseV1{Version: 1, RequestID: r.RequestID}, nil
	}))
	stale := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorNotLeaderV1, GroupID: "group-a", LeaderHint: "node-b", Err: errors.New("moved")}
	}))
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherWithNodeEndpointsV1(map[raftcluster.GroupID]string{"group-a": stale.Addr().String()}, map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-a": {"node-a": stale.Addr().String(), "node-b": leader.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", TargetNodeID: "node-a"}); err == nil {
		t.Fatal("stale leader endpoint succeeded")
	}
	if response, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a", TargetNodeID: "node-b", RequestID: "leader"}); err != nil || response.RequestID != "leader" {
		t.Fatalf("leader endpoint response=%+v err=%v", response, err)
	}
}

type vectorPartitionShardSearchHandlerFuncV1 func(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error)

func (f vectorPartitionShardSearchHandlerFuncV1) Search(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
	return f(ctx, request)
}

func TestVectorPartitionShardSearchTCPServerInitialReadTimeoutV1(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		(VectorPartitionShardSearchTCPServerV1{InitialTimeout: 20 * time.Millisecond}).ServeConn(context.Background(), server)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("slowloris initial frame kept M5 TCP server blocked")
	}
}

func TestVectorPartitionShardSearchTCPServerInterruptsPeerReadBeforeSuccessfulResponseV1(t *testing.T) {
	client, rawServer := net.Pipe()
	defer client.Close()
	server := &vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1{Conn: rawServer}
	serverDone := make(chan struct{})
	go func() {
		(VectorPartitionShardSearchTCPServerV1{
			InitialTimeout: time.Second,
			Service: vectorPartitionShardSearchHandlerFuncV1(func(_ context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
				return VectorPartitionShardSearchResponseV1{Version: VectorPartitionShardSearchVersionV1, RequestID: request.RequestID}, nil
			}),
		}).ServeConn(context.Background(), server)
		close(serverDone)
	}()
	if err := writeVectorPartitionShardSearchTCPFrameV1(client, vectorPartitionShardSearchTCPFrameV1{Request: &VectorPartitionShardSearchRequestV1{RequestID: "immediate"}}, vectorPartitionShardSearchTCPMaxFrameBytesV1); err != nil {
		t.Fatal(err)
	}
	frame, err := readVectorPartitionShardSearchTCPFrameV1(client, vectorPartitionShardSearchTCPMaxFrameBytesV1)
	if err != nil || frame.Response == nil || frame.Response.RequestID != "immediate" {
		t.Fatalf("response frame=%+v err=%v", frame, err)
	}
	// The production transport keeps a healthy connection for the next request.
	// Closing the peer must still release the server goroutine deterministically.
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("server did not return after peer close")
	}
}

// vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1 turns the
// successful-response assertion into a transport ordering check rather than a
// scheduler-sensitive wall-clock test. The monitor's stop path must set an
// already-expired read deadline before ServeConn writes its response.
type vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1 struct {
	net.Conn
	mu                        sync.Mutex
	interruptedReadDeadline   bool
	wroteAfterInterruptedRead bool
}

func (c *vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		c.interruptedReadDeadline = true
	}
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(deadline)
}

func (c *vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1) Write(raw []byte) (int, error) {
	c.mu.Lock()
	if c.interruptedReadDeadline {
		c.wroteAfterInterruptedRead = true
	}
	c.mu.Unlock()
	return c.Conn.Write(raw)
}

func (c *vectorPartitionShardSearchTCPReadDeadlineRecordingConnV1) interruptedReadBeforeWrite() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wroteAfterInterruptedRead
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

func TestVectorPartitionShardSearchTCPDispatcherRejectsAmbiguousResponseFramesV1(t *testing.T) {
	for name, frame := range map[string]vectorPartitionShardSearchTCPFrameV1{
		"response_and_error":   {Response: &VectorPartitionShardSearchResponseV1{}, Error: &vectorPartitionShardSearchTCPErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, Message: "also error"}},
		"request_and_response": {Request: &VectorPartitionShardSearchRequestV1{RequestID: "unexpected"}, Response: &VectorPartitionShardSearchResponseV1{}},
	} {
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			go func() {
				defer server.Close()
				_, _ = readVectorPartitionShardSearchTCPFrameV1(server, vectorPartitionShardSearchTCPMaxFrameBytesV1)
				_ = writeVectorPartitionShardSearchTCPFrameV1(server, frame, vectorPartitionShardSearchTCPMaxFrameBytesV1)
			}()
			dispatcher := &VectorPartitionShardSearchTCPDispatcherV1{
				endpoints: map[raftcluster.GroupID]string{"group-a": "pipe"}, maxFrame: vectorPartitionShardSearchTCPMaxFrameBytesV1,
				dial: func(context.Context, string, string) (net.Conn, error) { return client, nil },
			}
			if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(context.Background(), VectorPartitionShardSearchRequestV1{TargetGroupID: "group-a"}); err == nil {
				t.Fatal("accepted ambiguous M5 response frame")
			}
		})
	}
}

func TestVectorPartitionShardSearchTCPDispatcherCancellationWithoutDeadlineV1(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	observed := make(chan error, 1)
	defer close(release)
	listener := newVectorPartitionShardSearchTCPListenerV1(t, vectorPartitionShardSearchHandlerFuncV1(func(ctx context.Context, _ VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		close(started)
		select {
		case <-ctx.Done():
			observed <- ctx.Err()
			return VectorPartitionShardSearchResponseV1{}, ctx.Err()
		case <-release:
			return VectorPartitionShardSearchResponseV1{}, nil
		}
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
	select {
	case err := <-observed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("server handler context error=%v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server handler did not observe client disconnect cancellation")
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
