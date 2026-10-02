package nativewire

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestPeerSecurityDrainCapabilityLifetimeV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	a := transport.admission
	foreign, err := NewPeerTransportV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	parent, err := a.request(context.Background(), "native", 1, peerRequestIngressV1)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.release()
	if deadline, ok := parent.ctx.Deadline(); ok {
		t.Fatalf("admission added a new request deadline: %v", deadline)
	}
	foreignParent, err := foreign.admission.request(context.Background(), "native", 1, peerRequestIngressV1)
	if err != nil {
		t.Fatal(err)
	}
	defer foreignParent.release()
	a.beginDrain()
	if w, err := a.request(foreignParent.ctx, "native", 1, peerRequestDescendantV1); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		w.release()
		t.Fatalf("foreign owner accepted: %v", err)
	}
	// Even a caller that detaches cancellation cannot extend the capability.
	retained := context.WithoutCancel(parent.ctx)
	child, err := a.request(retained, "native", 1, peerRequestDescendantV1)
	if err != nil {
		t.Fatal(err)
	}
	defer child.release()
	parent.release()
	select {
	case <-child.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("descendant outlived originating request")
	}
	if w, err := a.request(retained, "native", 1, peerRequestDescendantV1); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		w.release()
		t.Fatalf("retired capability accepted: %v", err)
	}
	// Internal dependency admission still enforces the ordinary byte budget.
	if w, err := a.request(context.Background(), "shard:"+string(config.Groups[0].ID), 1<<40, peerRequestInternalV1); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		w.release()
		t.Fatalf("internal byte budget bypass: %v", err)
	}
	child.release()
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if w, err := a.request(context.Background(), "native", 1, peerRequestInternalV1); !errors.Is(err, net.ErrClosed) {
		w.release()
		t.Fatalf("closed internal admission: %v", err)
	}
	if current := transport.ResourceStatsV1().Current; current != (peerResourceAmountsV1{}) {
		t.Fatalf("leases: %v", current)
	}
}

func TestPeerSecurityDrainTimeoutCancelsV1(t *testing.T) {
	transport, _ := peerTransportFixtureV1(t)
	defer transport.Close()
	a := transport.admission
	origin, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	work, err := a.request(origin, "native", 1, peerRequestIngressV1)
	if err != nil {
		t.Fatal(err)
	}
	defer work.release()
	retained := context.WithoutCancel(work.ctx)
	select {
	case <-work.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("request timeout did not cancel")
	}
	a.beginDrain()
	if w, err := a.request(retained, "native", 1, peerRequestDescendantV1); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		w.release()
		t.Fatalf("expired capability accepted: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := a.drainRequests(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain timeout: %v", err)
	}
	work.release()
	if current := transport.ResourceStatsV1().Current; current != (peerResourceAmountsV1{}) {
		t.Fatalf("leases: %v", current)
	}
}

func TestPeerSecurityShutdownTimeoutAndForcedCloseV1(t *testing.T) {
	for _, graceful := range []bool{true, false} {
		t.Run(map[bool]string{true: "runtime_timeout", false: "forced_transport"}[graceful], func(t *testing.T) {
			transport, config := peerTransportFixtureV1(t)
			defer transport.Close()
			work, err := transport.admission.request(context.Background(), "native", 1, peerRequestIngressV1)
			if err != nil {
				t.Fatal(err)
			}
			defer work.release()
			finished := make(chan struct{})
			go func() { <-work.ctx.Done(); close(finished) }()
			started := time.Now()
			if graceful {
				control, err := NewFixedPeerTCPClientWithTransportV1(config, transport)
				if err != nil {
					t.Fatal(err)
				}
				control.ownPeerTransport = true
				// Shorten only this shutdown wait; admission must not impose a
				// request timeout or change the shared transport identity.
				config.RequestTimeout = 100 * time.Millisecond
				node := &FixedPeerTCPRuntimeV1{config: config, client: control}
				if err := node.Close(); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("shutdown timeout: %v", err)
				}
			} else if err := transport.Close(); err != nil {
				t.Fatal(err)
			}
			if time.Since(started) > time.Second {
				t.Fatal("close waited for uncooperative request")
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("close did not cancel request")
			}
			work.release()
			if current := transport.ResourceStatsV1().Current; current != (peerResourceAmountsV1{}) {
				t.Fatalf("leases: %v", current)
			}
		})
	}
}

// A speculative authenticated self-dial can lose to a connection returned by
// another request. Its completed TLS socket then sits unused in our own HTTP
// pool, still StateNew to the server. Close must retire that pool before Shutdown.
func TestFixedPeerCloseRetiresUnusedAuthenticatedSelfDialV1(t *testing.T) {
	caller, config := peerTransportFixtureV1(t)
	defer caller.Close()
	node, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	dialed, releaseDial := make(chan struct{}), make(chan struct{})
	var releaseOnce, releaseDialOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	unblockDial := func() { releaseDialOnce.Do(func() { close(releaseDial) }) }
	defer unblock()
	defer unblockDial()
	var handled, dials atomic.Int32
	// Install barriers before opening any HTTP connection to this runtime.
	node.server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node.serve(w, r)
		if handled.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
	transport := node.client.readHTTP.Transport.(*http.Transport)
	dial := transport.DialTLSContext
	transport.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err == nil && dials.Add(1) == 2 {
			close(dialed) // real authenticated handshake completed, no HTTP sent
			select {
			case <-releaseDial:
			case <-ctx.Done():
			}
		}
		return conn, err
	}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := node.client.Status(ctx, config.NodeID); first <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first request did not enter", ctx.Err())
	}
	go func() { _, err := node.client.Status(ctx, config.NodeID); second <- err }()
	select {
	case <-dialed:
	case <-ctx.Done():
		t.Fatal("second authenticated dial did not enter", ctx.Err())
	}
	unblock() // returning the first socket must satisfy the second request
	for _, done := range []<-chan error{first, second} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("request waited behind speculative dial", ctx.Err())
		}
	}
	unblockDial()
	if current := node.client.peerTransport.ResourceStatsV1().Current[peerRequestsV1]; current != 0 {
		t.Fatalf("completed status calls retained %d admitted requests", current)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close retained its unused authenticated self-dial: %v", err)
	}
	if current := node.client.peerTransport.ResourceStatsV1().Current; current != (peerResourceAmountsV1{}) {
		t.Fatalf("Close retained resources: %v", current)
	}
}

// TLS handshake completion does not imply an HTTP request was sent. Such a
// caller-owned socket is still StateNew to net/http and can outlive a recipient's
// graceful shutdown budget. Fixture teardown must retire callers first.
func TestFixedPeerAuthenticatedControlSocketCleanupOrderV1(t *testing.T) {
	for _, callerFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "recipient-first-times-out", true: "caller-first-drains"}[callerFirst], func(t *testing.T) {
			caller, config := peerTransportFixtureV1(t)
			defer caller.Close()
			recipient, err := OpenFixedPeerTCPRuntimeV1(config)
			if err != nil {
				t.Fatal(err)
			}
			defer recipient.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			conn, err := caller.dialScope(ctx, config.ListenAddress, config.NodeID, "control")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			// Bound only Close's test budget; the opened HTTP server keeps its
			// ordinary header timeout, so expiry cannot race this characterization.
			if !callerFirst {
				recipient.config.RequestTimeout = 100 * time.Millisecond
			}
			if callerFirst {
				if err := caller.Close(); err != nil {
					t.Fatal(err)
				}
			}
			err = recipient.Close()
			if callerFirst && err != nil {
				t.Fatalf("caller-first shutdown: %v", err)
			}
			if !callerFirst && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("recipient-first shutdown: %v", err)
			}
		})
	}
}

// Only the search adapter is stubbed; native vector framing/deadlines, service,
// coordinator planning/workers and authenticated socket dispatch are real.
type peerDrainVectorBackendV1 struct {
	public.BackendV1
	coordinator *VectorPartitionCoordinatorV1
}

func (b peerDrainVectorBackendV1) SearchVectorPartitionV1(ctx context.Context, request public.SearchRequestV1) (public.SearchResponseV1, error) {
	r := testVectorPartitionCoordinatorRequestV1(request.Probes)
	r.Query, r.TopK, r.EfSearch = request.Query, request.TopK, request.EfSearch
	r.RequestBytesLimit, r.CandidateBytesLimit, r.ResponseBytesLimit, r.MergeEntriesLimit = request.Limits.RequestBytes, request.Limits.CandidateBytes, request.Limits.ResponseBytes, request.Limits.MergeEntries
	r.DeadlineUnixNano = request.Deadline.UnixNano()
	response, err := b.coordinator.Search(ctx, r)
	result := public.SearchResponseV1{Generation: request.Generation}
	for _, neighbor := range response.Neighbors {
		result.Neighbors = append(result.Neighbors, public.NeighborV1{ID: neighbor.ID, Score: neighbor.Score})
	}
	return result, err
}

func TestPeerSecurityDrainNativeVectorSelfFanoutV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listen := func() net.Listener {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Close() })
		return l
	}
	shardListener, nativeListener := listen(), listen()
	group := config.Groups[0].ID
	coordinator, source, responses := testVectorPartitionCoordinatorV1(t,
		[]raftplacement.GroupV1{{ID: group, Members: []raftcluster.NodeID{config.NodeID}, LeaderHint: config.NodeID}},
		[]raftcluster.GroupID{group, group},
		map[uint32][]VectorPartitionShardSearchNeighborV1{0: {{ID: "a", Score: .1}}, 1: {{ID: "b", Score: .2}}},
		VectorPartitionCoordinatorLimitsV1{MaxPartitionsPerRequest: 1, MaxConcurrentRequests: 2})
	defer coordinator.Close()
	accepted, release := make(chan struct{}, 1), make(chan struct{})
	source.openStarted, source.openBlock = accepted, release
	var calls atomic.Int64
	bothShards := make(chan struct{})
	shard := VectorPartitionShardSearchTCPServerV1{PeerTransport: transport, PeerGroupID: group,
		Service: vectorPartitionShardSearchHandlerFuncV1(func(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
			if calls.Add(1) == 2 {
				close(bothShards)
			}
			select {
			case <-bothShards:
			case <-ctx.Done():
				return VectorPartitionShardSearchResponseV1{}, ctx.Err()
			}
			return responses.DispatchVectorPartitionShardSearchV1(ctx, request)
		}),
	}
	go func() { _ = shard.Serve(ctx, shardListener) }()
	dispatcher, err := NewAuthenticatedVectorPartitionShardSearchTCPDispatcherV1(transport,
		map[raftcluster.GroupID]string{group: shardListener.Addr().String()},
		map[raftcluster.GroupID]map[raftcluster.NodeID]string{group: {config.NodeID: shardListener.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	coordinator.dispatcher = dispatcher
	service, err := public.NewServiceV1(peerDrainVectorBackendV1{coordinator: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	operationConfig := public.ConservativeOperationsConfigV1()
	operationConfig.Enabled = true
	operations, err := public.NewOperationsV1(service, operationConfig, func(context.Context) (public.OperationsHealthV1, error) {
		return public.OperationsHealthV1{Ready: true}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(ServerOptions{PeerTransport: transport, VectorPartitionOperations: operations})
	defer server.Close()
	go func() { _ = server.Serve(ctx, nativeListener) }()
	// The client has its own node admission, so the fresh-ingress assertion
	// below reaches the draining server rather than failing on local outbound.
	caller, err := NewPeerTransportV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Close()
	client, err := caller.DialNativeContextV1(ctx, nativeListener.Addr().String(), config.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	fresh, err := caller.DialNativeContextV1(ctx, nativeListener.Addr().String(), config.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	result := make(chan error, 1)
	go func() {
		response, err := client.VectorSearchStrictV1(ctx, public.SearchRequestV1{
			Version: 1, Generation: public.GenerationIDV1{Index: "embedding", Generation: 7},
			Query: []float32{1, 0}, Metric: public.MetricCosineV1, TopK: 3, Probes: 2, EfSearch: 8,
			Consistency: public.ConsistencyGenerationSnapshotV1, Deadline: time.Now().Add(3 * time.Second),
			Limits: public.SearchLimitsV1{RequestBytes: 4 << 20, CandidateBytes: 64 << 20, ResponseBytes: 16 << 20, MergeEntries: 6},
		})
		if err == nil && len(response.Neighbors) != 2 {
			err = errors.New("missing self-fanout results")
		}
		result <- err
	}()
	select {
	case <-accepted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	control, err := NewFixedPeerTCPClientWithTransportV1(config, transport)
	if err != nil {
		t.Fatal(err)
	}
	control.ownPeerTransport = true
	node := &FixedPeerTCPRuntimeV1{config: config, client: control}
	node.BeginDrainV1()
	if err := fresh.Ping(ctx); !isRemoteError(err, iwire.ErrResourceExhausted) {
		t.Fatalf("fresh native ingress refusal: %v", err)
	}
	request := vectorPartitionShardSearchRequestTestV1([]uint32{1})
	request.TargetGroupID = group
	if _, err := dispatcher.DispatchVectorPartitionShardSearchV1(ctx, request); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("fresh outbound: %v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- node.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("shutdown passed live ingress: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("admitted fanout: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("shutdown unbounded")
	}
	if calls.Load() != 2 {
		t.Fatalf("self-directed shard calls=%d", calls.Load())
	}
	if current := transport.ResourceStatsV1().Current; current != (peerResourceAmountsV1{}) {
		t.Fatalf("leases: %v", current)
	}
}
