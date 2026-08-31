package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type hcBridgeFakeClientV1 struct{ search public.SearchResponseV1 }

func (c *hcBridgeFakeClientV1) callContext(_ context.Context, r vectorPartitionOperationsWireRequestV1) (vectorPartitionOperationsWireResponseV1, error) {
	if r.Operation == "status" {
		return vectorPartitionOperationsWireResponseV1{SchemaVersion: 1, NodeConfigSHA256: "node", Health: &public.OperationsHealthV1{Ready: true, State: public.GenerationActiveV1, Generation: public.GenerationIDV1{Index: "idx", Generation: 7}}}, nil
	}
	return vectorPartitionOperationsWireResponseV1{SchemaVersion: 1, Search: &c.search}, nil
}
func (*hcBridgeFakeClientV1) Close() error { return nil }

type hcBridgeCountingClientV1 struct {
	search  public.SearchResponseV1
	err     error
	calls   []string
	request []vectorPartitionOperationsWireRequestV1
	closes  int
}

func (c *hcBridgeCountingClientV1) callContext(_ context.Context, r vectorPartitionOperationsWireRequestV1) (vectorPartitionOperationsWireResponseV1, error) {
	c.calls = append(c.calls, r.Operation)
	c.request = append(c.request, r)
	if c.err != nil {
		return vectorPartitionOperationsWireResponseV1{}, c.err
	}
	if r.Operation == "status" {
		return vectorPartitionOperationsWireResponseV1{NodeConfigSHA256: "node", Health: &public.OperationsHealthV1{Ready: true, State: public.GenerationActiveV1, Generation: public.GenerationIDV1{Index: "idx", Generation: 7}}}, nil
	}
	return vectorPartitionOperationsWireResponseV1{Search: &c.search}, nil
}
func (c *hcBridgeCountingClientV1) Close() error { c.closes++; return nil }

func TestHCBridgeRejectsNonLoopbackV1(t *testing.T) {
	if err := runVectorPartitionHCBridgeV1([]string{"-listen", "0.0.0.0:0", "-endpoint", "127.0.0.1:1"}, nil); err == nil {
		t.Fatal("non-loopback accepted")
	}
}

func TestHCBridgeClientCapReservesSystemDiagnosticConnectionV1(t *testing.T) {
	if !validHCBridgeBoundsV1(vectorPartitionSystemMaxConnectionsV1-1, 1, time.Second) {
		t.Fatal("system-aligned bridge cap rejected")
	}
	if validHCBridgeBoundsV1(vectorPartitionSystemMaxConnectionsV1, 1, time.Second) {
		t.Fatal("bridge consumed reserved system diagnostic connection")
	}
}

func TestHCBridgeStatusAndSearchIdentityV1(t *testing.T) {
	b := hcBridgeTestV1(&hcBridgeFakeClientV1{search: public.SearchResponseV1{Generation: public.GenerationIDV1{Index: "idx", Generation: 7}, Neighbors: []public.NeighborV1{{ID: "doc-000042", Score: .5}}, Counters: public.SearchCountersV1{SelectedPartitions: 1, HNSWServedPartitions: 1}}})
	for _, tc := range []struct {
		method, path, body string
		want               int
	}{{http.MethodGet, "/v1/status", "", 200}, {http.MethodPost, "/v1/search", `{"version":1,"index":"idx","generation":7,"query":[1],"top_k":1,"probes":1,"ef_search":1}`, 200}} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		w := httptest.NewRecorder()
		b.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s %s = %d: %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}

func TestHCBridgeRejectsNonCanonicalNeighborIDV1(t *testing.T) {
	b := hcBridgeTestV1(&hcBridgeFakeClientV1{search: public.SearchResponseV1{Generation: public.GenerationIDV1{Index: "idx", Generation: 7}, Neighbors: []public.NeighborV1{{ID: "42"}}}})
	r := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"version":1,"index":"idx","generation":7,"query":[1],"top_k":1,"probes":1,"ef_search":1}`))
	w := httptest.NewRecorder()
	b.ServeHTTP(w, r)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestHCBridgeStatusPoolAcquireHonorsConfiguredDeadlineV1(t *testing.T) {
	b := &hcBridgeV1{clients: make(chan *hcBridgeSlotV1), timeout: 5 * time.Millisecond, maxBody: 1024}
	started := time.Now()
	w := httptest.NewRecorder()
	b.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	if w.Code != http.StatusGatewayTimeout || time.Since(started) > time.Second {
		t.Fatalf("status=%d elapsed=%s", w.Code, time.Since(started))
	}
}

func TestHCBridgeSearchRouteProofV1(t *testing.T) {
	valid := func() public.SearchResponseV1 {
		return public.SearchResponseV1{Generation: public.GenerationIDV1{Index: "idx", Generation: 7}, Neighbors: []public.NeighborV1{{ID: "doc-000042"}}, Counters: public.SearchCountersV1{SelectedPartitions: 1, HNSWServedPartitions: 1}}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*public.SearchResponseV1)
		want   int
	}{
		{"stale_generation", func(r *public.SearchResponseV1) { r.Generation.Generation = 8 }, http.StatusBadGateway},
		{"partial_top_k", func(r *public.SearchResponseV1) { r.Neighbors = nil }, http.StatusBadGateway},
		{"absent_route", func(r *public.SearchResponseV1) {
			r.Counters.SelectedPartitions, r.Counters.HNSWServedPartitions = 0, 0
		}, http.StatusBadGateway},
		{"missing_fanout", func(r *public.SearchResponseV1) { r.Counters.SelectedPartitions = 0 }, http.StatusBadGateway},
		{"extra_fanout", func(r *public.SearchResponseV1) {
			r.Counters.SelectedPartitions, r.Counters.HNSWServedPartitions = 2, 2
		}, http.StatusBadGateway},
		{"hnsw_over_selected", func(r *public.SearchResponseV1) { r.Counters.HNSWServedPartitions = 2 }, http.StatusBadGateway},
		{"overflow_shaped", func(r *public.SearchResponseV1) {
			r.Counters.SelectedPartitions, r.Counters.HNSWServedPartitions, r.Counters.ExactScanPartitions = 1, math.MaxUint64, 1
		}, http.StatusBadGateway},
		{"exact_valid", func(r *public.SearchResponseV1) {
			r.Counters.HNSWServedPartitions, r.Counters.ExactScanPartitions = 0, 1
		}, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := valid()
			tc.mutate(&response)
			b := hcBridgeTestV1(&hcBridgeFakeClientV1{search: response})
			w := httptest.NewRecorder()
			b.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"version":1,"index":"idx","generation":7,"query":[1],"top_k":1,"probes":1,"ef_search":1}`)))
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestHCBridgeRejectsOverCapBodyV1(t *testing.T) {
	b := hcBridgeTestV1(&hcBridgeFakeClientV1{})
	b.maxBody = int64(len(`{"version":1}`))
	body := `{"version":1}` + strings.Repeat(" ", 32)
	w := httptest.NewRecorder()
	b.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(body)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestHCBridgeFailedSlotRedialsOrFailsClosedV1(t *testing.T) {
	failed := &hcBridgeFakeClientV1{}
	b := hcBridgeTestV1(failed)
	b.clients = make(chan *hcBridgeSlotV1, 1)
	b.clients <- &hcBridgeSlotV1{}
	replacement := &hcBridgeFakeClientV1{}
	b.redial = func(context.Context) (hcBridgeClientV1, error) { return replacement, nil }
	if _, err := b.call(context.Background(), vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "status"}); err != nil {
		t.Fatalf("same identity redial: %v", err)
	}
	b.clients = make(chan *hcBridgeSlotV1, 1)
	b.clients <- &hcBridgeSlotV1{}
	b.redial = func(context.Context) (hcBridgeClientV1, error) { return nil, errors.New("node identity mismatch") }
	if _, err := b.call(context.Background(), vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "status"}); err == nil {
		t.Fatal("mismatched identity redial accepted")
	}
}

func hcBridgeTestV1(client hcBridgeClientV1) *hcBridgeV1 {
	b := &hcBridgeV1{clients: make(chan *hcBridgeSlotV1, 1), timeout: time.Second, maxBody: 1024, nodeConfigSHA256: "node"}
	b.clients <- &hcBridgeSlotV1{client: client}
	b.redial = func(context.Context) (hcBridgeClientV1, error) { return client, nil }
	return b
}

func TestHCBridgeRetiresFailedSlotThenRedialsV1(t *testing.T) {
	failed := &hcBridgeCountingClientV1{err: errors.New("transport failed")}
	recovered := &hcBridgeCountingClientV1{}
	b := hcBridgeTestV1(failed)
	redials := 0
	b.redial = func(context.Context) (hcBridgeClientV1, error) { redials++; return recovered, nil }
	request := vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "status"}
	if _, err := b.call(context.Background(), request); err == nil {
		t.Fatal("failed client accepted")
	}
	if _, err := b.call(context.Background(), request); err != nil {
		t.Fatalf("redial recovery: %v", err)
	}
	if len(failed.calls) != 1 || failed.closes != 1 || redials != 1 || len(recovered.calls) != 1 {
		t.Fatalf("failed=%+v closes=%d redials=%d recovered=%+v", failed.calls, failed.closes, redials, recovered.calls)
	}
}

func TestHCBridgeSearchRetriesRetiredClientOnceV1(t *testing.T) {
	failed := &hcBridgeCountingClientV1{err: errors.New("expired connection")}
	recovered := &hcBridgeCountingClientV1{}
	b := hcBridgeTestV1(failed)
	redials := 0
	b.redial = func(context.Context) (hcBridgeClientV1, error) { redials++; return recovered, nil }
	request := vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search"}
	if _, err := b.call(context.Background(), request); err != nil {
		t.Fatalf("same-request recovery: %v", err)
	}
	if len(failed.calls) != 1 || failed.closes != 1 || redials != 1 || len(recovered.calls) != 1 || recovered.calls[0] != "search" {
		t.Fatalf("failed=%+v closes=%d redials=%d recovered=%+v", failed.calls, failed.closes, redials, recovered.calls)
	}
}

func TestHCBridgeSearchDoesNotIssueStatusRPCV1(t *testing.T) {
	client := &hcBridgeCountingClientV1{search: public.SearchResponseV1{Generation: public.GenerationIDV1{Index: "idx", Generation: 7}, Neighbors: []public.NeighborV1{{ID: "doc-000042"}}, Counters: public.SearchCountersV1{SelectedPartitions: 1, HNSWServedPartitions: 1}}}
	b := hcBridgeTestV1(client)
	w := httptest.NewRecorder()
	b.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"version":1,"index":"idx","generation":7,"query":[1],"top_k":1,"probes":1,"ef_search":1}`)))
	if w.Code != http.StatusOK || len(client.calls) != 1 || client.calls[0] != "search" {
		t.Fatalf("status=%d calls=%+v", w.Code, client.calls)
	}
}

func TestHCBridgeSearchUsesRequestLocalMergeBudgetV1(t *testing.T) {
	client := &hcBridgeCountingClientV1{search: hcBridgeTenNeighborSearchV1()}
	b := hcBridgeTestV1(client)
	query := make([]float32, 128)
	query[0] = 1
	body, err := json.Marshal(hcBridgeSearchRequestV1{Version: 1, Index: "idx", Generation: 7, Query: query, TopK: 10, Probes: 1, EfSearch: 32})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	b.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(string(body))))
	if w.Code != http.StatusOK || len(client.calls) != 1 || client.calls[0] != "search" {
		t.Fatalf("status=%d calls=%+v", w.Code, client.calls)
	}
	if got := client.request[0].Search.Limits.MergeEntries; got != 10 || got > nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxMergeEntries {
		t.Fatalf("merge budget got=%d want=10", got)
	}
}

func TestHCBridgeSearchRejectsOverCapWithoutNativeCallV1(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request hcBridgeSearchRequestV1
	}{
		{"top_k", hcBridgeSearchRequestV1{Version: 1, Index: "idx", Generation: 7, Query: []float32{1}, TopK: 257, Probes: 1, EfSearch: 257}},
		{"ef_search", hcBridgeSearchRequestV1{Version: 1, Index: "idx", Generation: 7, Query: []float32{1}, TopK: 1, Probes: 1, EfSearch: 4097}},
		{"query", hcBridgeSearchRequestV1{Version: 1, Index: "idx", Generation: 7, Query: append([]float32{1}, make([]float32, 4096)...), TopK: 1, Probes: 1, EfSearch: 1}},
		{"probes", hcBridgeSearchRequestV1{Version: 1, Index: "idx", Generation: 7, Query: []float32{1}, TopK: 1, Probes: 257, EfSearch: 1}},
		{"merge_product", hcBridgeSearchRequestV1{Version: 1, Index: "idx", Generation: 7, Query: []float32{1}, TopK: 256, Probes: 257, EfSearch: 256}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &hcBridgeCountingClientV1{}
			body, err := json.Marshal(tc.request)
			if err != nil {
				t.Fatal(err)
			}
			bridge := hcBridgeTestV1(client)
			bridge.maxBody = 1 << 20
			w := httptest.NewRecorder()
			bridge.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(string(body))))
			if w.Code != http.StatusBadRequest || len(client.calls) != 0 {
				t.Fatalf("status=%d calls=%+v", w.Code, client.calls)
			}
		})
	}
}

func hcBridgeTenNeighborSearchV1() public.SearchResponseV1 {
	neighbors := make([]public.NeighborV1, 10)
	for i := range neighbors {
		neighbors[i] = public.NeighborV1{ID: fmt.Sprintf("doc-%06d", i), Score: float32(i)}
	}
	return public.SearchResponseV1{Generation: public.GenerationIDV1{Index: "idx", Generation: 7}, Neighbors: neighbors, Counters: public.SearchCountersV1{SelectedPartitions: 1, HNSWServedPartitions: 1}}
}

func TestHCBridgeServerReservesTimeoutResponseGraceV1(t *testing.T) {
	server := newHCBridgeServerV1(http.NotFoundHandler(), 5*time.Millisecond)
	if server.WriteTimeout != hcBridgeFullRequestBudgetV1(server.ReadTimeout) {
		t.Fatalf("server timeouts read=%s write=%s", server.ReadTimeout, server.WriteTimeout)
	}
}

func TestHCBridgeShutdownWaitsForInflightHandlerV1(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	server := newHCBridgeServerV1(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = w.Write([]byte("ok"))
	}), time.Second)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- serveHCBridgeV1(ctx, server, listener, time.Second) }()
	clientDone := make(chan error, 1)
	go func() {
		response, err := (&http.Client{Timeout: time.Second}).Get("http://" + listener.Addr().String())
		if response != nil {
			response.Body.Close()
		}
		clientDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case err := <-serveDone:
		t.Fatalf("shutdown returned before handler drain: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not join")
	}
	select {
	case err := <-clientDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not finish")
	}
}
