package main

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestHCBridgeRejectsNonLoopbackV1(t *testing.T) {
	if err := runVectorPartitionHCBridgeV1([]string{"-listen", "0.0.0.0:0", "-endpoint", "127.0.0.1:1"}, nil); err == nil {
		t.Fatal("non-loopback accepted")
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
