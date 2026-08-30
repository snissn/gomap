package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type hcBridgeFakeClientV1 struct{ search public.SearchResponseV1 }

func (c *hcBridgeFakeClientV1) call(r vectorPartitionOperationsWireRequestV1) (vectorPartitionOperationsWireResponseV1, error) {
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
	b := &hcBridgeV1{clients: make(chan hcBridgeClientV1, 1), timeout: time.Second, maxBody: 1024}
	b.clients <- &hcBridgeFakeClientV1{search: public.SearchResponseV1{Generation: public.GenerationIDV1{Index: "idx", Generation: 7}, Neighbors: []public.NeighborV1{{ID: "doc-000042", Score: .5}}, Counters: public.SearchCountersV1{HNSWServedPartitions: 1}}}
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
	b := &hcBridgeV1{clients: make(chan hcBridgeClientV1, 1), timeout: time.Second, maxBody: 1024}
	b.clients <- &hcBridgeFakeClientV1{search: public.SearchResponseV1{Generation: public.GenerationIDV1{Index: "idx", Generation: 7}, Neighbors: []public.NeighborV1{{ID: "42"}}}}
	r := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"version":1,"index":"idx","generation":7,"query":[1],"top_k":1,"probes":1,"ef_search":1}`))
	w := httptest.NewRecorder()
	b.ServeHTTP(w, r)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}
