package nativewire

import (
	"math"
	"net"
	"testing"

	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestDenseVectorSearchOrdinaryClientValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*documentservice.RawDenseVectorSearchResponse, *DenseVectorSearchRequest)
		valid  bool
	}{
		{name: "ANN physical ordinal ties", valid: true},
		{name: "ANN lexical ties", valid: true, change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0], r.Results[1] = r.Results[1], r.Results[0]
		}},
		{name: "ANN descending scores", valid: true, change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0].Score = 0.75
		}},
		{name: "ANN ascending scores", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[1].Score = 0.75
		}},
		{name: "ANN tiny ascending scores", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[1].Score = math.Nextafter(0.5, 1)
		}},
		{name: "exact physical ordinal ties", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Route = documentservice.RouteExact
			r.Exact = true
			r.SearchPackSearch = 0
			r.DocumentsScanned = 2
		}},
		{name: "exact lexical ties", valid: true, change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Route = documentservice.RouteExact
			r.Exact = true
			r.SearchPackSearch = 0
			r.DocumentsScanned = 2
			r.Results[0], r.Results[1] = r.Results[1], r.Results[0]
		}},
		{name: "duplicate IDs", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[1] = r.Results[0]
		}},
		{name: "empty ID", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0].ID = nil
		}},
		{name: "whitespace ID", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0].ID = []byte(" z")
			r.Results[0].Document = []byte(`{"id":" z"}`)
		}},
		{name: "invalid UTF8 ID", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0].ID = []byte{0xff}
		}},
		{name: "wrong document ID", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0].Document = []byte(`{"id":"other"}`)
		}},
		{name: "malformed document", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0].Document = []byte(`{"id":`)
		}},
		{name: "duplicate document field", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0].Document = []byte(`{"id":"other","id":"z"}`)
		}},
		{name: "unrequested embedding", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0].Document = []byte(`{"id":"z","embedding":[1,0]}`)
		}},
		{name: "requested embedding missing", change: func(_ *documentservice.RawDenseVectorSearchResponse, q *DenseVectorSearchRequest) {
			q.ReturnEmbedding = true
		}},
		{name: "requested embedding dimension", change: func(r *documentservice.RawDenseVectorSearchResponse, q *DenseVectorSearchRequest) {
			q.ReturnEmbedding = true
			r.Results[0].Document = []byte(`{"id":"z","embedding":[1]}`)
			r.Results[1].Document = []byte(`{"id":"a","embedding":[1,0]}`)
		}},
		{name: "requested embedding valid", valid: true, change: func(r *documentservice.RawDenseVectorSearchResponse, q *DenseVectorSearchRequest) {
			q.ReturnEmbedding = true
			r.Results[0].Document = []byte(`{"id":"z","embedding":[1,0]}`)
			r.Results[1].Document = []byte(`{"id":"a","embedding":[1,0]}`)
		}},
		{name: "filter mismatch", change: func(_ *documentservice.RawDenseVectorSearchResponse, q *DenseVectorSearchRequest) {
			q.Filter = &documentservice.Filter{Field: "meta.tenant", Operator: "==", Value: "blue"}
		}},
		{name: "NaN score", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0].Score = math.NaN()
		}},
		{name: "positive infinite score", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0].Score = math.Inf(1)
		}},
		{name: "negative infinite score", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.Results[0].Score = math.Inf(-1)
		}},
		{name: "fallback count", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) {
			r.ExactFallbacks = 1
		}},
		{name: "insufficient candidates", change: func(r *documentservice.RawDenseVectorSearchResponse, _ *DenseVectorSearchRequest) { r.Candidates = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 2, EfSearch: 8}
			response := documentservice.RawDenseVectorSearchResponse{
				Route: documentservice.RouteAnn, Candidates: 2, SearchPackSearch: 1,
				Results: []documentservice.RawDenseVectorResult{
					{ID: []byte("z"), Score: 0.5, Document: []byte(`{"id":"z"}`)},
					{ID: []byte("a"), Score: 0.5, Document: []byte(`{"id":"a"}`)},
				},
			}
			if tc.change != nil {
				tc.change(&response, &request)
			}
			var body []byte
			for _, section := range []iwire.Section{
				{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, response.Results[0].ID, response.Results[1].ID)},
				{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, response.Results[0].Document, response.Results[1].Document)},
				{ID: iwire.SectionDenseSearchResponse, Bytes: appendVersionedDenseVectorSearchResponse(nil, response, iwire.DenseVectorSearchOrdinaryVersion)},
			} {
				var err error
				body, err = iwire.AppendSection(body, section)
				if err != nil {
					t.Fatal(err)
				}
			}
			clientConn, serverConn := net.Pipe()
			client := NewClient(clientConn)
			t.Cleanup(func() { _ = client.Close(); _ = serverConn.Close() })
			ctx := denseSearchTestContext(t)
			done := make(chan error, 1)
			go func() {
				header, err := readOrdinaryDenseTestRequest(serverConn)
				if err == nil {
					err = writeFrame(serverConn, iwire.Header{Type: iwire.FrameResponse, RequestID: header.RequestID}, body)
				}
				done <- err
			}()
			if err := client.Hello(ctx); err != nil {
				t.Fatal(err)
			}
			got, err := client.DenseVectorSearch(ctx, request)
			waitOrdinaryDenseTestServer(t, ctx, done)
			if (err == nil) != tc.valid {
				t.Fatalf("response valid=%t, err=%v", tc.valid, err)
			}
			if tc.valid {
				if len(got.Results) != 2 || string(got.Results[0].ID) != string(response.Results[0].ID) || got.Results[0].Score != response.Results[0].Score {
					t.Fatalf("client changed producer order: %+v", got)
				}
			} else if len(got.Results) != 0 || len(client.denseResults) != 0 || len(client.denseIDs) != 0 || len(client.denseDocuments) != 0 {
				t.Fatal("rejected response retained result views")
			}
		})
	}
}
