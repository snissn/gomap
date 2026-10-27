package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

// Ordinary cosine keeps its omitted ColumnGraph strategy through the actual
// native owner and public service. Wire search must honor the same ordinary
// route choice without claiming either HNSW or typed ColumnGraph execution.
func TestDenseVectorSearchOrdinaryNativeDefaultParity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter *documentservice.Filter
		route  documentservice.Route
		ids    []string
	}{
		{name: "unfiltered_ann", route: documentservice.RouteAnn, ids: []string{"a", "b"}},
		{name: "filtered_exact", route: documentservice.RouteExact, ids: []string{"b", "c"}, filter: &documentservice.Filter{
			Operator: "AND", Conditions: []documentservice.Filter{
				{Field: "meta.tenant", Operator: "==", Value: "blue"},
				{Field: "meta.rank", Operator: ">=", Value: int64(2)},
			},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := denseSearchTestContext(t)
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_btree"
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(backend)
			t.Cleanup(func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
			})
			if !manager.UsesNativeCommandWALApply() {
				t.Fatal("public factory did not retain the native owner")
			}
			service := documentservice.NewWithDeferredVectorBuildMaintenance(manager, owner.DeferredVectorBuildMaintenance())
			t.Cleanup(func() {
				if err := service.Close(); err != nil {
					t.Error(err)
				}
			})
			// Do not supply vector_index_options: this tests the real default.
			info, err := service.CreateIndex(ctx, documentservice.CreateIndexRequest{
				Name: "docs", Dimension: 2,
				ScalarFields: []documentservice.ScalarFieldDeclaration{
					{Field: "meta.tenant", ValueType: documentservice.ScalarFieldString},
					{Field: "meta.rank", ValueType: documentservice.ScalarFieldInt64},
				},
			})
			if err != nil || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph || info.TypedInput {
				t.Fatalf("ordinary declaration: %+v err=%v", info, err)
			}
			documents := []documentservice.Document{
				{ID: "a", Content: "refund red", Embedding: []float32{1, 0}, Meta: map[string]any{"tenant": "red", "rank": int64(1)}},
				{ID: "b", Content: "refund blue", Embedding: []float32{0.8, 0.2}, Meta: map[string]any{"tenant": "blue", "rank": int64(2)}},
				{ID: "c", Content: "return blue", Embedding: []float32{0, 1}, Meta: map[string]any{"tenant": "blue", "rank": int64(3)}},
			}
			if _, err := service.UpsertDocuments(ctx, "docs", documentservice.UpsertDocumentsRequest{Documents: documents, DeferVectorIndexRebuild: true}); err != nil {
				t.Fatal(err)
			}
			if _, err := service.OptimizeIndex(ctx, "docs", documentservice.OptimizeIndexRequest{}); err != nil {
				t.Fatal(err)
			}
			request := DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 2, EfSearch: 8, ExpectedGeneration: info.Generation, Filter: tc.filter}
			want, err := service.SearchDenseVector(ctx, request.Index, documentservice.DenseVectorSearchRequest{
				ExpectedGeneration: request.ExpectedGeneration, QueryEmbedding: request.Query,
				TopK: request.TopK, EfSearch: request.EfSearch, Filter: request.Filter, Route: tc.route,
			})
			if err != nil || want.Route != tc.route || want.Exact != (tc.route == documentservice.RouteExact) || len(want.Documents) != len(tc.ids) {
				t.Fatalf("maintained ordinary service route: %+v err=%v", want, err)
			}
			if tc.route == documentservice.RouteAnn && (want.ColumnGraphPreparedSearch != 0 || want.SearchPackSearch != 1 || want.DocumentsScanned != 0) {
				t.Fatalf("public ANN did not report actual search-pack work: %+v", want)
			}
			if tc.route == documentservice.RouteExact && (want.DocumentsScanned != uint64(len(documents)) || want.ColumnGraphPreparedSearch != 0 || want.SearchPackSearch != 0) {
				t.Fatalf("public exact did not report its actual scan: %+v", want)
			}
			for i, id := range tc.ids {
				if want.Documents[i].ID != id {
					t.Fatalf("service result[%d]=%q want %q", i, want.Documents[i].ID, id)
				}
			}
			server := NewServer(ServerOptions{Collections: manager, Backend: backend, DocumentService: service})
			t.Cleanup(func() {
				if err := server.Close(); err != nil {
					t.Error(err)
				}
			})
			client, cleanup, err := NewInProcessClient(ctx, server)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := cleanup(); err != nil {
					t.Error(err)
				}
			})
			if err := client.Hello(ctx); err != nil {
				t.Fatal(err)
			}
			if !client.denseOrdinaryNegotiated {
				t.Fatal("hello did not negotiate the ordinary service capability")
			}
			// A peer advertising only the existing HNSW capability must still
			// refuse this omitted ColumnGraph index rather than relabel it.
			client.denseOrdinaryNegotiated = false
			if _, err := client.DenseVectorSearch(ctx, request); err == nil {
				t.Fatal("HNSW-only wire accepted ordinary ColumnGraph")
			}
			client.denseOrdinaryNegotiated = true
			got, err := client.DenseVectorSearch(ctx, request)
			if tc.route == documentservice.RouteExact {
				if !isRemoteError(err, iwire.ErrUnsupportedFeature) {
					t.Fatalf("ordinary filtered wire request must fail closed: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ordinary default wire search refused after successful public service query: %v", err)
			}
			if got.Route != want.Route || got.TypedColumnGraph || got.Candidates != want.Candidates || len(got.Results) != len(want.Documents) {
				t.Fatalf("wire ordinary route/candidates: %+v service=%+v", got, want)
			}
			if got.Exact != want.Exact || got.ColumnGraphPreparedSearch != want.ColumnGraphPreparedSearch || got.SearchPackSearch != want.SearchPackSearch || got.ColumnGraphDeltaScored != want.ColumnGraphDeltaScored || got.DocumentsScanned != want.DocumentsScanned {
				t.Fatalf("wire changed measured work: %+v service=%+v", got, want)
			}
			if got.ExactFallbacks != 0 || (tc.route == documentservice.RouteAnn && got.FullDocumentScanFallbacks != 0) {
				t.Fatalf("ordinary default selected an unintended fallback: %+v", got)
			}
			for i, result := range got.Results {
				expected := want.Documents[i]
				if string(result.ID) != expected.ID || expected.Score == nil || math.Abs(result.Score-*expected.Score) > 1e-12 {
					t.Fatalf("wire rank/score[%d]=%q/%g service=%+v", i, result.ID, result.Score, expected)
				}
				var doc documentservice.Document
				if err := json.Unmarshal(result.Document, &doc); err != nil {
					t.Fatal(err)
				}
				if doc.ID != expected.ID || doc.Content != expected.Content || doc.Meta["tenant"] != expected.Meta["tenant"] || len(doc.Embedding) != 0 || bytes.Contains(result.Document, []byte(`"embedding"`)) {
					t.Fatalf("wire projected document[%d]=%s service=%+v", i, result.Document, expected)
				}
			}
			if _, _, err := client.GetMany(ctx, "docs", [][]byte{[]byte("a")}); err != nil {
				t.Fatal(err)
			}
			if len(client.denseResults) != 0 || len(client.denseIDs) != 0 || len(client.denseDocuments) != 0 {
				t.Fatal("ordinary result views survived the next round trip")
			}
		})
	}
}

func TestDenseVectorSearchOrdinaryVersionTruthfulness(t *testing.T) {
	limits := iwire.DefaultLimits()
	ids := iwire.AppendByteVector(nil, []byte("a"))
	docs := iwire.AppendByteVector(nil, []byte(`{"id":"a","content":"refund"}`))
	base := documentservice.RawDenseVectorSearchResponse{
		Route: documentservice.RouteAnn, Candidates: 1, ColumnGraphPreparedSearch: 1,
		Results: []documentservice.RawDenseVectorResult{{ID: []byte("a"), Score: 1, Document: []byte(`{"id":"a","content":"refund"}`)}},
	}
	decode := func(version uint64, raw []byte) (DenseVectorSearchResponse, error) {
		out, _, _, _, err := decodeVersionedDenseVectorSearchResponse(version, ids, docs, raw, 1, limits, nil, nil, nil)
		return out, err
	}
	for _, tc := range []struct {
		name   string
		change func(*documentservice.RawDenseVectorSearchResponse)
	}{
		{name: "prepared_ann", change: func(r *documentservice.RawDenseVectorSearchResponse) { r.ColumnGraphDeltaScored = 2 }},
		{name: "search_pack_ann", change: func(r *documentservice.RawDenseVectorSearchResponse) {
			r.ColumnGraphPreparedSearch = 0
			r.SearchPackSearch = 1
		}},
		{name: "intentional_exact", change: func(r *documentservice.RawDenseVectorSearchResponse) {
			r.Route = documentservice.RouteExact
			r.Exact = true
			r.ColumnGraphPreparedSearch = 0
			r.Candidates = 2
			r.DocumentsScanned = 3
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			tc.change(&r)
			meta := appendVersionedDenseVectorSearchResponse(nil, r, iwire.DenseVectorSearchOrdinaryVersion)
			got, err := decode(iwire.DenseVectorSearchOrdinaryVersion, meta)
			if err != nil || got.Route != r.Route || got.Exact != r.Exact || got.ColumnGraphPreparedSearch != r.ColumnGraphPreparedSearch || got.SearchPackSearch != r.SearchPackSearch || got.ColumnGraphDeltaScored != r.ColumnGraphDeltaScored || got.DocumentsScanned != r.DocumentsScanned || got.Candidates != r.Candidates {
				t.Fatalf("ordinary result/counters=%+v want=%+v err=%v", got, r, err)
			}
			for _, version := range []uint64{1, iwire.DenseVectorSearchTypedVersion, iwire.DenseVectorSearchTypedQuantizedVersion, iwire.DenseVectorSearchNormalizedVersion} {
				if _, err := decode(version, meta); err == nil {
					t.Fatalf("ordinary metadata accepted as version %d", version)
				}
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*documentservice.RawDenseVectorSearchResponse)
	}{
		{name: "missing_prepared", change: func(r *documentservice.RawDenseVectorSearchResponse) { r.ColumnGraphPreparedSearch = 0 }},
		{name: "multiple_prepared", change: func(r *documentservice.RawDenseVectorSearchResponse) { r.ColumnGraphPreparedSearch = 2 }},
		{name: "two_graph_routes", change: func(r *documentservice.RawDenseVectorSearchResponse) { r.SearchPackSearch = 1 }},
		{name: "multiple_pack", change: func(r *documentservice.RawDenseVectorSearchResponse) {
			r.ColumnGraphPreparedSearch = 0
			r.SearchPackSearch = 2
		}},
		{name: "graph_counter_overflow", change: func(r *documentservice.RawDenseVectorSearchResponse) {
			r.ColumnGraphPreparedSearch = ^uint64(0)
			r.SearchPackSearch = 2
		}},
		{name: "ann_scanned", change: func(r *documentservice.RawDenseVectorSearchResponse) { r.DocumentsScanned = 1 }},
		{name: "exact_claims_prepared", change: func(r *documentservice.RawDenseVectorSearchResponse) {
			r.Route = documentservice.RouteExact
			r.DocumentsScanned = 1
		}},
		{name: "exact_missing_scan", change: func(r *documentservice.RawDenseVectorSearchResponse) {
			r.Route = documentservice.RouteExact
			r.ColumnGraphPreparedSearch = 0
		}},
		{name: "exact_claims_pack", change: func(r *documentservice.RawDenseVectorSearchResponse) {
			r.Route = documentservice.RouteExact
			r.ColumnGraphPreparedSearch = 0
			r.SearchPackSearch = 1
			r.DocumentsScanned = 1
		}},
		{name: "fallback", change: func(r *documentservice.RawDenseVectorSearchResponse) { r.FullDocumentScanFallbacks = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			tc.change(&r)
			if _, err := decode(iwire.DenseVectorSearchOrdinaryVersion, appendVersionedDenseVectorSearchResponse(nil, r, iwire.DenseVectorSearchOrdinaryVersion)); err == nil {
				t.Fatal("inconsistent route/counters accepted")
			}
		})
	}
	meta := appendVersionedDenseVectorSearchResponse(nil, base, iwire.DenseVectorSearchOrdinaryVersion)
	for _, tag := range []byte{0, 1, iwire.DenseVectorSearchTypedRouteTag, iwire.DenseVectorSearchTypedQuantizedRouteTag, 255} {
		bad := append([]byte(nil), meta...)
		bad[0] = tag
		if _, err := decode(iwire.DenseVectorSearchOrdinaryVersion, bad); err == nil {
			t.Fatalf("ordinary decoder accepted route tag %d", tag)
		}
	}
	if _, err := decode(iwire.DenseVectorSearchOrdinaryVersion, append(meta, 0)); err == nil {
		t.Fatal("ordinary decoder accepted trailing metadata")
	}
	for n := 0; n < len(meta); n++ {
		if _, err := decode(iwire.DenseVectorSearchOrdinaryVersion, meta[:n]); err == nil {
			t.Fatalf("ordinary decoder accepted truncated metadata %d", n)
		}
	}
	sections := []iwire.Section{{ID: iwire.SectionDocumentIDs}, {ID: iwire.SectionDocuments}, {ID: iwire.SectionDenseSearchResponse}}
	if err := validateOrdinaryDenseSections(sections); err != nil {
		t.Fatal(err)
	}
	sections[2] = iwire.Section{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical}
	if err := validateOrdinaryDenseSections(sections); err == nil {
		t.Fatal("ordinary version accepted a typed work proof")
	}
}

func TestDenseVectorSearchNativewireParityAndBorrowing(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	mgr := collections.NewCollectionManager(db)
	svc := documentservice.New(mgr)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, DocumentService: svc})
	t.Cleanup(func() { _ = server.Close(); _ = svc.Close(); _ = db.Close() })
	ctx := denseSearchTestContext(t)
	_, err = svc.CreateIndex(ctx, documentservice.CreateIndexRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    documentservice.MetricCosine,
		ScalarFields: []documentservice.ScalarFieldDeclaration{
			{Field: "meta.user_id", ValueType: documentservice.ScalarFieldString},
			{Field: "meta.rank", ValueType: documentservice.ScalarFieldInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	documents := []documentservice.Document{
		{ID: "a", Content: "a", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "alpha", "rank": int64(2)}},
		{ID: "b", Content: "b", Embedding: []float32{0, 1}, Meta: map[string]any{"user_id": "beta", "rank": int64(1)}},
		{ID: "c", Content: "c", Embedding: []float32{0.8, 0.2}, Meta: map[string]any{"user_id": "alpha", "rank": int64(3)}},
		{ID: "d", Content: string(bytes.Repeat([]byte("x"), 1024)), Embedding: []float32{-1, 0}, Meta: map[string]any{"user_id": "beta", "rank": int64(0)}},
	}
	if _, err = svc.UpsertDocuments(ctx, "docs", documentservice.UpsertDocumentsRequest{Documents: documents, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err = client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 1, TypedColumnGraph: true}); err == nil {
		t.Fatal("typed command accepted ordinary index")
	}
	client.denseOrdinaryNegotiated = false
	if _, err := client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 1}); err == nil {
		t.Fatal("ordinary request accepted without v5 negotiation")
	}
	client.denseOrdinaryNegotiated = true
	if _, err := svc.OptimizeIndex(ctx, "docs", documentservice.OptimizeIndexRequest{}); err != nil {
		t.Fatal(err)
	}

	filters := []*documentservice.Filter{nil}
	for _, filter := range filters {
		request := DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 2, EfSearch: 8, Filter: filter}
		got, err := client.DenseVectorSearch(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		wantRawResults, err := svc.SearchDenseVectorRawInto(ctx, request.Index, documentservice.DenseVectorSearchRequest{
			QueryEmbedding: request.Query,
			TopK:           request.TopK,
			EfSearch:       request.EfSearch,
			Route:          documentservice.RouteAnn,
			Filter:         request.Filter,
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		want, err := svc.SearchDenseVector(ctx, request.Index, documentservice.DenseVectorSearchRequest{
			QueryEmbedding: request.Query,
			TopK:           request.TopK,
			EfSearch:       request.EfSearch,
			Route:          documentservice.RouteAnn,
			Filter:         request.Filter,
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Route != documentservice.RouteAnn || got.ExactFallbacks != 0 || got.FullDocumentScanFallbacks != 0 {
			t.Fatalf("native route proof missing: %+v", got)
		}
		if len(got.Results) != len(want.Documents) || got.Candidates != want.Candidates {
			t.Fatalf("native results/candidates=%d/%d service=%d/%d", len(got.Results), got.Candidates, len(want.Documents), want.Candidates)
		}
		for i := range got.Results {
			wantDoc := want.Documents[i]
			wantRaw := wantRawResults.Results[i]
			wantScore := *wantDoc.Score
			if string(got.Results[i].ID) != wantDoc.ID || math.Abs(got.Results[i].Score-wantScore) > 1e-12 || !bytes.Equal(got.Results[i].ID, wantRaw.ID) || !bytes.Equal(got.Results[i].Document, wantRaw.Document) {
				t.Fatalf("result[%d]=%q/%g/%s want %q/%g/%s", i, got.Results[i].ID, got.Results[i].Score, got.Results[i].Document, wantRaw.ID, wantRaw.Score, wantRaw.Document)
			}
			if bytes.Contains(got.Results[i].Document, []byte(`"embedding"`)) {
				t.Fatalf("result[%d] retained excluded embedding: %s", i, got.Results[i].Document)
			}
		}
	}
	if cap(client.readBody) == 0 {
		t.Fatal("dense response buffer is empty")
	}
	oldBody := client.readBody[:cap(client.readBody)]
	oldBodyPtr := &oldBody[0]
	if _, _, err = client.GetMany(ctx, "docs", [][]byte{[]byte("d")}); err != nil {
		t.Fatal(err)
	}
	if cap(client.readBody) == 0 || &client.readBody[:cap(client.readBody)][0] == oldBodyPtr {
		t.Fatal("non-dense response did not replace dense response buffer")
	}
	if len(client.denseIDs) != 0 || len(client.denseDocuments) != 0 || len(client.denseResults) != 0 || len(client.vectorSections) != 0 {
		t.Fatalf("retained dense views ids=%d docs=%d results=%d sections=%d", len(client.denseIDs), len(client.denseDocuments), len(client.denseResults), len(client.vectorSections))
	}
	for _, ids := range client.denseIDs[:cap(client.denseIDs)] {
		if ids != nil {
			t.Fatalf("retained dense ID view: %q", ids)
		}
	}
	for _, docs := range client.denseDocuments[:cap(client.denseDocuments)] {
		if docs != nil {
			t.Fatalf("retained dense document view: %q", docs)
		}
	}
	for _, result := range client.denseResults[:cap(client.denseResults)] {
		if result.ID != nil || result.Document != nil {
			t.Fatalf("retained dense result view: %+v", result)
		}
	}
	for _, section := range client.vectorSections[:cap(client.vectorSections)] {
		if section.Bytes != nil {
			t.Fatalf("retained response section view: %+v", section)
		}
	}

	if _, err = client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 0}); err == nil {
		t.Fatal("invalid top_k succeeded")
	}
	if _, err = client.DenseVectorSearch(ctx, DenseVectorSearchRequest{
		Index: "docs", Query: []float32{1, 0}, TopK: 1,
		Filter: &documentservice.Filter{Operator: "OR", Conditions: []documentservice.Filter{
			{Field: "meta.user_id", Operator: "==", Value: "alpha"},
			{Field: "meta.user_id", Operator: "==", Value: "beta"},
		}},
	}); nativeCodeOf(err) != iwire.ErrUnsupportedFeature {
		t.Fatalf("OR filter local encoding error=%v code=%d", err, nativeCodeOf(err))
	}
	server.limits.MaxByteVectorBytes = 1
	if _, err = client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 1}); !isRemoteError(err, iwire.ErrResourceExhausted) {
		t.Fatalf("byte-vector limit err=%v want resource exhausted", err)
	}
	server.limits.MaxByteVectorBytes = iwire.DefaultLimits().MaxByteVectorBytes
	server.limits.MaxFrameSize = 96
	if _, err = client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 1}); !isRemoteError(err, iwire.ErrResourceExhausted) {
		t.Fatalf("frame limit err=%v want resource exhausted", err)
	}
	server.limits.MaxFrameSize = iwire.DefaultLimits().MaxFrameSize
	server.limits.MaxSections = 2
	if _, err = client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 1}); !isRemoteError(err, iwire.ErrResourceExhausted) {
		t.Fatalf("section count limit err=%v want resource exhausted", err)
	}
}

func TestDenseVectorSearchCodecFailsClosed(t *testing.T) {
	limits := iwire.DefaultLimits()
	request := DenseVectorSearchRequest{
		Index: "docs", Query: []float32{1, 0}, TopK: 3, EfSearch: 8,
		Filter: &documentservice.Filter{Operator: "AND", Conditions: []documentservice.Filter{
			{Field: "meta.tenant", Operator: "==", Value: "alpha"},
			{Field: "meta.rank", Operator: ">", Value: json.Number("2")},
		}},
	}
	raw, err := appendDenseVectorSearchRequest(nil, request, limits)
	if err != nil {
		t.Fatal(err)
	}
	var root documentservice.Filter
	decoded, leaves, err := decodeDenseVectorSearchRequest(raw, limits, nil, &root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Index != request.Index || decoded.TopK != request.TopK || decoded.EfSearch != request.EfSearch || len(leaves) != 2 || decoded.Filter == nil || decoded.Filter.Operator != "AND" {
		t.Fatalf("decoded=%+v leaves=%+v", decoded, leaves)
	}
	if _, _, err := decodeDenseVectorSearchRequest(raw[:len(raw)-1], limits, nil, &root, nil); err == nil {
		t.Fatal("truncated dense request decoded")
	}
	tightLimits := limits
	tightLimits.MaxByteVectorItems = 4
	request.Filter = nil
	request.EfSearch = tightLimits.MaxByteVectorItems + 1
	if _, err := appendDenseVectorSearchRequest(nil, request, tightLimits); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("oversized ef_search error=%v code=%d", err, nativeCodeOf(err))
	}
	raw, err = appendDenseVectorSearchRequest(nil, request, limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeDenseVectorSearchRequest(raw, tightLimits, nil, &root, nil); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("decoded oversized ef_search error=%v code=%d", err, nativeCodeOf(err))
	}
	request.EfSearch = 8
	request.Filter = nil
	request.TopK = limits.MaxByteVectorItems + 1
	if _, err := appendDenseVectorSearchRequest(nil, request, limits); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("oversized top_k error=%v code=%d", err, nativeCodeOf(err))
	}
	request.TopK = 3
	tooMany := make([]documentservice.Filter, denseFilterMaxLeaves+1)
	for i := range tooMany {
		tooMany[i] = documentservice.Filter{Field: "meta.x", Operator: "==", Value: "x"}
	}
	request.Filter = &documentservice.Filter{Operator: "AND", Conditions: tooMany}
	if _, err := appendDenseVectorSearchRequest(nil, request, limits); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("oversized filter error=%v code=%d", err, nativeCodeOf(err))
	}
	limits.MaxSectionLen = 64
	request.Filter = &documentservice.Filter{Field: "meta.x", Operator: "==", Value: string(make([]byte, 60))}
	if _, err := appendDenseVectorSearchRequest(nil, request, limits); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("oversized request error=%v code=%d", err, nativeCodeOf(err))
	}
}

func TestDenseVectorSearchUnconfiguredFailsClosed(t *testing.T) {
	server := NewServer(ServerOptions{})
	defer server.Close()
	ctx := denseSearchTestContext(t)
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err := client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1}, TopK: 1}); err == nil {
		t.Fatal("unconfigured search succeeded")
	}
}

func TestDenseVectorSearchRoutedClusterFailsClosedBeforeLocalSearch(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	mgr := collections.NewCollectionManager(db)
	svc := documentservice.New(mgr)
	routeProvider := &routingClusterSubmitter{fakeClusterSubmitter: &fakeClusterSubmitter{}}
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, ClusterSubmitter: routeProvider, DocumentService: svc})
	t.Cleanup(func() { _ = server.Close(); _ = svc.Close(); _ = db.Close() })
	ctx := denseSearchTestContext(t)
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err := client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	if client.denseOrdinaryNegotiated {
		t.Fatal("routed cluster advertised ordinary dense search")
	}
	request := DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 1}
	if _, err := client.DenseVectorSearch(ctx, request); nativeCodeOf(err) != iwire.ErrUnsupportedFeature {
		t.Fatalf("unnegotiated routed dense search err=%v want local unsupported feature", err)
	}
	// Routed peers deliberately omit dense capabilities. Send an explicit v5
	// frame to exercise the server's rejection before any local search or route.
	payload, err := appendDenseVectorSearchRequest(nil, request, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	body, err := appendVersionedCommandRequestBody(nil, iwire.CommandDenseVectorSearch, iwire.DenseVectorSearchOrdinaryVersion,
		iwire.Section{ID: iwire.SectionDenseSearchRequest, Bytes: payload},
		iwire.Section{ID: iwire.SectionDeadline, Bytes: binary.AppendUvarint(nil, uint64(time.Now().Add(time.Minute).UnixNano()))},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse); !isRemoteError(err, iwire.ErrReadOnly) || !strings.Contains(err.Error(), "dense vector search") {
		t.Fatalf("routed dense search err=%v want read-only route rejection", err)
	}
	if routes := routeProvider.snapshotRoutes(); len(routes) != 0 {
		t.Fatalf("routed dense search reached route provider: %+v", routes)
	}
}

func TestClearDenseVectorSearchScratchReleasesPointers(t *testing.T) {
	state := connState{
		denseMeta:    make([]byte, 1, 2),
		denseResults: make([]documentservice.RawDenseVectorResult, 1, 2),
		idsScratch:   make([][]byte, 1, 2),
		docsScratch:  make([][]byte, 1, 2),
		denseFilters: make([]documentservice.Filter, 1, 2),
	}
	state.denseResults[0] = documentservice.RawDenseVectorResult{ID: []byte("id"), Document: []byte("document")}
	state.idsScratch[0] = []byte("id")
	state.docsScratch[0] = []byte("document")
	state.denseFilters[0] = documentservice.Filter{Field: "meta.name", Operator: "==", Value: "retained"}
	state.denseFilter = documentservice.Filter{Operator: "AND", Conditions: state.denseFilters[:1]}

	clearDenseVectorSearchScratch(&state)
	if len(state.denseResults) != 0 || len(state.idsScratch) != 0 || len(state.docsScratch) != 0 || len(state.denseFilters) != 0 {
		t.Fatalf("scratch lengths results=%d ids=%d docs=%d filters=%d want zero", len(state.denseResults), len(state.idsScratch), len(state.docsScratch), len(state.denseFilters))
	}
	if cap(state.denseMeta) != 2 || cap(state.denseResults) != 2 || cap(state.idsScratch) != 2 || cap(state.docsScratch) != 2 || cap(state.denseFilters) != 2 {
		t.Fatalf("scratch capacities meta=%d results=%d ids=%d docs=%d filters=%d want two", cap(state.denseMeta), cap(state.denseResults), cap(state.idsScratch), cap(state.docsScratch), cap(state.denseFilters))
	}
	for _, result := range state.denseResults[:cap(state.denseResults)] {
		if result.ID != nil || result.Document != nil {
			t.Fatalf("retained dense result pointers: %+v", result)
		}
	}
	for _, ids := range state.idsScratch[:cap(state.idsScratch)] {
		if ids != nil {
			t.Fatalf("retained ID pointer: %q", ids)
		}
	}
	for _, docs := range state.docsScratch[:cap(state.docsScratch)] {
		if docs != nil {
			t.Fatalf("retained document pointer: %q", docs)
		}
	}
	for _, filter := range state.denseFilters[:cap(state.denseFilters)] {
		if filter.Field != "" || filter.Operator != "" || filter.Value != nil || len(filter.Conditions) != 0 {
			t.Fatalf("retained filter: %+v", filter)
		}
	}
	if state.denseFilter.Field != "" || state.denseFilter.Operator != "" || state.denseFilter.Value != nil || len(state.denseFilter.Conditions) != 0 {
		t.Fatalf("retained root filter: %+v", state.denseFilter)
	}

	state.denseResults = make([]documentservice.RawDenseVectorResult, 1, maxRetainedGetManyScratchItems+1)
	state.idsScratch = make([][]byte, 1, maxRetainedGetManyScratchItems+1)
	state.docsScratch = make([][]byte, 1, maxRetainedGetManyScratchItems+1)
	state.vectorQuery = make([]float32, 1, maxRetainedGetManyScratchItems+1)
	state.denseMeta = make([]byte, 1, maxRetainedGetManyPayloadBytes+1)
	state.denseResults[0] = documentservice.RawDenseVectorResult{ID: []byte("id"), Document: []byte("document")}
	state.idsScratch[0] = []byte("id")
	state.docsScratch[0] = []byte("document")
	clearDenseVectorSearchScratch(&state)
	if state.denseMeta != nil || state.denseResults != nil || state.idsScratch != nil || state.docsScratch != nil || state.vectorQuery != nil {
		t.Fatalf("oversized scratch retained meta=%d results=%d ids=%d docs=%d query=%d", cap(state.denseMeta), cap(state.denseResults), cap(state.idsScratch), cap(state.docsScratch), cap(state.vectorQuery))
	}
}

func TestRetainSmallPayloadScratchBoundsCapacity(t *testing.T) {
	client := Client{
		denseRequest: make([]byte, 1, maxRetainedGetManyPayloadBytes),
		requestBody:  make([]byte, 1, maxRetainedGetManyPayloadBytes),
	}
	client.denseRequest = retainSmallPayloadScratch(client.denseRequest)
	client.requestBody = retainSmallPayloadScratch(client.requestBody)
	if len(client.denseRequest) != 0 || cap(client.denseRequest) != maxRetainedGetManyPayloadBytes || len(client.requestBody) != 0 || cap(client.requestBody) != maxRetainedGetManyPayloadBytes {
		t.Fatalf("threshold scratch retained dense=%d/%d body=%d/%d", len(client.denseRequest), cap(client.denseRequest), len(client.requestBody), cap(client.requestBody))
	}
	if got := retainSmallPayloadScratch(make([]byte, 1, maxRetainedGetManyPayloadBytes+1)); got != nil {
		t.Fatalf("oversized payload retained cap=%d", cap(got))
	}
}

func TestDenseVectorSearchBoundsOversizedRequestScratch(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	client := NewClient(clientConn)
	t.Cleanup(func() { _ = client.Close(); _ = serverConn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() {
		header, err := readOrdinaryDenseTestRequest(serverConn)
		if err == nil {
			body, appendErr := iwire.AppendSection(nil, iwire.Section{ID: iwire.SectionError, Bytes: appendErrorPayload(nil, iwire.ErrInvalidCommand, false, "test")})
			if appendErr != nil {
				err = appendErr
			} else {
				err = writeFrame(serverConn, iwire.Header{Type: iwire.FrameError, RequestID: header.RequestID}, body)
			}
		}
		serverErr <- err
	}()
	if err := client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DenseVectorSearch(ctx, DenseVectorSearchRequest{
		Index: "docs", Query: make([]float32, maxRetainedGetManyPayloadBytes/4+1), TopK: 1,
	}); !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("oversized request err=%v want fake server rejection", err)
	}
	waitOrdinaryDenseTestServer(t, ctx, serverErr)
	if client.denseRequest != nil || client.requestBody != nil {
		t.Fatalf("oversized request scratch retained dense=%d body=%d", cap(client.denseRequest), cap(client.requestBody))
	}
}

func TestDenseVectorSearchReleasesRejectedOversizedResponse(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	client := NewClient(clientConn)
	t.Cleanup(func() { _ = client.Close(); _ = serverConn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const count = maxRetainedGetManyScratchItems + 1
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := range ids {
		ids[i] = []byte("id")
		docs[i] = []byte("document")
	}
	// Use a truthful ordinary v5 route proof so this refusal exercises the
	// oversized result count, independently of retired response-tag rejection.
	meta := appendVersionedDenseVectorSearchResponse(nil, documentservice.RawDenseVectorSearchResponse{
		Route: documentservice.RouteExact, Exact: true, Candidates: count, DocumentsScanned: count,
		Results: make([]documentservice.RawDenseVectorResult, count),
	}, iwire.DenseVectorSearchOrdinaryVersion)
	response, err := iwire.AppendSection(nil, iwire.Section{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, ids...)})
	if err != nil {
		t.Fatal(err)
	}
	response, err = iwire.AppendSection(response, iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, docs...)})
	if err != nil {
		t.Fatal(err)
	}
	response, err = iwire.AppendSection(response, iwire.Section{ID: iwire.SectionDenseSearchResponse, Bytes: meta})
	if err != nil {
		t.Fatal(err)
	}
	if len(response) <= maxBufferedWriteFrameBody {
		t.Fatalf("response length=%d want >%d", len(response), maxBufferedWriteFrameBody)
	}
	errCh := make(chan error, 1)
	go func() {
		header, err := readOrdinaryDenseTestRequest(serverConn)
		if err == nil {
			err = writeFrame(serverConn, iwire.Header{Type: iwire.FrameResponse, RequestID: header.RequestID}, response)
		}
		errCh <- err
	}()
	if err := client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1}, TopK: 1}); nativeCodeOf(err) != iwire.ErrMalformedFrame || !strings.Contains(err.Error(), "dense response lengths do not match") {
		t.Fatalf("dense response err=%v want malformed result-count refusal", err)
	}
	waitOrdinaryDenseTestServer(t, ctx, errCh)
	if client.readBody != nil || client.denseIDs != nil || client.denseDocuments != nil || client.denseResults != nil {
		t.Fatalf("retained rejected response body=%d ids=%d docs=%d results=%d", cap(client.readBody), cap(client.denseIDs), cap(client.denseDocuments), cap(client.denseResults))
	}
	if len(client.vectorSections) != 0 {
		t.Fatalf("retained rejected response sections=%d", len(client.vectorSections))
	}
	for _, section := range client.vectorSections[:cap(client.vectorSections)] {
		if section.Bytes != nil {
			t.Fatalf("retained rejected response section: %+v", section)
		}
	}
}

// Each fake peer performs a real Hello exchange and verifies the subsequent
// request is the retained ordinary command. Deadlines also bound regressions
// that cause the client to refuse locally before sending a request.
func readOrdinaryDenseTestRequest(conn net.Conn) (iwire.Header, error) {
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return iwire.Header{}, err
	}
	header, _, err := readFrame(conn, iwire.DefaultLimits())
	if err != nil {
		return iwire.Header{}, err
	}
	if header.Type != iwire.FrameHello {
		return iwire.Header{}, fmt.Errorf("fake dense peer expected Hello, got frame %d", header.Type)
	}
	body, err := iwire.AppendSection(nil, iwire.Section{ID: iwire.SectionCapabilitySet, Bytes: appendStringMap(nil, map[string]string{"dense_vector_search_versions": "5"})})
	if err != nil {
		return iwire.Header{}, err
	}
	if err := writeFrame(conn, iwire.Header{Type: iwire.FrameHelloOK, RequestID: header.RequestID}, body); err != nil {
		return iwire.Header{}, err
	}
	header, body, err = readFrame(conn, iwire.DefaultLimits())
	if err != nil {
		return iwire.Header{}, err
	}
	if header.Type != iwire.FrameRequest {
		return iwire.Header{}, fmt.Errorf("fake dense peer expected request, got frame %d", header.Type)
	}
	sections, err := iwire.DecodeSections(body, iwire.DefaultLimits())
	if err != nil {
		return iwire.Header{}, err
	}
	cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		return iwire.Header{}, err
	}
	if cmd.Header.ID != iwire.CommandDenseVectorSearch || cmd.Header.Version != iwire.DenseVectorSearchOrdinaryVersion {
		return iwire.Header{}, fmt.Errorf("fake dense peer got command %d version %d, want ordinary dense v5", cmd.Header.ID, cmd.Header.Version)
	}
	return header, nil
}

func waitOrdinaryDenseTestServer(t *testing.T, ctx context.Context, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("fake dense server: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("fake dense server did not finish: %v", ctx.Err())
	}
}

func denseSearchTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func TestDenseVectorSearchExpiredDeadlineStopsBeforeSearch(t *testing.T) {
	request, err := appendDenseVectorSearchRequest(nil, DenseVectorSearchRequest{Index: "docs", Query: []float32{1}, TopK: 1}, iwire.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	deadline := binary.AppendUvarint(nil, uint64(time.Now().Add(-time.Second).UnixNano()))
	server := NewServer(ServerOptions{})
	defer server.Close()
	_, err = server.handleVersionedDenseVectorSearch(context.Background(), &connState{}, iwire.DenseVectorSearchOrdinaryVersion, []iwire.Section{
		{ID: iwire.SectionDenseSearchRequest, Bytes: request},
		{ID: iwire.SectionDeadline, Bytes: deadline},
	}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired dense search error=%v want deadline exceeded", err)
	}
}

func TestClientClearBorrowedResponseViewsBoundsDenseTables(t *testing.T) {
	client := Client{
		vectorSections: make([]iwire.Section, 1, 2),
		denseIDs:       make([][]byte, 1, 2),
		denseDocuments: make([][]byte, 1, 2),
		denseResults:   make([]DenseVectorSearchResult, 1, 2),
	}
	client.vectorSections[0].Bytes = []byte("section")
	client.denseIDs[0] = []byte("id")
	client.denseDocuments[0] = []byte("document")
	client.denseResults[0] = DenseVectorSearchResult{ID: []byte("id"), Document: []byte("document")}
	client.clearBorrowedResponseViews()
	if len(client.vectorSections) != 0 || len(client.denseIDs) != 0 || len(client.denseDocuments) != 0 || len(client.denseResults) != 0 {
		t.Fatalf("retained client views sections=%d ids=%d docs=%d results=%d", len(client.vectorSections), len(client.denseIDs), len(client.denseDocuments), len(client.denseResults))
	}
	if cap(client.vectorSections) != 2 || cap(client.denseIDs) != 2 || cap(client.denseDocuments) != 2 || cap(client.denseResults) != 2 {
		t.Fatalf("client scratch capacities sections=%d ids=%d docs=%d results=%d want two", cap(client.vectorSections), cap(client.denseIDs), cap(client.denseDocuments), cap(client.denseResults))
	}

	client.denseIDs = make([][]byte, 1, maxRetainedGetManyScratchItems+1)
	client.denseDocuments = make([][]byte, 1, maxRetainedGetManyScratchItems+1)
	client.denseResults = make([]DenseVectorSearchResult, 1, maxRetainedGetManyScratchItems+1)
	client.denseIDs[0] = []byte("id")
	client.denseDocuments[0] = []byte("document")
	client.denseResults[0] = DenseVectorSearchResult{ID: []byte("id"), Document: []byte("document")}
	client.clearBorrowedResponseViews()
	if client.denseIDs != nil || client.denseDocuments != nil || client.denseResults != nil {
		t.Fatalf("oversized client views retained ids=%d docs=%d results=%d", cap(client.denseIDs), cap(client.denseDocuments), cap(client.denseResults))
	}
}
