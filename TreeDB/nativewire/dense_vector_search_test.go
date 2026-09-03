package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestDenseVectorSearchNativewireParityAndBorrowing(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
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
		VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyNativeRuntime,
		},
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
	if _, err = svc.UpsertDocuments(ctx, "docs", documentservice.UpsertDocumentsRequest{Documents: documents}); err != nil {
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

	filters := []*documentservice.Filter{
		{Field: "meta.user_id", Operator: "==", Value: "alpha"},
		{Operator: "AND", Conditions: []documentservice.Filter{
			{Field: "meta.user_id", Operator: "==", Value: "alpha"},
			{Field: "meta.rank", Operator: ">=", Value: int64(3)},
		}},
	}
	for _, filter := range filters {
		request := DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 2, EfSearch: 8, Filter: filter}
		got, err := client.DenseVectorSearch(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		wantRawResults, err := svc.SearchDenseVectorNativeRaw(ctx, request.Index, documentservice.DenseVectorSearchRequest{
			QueryEmbedding: request.Query,
			TopK:           request.TopK,
			EfSearch:       request.EfSearch,
			Route:          documentservice.RouteAnn,
			Filter:         request.Filter,
		})
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
		if got.Route != documentservice.RouteAnn || !got.NativeBasePlusLiveDelta || got.ExactFallbacks != 0 || got.FullDocumentScanFallbacks != 0 {
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
		t.Fatalf("OR filter error=%v code=%d", err, nativeCodeOf(err))
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
	_ = client.Hello(ctx)
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
	if _, err := client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 1}); !isRemoteError(err, iwire.ErrReadOnly) || !strings.Contains(err.Error(), "dense vector search") {
		t.Fatalf("routed dense search err=%v want read-only route rejection", err)
	}
	if routes := routeProvider.snapshotRoutes(); len(routes) != 0 {
		t.Fatalf("routed dense search reached route provider: %+v", routes)
	}
}

func TestDenseVectorSearchInvalidatesPreparedCacheOnNativeIndexMetadataMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	mgr := collections.NewCollectionManager(db)
	svc := documentservice.New(mgr)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, DocumentService: svc})
	t.Cleanup(func() { _ = server.Close(); _ = svc.Close(); _ = db.Close() })
	ctx := denseSearchTestContext(t)
	if _, err := svc.CreateIndex(ctx, documentservice.CreateIndexRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    documentservice.MetricCosine,
		VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyNativeRuntime,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", documentservice.UpsertDocumentsRequest{Documents: []documentservice.Document{{
		ID: "alpha", Content: "alpha", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "alpha"},
	}}}); err != nil {
		t.Fatal(err)
	}
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if _, err := client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 1, EfSearch: 8}); err != nil {
		t.Fatalf("warm dense search: %v", err)
	}
	if _, err := client.CreateIndex(ctx, "docs", collections.IndexDefinition{Name: "meta_user_id", Field: "meta.user_id", ValueType: collections.IndexValueString}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	filtered := DenseVectorSearchRequest{
		Index: "docs", Query: []float32{1, 0}, TopK: 1, EfSearch: 8,
		Filter: &documentservice.Filter{Field: "meta.user_id", Operator: "==", Value: "alpha"},
	}
	if _, err := client.DenseVectorSearch(ctx, filtered); err != nil && !isRemoteError(err, iwire.ErrConsistencyUnavailable) {
		t.Fatalf("filtered search after create err=%v; new scalar schema was not observed", err)
	}
	if _, err := client.DropIndex(ctx, "docs", "meta_user_id"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}
	if _, err := client.DenseVectorSearch(ctx, filtered); !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("filtered search after drop err=%v want invalid command", err)
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
	serverErr := make(chan error, 1)
	go func() {
		header, _, err := readFrame(serverConn, iwire.DefaultLimits())
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
	if _, err := client.DenseVectorSearch(denseSearchTestContext(t), DenseVectorSearchRequest{
		Index: "docs", Query: make([]float32, maxRetainedGetManyPayloadBytes/4+1), TopK: 1,
	}); err == nil {
		t.Fatal("dense search unexpectedly succeeded")
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("fake server: %v", err)
	}
	if client.denseRequest != nil || client.requestBody != nil {
		t.Fatalf("oversized request scratch retained dense=%d body=%d", cap(client.denseRequest), cap(client.requestBody))
	}
}

func TestDenseVectorSearchReleasesRejectedOversizedResponse(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	client := NewClient(clientConn)
	t.Cleanup(func() { _ = client.Close(); _ = serverConn.Close() })
	const count = maxRetainedGetManyScratchItems + 1
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := range ids {
		ids[i] = []byte("id")
		docs[i] = []byte("document")
	}
	meta := []byte{1}
	meta = binary.AppendUvarint(meta, count)
	meta = binary.AppendUvarint(meta, 0)
	meta = binary.AppendUvarint(meta, 0)
	meta = binary.AppendUvarint(meta, count)
	for range count {
		meta = binary.LittleEndian.AppendUint64(meta, math.Float64bits(0))
	}
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
		header, _, err := readFrame(serverConn, iwire.DefaultLimits())
		if err == nil {
			err = writeFrame(serverConn, iwire.Header{Type: iwire.FrameResponse, RequestID: header.RequestID}, response)
		}
		errCh <- err
	}()
	if _, err := client.DenseVectorSearch(denseSearchTestContext(t), DenseVectorSearchRequest{Index: "docs", Query: []float32{1}, TopK: 1}); nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("dense response err=%v want malformed frame", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server response: %v", err)
	}
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
	_, err = server.handleDenseVectorSearch(context.Background(), &connState{}, []iwire.Section{
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
