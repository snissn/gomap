package nativewire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

// Reproduce the public command fixture without its timing loop or external
// processes: equal vectors repeat every 17 documents in the 4096-row corpus.
func TestDenseVectorSearchOrdinaryRepeatedVectorTies(t *testing.T) {
	ctx := denseSearchTestContext(t)
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	if !t.Run("seed", func(t *testing.T) {
		service, _ := newOrdinaryTieTestService(t, opts)
		_, err := service.CreateIndex(ctx, documentservice.CreateIndexRequest{
			Name: "docs", Dimension: 8,
			VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 16, EfConstruction: 128, EfSearch: 64},
			ScalarFields: []documentservice.ScalarFieldDeclaration{
				{Field: "meta.tenant", ValueType: documentservice.ScalarFieldString},
				{Field: "meta.rank", ValueType: documentservice.ScalarFieldInt64},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		for first := 0; first < 4096; first += 128 {
			docs := make([]documentservice.Document, 128)
			for j := range docs {
				i := first + j
				vector := make([]float32, 8)
				vector[0] = 1
				for d := 1; d < 8; d++ {
					vector[d] = float32((i+d)%17) / 17
				}
				docs[j] = documentservice.Document{ID: fmt.Sprintf("d%06d", i), Content: "refund policy " + strings.Repeat("payload ", 32), Embedding: vector, Meta: map[string]any{"tenant": fmt.Sprintf("t%02d", i%16), "rank": i}}
			}
			if _, err := service.UpsertDocuments(ctx, "docs", documentservice.UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := service.OptimizeIndex(ctx, "docs", documentservice.OptimizeIndexRequest{}); err != nil {
			t.Fatal(err)
		}
	}) {
		return
	}
	service, server := newOrdinaryTieTestService(t, opts)
	request := DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0, 0, 0, 0, 0, 0, 0}, TopK: 10, EfSearch: 64}
	raw, err := service.SearchDenseVectorRawInto(ctx, request.Index, documentservice.DenseVectorSearchRequest{QueryEmbedding: request.Query, TopK: request.TopK, EfSearch: request.EfSearch}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]DenseVectorSearchResult, len(raw.Results))
	for i, result := range raw.Results {
		rows[i] = DenseVectorSearchResult{ID: bytes.Clone(result.ID), Score: result.Score, Document: bytes.Clone(result.Document)}
		t.Logf("row[%d] id=%q score=%.17g document=%s", i, result.ID, result.Score, result.Document)
		if result.Score != raw.Results[0].Score {
			t.Fatal("fixture results did not all tie")
		}
	}
	t.Logf("predicates IDs=%t documents=%t v3_score_ID_order=%t; route=%s candidates=%d prepared=%d pack=%d fallback=%d scanned=%d", denseV3ResultsHaveValidIDs(rows), denseResultDocumentsMatchRequest(rows, request, false), denseV3ResultsOrdered(rows), raw.Route, raw.Candidates, raw.ColumnGraphPreparedSearch, raw.SearchPackSearch, raw.ColumnGraphFallbackSearch, raw.DocumentsScanned)
	if len(rows) != request.TopK || !denseV3ResultsHaveValidIDs(rows) || !denseResultDocumentsMatchRequest(rows, request, false) || denseV3ResultsOrdered(rows) {
		t.Fatal("fixture did not isolate equal-score nonlexical ties")
	}
	requestRaw, err := appendDenseVectorSearchRequest(nil, request, server.limits)
	if err != nil {
		t.Fatal(err)
	}
	body, err := server.handleVersionedDenseVectorSearch(ctx, &connState{}, iwire.DenseVectorSearchOrdinaryVersion, []iwire.Section{
		{ID: iwire.SectionDenseSearchRequest, Bytes: requestRaw},
		{ID: iwire.SectionDeadline, Bytes: binary.AppendUvarint(nil, uint64(time.Now().Add(time.Minute).UnixNano()))},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sections, err := iwire.DecodeSections(body, server.limits)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateOrdinaryDenseSections(sections); err != nil {
		t.Fatal(err)
	}
	ids, _, err := singletonSection(sections, iwire.SectionDocumentIDs)
	if err != nil {
		t.Fatal(err)
	}
	docs, _, err := singletonSection(sections, iwire.SectionDocuments)
	if err != nil {
		t.Fatal(err)
	}
	meta, _, err := singletonSection(sections, iwire.SectionDenseSearchResponse)
	if err != nil {
		t.Fatal(err)
	}
	wire, _, _, _, err := decodeVersionedDenseVectorSearchResponse(iwire.DenseVectorSearchOrdinaryVersion, ids, docs, meta, request.TopK, server.limits, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("decoded wire predicates IDs=%t documents=%t v3_score_ID_order=%t", denseV3ResultsHaveValidIDs(wire.Results), denseResultDocumentsMatchRequest(wire.Results, request, false), denseV3ResultsOrdered(wire.Results))
	if len(wire.Results) != len(rows) {
		t.Fatal("wire changed result count")
	}
	for i, row := range rows {
		if !bytes.Equal(wire.Results[i].ID, row.ID) || wire.Results[i].Score != row.Score || !bytes.Equal(wire.Results[i].Document, row.Document) {
			t.Fatalf("decoded wire changed row[%d]", i)
		}
	}
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
	got, err := client.DenseVectorSearch(ctx, request)
	if err != nil {
		t.Fatalf("ordinary ANN rejected valid physical-order ties: %v", err)
	}
	if got.Route != raw.Route || got.Candidates != raw.Candidates || got.SearchPackSearch != raw.SearchPackSearch || got.DocumentsScanned != raw.DocumentsScanned || len(got.Results) != len(rows) {
		t.Fatalf("wire changed route or counts: %+v raw=%+v", got, raw)
	}
	for i, row := range rows {
		if string(got.Results[i].ID) != string(row.ID) || got.Results[i].Score != row.Score || string(got.Results[i].Document) != string(row.Document) {
			t.Fatalf("wire changed row[%d]: %+v want %+v", i, got.Results[i], row)
		}
	}
}

func newOrdinaryTieTestService(t *testing.T, opts treedb.Options) (*documentservice.Service, *Server) {
	t.Helper()
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
	service := documentservice.NewWithDeferredVectorBuildMaintenance(manager, owner.DeferredVectorBuildMaintenance())
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Error(err)
		}
	})
	server := NewServer(ServerOptions{Collections: manager, Backend: backend, DocumentService: service})
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
	return service, server
}
