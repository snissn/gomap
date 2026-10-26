package documentservice

import (
	"context"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

// Exercise the captured ColumnGraph reader on both sides of the maintained
// allow-set cutoff, including the vector-only path and empty predicates.
func TestNativeOrdinaryColumnGraphHybridFilters(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
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
	svc := NewWithDeferredVectorBuildMaintenance(manager, owner.DeferredVectorBuildMaintenance())
	t.Cleanup(func() {
		if err := svc.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "filters", Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}, {Field: "meta.scope", ValueType: ScalarFieldString}}})
	if err != nil {
		t.Fatal(err)
	}
	if info.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
		t.Fatalf("strategy=%s", info.VectorStrategy)
	}
	docs := make([]Document, 64)
	for i := range docs {
		tenant := "common"
		if i < 4 {
			tenant = "rare"
		}
		docs[i] = Document{ID: fmt.Sprintf("d%03d", i), Content: "alpha policy", Embedding: []float32{1, float32(i+1) / 64}, Meta: map[string]any{"tenant": tenant, "scope": "all"}}
	}
	if _, err := svc.UpsertDocuments(ctx, "filters", UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.OptimizeIndex(ctx, "filters", OptimizeIndexRequest{}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, field, value string
		text               bool
		candidates         int
		want               int
	}{
		{"small_allow_set", "scope", "all", true, 64, 5},
		{"ann_postfilter", "scope", "all", true, 8, 5},
		{"vector_only_small", "scope", "all", false, 64, 5},
		{"vector_only_postfilter", "scope", "all", false, 8, 5},
		{"selective_exact", "tenant", "rare", true, 8, 4},
		{"vector_only_selective_exact", "tenant", "rare", false, 8, 4},
		{"empty_mixed", "tenant", "missing", true, 8, 0},
		{"empty_vector_only", "tenant", "missing", false, 8, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := HybridSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 5, VectorCandidateLimit: tc.candidates, EfSearch: 64, Filter: &Filter{Field: "meta." + tc.field, Operator: "==", Value: tc.value}}
			if tc.text {
				req.Query = "policy"
				req.TextCandidateLimit = tc.candidates
			}
			out, err := svc.SearchHybrid(ctx, "filters", req)
			if err != nil {
				t.Fatal(err)
			}
			if len(out.Documents) != tc.want || out.Stats.FullDocumentScanFallbacks != 0 {
				t.Fatalf("count=%d want=%d stats=%+v", len(out.Documents), tc.want, out.Stats)
			}
			if tc.want == 0 && (out.Stats.VectorCandidatesReturned != 0 || out.Stats.VectorCandidatesExamined != 0) {
				t.Fatalf("empty predicate traversed graph: %+v", out.Stats)
			}
			for _, doc := range out.Documents {
				if doc.Meta[tc.field] != tc.value || len(doc.Embedding) != 0 {
					t.Fatalf("filter/projection: %+v", doc)
				}
			}
		})
	}
}
