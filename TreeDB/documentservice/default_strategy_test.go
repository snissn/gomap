package documentservice

import (
	"context"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestServiceDefaultNativeRuntimePublic(t *testing.T) {
	for _, native := range []bool{false, true} {
		role := "legacy"
		if native {
			role = "native"
		}
		for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
			t.Run(role+"/"+string(profile), func(t *testing.T) {
				opts := treedb.OptionsFor(profile, t.TempDir())
				opts.MemtableMode = "cow_btree"
				opts.BackgroundCheckpointInterval = -1
				opts.BackgroundCheckpointIdleDuration = -1
				var manager *collections.CollectionManager
				var svc *Service
				var closeOwner func() error
				closeAll := func() {
					if svc != nil {
						if err := svc.Close(); err != nil {
							t.Error(err)
						}
						svc = nil
					}
					if manager != nil {
						if err := manager.Close(); err != nil {
							t.Error(err)
						}
						manager = nil
					}
					if closeOwner != nil {
						if err := closeOwner(); err != nil {
							t.Error(err)
						}
						closeOwner = nil
					}
				}
				t.Cleanup(closeAll)
				open := func() {
					if native {
						owner, err := treedb.Open(opts)
						if err != nil {
							t.Fatal(err)
						}
						closeOwner = owner.Close
						backend, err := owner.NativeSemanticBackend()
						if err != nil {
							t.Fatal(err)
						}
						manager = collections.NewCollectionManager(backend)
					} else {
						backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
						if err != nil {
							t.Fatal(err)
						}
						closeOwner = cleanup
						manager = collections.NewCollectionManager(backend)
					}
					svc = New(manager)
				}
				open()
				ctx := context.Background()
				req := CreateIndexRequest{Name: "docs", Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}}
				info, err := svc.CreateIndex(ctx, req)
				if err != nil || info.VectorStrategy != collections.VectorIndexStrategyNativeRuntime {
					t.Fatalf("default: %+v %v", info, err)
				}
				col, err := manager.OpenCollection(req.Name)
				if err != nil {
					t.Fatal(err)
				}
				before := col.Meta()
				again, err := svc.CreateIndex(ctx, req)
				if err != nil || again.Generation != info.Generation || !reflect.DeepEqual(before, col.Meta()) {
					t.Fatalf("idempotent: %+v %v", again, err)
				}
				mismatch := req
				mismatch.VectorIndexOptions = &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}
				if _, err := svc.CreateIndex(ctx, mismatch); ErrorCodeOf(err) != CodeConflict {
					t.Fatalf("explicit mismatch: %v", err)
				}
				if !reflect.DeepEqual(before, col.Meta()) {
					t.Fatal("conflicting create changed schema")
				}
				if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "alpha", Embedding: []float32{1, 0}, Meta: map[string]any{"tenant": "one", "nested": map[string]any{"label": "alpha"}}}, {ID: "b", Content: "beta", Embedding: []float32{0, 1}, Meta: map[string]any{"tenant": "two"}}}}); err != nil {
					t.Fatal(err)
				}
				eq := Filter{Field: "meta.tenant", Operator: "==", Value: "one"}
				lower := Filter{Field: "meta.tenant", Operator: ">=", Value: "one"}
				upper := Filter{Field: "meta.tenant", Operator: "<", Value: "two"}
				check := func() {
					count, err := svc.CountDocuments(ctx, "docs", CountDocumentsRequest{})
					if err != nil || count.Count != 2 {
						t.Fatalf("count: %+v %v", count, err)
					}
					for _, filter := range []*Filter{&eq, &upper, {Operator: "AND", Conditions: []Filter{lower, upper}}} {
						dense, err := svc.SearchDenseVector(ctx, "docs", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 2, Filter: filter})
						if err != nil || len(dense.Documents) != 1 || dense.Documents[0].ID != "a" || dense.Route != RouteAnn || dense.Exact {
							t.Fatalf("dense: %+v %v", dense, err)
						}
						hybrid, err := svc.SearchHybrid(ctx, "docs", HybridSearchRequest{Query: "alpha", QueryEmbedding: []float32{1, 0}, TopK: 2, TextCandidateLimit: 2, VectorCandidateLimit: 2, Filter: filter})
						if err != nil || len(hybrid.Documents) != 1 || hybrid.Documents[0].ID != "a" || hybrid.Stats.FullDocumentScanFallbacks != 0 || hybrid.Stats.VectorCandidatesReturned == 0 {
							t.Fatalf("hybrid: %+v %v", hybrid, err)
						}
					}
					// These valid filters were served by the original exact default.
					// They need no newly declared index and never enter ANN implicitly.
					for _, filter := range []*Filter{
						{Field: "meta.nested.label", Operator: "==", Value: "alpha"},
						{Field: "meta.tenant", Operator: "!=", Value: "two"},
						{Operator: "OR", Conditions: []Filter{eq, {Field: "meta.tenant", Operator: "==", Value: "absent"}}},
					} {
						request := DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 2, Filter: filter}
						exact, err := svc.SearchDenseVector(ctx, "docs", request)
						if err != nil || exact.Route != RouteExact || !exact.Exact || exact.Candidates != 1 || len(exact.Documents) != 1 || exact.Documents[0].ID != "a" {
							t.Fatalf("existing exact filter: %+v %v", exact, err)
						}
						request.Route = RouteAnn
						if _, err := svc.SearchDenseVector(ctx, "docs", request); err == nil {
							t.Fatal("explicit ANN accepted an unsupported scalar filter")
						}
					}
					if _, err := svc.SearchDenseVector(ctx, "docs", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 2, Filter: &Filter{Operator: "AND"}}); ErrorCodeOf(err) != CodeInvalidRequest {
						t.Fatalf("malformed filter: %v", err)
					}
				}
				check()
				if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "alpha changed", Embedding: []float32{0.9, 0.1}, Meta: map[string]any{"tenant": "one", "nested": map[string]any{"label": "alpha"}}}}}); err != nil {
					t.Fatal(err)
				}
				check()
				if _, err := svc.OptimizeIndex(ctx, "docs", OptimizeIndexRequest{}); err != nil {
					t.Fatal(err)
				}
				closeAll()
				open()
				again, err = svc.CreateIndex(ctx, req)
				if err != nil || again.Generation != info.Generation || again.VectorStrategy != info.VectorStrategy {
					t.Fatalf("reopen default: %+v %v", again, err)
				}
				check()
			})
		}
	}
}

func TestServiceExistingStrategyPreserved(t *testing.T) {
	for _, strategy := range []collections.VectorIndexStrategy{collections.VectorIndexStrategyNativeRuntime, collections.VectorIndexStrategyColumnGraph} {
		t.Run(string(strategy), func(t *testing.T) {
			svc, db := newTestService(t)
			defer db.Close()
			defer svc.Close()
			ctx := context.Background()
			req := CreateIndexRequest{Name: "stored", Dimension: 2, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: strategy, M: 8, EfConstruction: 24, EfSearch: 12}}
			info, err := svc.CreateIndex(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			col, err := svc.manager.OpenCollection(req.Name)
			if err != nil {
				t.Fatal(err)
			}
			before := col.Meta()
			// Reopen via an independent service; only strategy is inferred, never tuning.
			other := New(collections.NewCollectionManager(db))
			defer other.Close()
			omitted := *req.VectorIndexOptions
			omitted.Strategy = ""
			req.VectorIndexOptions = &omitted
			again, err := other.CreateIndex(ctx, req)
			if err != nil || again.Generation != info.Generation || again.VectorStrategy != strategy || !reflect.DeepEqual(before, col.Meta()) {
				t.Fatalf("stored: %+v %v", again, err)
			}
			req.VectorIndexOptions = nil
			if _, err := other.CreateIndex(ctx, req); ErrorCodeOf(err) != CodeConflict {
				t.Fatalf("omitted nondefault tuning accepted: %v", err)
			}
			mismatch := omitted
			if strategy == collections.VectorIndexStrategyColumnGraph {
				mismatch.Strategy = collections.VectorIndexStrategyNativeRuntime
			} else {
				mismatch.Strategy = collections.VectorIndexStrategyColumnGraph
			}
			req.VectorIndexOptions = &mismatch
			if _, err := other.CreateIndex(ctx, req); ErrorCodeOf(err) != CodeConflict {
				t.Fatalf("explicit mismatch: %v", err)
			}
			if !reflect.DeepEqual(before, col.Meta()) {
				t.Fatal("conflicting create changed schema")
			}
		})
	}
	// Old default ColumnGraph declarations survive an actual owner reopen.
	dir := t.TempDir()
	db, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(collections.NewCollectionManager(db))
	req := CreateIndexRequest{Name: "old-default", Dimension: 2, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}}
	first, err := svc.CreateIndex(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	svc = New(collections.NewCollectionManager(db))
	defer svc.Close()
	req.VectorIndexOptions = nil
	again, err := svc.CreateIndex(context.Background(), req)
	if err != nil || again.Generation != first.Generation || again.VectorStrategy != first.VectorStrategy {
		t.Fatalf("old default: %+v %v", again, err)
	}
}

func TestServiceDefaultStrategyEligibility(t *testing.T) {
	for _, test := range []struct {
		name            string
		opts            *BenchmarkVectorIndexOptions
		defaultStrategy collections.VectorIndexStrategy
		want            collections.VectorIndexStrategy
		invalid         bool
	}{
		{name: "ordinary", want: collections.VectorIndexStrategyNativeRuntime},
		{name: "typed", defaultStrategy: collections.VectorIndexStrategyColumnGraph, want: collections.VectorIndexStrategyColumnGraph},
		{name: "normalized", opts: &BenchmarkVectorIndexOptions{Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1}, want: collections.VectorIndexStrategyColumnGraph},
		{name: "quantized", opts: &BenchmarkVectorIndexOptions{QuantizedIndexes: []QuantizedIndexInfo{{Name: "q"}}}, want: collections.VectorIndexStrategyColumnGraph},
		{name: "explicit-native-normalized", opts: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime, Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1}, invalid: true},
		{name: "explicit-native-quantized", opts: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime, QuantizedIndexes: []QuantizedIndexInfo{{Name: "q"}}}, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fallback := test.defaultStrategy
			if fallback == "" {
				fallback = collections.VectorIndexStrategyNativeRuntime
			}
			got, err := benchmarkVectorIndexOptionsForCreate(MetricCosine, test.opts, fallback)
			if test.invalid {
				if ErrorCodeOf(err) != CodeInvalidRequest {
					t.Fatalf("invalid accepted: %+v %v", got, err)
				}
				return
			}
			if err != nil || got.strategy != test.want {
				t.Fatalf("got %+v %v", got, err)
			}
		})
	}
}
