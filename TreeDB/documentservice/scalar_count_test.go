package documentservice

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestServiceScalarCountLifecycle(t *testing.T) {
	for _, typed := range []bool{false, true} {
		name := "legacy"
		if typed {
			name = "typed"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			db, err := backenddb.Open(testBackendOptions(dir))
			if err != nil {
				t.Fatal(err)
			}
			svc := New(collections.NewCollectionManager(db))
			defer func() { _ = svc.Close(); _ = db.Close() }()
			create := CreateIndexRequest{Name: "counts", Dimension: 8, TypedInput: typed, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}, {Field: "meta.fpath", ValueType: ScalarFieldString}}}
			if typed {
				create.VectorIndexOptions = &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 2}
			}
			info, err := svc.CreateIndex(ctx, create)
			if err != nil {
				t.Fatal(err)
			}
			docs := []Document{
				{ID: "a", Content: "alpha", Embedding: []float32{1, 0, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "u", "fpath": "/target"}},
				{ID: "b", Content: "beta", Embedding: []float32{0, 1, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "u", "fpath": "/other"}},
				{ID: "c", Content: "gamma", Embedding: []float32{0, 0, 1, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "v", "fpath": "/target"}},
			}
			if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: typed}); err != nil {
				t.Fatal(err)
			}
			if typed {
				options := typedServiceTestOptions()
				if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphServing: &options}); err != nil {
					t.Fatal(err)
				}
			}
			eq := &Filter{Field: "meta.user_id", Operator: "==", Value: "u"}
			and := &Filter{Operator: "AND", Conditions: []Filter{*eq, {Field: "meta.fpath", Operator: "==", Value: "/target"}}}
			assertCounts := func(wantEQ, wantAND int) {
				t.Helper()
				before := workstats.Read()
				for i, filter := range []*Filter{eq, and} {
					out, err := svc.CountDocuments(ctx, create.Name, CountDocumentsRequest{Filter: filter, ExpectedGeneration: info.Generation})
					want := []int{wantEQ, wantAND}[i]
					if err != nil || out.Count != want || out.Index.Generation != info.Generation {
						t.Fatalf("count=%+v want=%d err=%v", out, want, err)
					}
				}
				after := workstats.Read()
				if after.Scans.FilteredCount != before.Scans.FilteredCount || after.IndexedJSON != before.IndexedJSON {
					t.Fatalf("indexed count scanned documents or extracted JSON: before=%+v after=%+v", before, after)
				}
			}
			assertCounts(2, 1)
			docs[1].Meta["fpath"] = "/target"
			if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: docs[1:2]}); err != nil {
				t.Fatal(err)
			}
			assertCounts(2, 2)
			before := workstats.Read().Scans.MutationMatch
			if out, err := svc.DeleteDocuments(ctx, create.Name, DeleteDocumentsRequest{Filter: and}); err != nil || out.Deleted != 2 {
				t.Fatalf("delete=%+v %v", out, err)
			}
			if after := workstats.Read().Scans.MutationMatch; after != before {
				t.Fatalf("indexed delete scanned: before=%+v after=%+v", before, after)
			}
			assertCounts(0, 0)
			if err := db.Checkpoint(); err != nil {
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
			svc = New(collections.NewCollectionManager(db))
			assertCounts(0, 0)
			if out, err := svc.CountDocuments(ctx, create.Name, CountDocumentsRequest{Filter: &Filter{Field: "meta.user_id", Operator: "==", Value: "v"}}); err != nil || out.Count != 1 {
				t.Fatalf("reopen count=%+v %v", out, err)
			}
			before = workstats.Read().Scans.FilteredCount
			if _, err := svc.CountDocuments(ctx, create.Name, CountDocumentsRequest{Filter: &Filter{Field: "meta.user_id", Operator: "==", Value: 12}}); ErrorCodeOf(err) != CodeInvalidRequest {
				t.Fatalf("invalid declared type: %v", err)
			}
			if after := workstats.Read().Scans.FilteredCount; after != before {
				t.Fatal("invalid declared type fell back to scan")
			}
		})
	}
}

func TestServiceScalarCountFailClosedAndFallback(t *testing.T) {
	ctx := context.Background()
	svc, db := newTestService(t)
	defer db.Close()
	defer svc.Close()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "checks", Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}, {Field: "meta.fpath", ValueType: ScalarFieldString}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpsertDocuments(ctx, "checks", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "alpha", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "u", "fpath": "/a"}}}}); err != nil {
		t.Fatal(err)
	}
	filter := &Filter{Field: "meta.user_id", Operator: "==", Value: "u"}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	before := workstats.Read().Scans.FilteredCount
	if _, err := svc.CountDocuments(cancelled, "checks", CountDocumentsRequest{Filter: filter}); err == nil {
		t.Fatal("cancelled count succeeded")
	}
	if _, err := svc.CountDocuments(ctx, "checks", CountDocumentsRequest{Filter: filter, ExpectedGeneration: info.Generation + 1}); ErrorCodeOf(err) != CodeIndexStale {
		t.Fatalf("stale request=%v", err)
	}
	if workstats.Read().Scans.FilteredCount != before {
		t.Fatal("invalid count scanned")
	}
	// Generic undeclared predicates and non-EQ operators keep their existing scan.
	for _, generic := range []*Filter{{Field: "content", Operator: "==", Value: "alpha"}, {Field: "meta.user_id", Operator: "!=", Value: "other"}} {
		before := workstats.Read().Scans.FilteredCount
		if out, err := svc.CountDocuments(ctx, "checks", CountDocumentsRequest{Filter: generic}); err != nil || out.Count != 1 {
			t.Fatalf("generic=%+v %v", out, err)
		}
		after := workstats.Read().Scans.FilteredCount
		if after.Starts-before.Starts != 1 || after.Rows-before.Rows != 1 {
			t.Fatalf("generic scan before=%+v after=%+v", before, after)
		}
	}
	col, _, err := svc.openIndex(ctx, "checks", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise a declared-but-unavailable index at the captured service
	// helper. Catalog index drops are deliberately unsupported under command WAL.
	fields := append([]ScalarFieldInfo(nil), info.ScalarFields...)
	for i := range fields {
		if fields[i].Field == "meta.fpath" {
			fields[i].IndexName = "missing"
		}
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	missing := &Filter{Operator: "AND", Conditions: []Filter{{Field: "meta.user_id", Operator: "==", Value: "empty"}, {Field: "meta.fpath", Operator: "==", Value: "/a"}}}
	before = workstats.Read().Scans.FilteredCount
	if _, indexed, err := collectMatchingIDsFromScalarReadView(ctx, view, missing, newScalarSchema(fields)); !indexed || ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("missing physical index=%v code=%s", err, ErrorCodeOf(err))
	}
	if workstats.Read().Scans.FilteredCount != before {
		t.Fatal("missing physical index fell back")
	}
}
