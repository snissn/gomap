package documentservice

import (
	"context"
	"errors"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Omitted strategies select ColumnGraph, while unsupported metrics fail closed.
func TestServiceDefaultVectorStrategyPublic(t *testing.T) {
	for _, test := range []struct {
		name   string
		metric Metric
		want   collections.VectorIndexStrategy
	}{
		{name: "omitted-metric", want: collections.VectorIndexStrategyColumnGraph},
		{name: "cosine", metric: MetricCosine, want: collections.VectorIndexStrategyColumnGraph},
		{name: "l2", metric: MetricL2},
		{name: "inner-product", metric: MetricInnerProduct},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc, db := newTestService(t)
			defer db.Close()
			defer svc.Close()
			req := CreateIndexRequest{Name: "default", Dimension: 2, Metric: test.metric}
			info, err := svc.CreateIndex(context.Background(), req)
			if test.want == "" {
				if ErrorCodeOf(err) != CodeUnsupported {
					t.Fatalf("unsupported metric accepted: %+v %v", info, err)
				}
				return
			}
			if err != nil || info.VectorStrategy != test.want {
				t.Fatalf("omitted strategy: %+v %v", info, err)
			}
			again, err := svc.CreateIndex(context.Background(), req)
			if err != nil || again.Generation != info.Generation || again.VectorStrategy != test.want {
				t.Fatalf("idempotent omitted strategy: %+v %v", again, err)
			}
		})
	}
}

// Omitted cosine preserves ordinary ColumnGraph: durable native construction
// uses full retained JSON; unsupported profiles refuse before publication.
func TestServiceNativeDefaultColumnGraphProfiles(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}()
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(backend)
			defer func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
			}()
			svc := New(manager)
			defer func() {
				if err := svc.Close(); err != nil {
					t.Error(err)
				}
			}()
			ctx := context.Background()
			req := CreateIndexRequest{Name: "default", Dimension: 2}
			before, err := backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			defer before.Close()
			beforeToken, ok := before.Snapshot().StateToken()
			if !ok {
				t.Fatal("closed initial capture")
			}
			nextLSN := backend.CommandWALNextLSN()
			info, err := svc.CreateIndex(ctx, req)
			if profile == treedb.ProfileCommandWALDurable {
				if err != nil || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph || info.TypedInput {
					t.Fatalf("durable ordinary default: %+v %v", info, err)
				}
				col, err := manager.OpenCollection(req.Name)
				if err != nil {
					t.Fatal(err)
				}
				meta := col.Meta()
				if meta.Options.ColumnStore == nil || meta.Options.ColumnStore.RetainedPayload != collections.ColumnRetainedPayloadFull {
					t.Fatalf("native ordinary default lost full retained JSON: %+v", meta.Options.ColumnStore)
				}
				again, err := svc.CreateIndex(ctx, req)
				reopened, openErr := manager.OpenCollection(req.Name)
				if err != nil || openErr != nil {
					t.Fatalf("repeat default create: %+v create=%v open=%v", again, err, openErr)
				}
				if again.Generation != info.Generation || again.VectorStrategy != collections.VectorIndexStrategyColumnGraph || !reflect.DeepEqual(meta, reopened.Meta()) {
					t.Fatalf("repeat default changed generation/schema: first=%+v again=%+v", info, again)
				}
				return
			}
			if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("unsupported native ColumnGraph profile: %v", err)
			}
			after, err := backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			defer after.Close()
			afterToken, ok := after.Snapshot().StateToken()
			if !ok || afterToken != beforeToken || after.LogicalSequence() != before.LogicalSequence() || backend.CommandWALNextLSN() != nextLSN {
				t.Fatalf("refused default published roots/sequence/WAL: token=%+v/%+v sequence=%d/%d nextLSN=%d/%d", beforeToken, afterToken, before.LogicalSequence(), after.LogicalSequence(), nextLSN, backend.CommandWALNextLSN())
			}
			if _, err := manager.OpenCollection("default"); !errors.Is(err, collections.ErrCollectionNotFound) {
				t.Fatalf("refused default installed a collection: %v", err)
			}
			if metas, err := manager.ListCollections(); err != nil || len(metas) != 0 {
				t.Fatalf("refused default retained schema: %+v %v", metas, err)
			}
		})
	}
}

func TestServiceExistingStrategyPreserved(t *testing.T) {
	for _, strategy := range []collections.VectorIndexStrategy{collections.VectorIndexStrategyColumnGraph} {
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
			mismatch.Strategy = "native_runtime"
			req.VectorIndexOptions = &mismatch
			if _, err := other.CreateIndex(ctx, req); ErrorCodeOf(err) != CodeUnsupported {
				t.Fatalf("retired strategy: %v", err)
			}
			if !reflect.DeepEqual(before, col.Meta()) {
				t.Fatal("conflicting create changed schema")
			}
		})
	}
	// Omitted cosine ColumnGraph declarations survive an actual owner reopen.
	dir := t.TempDir()
	db, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(collections.NewCollectionManager(db))
	req := CreateIndexRequest{Name: "cosine-default", Dimension: 2}
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
		{name: "cosine", want: collections.VectorIndexStrategyColumnGraph},
		{name: "typed", defaultStrategy: collections.VectorIndexStrategyColumnGraph, want: collections.VectorIndexStrategyColumnGraph},
		{name: "normalized", opts: &BenchmarkVectorIndexOptions{Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1}, want: collections.VectorIndexStrategyColumnGraph},
		{name: "quantized", opts: &BenchmarkVectorIndexOptions{QuantizedIndexes: []QuantizedIndexInfo{{Name: "q"}}}, want: collections.VectorIndexStrategyColumnGraph},
		{name: "explicit-native-normalized", opts: &BenchmarkVectorIndexOptions{Strategy: "native_runtime", Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1}, invalid: true},
		{name: "explicit-native-quantized", opts: &BenchmarkVectorIndexOptions{Strategy: "native_runtime", QuantizedIndexes: []QuantizedIndexInfo{{Name: "q"}}}, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fallback := test.defaultStrategy
			if fallback == "" {
				fallback = collections.VectorIndexStrategyColumnGraph
			}
			got, err := benchmarkVectorIndexOptionsForCreate(MetricCosine, test.opts, fallback)
			if test.invalid {
				if ErrorCodeOf(err) != CodeUnsupported {
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
