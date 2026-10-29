package documentservice

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestVectorCreateMalformedOptionsBeforeCapabilityRefusal(t *testing.T) {
	for _, capability := range []struct {
		name     string
		metric   Metric
		strategy collections.VectorIndexStrategy
	}{
		{"supported", MetricCosine, collections.VectorIndexStrategyColumnGraph},
		{"retired", MetricCosine, "native_runtime"},
		{"l2", MetricL2, collections.VectorIndexStrategyColumnGraph},
	} {
		t.Run(capability.name, func(t *testing.T) {
			svc, db := newTestService(t)
			defer db.Close()
			defer svc.Close()
			for _, malformed := range []struct {
				name    string
				apply   func(*BenchmarkVectorIndexOptions)
				message string
			}{
				{"negative_m", func(o *BenchmarkVectorIndexOptions) { o.M = -1 }, "non-negative"},
				{"negative_ef_construction", func(o *BenchmarkVectorIndexOptions) { o.EfConstruction = -1 }, "non-negative"},
				{"negative_ef_search", func(o *BenchmarkVectorIndexOptions) { o.EfSearch = -1 }, "non-negative"},
				{"representation", func(o *BenchmarkVectorIndexOptions) { o.Representation = "unknown" }, "representation"},
				{"quantized_empty_name", func(o *BenchmarkVectorIndexOptions) { o.QuantizedIndexes = []QuantizedIndexInfo{{}} }, "name is required"},
				{"quantized_invalid_name", func(o *BenchmarkVectorIndexOptions) { o.QuantizedIndexes = []QuantizedIndexInfo{{Name: "bad/name"}} }, "name is invalid"},
				{"quantized_codec", func(o *BenchmarkVectorIndexOptions) {
					o.QuantizedIndexes = []QuantizedIndexInfo{{Name: "q", Codec: "unknown"}}
				}, "codec"},
				{"quantized_duplicates", func(o *BenchmarkVectorIndexOptions) {
					o.QuantizedIndexes = []QuantizedIndexInfo{{Name: "q"}, {Name: "q"}}
				}, "duplicate"},
				{"quantized_future_version", func(o *BenchmarkVectorIndexOptions) {
					o.QuantizedIndexes = []QuantizedIndexInfo{{Name: "q", Version: 2}}
				}, "version"},
				{"quantized_calibration", func(o *BenchmarkVectorIndexOptions) {
					o.QuantizedIndexes = []QuantizedIndexInfo{{Name: "q", ScalarU8Calibration: &collections.ScalarU8CalibrationConfig{Mode: "unknown"}}}
				}, "calibration"},
			} {
				t.Run(malformed.name, func(t *testing.T) {
					opts := &BenchmarkVectorIndexOptions{Strategy: capability.strategy}
					malformed.apply(opts)
					check := func(err error) {
						t.Helper()
						if ErrorCodeOf(err) != CodeInvalidRequest || httpStatusForError(err) != http.StatusBadRequest || !strings.Contains(err.Error(), malformed.message) {
							t.Fatalf("malformed options err=%v code=%q; want invalid_request containing %q", err, ErrorCodeOf(err), malformed.message)
						}
					}
					out, err := benchmarkVectorIndexOptionsForCreate(capability.metric, opts, collections.VectorIndexStrategyColumnGraph)
					check(err)
					if !reflect.DeepEqual(out, normalizedBenchmarkVectorIndexOptions{}) {
						t.Fatalf("returned partial options: %+v", out)
					}
					before, ok := db.StateToken()
					if !ok {
						t.Fatal("missing state token")
					}
					lsn := db.CommandWALNextLSN()
					info, err := svc.CreateIndex(context.Background(), CreateIndexRequest{Name: "rejected", Dimension: 2, Metric: capability.metric, VectorIndexOptions: opts})
					check(err)
					if !reflect.DeepEqual(info, IndexInfo{}) {
						t.Fatalf("returned partial index: %+v", info)
					}
					after, ok := db.StateToken()
					if !ok || before != after || db.CommandWALNextLSN() != lsn {
						t.Fatalf("refusal changed roots or WAL: before=%+v after=%+v nextLSN=%d/%d", before, after, lsn, db.CommandWALNextLSN())
					}
					if _, err := svc.manager.OpenCollection("rejected"); !errors.Is(err, collections.ErrCollectionNotFound) {
						t.Fatalf("refused index installed: %v", err)
					}
					if metas, err := svc.manager.ListCollections(); err != nil || len(metas) != 0 {
						t.Fatalf("refusal retained metadata: %+v %v", metas, err)
					}
				})
			}
		})
	}
}

func TestVectorCreateValidDefaultsAndUnsupportedControls(t *testing.T) {
	for _, opts := range []*BenchmarkVectorIndexOptions{nil, {}, {Strategy: collections.VectorIndexStrategyColumnGraph}} {
		out, err := benchmarkVectorIndexOptionsForCreate(MetricCosine, opts, collections.VectorIndexStrategyColumnGraph)
		if err != nil || out.strategy != collections.VectorIndexStrategyColumnGraph || out.m != 0 || out.efConstruction != 0 || out.efSearch != 0 || out.quantizedIndexes != nil {
			t.Fatalf("zero options must retain collection defaults: %+v %v", out, err)
		}
	}
	out, err := benchmarkVectorIndexOptionsForCreate(MetricCosine, &BenchmarkVectorIndexOptions{M: 8, EfConstruction: 24, EfSearch: 12, QuantizedIndexes: []QuantizedIndexInfo{{Name: "q"}}}, collections.VectorIndexStrategyColumnGraph)
	if err != nil || out.m != 8 || out.efConstruction != 24 || out.efSearch != 12 || len(out.quantizedIndexes) != 1 || out.quantizedIndexes[0].Codec != collections.QuantizedVectorCodecScalarU8 || out.quantizedIndexes[0].Version != 1 {
		t.Fatalf("valid options/default codec changed: %+v %v", out, err)
	}
	for _, tc := range []struct {
		metric Metric
		opts   BenchmarkVectorIndexOptions
	}{
		{metric: MetricCosine, opts: BenchmarkVectorIndexOptions{Strategy: "native_runtime"}},
		{metric: MetricCosine, opts: BenchmarkVectorIndexOptions{Strategy: "native_runtime", Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1}},
		{metric: MetricCosine, opts: BenchmarkVectorIndexOptions{Strategy: "native_runtime", QuantizedIndexes: []QuantizedIndexInfo{{Name: "q"}}}},
		{metric: MetricL2},
		{metric: MetricL2, opts: BenchmarkVectorIndexOptions{QuantizedIndexes: []QuantizedIndexInfo{{Name: "q"}}}},
	} {
		svc, db := newTestService(t)
		func() {
			defer db.Close()
			defer svc.Close()
			before, ok := db.StateToken()
			if !ok {
				t.Fatal("missing state token")
			}
			lsn := db.CommandWALNextLSN()
			out, err := benchmarkVectorIndexOptionsForCreate(tc.metric, &tc.opts, collections.VectorIndexStrategyColumnGraph)
			if ErrorCodeOf(err) != CodeUnsupported || httpStatusForError(err) != http.StatusNotImplemented || !reflect.DeepEqual(out, normalizedBenchmarkVectorIndexOptions{}) {
				t.Fatalf("valid unsupported options out=%+v err=%v", out, err)
			}
			info, err := svc.CreateIndex(context.Background(), CreateIndexRequest{Name: "rejected", Dimension: 2, Metric: tc.metric, VectorIndexOptions: &tc.opts})
			if ErrorCodeOf(err) != CodeUnsupported || !reflect.DeepEqual(info, IndexInfo{}) {
				t.Fatalf("valid unsupported create info=%+v err=%v", info, err)
			}
			after, ok := db.StateToken()
			if !ok || before != after || db.CommandWALNextLSN() != lsn {
				t.Fatal("valid unsupported create changed state/WAL")
			}
			if metas, err := svc.manager.ListCollections(); err != nil || len(metas) != 0 {
				t.Fatalf("valid unsupported create published metadata: %+v %v", metas, err)
			}
		}()
	}
}

func TestVectorCreateScalarSyntaxBeforeCapabilityRefusal(t *testing.T) {
	for _, backend := range []string{"legacy", string(treedb.ProfileCommandWALDurable), string(treedb.ProfileCommandWALRelaxed), string(treedb.ProfileNoWALFast)} {
		t.Run(backend, func(t *testing.T) {
			var svc *Service
			var db *backenddb.DB
			if backend == "legacy" {
				svc, db = newTestService(t)
				t.Cleanup(func() { _ = db.Close() })
			} else {
				opts := treedb.OptionsFor(treedb.Profile(backend), t.TempDir())
				opts.MemtableMode = "cow_btree"
				opts.BackgroundCheckpointInterval = -1
				opts.BackgroundCheckpointIdleDuration = -1
				owner, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = owner.Close() })
				db, err = owner.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				svc = New(collections.NewCollectionManager(db))
			}
			t.Cleanup(func() { _ = svc.manager.Close() })
			t.Cleanup(func() { _ = svc.Close() })
			capture := func(t *testing.T) (backenddb.StateToken, uint64) {
				t.Helper()
				if backend == "legacy" {
					token, ok := db.StateToken()
					if !ok {
						t.Fatal("missing legacy state token")
					}
					return token, 0
				}
				state, err := db.CaptureNativeSemanticState()
				if err != nil {
					t.Fatal(err)
				}
				defer state.Close()
				token, ok := state.Snapshot().StateToken()
				if !ok {
					t.Fatal("missing native state token")
				}
				return token, state.LogicalSequence()
			}
			for _, capability := range []struct {
				name     string
				metric   Metric
				strategy collections.VectorIndexStrategy
			}{
				{"supported", MetricCosine, collections.VectorIndexStrategyColumnGraph},
				{"retired", MetricCosine, "native_runtime"},
				{"l2", MetricL2, collections.VectorIndexStrategyColumnGraph},
			} {
				t.Run(capability.name, func(t *testing.T) {
					for _, declaration := range []struct {
						name    string
						typed   bool
						fields  []ScalarFieldDeclaration
						message string
					}{
						{"empty", false, []ScalarFieldDeclaration{{}}, "field must not be empty"},
						{"path", false, []ScalarFieldDeclaration{{Field: "meta.bad..path"}}, "valid meta path"},
						{"reserved", false, []ScalarFieldDeclaration{{Field: "embedding"}}, "reserved document field"},
						{"derived_name", false, []ScalarFieldDeclaration{{Field: "meta.bad/name"}}, "derives invalid index name"},
						{"unknown_type", false, []ScalarFieldDeclaration{{Field: "meta.tag", ValueType: "decimal"}}, "value_type"},
						{"duplicate_field", false, []ScalarFieldDeclaration{{Field: "tag"}, {Field: "meta.tag"}}, "more than once"},
						{"duplicate_index", false, []ScalarFieldDeclaration{{Field: "meta.a.b"}, {Field: "meta.a_b"}}, "already-used scalar index name"},
						{"typed_bool", true, []ScalarFieldDeclaration{{Field: "meta.tag", ValueType: ScalarFieldBool}}, "typed input requires declared string scalar fields"},
						{"typed_int64", true, []ScalarFieldDeclaration{{Field: "meta.tag", ValueType: ScalarFieldInt64}}, "typed input requires declared string scalar fields"},
						{"typed_double", true, []ScalarFieldDeclaration{{Field: "meta.tag", ValueType: ScalarFieldDouble}}, "typed input requires declared string scalar fields"},
					} {
						t.Run(declaration.name, func(t *testing.T) {
							before, sequence := capture(t)
							lsn := db.CommandWALNextLSN()
							info, err := svc.CreateIndex(context.Background(), CreateIndexRequest{Name: "rejected", Dimension: 2, Metric: capability.metric, TypedInput: declaration.typed, ScalarFields: declaration.fields, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: capability.strategy}})
							if ErrorCodeOf(err) != CodeInvalidRequest || httpStatusForError(err) != http.StatusBadRequest || !strings.Contains(err.Error(), declaration.message) || !reflect.DeepEqual(info, IndexInfo{}) {
								t.Fatalf("malformed scalar request info=%+v err=%v code=%q; want invalid containing %q", info, err, ErrorCodeOf(err), declaration.message)
							}
							after, afterSequence := capture(t)
							if before != after || sequence != afterSequence || db.CommandWALNextLSN() != lsn {
								t.Fatal("malformed scalar refusal changed roots, native sequence, or WAL")
							}
							if metas, err := svc.manager.ListCollections(); err != nil || len(metas) != 0 {
								t.Fatalf("refusal published metadata: %+v %v", metas, err)
							}
						})
					}
					if capability.name == "supported" {
						return
					}
					// Valid scalar declarations and representation/ownership combinations
					// retain Unsupported priority for retired or unavailable metrics.
					for _, typed := range []bool{false, true} {
						before, sequence := capture(t)
						lsn := db.CommandWALNextLSN()
						info, err := svc.CreateIndex(context.Background(), CreateIndexRequest{Name: "rejected", Dimension: 2, Metric: capability.metric, TypedInput: typed, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tag", ValueType: ScalarFieldString}}, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: capability.strategy, Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1}})
						if ErrorCodeOf(err) != CodeUnsupported || !reflect.DeepEqual(info, IndexInfo{}) {
							t.Fatalf("valid scalar unsupported control info=%+v err=%v", info, err)
						}
						after, afterSequence := capture(t)
						if before != after || sequence != afterSequence || db.CommandWALNextLSN() != lsn {
							t.Fatal("unsupported control changed roots, native sequence, or WAL")
						}
						if metas, err := svc.manager.ListCollections(); err != nil || len(metas) != 0 {
							t.Fatalf("unsupported control published metadata: %+v %v", metas, err)
						}
					}
				})
			}
		})
	}
}

func TestVectorCreateScalarValidationPreservesOwnershipConflict(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	defer svc.Close()
	req := CreateIndexRequest{Name: "existing", Dimension: 2}
	if _, err := svc.CreateIndex(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	before, ok := db.StateToken()
	if !ok {
		t.Fatal("missing state token")
	}
	lsn := db.CommandWALNextLSN()
	req.TypedInput = true
	req.ScalarFields = []ScalarFieldDeclaration{{Field: "embedding"}}
	if _, err := svc.CreateIndex(context.Background(), req); ErrorCodeOf(err) != CodeConflict || !strings.Contains(err.Error(), "typed input ownership") {
		t.Fatalf("existing ownership conflict precedence changed: %v", err)
	}
	after, ok := db.StateToken()
	if !ok || before != after || db.CommandWALNextLSN() != lsn {
		t.Fatal("ownership conflict changed state/WAL")
	}
}
