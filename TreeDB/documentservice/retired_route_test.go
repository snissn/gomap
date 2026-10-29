package documentservice

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestRetiredRuntimeRoutePolicy(t *testing.T) {
	filter := &Filter{Field: "category", Operator: "eq", Value: "a"}
	for _, tc := range []struct {
		name string
		req  DenseVectorSearchRequest
		info IndexInfo
		want Route
		code ErrorCode
	}{
		{name: "ordinary default", want: RouteAnn},
		{name: "ordinary ann", req: DenseVectorSearchRequest{Route: RouteAnn}, want: RouteAnn},
		{name: "no implicit exact fallback", info: IndexInfo{Capabilities: IndexCapabilities{NoDocumentVectorSearch: false}}, want: RouteAnn},
		{name: "ordinary filtered default", req: DenseVectorSearchRequest{Filter: filter}, code: CodeUnsupported},
		{name: "ordinary filtered ann", req: DenseVectorSearchRequest{Route: RouteAnn, Filter: filter}, code: CodeUnsupported},
		{name: "explicit filtered exact", req: DenseVectorSearchRequest{Route: RouteExact, Filter: filter}, want: RouteExact},
		{name: "typed filtered ann", req: DenseVectorSearchRequest{Filter: filter}, info: IndexInfo{TypedInput: true}, want: RouteAnn},
		{name: "typed exact", req: DenseVectorSearchRequest{Route: RouteExact}, info: IndexInfo{TypedInput: true}, code: CodeUnsupported},
		{name: "unknown", req: DenseVectorSearchRequest{Route: "bogus"}, code: CodeInvalidRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveDenseSearchRoute(tc.req, tc.info)
			if got != tc.want || ErrorCodeOf(err) != tc.code {
				t.Fatalf("route=%q code=%q err=%v; want route=%q code=%q", got, ErrorCodeOf(err), err, tc.want, tc.code)
			}
		})
	}
}

func TestRetiredRuntimeCreateOptions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metric   Metric
		strategy collections.VectorIndexStrategy
		code     ErrorCode
	}{
		{name: "default", metric: MetricCosine},
		{name: "modern", metric: MetricCosine, strategy: collections.VectorIndexStrategyColumnGraph},
		{name: "retired", metric: MetricCosine, strategy: "native_runtime", code: CodeUnsupported},
		{name: "unknown", metric: MetricCosine, strategy: "unknown", code: CodeInvalidRequest},
		{name: "l2 ANN unsupported", metric: MetricL2, code: CodeUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := benchmarkVectorIndexOptionsForCreate(tc.metric, &BenchmarkVectorIndexOptions{Strategy: tc.strategy}, collections.VectorIndexStrategyColumnGraph)
			if ErrorCodeOf(err) != tc.code {
				t.Fatalf("code=%q err=%v want=%q", ErrorCodeOf(err), err, tc.code)
			}
			if err == nil && out.strategy != collections.VectorIndexStrategyColumnGraph {
				t.Fatalf("strategy=%q", out.strategy)
			}
		})
	}
}

// This mapping test supplements actual native owner/wire parity on Windows and
// the real collection non-mmap traversal test. It does not simulate an end-to-end
// Windows platform on Linux.
func TestOrdinaryDenseANNCompatibilityRouteMapping(t *testing.T) {
	base := collections.VectorIndexSearchResponse{
		Strategy: collections.VectorIndexStrategyColumnGraph,
		Path:     collections.VectorIndexSearchPathColumnGraphNativeReader,
		Stats:    collections.VectorIndexSearchStats{SearchRouteColumnGraphFallback: 1, ColumnGraphDeltaScored: 2},
	}
	for _, tc := range []struct {
		name   string
		change func(*collections.VectorIndexSearchResponse)
		valid  bool
	}{
		{name: "compatibility", valid: true},
		{name: "prepared", valid: true, change: func(r *collections.VectorIndexSearchResponse) {
			r.Stats.SearchRouteColumnGraphFallback = 0
			r.Stats.SearchRouteColumnGraphPrepared = 1
		}},
		{name: "pack", valid: true, change: func(r *collections.VectorIndexSearchResponse) {
			r.Stats.SearchRouteColumnGraphFallback = 0
			r.Stats.SearchRouteHNSWSearchPack = 1
		}},
		{name: "missing", change: func(r *collections.VectorIndexSearchResponse) { r.Stats.SearchRouteColumnGraphFallback = 0 }},
		{name: "multiple", change: func(r *collections.VectorIndexSearchResponse) { r.Stats.SearchRouteColumnGraphFallback = 2 }},
		{name: "mixed_prepared", change: func(r *collections.VectorIndexSearchResponse) { r.Stats.SearchRouteColumnGraphPrepared = 1 }},
		{name: "mixed_pack", change: func(r *collections.VectorIndexSearchResponse) { r.Stats.SearchRouteHNSWSearchPack = 1 }},
		{name: "overflow", change: func(r *collections.VectorIndexSearchResponse) {
			r.Stats.SearchRouteColumnGraphFallback = ^uint64(0)
			r.Stats.SearchRouteColumnGraphPrepared = 2
		}},
		{name: "quantized", change: func(r *collections.VectorIndexSearchResponse) { r.Stats.SearchRouteQuantizedRerank = 1 }},
		{name: "other_strategy", change: func(r *collections.VectorIndexSearchResponse) { r.Strategy = "native_runtime" }},
		{name: "other_path", change: func(r *collections.VectorIndexSearchResponse) { r.Path = "unknown" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			search := base
			if tc.change != nil {
				tc.change(&search)
			}
			raw, err := ordinaryDenseVectorANNResponse(IndexInfo{}, search, nil)
			if !tc.valid {
				if ErrorCodeOf(err) != CodeIndexUnavailable {
					t.Fatalf("malformed route accepted: %+v err=%v", raw, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			out := raw.response(nil)
			if raw.Route != RouteAnn || raw.Exact || raw.DocumentsScanned != 0 || raw.ExactFallbacks != 0 || raw.FullDocumentScanFallbacks != 0 || raw.ColumnGraphFallbackSearch != search.Stats.SearchRouteColumnGraphFallback || out.ColumnGraphFallbackSearch != raw.ColumnGraphFallbackSearch || out.ColumnGraphPreparedSearch != search.Stats.SearchRouteColumnGraphPrepared || out.SearchPackSearch != search.Stats.SearchRouteHNSWSearchPack || out.ColumnGraphDeltaScored != 2 {
				t.Fatalf("collection route mapping raw=%+v public=%+v", raw, out)
			}
		})
	}
}
