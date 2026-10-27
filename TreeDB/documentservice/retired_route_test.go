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
