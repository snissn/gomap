package mongogateway

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestSameVectorIndexDefinitionComparesStrategy(t *testing.T) {
	native := collections.VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     collections.VectorMetricCosine,
		Dimensions: 128,
		M:          16,
		EfSearch:   64,
		Encoding:   collections.VectorIndexEncodingFloat32,
		Strategy:   collections.VectorIndexStrategyNativeRuntime,
	}
	columnGraph := native
	columnGraph.Strategy = collections.VectorIndexStrategyColumnGraph
	if sameVectorIndexDefinition(native, columnGraph) {
		t.Fatal("sameVectorIndexDefinition treated different vector strategies as equal")
	}
}
