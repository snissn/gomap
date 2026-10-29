package collections

import (
	"errors"
	"testing"
)

func TestColumnGraphCarrierRequiresCapturedModernOwner5275(t *testing.T) {
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Strategy: VectorIndexStrategyColumnGraph})
	if err != nil {
		t.Fatal(err)
	}
	collection := &Collection{}
	opts := vectorIndexOptionsFromDefinition(def)
	if _, err := newColumnGraphPartitionLiveDomain(collection, opts); err != nil {
		t.Fatalf("captured modern owner: %v", err)
	}
	for _, tc := range []struct {
		name    string
		options columnGraphPartitionLiveOptions
	}{
		{"uncaptured", columnGraphPartitionLiveOptions{Name: def.Name, Field: def.Field, Dimensions: def.Dimensions, Metric: def.Metric}},
		{"retired", vectorIndexOptionsFromDefinition(func() VectorIndexDefinition {
			retired := def
			retired.Strategy = VectorIndexStrategy("native_runtime")
			return retired
		}())},
		{"mismatched", func() columnGraphPartitionLiveOptions { changed := opts; changed.Name = "other"; return changed }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if carrier, err := newColumnGraphPartitionLiveDomain(collection, tc.options); carrier != nil || !errors.Is(err, errVectorIndexNotDeclared) {
				t.Fatalf("carrier=%v err=%v; want owner refusal", carrier, err)
			}
		})
	}
}
