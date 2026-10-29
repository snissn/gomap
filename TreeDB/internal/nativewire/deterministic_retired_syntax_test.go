package nativewire

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestDeterministicRetiredSyntaxCollectionPreservesEnums(t *testing.T) {
	for _, metric := range []struct {
		wire  uint64
		value collections.VectorMetric
	}{
		{1, collections.VectorMetricCosine}, {2, collections.VectorMetricL2}, {3, collections.VectorMetricInnerProduct},
	} {
		for _, encoding := range []struct {
			wire  uint64
			value collections.VectorIndexEncoding
		}{
			{1, collections.VectorIndexEncodingFloat32}, {2, collections.VectorIndexEncodingInt8},
		} {
			t.Run(fmt.Sprintf("metric%d/encoding%d", metric.wire, encoding.wire), func(t *testing.T) {
				raw := appendDeterministicString(nil, "embedding")
				raw = appendDeterministicString(raw, "embedding")
				raw = binary.AppendUvarint(raw, metric.wire)
				for _, value := range []int64{2, 16, 128, 32} {
					raw = binary.AppendVarint(raw, value)
				}
				for _, value := range []uint64{encoding.wire, 1, 0} {
					raw = binary.AppendUvarint(raw, value)
				}
				var def collections.VectorIndexDefinition
				var retired bool
				off, err := validateDeterministicVectorIndexDefinitionAt(raw, 0, 5, &def, &retired)
				if err != nil || off != len(raw) || !retired {
					t.Fatalf("parse: off=%d retired=%t err=%v", off, retired, err)
				}
				if def.Metric != metric.value || def.Encoding != encoding.value || def.Strategy != "native_runtime" || def.Dimensions != 2 || def.M != 16 || def.EfConstruction != 128 || def.EfSearch != 32 {
					t.Fatalf("wire enum/numeric values changed: %+v", def)
				}
			})
		}
	}
	for _, valueType := range []struct {
		wire  uint64
		value collections.IndexValueType
	}{
		{1, collections.IndexValueString}, {2, collections.IndexValueBool}, {3, collections.IndexValueInt64}, {4, collections.IndexValueDouble},
	} {
		t.Run(fmt.Sprintf("scalar%d", valueType.wire), func(t *testing.T) {
			raw := deterministicIndexDefinitionPayload("tag", "tag", valueType.wire, true, true, 2)
			var def collections.IndexDefinition
			off, err := validateDeterministicIndexDefinitionAt(raw, 0, true, Limits{}, &def)
			if err != nil || off != len(raw) || def.ValueType != valueType.value || !def.Unique || !def.MultiKey || def.StoragePolicy != collections.RootStorageCompressed {
				t.Fatalf("scalar enum value changed: %+v off=%d err=%v", def, off, err)
			}
		})
	}
}
