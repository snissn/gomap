package mongogateway

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestSameIndexDefinitionNormalizesLegacySingleAscendingComponent(t *testing.T) {
	legacy := collections.IndexDefinition{
		Name:      "created_1",
		Field:     "created",
		ValueType: collections.IndexValueBSONOrderedV2,
	}
	canonical := legacy
	canonical.Field = ""
	canonical.Components = []collections.IndexComponent{{Field: "created", Direction: collections.IndexDirectionAscending}}
	if !sameIndexDefinition(legacy, canonical) {
		t.Fatal("legacy single-field index and canonical ascending component form differ")
	}
	canonical.Components[0].Direction = collections.IndexDirectionDescending
	if sameIndexDefinition(legacy, canonical) {
		t.Fatal("ascending and descending definitions compare equal")
	}
}
