package mongogateway

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
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

func TestParseCreateVectorIndexDefinitionParsesStrategy(t *testing.T) {
	doc := mustDocument(t, bson.D{
		{Key: "key", Value: bson.D{{Key: "embedding", Value: treeDBIndexTypeVector}}},
		{Key: "name", Value: "embedding_column"},
		{Key: treeDBIndexTypeField, Value: treeDBIndexTypeVector},
		{Key: treeDBVectorOptionsField, Value: bson.D{
			{Key: "dimensions", Value: int32(128)},
			{Key: "strategy", Value: collections.VectorIndexStrategyColumnGraph.String()},
		}},
	})
	parsed, err := parseCreateIndexDefinition(doc)
	if err != nil {
		t.Fatalf("parse vector index definition: %v", err)
	}
	if !parsed.vector || parsed.vectorDef.Strategy != collections.VectorIndexStrategyColumnGraph {
		t.Fatalf("parsed vector strategy=%q vector=%v", parsed.vectorDef.Strategy, parsed.vector)
	}
}

func TestMongoIndexDocumentsRoundTripsColumnGraphStrategy(t *testing.T) {
	def := collections.VectorIndexDefinition{
		Name:           "embedding_column",
		Field:          "embedding",
		Metric:         collections.VectorMetricCosine,
		Dimensions:     128,
		M:              16,
		EfConstruction: 128,
		EfSearch:       64,
		Encoding:       collections.VectorIndexEncodingFloat32,
		Strategy:       collections.VectorIndexStrategyColumnGraph,
	}
	docs := mongoIndexDocuments(collections.CollectionMeta{VectorIndexes: []collections.VectorIndexDefinition{def}})
	if len(docs) != 2 {
		t.Fatalf("index document count=%d want 2", len(docs))
	}
	indexDoc, ok := docs[1].(bson.D)
	if !ok {
		t.Fatalf("index document type=%T want bson.D", docs[1])
	}
	raw := mustDocument(t, indexDoc)
	optionsValue := bson.Raw(raw).Lookup(treeDBVectorOptionsField)
	optionsRaw, ok := optionsValue.DocumentOK()
	if !ok {
		t.Fatalf("missing %s document in %+v", treeDBVectorOptionsField, indexDoc)
	}
	strategy, ok := bson.Raw(optionsRaw).Lookup("strategy").StringValueOK()
	if !ok || strategy != collections.VectorIndexStrategyColumnGraph.String() {
		t.Fatalf("emitted vector strategy=%q ok=%v", strategy, ok)
	}
	parsed, err := parseCreateIndexDefinition(raw)
	if err != nil {
		t.Fatalf("parse emitted vector index: %v", err)
	}
	if !parsed.vector || !sameVectorIndexDefinition(def, parsed.vectorDef) {
		t.Fatalf("roundtrip vector def=%+v parsed=%+v vector=%v", def, parsed.vectorDef, parsed.vector)
	}
}
