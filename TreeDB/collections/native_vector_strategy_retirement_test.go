package collections

import (
	"encoding/json"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestCollectionRejectsRemovedVectorStrategyBeforePublication(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	manager := NewCollectionManager(d)
	_, err = manager.CreateCollection(&CollectionMeta{Name: "retired", VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2, Strategy: VectorIndexStrategy("native_runtime")}}})
	if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("create retired strategy: %v; want unsupported", err)
	}
	if col, openErr := manager.OpenCollection("retired"); openErr == nil || col != nil {
		t.Fatalf("rejected create published collection: %v %v", col, openErr)
	}
}

func TestDecodeCollectionMetaRejectsRemovedVectorStrategy(t *testing.T) {
	raw, err := json.Marshal(collectionMetaDisk{Version: collectionMetaVersion, Name: "retired", VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2, Strategy: VectorIndexStrategy("native_runtime")}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCollectionMeta(raw); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("decode retired strategy: %v; want unsupported", err)
	}
}

func TestDecodeCollectionMetaDoesNotReinterpretLegacyOmittedVectorStrategy(t *testing.T) {
	for _, version := range []int{collectionMetaVersionV5, collectionMetaVersionV6, collectionMetaVersion} {
		raw, err := json.Marshal(collectionMetaDisk{Version: version, Name: "ambiguous", VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeCollectionMeta(raw); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
			t.Fatalf("decode v%d ambiguous vector: %v; want unsupported", version, err)
		}
		raw, err = json.Marshal(collectionMetaDisk{Version: version, Name: "scalar"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeCollectionMeta(raw); err != nil {
			t.Fatalf("decode v%d scalar metadata: %v", version, err)
		}
	}
}

func TestNewVectorStrategyDefaultsToColumnGraph(t *testing.T) {
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	if def.Strategy != VectorIndexStrategyColumnGraph {
		t.Fatalf("new omitted strategy=%q", def.Strategy)
	}
}

func TestCollectionUnsupportedVectorCapabilitiesBeforePublication(t *testing.T) {
	for _, tc := range []struct {
		name     string
		format   DocumentFormat
		metric   VectorMetric
		encoding VectorIndexEncoding
	}{
		{name: "bson", format: DocumentFormatBSON},
		{name: "l2", metric: VectorMetricL2},
		{name: "ip", metric: VectorMetricInnerProduct},
		{name: "int8", encoding: VectorIndexEncodingInt8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			manager := NewCollectionManager(d)
			_, err = manager.CreateCollection(&CollectionMeta{Name: tc.name, Options: CollectionOptions{DocumentFormat: tc.format}, VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: tc.metric, Encoding: tc.encoding, Strategy: VectorIndexStrategyColumnGraph}}})
			if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("unsupported %s create: %v", tc.name, err)
			}
			if col, openErr := manager.OpenCollection(tc.name); openErr == nil || col != nil {
				t.Fatalf("rejected create published collection: %v %v", col, openErr)
			}
		})
	}
}

func TestCatalogCreateReplayRejectsUnidentifiedOrRetiredVectorV7(t *testing.T) {
	for _, strategy := range []VectorIndexStrategy{"", VectorIndexStrategy("native_runtime")} {
		meta := collectionMetaDisk{Version: collectionMetaVersion, Name: "docs", VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2, Strategy: strategy}}}
		raw, err := json.Marshal(meta)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := commitlog.EncodeCatalogCreateCollectionPayload(meta.Name, raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = DecodeCatalogCreateCollectionCommandWALPayload(payload, false); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
			t.Fatalf("replay strategy %q err=%v", strategy, err)
		}
	}
	scalar := collectionMetaDisk{Version: collectionMetaVersion, Name: "scalar"}
	raw, err := json.Marshal(scalar)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := commitlog.EncodeCatalogCreateCollectionPayload(scalar.Name, raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeCatalogCreateCollectionCommandWALPayload(payload, false); err != nil {
		t.Fatalf("scalar replay rejected: %v", err)
	}
}

func TestValidateOrdinaryColumnGraphCollectionMeta5275(t *testing.T) {
	meta, _, _, _, _ := nativeOrdinaryGraphPhaseFixture(t, 1)
	meta.VectorIndexes[0].Strategy = ""
	if err := ValidateOrdinaryColumnGraphCollectionMeta(meta); err != nil {
		t.Fatalf("omitted new strategy: %v", err)
	}
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateOrdinaryColumnGraphCollectionMeta(normalized); err != nil {
		t.Fatalf("existing modern schema: %v", err)
	}
	incomplete := CollectionMeta{Name: meta.Name, VectorIndexes: meta.VectorIndexes}
	if err := ValidateOrdinaryColumnGraphCollectionMeta(incomplete); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("incomplete schema: %v", err)
	}
	retired := copyCollectionMeta(meta)
	retired.VectorIndexes[0].Strategy = VectorIndexStrategy("native_runtime")
	if err := ValidateOrdinaryColumnGraphCollectionMeta(retired); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("retired identity: %v", err)
	}
}
