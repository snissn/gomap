package collections

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Malformed declarations are invalid requests even when another option asks
// for a retired or unsupported capability. Both public mutation entry points
// must refuse before changing documents, catalog roots, or command WAL state.
func TestVectorIndexMalformedOptionsBeforeCapabilityRefusal(t *testing.T) {
	for _, capability := range []struct {
		name  string
		apply func(*VectorIndexDefinition)
	}{
		{"supported", func(d *VectorIndexDefinition) {}},
		{"retired", func(d *VectorIndexDefinition) { d.Strategy = "native_runtime" }},
		{"l2", func(d *VectorIndexDefinition) { d.Metric = VectorMetricL2 }},
		{"int8", func(d *VectorIndexDefinition) { d.Encoding = VectorIndexEncodingInt8 }},
	} {
		t.Run(capability.name, func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			manager := NewCollectionManager(db)
			defer manager.Close()
			if _, err := manager.CreateCollection(&CollectionMeta{Name: "original"}); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection("original")
			if err != nil {
				t.Fatal(err)
			}
			original := []byte(`{"content":"keep"}`)
			if _, err := col.Insert([]byte("keep"), original); err != nil {
				t.Fatal(err)
			}
			meta := col.Meta()
			for _, malformed := range []struct {
				name    string
				apply   func(*VectorIndexDefinition)
				message string
			}{
				{"negative_m", func(d *VectorIndexDefinition) { d.M = -1 }, "m cannot be negative"},
				{"negative_ef_construction", func(d *VectorIndexDefinition) { d.EfConstruction = -1 }, "ef_construction cannot be negative"},
				{"negative_ef_search", func(d *VectorIndexDefinition) { d.EfSearch = -1 }, "ef_search cannot be negative"},
				{"representation", func(d *VectorIndexDefinition) { d.Representation = "unknown" }, "representation"},
				{"quantized_empty_name", func(d *VectorIndexDefinition) { d.QuantizedIndexes = []QuantizedVectorIndexDefinition{{}} }, "name is required"},
				{"quantized_invalid_name", func(d *VectorIndexDefinition) {
					d.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "bad/name"}}
				}, "name"},
				{"quantized_codec", func(d *VectorIndexDefinition) {
					d.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "q", Codec: "unknown"}}
				}, "codec"},
				{"quantized_duplicates", func(d *VectorIndexDefinition) {
					d.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "q"}, {Name: "q"}}
				}, "duplicate"},
				{"quantized_version", func(d *VectorIndexDefinition) {
					d.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "q", Version: 2}}
				}, "version"},
				{"quantized_calibration", func(d *VectorIndexDefinition) {
					d.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "q", ScalarU8Calibration: &ScalarU8CalibrationConfig{Mode: "unknown"}}}
				}, "calibration mode"},
			} {
				t.Run(malformed.name, func(t *testing.T) {
					def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2}
					capability.apply(&def)
					malformed.apply(&def)
					check := func(err error) {
						t.Helper()
						if err == nil || errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || !strings.Contains(err.Error(), malformed.message) {
							t.Fatalf("malformed declaration error=%v; want invalid containing %q", err, malformed.message)
						}
					}
					normalized, err := normalizeVectorIndexDefinition(def)
					check(err)
					if !reflect.DeepEqual(normalized, VectorIndexDefinition{}) {
						t.Fatalf("partial normalization returned: %+v", normalized)
					}
					before, ok := db.StateToken()
					if !ok {
						t.Fatal("missing state token")
					}
					lsn := db.CommandWALNextLSN()
					created, err := manager.CreateCollection(&CollectionMeta{Name: "rejected", VectorIndexes: []VectorIndexDefinition{def}})
					check(err)
					if created != nil {
						t.Fatalf("create returned metadata: %+v", created)
					}
					if _, err := manager.OpenCollection("rejected"); !errors.Is(err, ErrCollectionNotFound) {
						t.Fatalf("rejected collection installed: %v", err)
					}
					updated, err := col.CreateVectorIndex(def)
					check(err)
					if updated != nil {
						t.Fatalf("index create returned metadata: %+v", updated)
					}
					after, ok := db.StateToken()
					if !ok || after != before || db.CommandWALNextLSN() != lsn {
						t.Fatalf("refusal changed roots or WAL: before=%+v after=%+v nextLSN=%d/%d", before, after, lsn, db.CommandWALNextLSN())
					}
					reopened, err := manager.OpenCollection("original")
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(meta, reopened.Meta()) {
						t.Fatal("refusal changed original schema")
					}
					doc, err := reopened.Get([]byte("keep"))
					if err != nil || !bytes.Equal(doc, original) {
						t.Fatalf("refusal changed original document: %s %v", doc, err)
					}
				})
			}
		})
	}
}

func TestVectorIndexNumericDefaultsAndUnsupportedControls(t *testing.T) {
	for _, strategy := range []VectorIndexStrategy{"", VectorIndexStrategyColumnGraph} {
		def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{Field: "embedding", Dimensions: 2, Strategy: strategy})
		if err != nil {
			t.Fatal(err)
		}
		if def.Strategy != VectorIndexStrategyColumnGraph || def.M != defaultVectorIndexM || def.EfConstruction != defaultVectorIndexEfConstruction || def.EfSearch != defaultVectorIndexEfSearch {
			t.Fatalf("zero/default normalization: %+v", def)
		}
	}
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{Field: "embedding", Dimensions: 2, M: 8, EfConstruction: 24, EfSearch: 12})
	if err != nil || def.M != 8 || def.EfConstruction != 24 || def.EfSearch != 12 {
		t.Fatalf("positive options changed: %+v %v", def, err)
	}
	def, err = normalizeVectorIndexDefinition(VectorIndexDefinition{Field: "embedding", Dimensions: 2, M: 8, EfConstruction: 4})
	if err != nil || def.EfConstruction != 8 {
		t.Fatalf("construction clamp changed: %+v %v", def, err)
	}
	for _, def := range []VectorIndexDefinition{
		{Strategy: "native_runtime"},
		{Metric: VectorMetricL2},
		{Encoding: VectorIndexEncodingInt8},
		{Strategy: "native_runtime", Representation: VectorIndexRepresentationCosineNormalizedF32V1},
		{Strategy: "native_runtime", QuantizedIndexes: []QuantizedVectorIndexDefinition{{Name: "q"}}},
		{Metric: VectorMetricL2, QuantizedIndexes: []QuantizedVectorIndexDefinition{{Name: "q"}}},
	} {
		def.Field, def.Dimensions = "embedding", 2
		normalized, err := normalizeVectorIndexDefinition(def)
		if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || !reflect.DeepEqual(normalized, VectorIndexDefinition{}) {
			t.Fatalf("valid unsupported declaration normalized=%+v err=%v", normalized, err)
		}
	}
}
