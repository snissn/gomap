package collections

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// A capability on the first vector must not conceal malformed later vectors,
// text declarations, or collisions in the collection-wide index namespace.
func TestCollectionWholeRequestSyntaxBeforeVectorCapability(t *testing.T) {
	for _, native := range []bool{false, true} {
		profile := "legacy"
		if native {
			profile = "native"
		}
		t.Run(profile, func(t *testing.T) {
			db, manager, original := openWholeRequestPrecedenceFixture(t, native)
			for _, capability := range []string{"retired", "l2", "int8", "bson"} {
				t.Run(capability, func(t *testing.T) {
					first := VectorIndexDefinition{Name: "first", Field: "embedding", Dimensions: 2}
					options := CollectionOptions{}
					switch capability {
					case "retired":
						first.Strategy = "native_runtime"
					case "l2":
						first.Metric = VectorMetricL2
					case "int8":
						first.Encoding = VectorIndexEncodingInt8
					case "bson":
						options.DocumentFormat = DocumentFormatBSON
					}
					for _, malformed := range []struct {
						name    string
						apply   func(*CollectionMeta)
						message string
					}{
						{"later_dimensions", func(m *CollectionMeta) {
							m.VectorIndexes = append(m.VectorIndexes, VectorIndexDefinition{Name: "later", Field: "other"})
						}, "dimensions must be positive"},
						{"later_numeric", func(m *CollectionMeta) {
							m.VectorIndexes = append(m.VectorIndexes, VectorIndexDefinition{Name: "later", Field: "other", Dimensions: 2, M: -1})
						}, "m cannot be negative"},
						{"later_unknown_strategy", func(m *CollectionMeta) {
							m.VectorIndexes = append(m.VectorIndexes, VectorIndexDefinition{Name: "later", Field: "other", Dimensions: 2, Strategy: "unknown"})
						}, "unsupported strategy"},
						{"later_nested_name", func(m *CollectionMeta) {
							m.VectorIndexes = append(m.VectorIndexes, VectorIndexDefinition{Name: "later", Field: "other", Dimensions: 2, QuantizedIndexes: []QuantizedVectorIndexDefinition{{}}})
						}, "name is required"},
						{"vector_duplicate", func(m *CollectionMeta) {
							m.VectorIndexes = append(m.VectorIndexes, VectorIndexDefinition{Name: "first", Field: "other", Dimensions: 2})
						}, "duplicate index"},
						{"scalar_vector_collision", func(m *CollectionMeta) {
							m.Indexes = []IndexDefinition{{Name: "first", Field: "marker", ValueType: IndexValueInt64}}
						}, "duplicate index"},
						{"vector_text_collision", func(m *CollectionMeta) {
							m.TextIndexes = []TextIndexDefinition{{Name: "first", Fields: []TextIndexField{{Field: "content"}}}}
						}, "duplicate index"},
						{"later_text_fields", func(m *CollectionMeta) { m.TextIndexes = []TextIndexDefinition{{Name: "text"}} }, "fields are required"},
						{"later_text_name", func(m *CollectionMeta) {
							m.TextIndexes = []TextIndexDefinition{{Name: "bad/name", Fields: []TextIndexField{{Field: "content"}}}}
						}, "name"},
						{"text_duplicate", func(m *CollectionMeta) {
							m.TextIndexes = []TextIndexDefinition{{Name: "text", Fields: []TextIndexField{{Field: "content"}}}, {Name: "text", Fields: []TextIndexField{{Field: "content"}}}}
						}, "duplicate index"},
					} {
						t.Run(malformed.name, func(t *testing.T) {
							meta := CollectionMeta{Name: "rejected", Options: options, VectorIndexes: []VectorIndexDefinition{first}}
							malformed.apply(&meta)
							normalized, err := normalizeCollectionMeta(meta)
							assertWholeRequestInvalid(t, err, malformed.message, nil)
							if !reflect.DeepEqual(normalized, CollectionMeta{}) {
								t.Fatalf("invalid request returned normalized metadata: %+v", normalized)
							}
							unchanged := wholeRequestNoEffects(t, db, manager, original)
							created, err := manager.CreateCollection(meta.copy())
							assertWholeRequestInvalid(t, err, malformed.message, nil)
							if created != nil {
								t.Fatalf("invalid create returned metadata: %+v", created)
							}
							created, existed, err := manager.CreateCollectionWithStatus(meta)
							assertWholeRequestInvalid(t, err, malformed.message, nil)
							if created != nil || existed {
								t.Fatalf("invalid status create returned metadata=%+v existed=%t", created, existed)
							}
							if _, err := manager.OpenCollection(meta.Name); !errors.Is(err, ErrCollectionNotFound) {
								t.Fatalf("rejected collection installed: %v", err)
							}
							unchanged()
						})
					}
					// Removing the malformed part must retain the typed capability
					// refusal on the same public create path.
					unchanged := wholeRequestNoEffects(t, db, manager, original)
					created, err := manager.CreateCollection(&CollectionMeta{Name: "rejected", Options: options, VectorIndexes: []VectorIndexDefinition{first}})
					if created != nil || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
						t.Fatalf("valid unsupported control: metadata=%+v err=%v", created, err)
					}
					unchanged()
				})
			}
		})
	}
}

func TestVectorDDLOwnerWholeRequestSyntaxBeforeCapability(t *testing.T) {
	for _, native := range []bool{false, true} {
		profile := "legacy"
		if native {
			profile = "native"
		}
		t.Run(profile, func(t *testing.T) {
			db, manager, original := openWholeRequestPrecedenceFixture(t, native)
			if _, err := manager.CreateCollection(&CollectionMeta{Name: "bson", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{Name: "taken", Field: "marker", ValueType: IndexValueBSONOrderedV2}}}); err != nil {
				t.Fatal(err)
			}
			bson, err := manager.OpenCollection("bson")
			if err != nil {
				t.Fatal(err)
			}
			for _, capability := range []string{"retired", "l2", "int8", "bson"} {
				target := original
				if capability == "bson" {
					target = bson
				}
				targetMeta := target.Meta()
				first := VectorIndexDefinition{Name: "first", Field: "embedding", Dimensions: 2}
				switch capability {
				case "retired":
					first.Strategy = "native_runtime"
				case "l2":
					first.Metric = VectorMetricL2
				case "int8":
					first.Encoding = VectorIndexEncodingInt8
				}
				for _, later := range []struct {
					name    string
					def     VectorIndexDefinition
					message string
					kind    error
				}{
					{"dimensions", VectorIndexDefinition{Name: "later", Field: "other"}, "dimensions must be positive", nil},
					{"unknown_strategy", VectorIndexDefinition{Name: "later", Field: "other", Dimensions: 2, Strategy: "unknown"}, "unsupported strategy", nil},
					{"nested_name", VectorIndexDefinition{Name: "later", Field: "other", Dimensions: 2, QuantizedIndexes: []QuantizedVectorIndexDefinition{{}}}, "name is required", nil},
					{"duplicate", VectorIndexDefinition{Name: "first", Field: "other", Dimensions: 2}, "duplicate index", nil},
					{"scalar_collision", VectorIndexDefinition{Name: "taken", Field: "other", Dimensions: 2}, "conflicts", ErrIndexNameConflict},
				} {
					t.Run(capability+"/"+later.name, func(t *testing.T) {
						unchanged := wholeRequestNoEffects(t, db, manager, original)
						called := false
						err := target.WithVectorIndexDDL([]VectorIndexDefinition{first, later.def}, func(*CollectionIndexDDL) error { called = true; return nil })
						assertWholeRequestInvalid(t, err, later.message, later.kind)
						if called {
							t.Fatal("invalid complete request reached DDL publication callback")
						}
						unchanged()
						if !reflect.DeepEqual(targetMeta, target.Meta()) {
							t.Fatal("refusal changed DDL target schema")
						}
					})
				}
				unchanged := wholeRequestNoEffects(t, db, manager, original)
				called := false
				err := target.WithVectorIndexDDL([]VectorIndexDefinition{first}, func(*CollectionIndexDDL) error { called = true; return nil })
				if called || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
					t.Fatalf("valid unsupported DDL control: called=%t err=%v", called, err)
				}
				unchanged()
			}
		})
	}
}

// A recognized representation is syntactically valid even without matching
// typed storage. Preserve capability refusal for retired strategies and unsupported
// metrics/encodings before checking that representation's schema compatibility.
func TestVectorRepresentationSchemaCompatibilityAfterCapability(t *testing.T) {
	for _, native := range []bool{false, true} {
		profile := "legacy"
		if native {
			profile = "native"
		}
		t.Run(profile, func(t *testing.T) {
			db, manager, original := openWholeRequestPrecedenceFixture(t, native)
			for _, capability := range []string{"retired", "l2", "int8", "supported"} {
				t.Run(capability, func(t *testing.T) {
					def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Representation: VectorIndexRepresentationCosineNormalizedF32V1}
					switch capability {
					case "retired":
						def.Strategy = "native_runtime"
					case "l2":
						def.Metric = VectorMetricL2
					case "int8":
						def.Encoding = VectorIndexEncodingInt8
					}
					check := func(err error) {
						t.Helper()
						if capability == "supported" {
							assertWholeRequestInvalid(t, err, "requires enabled managed column storage", nil)
						} else if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
							t.Fatalf("representation compatibility masked vector capability: %v", err)
						}
					}
					meta := CollectionMeta{Name: "rejected", VectorIndexes: []VectorIndexDefinition{def}}
					normalized, err := normalizeCollectionMeta(meta)
					check(err)
					if !reflect.DeepEqual(normalized, CollectionMeta{}) {
						t.Fatalf("refusal returned normalized metadata: %+v", normalized)
					}
					unchanged := wholeRequestNoEffects(t, db, manager, original)
					created, err := manager.CreateCollection(meta.copy())
					check(err)
					if created != nil {
						t.Fatalf("refused create returned metadata: %+v", created)
					}
					created, existed, err := manager.CreateCollectionWithStatus(meta)
					check(err)
					if created != nil || existed {
						t.Fatalf("refused status create returned metadata=%+v existed=%t", created, existed)
					}
					created, err = original.CreateVectorIndex(def)
					check(err)
					if created != nil {
						t.Fatalf("refused index create returned metadata: %+v", created)
					}
					called := false
					err = original.WithVectorIndexDDL([]VectorIndexDefinition{def}, func(*CollectionIndexDDL) error { called = true; return nil })
					check(err)
					if called {
						t.Fatal("refused representation request reached DDL callback")
					}
					if _, err := manager.OpenCollection(meta.Name); !errors.Is(err, ErrCollectionNotFound) {
						t.Fatalf("refused collection installed: %v", err)
					}
					unchanged()
				})
			}
		})
	}
}

// Admission must preserve whole-candidate namespace, policy and idempotence
// rules even though individual definitions are no longer added one at a time.
func TestVectorDDLWholeCandidatePoliciesAndIdentity(t *testing.T) {
	base := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
			DataRootStoragePolicy:        RootStorageFast,
			IndexStateStoragePolicy:      RootStorageCompressed,
		},
		Indexes:       []IndexDefinition{{Name: "scalar", Field: "marker", ValueType: IndexValueInt64, StoragePolicy: RootStorageCompressed}},
		VectorIndexes: []VectorIndexDefinition{{Name: "existing", Field: "embedding", Dimensions: 2, SchemaGeneration: 41}},
		TextIndexes:   []TextIndexDefinition{{Name: "text", Fields: []TextIndexField{{Field: "content"}}}},
	}
	base, err := normalizeCollectionMeta(base)
	if err != nil {
		t.Fatal(err)
	}
	before := *base.copy()
	existing := VectorIndexDefinition{Name: "existing", Field: "embedding", Dimensions: 2, SchemaGeneration: 99}
	added := VectorIndexDefinition{Name: "added", Field: "other", Dimensions: 2}
	vectors, err := validateVectorIndexDDL(base, []VectorIndexDefinition{existing, added}, false)
	if err != nil || len(vectors) != 2 {
		t.Fatalf("mixed idempotent/add admission: vectors=%+v err=%v", vectors, err)
	}
	if !reflect.DeepEqual(base, before) {
		t.Fatal("validation mutated original metadata or policies")
	}
	for _, tc := range []struct {
		name    string
		def     VectorIndexDefinition
		message string
		kind    error
	}{
		{"changed_existing", VectorIndexDefinition{Name: "existing", Field: "embedding", Dimensions: 3}, "different definition", nil},
		{"scalar_collision", VectorIndexDefinition{Name: "scalar", Field: "other", Dimensions: 2}, "conflicts", ErrIndexNameConflict},
		{"text_collision", VectorIndexDefinition{Name: "text", Field: "other", Dimensions: 2}, "duplicate index", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			retired := VectorIndexDefinition{Name: "first", Field: "other", Dimensions: 2, Strategy: "native_runtime"}
			_, err := validateVectorIndexDDL(base, []VectorIndexDefinition{retired, tc.def}, false)
			assertWholeRequestInvalid(t, err, tc.message, tc.kind)
			if !reflect.DeepEqual(base, before) {
				t.Fatal("failed admission mutated original metadata")
			}
		})
	}
	// A typed native collection refuses any added vector, but only after all
	// requested syntax/names have been validated. No physical owner is needed
	// for this pure admission check.
	typed := *base.copy()
	typed.Options.ColumnStore = &ColumnStoreConfig{Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON, Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}}}
	_, err = validateVectorIndexDDL(typed, []VectorIndexDefinition{added, {Name: "later", Field: "third"}}, true)
	assertWholeRequestInvalid(t, err, "dimensions must be positive", nil)
	_, err = validateVectorIndexDDL(typed, []VectorIndexDefinition{added, added}, true)
	assertWholeRequestInvalid(t, err, "duplicate index", nil)
	_, err = validateVectorIndexDDL(typed, []VectorIndexDefinition{added}, true)
	if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || !strings.Contains(err.Error(), "adding vector indexes") {
		t.Fatalf("typed valid unsupported addition: %v", err)
	}
}

func openWholeRequestPrecedenceFixture(t *testing.T, native bool) (*backenddb.DB, *CollectionManager, *Collection) {
	t.Helper()
	var db *backenddb.DB
	if native {
		owner, err := treedb.Open(nativeTypedGroupOptions(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = owner.Close() })
		db, err = owner.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
	} else {
		var err error
		db, err = backenddb.Open(backenddb.Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
	manager := NewCollectionManager(db)
	t.Cleanup(func() { _ = manager.Close() })
	if (manager.nativeJSON != nil) != native {
		t.Fatalf("fixture selected wrong producer: native=%t manager=%+v", native, manager)
	}
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "original", Options: CollectionOptions{DisableIndexedWriteMemtables: true}, Indexes: []IndexDefinition{{Name: "taken", Field: "marker", ValueType: IndexValueInt64}}}); err != nil {
		t.Fatal(err)
	}
	original, err := manager.OpenCollection("original")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := original.Insert([]byte("keep"), []byte(`{"marker":7,"content":"keep"}`)); err != nil {
		t.Fatal(err)
	}
	return db, manager, original
}

func assertWholeRequestInvalid(t *testing.T, err error, message string, kind error) {
	t.Helper()
	if err == nil || errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || !strings.Contains(err.Error(), message) || (kind != nil && !errors.Is(err, kind)) {
		t.Fatalf("request error=%v; want invalid containing %q kind=%v", err, message, kind)
	}
}

func wholeRequestNoEffects(t *testing.T, db *backenddb.DB, manager *CollectionManager, original *Collection) func() {
	t.Helper()
	before, ok := db.StateToken()
	if !ok {
		t.Fatal("missing physical state token")
	}
	next := db.CommandWALNextLSN()
	meta := original.Meta()
	var sequence uint64
	if manager.nativeJSON != nil {
		observation, err := db.ObserveNativeProducer()
		if err != nil {
			t.Fatal(err)
		}
		sequence = observation.LogicalSequence
	}
	return func() {
		t.Helper()
		if after, ok := db.StateToken(); !ok || after != before || db.CommandWALNextLSN() != next {
			t.Fatalf("refusal changed roots or WAL: before=%+v after=%+v nextLSN=%d/%d", before, after, next, db.CommandWALNextLSN())
		}
		if manager.nativeJSON != nil {
			observation, err := db.ObserveNativeProducer()
			if err != nil || observation.LogicalSequence != sequence {
				t.Fatalf("refusal changed native sequence: before=%d after=%+v err=%v", sequence, observation, err)
			}
		}
		reopened, err := manager.OpenCollection("original")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(meta, reopened.Meta()) {
			t.Fatal("refusal changed committed schema")
		}
		doc, err := reopened.Get([]byte("keep"))
		if err != nil || !bytes.Equal(doc, []byte(`{"marker":7,"content":"keep"}`)) {
			t.Fatalf("refusal changed committed document: %s %v", doc, err)
		}
	}
}
