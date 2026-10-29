package nativewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestClientCreateCollectionWholeRequestSyntaxBeforeCapability(t *testing.T) {
	for _, capability := range []string{"retired", "l2", "int8", "bson"} {
		t.Run(capability, func(t *testing.T) {
			client, mgr, db := serveCommandWALCollectionPipe(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatal(err)
			}
			fixture := func() collections.CollectionMeta {
				meta := clientVectorCreateDefaultFixture("", false)
				switch capability {
				case "retired":
					meta.VectorIndexes[0].Strategy = "native_runtime"
				case "l2":
					meta.VectorIndexes[0].Metric = collections.VectorMetricL2
				case "int8":
					meta.VectorIndexes[0].Encoding = collections.VectorIndexEncodingInt8
				case "bson":
					meta.Options.DocumentFormat = collections.DocumentFormatBSON
				}
				return meta
			}
			for _, malformed := range []struct {
				name, message string
				apply         func(*collections.CollectionMeta)
			}{
				{"later_dimensions", "dimensions must be positive", func(m *collections.CollectionMeta) {
					m.VectorIndexes = append(m.VectorIndexes, collections.VectorIndexDefinition{Name: "later", Field: "other"})
				}},
				{"later_numeric", "m cannot be negative", func(m *collections.CollectionMeta) {
					m.VectorIndexes = append(m.VectorIndexes, collections.VectorIndexDefinition{Name: "later", Field: "other", Dimensions: 2, M: -1})
				}},
				{"later_unknown_strategy", "unsupported strategy", func(m *collections.CollectionMeta) {
					m.VectorIndexes = append(m.VectorIndexes, collections.VectorIndexDefinition{Name: "later", Field: "other", Dimensions: 2, Strategy: "unknown"})
				}},
				{"later_nested_name", "name is required", func(m *collections.CollectionMeta) {
					m.VectorIndexes = append(m.VectorIndexes, collections.VectorIndexDefinition{Name: "later", Field: "other", Dimensions: 2, QuantizedIndexes: []collections.QuantizedVectorIndexDefinition{{}}})
				}},
				{"vector_duplicate", "duplicate index", func(m *collections.CollectionMeta) {
					m.VectorIndexes = append(m.VectorIndexes, collections.VectorIndexDefinition{Name: "embedding", Field: "other", Dimensions: 2})
				}},
				{"scalar_vector_collision", "duplicate index", func(m *collections.CollectionMeta) {
					m.Indexes = []collections.IndexDefinition{{Name: "embedding", Field: "tag", ValueType: collections.IndexValueString}}
				}},
				{"vector_text_collision", "duplicate index", func(m *collections.CollectionMeta) {
					m.TextIndexes = []collections.TextIndexDefinition{{Name: "embedding", Fields: []collections.TextIndexField{{Field: "content"}}}}
				}},
				{"text_fields", "fields are required", func(m *collections.CollectionMeta) {
					m.TextIndexes = []collections.TextIndexDefinition{{Name: "text"}}
				}},
				{"text_duplicate", "duplicate index", func(m *collections.CollectionMeta) {
					m.TextIndexes = []collections.TextIndexDefinition{{Name: "text", Fields: []collections.TextIndexField{{Field: "content"}}}, {Name: "text", Fields: []collections.TextIndexField{{Field: "content"}}}}
				}},
				{"column_schema", "requires enabled columns", func(m *collections.CollectionMeta) {
					m.Options.ColumnStore = &collections.ColumnStoreConfig{Enabled: true}
				}},
				{"column_physical_authority", "cannot supply durable physical authority", func(m *collections.CollectionMeta) {
					m.Options.ColumnStore = &collections.ColumnStoreConfig{Enabled: true, PhysicalMutationParts: 1, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}}}
				}},
			} {
				t.Run(malformed.name, func(t *testing.T) {
					input, unchanged := fixture(), fixture()
					malformed.apply(&input)
					malformed.apply(&unchanged)
					version, lsn := clientCatalogVersion(t, client, ctx), db.CommandWALNextLSN()
					created, err := client.CreateCollection(ctx, input)
					if nativeCodeOf(err) != iwire.ErrInvalidCommand || errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || !strings.Contains(err.Error(), malformed.message) {
						t.Errorf("syntax must precede capability: err=%v code=%d want invalid command containing %q", err, nativeCodeOf(err), malformed.message)
					}
					if !reflect.DeepEqual(created, collections.CollectionMeta{}) || !reflect.DeepEqual(input, unchanged) {
						t.Error("rejected create returned metadata or changed caller declarations")
					}
					if db.CommandWALNextLSN() != lsn || clientCatalogVersion(t, client, ctx) != version {
						t.Error("rejected create changed WAL/catalog state")
					}
					if _, err := mgr.OpenCollection(input.Name); !errors.Is(err, collections.ErrCollectionNotFound) {
						t.Errorf("rejected create published collection: %v", err)
					}
				})
			}
			// Removing the malformed declarations must retain typed capability refusal.
			input := fixture()
			version, lsn := clientCatalogVersion(t, client, ctx), db.CommandWALNextLSN()
			_, err := client.CreateCollection(ctx, input)
			if nativeCodeOf(err) != iwire.ErrUnsupportedFeature || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Errorf("valid unsupported control: err=%v code=%d", err, nativeCodeOf(err))
			}
			if !reflect.DeepEqual(input, fixture()) || db.CommandWALNextLSN() != lsn || clientCatalogVersion(t, client, ctx) != version {
				t.Error("capability refusal changed caller declarations or WAL/catalog state")
			}
		})
	}
}

func TestClientCreateCollectionVectorMetadataVersions(t *testing.T) {
	for _, version := range []uint64{5, 6, 7} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			fixture := func() collections.CollectionMeta {
				meta := clientVectorCreateDefaultFixture("", version == 7)
				if version >= 6 {
					meta.Options.ColumnStore = &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}}}
				}
				return meta
			}
			input := fixture()
			normalized, err := normalizeClientCollectionMeta(input)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := encodeCollectionMeta(normalized)
			if err != nil {
				t.Fatal(err)
			}
			gotVersion, _, err := readUvarint(raw)
			if err != nil || gotVersion != version {
				t.Fatalf("metadata version=%d want=%d err=%v", gotVersion, version, err)
			}
			decoded, err := decodeCollectionMetaForPurpose(raw, true)
			if err != nil {
				t.Fatal(err)
			}
			roundTrip, err := encodeCollectionMeta(decoded)
			if err != nil || !bytes.Equal(roundTrip, raw) {
				t.Fatalf("metadata round trip changed V%d bytes: %v", version, err)
			}
			client, _, _ := serveCommandWALCollectionPipe(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatal(err)
			}
			created, err := client.CreateCollection(ctx, input)
			if err != nil || created.Name != normalized.Name || !reflect.DeepEqual(created.VectorIndexes, normalized.VectorIndexes) || !reflect.DeepEqual(created.TextIndexes, normalized.TextIndexes) {
				t.Fatalf("public create: got=%+v want=%+v err=%v", created, normalized, err)
			}
			if version >= 6 && (created.Options.ColumnStore == nil || !reflect.DeepEqual(created.Options.ColumnStore.Columns, normalized.Options.ColumnStore.Columns)) {
				t.Fatal("public create changed column declarations")
			}
			if !reflect.DeepEqual(input, fixture()) {
				t.Fatal("normalization/create changed caller declarations")
			}
		})
	}
}

func TestNormalizeClientCollectionMetaVectorV5KeepsExistingSizeLimits(t *testing.T) {
	input := collections.CollectionMeta{Name: "large"}
	for i := 0; i < 8192; i++ {
		input.VectorIndexes = append(input.VectorIndexes, collections.VectorIndexDefinition{Name: fmt.Sprintf("embedding_%04d", i), Field: "embedding", Dimensions: 2})
	}
	payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) <= collections.CatalogCreateCollectionWireMaxBytesV1 {
		t.Fatal("fixture must exceed the full catalog create payload bound")
	}
	normalized, err := normalizeClientCollectionMeta(input)
	if err != nil {
		t.Fatalf("V5 vector declarations gained a full catalog payload limit: %v", err)
	}
	raw, err := encodeCollectionMeta(normalized)
	if err != nil {
		t.Fatal(err)
	}
	version, _, err := readUvarint(raw)
	if err != nil || version != 5 {
		t.Fatalf("vector metadata version=%d err=%v", version, err)
	}
	decoded, err := decodeCollectionMetaForPurpose(raw, true)
	if err != nil || len(decoded.VectorIndexes) != len(input.VectorIndexes) {
		t.Fatalf("bounded V5 vector request refused: %v", err)
	}
	roundTrip, err := encodeCollectionMeta(decoded)
	if err != nil || !bytes.Equal(roundTrip, raw) {
		t.Fatalf("large V5 metadata changed on round trip: %v", err)
	}
	for _, def := range input.VectorIndexes {
		if def.Strategy != "" || def.Metric != 0 || def.M != 0 {
			t.Fatal("normalization changed caller declarations")
		}
	}
}
