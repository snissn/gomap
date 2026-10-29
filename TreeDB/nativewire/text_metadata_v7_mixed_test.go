package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

// This is codec/admission composition coverage. A text+column declaration does
// not establish nativeTextMeta runtime eligibility or live mixed execution.
func TestTextMetadataV7MixedSchemaResponseAuthority(t *testing.T) {
	meta := textMetadataV7Fixture(collections.DocumentFormatJSON, collections.TextIndexVersionV1)
	meta.Indexes = []collections.IndexDefinition{{Name: "tag", Field: "tag", ValueType: collections.IndexValueString, StoragePolicy: collections.RootStorageCompressed}}
	meta.VectorIndexes = []collections.VectorIndexDefinition{{Name: "embedding", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, Strategy: collections.VectorIndexStrategyColumnGraph, Encoding: collections.VectorIndexEncodingFloat32}}
	meta.Options.ColumnStore = &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}}}
	payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(meta)
	if err != nil {
		t.Fatal(err)
	}
	want, err := collections.DecodeCatalogCreateCollectionCommandWALPayload(payload, true)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeCollectionMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, append(binary.AppendUvarint(nil, 7), payload...)) {
		t.Fatal("mixed schema did not use complete canonical V7 payload")
	}
	for _, create := range []bool{false, true} {
		decoded, err := decodeCollectionMetaForPurpose(raw, create)
		if err != nil || !reflect.DeepEqual(decoded, want) {
			t.Fatalf("mixed roundtrip create=%t: got=%+v want=%+v err=%v", create, decoded, want, err)
		}
	}
	if normalized, err := normalizeClientCollectionMeta(meta); err != nil || !reflect.DeepEqual(normalized, want) {
		t.Fatalf("mixed client admission: got=%+v err=%v", normalized, err)
	}
	sections := func(raw []byte, version uint64) []iwire.Section {
		return []iwire.Section{
			{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})},
			{ID: iwire.SectionIdempotencyKey, Bytes: []byte("mixed-text-column-authority")},
			{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)},
			{ID: iwire.SectionCollectionMeta, Bytes: raw},
		}
	}
	validated, err := iwire.MustV1Registry().ValidateRequestSections(sections(raw, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := iwire.AppendDeterministicEntry(nil, validated); err != nil {
		t.Fatalf("valid mixed create metadata refused: %v", err)
	}
	want.Options.ColumnStore.PhysicalMutationParts = 2
	response, err := encodeCollectionMeta(want)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCollectionMeta(response)
	if err != nil || !reflect.DeepEqual(decoded, want) {
		t.Fatalf("mixed response authority lost: got=%+v err=%v", decoded, err)
	}
	if _, err := normalizeClientCollectionMeta(decoded); err == nil {
		t.Fatal("client create accepted response physical authority")
	}
	if _, err := decodeCollectionMetaForPurpose(response, true); err == nil {
		t.Fatal("create-purpose decoder accepted response physical authority")
	}
	// This same create=true inverse is called directly by Raft metadata lowering.
	if _, err := collections.DecodeCatalogCreateCollectionCommandWALPayload(response[1:], true); err == nil {
		t.Fatal("shared Raft lowering codec accepted response physical authority")
	}
	validated, err = iwire.MustV1Registry().ValidateRequestSections(sections(response, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := iwire.AppendDeterministicEntry(nil, validated); err == nil {
		t.Fatal("deterministic create accepted response physical authority")
	}
	client, server, mgr, db := serveTextMetadataV7CommandWALPipe(t)
	ctx := context.Background()
	if err := client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	version, lsn := clientCatalogVersion(t, client, ctx), db.CommandWALNextLSN()
	if _, err := client.CreateCollection(ctx, decoded); err == nil {
		t.Fatal("actual client create accepted response physical authority")
	}
	if _, err := server.handleCreateCollection(sections(response, version)); err == nil {
		t.Fatal("actual server create accepted response physical authority")
	}
	if clientCatalogVersion(t, client, ctx) != version || db.CommandWALNextLSN() != lsn {
		t.Fatal("mixed hostile create changed catalog/WAL")
	}
	if _, err := mgr.OpenCollection(meta.Name); err == nil {
		t.Fatal("mixed hostile create published collection")
	}
}
