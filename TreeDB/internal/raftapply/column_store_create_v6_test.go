package raftapply

import (
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"testing"
)

func TestCreateCollectionPhysicalColumnMetaV6ActualWAL(t *testing.T) {
	cfg := &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}}}
	schema, err := collections.EncodeColumnStoreWireConfigV1("docs", cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	payload := testCreateCollectionMetaPayload("docs", testCreateCollectionMetaOptions{version: 5})
	payload[0] = 6
	payload = appendTestString(payload, string(schema))
	decoded, err := decodeCreateCollectionMetaV1(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Options.ColumnStore == nil || decoded.Options.ColumnStore.AssetManager == nil {
		t.Fatal("lowering lost physical schema")
	}
	dir := t.TempDir()
	db := openApplyHarnessDB(t, dir)
	defer db.Close()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandCreateCollection, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte("physical-create-v6")},
		{ID: nativewire.SectionCollectionMeta, Bytes: payload},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	command, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	result, err := ApplyCommittedEntryV1(db, entry, applyMeta(1, 1), Options{})
	if err != nil {
		t.Fatal(err)
	}
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	frames := readCommandWALFrames(t, dir)
	if len(frames) != 1 {
		t.Fatalf("frames=%d", len(frames))
	}
	persisted := catalogCreateFrameMeta(t, frames[0])
	if persisted.Options.ColumnStore == nil || persisted.Options.ColumnStore.AssetManager.Kind != collections.ColumnAssetManagerValueLogShaped || persisted.Options.ColumnStore.Columns[0].VectorDims != 2 {
		t.Fatalf("WAL lost schema: %+v", persisted)
	}
	for _, bad := range []string{`{"enabled":true,"unknown":1}`, string(schema) + "{}", `{"enabled":true,"physical_mutation_parts":1,"columns":[{"name":"x","path":"x","value_type":"string"}]}`} {
		malformed := testCreateCollectionMetaPayload("docs", testCreateCollectionMetaOptions{version: 5})
		malformed[0] = 6
		malformed = appendTestString(malformed, bad)
		if _, err := decodeCreateCollectionMetaV1(malformed); err == nil {
			t.Fatalf("lowering accepted invalid schema: %s", bad)
		}
	}
}
