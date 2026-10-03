package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCollectionMetaNilColumnStoreExactV5Bytes(t *testing.T) {
	for _, meta := range []collections.CollectionMeta{
		{Name: "users"},
		{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, BufferedIndexedWrites: true, BufferedIndexedWriteMaxDocuments: 7}, VectorIndexes: []collections.VectorIndexDefinition{{Name: "embedding", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, Strategy: collections.VectorIndexStrategyColumnGraph, Encoding: collections.VectorIndexEncodingFloat32}}},
	} {
		got, err := encodeCollectionMeta(meta)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, encodeCollectionMetaLegacyV5ForTest(meta)) || got[0] != 5 {
			t.Fatalf("nil schema changed legacy v5 bytes: %x", got)
		}
	}
}

func TestCollectionMetaPhysicalV6RoundTripAndCreateValidation(t *testing.T) {
	meta := collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{ColumnStore: &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}}}}}
	raw, err := encodeCollectionMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	if raw[0] != 6 {
		t.Fatalf("physical schema version=%d", raw[0])
	}
	decoded, err := decodeCollectionMeta(raw)
	if err != nil {
		t.Fatal(err)
	}
	cfg := decoded.Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil || cfg.AssetManager.Kind != collections.ColumnAssetManagerValueLogShaped || !cfg.AssetManager.IsolatedNamespace || cfg.Columns[0].VectorDims != 2 {
		t.Fatalf("physical schema lost: %+v", cfg)
	}
	sections := raftClusterCreateCollectionSectionsWithMeta(meta, 1, AckRaftCommitted)
	sections = append([]iwire.Section{{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})}}, sections...)
	validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, validated)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raftentry.DecodeCommandEntryV1(entry, raftentry.DecodeOptions{}); err != nil {
		t.Fatal(err)
	}
	cfg.PhysicalMutationParts = 2
	response, err := encodeCollectionMeta(decoded)
	if err != nil {
		t.Fatal(err)
	}
	responseMeta, err := decodeCollectionMeta(response)
	if err != nil || responseMeta.Options.ColumnStore.PhysicalMutationParts != 2 {
		t.Fatalf("response authority lost: %+v %v", responseMeta, err)
	}
	for i := range sections {
		if sections[i].ID == iwire.SectionCollectionMeta {
			sections[i].Bytes = response
		}
	}
	validated, err = iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err) // Structural validation deliberately does not inspect metadata semantics.
	}
	if _, err := iwire.AppendDeterministicEntry(nil, validated); err == nil {
		t.Fatal("deterministic create accepted response physical authority")
	}
	// Model a hostile peer bypassing the encoder. Decode and the actual
	// submit admission must both refuse it before any commit callback.
	decodedEntry, err := iwire.DecodeDeterministicEntry(entry, iwire.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	hostile := append([]byte(nil), iwire.DeterministicEntryMagic...)
	for _, v := range []uint64{decodedEntry.Version, uint64(decodedEntry.CommandID), decodedEntry.CommandVersion, decodedEntry.CommandFlags, uint64(len(decodedEntry.Sections))} {
		hostile = binary.AppendUvarint(hostile, v)
	}
	for _, section := range decodedEntry.Sections {
		if section.ID == iwire.SectionCollectionMeta {
			section.Bytes = response
		}
		hostile = binary.AppendUvarint(hostile, uint64(section.ID))
		hostile = binary.AppendUvarint(hostile, uint64(len(section.Bytes)))
		hostile = append(hostile, section.Bytes...)
	}
	if _, err := raftentry.DecodeCommandEntryV1(hostile, raftentry.DecodeOptions{}); err == nil {
		t.Fatal("decode accepted response physical authority")
	}
	cluster := raftClusterBridgeTestConfig(nil)
	cluster.Dir = t.TempDir()
	commits, preflights := 0, 0
	applier := &recordingRaftClusterApplier{}
	bridge, err := raftcluster.NewSingleGroupSubmitter(raftcluster.SingleGroupSubmitterOptions{
		Cluster:           cluster,
		AdmissionProvider: raftcluster.StaticAdmissionProvider{Status: raftcluster.LeaderAdmission()},
		CommitSource: raftcluster.CommitSourceFunc(func(context.Context, raftcluster.CommitCommandEntryV1Request) (raftcluster.CommitCommandEntryV1Result, error) {
			commits++
			return raftcluster.CommitCommandEntryV1Result{}, nil
		}),
		Preflight: raftcluster.CommandEntryPreflightFunc(func(context.Context, raftcluster.CommandEntryPreflightRequestV1) (raftcluster.CommandEntryPreflightResultV1, error) {
			preflights++
			return raftcluster.CommandEntryPreflightResultV1{}, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: raftcluster.CatalogVersionProviderFunc(func(context.Context) (uint64, bool, error) { return 1, true, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bridge.SubmitCommandEntryV1(context.Background(), hostile, raftentry.RequestMetadataV1{}); err == nil || preflights != 0 || commits != 0 || applier.calls != 0 {
		t.Fatalf("hostile create crossed preflight/commit/apply: err=%v preflights=%d commits=%d apply=%d", err, preflights, commits, applier.calls)
	}
	if _, err := normalizeClientCollectionMeta(decoded); err == nil {
		t.Fatal("client create accepted physical authority")
	}
	// Invalid config is returned as an error, without dropping it into v5.
	meta.Options.ColumnStore.Columns[0].Owner = "invalid-owner"
	if _, err := encodeCollectionMeta(meta); err == nil {
		t.Fatal("invalid schema encoded")
	}
}

// Exact encoder source from baseline HEAD 85d6e29, with version pinned to 5.
func encodeCollectionMetaLegacyV5ForTest(meta collections.CollectionMeta) []byte {
	dst := binary.AppendUvarint(nil, 5)
	dst = appendString(dst, meta.Name)
	dst = binary.AppendUvarint(dst, uint64(encodeDocumentFormat(meta.Options.DocumentFormat)))
	dst = binary.AppendUvarint(dst, uint64(encodeRootStorage(meta.Options.DataRootStoragePolicy)))
	dst = binary.AppendUvarint(dst, uint64(encodeRootStorage(meta.Options.IndexStateStoragePolicy)))
	dst = appendBool(dst, meta.Options.AllowArrayValuesInIndex)
	dst = appendBool(dst, meta.Options.DisableIndexedWriteMemtables)
	dst = appendBool(dst, meta.Options.BufferedIndexedWrites)
	dst = binary.AppendVarint(dst, int64(meta.Options.BufferedIndexedWriteMaxDocuments))
	dst = binary.AppendVarint(dst, meta.Options.BufferedIndexedWriteMaxBytes)
	dst = binary.AppendVarint(dst, int64(meta.Options.BufferedIndexedWriteMaxRootRuns))
	dst = appendBool(dst, meta.Options.BufferedIndexedAsyncFlush)
	dst = appendBool(dst, meta.Options.BufferedIndexedOverlayRoots)
	dst = binary.AppendVarint(dst, int64(meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits))
	dst = binary.AppendUvarint(dst, uint64(len(meta.Indexes)))
	for _, def := range meta.Indexes {
		dst = appendIndexDefinition(dst, def, false)
	}
	dst = binary.AppendUvarint(dst, uint64(len(meta.VectorIndexes)))
	for _, def := range meta.VectorIndexes {
		dst = appendVectorIndexDefinitionForCollectionMeta(dst, def, 5)
	}
	return dst
}

func TestMetadataDurableColumnStoreExceedsCreateBounds(t *testing.T) {
	t.Run("columns", func(t *testing.T) {
		cfg := &collections.ColumnStoreConfig{Enabled: true}
		for i := 0; i <= collections.ColumnStoreWireMaxColumnsV1; i++ {
			name := fmt.Sprintf("value_%d", i)
			cfg.Columns = append(cfg.Columns, collections.ColumnStoreColumn{Name: name, Path: name, ValueType: collections.ColumnStoreValueString})
		}
		client, server, mgr, _ := serveCollectionPipeWithServer(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := client.Hello(ctx); err != nil {
			t.Fatal(err)
		}
		// A real durable 33-column schema must remain available in metadata
		// responses without widening the bounded native create request.
		_, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{ColumnStore: cfg}})
		if err != nil {
			t.Fatalf("create durable schema: %v", err)
		}
		col, err := mgr.OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		want := col.Meta().Options.ColumnStore
		check := func(meta collections.CollectionMeta) {
			t.Helper()
			if !reflect.DeepEqual(meta.Options.ColumnStore, want) {
				t.Fatal("metadata response changed the durable column schema")
			}
		}
		listed, err := client.ListCollections(ctx)
		if err != nil || len(listed) != 1 {
			t.Fatalf("ListCollections: len=%d err=%v", len(listed), err)
		}
		check(listed[0])
		if _, ok := server.catalogMetadataFingerprint(); !ok {
			t.Fatal("durable schema prevented catalog fingerprinting")
		}
		if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "rejected", Options: collections.CollectionOptions{ColumnStore: cfg}}); err == nil {
			t.Fatal("native create accepted an oversized schema")
		}
		// Bypass client normalization to exercise actual server admission.
		hostile, err := encodeCollectionMeta(collections.CollectionMeta{Name: "rejected", Options: collections.CollectionOptions{ColumnStore: cfg}})
		if err != nil {
			t.Fatal(err)
		}
		refusedVersion := clientCatalogVersion(t, client, ctx)
		_, err = server.handleCreateCollection([]iwire.Section{
			{ID: iwire.SectionIdempotencyKey, Bytes: []byte("oversized-create-columns")},
			{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, refusedVersion)},
			{ID: iwire.SectionCollectionMeta, Bytes: hostile},
		})
		wantReason := "requires 1..32 enabled columns"
		if err == nil || nativeCodeOf(err) != iwire.ErrInvalidCommand || !strings.Contains(err.Error(), wantReason) {
			t.Fatalf("server create did not reject the oversized schema: err=%v want reason %q", err, wantReason)
		}
		if got := clientCatalogVersion(t, client, ctx); got != refusedVersion {
			t.Fatalf("rejected create changed catalog version=%d want %d", got, refusedVersion)
		}
		if _, err := mgr.OpenCollection("rejected"); err == nil {
			t.Fatal("rejected native create published a collection")
		}
		def := collections.IndexDefinition{Name: "tag", Field: "tag", ValueType: collections.IndexValueString}
		version := clientCatalogVersion(t, client, ctx)
		createCtx := WithExpectedCatalogVersion(WithIdempotencyKey(ctx, []byte("large-create-index")), version)
		first, err := client.CreateIndex(createCtx, "docs", def)
		if err != nil {
			t.Fatalf("CreateIndex committed response: %v", err)
		}
		check(first)
		replay, err := client.CreateIndex(createCtx, "docs", def)
		if err != nil || !reflect.DeepEqual(first, replay) {
			t.Fatalf("CreateIndex replay: %v", err)
		}
		if got := clientCatalogVersion(t, client, ctx); got != version+1 {
			t.Fatalf("CreateIndex catalog version=%d want %d", got, version+1)
		}
		version = clientCatalogVersion(t, client, ctx)
		dropCtx := WithExpectedCatalogVersion(WithIdempotencyKey(ctx, []byte("large-drop-index")), version)
		first, err = client.DropIndex(dropCtx, "docs", "tag")
		if err != nil {
			t.Fatalf("DropIndex committed response: %v", err)
		}
		check(first)
		replay, err = client.DropIndex(dropCtx, "docs", "tag")
		if err != nil || !reflect.DeepEqual(first, replay) {
			t.Fatalf("DropIndex replay: %v", err)
		}
		if got := clientCatalogVersion(t, client, ctx); got != version+1 {
			t.Fatalf("DropIndex catalog version=%d want %d", got, version+1)
		}
		col, err = mgr.OpenCollection("docs")
		if err != nil || len(col.Meta().Indexes) != 0 {
			t.Fatalf("DropIndex durable state: err=%v", err)
		}
		if _, ok := server.catalogMetadataFingerprint(); !ok {
			t.Fatal("index mutations prevented catalog fingerprinting")
		}
	})
}

func TestMetadataOversizedColumnStoreBytesRefusesCreate(t *testing.T) {
	client, server, mgr, _ := serveCollectionPipeWithServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	// Exercise response decoding and create admission independently of disk
	// storage: the system-root catalog publisher currently emits inline values.
	cfg := &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{
		{Name: "value", Path: strings.Repeat("x", collections.ColumnStoreWireMaxBytesV1), ValueType: collections.ColumnStoreValueString},
	}}
	meta := collections.CollectionMeta{Name: "rejected", Options: collections.CollectionOptions{ColumnStore: cfg}}
	raw, err := encodeCollectionMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= collections.ColumnStoreWireMaxBytesV1 {
		t.Fatal("fixture does not exceed the create byte bound")
	}
	decoded, err := decodeCollectionMeta(raw)
	if err != nil || decoded.Options.ColumnStore == nil || len(decoded.Options.ColumnStore.Columns) != 1 {
		t.Fatalf("oversized response decode: err=%v", err)
	}
	if decoded.Options.ColumnStore.Columns[0].Path != cfg.Columns[0].Path {
		t.Fatal("response decoding changed the oversized path")
	}
	if _, err := client.CreateCollection(ctx, meta); err == nil {
		t.Fatal("client create accepted oversized schema bytes")
	}
	version := clientCatalogVersion(t, client, ctx)
	_, err = server.handleCreateCollection([]iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("oversized-create-bytes")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)},
		{ID: iwire.SectionCollectionMeta, Bytes: raw},
	})
	if err == nil || nativeCodeOf(err) != iwire.ErrInvalidCommand || !strings.Contains(err.Error(), "exceeds bounded byte limit") {
		t.Fatalf("server did not refuse oversized schema bytes: %v", err)
	}
	if got := clientCatalogVersion(t, client, ctx); got != version {
		t.Fatalf("rejected create changed catalog version=%d want %d", got, version)
	}
	if _, err := mgr.OpenCollection(meta.Name); err == nil {
		t.Fatal("rejected create published a durable collection")
	}
}
