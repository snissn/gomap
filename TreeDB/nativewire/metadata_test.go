package nativewire

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func serveCollectionPipe(t *testing.T) (*Client, *collections.CollectionManager, *backenddb.DB) {
	t.Helper()
	return serveCollectionPipeWithOptions(t, ServerOptions{})
}

func serveCollectionPipeWithOptions(t *testing.T, opts ServerOptions) (*Client, *collections.CollectionManager, *backenddb.DB) {
	t.Helper()
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	opts.Collections = mgr
	opts.Backend = db
	server := NewServer(opts)
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	return client, mgr, db
}

func TestMetadataCommandsRoundTrip(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	meta, err := client.CreateCollection(ctx, collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat:        collections.DocumentFormatJSON,
			DataRootStoragePolicy: collections.RootStorageCompressed,
		},
	})
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if meta.Name != "users" || meta.Options.DataRootStoragePolicy != collections.RootStorageCompressed {
		t.Fatalf("created meta=%+v", meta)
	}

	handle, err := client.OpenCollection(ctx, "users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if handle == 0 {
		t.Fatal("zero collection handle")
	}
	if _, err := mgr.OpenCollection("users"); err != nil {
		t.Fatalf("direct OpenCollection: %v", err)
	}

	meta, err = client.CreateIndex(ctx, "users", collections.IndexDefinition{
		Name:      "email",
		Field:     "email",
		ValueType: collections.IndexValueString,
		Unique:    true,
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if len(meta.Indexes) != 1 || meta.Indexes[0].Name != "email" {
		t.Fatalf("indexes after create=%+v", meta.Indexes)
	}
	indexes, err := client.ListIndexes(ctx, "users")
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	if len(indexes) != 1 || indexes[0].Name != "email" || !indexes[0].Unique {
		t.Fatalf("indexes=%+v", indexes)
	}
	metas, err := client.ListCollections(ctx)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(metas) != 1 || metas[0].Name != "users" {
		t.Fatalf("collections=%+v", metas)
	}
	meta, err = client.DropIndex(ctx, "users", "email")
	if err != nil {
		t.Fatalf("DropIndex: %v", err)
	}
	if len(meta.Indexes) != 0 {
		t.Fatalf("indexes after drop=%+v", meta.Indexes)
	}
	if err := client.CloseCollection(ctx, handle); err != nil {
		t.Fatalf("CloseCollection: %v", err)
	}
}

func TestMetadataHandleRefWorksForListIndexes(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	handle, err := client.OpenCollection(ctx, "users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	sections, err := client.commandSections(ctx, iwire.CommandListIndexes, collectionHandleRef(handle))
	if err != nil {
		t.Fatalf("ListIndexes by handle: %v", err)
	}
	indexes, err := firstIndexVectorFromResponse(sections, client.limits)
	if err != nil {
		t.Fatalf("decode indexes: %v", err)
	}
	if len(indexes) != 0 {
		t.Fatalf("indexes=%+v want none", indexes)
	}
	if _, err := client.CreateIndex(ctx, "users", collections.IndexDefinition{Name: "email", Field: "email", ValueType: collections.IndexValueString}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	sections, err = client.commandSections(ctx, iwire.CommandListIndexes, collectionHandleRef(handle))
	if err != nil {
		t.Fatalf("ListIndexes by handle after create: %v", err)
	}
	indexes, err = firstIndexVectorFromResponse(sections, client.limits)
	if err != nil {
		t.Fatalf("decode indexes after create: %v", err)
	}
	if len(indexes) != 1 || indexes[0].Name != "email" {
		t.Fatalf("indexes after create=%+v want email", indexes)
	}
}

func TestMetadataOpenCollectionHandleLimit(t *testing.T) {
	client, _, _ := serveCollectionPipeWithOptions(t, ServerOptions{MaxCollectionHandles: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if _, err := client.OpenCollection(ctx, "users"); err != nil {
		t.Fatalf("OpenCollection first: %v", err)
	}
	if _, err := client.OpenCollection(ctx, "users"); !isRemoteError(err, iwire.ErrResourceExhausted) {
		t.Fatalf("OpenCollection second error=%v want resource exhausted", err)
	}
}

func TestDecodeCollectionMetaRejectsOversizedIndexCount(t *testing.T) {
	payload := testCollectionMetaPayload(0, 0, 0, maxCollectionMetaIndexDefinitions+1)
	if _, err := decodeCollectionMeta(payload); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("decodeCollectionMeta err=%v code=%d want resource exhausted", err, nativeCodeOf(err))
	}
}

func TestDecodeCollectionMetaRejectsUnknownEnums(t *testing.T) {
	for _, tc := range []struct {
		name        string
		docFormat   uint64
		dataPolicy  uint64
		indexPolicy uint64
	}{
		{name: "document_format", docFormat: 99},
		{name: "data_root_storage", dataPolicy: 99},
		{name: "index_state_root_storage", indexPolicy: 99},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := testCollectionMetaPayload(tc.docFormat, tc.dataPolicy, tc.indexPolicy, 0)
			if _, err := decodeCollectionMeta(payload); nativeCodeOf(err) != iwire.ErrInvalidCommand {
				t.Fatalf("decodeCollectionMeta err=%v code=%d want invalid command", err, nativeCodeOf(err))
			}
		})
	}
}

func TestDecodeCollectionMetaRejectsInvalidCounterValues(t *testing.T) {
	for _, tc := range []struct {
		name        string
		maxDocs     int64
		maxBytes    int64
		maxRootRuns int64
		maxQueued   int64
	}{
		{name: "max_documents", maxDocs: -1},
		{name: "max_bytes", maxBytes: -1},
		{name: "max_root_runs", maxRootRuns: -1},
		{name: "max_queued", maxQueued: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := testCollectionMetaPayloadWithCounters(0, 0, 0, tc.maxDocs, tc.maxBytes, tc.maxRootRuns, tc.maxQueued, 0)
			if _, err := decodeCollectionMeta(payload); nativeCodeOf(err) != iwire.ErrInvalidCommand {
				t.Fatalf("decodeCollectionMeta err=%v code=%d want invalid command", err, nativeCodeOf(err))
			}
		})
	}
}

func TestNormalizeClientCollectionMetaRejectsInvalidOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		meta collections.CollectionMeta
	}{
		{
			name: "data_root_storage",
			meta: collections.CollectionMeta{
				Name: "users",
				Options: collections.CollectionOptions{
					DataRootStoragePolicy: collections.RootStoragePolicy("mystery"),
				},
			},
		},
		{
			name: "index_state_root_storage",
			meta: collections.CollectionMeta{
				Name: "users",
				Options: collections.CollectionOptions{
					IndexStateStoragePolicy: collections.RootStoragePolicy("mystery"),
				},
			},
		},
		{
			name: "max_documents",
			meta: collections.CollectionMeta{
				Name: "users",
				Options: collections.CollectionOptions{
					BufferedIndexedWriteMaxDocuments: -1,
				},
			},
		},
		{
			name: "max_root_runs",
			meta: collections.CollectionMeta{
				Name: "users",
				Options: collections.CollectionOptions{
					BufferedIndexedWriteMaxRootRuns: -1,
				},
			},
		},
		{
			name: "max_queued",
			meta: collections.CollectionMeta{
				Name: "users",
				Options: collections.CollectionOptions{
					BufferedIndexedAsyncFlushMaxQueuedUnits: -1,
				},
			},
		},
		{
			name: "index_field",
			meta: collections.CollectionMeta{
				Name:    "users",
				Indexes: []collections.IndexDefinition{{Name: "email", Field: ".email", ValueType: collections.IndexValueString}},
			},
		},
		{
			name: "index_value_type",
			meta: collections.CollectionMeta{
				Name:    "users",
				Indexes: []collections.IndexDefinition{{Name: "email", Field: "email", ValueType: collections.IndexValueType("dynamic")}},
			},
		},
		{
			name: "index_storage",
			meta: collections.CollectionMeta{
				Name:    "users",
				Indexes: []collections.IndexDefinition{{Name: "email", Field: "email", ValueType: collections.IndexValueString, StoragePolicy: collections.RootStoragePolicy("mystery")}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := normalizeClientCollectionMeta(tc.meta); nativeCodeOf(err) != iwire.ErrInvalidCommand {
				t.Fatalf("normalizeClientCollectionMeta err=%v code=%d want invalid command", err, nativeCodeOf(err))
			}
		})
	}
}

func TestDropCollectionReserved(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, _, err := client.roundTrip(ctx, iwire.FrameRequest, mustCommandBody(t, iwire.CommandDropCollection, collectionNameRef("users")), iwire.FrameResponse)
	if !isRemoteError(err, iwire.ErrUnsupportedFeature) {
		t.Fatalf("drop_collection error=%v want unsupported feature", err)
	}
}

func testCollectionMetaPayload(docFormat, dataPolicy, indexStatePolicy, indexCount uint64) []byte {
	return testCollectionMetaPayloadWithCounters(docFormat, dataPolicy, indexStatePolicy, 0, 0, 0, 0, indexCount)
}

func testCollectionMetaPayloadWithCounters(docFormat, dataPolicy, indexStatePolicy uint64, maxDocs, maxBytes, maxRootRuns, maxQueued int64, indexCount uint64) []byte {
	dst := binary.AppendUvarint(nil, 1)
	dst = appendString(dst, "users")
	dst = binary.AppendUvarint(dst, docFormat)
	dst = binary.AppendUvarint(dst, dataPolicy)
	dst = binary.AppendUvarint(dst, indexStatePolicy)
	dst = appendBool(dst, false)
	dst = appendBool(dst, false)
	dst = appendBool(dst, false)
	dst = binary.AppendVarint(dst, maxDocs)
	dst = binary.AppendVarint(dst, maxBytes)
	dst = binary.AppendVarint(dst, maxRootRuns)
	dst = appendBool(dst, false)
	dst = appendBool(dst, false)
	dst = binary.AppendVarint(dst, maxQueued)
	return binary.AppendUvarint(dst, indexCount)
}

func nativeCodeOf(err error) iwire.ErrorCode {
	code, _ := iwire.ErrorCodeOf(err)
	return code
}

func mustCommandBody(t *testing.T, commandID iwire.CommandID, sections ...iwire.Section) []byte {
	t.Helper()
	body, err := appendCommandRequestBody(nil, commandID, sections...)
	if err != nil {
		t.Fatalf("append command body: %v", err)
	}
	return body
}
