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
}

func TestMetadataDuplicateIndexReturnsInvalidCommand(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	def := collections.IndexDefinition{Name: "email", Field: "email", ValueType: collections.IndexValueString}
	if _, err := client.CreateIndex(ctx, "users", def); err != nil {
		t.Fatalf("CreateIndex first: %v", err)
	}
	if _, err := client.CreateIndex(ctx, "users", def); !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("CreateIndex duplicate err=%v want invalid command", err)
	}
}

func TestDecodeCollectionRefPreservesRawNameCompatibility(t *testing.T) {
	name, wasHandle, err := decodeCollectionRef(nil, []byte("users"))
	if err != nil {
		t.Fatalf("decodeCollectionRef raw: %v", err)
	}
	if wasHandle || name != "users" {
		t.Fatalf("decodeCollectionRef raw name=%q handle=%v", name, wasHandle)
	}
}

func TestMetadataHandleRefsWorkForIndexMetadata(t *testing.T) {
	client, _, db := serveCollectionPipe(t)
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
	if _, err := client.commandSections(ctx, iwire.CommandOpenCollection, collectionHandleRef(handle)); !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("OpenCollection by handle err=%v want invalid command", err)
	}

	version := catalogVersion(t, db)
	createIndexReq := append(replicatedTestGuard("create_index_handle", version),
		collectionHandleRef(handle),
		iwire.Section{ID: iwire.SectionIndexDefinition, Bytes: encodeIndexDefinition(collections.IndexDefinition{
			Name:      "email",
			Field:     "email",
			ValueType: collections.IndexValueString,
		})},
	)
	if _, err := client.commandSections(ctx, iwire.CommandCreateIndex, createIndexReq...); err != nil {
		t.Fatalf("CreateIndex by handle: %v", err)
	}

	version = catalogVersion(t, db)
	dropIndexReq := append(replicatedTestGuard("drop_index_handle", version),
		collectionHandleRef(handle),
		iwire.Section{ID: iwire.SectionIndexName, Bytes: encodeIndexName("email")},
	)
	if _, err := client.commandSections(ctx, iwire.CommandDropIndex, dropIndexReq...); err != nil {
		t.Fatalf("DropIndex by handle: %v", err)
	}
}

func TestMetadataCatalogGuard(t *testing.T) {
	client, _, db := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	version := catalogVersion(t, db)
	createStaleReq := append(replicatedTestGuard("create_collection_stale", version+1),
		iwire.Section{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMeta(collections.CollectionMeta{Name: "users"})},
	)
	if _, err := client.commandSections(ctx, iwire.CommandCreateCollection, createStaleReq...); !isRemoteError(err, iwire.ErrCatalogVersionMismatch) {
		t.Fatalf("CreateCollection stale guard err=%v want catalog mismatch", err)
	}
	createReq := append(replicatedTestGuard("create_collection", version),
		iwire.Section{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMeta(collections.CollectionMeta{Name: "users"})},
	)
	if _, err := client.commandSections(ctx, iwire.CommandCreateCollection, createReq...); err != nil {
		t.Fatalf("CreateCollection guarded: %v", err)
	}

	version = catalogVersion(t, db)
	indexStaleReq := append(replicatedTestGuard("create_index_stale", version+1),
		collectionNameRef("users"),
		iwire.Section{ID: iwire.SectionIndexDefinition, Bytes: encodeIndexDefinition(collections.IndexDefinition{
			Name:      "email",
			Field:     "email",
			ValueType: collections.IndexValueString,
		})},
	)
	if _, err := client.commandSections(ctx, iwire.CommandCreateIndex, indexStaleReq...); !isRemoteError(err, iwire.ErrCatalogVersionMismatch) {
		t.Fatalf("CreateIndex stale guard err=%v want catalog mismatch", err)
	}
	indexReq := append(replicatedTestGuard("create_index", version),
		collectionNameRef("users"),
		iwire.Section{ID: iwire.SectionIndexDefinition, Bytes: encodeIndexDefinition(collections.IndexDefinition{
			Name:      "email",
			Field:     "email",
			ValueType: collections.IndexValueString,
		})},
	)
	if _, err := client.commandSections(ctx, iwire.CommandCreateIndex, indexReq...); err != nil {
		t.Fatalf("CreateIndex guarded: %v", err)
	}
}

func replicatedTestGuard(id string, version uint64) []iwire.Section {
	return []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte(id)},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)},
	}
}

func TestMetadataCloseUnknownHandle(t *testing.T) {
	client, _, _ := serveCollectionPipe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if err := client.CloseCollection(ctx, CollectionHandle(99)); !isRemoteError(err, iwire.ErrCollectionNotFound) {
		t.Fatalf("CloseCollection unknown handle err=%v want collection not found", err)
	}
}

func catalogVersion(t *testing.T, db *backenddb.DB) uint64 {
	t.Helper()
	snap := db.AcquireSnapshot()
	if snap == nil || snap.State() == nil {
		t.Fatal("missing DB snapshot state")
	}
	defer func() { _ = snap.Close() }()
	return snap.State().CommitSeq
}

func TestReadVarintRejectsInvalidOffsets(t *testing.T) {
	if _, err := readVarint(nil, nil); nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("nil offset err=%v code=%d", err, nativeCodeOf(err))
	}
	off := -1
	if _, err := readVarint([]byte{0}, &off); nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("negative offset err=%v code=%d", err, nativeCodeOf(err))
	}
	off = 2
	if _, err := readVarint([]byte{0}, &off); nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("oversized offset err=%v code=%d", err, nativeCodeOf(err))
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

func TestMetadataCollectionCacheBounded(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, MaxCollectionHandles: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatalf("NewInProcessClient: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })
	t.Cleanup(func() { _ = db.Close() })
	for _, name := range []string{"users_a", "users_b", "users_c"} {
		if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: name}); err != nil {
			t.Fatalf("CreateCollection %s: %v", name, err)
		}
		if _, err := client.ListIndexes(ctx, name); err != nil {
			t.Fatalf("ListIndexes %s: %v", name, err)
		}
	}
	state := client.local.state
	state.mu.Lock()
	cached := len(state.collections)
	state.mu.Unlock()
	if cached > 1 {
		t.Fatalf("cached collections=%d want <= 1", cached)
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

func TestReadEnumRejectsInvalidOffset(t *testing.T) {
	off := 2
	if _, err := readEnum([]byte{1}, &off); nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("readEnum err=%v code=%d want malformed", err, nativeCodeOf(err))
	}
}

func TestDecodeCollectionRefRejectsHandleWithoutState(t *testing.T) {
	if _, _, err := decodeCollectionRef(nil, collectionHandleRef(1).Bytes); nativeCodeOf(err) != iwire.ErrInvalidCommand {
		t.Fatalf("decodeCollectionRef err=%v code=%d want invalid command", err, nativeCodeOf(err))
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
