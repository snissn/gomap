package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionBSONFormatStoresNativeBSONAndIndexes(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "age", Field: "age", ValueType: IndexValueInt64},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	doc1 := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "email", Value: "ada@example.com"},
		{Key: "city", Value: "hnl"},
		{Key: "age", Value: int64(37)},
	})
	doc2 := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u2"},
		{Key: "email", Value: "grace@example.com"},
		{Key: "city", Value: "hnl"},
		{Key: "age", Value: int32(42)},
	})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{doc1, doc2},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush indexed memtables: %v", err)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Equal(got, doc1) {
		t.Fatalf("stored BSON changed\n got=%x\nwant=%x", got, doc1)
	}
	if err := bson.Raw(got).Validate(); err != nil {
		t.Fatalf("stored BSON did not validate: %v", err)
	}

	emailIDs, err := col.FindByIndexValue("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u2")) {
		t.Fatalf("email ids=%q want u2", emailIDs)
	}
	cityIDs, err := col.FindByIndexValue("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u1"), []byte("u2"))
	ageIDs, err := col.FindByIndexValue("age", int64(37))
	if err != nil {
		t.Fatalf("find age: %v", err)
	}
	if len(ageIDs) != 1 || !bytes.Equal(ageIDs[0], []byte("u1")) {
		t.Fatalf("age ids=%q want u1", ageIDs)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog: %v", err)
	}
	requireNoCollectionRootDescriptor(t, snap, collectionIndexStateRootName("users"))
	_ = snap.Close()
	if got := catalog.rootID(collectionSecondaryRootName("users", "email")); got == 0 {
		t.Fatal("email secondary root was not persisted")
	}
}

func TestCollectionBSONOrderedV2IndexUsesVersionedEntriesAndNumericEquality(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{Name: "value", Field: "value", ValueType: IndexValueBSONOrderedV2, Unique: true}}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := func(id string, value any) []byte {
		return mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: id}, {Key: "value", Value: value}})
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc("u1", int32(7))}); err != nil {
		t.Fatalf("insert int32: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u2")}, [][]byte{doc("u2", int64(7))}); !IsDuplicateKeyError(err) {
		t.Fatalf("numeric-equivalent unique insert error=%v, want duplicate", err)
	}
	query := bson.Raw(mustBSONCollectionDocument(t, bson.D{{Key: "value", Value: int64(7)}})).Lookup("value")
	ids, err := col.FindByIndexValue("value", query)
	if err != nil {
		t.Fatalf("find numeric-equivalent value: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("ids=%q want u1", ids)
	}
	if matched, err := col.Replace([]byte("u1"), mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "value", Value: int64(7)}, {Key: "note", Value: "same unique value"}})); err != nil || !matched {
		t.Fatalf("same-document unique update matched=%v err=%v", matched, err)
	}
}

func TestCollectionBSONOrderedV2IndexDistinguishesNullFromMissing(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{Name: "value", Field: "value", ValueType: IndexValueBSONOrderedV2, Unique: true}}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	missing := func(id string) []byte { return mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: id}}) }
	null := func(id string) []byte {
		return mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: id}, {Key: "value", Value: nil}})
	}
	if _, err := col.InsertBatch([][]byte{[]byte("missing-1"), []byte("null-1")}, [][]byte{missing("missing-1"), null("null-1")}); err != nil {
		t.Fatalf("insert missing/null: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("missing-2")}, [][]byte{missing("missing-2")}); !IsDuplicateKeyError(err) {
		t.Fatalf("second missing error=%v, want duplicate", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("null-2")}, [][]byte{null("null-2")}); !IsDuplicateKeyError(err) {
		t.Fatalf("second null error=%v, want duplicate", err)
	}
	query := bson.Raw(null("query")).Lookup("value")
	ids, err := col.FindByIndexValue("value", query)
	if err != nil {
		t.Fatalf("find null: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("null-1")) {
		t.Fatalf("null ids=%q want null-1", ids)
	}
	missingIDs, err := col.FindByIndexValue("value", bson.RawValue{})
	if err != nil {
		t.Fatalf("find missing: %v", err)
	}
	if len(missingIDs) != 1 || !bytes.Equal(missingIDs[0], []byte("missing-1")) {
		t.Fatalf("missing ids=%q want missing-1", missingIDs)
	}
}

func TestCollectionBSONOrderedV2RejectsUnsupportedValuesBeforeMutation(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	unsupportedArray := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "array"}, {Key: "value", Value: bson.A{"unsupported"}}})
	unsupportedDocument := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "document"}, {Key: "value", Value: bson.D{{Key: "unsupported", Value: true}}}})
	if _, err := col.InsertBatch([][]byte{[]byte("array"), []byte("document")}, [][]byte{unsupportedArray, unsupportedDocument}); err != nil {
		t.Fatalf("seed unsupported documents without index: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "value", Field: "value", ValueType: IndexValueBSONOrderedV2}); err == nil {
		t.Fatal("CreateIndex accepted unsupported BSON array")
	}
	if _, ok := findIndex(col.MetaView().Indexes, "value"); ok {
		t.Fatal("failed backfill published BSON v2 index metadata")
	}
	if _, err := col.InsertBatch([][]byte{[]byte("ok")}, [][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "ok"}, {Key: "value", Value: "ok"}})}); err != nil {
		t.Fatalf("post-failed-backfill insert: %v", err)
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "indexed", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{Name: "value", Field: "value", ValueType: IndexValueBSONOrderedV2}}}); err != nil {
		t.Fatalf("create indexed collection: %v", err)
	}
	indexed, err := mgr.OpenCollection("indexed")
	if err != nil {
		t.Fatalf("open indexed collection: %v", err)
	}
	if _, err := indexed.InsertBatch([][]byte{[]byte("array")}, [][]byte{unsupportedArray}); err == nil {
		t.Fatal("indexed insert accepted unsupported BSON array")
	}
	if got, err := indexed.Get([]byte("array")); err != nil || len(got) != 0 {
		t.Fatalf("unsupported indexed insert primary read=%x err=%v", got, err)
	}
	if _, err := indexed.InsertBatch([][]byte{[]byte("document")}, [][]byte{unsupportedDocument}); err == nil {
		t.Fatal("indexed insert accepted unsupported BSON document")
	}
	if got, err := indexed.Get([]byte("document")); err != nil || len(got) != 0 {
		t.Fatalf("unsupported document indexed insert primary read=%x err=%v", got, err)
	}
}

func TestCollectionBSONOrderedV2MetadataAndEntryCorruptionFailClosed(t *testing.T) {
	component, err := encodeBSONIndexKeyComponentV2(bson.Raw(mustBSONCollectionDocument(t, bson.D{{Key: "value", Value: "x"}})).Lookup("value"))
	if err != nil {
		t.Fatalf("encode component: %v", err)
	}
	entry, err := bsonIndexEntryKeyV2(component, []byte("doc\x00id"))
	if err != nil {
		t.Fatalf("encode entry: %v", err)
	}
	if _, err := indexKeyDocumentID(IndexValueBSONOrderedV2, entry[:len(entry)-1]); err == nil {
		t.Fatal("truncated v2 entry decoded without metadata-selected v2 decoder")
	}
	if got, err := indexKeyDocumentID(IndexValueBSONOrderedV2, entry); err != nil || !bytes.Equal(got, []byte("doc\x00id")) {
		t.Fatalf("v2 entry document ID=%q err=%v", got, err)
	}

	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	bad, err := json.Marshal(collectionMetaDisk{Version: collectionMetaVersion, Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, Indexes: []IndexDefinition{{Name: "value", Field: "value", ValueType: IndexValueBSONOrderedV2}}})
	if err != nil {
		t.Fatalf("marshal mismatched metadata: %v", err)
	}
	if _, _, err := d.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		snap := d.AcquireSnapshot()
		if snap == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = snap.Close() }()
		return buildSystemTargetIterator(snap, map[string][]byte{systemCollectionMetaKey("users"): bad})
	}); err != nil {
		t.Fatalf("publish mismatched metadata: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if _, err := NewCollectionManager(reopened).OpenCollection("users"); err == nil || !strings.Contains(err.Error(), "requires BSON") {
		t.Fatalf("reopen mismatched BSON v2 metadata err=%v, want fail-closed format validation", err)
	}
}

func TestCollectionBSONOrderedV2MetadataRejectsNonBSONCollections(t *testing.T) {
	for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatTemplateV1} {
		t.Run(string(format), func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()
			_, err = NewCollectionManager(d).CreateCollection(&CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: format}, Indexes: []IndexDefinition{{Name: "value", Field: "value", ValueType: IndexValueBSONOrderedV2}}})
			if err == nil {
				t.Fatalf("CreateCollection accepted BSON v2 index for %q", format)
			}
		})
	}
}

func TestCollectionBSONOrderedV2PersistedMalformedSecondaryEntryFailsClosed(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	index := IndexDefinition{Name: "value", Field: "value", ValueType: IndexValueBSONOrderedV2}
	meta := &CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{index}}
	if _, err := mgr.CreateCollection(meta); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	document := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "value", Value: int32(7)}})
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{document}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	baseCommitSeq := snapshotCommitSeq(snap)
	baseSystemRoot := snapshotSystemRoot(snap)
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	rootName := collectionSecondaryRootName("users", "value")
	baseRoot := catalog.rootID(rootName)
	policy, err := collectionRootStoragePolicyForDB(d, catalog.meta, rootName)
	if err != nil {
		t.Fatalf("secondary root policy: %v", err)
	}
	component, err := encodeBSONIndexKeyComponentV2(bson.Raw(document).Lookup("value"))
	if err != nil {
		t.Fatalf("encode component: %v", err)
	}
	// The component belongs to the public equality range, but lacks the frozen
	// v2 document-ID suffix terminator. A lookup must fail rather than skip it.
	malformed := append(append([]byte(nil), component...), bsonIndexKeyDocumentIDSuffixMarkerV2)
	table := newCollectionRunTable(1)
	setCollectionRunValue(table, malformed, nil)
	table.Freeze()
	defer resetCollectionRunTable(table)
	if _, _, err := d.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          table.NewIterator(nil, nil),
		StoragePolicy: policy,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return col.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, []string{rootName}, map[string]uint64{rootName: baseRoot}, rootIDs)
	}); err != nil {
		t.Fatalf("publish malformed secondary entry: %v", err)
	}
	query := bson.Raw(mustBSONCollectionDocument(t, bson.D{{Key: "value", Value: int64(7)}})).Lookup("value")
	if _, err := col.FindByIndexValue("value", query); err == nil {
		t.Fatal("public lookup accepted persisted malformed v2 secondary entry")
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint malformed entry: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close malformed entry db: %v", err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen malformed entry db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection with malformed entry: %v", err)
	}
	if _, err := reopenedCol.FindByIndexValue("value", query); err == nil {
		t.Fatal("reopened public lookup accepted persisted malformed v2 secondary entry")
	}
}

func TestCollectionBSONOrderedV2CheckpointReopenWithValueLogPointers(t *testing.T) {
	runCollectionBSONOrderedV2CheckpointReopenWithValueLogPointers(t, false)
}

func TestCollectionBSONOrderedV2CheckpointReopenWithValueLogPointersDirect(t *testing.T) {
	runCollectionBSONOrderedV2CheckpointReopenWithValueLogPointers(t, true)
}

func runCollectionBSONOrderedV2CheckpointReopenWithValueLogPointers(t *testing.T, disableIndexedWriteMemtables bool) {
	t.Helper()
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog = treedb.ValueLogOptions{PointerThreshold: 1, ForcePointers: true}
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if !d.HasValueLogAppender() {
		t.Fatal("cached backend did not install the value-log leaf appender")
	}
	mgr := NewCollectionManager(d)
	meta := &CollectionMeta{Name: "users", Options: CollectionOptions{
		DocumentFormat:               DocumentFormatBSON,
		DisableIndexedWriteMemtables: disableIndexedWriteMemtables,
	}, Indexes: []IndexDefinition{{Name: "value", Field: "value", ValueType: IndexValueBSONOrderedV2}}}
	if _, err := mgr.CreateCollection(meta); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	policy, err := collectionRootStoragePolicyForDB(d, *meta, collectionPrimaryRootName(meta.Name))
	if err != nil {
		t.Fatalf("primary root storage policy: %v", err)
	}
	if policy != backenddb.OrderedRootStorageValueLogLeaves {
		t.Fatalf("default primary root policy=%v want value-log leaves with cached appender", policy)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	document := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "value", Value: int32(7)}, {Key: "payload", Value: strings.Repeat("v", 4096)}})
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{document}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if !disableIndexedWriteMemtables {
		if err := col.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	requireCollectionPrimaryEntryPointer(t, d, "users", []byte("u1"))
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, reopenedCleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopenedCleanup() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	query := bson.Raw(mustBSONCollectionDocument(t, bson.D{{Key: "value", Value: int64(7)}})).Lookup("value")
	ids, err := reopenedCol.FindByIndexValue("value", query)
	if err != nil || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("reopened v2 index ids=%q err=%v", ids, err)
	}
	if got, err := reopenedCol.Get([]byte("u1")); err != nil || !bytes.Equal(got, document) {
		t.Fatalf("reopened pointer document len=%d err=%v", len(got), err)
	}
	requireCollectionPrimaryEntryPointer(t, reopened, "users", []byte("u1"))
}

func TestCollectionBSONOrderedV2IndexLifecycleSurvivesMaintenance(t *testing.T) {
	dir := t.TempDir()
	opts := backenddb.Options{Dir: dir, ValueLog: backenddb.ValueLogOptions{PointerThreshold: 1, ForcePointers: true}}
	d, err := backenddb.Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	index := IndexDefinition{Name: "value", Field: "value", ValueType: IndexValueBSONOrderedV2, Unique: true}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{index}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	var u1Document []byte
	for _, row := range []struct {
		id    string
		value any
	}{{"u1", int32(7)}, {"u2", "seven"}} {
		doc := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: row.id}, {Key: "value", Value: row.value}, {Key: "payload", Value: strings.Repeat(row.id, 256)}})
		if row.id == "u1" {
			u1Document = bytes.Clone(doc)
		}
		if _, err := col.InsertBatch([][]byte{[]byte(row.id)}, [][]byte{doc}); err != nil {
			t.Fatalf("insert %s: %v", row.id, err)
		}
	}
	if _, err := col.DropIndex("value"); err != nil {
		t.Fatalf("drop v2 index: %v", err)
	}
	if _, err := col.CreateIndex(index); err != nil {
		t.Fatalf("recreate/backfill v2 index: %v", err)
	}
	if _, err := d.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{}); err != nil {
		t.Fatalf("value-log rewrite: %v", err)
	}
	if _, err := d.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatalf("value-log GC: %v", err)
	}
	if err := col.Delete([]byte("u2")); err != nil {
		t.Fatalf("delete v2 indexed document: %v", err)
	}
	seven := bson.Raw(mustBSONCollectionDocument(t, bson.D{{Key: "value", Value: "seven"}})).Lookup("value")
	if got, err := col.FindByIndexValue("value", seven); err != nil || len(got) != 0 {
		t.Fatalf("post-delete v2 lookup ids=%q err=%v", got, err)
	}
	if err := d.VacuumIndexOnline(context.Background()); err != nil {
		if runtime.GOOS == "windows" && errors.Is(err, backenddb.ErrVacuumUnsupported) {
			t.Log("online vacuum unsupported on windows; retaining delete/reopen coverage")
		} else {
			t.Fatalf("vacuum v2 indexes: %v", err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := backenddb.Open(opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	query := bson.Raw(mustBSONCollectionDocument(t, bson.D{{Key: "value", Value: int64(7)}})).Lookup("value")
	ids, err := reopenedCol.FindByIndexValue("value", query)
	if err != nil || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("post-maintenance v2 lookup ids=%q err=%v", ids, err)
	}
	if got, err := reopenedCol.Get([]byte("u2")); err != nil || len(got) != 0 {
		t.Fatalf("deleted v2 document len=%d err=%v", len(got), err)
	}
	if got, err := reopenedCol.Get([]byte("u1")); err != nil || !bytes.Equal(got, u1Document) {
		t.Fatalf("post-maintenance u1 document len=%d err=%v", len(got), err)
	}
	if ids, err := reopenedCol.FindByIndexValue("value", query); err != nil || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("post-vacuum/reopen v2 lookup ids=%q err=%v", ids, err)
	}
}

func TestCollectionBSONFormatRejectsInvalidBSON(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("bad")}, [][]byte{[]byte{1, 2, 3}}); err == nil {
		t.Fatal("insert invalid BSON err=nil want error")
	}
}

func TestInsertBatchValidatedBSONRequiresBSONCollection(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}})
	_, err = col.InsertBatchValidatedBSON([][]byte{[]byte("u1")}, [][]byte{doc})
	if err == nil || !strings.Contains(err.Error(), "trusted BSON insert requires BSON document format") {
		t.Fatalf("InsertBatchValidatedBSON err=%v want BSON format error", err)
	}
}

func TestInsertBatchPlannerBSONSkipsIndexStateRun(t *testing.T) {
	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", valueType: IndexValueString, unique: true},
			{name: "city", field: "city", valueType: IndexValueString},
		},
		options: collectionOptions{documentFormat: DocumentFormatBSON},
	}

	plan, err := planner.planInsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: "ada@example.com"}, {Key: "city", Value: "hnl"}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: "grace@example.com"}, {Key: "city", Value: "hnl"}}),
		},
	)
	if err != nil {
		t.Fatalf("plan BSON insert batch: %v", err)
	}
	if idx := findRunIndex(plan, collectionRootIndexState, ""); idx >= 0 {
		t.Fatalf("BSON plan unexpectedly emitted index-state run at %d", idx)
	}
	if got := plan.stats.IndexStateRunBuild; got != 0 {
		t.Fatalf("BSON index-state run build=%s want 0", got)
	}
	_ = mustFindRun(t, plan, collectionRootPrimary, "")
	_ = mustFindRun(t, plan, collectionRootSecondary, "email")
	_ = mustFindRun(t, plan, collectionRootSecondary, "city")
}

func TestOrderedIndexStateForDocumentBSONHandlesScalarsAndArrays(t *testing.T) {
	runtimes := []indexRuntime{
		{def: indexDefinition{name: "email", field: "email", valueType: IndexValueString}, path: []string{"email"}},
		{def: indexDefinition{name: "age", field: "age", valueType: IndexValueInt64}, path: []string{"age"}},
		{def: indexDefinition{name: "tag", field: "tags", valueType: IndexValueString, multiKey: true}, path: []string{"tags"}},
		{def: indexDefinition{name: "deleted_at", field: "deleted_at", valueType: IndexValueString}, path: []string{"deleted_at"}},
	}
	doc := mustBSONCollectionDocument(t, bson.D{
		{Key: "email", Value: "ada@example.com"},
		{Key: "age", Value: int64(37)},
		{Key: "tags", Value: bson.A{"b", "a", "a"}},
		{Key: "deleted_at", Value: nil},
	})

	state, err := orderedIndexStateForDocument(doc, runtimes, collectionOptions{documentFormat: DocumentFormatBSON})
	if err != nil {
		t.Fatalf("ordered BSON index state: %v", err)
	}
	requireOrderedIndexValues(t, state, 0, mustEncodeTestIndexScalar(t, IndexValueString, "ada@example.com"))
	requireOrderedIndexValues(t, state, 1, mustEncodeTestIndexScalar(t, IndexValueInt64, int64(37)))
	requireOrderedIndexValues(t, state, 2,
		mustEncodeTestIndexScalar(t, IndexValueString, "a"),
		mustEncodeTestIndexScalar(t, IndexValueString, "b"),
	)
	requireOrderedIndexValues(t, state, 3)
}

func TestCollectionBSONUniqueIndexSkipsNullValues(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	doc1 := mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: nil}, {Key: "city", Value: "hnl"}})
	doc2 := mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: nil}, {Key: "city", Value: "sea"}})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{doc1, doc2},
	); err != nil {
		t.Fatalf("insert null unique values: %v", err)
	}
}

func requireOrderedIndexValues(tb testing.TB, state orderedDocumentIndexState, runtimeIdx int, want ...[]byte) {
	tb.Helper()
	got := state.valuesAt(runtimeIdx)
	if len(got) != len(want) {
		tb.Fatalf("runtime %d values=%q want %q", runtimeIdx, got, want)
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			tb.Fatalf("runtime %d value %d=%q want %q", runtimeIdx, i, got[i], want[i])
		}
	}
}

func mustBSONCollectionDocument(tb testing.TB, doc bson.D) []byte {
	tb.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		tb.Fatalf("marshal BSON document: %v", err)
	}
	return raw
}
