package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func mustTemplateV1Document(t *testing.T, fields []string, values []any) []byte {
	t.Helper()
	doc, err := EncodeTemplateV1Document(fields, values)
	if err != nil {
		t.Fatalf("encode template-v1 document: %v", err)
	}
	return doc
}

func requireNoCollectionRootDescriptor(t *testing.T, snap *backenddb.Snapshot, rootName string) {
	t.Helper()
	raw, ok, err := getSystemValue(snap, systemCollectionRootKey(rootName))
	if err != nil {
		t.Fatalf("read root descriptor %q: %v", rootName, err)
	}
	if !ok {
		return
	}
	rootID, err := decodeRootID(raw)
	if err != nil {
		t.Fatalf("decode root descriptor %q: %v", rootName, err)
	}
	if rootID != 0 {
		t.Fatalf("root descriptor %q persisted root %d", rootName, rootID)
	}
}

func TestEncodeTemplateV1DocumentRejectsInvalidFieldSlices(t *testing.T) {
	if _, err := EncodeTemplateV1Document([]string{"email", "email"}, []any{"ada@example.com", "grace@example.com"}); err == nil {
		t.Fatal("expected duplicate field error")
	}
	if _, err := EncodeTemplateV1Document([]string{"email"}, []any{}); err == nil {
		t.Fatal("expected field/value length mismatch error")
	}
}

func TestTemplateV1CollectionInsertBatchIndexesAndTemplateRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	doc1 := mustTemplateV1Document(t,
		[]string{"email", "city", "name"},
		[]any{"ada@example.com", "hnl", "ada"},
	)
	doc2 := mustTemplateV1Document(t,
		[]string{"name", "city", "email"},
		[]any{"grace", "hnl", "grace@example.com"},
	)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{doc1, doc2},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if bytes.Equal(got, doc1) {
		t.Fatalf("template-v1 primary stored the insert envelope instead of compact document bytes")
	}
	if !bytes.HasPrefix(got, []byte(templateV1StoredMagic)) {
		t.Fatalf("stored u1 prefix=%q want template-v1 stored magic", got[:min(len(got), len(templateV1StoredMagic))])
	}

	emailIDs, err := col.FindByIndex("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u2")) {
		t.Fatalf("email ids=%q want u2", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(cityIDs) != 2 || !bytes.Equal(cityIDs[0], []byte("u1")) || !bytes.Equal(cityIDs[1], []byte("u2")) {
		t.Fatalf("city ids=%q want [u1 u2]", cityIDs)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush indexed memtables: %v", err)
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
	templateRoot := catalog.rootID(collectionTemplateRootName("users"))
	if templateRoot == 0 {
		_ = snap.Close()
		t.Fatal("template root was not persisted")
	}
	requireNoCollectionRootDescriptor(t, snap, collectionIndexStateRootName("users"))
	root, err := parseTemplateV1StoredDocument(got)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("parse stored document: %v", err)
	}
	resolver := &templateV1SnapshotResolver{snap: snap, rootID: templateRoot}
	tpl, err := resolver.lookupTemplateV1(root.templateID)
	_ = snap.Close()
	if err != nil {
		t.Fatalf("lookup template: %v", err)
	}
	if got, want := tpl.fields, []string{"city", "email", "name"}; !equalStringSlices(got, want) {
		t.Fatalf("template fields=%q want %q", got, want)
	}
}

func TestTemplateV1StoredDocumentUsesVarintTemplateIDs(t *testing.T) {
	doc := mustTemplateV1Document(t,
		[]string{"profile", "email"},
		[]any{
			map[string]any{"city": "hnl"},
			"ada@example.com",
		},
	)
	prepared, records, _, resolver, err := prepareTemplateV1InsertDocuments([][]byte{doc}, nil, false, true)
	if err != nil {
		t.Fatalf("prepare template-v1 document: %v", err)
	}
	if got, want := len(records), 2; got != want {
		t.Fatalf("published template records=%d want %d", got, want)
	}
	if got := prepared[0]; !bytes.HasPrefix(got, []byte(templateV1StoredMagic)) {
		t.Fatalf("prepared prefix=%q want stored magic", got[:min(len(got), len(templateV1StoredMagic))])
	}
	pos := len(templateV1StoredMagic)
	if _, err := readTemplateV1TemplateID(prepared[0], &pos); err != nil {
		t.Fatalf("read root template id: %v", err)
	}
	if got, want := pos, len(templateV1StoredMagic)+1; got != want {
		t.Fatalf("root template id ended at byte %d want %d-byte varint", got, want)
	}
	root, err := parseTemplateV1StoredDocument(prepared[0])
	if err != nil {
		t.Fatalf("parse stored document: %v", err)
	}
	profile, found, err := templateV1ObjectFieldValue(root, "profile", resolver)
	if err != nil {
		t.Fatalf("extract profile: %v", err)
	}
	if !found {
		t.Fatal("profile field not found")
	}
	if len(profile) == 0 || profile[0] != templateV1KindObject {
		t.Fatalf("profile value prefix=%v want object", profile[:min(len(profile), 1)])
	}
	pos = 1
	if _, err := readTemplateV1TemplateID(profile, &pos); err != nil {
		t.Fatalf("read nested template id: %v", err)
	}
	if pos != 2 {
		t.Fatalf("nested template id ended at byte %d want 1-byte varint after kind", pos)
	}
}

func TestTemplateV1UniqueIndexSkipsNullValues(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	doc1 := mustTemplateV1Document(t, []string{"email", "city"}, []any{nil, "hnl"})
	doc2 := mustTemplateV1Document(t, []string{"email", "city"}, []any{nil, "sea"})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{doc1, doc2},
	); err != nil {
		t.Fatalf("insert null unique values: %v", err)
	}
}

func TestTemplateV1CollectionReopenFindAndDelete(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustTemplateV1Document(t, []string{"email", "city"}, []any{"ada@example.com", "hnl"}),
			mustTemplateV1Document(t, []string{"email", "city"}, []any{"grace@example.com", "hnl"}),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find reopened email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("reopened email ids=%q want u1", ids)
	}
	if err := reopenedCol.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
	}
	ids, err = reopenedCol.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after delete: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("city ids after delete=%q want u2", ids)
	}
}

func TestTemplateV1EncoderReusesPersistedTemplateRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	doc2, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	if !bytes.HasPrefix(doc1, []byte(templateV1InputMagic)) {
		t.Fatalf("first encoded doc should include template record")
	}
	if !bytes.HasPrefix(doc2, []byte(templateV1InsertDocumentMagic)) {
		t.Fatalf("second encoded doc should reuse emitted template and be hash-referenced insert bytes")
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc1}); err != nil {
		t.Fatalf("insert doc1: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush first indexed memtable: %v", err)
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
	templateRoot := catalog.rootID(collectionTemplateRootName("users"))
	if templateRoot == 0 {
		_ = snap.Close()
		t.Fatal("template root missing after first insert")
	}
	resolver := &templateV1SnapshotResolver{snap: snap, rootID: templateRoot}
	preparedDoc2, _, _, repeatResolver, err := prepareTemplateV1InsertDocuments([][]byte{doc2}, resolver, false, true)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("prepare doc2: %v", err)
	}
	if len(preparedDoc2) != 1 {
		_ = snap.Close()
		t.Fatalf("prepared doc2 count=%d want 1", len(preparedDoc2))
	}
	rootDoc2, err := parseTemplateV1StoredDocument(preparedDoc2[0])
	if err != nil {
		_ = snap.Close()
		t.Fatalf("parse doc2 root: %v", err)
	}
	if _, err := repeatResolver.lookupTemplateV1(rootDoc2.templateID); err != nil {
		_ = snap.Close()
		t.Fatalf("lookup doc2 template after first insert: %v", err)
	}
	_ = snap.Close()
	if _, err := col.InsertBatch([][]byte{[]byte("u2")}, [][]byte{doc2}); err != nil {
		t.Fatalf("insert doc2 using persisted template root: %v", err)
	}
	ids, err := col.FindByIndex("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("email ids=%q want u2", ids)
	}
}

func TestTemplateV1EncoderLearnsIDsAfterInsertBatch(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	if !bytes.HasPrefix(doc1, []byte(templateV1InputMagic)) {
		t.Fatalf("first encoded doc prefix=%q want input envelope", doc1[:min(len(doc1), len(templateV1InputMagic))])
	}
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u1")}, [][]byte{doc1}, &encoder); err != nil {
		t.Fatalf("insert doc1 with encoder feedback: %v", err)
	}

	doc2, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "sea"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	if !bytes.HasPrefix(doc2, []byte(templateV1StoredMagic)) {
		t.Fatalf("learned encoder doc2 prefix=%q want stored magic", doc2[:min(len(doc2), len(templateV1StoredMagic))])
	}
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u2")}, [][]byte{doc2}, &encoder); err != nil {
		t.Fatalf("insert learned doc2: %v", err)
	}
	got, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	gotJSON, err := col.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("materialize u2: %v", err)
	}
	if !bytes.Equal(gotJSON, []byte(`{"city":"sea","email":"grace@example.com"}`)) {
		t.Fatalf("u2 json=%s", gotJSON)
	}
}

func TestTemplateV1MaterializesPointerizedCompressedTemplateRootAfterReopen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, dir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true

	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatTemplateV1,
			DataRootStoragePolicy: RootStorageCompressed,
		},
	}); err != nil {
		_ = cleanup()
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = cleanup()
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc, err := encoder.EncodeDocument([]string{"email", "city", "pad"}, []any{"ada@example.com", "hnl", strings.Repeat("x", 128)})
	if err != nil {
		_ = cleanup()
		t.Fatalf("encode doc: %v", err)
	}
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u1")}, [][]byte{doc}, &encoder); err != nil {
		_ = cleanup()
		t.Fatalf("insert doc: %v", err)
	}
	if err := col.Flush(); err != nil {
		_ = cleanup()
		t.Fatalf("flush collection: %v", err)
	}
	if err := mgr.FlushAll(); err != nil {
		_ = cleanup()
		t.Fatalf("flush manager: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = cleanup()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("close db: %v", err)
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
	stored, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get reopened doc: %v", err)
	}
	gotJSON, err := reopenedCol.StoredDocumentJSON(stored)
	if err != nil {
		t.Fatalf("materialize reopened doc: %v", err)
	}
	for _, want := range [][]byte{[]byte(`"email":"ada@example.com"`), []byte(`"city":"hnl"`), []byte(`"pad":"`)} {
		if !bytes.Contains(gotJSON, want) {
			t.Fatalf("reopened json=%s missing %s", gotJSON, want)
		}
	}
}

func TestTemplateV1EncoderLearnsExistingTemplateIDFromHashInsert(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc1}); err != nil {
		t.Fatalf("insert doc1 without feedback: %v", err)
	}
	doc2, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "sea"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	if !bytes.HasPrefix(doc2, []byte(templateV1InsertDocumentMagic)) {
		t.Fatalf("doc2 prefix=%q want hash insert magic", doc2[:min(len(doc2), len(templateV1InsertDocumentMagic))])
	}
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u2")}, [][]byte{doc2}, &encoder); err != nil {
		t.Fatalf("insert doc2 with feedback: %v", err)
	}
	doc3, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"katherine@example.com", "koa"})
	if err != nil {
		t.Fatalf("encode doc3: %v", err)
	}
	if !bytes.HasPrefix(doc3, []byte(templateV1StoredMagic)) {
		t.Fatalf("doc3 prefix=%q want stored magic", doc3[:min(len(doc3), len(templateV1StoredMagic))])
	}
}

func TestTemplateV1EncoderRejectsLearnedIDsAcrossCollections(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatTemplateV1},
	}); err != nil {
		t.Fatalf("create users collection: %v", err)
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{DocumentFormat: DocumentFormatTemplateV1},
	}); err != nil {
		t.Fatalf("create events collection: %v", err)
	}
	users, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open users collection: %v", err)
	}
	events, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("open events collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	if _, err := users.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u1")}, [][]byte{doc1}, &encoder); err != nil {
		t.Fatalf("insert users doc1 with feedback: %v", err)
	}
	doc2, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "sea"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	if !bytes.HasPrefix(doc2, []byte(templateV1StoredMagic)) {
		t.Fatalf("doc2 prefix=%q want learned stored magic", doc2[:min(len(doc2), len(templateV1StoredMagic))])
	}
	if _, err := events.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("e1")}, [][]byte{doc2}, &encoder); err == nil {
		t.Fatal("expected cross-collection learned template ID rejection")
	}
}

func TestTemplateV1StoredDocsRequireScopedEncoderInsert(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatTemplateV1},
	}); err != nil {
		t.Fatalf("create users collection: %v", err)
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{DocumentFormat: DocumentFormatTemplateV1},
	}); err != nil {
		t.Fatalf("create events collection: %v", err)
	}
	users, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open users collection: %v", err)
	}
	events, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("open events collection: %v", err)
	}

	var eventsEncoder TemplateV1Encoder
	eventDoc, err := eventsEncoder.EncodeDocument([]string{"kind", "city"}, []any{"login", "hnl"})
	if err != nil {
		t.Fatalf("encode event doc: %v", err)
	}
	if _, err := events.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("e0")}, [][]byte{eventDoc}, &eventsEncoder); err != nil {
		t.Fatalf("seed events template: %v", err)
	}

	var usersEncoder TemplateV1Encoder
	userDoc, err := usersEncoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode user doc: %v", err)
	}
	if _, err := users.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u1")}, [][]byte{userDoc}, &usersEncoder); err != nil {
		t.Fatalf("seed users template: %v", err)
	}
	learnedDoc, err := usersEncoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "sea"})
	if err != nil {
		t.Fatalf("encode learned user doc: %v", err)
	}
	if !bytes.HasPrefix(learnedDoc, []byte(templateV1StoredMagic)) {
		t.Fatalf("learned doc prefix=%q want stored magic", learnedDoc[:min(len(learnedDoc), len(templateV1StoredMagic))])
	}
	if _, err := events.InsertBatch([][]byte{[]byte("e1")}, [][]byte{learnedDoc}); err == nil {
		t.Fatal("expected ordinary InsertBatch to reject learned stored template-v1 document")
	}
	var freshEncoder TemplateV1Encoder
	if _, err := events.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("e2")}, [][]byte{learnedDoc}, &freshEncoder); err == nil {
		t.Fatal("expected fresh encoder insert to reject learned stored template-v1 document")
	}
}

func TestTemplateV1EncoderAllowsSameCollectionHandleReuse(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatTemplateV1},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	firstHandle, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open first handle: %v", err)
	}
	secondHandle, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open second handle: %v", err)
	}

	var encoder TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	if _, err := firstHandle.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u1")}, [][]byte{doc1}, &encoder); err != nil {
		t.Fatalf("insert through first handle: %v", err)
	}
	doc2, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "sea"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	if _, err := secondHandle.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u2")}, [][]byte{doc2}, &encoder); err != nil {
		t.Fatalf("insert through second handle: %v", err)
	}
}

func TestTemplateV1EncoderResetClearsLearnedIDs(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatTemplateV1},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u1")}, [][]byte{doc1}, &encoder); err != nil {
		t.Fatalf("insert doc1 with feedback: %v", err)
	}
	encoder.Reset()
	doc2, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "sea"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	if !bytes.HasPrefix(doc2, []byte(templateV1InputMagic)) {
		t.Fatalf("reset doc2 prefix=%q want input envelope", doc2[:min(len(doc2), len(templateV1InputMagic))])
	}
}

func TestTemplateV1EncoderConvertsNestedRootShapeObjectsWithLearnedIDs(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatTemplateV1},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"child"}, []any{map[string]any{"child": "ada"}})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u1")}, [][]byte{doc1}, &encoder); err != nil {
		t.Fatalf("insert doc1 with feedback: %v", err)
	}
	doc2, err := encoder.EncodeDocument([]string{"child"}, []any{map[string]any{"child": "grace"}})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	if !bytes.HasPrefix(doc2, []byte(templateV1StoredMagic)) {
		t.Fatalf("doc2 prefix=%q want learned stored magic", doc2[:min(len(doc2), len(templateV1StoredMagic))])
	}
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u2")}, [][]byte{doc2}, &encoder); err != nil {
		t.Fatalf("insert learned nested doc2: %v", err)
	}
	got, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	gotJSON, err := col.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("materialize u2: %v", err)
	}
	if !bytes.Equal(gotJSON, []byte(`{"child":{"child":"grace"}}`)) {
		t.Fatalf("u2 json=%s", gotJSON)
	}
}

func TestTemplateV1EncoderLearnsBufferedTemplateIDs(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u1")}, [][]byte{doc1}, &encoder); err != nil {
		t.Fatalf("insert buffered doc1 with encoder feedback: %v", err)
	}
	doc2, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "sea"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	if !bytes.HasPrefix(doc2, []byte(templateV1StoredMagic)) {
		t.Fatalf("learned buffered doc2 prefix=%q want stored magic", doc2[:min(len(doc2), len(templateV1StoredMagic))])
	}
	if _, err := col.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("u2")}, [][]byte{doc2}, &encoder); err != nil {
		t.Fatalf("insert buffered learned doc2: %v", err)
	}
	ids, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("city ids=%q want [u2]", ids)
	}
}

func TestTemplateV1IndexedWriteMemtablesResolveBufferedTemplateAcrossBatches(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	doc2, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "sea"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	if !bytes.HasPrefix(doc2, []byte(templateV1InsertDocumentMagic)) {
		t.Fatalf("second encoded doc should reuse emitted template and be hash-referenced insert bytes")
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc1}); err != nil {
		t.Fatalf("insert first buffered batch: %v", err)
	}
	for i := range doc1 {
		doc1[i] = 0
	}
	col.writeDomain.mu.RLock()
	bufferedTemplateRuns := len(pendingIndexedRootRunsLocked(col.writeDomain, collectionTemplateRootName("users")))
	col.writeDomain.mu.RUnlock()
	if bufferedTemplateRuns != 1 {
		t.Fatalf("buffered template runs=%d want 1", bufferedTemplateRuns)
	}
	clonedTemplateRuns, err := cloneBufferedTemplateV1Runs(col.writeDomain, "users")
	if err != nil {
		t.Fatalf("clone buffered template resolver: %v", err)
	}
	defer resetCollectionTables(clonedTemplateRuns)
	opts := collectionOptionsWithBufferedTemplateV1RunsResolver(collectionOptions{
		documentFormat:   DocumentFormatTemplateV1,
		templateResolver: nil,
	}, clonedTemplateRuns)
	if err := mgr.FlushAll(); err != nil {
		t.Fatalf("flush source buffered template run: %v", err)
	}
	preparedDoc2, _, _, preparedResolver, err := prepareTemplateV1InsertDocuments([][]byte{doc2}, opts.templateResolver, false, true)
	if err != nil {
		t.Fatalf("prepare doc2 with cloned buffered resolver: %v", err)
	}
	rootDoc2, err := parseTemplateV1StoredDocument(preparedDoc2[0])
	if err != nil {
		t.Fatalf("parse doc2 root: %v", err)
	}
	if _, err := preparedResolver.lookupTemplateV1(rootDoc2.templateID); err != nil {
		t.Fatalf("lookup cloned buffered template after source flush: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u2")}, [][]byte{doc2}); err != nil {
		t.Fatalf("insert second buffered batch: %v", err)
	}
	ids, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find buffered city: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("city ids=%q want [u2]", ids)
	}
}

func TestTemplateV1BufferedResolverRefreshesFallbackAfterAsyncPublishRace(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc: %v", err)
	}
	prepared, _, _, _, err := prepareTemplateV1InsertDocuments([][]byte{doc}, nil, false, true)
	if err != nil {
		t.Fatalf("prepare stored doc: %v", err)
	}
	stored := prepared[0]
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
		t.Fatalf("insert buffered template-v1 doc: %v", err)
	}

	stale := d.AcquireSnapshot()
	if stale == nil {
		t.Fatal("acquire stale snapshot")
	}
	defer func() {
		if stale != nil {
			_ = stale.Close()
		}
	}()
	catalog, err := col.catalogForSnapshot(stale)
	if err != nil {
		t.Fatalf("load stale catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing stale catalog")
	}
	staleOptions, err := collectionPlannerOptions(col.meta)
	if err != nil {
		t.Fatalf("planner options: %v", err)
	}
	staleOptions = collectionOptionsWithTemplateV1Resolver(staleOptions, stale, catalog)

	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	if err := col.publishPreparedIndexedFlush(work); err != nil {
		t.Fatalf("publish async flush: %v", err)
	}

	clonedRuns, err := cloneBufferedTemplateV1Runs(col.writeDomain, "users")
	if err != nil {
		t.Fatalf("clone buffered template resolver: %v", err)
	}
	defer resetCollectionTables(clonedRuns)
	staleOptions = collectionOptionsWithBufferedTemplateV1RunsResolver(staleOptions, clonedRuns)
	if len(clonedRuns) != 0 {
		t.Fatalf("cloned template runs=%d want 0 after publish removed buffered overlay", len(clonedRuns))
	}
	err = validateTemplateV1StoredDocumentTemplates(stored, staleOptions.templateResolver)
	if err == nil {
		t.Fatal("stale resolver unexpectedly found just-published template")
	}
	if !errors.Is(err, errTemplateV1MissingTemplateRoot) && !errors.Is(err, errTemplateV1TemplateNotFound) {
		t.Fatalf("stale resolver err=%v want missing template", err)
	}
	if !templateV1PlanningSnapshotNeedsRefresh(col.writeDomain, stale, clonedRuns) {
		t.Fatal("empty clone after async publish did not request template-v1 snapshot refresh")
	}

	_, meta, refreshedOptions, err := col.refreshTemplateV1PlanningSnapshot(&stale, false, clonedRuns, false)
	if err != nil {
		t.Fatalf("refresh template-v1 planning snapshot: %v", err)
	}
	if meta.Name != "users" {
		t.Fatalf("refreshed meta name=%q want users", meta.Name)
	}
	if err := validateTemplateV1StoredDocumentTemplates(stored, refreshedOptions.templateResolver); err != nil {
		t.Fatalf("refreshed resolver did not find published template: %v", err)
	}
}

func TestTemplateV1MaterializerOwnsBufferedTemplateRuns(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
		t.Fatalf("insert buffered template-v1 doc: %v", err)
	}
	col.writeDomain.mu.RLock()
	bufferedTemplateRuns := len(pendingIndexedRootRunsLocked(col.writeDomain, collectionTemplateRootName("users")))
	col.writeDomain.mu.RUnlock()
	if bufferedTemplateRuns == 0 {
		t.Fatal("expected buffered template run before creating materializer")
	}
	begins, ends, active := 0, 0, 0
	unregister := d.RegisterForegroundReadObserver(func() {}, func() func() {
		begins++
		active++
		return func() {
			ends++
			active--
		}
	})
	defer unregister()
	materializer, err := col.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatalf("new materializer: %v", err)
	}
	defer func() { _ = materializer.Close() }()
	if active != 0 || begins != ends {
		t.Fatalf("foreground begin/end/active after materializer construction=%d/%d/%d want balanced idle", begins, ends, active)
	}

	stored, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get buffered stored doc: %v", err)
	}
	if err := mgr.FlushAll(); err != nil {
		t.Fatalf("flush buffered template run: %v", err)
	}

	beginsBeforeMaterialize := begins
	jsonDoc, err := materializer.StoredDocumentJSON(stored)
	if err != nil {
		t.Fatalf("materialize after buffered run release: %v", err)
	}
	if !bytes.Contains(jsonDoc, []byte(`"email":"ada@example.com"`)) || !bytes.Contains(jsonDoc, []byte(`"city":"hnl"`)) {
		t.Fatalf("materialized json=%s", jsonDoc)
	}
	if active != 0 || begins != ends || begins != beginsBeforeMaterialize+1 {
		t.Fatalf("foreground begin/end/active after materialize=%d/%d/%d want one balanced operation", begins, ends, active)
	}
}

func TestTemplateV1IndexedFlushUnitResolveBufferedTemplate(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	doc2, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "sea"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc1}); err != nil {
		t.Fatalf("insert first buffered batch: %v", err)
	}
	col.writeDomain.mu.Lock()
	if !rotateIndexedMutableToFlushUnitLocked(col.writeDomain) {
		t.Fatal("rotate indexed mutable state returned false")
	}
	col.writeDomain.mu.Unlock()
	if _, err := col.InsertBatch([][]byte{[]byte("u2")}, [][]byte{doc2}); err != nil {
		t.Fatalf("insert second batch using flush-unit template: %v", err)
	}
	ids, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("city ids=%q want [u2]", ids)
	}
}

func TestTemplateV1EncoderResetEmitsTemplateAgain(t *testing.T) {
	var encoder TemplateV1Encoder
	if _, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"}); err != nil {
		t.Fatalf("encode first doc: %v", err)
	}
	doc, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode second doc: %v", err)
	}
	if !bytes.HasPrefix(doc, []byte(templateV1InsertDocumentMagic)) {
		t.Fatalf("second encoded doc should omit previously emitted template record")
	}
	encoder.Reset()
	doc, err = encoder.EncodeDocument([]string{"email", "city"}, []any{"katherine@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode after reset: %v", err)
	}
	if !bytes.HasPrefix(doc, []byte(templateV1InputMagic)) {
		t.Fatalf("reset encoded doc should include template record")
	}
}

func TestPrepareTemplateV1InsertDocumentsSkipsFallbackTemplateRecord(t *testing.T) {
	var encoder TemplateV1Encoder
	doc1, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	_, records, _, resolver, err := prepareTemplateV1InsertDocuments([][]byte{doc1}, nil, false, true)
	if err != nil {
		t.Fatalf("prepare doc1: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("doc1 publish records=%d want 1", len(records))
	}

	var repeatEncoder TemplateV1Encoder
	doc2, err := repeatEncoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	prepared, records, _, repeatResolver, err := prepareTemplateV1InsertDocuments([][]byte{doc2}, resolver, false, true)
	if err != nil {
		t.Fatalf("prepare doc2 with fallback: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("doc2 publish records=%d want 0 for existing fallback template", len(records))
	}
	root, err := parseTemplateV1StoredDocument(prepared[0])
	if err != nil {
		t.Fatalf("parse prepared doc2: %v", err)
	}
	if _, err := repeatResolver.lookupTemplateV1(root.templateID); err != nil {
		t.Fatalf("lookup prepared doc2 template: %v", err)
	}

	var newShapeEncoder TemplateV1Encoder
	doc3, err := newShapeEncoder.EncodeDocument([]string{"email", "city", "name"}, []any{"katherine@example.com", "hnl", "katherine"})
	if err != nil {
		t.Fatalf("encode doc3: %v", err)
	}
	_, records, _, _, err = prepareTemplateV1InsertDocuments([][]byte{doc3}, resolver, false, true)
	if err != nil {
		t.Fatalf("prepare doc3 with fallback: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("doc3 publish records=%d want 1 for new template shape", len(records))
	}
}

type countingMissingTemplateV1Resolver struct {
	lookups int
}

func (r *countingMissingTemplateV1Resolver) lookupTemplateV1(uint64) (*templateV1Template, error) {
	return nil, errTemplateV1TemplateNotFound
}

func (r *countingMissingTemplateV1Resolver) lookupTemplateV1ByHash([32]byte) (*templateV1Template, error) {
	r.lookups++
	return nil, errTemplateV1TemplateNotFound
}

func (r *countingMissingTemplateV1Resolver) nextTemplateV1ID() (uint64, error) {
	return 1, nil
}

func TestPrepareTemplateV1InsertDocumentsDedupesBatchTemplateRecords(t *testing.T) {
	doc1, err := EncodeTemplateV1Document([]string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode doc1: %v", err)
	}
	doc2, err := EncodeTemplateV1Document([]string{"email", "city"}, []any{"grace@example.com", "sea"})
	if err != nil {
		t.Fatalf("encode doc2: %v", err)
	}
	fallback := &countingMissingTemplateV1Resolver{}
	_, records, _, _, err := prepareTemplateV1InsertDocuments([][]byte{doc1, doc2}, fallback, false, true)
	if err != nil {
		t.Fatalf("prepare duplicate template records: %v", err)
	}
	if got, want := len(records), 1; got != want {
		t.Fatalf("publish records=%d want %d", got, want)
	}
	if got, want := fallback.lookups, 1; got != want {
		t.Fatalf("fallback lookups=%d want %d", got, want)
	}
}

func TestPrepareTemplateV1InsertDocumentsRejectsShortHashDocument(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("prepare short template-v1 hash document panicked: %v", r)
		}
	}()
	if _, _, _, _, err := prepareTemplateV1InsertDocuments([][]byte{[]byte(templateV1InsertDocumentMagic)}, nil, false, true); err == nil {
		t.Fatal("expected malformed template-v1 insert document error")
	}
}

func TestTemplateV1CompactDocumentRequiresKnownTemplate(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	if _, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"}); err != nil {
		t.Fatalf("encode first doc: %v", err)
	}
	compact, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode compact doc: %v", err)
	}
	if !bytes.HasPrefix(compact, []byte(templateV1InsertDocumentMagic)) {
		t.Fatalf("expected compact hash-referenced insert document")
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{compact}); err == nil {
		t.Fatalf("insert compact doc without persisted template got nil error")
	}
}

func TestTemplateV1UnbufferedSingleInsertUsesTemplateRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	col.writeDomain = nil

	doc := mustTemplateV1Document(t, []string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if _, err := col.Insert([]byte("u1"), doc); err != nil {
		t.Fatalf("single insert: %v", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if bytes.Equal(got, doc) {
		t.Fatalf("unbuffered single insert stored template envelope")
	}
	if !bytes.HasPrefix(got, []byte(templateV1StoredMagic)) {
		t.Fatalf("stored u1 prefix=%q want template-v1 stored magic", got[:min(len(got), len(templateV1StoredMagic))])
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
	templateRoot := catalog.rootID(collectionTemplateRootName("users"))
	if templateRoot == 0 {
		_ = snap.Close()
		t.Fatal("template root was not persisted")
	}
	stored, err := parseTemplateV1StoredDocument(got)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("parse stored doc: %v", err)
	}
	resolver := &templateV1SnapshotResolver{snap: snap, rootID: templateRoot}
	if _, err := resolver.lookupTemplateV1(stored.templateID); err != nil {
		_ = snap.Close()
		t.Fatalf("lookup template: %v", err)
	}
	_ = snap.Close()
}

func TestTemplateV1RejectsOversizedArrayCount(t *testing.T) {
	pos := 0
	_, err := decodeTemplateV1Value([]byte{
		templateV1KindArray,
		2,
		templateV1KindNull,
	}, &pos, nil)
	if err == nil || !strings.Contains(err.Error(), "array length") {
		t.Fatalf("decode oversized array err=%v, want array length error", err)
	}
}

func TestTemplateV1RejectsExcessiveArrayCount(t *testing.T) {
	count := templateV1MaxArrayElements + 1
	raw := []byte{templateV1KindArray}
	raw = binary.AppendUvarint(raw, count)
	raw = append(raw, make([]byte, int(count))...)

	pos := 0
	_, err := decodeTemplateV1Value(raw, &pos, nil)
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("decode excessive array err=%v, want maximum error", err)
	}
}

func TestTemplateV1UpdatePublishesNewTemplateShape(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	doc := mustTemplateV1Document(t, []string{"email", "city"}, []any{"ada@example.com", "hnl"})
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	replacement, err := EncodeTemplateV1DocumentJSON([]byte(`{"email":"ada@example.com","city":"hnl","updated":true,"update_seq":1}`))
	if err != nil {
		t.Fatalf("encode replacement: %v", err)
	}
	if !bytes.HasPrefix(replacement, []byte(templateV1InputMagic)) {
		t.Fatalf("replacement prefix=%q want template envelope", replacement[:min(len(replacement), len(templateV1InputMagic))])
	}
	matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return replacement, true, nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("update matched=%v modified=%v want true,true", matched, modified)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.HasPrefix(got, []byte(templateV1StoredMagic)) {
		t.Fatalf("stored u1 prefix=%q want template-v1 stored magic", got[:min(len(got), len(templateV1StoredMagic))])
	}
	jsonDoc, err := col.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("materialize stored document: %v", err)
	}
	if !bytes.Contains(jsonDoc, []byte(`"updated":true`)) || !bytes.Contains(jsonDoc, []byte(`"update_seq":1`)) {
		t.Fatalf("materialized update json=%s", jsonDoc)
	}
	emailIDs, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u1")) {
		t.Fatalf("email ids=%q want u1", emailIDs)
	}
}

func TestTemplateV1CreateIndexBackfillsFromTemplateRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustTemplateV1Document(t, []string{"email", "city"}, []any{"ada@example.com", "hnl"}),
			mustTemplateV1Document(t, []string{"email", "city"}, []any{"grace@example.com", "sea"}),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	ids, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("city ids=%q want u2", ids)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	requireNoCollectionRootDescriptor(t, snap, collectionIndexStateRootName("users"))
	_ = snap.Close()
}

func TestTemplateV1MultiKeyIndex(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{
			{Name: "tag", Field: "tags", ValueType: IndexValueString, MultiKey: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1"), []byte("d2")},
		[][]byte{
			mustTemplateV1Document(t, []string{"tags"}, []any{[]any{"b", nil, "a", "a"}}),
			mustTemplateV1Document(t, []string{"tags"}, []any{[]any{nil, nil}}),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	ids, err := col.FindByIndex("tag", "a")
	if err != nil {
		t.Fatalf("find tag: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("d1")) {
		t.Fatalf("tag ids=%q want d1", ids)
	}
	ids, err = col.FindByIndex("tag", "b")
	if err != nil {
		t.Fatalf("find tag b: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("d1")) {
		t.Fatalf("tag b ids=%q want d1", ids)
	}
}

func TestTemplateV1NestedIndexExtraction(t *testing.T) {
	doc := mustTemplateV1Document(t,
		[]string{"profile", "email"},
		[]any{
			map[string]any{"city": "hnl", "age": float64(42)},
			"ada@example.com",
		},
	)
	prepared, _, _, resolver, err := prepareTemplateV1InsertDocuments([][]byte{doc}, nil, false, true)
	if err != nil {
		t.Fatalf("prepare template document: %v", err)
	}
	stored := prepared[0]
	runtimes := []indexRuntime{{
		def:  indexDefinition{name: "city", field: "profile.city", valueType: IndexValueString},
		path: []string{"profile", "city"},
	}}
	state, err := orderedIndexStateForDocument(stored, runtimes, collectionOptions{
		documentFormat:   DocumentFormatTemplateV1,
		templateResolver: resolver,
	})
	if err != nil {
		t.Fatalf("extract nested index state: %v", err)
	}
	if got, want := state.valuesAt(0), [][]byte{mustEncodeTestIndexScalar(t, IndexValueString, "hnl")}; !byteMatrixEqual(got, want) {
		t.Fatalf("nested values=%q want %q", got, want)
	}
}

func TestTemplateV1RootIndexExtraction(t *testing.T) {
	doc := mustTemplateV1Document(t,
		[]string{"profile", "email", "city"},
		[]any{
			map[string]any{"age": float64(42)},
			"ada@example.com",
			"hnl",
		},
	)
	prepared, _, _, resolver, err := prepareTemplateV1InsertDocuments([][]byte{doc}, nil, false, true)
	if err != nil {
		t.Fatalf("prepare template document: %v", err)
	}
	stored := prepared[0]
	planner := insertBatchPlanner{
		indexes: []indexDefinition{
			{name: "email", field: "email", valueType: IndexValueString},
			{name: "city", field: "city", valueType: IndexValueString},
		},
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		t.Fatalf("index runtimes: %v", err)
	}
	state, err := orderedIndexStateForDocument(stored, runtimes, collectionOptions{
		documentFormat:   DocumentFormatTemplateV1,
		templateResolver: resolver,
	})
	if err != nil {
		t.Fatalf("extract root index state: %v", err)
	}
	if got, want := state.valuesAt(0), [][]byte{mustEncodeTestIndexScalar(t, IndexValueString, "ada@example.com")}; !byteMatrixEqual(got, want) {
		t.Fatalf("email values=%q want %q", got, want)
	}
	if got, want := state.valuesAt(1), [][]byte{mustEncodeTestIndexScalar(t, IndexValueString, "hnl")}; !byteMatrixEqual(got, want) {
		t.Fatalf("city values=%q want %q", got, want)
	}
}
