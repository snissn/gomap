package collections

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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
	resolver := &templateV1SnapshotResolver{snap: snap, rootID: templateRoot, cache: make(map[string]*templateV1Template)}
	tpl, err := resolver.lookupTemplateV1(root.templateID)
	_ = snap.Close()
	if err != nil {
		t.Fatalf("lookup template: %v", err)
	}
	if got, want := tpl.fields, []string{"city", "email", "name"}; !equalStringSlices(got, want) {
		t.Fatalf("template fields=%q want %q", got, want)
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
	if !bytes.HasPrefix(doc2, []byte(templateV1StoredMagic)) {
		t.Fatalf("second encoded doc should reuse emitted template and be stored bytes")
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
	storedDoc2, _, err := parseTemplateV1InsertDocument(doc2)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("parse stored doc2: %v", err)
	}
	rootDoc2, err := parseTemplateV1StoredDocument(storedDoc2)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("parse doc2 root: %v", err)
	}
	resolver := &templateV1SnapshotResolver{snap: snap, rootID: templateRoot, cache: make(map[string]*templateV1Template)}
	if _, err := resolver.lookupTemplateV1(rootDoc2.templateID); err != nil {
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
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
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
	if !bytes.HasPrefix(doc2, []byte(templateV1StoredMagic)) {
		t.Fatalf("second encoded doc should reuse emitted template and be stored bytes")
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc1}); err != nil {
		t.Fatalf("insert first buffered batch: %v", err)
	}
	for i := range doc1 {
		doc1[i] = 0
	}
	col.writeDomain.mu.RLock()
	bufferedTemplateRuns := len(col.writeDomain.rootRuns[collectionTemplateRootName("users")])
	col.writeDomain.mu.RUnlock()
	if bufferedTemplateRuns != 1 {
		t.Fatalf("buffered template runs=%d want 1", bufferedTemplateRuns)
	}
	storedDoc2, _, err := parseTemplateV1InsertDocument(doc2)
	if err != nil {
		t.Fatalf("parse doc2: %v", err)
	}
	rootDoc2, err := parseTemplateV1StoredDocument(storedDoc2)
	if err != nil {
		t.Fatalf("parse doc2 root: %v", err)
	}
	opts := collectionOptionsWithBufferedTemplateV1Resolver(collectionOptions{
		documentFormat:   DocumentFormatTemplateV1,
		templateResolver: nil,
	}, col.writeDomain, "users")
	if _, err := opts.templateResolver.lookupTemplateV1(rootDoc2.templateID); err != nil {
		t.Fatalf("lookup buffered template directly: %v", err)
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

func TestTemplateV1EncoderResetEmitsTemplateAgain(t *testing.T) {
	var encoder TemplateV1Encoder
	if _, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"ada@example.com", "hnl"}); err != nil {
		t.Fatalf("encode first doc: %v", err)
	}
	doc, err := encoder.EncodeDocument([]string{"email", "city"}, []any{"grace@example.com", "hnl"})
	if err != nil {
		t.Fatalf("encode second doc: %v", err)
	}
	if !bytes.HasPrefix(doc, []byte(templateV1StoredMagic)) {
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
	_, records, resolver, err := prepareTemplateV1InsertDocuments([][]byte{doc1}, nil)
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
	prepared, records, repeatResolver, err := prepareTemplateV1InsertDocuments([][]byte{doc2}, resolver)
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
	_, records, _, err = prepareTemplateV1InsertDocuments([][]byte{doc3}, resolver)
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

func (r *countingMissingTemplateV1Resolver) lookupTemplateV1([32]byte) (*templateV1Template, error) {
	r.lookups++
	return nil, errTemplateV1TemplateNotFound
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
	_, records, _, err := prepareTemplateV1InsertDocuments([][]byte{doc1, doc2}, fallback)
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
	if !bytes.HasPrefix(compact, []byte(templateV1StoredMagic)) {
		t.Fatalf("expected compact stored document")
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
	resolver := &templateV1SnapshotResolver{snap: snap, rootID: templateRoot, cache: make(map[string]*templateV1Template)}
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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

	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city"}); err != nil {
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
			{Name: "tag", Field: "tags", MultiKey: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1")},
		[][]byte{mustTemplateV1Document(t, []string{"tags"}, []any{[]any{"b", "a", "a"}})},
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
}

func TestTemplateV1NestedIndexExtraction(t *testing.T) {
	doc := mustTemplateV1Document(t,
		[]string{"profile", "email"},
		[]any{
			map[string]any{"city": "hnl", "age": float64(42)},
			"ada@example.com",
		},
	)
	stored, records, err := parseTemplateV1InsertEnvelope(doc)
	if err != nil {
		t.Fatalf("parse template envelope: %v", err)
	}
	resolver := &templateV1MemoryResolver{}
	for _, record := range records {
		if _, err := resolver.addRecord(record); err != nil {
			t.Fatalf("add template record: %v", err)
		}
	}
	runtimes := []indexRuntime{{
		def:  indexDefinition{name: "city", field: "profile.city"},
		path: []string{"profile", "city"},
	}}
	state, err := orderedIndexStateForDocument(stored, runtimes, collectionOptions{
		documentFormat:   DocumentFormatTemplateV1,
		templateResolver: resolver,
	})
	if err != nil {
		t.Fatalf("extract nested index state: %v", err)
	}
	if got, want := state.valuesAt(0), [][]byte{[]byte("s:hnl")}; !byteMatrixEqual(got, want) {
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
	stored, records, err := parseTemplateV1InsertEnvelope(doc)
	if err != nil {
		t.Fatalf("parse template envelope: %v", err)
	}
	resolver := &templateV1MemoryResolver{}
	for _, record := range records {
		if _, err := resolver.addRecord(record); err != nil {
			t.Fatalf("add template record: %v", err)
		}
	}
	planner := insertBatchPlanner{
		indexes: []indexDefinition{
			{name: "email", field: "email"},
			{name: "city", field: "city"},
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
	if got, want := state.valuesAt(0), [][]byte{[]byte("s:ada@example.com")}; !byteMatrixEqual(got, want) {
		t.Fatalf("email values=%q want %q", got, want)
	}
	if got, want := state.valuesAt(1), [][]byte{[]byte("s:hnl")}; !byteMatrixEqual(got, want) {
		t.Fatalf("city values=%q want %q", got, want)
	}
}
