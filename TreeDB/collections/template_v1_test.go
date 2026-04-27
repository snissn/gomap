package collections

import (
	"bytes"
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
		if err := resolver.addRecord(record); err != nil {
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
