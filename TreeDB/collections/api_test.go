package collections

import (
	"bytes"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionInsertBatchBridge_RoundTripWithSecondaryIndexes(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if got, want := meta.Name, "users"; got != want {
		t.Fatalf("collection name=%q want %q", got, want)
	}

	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	ids, err := col.InsertBatch(
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
		},
	)
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if len(ids) != 2 || !bytes.Equal(ids[0], []byte("u2")) || !bytes.Equal(ids[1], []byte("u1")) {
		t.Fatalf("result ids=%q", ids)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("u1=%q want %q", got, want)
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
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	} {
		if got := catalog.rootID(rootName); got == 0 {
			t.Fatalf("root %q was not persisted", rootName)
		}
	}
}

func TestCollectionInsertBatchBridge_ReturnedIDsAndDocumentsAreOwned(t *testing.T) {
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

	inputID := []byte("u1")
	inputDocument := []byte(`{"name":"ada"}`)
	ids, err := col.InsertBatch(
		[][]byte{inputID},
		[][]byte{inputDocument},
	)
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	inputID[0] = 'x'
	inputDocument[9] = 'x'
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("returned ids=%q want owned u1", ids)
	}

	ids[0][0] = 'z'
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get original id after mutating returned id: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("original id value=%q want %q", got, want)
	}
	if got, err := col.Get([]byte("z1")); err != nil || got != nil {
		t.Fatalf("mutated returned id lookup got=%q err=%v want missing", got, err)
	}
}

func TestCollectionInsertBatchBridge_ReopenUsesPersistedRootDescriptors(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
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
		t.Fatalf("open collection after reopen: %v", err)
	}
	got, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com"}`); !bytes.Equal(got, want) {
		t.Fatalf("reopen u1=%q want %q", got, want)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("reopen email ids=%q want u1", ids)
	}
}

func TestCollectionInsertBatchBridge_AppendsWithoutDroppingExistingRoots(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
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
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert first batch: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"grace@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert second batch: %v", err)
	}

	for id, want := range map[string][]byte{
		"u1": []byte(`{"email":"ada@example.com","city":"hnl"}`),
		"u2": []byte(`{"email":"grace@example.com","city":"hnl"}`),
	} {
		got, err := col.Get([]byte(id))
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s=%q want %q", id, got, want)
		}
	}

	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(cityIDs) != 2 || !bytes.Equal(cityIDs[0], []byte("u1")) || !bytes.Equal(cityIDs[1], []byte("u2")) {
		t.Fatalf("city ids=%q want [u1 u2]", cityIDs)
	}
}

func TestCollectionCreateIndexBackfill_BuildsSecondaryAndIndexState(t *testing.T) {
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
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	meta, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city"})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, ok := findIndex(meta.Indexes, "city"); !ok {
		t.Fatalf("created meta missing city index: %+v", meta.Indexes)
	}
	if _, ok := findIndex(col.Meta().Indexes, "city"); !ok {
		t.Fatalf("collection meta missing city index after create: %+v", col.Meta().Indexes)
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
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if got := catalog.rootID(rootName); got == 0 {
			t.Fatalf("root %q was not persisted", rootName)
		}
	}

	if _, err := col.Insert([]byte("u3"), []byte(`{"email":"katherine@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert after create index: %v", err)
	}
	cityIDs, err = col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after insert: %v", err)
	}
	if len(cityIDs) != 3 ||
		!bytes.Equal(cityIDs[0], []byte("u1")) ||
		!bytes.Equal(cityIDs[1], []byte("u2")) ||
		!bytes.Equal(cityIDs[2], []byte("u3")) {
		t.Fatalf("city ids after insert=%q want [u1 u2 u3]", cityIDs)
	}
}

func TestCollectionCreateIndexBackfill_EmptyCollectionUpdatesSchema(t *testing.T) {
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
	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city"}); err != nil {
		t.Fatalf("create index on empty collection: %v", err)
	}
	if _, ok := findIndex(col.Meta().Indexes, "city"); !ok {
		t.Fatalf("collection meta missing city index after empty create: %+v", col.Meta().Indexes)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert after empty create index: %v", err)
	}
	ids, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after empty create: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("city ids after empty create=%q want [u1]", ids)
	}
}

func TestCollectionCreateIndexBackfill_PreservesExistingIndexState(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"sfo"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city"}); err != nil {
		t.Fatalf("create city index: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
	}

	emailIDs, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find deleted email: %v", err)
	}
	if len(emailIDs) != 0 {
		t.Fatalf("deleted email ids=%q want none", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find deleted city: %v", err)
	}
	if len(cityIDs) != 0 {
		t.Fatalf("deleted city ids=%q want none", cityIDs)
	}
	if _, err := col.Insert([]byte("u3"), []byte(`{"email":"ada@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("reuse unique email after delete: %v", err)
	}
}

func TestCollectionCreateIndexBackfill_ReopenUsesPersistedSchemaAndRoots(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "email", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create unique index: %v", err)
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
		t.Fatalf("find email after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("email ids after reopen=%q want [u1]", ids)
	}
	if _, err := reopenedCol.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("duplicate insert err=%v want unique index conflict", err)
	}
}

func TestCollectionCreateIndexBackfill_RejectsUniqueConflictAtomically(t *testing.T) {
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
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"ada@example.com"}`),
		},
	); err != nil {
		t.Fatalf("insert duplicate documents before unique index: %v", err)
	}

	_, err = col.CreateIndex(IndexDefinition{Name: "email", Field: "email", Unique: true})
	if err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("create unique index err=%v want unique index conflict", err)
	}
	if _, ok := findIndex(col.Meta().Indexes, "email"); ok {
		t.Fatalf("collection meta gained failed email index: %+v", col.Meta().Indexes)
	}
	ids, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find failed index: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("failed index visible ids=%q want none", ids)
	}
}

func TestCollectionInsertBatchBridge_RejectsPersistedUniqueConflictAtomically(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("seed"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	_, err = col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	)
	if err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("err=%v want unique index conflict", err)
	}
	for _, id := range [][]byte{[]byte("u1"), []byte("u2")} {
		got, err := col.Get(id)
		if err != nil {
			t.Fatalf("get %q: %v", id, err)
		}
		if got != nil {
			t.Fatalf("unexpected doc %q=%q", id, got)
		}
	}
}

func TestCollectionInsertBatchBridge_RejectsPersistedDocumentIDAtomically(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"seed"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	_, err = col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"name":"dup"}`), []byte(`{"name":"new"}`)},
	)
	if err == nil || !strings.Contains(err.Error(), "document already exists") {
		t.Fatalf("err=%v want persisted document id conflict", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if want := []byte(`{"name":"seed"}`); !bytes.Equal(got, want) {
		t.Fatalf("u1=%q want %q", got, want)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got != nil {
		t.Fatalf("unexpected u2 doc=%q", got)
	}
}

func TestCollectionInsertBatchBridge_RejectsDuplicateIDBeforePublish(t *testing.T) {
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
	_, err = col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u1")},
		[][]byte{[]byte(`{"name":"first"}`), []byte(`{"name":"second"}`)},
	)
	if err == nil || !strings.Contains(err.Error(), "duplicate document id") {
		t.Fatalf("err=%v want duplicate document id", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if got != nil {
		t.Fatalf("unexpected u1 doc=%q", got)
	}
}

func TestCollectionDeleteBridge_RemovesPrimaryAndSecondaryEntries(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
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
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get deleted u1: %v", err)
	}
	if got != nil {
		t.Fatalf("deleted u1 still visible: %q", got)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if want := []byte(`{"email":"grace@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("u2=%q want %q", got, want)
	}

	emailIDs, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find deleted email: %v", err)
	}
	if len(emailIDs) != 0 {
		t.Fatalf("deleted email ids=%q want none", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(cityIDs) != 1 || !bytes.Equal(cityIDs[0], []byte("u2")) {
		t.Fatalf("city ids=%q want [u2]", cityIDs)
	}
}

func TestCollectionDeleteBridge_AllowsUniqueValueReuse(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
	}
	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("reuse unique email: %v", err)
	}
	ids, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("email ids=%q want [u2]", ids)
	}
}

func TestCollectionDeleteBridge_ReopenUsesDeletedRootDescriptors(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
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
	got, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get deleted u1 after reopen: %v", err)
	}
	if got != nil {
		t.Fatalf("deleted u1 visible after reopen: %q", got)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find deleted email after reopen: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("deleted email ids after reopen=%q want none", ids)
	}
	ids, err = reopenedCol.FindByIndex("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find remaining email after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("remaining email ids=%q want [u2]", ids)
	}
}
