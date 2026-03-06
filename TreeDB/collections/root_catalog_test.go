package collections

import (
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestCreateCollection_AssignsPrimaryRootCatalogDescriptor(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if meta.PrimaryRoot == "" {
		t.Fatalf("expected primary root name to be assigned")
	}

	rootKey, err := SystemCollectionRootKey(meta.PrimaryRoot)
	if err != nil {
		t.Fatalf("root key: %v", err)
	}
	raw, err := d.GetSystem(rootKey)
	if err != nil {
		t.Fatalf("get root descriptor: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("expected root descriptor persisted in system root")
	}

	var desc CollectionRootDescriptor
	if err := desc.Decode(raw); err != nil {
		t.Fatalf("decode root descriptor: %v", err)
	}
	if desc.Name != meta.PrimaryRoot {
		t.Fatalf("root descriptor name = %q, want %q", desc.Name, meta.PrimaryRoot)
	}
	if desc.Collection != meta.Name {
		t.Fatalf("root descriptor collection = %q, want %q", desc.Collection, meta.Name)
	}
	if desc.Kind != CollectionRootKindPrimary {
		t.Fatalf("root kind = %v, want primary", desc.Kind)
	}
	if !desc.Format.OuterLeavesInValueLog {
		t.Fatalf("expected primary root to use outer leaves in value log")
	}
	if !desc.Format.AllowValues {
		t.Fatalf("expected primary root to allow values")
	}
}

func TestCreateCollection_AssignsIndexStateRootCatalogDescriptor(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}

	rootName, err := CollectionIndexStateRootName(meta.Name)
	if err != nil {
		t.Fatalf("state root name: %v", err)
	}
	rootKey, err := SystemCollectionRootKey(rootName)
	if err != nil {
		t.Fatalf("root key: %v", err)
	}
	raw, err := d.GetSystem(rootKey)
	if err != nil {
		t.Fatalf("get root descriptor: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("expected index-state root descriptor persisted in system root")
	}

	var desc CollectionRootDescriptor
	if err := desc.Decode(raw); err != nil {
		t.Fatalf("decode root descriptor: %v", err)
	}
	if desc.Name != rootName {
		t.Fatalf("root descriptor name = %q, want %q", desc.Name, rootName)
	}
	if desc.Collection != meta.Name {
		t.Fatalf("root descriptor collection = %q, want %q", desc.Collection, meta.Name)
	}
	if desc.Kind != CollectionRootKindIndexState {
		t.Fatalf("root kind = %v, want index-state", desc.Kind)
	}
	if desc.Format.OuterLeavesInValueLog {
		t.Fatalf("expected index-state root to disable outer-leaf value-log mode")
	}
	if !desc.Format.AllowValues {
		t.Fatalf("expected index-state root to allow values")
	}
}

func TestCreateIndex_AssignsSecondaryRootCatalogDescriptor(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if len(col.meta.Indexes) != 1 {
		t.Fatalf("expected one index, got %d", len(col.meta.Indexes))
	}
	if col.meta.Indexes[0].RootName == "" {
		t.Fatalf("expected index root name to be assigned")
	}

	rootKey, err := SystemCollectionRootKey(col.meta.Indexes[0].RootName)
	if err != nil {
		t.Fatalf("root key: %v", err)
	}
	raw, err := d.GetSystem(rootKey)
	if err != nil {
		t.Fatalf("get root descriptor: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("expected secondary root descriptor persisted in system root")
	}

	var desc CollectionRootDescriptor
	if err := desc.Decode(raw); err != nil {
		t.Fatalf("decode root descriptor: %v", err)
	}
	if desc.Kind != CollectionRootKindSecondaryIndex {
		t.Fatalf("root kind = %v, want secondary index", desc.Kind)
	}
	if desc.IndexName != "email_idx" {
		t.Fatalf("index name = %q, want email_idx", desc.IndexName)
	}
	if desc.Format.OuterLeavesInValueLog {
		t.Fatalf("expected secondary root to disable outer-leaf value-log mode")
	}
	if desc.Format.AllowValues {
		t.Fatalf("expected secondary root to disallow values")
	}
}

func TestOpenCollection_RejectsLegacySharedKeyspaceMetadata(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	metaKey, err := SystemCollectionMetaKey("users")
	if err != nil {
		t.Fatalf("meta key: %v", err)
	}
	if err := d.SetSystem(metaKey, encodeLegacyCollectionMetaV1ForTest(t, "users")); err != nil {
		t.Fatalf("write legacy metadata: %v", err)
	}

	mgr := NewCollectionManager(d)
	_, err = mgr.OpenCollection("users")
	if err == nil {
		t.Fatalf("expected legacy metadata open to fail")
	}
	if !strings.Contains(err.Error(), "legacy") {
		t.Fatalf("expected legacy-layout error, got %v", err)
	}
}

func encodeLegacyCollectionMetaV1ForTest(t *testing.T, name string) []byte {
	t.Helper()
	meta := &CollectionMeta{
		Version: 1,
		Name:    name,
		Options: DefaultCollectionOptions(),
	}
	raw, err := meta.encodeVersion1ForTest()
	if err != nil {
		t.Fatalf("encode legacy metadata: %v", err)
	}
	return raw
}
