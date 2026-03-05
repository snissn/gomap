package collections

import (
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestCreateCollection_AndDropCollection_Idempotent(t *testing.T) {
	dbx, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer dbx.Close()

	mgr := NewCollectionManager(dbx)
	meta := &CollectionMeta{
		Name:    "users",
		Options: DefaultCollectionOptions(),
	}

	got, err := mgr.CreateCollection(meta)
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if got == nil {
		t.Fatalf("got nil collection")
	}
	if got.Name != meta.Name {
		t.Fatalf("expected name %q, got %q", meta.Name, got.Name)
	}

	replayed, err := mgr.CreateCollection(meta)
	if err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	if replayed.Name != meta.Name {
		t.Fatalf("idempotent create name mismatch: %q", replayed.Name)
	}

	if err := mgr.DropCollection("users"); err != nil {
		t.Fatalf("drop collection: %v", err)
	}

	if err := mgr.DropCollection("users"); err != nil {
		t.Fatalf("drop collection idempotent: %v", err)
	}

	if err := mgr.DropCollection("users"); err != nil {
		t.Fatalf("drop collection third call: %v", err)
	}

	collections, err := mgr.ListCollections()
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	if len(collections) != 0 {
		t.Fatalf("expected empty catalog after drop, got %d entries", len(collections))
	}

	recreated, err := mgr.CreateCollection(meta)
	if err != nil {
		t.Fatalf("recreate collection: %v", err)
	}
	if recreated.Name != meta.Name {
		t.Fatalf("recreate collection name mismatch: %q", recreated.Name)
	}
}

func TestCreateCollectionRejectInvalidName(t *testing.T) {
	dbx, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer dbx.Close()

	mgr := NewCollectionManager(dbx)
	tests := []string{"", " users", "_sys", "a/b", string(make([]byte, 129))}
	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := mgr.CreateCollection(&CollectionMeta{
				Name:    name,
				Options: DefaultCollectionOptions(),
			})
			if err == nil {
				t.Fatalf("expected invalid collection name %q to fail", name)
			}
		})
	}
}

func TestCollectionOptions_Defaults(t *testing.T) {
	dbx, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer dbx.Close()

	mgr := NewCollectionManager(dbx)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "orders",
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}

	want := DefaultCollectionOptions()
	if meta.Options.IDMode != want.IDMode {
		t.Fatalf("IDMode default mismatch: got=%v want=%v", meta.Options.IDMode, want.IDMode)
	}
	if meta.Options.StorageMode != want.StorageMode {
		t.Fatalf("storage mode default mismatch: got=%v want=%v", meta.Options.StorageMode, want.StorageMode)
	}
	if !meta.Options.RejectMissingFields {
		t.Fatalf("expected RejectMissingFields default true")
	}
}

func TestListCollections_ReflectsCommittedState(t *testing.T) {
	dbx, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer dbx.Close()

	mgr := NewCollectionManager(dbx)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create users: %v", err)
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "logs"}); err != nil {
		t.Fatalf("create logs: %v", err)
	}

	collections, err := mgr.ListCollections()
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	names := make([]string, len(collections))
	for i := range collections {
		names[i] = collections[i].Name
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "logs" || names[1] != "users" {
		t.Fatalf("unexpected names: %#v", names)
	}

	if err := mgr.DropCollection("users"); err != nil {
		t.Fatalf("drop users: %v", err)
	}

	collections, err = mgr.ListCollections()
	if err != nil {
		t.Fatalf("list collections after drop: %v", err)
	}
	if len(collections) != 1 || collections[0].Name != "logs" {
		t.Fatalf("expected only logs after drop, got %#v", collections)
	}
}

func TestCreateCollectionFailsIfSchemaIncompatible(t *testing.T) {
	dbx, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer dbx.Close()

	mgr := NewCollectionManager(dbx)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode:                  idModeCallerProvided,
			StorageMode:             CollectionStorageModeOuterLeafInValueLog,
			RejectMissingFields:     true,
			AllowArrayValuesInIndex: false,
		},
	}); err != nil {
		t.Fatalf("create initial users collection: %v", err)
	}

	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode:                  idModeAuto,
			StorageMode:             CollectionStorageModeOuterLeafInValueLog,
			RejectMissingFields:     true,
			AllowArrayValuesInIndex: false,
		},
	}); err == nil {
		t.Fatalf("expected schema incompatible create to fail")
	}

	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			IDMode:                  idModeCallerProvided,
			StorageMode:             CollectionStorageModeInnerOnly,
			RejectMissingFields:     true,
			AllowArrayValuesInIndex: false,
		},
	}); err == nil {
		t.Fatalf("expected schema incompatible create to fail")
	}
}
