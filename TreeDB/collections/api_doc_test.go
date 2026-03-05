package collections_test

import (
	"fmt"
	"os"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/db"
)

func ExampleCollection_autoIDsAndIndexLookup() {
	dir, err := os.MkdirTemp("", "treedb-collections-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	database, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		panic(err)
	}
	defer database.Close()

	manager := collections.NewCollectionManager(database)
	meta, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			IDMode: collections.IDModeAuto,
		},
	})
	if err != nil {
		panic(err)
	}
	if _, err := manager.CreateIndex(meta.Name, collections.IndexDefinition{
		Name:   "email_idx",
		Field:  "email",
		Unique: true,
	}); err != nil {
		panic(err)
	}

	coll, err := manager.OpenCollection(meta.Name)
	if err != nil {
		panic(err)
	}
	id, err := coll.Insert(nil, []byte(`{"email":"ada@example.com","city":"hnl"}`))
	if err != nil {
		panic(err)
	}

	matches, err := coll.FindByIndex("email_idx", "ada@example.com")
	if err != nil {
		panic(err)
	}
	indexes, err := coll.ListIndexes()
	if err != nil {
		panic(err)
	}
	stats, err := coll.Stats()
	if err != nil {
		panic(err)
	}

	fmt.Printf("id-bytes=%d matches=%d indexes=%d docs=%d\n", len(id), len(matches), len(indexes), stats.DocumentCount)

	// Output:
	// id-bytes=8 matches=1 indexes=1 docs=1
}

func ExampleCollection_checkConsistency() {
	dir, err := os.MkdirTemp("", "treedb-collections-consistency-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	database, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		panic(err)
	}
	defer database.Close()

	manager := collections.NewCollectionManager(database)
	meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		panic(err)
	}
	if _, err := manager.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		panic(err)
	}

	coll, err := manager.OpenCollection(meta.Name)
	if err != nil {
		panic(err)
	}
	if _, err := coll.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		panic(err)
	}

	report, err := coll.CheckConsistency()
	if err != nil {
		panic(err)
	}
	fmt.Printf("missing=%d orphan=%d\n", report.MissingIndexEntries, report.OrphanIndexEntries)

	// Output:
	// missing=0 orphan=0
}
