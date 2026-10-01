package mvcc_test

import (
	"fmt"
	"os"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
)

func ExampleStore_IterateVersions() {
	dir, err := os.MkdirTemp("", "mvcc-exact-key-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := treedb.Open(treedb.Options{Dir: dir, Durability: treedb.DurabilityWALOffRelaxed, DisableSideStores: true, BackgroundCheckpointInterval: -1})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	store := mvcc.New(db)
	for _, ts := range []uint64{10, 20} {
		if err := store.CommitAt(ts, []mvcc.Mutation{{Key: []byte("posting"), Value: []byte(fmt.Sprint(ts))}}, mvcc.CommitRelaxed); err != nil {
			panic(err)
		}
	}
	it, err := store.IterateVersions(mvcc.VersionIteratorOptions{ExactKey: []byte("posting"), ReadTimestamp: 25})
	if err != nil {
		panic(err)
	}
	defer it.Close()
	for it.Valid() {
		entry := it.Entry() // Owned bytes remain valid after advancing or closing.
		fmt.Printf("%s@%d=%s\n", entry.Key, entry.Timestamp, entry.Value)
		it.Next()
	}
	if err := it.Error(); err != nil {
		panic(err)
	}
	// Output:
	// posting@20=20
	// posting@10=10
}
