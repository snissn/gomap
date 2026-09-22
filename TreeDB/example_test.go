package treedb_test

import (
	"fmt"
	"os"

	treedb "github.com/snissn/gomap/TreeDB"
)

func ExampleOpen() {
	dir, err := os.MkdirTemp("", "treedb-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	db, err := treedb.Open(treedb.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.SetSync([]byte("k"), []byte("v")); err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("k"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))

	// Output: v
}

func ExampleBatch() {
	dir, err := os.MkdirTemp("", "treedb-batch-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	batch := db.NewBatch()
	if batch == nil {
		panic("batch creation failed")
	}

	batch.Set([]byte("key1"), []byte("value1"))
	batch.Set([]byte("key2"), []byte("value2"))
	batch.Delete([]byte("key1"))

	// WriteSync ensures durability.
	if err := batch.WriteSync(); err != nil {
		panic(err)
	}

	val, _ := db.Get([]byte("key2"))
	fmt.Println("key2:", string(val))
	val, _ = db.Get([]byte("key1"))
	fmt.Println("key1:", val)

	// Output:
	// key2: value2
	// key1: []
}

func ExampleIterator() {
	dir, err := os.MkdirTemp("", "treedb-iter-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Insert keys in random order
	db.SetSync([]byte("b"), []byte("2"))
	db.SetSync([]byte("a"), []byte("1"))
	db.SetSync([]byte("c"), []byte("3"))

	// Iterate over all keys
	it, err := db.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}
	defer it.Close()

	for ; it.Valid(); it.Next() {
		fmt.Printf("%s: %s\n", it.Key(), it.Value())
	}

	// Output:
	// a: 1
	// b: 2
	// c: 3
}
