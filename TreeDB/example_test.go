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

func ExampleOpen_backendMode() {
	dir, err := os.MkdirTemp("", "treedb-backend-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	opts := treedb.Options{Dir: dir, ChunkSize: 64 * 1024, Mode: treedb.ModeBackend}
	db, err := treedb.Open(opts)
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
