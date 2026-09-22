package hashdb_test

import (
	"fmt"
	"os"

	hashdb "github.com/snissn/gomap/HashDB"
)

func ExampleOpen() {
	dir, err := os.MkdirTemp("", "hashdb-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	db, err := hashdb.Open(dir)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("k"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))

	// Output: v
}
