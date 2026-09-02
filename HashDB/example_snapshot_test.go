package hashdb

import (
	"bytes"
	"fmt"
	"os"
)

func ExampleDB_Export() {
	srcDir, err := os.MkdirTemp("", "hashdb-src-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(srcDir)

	src, err := OpenSingle(srcDir)
	if err != nil {
		panic(err)
	}
	defer src.Close()

	if err := src.PutSync([]byte("a"), []byte("1")); err != nil {
		panic(err)
	}

	var buf bytes.Buffer
	if err := src.Export(&buf); err != nil {
		panic(err)
	}

	dstDir, err := os.MkdirTemp("", "hashdb-dst-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dstDir)

	dst, err := OpenSingle(dstDir)
	if err != nil {
		panic(err)
	}
	defer dst.Close()

	if err := dst.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		panic(err)
	}
	v, err := dst.Get([]byte("a"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(v))
	// Output: 1
}

func ExampleHashDB_Export() {
	srcDir, err := os.MkdirTemp("", "hashdb-sharded-src-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(srcDir)

	src, err := OpenWithShards(srcDir, 8)
	if err != nil {
		panic(err)
	}
	defer src.Close()

	if err := src.PutSync([]byte("a"), []byte("1")); err != nil {
		panic(err)
	}

	var buf bytes.Buffer
	if err := src.Export(&buf); err != nil {
		panic(err)
	}

	dstDir, err := os.MkdirTemp("", "hashdb-sharded-dst-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dstDir)

	dst, err := OpenWithShards(dstDir, 8)
	if err != nil {
		panic(err)
	}
	defer dst.Close()

	if err := dst.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		panic(err)
	}
	v, err := dst.Get([]byte("a"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(v))
	// Output: 1
}
