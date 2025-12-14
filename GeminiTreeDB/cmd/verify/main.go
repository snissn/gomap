package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/snissn/gomap/GeminiTreeDB/db"
)

var dir = flag.String("dir", "", "Database directory")

func main() {
	flag.Parse()
	if *dir == "" {
		fmt.Println("Please provide -dir")
		os.Exit(1)
	}

	opts := db.Options{Dir: *dir}
	d, err := db.Open(opts)
	if err != nil {
		fmt.Printf("Failed to open DB: %v\n", err)
		os.Exit(1)
	}
	defer d.Close()

	it, err := d.Iterator(nil, nil)
	if err != nil {
		fmt.Printf("Failed to create iterator: %v\n", err)
		os.Exit(1)
	}
	defer it.Close()

	count := 0
	for ; it.Valid(); it.Next() {
		_ = it.Key()
		_ = it.Value()
		count++
	}

	if it.Error() != nil {
		fmt.Printf("Iterator error: %v\n", it.Error())
		os.Exit(1)
	}

	fmt.Printf("Verification successful. Items: %d\n", count)
}
