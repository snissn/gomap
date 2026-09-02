package main

import (
	"fmt"
	"os"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: rebind_snapshot <copied-db-directory>")
		os.Exit(2)
	}
	if err := backenddb.RebindDurableRootSnapshotV1(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
