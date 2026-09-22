package db_test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// TestManySmallCommits simulates the 'manytx' pattern: many small batches.
func TestManySmallCommitsFragmentation(t *testing.T) {
	dir := "./test-manytx-fragmentation"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)

	opts := treedb.Options{
		Dir:        dir,
		ChunkSize:  64 * 1024 * 1024,
		KeepRecent: 1,
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	const keysPerBatch = 5
	const batches = 1000 // 5000 keys total, but in many commits.

	fmt.Printf("Running %d small commits...\n", batches)
	rng := rand.New(rand.NewSource(42))
	for b := 0; b < batches; b++ {
		batch := db.NewBatch()
		for i := 0; i < keysPerBatch; i++ {
			key := make([]byte, 32)
			rng.Read(key)
			batch.Set(key, []byte("val"))
		}
		if err := batch.WriteSync(); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("Closing DB...")
	db.Close()

	fmt.Println("--- Fragmentation Report after Many Small Commits ---")
	treedb2, _ := treedb.Open(opts)
	rep, _ := treedb2.FragmentationReport()
	for k, v := range rep {
		fmt.Printf("  %s: %s\n", k, v)
	}
	treedb2.Close()
}
