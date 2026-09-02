package main

import (
	"fmt"
	"log"
	"math"
	"os"

	treedb "github.com/snissn/gomap/TreeDB"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: db_histogram <db_dir>")
	}
	dir := os.Args[1]

	opts := treedb.Options{
		Dir:        dir,
		ReadOnly:   true,
		KeepRecent: 1,
	}

	db, err := treedb.Open(opts)
	if err != nil {
		log.Fatalf("Open: %v", err)
	}
	defer db.Close()

	it, err := db.Iterator(nil, nil)
	if err != nil {
		log.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	var (
		keySizes  = make(map[int]int)
		valSizes  = make(map[int]int)
		totalKeys int
	)

	// Since we are using public API, we can't easily distinguish inline vs pointer
	// unless we check if value came from the value log. But public API hides this.
	// However, we can infer from size if we know the threshold.
	// OR we can use UnsafeValue? No, public API.

	// Actually, for deep analysis, we should use internal/db if possible, or just analyze sizes.
	// If we want "inline vs pointer", we can't see it from public API easily.
	// But `treemap` (internal tool) could.
	// I'll settle for sizes for now.

	for it.Valid() {
		k := it.Key()
		v := it.Value()

		kLen := len(k)
		vLen := len(v)

		bucket := func(n int) int {
			if n == 0 {
				return 0
			}
			return int(math.Pow(2, math.Ceil(math.Log2(float64(n)))))
		}

		keySizes[bucket(kLen)]++
		valSizes[bucket(vLen)]++

		totalKeys++
		it.Next()
	}

	if err := it.Error(); err != nil {
		log.Fatalf("Iterator failed: %v", err)
	}

	fmt.Printf("Total Keys: %d\n", totalKeys)
	fmt.Println("Key Size Histogram (Upper Bound Power of 2):")
	printHist(keySizes)
	fmt.Println("Value Size Histogram (Upper Bound Power of 2):")
	printHist(valSizes)
}

func printHist(m map[int]int) {
	for i := 0; i <= 65536*1024; i = nextPow2(i) {
		if count := m[i]; count > 0 {
			fmt.Printf("%8d: %d\n", i, count)
		}
		if i == 0 && m[0] > 0 {
			// handled above
		}
	}
}

func nextPow2(n int) int {
	if n == 0 {
		return 1
	}
	return n * 2
}
