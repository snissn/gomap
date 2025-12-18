package main

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/snissn/gomap/TreeDB/db"
)

var dir = flag.String("dir", "", "Database directory")
var report = flag.Bool("report", false, "Print fragmentation report and stats")

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

	if *report {
		stats := d.Stats()
		keys := make([]string, 0, len(stats))
		for k := range stats {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Println("Stats:")
		for _, k := range keys {
			fmt.Printf("  %s=%s\n", k, stats[k])
		}

		rep, err := d.FragmentationReport()
		if err != nil {
			fmt.Printf("FragmentationReport error: %v\n", err)
			os.Exit(1)
		}
		rk := make([]string, 0, len(rep))
		for k := range rep {
			rk = append(rk, k)
		}
		sort.Strings(rk)
		fmt.Println("Fragmentation:")
		for _, k := range rk {
			fmt.Printf("  %s=%s\n", k, rep[k])
		}
	}

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
