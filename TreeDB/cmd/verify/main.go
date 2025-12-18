package main

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/snissn/gomap/TreeDB/compaction"
	"github.com/snissn/gomap/TreeDB/db"
)

var dir = flag.String("dir", "", "Database directory")
var report = flag.Bool("report", false, "Print fragmentation report and stats")
var vacuumIndex = flag.Bool("vacuum-index", false, "Vacuum (rebuild) the user index before verification (improves scan locality; grows index.db)")
var compactDeadRatio = flag.Float64("compact-dead-ratio", 0, "If >0, compact slabs with dead ratio >= this threshold before verification")
var compactMinBytes = flag.Uint64("compact-min-bytes", 0, "Minimum slab total bytes to consider for compaction")
var compactMaxSlabs = flag.Int("compact-max-slabs", 1, "Maximum slabs to compact per run (0=unlimited)")
var compactMicroBatch = flag.Int("compact-microbatch", 256, "Compaction apply micro-batch size (keys per commit)")
var compactRotateBeforeWrite = flag.Bool("compact-rotate-before-write", false, "If set, rotate to a fresh active slab before copying live records")
var compactCopyBps = flag.Int64("compact-copy-bps", 0, "Compaction copy throttling (bytes/sec), 0=disabled")
var compactCopyBurst = flag.Int64("compact-copy-burst", 0, "Compaction copy throttling burst (bytes), 0=default")

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

	if *compactDeadRatio > 0 {
		c := compaction.New(d)
		if err := c.CompactCandidates(compaction.Options{
			DeadRatioThreshold: *compactDeadRatio,
			MinTotalBytes:      *compactMinBytes,
			MaxSlabs:           *compactMaxSlabs,
			MicroBatchSize:     *compactMicroBatch,
			RotateBeforeWrite:  *compactRotateBeforeWrite,
			CopyBytesPerSec:    *compactCopyBps,
			CopyBurstBytes:     *compactCopyBurst,
		}); err != nil {
			fmt.Printf("Compaction failed: %v\n", err)
			os.Exit(1)
		}
	}

	if *vacuumIndex {
		if *report {
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
			fmt.Println("Fragmentation (before vacuum):")
			for _, k := range rk {
				fmt.Printf("  %s=%s\n", k, rep[k])
			}
		}

		if err := d.CompactIndex(); err != nil {
			fmt.Printf("Index vacuum failed: %v\n", err)
			os.Exit(1)
		}
	}

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
