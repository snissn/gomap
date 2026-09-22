package main

import (
	"flag"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/db"
)

var (
	dir        = flag.String("dir", "", "Database directory (temp dir used if empty)")
	duration   = flag.Duration("duration", 10*time.Second, "Test duration")
	workers    = flag.Int("workers", 4, "Number of concurrent workers")
	keyRange   = flag.Int("keys", 10000, "Key range size")
	valSize    = flag.Int("valsize", 100, "Value size (bytes)")
	opsCount   = flag.Int64("ops", 0, "Total operations limit (0 = unlimited, time based)")
	keepRecent = flag.Int("keeprecent", 10000, "Keep recent versions (0 or 1 for aggressive reuse)")
	cpuprofile = flag.String("cpuprofile", "", "Write cpu profile to file")
	memprofile = flag.String("memprofile", "", "Write memory profile to file")
)

func main() {
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	dbDir := *dir
	if dbDir == "" {
		tmpDir, err := os.MkdirTemp("", "treedb-stress-*")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(tmpDir)
		dbDir = tmpDir
	}

	fmt.Printf("Stress Test Config:\n Dir: %s\n Duration: %v\n Workers: %d\n Keys: %d\n KeepRecent: %d\n", dbDir, *duration, *workers, *keyRange, *keepRecent)

	opts := db.Options{
		Dir:        dbDir,
		KeepRecent: uint64(*keepRecent),
	}
	d, err := db.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("Failed to open DB: %v", err))
	}
	defer d.Close()

	var wg sync.WaitGroup
	var totalOps atomic.Int64
	var errors atomic.Int64
	var logicalBytes atomic.Int64

	// Pre-allocate slice for latencies to avoid too much resizing overhead, though exact size is unknown.
	// Each worker will return its own slice.
	workerLatencies := make([][]time.Duration, *workers)

	start := time.Now()
	done := make(chan struct{})

	// Timer
	go func() {
		time.Sleep(*duration)
		close(done)
	}()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Local storage for this worker
			var latencies []time.Duration
			// Heuristic: estimate 10k ops/sec per worker?
			latencies = make([]time.Duration, 0, 100000)

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			for {
				select {
				case <-done:
					workerLatencies[id] = latencies
					return
				default:
				}

				if *opsCount > 0 && totalOps.Load() >= *opsCount {
					workerLatencies[id] = latencies
					return
				}

				op := r.Intn(100)
				k := r.Intn(*keyRange)
				key := []byte(fmt.Sprintf("key-%09d", k))

				opStart := time.Now()
				var err error
				if op < 50 {
					// Set
					val := make([]byte, *valSize)
					r.Read(val)
					err = d.Set(key, val)
					if err == nil {
						logicalBytes.Add(int64(len(key) + len(val)))
					}
				} else if op < 90 {
					// Get
					_, err = d.Get(key)
				} else {
					// Delete
					err = d.Delete(key)
				}

				latencies = append(latencies, time.Since(opStart))

				if err != nil {
					fmt.Printf("Worker %d error: %v\n", id, err)
					errors.Add(1)
				}
				totalOps.Add(1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)
	ops := totalOps.Load()

	// 1. Latency Analysis
	var allLatencies []time.Duration
	for _, l := range workerLatencies {
		allLatencies = append(allLatencies, l...)
	}
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })

	var p50, p95, p99 time.Duration
	if len(allLatencies) > 0 {
		p50 = allLatencies[len(allLatencies)*50/100]
		p95 = allLatencies[len(allLatencies)*95/100]
		p99 = allLatencies[len(allLatencies)*99/100]
	}

	// 2. Write Amplification Analysis
	var physicalBytes int64
	err = filepath.Walk(dbDir, func(_ string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			physicalBytes += info.Size()
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error calculating directory size: %v\n", err)
	}

	lBytes := logicalBytes.Load()
	writeAmp := 0.0
	if lBytes > 0 {
		writeAmp = float64(physicalBytes) / float64(lBytes)
	}

	fmt.Printf("\n--- Results ---\n")
	fmt.Printf("Elapsed: %v\n", elapsed)
	fmt.Printf("Total Ops: %d\n", ops)
	fmt.Printf("Errors: %d\n", errors.Load())
	fmt.Printf("Ops/sec: %.2f\n", float64(ops)/elapsed.Seconds())
	fmt.Printf("Latencies:\n  p50: %v\n  p95: %v\n  p99: %v\n", p50, p95, p99)
	fmt.Printf("Write Amplification:\n  Logical: %d bytes\n  Physical: %d bytes\n  Factor: %.2fx\n", lBytes, physicalBytes, writeAmp)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			panic(err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			panic(err)
		}
		f.Close()
	}

	if errors.Load() > 0 {
		os.Exit(1)
	}
}
