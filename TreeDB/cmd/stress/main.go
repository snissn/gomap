package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap-gemini/TreeDB/db"
)

var (
	dir         = flag.String("dir", "", "Database directory (temp dir used if empty)")
	duration    = flag.Duration("duration", 10*time.Second, "Test duration")
	workers     = flag.Int("workers", 4, "Number of concurrent workers")
	keyRange    = flag.Int("keys", 10000, "Key range size")
	valSize     = flag.Int("valsize", 100, "Value size (bytes)")
	opsCount    = flag.Int64("ops", 0, "Total operations limit (0 = unlimited, time based)")
)

func main() {
	flag.Parse()

	dbDir := *dir
	if dbDir == "" {
		tmpDir, err := os.MkdirTemp("", "treedb-stress-*")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(tmpDir)
		dbDir = tmpDir
	}
	
	fmt.Printf("Stress Test Config:\n Dir: %s\n Duration: %v\n Workers: %d\n Keys: %d\n", dbDir, *duration, *workers, *keyRange)

	opts := db.Options{Dir: dbDir}
	d, err := db.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("Failed to open DB: %v", err))
	}
	defer d.Close()

	var wg sync.WaitGroup
	var totalOps atomic.Int64
	var errors atomic.Int64
	
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
			
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			
			for {
				select {
				case <-done:
					return
				default:
				}
				
				if *opsCount > 0 && totalOps.Load() >= *opsCount {
					return
				}
				
				op := r.Intn(100)
				k := r.Intn(*keyRange)
				key := []byte(fmt.Sprintf("key-%09d", k))
				
				var err error
				if op < 50 {
					// Set
					val := make([]byte, *valSize)
					r.Read(val)
					err = d.Set(key, val)
				} else if op < 90 {
					// Get
					_, err = d.Get(key)
				} else {
					// Delete
					err = d.Delete(key)
				}
				
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
	
	fmt.Printf("Completed in %v\n", elapsed)
	fmt.Printf("Total Ops: %d\n", ops)
	fmt.Printf("Errors: %d\n", errors.Load())
	fmt.Printf("Ops/sec: %.2f\n", float64(ops)/elapsed.Seconds())
	
	if errors.Load() > 0 {
		os.Exit(1)
	}
}
