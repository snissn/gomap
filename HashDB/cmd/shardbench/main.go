package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/snissn/gomap/HashDB"
)

func main() {
	shardCounts := []int{1, 4, 8, 12, 16, 32, 64, 128, 256}
	concurrency := 200
	opsPerClient := 5000 // Total 1M ops

	fmt.Printf("Shard Bench: Concurrency=%d, Ops/Client=%d\n", concurrency, opsPerClient)
	fmt.Println("Shards | RPS       | Avg Latency")
	fmt.Println("--------------------------------")

	for _, shards := range shardCounts {
		rps, avgLat := runBench(shards, concurrency, opsPerClient)
		fmt.Printf("%6d | %9.0f | %10v\n", shards, rps, avgLat)
	}
}

func runBench(shards, concurrency, ops int) (float64, time.Duration) {
	dir, _ := os.MkdirTemp("", "shardbench")
	defer os.RemoveAll(dir)

	var store hashdb.HashDB
	if err := store.NewWithShards(dir, shards); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Use simple local random to avoid global lock contention in math/rand
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for j := 0; j < ops; j++ {
				k := []byte(fmt.Sprintf("key-%d", r.Intn(10000)))
				v := []byte("val")
				if j%5 == 0 { // 20% write
					if err := store.Put(k, v); err != nil {
						panic(err)
					}
				} else {
					if _, err := store.Get(k); err != nil {
						panic(err)
					}
				}
			}
		}()
	}
	wg.Wait()

	duration := time.Since(start)
	totalOps := float64(concurrency * ops)
	rps := totalOps / duration.Seconds()
	avgLat := duration / time.Duration(totalOps)

	return rps, avgLat
}
