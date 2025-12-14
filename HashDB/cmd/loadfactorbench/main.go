package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/snissn/gomap/HashDB"
)

func main() {
	loadFactors := []float64{0.25, 0.5, 0.65, 0.75, 0.85, 0.95}
	opsPerRun := 500_000
	writeRatio := 0.2 // 20% writes, 80% reads

	fmt.Printf("Load Factor Bench: Ops/Run=%d, WriteRatio=%.2f\n", opsPerRun, writeRatio)
	fmt.Println("Load |   RPS     | Avg Latency | Capacity | Keys")
	fmt.Println("------------------------------------------------")

	for _, lf := range loadFactors {
		rps, avgLat, capacity, keys := runLoadBench(lf, opsPerRun, writeRatio)
		fmt.Printf("%4.2f | %9.0f | %11v | %8d | %8d\n",
			lf, rps, avgLat, capacity, keys)
	}
}

func runLoadBench(loadFactor float64, ops int, writeRatio float64) (float64, time.Duration, uint64, int) {
	dir, _ := os.MkdirTemp("", "loadfactorbench")
	defer os.RemoveAll(dir)

	var db hashdb.DB
	if err := db.Open(dir); err != nil {
		panic(err)
	}

	// Disable resize so we can hold a fixed load factor.
	db.SetResizeThreshold(100)
	// Disable compression to focus on index/slab access cost.
	db.SetCompression(false)

	stats := db.Stats()
	capacity := stats.Capacity
	if capacity == 0 {
		panic("hashmap capacity is zero")
	}

	targetKeys := int(float64(capacity) * loadFactor)
	if targetKeys < 1 {
		targetKeys = 1
	}

	// Preload unique keys up to the desired load factor.
	for i := 0; i < targetKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte("value")
		if err := db.Put(key, val); err != nil {
			panic(err)
		}
	}

	r := rand.New(rand.NewSource(1))

	start := time.Now()
	writeEvery := int(1.0 / writeRatio)
	if writeEvery <= 0 {
		writeEvery = ops + 1 // effectively no writes
	}

	for i := 0; i < ops; i++ {
		idx := r.Intn(targetKeys)
		key := []byte(fmt.Sprintf("key-%d", idx))

		if i%writeEvery == 0 {
			if err := db.Put(key, []byte("value")); err != nil {
				panic(err)
			}
		} else {
			if _, err := db.Get(key); err != nil {
				panic(err)
			}
		}
	}

	duration := time.Since(start)
	rps := float64(ops) / duration.Seconds()
	avgLat := duration / time.Duration(ops)

	return rps, avgLat, capacity, targetKeys
}
