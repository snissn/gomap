package main

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/snissn/gomap"
)

// resizebench measures per-operation latency across one or more resizes on a single Hashmap.
// Run on different gomap versions (e.g. before vs after incremental rehash) to compare tails.
func main() {
	const ops = 200000

	dir, err := os.MkdirTemp("", "resizebench")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	var h gomap.Hashmap
	if err := h.New(dir); err != nil {
		panic(err)
	}

	// Use the default resize threshold (65%) to exercise the normal path.
	h.SetResizeThreshold(65)
	h.SetCompression(false)

	durs := make([]time.Duration, 0, ops)

	startTotal := time.Now()
	for i := 0; i < ops; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte("value")

		start := time.Now()
		if err := h.Add(key, val); err != nil {
			panic(err)
		}
		durs = append(durs, time.Since(start))
	}
	total := time.Since(startTotal)

	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })

	p := func(p float64) time.Duration {
		if len(durs) == 0 {
			return 0
		}
		idx := int(float64(len(durs)-1) * p)
		return durs[idx]
	}

	rps := float64(ops) / total.Seconds()

	fmt.Printf("Resize Bench: Ops=%d\n", ops)
	fmt.Printf("Total time: %s, RPS=%.0f\n", total, rps)
	fmt.Printf("Per-op latency: p50=%s, p95=%s, p99=%s, max=%s\n",
		p(0.50), p(0.95), p(0.99), durs[len(durs)-1])
}

