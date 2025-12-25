package treedb

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func benchMemtableShards() int {
	val := os.Getenv("TREEDB_BENCH_SHARDS")
	if val == "" {
		return 0
	}
	n, err := strconv.Atoi(val)
	if err != nil || n < 1 {
		return 0
	}
	return n
}

func BenchmarkReadUnderWriteCached(b *testing.B) {
	writerCounts := []int{0, 1, 4}
	val := make([]byte, 128)
	keys := make([][]byte, 4096)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}
	shards := benchMemtableShards()

	for _, writers := range writerCounts {
		b.Run(fmt.Sprintf("W=%d", writers), func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", "treedb-bench-read-write-cached-*")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tmpDir)

			opts := Options{Dir: tmpDir}
			if shards > 0 {
				opts.MemtableShards = shards
			}
			d, err := Open(opts)
			if err != nil {
				b.Fatalf("Failed to open DB: %v", err)
			}
			defer d.Close()

			for i := 0; i < len(keys); i += 512 {
				batch := d.NewBatch()
				for j := 0; j < 512 && i+j < len(keys); j++ {
					if err := batch.Set(keys[i+j], val); err != nil {
						b.Fatal(err)
					}
				}
				if err := batch.WriteSync(); err != nil {
					b.Fatal(err)
				}
			}

			stop := make(chan struct{})
			var wg sync.WaitGroup
			errCh := make(chan error, writers)
			for i := 0; i < writers; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
					for {
						select {
						case <-stop:
							return
						default:
						}
						key := keys[r.Intn(len(keys))]
						if err := d.Set(key, val); err != nil {
							select {
							case errCh <- err:
							default:
							}
							return
						}
					}
				}(i)
			}

			if writers > 0 {
				time.Sleep(10 * time.Millisecond)
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := keys[i%len(keys)]
				if _, err := d.Get(key); err != nil {
					b.Fatalf("Get failed: %v", err)
				}
			}

			b.StopTimer()
			close(stop)
			wg.Wait()
			for {
				select {
				case err := <-errCh:
					if err != nil {
						b.Fatalf("writer failed: %v", err)
					}
				default:
					return
				}
			}
		})
	}
}

func BenchmarkWriteParallelCached(b *testing.B) {
	workers := []int{1, 2, 4, 8}
	val := make([]byte, 128)
	keys := make([][]byte, 1024)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}
	shards := benchMemtableShards()

	for _, n := range workers {
		b.Run(fmt.Sprintf("G=%d", n), func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", "treedb-bench-write-cached-*")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tmpDir)

			opts := Options{Dir: tmpDir}
			if shards > 0 {
				opts.MemtableShards = shards
			}
			d, err := Open(opts)
			if err != nil {
				b.Fatalf("Failed to open DB: %v", err)
			}
			defer d.Close()

			var counter uint64
			b.ResetTimer()

			var wg sync.WaitGroup
			var failed atomic.Bool
			errCh := make(chan error, 1)
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func(id int) {
					defer wg.Done()
					for {
						if failed.Load() {
							return
						}
						idx := int(atomic.AddUint64(&counter, 1)) - 1
						if idx >= b.N {
							return
						}
						key := keys[idx%len(keys)]
						if err := d.Set(key, val); err != nil {
							if failed.CompareAndSwap(false, true) {
								errCh <- err
							}
							return
						}
					}
				}(i)
			}
			wg.Wait()
			if failed.Load() {
				b.Fatalf("Set failed: %v", <-errCh)
			}
		})
	}
}

func BenchmarkWriteParallelCachedRotationStress(b *testing.B) {
	workers := []int{1, 2, 4, 8}
	val := make([]byte, 128)
	keys := make([][]byte, 1024)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}
	shards := benchMemtableShards()

	const flushThreshold int64 = 1 << 20 // 1MiB

	for _, n := range workers {
		b.Run(fmt.Sprintf("G=%d", n), func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", "treedb-bench-write-cached-rotate-*")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tmpDir)

			opts := Options{
				Dir:                              tmpDir,
				FlushThreshold:                   flushThreshold,
				BackgroundCheckpointInterval:     -1,
				BackgroundCheckpointIdleDuration: -1,
				MaxWALBytes:                      -1,
				BackgroundIndexVacuumInterval:    -1,
			}
			if shards > 0 {
				opts.MemtableShards = shards
			}
			d, err := Open(opts)
			if err != nil {
				b.Fatalf("Failed to open DB: %v", err)
			}
			defer d.Close()

			var counter uint64
			b.ResetTimer()

			var wg sync.WaitGroup
			var failed atomic.Bool
			errCh := make(chan error, 1)
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func(id int) {
					defer wg.Done()
					for {
						if failed.Load() {
							return
						}
						idx := int(atomic.AddUint64(&counter, 1)) - 1
						if idx >= b.N {
							return
						}
						key := keys[idx%len(keys)]
						if err := d.Set(key, val); err != nil {
							if failed.CompareAndSwap(false, true) {
								errCh <- err
							}
							return
						}
					}
				}(i)
			}
			wg.Wait()
			if failed.Load() {
				b.Fatalf("Set failed: %v", <-errCh)
			}
		})
	}
}

func BenchmarkMixedLatencyCached(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-mixed-cached-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := Options{Dir: tmpDir}
	if shards := benchMemtableShards(); shards > 0 {
		opts.MemtableShards = shards
	}
	d, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer d.Close()

	val := make([]byte, 128)
	keys := make([][]byte, 8192)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}
	for i := 0; i < len(keys); i += 512 {
		batch := d.NewBatch()
		for j := 0; j < 512 && i+j < len(keys); j++ {
			if err := batch.Set(keys[i+j], val); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			b.Fatal(err)
		}
	}

	r := rand.New(rand.NewSource(1))
	samples := make([]int64, 0, 2000)
	const sampleMask = 0x7F

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		key := keys[i%len(keys)]
		if r.Intn(10) < 7 {
			if _, err := d.Get(key); err != nil {
				b.Fatalf("Get failed: %v", err)
			}
		} else {
			if err := d.Set(key, val); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
		}
		if i&sampleMask == 0 {
			samples = append(samples, time.Since(start).Nanoseconds())
		}
	}
	b.StopTimer()

	if len(samples) > 0 {
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		p50 := samples[len(samples)/2]
		p95 := samples[(len(samples)*95)/100]
		p99 := samples[(len(samples)*99)/100]
		b.ReportMetric(float64(p50), "p50_ns")
		b.ReportMetric(float64(p95), "p95_ns")
		b.ReportMetric(float64(p99), "p99_ns")
	}
}
