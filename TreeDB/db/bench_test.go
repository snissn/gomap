package db

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// BenchmarkStress performs mixed Read/Write operations.
func BenchmarkStress(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := Options{
		Dir:        tmpDir,
		KeepRecent: 10000,
	}
	d, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer d.Close()

	keyRange := 10000
	valSize := 100

	keys := make([][]byte, keyRange)
	for i := 0; i < keyRange; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		valBuf := make([]byte, valSize)

		for pb.Next() {
			op := r.Intn(100)
			k := r.Intn(keyRange)
			key := keys[k]

			if op < 50 { // 50% Write
				r.Read(valBuf)
				if err := d.Set(key, valBuf); err != nil {
					b.Errorf("Set failed: %v", err)
				}
			} else if op < 90 { // 40% Read
				if _, err := d.Get(key); err != nil {
				}
			} else { // 10% Delete
				if err := d.Delete(key); err != nil {
					b.Errorf("Delete failed: %v", err)
				}
			}
		}
	})
}

// BenchmarkGet performs 100% Read operations on a pre-filled DB.
func BenchmarkGet(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-get-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := Open(Options{Dir: tmpDir})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	// Pre-fill 10k items
	count := 10000
	val := make([]byte, 100)

	keys := make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}

	for i := 0; i < count; i += 1000 {
		batch := d.NewBatch()
		for j := 0; j < 1000 && i+j < count; j++ {
			if err := batch.Set(keys[i+j], val); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			key := keys[r.Intn(count)]
			if _, err := d.Get(key); err != nil {
				// b.Errorf("Get failed: %v", err)
			}
		}
	})
}

// BenchmarkScan iterates over all keys.
func BenchmarkScan(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-scan-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := Open(Options{Dir: tmpDir})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	// Pre-fill 10k items
	count := 10000
	val := make([]byte, 100)

	keys := make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}

	for i := 0; i < count; i += 1000 {
		batch := d.NewBatch()
		for j := 0; j < 1000 && i+j < count; j++ {
			if err := batch.Set(keys[i+j], val); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := d.Iterator(nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		items := 0
		for ; iter.Valid(); iter.Next() {
			items++
		}
		iter.Close()
		if items != count {
			b.Fatalf("Expected %d items, got %d", count, items)
		}
	}
}

// BenchmarkBatch performs batched writes (1000 items/batch).
func BenchmarkBatch(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-batch-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	opts := Options{
		Dir:        tmpDir,
		KeepRecent: 10000,
	}
	d, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer d.Close()

	batchSize := 1000
	valSize := 100
	totalKeys := 100000

	keys := make([][]byte, totalKeys)
	for i := 0; i < totalKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}

	valBuf := make([]byte, valSize)
	rand.Read(valBuf)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch := d.NewBatch()
		startIdx := (i * batchSize) % (totalKeys - batchSize)
		for j := 0; j < batchSize; j++ {
			if j%10 == 0 {
				batch.Delete(keys[startIdx+j])
			} else {
				batch.Set(keys[startIdx+j], valBuf)
			}
		}
		if err := batch.WriteSync(); err != nil {
			b.Fatalf("Batch write failed: %v", err)
		}
	}
}

func BenchmarkWriteParallel(b *testing.B) {
	workers := []int{1, 2, 4, 8}
	val := make([]byte, 128)
	keys := make([][]byte, 1024)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}

	for _, n := range workers {
		b.Run(fmt.Sprintf("G=%d", n), func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", "treedb-bench-write-*")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tmpDir)

			d, err := Open(Options{Dir: tmpDir})
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

func BenchmarkReadUnderWrite(b *testing.B) {
	writerCounts := []int{0, 1, 4}
	val := make([]byte, 128)
	keys := make([][]byte, 4096)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}

	for _, writers := range writerCounts {
		b.Run(fmt.Sprintf("W=%d", writers), func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", "treedb-bench-read-write-*")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tmpDir)

			d, err := Open(Options{Dir: tmpDir})
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

// BenchmarkLargeVal writes larger values (4KB).
func BenchmarkLargeVal(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "treedb-bench-large-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := Open(Options{Dir: tmpDir})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	valueLogDir := filepath.Join(tmpDir, "value_vlog")
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		b.Fatalf("mkdir value log: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		b.Fatalf("encode value-log file id: %v", err)
	}
	valueLogPath := filepath.Join(valueLogDir, "value-l0-000001.log")
	valueWriter, err := valuelog.NewWriter(valueLogPath, fileID)
	if err != nil {
		b.Fatalf("new value-log writer: %v", err)
	}
	defer valueWriter.Close()
	if err := d.valueLogManager.RegisterSegment(valueLogPath, fileID); err != nil {
		b.Fatalf("register value-log segment: %v", err)
	}
	if err := d.valueLogManager.PromoteCurrentWritable(fileID); err != nil {
		b.Fatalf("promote value-log segment: %v", err)
	}

	var (
		valueLogMu sync.Mutex
		nextRID    uint64
	)
	valSize := 4096 // 4KB
	valBuf := make([]byte, valSize)
	rand.Read(valBuf)

	keys := make([][]byte, 10000)
	for i := 0; i < len(keys); i++ {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			key := keys[r.Intn(len(keys))]

			// Value-log writer append is intentionally serialized here; this
			// benchmark exercises the raw pointer write path without assuming
			// valuelog.Writer is goroutine-safe.
			valueLogMu.Lock()
			nextRID++
			ptr, err := valueWriter.Append(0, nil, nextRID, valBuf)
			valueLogMu.Unlock()
			if err != nil {
				b.Errorf("value-log append failed: %v", err)
				continue
			}

			guard := d.lockUpdateKey(key)
			wb := d.NewBatch().(*Batch)
			err = wb.SetPointer(key, ptr)
			if err == nil {
				err = wb.Write()
			}
			if closeErr := wb.Close(); err == nil {
				err = closeErr
			}
			guard.Unlock()
			if err != nil {
				b.Errorf("SetPointer failed: %v", err)
			}
		}
	})
}
