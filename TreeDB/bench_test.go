package treedb

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func BenchmarkReadUnderWriteCached(b *testing.B) {
	writerCounts := []int{0, 1, 4}
	val := make([]byte, 128)
	keys := make([][]byte, 4096)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}

	for _, writers := range writerCounts {
		b.Run(fmt.Sprintf("W=%d", writers), func(b *testing.B) {
			tmpDir, err := os.MkdirTemp("", "treedb-bench-read-write-cached-*")
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
