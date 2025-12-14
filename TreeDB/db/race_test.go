package db

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentReadsWrites(t *testing.T) {
	opts := Options{Dir: t.TempDir()}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer d.Close()

	var wg sync.WaitGroup
	workers := 4
	duration := 2 * time.Second
	keyRange := 1000
	valSize := 100

	done := make(chan struct{})
	go func() {
		time.Sleep(duration)
		close(done)
	}()

	var errors atomic.Int64

	for i := 0; i < workers; i++ {
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

				op := r.Intn(100)
				k := r.Intn(keyRange)
				key := []byte(fmt.Sprintf("key-%09d", k))

				var err error
				if op < 50 {
					// Set
					val := make([]byte, valSize)
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
					t.Logf("Worker %d error: %v", id, err)
					errors.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	if errors.Load() > 0 {
		t.Fatalf("Encountered %d errors during concurrent stress test", errors.Load())
	}
}
