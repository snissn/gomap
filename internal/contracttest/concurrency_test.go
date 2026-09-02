package contracttest

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func TestContract_ConcurrentUsage(t *testing.T) {
	cases := []struct {
		name     string
		engine   string
		writers  int
		readers  int
		iters    int
		keySpace int
	}{
		{name: "hashdb-sharded", engine: "hashdb-sharded", writers: 4, readers: 4, iters: 2000, keySpace: 64},
		{name: "treedb-cached", engine: "treedb-cached", writers: 1, readers: 4, iters: 500, keySpace: 32},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()

			db, err := openEngine(tc.engine, dir)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })

			var errs atomic.Int64
			var wg sync.WaitGroup

			for w := 0; w < tc.writers; w++ {
				wg.Add(1)
				go func(writerID int) {
					defer wg.Done()
					for i := 0; i < tc.iters; i++ {
						key := []byte(fmt.Sprintf("k-%d", (i+writerID*tc.iters)%tc.keySpace))
						val := []byte(strconv.Itoa(i))
						if err := db.Put(key, val); err != nil {
							errs.Add(1)
							return
						}
						if i%250 == 0 {
							if err := db.Delete(key); err != nil {
								errs.Add(1)
								return
							}
						}
					}
				}(w)
			}

			for r := 0; r < tc.readers; r++ {
				wg.Add(1)
				go func(readerID int) {
					defer wg.Done()
					for i := 0; i < tc.iters; i++ {
						key := []byte(fmt.Sprintf("k-%d", (i+readerID*tc.iters)%tc.keySpace))
						_, err := db.Get(key)
						if err != nil {
							errs.Add(1)
							return
						}
					}
				}(r)
			}

			wg.Wait()
			if errs.Load() != 0 {
				t.Fatalf("encountered %d errors during concurrent usage", errs.Load())
			}
		})
	}
}
