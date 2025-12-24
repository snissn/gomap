package db

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentWrites_NoLostUpdates(t *testing.T) {
	opts := Options{Dir: t.TempDir()}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	workers := 4
	perWorker := 50

	start := make(chan struct{})
	errCh := make(chan error, workers*perWorker)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWorker; i++ {
				key := []byte(fmt.Sprintf("k-%02d-%03d", id, i))
				val := []byte(fmt.Sprintf("v-%02d-%03d", id, i))
				if err := db.Set(key, val); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent write error: %v", err)
		}
	}

	for w := 0; w < workers; w++ {
		for i := 0; i < perWorker; i++ {
			key := []byte(fmt.Sprintf("k-%02d-%03d", w, i))
			want := fmt.Sprintf("v-%02d-%03d", w, i)
			got, err := db.Get(key)
			if err != nil {
				t.Fatalf("get %q: %v", key, err)
			}
			if string(got) != want {
				t.Fatalf("get %q: got %q, want %q", key, string(got), want)
			}
		}
	}
}
