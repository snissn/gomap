package db

import (
	"strconv"
	"sync"
	"testing"
)

func TestUpdateConcurrentCounterNoLostUpdates(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const (
		workers    = 32
		increments = 10
	)
	key := []byte("counter")
	start := make(chan struct{})
	errs := make(chan error, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < increments; j++ {
				if err := d.Update(key, func(old []byte) (UpdateResult, error) {
					n := 0
					if old != nil {
						parsed, err := strconv.Atoi(string(old))
						if err != nil {
							return UpdateResult{}, err
						}
						n = parsed
					}
					return SetUpdate([]byte(strconv.Itoa(n + 1))), nil
				}); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
	}
	got, err := d.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	n, err := strconv.Atoi(string(got))
	if err != nil {
		t.Fatalf("parse counter %q: %v", got, err)
	}
	if want := workers * increments; n != want {
		t.Fatalf("counter=%d want=%d", n, want)
	}
}
