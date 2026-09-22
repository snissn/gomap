package db

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
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

func TestUpdatePreservesEmptyValuePresence(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	key := []byte("empty")
	if err := d.Set(key, []byte{}); err != nil {
		t.Fatalf("Set empty: %v", err)
	}
	if err := d.Update(key, func(old []byte) (UpdateResult, error) {
		if old == nil {
			t.Fatalf("old = nil, want non-nil empty slice for present empty value")
		}
		if len(old) != 0 {
			t.Fatalf("old len=%d, want 0", len(old))
		}
		return SetUpdate([]byte("present-empty")), nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, err := d.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("present-empty")) {
		t.Fatalf("value got=%q want present-empty", got)
	}
}

func TestUpdateCallbackCanReenterSameKeyWithoutDeadlock(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	key := []byte("reentrant")
	if err := d.Set(key, []byte("seed")); err != nil {
		t.Fatalf("Set seed: %v", err)
	}

	done := make(chan error, 1)
	calls := 0
	go func() {
		done <- d.Update(key, func(old []byte) (UpdateResult, error) {
			calls++
			switch string(old) {
			case "seed":
				if err := d.Update(key, func(innerOld []byte) (UpdateResult, error) {
					if !bytes.Equal(innerOld, []byte("seed")) {
						return UpdateResult{}, fmt.Errorf("inner old = %q, want seed", innerOld)
					}
					return SetUpdate([]byte("inner")), nil
				}); err != nil {
					return UpdateResult{}, err
				}
				return NoopUpdate(), nil
			case "inner":
				return SetUpdate([]byte("outer")), nil
			default:
				return UpdateResult{}, fmt.Errorf("outer old = %q, want seed or inner", old)
			}
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Update deadlocked during reentrant same-key callback")
	}

	if calls != 2 {
		t.Fatalf("outer callback calls=%d want 2", calls)
	}
	got, err := d.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("outer")) {
		t.Fatalf("value got=%q want outer", got)
	}
}
