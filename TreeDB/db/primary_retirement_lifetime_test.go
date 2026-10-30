package db

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// Retirement must keep private ownership after publication unlocks: a later
// publisher may reuse the runtime's seal backing while an old reader is pinned.
func TestPrimaryRetirementPinnedReaderAndConcurrentPublication(t *testing.T) {
	for _, writers := range []int{1, 4} {
		t.Run(fmt.Sprintf("writers-%d", writers), func(t *testing.T) {
			opts := Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
			d, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if d != nil {
					_ = d.Close()
				}
			}()
			for i := 0; i < writers; i++ {
				if err := d.SetSync([]byte(fmt.Sprintf("key-%d", i)), []byte("before")); err != nil {
					t.Fatal(err)
				}
			}
			old := d.AcquireSnapshot()
			if old == nil {
				t.Fatal("missing pinned reader")
			}
			defer old.Close()
			const rounds = 8
			errs := make(chan error, writers)
			var wg sync.WaitGroup
			for i := 0; i < writers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					key := []byte(fmt.Sprintf("key-%d", i))
					for j := 0; j < rounds; j++ {
						if err := d.SetSync(key, []byte(fmt.Sprintf("after-%d", j))); err != nil {
							errs <- err
							return
						}
					}
				}(i)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
			for i := 0; i < writers; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				if got, err := old.Get(key); err != nil || !bytes.Equal(got, []byte("before")) {
					t.Fatalf("pinned reader key %q: %q, %v", key, got, err)
				}
				if got, err := d.Get(key); err != nil || !bytes.Equal(got, []byte("after-7")) {
					t.Fatalf("current key %q: %q, %v", key, got, err)
				}
			}
			// Retire the reader before closing storage, then verify published durability.
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d = nil
			d, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < writers; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				if got, err := d.Get(key); err != nil || !bytes.Equal(got, []byte("after-7")) {
					t.Fatalf("reopened key %q: %q, %v", key, got, err)
				}
			}
		})
	}
}
