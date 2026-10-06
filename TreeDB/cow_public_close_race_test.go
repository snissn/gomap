package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/caching"
)

func TestCOWPublicReadOwnerCaptureRacesClose(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
			opts.ValueLog.Compression = ValueLogCompressionOff
			opts.ValueLog.CompressionAutotune = AutotuneOptions{Mode: AutotuneOff}
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			value := bytes.Repeat([]byte("pinned"), 256)
			if err := database.Set([]byte("key"), value); err != nil {
				t.Fatal(err)
			}
			_, revision, err := database.GetVersioned([]byte("key"))
			if err != nil || revision == 0 {
				t.Fatalf("seed revision=%d err=%v", revision, err)
			}
			check := func(err error) {
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Errorf("read versus Close: %v", err)
				}
			}
			checkValue := func(v []byte, err error) {
				check(err)
				if err == nil && !bytes.Equal(v, value) {
					t.Errorf("successful admitted read changed value: %d bytes", len(v))
				}
			}
			checkVersioned := func(v []byte, r EntryRevision, err error) {
				checkValue(v, err)
				if err == nil && r != revision {
					t.Errorf("successful admitted read revision=%d want=%d", r, revision)
				}
			}
			checkSeek := func(key, v []byte, found bool, err error) {
				checkValue(v, err)
				if err == nil && (!found || string(key) != "key") {
					t.Errorf("successful Seek key=%q found=%t", key, found)
				}
			}
			operations := []func(){
				func() { checkValue(database.Get([]byte("key"))) },
				func() { checkVersioned(database.GetVersioned([]byte("key"))) },
				func() { checkValue(database.GetAppend([]byte("key"), nil)) },
				func() { checkVersioned(database.GetVersionedAppend([]byte("key"), nil)) },
				func() {
					found, err := database.Has([]byte("key"))
					check(err)
					if err == nil && !found {
						t.Error("successful Has lost seed")
					}
				},
				func() {
					found, err := database.HasMany([][]byte{[]byte("key")})
					check(err)
					if err == nil && (len(found) != 1 || !found[0]) {
						t.Error("successful HasMany lost seed")
					}
				},
				func() {
					found, err := database.HasPrefixes([][]byte{[]byte("k")})
					check(err)
					if err == nil && (len(found) != 1 || !found[0]) {
						t.Error("successful HasPrefixes lost seed")
					}
				},
				func() {
					values, err := database.GetMany([][]byte{[]byte("key")})
					check(err)
					if err == nil && (len(values) != 1 || !bytes.Equal(values[0], value)) {
						t.Error("successful GetMany lost seed")
					}
				},
				func() {
					check(database.GetManyView([][]byte{[]byte("key")}, func(_ int, _ []byte, v []byte, found bool) error {
						if !found || !bytes.Equal(v, value) {
							return fmt.Errorf("admitted view changed")
						}
						return nil
					}))
				},
				func() { checkSeek(database.SeekGE([]byte("key"), nil)) },
				func() { checkSeek(database.SeekGEVersionRange([]byte("key"), nil)) },
				func() {
					it, err := database.Iterator(nil, nil)
					check(err)
					if it != nil {
						key, v := it.KeyCopy(nil), it.ValueCopy(nil)
						readErr := it.Error()
						check(readErr)
						if readErr == nil && (string(key) != "key" || !bytes.Equal(v, value)) {
							t.Error("successful iterator read lost seed")
						}
						it.Next()
						check(it.Error())
						check(it.Close())
					}
				},
				func() {
					_, err := database.ReverseIterator(nil, nil)
					if err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, caching.ErrCOWUnsupported) {
						t.Errorf("reverse refusal: %v", err)
					}
				},
				func() {
					if snap := database.AcquireSnapshot(); snap != nil {
						checkValue(snap.Get([]byte("key")))
						check(snap.Close())
					}
				},
				func() { _, _ = database.GetManyParallelPlan(64); _ = database.Stats() },
			}
			var wg sync.WaitGroup
			started := make(chan struct{}, len(operations))
			proceed := make(chan struct{})
			for _, operation := range operations {
				wg.Add(1)
				go func(operation func()) {
					defer wg.Done()
					operation()
					started <- struct{}{}
					<-proceed
					for i := 0; i < 64; i++ {
						operation()
					}
				}(operation)
			}
			for range operations {
				<-started
			}
			close(proceed)
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			wg.Wait()
			if _, err := database.Get([]byte("key")); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed public Get: %v", err)
			}
			if snap := database.AcquireSnapshot(); snap != nil {
				_ = snap.Close()
				t.Fatal("closed public snapshot acquired")
			}
		})
	}
}

func TestCOWPublicViewCallbackCanCloseDatabase(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.Set([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			cache := database.cached
			done := make(chan error, 1)
			go func() {
				done <- database.GetManyView([][]byte{[]byte("key")}, func(_ int, _ []byte, value []byte, found bool) error {
					if !found || string(value) != "value" {
						return fmt.Errorf("missing admitted callback value")
					}
					return database.Close()
				})
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("callback Close blocked by public read lock")
			}
			if s := cache.COWMemoryStats(); s.TotalBytes != 0 || s.Views != 0 || s.ExternalLeases != 0 {
				t.Fatalf("callback owner did not drain: %+v", s)
			}
		})
	}
}
