package treedb

import (
	"errors"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCOWCloseErrorNotificationCanReadClosedDatabase(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			var database *DB
			notifications := make(chan error, 1)
			opts := OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.FlushThreshold = 1 << 30
			opts.NotifyError = func(err error) {
				if database == nil || !strings.Contains(err.Error(), "final command WAL checkpoint during close") {
					return
				}
				// Notification ordering stays synchronous. Exclusive Close
				// refuses new read admission rather than blocking this callback.
				_ = database.Stats()
				_, readErr := database.Get([]byte("key"))
				notifications <- readErr
			}
			var err error
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.Set([]byte("key"), []byte("accepted")); err != nil {
				t.Fatal(err)
			}
			cache := database.cached
			stats := cache.COWMemoryStats()
			overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			pressure, err := cache.AcquireCOWAllocation(memtable.DefaultCOWLimits().MaxInFlightBytes - stats.ReservedBytes - overhead)
			if err != nil {
				t.Fatal(err)
			}
			defer pressure.Close()
			done := make(chan error, 1)
			go func() { done <- database.Close() }()
			select {
			case err := <-done:
				if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "final command WAL checkpoint during close") {
					t.Fatalf("actual final checkpoint refusal: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("close error notification blocked on public lifecycle lock")
			}
			select {
			case err := <-notifications:
				if !errors.Is(err, ErrClosed) {
					t.Fatalf("notification read during exclusive Close: %v", err)
				}
			default:
				t.Fatal("final checkpoint error was not notified")
			}
			pressure.Close()
			if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("closed error-path owners did not drain: %+v", stats)
			}
		})
	}
}

func TestCOWCloseHookCanWaitForAsynchronousNotificationRead(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			var database *DB
			callbackDone := make(chan error, 1)
			invokeCallback := make(chan struct{})
			opts := OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.NotifyError = func(err error) {
				if err.Error() != "asynchronous worker notification" {
					return
				}
				if stats := database.Stats(); stats != nil {
					t.Error("diagnostics entered exclusive Close")
				}
				_, readErr := database.Get([]byte("key"))
				callbackDone <- readErr
			}
			var err error
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.Set([]byte("key"), []byte("accepted")); err != nil {
				t.Fatal(err)
			}
			// The existing actual Close hook waits for an asynchronous callback
			// while Close owns lifecycleMu. This deterministically models the
			// worker-wait cycle; it is not a fabricated background flush failure.
			workerDone := make(chan struct{})
			go func() {
				defer close(workerDone)
				<-invokeCallback
				opts.NotifyError(errors.New("asynchronous worker notification"))
			}()
			testDuringPublicCloseAfterCheckpoint = func() {
				close(invokeCallback)
				select {
				case err := <-callbackDone:
					if !errors.Is(err, ErrClosed) {
						t.Errorf("asynchronous notification read: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("Close could not wait for notification callback")
				}
			}
			defer func() { testDuringPublicCloseAfterCheckpoint = nil }()
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			<-workerDone
		})
	}
}
