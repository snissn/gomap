package treedb

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCOWCloseErrorNotificationCanReadClosedDatabase(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			// Directory removal belongs to this test only after its Close owner
			// has finished. A watchdog failure must not race open Windows files.
			dir, err := os.MkdirTemp("", "gomap-cow-close-notification-")
			if err != nil {
				t.Fatal(err)
			}
			var database *DB
			var pressure *memtable.COWExternalLease
			entered := make(chan struct{}, 1)
			type readResult struct {
				statsRefused bool
				err          error
			}
			completed := make(chan readResult, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseCallback := func() { releaseOnce.Do(func() { close(release) }) }
			finished := make(chan struct{})
			var closeErr error // Read only after finished closes.
			closeStarted := false
			t.Cleanup(func() {
				releaseCallback()
				if pressure != nil {
					pressure.Close()
				}
				if database != nil && !closeStarted {
					closeStarted = true
					go func() { closeErr = database.Close(); close(finished) }()
				}
				if closeStarted {
					select {
					case <-finished:
					case <-time.After(30 * time.Second):
						t.Errorf("Close owner did not join during cleanup; preserving database directory %s", dir)
						return
					}
				}
				if err := os.RemoveAll(dir); err != nil {
					t.Errorf("remove closed database directory: %v", err)
				}
			})
			opts := OptionsFor(profile, dir)
			opts.MemtableMode = "cow_btree"
			opts.FlushThreshold = 1 << 30
			opts.NotifyError = func(err error) {
				if database == nil || !strings.Contains(err.Error(), "final command WAL checkpoint during close") {
					return
				}
				// Start the callback watchdog at actual notification entry,
				// independently of checkpoint and later storage teardown IO.
				entered <- struct{}{}
				statsRefused := database.Stats() == nil
				_, readErr := database.Get([]byte("key"))
				completed <- readResult{statsRefused: statsRefused, err: readErr}
				<-release
			}
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
			pressure, err = cache.AcquireCOWAllocation(memtable.DefaultCOWLimits().MaxInFlightBytes - stats.ReservedBytes - overhead)
			if err != nil {
				t.Fatal(err)
			}
			// Bound the whole storage operation separately; its expiry cannot
			// be classified as a callback lock failure without entry evidence.
			storageDeadline := time.NewTimer(30 * time.Second)
			defer storageDeadline.Stop()
			closeStarted = true
			go func() { closeErr = database.Close(); close(finished) }()
			select {
			case <-entered:
			case <-finished:
				t.Fatalf("Close returned without final checkpoint notification: %v", closeErr)
			case <-storageDeadline.C:
				t.Fatal("Close did not reach final checkpoint notification within storage watchdog")
			}
			select {
			case result := <-completed:
				if !result.statsRefused || !errors.Is(result.err, ErrClosed) {
					t.Fatalf("notification read during exclusive Close: stats refused=%v, error=%v", result.statsRefused, result.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("entered notification blocked reading through exclusive public lifecycle ownership")
			}
			select {
			case <-finished:
				t.Fatal("Close returned before synchronous notification was released")
			default:
			}
			releaseCallback()
			select {
			case <-finished:
				if !errors.Is(closeErr, memtable.ErrCOWCapacity) || !strings.Contains(closeErr.Error(), "final command WAL checkpoint during close") {
					t.Fatalf("actual final checkpoint refusal: %v", closeErr)
				}
			case <-storageDeadline.C:
				t.Fatal("notification reads completed, but Close storage teardown exceeded watchdog")
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
