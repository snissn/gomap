package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// Ordinary public COW writes must install complete successor cuts while the
// predecessor's physical WAL barrier is blocked. Their ACK still needs a
// second captured barrier, even if its bytes were incidentally persisted.
func TestPublicCOWCapturedPrefixAppendInstallAndExactACK(t *testing.T) {
	for _, tc := range []struct {
		mode   string
		rotate bool
	}{{"cow_btree", false}, {"cow_btree", true}, {"cow_ordered", false}, {"cow_ordered", true}} {
		rotate := tc.rotate
		t.Run(fmt.Sprintf("%s/rotate=%t", tc.mode, rotate), func(t *testing.T) {
			opts := commandWALDurabilityProofOptions(t.TempDir())
			opts.MemtableMode = tc.mode
			firstValue, secondValue := []byte("one"), []byte("two")
			if rotate {
				opts.CommandWALSegmentTargetBytes = 1
				opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
				// Exceed the self-contained materialized-RID frame limit.
				firstValue, secondValue = bytes.Repeat([]byte("a"), 65<<10), bytes.Repeat([]byte("b"), 65<<10)
			}
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			entered := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			released := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			var releaseOnce [2]sync.Once
			release := func(i int) { releaseOnce[i].Do(func() { close(released[i]) }) }
			defer func() { release(0); release(1) }()
			var syncs atomic.Int32
			var barrierAppended atomic.Bool
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				// Rotated prefix files may also need syncing; gate the active target via
				// its file-sync event. Blocking the first physical WAL sync is sufficient.
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend && e.LSN == 1 {
					barrierAppended.Store(true)
				}
				if barrierAppended.Load() && e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && e.LSN != 0 {
					i := int(syncs.Add(1)) - 1
					if i < 2 {
						close(entered[i])
						<-released[i]
					}
				}
				return nil
			})
			defer restore()
			first := make(chan error, 1)
			second := make(chan error, 1)
			go func() { first <- database.SetSync([]byte("first"), firstValue) }()
			select {
			case <-entered[0]:
			case <-time.After(5 * time.Second):
				t.Fatal("first fsync did not start")
			}
			go func() { second <- database.SetSync([]byte("second"), secondValue) }()
			deadline := time.Now().Add(5 * time.Second)
			for {
				got, readErr := database.Get([]byte("second"))
				if readErr != nil {
					t.Fatal(readErr)
				}
				if bytes.Equal(got, secondValue) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("successor did not install while predecessor fsync blocked")
				}
				time.Sleep(time.Millisecond)
			}
			requireRawKVValue(t, database, []byte("first"), firstValue)
			select {
			case err := <-first:
				t.Fatalf("first ACK before sync: %v", err)
			default:
			}
			select {
			case err := <-second:
				t.Fatalf("second ACK before captured sync: %v", err)
			default:
			}
			// Rotation can add predecessor file syncs to the first target. Only the
			// no-rotation case binds sync #2 to the successor's separate exact target.
			if rotate {
				release(0)
				release(1)
			} else {
				release(0)
				select {
				case <-entered[1]:
				case <-time.After(5 * time.Second):
					t.Fatal("successor fsync did not start")
				}
				select {
				case err := <-first:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("first ACK waited for successor fsync")
				}
				first = nil
				select {
				case err := <-second:
					t.Fatalf("successor ACK covered by predecessor sync: %v", err)
				default:
				}
				release(1)
			}
			if first != nil {
				if err := <-first; err != nil {
					t.Fatal(err)
				}
			}
			if err := <-second; err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			requireRawKVValue(t, reopened, []byte("first"), firstValue)
			requireRawKVValue(t, reopened, []byte("second"), secondValue)
		})
	}
}

func TestPublicCOWCapturedPrefixFailureWakesSuccessorAndPoisons(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	opts.MemtableMode = "cow_ordered"
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	// Reuse collector-owned storage through completed captures before the
	// injected failure. Old retained resources must not survive that reuse.
	for i := 0; i < 8; i++ {
		if err := database.SetSync([]byte("warm"), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	failure := errors.New("captured physical barrier failed")
	var syncOnce sync.Once
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		var err error
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			syncOnce.Do(func() { close(entered); <-release; err = failure })
		}
		return err
	})
	defer restore()
	results := make(chan error, 2)
	go func() { results <- database.SetSync([]byte("first"), []byte("one")) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first sync did not start")
	}
	go func() { results <- database.SetSync([]byte("second"), []byte("two")) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, _ := database.Get([]byte("second"))
		if string(got) == "two" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("successor cut was not installed")
		}
		time.Sleep(time.Millisecond)
	}
	once.Do(func() { close(release) })
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if !errors.Is(err, failure) {
				t.Fatalf("waiter error=%v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("failed barrier stranded waiter")
		}
	}
	if err := database.SetSync([]byte("later"), []byte("three")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("later append=%v, want recovery required", err)
	}
}
