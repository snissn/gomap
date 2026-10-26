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
				if barrierAppended.Load() && e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && (!rotate || e.LSN != 0) {
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
			case <-time.After(2 * time.Second):
				t.Fatal("first fsync did not start")
			}
			go func() { second <- database.SetSync([]byte("second"), secondValue) }()
			deadline := time.Now().Add(2 * time.Second)
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
				case <-time.After(2 * time.Second):
					t.Fatal("successor fsync did not start")
				}
				select {
				case err := <-first:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
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
	case <-time.After(2 * time.Second):
		t.Fatal("first sync did not start")
	}
	go func() { results <- database.SetSync([]byte("second"), []byte("two")) }()
	deadline := time.Now().Add(2 * time.Second)
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
		case <-time.After(2 * time.Second):
			t.Fatal("failed barrier stranded waiter")
		}
	}
	if err := database.SetSync([]byte("later"), []byte("three")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("later append=%v, want recovery required", err)
	}
}

// Production defaults must keep a whole successor cohort while the first exact
// WAL target is in flight. The observer does not change collector eligibility.
func TestPublicDurableDefaultBlockedSyncCollectsSuccessorEpoch(t *testing.T) {
	for _, tc := range []struct {
		mode       string
		valueBytes int
	}{
		{"btree", 0}, {"cow_ordered", 0}, {"btree", 4096}, {"cow_ordered", 4096},
		{"btree", 65 << 10}, {"cow_ordered", 65 << 10},
	} {
		mode := tc.mode
		t.Run(fmt.Sprintf("%s/valueBytes=%d", mode, tc.valueBytes), func(t *testing.T) {
			opts := commandWALDurabilityProofOptions(t.TempDir())
			opts.MemtableMode = mode
			firstValue, nextValue := []byte("one"), []byte("two")
			if tc.valueBytes != 0 {
				opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
				firstValue = bytes.Repeat([]byte("a"), tc.valueBytes)
				nextValue = bytes.Repeat([]byte("b"), tc.valueBytes)
			}
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			before := database.Stats()
			entered := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			released := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			var releaseOnce [2]sync.Once
			release := func(i int) { releaseOnce[i].Do(func() { close(released[i]) }) }
			defer func() { release(0); release(1) }()
			var syncs, appends atomic.Int32
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource != durabilitycut.ResourceCommandWAL {
					return nil
				}
				if e.Point == durabilitycut.AfterDependencyAppend {
					appends.Add(1)
				}
				if e.Point == durabilitycut.BeforeDependencyFileSync {
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
			go func() { first <- database.Set([]byte("first"), firstValue) }()
			select {
			case <-entered[0]:
			case <-time.After(3 * time.Second):
				t.Fatal("first WAL sync did not start")
			}
			const successors = 4
			done := make(chan error, successors)
			for i := 0; i < successors; i++ {
				go func(i int) { done <- database.Set([]byte(fmt.Sprintf("next/%d", i)), nextValue) }(i)
			}
			deadline := time.Now().Add(3 * time.Second)
			for appends.Load() < successors+1 {
				if time.Now().After(deadline) {
					t.Fatal("successors could not append while first WAL sync blocked")
				}
				time.Sleep(time.Millisecond)
			}
			if mode == "cow_ordered" {
				for i := 0; i < successors; i++ {
					requireRawKVValue(t, database, []byte(fmt.Sprintf("next/%d", i)), nextValue)
				}
			}
			if got := syncs.Load(); got != 1 {
				t.Fatalf("queued successor appends performed %d WAL synchronizations before epoch closure, want 1 active predecessor", got)
			}
			select {
			case err := <-done:
				t.Fatalf("successor ACK before its sync: %v", err)
			default:
			}
			release(0)
			select {
			case <-entered[1]:
			case <-time.After(3 * time.Second):
				t.Fatal("successor epoch did not start")
			}
			select {
			case err := <-first:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("first ACK waited for successor epoch")
			}
			select {
			case err := <-done:
				t.Fatalf("successor ACK borrowed preceding certificate: %v", err)
			default:
			}
			release(1)
			for i := 0; i < successors; i++ {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("successor ACK stranded")
				}
			}
			after := database.Stats()
			requirePublicStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total", 2)
			requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.groups_total", 2)
			requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.commits_total", successors+1)
		})
	}
}

// V2 certifies the entire preceding canonical prefix. External pointer bytes
// must be stable before a durable-class marker can be appended. Keep this
// earlier dependency wait explicit: only the final captured WAL sync admits
// unrelated canonical appends in the present format.
func TestPublicCOWCapturedPrefixExternalDependencyWaitBoundary(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	entered, released := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(released) }) }
	defer release()
	var first sync.Once
	var appends atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend {
			appends.Add(1)
		}
		if e.Resource == durabilitycut.ResourceAuxiliary && e.Point == durabilitycut.BeforeDependencyFileSync {
			first.Do(func() { close(entered); <-released })
		}
		return nil
	})
	defer restore()
	done := make(chan error, 2)
	go func() { done <- database.SetSync([]byte("external"), bytes.Repeat([]byte("x"), 65<<10)) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("external dependency synchronization did not start")
	}
	go func() { done <- database.SetSync([]byte("successor"), []byte("inline")) }()
	deadline := time.Now().Add(3 * time.Second)
	for database.commandWALGroupCommit.durableIntents.Load() != 2 {
		if time.Now().After(deadline) {
			t.Fatal("successor did not announce its durable intent before cache admission")
		}
		time.Sleep(time.Millisecond)
	}
	if got := appends.Load(); got != 0 {
		t.Fatalf("canonical appends=%d while first dependency is unstable, want 0 under V2", got)
	}
	release()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("dependency release stranded public writer")
		}
	}
	requireRawKVValue(t, database, []byte("successor"), []byte("inline"))
}
