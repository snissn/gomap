package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commandwalbarrier"
)

func TestNoWALSyncBarrierDrainAndFailure(t *testing.T) {
	for _, boundary := range []string{"checkpoint", "batch", "empty_batch", "conditional"} {
		for _, fail := range []bool{false, true} {
			t.Run(boundary+map[bool]string{false: "_success", true: "_failure"}[fail], func(t *testing.T) {
				database, err := Open(Options{Dir: t.TempDir()})
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close()
				drained := false
				sentinel := errors.New("local drain failed")
				unregister := database.RegisterCommandWALRawPublishBarrier(func() error {
					if !database.teardownMu.TryLock() {
						return errors.New("observer retained teardown lease")
					}
					database.teardownMu.Unlock()
					if drained {
						return nil
					}
					return &commandwalbarrier.PendingDrain{Drain: func() error {
						if !database.teardownMu.TryLock() {
							return errors.New("drain retained teardown lease")
						}
						database.teardownMu.Unlock()
						if !database.writeMu.TryLock() {
							return errors.New("drain retained write mutex")
						}
						database.writeMu.Unlock()
						if fail {
							return sentinel
						}
						drained = true
						return nil
					}}
				})
				defer unregister()
				switch boundary {
				case "checkpoint":
					err = database.Checkpoint()
				case "batch", "empty_batch":
					batch := database.NewBatch()
					if boundary == "batch" {
						if e := batch.Set([]byte("later"), []byte("value")); e != nil {
							t.Fatal(e)
						}
					}
					err = batch.WriteSync()
					_ = batch.Close()
				case "conditional":
					tx, e := database.NewConditionalTxn()
					if e != nil {
						t.Fatal(e)
					}
					defer tx.Close()
					if e = tx.Set([]byte("later"), []byte("value")); e != nil {
						t.Fatal(e)
					}
					err = tx.CommitSync()
				}
				if fail {
					if !errors.Is(err, sentinel) {
						t.Fatalf("err=%v want drain failure", err)
					}
					got, _ := database.Get([]byte("later"))
					if len(got) != 0 {
						t.Fatal("failed drain published later KV")
					}
				} else if err != nil || !drained {
					t.Fatalf("drained=%v err=%v", drained, err)
				}
			})
		}
	}
}

func TestNoWALReadOnlyConditionalSyncValidatesAfterDrain(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.SetSync([]byte("read"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	tx, err := database.NewConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err := tx.Get([]byte("read")); err != nil {
		t.Fatal(err)
	}
	drained := false
	unregister := database.RegisterCommandWALRawPublishBarrier(func() error {
		if drained {
			return nil
		}
		return &commandwalbarrier.PendingDrain{Drain: func() error { drained = true; return database.Set([]byte("read"), []byte("after")) }}
	})
	defer unregister()
	if err := tx.CommitSync(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("err=%v want conflict after drain", err)
	}
}

func TestNoWALSyncPreflightRejectsUnavailableDBBeforeHooks(t *testing.T) {
	for _, state := range []string{"closed", "readonly", "poisoned"} {
		t.Run(state, func(t *testing.T) {
			database := &DB{}
			called := false
			database.RegisterCommandWALRawPublishBarrier(func() error { called = true; return nil })
			var want error
			switch state {
			case "closed":
				database.closing.Store(true)
				want = ErrClosed
			case "readonly":
				database.readOnly = true
				want = ErrReadOnly
			case "poisoned":
				database.publicationPoisoned.Store(true)
				want = ErrRecoveryRequired
			}
			if err := database.drainNoWALSyncPublishBarriers(); !errors.Is(err, want) {
				t.Fatalf("err=%v want %v", err, want)
			}
			if called {
				t.Fatal("unavailable DB invoked local hook")
			}
		})
	}
}

// A later refill must not extend an explicit boundary's drain indefinitely.
// The sentinel makes the old retry loop fail deterministically instead of hang.
func TestNoWALSyncBarrierDoesNotChasePostDrainWrites(t *testing.T) {
	for _, boundary := range []string{"checkpoint", "batch", "empty_batch", "conditional"} {
		t.Run(boundary, func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			var observed, drained [2]int
			for i := range observed {
				unregister := database.RegisterCommandWALRawPublishBarrier(func() error {
					observed[i]++
					if observed[i] != 1 {
						return errors.New("boundary revisited a post-drain refill")
					}
					// This hook deliberately keeps advertising another drain.
					return &commandwalbarrier.PendingDrain{Drain: func() error {
						drained[i]++
						return nil
					}}
				})
				defer unregister()
			}
			switch boundary {
			case "checkpoint":
				err = database.Checkpoint()
			case "batch", "empty_batch":
				batch := database.NewBatch()
				if boundary == "batch" {
					if err := batch.Set([]byte("later"), []byte("value")); err != nil {
						t.Fatal(err)
					}
				}
				err = batch.WriteSync()
				_ = batch.Close()
			case "conditional":
				tx, e := database.NewConditionalTxn()
				if e != nil {
					t.Fatal(e)
				}
				defer tx.Close()
				if err := tx.Set([]byte("later"), []byte("value")); err != nil {
					t.Fatal(err)
				}
				err = tx.CommitSync()
			}
			if err != nil {
				t.Fatal(err)
			}
			if observed != [2]int{1, 1} || drained != [2]int{1, 1} {
				t.Fatalf("observed=%v drained=%v want each registered hook drained once", observed, drained)
			}
		})
	}
}

func TestNoWALSyncBarrierSnapshotPreservesUnregisterAndRegistrationCut(t *testing.T) {
	database := &DB{}
	var unregisterSecond, unregisterLate func()
	firstCalled, secondCalled, lateCalled := false, false, false
	defer func() {
		if unregisterLate != nil {
			unregisterLate()
		}
	}()
	unregisterFirst := database.RegisterCommandWALRawPublishBarrier(func() error {
		if firstCalled {
			return errors.New("boundary restarted its registry snapshot")
		}
		firstCalled = true
		return &commandwalbarrier.PendingDrain{Drain: func() error {
			unregisterSecond()
			unregisterLate = database.RegisterCommandWALRawPublishBarrier(func() error {
				lateCalled = true
				return nil
			})
			return nil
		}}
	})
	defer unregisterFirst()
	unregisterSecond = database.RegisterCommandWALRawPublishBarrier(func() error {
		secondCalled = true
		return nil
	})
	defer unregisterSecond()
	if err := database.drainNoWALSyncPublishBarriers(); err != nil {
		t.Fatal(err)
	}
	if secondCalled || lateCalled {
		t.Fatalf("second=%v late=%v want removed and post-cut hooks skipped", secondCalled, lateCalled)
	}
}
