package db

import (
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/commandwalbarrier"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The still-pending Drain must permit ordinary progress; once its failure is
// observed the assigned acquisition poisons before propagating it. Work that
// legitimately completed during the unlocked interval is not retroactively
// rejected. The panic case preserves the original panic value too.
func TestCommandWALAssignedCaptureUnlockedDrainFailure(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%t", panics), func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			injected := errors.New("unlocked assigned drain failure")
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var pending atomic.Bool
			unregister := d.RegisterCommandWALRawPublishBarrier(func() error {
				if !pending.Load() {
					return nil
				}
				return &commandwalbarrier.PendingDrain{Drain: func() error {
					if !d.teardownMu.TryLock() {
						return errors.New("Drain retained teardown")
					}
					d.teardownMu.Unlock()
					if !d.commandWALRawAdmissionMu.TryLock() {
						return errors.New("Drain retained admission")
					}
					d.commandWALRawAdmissionMu.Unlock()
					if !d.commandWALRawPublishMu.TryLock() {
						return errors.New("Drain retained publication")
					}
					d.commandWALRawPublishMu.Unlock()
					pending.Store(false)
					close(entered)
					<-release
					if panics {
						panic(injected)
					}
					return injected
				}}
			})
			defer unregister()
			var observed sync.Once
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 {
					observed.Do(func() { pending.Store(true) })
				}
				return nil
			})
			defer restore()
			payload := continuousCapturePayload(t)
			type outcome struct {
				lsn       uint64
				err       error
				recovered any
			}
			done := make(chan outcome, 1)
			go func() {
				r := outcome{}
				defer func() { r.recovered = recover(); done <- r }()
				var target CommandWALCapturedPrefix
				r.lsn, _, _, r.err = d.AppendRawKVBatchPayloadCommandWALTrustedPreparedCapture(func() ([]byte, error) { return payload, nil }, &target, func(uint64) bool { return true }, false)
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("assigned Drain not reached")
			}
			// No terminal failure is known yet. A legitimate append can complete.
			concurrent, err := d.AppendRawKVBatchPayloadCommandWALTrusted(payload, false)
			if err != nil || concurrent != 2 {
				t.Fatal("pending unlocked Drain prevented legitimate progress", concurrent, err)
			}
			unblock()
			select {
			case r := <-done:
				if panics {
					if r.recovered != injected {
						t.Fatal("changed panic identity", r.recovered)
					}
				} else if r.lsn != 1 || !errors.Is(r.err, injected) {
					t.Fatal("changed assigned error identity", r.lsn, r.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("failed Drain did not unwind")
			}
			if _, err = d.AppendRawKVBatchPayloadCommandWALTrusted(payload, false); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatal("known assigned Drain failure allowed later append", err)
			}
			if d.CommandWALNextLSN() != 3 {
				t.Fatal("assigned command retried after drain failure")
			}
		})
	}
}

func TestCommandWALAssignedCaptureReacquireHookPanicUnwinds(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	injected := errors.New("assigned reacquisition hook panic")
	var calls atomic.Int32
	unregister := d.RegisterCommandWALRawPublishBarrier(func() error {
		switch calls.Add(1) {
		case 1:
			return nil
		case 2:
			return &commandwalbarrier.PendingDrain{Drain: func() error { return nil }}
		default:
			panic(injected)
		}
	})
	defer unregister()
	payload := continuousCapturePayload(t)
	func() {
		defer func() {
			if r := recover(); r != injected {
				t.Errorf("changed panic identity: %v", r)
			}
		}()
		var target CommandWALCapturedPrefix
		_, _, _, _ = d.AppendRawKVBatchPayloadCommandWALTrustedPreparedCapture(func() ([]byte, error) { return payload, nil }, &target, func(uint64) bool { return true }, false)
		t.Error("hook panic swallowed")
	}()
	// Test-only forensic cleanup on failure prevents a broken implementation
	// from turning this precise assertion into an unrelated Close timeout. The
	// panicking synchronous acquisition is the sole owner of these exact leases.
	if d.commandWALRawPublishMu.TryLock() {
		d.commandWALRawPublishMu.Unlock()
	} else {
		t.Error("assigned hook panic retained publication")
		d.commandWALRawPublishMu.Unlock()
	}
	if d.commandWALRawAdmissionMu.TryLock() {
		d.commandWALRawAdmissionMu.Unlock()
	} else {
		t.Error("assigned hook panic retained admission")
		d.commandWALRawAdmissionMu.Unlock()
	}
	if d.teardownMu.TryLock() {
		d.teardownMu.Unlock()
	} else {
		t.Error("assigned hook panic retained teardown")
		d.teardownMu.RUnlock()
	}
	unregister()
	if _, err = d.AppendRawKVBatchPayloadCommandWALTrusted(payload, false); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatal("assigned hook panic failed to poison", err)
	}
}

func TestCommandWALPreparedCapturePreassignmentRejectionDoesNotPoison(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	injected := errors.New("preassignment hook rejection")
	unregister := d.RegisterCommandWALRawPublishBarrier(func() error { return injected })
	defer unregister()
	payload := continuousCapturePayload(t)
	var target CommandWALCapturedPrefix
	selected := false
	lsn, _, _, err := d.AppendRawKVBatchPayloadCommandWALTrustedPreparedCapture(func() ([]byte, error) { return payload, nil }, &target, func(uint64) bool { selected = true; return true }, false)
	if lsn != 0 || !errors.Is(err, injected) || selected || target.db != nil {
		t.Fatal("preassignment rejection changed custody", lsn, selected, err)
	}
	unregister()
	if lsn, err = d.AppendRawKVBatchPayloadCommandWALTrusted(payload, false); err != nil || lsn != 1 {
		t.Fatal("preassignment rejection poisoned later work", lsn, err)
	}
}
