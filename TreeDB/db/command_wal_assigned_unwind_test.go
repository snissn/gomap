package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/commandwalbarrier"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestCommandWALAssignedCaptureAbnormalUnwind(t *testing.T) {
	for _, mode := range []string{"nil", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			unregister := d.RegisterCommandWALRawPublishBarrier(func() error {
				switch calls.Add(1) {
				case 1:
					return nil
				case 2:
					return &commandwalbarrier.PendingDrain{Drain: func() error { return nil }}
				default:
					if mode == "goexit" {
						runtime.Goexit()
					}
					panic(nil)
				}
			})
			payload := continuousCapturePayload(t)
			done := make(chan struct{})
			returned := false
			var recovered any
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				var target CommandWALCapturedPrefix
				_, _, _, _ = d.AppendRawKVBatchPayloadCommandWALTrustedPreparedCapture(func() ([]byte, error) { return payload, nil }, &target, func(uint64) bool { return true }, false)
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("assigned unwind stuck")
			}
			if returned {
				t.Fatal("abnormal acquisition returned normally")
			}
			if recovered != nil {
				if _, ok := recovered.(*runtime.PanicNilError); !ok || mode == "goexit" {
					t.Fatalf("changed unwind %v", recovered)
				}
			}
			if !d.commandWALRawPublishMu.TryLock() {
				t.Fatal("publication retained")
			}
			d.commandWALRawPublishMu.Unlock()
			if !d.commandWALRawAdmissionMu.TryLock() {
				t.Fatal("admission retained")
			}
			d.commandWALRawAdmissionMu.Unlock()
			if !d.teardownMu.TryLock() {
				t.Fatal("teardown retained")
			}
			d.teardownMu.Unlock()
			unregister()
			if _, err := d.AppendRawKVBatchPayloadCommandWALTrusted(payload, false); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("later append=%v", err)
			}
			_ = d.Close()
		})
	}
}

// Unassigned acquisition has the same lease obligation without acquiring the
// assigned-command poisoning obligation.
func TestCommandWALUnassignedBarrierAbnormalUnwind(t *testing.T) {
	for _, mode := range []string{"nil", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			unregister := d.RegisterCommandWALRawPublishBarrier(func() error {
				if mode == "goexit" {
					runtime.Goexit()
				}
				panic(nil)
			})
			done := make(chan struct{})
			returned := false
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				_, _ = d.LockCommandWALPublishWithBarriers()
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("unassigned unwind stuck")
			}
			unregister()
			if returned {
				t.Fatal("abnormal hook returned")
			}
			if !d.commandWALRawPublishMu.TryLock() {
				t.Fatal("publication retained")
			}
			d.commandWALRawPublishMu.Unlock()
			if !d.commandWALRawAdmissionMu.TryLock() {
				t.Fatal("admission retained")
			}
			d.commandWALRawAdmissionMu.Unlock()
			if !d.teardownMu.TryLock() {
				t.Fatal("teardown retained")
			}
			d.teardownMu.Unlock()
			if d.commandWALFlushPoisoned.Load() {
				t.Fatal("unassigned unwind incorrectly poisoned append")
			}
			if _, err := d.AppendRawKVBatchPayloadCommandWALTrusted(continuousCapturePayload(t), false); err != nil {
				t.Fatalf("later append=%v", err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
