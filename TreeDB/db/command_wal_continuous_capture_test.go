package db

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commandwalbarrier"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func continuousCapturePayload(t *testing.T) []byte {
	t.Helper()
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("continuous"), Value: []byte("value")}})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// The split control deterministically admits a queued teardown writer between
// append and capture. The continuous route must transfer the ORIGINAL read
// lease, without recursively taking it behind that writer.
func TestCommandWALPreparedCaptureRetainsOriginalTeardown(t *testing.T) {
	for _, continuous := range []bool{false, true} {
		t.Run(map[bool]string{false: "split_control", true: "continuous"}[continuous], func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			payload := continuousCapturePayload(t)
			acquired, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce, queueOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 {
					queueOnce.Do(func() {
						go func() { d.teardownMu.Lock(); close(acquired); <-release; d.teardownMu.Unlock(); close(finished) }()
						deadline := time.Now().Add(2 * time.Second)
						for d.teardownMu.TryRLock() {
							d.teardownMu.RUnlock()
							if time.Now().After(deadline) {
								return
							}
							runtime.Gosched()
						}
					})
				}
				return nil
			})
			defer restore()
			var target CommandWALCapturedPrefix
			var lsn uint64
			if continuous {
				lsn, _, _, err = d.AppendRawKVBatchPayloadCommandWALTrustedPreparedCapture(func() ([]byte, error) { return payload, nil }, &target, func(uint64) bool {
					select {
					case <-acquired:
						t.Error("original teardown lease released before capture")
					default:
					}
					return true
				}, false)
			} else {
				lsn, err = d.AppendRawKVBatchPayloadCommandWALTrustedWithMode(payload, RawKVCommandWALAppendCapturedDurable)
				select {
				case <-acquired:
				case <-time.After(2 * time.Second):
					t.Fatal("split control did not expose its admission gap")
				}
				unblock()
				_, _, err = d.CaptureCommandWALPrefixInto(&target, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if d.commandWALDurableLSN.Load() >= lsn {
				t.Fatal("capture alone certified physical durability")
			}
			if _, err = target.Sync(); err != nil {
				t.Fatal(err)
			}
			if d.commandWALDurableLSN.Load() != lsn {
				t.Fatal("lost assigned prefix")
			}
			select {
			case <-acquired:
			case <-time.After(2 * time.Second):
				t.Fatal("Sync failed to release original teardown")
			}
			unblock()
			select {
			case <-finished:
			case <-time.After(2 * time.Second):
				t.Fatal("teardown writer remained blocked")
			}
		})
	}
}

// Existing hook STATE can change after assignment without a new registration.
// A final PendingDrain must unwind every original lease, run the existing drain,
// rerun hooks, and capture the same assigned mutation without appending again.
func TestCommandWALPreparedCaptureFinalPendingDrain(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var pending atomic.Bool
	var drains, hooks atomic.Int32
	unregister := d.RegisterCommandWALRawPublishBarrier(func() error {
		hooks.Add(1)
		if !pending.Load() {
			return nil
		}
		return &commandwalbarrier.PendingDrain{Drain: func() error {
			if !d.teardownMu.TryLock() {
				return errors.New("drain retained teardown")
			}
			d.teardownMu.Unlock()
			if !d.commandWALRawAdmissionMu.TryLock() {
				return errors.New("drain retained admission")
			}
			d.commandWALRawAdmissionMu.Unlock()
			if !d.commandWALRawPublishMu.TryLock() {
				return errors.New("drain retained publication")
			}
			d.commandWALRawPublishMu.Unlock()
			drains.Add(1)
			pending.Store(false)
			return nil
		}}
	})
	defer unregister()
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 {
			pending.Store(true)
		}
		return nil
	})
	defer restore()
	payload := continuousCapturePayload(t)
	var target CommandWALCapturedPrefix
	lsn, _, _, err := d.AppendRawKVBatchPayloadCommandWALTrustedPreparedCapture(func() ([]byte, error) { return payload, nil }, &target, func(uint64) bool { return true }, true)
	if err != nil {
		t.Fatal(err)
	}
	if drains.Load() != 1 || hooks.Load() < 4 {
		t.Fatalf("drains=%d hooks=%d", drains.Load(), hooks.Load())
	}
	if d.CommandWALNextLSN() != lsn+1 {
		t.Fatal("assigned command was retried")
	}
	if _, err = target.Sync(); err != nil {
		t.Fatal(err)
	}
	if d.commandWALDurableLSN.Load() != lsn {
		t.Fatal("drained capture lost prefix")
	}
}

func TestCommandWALPreparedCaptureUsesRotatedActualFile(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var appendedPath, syncedPath string
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL {
			if e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 {
				appendedPath = e.Path
			}
			if e.Point == durabilitycut.AfterDependencyFileSync {
				syncedPath = e.Path
			}
		}
		return nil
	})
	defer restore()
	payload := continuousCapturePayload(t)
	var target CommandWALCapturedPrefix
	var rotateErr error
	lsn, _, _, err := d.AppendRawKVBatchPayloadCommandWALTrustedPreparedCapture(func() ([]byte, error) { return payload, nil }, &target, func(uint64) bool {
		rotateErr = d.RotateCommandWALActiveSegment(true)
		return true
	}, false)
	if err != nil || rotateErr != nil {
		t.Fatal(err, rotateErr)
	}
	// The independently rotated successor is empty. A target inferred from the
	// append's LSN/file would incorrectly retain the predecessor here.
	if target.journal.LSN != 0 {
		t.Fatal("capture reused append LSN after rotation")
	}
	if _, err = target.Sync(); err != nil {
		t.Fatal(err)
	}
	if appendedPath == "" || syncedPath == "" || appendedPath == syncedPath {
		t.Fatal("capture did not use actual successor file", appendedPath, syncedPath)
	}
	if d.CommandWALNextLSN() != lsn+1 {
		t.Fatal("rotation retried assigned mutation")
	}
}

// errors.As is invoked by the final boundary's PendingDrain classification.
// Probe the ORIGINAL locks there, without scheduling, sleeps or new hooks.
type continuousCaptureHookError struct{ inspect func() }

func (e *continuousCaptureHookError) Error() string { return "final hook failure" }
func (e *continuousCaptureHookError) As(any) bool   { e.inspect(); return false }

func TestCommandWALPreparedCaptureFinalHookFailurePoisons(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var releasedBeforeClassification atomic.Bool
	injected := &continuousCaptureHookError{inspect: func() {
		if d.commandWALRawAdmissionMu.TryLock() {
			releasedBeforeClassification.Store(true)
			d.commandWALRawAdmissionMu.Unlock()
		}
	}}
	var pending atomic.Bool
	unregister := d.RegisterCommandWALRawPublishBarrier(func() error {
		if pending.Load() {
			return injected
		}
		return nil
	})
	defer unregister()
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 {
			pending.Store(true)
		}
		return nil
	})
	defer restore()
	payload := continuousCapturePayload(t)
	var target CommandWALCapturedPrefix
	lsn, _, _, err := d.AppendRawKVBatchPayloadCommandWALTrustedPreparedCapture(func() ([]byte, error) { return payload, nil }, &target, func(uint64) bool { return true }, false)
	if lsn == 0 || !errors.Is(err, injected) {
		t.Fatal("lost postassignment failure", lsn, err)
	}
	if releasedBeforeClassification.Load() {
		t.Error("terminal hook classified after releasing assigned admission")
	}
	if target.db != nil {
		t.Fatal("failed final hook retained target custody")
	}
	if _, err = d.AppendRawKVBatchPayloadCommandWALTrusted(payload, false); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatal("failed assigned capture did not poison next append", err)
	}
	if !d.teardownMu.TryLock() {
		t.Fatal("failure retained teardown")
	}
	d.teardownMu.Unlock()
}

func TestCommandWALPreparedCaptureDeclinedUsesSameAppend(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	payload := continuousCapturePayload(t)
	var target CommandWALCapturedPrefix
	selected := 0
	lsn, _, _, err := d.AppendRawKVBatchPayloadCommandWALTrustedPreparedCapture(func() ([]byte, error) { return payload, nil }, &target, func(got uint64) bool {
		selected++
		if got == 0 {
			t.Error("unassigned selection")
		}
		return false
	}, false)
	if err != nil || lsn == 0 || selected != 1 || target.db != nil {
		t.Fatal("declined continuation changed assignment/custody", lsn, selected, err)
	}
	if _, _, err = d.CaptureCommandWALPrefixInto(&target, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = target.Sync(); err != nil {
		t.Fatal(err)
	}
	if d.CommandWALNextLSN() != lsn+1 || d.commandWALDurableLSN.Load() != lsn {
		t.Fatal("fallback did not consume exact same assigned prefix")
	}
}

func TestCommandWALPreparedCaptureSelectionPanicPoisons(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	payload := continuousCapturePayload(t)
	var target CommandWALCapturedPrefix
	injected := errors.New("assigned selector panic")
	func() {
		defer func() {
			if recovered := recover(); recovered != injected {
				t.Errorf("panic=%v", recovered)
			}
		}()
		_, _, _, _ = d.AppendRawKVBatchPayloadCommandWALTrustedPreparedCapture(func() ([]byte, error) { return payload, nil }, &target, func(uint64) bool { panic(injected) }, false)
		t.Error("selector panic was swallowed")
	}()
	if !d.teardownMu.TryLock() {
		t.Fatal("selector panic retained teardown")
	}
	d.teardownMu.Unlock()
	if !d.commandWALRawAdmissionMu.TryLock() {
		t.Fatal("selector panic retained admission")
	}
	d.commandWALRawAdmissionMu.Unlock()
	if !d.commandWALRawPublishMu.TryLock() {
		t.Fatal("selector panic retained publication")
	}
	d.commandWALRawPublishMu.Unlock()
	if d.CommandWALNextLSN() != 2 {
		t.Fatal("selector panic lost assigned identity")
	}
	if _, err = d.AppendRawKVBatchPayloadCommandWALTrusted(payload, false); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatal("assigned selector panic allowed a later append", err)
	}
}
