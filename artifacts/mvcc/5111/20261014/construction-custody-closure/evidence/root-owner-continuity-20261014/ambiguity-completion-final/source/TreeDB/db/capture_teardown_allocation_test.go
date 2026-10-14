package db

import (
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCaptureTeardownArrayUsesOriginalRegistryBeforeGrowth(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	owner := d.valueLogIdentityPins.MetadataOwner()
	var foreign retainedalloc.Owner
	foreign.Initialize(0)
	hook := func() error { return nil }
	before := owner.Bytes()
	if ok, err := d.tryRegisterCaptureTeardownHookWithMetadata(hook, &foreign); ok || !errors.Is(err, rootpublication.ErrResourceOwnership) || owner.Bytes() != before || len(d.captureTeardownHooks) != 0 {
		t.Fatal("foreign owner allocated or appended", err)
	}
	if ok, err := d.tryRegisterCaptureTeardownHookWithMetadata(hook, owner); !ok || err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() != before+d.captureTeardownHooksCharge || d.captureTeardownHooksOwner != owner {
		t.Fatal("array birth lacked exact original producer admission")
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	budget.Close()
	oldLen, oldCap, oldBytes := len(d.captureTeardownHooks), cap(d.captureTeardownHooks), owner.Bytes()
	if ok, err := d.tryRegisterCaptureTeardownHookWithMetadata(hook, owner); ok || err == nil || len(d.captureTeardownHooks) != oldLen || cap(d.captureTeardownHooks) != oldCap || owner.Bytes() != oldBytes {
		t.Fatal("refused geometric overlap changed backing", err)
	}
	enrollment.Close()
	d.teardownMu.Lock()
	err = d.runCaptureTeardownHooksLocked()
	d.teardownMu.Unlock()
	if err != nil || d.captureTeardownHooks != nil || owner.Bytes() != before {
		t.Fatal("actual transferred backing did not refund after callbacks", err, owner.Bytes(), before)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Stats().TotalBytes != 0 {
		t.Fatal("capture governor retained after actual cleanup")
	}
}

func TestCaptureTeardownArrayRetainsAllActualCallbackErrors(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	owner := d.valueLogIdentityPins.MetadataOwner()
	first, second := errors.New("first terminal error"), errors.New("second terminal error")
	calls := 0
	for _, cause := range []error{first, nil, second} {
		cause := cause
		if ok, err := d.tryRegisterCaptureTeardownHookWithMetadata(func() error { calls++; return cause }, owner); !ok || err != nil {
			t.Fatal(err)
		}
	}
	d.teardownMu.Lock()
	err = d.runCaptureTeardownHooksLocked()
	d.teardownMu.Unlock()
	if !errors.Is(err, first) || !errors.Is(err, second) || calls != 3 || len(d.captureTeardownHooks) != 3 || d.captureTeardownHooks[0].hook == nil || d.captureTeardownHooks[1].hook != nil || d.captureTeardownHooks[2].hook == nil || len(d.captureTeardownFailure.causes) != 2 || d.captureTeardownErrorCharge == 0 {
		t.Fatal("lost exact error/backing custody", err, calls)
	}
	d.teardownMu.Lock()
	again := d.runCaptureTeardownHooksLocked()
	d.teardownMu.Unlock()
	if again != err || calls != 3 {
		t.Fatal("terminal callback retried or first result replaced")
	}
	if err = d.Close(); !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatal("original Close lost terminal causes", err)
	}
}

// This receiver exercises a real method-value alias and its admitted original
// containing/control charge. The channels are test interleave controls.
type captureTerminalTestAuthority struct {
	owner          *retainedalloc.Owner
	charge         uint64
	entered, leave chan struct{}
	outcome        error
}

func (a *captureTerminalTestAuthority) terminal() error {
	close(a.entered)
	<-a.leave
	if a.owner.Bytes() != a.charge {
		panic("authority charge consumed before callback return")
	}
	if a.outcome != nil {
		return a.outcome
	}
	// Successful producer cleanup removes its own payload aliases; the caller
	// still owns the method value until this invocation returns.
	a.owner = nil
	return nil
}

func TestCaptureTeardownOwnedMethodChargeSurvivesCallbackReturn(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			registry := d.valueLogIdentityPins.MetadataOwner()
			var original retainedalloc.Owner
			original.Initialize(0)
			charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(captureTerminalTestAuthority{})) + 2*uint64(unsafe.Sizeof(uintptr(0))))
			if err := original.AddPending(charge); err != nil {
				t.Fatal(err)
			}
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			enrollment, err := original.Enroll(budget)
			if err != nil {
				t.Fatal(err)
			}
			cause := errors.New("retained original terminal outcome")
			authority := &captureTerminalTestAuthority{owner: &original, charge: original.Bytes(), entered: make(chan struct{}), leave: make(chan struct{})}
			if fail {
				authority.outcome = cause
			}
			if ok, err := d.tryRegisterCaptureTeardownOwnedHook(authority.terminal, registry, &original, charge); !ok || err != nil {
				t.Fatal(err)
			}
			authorityEntered, authorityLeave := authority.entered, authority.leave
			authority = nil
			enrolledBytes := original.Bytes()
			done := make(chan error, 1)
			go func() {
				d.teardownMu.Lock()
				err := d.runCaptureTeardownHooksLocked()
				d.teardownMu.Unlock()
				done <- err
			}()
			<-authorityEntered
			if original.Bytes() != enrolledBytes || budget.Stats().TotalBytes == 0 {
				t.Fatal("paused callback lost actual containing custody")
			}
			close(authorityLeave)
			outcome := <-done
			if fail {
				if !errors.Is(outcome, cause) || original.Bytes() != enrolledBytes || len(d.captureTeardownHooks) != 1 || d.captureTeardownHooks[0].hook == nil || d.captureTeardownHooks[0].completionOwner != &original || d.captureTeardownHooks[0].completionCharge != charge {
					t.Fatal("failed callback lost original method/charge custody", outcome)
				}
				d.teardownMu.Lock()
				again := d.runCaptureTeardownHooksLocked()
				d.teardownMu.Unlock()
				if again != outcome {
					t.Fatal("failed terminal callback retried")
				}
				if err := d.Close(); !errors.Is(err, cause) {
					t.Fatal(err)
				}
			} else {
				if outcome != nil || original.Bytes() != enrolledBytes-charge || d.captureTeardownHooks != nil {
					t.Fatal("successful return did not consume exact charge after alias clear", outcome)
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
			}
			enrollment.Close()
			budget.Close()
		})
	}
}

func TestCaptureTeardownCompatibilityCancellationKeepsArrayAdmission(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	owner := d.valueLogIdentityPins.MetadataOwner()
	before := owner.Bytes()
	calls := 0
	cancel, ok := d.tryRegisterCaptureTeardownHook(func() error { calls++; return nil })
	if !ok {
		t.Fatal("original compatibility registration refused")
	}
	charge := d.captureTeardownHooksCharge
	cancel()
	cancel()
	if d.captureTeardownHooks[0].hook != nil || charge == 0 || owner.Bytes() != before+charge {
		t.Fatal("cancellation refunded retained array or kept callback")
	}
	d.teardownMu.Lock()
	err = d.runCaptureTeardownHooksLocked()
	d.teardownMu.Unlock()
	if err != nil || calls != 0 || owner.Bytes() != before {
		t.Fatal("canceled original callback ran or array was not consumed", err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureTeardownCancellationCannotDropTerminalFailedRecord(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("original failed compatibility callback")
	cancel, ok := d.tryRegisterCaptureTeardownHook(func() error { return cause })
	if !ok {
		t.Fatal("register")
	}
	d.teardownMu.Lock()
	outcome := d.runCaptureTeardownHooksLocked()
	d.teardownMu.Unlock()
	cancel()
	if !errors.Is(outcome, cause) || len(d.captureTeardownHooks) != 1 || d.captureTeardownHooks[0].hook == nil {
		t.Fatal("cancel discarded original failed callback custody", outcome)
	}
	if err := d.Close(); !errors.Is(err, cause) {
		t.Fatal(err)
	}
}
