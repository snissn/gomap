package db

import (
	"errors"
	"testing"

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
	if !errors.Is(err, first) || !errors.Is(err, second) || calls != 3 || d.captureTeardownHooks != nil || len(d.captureTeardownFailure.causes) != 2 || d.captureTeardownErrorCharge == 0 {
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
