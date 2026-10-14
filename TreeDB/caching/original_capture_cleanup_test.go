package caching

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type originalCaptureBackend struct {
	MockBackend
	owner *retainedalloc.Owner
}

func (b *originalCaptureBackend) StableResourceMetadataOwner() *retainedalloc.Owner { return b.owner }

type originalCaptureOperations struct {
	failure rootpublication.OriginalReleaseFailure
	owner   *retainedalloc.Owner
	file    *os.File
	refs    int
	calls   int
	charge  uint64
}

func (o *originalCaptureOperations) MetadataOwner() *retainedalloc.Owner { return o.owner }
func (o *originalCaptureOperations) Retain() error                       { o.refs++; return nil }
func (o *originalCaptureOperations) Release() (bool, error) {
	o.refs--
	if o.refs != 0 {
		return true, nil
	}
	o.calls++
	err := o.file.Close()
	if err == nil {
		o.file = nil
		o.owner.RemovePending(o.charge)
		o.charge = 0
	} else {
		o.owner.CleanupFailed()
		err = o.failure.RetainOriginalCleanupFailure(o, err)
	}
	return true, err
}
func (*originalCaptureOperations) FlushThrough(*os.File, rootpublication.DurableFrontier) error {
	return nil
}
func (*originalCaptureOperations) SyncThrough(*os.File, rootpublication.DurableFrontier) error {
	return nil
}
func originalFailedCapture(t *testing.T, d *DB) (*stableOuterLeafCapture, error, *originalCaptureOperations) {
	t.Helper()
	registry := rootpublication.NewIdentityPinRegistry()
	owner := registry.MetadataOwner()
	d.backend = &originalCaptureBackend{owner: owner}
	capture, err := newStableOuterLeafCapture(d, &lane{})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "original-capture")
	if err != nil {
		t.Fatal(err)
	}
	identity, e := rootpublication.StableIdentityFromFile(file)
	if e != nil {
		t.Fatal(e)
	}
	if e = registry.Observe(identity); e != nil {
		t.Fatal(e)
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(originalCaptureOperations{})))
	if err = owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	ops := &originalCaptureOperations{owner: owner, file: file, charge: charge, refs: 1}
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
		MetadataOwner: owner, OwnedOperations: ops, PinRegistry: registry, Kind: rootpublication.ResourceIndex,
		LogicalLane: "original", ResourceID: "index.db", Generation: 1, DiagnosticPath: "index.db", File: file,
		Reachability: rootpublication.ReachabilityIndexFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if consumed, e := ops.Release(); !consumed || e != nil {
		t.Fatal(e)
	}
	if err = capture.addToken(token); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	receipt := capture.abandon()
	if !errors.Is(receipt, os.ErrClosed) || !errors.Is(receipt, &ops.failure) || capture.cleanupErr != receipt || capture.metadata != owner || capture.builder != nil || len(capture.tokens) != 1 || capture.tokens[0] != token || ops.calls != 1 || ops.charge == 0 {
		t.Fatalf("original capture/refusal custody lost: %v", receipt)
	}
	bytes := owner.Bytes()
	for i := 0; i < 3; i++ {
		if capture.abandon() != receipt || ops.calls != 1 || owner.Bytes() != bytes {
			t.Fatal("replayed/refunded original capture")
		}
	}
	return capture, receipt, ops
}
func TestOriginalCaptureClosingSiblingCustodyAndReentrantNotification(t *testing.T) {
	prior := errors.New("preexisting unrelated cache error")
	d := &DB{bgErr: prior}
	d.closing.Store(true)
	a, ar, _ := originalFailedCapture(t, d)
	b, br, _ := originalFailedCapture(t, d)
	historic := errors.New("original already linked sibling")
	ar.(rootpublication.OriginalCleanupPriorBinder).LinkPriorCleanupError(historic)
	incoming := fmt.Errorf("original capture graph: %w", errors.Join(ar, br))
	notifications := 0
	d.notifyError = func(err error) {
		notifications++
		retained := d.backgroundError()
		if !errors.Is(retained, err) {
			t.Fatal("notification preceded original retention")
		}
		d.retainOriginalCleanupError(err)
	}
	d.reportError(incoming)
	for _, target := range []error{ar, br, prior, historic, os.ErrClosed} {
		if !errors.Is(d.backgroundError(), target) {
			t.Fatalf("lost sibling %v", target)
		}
	}
	if notifications != 1 || a.metadataCharge == 0 || b.metadataCharge == 0 {
		t.Fatal("closing dropped original custody")
	}
	d.reportError(errors.New("legacy closing report"))
	if notifications != 1 {
		t.Fatal("legacy closing error policy changed")
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 32; j++ {
				d.retainOriginalCleanupError(incoming)
				_ = errors.Is(d.backgroundError(), prior)
			}
		}()
	}
	wg.Wait()
}

type originalCaptureReentrantError struct {
	db            *DB
	cause, nested error
	entered       atomic.Bool
}

func (e *originalCaptureReentrantError) Error() string { return "original capture reentrant wrapper" }
func (e *originalCaptureReentrantError) Unwrap() error { return e.cause }
func (e *originalCaptureReentrantError) Is(error) bool {
	_ = e.db.backgroundError()
	if e.entered.CompareAndSwap(false, true) {
		e.db.retainOriginalCleanupError(e.nested)
	}
	return false
}
func TestOriginalCaptureReentrantTraversalKeepsChangedRoot(t *testing.T) {
	prior := errors.New("original cache prior")
	d := &DB{bgErr: prior}
	_, ar, _ := originalFailedCapture(t, d)
	_, br, _ := originalFailedCapture(t, d)
	incoming := &originalCaptureReentrantError{db: d, cause: ar, nested: br}
	d.reportError(incoming)
	if !incoming.entered.Load() {
		t.Fatal("reentrant discriminator inactive")
	}
	for _, target := range []error{ar, br, prior, os.ErrClosed} {
		if !errors.Is(d.backgroundError(), target) {
			t.Fatalf("changed original root lost %v", target)
		}
	}
}
