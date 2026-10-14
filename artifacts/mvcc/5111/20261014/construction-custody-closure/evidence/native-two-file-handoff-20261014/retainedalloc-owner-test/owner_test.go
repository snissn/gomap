package retainedalloc

import (
	"errors"
	"testing"
)

type testBudget struct {
	cap, bytes uint64
	closed     bool
}
type testLease struct {
	b      *testBudget
	bytes  uint64
	closed bool
}

func (b *testBudget) AcquireRetention(n uint64) (Lease, error) {
	if b.closed || n > b.cap-b.bytes {
		return nil, ErrCapacity
	}
	b.bytes += n
	return &testLease{b: b, bytes: n}, nil
}
func (l *testLease) Resize(n uint64) error {
	if l.closed {
		return ErrClosed
	}
	if n > l.bytes && (l.b.closed || n-l.bytes > l.b.cap-l.b.bytes) {
		return ErrCapacity
	}
	l.b.bytes = l.b.bytes - l.bytes + n
	l.bytes = n
	return nil
}
func (l *testLease) Close() {
	if !l.closed {
		l.closed = true
		l.b.bytes -= l.bytes
		l.bytes = 0
	}
}
func TestOwnerSharedDistinctAdmissionAndFailureUnwind(t *testing.T) {
	var o Owner
	o.Initialize(256)
	a := &testBudget{cap: 4096}
	b := &testBudget{cap: 512}
	e1, err := o.Enroll(a)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := o.Enroll(a)
	if err != nil {
		t.Fatal(err)
	}
	before := o.Bytes()
	if _, err = o.Enroll(&testBudget{cap: 1}); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if o.Bytes() != before || a.bytes != before {
		t.Fatal("failed enrollment changed charge")
	}
	eb, err := o.Enroll(b)
	if err != nil {
		t.Fatal(err)
	}
	before = o.Bytes()
	if err = o.Add(1024); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if o.Bytes() != before || a.bytes != before || b.bytes != before {
		t.Fatal("partial growth admission leaked")
	}
	e1.Close()
	if a.bytes == 0 {
		t.Fatal("shared capture released governor")
	}
	e2.Close()
	if a.bytes != 0 {
		t.Fatal("last successful capture retained governor")
	}
	if err = o.Add(16); err != nil {
		t.Fatal(err)
	}
	eb.Close()
	if b.bytes != 0 {
		t.Fatal("distinct governor leaked")
	}
}
func TestOwnerRetiredOrFailedCleanupKeepsExactGovernor(t *testing.T) {
	for _, retired := range []bool{false, true} {
		var o Owner
		o.Initialize(128)
		b := &testBudget{cap: 4096}
		e, err := o.Enroll(b)
		if err != nil {
			t.Fatal(err)
		}
		if err = o.Add(512); err != nil {
			t.Fatal(err)
		}
		if retired {
			o.Retire()
			e.Close()
		} else {
			e.CloseAfterCleanup(errors.New("remaining physical storage"))
		}
		if b.bytes == 0 {
			t.Fatal("residual storage lost governor")
		}
		b.closed = true
		if err = o.Add(16); !errors.Is(err, ErrCapacity) {
			t.Fatal("closed governor allowed growth", err)
		}
		if err = o.Close(); err != nil {
			t.Fatal(err)
		}
		if b.bytes != 0 || o.Bytes() != 0 {
			t.Fatal("actual cleanup did not refund")
		}
	}
}
func TestOwnerPhysicalCloseWithActiveCaptureKeepsFixedOwner(t *testing.T) {
	var o Owner
	o.Initialize(256, 128)
	b := &testBudget{cap: 4096}
	e, err := o.Enroll(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.Close(); err != nil {
		t.Fatal(err)
	}
	if b.bytes == 0 {
		t.Fatal("active capture lost fixed owner")
	}
	e.Close()
	if b.bytes != 0 || o.Bytes() != 0 {
		t.Fatal("last capture leaked")
	}
}

func TestOwnerPhysicalCloseKeepsPendingTailThroughLastCapture(t *testing.T) {
	var o Owner
	o.Initialize(256, 128)
	b := &testBudget{cap: 4096}
	e, err := o.Enroll(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.AddPending(512); err != nil {
		t.Fatal(err)
	}
	if err = o.Close(); err != nil {
		t.Fatal(err)
	}
	e.Close()
	if b.bytes < 512 || o.Bytes() < 512 {
		t.Fatal("pending tail refunded with capture")
	}
	if err = o.AddPending(16); !errors.Is(err, ErrClosed) {
		t.Fatal("closed owner grew", err)
	}
	o.RemovePending(512)
	if b.bytes != 0 || o.Bytes() != 0 {
		t.Fatal("actual tail cleanup leaked", b.bytes, o.Bytes())
	}
}

func TestOwnerPairBorrowProducerAndRollback(t *testing.T) {
	var a, b Owner
	a.Initialize(256)
	b.Initialize(512)
	budget := &testBudget{cap: 1 << 20}
	beforeA, beforeB := a.Bytes(), b.Bytes()
	budget.cap = beforeA + AllocationCharge(64) + AllocationCharge(128)
	if _, err := EnrollPair(&a, &b, budget); err == nil {
		t.Fatal("second owner refusal missing")
	}
	if budget.bytes != 0 || a.Bytes() != beforeA || b.Bytes() != beforeB {
		t.Fatal("pair refusal leaked first ownership")
	}
	budget.cap = 1 << 20
	borrow, err := EnrollPair(&a, &b, budget)
	if err != nil {
		t.Fatal(err)
	}
	if !borrow.BelongsToPair(&a, &b) || borrow.BelongsToPair(&b, &a) {
		t.Fatal("pair identity")
	}
	borrow.Close()
	if budget.bytes != 0 || a.Bytes() != beforeA || b.Bytes() != beforeB {
		t.Fatal("temporary borrow anchored backing")
	}
	producer, err := EnrollPair(&a, &b, budget)
	if err != nil {
		t.Fatal(err)
	}
	alias, err := EnrollPair(&a, &b, budget)
	if err != nil {
		t.Fatal(err)
	}
	if err = producer.ActivateProducer(); err != nil {
		t.Fatal(err)
	}
	producer.Close()
	alias.Close()
	if budget.bytes == 0 {
		t.Fatal("capture release discarded producer authority")
	}
	budget.closed = true
	n := b.Bytes()
	if err = b.Add(128); err == nil || b.Bytes() != n {
		t.Fatal("closed producer accepted growth")
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.bytes == 0 {
		t.Fatal("first disposal erased distinct live owner")
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.bytes != 0 {
		t.Fatal("actual two-owner disposal leaked charge")
	}
}
