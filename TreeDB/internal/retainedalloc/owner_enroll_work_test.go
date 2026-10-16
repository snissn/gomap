package retainedalloc_test

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
)

func enrollmentQuantum() *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
}
func enrollmentBudget(t *testing.T, total uint64) *memtable.COWBudget {
	t.Helper()
	l := memtable.DefaultCOWLimits()
	l.MaxTotalBytes = total
	l.MaxGenerationBytes = total
	l.MaxRetiredBytes = total
	l.MaxInFlightBytes = total
	b, err := memtable.NewCOWBudget(l)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestEnrollmentWorkEightyGovernorsFutureGrowthAndOrdinaryCaptures(t *testing.T) {
	var o retainedalloc.Owner
	o.Initialize(256)
	var budgets []*memtable.COWBudget
	var captures []*retainedalloc.Enrollment
	for i := 0; i < 80; i++ {
		b := enrollmentBudget(t, 1<<20)
		e, err := o.Enroll(b)
		if err != nil {
			t.Fatal(err)
		}
		budgets = append(budgets, b)
		captures = append(captures, e)
	}
	b := enrollmentBudget(t, 1<<20)
	budgets = append(budgets, b)
	var state retainedalloc.EnrollmentWork
	var result *retainedalloc.Enrollment
	calls := 0
	for ; calls < 256; calls++ {
		w := enrollmentQuantum()
		e, ready, err := o.EnrollWithWork(b, &state, w)
		if err != nil {
			t.Fatal(err)
		}
		if w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("quantum exceeded")
		}
		if ready {
			result = e
			break
		}
		// Supported ordinary consumers keep succeeding at every actual yield.
		ordinary, err := o.Enroll(b)
		if err != nil {
			t.Fatal("ordinary capture at yield", err)
		}
		ordinary.Close()
		if err = o.Add(16); err != nil {
			t.Fatal("future producer growth", err)
		}
		if calls < len(captures) {
			captures[calls].Close()
			captures[calls] = nil
		}
	}
	if result == nil || calls < 2 || calls == 256 {
		t.Fatal("bounded enrollment did not consume actual large binding frontier", calls)
	}
	for _, e := range captures {
		if e != nil {
			e.Close()
		}
	}
	result.Close()
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	for _, b := range budgets {
		b.Close()
		if st := b.Stats(); st.TotalBytes != 0 || st.ExternalLeases != 0 {
			t.Fatal("governor leaked", st)
		}
	}
	if o.Bytes() != 0 {
		t.Fatal("owner leaked", o.Bytes())
	}
}
func TestEnrollmentWorkRefusalBeforeEffectsAndPairUnwind(t *testing.T) {
	var a, b retainedalloc.Owner
	a.Initialize(256)
	b.Initialize(4096)
	budget := enrollmentBudget(t, 2048)
	before := budget.Stats()
	var single retainedalloc.EnrollmentWork
	w := &iterator.OrdinalScanWork{RecordLimit: 0, ByteLimit: 1 << 20}
	if e, ready, err := a.EnrollWithWork(budget, &single, w); e != nil || ready || err != nil {
		t.Fatal(e, ready, err)
	}
	if a.Bytes() != 256 || budget.Stats() != before {
		t.Fatal("unadmitted effects")
	}
	var pair retainedalloc.EnrollmentPairWork
	done := false
	for i := 0; i < 128; i++ {
		e, ready, err := retainedalloc.EnrollPairWithWork(&a, &b, budget, &pair, enrollmentQuantum())
		if e != nil {
			t.Fatal("refused pair exposed capture")
		}
		if ready {
			if !errors.Is(err, memtable.ErrCOWCapacity) {
				t.Fatal(err)
			}
			done = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	current := budget.Stats()
	// Peak records the legitimate first-owner admission before second refusal.
	current.PeakBytes = before.PeakBytes
	if !done || a.Bytes() != 256 || b.Bytes() != 4096 || current != before {
		t.Fatal("pair unwind lost exact capacity", done, a.Bytes(), b.Bytes(), budget.Stats(), before)
	}
	if e, ready, err := retainedalloc.EnrollPairWithWork(&a, &b, budget, &pair, enrollmentQuantum()); e != nil || !ready || !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatal("terminal retry changed outcome", e, ready, err)
	}
	budget.Close()
}
func TestEnrollmentWorkCancelAndPhysicalCloseAcrossYield(t *testing.T) {
	for _, closeOwner := range []bool{false, true} {
		var o retainedalloc.Owner
		o.Initialize(256)
		var captures []*retainedalloc.Enrollment
		var budgets []*memtable.COWBudget
		for i := 0; i < 80; i++ {
			b := enrollmentBudget(t, 1<<20)
			e, err := o.Enroll(b)
			if err != nil {
				t.Fatal(err)
			}
			captures = append(captures, e)
			budgets = append(budgets, b)
		}
		target := enrollmentBudget(t, 1<<20)
		budgets = append(budgets, target)
		var state retainedalloc.EnrollmentWork
		if _, ready, err := o.EnrollWithWork(target, &state, enrollmentQuantum()); ready || err != nil {
			t.Fatal(ready, err)
		}
		if closeOwner {
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
		}
		done := false
		for i := 0; i < 128; i++ {
			ready, err := state.CancelWithWork(enrollmentQuantum())
			if ready {
				if !errors.Is(err, retainedalloc.ErrClosed) {
					t.Fatal(err)
				}
				done = true
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if !done {
			t.Fatal("cancel did not complete")
		}
		for _, e := range captures {
			e.Close()
		}
		if err := o.Close(); err != nil {
			t.Fatal(err)
		}
		for _, b := range budgets {
			b.Close()
			if st := b.Stats(); st.TotalBytes != 0 {
				t.Fatal("cancel leaked", st)
			}
		}
	}
}

func TestEnrollmentWorkFailedFutureGrowthRestoresActualLeaseTargets(t *testing.T) {
	var o retainedalloc.Owner
	o.Initialize(256)
	var captures []*retainedalloc.Enrollment
	var budgets []*memtable.COWBudget
	for i := 0; i < 80; i++ {
		b := enrollmentBudget(t, 1<<20)
		e, err := o.Enroll(b)
		if err != nil {
			t.Fatal(err)
		}
		captures = append(captures, e)
		budgets = append(budgets, b)
	}
	target := enrollmentBudget(t, 1<<20)
	budgets = append(budgets, target)
	var state retainedalloc.EnrollmentWork
	// Small actual quanta enter partial lease resizing before the closed-tail
	// future Add. Different original bindings now have different admitted targets.
	reserved := false
	initial := o.Bytes()
	for i := 0; i < 256; i++ {
		_, ready, err := o.EnrollWithWork(target, &state, &iterator.OrdinalScanWork{RecordLimit: 8, ByteLimit: 1 << 20})
		if err != nil || ready {
			t.Fatal("unexpected early completion", ready, err)
		}
		if o.Bytes() > initial {
			reserved = true
			break
		}
	}
	if !reserved {
		t.Fatal("unallocated control reservation absent")
	}
	budgets[0].Close()
	done := false
	for i := 0; i < 256; i++ {
		before := o.Bytes()
		totals := make([]uint64, len(budgets))
		for j, b := range budgets {
			totals[j] = b.Stats().ExternalBytes
		}
		if err := o.Add(16); !errors.Is(err, memtable.ErrCOWClosed) {
			t.Fatal("closed original governor accepted future growth", err)
		}
		if o.Bytes() != before {
			t.Fatal("failed future growth changed owner")
		}
		for j, b := range budgets {
			if b.Stats().ExternalBytes != totals[j] {
				t.Fatal("rollback did not restore original lease target", j, b.Stats().ExternalBytes, totals[j])
			}
		}
		_, ready, err := o.EnrollWithWork(target, &state, enrollmentQuantum())
		if ready {
			if !errors.Is(err, memtable.ErrCOWClosed) {
				t.Fatal(err)
			}
			done = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !done || o.Bytes() != initial {
		t.Fatal("failed staged admission leaked unallocated controls", done, o.Bytes(), initial)
	}
	for _, e := range captures {
		e.Close()
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	for _, b := range budgets {
		b.Close()
		if b.Stats().TotalBytes != 0 {
			t.Fatal("failed growth leaked governor", b.Stats())
		}
	}
}

func TestEnrollmentWorkConstructorCustodySurvivesOriginalClose(t *testing.T) {
	var o retainedalloc.Owner
	o.Initialize(4096)
	b := enrollmentBudget(t, 1<<20)
	var state retainedalloc.EnrollmentWork
	// This is the real constructor-only seam called under capture exclusion,
	// before Snapshot can acquire original physical borrows.
	ready, err := o.BeginEnrollmentWithWork(b, &state, &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20})
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	if st := b.Stats(); st.ExternalLeases != 1 || st.ExternalBytes < 4096 {
		t.Fatal("source carried without original lease", st)
	}
	if err := o.Add(1024); err != nil {
		t.Fatal(err)
	}
	if b.Stats().ExternalBytes < o.Bytes() {
		t.Fatal("future source growth uncharged")
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	if o.Bytes() != 4096 || b.Stats().ExternalBytes < o.Bytes() {
		t.Fatal("Close refunded pending source header", o.Bytes(), b.Stats())
	}
	// Actual zero-work cancellation refuses with original capacity/custody intact.
	before := b.Stats()
	if done, err := state.CancelWithWork(&iterator.OrdinalScanWork{ByteLimit: 1 << 20}); done || err != nil || b.Stats() != before {
		t.Fatal(done, err, b.Stats())
	}
	done := false
	for i := 0; i < 128; i++ {
		ready, err := state.CancelWithWork(enrollmentQuantum())
		if ready {
			if !errors.Is(err, retainedalloc.ErrClosed) {
				t.Fatal(err)
			}
			done = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !done || o.Bytes() != 0 || b.Stats().ExternalLeases != 0 {
		t.Fatal("original custody survived terminal cancellation", done, o.Bytes(), b.Stats())
	}
	b.Close()
	if b.Stats().TotalBytes != 0 {
		t.Fatal(b.Stats())
	}
}

func TestEnrollmentWorkPromotedConstructorCancelAfterPhysicalClose(t *testing.T) {
	var o retainedalloc.Owner
	o.Initialize(4096)
	b := enrollmentBudget(t, 1<<20)
	var state retainedalloc.EnrollmentWork
	ready, err := o.BeginEnrollmentWithWork(b, &state, enrollmentQuantum())
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	ordinary, err := o.Enroll(b)
	if err != nil {
		t.Fatal(err)
	}
	ordinary.Close()
	if err = o.Close(); err != nil {
		t.Fatal(err)
	}
	done := false
	for i := 0; i < 128; i++ {
		ready, err := state.CancelWithWork(enrollmentQuantum())
		if ready {
			if !errors.Is(err, retainedalloc.ErrClosed) {
				t.Fatal(err)
			}
			done = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !done || o.Bytes() != 0 || b.Stats().ExternalLeases != 0 {
		t.Fatal("promoted constructor lost terminal disposal", done, o.Bytes(), b.Stats())
	}
	b.Close()
	if b.Stats().TotalBytes != 0 {
		t.Fatal(b.Stats())
	}
}
