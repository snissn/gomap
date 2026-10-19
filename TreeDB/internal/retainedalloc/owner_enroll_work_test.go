package retainedalloc_test

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
)

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

func TestEnrollmentEightyGovernorsFutureGrowthAndOrdinaryCaptures(t *testing.T) {
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
	for i := 0; i < 80; i++ {
		if err := o.Add(16); err != nil {
			t.Fatal(err)
		}
		e, err := o.Enroll(budgets[i])
		if err != nil {
			t.Fatal(err)
		}
		e.Close()
		if budgets[i].Stats().ExternalBytes < o.Bytes() {
			t.Fatal("future producer growth uncharged")
		}
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	for _, e := range captures {
		e.Close()
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
func TestEnrollmentRefusalBeforeEffectsAndPairUnwind(t *testing.T) {
	var a, b retainedalloc.Owner
	a.Initialize(256)
	b.Initialize(4096)
	budget := enrollmentBudget(t, 2048)
	before := budget.Stats()
	e, err := retainedalloc.EnrollPair(&a, &b, budget)
	if e != nil || !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatal("refused pair exposed capture", e, err)
	}
	current := budget.Stats()
	current.PeakBytes = before.PeakBytes
	if a.Bytes() != 256 || b.Bytes() != 4096 || current != before {
		t.Fatal("pair unwind lost exact capacity", a.Bytes(), b.Bytes(), current, before)
	}
	budget.Close()
}
func TestEnrollmentFailedFutureGrowthRestoresActualLeaseTargets(t *testing.T) {
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
		budgets = append(budgets, b)
		captures = append(captures, e)
	}
	before := o.Bytes()
	totals := make([]uint64, len(budgets))
	for i, b := range budgets {
		totals[i] = b.Stats().ExternalBytes
	}
	// The first enrolled budget is at the tail, after successful resize steps.
	budgets[0].Close()
	if err := o.Add(16); !errors.Is(err, memtable.ErrCOWClosed) {
		t.Fatal("closed governor accepted growth", err)
	}
	if o.Bytes() != before {
		t.Fatal("failed growth changed owner")
	}
	for i, b := range budgets {
		if b.Stats().ExternalBytes != totals[i] {
			t.Fatal("rollback changed original lease", i, b.Stats().ExternalBytes, totals[i])
		}
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
