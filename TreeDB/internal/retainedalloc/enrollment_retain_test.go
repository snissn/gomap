package retainedalloc_test

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestEnrollmentRetainForOwnerSurvivesOriginalClose(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := enrollmentBudget(t, 1<<20)
	defer budget.Close()
	original, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := original.RetainForOwner(&owner)
	if err != nil {
		original.Close()
		t.Fatal(err)
	}
	original.Close()
	if !retained.BelongsTo(&owner) || budget.Stats().ExternalLeases == 0 {
		t.Fatal("last owned edge did not retain governor")
	}
	if err := owner.AddPending(16); err != nil {
		retained.Close()
		t.Fatal(err)
	}
	owner.RemovePending(16)
	retained.Close()
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Stats().ExternalLeases != 0 {
		t.Fatal("last edge did not refund")
	}
	if again, err := original.RetainForOwner(&owner); again != nil || !errors.Is(err, retainedalloc.ErrClosed) {
		t.Fatalf("closed edge retained: %v %v", again, err)
	}
}

func TestEnrollmentRetainForOwnerRejectsPairAndWrongOwner(t *testing.T) {
	var first, second, other retainedalloc.Owner
	first.Initialize(0)
	second.Initialize(0)
	other.Initialize(0)
	budget := enrollmentBudget(t, 1<<20)
	defer budget.Close()
	pair, err := retainedalloc.EnrollPair(&first, &second, budget)
	if err != nil {
		t.Fatal(err)
	}
	defer pair.Close()
	if edge, err := pair.RetainForOwner(&first); edge != nil || !errors.Is(err, retainedalloc.ErrClosed) {
		t.Fatal("paired edge admitted")
	}
	if edge, err := pair.RetainForOwner(&other); edge != nil || !errors.Is(err, retainedalloc.ErrClosed) {
		t.Fatal("foreign owner admitted")
	}
}

func TestEnrollmentRetainForOwnerConcurrentClose(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := enrollmentBudget(t, 1<<20)
	defer budget.Close()
	original, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	type result struct {
		edge *retainedalloc.Enrollment
		err  error
	}
	done := make(chan result, 1)
	go func() { <-start; edge, err := original.RetainForOwner(&owner); done <- result{edge, err} }()
	close(start)
	original.Close()
	r := <-done
	if r.err == nil {
		if r.edge == nil || !r.edge.BelongsTo(&owner) {
			t.Fatal("successful retain lost its edge")
		}
		r.edge.Close()
	} else if r.edge != nil || !errors.Is(r.err, retainedalloc.ErrClosed) {
		t.Fatalf("unexpected retain: %v %v", r.edge, r.err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Stats().ExternalLeases != 0 {
		t.Fatal("concurrent retain leaked lease")
	}
}
