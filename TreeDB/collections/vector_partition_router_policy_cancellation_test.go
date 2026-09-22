package collections

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// This deterministic context gives the algorithm a bounded number of polls.
// An uncancellable sort with only entry/exit checks never reaches the third
// poll and incorrectly returns a result in the nearest-width test below.
type policySortPollContextV1 struct {
	context.Context
	calls, cancelAt int
}

func (c *policySortPollContextV1) Err() error {
	c.calls++
	if c.calls >= c.cancelAt {
		return context.Canceled
	}
	return nil
}

func TestVectorPartitionRouterPolicyNearestWidthSortCancellation(t *testing.T) {
	const n = 8192
	makeInput := func() []vectorPartitionPolicyCandidateV1 {
		input := make([]vectorPartitionPolicyCandidateV1, n)
		for i := range input {
			input[i] = vectorPartitionPolicyCandidateV1{Ordinal: i, Domain: uint32(i % 4), Score: float64(i) / n}
		}
		return input
	}
	for _, cancelAt := range []int{1, 3, 5, 12} {
		ctx := &policySortPollContextV1{Context: context.Background(), cancelAt: cancelAt}
		got, err := nearestVectorPartitionPolicyCandidatesV1(ctx, makeInput(), 64)
		if !errors.Is(err, context.Canceled) || got != nil || ctx.calls != cancelAt {
			t.Fatalf("cancelAt=%d calls=%d len=%d err=%v", cancelAt, ctx.calls, len(got), err)
		}
	}
	got, err := nearestVectorPartitionPolicyCandidatesV1(context.Background(), makeInput(), 64)
	if err != nil || len(got) != 64 {
		t.Fatal(len(got), err)
	}
	for i, c := range got {
		if c.Ordinal != n-1-i {
			t.Fatalf("nearest[%d]=%d", i, c.Ordinal)
		}
	}
}

func TestVectorPartitionRouterPolicyReductionCancellationNoPartial(t *testing.T) {
	const n = 8192
	meta := policyTestContext()
	meta.Mode = "exact"
	meta.RepresentativeCount = n
	meta.ScoreBudget = n
	meta.ReturnedWidth = 64
	input := make([]vectorPartitionPolicyCandidateV1, n)
	for i := range input {
		input[i] = vectorPartitionPolicyCandidateV1{Ordinal: i, Domain: uint32(i % 4), Score: float64(i) / n}
	}
	before := append([]vectorPartitionPolicyCandidateV1(nil), input...)
	ctx := &policySortPollContextV1{Context: context.Background(), cancelAt: 80}
	got, err := reduceVectorPartitionRouterPoliciesV1(ctx, meta, input)
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, vectorPartitionPolicyReductionV1{}) {
		t.Fatalf("partial reduction on cancel: %+v %v", got, err)
	}
	if !reflect.DeepEqual(input, before) {
		t.Fatal("cancellation mutated borrowed input")
	}
}
