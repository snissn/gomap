package raftplacement

import (
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestPlanTokenRingDistributesVirtualPartitions(t *testing.T) {
	catalog, err := Validate(validCatalog())
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	plan, err := PlanTokenRing(catalog, TokenRingPlanOptions{PartitionCount: 8})
	if err != nil {
		t.Fatalf("PlanTokenRing: %v", err)
	}
	if got := len(plan.Partitions); got != 8 {
		t.Fatalf("partitions=%d want 8", got)
	}
	if plan.Partitions[0].Start != 0 {
		t.Fatalf("first partition starts at %d want 0", plan.Partitions[0].Start)
	}
	if plan.Partitions[len(plan.Partitions)-1].End != maxTokenV1 {
		t.Fatalf("last partition ends at %d want %d", plan.Partitions[len(plan.Partitions)-1].End, maxTokenV1)
	}
	wantCounts := map[raftcluster.GroupID]int{"group-a": 4, "group-b": 4}
	if got := plan.GroupPartitionCounts(); !reflect.DeepEqual(got, wantCounts) {
		t.Fatalf("group counts=%v want %v", got, wantCounts)
	}

	firstEnd := plan.Partitions[0].End
	assertTokenGroup(t, plan, 0, "token-000000", "group-a")
	assertTokenGroup(t, plan, firstEnd+1, "token-000001", "group-b")
	assertTokenGroup(t, plan, maxTokenV1, "token-000007", "group-b")
}

func TestPlanTokenRingRejectsInvalidInputs(t *testing.T) {
	catalog, err := Validate(validCatalog())
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	_, err = PlanTokenRing(catalog, TokenRingPlanOptions{})
	if !errors.Is(err, ErrInvalidTokenPartitionCount) {
		t.Fatalf("PlanTokenRing zero partitions err=%v want ErrInvalidTokenPartitionCount", err)
	}
	if !errors.Is(err, ErrInvalidTokenRing) {
		t.Fatalf("PlanTokenRing zero partitions err=%v want ErrInvalidTokenRing", err)
	}

	_, err = PlanTokenRing(ResolvedCatalogV1{}, TokenRingPlanOptions{PartitionCount: 1})
	if !errors.Is(err, ErrMissingGroup) {
		t.Fatalf("PlanTokenRing empty catalog err=%v want ErrMissingGroup", err)
	}
	if !errors.Is(err, ErrInvalidTokenRing) {
		t.Fatalf("PlanTokenRing empty catalog err=%v want ErrInvalidTokenRing", err)
	}
}

func TestValidateTokenRingPlanRejectsInvalidPlans(t *testing.T) {
	catalog, err := Validate(validCatalog())
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	tests := []struct {
		name string
		plan func() TokenRingPlanV1
		want error
	}{
		{
			name: "missing partitions",
			plan: func() TokenRingPlanV1 {
				return TokenRingPlanV1{}
			},
			want: ErrMissingTokenPartition,
		},
		{
			name: "invalid partition id",
			plan: func() TokenRingPlanV1 {
				plan := validTwoPartitionTokenPlan()
				plan.Partitions[0].ID = "bad/name"
				return plan
			},
			want: ErrInvalidTokenPartition,
		},
		{
			name: "duplicate partition id",
			plan: func() TokenRingPlanV1 {
				plan := validTwoPartitionTokenPlan()
				plan.Partitions[1].ID = plan.Partitions[0].ID
				return plan
			},
			want: ErrDuplicateTokenPartition,
		},
		{
			name: "unknown group",
			plan: func() TokenRingPlanV1 {
				plan := validTwoPartitionTokenPlan()
				plan.Partitions[0].GroupID = "group-z"
				return plan
			},
			want: ErrUnknownGroup,
		},
		{
			name: "invalid range",
			plan: func() TokenRingPlanV1 {
				plan := validTwoPartitionTokenPlan()
				plan.Partitions[0].Start = 10
				plan.Partitions[0].End = 9
				return plan
			},
			want: ErrInvalidTokenRange,
		},
		{
			name: "gap at start",
			plan: func() TokenRingPlanV1 {
				plan := validTwoPartitionTokenPlan()
				plan.Partitions[0].Start = 1
				return plan
			},
			want: ErrTokenRangeGap,
		},
		{
			name: "overlap",
			plan: func() TokenRingPlanV1 {
				plan := validTwoPartitionTokenPlan()
				plan.Partitions[1].Start = plan.Partitions[0].End
				return plan
			},
			want: ErrTokenRangeOverlap,
		},
		{
			name: "middle gap",
			plan: func() TokenRingPlanV1 {
				plan := validTwoPartitionTokenPlan()
				plan.Partitions[1].Start = plan.Partitions[0].End + 2
				return plan
			},
			want: ErrTokenRangeGap,
		},
		{
			name: "not full",
			plan: func() TokenRingPlanV1 {
				plan := validTwoPartitionTokenPlan()
				plan.Partitions[1].End = maxTokenV1 - 1
				return plan
			},
			want: ErrTokenRingNotFull,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateTokenRingPlan(catalog, tc.plan())
			if !errors.Is(err, tc.want) {
				t.Fatalf("ValidateTokenRingPlan err=%v want errors.Is(%v)", err, tc.want)
			}
			if !errors.Is(err, ErrInvalidTokenRing) {
				t.Fatalf("ValidateTokenRingPlan err=%v want ErrInvalidTokenRing", err)
			}
		})
	}
}

func TestValidateTokenRingPlanSortsAndProtectsResolvedPlan(t *testing.T) {
	catalog, err := Validate(validCatalog())
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	plan := validTwoPartitionTokenPlan()
	plan.Partitions[0], plan.Partitions[1] = plan.Partitions[1], plan.Partitions[0]
	resolved, err := ValidateTokenRingPlan(catalog, plan)
	if err != nil {
		t.Fatalf("ValidateTokenRingPlan: %v", err)
	}
	if got := resolved.Partitions[0].ID; got != "token-000000" {
		t.Fatalf("first partition id=%q want token-000000", got)
	}
	resolved.Partitions[0].GroupID = "group-z"
	assertTokenGroup(t, resolved, 0, "token-000000", "group-a")
}

func validTwoPartitionTokenPlan() TokenRingPlanV1 {
	split := maxTokenV1 / 2
	return TokenRingPlanV1{
		Partitions: []TokenPartitionV1{
			{ID: "token-000000", GroupID: "group-a", Start: 0, End: split},
			{ID: "token-000001", GroupID: "group-b", Start: split + 1, End: maxTokenV1},
		},
	}
}

func assertTokenGroup(t *testing.T, plan ResolvedTokenRingPlanV1, token uint64, wantPartition TokenPartitionID, wantGroup raftcluster.GroupID) {
	t.Helper()
	partition, err := plan.ResolveToken(token)
	if err != nil {
		t.Fatalf("ResolveToken(%d): %v", token, err)
	}
	if partition.ID != wantPartition || partition.GroupID != wantGroup {
		t.Fatalf("ResolveToken(%d)=(%q,%q) want (%q,%q)", token, partition.ID, partition.GroupID, wantPartition, wantGroup)
	}
}
