package caching

import (
	"bytes"
	"testing"
)

func TestFenceSourceRewritePlanSetValueWithOwnership_CollisionKeepsSetIndexAnchor(t *testing.T) {
	targetKey := []byte("target-key")
	collisionKey := []byte("collision-key")
	hash := fenceRewriteKeyHash(targetKey)

	plan := &fenceSourceRewritePlan{
		sets: []fenceSourcePendingSet{
			{key: append([]byte(nil), targetKey...), value: []byte("old-target"), hash: hash},
			{key: append([]byte(nil), collisionKey...), value: []byte("collision"), hash: hash},
		},
		setIndex: map[uint64]int{
			hash: 2,
		},
	}

	plan.setValue(targetKey, []byte("new-target"))

	if got := plan.setIndex[hash]; got != 2 {
		t.Fatalf("setIndex moved to %d; want 2 (most recent slot)", got)
	}
	if got := plan.sets[0].value; !bytes.Equal(got, []byte("new-target")) {
		t.Fatalf("target value=%q want %q", got, []byte("new-target"))
	}
	if len(plan.sets) != 2 {
		t.Fatalf("unexpected set length: got=%d want=2", len(plan.sets))
	}
}

func TestFenceSourceRewritePlanLookupValue_CollisionKeepsSetIndexAnchor(t *testing.T) {
	targetKey := []byte("target-key")
	collisionKey := []byte("collision-key")
	hash := fenceRewriteKeyHash(targetKey)

	plan := &fenceSourceRewritePlan{
		sets: []fenceSourcePendingSet{
			{key: append([]byte(nil), targetKey...), value: []byte("target-value"), hash: hash},
			{key: append([]byte(nil), collisionKey...), value: []byte("collision"), hash: hash},
		},
		setIndex: map[uint64]int{
			hash: 2,
		},
	}

	got, ok := plan.lookupValue(targetKey)
	if !ok {
		t.Fatalf("lookupValue reported miss")
	}
	if !bytes.Equal(got, []byte("target-value")) {
		t.Fatalf("lookupValue=%q want %q", got, []byte("target-value"))
	}
	if got := plan.setIndex[hash]; got != 2 {
		t.Fatalf("setIndex moved to %d; want 2 (most recent slot)", got)
	}
}
