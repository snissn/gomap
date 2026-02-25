package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
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

func TestFenceAnchorPromoterPutRewriteEntries_ClearsRetainedReferences(t *testing.T) {
	p := &fenceAnchorPromoter{}
	entries := make([]outerleaf.TypedEntry, 0, 8)
	entries = append(entries,
		outerleaf.TypedEntry{Key: []byte("k1"), Kind: outerleaf.EntryKindInline, Value: []byte("v1")},
		outerleaf.TypedEntry{Key: []byte("k2"), Kind: outerleaf.EntryKindInline, Value: []byte("v2")},
	)

	p.putRewriteEntries(entries)

	if p.rewriteEntries == nil {
		t.Fatalf("rewriteEntries dropped unexpectedly")
	}
	if len(p.rewriteEntries) != 0 || cap(p.rewriteEntries) != cap(entries) {
		t.Fatalf("rewriteEntries shape mismatch: len=%d cap=%d want len=0 cap=%d", len(p.rewriteEntries), cap(p.rewriteEntries), cap(entries))
	}
	full := p.rewriteEntries[:cap(p.rewriteEntries)]
	for i := range full {
		if full[i].Key != nil || full[i].Value != nil {
			t.Fatalf("entry %d retained key/value references", i)
		}
	}
}

func TestFenceAnchorPromoterPutRewriteEntries_DropsOversizedBuffer(t *testing.T) {
	p := &fenceAnchorPromoter{}
	entries := make([]outerleaf.TypedEntry, 0, maxFenceRewriteEntriesRetain+1)
	entries = append(entries, outerleaf.TypedEntry{Key: []byte("k"), Kind: outerleaf.EntryKindInline, Value: []byte("v")})

	p.putRewriteEntries(entries)

	if p.rewriteEntries != nil {
		t.Fatalf("expected oversized rewriteEntries buffer to be dropped, got len=%d cap=%d", len(p.rewriteEntries), cap(p.rewriteEntries))
	}
}
