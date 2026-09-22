package db

import "testing"

func valueLogRefDeltaSnapshot(t *testing.T, d *valueLogRefDelta) map[uint32]int64 {
	t.Helper()
	out := make(map[uint32]int64)
	if err := d.forEachChange(func(fileID uint32, change int64) error {
		out[fileID] = change
		return nil
	}); err != nil {
		t.Fatalf("forEachChange: %v", err)
	}
	return out
}

func TestValueLogRefDelta_InlineThenMapPromotion(t *testing.T) {
	d := newValueLogRefDelta()
	if d == nil {
		t.Fatalf("newValueLogRefDelta returned nil")
	}
	if d.changes != nil {
		t.Fatalf("unexpected eager map allocation")
	}

	for i := 0; i < len(d.inline); i++ {
		fileID := uint32(i + 1)
		d.add(fileID, int64(10+i))
	}
	if d.changes != nil {
		t.Fatalf("delta promoted map too early")
	}

	// Overflow inline capacity -> map promotion.
	d.add(999, 1)
	if d.changes == nil {
		t.Fatalf("expected map promotion after inline overflow")
	}

	snap := valueLogRefDeltaSnapshot(t, d)
	for i := 0; i < len(d.inline); i++ {
		fileID := uint32(i + 1)
		want := int64(10 + i)
		if got := snap[fileID]; got != want {
			t.Fatalf("file %d change: got=%d want=%d", fileID, got, want)
		}
	}
	if got := snap[999]; got != 1 {
		t.Fatalf("file 999 change: got=%d want=1", got)
	}
}

func TestValueLogRefDelta_AddCancelsToZero(t *testing.T) {
	d := newValueLogRefDelta()

	d.add(7, 3)
	d.add(7, -1)
	d.add(7, -2)

	snap := valueLogRefDeltaSnapshot(t, d)
	if len(snap) != 0 {
		t.Fatalf("expected empty delta after zero-cancel, got=%v", snap)
	}

	// Verify cancellation also works after map promotion.
	for i := 0; i < len(d.inline); i++ {
		d.add(uint32(i+100), 1)
	}
	d.add(500, 5) // promote
	d.add(500, -5)

	snap = valueLogRefDeltaSnapshot(t, d)
	if _, ok := snap[500]; ok {
		t.Fatalf("expected promoted key cancellation to remove entry: %v", snap)
	}
}

func TestValueLogRefDelta_MergePreservesPositiveAppendAccounting(t *testing.T) {
	added := newValueLogRefDelta()
	added.add(7, 3)
	removed := newValueLogRefDelta()
	removed.add(7, -3)
	merged := newValueLogRefDelta()
	for _, delta := range []*valueLogRefDelta{added, removed} {
		if err := delta.forEachChange(func(fileID uint32, change int64) error {
			merged.addChange(fileID, change)
			return nil
		}); err != nil {
			t.Fatalf("forEachChange: %v", err)
		}
		if err := delta.forEachPositive(func(fileID uint32, count int64) error {
			merged.addPositive(fileID, count)
			return nil
		}); err != nil {
			t.Fatalf("forEachPositive: %v", err)
		}
	}
	if changes := valueLogRefDeltaSnapshot(t, merged); len(changes) != 0 {
		t.Fatalf("merged net changes=%v want none", changes)
	}
	positives := make(map[uint32]int64)
	if err := merged.forEachPositive(func(fileID uint32, count int64) error {
		positives[fileID] = count
		return nil
	}); err != nil {
		t.Fatalf("merged forEachPositive: %v", err)
	}
	if len(positives) != 1 || positives[7] != 3 {
		t.Fatalf("merged positives=%v want map[7:3]", positives)
	}
}
