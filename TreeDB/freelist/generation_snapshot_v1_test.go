package freelist

import "testing"

func TestSnapshotPageUnusedV1PreservesPinnedRetirementBoundary(t *testing.T) {
	g := MustNewFreelistGenerationV1(9, 20, []uint64{2}, map[uint64]uint64{3: 6, 4: 7, 5: 8})
	g.record.Extents = []ReservationExtentV1{
		{StartPageID: 8, Count: 2, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 6},
		{StartPageID: 12, Count: 2, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 7},
		{StartPageID: 15, Count: 1, Kind: ReservationTargetMetadata},
	}
	for id := uint64(2); id < 20; id++ {
		unused, err := g.SnapshotPageUnusedV1(id, 7)
		want := id == 2 || id == 3 || id == 8 || id == 9
		if err != nil || unused != want {
			t.Fatalf("page %d unused=%v err=%v want %v", id, unused, err, want)
		}
	}
	for _, id := range []uint64{0, 1, 20, ^uint64(0)} {
		if _, err := g.SnapshotPageUnusedV1(id, 7); err == nil {
			t.Fatalf("accepted page %d outside managed extent", id)
		}
	}
	if _, err := g.SnapshotPageUnusedV1(2, 0); err == nil {
		t.Fatal("accepted missing registry horizon")
	}
}
