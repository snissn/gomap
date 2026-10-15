package freelist

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"testing"
	"unsafe"
)

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

// The canonical trie can skip most nibbles and can terminate at its root.
// Wrong-key terminal rejection must not spend or inspect the chunk payload.
func TestSnapshotPageUnusedCompressedTrieWorkV1(t *testing.T) {
	const high = uint64(1 << 24)
	g := MustNewFreelistGenerationV1(9, high, []uint64{2, 0x100002, 0x100102}, map[uint64]uint64{0x100103: 7})
	for _, id := range []uint64{2, 0x100002, 0x100102} {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		unused, ready, err := g.SnapshotPageUnusedWithWorkV1(id, 7, w)
		if err != nil || !ready || !unused {
			t.Fatalf("compressed free %x: unused=%v ready=%v err=%v", id, unused, ready, err)
		}
	}
	unused, ready, err := g.pageUnavailableWithWorkV1(0x100103, 7, true, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if err != nil || !ready || !unused {
		t.Fatalf("candidate retired boundary: %v %v %v", unused, ready, err)
	}
	unused, err = g.SnapshotPageUnusedV1(0x100103, 7)
	if err != nil || unused {
		t.Fatalf("snapshot retired boundary: %v %v", unused, err)
	}
	leaf := MustNewFreelistGenerationV1(9, high, []uint64{2}, nil)
	// Exactly the generation and one node are admitted. The wrong terminal key
	// must finish as live without requiring a chunk admission.
	bytes := 2*uint64(unsafe.Sizeof(FreelistGenerationV1{})) + uint64(unsafe.Sizeof(stateNode{}))
	w := &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: bytes}
	unused, ready, err = leaf.SnapshotPageUnusedWithWorkV1(0x10002, 7, w)
	if err != nil || !ready || unused || w.Records != 2 {
		t.Fatalf("wrong terminal inspected chunk: unused=%v ready=%v err=%v work=%+v", unused, ready, err, w)
	}
	w = &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: bytes}
	unused, ready, err = leaf.SnapshotPageUnusedWithWorkV1(2, 7, w)
	if err != nil || ready || unused {
		t.Fatalf("unadmitted exact chunk: unused=%v ready=%v err=%v", unused, ready, err)
	}
}
