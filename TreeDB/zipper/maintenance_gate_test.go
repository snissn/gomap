package zipper

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestZipper_ShouldRunMaintenance_HasDeletesTrue(t *testing.T) {
	z := &Zipper{}
	ops := []batch.Entry{{Type: batch.OpDelete, Key: []byte("k")}}
	got, delCount := z.shouldRunMaintenance(ops)
	if delCount != 1 {
		t.Fatalf("expected deleteCount=1, got %d", delCount)
	}
	if !got {
		t.Fatalf("expected maintenance=true when batch has deletes")
	}
}

func TestZipper_ShouldRunMaintenance_PiggybackDoesNotTrigger(t *testing.T) {
	z := &Zipper{piggybackCompaction: true}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v")}}
	got, delCount := z.shouldRunMaintenance(ops)
	if delCount != 0 {
		t.Fatalf("expected deleteCount=0, got %d", delCount)
	}
	if got {
		t.Fatalf("expected maintenance=false for pure puts (piggyback should not force maintenance)")
	}
}

func TestZipper_ShouldRunMaintenance_ReserveBytesDoesNotTrigger(t *testing.T) {
	z := &Zipper{leafReserveBytes: 1}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v")}}
	got, delCount := z.shouldRunMaintenance(ops)
	if delCount != 0 {
		t.Fatalf("expected deleteCount=0, got %d", delCount)
	}
	if got {
		t.Fatalf("expected maintenance=false for pure puts (reserve bytes should not force maintenance)")
	}
}

func TestZipper_ShouldRunMaintenance_PurePutsNoReservesNoPiggybackFalse(t *testing.T) {
	z := &Zipper{}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v")}}
	got, delCount := z.shouldRunMaintenance(ops)
	if delCount != 0 {
		t.Fatalf("expected deleteCount=0, got %d", delCount)
	}
	if got {
		t.Fatalf("expected maintenance=false for pure puts with no reserves/piggyback")
	}
}
