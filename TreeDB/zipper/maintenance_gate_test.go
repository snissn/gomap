package zipper

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestZipper_ShouldRunMaintenance_ForceMaintenanceTrue(t *testing.T) {
	z := &Zipper{}
	ops := []batch.Entry{{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v")}}
	got, delCount := z.shouldRunMaintenance(true, ops)
	if delCount != 0 {
		t.Fatalf("expected deleteCount=0, got %d", delCount)
	}
	if !got {
		t.Fatalf("expected maintenance=true when forceMaintenance is true")
	}
}

func TestZipper_ShouldRunMaintenance_HasDeletesTrue(t *testing.T) {
	z := &Zipper{}
	ops := []batch.Entry{{Type: batch.OpDelete, Key: []byte("k")}}
	got, delCount := z.shouldRunMaintenance(false, ops)
	if delCount != 1 {
		t.Fatalf("expected deleteCount=1, got %d", delCount)
	}
	if !got {
		t.Fatalf("expected maintenance=true when batch has deletes")
	}
}

func TestZipper_ShouldRunMaintenance_PurePutsNoForce_NoDeletes_DoesNotTrigger(t *testing.T) {
	z := &Zipper{
		leafReserveBytes:     123, // pretend fill targets are configured
		internalReserveBytes: 456,
	}
	ops := []batch.Entry{
		{Type: batch.OpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Type: batch.OpPut, Key: []byte("k2"), Value: []byte("v2")},
	}

	got, delCount := z.shouldRunMaintenance(false, ops)
	if delCount != 0 {
		t.Fatalf("expected deleteCount=0, got %d", delCount)
	}
	if got {
		t.Fatalf("expected maintenance=false for pure puts when not forced (reserve bytes alone should not trigger maintenance)")
	}
}
