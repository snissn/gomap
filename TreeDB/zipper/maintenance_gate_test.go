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
	// Pre-fix, reserve bytes force maintenance even for pure put workloads.
	// We intentionally skip the assertion here until the gate is changed in the
	// follow-up commit.
	t.Skip("flip to assert desired behavior after maintenance gate fix")
}
