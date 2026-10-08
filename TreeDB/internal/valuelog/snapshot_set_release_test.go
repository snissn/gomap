package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"testing"
)

func TestSnapshotSetCheckedReleasePreservesOpenLastZombie(t *testing.T) {
	manager, id, path := registrationCellFixture(t)
	set := manager.CurrentSetNoRefresh()
	file := set.Files[id]
	manager.Acquire(set)
	before := file.RefCount.Load()
	if err := manager.ReleaseSnapshotSetChecked(set); err != nil {
		t.Fatal(err)
	}
	if set.RefCount.Load() != 1 || file.RefCount.Load() != before {
		t.Fatal("nonlast Set release touched File reference")
	}
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReleaseSnapshotSetChecked(set); !errors.Is(err, rootpublication.ErrStableMetadataShapeUnsupported) {
		t.Fatalf("live last-zombie release=%v", err)
	}
	if set.RefCount.Load() != 1 || file.RefCount.Load() != before {
		t.Fatal("refusal consumed actual reference")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("refusal performed deletion", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReleaseSnapshotSetChecked(set); err != nil {
		t.Fatal(err)
	}
	if set.RefCount.Load() != 0 || file.RefCount.Load() != before-1 {
		t.Fatal("closed exact File references not balanced")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("checked closed release performed unlink", err)
	}
}
