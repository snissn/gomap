package treedb

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"testing"
)

// This discriminator proves Create activation only. The selected larger proof
// still requires InsertBatch/Get, held old reads, newer suffix handoff, canonical
// WAL crash recovery, and recovered native catalog bootstrap.
func TestNativeJSONPublicCreateActivation(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 2
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	manager, err := collections.NewNativeJSONManager(database.backend)
	if err != nil {
		t.Fatal(err)
	}
	refused := collections.NewCollectionManager(database.backend)
	if _, err := refused.CreateCollection(&collections.CollectionMeta{Name: "legacy"}); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("legacy admission: %v", err)
	}
	if _, _, err := refused.ListCollectionsBounded(1); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("legacy list: %v", err)
	}
	if err := refused.Close(); err != nil {
		t.Fatal(err)
	}
	created, err := manager.CreateCollection(collections.CollectionMeta{Name: "native-json"})
	if err != nil {
		t.Fatalf("FIRST PUBLIC CREATE: %v", err)
	}
	if created.Name != "native-json" {
		t.Fatal(created)
	}
	t.Log("FIRST PUBLIC CREATE installed")
	captured, err := database.backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	if captured.State() == nil || captured.LogicalSequence() == 0 {
		t.Fatal("Create did not install native engine at a logical sequence")
	}
	if err := captured.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("CAPTURED NATIVE STATE retained and released")
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("PUBLIC CHECKPOINT: %v", err)
	}
	t.Log("PUBLIC CHECKPOINT completed")
	if err := database.Close(); err != nil {
		t.Fatalf("PUBLIC CLOSE: %v", err)
	}
	t.Log("PUBLIC CLOSE completed")
	for _, key := range []string{"total_bytes", "sources", "views", "external_leases", "generations"} {
		if got := database.cached.COWMemoryStats(); got.TotalBytes != 0 || got.Sources != 0 || got.Views != 0 || got.ExternalLeases != 0 || got.Generations != 0 {
			t.Fatalf("final %s: %+v", key, got)
		}
	}
}
