package treedb

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// This discriminator proves Create activation and checkpoint/Close continuity.
// The selected larger proof
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
	closed := false
	t.Cleanup(func() {
		if !closed {
			closed = true
			if err := database.Close(); err != nil {
				t.Errorf("cleanup PUBLIC CLOSE: %v", err)
			}
		}
	})
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
	createdSequence := captured.LogicalSequence()
	if err := captured.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("CAPTURED NATIVE STATE retained and released")
	for pass := 0; pass < 3; pass++ {
		// A real ordinary write forces another physical build while the native
		// catalog is unchanged. Idle checkpoints alone can skip that build.
		if pass == 1 {
			if err := database.Set([]byte("mixed-raw"), []byte("after-native-create")); err != nil {
				t.Fatalf("PUBLIC RAW SET: %v", err)
			}
			t.Log("PUBLIC RAW SET installed")
		}
		wantSequence, wantLSN := createdSequence, uint64(1)
		if pass > 0 {
			wantSequence++
			wantLSN++
			value, err := database.Get([]byte("mixed-raw"))
			if err != nil || string(value) != "after-native-create" {
				t.Fatalf("PUBLIC RAW GET pass %d: value %q, error %v", pass, value, err)
			}
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatalf("PUBLIC CHECKPOINT pass %d: %v", pass, err)
		}
		current, err := database.backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		sequence, hasState := current.LogicalSequence(), current.State() != nil
		basis, ok := current.Snapshot().StateToken()
		if err := current.Close(); err != nil {
			t.Fatal(err)
		}
		if !hasState || sequence != wantSequence || !ok || basis.AppliedCommandLSN != wantLSN {
			t.Fatalf("PUBLIC CHECKPOINT pass %d: native=%v logical=%d want=%d basis=%+v wantLSN=%d", pass, hasState, sequence, wantSequence, basis, wantLSN)
		}
		t.Logf("PUBLIC CHECKPOINT pass %d completed", pass)
	}
	cache := database.cached // retain the original diagnostic owner through Close
	closed = true
	if err := database.Close(); err != nil {
		t.Fatalf("PUBLIC CLOSE: %v", err)
	}
	t.Log("PUBLIC CLOSE completed")
	if got := cache.COWMemoryStats(); got.TotalBytes != 0 || got.Sources != 0 || got.Views != 0 || got.ExternalLeases != 0 || got.Generations != 0 {
		t.Fatalf("original final accounting: %+v", got)
	}
	t.Log("ORIGINAL FINAL ACCOUNTING zero")
}
