package collections

import (
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Native semantic admission is shared by modern scalar, text and ColumnGraph owners.
type nativeHNSWAdmissionBudget interface {
	Stats() memtable.COWStats
	Limits() memtable.COWLimits
}

// This fixture tests shared finite-budget admission, independently of vector DDL.
func openPinnedNativeScalarAdmissionFixture(t *testing.T) (*Collection, nativeHNSWAdmissionBudget) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *CollectionManager
	var pins []backenddb.NativeSemanticCapture
	var budget nativeHNSWAdmissionBudget
	t.Cleanup(func() {
		for i := len(pins) - 1; i >= 0; i-- {
			if err := pins[i].Close(); err != nil {
				t.Error(err)
			}
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
		if err := database.Close(); err != nil {
			t.Error(err)
		}
		if budget != nil {
			stats := budget.Stats()
			if stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Errorf("full Close retained admission fixture: %+v", stats)
			}
		}
	})
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	pin := func() {
		t.Helper()
		captured, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		pins = append(pins, captured)
	}
	pin() // OPEN, before the first catalog publication
	budget = pins[0].NativeAllocator().(nativeHNSWAdmissionBudget)
	manager = NewCollectionManager(backend)
	if _, err = manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}}}); err != nil {
		t.Fatal(err)
	}
	pin()
	collection, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	return collection, budget
}

func nativeHNSWAdmissionCaptureBytes(t *testing.T, c *Collection, budget nativeHNSWAdmissionBudget) uint64 {
	t.Helper()
	before := budget.Stats()
	probe, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	if probe.capture.State() == nil || probe.privateEngine != nil {
		probe.Close()
		t.Fatal("admission fixture lacks logical native state")
	}
	during := budget.Stats()
	if err = probe.Close(); err != nil {
		t.Fatal(err)
	}
	after := budget.Stats()
	if before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases {
		t.Fatalf("Capture/Close changed stable setup receipts: before=%+v after=%+v", before, after)
	}
	if during.ReservedBytes <= before.ReservedBytes || during.ExternalLeases <= before.ExternalLeases {
		t.Fatal("capture did not admit independent receipts")
	}
	return during.ReservedBytes - before.ReservedBytes
}

func partitionLivePersistedBytes5275(index *columnGraphPartitionLiveCarrier) int64 {
	index.mu.RLock()
	defer index.mu.RUnlock()
	return index.persistedBytesDisk
}
