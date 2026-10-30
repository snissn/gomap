package treedb

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// A failed publication boundary must reach its caller and must not discard an
// already acknowledged COW write. The test exhausts real maintenance
// admission at the owner seam, rather than substituting a fake storage owner.
func TestCOWCheckpointErrorPropagation(t *testing.T) {
	for _, atClose := range []bool{false, true} {
		name := "checkpoint"
		if atClose {
			name = "close"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opts := OptionsFor(ProfileCommandWALDurable, dir)
			opts.MemtableMode = "cow_btree"
			opts.FlushThreshold = 1 << 30
			opts.BackgroundCheckpointInterval = -1
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					if err := database.Close(); err != nil {
						t.Errorf("cleanup close: %v", err)
					}
				}
			})
			if got := database.Stats()["treedb.cache.memtable_mode"]; got != "cow_btree" {
				t.Fatalf("memtable mode=%q, want cow_btree", got)
			}
			if _, err := database.NativeSemanticBackend(); err != nil {
				t.Fatalf("native COW owner: %v", err)
			}
			key, value := []byte("acked-key"), []byte("acked-value")
			if err := database.SetSync(key, value); err != nil {
				t.Fatalf("durable SetSync: %v", err)
			}
			requireRawKVValue(t, database, key, value)
			// Keep an additional dirty write so the checkpoint must build state.
			if err := database.Set([]byte("pending"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			capture, err := database.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			maintenance := capture.NativeMaintenanceAllocator().(*memtable.COWBudget)
			if err := capture.Close(); err != nil {
				t.Fatal(err)
			}
			stats, limits := maintenance.Stats(), maintenance.Limits()
			overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			available := min(limits.MaxInFlightBytes-stats.ReservedBytes, limits.MaxTotalBytes-stats.TotalBytes,
				limits.MaxRetiredBytes-stats.HistoryBytes-stats.ReservedBytes-stats.DeferredBytes)
			if available <= overhead {
				t.Fatal("maintenance fixture has no admission headroom")
			}
			pressure, err := maintenance.AcquireExternal(available - overhead)
			if err != nil {
				t.Fatal(err)
			}
			defer pressure.Close()
			if atClose {
				closed = true
				if err := database.Close(); !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("Close error=%v, want COW capacity error", err)
				}
			} else {
				if err := database.Checkpoint(); !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("Checkpoint error=%v, want COW capacity error", err)
				}
				requireRawKVValue(t, database, key, value)
				pressure.Close()
				if err := database.Checkpoint(); err != nil {
					t.Fatalf("checkpoint after releasing pressure: %v", err)
				}
				closed = true
				if err := database.Close(); err != nil {
					t.Fatalf("close after checkpoint: %v", err)
				}
			}
			pressure.Close()
			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			requireRawKVValue(t, reopened, key, value)
			if !atClose {
				requireRawKVValue(t, reopened, []byte("pending"), []byte("value"))
			}
			if err := reopened.Close(); err != nil {
				t.Fatalf("reopened close: %v", err)
			}
		})
	}
}
