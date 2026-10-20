package treedb

import (
	"context"
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"runtime"
	"testing"
)

// Retained public views must not consume the maintenance constructor's progress
// capacity or leave the cache on an unrefreshable physical generation.
func TestRetainedViewMaintenanceRefreshAndClose(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			opts.MemtableMode, opts.MemtableShards = "cow_btree", 2
			opts.FlushThreshold = 1 << 30
			opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			if err := database.Set([]byte("key"), []byte("opening")); err != nil {
				t.Fatal(err)
			}
			old := database.AcquireSnapshot()
			if old == nil {
				t.Fatal("missing public snapshot")
			}
			defer old.Close()
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			before, ok := database.backend.StateToken()
			if !ok {
				t.Fatal("missing pre-vacuum state token")
			}
			err = database.VacuumIndexOnline(context.Background())
			if runtime.GOOS == "windows" {
				after, valid := database.backend.StateToken()
				if !errors.Is(err, errVacuumUnsupported) || !valid || after != before {
					t.Fatalf("unsupported retained-view maintenance changed state: %v", err)
				}
			} else if err != nil {
				t.Fatalf("retained-view maintenance: %v", err)
			}
			if got, err := old.Get([]byte("key")); err != nil || string(got) != "opening" {
				t.Fatalf("retained value=%q error=%v", got, err)
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMinimumCapacityCheckpointMakesProgress(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			opts.MemtableMode, opts.MemtableShards = "cow_btree", 2
			opts.FlushThreshold = 1 << 30
			opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
			opts.COWMemtableLimits = DefaultCOWMemtableLimits()
			opts.COWMemtableLimits.MaxViews = 2
			opts.COWMemtableLimits.MaxGenerations, opts.COWMemtableLimits.MaxSources = 8, 4
			opts.COWMemtableLimits.MaxResources = 8
			opts.COWMemtableLimits.MaxGenerationBytes = 128 << 10
			opts.COWMemtableLimits.MaxTotalBytes, opts.COWMemtableLimits.MaxRetiredBytes, opts.COWMemtableLimits.MaxInFlightBytes = 1<<20, 1<<20, 1<<20
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			inventory := func(phase string) {
				inspection := errors.New("test: inventory only")
				_, err := database.backend.AcquireSnapshotWithAllocationAdmission(func(s backenddb.SnapshotAllocationSizes) error {
					var registry uint64

					if s.RegistryMetadata != nil {
						registry = s.RegistryMetadata.Bytes()
					}
					stats := database.Stats()
					t.Logf("%s: maintenance=%s registry_owner=%d snapshot_wrapper=%d vlog=%+v sources=%s generations=%s cuts=%s", phase, stats["treedb.cache.cow.maintenance_used_bytes"], registry, s.Wrapper, s.ValueLog, stats["treedb.cache.cow.sources"], stats["treedb.cache.cow.generations"], stats["treedb.cache.cow.active_cuts"])
					return inspection
				})
				if !errors.Is(err, inspection) {
					t.Fatalf("inventory %s: %v", phase, err)
				}
			}
			inventory("startup")
			if err := database.Set([]byte("key"), []byte("opening")); err != nil {
				t.Fatal(err)
			}
			old := database.AcquireSnapshot()
			if old == nil {
				t.Fatal("minimum-capacity view refused")
			}
			defer old.Close()
			inventory("old admitted")
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			inventory("first checkpoint")
			before, ok := database.backend.StateToken()
			if !ok {
				t.Fatal("missing pre-vacuum state token")
			}
			err = database.VacuumIndexOnline(context.Background())
			if runtime.GOOS == "windows" {
				after, valid := database.backend.StateToken()
				if !errors.Is(err, errVacuumUnsupported) || !valid || after != before {
					t.Fatalf("unsupported minimum-capacity vacuum changed state: %v", err)
				}
				inventory("vacuum refused without publication")
			} else if err != nil {
				t.Fatal(err)
			} else {
				inventory("vacuum refreshed")
			}
			if got, err := old.Get([]byte("key")); err != nil || string(got) != "opening" {
				t.Fatalf("minimum-capacity old view=%q error=%v", got, err)
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			inventory("old closed")
			if err := database.Set([]byte("key"), []byte("reused")); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				stats := database.Stats()
				var shape backenddb.SnapshotAllocationSizes
				snapshot, captureErr := database.backend.AcquireSnapshotWithAllocationAdmission(func(got backenddb.SnapshotAllocationSizes) error { shape = got; return nil })
				if snapshot != nil {
					_ = snapshot.Close()
				}
				t.Logf("reuse basis inventory: files=%d mapHint=%d resourceLimit=%d capture=%v", shape.ValueLog.FileCount, shape.ValueLog.MapHint, opts.COWMemtableLimits.MaxResources, captureErr)
				t.Fatalf("reuse checkpoint: %v; producer total=%s reserved=%s history=%s sources=%s generations=%s maintenance=%s/%s", err, stats["treedb.cache.cow.total_bytes"], stats["treedb.cache.cow.reserved_bytes"], stats["treedb.cache.cow.history_bytes"], stats["treedb.cache.cow.sources"], stats["treedb.cache.cow.generations"], stats["treedb.cache.cow.maintenance_used_bytes"], stats["treedb.cache.cow.maintenance_capacity_bytes"])
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
