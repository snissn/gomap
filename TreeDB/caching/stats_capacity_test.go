package caching

import (
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"testing"
)

// Embedding the native backend must not bypass an overridden Stats method.
type statsOverrideBackend struct{ *backenddb.DB }

func (b *statsOverrideBackend) Stats() map[string]string {
	s := b.DB.Stats()
	s["decorator-owned"] = "present"
	return s
}

func TestStatsNativeCapacityPreservesDecoratorAndOwnership(t *testing.T) {
	for _, tc := range []struct {
		name      string
		decorated bool
		lanes     int
	}{{"native_minimal", false, 1}, {"native_many_lanes", false, 32}, {"decorator", true, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			decorated := tc.decorated
			dir := t.TempDir()
			native, e := backenddb.Open(backenddb.Options{Dir: dir})
			if e != nil {
				t.Fatal(e)
			}
			defer native.Close()
			var backend BackendDB = native
			if decorated {
				backend = &statsOverrideBackend{native}
			}
			cached, e := Open(dir, backend, Options{FlushThreshold: 64 << 20, MemtableShards: tc.lanes, JournalLanes: tc.lanes})
			if e != nil {
				t.Fatal(e)
			}
			defer cached.Close()
			first := cached.Stats()
			if first["cosmos.db.type"] != "treedb" || first["treedb.process.memory.pool_pressure_level"] == "" {
				t.Fatal("missing backend or cache fields")
			}
			for lane := 0; lane < tc.lanes; lane++ {
				key := fmt.Sprintf("treedb.cache.vlog_queue.lane.%d.enqueued", lane)
				if _, ok := first[key]; !ok {
					t.Fatalf("missing dynamic lane field %s", key)
				}
			}
			if decorated && first["decorator-owned"] != "present" {
				t.Fatal("decorator override bypassed")
			}
			for key := range native.Stats() {
				if _, ok := first[key]; !ok {
					t.Fatalf("missing backend field %s", key)
				}
			}
			first["caller-owned"] = "sentinel"
			delete(first, "cosmos.db.type")
			if second := cached.Stats(); second["caller-owned"] != "" || second["cosmos.db.type"] != "treedb" {
				t.Fatal("snapshot mutations escaped")
			}
		})
	}
}
