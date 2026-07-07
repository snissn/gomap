package treedb

import (
	"encoding/json"
	"expvar"
	"path/filepath"
	"testing"
)

func TestPublicCommandWALExpvarIncludesPublicStatsHookCounters(t *testing.T) {
	db, err := Open(Options{
		Dir:                           t.TempDir(),
		CommandWAL:                    true,
		BackgroundCheckpointInterval:  -1,
		BackgroundIndexVacuumInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	published := expvar.Get("treedb")
	if published == nil {
		t.Fatalf("expvar treedb publisher missing")
	}
	var stats map[string]any
	if err := json.Unmarshal([]byte(published.String()), &stats); err != nil {
		t.Fatalf("parse expvar treedb JSON: %v", err)
	}
	instances, ok := stats["instances"].(map[string]any)
	if !ok {
		t.Fatalf("instances=%T want object", stats["instances"])
	}
	if len(instances) == 0 {
		t.Fatalf("expvar instances missing")
	}
	wantWALDir := filepath.Join(db.dir, "maindb", "wal")
	for _, raw := range instances {
		instance, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if instance["treedb.expvar.wal_dir"] != wantWALDir {
			continue
		}
		if _, ok := instance["treedb.maintenance.full_scan.deferrals"]; !ok {
			t.Fatalf("maintenance counter missing from public expvar instance: %+v", instance)
		}
		if _, ok := instance["treedb.bg_vacuum.vacuums"]; !ok {
			t.Fatalf("background-vacuum counter missing from public expvar instance: %+v", instance)
		}
		if _, ok := instance["treedb.command_wal.public_batch.set_view.calls_total"]; !ok {
			t.Fatalf("public command-WAL batch counter missing from public expvar instance: %+v", instance)
		}
		return
	}
	t.Fatalf("expvar instance for WAL dir %q missing: %+v", wantWALDir, instances)
}
