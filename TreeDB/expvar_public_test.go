package treedb

import (
	"encoding/json"
	"expvar"
	"path/filepath"
	"testing"
)

func TestPublicExpvarIncludesPublicStatsHookCounters(t *testing.T) {
	for _, tc := range []struct {
		name           string
		profile        Profile
		wantCommandWAL bool
	}{
		{name: "no_wal_fast", profile: ProfileNoWALFast},
		{name: "command_wal", profile: ProfileCommandWALDurable, wantCommandWAL: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := OptionsFor(tc.profile, t.TempDir())
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundIndexVacuumInterval = -1
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
			})
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}

			assertPublicExpvarStatsHookCounters(t, db, tc.wantCommandWAL)
		})
	}
}

func assertPublicExpvarStatsHookCounters(t *testing.T, db *DB, wantCommandWAL bool) {
	t.Helper()
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
		if got, ok := instance["treedb.public.checkpoint.calls_total"].(float64); !ok || got != 1 {
			t.Fatalf("public checkpoint calls=%T(%v) want float64(1)", instance["treedb.public.checkpoint.calls_total"], instance["treedb.public.checkpoint.calls_total"])
		}
		if _, ok := instance["treedb.public.checkpoint.ns_total"]; !ok {
			t.Fatalf("public checkpoint ns_total missing from public expvar instance: %+v", instance)
		}
		_, hasCommandWALBatch := instance["treedb.command_wal.public_batch.set_view.calls_total"]
		if wantCommandWAL && !hasCommandWALBatch {
			t.Fatalf("public command-WAL batch counter missing from public expvar instance: %+v", instance)
		}
		if !wantCommandWAL && hasCommandWALBatch {
			t.Fatalf("public command-WAL batch counter present for non-command-WAL open: %+v", instance)
		}
		return
	}
	t.Fatalf("expvar instance for WAL dir %q missing: %+v", wantWALDir, instances)
}
