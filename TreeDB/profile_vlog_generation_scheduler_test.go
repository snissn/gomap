package treedb

import "testing"

func TestProfilesFastAndWALOnFast_StartVlogGenerationSchedulerEnabled(t *testing.T) {
	profiles := []Profile{ProfileFast, ProfileWALOnFast}
	for _, profile := range profiles {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("open %s: %v", profile, err)
			}
			t.Cleanup(func() { _ = db.Close() })

			stats := db.Stats()
			if got := stats["treedb.cache.vlog_generation.enabled"]; got != "true" {
				t.Fatalf("vlog generation enabled=%q want true for profile %s", got, profile)
			}
			if got := stats["treedb.cache.vlog_generation.scheduler_state"]; got != "idle" {
				t.Fatalf("scheduler state=%q want idle for profile %s", got, profile)
			}
		})
	}
}
