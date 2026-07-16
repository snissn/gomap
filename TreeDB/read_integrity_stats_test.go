package treedb

import "testing"

func TestStatsReportsValueLogReadIntegrityAndReadCounters(t *testing.T) {
	for _, tt := range []struct {
		name string
		mode IntegrityMode
		want string
	}{
		{name: "verify", mode: IntegrityVerify, want: "verify"},
		{name: "skip", mode: IntegritySkipChecksums, want: "unsafe-skip-checksums"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
			if tt.mode == IntegritySkipChecksums {
				opts = OptionsForBenchmark(ProfileBenchUnsafe, opts.Dir)
			}
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer db.Close()

			stats := db.Stats()
			if got := stats["treedb.vlog.read_integrity"]; got != tt.want {
				t.Fatalf("treedb.vlog.read_integrity=%q want %q", got, tt.want)
			}
			if !statsHasAny(stats, "treedb.vlog.read.crc32_checks_total", "treedb.cache.vlog_read.crc32_checks_total") {
				t.Fatalf("Stats missing value-log CRC read counter")
			}
			if !statsHasAny(stats, "treedb.vlog.grouped_frame_cache.hits", "treedb.cache.vlog_grouped_frame_cache.hits") {
				t.Fatalf("Stats missing grouped-frame cache counters")
			}
			if !statsHasAny(stats, "treedb.vlog.grouped_frame_cache.allocated_slots", "treedb.cache.vlog_grouped_frame_cache.allocated_slots") {
				t.Fatalf("Stats missing grouped-frame cache allocation counters")
			}
		})
	}
}

func statsHasAny(stats map[string]string, keys ...string) bool {
	for _, key := range keys {
		if _, ok := stats[key]; ok {
			return true
		}
	}
	return false
}
