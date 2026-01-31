package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestCachedSeed_FlushThresholdAffectsFill(t *testing.T) {
	const keys = 20000
	val := bytes.Repeat([]byte("a"), 128)

	type result struct {
		label string
		rep   map[string]string
		stats map[string]string
	}

	run := func(label string, flushThreshold int64) result {
		t.Helper()
		dir := t.TempDir()
		backend, err := db.Open(db.Options{
			Dir:               dir,
			PreferAppendAlloc: false,
			KeepRecent:        1,
		})
		if err != nil {
			t.Fatalf("%s: backend open: %v", label, err)
		}
		cached, err := Open(dir, backend, Options{FlushThreshold: flushThreshold})
		if err != nil {
			_ = backend.Close()
			t.Fatalf("%s: cached open: %v", label, err)
		}

		seedBatches(t, cached, keys, val)
		if err := cached.Checkpoint(); err != nil {
			t.Fatalf("%s: checkpoint: %v", label, err)
		}

		rep, err := backend.FragmentationReport()
		if err != nil {
			t.Fatalf("%s: FragmentationReport: %v", label, err)
		}
		stats := cached.Stats()

		_ = cached.Close()
		_ = backend.Close()
		return result{label: label, rep: rep, stats: stats}
	}

	a := run("flush_threshold_1MiB", 1<<20)
	b := run("flush_threshold_1EiB", 1<<60)

	for _, r := range []result{a, b} {
		rep := r.rep
		t.Logf("%s: user.pages=%s leaf.avg=%s leaf.p50=%s leaf.min=%s pages.total=%s span_ratio_ppm=%s",
			r.label,
			rep["treedb.user.pages"],
			rep["treedb.user.leaf_fill_ppm_avg"],
			rep["treedb.user.leaf_fill_ppm_p50"],
			rep["treedb.user.leaf_fill_ppm_min"],
			rep["treedb.pages.total"],
			rep["treedb.user.pages.span_ratio_ppm"],
		)

		stats := r.stats
		keys := []string{
			"treedb.cache.stats.memtable_rotations_total",
			"treedb.cache.stats.flush_lane_once_total",
			"treedb.cache.stats.backend_write_batches_total",
			"treedb.cache.stats.flush_units_flushed_total",
			"treedb.cache.stats.flush_entries_flushed_total",
		}
		for _, k := range keys {
			v := stats[k]
			if v == "" {
				t.Fatalf("%s: missing stats key %q", r.label, k)
			}
			t.Logf("%s: %s=%s", r.label, k, v)
		}
	}
}
