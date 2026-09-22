package gethhotkv

import (
	"os"
	"strings"
	"testing"
)

func TestHarnessPreservesIntegratedGethWorkloadShape(t *testing.T) {
	src := readHarnessSource(t)
	for _, want := range []string{
		"OpenDatabase(\"chaindata\"",
		"DBEngine: engine",
		"db.NewBatch()",
		"batch.Put",
		"db.Get",
		"db.NewIterator",
		"db.DeleteRange",
		"batch.DeleteRange",
		"openEngine(dbRoot, engine, false, cfg)",
		"key-shape",
		"value-shape",
		"batch-target-bytes",
		"pathWithin(cfg.WorkDir, cfg.ProfileDir)",
		"DeleteRange keys/sec",
	} {
		if !strings.Contains(src, want) {
			t.Fatalf("harness missing %q", want)
		}
	}
}

func TestHarnessExposesReadIntegrityIterationAndCounterLabels(t *testing.T) {
	src := readHarnessSource(t)
	for _, want := range []string{
		`"treedb-read-integrity"`,
		`"unsafe-skip-checksums"`,
		`"iteration-mode"`,
		`json:"treedb_read_integrity"`,
		`json:"read_integrity"`,
		`json:"iteration_mode"`,
		`json:"stat_delta,omitempty"`,
		"TreeDB read-integrity",
		"iteration mode",
		"treedb.vlog.read.crc32_checks_total",
		"treedb.cache.vlog_read.crc32_checks_total",
		"treedb.vlog.grouped_frame_cache.hits",
		"treedb.cache.vlog_grouped_frame_cache.hits",
		"treedb.vlog.mmap_read.miss_out_of_range",
		"treedb.vlog.mmap_read.miss_no_mapping",
		"treedb.vlog.mmap_read.miss_dead_mapping_cap",
		"treedb.process.read_path.outer_leaf.loads_total",
		"treedb.process.read_path.outer_leaf.point_loads_total",
		"treedb.process.read_path.outer_leaf.iterator_loads_total",
		"treedb.process.read_path.outer_leaf.checksum.verifications_total",
		"treedb.process.read_path.outer_leaf.checksum.skips_total",
		"treedb.cache.delete_range.calls_total",
		"treedb.cache.delete_range.batch_calls_total",
		"treedb.cache.delete_range.coalesced_ranges_total",
		"treedb.cache.delete_range.snapshot_iterators_total",
		"treedb.cache.delete_range.materialized_keys_total",
		"treedb.cache.delete_range.backend_direct_batches_total",
		"treedb.cache.range_span.layers_total",
		"treedb.cache.range_span.iterator_skips_total",
		"treedb.cache.range_span.flush_batches_total",
		"if hasReadCounterDeltas(runs) {",
		"if !hasDeleteRangeCounterDeltas(runs) {",
	} {
		if !strings.Contains(src, want) {
			t.Fatalf("harness missing instrumentation label %q", want)
		}
	}
}

func TestHarnessKeyOnlyIterationDoesNotMaterializeValues(t *testing.T) {
	src := readHarnessSource(t)
	if !strings.Contains(src, "if mode == iterationModeValue {\n\t\t\t_ = it.Value()\n\t\t}") {
		t.Fatalf("iterateData must guard Iterator.Value() behind value iteration mode")
	}
	if got := strings.Count(src, ".Value()"); got != 1 {
		t.Fatalf("harness has %d Iterator.Value() calls, want exactly the guarded iterateData call", got)
	}
}

func readHarnessSource(t *testing.T) string {
	t.Helper()
	blob, err := os.ReadFile("testdata/treedb_nitro_soak.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(blob)
}
