package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIronbirdAttributionContractDocumentsRequiredSignals(t *testing.T) {
	_, repoRoot := repoRoots(t)
	path := filepath.Join(repoRoot, "docs", "TREEDB_IRONBIRD_ATTRIBUTION.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read attribution contract: %v", err)
	}
	text := string(data)
	for _, want := range []string{
		"# TreeDB Ironbird Attribution Contract",
		"command-WAL append and sync work",
		"checkpoint and frontier flush work",
		"value-log generation, GC, and rewrite work",
		"host I/O wait, lock contention, and harness or non-ABCI time",
		"`pre_load`",
		"`load_start`",
		"`load_end`",
		"`post_dwell`",
		"`treedb.command_wal.*`",
		"`treedb.cache.checkpoint.*`",
		"`treedb.vlog.lifecycle.*`",
		"`treedb.vlog.gc.*`",
		"`treedb.vlog.rewrite.*`",
		"`treedb.cache.queue_backlog_bytes`",
		"`treedb.process.memory.*`",
		"`treedb.cache.vlog_writev.bytes`",
		"`treedb.cache.vlog_write.bytes`",
		"`treedb.cache.vlog_io.bytes`",
		"`treedb.cache.vlog_write_mode.raw_bytes.*`",
		"`treedb.cache.vlog_payload_kind.raw_bytes.*`",
		"`treedb.cache.vlog_payload_split.raw_bytes.*`",
		"`treedb.cache.vlog_outer_leaf_codec.raw_bytes.*`",
		"`treedb.cache.vlog_auto.bytes.*`",
		"ratio, fraction, bytes-per, and per-byte timing fields remain gauges.",
		"are monotonic counters even though they predate",
		"load-window deltas",
		"`_bytes_total`",
		"`accepted_load_window_start`",
		"`accepted_load_window_end`",
		"ABCI busy union",
		"non-ABCI residual must be labeled approximate",
		"load-window accounting",
		"dwell classification",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("%s missing required attribution wording %q", path, want)
		}
	}
}
