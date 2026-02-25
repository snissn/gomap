package treedb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAddVlogGenerationStats(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	const payload = `{
  "segments": {
    "1": {"segment_bytes": 100, "live_bytes": 70, "rewrite_count": 0},
    "2": {"segment_bytes": 200, "live_bytes": 150, "rewrite_count": 1},
    "3": {"segment_bytes": 300, "live_bytes": 180, "rewrite_count": 2}
  }
}`
	if err := os.WriteFile(filepath.Join(mainDir, "vlog_health.json"), []byte(payload), 0o644); err != nil {
		t.Fatalf("write health: %v", err)
	}

	stats := map[string]string{}
	addVlogGenerationStats(stats, root)

	if got := stats["treedb.vlog.generation.hot.segments"]; got != "1" {
		t.Fatalf("hot segments: got %q", got)
	}
	if got := stats["treedb.vlog.generation.warm.bytes_stale"]; got != "50" {
		t.Fatalf("warm stale: got %q", got)
	}
	if got := stats["treedb.vlog.generation.cold.bytes_live"]; got != "180" {
		t.Fatalf("cold live: got %q", got)
	}
}
