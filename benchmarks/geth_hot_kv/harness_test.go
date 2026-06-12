package gethhotkv

import (
	"os"
	"strings"
	"testing"
)

func TestHarnessPreservesIntegratedGethWorkloadShape(t *testing.T) {
	blob, err := os.ReadFile("testdata/treedb_nitro_soak.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(blob)
	for _, want := range []string{
		"node.OpenDatabase",
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
