package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocsVectorPartitionRaftM0Contract(t *testing.T) {
	root, _ := repoRoots(t)
	b, err := os.ReadFile(filepath.Join(root, "docs", "spec", "vector-partition-raft-v1.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"_id", "logical vector", "Raft replica", "overlap membership", "vector_index_id", "source_snapshot_generation", "partition_generation", "router_generation", "building", "ready", "cutover", "invalidated", "snapshot-bound", "fail closed", "simulation_only", "gp-ann", "Parent invariant matrix", "TestTruthOracleTieOrderingAndAllPartitionParity", "M1 owns", "M8"} {
		if !strings.Contains(string(b), s) {
			t.Fatalf("contract missing %q", s)
		}
	}
}
