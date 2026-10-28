package collections

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
)

// Diagnostic only: GC, all-allocation sampling and profile serialization perturb
// execution. Profiles include diagnostic and other child-goroutine allocations;
// neither their deltas nor a passing probe qualify the original zero-allocation gate.
func rabitQAllocationDiagnosticSnapshot(t *testing.T, col *Collection, phase string) {
	t.Helper()
	root := os.Getenv("TREEDB_RABITQ_DIAGNOSTIC_DIR")
	if root == "" {
		t.Fatal("diagnostic probe requires TREEDB_RABITQ_DIAGNOSTIC_DIR")
	}
	root = filepath.Join(root, fmt.Sprintf("child-%d", os.Getpid()))
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	state, valid := col.db.StateToken()
	t.Logf("DIAGNOSTIC_NONQUALIFYING phase=%s cache=%+v state=%+v state_valid=%t goroutines=%d MemProfileRate=%d", phase, col.collectionVectorIndexPreparedSearchCacheSnapshot(), state, valid, runtime.NumGoroutine(), runtime.MemProfileRate)
	// Force accounting to become visible. This may clear pools and is deliberately
	// outside the original AllocsPerRun closure, count and zero threshold.
	runtime.GC()
	for _, kind := range []string{"allocs", "goroutine"} {
		suffix, debug := ".pprof", 0
		if kind == "goroutine" {
			suffix, debug = ".txt", 2
		}
		f, err := os.Create(filepath.Join(root, phase+"-"+kind+suffix))
		if err != nil {
			t.Fatal(err)
		}
		err = pprof.Lookup(kind).WriteTo(f, debug)
		closeErr := f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
}
