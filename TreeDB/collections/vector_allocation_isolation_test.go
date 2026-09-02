package collections

import (
	"context"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

const collectionsVectorAllocationGateEnv = "TREEDB_COLLECTIONS_VECTOR_ALLOCATION_GATE"

// enterIsolatedVectorAllocationGate runs the current test once in a fresh copy
// of the test binary. testing.AllocsPerRun reads process-global malloc counters,
// so an exact hot-path contract must not share its measurement process with
// unrelated collection background work from the full package suite.
//
// The parent has already exercised setup, warmup, and correctness assertions.
// It returns false after the isolated child passes. The selected child returns
// true and performs the exact allocation measurement below the call site.
func enterIsolatedVectorAllocationGate(t *testing.T, gate string) bool {
	t.Helper()

	selected := os.Getenv(collectionsVectorAllocationGateEnv)
	if selected != "" {
		if selected != gate {
			t.Fatalf("isolated vector allocation gate selector=%q want %q", selected, gate)
		}
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^"+regexp.QuoteMeta(t.Name())+"$",
		"-test.count=1",
		"-test.timeout=90s",
	)
	prefix := collectionsVectorAllocationGateEnv + "="
	cmd.Env = make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, prefix) {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, prefix+gate)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("isolated vector allocation gate %q timed out: %v\n%s", gate, ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("isolated vector allocation gate %q: %v\n%s", gate, err, output)
	}
	return false
}
