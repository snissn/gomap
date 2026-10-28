package collections

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Diagnostic only. Sampling, two explicit GCs at each boundary and raw runtime
// profiling perturb execution. The profile also includes the warmup invocation
// and any other child-goroutine allocations. It never qualifies the original gate.
type rabitQAllocationDiagnosticProbe struct {
	before, after        []runtime.MemProfileRecord
	beforeN, afterN      int
	beforeCache          collectionVectorIndexPreparedSearchCacheSnapshot
	beforeState          backenddb.StateToken
	beforeStateValid     bool
	beforeGoroutines     []byte
	afterGoroutines      []byte
	beforeStackN         int
	beforeStackTruncated bool
}

func beginRabitQAllocationDiagnostic(t *testing.T, col *Collection) *rabitQAllocationDiagnosticProbe {
	t.Helper()
	n, _ := runtime.MemProfile(nil, true)
	// Fixed bounded headroom, allocated before the first snapshot. Overflow fails
	// the diagnostic instead of resizing or silently dropping allocation sites.
	p := &rabitQAllocationDiagnosticProbe{
		before:           make([]runtime.MemProfileRecord, n+4096),
		after:            make([]runtime.MemProfileRecord, n+4096),
		beforeGoroutines: make([]byte, 1<<20),
		afterGoroutines:  make([]byte, 1<<20),
		beforeCache:      col.collectionVectorIndexPreparedSearchCacheSnapshot(),
	}
	p.beforeState, p.beforeStateValid = col.db.StateToken()
	p.beforeStackN = runtime.Stack(p.beforeGoroutines, true)
	p.beforeStackTruncated = p.beforeStackN == len(p.beforeGoroutines)
	// MemProfile can lag by two GC cycles. Both GCs are outside AllocsPerRun;
	// clearing pools is a known perturbation, not an authoritative fixture change.
	runtime.GC()
	runtime.GC()
	var ok bool
	p.beforeN, ok = runtime.MemProfile(p.before, true)
	if !ok {
		t.Fatalf("DIAGNOSTIC profile capacity exceeded: before need=%d capacity=%d", p.beforeN, len(p.before))
	}
	return p
}

func (p *rabitQAllocationDiagnosticProbe) finish(t *testing.T, col *Collection, phase string, allocsPerRun float64) {
	// No serialization, logging, cache formatting or filesystem operations occur
	// between the raw snapshots. Finish accounting before taking the second one.
	runtime.GC()
	runtime.GC()
	var ok bool
	p.afterN, ok = runtime.MemProfile(p.after, true)
	if !ok {
		t.Fatalf("DIAGNOSTIC profile capacity exceeded: after need=%d capacity=%d", p.afterN, len(p.after))
	}
	t.Helper() // Helper registration can allocate; keep it after the second snapshot.
	afterCache := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	afterState, afterStateValid := col.db.StateToken()
	afterStackN := runtime.Stack(p.afterGoroutines, true)
	root := os.Getenv("TREEDB_RABITQ_DIAGNOSTIC_DIR")
	if root == "" {
		t.Fatal("diagnostic probe requires TREEDB_RABITQ_DIAGNOSTIC_DIR")
	}
	root = filepath.Join(root, fmt.Sprintf("child-%d", os.Getpid()))
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]any{
		"before-records": p.before[:p.beforeN], "after-records": p.after[:p.afterN],
		"state": map[string]any{
			"diagnostic_only": true, "phase": phase, "allocs_per_run": allocsPerRun,
			"mem_profile_rate": runtime.MemProfileRate, "explicit_gc_cycles_per_boundary": 2, "sampling_started_after_setup": true,
			"before_cache": p.beforeCache, "after_cache": afterCache,
			"before_state": p.beforeState, "before_state_valid": p.beforeStateValid,
			"after_state": afterState, "after_state_valid": afterStateValid,
			"profile_capacity": len(p.before), "before_records": p.beforeN, "after_records": p.afterN,
			"before_stack_truncated": p.beforeStackTruncated, "after_stack_truncated": afterStackN == len(p.afterGoroutines),
		},
	} {
		raw, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, phase+"-"+name+".json"), raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
	for name, data := range map[string][]byte{
		"before-goroutines": p.beforeGoroutines[:p.beforeStackN], "after-goroutines": p.afterGoroutines[:afterStackN],
	} {
		if err := os.WriteFile(filepath.Join(root, phase+"-"+name+".txt"), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	// Symbolize only after both snapshots. Retain exact raw records as well as
	// stack-keyed net allocation deltas; positive deltas are not per-call counts.
	before := make(map[[32]uintptr]runtime.MemProfileRecord, p.beforeN)
	for _, r := range p.before[:p.beforeN] {
		b := before[r.Stack0]
		b.AllocObjects += r.AllocObjects
		b.AllocBytes += r.AllocBytes
		before[r.Stack0] = b
	}
	after := make(map[[32]uintptr]runtime.MemProfileRecord, p.afterN)
	for _, r := range p.after[:p.afterN] {
		a := after[r.Stack0]
		a.Stack0 = r.Stack0
		a.AllocObjects += r.AllocObjects
		a.AllocBytes += r.AllocBytes
		after[r.Stack0] = a
	}
	var delta strings.Builder
	for _, r := range after {
		b := before[r.Stack0]
		objects, bytes := r.AllocObjects-b.AllocObjects, r.AllocBytes-b.AllocBytes
		if objects == 0 && bytes == 0 {
			continue
		}
		fmt.Fprintf(&delta, "alloc_objects_delta=%d alloc_bytes_delta=%d\n", objects, bytes)
		frames := runtime.CallersFrames(r.Stack())
		for {
			frame, more := frames.Next()
			fmt.Fprintf(&delta, "  %s %s:%d\n", frame.Function, frame.File, frame.Line)
			if !more {
				break
			}
		}
	}
	if err := os.WriteFile(filepath.Join(root, phase+"-allocation-deltas.txt"), []byte(delta.String()), 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("DIAGNOSTIC_NONQUALIFYING phase=%s allocs/run=%v before_cache=%+v after_cache=%+v before_state=%+v after_state=%+v records=%d->%d artifacts=%s", phase, allocsPerRun, p.beforeCache, afterCache, p.beforeState, afterState, p.beforeN, p.afterN, root)
}
