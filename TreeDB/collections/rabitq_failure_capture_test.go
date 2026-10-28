package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const rabitQFailureCaptureEnv5313 = "TREEDB_RABITQ_FAILURE_CAPTURE"

// This diagnostic overlay never changes the allocation rate, counted closure,
// count, or zero threshold. All fixed storage is allocated before fixture setup.
// Baseline collection still perturbs scheduling; a passing diagnostic is not a
// reproduction, attribution, or repair of the retained isolated-child failure.
type rabitQFailureCapture5313 struct {
	before, immediate, accounted [2048]runtime.MemProfileRecord
	beforeStacks, afterStacks    [64 << 10]byte
	beforeN, beforeStackN        int
	beforeComplete               bool
	beforeProfileRate            int
	beforeCache                  collectionVectorIndexPreparedSearchCacheSnapshot
	beforeState                  backenddb.StateToken
	beforeStateOK                bool
}

func newRabitQFailureCapture5313() *rabitQFailureCapture5313 {
	if os.Getenv(rabitQFailureCaptureEnv5313) == "" || os.Getenv(collectionsVectorAllocationGateEnv) == "" {
		return nil
	}
	return new(rabitQFailureCapture5313)
}

func (p *rabitQFailureCapture5313) begin(c *Collection) {
	if p == nil {
		return
	}
	p.beforeProfileRate = runtime.MemProfileRate
	p.beforeN, p.beforeComplete = runtime.MemProfile(p.before[:], true)
	p.beforeStackN = runtime.Stack(p.beforeStacks[:], true)
	p.beforeCache = c.collectionVectorIndexPreparedSearchCacheSnapshot()
	p.beforeState, p.beforeStateOK = c.db.StateToken()
}

// capture is invoked only after the original gate has already returned a
// positive count, before DB Close. It records the immediate state before any
// formatting, serialization, filesystem output or explicit GC. Two subsequent
// GCs settle delayed profile accounting, and cannot affect the failed gate.
func (p *rabitQFailureCapture5313) capture(t *testing.T, c *Collection, phase string, allocations float64) {
	if p == nil {
		return
	}
	afterCache := c.collectionVectorIndexPreparedSearchCacheSnapshot()
	afterState, afterStateOK := c.db.StateToken()
	immediateN, immediateComplete := runtime.MemProfile(p.immediate[:], true)
	afterStackN := runtime.Stack(p.afterStacks[:], true)
	runtime.GC()
	runtime.GC()
	accountedN, accountedComplete := runtime.MemProfile(p.accounted[:], true)
	t.Helper()
	root := filepath.Join(os.Getenv(rabitQFailureCaptureEnv5313), fmt.Sprintf("child-%d-%s", os.Getpid(), phase))
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Errorf("allocation failure capture: %v", err)
		return
	}
	metadata := map[string]any{
		"diagnostic_only": true, "phase": phase, "allocs_per_run": allocations,
		"go_version":              runtime.Version(),
		"mem_profile_rate_before": p.beforeProfileRate, "mem_profile_rate_after": runtime.MemProfileRate,
		"explicit_gc_before_or_during_measurement": 0,
		"explicit_gc_after_failed_measurement":     2,
		"before_cache":                             p.beforeCache, "after_cache": afterCache,
		"before_state": p.beforeState, "before_state_valid": p.beforeStateOK,
		"after_state": afterState, "after_state_valid": afterStateOK,
		"profile_capacity": len(p.before),
		"before_records":   p.beforeN, "before_complete": p.beforeComplete,
		"immediate_records": immediateN, "immediate_complete": immediateComplete,
		"accounted_records": accountedN, "accounted_complete": accountedComplete,
		"before_stacks_truncated": p.beforeStackN == len(p.beforeStacks),
		"after_stacks_truncated":  afterStackN == len(p.afterStacks),
	}
	executable, err := os.Executable()
	if err != nil {
		t.Errorf("allocation failure capture executable: %v", err)
		return
	}
	in, err := os.Open(executable)
	if err != nil {
		t.Errorf("allocation failure capture executable: %v", err)
		return
	}
	defer in.Close()
	out, err := os.Create(filepath.Join(root, "collections.test"))
	if err != nil {
		t.Errorf("allocation failure capture executable: %v", err)
		return
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(out, hash), in)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil {
		t.Errorf("allocation failure capture executable: copy=%v close=%v", copyErr, closeErr)
		return
	}
	metadata["executable_sha256"] = fmt.Sprintf("%x", hash.Sum(nil))
	if info, ok := debug.ReadBuildInfo(); ok {
		metadata["build_info"] = info.String()
	}
	for name, value := range map[string]any{
		"metadata":          metadata,
		"before-records":    p.before[:min(p.beforeN, len(p.before))],
		"immediate-records": p.immediate[:min(immediateN, len(p.immediate))],
		"accounted-records": p.accounted[:min(accountedN, len(p.accounted))],
	} {
		raw, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Errorf("allocation failure capture: %v", err)
			return
		}
		if err := os.WriteFile(filepath.Join(root, name+".json"), raw, 0644); err != nil {
			t.Errorf("allocation failure capture: %v", err)
			return
		}
	}
	for name, data := range map[string][]byte{
		"before-stacks": p.beforeStacks[:p.beforeStackN], "after-stacks": p.afterStacks[:afterStackN],
	} {
		if err := os.WriteFile(filepath.Join(root, name+".txt"), data, 0644); err != nil {
			t.Errorf("allocation failure capture: %v", err)
			return
		}
	}
	// Raw PCs bind to the retained exact binary. Symbolize only after capture.
	var symbols bytes.Buffer
	for label, records := range map[string][]runtime.MemProfileRecord{
		"before":    p.before[:min(p.beforeN, len(p.before))],
		"immediate": p.immediate[:min(immediateN, len(p.immediate))],
		"accounted": p.accounted[:min(accountedN, len(p.accounted))],
	} {
		for _, record := range records {
			if record.AllocObjects == 0 {
				continue
			}
			fmt.Fprintf(&symbols, "%s objects=%d bytes=%d freed_objects=%d freed_bytes=%d\n", label, record.AllocObjects, record.AllocBytes, record.FreeObjects, record.FreeBytes)
			frames := runtime.CallersFrames(record.Stack())
			for {
				frame, more := frames.Next()
				fmt.Fprintf(&symbols, "  %#x %s %s:%d\n", frame.PC, frame.Function, frame.File, frame.Line)
				if !more {
					break
				}
			}
		}
	}
	if err := os.WriteFile(filepath.Join(root, "symbolized-records.txt"), symbols.Bytes(), 0644); err != nil {
		t.Errorf("allocation failure capture: %v", err)
		return
	}

	t.Logf("DIAGNOSTIC_NONQUALIFYING original_gate_failed=true metadata=%+v artifacts=%s", metadata, root)
	if !p.beforeComplete || !immediateComplete || !accountedComplete || p.beforeStackN == len(p.beforeStacks) || afterStackN == len(p.afterStacks) {
		t.Error("allocation failure diagnostic capacity exceeded; original positive gate remains failed")
	}
}
