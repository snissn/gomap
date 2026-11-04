//go:build treedb_test

package mvcc

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/snissn/gomap/TreeDB/caching"
)

var primaryObserverDir = flag.String("primary-allocation-observer-dir", "", "diagnostic only: existing absolute output directory; never an acceptance capture")

type primaryAllocationDivision struct {
	TotalAlloc uint64
	Mallocs uint64
	BytesQuotient uint64
	BytesRemainder uint64
	MallocsQuotient uint64
	MallocsRemainder uint64
}

func primaryDivideAllocation(before, after *runtime.MemStats, n int) (*primaryAllocationDivision, error) {
	if before == nil || after == nil || n != 128 {
		return nil, errors.New("missing counters or wrong requested population")
	}
	if after.TotalAlloc < before.TotalAlloc || after.Mallocs < before.Mallocs || after.Frees < before.Frees {
		return nil, errors.New("regressing process allocation counters")
	}
	for i, bucket := range before.BySize {
		if after.BySize[i].Size != bucket.Size || after.BySize[i].Mallocs < bucket.Mallocs || after.BySize[i].Frees < bucket.Frees {
			return nil, errors.New("regressing or mismatched allocation size bucket")
		}
	}
	bytes, mallocs := after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs
	return &primaryAllocationDivision{bytes, mallocs, bytes/uint64(n), bytes%uint64(n), mallocs/uint64(n), mallocs%uint64(n)}, nil
}

type primaryAllocationPhase struct {
	Position string
	Seam *caching.PrimaryAllocationSeamCounters `json:",omitempty"`
}

type primaryAllocationObserver struct {
	dir string
	n int
	seam *caching.PrimaryAllocationObservation
	before, after runtime.MemStats
	beforeRead, afterRead bool
	loopBefore, loopAfter caching.PrimaryAllocationSeamCounters
	loopComplete, timerStopped, statsComplete, profilesComplete bool
	phases [16]primaryAllocationPhase
	// Fixed backing: phase snapshots allocate outside the raw counter window.
	seamPhases [16]caching.PrimaryAllocationSeamCounters
	phaseCount int
}

func newPrimaryAllocationObserver(b *testing.B) *primaryAllocationObserver {
	if *primaryObserverDir == "" { return nil }
	if b.N != 1 && b.N != 128 { b.Fatalf("PRIMARY observer requires original N1 calibration then requested N128, got %d", b.N) }
	if runtime.MemProfileRate != 1 { b.Fatal("PRIMARY observer requires -test.memprofilerate=1") }
	if !filepath.IsAbs(*primaryObserverDir) { b.Fatal("PRIMARY observer requires absolute output directory") }
	info, err := os.Stat(*primaryObserverDir)
	if err != nil || !info.IsDir() { b.Fatal("PRIMARY observer output directory missing", err) }
	o := &primaryAllocationObserver{dir: *primaryObserverDir, n: b.N}
	if b.N == 128 {
		o.seam, err = caching.BeginPrimaryAllocationObservation()
		if err != nil { b.Fatal(err) }
		// Release even if Open/setup/profile fails. Successful finish releases
		// after the original checked Close; this cleanup then sees nil.
		b.Cleanup(func() { if o.seam != nil { if err := o.seam.Close(); err != nil { b.Error(err) }; o.seam = nil } })
		o.mark("before-profile-start-before-open")
		runtime.GC()
		runtime.GC()
		primaryObserverProfile(b, filepath.Join(o.dir, "before.allocs.pprof"))
		o.mark("before-profile-written-before-open")
	}
	return o
}

func (o *primaryAllocationObserver) mark(position string) {
	if o == nil { return }
	if o.phaseCount == len(o.phases) { panic("PRIMARY diagnostic phase bound exceeded") }
	i := o.phaseCount
	o.phases[i].Position = position
	if o.seam != nil {
		o.seamPhases[i] = o.seam.Snapshot()
		o.phases[i].Seam = &o.seamPhases[i]
	}
	o.phaseCount++
}

// The two extra STW reads are inside the unchanged benchmark timer boundaries,
// immediately adjacent to the original loop. Their cost perturbs diagnostic
// timing; testing's own counters are different snapshots, not asserted equal.
func (o *primaryAllocationObserver) beforeLoop() {
	if o == nil { return }
	o.mark("timer-reset-before-loop-counter-read")
	if o.n == 128 {
		o.loopBefore = o.seam.Snapshot()
		runtime.ReadMemStats(&o.before)
		o.beforeRead = true
	}
}

func (o *primaryAllocationObserver) afterLoop() {
	if o == nil { return }
	if o.n == 128 {
		runtime.ReadMemStats(&o.after)
		o.afterRead = true
		o.loopAfter = o.seam.Snapshot()
	}
	o.loopComplete = true
	o.mark("loop-returned-after-counter-read-before-stop")
}

func (o *primaryAllocationObserver) afterStats(b *testing.B) {
	if o == nil { return }
	o.statsComplete = true
	o.mark("original-generation-stats-and-report-returned")
	if o.n == 128 {
		o.mark("after-profile-start-after-stats-before-close")
		runtime.GC()
		runtime.GC()
		primaryObserverProfile(b, filepath.Join(o.dir, "after.allocs.pprof"))
		o.profilesComplete = true
		o.mark("after-profile-written-before-close")
	}
}

func (o *primaryAllocationObserver) finish(b *testing.B, closeErr error) {
	if o == nil { return }
	o.mark("original-checked-close-returned")
	if o.seam != nil {
		if err := o.seam.Close(); err != nil { b.Error(err) }
		o.seam = nil
	}
	if !o.timerStopped { b.Error("PRIMARY observer incomplete: original timer stop not reached; no raw success receipt"); return }
	status, invocation, filename := "INCOMPLETE", "requested", "primary-raw.json"
	var before, after *runtime.MemStats
	var division *primaryAllocationDivision
	var loopBefore, loopAfter *caching.PrimaryAllocationSeamCounters
	complete := !b.Failed() && closeErr == nil && o.loopComplete && o.statsComplete
	if o.n == 1 {
		invocation, filename = "calibration", "primary-calibration.json"
		if complete { status = "CALIBRATION_UNOBSERVED" }
	} else {
		if o.beforeRead { before = &o.before; loopBefore = &o.loopBefore }
		if o.afterRead { after = &o.after; loopAfter = &o.loopAfter }
		var err error
		division, err = primaryDivideAllocation(before, after, o.n)
		if err != nil { b.Error(err); complete = false }
		if complete && o.profilesComplete { status = "DIAGNOSTIC_COMPLETE_EXTERNAL_PROFILE_VALIDATION_PENDING" }
	}
	value := struct {
		Schema string
		DiagnosticOnly bool
		Status string
		N int
		RequestedN int
		Invocation string
		LoopComplete, CheckedClose bool
		Before, After *runtime.MemStats
		RawDelta *primaryAllocationDivision
		LoopSeamBefore, LoopSeamAfter *caching.PrimaryAllocationSeamCounters
		Phases []primaryAllocationPhase
		CounterScope, ProfileScope, SourceAndELFBinding string
	}{"primary-allocation-observer-v2", true, status, o.n, 128, invocation,
		o.loopComplete, closeErr == nil, before, after, division, loopBefore, loopAfter,
		o.phases[:o.phaseCount],
		"Process-wide; background/observer residual unassigned. Seam fields are separate atomic loads, not one transactional snapshot. No per-DB causal attribution.",
		"Before: two GCs/profile before Open. After: two GCs/profile after original Stats, before Close. Includes setup, workers, Stats and observer allocations; broader than raw loop counters. Close is outside both profiles.",
		"Required external exact-source/ELF/workflow manifest; this JSON alone is insufficient."}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil { b.Fatal(err) }
	if err := primaryObserverWrite(filepath.Join(o.dir, filename), data); err != nil { b.Fatal(err) }
	if status == "INCOMPLETE" { b.Error("PRIMARY diagnostic incomplete; retain receipt and original failure") }
}

func primaryObserverProfile(b *testing.B, path string) {
	b.Helper()
	var buf bytes.Buffer
	if err := pprof.Lookup("allocs").WriteTo(&buf, 0); err != nil { b.Fatal(err) }
	if err := primaryObserverWrite(path, buf.Bytes()); err != nil { b.Fatal(err) }
}

func primaryObserverWrite(path string, data []byte) error {
	if len(data) == 0 || len(data) > 32<<20 { return errors.New("PRIMARY diagnostic output outside 1..32MiB") }
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil { return err }
	n, writeErr := f.Write(data)
	if writeErr == nil && n != len(data) { writeErr = io.ErrShortWrite }
	if writeErr == nil { writeErr = f.Sync() }
	closeErr := f.Close()
	return errors.Join(writeErr, closeErr)
}

func TestPrimaryAllocationRawDeltaAndRefusals(t *testing.T) {
	before := runtime.MemStats{TotalAlloc: 1000, Mallocs: 20}
	after := runtime.MemStats{TotalAlloc: 1385, Mallocs: 149}
	d, err := primaryDivideAllocation(&before, &after, 128)
	if err != nil || d.TotalAlloc != 385 || d.BytesQuotient != 3 || d.BytesRemainder != 1 || d.Mallocs != 129 || d.MallocsQuotient != 1 || d.MallocsRemainder != 1 { t.Fatalf("division: %+v %v", d, err) }
	for i, bad := range []struct { before, after *runtime.MemStats; n int }{
		{nil, &after, 128}, {&before, nil, 128}, {&before, &after, 1}, {&after, &before, 128},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) { if _, err := primaryDivideAllocation(bad.before, bad.after, bad.n); err == nil { t.Fatal("invalid observation accepted") } })
	}
}

func TestPrimaryAllocationOutputIsExclusiveAndBounded(t *testing.T) {
	p := filepath.Join(t.TempDir(), "receipt.json")
	if err := primaryObserverWrite(p, []byte("{}")); err != nil { t.Fatal(err) }
	if err := primaryObserverWrite(p, []byte("different")); err == nil { t.Fatal("overwrite accepted") }
	if err := primaryObserverWrite(filepath.Join(t.TempDir(), "empty"), nil); err == nil { t.Fatal("empty output accepted") }
}
