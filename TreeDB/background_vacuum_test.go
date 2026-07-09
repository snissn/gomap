package treedb

import (
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestBackgroundIndexVacuumIdleUnchangedCommitSkipsStructuralWalks(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                           dir,
		KeepRecent:                    1,
		BackgroundIndexVacuumInterval: -1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	for i := 0; i < 128; i++ {
		key := []byte(fmt.Sprintf("idle-k%04d", i))
		if err := d.Set(key, []byte("value")); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	expected, err := d.backend.IndexVacuumTriggerReport()
	if err != nil {
		t.Fatalf("trigger report: %v", err)
	}
	d.bgVac.spanRatioPPM = ^uint32(0)
	d.bgVac.runOnce(d)
	if got := d.bgVac.lastPages.Load(); got != expected.UserPages {
		t.Fatalf("first probe lastPages=%d want %d", got, expected.UserPages)
	}
	if got := d.bgVac.lastSpanRatio.Load(); got != expected.UserSpanRatioPPM {
		t.Fatalf("first probe lastSpanRatio=%d want %d", got, expected.UserSpanRatioPPM)
	}

	counts, restore := installBackgroundVacuumProbeCounter()
	defer restore()

	d.bgVac.runOnce(d)
	assertNoBackgroundVacuumStructuralWalks(t, counts)

	allocs := testing.AllocsPerRun(100, func() {
		d.bgVac.runOnce(d)
	})
	if allocs > 1 {
		t.Fatalf("unchanged CommitSeq tick allocations = %.2f, want <= 1", allocs)
	}
	assertNoBackgroundVacuumStructuralWalks(t, counts)
}

func TestBackgroundIndexVacuumChangedCommitUsesCheapTrigger(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                           dir,
		KeepRecent:                    1,
		BackgroundIndexVacuumInterval: -1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	for i := 0; i < 128; i++ {
		key := []byte(fmt.Sprintf("changed-k%04d", i))
		if err := d.Set(key, []byte("value")); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	d.bgVac.spanRatioPPM = ^uint32(0)
	d.bgVac.runOnce(d)
	firstProbeSeq := d.bgVac.lastProbeCommitSeq
	if firstProbeSeq == 0 {
		t.Fatalf("first probe did not record commit seq")
	}

	if err := d.Set([]byte("changed-new-key"), []byte("value")); err != nil {
		t.Fatalf("set changed: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint changed: %v", err)
	}
	state := d.backend.State()
	if state == nil {
		t.Fatalf("missing backend state")
	}
	if state.CommitSeq == firstProbeSeq {
		t.Fatalf("write did not advance CommitSeq: %d", state.CommitSeq)
	}

	expected, err := d.backend.IndexVacuumTriggerReport()
	if err != nil {
		t.Fatalf("trigger report after change: %v", err)
	}
	counts, restore := installBackgroundVacuumProbeCounter()
	defer restore()

	d.bgVac.runOnce(d)

	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 1 {
		t.Fatalf("trigger report calls=%d want 1", got)
	}
	if got := counts[backenddb.FragmentationProbeEventTriggerUserTreeWalk]; got != 1 {
		t.Fatalf("trigger user-tree walks=%d want 1", got)
	}
	if got := counts[backenddb.FragmentationProbeEventTriggerFreelistCounters]; got != 1 {
		t.Fatalf("trigger freelist counter reads=%d want 1", got)
	}
	if got := d.bgVac.lastPages.Load(); got != expected.UserPages {
		t.Fatalf("lastPages=%d want %d", got, expected.UserPages)
	}
	if got := d.bgVac.lastSpanRatio.Load(); got != expected.UserSpanRatioPPM {
		t.Fatalf("lastSpanRatio=%d want %d", got, expected.UserSpanRatioPPM)
	}
	if got := d.bgVac.lastProbeCommitSeq; got != expected.CommitSeq {
		t.Fatalf("lastProbeCommitSeq=%d want %d", got, expected.CommitSeq)
	}
	if d.bgVac.retryProbe {
		t.Fatalf("retryProbe left set after successful changed-state trigger")
	}
	assertNoBackgroundVacuumFullFragmentationWalks(t, counts)
}

func installBackgroundVacuumProbeCounter() (map[backenddb.FragmentationProbeEvent]int, func()) {
	counts := make(map[backenddb.FragmentationProbeEvent]int)
	restore := backenddb.SetFragmentationProbeHookForTest(func(event backenddb.FragmentationProbeEvent) {
		counts[event]++
	})
	return counts, restore
}

func assertNoBackgroundVacuumStructuralWalks(t *testing.T, counts map[backenddb.FragmentationProbeEvent]int) {
	t.Helper()
	if got := counts[backenddb.FragmentationProbeEventTriggerReport]; got != 0 {
		t.Fatalf("cheap trigger reports=%d want 0", got)
	}
	if got := counts[backenddb.FragmentationProbeEventTriggerUserTreeWalk]; got != 0 {
		t.Fatalf("cheap trigger user-tree walks=%d want 0", got)
	}
	if got := counts[backenddb.FragmentationProbeEventTriggerFreelistCounters]; got != 0 {
		t.Fatalf("cheap trigger freelist counter reads=%d want 0", got)
	}
	assertNoBackgroundVacuumFullFragmentationWalks(t, counts)
}

func assertNoBackgroundVacuumFullFragmentationWalks(t *testing.T, counts map[backenddb.FragmentationProbeEvent]int) {
	t.Helper()
	if got := counts[backenddb.FragmentationProbeEventFullReport]; got != 0 {
		t.Fatalf("full fragmentation reports=%d want 0", got)
	}
	if got := counts[backenddb.FragmentationProbeEventFullUserTreeWalk]; got != 0 {
		t.Fatalf("full user-tree walks=%d want 0", got)
	}
	if got := counts[backenddb.FragmentationProbeEventFullFreelistChainWalk]; got != 0 {
		t.Fatalf("full freelist-chain walks=%d want 0", got)
	}
	if got := counts[backenddb.FragmentationProbeEventFullCollectionRootWalk]; got != 0 {
		t.Fatalf("full collection-root walks=%d want 0", got)
	}
}

func TestBackgroundIndexVacuumRunsAndStops(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                               dir,
		KeepRecent:                        1,
		BackgroundIndexVacuumInterval:     5 * time.Millisecond,
		BackgroundIndexVacuumSpanRatioPPM: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		val := []byte("v")
		if err := d.Set(key, val); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	var vacuums uint64
	for time.Now().Before(deadline) {
		stats := d.Stats()
		vacuumsStr := stats["treedb.bg_vacuum.vacuums"]
		if vacuumsStr != "" {
			vacuums, _ = strconv.ParseUint(vacuumsStr, 10, 64)
			if vacuums > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if vacuums == 0 {
		t.Fatalf("expected background vacuum to run")
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
