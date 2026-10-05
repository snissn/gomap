//go:build mvcc_native_memory && treedb_test

package mvcc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
)

type nativeMemoryCut struct {
	Name                                                   string `json:"name"`
	HeapAlloc, HeapInuse, HeapObjects, TotalAlloc, Mallocs uint64
	RSS, ProcessHWM                                        uint64
	State                                                  treedb.MaintenanceMemoryStateForTest
	OutputPages                                            int
	OutputBufferBytes                                      uint64
}

// Two scalar copies witness the observed maxima without retaining any owner.
// A zero maximum keeps the zero-value witness: no call or phase is claimed.
type nativeMemoryPeak struct {
	Call  uint64
	State treedb.MaintenanceMemoryStateForTest
}

// The periodic maximum retains only its observing call and RSS scalar.
// Named-cut RSS samples already live in the fixed lifecycle cut list.
type nativeMemoryRSSPeak struct {
	Call, RSS uint64
}

type nativeMemoryResult struct {
	RSSPeriodicPeak                      nativeMemoryRSSPeak
	RSSPeriodicSamples                   uint64
	RetirementPeak, SourceRetirementPeak nativeMemoryPeak
	CursorCloseOracle                    bool

	PID                      int `json:"pid"`
	MaxSourceRetirementCells uint64

	Schema                                                   string `json:"schema"`
	N                                                        int    `json:"n"`
	Mode                                                     string `json:"mode"`
	Pinned                                                   bool   `json:"pinned"`
	Calls, Records, Bytes, Pruned, MaxRecords, MaxBytes      uint64
	MaxRetirementCells                                       uint64
	MaxWindow, MaxFrames, MaxFlatRetiredCap                  int
	SampledMaintenanceRSSPeak                                uint64
	PhysicalBefore, PhysicalAfter                            int
	FixedSurvivors                                           int
	PointerOracle, ReaderOracle, ReopenOracle, CleanupOracle bool
	PartialOutput, RelaxedCustody, ChargedCancel             bool
	Cuts                                                     []nativeMemoryCut `json:"cuts"`
}

func nativeMemoryProc() (rss, hwm uint64) {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		n, _ := strconv.ParseUint(f[1], 10, 64)
		if f[0] == "VmRSS:" {
			rss = n * 1024
		}
		if f[0] == "VmHWM:" {
			hwm = n * 1024
		}
	}
	return
}

// Opt-in: each invocation is one fresh process. All memory numbers are aggregate
// process/Go-runtime observations, including this bounded observer instrumentation.
// RetirementCells*8 is a logical payload lower bound, never exclusive heap usage.
func TestNativePruneMemoryLifecycle(t *testing.T) {
	path := os.Getenv("MVCC_MEMORY_RESULT")
	if path == "" {
		t.Skip("opt-in fresh-process memory harness")
	}
	if !filepath.IsAbs(path) {
		t.Fatal("result path must be absolute")
	}
	n, err := strconv.Atoi(os.Getenv("MVCC_MEMORY_N"))
	if err != nil || n < 64 {
		t.Fatal("MVCC_MEMORY_N must be >=64")
	}
	mode := os.Getenv("MVCC_MEMORY_MODE")
	if mode != "prune" && mode != "control" && mode != "cancel" {
		t.Fatal("invalid memory mode")
	}
	pinned := os.Getenv("MVCC_MEMORY_PINNED") == "1"
	result := nativeMemoryResult{Schema: "gomap-native-memory-v2", PID: os.Getpid(), N: n, Mode: mode, Pinned: pinned, FixedSurvivors: 3}
	var db *treedb.DB
	var cursor *PruneCursor
	var reader treedb.Snapshot
	state := func() treedb.MaintenanceMemoryStateForTest {
		if cursor != nil && cursor.preparation != nil {
			return cursor.preparation.MaintenanceMemoryStateForTest()
		}
		if db != nil {
			return db.MaintenanceMemoryStateForTest()
		}
		return treedb.MaintenanceMemoryStateForTest{}
	}
	cut := func(name string) {
		if len(result.Cuts) >= 16 {
			t.Fatal("unbounded sample list")
		}
		s := state()
		pages := s.Private.Native.ObservedOutputPages
		buffers := s.Private.Native.ObservedOutputBufferBytes
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		rss, hwm := nativeMemoryProc()
		if name != "process_start" && name != "fixture_baseline" && name != "reader_released" && name != "db_close" && name != "reopen" && name != "final_db_close" && name != "directory_cleanup" && rss > result.SampledMaintenanceRSSPeak {
			result.SampledMaintenanceRSSPeak = rss
		}
		result.Cuts = append(result.Cuts, nativeMemoryCut{Name: name, HeapAlloc: m.HeapAlloc, HeapInuse: m.HeapInuse, HeapObjects: m.HeapObjects, TotalAlloc: m.TotalAlloc, Mallocs: m.Mallocs, RSS: rss, ProcessHWM: hwm, State: s, OutputPages: pages, OutputBufferBytes: buffers})
		runtime.KeepAlive(db)
		runtime.KeepAlive(cursor)
		runtime.KeepAlive(reader)
	}
	cut("process_start")
	dir, err := os.MkdirTemp("", "treedb_native_memory_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	options := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	options.IndexOuterLeavesInValueLog = false
	options.ValueLog.PointerThreshold = 2048
	db, err = treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	store := New(db)
	key := []byte("z-history")
	payload := bytes.Repeat([]byte("p"), 8192)
	obsolete := bytes.Repeat([]byte("i"), 1024)
	for ts := 1; ts <= n; ts++ {
		value := obsolete
		if ts == n {
			value = payload
		}
		if err = store.CommitAt(uint64(ts), []Mutation{{Key: key, Value: value}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if err = store.CommitAt(uint64(n+i+1), []Mutation{{Key: []byte(fmt.Sprintf("a-future-%d", i)), Value: payload}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if mode != "control" {
		if err = store.AdvanceDiscardFloor(uint64(n), CommitDurable); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	store = New(db)
	result.PhysicalBefore = n + 2
	eligibilityPhysicalCount(t, store, n+2)
	anchor, _ := mvcckey.Encode(key, uint64(n))
	original, err := db.Get(anchor)
	if err != nil {
		t.Fatal(err)
	}
	oldPhysical, _ := mvcckey.Encode(key, 1)
	oldRecord, err := db.Get(oldPhysical)
	if err != nil {
		t.Fatal(err)
	}
	probe := db.AcquireSnapshot()
	if probe == nil {
		t.Fatal("snapshot")
	}
	beforeReader, err := probe.Get(oldPhysical)
	if err != nil || !bytes.Equal(beforeReader, oldRecord) {
		t.Fatalf("initial raw reader: %v", err)
	}
	entry, err := probe.GetEntryExact(anchor)
	if err != nil || entry.Flags&node.FlagPointer == 0 {
		t.Fatalf("actual persistent pointer missing: %+v %v", entry, err)
	}
	if pinned {
		reader = probe
	} else {
		if err = probe.Close(); err != nil {
			t.Fatal(err)
		}
	}
	probe = nil
	result.PointerOracle = true
	cut("fixture_baseline")
	prepared, partial, accepted, canceling := false, false, false, false
	var cancelRecords, cancelBytes uint64
	for calls := 1; calls <= n*64+32768; calls++ {
		previous := cursor
		stats, e := eligibilityQuantum(t, store, cursor)
		result.Calls++
		result.Records += stats.WorkRecords
		result.Bytes += stats.WorkBytes
		result.Pruned += stats.Pruned
		if stats.WorkRecords > result.MaxRecords {
			result.MaxRecords = stats.WorkRecords
		}
		if stats.WorkBytes > result.MaxBytes {
			result.MaxBytes = stats.WorkBytes
		}
		cursor = stats.Cursor
		// Retain only the last current cursor at a terminal return so its actual
		// Close is included; never retain the continuation-copy chain.
		if cursor == nil && (stats.Complete || canceling) {
			cursor = previous
		}
		s := state()
		cells := s.Private.RetirementCells
		if !s.EOF && !s.Accepted && cells > result.MaxSourceRetirementCells {
			result.MaxSourceRetirementCells = cells
			result.SourceRetirementPeak = nativeMemoryPeak{Call: result.Calls, State: s}
		}
		if cells > result.MaxRetirementCells {
			result.MaxRetirementCells = cells
			result.RetirementPeak = nativeMemoryPeak{Call: result.Calls, State: s}
		}
		window := s.Private.Native.Window
		if s.InputCount > window {
			window = s.InputCount
		}
		if window > result.MaxWindow {
			result.MaxWindow = window
		}
		if s.Private.Native.Frames > result.MaxFrames {
			result.MaxFrames = s.Private.Native.Frames
		}
		flat := s.Private.FlatRetiredCap + s.Private.Native.FlatRetiredCap
		if flat > result.MaxFlatRetiredCap {
			result.MaxFlatRetiredCap = flat
		}
		if window > 32 || flat != 0 {
			t.Fatalf("unbounded descriptor inventory: %+v", s)
		}
		if calls%128 == 0 {
			rss, _ := nativeMemoryProc()
			if rss == 0 {
				t.Fatal("missing periodic Linux RSS observation")
			}
			result.RSSPeriodicSamples++
			if rss > result.RSSPeriodicPeak.RSS {
				result.RSSPeriodicPeak = nativeMemoryRSSPeak{Call: result.Calls, RSS: rss}
			}
			if rss > result.SampledMaintenanceRSSPeak {
				result.SampledMaintenanceRSSPeak = rss
			}
		}
		if !prepared && s.Private.Build {
			prepared = true
			cut("prepared")
		}
		if !partial && s.Private.AllocatedPages > 0 && s.Private.Native.ObservedOutputBufferBytes > 0 && cells > 0 && !s.EOF && !s.Accepted {
			if stats.Pruned != 0 || stats.Batches != 0 {
				t.Fatal("ACK before acceptance")
			}
			partial = true
			result.PartialOutput = true
			cut("partial_private_output")
			if mode == "cancel" {
				if err = db.Set([]byte("foreign-memory-publication"), []byte("fresh")); err != nil {
					t.Fatal(err)
				}
				canceling = true
				cut("cancel_requested")
				continue
			}
		}
		if canceling {
			cancelRecords += stats.WorkRecords
			cancelBytes += stats.WorkBytes
			if stats.Pruned != 0 || stats.Batches != 0 {
				t.Fatal("cancel ACK")
			}
			if !s.Native {
				if e != nil {
					t.Fatal(e)
				}
				if cancelRecords == 0 || cancelBytes == 0 {
					t.Fatal("uncharged cancel")
				}
				result.ChargedCancel = true
				cut("cancel_drained")
				break
			}
		}
		if e != nil {
			t.Fatalf("call%d: %v %+v", calls, e, stats)
		}
		if !accepted && stats.Pruned > 0 {
			if !s.Native || !s.Accepted {
				t.Fatalf("missing actual RELAXED DB custody: %+v", s)
			}
			accepted = true
			result.RelaxedCustody = true
			cut("accepted_relaxed")
		}
		if stats.Complete {
			cut("finish")
			break
		}
		if cursor == nil {
			t.Fatal("lost cursor")
		}
		if calls == n*64+32768 {
			t.Fatalf("lifecycle did not finish: %+v", s)
		}
	}
	if mode != "control" && (!prepared || !partial || result.MaxRetirementCells == 0) {
		t.Fatalf("physical-page/private-output witness missing: %+v", result)
	}
	want := n + 2
	if mode == "prune" {
		want = 3
		if result.Pruned != uint64(n-1) || !accepted {
			t.Fatalf("actual ACK: %+v", result)
		}
	} else if result.Pruned != 0 {
		t.Fatal("unexpected deletion ACK")
	}
	if mode == "cancel" && !result.ChargedCancel {
		t.Fatal("cancel cut never reached")
	}
	if cursor == nil {
		t.Fatal("terminal current cursor missing before Close")
	}
	if cursor != nil {
		if err = cursor.Close(); err != nil {
			t.Fatal(err)
		}
	}
	result.CursorCloseOracle = true
	cursor = nil
	cut("cursor_close")
	s := db.MaintenanceMemoryStateForTest()
	if s.Native || s.Private.Build {
		t.Fatalf("DB cleanup incomplete: %+v", s)
	}
	result.CleanupOracle = true
	cut("db_owned_cleanup")
	eligibilityPhysicalCount(t, store, want)
	result.PhysicalAfter = want
	requireResult(t, store, key, uint64(n+3), Present, uint64(n), payload)
	for i := 0; i < 2; i++ {
		requireResult(t, store, []byte(fmt.Sprintf("a-future-%d", i)), uint64(n+3), Present, uint64(n+i+1), payload)
	}
	if reader != nil {
		retained, e := reader.Get(oldPhysical)
		if e != nil || !bytes.Equal(retained, oldRecord) {
			t.Fatalf("live raw reader: %v", e)
		}
		if err = reader.Close(); err != nil {
			t.Fatal(err)
		}
		reader = nil
	}
	result.ReaderOracle = true
	cut("reader_released")
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	store = nil
	cut("db_close")
	db, err = treedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	store = New(db)
	eligibilityPhysicalCount(t, store, want)
	requireResult(t, store, key, uint64(n+3), Present, uint64(n), payload)
	reopened, e := db.Get(anchor)
	if e != nil || !bytes.Equal(reopened, original) {
		t.Fatalf("reopen pointer: %v", e)
	}
	for i := 0; i < 2; i++ {
		requireResult(t, store, []byte(fmt.Sprintf("a-future-%d", i)), uint64(n+3), Present, uint64(n+i+1), payload)
	}
	if mode == "cancel" {
		foreign, e := db.Get([]byte("foreign-memory-publication"))
		if e != nil || !bytes.Equal(foreign, []byte("fresh")) {
			t.Fatalf("foreign publication reopen: %v", e)
		}
	}
	result.ReopenOracle = true
	cut("reopen")
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	store = nil
	cut("final_db_close")
	if err = os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	cut("directory_cleanup")
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.NewEncoder(f).Encode(result); e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
}
