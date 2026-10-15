//go:build treedb_test

package mvcc

// Cost-only observations reuse the maintained composite fixture and physical
// validation helpers. Production selectors never fall back to the diagnostic.
// Timing and processwide allocation use separate fresh-process invocations.
import (
	"bytes"
	"encoding/json"
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type nativeCostModeV6 string

const (
	nativeCostProductionLatencyV6 nativeCostModeV6 = "production_latency"
	nativeCostProductionAllocV6   nativeCostModeV6 = "production_outer_alloc"
	nativeCostDiagnosticProfileV6 nativeCostModeV6 = "UNBOUNDED_DIAGNOSTIC_allocation_attribution"
)

func TestNativePruneCOWRetainedOutputProductionLatency(t *testing.T) {
	runNativeProductionCostV6(t, nativeCostProductionLatencyV6)
}
func TestNativePruneCOWRetainedOutputProductionAlloc(t *testing.T) {
	runNativeProductionCostV6(t, nativeCostProductionAllocV6)
}
func TestNativePruneCOWRetainedOutputDiagnosticAllocationAttribution(t *testing.T) {
	runNativeProductionCostV6(t, nativeCostDiagnosticProfileV6)
}

func runNativeProductionCostV6(t *testing.T, mode nativeCostModeV6) {
	fixed, multileaf := true, true
	if mode == nativeCostDiagnosticProfileV6 {
		runtime.MemProfileRate = 1
	}
	layouts := []bool{false, true}
	if multileaf {
		layouts = []bool{true}
	}
	for _, pointers := range layouts {
		t.Run(fmt.Sprintf("pointer%t", pointers), func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.MemtableShards = 4
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			opts.ValueLog.ForcePointers = pointers
			opts.ValueLog.PointerThreshold = 4096
			if pointers {
				opts.ValueLog.PointerThreshold = 1
			}
			d, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if e := d.Close(); e != nil {
					t.Error("checked DB Close", e)
				}
			})
			s := New(d)
			selected := []byte("a")
			future := []byte("b")
			if err = s.CommitAt(1, []Mutation{{Key: selected, Value: []byte("disk-original")}}, CommitDurable); err != nil {
				t.Fatal(err)
			}
			if multileaf {
				seed := make([]Mutation, 1024)
				for i := range seed {
					seed[i] = Mutation{Key: []byte(fmt.Sprintf("z%04d-unchanged-backend-long-key-to-form-real-leaves", i)), Value: []byte("unchanged-backend-value")}
				}
				if err = s.CommitAt(100, seed, CommitDurable); err != nil {
					t.Fatal(err)
				}
			}
			if err = d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			var beforeChildren map[page.ChildRef]bool
			if multileaf {
				beforeChildren = nativeDiagnosticDATAChildren(t, d)
				if len(beforeChildren) < 3 {
					t.Fatal("fixture needs actual multileaf DATA", len(beforeChildren))
				}
			}
			// No checkpoint/drain follows: these are genuine C1 overlay publications.
			if err = s.CommitAt(1, []Mutation{{Key: selected, Value: []byte("overlay-replacement")}}, CommitDurable); err != nil {
				t.Fatal(err)
			}
			if err = s.CommitAt(2, []Mutation{{Key: selected, Value: []byte("keeper")}}, CommitDurable); err != nil {
				t.Fatal(err)
			}
			if err = s.CommitAt(100, []Mutation{{Key: future, Value: []byte("future-before")}}, CommitDurable); err != nil {
				t.Fatal(err)
			}
			if err = s.AdvanceDiscardFloor(2, CommitDurable); err != nil {
				t.Fatal(err)
			}
			stats := d.Stats()
			mutable, e := strconv.ParseUint(stats["treedb.cache.mutable_bytes"], 10, 64)
			if e != nil || mutable == 0 || stats["treedb.cache.memtable_mode"] != "cow_btree" {
				t.Fatal("missing actual nonempty COW overlay", stats["treedb.cache.mutable_bytes"], e)
			}
			old, err := d.AcquireMVCCReadCut()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if e := old.Close(); e != nil {
					t.Error("checked old cut Close", e)
				}
			})
			selectedKey, _ := mvcckey.Encode(selected, 1)
			keeperKey, _ := mvcckey.Encode(selected, 2)
			futureKey, _ := mvcckey.Encode(future, 100)
			oldBytes, e := old.Get(selectedKey)
			if e != nil || !bytes.Equal(oldBytes, append([]byte{1}, []byte("overlay-replacement")...)) {
				t.Fatal("overlay winner", e, oldBytes)
			}
			var cursor *PruneCursor
			var ack, writes, prepared, cut, visited uint64
			var pruneErr error
			var installedID, adoptionInputID, publishedID uint64
			var committedAfterInstall, resumedSameID bool
			var firstACKComplete, firstACKIntegrity bool
			var writeElapsed time.Duration
			var wholeBefore, wholeAfter runtime.MemStats
			var profileBefore, profileAfter []runtime.MemProfileRecord
			if mode == nativeCostDiagnosticProfileV6 {
				if os.Getenv("NATIVE_COST_PROFILE_PATH") == "" {
					t.Fatal("NATIVE_COST_PROFILE_PATH is required")
				}
				n, _ := runtime.MemProfile(nil, true)
				profileBefore = make([]runtime.MemProfileRecord, n+4096)
				profileAfter = make([]runtime.MemProfileRecord, n+4096)
				runtime.GC()
				runtime.GC()
				profileBefore = nativeCostMemProfileV6(t, profileBefore)
			}
			if mode == nativeCostProductionAllocV6 {
				runtime.ReadMemStats(&wholeBefore)
			}
			prune := s.PruneVersions
			api := "PruneVersions"
			if mode == nativeCostDiagnosticProfileV6 {
				prune = s.PruneVersionsUnboundedDiagnosticForTest
				api = "PruneVersionsUnboundedDiagnosticForTest"
			}
			var sequenceWall time.Duration
			sequenceStarted := time.Now()
			for call := 0; call < 4096; call++ {
				// Observe the retained ID at call entry, before publication consumes
				// the producer. Final output must bind this exact page below.
				var inputID uint64
				if cursor != nil && cursor.preparation != nil {
					inputID = cursor.preparation.MaterializedInstalledPageForTest()
				}
				x, e := prune(PruneOptions{BatchSize: 1, Mode: CommitDurable, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
				if x.Pruned != 0 {
					sequenceWall = time.Since(sequenceStarted)
				}
				if x.Cursor != nil {
					cursor = x.Cursor
				}
				if cursor != nil && cursor.preparation != nil {
					id := cursor.preparation.MaterializedInstalledPageForTest()
					if committedAfterInstall && id != 0 && id == installedID {
						resumedSameID = true
					}
					if id != 0 && installedID == 0 {
						installedID = id
					}
				}
				visited += x.Visited
				ack += x.Pruned
				if x.Pruned != 0 {
					firstACKComplete, firstACKIntegrity = x.Complete, cursor.integrityDone
					if inputID != 0 && inputID == installedID {
						adoptionInputID = inputID
					}
				}
				if cursor != nil && cursor.preparation != nil {
					_, p, c, _ := cursor.preparation.MaintenanceCurrentDeleteScalarsForTest()
					if p {
						prepared++
					}
					if c {
						cut++
					}
				}
				if e != nil {
					pruneErr = e
					break
				}
				// Every legal nonterminal return must let this same caller complete
				// an ordinary durable write, including an ACK-bearing return.
				if !x.Complete && (!fixed || (cursor.integrityDone && !cursor.loadingFloor && writes == 0 && ack == 0)) {
					writeStarted := time.Now()
					if e = s.CommitAt(100, []Mutation{{Key: future, Value: []byte("future-after")}}, CommitDurable); e != nil {
						t.Fatal(e)
					}
					writeElapsed += time.Since(writeStarted)
					writes++
					if installedID != 0 {
						committedAfterInstall = true
					}
				}
				if ack != 0 {
					break
				}
				if x.Complete {
					break
				}
			}
			if sequenceWall == 0 {
				sequenceWall = time.Since(sequenceStarted)
			}
			if mode == nativeCostProductionAllocV6 {
				runtime.ReadMemStats(&wholeAfter)
			}
			if mode == nativeCostDiagnosticProfileV6 {
				runtime.GC()
				runtime.GC()
				profileAfter = nativeCostMemProfileV6(t, profileAfter)
				nativeCostWriteProfileV6(t, os.Getenv("NATIVE_COST_PROFILE_PATH"), profileBefore, profileAfter)
			}
			// Terminal ACK must release the original prepared/publication authority.
			// This same caller immediately publishes a later durable version.
			if ack == 1 {
				if e = s.CommitAt(101, []Mutation{{Key: future, Value: []byte("after-ACK")}}, CommitDurable); e != nil {
					t.Fatal("post-ACK durable writer", e)
				}
			}
			postACKKey, _ := mvcckey.Encode(future, 101)
			cleanupStarted := time.Now()
			if cursor != nil {
				if e = cursor.Close(); e != nil {
					t.Error("checked native cursor Close", e)
				}
			}
			cursorCleanup := time.Since(cleanupStarted)
			t.Logf("NATIVE_COST_V6 mode=%s api=%s diagnostic=%t fixed_one_write=true first_call_to_ACK_wall_ns=%d intervening_writes=%d write_ns=%d cursor_cleanup_ns=%d whole_sequence_alloc_bytes=%d whole_sequence_alloc_count=%d allocation_scope=processwide_includes_intervening_write_and_runtime ACK=%d nativePrepared_observations=%d nativeCut_observations=%d error=%v", mode, api, mode == nativeCostDiagnosticProfileV6, sequenceWall.Nanoseconds(), writes, writeElapsed.Nanoseconds(), cursorCleanup.Nanoseconds(), wholeAfter.TotalAlloc-wholeBefore.TotalAlloc, wholeAfter.Mallocs-wholeBefore.Mallocs, ack, prepared, cut, pruneErr)
			if ack == 1 {
				got, e := d.Get(selectedKey)
				if e != nil || got != nil {
					t.Fatal("selected survives actual ACK", e, got)
				}
				got, e = d.Get(keeperKey)
				if e != nil || !bytes.Equal(got, append([]byte{1}, []byte("keeper")...)) {
					t.Fatal("keeper", e, got)
				}
			}
			got, e := old.Get(selectedKey)
			if e != nil || !bytes.Equal(got, oldBytes) {
				t.Fatal("retained old cut", e, got)
			}
			wantFuture := []byte("future-before")
			if writes > 0 {
				wantFuture = []byte("future-after")
			}
			got, e = d.Get(futureKey)
			if e != nil || !bytes.Equal(got, append([]byte{1}, wantFuture...)) {
				t.Fatal("current future", e, got)
			}
			if ack == 1 {
				got, e = d.Get(postACKKey)
				if e != nil || !bytes.Equal(got, append([]byte{1}, []byte("after-ACK")...)) {
					t.Fatal("current post-ACK writer", e, got)
				}
			}
			sharedChildren := 0
			if multileaf && ack == 1 {
				afterChildren := nativeDiagnosticDATAChildren(t, d, &publishedID)
				if adoptionInputID != 0 && publishedID == adoptionInputID {
					resumedSameID = true
				}
				nativeDiagnosticPublishedLeafEncoding(t, d, beforeChildren, afterChildren)
				for ref := range afterChildren {
					if beforeChildren[ref] {
						sharedChildren++
					}
				}
				t.Logf("INCREMENTAL_DATA_PATH before_children=%d after_children=%d shared_children=%d", len(beforeChildren), len(afterChildren), sharedChildren)
			}
			if e = old.Close(); e != nil {
				t.Fatal(e)
			}
			if e = d.Close(); e != nil {
				t.Fatal(e)
			}
			reopened, e := treedb.Open(opts)
			if e != nil {
				t.Fatal(e)
			}
			if ack == 1 {
				got, e = reopened.Get(selectedKey)
				if e != nil || got != nil {
					t.Fatal("selected resurrects after reopen", e, got)
				}
			}
			got, e = reopened.Get(futureKey)
			if e != nil || !bytes.Equal(got, append([]byte{1}, wantFuture...)) {
				t.Fatal("reopen future", e, got)
			}
			if ack == 1 {
				got, e = reopened.Get(postACKKey)
				if e != nil || !bytes.Equal(got, append([]byte{1}, []byte("after-ACK")...)) {
					t.Fatal("reopen post-ACK writer", e, got)
				}
			}
			if e = reopened.Close(); e != nil {
				t.Fatal(e)
			}
			if multileaf {
				t.Logf("STREAMING_FIRST_ACK visited=%d expected_anchor_and_obsolete=2 namespace_complete=%t selection_integrity=%t", visited, firstACKComplete, firstACKIntegrity)
				if ack == 1 && (visited != 2 || firstACKComplete || !firstACKIntegrity) {
					t.Fatalf("first selected deletion waited for unrelated future namespace: visited=%d complete=%t selected_integrity=%t", visited, firstACKComplete, firstACKIntegrity)
				}
			}
			if multileaf && ack == 1 && sharedChildren == 0 {
				t.Fatal("incremental publication replaced every unrelated backend ChildRef")
			}
			if multileaf && ack == 1 {
				t.Logf("RETAINED_OUTPUT_ACTIVATION installed_page=%d commit_after_install=%t same_id_resumed=%t published_DATA_page=%d ACK=%d", installedID, committedAfterInstall, resumedSameID, publishedID, ack)
				if installedID == 0 || !resumedSameID || (!fixed && !committedAfterInstall) {
					t.Error("retained original output page did not survive CommitAt and resume before adoption")
				}
			}
			t.Log("NATIVE_COST_CLOSE_REOPEN_CHECKS_COMPLETE")
			if fixed && writes != 1 {
				t.Errorf("fixed-input cost requires exactly one legal pre-ACK write, got %d", writes)
			}
			if pruneErr != nil || ack != 1 || prepared == 0 || writes == 0 {
				t.Fatalf("positive composite publication: error=%v ACK=%d writes=%d prepared=%d cut=%d", pruneErr, ack, writes, prepared, cut)
			}
		})
	}
}

// Raw preallocated snapshots avoid profile encoder allocations in the window.
// Two GCs on each side settle delayed samples, outside clean latency timing.
func nativeCostMemProfileV6(t *testing.T, dst []runtime.MemProfileRecord) []runtime.MemProfileRecord {
	t.Helper()
	n, ok := runtime.MemProfile(dst, true)
	if !ok {
		t.Fatalf("raw allocation profile capacity exceeded: need=%d capacity=%d", n, len(dst))
	}
	return dst[:n]
}

type nativeCostProfileRowV6 struct {
	PCs                   []string
	Frames                []string
	AllocBytes            int64
	AllocObjects          int64
	WindowProductionStack bool
}

func nativeCostWriteProfileV6(t *testing.T, path string, before, after []runtime.MemProfileRecord) {
	t.Helper()
	type counts struct{ b, n int64 }
	old := make(map[[32]uintptr]counts, len(before))
	for _, r := range before {
		c := old[r.Stack0]
		c.b += r.AllocBytes
		c.n += r.AllocObjects
		old[r.Stack0] = c
	}
	// Runtime buckets distinguish allocation size as well as stack. Collapse
	// BOTH snapshots by stack before subtracting cumulative allocation counts.
	current := make(map[[32]uintptr]runtime.MemProfileRecord, len(after))
	for _, r := range after {
		c := current[r.Stack0]
		c.Stack0 = r.Stack0
		c.AllocBytes += r.AllocBytes
		c.AllocObjects += r.AllocObjects
		current[r.Stack0] = c
	}
	rows := make([]nativeCostProfileRowV6, 0, len(after))
	var totalB, totalN, attributedB, attributedN int64
	for _, r := range current {
		prior := old[r.Stack0]
		db, dn := r.AllocBytes-prior.b, r.AllocObjects-prior.n
		if db == 0 && dn == 0 {
			continue
		}
		if db < 0 || dn < 0 {
			t.Fatalf("negative cumulative allocation delta: bytes=%d objects=%d", db, dn)
		}
		row := nativeCostProfileRowV6{AllocBytes: db, AllocObjects: dn}
		for _, pc := range r.Stack() {
			row.PCs = append(row.PCs, fmt.Sprintf("%#x", pc))
		}
		frames := runtime.CallersFrames(r.Stack())
		for {
			f, more := frames.Next()
			row.Frames = append(row.Frames, fmt.Sprintf("%s %s:%d", f.Function, f.File, f.Line))
			if strings.Contains(f.Function, "(*Store).PruneVersions") || strings.Contains(f.Function, "(*Store).CommitAt") {
				row.WindowProductionStack = true
			}
			if !more {
				break
			}
		}
		totalB += db
		totalN += dn
		if row.WindowProductionStack {
			attributedB += db
			attributedN += dn
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].AllocBytes > rows[j].AllocBytes })
	data, err := json.MarshalIndent(struct {
		Scope             string
		Rate              int
		TotalBytes        int64
		TotalObjects      int64
		ProductionBytes   int64
		ProductionObjects int64
		Before            []runtime.MemProfileRecord
		After             []runtime.MemProfileRecord
		Rows              []nativeCostProfileRowV6
	}{"UNBOUNDED_DIAGNOSTIC_only; raw cumulative allocation delta settled with two GCs; production-stack rows include prune and one ordinary CommitAt; all-delta includes runtime/profiler; no production acceptance", 1, totalB, totalN, attributedB, attributedN, before, after, rows}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("DIAGNOSTIC_ALLOCATION_ATTRIBUTION path=%s all_delta_bytes=%d all_delta_objects=%d window_production_stack_bytes=%d window_production_stack_objects=%d", path, totalB, totalN, attributedB, attributedN)
}
