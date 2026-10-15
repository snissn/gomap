//go:build treedb_test

package mvcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// Positive target-profile control: the same physical key exists in the backend
// and a newer overlay incarnation. Backend-only deletion would resurrect it.
func TestNativePruneCOWUnboundedDiagnosticPublication(t *testing.T) {
	runNativePruneCOWDiagnostic(t, false)
}

// The public lifecycle may be correct while a whole-tree rebuild silently
// discards every unrelated immutable DATA child. This is the path oracle.
func TestNativePruneCOWIncrementalPublication(t *testing.T) {
	runNativePruneCOWDiagnostic(t, true)
}

// A fixed-input absolute cost control, separate from the every-return progress
// test. Exactly one legal selected-integrity yield admits one ordinary write.
func TestNativePruneCOWRetainedOutputCost(t *testing.T) {
	runNativePruneCOWDiagnostic(t, true, true)
}

func runNativePruneCOWDiagnostic(t *testing.T, multileaf bool, fixedInput ...bool) {
	fixed := len(fixedInput) != 0 && fixedInput[0]
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
			var elapsed, writeElapsed time.Duration
			var wholeBefore, wholeAfter runtime.MemStats
			runtime.ReadMemStats(&wholeBefore)
			sequenceStarted := time.Now()
			var sequenceWall time.Duration
			var allocBytes, allocCount uint64
			// Entry-phase buckets include all work performed by that public call.
			// A call may advance through multiple phases; these are not inner-phase timings.
			var phaseCalls, phaseNS, phaseBytes, phaseAllocs [256]uint64
			for call := 0; call < 4096; call++ {
				phase := uint8(0)
				if cursor != nil {
					phase = cursor.phase
				}
				// Observe the retained ID at call entry, before publication consumes
				// the producer. Final output must bind this exact page below.
				var inputID uint64
				if cursor != nil && cursor.preparation != nil {
					inputID = cursor.preparation.MaterializedInstalledPageForTest()
				}
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				x, e := s.PruneVersionsUnboundedDiagnosticForTest(PruneOptions{BatchSize: 1, Mode: CommitDurable, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})
				callElapsed := time.Since(started)
				elapsed += callElapsed
				runtime.ReadMemStats(&after)
				allocBytes += after.TotalAlloc - before.TotalAlloc
				allocCount += after.Mallocs - before.Mallocs
				phaseCalls[phase]++
				phaseNS[phase] += uint64(callElapsed.Nanoseconds())
				phaseBytes[phase] += after.TotalAlloc - before.TotalAlloc
				phaseAllocs[phase] += after.Mallocs - before.Mallocs
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
			sequenceWall = time.Since(sequenceStarted)
			runtime.ReadMemStats(&wholeAfter)
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
			t.Logf("UNBOUNDED_RETAINED_OUTPUT_SEQUENCE fixed_one_write=%t prune_calls_ns=%d first_call_to_ACK_wall_ns=%d intervening_writes=%d write_ns=%d whole_sequence_alloc_bytes=%d whole_sequence_alloc_count=%d allocation_scope=processwide_includes_intervening_writes_and_runtime", fixed, elapsed.Nanoseconds(), sequenceWall.Nanoseconds(), writes, writeElapsed.Nanoseconds(), wholeAfter.TotalAlloc-wholeBefore.TotalAlloc, wholeAfter.Mallocs-wholeBefore.Mallocs)
			t.Logf("UNBOUNDED_INCREMENTAL_CURRENT_DELTA public_ns=%d alloc_bytes=%d alloc_count=%d cursor_cleanup_ns=%d work_counters_incomplete=true", elapsed.Nanoseconds(), allocBytes, allocCount, cursorCleanup.Nanoseconds())
			for phase, calls := range phaseCalls {
				if calls != 0 {
					t.Logf("NATIVE_PUBLIC_ENTRY_PHASE phase=%d calls=%d ns=%d alloc_bytes=%d alloc_count=%d", phase, calls, phaseNS[phase], phaseBytes[phase], phaseAllocs[phase])
				}
			}
			t.Logf("COW_NATIVE_UNBOUNDED_DIAGNOSTIC pointers=%t overlayBytes=%d ACK=%d writes=%d nativePrepared=%d nativeCut=%d error=%v", pointers, mutable, ack, writes, prepared, cut, pruneErr)
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
			if multileaf {
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
			if multileaf && sharedChildren == 0 {
				t.Fatal("incremental publication replaced every unrelated backend ChildRef")
			}
			if multileaf {
				t.Logf("RETAINED_OUTPUT_ACTIVATION installed_page=%d commit_after_install=%t same_id_resumed=%t published_DATA_page=%d ACK=%d", installedID, committedAfterInstall, resumedSameID, publishedID, ack)
				if installedID == 0 || !resumedSameID || (!fixed && !committedAfterInstall) {
					t.Error("retained original output page did not survive CommitAt and resume before adoption")
				}
			}
			if fixed && writes != 1 {
				t.Errorf("fixed-input cost requires exactly one legal pre-ACK write, got %d", writes)
			}
			if pruneErr != nil || ack != 1 || prepared == 0 || writes == 0 {
				t.Fatalf("positive composite publication: error=%v ACK=%d writes=%d prepared=%d cut=%d", pruneErr, ack, writes, prepared, cut)
			}
		})
	}
}

// Scoped scalar copies only: the certificate/image never outlive its original
// backend Snapshot. The child references are observations, not retained owners.
func nativeDiagnosticDATAChildren(t *testing.T, d *treedb.DB, rootOut ...*uint64) map[page.ChildRef]bool {
	t.Helper()
	snapshot := d.NativeMembershipSnapshotForTest()
	if snapshot == nil {
		t.Fatal("backend snapshot")
	}
	defer func() {
		if e := snapshot.Close(); e != nil {
			t.Error(e)
		}
	}()
	cert, e := snapshot.PrimaryReadRootCertificateV6()
	if e != nil || cert == nil {
		t.Fatal("primary certificate", e)
	}
	base, _ := cert.Directory().Base()
	if base.Ref.Kind != page.ChildRefPage {
		t.Fatal("DATA page", base.Ref)
	}
	if len(rootOut) != 0 {
		*rootOut[0] = base.Ref.Page
	}
	image, e := snapshot.Pager().Get(base.Ref.Page)
	if e != nil {
		t.Fatal(e)
	}
	n := node.NewNode(image)
	if n.Type() != page.PageTypeInternal {
		t.Fatal("DATA root must be internal", n.Type())
	}
	refs := make(map[page.ChildRef]bool, int(n.Count()))
	for i := uint16(0); i < n.Count(); i++ {
		_, ref, e := n.GetInternalEntryRefView(i)
		if e != nil {
			t.Fatal(e)
		}
		refs[ref] = true
	}
	return refs
}

// Read only newly published leaf children under the original backend Snapshot.
// This asserts the configured on-disk representation rather than builder options
// or a route counter. These masks are the current pre-alpha node format flags.
func nativeDiagnosticPublishedLeafEncoding(t *testing.T, d *treedb.DB, before, after map[page.ChildRef]bool) {
	t.Helper()
	snapshot := d.NativeMembershipSnapshotForTest()
	if snapshot == nil {
		t.Fatal("encoding snapshot")
	}
	defer func() {
		if e := snapshot.Close(); e != nil {
			t.Error("encoding snapshot Close", e)
		}
	}()
	reader, ok := backenddb.ValueReaderForState(snapshot.State()).(interface {
		ReadLeafLogPageUnsafeTo(page.LeafLogPtr, []byte) ([]byte, bool, error)
	})
	if !ok {
		t.Fatal("original leaf-log reader unavailable")
	}
	checked := 0
	for ref := range after {
		if before[ref] {
			continue
		}
		if !ref.IsLeafLog() {
			t.Fatalf("configured outer leaf placement changed: %+v", ref)
		}
		image, _, e := reader.ReadLeafLogPageUnsafeTo(ref.Log, make([]byte, page.PageSize))
		if e != nil || len(image) != page.PageSize {
			t.Fatal("published leaf image", e, len(image))
		}
		n := node.NewNode(image)
		flags := binary.LittleEndian.Uint16(image[12:14])
		const prefix, columnar, prefixV2, packed, columnarOnlyV2, revision = uint16(0x8000), uint16(0x4000), uint16(0x2000), uint16(0x1000), uint16(0x0400), uint16(0x0040)
		required := prefix | columnar | prefixV2 | packed | revision
		if n.Type() != page.PageTypeLeaf || flags&required != required || flags&columnarOnlyV2 != 0 {
			t.Fatalf("published DATA leaf lost combined-columnar-prefix V2/packed/revision encoding: ref=%+v flags=%#x", ref, flags)
		}
		checked++
		t.Logf("PUBLISHED_LEAF_ENCODING ref=%+v flags=%#x entries=%d combined_columnar_prefix_v2=true packed_pointer=true revisions=true", ref, flags, n.Count())
	}
	if checked == 0 {
		t.Fatal("no newly published leaf encoding observed")
	}
}
