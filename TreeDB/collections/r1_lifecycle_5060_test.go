package collections

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// The oracle uses one captured view for full rows and postings. It neither
// reconstructs expected rows from storage nor depends on range-document reads.
func r1LifecycleAssert5060(t testing.TB, view *CollectionReadView, want map[string]map[string]any, known map[string]map[string]bool) {
	t.Helper()
	var ids [][]byte
	for id := range known["id"] {
		ids = append(ids, []byte(id))
	}
	slices.SortFunc(ids, func(a, b []byte) int { return cmp.Compare(string(a), string(b)) })
	response, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil || len(response.Results) != len(ids) {
		t.Fatalf("full fetch results=%d want=%d err=%v", len(response.Results), len(ids), err)
	}
	for i, id := range ids {
		row, found := want[string(id)]
		result := response.Results[i]
		if result.Found != found {
			t.Fatalf("id=%s found=%t want=%t", id, result.Found, found)
		}
		if found {
			var got map[string]any
			if err := json.Unmarshal(result.Document, &got); err != nil || !reflect.DeepEqual(got, row) {
				t.Fatalf("id=%s got=%s want=%s err=%v", id, result.Document, r1MutationJSON5059(t, row), err)
			}
		}
	}
	meta, err := view.Meta()
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range meta.Indexes {
		for key := range known[index.Field] {
			var expected, got []string
			for id, row := range want {
				if row[index.Field] == key {
					expected = append(expected, id)
				}
			}
			if err := view.VisitIndexValueIDs(index.Name, key, func(id []byte) error {
				got = append(got, string(id))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			slices.Sort(expected)
			slices.Sort(got)
			if !slices.Equal(got, expected) {
				t.Fatalf("index=%s key=%s got=%q want=%q", index.Name, key, got, expected)
			}
		}
	}
}

func r1LifecycleCurrent5060(t testing.TB, col *Collection, want map[string]map[string]any, known map[string]map[string]bool) {
	t.Helper()
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	r1LifecycleAssert5060(t, view, want, known)
}

func r1LifecycleSeed5060(t testing.TB, col *Collection) (map[string]map[string]any, map[string]map[string]bool) {
	t.Helper()
	want, known := make(map[string]map[string]any), r1MutationKnown5059()
	var rows []map[string]any
	for n := range 32 {
		row := r1MutationRow5059(n)
		rows = append(rows, row)
		want[row["id"].(string)] = row
	}
	r1MutationRemember5059(known, rows...)
	ids, retained, columns := r1MutationBatch5059(t, rows...)
	if out, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil || !reflect.DeepEqual(out, ids) {
		t.Fatalf("seed out=%q err=%v", out, err)
	}
	return want, known
}

// Each cycle admits two new IDs and removes two old IDs, bounding live rows at
// 32 while changing declared, indexed and retained fields across 12 cycles.
func r1LifecycleCycle5060(t *testing.T, col *Collection, cycle int, want map[string]map[string]any, known map[string]map[string]bool) {
	t.Helper()
	fresh := r1MutationRow5059(32 + cycle*2)
	ids, retained, columns := r1MutationBatch5059(t, fresh)
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	want[fresh["id"].(string)] = fresh
	r1MutationRemember5059(known, fresh)
	r1LifecycleCurrent5060(t, col, want, known)

	a, b := r1MutationCopy5059(want["row-024"]), r1MutationCopy5059(want["row-025"])
	a["email"], b["email"] = b["email"], a["email"] // final-owner handoff, repeatedly
	a["bio"], b["revision"] = fmt.Sprintf("cycle %d: 雪", cycle), float64(cycle+1)
	ids, retained, columns = r1MutationBatch5059(t, a, b)
	if result, err := col.ReplaceTypedBatch(ids, retained, columns); err != nil || !reflect.DeepEqual(result, []UpdateBatchResult{{true, true}, {true, true}}) {
		t.Fatalf("replace=%+v err=%v", result, err)
	}
	want["row-024"], want["row-025"] = a, b
	r1MutationRemember5059(known, a, b)
	r1LifecycleCurrent5060(t, col, want, known)

	for _, indexed := range []bool{true, false} {
		id := "row-026"
		if !indexed {
			id = "row-027"
		}
		old, changed := want[id], r1MutationCopy5059(want[id])
		if indexed {
			changed["email"], changed["city"] = fmt.Sprintf("cycle-%d@example.test", cycle), fmt.Sprintf("city-%d", (cycle+3)%8)
		} else {
			changed["name"], changed["score"], changed["active"] = fmt.Sprintf("Updated %d", cycle), float64(cycle)+.75, cycle%2 == 0
			if cycle%2 == 0 {
				changed["optional"] = nil
			} else {
				delete(changed, "optional")
			}
		}
		changed["revision"] = float64(cycle + 1)
		result, err := col.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte(id), Update: func(current []byte) ([]byte, bool, error) {
			assertJSONEqualM13C(t, current, r1MutationJSON5059(t, old))
			return r1MutationJSON5059(t, changed), true, nil
		}}})
		if err != nil || !reflect.DeepEqual(result, []UpdateBatchResult{{true, true}}) {
			t.Fatalf("update=%+v err=%v", result, err)
		}
		want[id] = changed
		r1MutationRemember5059(known, changed)
		r1LifecycleCurrent5060(t, col, want, known)
	}

	newRow, existing := r1MutationRow5059(33+cycle*2), r1MutationCopy5059(want["row-028"])
	existing["age"], existing["revision"] = float64(100+cycle), float64(cycle+1)
	ids, retained, columns = r1MutationBatch5059(t, newRow, existing)
	if matched, err := col.UpsertTypedBatch(ids, retained, columns); err != nil || matched != 1 {
		t.Fatalf("upsert matched=%d err=%v", matched, err)
	}
	want[newRow["id"].(string)], want["row-028"] = newRow, existing
	r1MutationRemember5059(known, newRow, existing)
	r1LifecycleCurrent5060(t, col, want, known)

	deleteIDs := [][]byte{[]byte(fmt.Sprintf("row-%03d", cycle*2)), []byte(fmt.Sprintf("row-%03d", cycle*2+1))}
	if deleted, err := col.DeleteBatch(deleteIDs); err != nil || deleted != 2 {
		t.Fatalf("delete=%d err=%v", deleted, err)
	}
	for _, id := range deleteIDs {
		delete(want, string(id))
	}
	if len(want) != 32 {
		t.Fatalf("live rows=%d want=32", len(want))
	}
	r1LifecycleCurrent5060(t, col, want, known)
}

func TestR1LifecycleMixedCycles5060(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			dir, db, col, cleanup := r1LifecycleNew5060(t, indexed)
			defer func() {
				if err := cleanup(); err != nil {
					t.Error(err)
				}
			}()
			want, known := r1LifecycleSeed5060(t, col)
			var held []*CollectionReadView
			var captured []map[string]map[string]any
			defer func() {
				for _, view := range held {
					_ = view.Close()
				}
			}()
			for cycle := range 12 {
				if cycle == 0 || cycle == 6 {
					view, err := col.OpenCollectionReadView()
					if err != nil {
						t.Fatal(err)
					}
					snapshot := make(map[string]map[string]any, len(want))
					for id, row := range want {
						snapshot[id] = r1MutationCopy5059(row)
					}
					r1LifecycleAssert5060(t, view, snapshot, known) // warm assets before churn
					if stats := view.assetManager.Stats(); stats.ActiveHandles == 0 {
						t.Fatal("warmed view has no active asset handles")
					}
					held, captured = append(held, view), append(captured, snapshot)
				}
				r1LifecycleCycle5060(t, col, cycle, want, known)
				if cycle%3 == 2 {
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
					stats, err := col.CompactRootOverlays(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("cycle=%d overlay compaction=%+v", cycle, stats)
					if stats, err := db.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
						t.Fatal(err)
					} else {
						t.Logf("cycle=%d vlog segments=%d referenced=%d deleted=%d bytes=%d", cycle, stats.SegmentsTotal, stats.SegmentsReferenced, stats.SegmentsDeleted, stats.BytesTotal)
					}
				}
				for i, view := range held {
					r1LifecycleAssert5060(t, view, captured[i], known)
				}
			}
			ptr := requireColumnRetainedPlacementPointer(t, db, "r1", []byte("row-024"))
			vlogRewrite, err := db.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{SourceFileIDs: []uint32{ptr.FileID}, BatchSize: 16, SyncEachBatch: true})
			if err != nil || vlogRewrite.RecordsCopied == 0 {
				t.Fatalf("vlog rewrite copied=%d err=%v", vlogRewrite.RecordsCopied, err)
			}
			t.Logf("vlog rewrite copied=%d reclaimed=%d", vlogRewrite.RecordsCopied, vlogRewrite.SourceSegmentsReclaimed)
			if _, err := db.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
				t.Fatal(err)
			}
			r1LifecycleCurrent5060(t, col, want, known)
			for i, view := range held {
				r1LifecycleAssert5060(t, view, captured[i], known)
			}
			r1LifecycleMaintenance5060(t, db, col, want, known, held, captured)
			if err := cleanup(); err != nil {
				t.Fatal(err)
			}
			db, cleanup, _ = r1LifecycleOpen5060(t, dir)
			col, err = NewCollectionManager(db).OpenCollection("r1")
			if err != nil {
				t.Fatal(err)
			}
			r1LifecycleCurrent5060(t, col, want, known)
		})
	}
}

func r1LifecycleMaintenance5060(t *testing.T, db *backenddb.DB, col *Collection, want map[string]map[string]any, known map[string]map[string]bool, held []*CollectionReadView, captured []map[string]map[string]any) {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	rows := []columnDeclaredRow{{ID: []byte("unpublished-candidate"), Values: []columnDeclaredValue{
		{Type: ColumnStoreValueString, Present: true, String: "orphan@example.test"},
		{Type: ColumnStoreValueString, Present: true, String: "city-0"},
		{Type: ColumnStoreValueString, Present: true, String: "Unpublished"},
		{Type: ColumnStoreValueString, Present: true, String: "Candidate 雪"},
	}}}
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{Collection: "r1", Namespace: cfg.AssetManager.Namespace,
		Generation: 1000, PartID: 1000, AppliedCommandLSN: db.State().AppliedCommandLSN + 1,
		Operation: ColumnPublishOperationInsert, SchemaHash: cfg.SchemaHash, Columns: cfg.Columns, Rows: rows})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := writeColumnPhysicalAssetToManager(db.ColumnAssetRootDir(), *cfg, encoded, 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	opts := ColumnAssetRewriteOptions{Detailed: true, CandidateRefs: []ColumnAssetRef{candidate}}
	pinned, err := col.ColumnAssetRewrite(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if pinned.SegmentsRewritten != 0 || pinned.Plan.MappedResources.ActiveHandles == 0 || pinned.Plan.Sources.MappedResourcePins == 0 {
		t.Fatalf("held-view rewrite=%+v", pinned)
	}
	for i, view := range held {
		r1LifecycleAssert5060(t, view, captured[i], known)
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
		if stats := view.assetManager.Stats(); stats.ActiveHandles != 0 {
			t.Fatalf("view leaked active handles after close: %+v", stats)
		}
	}
	rewrite, err := col.ColumnAssetRewrite(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rewrite.SegmentsRewritten != 1 || rewrite.RefsRemapped == 0 || len(rewrite.SupersededRefs) == 0 {
		t.Fatalf("released rewrite=%+v", rewrite)
	}
	t.Logf("typed rewrite remapped=%d copied=%d retained=%d", rewrite.RefsRemapped, rewrite.BytesCopied, rewrite.BytesRetained)
	r1LifecycleCurrent5060(t, col, want, known)
	namespace, err := columnAssetManagerNamespaceForRoot(db.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(candidate.FileID))
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("rewrite must retain source before GC: %v", err)
	}
	// Settle the rewrite's asynchronous durable publication before asking GC
	// to preserve the still-selectable older fallback. This does not refresh it.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint rewritten roots before protected GC: %v", err)
	}
	candidates := append(slices.Clone(rewrite.SupersededRefs), candidate)
	gc, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{Detailed: true, CandidateRefs: candidates})
	if err != nil {
		t.Fatalf("GC before fallback refresh: %v", err)
	}
	if gc.SegmentsDeleted != 0 {
		t.Fatalf("selectable recovery generation reclaimed early: %+v", gc)
	}
	final := r1LifecycleFinal5060(t, db, col, &candidates)
	gc = final.TypedGC
	if gc.SegmentsDeleted != 1 || gc.BytesDeleted <= 0 {
		t.Fatalf("released settled segment not reclaimed: %+v", gc)
	}
	t.Logf("typed GC deleted=%d bytes=%d after view release and recovery advance", gc.SegmentsDeleted, gc.BytesDeleted)
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old segment stat=%v want removed", err)
	}
	if stats, err := db.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("post-release vlog GC deleted=%d pending=%d bytes=%d", stats.SegmentsDeleted, stats.SegmentsPending, stats.BytesDeleted)
	}
	r1LifecycleCurrent5060(t, col, want, known)
}

// Reuse #5059's actual command-WAL cut process after mixed churn, compaction,
// checkpoint, rewrite and GC; avoid another recovery seam or retry mechanism.
func TestR1LifecycleRecoveryAfterMaintenance5060(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	for _, cut := range []string{"before_append", "after_sync", "ack"} {
		t.Run(cut, func(t *testing.T) {
			dir, db, col, cleanup := r1LifecycleNew5060(t, true)
			want, known := r1LifecycleSeed5060(t, col)
			// Keep rows 0/1 for the shared crash upsert; churn the later seed IDs.
			for cycle := 1; cycle < 4; cycle++ {
				r1LifecycleCycle5060(t, col, cycle, want, known)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if _, err := col.CompactRootOverlays(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
				t.Fatal(err)
			}
			view, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			r1LifecycleAssert5060(t, view, want, known)
			r1LifecycleMaintenance5060(t, db, col, want, known, []*CollectionReadView{view}, []map[string]map[string]any{want})
			if err := cleanup(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestR1LifecycleCrashChild5060$")
			cmd.Env = append(os.Environ(), "GOMAP_R1_5060_CRASH_DIR="+dir, "GOMAP_R1_5059_OPERATION=upsert", "GOMAP_R1_5059_CUT="+cut)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("helper: %v\n%s", err, output)
			}
			rows, _ := r1MutationCrashRows5059("upsert")
			r1MutationRemember5059(known, rows...)
			if cut != "before_append" {
				for _, row := range rows {
					want[row["id"].(string)] = row
				}
			}
			db, cleanup, _ = r1LifecycleOpen5060(t, dir)
			defer cleanup()
			col, err = NewCollectionManager(db).OpenCollection("r1")
			if err != nil {
				t.Fatal(err)
			}
			r1LifecycleCurrent5060(t, col, want, known)
		})
	}
}

func TestR1LifecycleCrashChild5060(t *testing.T) {
	dir := os.Getenv("GOMAP_R1_5060_CRASH_DIR")
	if dir == "" {
		return
	}
	db, _, _ := r1LifecycleOpen5060(t, dir)
	col, err := NewCollectionManager(db).OpenCollection("r1")
	if err != nil {
		t.Fatal(err)
	}
	cut := os.Getenv("GOMAP_R1_5059_CUT")
	injected := errors.New("R1 supported-profile command WAL cut")
	var fired atomic.Bool
	if cut != "ack" {
		point := durabilitycut.BeforeDependencyAppend
		if cut == "after_sync" {
			point = durabilitycut.AfterDependencyFileSync
		}
		durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == point && fired.CompareAndSwap(false, true) {
				return injected
			}
			return nil
		})
	}
	_, err = r1MutationCrashOperation5059(t, col, os.Getenv("GOMAP_R1_5059_OPERATION"))
	if cut == "ack" {
		if err != nil {
			t.Fatal(err)
		}
	} else if !fired.Load() || !errors.Is(err, injected) || errors.Is(err, ErrCommitAmbiguous) != (cut == "after_sync") {
		t.Fatalf("cut=%s fired=%t err=%v", cut, fired.Load(), err)
	}
	if cut == "after_sync" && isRetriableCollectionMutationError(err) {
		t.Fatal("accepted ambiguous command automatically retryable")
	}
	os.Exit(0)
}
