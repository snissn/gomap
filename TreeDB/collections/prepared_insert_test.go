package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPreparedInsertOverlapsOrderedCommit(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	sibling := createColumnRetainedSemanticStreamCollection(t, d, "sibling")
	docs := [][]byte{[]byte(`{"row_id":1,"kind":"one"}`), []byte(`{"row_id":2,"kind":"two"}`)}
	first, err := col.PrepareInsertBatchOwned([][]byte{[]byte("b")}, docs[:1], 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var firstPublication sync.Once
	restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error {
		firstPublication.Do(func() { close(entered); <-release })
		return nil
	})
	defer restore()
	committed := make(chan error, 1)
	go func() { _, committedErr := first.Commit(); committed <- committedErr }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not reach publication")
	}
	second, err := col.PrepareInsertBatchOwned([][]byte{[]byte("a")}, docs[1:], 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-committed:
		t.Fatalf("commit acknowledged before release: %v", err)
	default:
	}
	if got, err := col.Get([]byte("b")); err != nil || got != nil {
		t.Fatalf("first batch visible before ordered publication: %s, %v", got, err)
	}
	if got, err := col.Get([]byte("a")); err != nil || got != nil {
		t.Fatalf("second prepared batch visible before its commit: %s, %v", got, err)
	}
	// Launch an ordinary sibling writer while the prepared commit is held.
	// The start signal is before its InsertBatch call; this checks liveness
	// after release, not where inside the write path it was blocked.
	siblingStarted, siblingDone := make(chan struct{}), make(chan error, 1)
	go func() {
		close(siblingStarted)
		_, err := sibling.InsertBatch([][]byte{[]byte("s")}, [][]byte{[]byte(`{"row_id":3,"kind":"sibling"}`)})
		siblingDone <- err
	}()
	<-siblingStarted
	close(release)
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-siblingDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sibling ordinary writer stalled behind prepared commit")
	}
	ids, err := second.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("a")) {
		t.Fatalf("result IDs=%q", ids)
	}
	for _, id := range [][]byte{[]byte("a"), []byte("b")} {
		if _, err := col.Get(id); err != nil {
			t.Fatalf("Get %q: %v", id, err)
		}
	}
	if got, err := sibling.Get([]byte("s")); err != nil || !bytes.Contains(got, []byte(`"sibling"`)) {
		t.Fatalf("sibling ordinary row=%s, %v", got, err)
	}
}

func TestPreparedInsertSortedValuesAndReopen(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids := [][]byte{[]byte("z"), []byte("a"), []byte("m")}
	docs := [][]byte{
		[]byte(`{"row_id":3,"kind":"z","extra":null}`),
		[]byte(`{"row_id":1,"kind":"a"}`),
		[]byte(`{"row_id":2,"kind":"m","extra":{"nested":true}}`),
	}
	prepared, err := col.PrepareInsertBatchOwned(ids, docs, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.OwnedBytes() <= 0 || prepared.OwnedBytes() > 16<<20 {
		t.Fatalf("owned bytes=%d", prepared.OwnedBytes())
	}
	if prepared.ReservedBytes() <= prepared.OwnedBytes() || prepared.ReservedBytes() > 16<<20 {
		t.Fatalf("reservation=%d owned=%d", prepared.ReservedBytes(), prepared.OwnedBytes())
	}
	resultIDs, err := prepared.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if prepared.collection != nil || prepared.meta.Name != "" || prepared.retained.semanticStreamBlocks != nil {
		t.Fatal("consumed batch still retains collection or prepared buffers")
	}
	for i := range ids {
		if !bytes.Equal(resultIDs[i], ids[i]) {
			t.Fatalf("result ID %d=%q want %q", i, resultIDs[i], ids[i])
		}
		got, err := col.Get(ids[i])
		if err != nil || !bytes.Equal(got, docs[i]) {
			t.Fatalf("Get %q=%s, %v want %s", ids[i], got, err, docs[i])
		}
	}
	if _, err := prepared.Commit(); err == nil {
		t.Fatal("second commit succeeded")
	}
	// Ownership returns after Commit. Persistent values and returned IDs must
	// remain intact when the caller reuses the handed-off input buffers.
	wantIDs, wantDocs := make([][]byte, len(ids)), make([][]byte, len(docs))
	for i := range ids {
		wantIDs[i] = bytes.Clone(ids[i])
		wantDocs[i] = bytes.Clone(docs[i])
		for j := range ids[i] {
			ids[i][j] = 'x'
		}
		for j := range docs[i] {
			docs[i][j] = 'x'
		}
		if !bytes.Equal(resultIDs[i], wantIDs[i]) {
			t.Fatalf("returned ID %d changed after caller reuse: %q", i, resultIDs[i])
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col = openColumnRetainedPlacementCollection(t, d, "events")
	for i := range ids {
		got, err := col.Get(wantIDs[i])
		if err != nil || !bytes.Equal(got, wantDocs[i]) {
			t.Fatalf("reopen Get %q=%s, %v want %s", wantIDs[i], got, err, wantDocs[i])
		}
	}
}

func TestPreparedInsertAbandonBoundsAndLateConflict(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	id := []byte("id")
	doc := []byte(`{"row_id":1,"kind":"first"}`)
	if _, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 1); !errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("oversized prepare error=%v", err)
	}
	prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	prepared.Abandon()
	if prepared.collection != nil || prepared.meta.Name != "" || prepared.retained.semanticStreamBlocks != nil {
		t.Fatal("abandoned batch still retains collection or prepared buffers")
	}
	if _, err := prepared.Commit(); err == nil {
		t.Fatal("abandoned commit succeeded")
	}
	if got, err := col.Get(id); err != nil || got != nil {
		t.Fatalf("abandoned prepare published a row: %s, %v", got, err)
	}
	prepared, err = col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Commit(); !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("late conflict error=%v, want document exists", err)
	}
	if _, err := col.PrepareInsertBatchOwned([][]byte{id, id}, [][]byte{doc, doc}, 16<<20); !errors.Is(err, ErrDuplicateDocumentID) {
		t.Fatalf("within-batch duplicate error=%v", err)
	}
}

func TestPreparedInsertCheckpointBeforeCommitAndReopen(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	sibling := createColumnRetainedSemanticStreamCollection(t, d, "sibling")
	id, doc := []byte("checkpoint-row"), []byte(`{"row_id":41,"kind":"checkpoint"}`)
	siblingID, siblingDoc := []byte("sibling-row"), []byte(`{"row_id":42,"kind":"ordinary"}`)
	beforeLSN := d.State().AppliedCommandLSN
	prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	if got := d.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("prepare advanced command LSN from %d to %d", beforeLSN, got)
	}
	if _, err := sibling.InsertBatch([][]byte{siblingID}, [][]byte{siblingDoc}); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, err := col.Get(id); err != nil || got != nil {
		t.Fatalf("prepared row visible before commit: %s, %v", got, err)
	}
	if _, err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := d.State().AppliedCommandLSN; got <= beforeLSN {
		t.Fatalf("commit did not advance command LSN: %d <= %d", got, beforeLSN)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col = openColumnRetainedPlacementCollection(t, d, "events")
	if got, err := col.Get(id); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("reopened committed row=%s, %v want %s", got, err, doc)
	}
	sibling = openColumnRetainedPlacementCollection(t, d, "sibling")
	if got, err := sibling.Get(siblingID); err != nil || !bytes.Equal(got, siblingDoc) {
		t.Fatalf("reopened sibling row=%s, %v want %s", got, err, siblingDoc)
	}
}

func TestPreparedInsertValueLogBlockPointerSurvivesReopenAndGC(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids, docs := retainedSemanticStreamDocuments(96)
	prepared, err := col.PrepareInsertBatchOwned(ids, docs, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	blockKey, row, ptr := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[17])
	if row != 17 || !page.IsValueLogFileID(ptr.FileID) {
		t.Fatalf("prepared locator row=%d pointer=%+v", row, ptr)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col = openColumnRetainedPlacementCollection(t, d, "events")
	if _, err := d.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatal(err)
	}
	reopenedKey, reopenedRow, reopenedPtr := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[17])
	if !bytes.Equal(reopenedKey, blockKey) || reopenedRow != row || reopenedPtr != ptr {
		t.Fatalf("prepared block changed after reopen and GC: row %d/%d pointer %+v/%+v", reopenedRow, row, reopenedPtr, ptr)
	}
	got, err := col.Get(ids[17])
	if err != nil {
		t.Fatal(err)
	}
	assertRetainedSemanticStreamDocument(t, got, 17)
}

func TestPreparedInsertCrashRecoveryCuts(t *testing.T) {
	const id = "prepared-cut-row"
	doc := []byte(`{"row_id":51,"kind":"crash-cut"}`)
	opts := backenddb.Options{CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable}
	if dir := os.Getenv("GOMAP_PREPARED_INSERT_CRASH_DIR"); dir != "" {
		d := openColumnRetainedPlacementDB(t, dir, opts)
		col := openColumnRetainedPlacementCollection(t, d, "events")
		prepared, err := col.PrepareInsertBatchOwned([][]byte{[]byte(id)}, [][]byte{doc}, 16<<20)
		if err != nil {
			t.Fatal(err)
		}
		mode := os.Getenv("GOMAP_PREPARED_INSERT_CRASH_MODE")
		injected := errors.New("prepared commit crash cut")
		var fired atomic.Bool
		if mode == "ack" {
			durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyFileSync {
					fired.Store(true)
				}
				return nil
			})
		} else {
			resource, point := durabilitycut.ResourceCommandWAL, durabilitycut.BeforeDependencyAppend
			switch mode {
			case "wal_after_lsn_assignment":
				point = durabilitycut.AfterDependencyAppend
			case "wal_after_sync":
				point = durabilitycut.AfterDependencyFileSync
			case "asset_before_sync":
				resource, point = durabilitycut.ResourceAuxiliary, durabilitycut.BeforeDependencyFileSync
			case "before_applied_lsn_candidate":
				resource, point = durabilitycut.ResourceMeta, durabilitycut.BeforeAppliedLSNAdvance
			case "after_applied_lsn_candidate":
				resource, point = durabilitycut.ResourceMeta, durabilitycut.AfterAppliedLSNAdvance
			case "before_seal_write":
				resource, point = durabilitycut.ResourceSeal, durabilitycut.BeforePublicationSealWrite
			case "before_meta_write":
				resource, point = durabilitycut.ResourceMeta, durabilitycut.BeforeMetaWrite
			case "wal_before_append":
			default:
				t.Fatalf("unknown crash mode %q", mode)
			}
			durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == resource && event.Point == point && fired.CompareAndSwap(false, true) {
					return injected
				}
				return nil
			})
		}
		_, err = prepared.Commit()
		if (mode == "before_seal_write" || mode == "before_meta_write") && err == nil {
			// Command-WAL acknowledgment can precede queued root installation.
			// Checkpoint waits for that installation while the cut is active.
			err = d.Checkpoint()
		}
		if mode == "ack" {
			if err != nil || !fired.Load() {
				t.Fatalf("durable prepared acknowledgment err=%v WAL sync observed=%t", err, fired.Load())
			}
		} else if !fired.Load() || !errors.Is(err, injected) {
			t.Fatalf("cut %s fired=%t err=%v", mode, fired.Load(), err)
		}
		os.Exit(0) // Simulate process loss without Close or deferred sync.
	}
	for _, mode := range []string{"ack", "wal_before_append", "wal_after_lsn_assignment", "wal_after_sync", "asset_before_sync", "before_applied_lsn_candidate", "after_applied_lsn_candidate", "before_seal_write", "before_meta_write"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
				t.Fatal(err)
			}
			d := openColumnRetainedPlacementDB(t, dir, opts)
			createColumnRetainedSemanticStreamCollection(t, d, "events")
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPreparedInsertCrashRecoveryCuts$")
			cmd.Env = append(os.Environ(), "GOMAP_PREPARED_INSERT_CRASH_DIR="+dir, "GOMAP_PREPARED_INSERT_CRASH_MODE="+mode)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child %s: %v\n%s", mode, err, output)
			}
			d = openColumnRetainedPlacementDB(t, dir, opts)
			defer d.Close()
			col := openColumnRetainedPlacementCollection(t, d, "events")
			got, err := col.Get([]byte(id))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "wal_before_append" {
				if got != nil {
					t.Fatalf("pre-WAL cut recovered uncommitted row: %s", got)
				}
			} else if mode == "wal_after_lsn_assignment" {
				// An append without a successful sync is ambiguous. Recovery may
				// select the complete frame or discard its unsynced suffix.
				if got != nil && !bytes.Equal(got, doc) {
					t.Fatalf("post-append cut recovered wrong row: %s", got)
				}
			} else if !bytes.Equal(got, doc) {
				t.Fatalf("%s recovered row=%s, want %s", mode, got, doc)
			}
		})
	}
}

func TestPreparedInsertRejectsMismatchedCapturedSchema(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	id := []byte("schema-row")
	prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{[]byte(`{"row_id":42,"kind":"one","payload":"value"}`)}, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	// The command-WAL profile rejects catalog index DDL. Inject a mismatch in
	// the captured prepared metadata to exercise commit's catalog comparison.
	prepared.meta.Options.ColumnStore.Columns[1].Name = "changed_kind"
	beforeLSN := d.State().AppliedCommandLSN
	if _, err := prepared.Commit(); err == nil || !strings.Contains(err.Error(), "concurrent schema modification") {
		t.Fatalf("stale prepared commit error=%v", err)
	}
	if got := d.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("rejected commit advanced command LSN from %d to %d", beforeLSN, got)
	}
	if got, err := openColumnRetainedPlacementCollection(t, d, "events").Get(id); err != nil || got != nil {
		t.Fatalf("rejected row visible: %s, %v", got, err)
	}
}

func TestPreparedInsertRarePathsStayWithinBudget(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	var document strings.Builder
	document.WriteString(`{"row_id":1,"kind":"many"`)
	for i := 0; i < 512; i++ {
		fmt.Fprintf(&document, `,"field_%d":%d`, i, i)
	}
	document.WriteByte('}')
	prepared, err := col.PrepareInsertBatchOwned([][]byte{[]byte("wide")}, [][]byte{[]byte(document.String())}, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.OwnedBytes() > 16<<20 {
		t.Fatalf("owned bytes=%d exceed budget", prepared.OwnedBytes())
	}
	if _, err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := col.Get([]byte("wide"))
	if err != nil {
		t.Fatal(err)
	}
	var gotObject, wantObject map[string]any
	if err := json.Unmarshal(got, &gotObject); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(document.String()), &wantObject); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotObject, wantObject) {
		t.Fatal("wide row changed after commit")
	}
}

func TestPreparedInsertRejectsUnsupportedStructuralShapesBeforeCommit(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")

	deep := `{"row_id":1,"kind":"deep","extra":`
	for range preparedSemanticStreamMaxCursorDepth + 1 {
		deep += `{"nested":`
	}
	deep += `true` + strings.Repeat("}", preparedSemanticStreamMaxCursorDepth+1) + `}`
	wide := `{"row_id":2,"kind":"wide"`
	for i := 0; i < preparedSemanticStreamMaxCursorDescriptors+1; i++ {
		wide += fmt.Sprintf(`,"field_%d":%d`, i, i)
	}
	wide += `}`
	oversize := `{"row_id":3,"kind":"oversize","extra":"` + strings.Repeat("x", preparedInsertMaxDocumentBytes) + `"}`
	for i, document := range []string{deep, wide, oversize} {
		id := []byte(fmt.Sprintf("shape-%d", i))
		if _, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{[]byte(document)}, 32<<20); !errors.Is(err, ErrPreparedInsertResourceLimit) {
			t.Errorf("shape %d prepared error=%v, want resource limit", i, err)
		}
		if got, err := col.Get(id); err != nil || got != nil {
			t.Errorf("shape %d visible after rejected prepare: %s, %v", i, got, err)
		}
		if _, err := col.InsertBatch([][]byte{id}, [][]byte{[]byte(document)}); err != nil {
			t.Errorf("shape %d ordinary fallback: %v", i, err)
		}
	}
}

func TestPreparedInsertFallsBackBeforeUnboundedDeclaredRowExtraction(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer d.Close()
	meta := CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "active", Path: "active", ValueType: ColumnStoreValueBool, Owner: TypedStorageOwnerColumnPart}},
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction: ColumnReconstructionRetainedPayloadAndColumns,
	}}}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col := openColumnRetainedPlacementCollection(t, d, "events")
	id, doc := []byte("bool-row"), []byte(`{"row_id":7,"active":true}`)
	if _, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 16<<20); !errors.Is(err, ErrPreparedInsertIneligible) || errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("unsupported declared-row parser shape prepared: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatalf("ordinary fallback: %v", err)
	}
	if got, err := col.Get(id); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("ordinary fallback row=%s, err=%v", got, err)
	}
}
