package collections

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestPreparedInsertOverlapsOrderedCommit(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	docs := [][]byte{[]byte(`{"row_id":1,"kind":"one"}`), []byte(`{"row_id":2,"kind":"two"}`)}
	first, err := col.PrepareInsertBatchOwned([][]byte{[]byte("b")}, docs[:1], 1<<20)
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
	second, err := col.PrepareInsertBatchOwned([][]byte{[]byte("a")}, docs[1:], 1<<20)
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
	close(release)
	if err := <-committed; err != nil {
		t.Fatal(err)
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
	prepared, err := col.PrepareInsertBatchOwned(ids, docs, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.OwnedBytes() <= 0 || prepared.OwnedBytes() > 1<<20 {
		t.Fatalf("owned bytes=%d", prepared.OwnedBytes())
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
	if _, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 1); !errors.Is(err, ErrPreparedInsertIneligible) {
		t.Fatalf("oversized prepare error=%v", err)
	}
	prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 1<<20)
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
	prepared, err = col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Commit(); !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("late conflict error=%v, want document exists", err)
	}
	if _, err := col.PrepareInsertBatchOwned([][]byte{id, id}, [][]byte{doc, doc}, 1<<20); !errors.Is(err, ErrDuplicateDocumentID) {
		t.Fatalf("within-batch duplicate error=%v", err)
	}
}

func TestPreparedInsertCheckpointBeforeCommitAndReopen(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	id, doc := []byte("checkpoint-row"), []byte(`{"row_id":41,"kind":"checkpoint"}`)
	beforeLSN := d.State().AppliedCommandLSN
	prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if got := d.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("prepare advanced command LSN from %d to %d", beforeLSN, got)
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
}

func TestPreparedInsertRejectsMismatchedCapturedSchema(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	id := []byte("schema-row")
	prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{[]byte(`{"row_id":42,"kind":"one","payload":"value"}`)}, 1<<20)
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
