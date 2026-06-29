package collections

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCollectionCatalogEOFInsertBatchPreCommitRetryUsesCatalogLoadFaults(t *testing.T) {
	_, col := newCatalogFaultTestCollection(t, CollectionMeta{Name: "users"})
	clearCatalogFaultTestCaches(col)

	var metaFaults atomic.Int32
	var rootFaults atomic.Int32
	restore := setTestCollectionCatalogLoadHookForTest(func(ctx collectionCatalogLoadFaultContext) error {
		if ctx.Collection != "users" {
			return nil
		}
		switch ctx.Stage {
		case collectionCatalogLoadFaultMeta:
			if metaFaults.CompareAndSwap(0, 1) {
				return io.EOF
			}
		case collectionCatalogLoadFaultRoot:
			if ctx.RootName == collectionPrimaryRootName("users") && rootFaults.CompareAndSwap(0, 1) {
				return io.ErrUnexpectedEOF
			}
		}
		return nil
	})
	ids, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"ada"}`)},
	)
	restore()
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if got := metaFaults.Load(); got != 1 {
		t.Fatalf("meta fault count=%d want 1", got)
	}
	if got := rootFaults.Load(); got != 1 {
		t.Fatalf("root fault count=%d want 1", got)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("ids=%q want [u1]", ids)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get(u1): %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("document=%q want %q", got, want)
	}
}

func TestCollectionCatalogEOFInsertBatchRetryExhaustionIncludesCatalogContext(t *testing.T) {
	_, col := newCatalogFaultTestCollection(t, CollectionMeta{Name: "users"})
	clearCatalogFaultTestCaches(col)

	var attempts atomic.Int32
	restore := setTestCollectionCatalogLoadHookForTest(func(ctx collectionCatalogLoadFaultContext) error {
		if ctx.Collection == "users" && ctx.Stage == collectionCatalogLoadFaultMeta {
			attempts.Add(1)
			return io.EOF
		}
		return nil
	})
	_, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"ada"}`)},
	)
	restore()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("InsertBatch err=%v want EOF", err)
	}
	for _, want := range []string{
		fmt.Sprintf("retry budget exceeded after %d attempts", maxCollectionMutationRetries),
		`collections: load catalog "users" meta`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("InsertBatch err=%q missing %q", err, want)
		}
	}
	if got := attempts.Load(); got != maxCollectionMutationRetries {
		t.Fatalf("attempts=%d want %d", got, maxCollectionMutationRetries)
	}
	got, getErr := col.Get([]byte("u1"))
	if getErr != nil {
		t.Fatalf("Get(u1): %v", getErr)
	}
	if got != nil {
		t.Fatalf("Get(u1)=%q want nil after failed insert", got)
	}
}

func TestCollectionCatalogEOFInsertBatchPostCommitReturnsCommitAmbiguous(t *testing.T) {
	d, col := newCatalogFaultTestCollection(t, CollectionMeta{
		Name: "users",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 2,
		}},
	})
	baseCommitSeq, _ := dbCommitSeqAndSystemRoot(d)

	var faults atomic.Int32
	restore := setTestCollectionCatalogLoadHookForTest(func(ctx collectionCatalogLoadFaultContext) error {
		if ctx.Collection == "users" &&
			ctx.Stage == collectionCatalogLoadFaultMeta &&
			ctx.CommitSeq > baseCommitSeq &&
			faults.CompareAndSwap(0, 1) {
			return io.EOF
		}
		return nil
	})
	doc := []byte(`{"name":"ada","embedding":[0.1,0.2]}`)
	ids, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{doc},
	)
	restore()
	if !errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("InsertBatch err=%v want ErrCommitAmbiguous", err)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("InsertBatch err=%v want EOF cause", err)
	}
	if !strings.Contains(err.Error(), "InsertBatch vector index maintenance") {
		t.Fatalf("InsertBatch err=%q missing post-commit operation context", err)
	}
	if got := faults.Load(); got != 1 {
		t.Fatalf("post-commit fault count=%d want 1", got)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("ids=%q want [u1]", ids)
	}
	got, getErr := col.Get([]byte("u1"))
	if getErr != nil {
		t.Fatalf("Get(u1): %v", getErr)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("document=%q want %q", got, doc)
	}
}

func TestCollectionCatalogRootEOFInsertBatchPostCommitReturnsCommitAmbiguous(t *testing.T) {
	d, col := newCatalogFaultTestCollection(t, CollectionMeta{
		Name: "users",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 2,
		}},
	})
	baseCommitSeq, baseSystemRoot := dbCommitSeqAndSystemRoot(d)

	var faults atomic.Int32
	var faultCommitSeq atomic.Uint64
	var faultSystemRoot atomic.Uint64
	restore := setTestCollectionCatalogLoadHookForTest(func(ctx collectionCatalogLoadFaultContext) error {
		if ctx.Collection == "users" &&
			ctx.Stage == collectionCatalogLoadFaultRoot &&
			ctx.RootName == collectionPrimaryRootName("users") &&
			ctx.CommitSeq > baseCommitSeq &&
			ctx.SystemRoot != 0 &&
			ctx.SystemRoot != baseSystemRoot &&
			faults.CompareAndSwap(0, 1) {
			faultCommitSeq.Store(ctx.CommitSeq)
			faultSystemRoot.Store(ctx.SystemRoot)
			return io.ErrUnexpectedEOF
		}
		return nil
	})
	doc := []byte(`{"name":"ada","embedding":[0.1,0.2]}`)
	ids, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{doc},
	)
	restore()
	if !errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("InsertBatch err=%v want ErrCommitAmbiguous", err)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("InsertBatch err=%v want unexpected EOF cause", err)
	}
	if !strings.Contains(err.Error(), `collections: load catalog "users" root "users/primary"`) {
		t.Fatalf("InsertBatch err=%q missing committed root context", err)
	}
	if got := faults.Load(); got != 1 {
		t.Fatalf("post-commit root fault count=%d want 1", got)
	}
	if got := faultCommitSeq.Load(); got <= baseCommitSeq {
		t.Fatalf("fault commit seq=%d want > base %d", got, baseCommitSeq)
	}
	if got := faultSystemRoot.Load(); got == 0 || got == baseSystemRoot {
		t.Fatalf("fault system root=%d want non-zero and different from base %d", got, baseSystemRoot)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("ids=%q want [u1]", ids)
	}
	got, getErr := col.Get([]byte("u1"))
	if getErr != nil {
		t.Fatalf("Get(u1): %v", getErr)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("document=%q want %q", got, doc)
	}
}

func TestCollectionCatalogCachedForcedPointersReadableFromFreshSnapshot(t *testing.T) {
	const collectionName = "ycsb.usertable"
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff

	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open cached backend: %v", err)
	}
	closeDB := collectionMaintenanceCloseOnce(cleanup)
	t.Cleanup(func() { _ = closeDB() })

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: collectionName,
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(collectionName)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	const docCount = 32
	ids := make([][]byte, docCount)
	docs := make([][]byte, docCount)
	for i := 0; i < docCount; i++ {
		id := []byte(fmt.Sprintf("user%06d", i))
		ids[i] = id
		docs[i] = ycsbBSONDocumentForID(t, string(id), i)
	}
	inserted, err := col.InsertBatchValidatedBSON(ids, docs)
	if err != nil {
		t.Fatalf("InsertBatchValidatedBSON: %v", err)
	}
	if len(inserted) != len(ids) {
		t.Fatalf("inserted ids=%d want %d", len(inserted), len(ids))
	}
	for i := range inserted {
		if !bytes.Equal(inserted[i], ids[i]) {
			t.Fatalf("inserted id[%d]=%q want %q", i, inserted[i], ids[i])
		}
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush collection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	requireCollectionCatalogSnapshotDocument(t, d, collectionName, ids[0], docs[0])
	requireCollectionCatalogSnapshotDocument(t, d, collectionName, ids[docCount-1], docs[docCount-1])
	requireCollectionCatalogPrimaryEntryValueLogPointer(t, d, collectionName, ids[docCount-1])

	if err := closeDB(); err != nil {
		t.Fatalf("close cached backend: %v", err)
	}
	reopened, reopenedCleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("reopen cached backend: %v", err)
	}
	defer func() { _ = reopenedCleanup() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection(collectionName)
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	reopenedDoc, found, err := reopenedCol.GetInto(ids[0], nil)
	if err != nil {
		t.Fatalf("reopened GetInto %q: %v", ids[0], err)
	}
	if !found {
		t.Fatalf("reopened GetInto %q found=false", ids[0])
	}
	if !bytes.Equal(reopenedDoc, docs[0]) {
		t.Fatalf("reopened document mismatch: got %d bytes want %d", len(reopenedDoc), len(docs[0]))
	}
	requireCollectionCatalogSnapshotDocument(t, reopened, collectionName, ids[docCount-1], docs[docCount-1])
	requireCollectionCatalogPrimaryEntryValueLogPointer(t, reopened, collectionName, ids[docCount-1])
}

func newCatalogFaultTestCollection(t *testing.T, meta CollectionMeta) (*backenddb.DB, *Collection) {
	t.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&meta); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	return d, col
}

func clearCatalogFaultTestCaches(col *Collection) {
	col.catalogMu.Lock()
	col.catalog = nil
	col.catalogCommitSeq = 0
	col.catalogSystemRoot = 0
	col.catalogMu.Unlock()

	if col.writeDomain == nil {
		return
	}
	col.writeDomain.mu.Lock()
	col.writeDomain.loaded = false
	col.writeDomain.catalog = nil
	col.writeDomain.baseCommitSeq = 0
	col.writeDomain.baseSystemRoot = 0
	col.writeDomain.mu.Unlock()
}

func requireCollectionCatalogSnapshotDocument(t *testing.T, d *backenddb.DB, collectionName string, id, want []byte) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		t.Fatalf("load collection catalog: %v", err)
	}
	if catalog == nil {
		t.Fatalf("collection %q not found", collectionName)
	}
	got, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionPrimaryRootName(collectionName), id, nil)
	if err != nil {
		t.Fatalf("fresh catalog Get %q: %v", id, err)
	}
	if !found {
		t.Fatalf("fresh catalog Get %q found=false", id)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("fresh catalog document %q mismatch: got %d bytes want %d", id, len(got), len(want))
	}
}

func requireCollectionCatalogPrimaryEntryValueLogPointer(t *testing.T, d *backenddb.DB, collectionName string, id []byte) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		t.Fatalf("load collection catalog: %v", err)
	}
	if catalog == nil {
		t.Fatalf("collection %q not found", collectionName)
	}
	entry, rootID, err := collectionGetEntryAtCatalogRoot(snap, catalog, collectionPrimaryRootName(collectionName), id)
	if err != nil {
		t.Fatalf("fresh catalog GetEntry %q: %v", id, err)
	}
	if rootID == 0 {
		t.Fatalf("fresh catalog GetEntry %q returned root 0", id)
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		t.Fatalf("primary catalog entry %q root=%d flags=%#x ptr=%+v, want value-log pointer", id, rootID, entry.Flags, entry.ValuePtr)
	}
}
