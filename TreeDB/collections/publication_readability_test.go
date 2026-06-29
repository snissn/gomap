package collections

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
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
