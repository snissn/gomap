package collections_test

import (
	"bytes"
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionGetIntoAllocationBudgetWithCachingObserver(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := treedb.OptionsForBenchmark(treedb.ProfileFast, t.TempDir())
	opts.IndexOuterLeavesInValueLog = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundIndexVacuumInterval = -1
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open backend with cached leaf log: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := cleanup(); err != nil {
				t.Errorf("cleanup backend: %v", err)
			}
		}
	}()

	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "docs",
		Indexes: []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	writerCol, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open writer collection: %v", err)
	}
	docID := []byte("doc-0001")
	want := []byte(`{"email":"alice@example.com","name":"alice"}`)
	if _, err := col.Insert(docID, want); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Drain asynchronous durable-root publication before reading process-global
	// malloc counters in testing.AllocsPerRun.
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after flush: %v", err)
	}

	dst := make([]byte, 0, 256)
	got, found, err := col.GetInto(docID, dst)
	if err != nil || !found || !bytes.Equal(got, want) {
		t.Fatalf("warmup GetInto=(%q, %v, %v), want (%q, true, nil)", got, found, err, want)
	}

	allocs := testing.AllocsPerRun(200, func() {
		var ok bool
		var readErr error
		dst, ok, readErr = col.GetInto(docID, dst)
		if readErr != nil || !ok {
			t.Fatalf("GetInto=(%v, %v), want (true, nil)", ok, readErr)
		}
	})
	if !bytes.Equal(dst, want) {
		t.Fatalf("GetInto=%q, want %q", dst, want)
	}
	t.Logf("Collection.GetInto hit allocs/op=%.0f", allocs)
	if allocs > 0 {
		t.Fatalf("Collection.GetInto hit allocs/op=%.0f, want 0", allocs)
	}

	missingID := []byte("doc-missing")
	missAllocs := testing.AllocsPerRun(200, func() {
		var ok bool
		var readErr error
		dst, ok, readErr = col.GetInto(missingID, dst)
		if readErr != nil || ok || len(dst) != 0 {
			t.Fatalf("missing GetInto=(%q, %v, %v), want (\"\", false, nil)", dst, ok, readErr)
		}
	})
	if missAllocs > 0 {
		t.Fatalf("Collection.GetInto missing allocs/op=%.0f, want 0", missAllocs)
	}

	// Advance commitSeq via a separate handle so col's cached catalog misses once
	// and falls back cleanly to AcquireSnapshot(), then re-warms to 0 allocs/op.
	docID2 := []byte("doc-0002")
	want2 := []byte(`{"email":"bob@example.com","name":"bob"}`)
	if _, err := writerCol.Insert(docID2, want2); err != nil {
		t.Fatalf("writer insert: %v", err)
	}
	if err := writerCol.Flush(); err != nil {
		t.Fatalf("writer flush: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after writer flush: %v", err)
	}
	got2, found2, err := col.GetInto(docID2, dst)
	if err != nil || !found2 || !bytes.Equal(got2, want2) {
		t.Fatalf("post-commit GetInto=(%q, %v, %v), want (%q, true, nil)", got2, found2, err, want2)
	}
	dst = got2
	rewarmAllocs := testing.AllocsPerRun(200, func() {
		var ok bool
		var readErr error
		dst, ok, readErr = col.GetInto(docID2, dst)
		if readErr != nil || !ok {
			t.Fatalf("rewarm GetInto=(%v, %v), want (true, nil)", ok, readErr)
		}
	})
	if rewarmAllocs > 0 {
		t.Fatalf("Collection.GetInto post-commit rewarm allocs/op=%.0f, want 0", rewarmAllocs)
	}

	closed = true
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup backend: %v", err)
	}
	if _, _, err := col.GetInto(docID, dst); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("closed GetInto err=%v, want ErrClosed", err)
	}
}
