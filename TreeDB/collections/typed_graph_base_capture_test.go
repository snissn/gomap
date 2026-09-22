package collections

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestTypedGraphBaseCaptureAckBeforeSealReplay(t *testing.T) {
	if dir := os.Getenv("GOMAP_TYPED_BASE_CAPTURE_CRASH_DIR"); dir != "" {
		db := openTypedMinimaDB(t, dir)
		col, err := NewCollectionManager(db).OpenCollection("minima")
		if err != nil {
			t.Fatal(err)
		}
		if os.Getenv("GOMAP_TYPED_BASE_CAPTURE_REPLACE") == "1" {
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{0, 1, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"changed"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
			if _, err := col.ReplaceTypedBatch([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		entered := make(chan struct{})
		blocked := make(chan struct{})
		var fired atomic.Bool
		durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Root == dir && event.Resource == durabilitycut.ResourceSeal && event.Point == durabilitycut.BeforePublicationSealWrite && fired.CompareAndSwap(false, true) {
				close(entered)
				<-blocked // os.Exit deliberately leaves the new root unsealed.
			}
			return nil
		})
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			t.Fatal(err)
		}
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("acknowledged rebuild never reached blocked seal")
		}
		os.Exit(0)
	}
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial_empty", true: "replacement"}[replace], func(t *testing.T) {
			dir, db, col := openTypedMinimaCollection(t)
			if replace {
				columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
				if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
					t.Fatal(err)
				}
				if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestTypedGraphBaseCaptureAckBeforeSealReplay$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), "GOMAP_TYPED_BASE_CAPTURE_CRASH_DIR="+dir)
			if replace {
				cmd.Env = append(cmd.Env, "GOMAP_TYPED_BASE_CAPTURE_REPLACE=1")
			}
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("ack-before-seal helper: %v\n%s", err, out)
			}
			var jsonScans atomic.Uint64
			restore := setColumnVectorGraphCanonicalRowsTestHook(func() { jsonScans.Add(1) })
			defer restore()
			db = openTypedMinimaDB(t, dir)
			defer db.Close()
			if jsonScans.Load() != 0 {
				t.Fatal("rebuild replay extracted indexed JSON")
			}
			col, err := NewCollectionManager(db).OpenCollection("minima")
			if err != nil {
				t.Fatal(err)
			}
			snap := db.AcquireSnapshot()
			defer snap.Close()
			catalog, err := loadCollectionCatalog(snap, col.Name())
			if err != nil || catalog == nil || catalog.typedGraphBase == nil || !collectionMetaValuesEqual(catalog.meta, catalog.typedGraphBase.meta) {
				t.Fatalf("replayed capture identity missing: %v", err)
			}
			if _, err := catalog.typedGraphBase.catalog(col, snap); err != nil {
				t.Fatal(err)
			}
			result, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: "embedding_graph", Query: []float32{0, 1, 0, 0, 0, 0, 0, 0}, TopK: 1, EfSearch: 8, IncludeDocuments: true})
			if err != nil {
				t.Fatal(err)
			}
			if replace && (len(result.Results) != 1 || result.Results[0].Score < .999 || !bytes.Contains(result.Results[0].Document, []byte(`"content":"changed"`))) {
				t.Fatalf("replayed replacement missing: %+v", result.Results)
			}
			if !replace && len(result.Results) != 0 {
				t.Fatal("empty replay acquired rows")
			}
		})
	}
}

func TestTypedGraphBaseCutoverProtectsLeasedLazyOldReader(t *testing.T) {
	_, db, col := openTypedMinimaCollection(t)
	defer db.Close()
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original"}}, {Name: "user", Strings: []string{"tenant"}}, {Name: "path", Strings: []string{"source"}}}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	oldPin := db.AcquireSnapshot()
	defer oldPin.Close()
	old, err := loadCollectionCatalog(oldPin, col.Name())
	if err != nil {
		t.Fatal(err)
	}
	// Load only manifest metadata, not graph/typed mapped readers yet.
	requirements, _, err := old.typedGraphBase.requirementsAtSnapshot(oldPin)
	if err != nil {
		t.Fatal(err)
	}
	refs := make([]ColumnAssetRef, 0, len(requirements.Obligations))
	for _, o := range requirements.Obligations {
		refs = append(refs, ColumnAssetRef{Kind: ColumnAssetKind(o.Kind), Namespace: o.Namespace, Generation: o.Generation, PartID: o.PartID, FileID: uint32(o.FileID), Offset: o.Offset, Length: o.Length, Checksum: o.Checksum})
	}
	// This sequential fixture explicitly acquires the existing owner lease
	// before retirement. A raw Snapshot alone does not protect unopened assets;
	// the public capture-to-lease race remains a separate owner admission gate.
	lease, err := col.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{
		Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "captured-base-test", Refs: refs,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	for i := 0; i < 3; i++ {
		columns[0].Float32Vectors[0] = []float32{0, 1, float32(i), 0, 0, 0, 0, 0}
		columns[1].Strings[0] = "changed"
		if _, err := col.ReplaceTypedBatch([][]byte{[]byte("base")}, [][]byte{[]byte(`{"id":"base"}`)}, columns); err != nil {
			t.Fatal(err)
		}
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			t.Fatal(err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	gc, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{Detailed: true})
	if !rootpublication.StableRelativeNamespaceSupported() {
		if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) || gc.SegmentsDeleted != 0 {
			t.Fatalf("unsupported destructive GC must preserve assets: stats=%+v err=%v", gc, err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	t.Logf("default GC deleted=%d", gc.SegmentsDeleted)
	if _, err := old.typedGraphBase.catalog(col, oldPin); err != nil {
		t.Fatalf("lazy old graph open after default GC: %v", err)
	}
	for _, ref := range refs {
		protected := false
		for _, entry := range gc.Plan.Entries {
			if entry.Ref == ref && entry.Status == ColumnAssetReachabilityProtected {
				protected = true
			}
		}
		if !protected {
			t.Fatalf("old snapshot reference not protected: %+v", ref)
		}
	}
	t.Logf("old references protected=%d; unrelated retired segments deleted=%d", len(refs), gc.SegmentsDeleted)
}
