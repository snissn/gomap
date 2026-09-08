package collections

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestTypedGraphFoldCanceledStorageBarrier(t *testing.T) {
	for _, phase := range []string{"capture", "install"} {
		t.Run(phase, func(t *testing.T) {
			col, _, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
			root, err := canonicalVectorPartitionStorageRootV1(col.db.Dir())
			if err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			held := make(chan struct{})
			holderDone := make(chan error, 1)
			hold := func() error {
				go func() {
					holderDone <- WithVectorPartitionStorageBarrierV1(root, func() error { close(held); <-release; return nil })
				}()
				select {
				case <-held:
					return nil
				case <-time.After(5 * time.Second):
					return errors.New("barrier holder timed out")
				}
			}
			defer func() {
				close(release)
				select {
				case err := <-holderDone:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("barrier holder did not exit")
				}
			}()
			var afterCapture func() error
			if phase == "capture" {
				if err := hold(); err != nil {
					t.Fatal(err)
				}
			} else {
				afterCapture = hold
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			seq, system := dbCommitSeqAndSystemRoot(col.db)
			done := make(chan error, 1)
			go func() {
				done <- col.foldTypedGraph(ctx, typedGraphOverlapLimits().Cold, 128, typedGraphFoldTestAssetLimits(), afterCapture)
			}()
			// Observe the actual barrier waiter, not elapsed time or goroutine scheduling.
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				vectorPartitionStorageBarriersV1.Lock()
				entry := vectorPartitionStorageBarriersV1.entries[root]
				waiting := entry != nil && entry.refs == 2
				vectorPartitionStorageBarriersV1.Unlock()
				if waiting {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("fold exited before barrier wait: %v", err)
				case <-deadline.C:
					t.Fatal("fold did not reach held barrier")
				case <-tick.C:
				}
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("fold ignored cancellation while storage barrier remained held")
			}
			if col.collectionSchemaCoordinator().typedGraphFoldActive.Load() {
				t.Fatal("canceled fold retained builder admission")
			}
			if afterSeq, afterSystem := dbCommitSeqAndSystemRoot(col.db); afterSeq != seq || afterSystem != system {
				t.Fatal("canceled fold changed publication authority")
			}
		})
	}
}

func TestTypedGraphFoldKeepsPostCaptureMutation(t *testing.T) {
	col, _, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	limits := typedGraphOverlapLimits()
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 128, Tombstones: 128, ValueSlots: 512, OwnedBytes: 8 << 20}, limits.Cold); err != nil {
		t.Fatal(err)
	}
	oldOwner, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer oldOwner.Close()
	folder, ok := any(col).(interface {
		foldTypedGraph(context.Context, typedGraphColdLimits, int, typedGraphFoldAssetLimits, func() error) error
	})
	if !ok {
		t.Fatal("internal captured-frontier fold is unavailable")
	}
	before, closeBefore, err := col.loadColumnStoreCompactionState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeBefore()
	var latestLSN uint64
	err = folder.foldTypedGraph(context.Background(), typedGraphColdLimits{ManifestRecords: 4096, ManifestBytes: 4 << 20, AssetBytes: 64 << 20, DecodedTermBytes: 64 << 20}, 128, typedGraphFoldTestAssetLimits(), func() error {
		changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"fold-after-capture"}}, {Name: "user", Strings: []string{"new-user"}}, {Name: "path", Strings: []string{"new-path"}}}
		if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
			return err
		}
		state, closeState, err := col.loadColumnStoreCompactionState(context.Background())
		if err != nil {
			return err
		}
		latestLSN = state.manifest.AppliedCommandLSN
		closeState()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	after, closeAfter, err := col.loadColumnStoreCompactionState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeAfter()
	if latestLSN <= before.manifest.AppliedCommandLSN || after.manifest.AppliedCommandLSN != latestLSN {
		t.Fatalf("fold advanced/lost logical frontier: captured=%d mutation=%d current=%d", before.manifest.AppliedCommandLSN, latestLSN, after.manifest.AppliedCommandLSN)
	}
	if after.catalog.typedGraphBase == nil || after.catalog.typedGraphBase.meta.Options.ColumnStore.RecoveryAuthoritativeAppliedCommandLSN != before.manifest.AppliedCommandLSN {
		t.Fatal("fold did not install exact captured base frontier")
	}
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, col.db, col.Name())
	state := columnVectorIndexStateFromRecords1987(t, records, col.Meta().VectorIndexes[0])
	assertTypedGraphSelectedMetadataInventory(t, state.Assets, state.RowCount, 0)
	owner, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if len(owner.overlay.rows) != 1 || owner.state.invalid {
		t.Fatalf("post-fold state rows=%d invalid=%v", len(owner.overlay.rows), owner.state.invalid)
	}
	gc, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{Detailed: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("post-fold default GC removed %d segments", gc.SegmentsDeleted)
	for _, test := range []struct {
		owner   *typedGraphReadOwner
		content string
	}{{owner, "fold-after-capture"}, {oldOwner, "content"}} {
		var buffer VectorIndexSearchBuffer
		results, _, err := test.owner.overlay.search(columns[0].Float32Vectors[0], 1, 16, 128, &buffer)
		if err != nil || len(results) != 1 || !bytes.Equal(results[0].ID, ids[0]) || results[0].Score < .999 {
			t.Fatalf("search=%+v err=%v", results, err)
		}
		fetched, err := test.owner.overlay.current.FetchDocumentsForVectorIndexSearchResults(results, DocumentFetchOptions{})
		if err != nil || len(fetched.Results) != 1 || !bytes.Contains(fetched.Results[0].Document, []byte(`"content":"`+test.content+`"`)) {
			t.Fatalf("fetch=%+v err=%v", fetched, err)
		}
	}
}

func TestTypedGraphFoldSourceSuffixAndStaleCandidate(t *testing.T) {
	col, _, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	limits := typedGraphOverlapLimits()
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 128, Tombstones: 128, ValueSlots: 512, OwnedBytes: 8 << 20}, limits.Cold); err != nil {
		t.Fatal(err)
	}
	other, err := NewCollectionManager(col.db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"new-source"}}, {Name: "user", Strings: []string{"new-user"}}, {Name: "path", Strings: []string{"new-path"}}}
	if err := col.foldTypedGraph(context.Background(), limits.Cold, 128, typedGraphFoldTestAssetLimits(), func() error {
		if _, err := other.DeleteBatch(ids[:2]); err != nil {
			return err
		}
		if _, _, err := other.InsertTypedBatchWithStats(ids[:1], retained[:1], changed); err != nil {
			return err
		}
		_, err := other.ReplaceTypedSourceByID(ids[2:4], ids[2:3], retained[2:3], changed)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 2} {
		doc, err := col.Get(ids[i])
		if err != nil || !bytes.Contains(doc, []byte(`"content":"new-source"`)) {
			t.Fatalf("id%d doc=%s err=%v", i, doc, err)
		}
	}
	for _, i := range []int{1, 3} {
		if doc, err := col.Get(ids[i]); err == nil && len(doc) != 0 {
			t.Fatalf("deleted id%d returned %s", i, doc)
		}
	}
	owner, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		t.Fatal(err)
	}
	if len(owner.overlay.rows) != 4 {
		t.Fatalf("suffix rows=%d want4", len(owner.overlay.rows))
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	beforeDebt := col.collectionSchemaCoordinator().typedGraphCandidateBytes
	err = col.foldTypedGraph(context.Background(), limits.Cold, 128, typedGraphFoldTestAssetLimits(), func() error {
		_, err := other.RebuildVectorIndex("embedding_graph")
		return err
	})
	if !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("stale captured base: %v", err)
	}
	if col.collectionSchemaCoordinator().typedGraphCandidateBytes <= beforeDebt {
		t.Fatal("stale install discarded candidate output debt")
	}
}

func TestTypedGraphFoldEmptyRepeatAndCancel(t *testing.T) {
	for _, n := range []int{0, 1, 8} {
		t.Run(string(rune('0'+n)), func(t *testing.T) {
			col, _, _, _, _, _ := openTypedGraphQualityFixture(t, n)
			limits := typedGraphOverlapLimits()
			for i := 0; i < 2; i++ {
				if err := col.foldTypedGraph(context.Background(), limits.Cold, 128, typedGraphFoldTestAssetLimits(), nil); err != nil {
					t.Fatalf("fold%d: %v", i, err)
				}
				if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 128, Tombstones: 128, ValueSlots: 512, OwnedBytes: 8 << 20}, limits.Cold); err != nil {
					t.Fatal(err)
				}
				owner, err := col.openTypedGraphReadOwner(limits)
				if err != nil {
					t.Fatal(err)
				}
				if len(owner.overlay.rows) != 0 {
					t.Fatal("empty post-fold suffix")
				}
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			before := col.typedGraphPublicationSnapshot()
			ctx, cancel := context.WithCancel(context.Background())
			err := col.foldTypedGraph(ctx, limits.Cold, 128, typedGraphFoldTestAssetLimits(), func() error { cancel(); return nil })
			if !errors.Is(err, context.Canceled) || col.typedGraphPublicationSnapshot() != before {
				t.Fatalf("cancel err=%v changed state=%v", err, col.typedGraphPublicationSnapshot() != before)
			}
			err = col.foldTypedGraph(context.Background(), limits.Cold, 128, typedGraphFoldTestAssetLimits(), func() error {
				return col.foldTypedGraph(context.Background(), limits.Cold, 128, typedGraphFoldTestAssetLimits(), nil)
			})
			if !errors.Is(err, ErrConcurrentMutation) {
				t.Fatalf("second builder: %v", err)
			}
		})
	}
}

func TestTypedGraphFoldBoundsBeforeCaptureAndInstall(t *testing.T) {
	col, _, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	cold := typedGraphOverlapLimits().Cold
	seq, root := dbCommitSeqAndSystemRoot(col.db)
	for _, limit := range []typedGraphColdLimits{
		{ManifestRecords: 1, ManifestBytes: cold.ManifestBytes, AssetBytes: cold.AssetBytes, DecodedTermBytes: cold.DecodedTermBytes},
		{ManifestRecords: cold.ManifestRecords, ManifestBytes: 1, AssetBytes: cold.AssetBytes, DecodedTermBytes: cold.DecodedTermBytes},
		{ManifestRecords: cold.ManifestRecords, ManifestBytes: cold.ManifestBytes, AssetBytes: 1, DecodedTermBytes: cold.DecodedTermBytes},
		{ManifestRecords: cold.ManifestRecords, ManifestBytes: cold.ManifestBytes, AssetBytes: cold.AssetBytes, DecodedTermBytes: 1},
	} {
		if err := col.foldTypedGraph(context.Background(), limit, 128, typedGraphFoldTestAssetLimits(), nil); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
			t.Fatalf("tiny cap: %v", err)
		}
		if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(col.db); afterSeq != seq || afterRoot != root {
			t.Fatal("capture rejection changed authority")
		}
	}
	var latestSeq, latestRoot uint64
	err := col.foldTypedGraph(context.Background(), cold, 8, typedGraphFoldTestAssetLimits(), func() error {
		changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"after-capture"}}, {Name: "user", Strings: []string{"user"}}, {Name: "path", Strings: []string{"path"}}}
		if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
			return err
		}
		latestSeq, latestRoot = dbCommitSeqAndSystemRoot(col.db)
		return nil
	})
	if !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("latest physical rows cap: %v", err)
	}
	if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(col.db); afterSeq != latestSeq || afterRoot != latestRoot {
		t.Fatal("install rejection changed current authority")
	}
	if col.collectionSchemaCoordinator().typedGraphFoldActive.Load() {
		t.Fatal("failed fold retained builder admission")
	}
}
