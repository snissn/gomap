package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogRefTrackerSkipsEmptyCachePersistence(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, valueLogRefCountsFileName)

	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	initialRaw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("expected initial %s after Open: %v", valueLogRefCountsFileName, err)
	}

	// Track atomicfile writes to vlog_ref_counts.meta via durabilitycut:
	// inline writes + ValueLogGC + Checkpoint + Close must NOT rewrite/fsync
	// vlog_ref_counts.meta while value_vlog remains empty.
	var metaSyncs atomic.Int64
	restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeDependencyFileSync &&
			filepath.Dir(e.Path) == filepath.Clean(dir) &&
			bytes.Contains([]byte(filepath.Base(e.Path)), []byte(valueLogRefCountsFileName)) {
			metaSyncs.Add(1)
		}
		return nil
	}))
	defer restore()

	if err := database.SetSync([]byte("inline-k"), []byte("inline-v")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	if _, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	restore()

	if got := metaSyncs.Load(); got != 0 {
		t.Fatalf("empty value_vlog lifecycle synced %s %d times, want 0", valueLogRefCountsFileName, got)
	}
	afterRaw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read %s after close: %v", valueLogRefCountsFileName, err)
	}
	if !bytes.Equal(initialRaw, afterRaw) {
		t.Fatalf("empty value_vlog lifecycle rewrote %s unnecessarily", valueLogRefCountsFileName)
	}

	// Reopen must still recover inline data and rebuild the tracker cleanly.
	reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := reopened.Get([]byte("inline-k"))
	if err != nil || !bytes.Equal(got, []byte("inline-v")) {
		t.Fatalf("reopen Get=(%q, %v), want inline-v", got, err)
	}
	requireValueLogRefTrackerValid(t, reopened)

	// Once an ordinary value_vlog pointer is stored, persistence must write
	// vlog_ref_counts.meta with the non-zero ref count.
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 100, 1, func(int) []byte { return []byte("vlog-payload") })
	if err := reopened.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	b := reopened.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("ptr-k"), ptrs[0]); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := errors.Join(b.WriteSync(), b.Close()); err != nil {
		t.Fatalf("WriteSync pointer: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close after pointer: %v", err)
	}
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("expected %s after value_vlog pointer: %v", valueLogRefCountsFileName, err)
	}
	decoded, err := decodeValueLogRefCounts(raw)
	if err != nil {
		t.Fatalf("decode %s: %v", valueLogRefCountsFileName, err)
	}
	if decoded.counts[ptrs[0].FileID] != 1 {
		t.Fatalf("persisted count for file %d = %d, want 1", ptrs[0].FileID, decoded.counts[ptrs[0].FileID])
	}

	// Deleting the last pointer transitions from non-empty to empty, which MUST
	// refresh the on-disk file so stale non-zero counts are cleared.
	reopened2, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen2: %v", err)
	}
	if err := reopened2.DeleteSync([]byte("ptr-k")); err != nil {
		t.Fatalf("DeleteSync: %v", err)
	}
	wantSeq := reopened2.currentCommitSeq()
	if err := reopened2.Close(); err != nil {
		t.Fatalf("Close after delete: %v", err)
	}
	rawAfterDelete, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read %s after delete: %v", valueLogRefCountsFileName, err)
	}
	decodedAfterDelete, err := decodeValueLogRefCounts(rawAfterDelete)
	if err != nil {
		t.Fatalf("decode after delete: %v", err)
	}
	if decodedAfterDelete.commitSeq != wantSeq || len(decodedAfterDelete.counts) != 0 {
		t.Fatalf("refreshed cache after delete = (seq=%d counts=%v), want (seq=%d empty)",
			decodedAfterDelete.commitSeq, decodedAfterDelete.counts, wantSeq)
	}
}

func TestOpenSeedsStableIndexNamespaceProof(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable relative namespace unsupported on this platform")
	}
	for _, primary := range []bool{false, true} {
		name := "DATA"
		if primary {
			name = "PRIMARY"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			database, err := Open(Options{
				Dir:                    dir,
				IndexPrimaryDirectory:  primary,
				DisableBackgroundPrune: true,
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() {
				if err := database.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			}()

			gen := database.idx.Load()
			gen.stableNamespaceMu.Lock()
			proofBefore := gen.stableNamespaceProof
			parentBefore := gen.stableNamespaceParent
			gen.stableNamespaceMu.Unlock()
			if proofBefore == nil || parentBefore == nil {
				t.Fatalf("Open did not seed index.db stable namespace proof: proof=%v parent=%v", proofBefore, parentBefore)
			}

			if err := database.SetSync([]byte("k1"), []byte("v1")); err != nil {
				t.Fatalf("SetSync: %v", err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}

			gen.stableNamespaceMu.Lock()
			proofAfter := gen.stableNamespaceProof
			parentAfter := gen.stableNamespaceParent
			gen.stableNamespaceMu.Unlock()
			if proofAfter != proofBefore || parentAfter != parentBefore {
				t.Fatal("first SetSync/Checkpoint replaced seeded index.db stable namespace proof")
			}
		})
	}
}

func TestDeferredLeafLogDirSyncAmortizedAndFailClosed(t *testing.T) {
	t.Run("amortized_by_manifest_replacement", func(t *testing.T) {
		if !rootpublication.StableRelativeNamespaceSupported() {
			t.Skip("stable relative namespace unsupported on this platform")
		}
		database, leafLog, opts := openLeafLogLaneGCTestDB(t, 64<<10)
		defer closeLeafLogLaneGCTestDB(t, database, leafLog)

		// Simulate a deferred leaf_vlog segment creation before a commit that
		// binds a new immutable leaf-generation manifest revision.
		database.NoteLeafLogDirSyncPending()
		b := database.NewBatch()
		for i := 0; i < 64; i++ {
			if err := b.Set([]byte(fmt.Sprintf("k-%04d", i)), bytes.Repeat([]byte("a"), 128)); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
		if err := errors.Join(b.WriteSync(), b.Close()); err != nil {
			t.Fatalf("WriteSync: %v", err)
		}

		if synced, unsynced := database.leafLogDirSyncedSeq.Load(), database.leafLogDirUnsyncedSeq.Load(); synced < unsynced {
			t.Fatalf("manifest replacement did not cover deferred leaf_vlog dir sync: synced=%d unsynced=%d", synced, unsynced)
		}

		// Calling the pre-meta helper afterwards must be a zero-I/O no-op.
		var extraDirSyncs atomic.Int64
		restore := durabilitycut.Install(func(e durabilitycut.Event) error {
			if e.Point == durabilitycut.BeforeNewFileDirectorySync && filepath.Clean(e.Root) == filepath.Clean(LeafLogDirPath(opts.Dir)) {
				extraDirSyncs.Add(1)
			}
			return nil
		})
		defer restore()
		if err := database.syncPendingLeafLogDirIfNeeded(); err != nil {
			t.Fatalf("syncPendingLeafLogDirIfNeeded: %v", err)
		}
		if got := extraDirSyncs.Load(); got != 0 {
			t.Fatalf("already-covered syncPendingLeafLogDirIfNeeded emitted %d dir syncs, want 0", got)
		}
	})

	t.Run("fail_closed_when_no_manifest_replacement", func(t *testing.T) {
		database, leafLog, opts := openLeafLogLaneGCTestDB(t, 64<<10)
		defer closeLeafLogLaneGCTestDB(t, database, leafLog)

		// Establish an initial leaf segment + manifest revision so a small follow-up
		// write reuses the existing segment and does NOT replace the manifest.
		if err := database.SetSync([]byte("seed"), bytes.Repeat([]byte("s"), 64)); err != nil {
			t.Fatalf("seed SetSync: %v", err)
		}

		leafDir := filepath.Clean(LeafLogDirPath(opts.Dir))
		var mu sync.Mutex
		var order []durabilitycut.Point
		restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
			mu.Lock()
			defer mu.Unlock()
			switch e.Point {
			case durabilitycut.BeforeNewFileDirectorySync, durabilitycut.AfterNewFileDirectorySync:
				if e.Resource == durabilitycut.ResourceOuterLeaf && filepath.Clean(e.Root) == leafDir {
					order = append(order, e.Point)
				}
			case durabilitycut.BeforeMetaWrite:
				order = append(order, e.Point)
			}
			return nil
		}))
		defer restore()

		// Record a deferred leaf_vlog creation that no manifest replacement covers.
		database.NoteLeafLogDirSyncPending()
		if err := database.SetSync([]byte("follow"), []byte("val")); err != nil {
			t.Fatalf("follow SetSync: %v", err)
		}
		restore()

		mu.Lock()
		gotOrder := append([]durabilitycut.Point(nil), order...)
		mu.Unlock()
		beforeSyncIdx, afterSyncIdx, beforeMetaIdx := -1, -1, -1
		for i, pt := range gotOrder {
			if pt == durabilitycut.BeforeNewFileDirectorySync && beforeSyncIdx < 0 {
				beforeSyncIdx = i
			}
			if pt == durabilitycut.AfterNewFileDirectorySync && afterSyncIdx < 0 {
				afterSyncIdx = i
			}
			if pt == durabilitycut.BeforeMetaWrite && beforeMetaIdx < 0 {
				beforeMetaIdx = i
			}
		}
		if beforeSyncIdx < 0 || afterSyncIdx <= beforeSyncIdx || beforeMetaIdx <= afterSyncIdx {
			t.Fatalf("fail-closed leaf_vlog dir sync must precede BeforeMetaWrite; observed=%v", gotOrder)
		}
	})

	t.Run("fail_closed_error_blocks_meta_write", func(t *testing.T) {
		database, leafLog, opts := openLeafLogLaneGCTestDB(t, 64<<10)
		defer closeLeafLogLaneGCTestDB(t, database, leafLog)

		if err := database.SetSync([]byte("seed"), []byte("v0")); err != nil {
			t.Fatalf("seed SetSync: %v", err)
		}

		leafDir := filepath.Clean(LeafLogDirPath(opts.Dir))
		injected := errors.New("injected leaf_vlog dir sync failure")
		var metaWrites atomic.Int64
		restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
			if e.Point == durabilitycut.BeforeNewFileDirectorySync && e.Resource == durabilitycut.ResourceOuterLeaf && filepath.Clean(e.Root) == leafDir {
				return injected
			}
			if e.Point == durabilitycut.BeforeMetaWrite {
				metaWrites.Add(1)
			}
			return nil
		}))
		defer restore()

		database.NoteLeafLogDirSyncPending()
		err := database.SetSync([]byte("blocked"), []byte("v1"))
		restore()
		if !errors.Is(err, injected) {
			t.Fatalf("SetSync error=%v, want %v", err, injected)
		}
		if got := metaWrites.Load(); got != 0 {
			t.Fatalf("failed leaf_vlog dir sync still performed %d meta writes, want 0", got)
		}
		// Unsynced sequence must not advance on error.
		if database.leafLogDirSyncedSeq.Load() >= database.leafLogDirUnsyncedSeq.Load() {
			t.Fatal("failed dir sync incorrectly advanced leafLogDirSyncedSeq")
		}
	})

	t.Run("concurrent_creation_after_manifest_sample_still_synced", func(t *testing.T) {
		database, leafLog, opts := openLeafLogLaneGCTestDB(t, 64<<10)
		defer closeLeafLogLaneGCTestDB(t, database, leafLog)

		database.NoteLeafLogDirSyncPending() // seq = 1
		seq1 := database.leafLogDirUnsyncedSeq.Load()
		database.NoteLeafLogDirSyncPending() // seq = 2 (created after sample!)
		database.markLeafLogDirSyncedThrough(seq1)

		leafDir := filepath.Clean(LeafLogDirPath(opts.Dir))
		var synced atomic.Int64
		restore := durabilitycut.Install(func(e durabilitycut.Event) error {
			if e.Point == durabilitycut.AfterNewFileDirectorySync && e.Resource == durabilitycut.ResourceOuterLeaf && filepath.Clean(e.Root) == leafDir {
				synced.Add(1)
			}
			return nil
		})
		defer restore()

		if err := database.syncPendingLeafLogDirIfNeeded(); err != nil {
			t.Fatalf("syncPendingLeafLogDirIfNeeded: %v", err)
		}
		if got := synced.Load(); got != 1 {
			t.Fatalf("concurrent post-sample creation synced %d times, want 1", got)
		}
		if database.leafLogDirSyncedSeq.Load() < database.leafLogDirUnsyncedSeq.Load() {
			t.Fatal("leafLogDirSyncedSeq did not catch up to concurrent creation")
		}
	})
}

func TestPreSyncedDependenciesBeforeManifestReplacement(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable relative namespace unsupported on this platform")
	}

	t.Run("syncs_dependency_once_without_redundant_publish_sync", func(t *testing.T) {
		database, leafLog, _ := openLeafLogLaneGCTestDB(t, 64<<10)
		defer closeLeafLogLaneGCTestDB(t, database, leafLog)

		// Without durabilitycut enabled, the first write that creates a leaf_vlog
		// segment and replaces the manifest pre-syncs external dependencies
		// sequentially before manifest replacement and skips re-syncing in Publish().
		b := database.NewBatch()
		for i := 0; i < 64; i++ {
			if err := b.Set([]byte(fmt.Sprintf("pk-%04d", i)), bytes.Repeat([]byte("p"), 128)); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
		if err := errors.Join(b.WriteSync(), b.Close()); err != nil {
			t.Fatalf("WriteSync: %v", err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}

		database.durablePublishMu.Lock()
		slotRes := database.durableRoot.slotResources[database.durableRoot.slot]
		var leafPhysicalSyncs uint64
		if slotRes != nil {
			for _, st := range slotRes.Stats(time.Now()) {
				if st.Kind == rootpublication.ResourceOuterLeafLog {
					leafPhysicalSyncs += st.PhysicalFileSyncs
				}
			}
		}
		database.durablePublishMu.Unlock()
		if leafPhysicalSyncs > 1 {
			t.Fatalf("outer-leaf log physical syncs = %d, want <= 1 (no redundant sync in Publish after pre-sync)", leafPhysicalSyncs)
		}
	})

	t.Run("syncs_dependency_strictly_before_manifest_temp_create", func(t *testing.T) {
		database, leafLog, opts := openLeafLogLaneGCTestDB(t, 64<<10)
		defer closeLeafLogLaneGCTestDB(t, database, leafLog)

		if err := database.SetSync([]byte("base"), bytes.Repeat([]byte("b"), 64)); err != nil {
			t.Fatalf("SetSync base: %v", err)
		}
		leafDir := LeafLogDirPath(opts.Dir)
		tmpFile, err := os.CreateTemp(t.TempDir(), "ordered-dep-*")
		if err != nil {
			t.Fatalf("CreateTemp: %v", err)
		}
		defer func() { _ = tmpFile.Close() }()
		if _, err := tmpFile.Write([]byte("payload")); err != nil {
			t.Fatalf("Write: %v", err)
		}

		var mu sync.Mutex
		var events []string
		depToken, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
			Kind:           rootpublication.ResourceValueLog,
			LogicalLane:    "test/order",
			ResourceID:     "ordered-dep",
			Generation:     998,
			DiagnosticPath: "value_vlog/ordered-dep",
			Reachability:   rootpublication.ReachabilityValueLogPointer,
			File:           tmpFile,
			Frontier:       rootpublication.DurableFrontier{Bytes: 7},
			SyncThrough: func(*os.File, rootpublication.DurableFrontier) error {
				mu.Lock()
				events = append(events, "dep-sync")
				mu.Unlock()
				return nil
			},
		})
		if err != nil {
			t.Fatalf("NewStableResourceToken: %v", err)
		}
		builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
		if err := builder.Add(depToken); err != nil {
			depToken.Release()
			t.Fatalf("Add dep token: %v", err)
		}
		depSet, err := builder.Freeze()
		if err != nil {
			builder.Abandon()
			t.Fatalf("Freeze dep set: %v", err)
		}

		packFileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 98)
		if err != nil {
			depSet.Release()
			t.Fatalf("EncodeFileID: %v", err)
		}
		packPath := filepath.Join(leafDir, "value-l255-000098.log")
		if err := os.WriteFile(packPath, []byte("leaf-pack-stub"), 0o644); err != nil {
			depSet.Release()
			t.Fatalf("write stub leaf segment: %v", err)
		}
		if err := database.RefreshValueLogSet(); err != nil {
			depSet.Release()
			t.Fatalf("RefreshValueLogSet: %v", err)
		}
		packRawID := page.ValueLogSegmentID(packFileID)

		store := database.leafGenerationManifestStore
		prevHook := store.hooks.BeforeTempCreate
		store.hooks.BeforeTempCreate = func() error {
			mu.Lock()
			events = append(events, "manifest-temp-create")
			mu.Unlock()
			return nil
		}
		defer func() { store.hooks.BeforeTempCreate = prevHook }()

		database.mu.RLock()
		packBase := database.leafGenerationManifest.clone()
		nextMeta := database.meta
		database.mu.RUnlock()
		nextMeta.CommitSeq++

		finalizeOpts := finalizeCommitOptions{
			packedRootPublication: &packedRootPublication{
				afterManifestPreparation: func() error { return nil },
			},
		}
		res, update, bindErr := database.bindPublicationLeafManifest(depSet, database.idx.Load(), nextMeta, packBase, []uint32{packRawID}, &finalizeOpts)
		if bindErr != nil {
			t.Fatalf("bindPublicationLeafManifest: %v", bindErr)
		}
		defer func() {
			if res != nil {
				res.Release()
			}
			_ = update.finish(false)
		}()
		if !update.dependenciesPreSynced {
			t.Fatal("expected update.dependenciesPreSynced == true")
		}
		mu.Lock()
		gotEvents := append([]string(nil), events...)
		mu.Unlock()
		if len(gotEvents) != 2 || gotEvents[0] != "dep-sync" || gotEvents[1] != "manifest-temp-create" {
			t.Fatalf("unexpected barrier order: got=%v, want [dep-sync manifest-temp-create]", gotEvents)
		}
	})

	t.Run("dependency_sync_error_skips_manifest_creation", func(t *testing.T) {
		database, leafLog, opts := openLeafLogLaneGCTestDB(t, 64<<10)
		defer closeLeafLogLaneGCTestDB(t, database, leafLog)

		// Create a valid initial state first.
		if err := database.SetSync([]byte("base"), bytes.Repeat([]byte("b"), 64)); err != nil {
			t.Fatalf("SetSync base: %v", err)
		}
		leafDir := LeafLogDirPath(opts.Dir)
		beforeFiles, err := filepath.Glob(filepath.Join(leafDir, "manifest.durable.*.json"))
		if err != nil {
			t.Fatalf("glob before: %v", err)
		}

		// Build a synthetic resource set with a failing SyncThrough and invoke
		// bindPublicationLeafManifest directly with !durabilitycut.Enabled() to
		// verify that dependency sync failure fails closed BEFORE creating any
		// manifest temp/revision file.
		syncBoom := errors.New("injected dependency sync error")
		tmpFile, err := os.CreateTemp(t.TempDir(), "failing-dep-*")
		if err != nil {
			t.Fatalf("CreateTemp: %v", err)
		}
		defer func() { _ = tmpFile.Close() }()
		if _, err := tmpFile.Write([]byte("payload")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		failingToken, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
			Kind:           rootpublication.ResourceValueLog,
			LogicalLane:    "test/fail",
			ResourceID:     "failing-dep",
			Generation:     999,
			DiagnosticPath: "value_vlog/failing-dep",
			Reachability:   rootpublication.ReachabilityValueLogPointer,
			File:           tmpFile,
			Frontier:       rootpublication.DurableFrontier{Bytes: 7},
			SyncThrough: func(*os.File, rootpublication.DurableFrontier) error {
				return syncBoom
			},
		})
		if err != nil {
			t.Fatalf("NewStableResourceToken: %v", err)
		}
		failingBuilder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
		if err := failingBuilder.Add(failingToken); err != nil {
			failingToken.Release()
			t.Fatalf("Add failing token: %v", err)
		}
		failingSet, err := failingBuilder.Freeze()
		if err != nil {
			failingBuilder.Abandon()
			t.Fatalf("Freeze failing set: %v", err)
		}

		packFileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 99)
		if err != nil {
			failingSet.Release()
			t.Fatalf("EncodeFileID: %v", err)
		}
		packPath := filepath.Join(leafDir, "value-l255-000099.log")
		if err := os.WriteFile(packPath, []byte("leaf-pack-stub"), 0o644); err != nil {
			failingSet.Release()
			t.Fatalf("write stub leaf segment: %v", err)
		}
		if err := database.RefreshValueLogSet(); err != nil {
			failingSet.Release()
			t.Fatalf("RefreshValueLogSet: %v", err)
		}
		packRawID := page.ValueLogSegmentID(packFileID)

		var tempCreates atomic.Int64
		store := database.leafGenerationManifestStore
		prevHook := store.hooks.BeforeTempCreate
		store.hooks.BeforeTempCreate = func() error {
			tempCreates.Add(1)
			return nil
		}
		defer func() { store.hooks.BeforeTempCreate = prevHook }()

		database.mu.RLock()
		packBase := database.leafGenerationManifest.clone()
		nextMeta := database.meta
		database.mu.RUnlock()

		finalizeOpts := finalizeCommitOptions{
			packedRootPublication: &packedRootPublication{
				afterManifestPreparation: func() error { return nil },
			},
		}
		idx := database.idx.Load()
		nextMeta.CommitSeq++

		_, _, bindErr := database.bindPublicationLeafManifest(failingSet, idx, nextMeta, packBase, []uint32{packRawID}, &finalizeOpts)
		if !errors.Is(bindErr, syncBoom) {
			t.Fatalf("bindPublicationLeafManifest error=%v, want %v", bindErr, syncBoom)
		}
		if got := tempCreates.Load(); got != 0 {
			t.Fatalf("manifest temp create ran %d times after dependency sync error, want 0", got)
		}

		afterFiles, err := filepath.Glob(filepath.Join(leafDir, "manifest.durable.*.json"))
		if err != nil {
			t.Fatalf("glob after: %v", err)
		}
		if len(afterFiles) != len(beforeFiles) {
			t.Fatalf("manifest revision created after dependency sync failure: before=%v after=%v", beforeFiles, afterFiles)
		}
	})
}

func TestCommandWALCheckpointZeroFrontierSkipsStreamScanAndFlushesDirtyDir(t *testing.T) {
	t.Run("stream_segments_skip_scan", func(t *testing.T) {
		dir := t.TempDir()
		writeCommandWALFrame(t, dir, 1, 1)
		writeCommandWALFrame(t, dir, 2, 2)
		database := zeroFrontierCleanupDBForTest(t, dir, 2)

		if err := database.CleanupCommandWALCoveredSegmentsAtCheckpoint(true); err != nil {
			t.Fatalf("CleanupCommandWALCoveredSegmentsAtCheckpoint: %v", err)
		}
		if got := database.commandWALCleanupScans.Load(); got != 0 {
			t.Fatalf("zero-frontier checkpoint cleanup scanned %d times for stream-only WAL, want 0", got)
		}
		if got := database.commandWALCleanupScanBytes.Load(); got != 0 {
			t.Fatalf("zero-frontier checkpoint cleanup scanned %d bytes, want 0", got)
		}
		for _, seq := range []uint64{1, 2} {
			p := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, seq))
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("retained stream segment %d missing: %v", seq, err)
			}
		}

		// Explicit strict CleanupCommandWALCoveredSegments must still run the full scan.
		if err := database.CleanupCommandWALCoveredSegments(false); err != nil {
			t.Fatalf("CleanupCommandWALCoveredSegments: %v", err)
		}
		if got := database.commandWALCleanupScans.Load(); got != 1 {
			t.Fatalf("explicit zero-frontier cleanup scans=%d, want 1", got)
		}
	})

	t.Run("dirty_namespace_still_synced", func(t *testing.T) {
		dir := t.TempDir()
		writeCommandWALFrame(t, dir, 1, 1)
		database := zeroFrontierCleanupDBForTest(t, dir, 1)
		database.commandWALCleanupNamespaceDirty.Store(true)

		cutErr := errors.New("injected dirty WAL dir sync error")
		var syncs atomic.Int64
		restore := durabilitycut.Install(func(e durabilitycut.Event) error {
			if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDeletionDirectorySync {
				syncs.Add(1)
				return cutErr
			}
			return nil
		})
		err := database.CleanupCommandWALCoveredSegmentsAtCheckpoint(true)
		restore()
		if !errors.Is(err, cutErr) || !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("dirty namespace sync error=%v, want %v + ErrRecoveryRequired", err, cutErr)
		}
		if got := syncs.Load(); got != 1 {
			t.Fatalf("dirty namespace sync count=%d, want 1", got)
		}
		if !database.commandWALCleanupNamespaceDirty.Load() {
			t.Fatal("failed dir sync cleared dirty flag")
		}

		// Retry without fault must flush dirty namespace while still skipping stream scan.
		if err := database.CleanupCommandWALCoveredSegmentsAtCheckpoint(true); err != nil {
			t.Fatalf("retry CleanupCommandWALCoveredSegmentsAtCheckpoint: %v", err)
		}
		if database.commandWALCleanupNamespaceDirty.Load() {
			t.Fatal("retry did not clear dirty namespace flag")
		}
		if got := database.commandWALCleanupScans.Load(); got != 0 {
			t.Fatalf("stream-only zero-frontier cleanup scans=%d, want 0", got)
		}
	})

	t.Run("nonempty_fixed_skips_scan", func(t *testing.T) {
		if !commitlog.FixedWALCreationSupported() {
			t.Skip("requires fixed-container creation")
		}
		dir := t.TempDir()
		if err := os.MkdirAll(WALDirPath(dir), 0o755); err != nil {
			t.Fatal(err)
		}
		segPath := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
		w, err := commitlog.NewFixedWriterWithCapacity(segPath, 8192)
		if err != nil {
			t.Fatalf("NewFixedWriterWithCapacity: %v", err)
		}
		payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
			Op: commitlog.RawKVOpSet, Key: []byte("fk"), Value: []byte("fv"),
		}})
		if err != nil {
			_ = w.Close()
			t.Fatalf("EncodeRawKVBatchPayload: %v", err)
		}
		if err := w.AppendCommand(commitlog.CommandEnvelope{
			Version:         commitlog.CommandFrameVersionV2,
			LSN:             1,
			DurabilityClass: commitlog.CommandDurabilityDurable,
			Kind:            commitlog.CommandKindRawKVBatch,
			Scope:           commitlog.CommandScopeRawKV,
			PayloadFormat:   commitlog.PayloadFormatRawKVBatchV1,
			Payload:         payload,
		}); err != nil {
			_ = w.Close()
			t.Fatalf("AppendCommand: %v", err)
		}
		if err := w.Sync(); err != nil {
			_ = w.Close()
			t.Fatalf("Sync: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close fixed writer: %v", err)
		}

		database := zeroFrontierCleanupDBWithActiveForTest(t, dir, 1, true)
		if err := database.CleanupCommandWALCoveredSegmentsAtCheckpoint(true); err != nil {
			t.Fatalf("CleanupCommandWALCoveredSegmentsAtCheckpoint: %v", err)
		}
		if got := database.commandWALCleanupScans.Load(); got != 0 {
			t.Fatalf("zero-frontier checkpoint cleanup scans for non-empty fixed container = %d, want 0", got)
		}
		if _, err := os.Stat(segPath); err != nil {
			t.Fatalf("non-empty fixed container missing: %v", err)
		}
	})

	t.Run("empty_fixed_still_reclaimed", func(t *testing.T) {
		dir := t.TempDir()
		writeCommandWALFrame(t, dir, 1, 1)
		empty := writeEmptyFixedCleanupSegment(t, dir, 2, 8192)
		database := zeroFrontierCleanupDBForTest(t, dir, 1)

		if err := database.CleanupCommandWALCoveredSegmentsAtCheckpoint(true); err != nil {
			t.Fatalf("CleanupCommandWALCoveredSegmentsAtCheckpoint: %v", err)
		}
		if _, err := os.Stat(empty); !os.IsNotExist(err) {
			t.Fatalf("empty fixed container was not reclaimed at zero frontier: %v", err)
		}
		if got := database.commandWALCleanupScans.Load(); got != 1 {
			t.Fatalf("zero-frontier cleanup with empty fixed container scans=%d, want 1", got)
		}
		if got := database.commandWALCleanupRemoved.Load(); got != 1 {
			t.Fatalf("removed segments=%d, want 1", got)
		}
	})
}
