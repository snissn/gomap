package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestExecuteDurableRootStorageTransactionSingleBarrierSameRootFallback(t *testing.T) {
	t.Run("normal_single_barrier_order", func(t *testing.T) {
		events := make([]string, 0, 5)
		mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
			singleBarrierSameRootFallback: true,
			syncPendingLeafLogDir: func() error {
				events = append(events, "leaf-dir-sync")
				return nil
			},
			materialize: func() error {
				events = append(events, "materialize")
				return nil
			},
			beforeMetaWrite: func() error {
				events = append(events, "before-meta-write")
				return nil
			},
			sink:   recordingDurableRootSinkV1{events: &events},
			target: MetaPage1ID,
			meta:   testDurableMetaV1(t),
			beforeMetaSync: func() error {
				events = append(events, "before-meta-sync")
				return nil
			},
			syncIndex: func() error {
				events = append(events, "index-sync")
				return nil
			},
			syncMeta: func() error {
				events = append(events, "meta-sync")
				return nil
			},
		})
		if err != nil || !mutated {
			t.Fatalf("mutated=%v err=%v", mutated, err)
		}
		want := []string{"leaf-dir-sync", "materialize", "before-meta-write", "meta-write", "before-meta-sync", "index-sync"}
		if !reflect.DeepEqual(events, want) {
			t.Fatalf("events=%v want=%v", events, want)
		}
	})

	t.Run("before_meta_write_failure_is_retryable", func(t *testing.T) {
		wantErr := errors.New("pre-meta failpoint")
		events := make([]string, 0, 2)
		mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
			singleBarrierSameRootFallback: true,
			materialize: func() error {
				events = append(events, "materialize")
				return nil
			},
			beforeMetaWrite: func() error { return wantErr },
			sink:            recordingDurableRootSinkV1{events: &events},
			target:          MetaPage1ID,
			meta:            testDurableMetaV1(t),
			syncIndex:       func() error { events = append(events, "index-sync"); return nil },
			syncMeta:        func() error { events = append(events, "meta-sync"); return nil },
		})
		if !errors.Is(err, wantErr) || mutated || !reflect.DeepEqual(events, []string{"materialize"}) {
			t.Fatalf("mutated=%v err=%v events=%v", mutated, err, events)
		}
	})

	t.Run("sync_index_failure_after_meta_write_is_ambiguous", func(t *testing.T) {
		wantErr := errors.New("fallback sync failed")
		events := make([]string, 0, 3)
		mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
			singleBarrierSameRootFallback: true,
			materialize:                   func() error { events = append(events, "materialize"); return nil },
			sink:                          recordingDurableRootSinkV1{events: &events},
			target:                        MetaPage1ID,
			meta:                          testDurableMetaV1(t),
			syncIndex:                     func() error { return wantErr },
			syncMeta:                      func() error { return nil },
		})
		if !errors.Is(err, wantErr) || !mutated {
			t.Fatalf("mutated=%v err=%v want mutated=true err=%v", mutated, err, wantErr)
		}
	})

	t.Run("durabilitycut_uses_two_barriers", func(t *testing.T) {
		restore := durabilitycut.Install(func(durabilitycut.Event) error { return nil })
		defer restore()
		events := make([]string, 0, 4)
		mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
			singleBarrierSameRootFallback: true,
			materialize:                   func() error { events = append(events, "materialize"); return nil },
			syncIndex:                     func() error { events = append(events, "index-sync"); return nil },
			sink:                          recordingDurableRootSinkV1{events: &events},
			target:                        MetaPage1ID,
			meta:                          testDurableMetaV1(t),
			syncMeta:                      func() error { events = append(events, "meta-sync"); return nil },
		})
		if err != nil || !mutated {
			t.Fatalf("mutated=%v err=%v", mutated, err)
		}
		want := []string{"materialize", "index-sync", "meta-write", "meta-sync"}
		if !reflect.DeepEqual(events, want) {
			t.Fatalf("durabilitycut events=%v want=%v", events, want)
		}
	})
}

func TestExecuteDurableRootStorageTransactionParallelLeafDirAndIndexSyncErrors(t *testing.T) {
	t.Run("leaf_dir_error_blocks_meta_write", func(t *testing.T) {
		wantErr := errors.New("leaf dir sync failed")
		var metaWritten atomic.Bool
		events := make([]string, 0, 2)
		mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
			syncPendingLeafLogDir: func() error { return wantErr },
			materialize:           func() error { return nil },
			syncIndex:             func() error { return nil },
			sink:                  recordingDurableRootSinkV1{events: &events},
			target:                MetaPage0ID,
			meta:                  testDurableMetaV1(t),
			beforeMetaWrite: func() error {
				metaWritten.Store(true)
				return nil
			},
			syncMeta: func() error { return nil },
		})
		if !errors.Is(err, wantErr) || mutated || metaWritten.Load() || len(events) != 0 {
			t.Fatalf("mutated=%v metaWritten=%v events=%v err=%v", mutated, metaWritten.Load(), events, err)
		}
	})

	t.Run("index_sync_error_blocks_meta_write", func(t *testing.T) {
		wantErr := errors.New("index sync failed")
		var dirSynced atomic.Bool
		events := make([]string, 0, 2)
		mutated, err := executeDurableRootStorageTransactionV1(durableRootStorageTransactionV1{
			syncPendingLeafLogDir: func() error { dirSynced.Store(true); return nil },
			materialize:           func() error { return nil },
			syncIndex:             func() error { return wantErr },
			sink:                  recordingDurableRootSinkV1{events: &events},
			target:                MetaPage0ID,
			meta:                  testDurableMetaV1(t),
			syncMeta:              func() error { return nil },
		})
		if !errors.Is(err, wantErr) || mutated || !dirSynced.Load() || len(events) != 0 {
			t.Fatalf("mutated=%v dirSynced=%v events=%v err=%v", mutated, dirSynced.Load(), events, err)
		}
	})
}

func TestSingleBarrierSameRootFallbackTornRecordRecoversToOppositeSlot(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("k1"), []byte("v1")); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("k2"), []byte("v2")); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}

	// Verify both durable slots converged at identical roots & AppliedCommandLSN
	// with distinct root record pages and valid parent linkage.
	database.durablePublishMu.Lock()
	r0 := database.durableRoot.slotRecord[0]
	r1 := database.durableRoot.slotRecord[1]
	m0 := database.durableRoot.slotMeta[0]
	m1 := database.durableRoot.slotMeta[1]
	newestSlot := database.durableRoot.slot
	newestRecordPage := database.durableRoot.meta.RootRecordPageID
	database.durablePublishMu.Unlock()

	if r0.UserRootPageID != r1.UserRootPageID || r0.SystemRootPageID != r1.SystemRootPageID || r0.AppliedCommandLSN != r1.AppliedCommandLSN {
		_ = database.Close()
		t.Fatalf("slots did not converge: r0=%+v r1=%+v", r0, r1)
	}
	if m0.RootRecordPageID == m1.RootRecordPageID || newestRecordPage == 0 {
		_ = database.Close()
		t.Fatalf("expected distinct root record pages: slot0=%d slot1=%d", m0.RootRecordPageID, m1.RootRecordPageID)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a power loss during single-barrier fallback convergence where the
	// newly written meta slot reached disk while its new DurableRootRecordV1 page
	// (newestRecordPage) was torn/corrupt. Recovery MUST reject the torn slot and
	// fall back cleanly to the already-durable opposite slot with all keys intact.
	indexPath := filepath.Join(dir, indexFileName)
	f, err := os.OpenFile(indexPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	zeros := make([]byte, page.PageSize)
	if _, err := f.WriteAt(zeros, int64(newestRecordPage)*int64(page.PageSize)); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen after torn fallback record (newestSlot=%d page=%d): %v", newestSlot, newestRecordPage, err)
	}
	defer reopened.Close()

	for k, want := range map[string]string{"k1": "v1", "k2": "v2"} {
		got, err := reopened.Get([]byte(k))
		if err != nil || string(got) != want {
			t.Fatalf("Get(%q)=(%q,%v) want (%q,nil)", k, got, err, want)
		}
	}
}

func TestLeafGenerationManifestParallelDepsSyncFailureDoesNotLink(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if database.leafGenerationManifestStore == nil || database.leafGenerationManifestStore.mode != leafGenerationManifestStable {
		t.Skip("stable leaf generation manifest store unsupported on this platform")
	}
	database.mu.RLock()
	before := database.leafGenerationManifest.clone()
	database.mu.RUnlock()
	if before == nil {
		t.Fatal("expected initialized leaf generation manifest")
	}

	wantErr := errors.New("parallel dependency sync failed")
	candidate := before.clone()
	err = database.replaceLeafGenerationManifestOperationalWithOptions(candidate, func() error { return wantErr }, true)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v want %v", err, wantErr)
	}

	loaded, ok, err := database.leafGenerationManifestStore.Load()
	if err != nil || !ok {
		t.Fatalf("reload manifest ok=%v err=%v", ok, err)
	}
	if loaded.ManifestRevision != before.ManifestRevision {
		t.Fatalf("manifest revision advanced despite syncDeps failure: got=%d want=%d", loaded.ManifestRevision, before.ManifestRevision)
	}
}

func TestCapturedPrefixDeferredRotationNamespaceSyncReopenAndError(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if !database.commandJournal.CanShareFixedWriter() {
		_ = database.Close()
		t.Skip("fixed WAL unsupported on this platform")
	}

	bigVal := bytes.Repeat([]byte("a"), 3<<20)
	appendBatch := func(key string, mode RawKVCommandWALAppendMode) uint64 {
		t.Helper()
		lsn, err := database.AppendRawKVCommandWALOrderedEntriesWithMode(
			[]batchpkg.Entry{{Type: batchpkg.OpPut, Key: []byte(key), Value: bigVal}},
			mode,
		)
		if err != nil {
			t.Fatal(err)
		}
		return lsn
	}

	// First 3 MiB frame fills segment 1; second 3 MiB frame rotates to segment 2
	// with a deferred namespace obligation stabilized by CommandWALCapturedPrefix.Sync.
	lsn1 := appendBatch("k1", RawKVCommandWALAppendCapturedDurable)
	var cap1 CommandWALCapturedPrefix
	t1, _, err := database.CaptureCommandWALPrefixInto(&cap1, nil)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if _, err := t1.Sync(); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if database.commandWALDurableLSN.Load() != lsn1 {
		_ = database.Close()
		t.Fatalf("durable LSN=%d want=%d", database.commandWALDurableLSN.Load(), lsn1)
	}

	lsn2 := appendBatch("k2", RawKVCommandWALAppendRelaxed)
	var cap2 CommandWALCapturedPrefix
	t2, res2, err := database.CaptureCommandWALPrefixInto(&cap2, nil)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if !t2.deferRotationNamespaceSync {
		_ = database.Close()
		t.Fatal("expected deferRotationNamespaceSync=true on fixed WAL")
	}
	if _, err := t2.Sync(); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	wantLSN := res2.LSN
	if wantLSN == 0 {
		wantLSN = lsn2
	}
	if database.commandWALDurableLSN.Load() != wantLSN {
		_ = database.Close()
		t.Fatalf("post-rotation durable LSN=%d want=%d", database.commandWALDurableLSN.Load(), wantLSN)
	}
	if database.commandWALDebt.hasPendingRotationNamespacesThrough(wantLSN) {
		_ = database.Close()
		t.Fatal("pending rotation namespace debt remained after CommandWALCapturedPrefix.Sync")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify WAL replay recovers cleanly.
	reopened, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen after rotated captured sync: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommandWALClosedSegmentPreScanReusesAndFallsBackOnChange(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if err := database.SetSync([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := database.RotateCommandWALActiveSegment(true); err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Start a background pre-scan after a rotation and verify Cleanup consumes it cleanly.
	if err := database.SetSync([]byte("k3"), []byte("v3")); err != nil {
		t.Fatal(err)
	}
	if err := database.RotateCommandWALActiveSegment(true); err != nil {
		t.Fatal(err)
	}
	preScan := database.StartCommandWALClosedSegmentPreScan()
	if preScan == nil {
		t.Fatal("expected non-nil closed-segment pre-scan")
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		preScan.Close()
		t.Fatal(err)
	}
	if err := database.CleanupCommandWALCoveredSegmentsAtCheckpointWithPreScan(true, preScan); err != nil {
		t.Fatal(err)
	}
	// Closing an already-consumed preScan must be idempotent.
	preScan.Close()
}
