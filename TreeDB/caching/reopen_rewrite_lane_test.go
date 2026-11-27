package caching

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type reopenRewriteLeafLogMockBackend struct {
	*MockBackend
}

func (*reopenRewriteLeafLogMockBackend) SetLeafPageLog(backenddb.LeafPageLog) {}

func newReopenRewriteLeafLogMockBackend() *reopenRewriteLeafLogMockBackend {
	return &reopenRewriteLeafLogMockBackend{MockBackend: NewMockBackend()}
}

func TestReopenRewriteHighLaneDoesNotExpandLanes(t *testing.T) {
	writeSegment := func(t *testing.T, dir string, lane uint32, seq uint32, rid uint64, val []byte) (uint32, string) {
		t.Helper()
		fileID, err := valuelog.EncodeFileID(lane, seq)
		if err != nil {
			t.Fatalf("EncodeFileID(%d, %d): %v", lane, seq, err)
		}
		path := valuelog.SegmentPath(dir, fileID)
		w, err := valuelog.NewWriter(path, fileID)
		if err != nil {
			t.Fatalf("NewWriter(%s): %v", path, err)
		}
		if _, err := w.Append(0, nil, rid, val); err != nil {
			_ = w.Close()
			t.Fatalf("Append(%s): %v", path, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close(%s): %v", path, err)
		}
		return fileID, path
	}

	t.Run("default_journal_lanes_with_lane254_rewrite_segment", func(t *testing.T) {
		dir := t.TempDir()
		valueDir := filepath.Join(dir, "value_vlog")
		if err := os.MkdirAll(valueDir, 0o755); err != nil {
			t.Fatalf("MkdirAll(value_vlog): %v", err)
		}
		writeSegment(t, valueDir, 0, 1, 1, bytes.Repeat([]byte("a"), 128))
		rewriteFileID, _ := writeSegment(t, valueDir, 254, 1, 2, bytes.Repeat([]byte("r"), 128))

		policy := backenddb.ValueLogGenerationHotWarmCold
		defaults := backenddb.ResolveJournalLaneDefaults(0, runtime.GOMAXPROCS(0), backenddb.DetectPhysicalCores(), policy)
		wantLanes := defaults.Effective
		if defaults.Defaulted && wantLanes < 3 {
			wantLanes = 3
		}

		db, err := Open(dir, newReopenRewriteLeafLogMockBackend(), Options{
			DisableWAL:                 true,
			AllowUnsafe:                true,
			IndexOuterLeavesInValueLog: true,
			ValueLogGenerationPolicy:   uint8(policy),
			ValueLogPointerThreshold:   1,
			ForceValueLogPointers:      true,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()

		if got := len(db.lanes); got != wantLanes {
			t.Errorf("reopen with lane-254 rewrite segment expanded len(db.lanes)=%d, want %d (hotLanes=%d)", got, wantLanes, len(db.valueLogHotLanes))
		}
		if !db.valueLogReader.HasSegment(rewriteFileID) {
			t.Fatalf("valueLogReader missing pre-existing lane-254 rewrite segment %d", rewriteFileID)
		}

		// Appending through cachingValueLogAppender must stay within active lanes
		// and never spray writers/files across ~253 high lanes.
		appender := newCachingValueLogAppender(db, &db.lanes[0])
		values := make([][]byte, 300)
		for i := range values {
			values[i] = bytes.Repeat([]byte{byte(i)}, 64)
		}
		ptrs, err := appender.AppendValues(values)
		if err != nil {
			t.Fatalf("AppendValues: %v", err)
		}
		if err := appender.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		writtenLanes := make(map[uint32]struct{})
		outOfRangeLanes := 0
		for _, ptr := range ptrs {
			laneID, _ := valuelog.DecodeFileID(ptr.FileID)
			if _, seen := writtenLanes[laneID]; !seen && int(laneID) >= wantLanes {
				outOfRangeLanes++
			}
			writtenLanes[laneID] = struct{}{}
		}
		entries, err := os.ReadDir(valueDir)
		if err != nil {
			t.Fatalf("ReadDir(value_vlog): %v", err)
		}
		t.Logf("baseline metrics: len(db.lanes)=%d wantLanes=%d hotLanes=%d writtenLanes=%d outOfRangeLanes=%d valueLogFiles=%d",
			len(db.lanes), wantLanes, len(db.valueLogHotLanes), len(writtenLanes), outOfRangeLanes, len(entries))
		if outOfRangeLanes > 0 {
			t.Errorf("AppendValues routed writes to %d out-of-range lanes (total writtenLanes=%d, want all < %d)", outOfRangeLanes, len(writtenLanes), wantLanes)
		}
		// Existing lane-0 + lane-254 + newly opened active lane files must remain bounded.
		maxExpectedFiles := 2 + wantLanes*8
		if len(entries) > maxExpectedFiles {
			t.Errorf("value_vlog segment file explosion: got %d files (writtenLanes=%d), want <= %d", len(entries), len(writtenLanes), maxExpectedFiles)
		}
	})

	t.Run("explicit_journal_lanes_with_lane254_rewrite_segment", func(t *testing.T) {
		dir := t.TempDir()
		valueDir := filepath.Join(dir, "value_vlog")
		if err := os.MkdirAll(valueDir, 0o755); err != nil {
			t.Fatalf("MkdirAll(value_vlog): %v", err)
		}
		writeSegment(t, valueDir, 254, 3, 10, bytes.Repeat([]byte("z"), 128))

		db, err := Open(dir, newReopenRewriteLeafLogMockBackend(), Options{
			JournalLanes:               4,
			IndexOuterLeavesInValueLog: true,
			ValueLogGenerationPolicy:   uint8(backenddb.ValueLogGenerationHotWarmCold),
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()

		if got := len(db.lanes); got != 4 {
			t.Fatalf("len(db.lanes)=%d, want 4 (hotLanes=%d)", got, len(db.valueLogHotLanes))
		}
		if got := db.nextRID.Load(); got < 10 {
			t.Fatalf("nextRID=%d, want >= 10 from lane-254 segment", got)
		}
	})

	t.Run("redo_wal_high_lane_still_expands_lanes_for_recovery", func(t *testing.T) {
		dir := t.TempDir()
		walDir := filepath.Join(dir, "wal")
		if err := os.MkdirAll(walDir, 0o755); err != nil {
			t.Fatalf("MkdirAll(wal): %v", err)
		}
		walPath := filepath.Join(walDir, "commit-l5-000002.log")
		if err := os.WriteFile(walPath, bytes.Repeat([]byte("w"), 64), 0o600); err != nil {
			t.Fatalf("WriteFile(wal): %v", err)
		}

		db, err := Open(dir, newReopenRewriteLeafLogMockBackend(), Options{
			JournalLanes:               2,
			IndexOuterLeavesInValueLog: true,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()

		if got := len(db.lanes); got != 6 {
			t.Fatalf("redo WAL segment commit-l5 did not preserve lane expansion: len(db.lanes)=%d, want 6", got)
		}
		db.lanes[5].walMu.Lock()
		_, tracked := db.lanes[5].walClosedSizes[walPath]
		db.lanes[5].walMu.Unlock()
		if !tracked {
			t.Fatalf("recovered redo WAL segment %s missing from lanes[5].walClosedSizes", walPath)
		}
	})
}

func TestReopenLane254RetainedBytesAccountingAndPrune(t *testing.T) {
	dir := t.TempDir()
	valueDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(valueDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(value_vlog): %v", err)
	}
	fileID, err := valuelog.EncodeFileID(254, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	lane254Path := valuelog.SegmentPath(valueDir, fileID)
	w, err := valuelog.NewWriter(lane254Path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("dead-lane254"), 64)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	info, err := os.Stat(lane254Path)
	if err != nil {
		t.Fatalf("Stat(%s): %v", lane254Path, err)
	}
	segSize := info.Size()
	if segSize <= 0 {
		t.Fatalf("expected positive segment size, got %d", segSize)
	}

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		DisableBackgroundPrune: true,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	})
	if err != nil {
		t.Fatalf("backenddb.Open: %v", err)
	}
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("backend.Close: %v", err)
		}
	}()

	var hardCapErrMu sync.Mutex
	var hardCapErrs []error
	db, err := Open(dir, backend, Options{
		DisableWAL:                   true,
		AllowUnsafe:                  true,
		JournalLanes:                 4,
		ValueLogPointerThreshold:     1,
		ForceValueLogPointers:        true,
		ValueLogGenerationPolicy:     uint8(backenddb.ValueLogGenerationHotWarmCold),
		MaxValueLogRetainedBytesHard: 1,
		NotifyError: func(err error) {
			hardCapErrMu.Lock()
			hardCapErrs = append(hardCapErrs, err)
			hardCapErrMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("caching.Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil && !strings.Contains(err.Error(), "exceed hard cap 1") {
			t.Errorf("caching.Close unexpected error: %v", err)
		}
	}()

	if got := len(db.lanes); got != 4 {
		t.Fatalf("len(db.lanes)=%d, want 4", got)
	}
	if got := db.valueLogRetainedClosedBytes.Load(); got != segSize {
		t.Fatalf("valueLogRetainedClosedBytes=%d, want %d for reopened lane-254 segment", got, segSize)
	}
	if got := db.valueLogClosedSegmentSize(lane254Path); got != segSize {
		t.Fatalf("valueLogClosedSegmentSize(lane254)=%d, want %d", got, segSize)
	}
	detailed := db.valueLogRetainedStatsDetailed()
	if detailed.SegmentsTotal != 1 || detailed.BytesTotal != segSize {
		t.Fatalf("valueLogRetainedStatsDetailed=%+v, want SegmentsTotal=1 BytesTotal=%d", detailed, segSize)
	}
	if !db.shouldScheduleRetainedValueLogPruneWithForce(false) {
		t.Fatalf("shouldScheduleRetainedValueLogPruneWithForce(false)=false under MaxValueLogRetainedBytesHard=1 with reopened lane-254 segment")
	}
	if !db.shouldScheduleRetainedValueLogPruneWithForce(true) {
		t.Fatalf("shouldScheduleRetainedValueLogPruneWithForce(true)=false with reopened lane-254 segment retained")
	}
	st := db.Stats()
	if got, err := strconv.ParseInt(st["treedb.cache.vlog_shape.bytes_total"], 10, 64); err != nil || got < segSize {
		t.Fatalf("treedb.cache.vlog_shape.bytes_total=%q (err=%v), want >= %d", st["treedb.cache.vlog_shape.bytes_total"], err, segSize)
	}
	if got, err := strconv.Atoi(st["treedb.cache.vlog_shape.segments_total"]); err != nil || got < 1 {
		t.Fatalf("treedb.cache.vlog_shape.segments_total=%q (err=%v), want >= 1", st["treedb.cache.vlog_shape.segments_total"], err)
	}
	if db.allowValueLogPointers() {
		t.Fatalf("allowValueLogPointers()=true despite reopened lane-254 retained bytes (%d) >= MaxValueLogRetainedBytesHard (1)", segSize)
	}
	hardCapErrMu.Lock()
	sawHardCapNotice := false
	for _, notice := range hardCapErrs {
		if strings.Contains(notice.Error(), "exceed hard cap 1") {
			sawHardCapNotice = true
		} else {
			t.Errorf("unexpected NotifyError callback: %v", notice)
		}
	}
	hardCapErrMu.Unlock()
	if !sawHardCapNotice {
		t.Fatalf("expected hard-cap NotifyError callback for reopened lane-254 segment, got %v", hardCapErrs)
	}

	// Wait for or trigger retained prune and verify the dead lane-254 segment is
	// unretained and deleted rather than stranded behind ProtectedRetainedPaths.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(lane254Path); os.IsNotExist(err) && db.valueLogRetainedClosedBytes.Load() == 0 {
			break
		}
		if time.Now().After(deadline) {
			pruneStats := db.pruneRetainedValueLogs(true)
			t.Fatalf("reopened lane-254 segment was not pruned automatically under MaxValueLogRetainedBytesHard=1 (fallback prune=%+v retainedBytes=%d)",
				pruneStats, db.valueLogRetainedClosedBytes.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, p := range db.ValueLogRetainedPaths() {
		if p == lane254Path {
			t.Fatalf("dead lane-254 segment %s remained in ValueLogRetainedPaths after prune", lane254Path)
		}
	}
	if got := db.valueLogRetainedClosedBytes.Load(); got != 0 {
		t.Fatalf("valueLogRetainedClosedBytes=%d after prune, want 0", got)
	}
	if got := db.externalVlogClosedBytes.Load(); got != 0 {
		t.Fatalf("externalVlogClosedBytes=%d after prune, want 0", got)
	}
	if !db.allowValueLogPointers() {
		t.Fatalf("allowValueLogPointers()=false after dead lane-254 segment was pruned")
	}
}

func TestSecondaryReconcileFailureIsolatedFromPrimarySnapshotAndRefresh(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		DisableBackgroundPrune: true,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		KeepRecent:             1,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
			Compression:      backenddb.ValueLogCompressionOff,
		},
	})
	if err != nil {
		t.Fatalf("backenddb.Open: %v", err)
	}
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("backend.Close: %v", err)
		}
	}()

	var notifyMu sync.Mutex
	var notifyErrs []error
	notifyCh := make(chan error, 8)
	cached, err := Open(dir, backend, Options{
		DisableWAL:                              true,
		AllowUnsafe:                             true,
		JournalLanes:                            2,
		ValueLogPointerThreshold:                1,
		ForceValueLogPointers:                   true,
		ValueLogCompression:                     uint8(backenddb.ValueLogCompressionOff),
		ValueLogGenerationPolicy:                uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogGenerationHotSegmentTargetBytes: 8 << 10,
		NotifyError: func(err error) {
			notifyMu.Lock()
			notifyErrs = append(notifyErrs, err)
			notifyMu.Unlock()
			select {
			case notifyCh <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("caching.Open: %v", err)
	}
	expectClosedReceipt := false
	defer func() {
		backend.SetPostValueLogGCHook(nil)
		backend.SetValueLogSegmentRetiredHook(nil)
		err := cached.Close()
		if expectClosedReceipt {
			if !errors.Is(err, os.ErrClosed) {
				t.Errorf("cached.Close() = %v, want os.ErrClosed", err)
			}
		} else if err != nil && !errors.Is(err, os.ErrClosed) {
			t.Errorf("cached.Close() unexpected error: %v", err)
		}
	}()
	backend.SetPostValueLogGCHook(func() error {
		return cached.ReconcileRetiredValueLogReader()
	})
	backend.SetValueLogSegmentRetiredHook(func(fileID uint32) {
		cached.NotifyRetiredValueLogSegment(fileID)
	})

	// Prove NotifyRetiredValueLogSegment never blocks even while the reconciler
	// execution mutex is held by another goroutine (zero cross-layer lock edges).
	cached.retiredVlogReconcileRunMu.Lock()
	doneNotify := make(chan struct{})
	go func() {
		for i := uint32(1); i <= 64; i++ {
			cached.NotifyRetiredValueLogSegment(i)
		}
		close(doneNotify)
	}()
	select {
	case <-doneNotify:
	case <-time.After(2 * time.Second):
		cached.retiredVlogReconcileRunMu.Unlock()
		t.Fatal("NotifyRetiredValueLogSegment blocked while retiredVlogReconcileRunMu was held")
	}
	cached.retiredVlogReconcileRunMu.Unlock()

	// Write enough rounds through backend appender to create sealed segments.
	writeRound := func(round int) {
		vals := make([][]byte, 64)
		for i := range vals {
			buf := make([]byte, 512)
			for j := range buf {
				buf[j] = byte((round*29 + i*13 + j*7) & 0xff)
			}
			vals[i] = buf
		}
		ptrs, err := backend.AppendValueLogValues(vals)
		if err != nil {
			t.Fatalf("round %d AppendValueLogValues: %v", round, err)
		}
		b := backend.NewBatch()
		pb := b.(interface {
			SetPointer([]byte, page.ValuePtr) error
		})
		for i := range vals {
			if err := pb.SetPointer([]byte("k-"+strconv.Itoa(i)), ptrs[i]); err != nil {
				_ = b.Close()
				t.Fatalf("SetPointer: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("WriteSync: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Batch.Close: %v", err)
		}
	}
	for round := 0; round < 6; round++ {
		writeRound(round)
	}
	for i := 0; i < 2; i++ {
		b := backend.NewBatch()
		if err := b.Set([]byte("bump-"+strconv.Itoa(i)), nil); err != nil {
			_ = b.Close()
			t.Fatalf("Set bump: %v", err)
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("WriteSync bump: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Batch.Close bump: %v", err)
		}
	}

	// Pin all current backend segments with a primary snapshot across ValueLogGC.
	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}

	ctx := context.Background()
	gcStats, err := backend.ValueLogGC(ctx, backenddb.ValueLogGCOptions{})
	if err != nil {
		_ = snap.Close()
		t.Fatalf("ValueLogGC while snapshot pinned: %v", err)
	}
	if gcStats.SegmentsPending == 0 {
		_ = snap.Close()
		t.Fatalf("expected pending zombie segments while snapshot is pinned, got %+v", gcStats)
	}

	// Pre-close one tracked non-zombie handle inside cached.valueLogReader that
	// is currently zombie on backend (absent from backend's current ValueLogSet)
	// so secondary reconciliation fails its close.
	backendLive := backend.State()
	readerSet := cached.valueLogReader.CurrentSetNoRefresh()
	var poisonedID uint32
	for id, f := range readerSet.Files {
		laneID, _ := valuelog.DecodeFileID(id)
		if laneID == leafLogLaneID {
			continue
		}
		if backendLive == nil || backendLive.ValueLogSet == nil || backendLive.ValueLogSet.Files[id] == nil {
			if err := f.File.Close(); err != nil {
				_ = cached.valueLogReader.Release(readerSet)
				_ = snap.Close()
				t.Fatalf("pre-close secondary file %d: %v", id, err)
			}
			poisonedID = id
			break
		}
	}
	if err := cached.valueLogReader.Release(readerSet); err != nil {
		_ = snap.Close()
		t.Fatalf("release readerSet: %v", err)
	}
	if poisonedID == 0 {
		_ = snap.Close()
		t.Fatal("failed to locate zombie segment in cached.valueLogReader")
	}

	// Closing primary snapshot triggers physical zombie deletion + async notify.
	// Primary Snapshot.Close() and RefreshValueLogSet() MUST NOT inherit secondary
	// reader errors or escalate to ErrRecoveryRequired.
	if err := snap.Close(); err != nil {
		t.Fatalf("primary Snapshot.Close() inherited secondary error: %v", err)
	}
	if err := backend.RefreshValueLogSet(); err != nil {
		t.Fatalf("primary RefreshValueLogSet() inherited secondary error (must not trigger ErrRecoveryRequired): %v", err)
	}

	// Wait for the async reconciler to observe and report the secondary close failure.
	select {
	case err := <-notifyCh:
		var receipt *valuelog.ManagerCleanupFailure
		if !errors.Is(err, os.ErrClosed) || !errors.As(err, &receipt) {
			t.Fatalf("NotifyError received %v, want ManagerCleanupFailure(os.ErrClosed)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for async retired-segment reconciler to report secondary close failure")
	}

	// Subsequent ValueLogGC (even with 0 new deletions) MUST remain fail-closed
	// and surface the secondary cleanup error rather than silently dropping it.
	_, err = backend.ValueLogGC(ctx, backenddb.ValueLogGCOptions{})
	var gcReceipt *valuelog.ManagerCleanupFailure
	if !errors.Is(err, os.ErrClosed) || !errors.As(err, &gcReceipt) {
		t.Fatalf("post-failure ValueLogGC = %v, want ManagerCleanupFailure(os.ErrClosed)", err)
	}

	// Deferred teardown unhooks and asserts cached.Close() preserves the sticky
	// os.ErrClosed receipt while primary backend.Close() completes cleanly.
	expectClosedReceipt = true
}

func TestRetiredVlogReconcileNotifyErrorCallingCloseDoesNotDeadlock(t *testing.T) {
	t.Run("reentrant_close_from_notify_error_rejected_without_deadlock", func(t *testing.T) {
		dir := t.TempDir()
		backend, err := backenddb.Open(backenddb.Options{
			Dir:                    dir,
			DisableBackgroundPrune: true,
			Durability:             backenddb.DurabilityWALOffRelaxed,
			KeepRecent:             1,
			ValueLog: backenddb.ValueLogOptions{
				PointerThreshold: 1,
				ForcePointers:    true,
				Compression:      backenddb.ValueLogCompressionOff,
			},
		})
		if err != nil {
			t.Fatalf("backenddb.Open: %v", err)
		}
		defer func() {
			if err := backend.Close(); err != nil {
				t.Errorf("backend.Close: %v", err)
			}
		}()

		var cached *DB
		var reentrantCloseOnce sync.Once
		reentrantCloseErrCh := make(chan error, 1)
		cached, err = Open(dir, backend, Options{
			DisableWAL:                              true,
			AllowUnsafe:                             true,
			JournalLanes:                            2,
			ValueLogPointerThreshold:                1,
			ForceValueLogPointers:                   true,
			ValueLogCompression:                     uint8(backenddb.ValueLogCompressionOff),
			ValueLogGenerationPolicy:                uint8(backenddb.ValueLogGenerationHotWarmCold),
			ValueLogGenerationHotSegmentTargetBytes: 8 << 10,
			NotifyError: func(err error) {
				if !errors.Is(err, os.ErrClosed) {
					return
				}
				reentrantCloseOnce.Do(func() {
					// Must be rejected by ownedCloseGuard.CheckClose() without
					// waiting on db.wg (which would deadlock this worker).
					reentrantCloseErrCh <- cached.Close()
				})
			},
		})
		if err != nil {
			t.Fatalf("caching.Open: %v", err)
		}
		expectClosedReceipt := false
		defer func() {
			backend.SetPostValueLogGCHook(nil)
			backend.SetValueLogSegmentRetiredHook(nil)
			err := cached.Close()
			if expectClosedReceipt {
				if !errors.Is(err, os.ErrClosed) {
					t.Errorf("cached.Close() = %v, want os.ErrClosed", err)
				}
			} else if err != nil && !errors.Is(err, os.ErrClosed) {
				t.Errorf("cached.Close() unexpected error: %v", err)
			}
		}()
		backend.SetValueLogSegmentRetiredHook(func(fileID uint32) {
			cached.NotifyRetiredValueLogSegment(fileID)
		})

		for round := 0; round < 6; round++ {
			vals := make([][]byte, 64)
			for i := range vals {
				buf := make([]byte, 512)
				for j := range buf {
					buf[j] = byte((round*31 + i*17 + j*5) & 0xff)
				}
				vals[i] = buf
			}
			ptrs, err := backend.AppendValueLogValues(vals)
			if err != nil {
				t.Fatalf("round %d AppendValueLogValues: %v", round, err)
			}
			b := backend.NewBatch()
			pb := b.(interface {
				SetPointer([]byte, page.ValuePtr) error
			})
			for i := range vals {
				if err := pb.SetPointer([]byte("k-"+strconv.Itoa(i)), ptrs[i]); err != nil {
					_ = b.Close()
					t.Fatalf("SetPointer: %v", err)
				}
			}
			if err := b.WriteSync(); err != nil {
				_ = b.Close()
				t.Fatalf("WriteSync: %v", err)
			}
			if err := b.Close(); err != nil {
				t.Fatalf("Batch.Close: %v", err)
			}
		}
		for i := 0; i < 2; i++ {
			b := backend.NewBatch()
			if err := b.Set([]byte("bump-"+strconv.Itoa(i)), nil); err != nil {
				_ = b.Close()
				t.Fatalf("Set bump: %v", err)
			}
			if err := b.WriteSync(); err != nil {
				_ = b.Close()
				t.Fatalf("WriteSync bump: %v", err)
			}
			if err := b.Close(); err != nil {
				t.Fatalf("Batch.Close bump: %v", err)
			}
		}

		snap := backend.AcquireSnapshot()
		if snap == nil {
			t.Fatal("AcquireSnapshot returned nil")
		}
		gcStats, err := backend.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{})
		if err != nil {
			_ = snap.Close()
			t.Fatalf("ValueLogGC: %v", err)
		}
		if gcStats.SegmentsPending == 0 {
			_ = snap.Close()
			t.Fatalf("expected SegmentsPending > 0, got %+v", gcStats)
		}

		backendLive := backend.State()
		readerSet := cached.valueLogReader.CurrentSetNoRefresh()
		var poisonedID uint32
		for id, f := range readerSet.Files {
			laneID, _ := valuelog.DecodeFileID(id)
			if laneID == leafLogLaneID {
				continue
			}
			if backendLive == nil || backendLive.ValueLogSet == nil || backendLive.ValueLogSet.Files[id] == nil {
				if err := f.File.Close(); err != nil {
					_ = cached.valueLogReader.Release(readerSet)
					_ = snap.Close()
					t.Fatalf("pre-close secondary file %d: %v", id, err)
				}
				poisonedID = id
				break
			}
		}
		if err := cached.valueLogReader.Release(readerSet); err != nil {
			_ = snap.Close()
			t.Fatalf("release readerSet: %v", err)
		}
		if poisonedID == 0 {
			_ = snap.Close()
			t.Fatal("failed to locate zombie segment in cached.valueLogReader")
		}

		if err := snap.Close(); err != nil {
			t.Fatalf("snap.Close: %v", err)
		}

		select {
		case closeErr := <-reentrantCloseErrCh:
			if closeErr == nil {
				t.Fatal("re-entrant cached.Close() inside NotifyError returned nil, want callback-close refusal")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for NotifyError re-entrant cached.Close() (possible shutdown deadlock)")
		}
		expectClosedReceipt = true
	})

	t.Run("close_surfaces_unconsumed_async_reconcile_error", func(t *testing.T) {
		dir := t.TempDir()
		db, err := Open(dir, NewMockBackend(), Options{
			DisableWAL:  true,
			AllowUnsafe: true,
		})
		if err != nil {
			t.Fatalf("caching.Open: %v", err)
		}
		firstErr := errors.New("unrelated prior background error")
		reconcileErr := errors.New("unconsumed async retired reconcile error")
		db.retainOriginalCleanupError(firstErr)
		db.recordRetiredVlogReconcileError(reconcileErr)
		closeErr := db.Close()
		if !errors.Is(closeErr, firstErr) || !errors.Is(closeErr, reconcileErr) {
			t.Fatalf("Close() = %v, want both %q and %q", closeErr, firstErr, reconcileErr)
		}
	})
}
