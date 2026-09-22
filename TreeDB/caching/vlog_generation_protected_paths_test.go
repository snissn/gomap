package caching

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type rewriteRecordingBackend struct {
	*backenddb.DB

	mu             sync.Mutex
	protectedPaths []string
	reservedStarts []uint64
}

func (b *rewriteRecordingBackend) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
	b.mu.Lock()
	b.protectedPaths = append([]string(nil), opts.ProtectedPaths...)
	b.mu.Unlock()
	if opts.ReserveRIDs != nil {
		start, err := opts.ReserveRIDs(3)
		if err != nil {
			return backenddb.ValueLogRewriteStats{}, err
		}
		b.mu.Lock()
		b.reservedStarts = append(b.reservedStarts, start)
		b.mu.Unlock()
	}
	return backenddb.ValueLogRewriteStats{}, nil
}

func (b *rewriteRecordingBackend) recordedProtectedPaths() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.protectedPaths...)
}

func (b *rewriteRecordingBackend) recordedReservedStarts() []uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]uint64(nil), b.reservedStarts...)
}

func TestVlogGenerationRewrite_ProtectedPathsIncludeCurrentValueLogPaths(t *testing.T) {
	disableVlogGenerationLoop(t)
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	recorder := &rewriteRecordingBackend{DB: backend}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogCompression:              1, // off; keep segment sizes deterministic
		ValueLogMaxSegmentBytes:          256 << 10,
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)
	db.testSkipVlogCheckpointKick = true
	// Create eligible stale bytes (segment has both live and stale bytes) and
	// ensure it is not the active segment:
	// 1) Write k1, k2 into segment A.
	// 2) Overwrite k1 in segment A (k1's first record becomes stale).
	// 3) Write k3 which triggers rotation, closing segment A.
	payload := make([]byte, 96<<10)
	keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k1"), []byte("k3")}
	for i := range keys {
		b := db.NewBatch()
		if err := b.Set(keys[i], payload); err != nil {
			_ = b.Close()
			t.Fatalf("set: %v", err)
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "value_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob value_vlog files: %v", err)
	}
	old := time.Now().Add(-5 * time.Minute)
	for _, path := range paths {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1 << 20)
	forceVlogMaintenanceIdle(db)
	retained := db.valueLogRetainedStatsDetailed()
	if retained.BytesTotal <= 0 || retained.SegmentsTotal == 0 {
		t.Fatalf("expected retained value-log bytes before maintenance; got bytes=%d segments=%d", retained.BytesTotal, retained.SegmentsTotal)
	}
	if ok, _ := db.shouldRunVlogGenerationRewrite(retained.BytesTotal, 0, 0); !ok {
		t.Fatalf("expected retained bytes to trigger rewrite; trigger=%d bytes_total=%d", db.valueLogRewriteTriggerBytes, retained.BytesTotal)
	}
	plan, err := recorder.ValueLogRewritePlan(context.Background(), backenddb.ValueLogRewriteOnlineOptions{MaxSourceBytes: 1 << 20})
	if err != nil {
		t.Fatalf("rewrite plan: %v", err)
	}
	if len(plan.SourceFileIDs) == 0 {
		t.Fatalf("expected rewrite plan to select segments; bytes_total=%d bytes_live=%d bytes_stale=%d segments_total=%d", plan.BytesTotal, plan.BytesLive, plan.BytesStale, plan.SegmentsTotal)
	}
	db.maybeRunVlogGenerationMaintenance(false)

	got := recorder.recordedProtectedPaths()
	if len(got) == 0 {
		t.Fatalf("expected rewrite to be invoked and to record protected paths")
	}
	gotSet := make(map[string]struct{}, len(got))
	for _, path := range got {
		if path == "" {
			continue
		}
		gotSet[path] = struct{}{}
	}

	want := db.currentValueLogPaths()
	if len(want) == 0 {
		t.Fatalf("missing current value-log paths")
	}
	for _, path := range want {
		if path == "" {
			continue
		}
		if _, ok := gotSet[path]; !ok {
			t.Fatalf("protected paths missing current value-log segment: %s", path)
		}
	}
}

func TestValueLogProtectedPaths_IncludeRetainedPaths(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	recorder := &rewriteRecordingBackend{DB: backend}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	retainedPath := filepath.Join(dir, "value_vlog", "value-l0-000999.log")
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o755); err != nil {
		t.Fatalf("mkdir value_vlog dir: %v", err)
	}
	if err := os.WriteFile(retainedPath, []byte("retained"), 0o644); err != nil {
		t.Fatalf("write retained path: %v", err)
	}
	db.valueLogMu.Lock()
	db.valueLogRetain = map[string]struct{}{retainedPath: {}}
	db.valueLogMu.Unlock()

	got := db.valueLogProtectedPaths()
	found := false
	for _, path := range got {
		if path == retainedPath {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("protected paths missing retained segment: %s", retainedPath)
	}
}

func TestVlogGenerationRewrite_UsesSharedRIDAllocator(t *testing.T) {
	disableVlogGenerationLoop(t)
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	recorder := &rewriteRecordingBackend{DB: backend}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogCompression:              1, // off; keep segment sizes deterministic
		ValueLogMaxSegmentBytes:          256 << 10,
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
		ValueLogPointerThreshold:         1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)
	db.testSkipVlogCheckpointKick = true
	// Create eligible stale bytes and force rotation (see ProtectedPaths test above).
	payload := make([]byte, 96<<10)
	if err := db.Set([]byte("k1"), payload); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := db.Set([]byte("k2"), payload); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := db.Set([]byte("k1"), payload); err != nil {
		t.Fatalf("overwrite k1: %v", err)
	}
	if err := db.Set([]byte("k3"), payload); err != nil {
		t.Fatalf("set k3: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "value_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob value_vlog files: %v", err)
	}
	old := time.Now().Add(-5 * time.Minute)
	for _, path := range paths {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}

	before := db.nextRID.Load()
	if before == 0 {
		t.Fatalf("expected nextRID to be seeded by pointer write")
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1 << 20)
	forceVlogMaintenanceIdle(db)
	retained := db.valueLogRetainedStatsDetailed()
	if retained.BytesTotal <= 0 || retained.SegmentsTotal == 0 {
		t.Fatalf("expected retained value-log bytes before maintenance; got bytes=%d segments=%d", retained.BytesTotal, retained.SegmentsTotal)
	}
	if ok, _ := db.shouldRunVlogGenerationRewrite(retained.BytesTotal, 0, 0); !ok {
		t.Fatalf("expected retained bytes to trigger rewrite; trigger=%d bytes_total=%d", db.valueLogRewriteTriggerBytes, retained.BytesTotal)
	}
	plan, err := recorder.ValueLogRewritePlan(context.Background(), backenddb.ValueLogRewriteOnlineOptions{MaxSourceBytes: 1 << 20})
	if err != nil {
		t.Fatalf("rewrite plan: %v", err)
	}
	if len(plan.SourceFileIDs) == 0 {
		t.Fatalf("expected rewrite plan to select segments; bytes_total=%d bytes_live=%d bytes_stale=%d segments_total=%d", plan.BytesTotal, plan.BytesLive, plan.BytesStale, plan.SegmentsTotal)
	}
	db.maybeRunVlogGenerationMaintenance(false)

	got := recorder.recordedReservedStarts()
	if len(got) != 1 {
		t.Fatalf("expected one ReserveRIDs call, got %d", len(got))
	}
	if got[0] != before+1 {
		t.Fatalf("expected ReserveRIDs to start at %d, got %d", before+1, got[0])
	}
	if after := db.nextRID.Load(); after != before+3 {
		t.Fatalf("expected nextRID advanced to %d after reserve, got %d", before+3, after)
	}
}

func openSplitValueLogOwnershipTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			Generational: backenddb.ValueLogGenerationConfig{
				HotSegmentTargetBytes: 64 << 10,
			},
		},
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               true,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ForceValueLogPointers:                    true,
		ValueLogPointerThreshold:                 1,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogMaxSegmentBytes:                  64 << 10,
		ValueLogGenerationHotSegmentTargetBytes:  64 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 64 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 64 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)
	payload := make([]byte, 32<<10)
	for i := 0; i < 4; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k-%d", i)), payload); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	return db
}

func mustCurrentValueLogPathsForOwnershipTest(t *testing.T, db *DB) (valuePath string, leafPath string) {
	t.Helper()
	if db == nil {
		t.Fatal("missing db")
	}
	if len(db.lanes) == 0 {
		t.Fatal("missing value-log lanes")
	}
	db.lanes[0].vlogMu.Lock()
	valuePath = db.lanes[0].vlogPath
	db.lanes[0].vlogMu.Unlock()
	for _, leafLane := range db.leafLogAppendLanesSnapshot() {
		if leafLane == nil {
			continue
		}
		leafLane.vlogMu.Lock()
		path := leafLane.vlogPath
		leafLane.vlogMu.Unlock()
		if path != "" {
			leafPath = path
			break
		}
	}
	if valuePath == "" {
		t.Fatal("missing current value_vlog path")
	}
	if leafPath == "" {
		t.Fatal("missing current leaf_vlog path")
	}
	return valuePath, leafPath
}

func mustValueLogIDForPath(t *testing.T, path string) uint32 {
	t.Helper()
	laneID, seq, valueLog, ok := parseLogSeq(filepath.Base(path))
	if !ok || !valueLog || laneID < 0 {
		t.Fatalf("parseLogSeq(%q) failed", path)
	}
	id, err := valuelog.EncodeFileID(uint32(laneID), uint32(seq))
	if err != nil {
		t.Fatalf("EncodeFileID(%q): %v", path, err)
	}
	return id
}

func TestValueLogProtectedPaths_ExcludeLeafPathsInSplitMode(t *testing.T) {
	db := openSplitValueLogOwnershipTestDB(t)
	valuePath, leafPath := mustCurrentValueLogPathsForOwnershipTest(t, db)
	retainedValuePath := filepath.Join(filepath.Dir(valuePath), "value-l0-000777.log")
	retainedLeafPath := filepath.Join(filepath.Dir(leafPath), "value-l248-000777.log")
	if err := os.WriteFile(retainedValuePath, []byte("retained-value"), 0o644); err != nil {
		t.Fatalf("write retained value path: %v", err)
	}
	if err := os.WriteFile(retainedLeafPath, []byte("retained-leaf"), 0o644); err != nil {
		t.Fatalf("write retained leaf path: %v", err)
	}
	db.valueLogMu.Lock()
	db.valueLogRetain[retainedValuePath] = struct{}{}
	db.valueLogRetain[retainedLeafPath] = struct{}{}
	db.valueLogMu.Unlock()

	got := db.valueLogProtectedPaths()
	gotSet := make(map[string]struct{}, len(got))
	for _, path := range got {
		gotSet[path] = struct{}{}
	}
	if _, ok := gotSet[valuePath]; !ok {
		t.Fatalf("protected paths missing current value_vlog path: %s", valuePath)
	}
	if _, ok := gotSet[retainedValuePath]; !ok {
		t.Fatalf("protected paths missing retained value_vlog path: %s", retainedValuePath)
	}
	if _, ok := gotSet[leafPath]; ok {
		t.Fatalf("protected paths unexpectedly include current leaf_vlog path: %s", leafPath)
	}
	if _, ok := gotSet[retainedLeafPath]; ok {
		t.Fatalf("protected paths unexpectedly include retained leaf_vlog path: %s", retainedLeafPath)
	}
}

func TestCollectValueLogLiveIDs_ExcludesLeafLogIDsInSplitMode(t *testing.T) {
	db := openSplitValueLogOwnershipTestDB(t)
	valuePath, leafPath := mustCurrentValueLogPathsForOwnershipTest(t, db)
	valueID := mustValueLogIDForPath(t, valuePath)
	leafID := mustValueLogIDForPath(t, leafPath)

	live, err := db.collectValueLogLiveIDs()
	if err != nil {
		t.Fatalf("collectValueLogLiveIDs: %v", err)
	}
	if _, ok := live[valueID]; !ok {
		t.Fatalf("live IDs missing current value_vlog file id %d", valueID)
	}
	if _, ok := live[leafID]; ok {
		t.Fatalf("live IDs unexpectedly include current leaf_vlog file id %d", leafID)
	}
}

func TestPruneRetainedValueLogs_IgnoresLeafPathsInSplitMode(t *testing.T) {
	db := openSplitValueLogOwnershipTestDB(t)
	valuePath, leafPath := mustCurrentValueLogPathsForOwnershipTest(t, db)
	retainedValuePath := filepath.Join(filepath.Dir(valuePath), "value-l0-000778.log")
	retainedLeafPath := filepath.Join(filepath.Dir(leafPath), "value-l248-000778.log")
	if err := os.WriteFile(retainedValuePath, []byte("retained-value"), 0o644); err != nil {
		t.Fatalf("write retained value path: %v", err)
	}
	if err := os.WriteFile(retainedLeafPath, []byte("retained-leaf"), 0o644); err != nil {
		t.Fatalf("write retained leaf path: %v", err)
	}
	db.valueLogMu.Lock()
	db.valueLogRetain[retainedValuePath] = struct{}{}
	db.valueLogRetain[retainedLeafPath] = struct{}{}
	db.valueLogMu.Unlock()

	stats := db.pruneRetainedValueLogs(true)
	if _, err := os.Stat(retainedLeafPath); err != nil {
		t.Fatalf("expected retained leaf_vlog path to remain, stat err=%v", err)
	}
	if stats.CandidateSegments != 1 {
		t.Fatalf("candidate segments=%d want 1", stats.CandidateSegments)
	}
	if stats.RemovedSegments+stats.ZombieMarkedSegments != 1 {
		t.Fatalf("value action count=%d want 1 (removed=%d zombie_marked=%d)", stats.RemovedSegments+stats.ZombieMarkedSegments, stats.RemovedSegments, stats.ZombieMarkedSegments)
	}
}

func TestSplitValueLogOwnership_LeafRefStateReaderDecodesCompactLeafPages(t *testing.T) {
	db := openSplitValueLogOwnershipTestDB(t)
	snapper, ok := db.backend.(interface{ AcquireSnapshot() *backenddb.Snapshot })
	if !ok {
		t.Fatal("backend missing AcquireSnapshot")
	}
	snap := snapper.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing backend snapshot")
	}
	defer func() { _ = snap.Close() }()
	state := snap.State()
	p := snap.Pager()
	if state == nil || p == nil {
		t.Fatal("missing snapshot state/pager")
	}
	reader := valueReaderForBackendState(state)
	if reader == nil {
		t.Fatal("missing state value-log reader")
	}
	refs := collectLeafRefs(t, p, state.RootPageID)
	if len(refs) == 0 {
		t.Fatal("expected at least one leaf ref")
	}
	ptr := refs[0]
	leafPage, err := reader.ReadUnsafe(ptr.ValuePtr())
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	if len(leafPage) != page.PageSize {
		setPage, setErr := state.ValueLogSet.ReadUnsafe(ptr.ValuePtr())
		managerPage, managerErr := db.valueLogReader.ReadUnsafe(ptr.ValuePtr())
		t.Fatalf("leaf page len=%d want %d (set_len=%d set_err=%v manager_len=%d manager_err=%v)", len(leafPage), page.PageSize, len(setPage), setErr, len(managerPage), managerErr)
	}
}

func TestSplitValueLogOwnership_PointerProjectionIteratorDecodesCompactLeafPages(t *testing.T) {
	db := openSplitValueLogOwnershipTestDB(t)
	snapper, ok := db.backend.(interface{ AcquireSnapshot() *backenddb.Snapshot })
	if !ok {
		t.Fatal("backend missing AcquireSnapshot")
	}
	snap := snapper.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing backend snapshot")
	}
	defer func() { _ = snap.Close() }()
	state := snap.State()
	p := snap.Pager()
	if state == nil || p == nil {
		t.Fatal("missing snapshot state/pager")
	}
	reader := newCachedLiveScanReader(valueReaderForBackendState(state), db.valueLogReader)
	it := tree.New(p, reader, state.RootPageID).IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	defer it.Close()
	if !it.Valid() {
		t.Fatal("expected iterator to yield entries")
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
}
