package caching

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

var missingValueLogIDPattern = regexp.MustCompile(`valuelog file ([0-9]+) not found(?: in snapshot)?`)

func ageValueLogFilesForTest(t *testing.T, dir string, age time.Duration) {
	t.Helper()
	var paths []string
	for _, subdir := range []string{"value_vlog", "leaf_vlog"} {
		matches, err := filepath.Glob(filepath.Join(dir, subdir, "value-l*.log"))
		if err != nil {
			t.Fatalf("glob %s files: %v", subdir, err)
		}
		paths = append(paths, matches...)
	}
	old := time.Now().Add(-age)
	for _, path := range paths {
		if err := os.Chtimes(path, old, old); err != nil && !os.IsNotExist(err) {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}
}

func TestAgeValueLogFilesForTestToleratesDisappearedMatch(t *testing.T) {
	leafDir := filepath.Join(t.TempDir(), "leaf_vlog")
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("mkdir leaf vlog: %v", err)
	}
	path := filepath.Join(leafDir, "value-l255-000001.log")
	if err := os.Symlink(filepath.Join(leafDir, "already-removed.log"), path); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	ageValueLogFilesForTest(t, filepath.Dir(leafDir), time.Hour)
}

func valueLogPathForFileID(root string, fileID uint32) string {
	lane, seq := valuelog.DecodeFileID(fileID)
	subdir := "value_vlog"
	if int(lane) == leafLogLaneID {
		subdir = "leaf_vlog"
	}
	return filepath.Join(root, subdir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
}

func extractMissingValueLogID(err error) (uint32, bool) {
	if err == nil {
		return 0, false
	}
	m := missingValueLogIDPattern.FindStringSubmatch(err.Error())
	if len(m) != 2 {
		return 0, false
	}
	id, convErr := strconv.ParseUint(m[1], 10, 32)
	if convErr != nil {
		return 0, false
	}
	return uint32(id), true
}

func countLeafRefHits(counts map[uint32]int, fileIDs []uint32) int {
	total := 0
	for _, fileID := range fileIDs {
		total += counts[fileID]
	}
	return total
}

func missingLeafRefPaths(root string, counts map[uint32]int) []string {
	if len(counts) == 0 {
		return nil
	}
	out := make([]string, 0)
	for fileID, count := range counts {
		if count == 0 {
			continue
		}
		path := valueLogPathForFileID(root, fileID)
		if _, err := os.Stat(path); err == nil {
			continue
		} else if os.IsNotExist(err) {
			out = append(out, path)
		}
	}
	sort.Strings(out)
	return out
}

func liveFileIDsMissingFromSet(db pointerProjectionBackend, counts map[uint32]int) []uint32 {
	state := db.State()
	if state == nil || state.ValueLogSet == nil {
		out := make([]uint32, 0, len(counts))
		for fileID, count := range counts {
			if count > 0 {
				out = append(out, fileID)
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}
	out := make([]uint32, 0, len(counts))
	for fileID, count := range counts {
		if count == 0 {
			continue
		}
		if _, ok := state.ValueLogSet.Files[fileID]; ok {
			continue
		}
		out = append(out, fileID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

type pointerProjectionBackend interface {
	IteratorWithOptions(start, end []byte, opts tree.IteratorOptions) (iterator.UnsafeIterator, error)
	Pager() *pager.Pager
	State() *backenddb.DBState
	Dir() string
}

type backendLiveFileSources struct {
	direct map[uint32]int
	leaf   map[uint32]int
	nested map[uint32]int
}

func collectBackendLiveFileSources(t *testing.T, db pointerProjectionBackend) backendLiveFileSources {
	t.Helper()
	out := backendLiveFileSources{
		direct: make(map[uint32]int, 128),
		leaf:   make(map[uint32]int, 128),
		nested: make(map[uint32]int, 128),
	}
	it, err := db.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("IteratorWithOptions: %v", err)
	}
	defer it.Close()
	for it.Valid() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			out.direct[ptr.FileID]++
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	state := db.State()
	if state == nil {
		return out
	}
	for fileID, count := range collectLeafRefFileCounts(t, db.Pager(), state.RootPageID) {
		out.leaf[fileID] += count
	}
	for fileID, count := range collectLeafRefFileCounts(t, db.Pager(), state.SystemRootPageID) {
		out.leaf[fileID] += count
	}
	reader := valueReaderForBackendState(state)
	for fileID, count := range collectNestedLeafValueLogFileCounts(t, reader, db.Pager(), state.RootPageID, nil) {
		out.nested[fileID] += count
	}
	for fileID, count := range collectNestedLeafValueLogFileCounts(t, reader, db.Pager(), state.SystemRootPageID, nil) {
		out.nested[fileID] += count
	}
	return out
}

func collectBackendLiveFileCounts(t *testing.T, db pointerProjectionBackend) map[uint32]int {
	t.Helper()
	out, err := tryCollectBackendLiveFileCounts(t, db)
	if err != nil {
		t.Fatalf("collectBackendLiveFileCounts: %v", err)
	}
	return out
}

func tryCollectBackendLiveFileCounts(t *testing.T, db pointerProjectionBackend) (map[uint32]int, error) {
	t.Helper()
	out := make(map[uint32]int, 128)
	it, err := db.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.Valid() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			out[ptr.FileID]++
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	state := db.State()
	if state != nil {
		reader := valueReaderForBackendState(state)
		for fileID, count := range collectLeafRefFileCounts(t, db.Pager(), state.RootPageID) {
			out[fileID] += count
		}
		for fileID, count := range collectLeafRefFileCounts(t, db.Pager(), state.SystemRootPageID) {
			out[fileID] += count
		}
		for fileID, count := range collectNestedLeafValueLogFileCounts(t, reader, db.Pager(), state.RootPageID, nil) {
			out[fileID] += count
		}
		for fileID, count := range collectNestedLeafValueLogFileCounts(t, reader, db.Pager(), state.SystemRootPageID, nil) {
			out[fileID] += count
		}
	}
	return out, nil
}

func nestedLiveFileCountsWithReader(t *testing.T, reader interface {
	ReadUnsafe(ptr page.ValuePtr) ([]byte, error)
}, p *pager.Pager, state *backenddb.DBState) map[uint32]int {
	t.Helper()
	out := make(map[uint32]int, 128)
	if state == nil || p == nil || reader == nil {
		return out
	}
	for fileID, count := range collectNestedLeafValueLogFileCounts(t, reader, p, state.RootPageID, nil) {
		out[fileID] += count
	}
	for fileID, count := range collectNestedLeafValueLogFileCounts(t, reader, p, state.SystemRootPageID, nil) {
		out[fileID] += count
	}
	return out
}

func collectLeafRefFileCounts(t *testing.T, p *pager.Pager, rootID uint64) map[uint32]int {
	t.Helper()
	if p == nil || rootID == 0 {
		return nil
	}

	stack := []uint64{rootID}
	seen := make(map[uint64]struct{}, 1024)
	out := make(map[uint32]int, 128)
	for len(stack) > 0 {
		pageID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := seen[pageID]; ok {
			continue
		}
		seen[pageID] = struct{}{}
		data, err := p.Get(pageID)
		if err != nil {
			t.Fatalf("pager.Get(%d): %v", pageID, err)
		}
		n := node.NewNodeView(data)
		switch n.Type() {
		case page.PageTypeLeaf:
		case page.PageTypeInternal:
			for i := uint16(0); i < n.Count(); i++ {
				childRef, err := n.GetInternalChildRef(i)
				if err != nil {
					t.Fatalf("GetInternalChildRef(%d,%d): %v", pageID, i, err)
				}
				if childRef.Kind == page.ChildRefLeafLog {
					out[childRef.Log.ValueLogFileID()]++
					continue
				}
				stack = append(stack, childRef.Page)
			}
		default:
			t.Fatalf("unexpected page type %d at page %d", n.Type(), pageID)
		}
	}
	return out
}

func collectLeafRefs(t *testing.T, p *pager.Pager, rootID uint64) []page.LeafLogPtr {
	t.Helper()
	if p == nil || rootID == 0 {
		return nil
	}

	stack := []uint64{rootID}
	seen := make(map[uint64]struct{}, 1024)
	out := make([]page.LeafLogPtr, 0, 128)
	for len(stack) > 0 {
		pageID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := seen[pageID]; ok {
			continue
		}
		seen[pageID] = struct{}{}
		data, err := p.Get(pageID)
		if err != nil {
			t.Fatalf("pager.Get(%d): %v", pageID, err)
		}
		n := node.NewNodeView(data)
		switch n.Type() {
		case page.PageTypeLeaf:
		case page.PageTypeInternal:
			for i := uint16(0); i < n.Count(); i++ {
				childRef, err := n.GetInternalChildRef(i)
				if err != nil {
					t.Fatalf("GetInternalChildRef(%d,%d): %v", pageID, i, err)
				}
				if childRef.Kind == page.ChildRefLeafLog {
					out = append(out, childRef.Log)
					continue
				}
				stack = append(stack, childRef.Page)
			}
		default:
			t.Fatalf("unexpected page type %d at page %d", n.Type(), pageID)
		}
	}
	return out
}

func collectNestedLeafValueLogFileCounts(t *testing.T, reader interface {
	ReadUnsafe(ptr page.ValuePtr) ([]byte, error)
}, p *pager.Pager, rootID uint64, sourceSet map[uint32]struct{}) map[uint32]int {
	t.Helper()
	out := make(map[uint32]int, 128)
	if reader == nil || p == nil || rootID == 0 {
		return out
	}
	for _, ptr := range collectLeafRefs(t, p, rootID) {
		if len(sourceSet) > 0 {
			if _, ok := sourceSet[ptr.ValueLogFileID()]; !ok {
				continue
			}
		}
		leafPage, err := reader.ReadUnsafe(ptr.ValuePtr())
		if err != nil {
			t.Fatalf("read leaf page file=%d offset=%d: %v", ptr.FileID, ptr.Offset, err)
		}
		if len(leafPage) != page.PageSize {
			t.Fatalf("leaf page size file=%d offset=%d got=%d want=%d", ptr.FileID, ptr.Offset, len(leafPage), page.PageSize)
		}
		leaf := node.NewNodeView(leafPage)
		if leaf.Type() != page.PageTypeLeaf {
			t.Fatalf("expected leaf page in value log file=%d offset=%d, got type=%d", ptr.FileID, ptr.Offset, leaf.Type())
		}
		if !leaf.VerifyChecksum() {
			t.Fatalf("leaf page checksum mismatch file=%d offset=%d", ptr.FileID, ptr.Offset)
		}
		for i := uint16(0); i < leaf.Count(); i++ {
			_, _, valPtr, flags, err := leaf.GetLeafEntryView(i)
			if err != nil {
				t.Fatalf("GetLeafEntryView(file=%d, idx=%d): %v", ptr.FileID, i, err)
			}
			if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(valPtr.FileID) {
				continue
			}
			out[valPtr.FileID]++
		}
	}
	return out
}

func TestCachedRewriteLeafRefs_RemainReopenableAfterLaterCheckpoint(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: backenddb.ValueLogOptions{
			Generational: backenddb.ValueLogGenerationConfig{
				HotSegmentTargetBytes: 16 << 10,
			},
		},
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               true,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogMaxSegmentBytes:                  16 << 10,
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		FlushThreshold:                           1 << 30,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	db.testSkipRetainedPrune = true
	db.testSkipCheckpointAutoVacuum = true
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})

	for batchNum := 0; batchNum < 12; batchNum++ {
		b := db.NewBatch()
		for i := 0; i < 6000; i++ {
			key := []byte(fmt.Sprintf("k%02d-%05d", batchNum, i))
			val := bytes.Repeat([]byte(fmt.Sprintf("value-batch-%02d-", batchNum)), 24)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("set batch=%d key=%d: %v", batchNum, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("write batch=%d: %v", batchNum, err)
		}
		_ = b.Close()
	}

	state := backend.State()
	if state == nil {
		t.Fatalf("missing backend state")
	}
	leafCounts := collectLeafRefFileCounts(t, backend.Pager(), state.RootPageID)
	if len(leafCounts) < 8 {
		t.Fatalf("expected many leafref source files, got %d", len(leafCounts))
	}

	var (
		packStats backenddb.LeafGenerationPackRunOnceStats
		gcStats   backenddb.LeafGenerationGCStats
	)
	if err := db.runWithBackendMaintenance(func() error {
		var err error
		packStats, err = backend.LeafGenerationPackRunOnce(context.Background(), backenddb.LeafGenerationPackFromPlanOptions{
			Sync:                    true,
			MinPublishedAgeCommits:  1,
			MinCandidateGenerations: 2,
			MaxGenerations:          4,
			MaxBytesToCopy:          1 << 30,
		})
		if err != nil {
			return err
		}
		gcStats, err = backend.LeafGenerationGC(context.Background(), backenddb.LeafGenerationGCOptions{})
		return err
	}); err != nil {
		t.Fatalf("backend leaf generation maintenance: %v", err)
	}
	if !packStats.Ran {
		t.Fatalf("expected leaf generation pack to run, skip=%s", packStats.SkipReason)
	}
	sourceIDSet := make(map[uint32]struct{})
	for _, gen := range packStats.Selection.Generations {
		for _, fileID := range gen.FileIDs {
			if fileID == 0 {
				continue
			}
			sourceIDSet[fileID] = struct{}{}
		}
	}
	sourceIDs := make([]uint32, 0, len(sourceIDSet))
	for fileID := range sourceIDSet {
		sourceIDs = append(sourceIDs, fileID)
	}
	sort.Slice(sourceIDs, func(i, j int) bool { return sourceIDs[i] < sourceIDs[j] })
	if len(sourceIDs) == 0 {
		t.Fatal("expected packed source file IDs")
	}
	postPackCounts := collectLeafRefFileCounts(t, backend.Pager(), backend.State().RootPageID)
	if hits := countLeafRefHits(postPackCounts, sourceIDs); hits != 0 {
		t.Fatalf("pack+gc left %d leafrefs on packed source files", hits)
	}
	if gcStats.GenerationsDeleted == 0 && gcStats.BytesDeleted == 0 && gcStats.GenerationsRetiring == 0 {
		t.Fatalf("expected leaf generation gc to delete or at least tombstone packed generations, got %+v", gcStats)
	}

	b := db.NewBatch()
	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("post-%05d", i))
		val := bytes.Repeat([]byte{byte(i%251 + 1)}, 96)
		if err := b.Set(key, val); err != nil {
			_ = b.Close()
			t.Fatalf("post set %d: %v", i, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("post write: %v", err)
	}
	_ = b.Close()
	postWriteCounts := collectLeafRefFileCounts(t, backend.Pager(), backend.State().RootPageID)
	if hits := countLeafRefHits(postWriteCounts, sourceIDs); hits != 0 {
		t.Fatalf("later cached write reintroduced %d leafrefs on rewritten source files", hits)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	closed = true

	reopen, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("reopen rw backend: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("post-00042"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	want := bytes.Repeat([]byte{byte(42%251 + 1)}, 96)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen")
	}
}

func TestCachedGenerationalMaintenance_LeafRefsRemainReopenable(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envLeafGenerationPackMaintenanceMinReclaimPerByteCopiedPPM, "0")
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: backenddb.ValueLogOptions{
			Generational: backenddb.ValueLogGenerationConfig{
				HotSegmentTargetBytes: 16 << 10,
			},
		},
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               true,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogMaxSegmentBytes:                  16 << 10,
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		FlushThreshold:                           1 << 30,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	db.testSkipRetainedPrune = true
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})

	writeBatch := func(prefix string, n int) {
		t.Helper()
		b := db.NewBatch()
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%s-%05d", prefix, i))
			val := bytes.Repeat([]byte(fmt.Sprintf("%s-value-", prefix)), 12)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("set %s %d: %v", prefix, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("write %s: %v", prefix, err)
		}
		_ = b.Close()
	}

	for i := 0; i < 8; i++ {
		writeBatch(fmt.Sprintf("seed-%02d", i), 6000)
	}
	if err := db.checkpointForBackendMaintenance(); err != nil {
		t.Fatalf("checkpoint before maintenance: %v", err)
	}

	for round := 0; round < 6; round++ {
		ageValueLogFilesForTest(t, dir, vlogGenerationRewriteMinSegmentAge+time.Second)
		forceVlogMaintenanceIdle(db)
		db.maybeRunVlogGenerationMaintenance(false)
		writeBatch(fmt.Sprintf("post-%02d", round), 512)
		ageValueLogFilesForTest(t, dir, vlogGenerationRewriteMinSegmentAge+time.Second)
		forceVlogMaintenanceIdle(db)
		db.maybeRunVlogGenerationMaintenance(false)
	}

	if db.vlogGenerationLeafPackRuns.Load() == 0 {
		t.Fatalf("expected leaf generation pack maintenance to run")
	}
	if db.vlogGenerationLeafPackGCRuns.Load() == 0 {
		t.Fatalf("expected leaf generation gc maintenance to run")
	}

	preCloseCounts := collectBackendLiveFileCounts(t, backend)
	if missingIDs := liveFileIDsMissingFromSet(backend, preCloseCounts); len(missingIDs) != 0 {
		t.Fatalf("pre-close backend references file IDs missing from current set: %v", missingIDs[:min(8, len(missingIDs))])
	}
	if missing := missingLeafRefPaths(dir, preCloseCounts); len(missing) != 0 {
		t.Fatalf("pre-close backend already references missing value-log files: %v", missing[:min(4, len(missing))])
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	closed = true

	reopenRO, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ReadOnly:                   true,
	})
	if err != nil {
		t.Fatalf("reopen ro backend: %v", err)
	}

	postCloseCounts := collectBackendLiveFileCounts(t, reopenRO)
	if missingIDs := liveFileIDsMissingFromSet(reopenRO, postCloseCounts); len(missingIDs) != 0 {
		_ = reopenRO.Close()
		t.Fatalf("post-close backend references file IDs missing from current set: %v", missingIDs[:min(8, len(missingIDs))])
	}
	if missing := missingLeafRefPaths(dir, postCloseCounts); len(missing) != 0 {
		_ = reopenRO.Close()
		t.Fatalf("post-close backend references missing value-log files: %v", missing[:min(8, len(missing))])
	}
	if err := reopenRO.Close(); err != nil {
		t.Fatalf("close ro backend: %v", err)
	}

	reopenRW, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("reopen rw backend: %v", err)
	}
	defer reopenRW.Close()

	got, err := reopenRW.Get([]byte("post-05-00042"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	want := bytes.Repeat([]byte("post-05-value-"), 12)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen")
	}
}

func TestCachedGenerationalMaintenance_DirectPointersRemainInCurrentSet_WALOn(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			Generational: backenddb.ValueLogGenerationConfig{
				HotSegmentTargetBytes: 16 << 10,
			},
		},
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               false,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogPointerThreshold:                 1,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogMaxSegmentBytes:                  16 << 10,
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		FlushThreshold:                           1 << 30,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	skipRetainedPrune(db)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})

	writeBatch := func(prefix string, n int) {
		t.Helper()
		b := db.NewBatch()
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%s-%05d", prefix, i))
			val := bytes.Repeat([]byte(fmt.Sprintf("%s-direct-value-", prefix)), 96)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("set %s %d: %v", prefix, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("write %s: %v", prefix, err)
		}
		_ = b.Close()
	}

	for i := 0; i < 2; i++ {
		writeBatch(fmt.Sprintf("seed-%02d", i), 384)
	}

	if err := db.checkpointForBackendMaintenance(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	before := collectBackendLiveFileCounts(t, backend)
	if missingIDs := liveFileIDsMissingFromSet(backend, before); len(missingIDs) != 0 {
		t.Fatalf("pre-maintenance backend references file IDs missing from current set: %v", missingIDs[:min(8, len(missingIDs))])
	}
	if missing := missingLeafRefPaths(dir, before); len(missing) != 0 {
		t.Fatalf("pre-maintenance backend references missing value-log files: %v", missing[:min(8, len(missing))])
	}

	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	after, err := tryCollectBackendLiveFileCounts(t, backend)
	if err != nil {
		t.Fatalf("post-maintenance iterator error: %v", err)
	}
	if missingIDs := liveFileIDsMissingFromSet(backend, after); len(missingIDs) != 0 {
		t.Fatalf("post-maintenance backend references file IDs missing from current set: %v", missingIDs[:min(8, len(missingIDs))])
	}
	if missing := missingLeafRefPaths(dir, after); len(missing) != 0 {
		t.Fatalf("post-maintenance backend references missing value-log files: %v", missing[:min(8, len(missing))])
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	closed = true
}

func TestCachedGenerationalMaintenance_BackgroundSchedulerIdle_WALOn(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			Generational: backenddb.ValueLogGenerationConfig{
				HotSegmentTargetBytes: 16 << 10,
			},
		},
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               false,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogPointerThreshold:                 1,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogMaxSegmentBytes:                  16 << 10,
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		FlushThreshold:                           1 << 30,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	skipRetainedPrune(db)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})

	writeBatch := func(prefix string, n int) {
		t.Helper()
		b := db.NewBatch()
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%s-%05d", prefix, i))
			val := bytes.Repeat([]byte(fmt.Sprintf("%s-direct-value-", prefix)), 96)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("set %s %d: %v", prefix, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("write %s: %v", prefix, err)
		}
		_ = b.Close()
	}

	for i := 0; i < 3; i++ {
		writeBatch(fmt.Sprintf("seed-%02d", i), 384)
	}

	if got := db.vlogGenerationSchedulerState.Load(); got != vlogGenerationSchedulerDisabled {
		t.Fatalf("scheduler state=%d want disabled", got)
	}

	if err := db.checkpointForBackendMaintenance(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	after, err := tryCollectBackendLiveFileCounts(t, backend)
	if err != nil {
		t.Fatalf("post-checkpoint iterator error: %v", err)
	}
	if missingIDs := liveFileIDsMissingFromSet(backend, after); len(missingIDs) != 0 {
		t.Fatalf("post-checkpoint backend references file IDs missing from current set: %v", missingIDs[:min(8, len(missingIDs))])
	}
	if missing := missingLeafRefPaths(dir, after); len(missing) != 0 {
		t.Fatalf("post-checkpoint backend references missing value-log files: %v", missing[:min(8, len(missing))])
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	closed = true
}

func TestCachedGenerationalMaintenance_LeafRefsBackgroundReopenable_WALOn(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: backenddb.ValueLogOptions{
			Generational: backenddb.ValueLogGenerationConfig{
				HotSegmentTargetBytes: 16 << 10,
			},
		},
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               false,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogMaxSegmentBytes:                  16 << 10,
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		FlushThreshold:                           1 << 30,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	skipRetainedPrune(db)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})

	writeBatch := func(prefix string, n int) {
		t.Helper()
		b := db.NewBatch()
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%s-%05d", prefix, i))
			val := bytes.Repeat([]byte(fmt.Sprintf("%s-value-", prefix)), 12)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("set %s %d: %v", prefix, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("write %s: %v", prefix, err)
		}
		_ = b.Close()
	}

	seedBatches := 6
	rounds := 4
	postRoundWrites := 512

	for i := 0; i < seedBatches; i++ {
		writeBatch(fmt.Sprintf("seed-%02d", i), 3000)
		ageValueLogFilesForTest(t, dir, 5*time.Minute)
	}
	if err := db.checkpointForBackendMaintenance(); err != nil {
		t.Fatalf("checkpoint before maintenance: %v", err)
	}

	for round := 0; round < rounds; round++ {
		forceVlogMaintenanceIdle(db)
		db.maybeRunVlogGenerationMaintenance(false)
		writeBatch(fmt.Sprintf("round-%02d", round), postRoundWrites)
		ageValueLogFilesForTest(t, dir, 5*time.Minute)
		forceVlogMaintenanceIdle(db)
		db.maybeRunVlogGenerationMaintenance(false)

		counts, err := tryCollectBackendLiveFileCounts(t, backend)
		if err != nil {
			t.Fatalf("round %d iterator error: %v", round, err)
		}
		if missingIDs := liveFileIDsMissingFromSet(backend, counts); len(missingIDs) != 0 {
			t.Fatalf("round %d backend references file IDs missing from current set: %v", round, missingIDs[:min(8, len(missingIDs))])
		}
		if missing := missingLeafRefPaths(dir, counts); len(missing) != 0 {
			t.Fatalf("round %d backend references missing value-log files: %v", round, missing[:min(8, len(missing))])
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	closed = true

	reopenRO, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ReadOnly:                   true,
	})
	if err != nil {
		t.Fatalf("reopen ro backend: %v", err)
	}
	if err := reopenRO.Close(); err != nil {
		t.Fatalf("close ro backend: %v", err)
	}

	reopenRW, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("reopen rw backend: %v", err)
	}
	defer reopenRW.Close()

	got, err := reopenRW.Get([]byte(fmt.Sprintf("round-%02d-00042", rounds-1)))
	if err != nil {
		t.Fatalf("get after rw reopen: %v", err)
	}
	want := bytes.Repeat([]byte(fmt.Sprintf("round-%02d-value-", rounds-1)), 12)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after rw reopen")
	}
}

func TestCachedGenerationalMaintenance_DirectPointersSeedPhaseLarge_WALOn(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			Generational: backenddb.ValueLogGenerationConfig{
				HotSegmentTargetBytes: 16 << 10,
			},
		},
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               false,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogPointerThreshold:                 1,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogMaxSegmentBytes:                  16 << 10,
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		FlushThreshold:                           1 << 30,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	skipRetainedPrune(db)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})

	writeBatch := func(prefix string, n int) {
		t.Helper()
		b := db.NewBatch()
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%s-%05d", prefix, i))
			val := bytes.Repeat([]byte(fmt.Sprintf("%s-direct-value-", prefix)), 96)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("set %s %d: %v", prefix, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("write %s: %v", prefix, err)
		}
		_ = b.Close()
	}

	for i := 0; i < 4; i++ {
		writeBatch(fmt.Sprintf("seed-%02d", i), 1024)
	}
	if err := db.checkpointForBackendMaintenance(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	after, err := tryCollectBackendLiveFileCounts(t, backend)
	if err != nil {
		t.Fatalf("post-checkpoint iterator error: %v", err)
	}
	if missingIDs := liveFileIDsMissingFromSet(backend, after); len(missingIDs) != 0 {
		sources := collectBackendLiveFileSources(t, backend)
		type sourceKind struct {
			id           uint32
			direct       int
			leaf         int
			nested       int
			path         string
			exists       bool
			afterRefresh bool
		}
		if err := backend.RefreshValueLogSet(); err != nil {
			t.Fatalf("RefreshValueLogSet after missing IDs: %v", err)
		}
		refreshedState := backend.State()
		sample := make([]sourceKind, 0, min(8, len(missingIDs)))
		for _, id := range missingIDs[:min(8, len(missingIDs))] {
			path := valueLogPathForFileID(dir, id)
			_, statErr := os.Stat(path)
			inRefreshed := false
			if refreshedState != nil && refreshedState.ValueLogSet != nil {
				_, inRefreshed = refreshedState.ValueLogSet.Files[id]
			}
			sample = append(sample, sourceKind{
				id:           id,
				direct:       sources.direct[id],
				leaf:         sources.leaf[id],
				nested:       sources.nested[id],
				path:         path,
				exists:       statErr == nil,
				afterRefresh: inRefreshed,
			})
		}
		t.Fatalf("post-checkpoint backend references file IDs missing from current set: %v (sample=%+v rewrite_runs=%d gc_runs=%d retained=%v)", missingIDs[:min(8, len(missingIDs))], sample, db.vlogGenerationRewriteRuns.Load(), db.vlogGenerationGCRuns.Load(), db.valueLogRetainedPaths())
	}
	if missing := missingLeafRefPaths(dir, after); len(missing) != 0 {
		t.Fatalf("post-checkpoint backend references missing value-log files: %v", missing[:min(8, len(missing))])
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	closed = true
}

func TestCachedGenerationalMaintenance_DirectPointersManualGC_WALOn(t *testing.T) {
	t.Setenv(envDisableVlogGenerationRewrite, "1")
	t.Setenv(envDisableVlogGenerationGC, "1")
	t.Setenv(envDisableVlogGenerationVacuum, "1")

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOnRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			Generational: backenddb.ValueLogGenerationConfig{
				HotSegmentTargetBytes:  16 << 10,
				WarmSegmentTargetBytes: 16 << 10,
				ColdSegmentTargetBytes: 16 << 10,
			},
		},
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               false,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogPointerThreshold:                 1,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogMaxSegmentBytes:                  16 << 10,
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		FlushThreshold:                           1 << 30,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	skipRetainedPrune(db)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})

	writeBatch := func(prefix string, n int) {
		t.Helper()
		b := db.NewBatch()
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%s-%05d", prefix, i))
			val := bytes.Repeat([]byte(fmt.Sprintf("%s-direct-value-", prefix)), 96)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("set %s %d: %v", prefix, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("write %s: %v", prefix, err)
		}
		_ = b.Close()
	}

	for i := 0; i < 4; i++ {
		writeBatch(fmt.Sprintf("seed-%02d", i), 1024)
	}
	if err := db.checkpointForBackendMaintenance(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	before, err := tryCollectBackendLiveFileCounts(t, backend)
	if err != nil {
		t.Fatalf("pre-gc iterator error: %v", err)
	}
	if missingIDs := liveFileIDsMissingFromSet(backend, before); len(missingIDs) != 0 {
		t.Fatalf("pre-gc backend references file IDs missing from current set: %v", missingIDs[:min(8, len(missingIDs))])
	}

	stats, err := backend.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{
		ProtectedPaths: db.valueLogProtectedPaths(),
	})
	if err != nil {
		t.Fatalf("manual ValueLogGC: %v", err)
	}
	if stats.SegmentsTotal == 0 || stats.BytesTotal == 0 {
		t.Fatalf("expected manual gc to observe value-log segments in large seed phase, got %+v", stats)
	}

	after, err := tryCollectBackendLiveFileCounts(t, backend)
	if err != nil {
		t.Fatalf("post-gc iterator error: %v", err)
	}
	if missingIDs := liveFileIDsMissingFromSet(backend, after); len(missingIDs) != 0 {
		t.Fatalf("post-gc backend references file IDs missing from current set: %v", missingIDs[:min(8, len(missingIDs))])
	}
	if missing := missingLeafRefPaths(dir, after); len(missing) != 0 {
		t.Fatalf("post-gc backend references missing value-log files: %v", missing[:min(8, len(missing))])
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	closed = true
}

func testCachedRepeatedRewriteVacuumLeafRefsRemainReopenable(t *testing.T, disableWAL bool) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: backenddb.ValueLogOptions{
			Generational: backenddb.ValueLogGenerationConfig{
				HotSegmentTargetBytes: 16 << 10,
			},
		},
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               disableWAL,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogMaxSegmentBytes:                  16 << 10,
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		FlushThreshold:                           1 << 30,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	skipRetainedPrune(db)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})

	writeBatch := func(prefix string, n int) {
		t.Helper()
		b := db.NewBatch()
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%s-%05d", prefix, i))
			val := bytes.Repeat([]byte(fmt.Sprintf("%s-value-", prefix)), 12)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("set %s %d: %v", prefix, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("write %s: %v", prefix, err)
		}
		_ = b.Close()
	}

	seedBatches := 4
	rounds := 3
	postRoundWrites := 512
	if !disableWAL {
		seedBatches = 6
		rounds = 4
		postRoundWrites = 768
		if runtime.GOOS == "windows" {
			seedBatches = 4
			rounds = 3
			postRoundWrites = 512
		}
	}

	for i := 0; i < seedBatches; i++ {
		writeBatch(fmt.Sprintf("seed-%02d", i), 3000)
	}

	rewriteRounds := 0
	for round := 0; round < rounds; round++ {
		preRoundCounts := collectBackendLiveFileCounts(t, backend)
		if missingIDs := liveFileIDsMissingFromSet(backend, preRoundCounts); len(missingIDs) != 0 {
			t.Fatalf("round %d pre-rewrite backend references file IDs missing from current set: %v", round, missingIDs[:min(8, len(missingIDs))])
		}
		if missing := missingLeafRefPaths(dir, preRoundCounts); len(missing) != 0 {
			t.Fatalf("round %d pre-rewrite backend references missing value-log files: %v", round, missing[:min(8, len(missing))])
		}

		err := db.runWithBackendMaintenance(func() error {
			postCheckpointCounts := collectBackendLiveFileCounts(t, backend)
			if missingIDs := liveFileIDsMissingFromSet(backend, postCheckpointCounts); len(missingIDs) != 0 {
				liveIDs, liveErr := db.collectValueLogLiveIDs()
				liveSample := make([]uint32, 0, min(8, len(missingIDs)))
				for _, id := range missingIDs[:min(8, len(missingIDs))] {
					if _, ok := liveIDs[id]; ok {
						liveSample = append(liveSample, id)
					}
				}
				t.Fatalf("round %d post-checkpoint backend references file IDs missing from current set: %v (collect_live_sample=%v collect_live_err=%v retained=%v)",
					round, missingIDs[:min(8, len(missingIDs))], liveSample, liveErr, db.valueLogRetainedPaths())
			}
			if missing := missingLeafRefPaths(dir, postCheckpointCounts); len(missing) != 0 {
				t.Fatalf("round %d post-checkpoint backend references missing value-log files: %v", round, missing[:min(8, len(missing))])
			}

			state := backend.State()
			if state == nil {
				t.Fatalf("missing backend state round %d", round)
			}
			leafCounts := collectLeafRefFileCounts(t, backend.Pager(), state.RootPageID)
			currentSet := backend.State().ValueLogSet
			if currentSet == nil {
				t.Fatalf("missing current value-log set round %d", round)
			}
			maxByLane := make(map[uint32]uint32)
			for id := range currentSet.Files {
				seg := page.ValueLogSegmentID(id)
				lane := seg >> 23
				seq := seg & ((1 << 23) - 1)
				if cur, ok := maxByLane[lane]; !ok || seq > cur {
					maxByLane[lane] = seq
				}
			}

			sourceIDs := make([]uint32, 0, 8)
			for fileID, count := range leafCounts {
				if count < 8 {
					continue
				}
				seg := page.ValueLogSegmentID(fileID)
				lane := seg >> 23
				seq := seg & ((1 << 23) - 1)
				if maxByLane[lane] == seq {
					continue
				}
				sourceIDs = append(sourceIDs, fileID)
			}
			sort.Slice(sourceIDs, func(i, j int) bool { return sourceIDs[i] < sourceIDs[j] })
			if len(sourceIDs) < 4 {
				return nil
			}
			stats, err := backend.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
				BatchSize:      32,
				SyncEachBatch:  false,
				ProtectedPaths: db.valueLogProtectedPaths(),
				SourceFileIDs:  sourceIDs[:4],
			})
			if err != nil {
				var missingID uint32
				if id, ok := extractMissingValueLogID(err); ok {
					missingID = id
					path := valueLogPathForFileID(dir, missingID)
					_, statErr := os.Stat(path)
					t.Fatalf("rewrite round %d: %v (missing_id=%d in_leaf_counts=%d in_live_counts=%d in_current_set=%v path=%s stat_err=%v sources=%v)",
						round, err, missingID, leafCounts[missingID], preRoundCounts[missingID], currentSet.Files[missingID] != nil, path, statErr, sourceIDs[:min(8, len(sourceIDs))])
				}
				t.Fatalf("rewrite round %d: %v", round, err)
			}
			rewriteRounds++
			db.maybeRunVlogGenerationIndexVacuum(int64(stats.BytesBefore), false)
			return nil
		})
		if err != nil {
			t.Fatalf("maintenance round %d: %v", round, err)
		}

		writeBatch(fmt.Sprintf("round-%02d", round), postRoundWrites)
		postRoundCounts, err := tryCollectBackendLiveFileCounts(t, backend)
		if err != nil {
			var missingID uint32
			if id, ok := extractMissingValueLogID(err); ok {
				missingID = id
				state := backend.State()
				inSet := false
				if state != nil && state.ValueLogSet != nil {
					_, inSet = state.ValueLogSet.Files[missingID]
				}
				if refresher, ok := any(backend).(interface{ RefreshValueLogSet() error }); ok {
					if refreshErr := refresher.RefreshValueLogSet(); refreshErr != nil {
						t.Fatalf("round %d post-write iterator error: %v (missing_id=%d refresh_err=%v)", round, err, missingID, refreshErr)
					}
				}
				refreshed := backend.State()
				inRefreshedSet := false
				if refreshed != nil && refreshed.ValueLogSet != nil {
					_, inRefreshedSet = refreshed.ValueLogSet.Files[missingID]
				}
				path := valueLogPathForFileID(dir, missingID)
				_, statErr := os.Stat(path)
				laneID, _ := valuelog.DecodeFileID(missingID)
				currentPath := ""
				currentSeq := 0
				currentRetained := false
				if lane := db.valueLogLaneByID(int(laneID)); lane != nil {
					lane.vlogMu.Lock()
					currentPath = lane.vlogPath
					currentSeq = lane.vlogSeq
					currentRetained = lane.vlogPath != "" && lane.vlogPath == lane.vlogRetainedPath
					lane.vlogMu.Unlock()
				}
				t.Fatalf("round %d post-write iterator error: %v (missing_id=%d in_set=%v in_refreshed_set=%v path=%s stat_err=%v current_lane_path=%s current_lane_seq=%d current_lane_retained=%v)",
					round, err, missingID, inSet, inRefreshedSet, path, statErr, currentPath, currentSeq, currentRetained)
			}
			t.Fatalf("round %d post-write iterator error: %v", round, err)
		}
		if missingIDs := liveFileIDsMissingFromSet(backend, postRoundCounts); len(missingIDs) != 0 {
			t.Fatalf("round %d post-write backend references file IDs missing from current set: %v", round, missingIDs[:min(8, len(missingIDs))])
		}
		if missing := missingLeafRefPaths(dir, postRoundCounts); len(missing) != 0 {
			t.Fatalf("round %d post-write backend references missing value-log files: %v", round, missing[:min(8, len(missing))])
		}
	}

	if rewriteRounds == 0 {
		t.Fatalf("expected at least one rewrite round")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	closed = true

	reopenRO, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ReadOnly:                   true,
	})
	if err != nil {
		t.Fatalf("reopen ro backend: %v", err)
	}
	defer reopenRO.Close()

	postCloseCounts := collectBackendLiveFileCounts(t, reopenRO)
	if missingIDs := liveFileIDsMissingFromSet(reopenRO, postCloseCounts); len(missingIDs) != 0 {
		t.Fatalf("post-close backend references file IDs missing from current set: %v", missingIDs[:min(8, len(missingIDs))])
	}
	if missing := missingLeafRefPaths(dir, postCloseCounts); len(missing) != 0 {
		t.Fatalf("post-close backend references missing value-log files: %v", missing[:min(8, len(missing))])
	}
	if err := reopenRO.Close(); err != nil {
		t.Fatalf("close ro backend: %v", err)
	}

	reopenRW, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("reopen rw backend: %v", err)
	}
	defer reopenRW.Close()

	lastPrefix := fmt.Sprintf("round-%02d", rounds-1)
	got, err := reopenRW.Get([]byte(lastPrefix + "-00042"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	want := bytes.Repeat([]byte(fmt.Sprintf("%s-value-", lastPrefix)), 12)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen")
	}

}

func TestCachedRepeatedRewriteVacuumLeafRefsRemainReopenable(t *testing.T) {
	testCachedRepeatedRewriteVacuumLeafRefsRemainReopenable(t, true)
}

func TestCachedRepeatedRewriteVacuumLeafRefsRemainReopenable_WALOn(t *testing.T) {
	testCachedRepeatedRewriteVacuumLeafRefsRemainReopenable(t, false)
}

func TestCachedManualMaintenanceDirectPointersRemainReopenable_WALOn(t *testing.T) {
	t.Setenv(envDisableVlogGenerationRewrite, "1")
	t.Setenv(envDisableVlogGenerationGC, "1")
	t.Setenv(envDisableVlogGenerationVacuum, "1")

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			Generational: backenddb.ValueLogGenerationConfig{
				HotSegmentTargetBytes: 16 << 10,
			},
		},
	})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                              true,
		DisableWAL:                               false,
		JournalLanes:                             1,
		IndexOuterLeavesInValueLog:               true,
		ValueLogPointerThreshold:                 1,
		ValueLogGenerationPolicy:                 uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogMaxSegmentBytes:                  16 << 10,
		ValueLogGenerationHotSegmentTargetBytes:  16 << 10,
		ValueLogGenerationWarmSegmentTargetBytes: 16 << 10,
		ValueLogGenerationColdSegmentTargetBytes: 16 << 10,
		ValueLogRewriteTriggerTotalBytes:         1,
		FlushThreshold:                           1 << 30,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
	skipRetainedPrune(db)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})

	writeBatch := func(prefix string, n int) {
		t.Helper()
		b := db.NewBatch()
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%s-%05d", prefix, i))
			val := bytes.Repeat([]byte(fmt.Sprintf("%s-direct-value-", prefix)), 96)
			if err := b.Set(key, val); err != nil {
				_ = b.Close()
				t.Fatalf("set %s %d: %v", prefix, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("write %s: %v", prefix, err)
		}
		_ = b.Close()
	}

	seedBatches := 3
	rounds := 2
	postRoundWrites := 256
	if runtime.GOOS == "windows" {
		seedBatches = 2
		rounds = 2
		postRoundWrites = 256
	}
	for i := 0; i < seedBatches; i++ {
		writeBatch(fmt.Sprintf("seed-%02d", i), 1024)
	}

	rewriteRounds := 0
	for round := 0; round < rounds; round++ {
		preRoundCounts := collectBackendLiveFileCounts(t, backend)
		if missingIDs := liveFileIDsMissingFromSet(backend, preRoundCounts); len(missingIDs) != 0 {
			t.Fatalf("round %d pre-rewrite backend references file IDs missing from current set: %v", round, missingIDs[:min(8, len(missingIDs))])
		}
		if missing := missingLeafRefPaths(dir, preRoundCounts); len(missing) != 0 {
			t.Fatalf("round %d pre-rewrite backend references missing value-log files: %v", round, missing[:min(8, len(missing))])
		}

		err := db.runWithBackendMaintenance(func() error {
			postCheckpointCounts := collectBackendLiveFileCounts(t, backend)
			if missingIDs := liveFileIDsMissingFromSet(backend, postCheckpointCounts); len(missingIDs) != 0 {
				liveIDs, liveErr := db.collectValueLogLiveIDs()
				liveSample := make([]uint32, 0, min(8, len(missingIDs)))
				for _, id := range missingIDs[:min(8, len(missingIDs))] {
					if _, ok := liveIDs[id]; ok {
						liveSample = append(liveSample, id)
					}
				}
				t.Fatalf("round %d post-checkpoint backend references file IDs missing from current set: %v (collect_live_sample=%v collect_live_err=%v retained=%v)",
					round, missingIDs[:min(8, len(missingIDs))], liveSample, liveErr, db.valueLogRetainedPaths())
			}
			if missing := missingLeafRefPaths(dir, postCheckpointCounts); len(missing) != 0 {
				t.Fatalf("round %d post-checkpoint backend references missing value-log files: %v", round, missing[:min(8, len(missing))])
			}

			state := backend.State()
			if state == nil || state.ValueLogSet == nil {
				t.Fatalf("missing backend state/value-log set round %d", round)
			}
			maxByLane := make(map[uint32]uint32)
			for id := range state.ValueLogSet.Files {
				lane, seq := uint32(page.ValueLogSegmentID(id)>>23), uint32(page.ValueLogSegmentID(id)&((1<<23)-1))
				if cur, ok := maxByLane[lane]; !ok || seq > cur {
					maxByLane[lane] = seq
				}
			}

			sourceIDs := make([]uint32, 0, 8)
			for fileID, count := range postCheckpointCounts {
				if count < 32 {
					continue
				}
				lane, seq := uint32(page.ValueLogSegmentID(fileID)>>23), uint32(page.ValueLogSegmentID(fileID)&((1<<23)-1))
				if lane >= 248 {
					continue
				}
				if maxByLane[lane] == seq {
					continue
				}
				sourceIDs = append(sourceIDs, fileID)
			}
			sort.Slice(sourceIDs, func(i, j int) bool { return sourceIDs[i] < sourceIDs[j] })
			if len(sourceIDs) < 4 {
				return nil
			}
			stats, err := backend.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
				BatchSize:      32,
				SyncEachBatch:  false,
				ProtectedPaths: db.valueLogProtectedPaths(),
				SourceFileIDs:  sourceIDs[:4],
			})
			if err != nil {
				var missingID uint32
				if id, ok := extractMissingValueLogID(err); ok {
					missingID = id
					refreshedState := backend.State()
					inSet := false
					if refreshedState != nil && refreshedState.ValueLogSet != nil {
						_, inSet = refreshedState.ValueLogSet.Files[missingID]
					}
					path := valueLogPathForFileID(dir, missingID)
					_, statErr := os.Stat(path)
					t.Fatalf("rewrite round %d: %v (missing_id=%d in_set=%v path=%s stat_err=%v sources=%v)",
						round, err, missingID, inSet, path, statErr, sourceIDs[:min(8, len(sourceIDs))])
				}
				return fmt.Errorf("rewrite round %d: %w", round, err)
			}
			rewriteRounds++
			db.maybeRunVlogGenerationIndexVacuum(int64(stats.BytesBefore), false)
			return nil
		})
		if err != nil {
			t.Fatalf("maintenance round %d: %v", round, err)
		}

		writeBatch(fmt.Sprintf("round-%02d", round), postRoundWrites)
		postRoundCounts, err := tryCollectBackendLiveFileCounts(t, backend)
		if err != nil {
			var missingID uint32
			if id, ok := extractMissingValueLogID(err); ok {
				missingID = id
				refreshedState := backend.State()
				inSet := false
				if refreshedState != nil && refreshedState.ValueLogSet != nil {
					_, inSet = refreshedState.ValueLogSet.Files[missingID]
				}
				path := valueLogPathForFileID(dir, missingID)
				_, statErr := os.Stat(path)
				t.Fatalf("round %d post-write iterator error: %v (missing_id=%d in_set=%v path=%s stat_err=%v)", round, err, missingID, inSet, path, statErr)
			}
			t.Fatalf("round %d post-write iterator error: %v", round, err)
		}
		if missingIDs := liveFileIDsMissingFromSet(backend, postRoundCounts); len(missingIDs) != 0 {
			t.Fatalf("round %d post-write backend references file IDs missing from current set: %v", round, missingIDs[:min(8, len(missingIDs))])
		}
		if missing := missingLeafRefPaths(dir, postRoundCounts); len(missing) != 0 {
			t.Fatalf("round %d post-write backend references missing value-log files: %v", round, missing[:min(8, len(missing))])
		}
	}

	if rewriteRounds == 0 {
		t.Fatalf("expected at least one rewrite round")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	closed = true

	reopen, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ReadOnly:                   true,
	})
	if err != nil {
		t.Fatalf("reopen ro backend: %v", err)
	}
	defer reopen.Close()

	lastPrefix := fmt.Sprintf("round-%02d", rounds-1)
	got, err := reopen.Get([]byte(lastPrefix + "-00042"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	want := bytes.Repeat([]byte(fmt.Sprintf("%s-direct-value-", lastPrefix)), 96)
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after reopen")
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("close ro backend: %v", err)
	}

	reopenRW, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("reopen rw backend: %v", err)
	}
	defer reopenRW.Close()

	gotRW, err := reopenRW.Get([]byte(lastPrefix + "-00042"))
	if err != nil {
		t.Fatalf("get after rw reopen: %v", err)
	}
	if !bytes.Equal(gotRW, want) {
		t.Fatalf("value mismatch after rw reopen")
	}
}
