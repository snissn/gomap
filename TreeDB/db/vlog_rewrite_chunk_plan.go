package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const defaultValueLogRewriteChunkBytes = 16 << 20

// ValueLogRewritePlanChunk summarizes one sub-file chunk of a value-log
// segment. This is a planning primitive for future incremental rewrite work;
// it does not yet change execution.
type ValueLogRewritePlanChunk struct {
	FileID      uint32
	ChunkOffset int64
	BytesTotal  int64
	BytesLive   int64
	BytesStale  int64
	StaleRatio  float64
}

// ValueLogRewriteChunkPlan mirrors ValueLogRewritePlan, but at chunk granularity.
// It is intended for future incremental rewrite scheduling work.
type ValueLogRewriteChunkPlan struct {
	ChunkBytes int64

	SourceChunks []ValueLogRewritePlanChunk

	ChunksTotal    int
	ChunksSelected int

	BytesTotal int64
	BytesLive  int64
	BytesStale int64

	SelectedBytesTotal int64
	SelectedBytesLive  int64
	SelectedBytesStale int64

	AgeBlockedChunks          int
	AgeBlockedBytesTotal      int64
	AgeBlockedBytesLive       int64
	AgeBlockedBytesStale      int64
	AgeBlockedMinRemainingAge time.Duration
}

type valueLogChunkKey struct {
	fileID      uint32
	chunkOffset int64
}

type rewriteSourceChunk struct {
	fileID      uint32
	chunkOffset int64
	totalBytes  int64
	liveBytes   int64
	staleBytes  int64
	staleRatio  float64
}

func normalizeValueLogRewriteChunkBytes(chunkBytes int64) int64 {
	if chunkBytes <= 0 {
		return defaultValueLogRewriteChunkBytes
	}
	return chunkBytes
}

func valueLogRecordStartOffset(ptr page.ValuePtr) (int64, error) {
	if ptr.Offset < 4 {
		return 0, fmt.Errorf("vlog-rewrite: invalid pointer offset %d", ptr.Offset)
	}
	return int64(ptr.Offset - 4), nil
}

func valueLogChunkOffsetForPtr(ptr page.ValuePtr, chunkBytes int64) (int64, error) {
	chunkBytes = normalizeValueLogRewriteChunkBytes(chunkBytes)
	start, err := valueLogRecordStartOffset(ptr)
	if err != nil {
		return 0, err
	}
	return (start / chunkBytes) * chunkBytes, nil
}

func shouldCountGroupedRewriteRecord(ptr page.ValuePtr, seenGroupedRecords *map[groupedRecordKey]struct{}) (bool, error) {
	if !page.ValuePtrIsGrouped(ptr) {
		return true, nil
	}
	k, err := groupedRecordKeyForPtr(ptr)
	if err != nil {
		return false, err
	}
	seen := map[groupedRecordKey]struct{}(nil)
	if seenGroupedRecords != nil {
		seen = *seenGroupedRecords
	}
	if seen == nil {
		seen = make(map[groupedRecordKey]struct{}, 1024)
		if seenGroupedRecords != nil {
			*seenGroupedRecords = seen
		}
	}
	if _, ok := seen[k]; ok {
		return false, nil
	}
	seen[k] = struct{}{}
	return true, nil
}

func (db *DB) collectValueLogPtrLiveBytes(ptr page.ValuePtr, seenGroupedRecords *map[groupedRecordKey]struct{}, set *valuelog.Set, observe func(page.ValuePtr, uint32) error) error {
	if observe == nil {
		return nil
	}
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil
	}
	count, err := shouldCountGroupedRewriteRecord(ptr, seenGroupedRecords)
	if err != nil || !count {
		return err
	}
	recordLen, err := db.valueLogRecordLengthForRewriteInSet(ptr, set)
	if err != nil {
		return err
	}
	return observe(ptr, recordLen)
}

func (db *DB) estimateValueLogLiveBytesByChunk(ctx context.Context, chunkBytes int64) (_ map[valueLogChunkKey]int64, err error) {
	if db == nil {
		return nil, fmt.Errorf("missing db")
	}
	chunkBytes = normalizeValueLogRewriteChunkBytes(chunkBytes)
	estimate := func() (_ map[valueLogChunkKey]int64, err error) {
		if ctx == nil {
			ctx = context.Background()
		}

		snap := db.AcquireSnapshot()
		if snap == nil || snap.state == nil || snap.idx == nil {
			closeRewriteSnapshot(&err, snap)
			return nil, fmt.Errorf("missing snapshot state")
		}
		defer closeRewriteSnapshot(&err, snap)

		liveByChunk := make(map[valueLogChunkKey]int64)
		var seenGroupedRecords map[groupedRecordKey]struct{}

		roots, err := maintenanceRootsForSnapshot(snap)
		if err != nil {
			return nil, err
		}
		roots = dedupeMaintenanceRootsByRootID(roots)
		for _, root := range roots {
			iter := tree.New(snap.idx.pager, &snap.reader, root.rootID).
				IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
			if err := db.collectValueLogLiveBytesByChunk(ctx, iter, liveByChunk, &seenGroupedRecords, snap.state.ValueLogSet, chunkBytes); err != nil {
				_ = iter.Close()
				return nil, err
			}
			_ = iter.Close()
		}

		return liveByChunk, nil
	}

	liveByChunk, err := estimate()
	if err != nil && errors.Is(err, valuelog.ErrFileNotFound) {
		if refreshErr := db.RefreshValueLogSet(); refreshErr != nil {
			return nil, refreshErr
		}
		return estimate()
	}
	return liveByChunk, err
}

func (db *DB) collectValueLogLiveBytesByChunk(ctx context.Context, it iterator.UnsafeIterator, liveByChunk map[valueLogChunkKey]int64, seenGroupedRecords *map[groupedRecordKey]struct{}, set *valuelog.Set, chunkBytes int64) error {
	for it.Valid() {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
			it.Next()
			continue
		}
		if err := db.collectValueLogPtrLiveBytes(ptr, seenGroupedRecords, set, func(ptr page.ValuePtr, recordLen uint32) error {
			chunkOffset, err := valueLogChunkOffsetForPtr(ptr, chunkBytes)
			if err != nil {
				return err
			}
			liveByChunk[valueLogChunkKey{fileID: ptr.FileID, chunkOffset: chunkOffset}] += int64(recordLen)
			return nil
		}); err != nil {
			return err
		}
		it.Next()
	}
	return it.Error()
}

func (db *DB) ValueLogRewriteChunkPlan(ctx context.Context, opts ValueLogRewriteOnlineOptions, chunkBytes int64) (ValueLogRewriteChunkPlan, error) {
	var plan ValueLogRewriteChunkPlan
	if db == nil {
		return plan, fmt.Errorf("missing db")
	}
	if db.valueLogManager == nil {
		return plan, fmt.Errorf("value log manager unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	chunkBytes = normalizeValueLogRewriteChunkBytes(chunkBytes)
	plan.ChunkBytes = chunkBytes
	if err := db.publishValueLogSetNoRefresh(); err != nil {
		return plan, err
	}

	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil || len(set.Files) == 0 {
		if set != nil {
			_ = db.valueLogManager.Release(set)
		}
		if err := db.valueLogManager.Refresh(); err != nil {
			return plan, err
		}
		set = db.valueLogManager.CurrentSetNoRefresh()
	}
	if set != nil {
		defer func() { _ = db.valueLogManager.Release(set) }()
	}
	if set == nil || len(set.Files) == 0 {
		return plan, nil
	}
	files := db.valueOnlyValueLogFiles(set.Files)
	if len(files) == 0 {
		return plan, nil
	}

	fileIDs := make([]uint32, 0, len(files))
	for id := range files {
		fileIDs = append(fileIDs, id)
	}
	sort.Slice(fileIDs, func(i, j int) bool { return fileIDs[i] < fileIDs[j] })
	for _, id := range fileIDs {
		size := fileSize(files[id])
		if size <= 0 {
			continue
		}
		plan.BytesTotal += size
		for chunkOffset := int64(0); chunkOffset < size; chunkOffset += chunkBytes {
			plan.ChunksTotal++
		}
	}

	var liveByChunk map[valueLogChunkKey]int64
	var err error
	if rewritePlanNeedsLiveEstimate(opts) {
		liveByChunk, err = db.estimateValueLogLiveBytesByChunk(ctx, chunkBytes)
		if err != nil {
			return plan, err
		}
		for _, id := range fileIDs {
			size := fileSize(files[id])
			if size <= 0 {
				continue
			}
			for chunkOffset := int64(0); chunkOffset < size; chunkOffset += chunkBytes {
				chunkTotal := chunkBytes
				if remaining := size - chunkOffset; remaining < chunkTotal {
					chunkTotal = remaining
				}
				live := liveByChunk[valueLogChunkKey{fileID: id, chunkOffset: chunkOffset}]
				if live < 0 {
					live = 0
				}
				if live > chunkTotal {
					live = chunkTotal
				}
				plan.BytesLive += live
				plan.BytesStale += chunkTotal - live
			}
		}
	}

	active := currentValueLogIDs(&valuelog.Set{Files: files})
	selectedChunks, selectionStats := selectRewriteSourceChunksWithStats(opts, files, active, liveByChunk, chunkBytes)
	plan.AgeBlockedChunks = selectionStats.ageBlockedSegments
	plan.AgeBlockedBytesTotal = selectionStats.ageBlockedBytesTotal
	plan.AgeBlockedBytesLive = selectionStats.ageBlockedBytesLive
	plan.AgeBlockedBytesStale = selectionStats.ageBlockedBytesStale
	plan.AgeBlockedMinRemainingAge = selectionStats.ageBlockedMinRemainingAge
	plan.SourceChunks = selectedChunks
	plan.ChunksSelected = len(selectedChunks)
	for _, chunk := range selectedChunks {
		plan.SelectedBytesTotal += chunk.BytesTotal
		plan.SelectedBytesLive += chunk.BytesLive
		plan.SelectedBytesStale += chunk.BytesStale
	}
	return plan, nil
}

func selectRewriteSourceChunksWithStats(opts ValueLogRewriteOnlineOptions, files map[uint32]*valuelog.File, active map[uint32]struct{}, liveByChunk map[valueLogChunkKey]int64, chunkBytes int64) ([]ValueLogRewritePlanChunk, rewriteSourceSelectionStats) {
	var stats rewriteSourceSelectionStats

	chunkBytes = normalizeValueLogRewriteChunkBytes(chunkBytes)
	minStaleRatio := normalizeStaleRatio(opts.MinSegmentStaleRatio)
	minStaleBytes := opts.MinSegmentStaleBytes
	maxSourceSegments := opts.MaxSourceSegments
	maxSourceBytes := opts.MaxSourceBytes
	minSegmentAge := opts.MinSegmentAge
	now := time.Now()
	protectedIDs := make(map[uint32]struct{})
	if len(opts.ProtectedPaths) > 0 && len(files) > 0 {
		protectedPaths := make(map[string]struct{}, len(opts.ProtectedPaths))
		for _, path := range opts.ProtectedPaths {
			if path == "" {
				continue
			}
			protectedPaths[path] = struct{}{}
		}
		for id, f := range files {
			if f == nil || f.Path == "" {
				continue
			}
			if _, ok := protectedPaths[f.Path]; ok {
				protectedIDs[id] = struct{}{}
			}
		}
		if recent := recentValueLogIDsForProtectedPaths(&valuelog.Set{Files: files}, valueLogKeepRecentSegmentsPerLane, opts.ProtectedPaths); len(recent) > 0 {
			for id := range recent {
				protectedIDs[id] = struct{}{}
			}
		}
	}

	candidateFileIDs := make([]uint32, 0, len(files))
	if len(opts.SourceFileIDs) > 0 {
		seen := make(map[uint32]struct{}, len(opts.SourceFileIDs))
		for _, id := range opts.SourceFileIDs {
			if _, ok := files[id]; !ok {
				continue
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			candidateFileIDs = append(candidateFileIDs, id)
		}
	} else {
		for id := range files {
			candidateFileIDs = append(candidateFileIDs, id)
		}
	}

	explicitSources := len(opts.SourceFileIDs) > 0
	candidates := make([]rewriteSourceChunk, 0, len(candidateFileIDs))
	for _, id := range candidateFileIDs {
		f := files[id]
		if f == nil {
			continue
		}
		if !explicitSources {
			if _, ok := active[id]; ok {
				continue
			}
			if _, ok := protectedIDs[id]; ok {
				continue
			}
		}
		size := fileSize(f)
		if size <= 0 {
			continue
		}

		tooYoung := false
		if minSegmentAge > 0 && f.Path != "" {
			if info, err := os.Stat(f.Path); err == nil {
				if age := now.Sub(info.ModTime()); age < minSegmentAge {
					remaining := minSegmentAge - age
					if remaining < 0 {
						remaining = 0
					}
					if stats.ageBlockedMinRemainingAge == 0 || remaining < stats.ageBlockedMinRemainingAge {
						stats.ageBlockedMinRemainingAge = remaining
					}
					tooYoung = true
				}
			}
		}

		for chunkOffset := int64(0); chunkOffset < size; chunkOffset += chunkBytes {
			chunkTotal := chunkBytes
			if remaining := size - chunkOffset; remaining < chunkTotal {
				chunkTotal = remaining
			}
			if chunkTotal <= 0 {
				continue
			}

			live := chunkTotal
			if liveByChunk != nil {
				live = liveByChunk[valueLogChunkKey{fileID: id, chunkOffset: chunkOffset}]
			}
			if live < 0 {
				live = 0
			}
			if live > chunkTotal {
				live = chunkTotal
			}
			stale := chunkTotal - live

			if tooYoung {
				stats.ageBlockedSegments++
				stats.ageBlockedBytesTotal += chunkTotal
				stats.ageBlockedBytesLive += live
				stats.ageBlockedBytesStale += stale
				continue
			}
			if live == 0 || stale <= 0 {
				continue
			}
			staleRatio := float64(stale) / float64(chunkTotal)
			if minStaleRatio > 0 && staleRatio < minStaleRatio {
				continue
			}
			if minStaleBytes > 0 && stale < minStaleBytes {
				continue
			}
			candidates = append(candidates, rewriteSourceChunk{
				fileID:      id,
				chunkOffset: chunkOffset,
				totalBytes:  chunkTotal,
				liveBytes:   live,
				staleBytes:  stale,
				staleRatio:  staleRatio,
			})
		}
	}

	if len(candidates) == 0 {
		return nil, stats
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
		if a.staleRatio != b.staleRatio {
			return a.staleRatio > b.staleRatio
		}
		if a.staleBytes != b.staleBytes {
			return a.staleBytes > b.staleBytes
		}
		if a.liveBytes != b.liveBytes {
			return a.liveBytes < b.liveBytes
		}
		if a.fileID != b.fileID {
			return a.fileID < b.fileID
		}
		return a.chunkOffset < b.chunkOffset
	})

	selected := make([]ValueLogRewritePlanChunk, 0, len(candidates))
	selectedBytes := int64(0)
	selectedIDs := make(map[uint32]struct{}, maxSourceSegments)
	for _, candidate := range candidates {
		if !explicitSources {
			if maxSourceBytes > 0 {
				next := selectedBytes + candidate.liveBytes
				if next > maxSourceBytes && len(selected) > 0 {
					continue
				}
			}
			if maxSourceSegments > 0 {
				if _, ok := selectedIDs[candidate.fileID]; !ok && len(selectedIDs) >= maxSourceSegments {
					continue
				}
			}
		}
		selected = append(selected, ValueLogRewritePlanChunk{
			FileID:      candidate.fileID,
			ChunkOffset: candidate.chunkOffset,
			BytesTotal:  candidate.totalBytes,
			BytesLive:   candidate.liveBytes,
			BytesStale:  candidate.staleBytes,
			StaleRatio:  candidate.staleRatio,
		})
		selectedBytes += candidate.liveBytes
		selectedIDs[candidate.fileID] = struct{}{}
	}
	return selected, stats
}

func hasExplicitRewriteChunks(opts ValueLogRewriteOnlineOptions) bool {
	return len(opts.SourceChunks) > 0
}

func buildExplicitRewriteSourceChunkSet(chunks []ValueLogRewritePlanChunk, files map[uint32]*valuelog.File, chunkBytes int64) (map[valueLogChunkKey]ValueLogRewritePlanChunk, map[uint32]struct{}, int64) {
	if len(chunks) == 0 || len(files) == 0 {
		return nil, nil, 0
	}
	chunkBytes = normalizeValueLogRewriteChunkBytes(chunkBytes)
	chunkSet := make(map[valueLogChunkKey]ValueLogRewritePlanChunk, len(chunks))
	sourceIDs := make(map[uint32]struct{}, len(chunks))
	for _, chunk := range chunks {
		if chunk.FileID == 0 {
			continue
		}
		f := files[chunk.FileID]
		if f == nil {
			continue
		}
		totalBytes := chunk.BytesTotal
		if totalBytes <= 0 {
			size := fileSize(f)
			if chunk.ChunkOffset < 0 || chunk.ChunkOffset >= size {
				continue
			}
			totalBytes = chunkBytes
			if remaining := size - chunk.ChunkOffset; remaining < totalBytes {
				totalBytes = remaining
			}
		}
		if totalBytes <= 0 {
			continue
		}
		normalized := ValueLogRewritePlanChunk{
			FileID:      chunk.FileID,
			ChunkOffset: chunk.ChunkOffset,
			BytesTotal:  totalBytes,
			BytesLive:   chunk.BytesLive,
			BytesStale:  chunk.BytesStale,
			StaleRatio:  chunk.StaleRatio,
		}
		key := valueLogChunkKey{fileID: normalized.FileID, chunkOffset: normalized.ChunkOffset}
		if _, dup := chunkSet[key]; dup {
			continue
		}
		chunkSet[key] = normalized
		sourceIDs[normalized.FileID] = struct{}{}
	}
	if len(chunkSet) == 0 {
		return nil, nil, 0
	}
	requestedBytes := int64(0)
	for id := range sourceIDs {
		f := files[id]
		if f == nil {
			continue
		}
		if size := fileSize(f); size > 0 {
			requestedBytes += size
		}
	}
	return chunkSet, sourceIDs, requestedBytes
}

func rewriteSourceChunkSelected(ptr page.ValuePtr, chunkSet map[valueLogChunkKey]ValueLogRewritePlanChunk, chunkBytes int64) (bool, error) {
	if len(chunkSet) == 0 {
		return true, nil
	}
	chunkOffset, err := valueLogChunkOffsetForPtr(ptr, chunkBytes)
	if err != nil {
		return false, err
	}
	_, ok := chunkSet[valueLogChunkKey{fileID: ptr.FileID, chunkOffset: chunkOffset}]
	return ok, nil
}
