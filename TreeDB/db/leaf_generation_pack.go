package db

import (
	"context"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type LeafGenerationPackOptions struct {
	GenerationIDs []uint64
	Sync          bool
}

type LeafGenerationPackStats struct {
	GenerationsRequested int
	GenerationsMatched   int
	SourceFilesRequested int
	SourceFileIDs        []uint32
	LeafPagesCopied      int
	BytesCopied          int64
	CreatedFileIDs       []uint32
}

// LeafGenerationPack rewrites live LeafRef pages from sealed source generations
// into a fresh leaf-log output so the old generations can later be reclaimed by
// ordinary generation GC.
func (db *DB) LeafGenerationPack(ctx context.Context, opts LeafGenerationPackOptions) (LeafGenerationPackStats, error) {
	var stats LeafGenerationPackStats
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return stats, ErrReadOnly
	}
	if !db.indexOuterLeavesInValueLog {
		return stats, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stats.GenerationsRequested = len(opts.GenerationIDs)

	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	rawSourceIDs, matchedGenerations, err := db.resolveLeafGenerationPackSourceFileIDs(opts.GenerationIDs)
	if err != nil {
		return stats, err
	}
	stats.GenerationsMatched = matchedGenerations
	stats.SourceFileIDs = append(stats.SourceFileIDs, rawSourceIDs...)
	stats.SourceFilesRequested = len(rawSourceIDs)
	if len(rawSourceIDs) == 0 {
		return stats, nil
	}
	if err := db.publishValueLogSetNoRefresh(); err != nil {
		return stats, err
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil || len(set.Files) == 0 {
		if set != nil {
			_ = db.valueLogManager.Release(set)
		}
		if err := db.valueLogManager.Refresh(); err != nil {
			return stats, err
		}
		set = db.valueLogManager.CurrentSetNoRefresh()
	}
	if set == nil {
		return stats, fmt.Errorf("leaf generation pack: value-log set unavailable")
	}
	leafStartSeq := maxRewriteLaneSeqFromSet(set, rewriteLeafLogLaneID)
	nextRID, err := nextRewriteRIDStartFromSet(set)
	_ = db.valueLogManager.Release(set)
	if err != nil {
		return stats, err
	}

	layout := resolveStorageLayout(db.dir)
	writer := newRewriteWriter(layout.valueVLogDir, 0, 0, 0)
	writer.ConfigureLeafLog(layout.leafVLogDir, rewriteLeafLogLaneID, leafStartSeq)
	writer.blockCompression = db.valueLogCompression != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(db.valueLogBlockCodec)
	defer func() { _ = writer.Close() }()

	sourceValueIDs := make(map[uint32]struct{}, len(rawSourceIDs))
	for _, rawID := range rawSourceIDs {
		sourceValueIDs[page.ValueLogFileID(rawID)] = struct{}{}
	}
	ridAlloc := newRewriteRIDAllocator(nextRID, nil)
	stats.LeafPagesCopied, stats.BytesCopied, err = db.rewriteLeafRefsOnline(ctx, writer, ridAlloc, sourceValueIDs, nil, 0, 0, false, 0, opts.Sync)
	if err != nil {
		return stats, err
	}
	createdIDs, err := writer.createdFileIDs()
	if err != nil {
		return stats, err
	}
	stats.CreatedFileIDs = filterLeafGenerationRawFileIDs(createdIDs)
	return stats, nil
}

func (db *DB) resolveLeafGenerationPackSourceFileIDs(generationIDs []uint64) ([]uint32, int, error) {
	if len(generationIDs) == 0 {
		return nil, 0, nil
	}
	db.writeMu.RLock()
	manifest := db.leafGenerationManifest.clone()
	db.writeMu.RUnlock()
	if manifest == nil {
		return nil, 0, fmt.Errorf("leaf generation pack: manifest unavailable")
	}

	generationByID := make(map[uint64]leafGenerationRecord, len(manifest.Generations))
	for _, gen := range manifest.Generations {
		generationByID[gen.GenerationID] = gen
	}
	seenGenerations := make(map[uint64]struct{}, len(generationIDs))
	seenFiles := make(map[uint32]struct{}, len(generationIDs))
	out := make([]uint32, 0, len(generationIDs))
	matched := 0
	for _, generationID := range generationIDs {
		if generationID == 0 {
			continue
		}
		if _, seen := seenGenerations[generationID]; seen {
			continue
		}
		seenGenerations[generationID] = struct{}{}
		gen, ok := generationByID[generationID]
		if !ok {
			return nil, 0, fmt.Errorf("leaf generation pack: generation %d not found", generationID)
		}
		if gen.State != leafGenerationStateSealed {
			return nil, 0, fmt.Errorf("leaf generation pack: generation %d state=%q, want %q", generationID, gen.State, leafGenerationStateSealed)
		}
		matched++
		for _, rawID := range gen.FileIDs {
			if rawID == 0 {
				continue
			}
			if _, seen := seenFiles[rawID]; seen {
				continue
			}
			seenFiles[rawID] = struct{}{}
			out = append(out, rawID)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out, matched, nil
}

func (db *DB) noteCreatedLeafGenerationFileIDs(commitSeq uint64, fileIDs []uint32) error {
	if db == nil || db.leafGenerationManifest == nil || commitSeq == 0 || len(fileIDs) == 0 {
		return nil
	}
	changed := false
	for _, fileID := range fileIDs {
		if !page.IsValueLogFileID(fileID) {
			continue
		}
		lane, _ := valuelog.DecodeFileID(fileID)
		if lane != rewriteLeafLogLaneID {
			continue
		}
		registered, err := db.leafGenerationManifest.registerCurrentGenerationFileID(page.ValueLogSegmentID(fileID), commitSeq)
		if err != nil {
			return err
		}
		if registered {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return saveLeafGenerationManifest(LeafLogDirPath(db.dir), db.leafGenerationManifest)
}

func filterLeafGenerationRawFileIDs(fileIDs []uint32) []uint32 {
	if len(fileIDs) == 0 {
		return nil
	}
	out := make([]uint32, 0, len(fileIDs))
	seen := make(map[uint32]struct{}, len(fileIDs))
	for _, fileID := range fileIDs {
		if !page.IsValueLogFileID(fileID) {
			continue
		}
		lane, _ := valuelog.DecodeFileID(fileID)
		if lane != rewriteLeafLogLaneID {
			continue
		}
		rawID := page.ValueLogSegmentID(fileID)
		if rawID == 0 {
			continue
		}
		if _, ok := seen[rawID]; ok {
			continue
		}
		seen[rawID] = struct{}{}
		out = append(out, rawID)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}
