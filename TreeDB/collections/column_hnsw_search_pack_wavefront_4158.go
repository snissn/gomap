package collections

import (
	"context"
	"fmt"
)

func (v *columnHNSWSearchPackPreparedView) searchLayer0Wavefront4158(
	ctx context.Context,
	normalizedQuery []float32,
	scoreMode columnVectorGraphScoreBatchMode,
	scratch *columnVectorGraphNativeSearchScratch,
	stats *columnVectorGraphNativeSearchStats,
	trace *columnHNSWSearchPackAttributionTrace,
	width int,
	limit uint64,
	rowCount int,
	visited, edges *uint64,
	nextSeed *int,
	countEdges bool,
) (string, error) {
	visitMarks, visitEpoch := scratch.visitMarks, scratch.visitEpoch
	for *visited < limit {
		scratch.wavefrontCandidates = ensureColumnVectorGraphNativeCandidateScratch(scratch.wavefrontCandidates, width)
		wave := scratch.wavefrontCandidates[:0]
		for len(wave) < width {
			candidate, ok := scratch.popFrontierAccounting(stats)
			if !ok {
				if len(wave) != 0 || len(scratch.top) >= cap(scratch.top) {
					break
				}
				seed, seedOK, err := columnHNSWSearchPackNextCandidateSeedWithContext(ctx, *nextSeed, rowCount, visitMarks, visitEpoch)
				if err != nil {
					return "", err
				}
				if !seedOK {
					break
				}
				*nextSeed = seed + 1
				visitMarks[seed] = visitEpoch
				if trace != nil {
					trace.ScoreOrdinals = append(trace.ScoreOrdinals, uint32(seed))
				}
				if err := v.scoreAndPushFrontierVisited(normalizedQuery, seed, cap(scratch.top), scoreMode, scratch, stats, visited); err != nil {
					return "", err
				}
				continue
			}
			wave = append(wave, candidate)
		}
		if len(wave) == 0 {
			if *visited >= limit {
				return "candidate_limit", nil
			}
			return "frontier_empty", nil
		}

		remaining := int(limit - *visited)
		scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, remaining)
		tile := scratch.scoreTileRowIDs[:0]
		for _, candidate := range wave {
			if trace != nil {
				trace.AdjacencyReads = append(trace.AdjacencyReads, columnHNSWSearchPackPageRead{Layer: 0, Ordinal: candidate.ordinal})
			}
			adjacency, err := v.adjacencyLayerForOrdinal(candidate.ordinal, 0, stats)
			if err != nil {
				return "", err
			}
			for i, neighbor := range adjacency {
				if countEdges {
					*edges++
				}
				if uint64(neighbor) >= uint64(rowCount) {
					return "", fmt.Errorf("collections: hnsw_search_pack_v1 ordinal=%d adjacency[%d]=%d outside row_count=%d: %w", candidate.ordinal, i, neighbor, rowCount, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
				}
				if visitMarks[neighbor] == visitEpoch {
					continue
				}
				visitMarks[neighbor] = visitEpoch
				tile = append(tile, neighbor)
				if len(tile) == remaining {
					break
				}
			}
			if len(tile) == remaining {
				break
			}
		}
		stats.WavefrontRounds++
		stats.WavefrontCandidatePops += uint64(len(wave))
		stats.WavefrontStagedNeighbors += uint64(len(tile))
		if uint64(len(tile)) > stats.WavefrontMaxTileSize {
			stats.WavefrontMaxTileSize = uint64(len(tile))
		}
		if trace != nil {
			trace.ScoreOrdinals = append(trace.ScoreOrdinals, tile...)
		}
		if err := v.scoreAndPushFrontierVisitedTile(normalizedQuery, tile, cap(scratch.top), scoreMode, scratch, stats, visited); err != nil {
			return "", err
		}

		remaining = int(limit - *visited)
		if remaining == 0 || !v.Header.HasAuxiliaryNavigation {
			continue
		}
		scratch.scoreTileRowIDs = ensureColumnVectorGraphNativeUint32Scratch(scratch.scoreTileRowIDs, remaining)
		tile = scratch.scoreTileRowIDs[:0]
		for _, candidate := range wave {
			if trace != nil {
				trace.AdjacencyReads = append(trace.AdjacencyReads, columnHNSWSearchPackPageRead{Ordinal: candidate.ordinal, Auxiliary: true})
			}
			start, end := v.AuxiliaryNavigation.Offsets[candidate.ordinal], v.AuxiliaryNavigation.Offsets[candidate.ordinal+1]
			for _, neighbor := range v.AuxiliaryNavigation.Neighbors[start:end] {
				if countEdges {
					stats.AuxiliaryEdges++
				}
				if uint64(neighbor) >= uint64(rowCount) {
					return "", errColumnVectorGraphAdjacencyOrdinalOutOfBounds
				}
				if visitMarks[neighbor] == visitEpoch {
					continue
				}
				visitMarks[neighbor] = visitEpoch
				tile = append(tile, neighbor)
				if len(tile) == remaining {
					break
				}
			}
			if len(tile) == remaining {
				break
			}
		}
		if trace != nil {
			trace.ScoreOrdinals = append(trace.ScoreOrdinals, tile...)
		}
		beforeCandidates, beforeFrontier := *visited, len(scratch.frontier)
		if err := v.scoreAndPushFrontierVisitedTile(normalizedQuery, tile, cap(scratch.top), scoreMode, scratch, stats, visited); err != nil {
			return "", err
		}
		stats.AuxiliaryCandidates += *visited - beforeCandidates
		if len(scratch.frontier) > beforeFrontier {
			stats.AuxiliaryAdmissions += uint64(len(scratch.frontier) - beforeFrontier)
		}
	}
	return "candidate_limit", nil
}
