package db

import (
	"fmt"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafGenerationMetrics struct {
	enabled                       bool
	currentGenerationID           uint64
	generationsTotal              int
	generationsWritable           int
	generationsSealed             int
	generationsRetiring           int
	generationsDeleted            int
	generationsRetained           int
	generationsPinned             int
	pinsTotal                     uint64
	filesTotal                    int
	filesWritable                 int
	filesSealed                   int
	filesRetiring                 int
	filesDeleted                  int
	filesRetained                 int
	bytesTotal                    int64
	bytesWritable                 int64
	bytesSealed                   int64
	bytesRetiring                 int64
	bytesDeleted                  int64
	bytesRetained                 int64
	currentFiles                  int
	currentBytes                  int64
	reachabilitySubtreeCachePages int
}

func (db *DB) collectLeafGenerationMetrics(set *valuelog.Set, excludePinIDs []uint64) leafGenerationMetrics {
	var m leafGenerationMetrics
	if db == nil || !db.indexOuterLeavesInValueLog {
		return m
	}
	m.enabled = true

	// Manifest publication is serialized by writeMu in maintenance paths and
	// by mu in post-commit paths. Take both in the documented lock order while
	// cloning so metrics collection is synchronized with either publisher.
	db.writeMu.RLock()
	db.mu.RLock()
	manifest := db.leafGenerationManifest.clone()
	db.mu.RUnlock()
	db.writeMu.RUnlock()
	if manifest == nil {
		return m
	}
	m.currentGenerationID = manifest.CurrentGenerationID
	db.leafGenerationSubtreeStatsMu.RLock()
	m.reachabilitySubtreeCachePages = len(db.leafGenerationSubtreeStatsByPage)
	db.leafGenerationSubtreeStatsMu.RUnlock()

	var excludedPins map[uint64]struct{}
	if len(excludePinIDs) > 0 {
		excludedPins = make(map[uint64]struct{}, len(excludePinIDs))
		for _, id := range excludePinIDs {
			if id != 0 {
				excludedPins[id] = struct{}{}
			}
		}
	}

	for _, gen := range manifest.Generations {
		genFiles := len(gen.FileIDs)
		genBytes := int64(0)
		for _, rawFileID := range gen.FileIDs {
			genBytes += leafGenerationRawFileSizeBestEffort(db.dir, set, rawFileID)
		}
		m.generationsTotal++
		m.filesTotal += genFiles
		m.bytesTotal += genBytes

		if gen.GenerationID == manifest.CurrentGenerationID {
			m.currentFiles += genFiles
			m.currentBytes += genBytes
		}

		switch gen.State {
		case leafGenerationStateWritable:
			m.generationsWritable++
			m.filesWritable += genFiles
			m.bytesWritable += genBytes
		case leafGenerationStateSealed:
			m.generationsSealed++
			m.generationsRetained++
			m.filesSealed += genFiles
			m.filesRetained += genFiles
			m.bytesSealed += genBytes
			m.bytesRetained += genBytes
		case leafGenerationStateRetiring:
			m.generationsRetiring++
			m.generationsRetained++
			m.filesRetiring += genFiles
			m.filesRetained += genFiles
			m.bytesRetiring += genBytes
			m.bytesRetained += genBytes
		case leafGenerationStateDeleted:
			m.generationsDeleted++
			m.filesDeleted += genFiles
			m.bytesDeleted += genBytes
		}
		if pins := db.leafGenerationPins.count(gen.GenerationID); pins > 0 {
			if _, ok := excludedPins[gen.GenerationID]; ok && pins > 0 {
				pins--
			}
			if pins > 0 {
				m.generationsPinned++
				m.pinsTotal += pins
			}
		}
	}
	return m
}

func leafGenerationRawFileSizeBestEffort(rootDir string, set *valuelog.Set, rawFileID uint32) int64 {
	if rawFileID == 0 {
		return 0
	}
	// Stats are often read as lightweight telemetry. Preserve the cached-first
	// path here; maintenance planning uses leafGenerationRawFilePhysicalSize.
	cached, path := leafGenerationRawFileCachedSizeAndPath(set, rawFileID)
	if cached > 0 {
		return cached
	}
	return leafGenerationRawFileSizeFromPath(rootDir, rawFileID, path, 0)
}

func leafGenerationRawFilePhysicalSize(rootDir string, set *valuelog.Set, rawFileID uint32) int64 {
	if rawFileID == 0 {
		return 0
	}
	cached, path := leafGenerationRawFileCachedSizeAndPath(set, rawFileID)
	return leafGenerationRawFileSizeFromPath(rootDir, rawFileID, path, cached)
}

func leafGenerationRawFileCachedSizeAndPath(set *valuelog.Set, rawFileID uint32) (int64, string) {
	if set == nil || rawFileID == 0 {
		return 0, ""
	}
	f := set.Files[page.ValueLogFileID(rawFileID)]
	if f == nil {
		return 0, ""
	}
	return fileSize(f), f.Path
}

func leafGenerationRawFileSizeFromPath(rootDir string, rawFileID uint32, path string, fallback int64) int64 {
	if path == "" {
		path = leafGenerationFallbackPath(rootDir, rawFileID)
	}
	if path == "" {
		return fallback
	}
	info, err := os.Stat(path)
	if err != nil {
		return fallback
	}
	return info.Size()
}

func writeLeafGenerationMetrics(stats map[string]string, m leafGenerationMetrics) {
	if stats == nil {
		return
	}
	stats["treedb.leaf_generation.enabled"] = fmt.Sprintf("%t", m.enabled)
	stats["treedb.leaf_generation.current_generation_id"] = fmt.Sprintf("%d", m.currentGenerationID)
	stats["treedb.leaf_generation.generations.total"] = fmt.Sprintf("%d", m.generationsTotal)
	stats["treedb.leaf_generation.generations.writable"] = fmt.Sprintf("%d", m.generationsWritable)
	stats["treedb.leaf_generation.generations.sealed"] = fmt.Sprintf("%d", m.generationsSealed)
	stats["treedb.leaf_generation.generations.retiring"] = fmt.Sprintf("%d", m.generationsRetiring)
	stats["treedb.leaf_generation.generations.deleted"] = fmt.Sprintf("%d", m.generationsDeleted)
	stats["treedb.leaf_generation.generations.retained"] = fmt.Sprintf("%d", m.generationsRetained)
	stats["treedb.leaf_generation.generations.pinned"] = fmt.Sprintf("%d", m.generationsPinned)
	stats["treedb.leaf_generation.pins.total"] = fmt.Sprintf("%d", m.pinsTotal)
	stats["treedb.leaf_generation.files.total"] = fmt.Sprintf("%d", m.filesTotal)
	stats["treedb.leaf_generation.files.writable"] = fmt.Sprintf("%d", m.filesWritable)
	stats["treedb.leaf_generation.files.sealed"] = fmt.Sprintf("%d", m.filesSealed)
	stats["treedb.leaf_generation.files.retiring"] = fmt.Sprintf("%d", m.filesRetiring)
	stats["treedb.leaf_generation.files.deleted"] = fmt.Sprintf("%d", m.filesDeleted)
	stats["treedb.leaf_generation.files.retained"] = fmt.Sprintf("%d", m.filesRetained)
	stats["treedb.leaf_generation.bytes.total"] = fmt.Sprintf("%d", m.bytesTotal)
	stats["treedb.leaf_generation.bytes.writable"] = fmt.Sprintf("%d", m.bytesWritable)
	stats["treedb.leaf_generation.bytes.sealed"] = fmt.Sprintf("%d", m.bytesSealed)
	stats["treedb.leaf_generation.bytes.retiring"] = fmt.Sprintf("%d", m.bytesRetiring)
	stats["treedb.leaf_generation.bytes.deleted"] = fmt.Sprintf("%d", m.bytesDeleted)
	stats["treedb.leaf_generation.bytes.retained"] = fmt.Sprintf("%d", m.bytesRetained)
	stats["treedb.leaf_generation.current.files"] = fmt.Sprintf("%d", m.currentFiles)
	stats["treedb.leaf_generation.current.bytes"] = fmt.Sprintf("%d", m.currentBytes)
	stats["treedb.leaf_generation.plan_cache.subtree_pages"] = fmt.Sprintf("%d", m.reachabilitySubtreeCachePages)
}
