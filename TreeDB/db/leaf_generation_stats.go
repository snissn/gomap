package db

import (
	"fmt"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafGenerationMetrics struct {
	enabled             bool
	currentGenerationID uint64
	generationsTotal    int
	generationsWritable int
	generationsSealed   int
	generationsRetiring int
	generationsDeleted  int
	generationsRetained int
	generationsPinned   int
	pinsTotal           uint64
	filesTotal          int
	filesWritable       int
	filesSealed         int
	filesRetiring       int
	filesDeleted        int
	filesRetained       int
	bytesTotal          int64
	bytesWritable       int64
	bytesSealed         int64
	bytesRetiring       int64
	bytesDeleted        int64
	bytesRetained       int64
	currentFiles        int
	currentBytes        int64
}

func (db *DB) collectLeafGenerationMetrics(set *valuelog.Set) leafGenerationMetrics {
	var m leafGenerationMetrics
	if db == nil || !db.indexOuterLeavesInValueLog {
		return m
	}
	m.enabled = true

	db.writeMu.RLock()
	manifest := db.leafGenerationManifest.clone()
	db.writeMu.RUnlock()
	if manifest == nil {
		return m
	}
	m.currentGenerationID = manifest.CurrentGenerationID

	for _, gen := range manifest.Generations {
		genFiles := len(gen.FileIDs)
		genBytes := int64(0)
		for _, rawFileID := range gen.FileIDs {
			genBytes += leafGenerationRawFileSize(db.dir, set, rawFileID)
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
			m.generationsPinned++
			m.pinsTotal += pins
		}
	}
	return m
}

func leafGenerationRawFileSize(rootDir string, set *valuelog.Set, rawFileID uint32) int64 {
	if rawFileID == 0 {
		return 0
	}
	if set != nil {
		if f := set.Files[page.ValueLogFileID(rawFileID)]; f != nil {
			if size := fileSize(f); size > 0 {
				return size
			}
		}
	}
	path := leafGenerationFallbackPath(rootDir, rawFileID)
	if path == "" {
		return 0
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0
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
}
