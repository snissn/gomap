package caching

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
)

const valueLogGenerationStateFileName = "vlog_generation_state.json"

const vlogGenerationRewriteResumeMaxSegments = 1

const vlogGenerationRewriteResumeMinInterval = 1 * time.Second

type valueLogGenerationStateFile struct {
	RewriteSourceFileIDs []string `json:"rewrite_source_file_ids,omitempty"`
}

func (db *DB) valueLogGenerationStateRootDir() string {
	if db == nil || db.dir == "" {
		return ""
	}
	return filepath.Dir(db.dir)
}

func (db *DB) valueLogGenerationStatePath() string {
	root := db.valueLogGenerationStateRootDir()
	if root == "" {
		return ""
	}
	return filepath.Join(root, valueLogGenerationStateFileName)
}

func loadValueLogGenerationRewriteQueue(path string) ([]uint32, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var raw valueLogGenerationStateFile
	if len(data) > 0 {
		if err := json.Unmarshal(data, &raw); err != nil {
			// Rewrite queue state is rebuildable from the next maintenance plan, so
			// tolerate torn/corrupt JSON here.
			return nil, nil
		}
	}
	if len(raw.RewriteSourceFileIDs) == 0 {
		return nil, nil
	}
	out := make([]uint32, 0, len(raw.RewriteSourceFileIDs))
	for _, s := range raw.RewriteSourceFileIDs {
		id64, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			continue
		}
		out = append(out, uint32(id64))
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func saveValueLogGenerationRewriteQueue(path string, ids []uint32) error {
	if path == "" {
		return nil
	}
	if len(ids) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	raw := valueLogGenerationStateFile{RewriteSourceFileIDs: make([]string, 0, len(ids))}
	for _, id := range ids {
		raw.RewriteSourceFileIDs = append(raw.RewriteSourceFileIDs, strconv.FormatUint(uint64(id), 10))
	}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	return atomicfile.Write(path, data, 0o600)
}

func (db *DB) loadVlogGenerationRewriteQueueLocked() error {
	if db == nil {
		return nil
	}
	if db.vlogGenerationRewriteQueueLoaded {
		return nil
	}
	ids, err := loadValueLogGenerationRewriteQueue(db.valueLogGenerationStatePath())
	if err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = ids
	db.vlogGenerationRewriteQueueLoaded = true
	return nil
}

func (db *DB) setVlogGenerationRewriteQueue(ids []uint32) error {
	if db == nil {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	next := append([]uint32(nil), ids...)
	if err := saveValueLogGenerationRewriteQueue(db.valueLogGenerationStatePath(), next); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = next
	return nil
}

func (db *DB) currentVlogGenerationRewriteQueue() ([]uint32, error) {
	if db == nil {
		return nil, nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return nil, err
	}
	return append([]uint32(nil), db.vlogGenerationRewriteQueue...), nil
}

func vlogGenerationRewriteQueueChunk(ids []uint32, maxSegments int) []uint32 {
	if len(ids) == 0 || maxSegments <= 0 {
		return nil
	}
	if len(ids) > maxSegments {
		ids = ids[:maxSegments]
	}
	return append([]uint32(nil), ids...)
}

func (db *DB) consumeVlogGenerationRewriteQueueChunk(processed []uint32) error {
	if db == nil || len(processed) == 0 {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	remaining := db.vlogGenerationRewriteQueue
	if len(remaining) >= len(processed) {
		match := true
		for i := range processed {
			if remaining[i] != processed[i] {
				match = false
				break
			}
		}
		if match {
			remaining = append([]uint32(nil), remaining[len(processed):]...)
		} else {
			processedSet := make(map[uint32]struct{}, len(processed))
			for _, id := range processed {
				processedSet[id] = struct{}{}
			}
			filtered := make([]uint32, 0, len(remaining))
			for _, id := range remaining {
				if _, ok := processedSet[id]; ok {
					continue
				}
				filtered = append(filtered, id)
			}
			remaining = filtered
		}
	}
	if err := saveValueLogGenerationRewriteQueue(db.valueLogGenerationStatePath(), remaining); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = remaining
	return nil
}
