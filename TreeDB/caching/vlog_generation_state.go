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

const vlogGenerationGCResumeMaxSegments = 1

type valueLogGenerationStateFile struct {
	RewriteSourceFileIDs []string `json:"rewrite_source_file_ids,omitempty"`
	GCSourceFileIDs      []string `json:"gc_source_file_ids,omitempty"`
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

func loadValueLogGenerationQueue(path string, selector func(*valueLogGenerationStateFile) []string) ([]uint32, error) {
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
			// Queue state is rebuildable from the next maintenance plan, so tolerate
			// torn/corrupt JSON here.
			return nil, nil
		}
	}
	encoded := selector(&raw)
	if len(encoded) == 0 {
		return nil, nil
	}
	out := make([]uint32, 0, len(encoded))
	for _, s := range encoded {
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

func loadValueLogGenerationRewriteQueue(path string) ([]uint32, error) {
	return loadValueLogGenerationQueue(path, func(raw *valueLogGenerationStateFile) []string {
		return raw.RewriteSourceFileIDs
	})
}

func loadValueLogGenerationGCQueue(path string) ([]uint32, error) {
	return loadValueLogGenerationQueue(path, func(raw *valueLogGenerationStateFile) []string {
		return raw.GCSourceFileIDs
	})
}

func saveValueLogGenerationQueue(path string, ids []uint32, apply func(*valueLogGenerationStateFile, []string)) error {
	if path == "" {
		return nil
	}
	var raw valueLogGenerationStateFile
	if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
		if err := json.Unmarshal(data, &raw); err != nil {
			raw = valueLogGenerationStateFile{}
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(ids) == 0 {
		apply(&raw, nil)
		if len(raw.RewriteSourceFileIDs) == 0 && len(raw.GCSourceFileIDs) == 0 {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			return nil
		}
	} else {
		encoded := make([]string, 0, len(ids))
		for _, id := range ids {
			encoded = append(encoded, strconv.FormatUint(uint64(id), 10))
		}
		apply(&raw, encoded)
	}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	return atomicfile.Write(path, data, 0o600)
}

func saveValueLogGenerationRewriteQueue(path string, ids []uint32) error {
	return saveValueLogGenerationQueue(path, ids, func(raw *valueLogGenerationStateFile, encoded []string) {
		raw.RewriteSourceFileIDs = encoded
	})
}

func saveValueLogGenerationGCQueue(path string, ids []uint32) error {
	return saveValueLogGenerationQueue(path, ids, func(raw *valueLogGenerationStateFile, encoded []string) {
		raw.GCSourceFileIDs = encoded
	})
}

func (db *DB) loadVlogGenerationQueuesLocked() error {
	if db == nil {
		return nil
	}
	if db.vlogGenerationQueuesLoaded {
		return nil
	}
	rewriteIDs, err := loadValueLogGenerationRewriteQueue(db.valueLogGenerationStatePath())
	if err != nil {
		return err
	}
	gcIDs, err := loadValueLogGenerationGCQueue(db.valueLogGenerationStatePath())
	if err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = rewriteIDs
	db.vlogGenerationGCQueue = gcIDs
	db.vlogGenerationQueuesLoaded = true
	return nil
}

func (db *DB) setVlogGenerationRewriteQueue(ids []uint32) error {
	if db == nil {
		return nil
	}
	db.vlogGenerationQueueMu.Lock()
	defer db.vlogGenerationQueueMu.Unlock()
	if err := db.loadVlogGenerationQueuesLocked(); err != nil {
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
	db.vlogGenerationQueueMu.Lock()
	defer db.vlogGenerationQueueMu.Unlock()
	if err := db.loadVlogGenerationQueuesLocked(); err != nil {
		return nil, err
	}
	return append([]uint32(nil), db.vlogGenerationRewriteQueue...), nil
}

func (db *DB) setVlogGenerationGCQueue(ids []uint32) error {
	if db == nil {
		return nil
	}
	db.vlogGenerationQueueMu.Lock()
	defer db.vlogGenerationQueueMu.Unlock()
	if err := db.loadVlogGenerationQueuesLocked(); err != nil {
		return err
	}
	next := append([]uint32(nil), ids...)
	if err := saveValueLogGenerationGCQueue(db.valueLogGenerationStatePath(), next); err != nil {
		return err
	}
	db.vlogGenerationGCQueue = next
	return nil
}

func (db *DB) currentVlogGenerationGCQueue() ([]uint32, error) {
	if db == nil {
		return nil, nil
	}
	db.vlogGenerationQueueMu.Lock()
	defer db.vlogGenerationQueueMu.Unlock()
	if err := db.loadVlogGenerationQueuesLocked(); err != nil {
		return nil, err
	}
	return append([]uint32(nil), db.vlogGenerationGCQueue...), nil
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
	db.vlogGenerationQueueMu.Lock()
	defer db.vlogGenerationQueueMu.Unlock()
	if err := db.loadVlogGenerationQueuesLocked(); err != nil {
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

func vlogGenerationGCQueueChunk(ids []uint32, maxSegments int) []uint32 {
	if len(ids) == 0 || maxSegments <= 0 {
		return nil
	}
	if len(ids) > maxSegments {
		ids = ids[:maxSegments]
	}
	return append([]uint32(nil), ids...)
}

func (db *DB) consumeVlogGenerationGCQueueChunk(processed []uint32) error {
	if db == nil || len(processed) == 0 {
		return nil
	}
	db.vlogGenerationQueueMu.Lock()
	defer db.vlogGenerationQueueMu.Unlock()
	if err := db.loadVlogGenerationQueuesLocked(); err != nil {
		return err
	}
	remaining := db.vlogGenerationGCQueue
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
	if err := saveValueLogGenerationGCQueue(db.valueLogGenerationStatePath(), remaining); err != nil {
		return err
	}
	db.vlogGenerationGCQueue = remaining
	return nil
}
