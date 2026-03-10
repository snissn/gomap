package caching

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
)

const valueLogGenerationStateFileName = "vlog_generation_state.json"
const valueLogGenerationStateVersion = 1

const vlogGenerationRewriteResumeMinInterval = 1 * time.Second

type valueLogGenerationRewriteQueueEntry struct {
	FileID       uint32 `json:"file_id"`
	EstLiveBytes int64  `json:"est_live_bytes,omitempty"`
}

type valueLogGenerationStateFile struct {
	Version              int                                   `json:"version,omitempty"`
	RewriteQueue         []valueLogGenerationRewriteQueueEntry `json:"rewrite_queue,omitempty"`
	RewriteSourceFileIDs []string                              `json:"rewrite_source_file_ids,omitempty"`
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

func loadValueLogGenerationRewriteQueue(path string) ([]valueLogGenerationRewriteQueueEntry, error) {
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
	if len(raw.RewriteQueue) > 0 {
		out := make([]valueLogGenerationRewriteQueueEntry, 0, len(raw.RewriteQueue))
		for _, entry := range raw.RewriteQueue {
			if entry.FileID == 0 {
				continue
			}
			if entry.EstLiveBytes < 0 {
				entry.EstLiveBytes = 0
			}
			out = append(out, entry)
		}
		if len(out) == 0 {
			return nil, nil
		}
		return out, nil
	}
	if len(raw.RewriteSourceFileIDs) == 0 {
		return nil, nil
	}
	out := make([]valueLogGenerationRewriteQueueEntry, 0, len(raw.RewriteSourceFileIDs))
	for _, s := range raw.RewriteSourceFileIDs {
		id64, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			continue
		}
		out = append(out, valueLogGenerationRewriteQueueEntry{FileID: uint32(id64)})
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func saveValueLogGenerationRewriteQueue(path string, entries []valueLogGenerationRewriteQueueEntry) error {
	if path == "" {
		return nil
	}
	if len(entries) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	raw := valueLogGenerationStateFile{
		Version:      valueLogGenerationStateVersion,
		RewriteQueue: make([]valueLogGenerationRewriteQueueEntry, 0, len(entries)),
	}
	for _, entry := range entries {
		if entry.FileID == 0 {
			continue
		}
		if entry.EstLiveBytes < 0 {
			entry.EstLiveBytes = 0
		}
		raw.RewriteQueue = append(raw.RewriteQueue, entry)
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

func (db *DB) setVlogGenerationRewriteQueue(entries []valueLogGenerationRewriteQueueEntry) error {
	if db == nil {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	next := append([]valueLogGenerationRewriteQueueEntry(nil), entries...)
	if err := saveValueLogGenerationRewriteQueue(db.valueLogGenerationStatePath(), next); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = next
	return nil
}

func (db *DB) currentVlogGenerationRewriteQueue() ([]valueLogGenerationRewriteQueueEntry, error) {
	if db == nil {
		return nil, nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return nil, err
	}
	return append([]valueLogGenerationRewriteQueueEntry(nil), db.vlogGenerationRewriteQueue...), nil
}

func valueLogGenerationRewriteQueueEntriesFromPlan(plan backenddb.ValueLogRewritePlan) []valueLogGenerationRewriteQueueEntry {
	if len(plan.SelectedSources) > 0 {
		out := make([]valueLogGenerationRewriteQueueEntry, 0, len(plan.SelectedSources))
		for _, source := range plan.SelectedSources {
			if source.FileID == 0 {
				continue
			}
			est := source.EstimatedLiveBytes
			if est < 0 {
				est = 0
			}
			out = append(out, valueLogGenerationRewriteQueueEntry{
				FileID:       source.FileID,
				EstLiveBytes: est,
			})
		}
		if len(out) > 0 {
			return out
		}
	}
	out := make([]valueLogGenerationRewriteQueueEntry, 0, len(plan.SourceFileIDs))
	for _, id := range plan.SourceFileIDs {
		if id == 0 {
			continue
		}
		out = append(out, valueLogGenerationRewriteQueueEntry{FileID: id})
	}
	return out
}

func valueLogGenerationRewriteQueueIDs(entries []valueLogGenerationRewriteQueueEntry) []uint32 {
	if len(entries) == 0 {
		return nil
	}
	out := make([]uint32, 0, len(entries))
	for _, entry := range entries {
		if entry.FileID == 0 {
			continue
		}
		out = append(out, entry.FileID)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func vlogGenerationRewriteQueueChunk(entries []valueLogGenerationRewriteQueueEntry, maxBytes int64) []valueLogGenerationRewriteQueueEntry {
	if len(entries) == 0 || maxBytes <= 0 {
		return nil
	}
	chunk := make([]valueLogGenerationRewriteQueueEntry, 0, len(entries))
	var used int64
	for i, entry := range entries {
		if entry.FileID == 0 {
			continue
		}
		est := entry.EstLiveBytes
		if est < 0 {
			est = 0
		}
		if est == 0 {
			chunk = append(chunk, entry)
			break
		}
		if i > 0 {
			if used+est > maxBytes {
				break
			}
		}
		chunk = append(chunk, entry)
		used += est
	}
	return chunk
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
			if remaining[i].FileID != processed[i] {
				match = false
				break
			}
		}
		if match {
			remaining = append([]valueLogGenerationRewriteQueueEntry(nil), remaining[len(processed):]...)
		} else {
			processedSet := make(map[uint32]struct{}, len(processed))
			for _, id := range processed {
				processedSet[id] = struct{}{}
			}
			filtered := make([]valueLogGenerationRewriteQueueEntry, 0, len(remaining))
			for _, entry := range remaining {
				if _, ok := processedSet[entry.FileID]; ok {
					continue
				}
				filtered = append(filtered, entry)
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

func valueLogGenerationRewriteQueueSelectedLiveBytes(entries []valueLogGenerationRewriteQueueEntry, selectedIDs []uint32) int64 {
	if len(entries) == 0 || len(selectedIDs) == 0 {
		return 0
	}
	selectedSet := make(map[uint32]struct{}, len(selectedIDs))
	for _, id := range selectedIDs {
		selectedSet[id] = struct{}{}
	}
	total := int64(0)
	for _, entry := range entries {
		if _, ok := selectedSet[entry.FileID]; !ok {
			continue
		}
		if entry.EstLiveBytes <= 0 {
			continue
		}
		total += entry.EstLiveBytes
	}
	return total
}
