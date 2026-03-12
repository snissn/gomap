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

type valueLogGenerationRewriteQueueEntry struct {
	FileID        uint32
	LiveBytes     int64
	StaleBytes    int64
	StaleRatioPPM uint32
}

type valueLogGenerationStateFile struct {
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

func loadValueLogGenerationRewriteQueueEntries(path string) ([]valueLogGenerationRewriteQueueEntry, error) {
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
		seen := make(map[uint32]struct{}, len(raw.RewriteQueue))
		for _, entry := range raw.RewriteQueue {
			if entry.FileID == 0 {
				continue
			}
			if _, dup := seen[entry.FileID]; dup {
				continue
			}
			seen[entry.FileID] = struct{}{}
			if entry.LiveBytes < 0 {
				entry.LiveBytes = 0
			}
			if entry.StaleBytes < 0 {
				entry.StaleBytes = 0
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
	seen := make(map[uint32]struct{}, len(raw.RewriteSourceFileIDs))
	for _, s := range raw.RewriteSourceFileIDs {
		id64, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			continue
		}
		id := uint32(id64)
		if id == 0 {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, valueLogGenerationRewriteQueueEntry{FileID: id})
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func loadValueLogGenerationRewriteQueue(path string) ([]uint32, error) {
	entries, err := loadValueLogGenerationRewriteQueueEntries(path)
	if err != nil || len(entries) == 0 {
		return nil, err
	}
	return rewriteQueueEntryIDs(entries), nil
}

func saveValueLogGenerationRewriteQueueEntries(path string, entries []valueLogGenerationRewriteQueueEntry) error {
	if path == "" {
		return nil
	}
	if len(entries) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	raw := valueLogGenerationStateFile{RewriteQueue: make([]valueLogGenerationRewriteQueueEntry, 0, len(entries))}
	for _, entry := range entries {
		if entry.FileID == 0 {
			continue
		}
		if entry.LiveBytes < 0 {
			entry.LiveBytes = 0
		}
		if entry.StaleBytes < 0 {
			entry.StaleBytes = 0
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
	entries, err := loadValueLogGenerationRewriteQueueEntries(db.valueLogGenerationStatePath())
	if err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = entries
	db.vlogGenerationRewriteQueueLoaded = true
	return nil
}

func rewriteQueueEntryIDs(entries []valueLogGenerationRewriteQueueEntry) []uint32 {
	if len(entries) == 0 {
		return nil
	}
	ids := make([]uint32, 0, len(entries))
	for _, entry := range entries {
		if entry.FileID == 0 {
			continue
		}
		ids = append(ids, entry.FileID)
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

func (db *DB) setVlogGenerationRewriteQueueEntries(entries []valueLogGenerationRewriteQueueEntry) error {
	if db == nil {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	next := append([]valueLogGenerationRewriteQueueEntry(nil), entries...)
	if err := saveValueLogGenerationRewriteQueueEntries(db.valueLogGenerationStatePath(), next); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = next
	return nil
}

func (db *DB) setVlogGenerationRewriteQueue(ids []uint32) error {
	if len(ids) == 0 {
		return db.setVlogGenerationRewriteQueueEntries(nil)
	}
	entries := make([]valueLogGenerationRewriteQueueEntry, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		entries = append(entries, valueLogGenerationRewriteQueueEntry{FileID: id})
	}
	return db.setVlogGenerationRewriteQueueEntries(entries)
}

func (db *DB) currentVlogGenerationRewriteQueueEntries() ([]valueLogGenerationRewriteQueueEntry, error) {
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

func (db *DB) currentVlogGenerationRewriteQueue() ([]uint32, error) {
	entries, err := db.currentVlogGenerationRewriteQueueEntries()
	if err != nil {
		return nil, err
	}
	return rewriteQueueEntryIDs(entries), nil
}

func vlogGenerationRewriteQueueChunkEntries(entries []valueLogGenerationRewriteQueueEntry, maxSegments int) []valueLogGenerationRewriteQueueEntry {
	if len(entries) == 0 || maxSegments <= 0 {
		return nil
	}
	if len(entries) > maxSegments {
		entries = entries[:maxSegments]
	}
	return append([]valueLogGenerationRewriteQueueEntry(nil), entries...)
}

func vlogGenerationRewriteQueueChunk(ids []uint32, maxSegments int) []uint32 {
	return rewriteQueueEntryIDs(vlogGenerationRewriteQueueChunkEntries(func() []valueLogGenerationRewriteQueueEntry {
		if len(ids) == 0 {
			return nil
		}
		entries := make([]valueLogGenerationRewriteQueueEntry, 0, len(ids))
		for _, id := range ids {
			if id == 0 {
				continue
			}
			entries = append(entries, valueLogGenerationRewriteQueueEntry{FileID: id})
		}
		return entries
	}(), maxSegments))
}

func (db *DB) consumeVlogGenerationRewriteQueueChunkEntries(processed []valueLogGenerationRewriteQueueEntry) error {
	if db == nil || len(processed) == 0 {
		return nil
	}
	processedIDs := rewriteQueueEntryIDs(processed)
	if len(processedIDs) == 0 {
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
		for i := range processedIDs {
			if remaining[i].FileID != processedIDs[i] {
				match = false
				break
			}
		}
		if match {
			remaining = append([]valueLogGenerationRewriteQueueEntry(nil), remaining[len(processedIDs):]...)
		} else {
			processedSet := make(map[uint32]struct{}, len(processedIDs))
			for _, id := range processedIDs {
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
	if err := saveValueLogGenerationRewriteQueueEntries(db.valueLogGenerationStatePath(), remaining); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = remaining
	return nil
}

func (db *DB) consumeVlogGenerationRewriteQueueChunk(processed []uint32) error {
	if len(processed) == 0 {
		return nil
	}
	entries := make([]valueLogGenerationRewriteQueueEntry, 0, len(processed))
	for _, id := range processed {
		if id == 0 {
			continue
		}
		entries = append(entries, valueLogGenerationRewriteQueueEntry{FileID: id})
	}
	return db.consumeVlogGenerationRewriteQueueChunkEntries(entries)
}
