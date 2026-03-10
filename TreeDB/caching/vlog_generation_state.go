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

type valueLogGenerationRewriteDebtEntry struct {
	FileID               uint32 `json:"file_id"`
	BytesTotal           int64  `json:"bytes_total,omitempty"`
	BytesLive            int64  `json:"bytes_live,omitempty"`
	BytesStale           int64  `json:"bytes_stale,omitempty"`
	StaleRatioPPM        uint32 `json:"stale_ratio_ppm,omitempty"`
	LastEstimateUnixNano int64  `json:"last_estimate_unix_nano,omitempty"`
}

type valueLogGenerationStateFile struct {
	RewriteSourceFileIDs []string                             `json:"rewrite_source_file_ids,omitempty"`
	RewriteDebt          []valueLogGenerationRewriteDebtEntry `json:"rewrite_debt,omitempty"`
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

func loadValueLogGenerationRewriteDebt(path string) ([]valueLogGenerationRewriteDebtEntry, error) {
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
	if len(raw.RewriteDebt) > 0 {
		out := make([]valueLogGenerationRewriteDebtEntry, 0, len(raw.RewriteDebt))
		for _, entry := range raw.RewriteDebt {
			if entry.FileID == 0 {
				continue
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
	out := make([]valueLogGenerationRewriteDebtEntry, 0, len(raw.RewriteSourceFileIDs))
	for _, s := range raw.RewriteSourceFileIDs {
		id64, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			continue
		}
		out = append(out, valueLogGenerationRewriteDebtEntry{FileID: uint32(id64)})
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func saveValueLogGenerationRewriteDebt(path string, debt []valueLogGenerationRewriteDebtEntry) error {
	if path == "" {
		return nil
	}
	if len(debt) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	raw := valueLogGenerationStateFile{RewriteDebt: make([]valueLogGenerationRewriteDebtEntry, 0, len(debt))}
	for _, entry := range debt {
		if entry.FileID == 0 {
			continue
		}
		raw.RewriteDebt = append(raw.RewriteDebt, entry)
	}
	if len(raw.RewriteDebt) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
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
	debt, err := loadValueLogGenerationRewriteDebt(db.valueLogGenerationStatePath())
	if err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = debt
	db.vlogGenerationRewriteQueueLoaded = true
	return nil
}

func (db *DB) setVlogGenerationRewriteQueue(ids []uint32) error {
	debt := make([]valueLogGenerationRewriteDebtEntry, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		debt = append(debt, valueLogGenerationRewriteDebtEntry{FileID: id})
	}
	return db.setVlogGenerationRewriteDebt(debt)
}

func (db *DB) setVlogGenerationRewriteDebt(debt []valueLogGenerationRewriteDebtEntry) error {
	if db == nil {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	next := append([]valueLogGenerationRewriteDebtEntry(nil), debt...)
	if err := saveValueLogGenerationRewriteDebt(db.valueLogGenerationStatePath(), next); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = next
	return nil
}

func (db *DB) currentVlogGenerationRewriteDebt() ([]valueLogGenerationRewriteDebtEntry, error) {
	if db == nil {
		return nil, nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return nil, err
	}
	return append([]valueLogGenerationRewriteDebtEntry(nil), db.vlogGenerationRewriteQueue...), nil
}

func (db *DB) currentVlogGenerationRewriteQueue() ([]uint32, error) {
	debt, err := db.currentVlogGenerationRewriteDebt()
	if err != nil {
		return nil, err
	}
	return vlogGenerationRewriteDebtFileIDs(debt), nil
}

func vlogGenerationRewriteDebtFileIDs(entries []valueLogGenerationRewriteDebtEntry) []uint32 {
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

func vlogGenerationRewriteDebtChunk(entries []valueLogGenerationRewriteDebtEntry, maxSegments int, maxLiveBytes int64) []valueLogGenerationRewriteDebtEntry {
	if len(entries) == 0 || maxSegments <= 0 {
		return nil
	}
	remainingBytes := maxLiveBytes
	chunk := make([]valueLogGenerationRewriteDebtEntry, 0, minInt(len(entries), maxSegments))
	for _, entry := range entries {
		if entry.FileID == 0 {
			continue
		}
		if len(chunk) >= maxSegments {
			break
		}
		if remainingBytes > 0 && entry.BytesLive > 0 && len(chunk) > 0 && entry.BytesLive > remainingBytes {
			break
		}
		chunk = append(chunk, entry)
		if remainingBytes > 0 && entry.BytesLive > 0 {
			remainingBytes -= entry.BytesLive
			if remainingBytes < 0 {
				remainingBytes = 0
			}
		}
	}
	return chunk
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

func (db *DB) consumeVlogGenerationRewriteDebtChunk(processed []valueLogGenerationRewriteDebtEntry) error {
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
			if remaining[i].FileID != processed[i].FileID {
				match = false
				break
			}
		}
		if match {
			remaining = append([]valueLogGenerationRewriteDebtEntry(nil), remaining[len(processed):]...)
		} else {
			processedSet := make(map[uint32]struct{}, len(processed))
			for _, entry := range processed {
				if entry.FileID == 0 {
					continue
				}
				processedSet[entry.FileID] = struct{}{}
			}
			filtered := make([]valueLogGenerationRewriteDebtEntry, 0, len(remaining))
			for _, entry := range remaining {
				if _, ok := processedSet[entry.FileID]; ok {
					continue
				}
				filtered = append(filtered, entry)
			}
			remaining = filtered
		}
	}
	if err := saveValueLogGenerationRewriteDebt(db.valueLogGenerationStatePath(), remaining); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = remaining
	return nil
}

func (db *DB) consumeVlogGenerationRewriteQueueChunk(processed []uint32) error {
	if len(processed) == 0 {
		return nil
	}
	debt := make([]valueLogGenerationRewriteDebtEntry, 0, len(processed))
	for _, id := range processed {
		if id == 0 {
			continue
		}
		debt = append(debt, valueLogGenerationRewriteDebtEntry{FileID: id})
	}
	return db.consumeVlogGenerationRewriteDebtChunk(debt)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
