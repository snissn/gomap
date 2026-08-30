package caching

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

const valueLogGenerationStateFileName = "vlog_generation_state.json"

const vlogGenerationRewriteResumeMaxSegments = 1

const vlogGenerationRewriteResumeMinInterval = 1 * time.Second

type valueLogGenerationStateFile struct {
	RewriteSourceFileIDs  []string                                   `json:"rewrite_source_file_ids,omitempty"`
	RewriteDebtLedger     []valueLogGenerationRewriteDebtLedgerEntry `json:"rewrite_debt_ledger,omitempty"`
	RewriteDebtChunks     []valueLogGenerationRewriteDebtChunkEntry  `json:"rewrite_debt_chunks,omitempty"`
	RewriteDebtChunkBytes int64                                      `json:"rewrite_debt_chunk_bytes,omitempty"`
	RewriteHistory        []valueLogGenerationRewriteHistoryEntry    `json:"rewrite_history,omitempty"`
	RewritePenalties      []valueLogGenerationRewritePenaltyEntry    `json:"rewrite_penalties,omitempty"`
	RewriteStagePending   bool                                       `json:"rewrite_stage_pending,omitempty"`
	RewriteStageObserved  int64                                      `json:"rewrite_stage_observed_unix_nano,omitempty"`
}

type valueLogGenerationRewriteDebtLedgerEntry struct {
	FileID     string  `json:"file_id,omitempty"`
	BytesTotal int64   `json:"bytes_total,omitempty"`
	BytesLive  int64   `json:"bytes_live,omitempty"`
	BytesStale int64   `json:"bytes_stale,omitempty"`
	StaleRatio float64 `json:"stale_ratio,omitempty"`
}

type valueLogGenerationRewriteDebtChunkEntry struct {
	FileID      string  `json:"file_id,omitempty"`
	ChunkOffset int64   `json:"chunk_offset,omitempty"`
	BytesTotal  int64   `json:"bytes_total,omitempty"`
	BytesLive   int64   `json:"bytes_live,omitempty"`
	BytesStale  int64   `json:"bytes_stale,omitempty"`
	StaleRatio  float64 `json:"stale_ratio,omitempty"`
}

type valueLogGenerationRewritePenaltyEntry struct {
	FileID                string `json:"file_id,omitempty"`
	Attempts              int    `json:"attempts,omitempty"`
	CooldownUntilUnixNano int64  `json:"cooldown_until_unix_nano,omitempty"`
	LastGrowthBytes       int64  `json:"last_growth_bytes,omitempty"`
	LastStaleBytes        int64  `json:"last_stale_bytes,omitempty"`
}

type valueLogGenerationRewriteHistoryEntry struct {
	FileID                      string `json:"file_id,omitempty"`
	LastAttemptUnixNano         int64  `json:"last_attempt_unix_nano,omitempty"`
	LastProcessedLiveBytes      int64  `json:"last_processed_live_bytes,omitempty"`
	LastSourceBytesUnreferenced int64  `json:"last_source_bytes_unreferenced,omitempty"`
	LastReclaimedBytes          int64  `json:"last_reclaimed_bytes,omitempty"`
	LastStaleBytes              int64  `json:"last_stale_bytes,omitempty"`
}

type valueLogGenerationRewritePenalty struct {
	Attempts              int
	CooldownUntilUnixNano int64
	LastGrowthBytes       int64
	LastStaleBytes        int64
}

type valueLogGenerationRewriteHistory struct {
	LastAttemptUnixNano         int64
	LastProcessedLiveBytes      int64
	LastSourceBytesUnreferenced int64
	LastReclaimedBytes          int64
	LastStaleBytes              int64
}

func buildVlogGenerationRewriteLedgerByFileID(ledger []backenddb.ValueLogRewritePlanSegment) map[uint32]backenddb.ValueLogRewritePlanSegment {
	if len(ledger) == 0 {
		return nil
	}
	byFileID := make(map[uint32]backenddb.ValueLogRewritePlanSegment, len(ledger))
	for _, seg := range ledger {
		if seg.FileID == 0 {
			continue
		}
		byFileID[seg.FileID] = seg
	}
	if len(byFileID) == 0 {
		return nil
	}
	return byFileID
}

func synthesizeVlogGenerationRewriteLedgerFromChunks(chunks []backenddb.ValueLogRewritePlanChunk) []backenddb.ValueLogRewritePlanSegment {
	if len(chunks) == 0 {
		return nil
	}
	byFileID := buildVlogGenerationRewriteChunkLedgerByFileID(chunks)
	if len(byFileID) == 0 {
		return nil
	}
	ids := vlogGenerationRewriteChunkLedgerIDs(chunks)
	out := make([]backenddb.ValueLogRewritePlanSegment, 0, len(ids))
	for _, id := range ids {
		seg, ok := byFileID[id]
		if !ok || seg.FileID == 0 {
			continue
		}
		out = append(out, seg)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildVlogGenerationRewriteChunkLedgerByFileID(chunks []backenddb.ValueLogRewritePlanChunk) map[uint32]backenddb.ValueLogRewritePlanSegment {
	if len(chunks) == 0 {
		return nil
	}
	byFileID := make(map[uint32]backenddb.ValueLogRewritePlanSegment, len(chunks))
	for _, chunk := range chunks {
		if chunk.FileID == 0 {
			continue
		}
		seg := byFileID[chunk.FileID]
		seg.FileID = chunk.FileID
		seg.BytesTotal += chunk.BytesTotal
		seg.BytesLive += chunk.BytesLive
		seg.BytesStale += chunk.BytesStale
		byFileID[chunk.FileID] = seg
	}
	if len(byFileID) == 0 {
		return nil
	}
	for id, seg := range byFileID {
		if seg.BytesTotal > 0 {
			seg.StaleRatio = float64(seg.BytesStale) / float64(seg.BytesTotal)
		}
		byFileID[id] = seg
	}
	return byFileID
}

func vlogGenerationRewriteChunkLedgerIDs(chunks []backenddb.ValueLogRewritePlanChunk) []uint32 {
	if len(chunks) == 0 {
		return nil
	}
	seen := make(map[uint32]struct{}, len(chunks))
	ids := make([]uint32, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk.FileID == 0 {
			continue
		}
		if _, ok := seen[chunk.FileID]; ok {
			continue
		}
		seen[chunk.FileID] = struct{}{}
		ids = append(ids, chunk.FileID)
	}
	return ids
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

func loadValueLogGenerationState(path string) (valueLogGenerationStateFile, error) {
	var raw valueLogGenerationStateFile
	if path == "" {
		return raw, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return raw, nil
		}
		return raw, err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &raw); err != nil {
			// Rewrite queue state is rebuildable from the next maintenance plan, so
			// tolerate torn/corrupt JSON here.
			return valueLogGenerationStateFile{}, nil
		}
	}
	return raw, nil
}

func saveValueLogGenerationState(path string, raw valueLogGenerationStateFile) error {
	if path == "" {
		return nil
	}
	if len(raw.RewriteSourceFileIDs) == 0 && len(raw.RewriteDebtLedger) == 0 && len(raw.RewriteDebtChunks) == 0 && len(raw.RewriteHistory) == 0 && len(raw.RewritePenalties) == 0 {
		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if err := durabilitycut.EmitNamespace(
			durabilitycut.NamespaceUnlink,
			durabilitycut.ResourceValueLog,
			filepath.Dir(path),
			path,
			"",
		); err != nil {
			return errors.Join(err, backenddb.ErrRecoveryRequired)
		}
		return nil
	}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	return atomicfile.Write(path, data, 0o600)
}

func loadValueLogGenerationRewriteState(path string) ([]uint32, []backenddb.ValueLogRewritePlanSegment, []backenddb.ValueLogRewritePlanChunk, int64, map[uint32]valueLogGenerationRewriteHistory, map[uint32]valueLogGenerationRewritePenalty, bool, int64, error) {
	raw, err := loadValueLogGenerationState(path)
	if err != nil {
		return nil, nil, nil, 0, nil, nil, false, 0, err
	}

	history := make(map[uint32]valueLogGenerationRewriteHistory, len(raw.RewriteHistory))
	for _, e := range raw.RewriteHistory {
		if e.FileID == "" {
			continue
		}
		id64, err := strconv.ParseUint(e.FileID, 10, 32)
		if err != nil || id64 == 0 {
			continue
		}
		if e.LastAttemptUnixNano == 0 &&
			e.LastProcessedLiveBytes == 0 &&
			e.LastSourceBytesUnreferenced == 0 &&
			e.LastReclaimedBytes == 0 &&
			e.LastStaleBytes == 0 {
			continue
		}
		history[uint32(id64)] = valueLogGenerationRewriteHistory{
			LastAttemptUnixNano:         e.LastAttemptUnixNano,
			LastProcessedLiveBytes:      e.LastProcessedLiveBytes,
			LastSourceBytesUnreferenced: e.LastSourceBytesUnreferenced,
			LastReclaimedBytes:          e.LastReclaimedBytes,
			LastStaleBytes:              e.LastStaleBytes,
		}
	}

	penalties := make(map[uint32]valueLogGenerationRewritePenalty, len(raw.RewritePenalties))
	for _, e := range raw.RewritePenalties {
		if e.FileID == "" {
			continue
		}
		id64, err := strconv.ParseUint(e.FileID, 10, 32)
		if err != nil || id64 == 0 {
			continue
		}
		if e.Attempts <= 0 && e.CooldownUntilUnixNano <= 0 && e.LastGrowthBytes == 0 && e.LastStaleBytes == 0 {
			continue
		}
		penalties[uint32(id64)] = valueLogGenerationRewritePenalty{
			Attempts:              e.Attempts,
			CooldownUntilUnixNano: e.CooldownUntilUnixNano,
			LastGrowthBytes:       e.LastGrowthBytes,
			LastStaleBytes:        e.LastStaleBytes,
		}
	}

	ledger := make([]backenddb.ValueLogRewritePlanSegment, 0, len(raw.RewriteDebtLedger))
	if len(raw.RewriteDebtLedger) > 0 {
		for _, e := range raw.RewriteDebtLedger {
			if e.FileID == "" {
				continue
			}
			id64, err := strconv.ParseUint(e.FileID, 10, 32)
			if err != nil {
				continue
			}
			if id64 == 0 {
				continue
			}
			ledger = append(ledger, backenddb.ValueLogRewritePlanSegment{
				FileID:     uint32(id64),
				BytesTotal: e.BytesTotal,
				BytesLive:  e.BytesLive,
				BytesStale: e.BytesStale,
				StaleRatio: e.StaleRatio,
			})
		}
	}

	chunkLedger := make([]backenddb.ValueLogRewritePlanChunk, 0, len(raw.RewriteDebtChunks))
	if len(raw.RewriteDebtChunks) > 0 {
		for _, e := range raw.RewriteDebtChunks {
			if e.FileID == "" {
				continue
			}
			id64, err := strconv.ParseUint(e.FileID, 10, 32)
			if err != nil || id64 == 0 {
				continue
			}
			chunkLedger = append(chunkLedger, backenddb.ValueLogRewritePlanChunk{
				FileID:      uint32(id64),
				ChunkOffset: e.ChunkOffset,
				BytesTotal:  e.BytesTotal,
				BytesLive:   e.BytesLive,
				BytesStale:  e.BytesStale,
				StaleRatio:  e.StaleRatio,
			})
		}
	}

	if len(chunkLedger) > 0 {
		ids := vlogGenerationRewriteChunkLedgerIDs(chunkLedger)
		return ids, nil, chunkLedger, raw.RewriteDebtChunkBytes, history, penalties, raw.RewriteStagePending, raw.RewriteStageObserved, nil
	}

	if len(ledger) > 0 {
		ids := make([]uint32, 0, len(ledger))
		for _, seg := range ledger {
			ids = append(ids, seg.FileID)
		}
		return ids, ledger, nil, 0, history, penalties, raw.RewriteStagePending, raw.RewriteStageObserved, nil
	}

	if len(raw.RewriteSourceFileIDs) == 0 {
		return nil, nil, nil, 0, history, penalties, false, 0, nil
	}
	out := make([]uint32, 0, len(raw.RewriteSourceFileIDs))
	for _, s := range raw.RewriteSourceFileIDs {
		id64, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			continue
		}
		if id64 == 0 {
			continue
		}
		out = append(out, uint32(id64))
	}
	if len(out) == 0 {
		return nil, nil, nil, 0, history, penalties, false, 0, nil
	}
	return out, nil, nil, 0, history, penalties, false, 0, nil
}

func retainVlogGenerationRewriteHistory(ids []uint32, history map[uint32]valueLogGenerationRewriteHistory) map[uint32]valueLogGenerationRewriteHistory {
	if len(history) == 0 || len(ids) == 0 {
		return nil
	}
	keep := make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		keep[id] = struct{}{}
	}
	if len(keep) == 0 {
		return nil
	}
	filtered := make(map[uint32]valueLogGenerationRewriteHistory, len(keep))
	for id, entry := range history {
		if _, ok := keep[id]; !ok {
			continue
		}
		filtered[id] = entry
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func saveValueLogGenerationRewriteState(path string, ids []uint32, ledger []backenddb.ValueLogRewritePlanSegment, chunkLedger []backenddb.ValueLogRewritePlanChunk, chunkBytes int64, history map[uint32]valueLogGenerationRewriteHistory, penalties map[uint32]valueLogGenerationRewritePenalty, stagePending bool, stageObservedAt int64) error {
	raw, err := loadValueLogGenerationState(path)
	if err != nil {
		return err
	}
	raw.RewriteSourceFileIDs = raw.RewriteSourceFileIDs[:0]
	if len(ids) > 0 {
		raw.RewriteSourceFileIDs = make([]string, 0, len(ids))
		for _, id := range ids {
			raw.RewriteSourceFileIDs = append(raw.RewriteSourceFileIDs, strconv.FormatUint(uint64(id), 10))
		}
	}
	raw.RewriteDebtLedger = raw.RewriteDebtLedger[:0]
	if len(ledger) > 0 {
		raw.RewriteDebtLedger = make([]valueLogGenerationRewriteDebtLedgerEntry, 0, len(ledger))
		for _, seg := range ledger {
			raw.RewriteDebtLedger = append(raw.RewriteDebtLedger, valueLogGenerationRewriteDebtLedgerEntry{
				FileID:     strconv.FormatUint(uint64(seg.FileID), 10),
				BytesTotal: seg.BytesTotal,
				BytesLive:  seg.BytesLive,
				BytesStale: seg.BytesStale,
				StaleRatio: seg.StaleRatio,
			})
		}
	}
	raw.RewriteDebtChunks = raw.RewriteDebtChunks[:0]
	raw.RewriteDebtChunkBytes = 0
	if len(chunkLedger) > 0 {
		raw.RewriteDebtChunks = make([]valueLogGenerationRewriteDebtChunkEntry, 0, len(chunkLedger))
		for _, chunk := range chunkLedger {
			raw.RewriteDebtChunks = append(raw.RewriteDebtChunks, valueLogGenerationRewriteDebtChunkEntry{
				FileID:      strconv.FormatUint(uint64(chunk.FileID), 10),
				ChunkOffset: chunk.ChunkOffset,
				BytesTotal:  chunk.BytesTotal,
				BytesLive:   chunk.BytesLive,
				BytesStale:  chunk.BytesStale,
				StaleRatio:  chunk.StaleRatio,
			})
		}
		raw.RewriteDebtChunkBytes = chunkBytes
	}
	raw.RewriteHistory = raw.RewriteHistory[:0]
	history = retainVlogGenerationRewriteHistory(ids, history)
	if len(history) > 0 {
		keys := make([]uint32, 0, len(history))
		for id, entry := range history {
			if id == 0 {
				continue
			}
			if entry.LastAttemptUnixNano == 0 &&
				entry.LastProcessedLiveBytes == 0 &&
				entry.LastSourceBytesUnreferenced == 0 &&
				entry.LastReclaimedBytes == 0 &&
				entry.LastStaleBytes == 0 {
				continue
			}
			keys = append(keys, id)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		raw.RewriteHistory = make([]valueLogGenerationRewriteHistoryEntry, 0, len(keys))
		for _, id := range keys {
			entry := history[id]
			raw.RewriteHistory = append(raw.RewriteHistory, valueLogGenerationRewriteHistoryEntry{
				FileID:                      strconv.FormatUint(uint64(id), 10),
				LastAttemptUnixNano:         entry.LastAttemptUnixNano,
				LastProcessedLiveBytes:      entry.LastProcessedLiveBytes,
				LastSourceBytesUnreferenced: entry.LastSourceBytesUnreferenced,
				LastReclaimedBytes:          entry.LastReclaimedBytes,
				LastStaleBytes:              entry.LastStaleBytes,
			})
		}
	}
	raw.RewritePenalties = raw.RewritePenalties[:0]
	if len(penalties) > 0 {
		keys := make([]uint32, 0, len(penalties))
		for id, penalty := range penalties {
			if id == 0 {
				continue
			}
			if penalty.Attempts <= 0 && penalty.CooldownUntilUnixNano <= 0 && penalty.LastGrowthBytes == 0 && penalty.LastStaleBytes == 0 {
				continue
			}
			keys = append(keys, id)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		raw.RewritePenalties = make([]valueLogGenerationRewritePenaltyEntry, 0, len(keys))
		for _, id := range keys {
			penalty := penalties[id]
			raw.RewritePenalties = append(raw.RewritePenalties, valueLogGenerationRewritePenaltyEntry{
				FileID:                strconv.FormatUint(uint64(id), 10),
				Attempts:              penalty.Attempts,
				CooldownUntilUnixNano: penalty.CooldownUntilUnixNano,
				LastGrowthBytes:       penalty.LastGrowthBytes,
				LastStaleBytes:        penalty.LastStaleBytes,
			})
		}
	}
	raw.RewriteStagePending = stagePending
	raw.RewriteStageObserved = stageObservedAt
	return saveValueLogGenerationState(path, raw)
}

func (db *DB) loadVlogGenerationRewriteQueueLocked() error {
	if db == nil {
		return nil
	}
	if db.vlogGenerationRewriteQueueLoaded {
		return nil
	}
	ids, ledger, chunkLedger, chunkBytes, history, penalties, stagePending, stageObservedAt, err := loadValueLogGenerationRewriteState(db.valueLogGenerationStatePath())
	if err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = ids
	db.vlogGenerationRewriteLedger = ledger
	db.vlogGenerationRewriteChunkLedger = chunkLedger
	db.vlogGenerationRewriteChunkBytes = chunkBytes
	db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteLedgerByFileID(ledger)
	if len(db.vlogGenerationRewriteLedgerByFileID) == 0 && len(chunkLedger) > 0 {
		db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteChunkLedgerByFileID(chunkLedger)
	}
	db.vlogGenerationRewriteHistory = history
	db.vlogGenerationRewritePenalties = penalties
	db.vlogGenerationRewriteStagePending = stagePending
	db.vlogGenerationRewriteStageObservedUnixNano = stageObservedAt
	db.vlogGenerationRewriteQueueLoaded = true
	if stagePending && stageObservedAt > 0 {
		db.scheduleVlogGenerationRewriteStageConfirmation(stageObservedAt)
	} else {
		db.clearVlogGenerationRewriteStageConfirmation(true)
	}
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
	nextHistory := retainVlogGenerationRewriteHistory(next, db.vlogGenerationRewriteHistory)
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), next, nil, nil, 0, nextHistory, db.vlogGenerationRewritePenalties, false, 0); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = next
	db.vlogGenerationRewriteLedger = nil
	db.vlogGenerationRewriteChunkLedger = nil
	db.vlogGenerationRewriteChunkBytes = 0
	db.vlogGenerationRewriteLedgerByFileID = nil
	db.vlogGenerationRewriteHistory = nextHistory
	db.vlogGenerationRewriteStagePending = false
	db.vlogGenerationRewriteStageObservedUnixNano = 0
	db.clearVlogGenerationRewriteStageConfirmation(true)
	return nil
}

func (db *DB) setVlogGenerationRewriteLedger(segments []backenddb.ValueLogRewritePlanSegment) error {
	return db.setVlogGenerationRewriteLedgerWithStage(segments, false, 0)
}

func (db *DB) setVlogGenerationRewriteChunkLedger(chunks []backenddb.ValueLogRewritePlanChunk, chunkBytes int64) error {
	return db.setVlogGenerationRewriteChunkLedgerWithStage(chunks, chunkBytes, false, 0)
}

func (db *DB) setVlogGenerationRewriteLedgerWithStage(segments []backenddb.ValueLogRewritePlanSegment, stagePending bool, stageObservedAt int64) error {
	if db == nil {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	nextLedger := append([]backenddb.ValueLogRewritePlanSegment(nil), segments...)
	nextIDs := make([]uint32, 0, len(nextLedger))
	for _, seg := range nextLedger {
		if seg.FileID == 0 {
			continue
		}
		nextIDs = append(nextIDs, seg.FileID)
	}
	nextHistory := retainVlogGenerationRewriteHistory(nextIDs, db.vlogGenerationRewriteHistory)
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), nextIDs, nextLedger, nil, 0, nextHistory, db.vlogGenerationRewritePenalties, stagePending, stageObservedAt); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = nextIDs
	db.vlogGenerationRewriteLedger = nextLedger
	db.vlogGenerationRewriteChunkLedger = nil
	db.vlogGenerationRewriteChunkBytes = 0
	db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteLedgerByFileID(nextLedger)
	db.vlogGenerationRewriteHistory = nextHistory
	db.vlogGenerationRewriteStagePending = stagePending
	db.vlogGenerationRewriteStageObservedUnixNano = stageObservedAt
	if stagePending && stageObservedAt > 0 {
		db.scheduleVlogGenerationRewriteStageConfirmation(stageObservedAt)
	} else {
		db.clearVlogGenerationRewriteStageConfirmation(true)
	}
	return nil
}

func (db *DB) setVlogGenerationRewriteChunkLedgerWithStage(chunks []backenddb.ValueLogRewritePlanChunk, chunkBytes int64, stagePending bool, stageObservedAt int64) error {
	if db == nil {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	nextChunks := append([]backenddb.ValueLogRewritePlanChunk(nil), chunks...)
	nextIDs := vlogGenerationRewriteChunkLedgerIDs(nextChunks)
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), nextIDs, nil, nextChunks, chunkBytes, retainVlogGenerationRewriteHistory(nextIDs, db.vlogGenerationRewriteHistory), db.vlogGenerationRewritePenalties, stagePending, stageObservedAt); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = nextIDs
	db.vlogGenerationRewriteLedger = nil
	db.vlogGenerationRewriteChunkLedger = nextChunks
	db.vlogGenerationRewriteChunkBytes = chunkBytes
	db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteChunkLedgerByFileID(nextChunks)
	db.vlogGenerationRewriteStagePending = stagePending
	db.vlogGenerationRewriteStageObservedUnixNano = stageObservedAt
	if stagePending && stageObservedAt > 0 {
		db.scheduleVlogGenerationRewriteStageConfirmation(stageObservedAt)
	} else {
		db.clearVlogGenerationRewriteStageConfirmation(true)
	}
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

func (db *DB) currentVlogGenerationRewriteLedger() ([]backenddb.ValueLogRewritePlanSegment, error) {
	if db == nil {
		return nil, nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return nil, err
	}
	if len(db.vlogGenerationRewriteLedger) > 0 {
		return append([]backenddb.ValueLogRewritePlanSegment(nil), db.vlogGenerationRewriteLedger...), nil
	}
	return synthesizeVlogGenerationRewriteLedgerFromChunks(db.vlogGenerationRewriteChunkLedger), nil
}

func (db *DB) currentVlogGenerationRewriteChunkLedger() ([]backenddb.ValueLogRewritePlanChunk, int64, error) {
	if db == nil {
		return nil, 0, nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return nil, 0, err
	}
	return append([]backenddb.ValueLogRewritePlanChunk(nil), db.vlogGenerationRewriteChunkLedger...), db.vlogGenerationRewriteChunkBytes, nil
}

func (db *DB) currentVlogGenerationRewriteEligible(now time.Time) ([]uint32, []backenddb.ValueLogRewritePlanSegment, error) {
	if db == nil {
		return nil, nil, nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return nil, nil, err
	}
	queue := append([]uint32(nil), db.vlogGenerationRewriteQueue...)
	ledger := append([]backenddb.ValueLogRewritePlanSegment(nil), db.vlogGenerationRewriteLedger...)
	if len(queue) == 0 {
		return queue, ledger, nil
	}
	if len(db.vlogGenerationRewritePenalties) > 0 {
		queue = filterVlogGenerationRewriteIDsByPenalty(queue, db.vlogGenerationRewritePenalties, now)
		ledger = filterVlogGenerationRewriteLedgerByPenalty(ledger, db.vlogGenerationRewritePenalties, now)
	}
	queue = prioritizeVlogGenerationRewriteIDs(queue, db.vlogGenerationRewriteHistory, db.vlogGenerationRewritePenalties)
	ledger = prioritizeVlogGenerationRewriteLedger(ledger, db.vlogGenerationRewriteHistory, db.vlogGenerationRewritePenalties)
	return queue, ledger, nil
}

func (db *DB) currentVlogGenerationRewriteStage() (bool, int64, error) {
	if db == nil {
		return false, 0, nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return false, 0, err
	}
	return db.vlogGenerationRewriteStagePending, db.vlogGenerationRewriteStageObservedUnixNano, nil
}

func (db *DB) clearVlogGenerationRewriteStage() error {
	if db == nil {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	if !db.vlogGenerationRewriteStagePending {
		return nil
	}
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), db.vlogGenerationRewriteQueue, db.vlogGenerationRewriteLedger, db.vlogGenerationRewriteChunkLedger, db.vlogGenerationRewriteChunkBytes, db.vlogGenerationRewriteHistory, db.vlogGenerationRewritePenalties, false, 0); err != nil {
		return err
	}
	db.vlogGenerationRewriteStagePending = false
	db.vlogGenerationRewriteStageObservedUnixNano = 0
	db.clearVlogGenerationRewriteStageConfirmation(true)
	return nil
}

func (db *DB) pruneVlogGenerationRewriteLedgerNonPositiveLive() ([]uint32, int, error) {
	if db == nil {
		return nil, 0, nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return nil, 0, err
	}
	if len(db.vlogGenerationRewriteChunkLedger) > 0 {
		filteredChunks := make([]backenddb.ValueLogRewritePlanChunk, 0, len(db.vlogGenerationRewriteChunkLedger))
		dropped := 0
		for _, chunk := range db.vlogGenerationRewriteChunkLedger {
			if chunk.FileID == 0 || chunk.BytesLive <= 0 {
				dropped++
				continue
			}
			filteredChunks = append(filteredChunks, chunk)
		}
		if dropped == 0 {
			return append([]uint32(nil), db.vlogGenerationRewriteQueue...), 0, nil
		}
		filteredIDs := vlogGenerationRewriteChunkLedgerIDs(filteredChunks)
		stagePending := db.vlogGenerationRewriteStagePending && len(filteredChunks) > 0
		stageObservedAt := db.vlogGenerationRewriteStageObservedUnixNano
		if !stagePending {
			stageObservedAt = 0
		}
		if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), filteredIDs, nil, filteredChunks, db.vlogGenerationRewriteChunkBytes, retainVlogGenerationRewriteHistory(filteredIDs, db.vlogGenerationRewriteHistory), db.vlogGenerationRewritePenalties, stagePending, stageObservedAt); err != nil {
			return nil, 0, err
		}
		db.vlogGenerationRewriteQueue = filteredIDs
		db.vlogGenerationRewriteLedger = nil
		db.vlogGenerationRewriteChunkLedger = filteredChunks
		db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteChunkLedgerByFileID(filteredChunks)
		db.vlogGenerationRewriteStagePending = stagePending
		db.vlogGenerationRewriteStageObservedUnixNano = stageObservedAt
		if stagePending && stageObservedAt > 0 {
			db.scheduleVlogGenerationRewriteStageConfirmation(stageObservedAt)
		} else {
			db.clearVlogGenerationRewriteStageConfirmation(true)
		}
		return append([]uint32(nil), filteredIDs...), dropped, nil
	}
	if len(db.vlogGenerationRewriteLedger) == 0 {
		return append([]uint32(nil), db.vlogGenerationRewriteQueue...), 0, nil
	}
	filteredLedger := make([]backenddb.ValueLogRewritePlanSegment, 0, len(db.vlogGenerationRewriteLedger))
	filteredIDs := make([]uint32, 0, len(db.vlogGenerationRewriteLedger))
	dropped := 0
	for _, seg := range db.vlogGenerationRewriteLedger {
		if seg.FileID == 0 || seg.BytesLive <= 0 {
			dropped++
			continue
		}
		filteredLedger = append(filteredLedger, seg)
		filteredIDs = append(filteredIDs, seg.FileID)
	}
	if dropped == 0 {
		return append([]uint32(nil), db.vlogGenerationRewriteQueue...), 0, nil
	}
	stagePending := db.vlogGenerationRewriteStagePending && len(filteredLedger) > 0
	stageObservedAt := db.vlogGenerationRewriteStageObservedUnixNano
	if !stagePending {
		stageObservedAt = 0
	}
	nextHistory := retainVlogGenerationRewriteHistory(filteredIDs, db.vlogGenerationRewriteHistory)
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), filteredIDs, filteredLedger, nil, 0, nextHistory, db.vlogGenerationRewritePenalties, stagePending, stageObservedAt); err != nil {
		return nil, 0, err
	}
	db.vlogGenerationRewriteQueue = filteredIDs
	db.vlogGenerationRewriteLedger = filteredLedger
	db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteLedgerByFileID(filteredLedger)
	db.vlogGenerationRewriteHistory = nextHistory
	db.vlogGenerationRewriteStagePending = stagePending
	db.vlogGenerationRewriteStageObservedUnixNano = stageObservedAt
	if stagePending && stageObservedAt > 0 {
		db.scheduleVlogGenerationRewriteStageConfirmation(stageObservedAt)
	} else {
		db.clearVlogGenerationRewriteStageConfirmation(true)
	}
	return append([]uint32(nil), filteredIDs...), dropped, nil
}

func vlogGenerationRewritePenaltySortKey(id uint32, penalties map[uint32]valueLogGenerationRewritePenalty) (attempts int, growth int64) {
	if id == 0 || len(penalties) == 0 {
		return 0, 0
	}
	penalty, ok := penalties[id]
	if !ok {
		return 0, 0
	}
	return penalty.Attempts, penalty.LastGrowthBytes
}

func vlogGenerationRewriteHistoryUsefulness(id uint32, history map[uint32]valueLogGenerationRewriteHistory) (ratioPPM int64, usefulBytes int64) {
	if id == 0 || len(history) == 0 {
		return 0, 0
	}
	entry, ok := history[id]
	if !ok {
		return 0, 0
	}
	usefulBytes = entry.LastSourceBytesUnreferenced
	if entry.LastReclaimedBytes > usefulBytes {
		usefulBytes = entry.LastReclaimedBytes
	}
	if usefulBytes <= 0 {
		return 0, 0
	}
	if entry.LastProcessedLiveBytes <= 0 {
		return 1_000_000, usefulBytes
	}
	ratioPPM = usefulBytes * 1_000_000 / entry.LastProcessedLiveBytes
	if ratioPPM > 1_000_000 {
		ratioPPM = 1_000_000
	}
	return ratioPPM, usefulBytes
}

func vlogGenerationRewriteHistoryClass(id uint32, history map[uint32]valueLogGenerationRewriteHistory) int {
	if id == 0 || len(history) == 0 {
		return 1
	}
	entry, ok := history[id]
	if !ok {
		return 1
	}
	usefulRatio, usefulBytes := vlogGenerationRewriteHistoryUsefulness(id, history)
	if usefulRatio > 0 || usefulBytes > 0 {
		return 0
	}
	if entry.LastAttemptUnixNano > 0 || entry.LastProcessedLiveBytes > 0 || entry.LastStaleBytes > 0 {
		return 2
	}
	return 1
}

func vlogGenerationRewriteLedgerHistoryClass(seg backenddb.ValueLogRewritePlanSegment, history map[uint32]valueLogGenerationRewriteHistory, penalties map[uint32]valueLogGenerationRewritePenalty) int {
	if seg.FileID == 0 {
		return 1
	}
	usefulRatio, usefulBytes := vlogGenerationRewriteHistoryUsefulness(seg.FileID, history)
	if usefulRatio > 0 || usefulBytes > 0 {
		return 0
	}
	if vlogGenerationRewriteStaleDelta(seg, history, penalties) > 0 {
		return 1
	}
	if _, ok := history[seg.FileID]; ok {
		return 3
	}
	return 2
}

func vlogGenerationRewriteStaleDelta(seg backenddb.ValueLogRewritePlanSegment, history map[uint32]valueLogGenerationRewriteHistory, penalties map[uint32]valueLogGenerationRewritePenalty) int64 {
	if seg.FileID == 0 {
		return 0
	}
	if penalty, ok := penalties[seg.FileID]; ok && penalty.LastStaleBytes > 0 {
		delta := seg.BytesStale - penalty.LastStaleBytes
		if delta > 0 {
			return delta
		}
	}
	if len(history) == 0 {
		return 0
	}
	entry, ok := history[seg.FileID]
	if !ok || entry.LastStaleBytes <= 0 {
		return 0
	}
	delta := seg.BytesStale - entry.LastStaleBytes
	if delta <= 0 {
		return 0
	}
	return delta
}

func prioritizeVlogGenerationRewriteIDs(ids []uint32, history map[uint32]valueLogGenerationRewriteHistory, penalties map[uint32]valueLogGenerationRewritePenalty) []uint32 {
	if len(ids) <= 1 {
		return append([]uint32(nil), ids...)
	}
	ordered := append([]uint32(nil), ids...)
	sort.SliceStable(ordered, func(i, j int) bool {
		ai, ag := vlogGenerationRewritePenaltySortKey(ordered[i], penalties)
		bj, bg := vlogGenerationRewritePenaltySortKey(ordered[j], penalties)
		if ai != bj {
			return ai < bj
		}
		ac := vlogGenerationRewriteHistoryClass(ordered[i], history)
		bc := vlogGenerationRewriteHistoryClass(ordered[j], history)
		if ac != bc {
			return ac < bc
		}
		ar, au := vlogGenerationRewriteHistoryUsefulness(ordered[i], history)
		br, bu := vlogGenerationRewriteHistoryUsefulness(ordered[j], history)
		if ar != br {
			return ar > br
		}
		if au != bu {
			return au > bu
		}
		if ag != bg {
			return ag < bg
		}
		return ordered[i] < ordered[j]
	})
	return ordered
}

func prioritizeVlogGenerationRewriteLedger(ledger []backenddb.ValueLogRewritePlanSegment, history map[uint32]valueLogGenerationRewriteHistory, penalties map[uint32]valueLogGenerationRewritePenalty) []backenddb.ValueLogRewritePlanSegment {
	if len(ledger) <= 1 {
		return append([]backenddb.ValueLogRewritePlanSegment(nil), ledger...)
	}
	ordered := append([]backenddb.ValueLogRewritePlanSegment(nil), ledger...)
	sort.SliceStable(ordered, func(i, j int) bool {
		ai, ag := vlogGenerationRewritePenaltySortKey(ordered[i].FileID, penalties)
		bj, bg := vlogGenerationRewritePenaltySortKey(ordered[j].FileID, penalties)
		if ai != bj {
			return ai < bj
		}
		ac := vlogGenerationRewriteLedgerHistoryClass(ordered[i], history, penalties)
		bc := vlogGenerationRewriteLedgerHistoryClass(ordered[j], history, penalties)
		if ac != bc {
			return ac < bc
		}
		ar, au := vlogGenerationRewriteHistoryUsefulness(ordered[i].FileID, history)
		br, bu := vlogGenerationRewriteHistoryUsefulness(ordered[j].FileID, history)
		if ar != br {
			return ar > br
		}
		if au != bu {
			return au > bu
		}
		ad := vlogGenerationRewriteStaleDelta(ordered[i], history, penalties)
		bd := vlogGenerationRewriteStaleDelta(ordered[j], history, penalties)
		if ad != bd {
			return ad > bd
		}
		if ag != bg {
			return ag < bg
		}
		a := ordered[i]
		b := ordered[j]
		if a.StaleRatio != b.StaleRatio {
			return a.StaleRatio > b.StaleRatio
		}
		if a.BytesStale != b.BytesStale {
			return a.BytesStale > b.BytesStale
		}
		if a.BytesTotal != b.BytesTotal {
			return a.BytesTotal > b.BytesTotal
		}
		return a.FileID < b.FileID
	})
	return ordered
}

func admitVlogGenerationRewriteIDs(ids []uint32, history map[uint32]valueLogGenerationRewriteHistory, penalties map[uint32]valueLogGenerationRewritePenalty, maxRetried int) []uint32 {
	if len(ids) == 0 || len(penalties) == 0 || maxRetried < 0 {
		return append([]uint32(nil), ids...)
	}
	haveUsefulRetry := false
	for _, id := range ids {
		attempts, _ := vlogGenerationRewritePenaltySortKey(id, penalties)
		if attempts <= 0 {
			continue
		}
		_, usefulBytes := vlogGenerationRewriteHistoryUsefulness(id, history)
		if usefulBytes > 0 {
			haveUsefulRetry = true
			break
		}
	}
	admitted := make([]uint32, 0, len(ids))
	retried := 0
	for _, id := range ids {
		attempts, _ := vlogGenerationRewritePenaltySortKey(id, penalties)
		if attempts <= 0 {
			admitted = append(admitted, id)
			continue
		}
		_, usefulBytes := vlogGenerationRewriteHistoryUsefulness(id, history)
		if usefulBytes > 0 {
			admitted = append(admitted, id)
			continue
		}
		if haveUsefulRetry || retried >= maxRetried {
			continue
		}
		retried++
		admitted = append(admitted, id)
	}
	return admitted
}

func admitVlogGenerationRewriteLedger(ledger []backenddb.ValueLogRewritePlanSegment, history map[uint32]valueLogGenerationRewriteHistory, penalties map[uint32]valueLogGenerationRewritePenalty, maxRetried int) []backenddb.ValueLogRewritePlanSegment {
	if len(ledger) == 0 || len(penalties) == 0 || maxRetried < 0 {
		return append([]backenddb.ValueLogRewritePlanSegment(nil), ledger...)
	}
	haveUsefulRetry := false
	for _, seg := range ledger {
		attempts, _ := vlogGenerationRewritePenaltySortKey(seg.FileID, penalties)
		if attempts <= 0 {
			continue
		}
		_, usefulBytes := vlogGenerationRewriteHistoryUsefulness(seg.FileID, history)
		if usefulBytes > 0 {
			haveUsefulRetry = true
			break
		}
	}
	admitted := make([]backenddb.ValueLogRewritePlanSegment, 0, len(ledger))
	retried := 0
	for _, seg := range ledger {
		attempts, _ := vlogGenerationRewritePenaltySortKey(seg.FileID, penalties)
		if attempts <= 0 {
			admitted = append(admitted, seg)
			continue
		}
		_, usefulBytes := vlogGenerationRewriteHistoryUsefulness(seg.FileID, history)
		if usefulBytes > 0 {
			admitted = append(admitted, seg)
			continue
		}
		if haveUsefulRetry || retried >= maxRetried {
			continue
		}
		retried++
		admitted = append(admitted, seg)
	}
	return admitted
}

func vlogGenerationRewriteQueueChunkWithPenalty(ids []uint32, history map[uint32]valueLogGenerationRewriteHistory, penalties map[uint32]valueLogGenerationRewritePenalty, maxSegments int) []uint32 {
	if len(ids) == 0 || maxSegments <= 0 {
		return nil
	}
	if len(penalties) > 0 {
		ids = admitVlogGenerationRewriteIDs(ids, history, penalties, vlogGenerationRewriteRetriedSelectionLimit)
	}
	if len(ids) > maxSegments {
		ids = ids[:maxSegments]
	}
	return append([]uint32(nil), ids...)
}

func vlogGenerationRewriteQueueChunk(ids []uint32, maxSegments int) []uint32 {
	return vlogGenerationRewriteQueueChunkWithPenalty(ids, nil, nil, maxSegments)
}

func vlogGenerationRewriteLedgerChunkWithPenalty(ledger []backenddb.ValueLogRewritePlanSegment, history map[uint32]valueLogGenerationRewriteHistory, penalties map[uint32]valueLogGenerationRewritePenalty, maxSegments int, budgetLiveBytes int64) []uint32 {
	if len(ledger) == 0 || maxSegments <= 0 {
		return nil
	}
	candidates := make([]backenddb.ValueLogRewritePlanSegment, 0, len(ledger))
	for _, seg := range ledger {
		if seg.FileID == 0 {
			continue
		}
		if seg.BytesLive <= 0 {
			continue
		}
		candidates = append(candidates, seg)
	}
	if len(candidates) == 0 {
		return nil
	}
	candidates = prioritizeVlogGenerationRewriteLedger(candidates, history, penalties)
	if len(penalties) > 0 {
		candidates = admitVlogGenerationRewriteLedger(candidates, history, penalties, vlogGenerationRewriteRetriedSelectionLimit)
	}

	ids := make([]uint32, 0, maxSegments)
	remaining := budgetLiveBytes
	for _, seg := range candidates {
		if len(ids) >= maxSegments {
			break
		}
		if budgetLiveBytes > 0 && remaining <= 0 && len(ids) > 0 {
			break
		}
		live := seg.BytesLive
		if live <= 0 {
			live = 0
		}
		if budgetLiveBytes > 0 && remaining > 0 && live > remaining && len(ids) > 0 {
			continue
		}
		ids = append(ids, seg.FileID)
		if budgetLiveBytes > 0 && remaining > 0 {
			remaining -= live
		}
	}
	return ids
}

func vlogGenerationRewriteLedgerChunk(ledger []backenddb.ValueLogRewritePlanSegment, maxSegments int, budgetLiveBytes int64) []uint32 {
	return vlogGenerationRewriteLedgerChunkWithPenalty(ledger, nil, nil, maxSegments, budgetLiveBytes)
}

func stableVlogGenerationRewriteLedgerSegments(prev, planned []backenddb.ValueLogRewritePlanSegment) []backenddb.ValueLogRewritePlanSegment {
	if len(prev) == 0 || len(planned) == 0 {
		return nil
	}
	seen := make(map[uint32]struct{}, len(prev))
	for _, seg := range prev {
		if seg.FileID == 0 || seg.BytesLive <= 0 {
			continue
		}
		seen[seg.FileID] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]backenddb.ValueLogRewritePlanSegment, 0, len(planned))
	for _, seg := range planned {
		if seg.FileID == 0 || seg.BytesLive <= 0 {
			continue
		}
		if _, ok := seen[seg.FileID]; !ok {
			continue
		}
		out = append(out, seg)
	}
	return out
}

func mergeVlogGenerationRewriteIDs(primary, secondary []uint32) []uint32 {
	if len(primary) == 0 && len(secondary) == 0 {
		return nil
	}
	seen := make(map[uint32]struct{}, len(primary)+len(secondary))
	out := make([]uint32, 0, len(primary)+len(secondary))
	for _, id := range primary {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, id := range secondary {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func mergeVlogGenerationRewriteLedgerSegments(primary, secondary []backenddb.ValueLogRewritePlanSegment) []backenddb.ValueLogRewritePlanSegment {
	if len(primary) == 0 && len(secondary) == 0 {
		return nil
	}
	seen := make(map[uint32]struct{}, len(primary)+len(secondary))
	out := make([]backenddb.ValueLogRewritePlanSegment, 0, len(primary)+len(secondary))
	for _, seg := range primary {
		if seg.FileID == 0 {
			continue
		}
		if _, ok := seen[seg.FileID]; ok {
			continue
		}
		seen[seg.FileID] = struct{}{}
		out = append(out, seg)
	}
	for _, seg := range secondary {
		if seg.FileID == 0 {
			continue
		}
		if _, ok := seen[seg.FileID]; ok {
			continue
		}
		seen[seg.FileID] = struct{}{}
		out = append(out, seg)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

type valueLogGenerationRewriteChunkKey struct {
	FileID      uint32
	ChunkOffset int64
}

func vlogGenerationRewriteChunkLedgerChunk(chunks []backenddb.ValueLogRewritePlanChunk, maxSegments int, budgetLiveBytes int64) []backenddb.ValueLogRewritePlanChunk {
	if len(chunks) == 0 || maxSegments <= 0 {
		return nil
	}

	candidates := make([]backenddb.ValueLogRewritePlanChunk, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk.FileID == 0 || chunk.BytesLive <= 0 {
			continue
		}
		candidates = append(candidates, chunk)
	}
	if len(candidates) == 0 {
		return nil
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
		if a.StaleRatio != b.StaleRatio {
			return a.StaleRatio > b.StaleRatio
		}
		if a.BytesStale != b.BytesStale {
			return a.BytesStale > b.BytesStale
		}
		if a.BytesLive != b.BytesLive {
			return a.BytesLive < b.BytesLive
		}
		if a.FileID != b.FileID {
			return a.FileID < b.FileID
		}
		return a.ChunkOffset < b.ChunkOffset
	})

	selected := make([]backenddb.ValueLogRewritePlanChunk, 0, len(candidates))
	selectedIDs := make(map[uint32]struct{}, maxSegments)
	remaining := budgetLiveBytes
	for _, chunk := range candidates {
		if chunk.FileID == 0 {
			continue
		}
		if _, ok := selectedIDs[chunk.FileID]; !ok && len(selectedIDs) >= maxSegments {
			continue
		}
		if budgetLiveBytes > 0 && remaining <= 0 && len(selected) > 0 {
			break
		}
		live := chunk.BytesLive
		if live < 0 {
			live = 0
		}
		if budgetLiveBytes > 0 && remaining > 0 && live > remaining && len(selected) > 0 {
			continue
		}
		selected = append(selected, chunk)
		selectedIDs[chunk.FileID] = struct{}{}
		if budgetLiveBytes > 0 && remaining > 0 {
			remaining -= live
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return selected
}

func stableVlogGenerationRewriteLedgerChunks(prev, planned []backenddb.ValueLogRewritePlanChunk) []backenddb.ValueLogRewritePlanChunk {
	if len(prev) == 0 || len(planned) == 0 {
		return nil
	}
	seen := make(map[valueLogGenerationRewriteChunkKey]struct{}, len(prev))
	for _, chunk := range prev {
		if chunk.FileID == 0 || chunk.BytesLive <= 0 {
			continue
		}
		seen[valueLogGenerationRewriteChunkKey{FileID: chunk.FileID, ChunkOffset: chunk.ChunkOffset}] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]backenddb.ValueLogRewritePlanChunk, 0, len(planned))
	for _, chunk := range planned {
		if chunk.FileID == 0 || chunk.BytesLive <= 0 {
			continue
		}
		if _, ok := seen[valueLogGenerationRewriteChunkKey{FileID: chunk.FileID, ChunkOffset: chunk.ChunkOffset}]; !ok {
			continue
		}
		out = append(out, chunk)
	}
	return out
}

func stableVlogGenerationRewriteLedgerChunksWithSegmentFallback(prevChunks []backenddb.ValueLogRewritePlanChunk, prevSegments []backenddb.ValueLogRewritePlanSegment, planned []backenddb.ValueLogRewritePlanChunk) []backenddb.ValueLogRewritePlanChunk {
	if len(planned) == 0 {
		return nil
	}
	if len(prevChunks) > 0 {
		return stableVlogGenerationRewriteLedgerChunks(prevChunks, planned)
	}
	if len(prevSegments) == 0 {
		return nil
	}
	seen := make(map[uint32]struct{}, len(prevSegments))
	for _, seg := range prevSegments {
		if seg.FileID == 0 || seg.BytesLive <= 0 {
			continue
		}
		seen[seg.FileID] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]backenddb.ValueLogRewritePlanChunk, 0, len(planned))
	for _, chunk := range planned {
		if chunk.FileID == 0 || chunk.BytesLive <= 0 {
			continue
		}
		if _, ok := seen[chunk.FileID]; !ok {
			continue
		}
		out = append(out, chunk)
	}
	return out
}

func (db *DB) consumeVlogGenerationRewriteChunkLedger(processed []backenddb.ValueLogRewritePlanChunk) error {
	if db == nil || len(processed) == 0 {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	if len(db.vlogGenerationRewriteChunkLedger) == 0 {
		return nil
	}
	processedSet := make(map[valueLogGenerationRewriteChunkKey]struct{}, len(processed))
	for _, chunk := range processed {
		if chunk.FileID == 0 {
			continue
		}
		processedSet[valueLogGenerationRewriteChunkKey{FileID: chunk.FileID, ChunkOffset: chunk.ChunkOffset}] = struct{}{}
	}
	if len(processedSet) == 0 {
		return nil
	}
	remainingChunks := make([]backenddb.ValueLogRewritePlanChunk, 0, len(db.vlogGenerationRewriteChunkLedger))
	for _, chunk := range db.vlogGenerationRewriteChunkLedger {
		if _, ok := processedSet[valueLogGenerationRewriteChunkKey{FileID: chunk.FileID, ChunkOffset: chunk.ChunkOffset}]; ok {
			continue
		}
		remainingChunks = append(remainingChunks, chunk)
	}
	remainingIDs := vlogGenerationRewriteChunkLedgerIDs(remainingChunks)
	stagePending := db.vlogGenerationRewriteStagePending && len(remainingChunks) > 0
	stageObservedAt := db.vlogGenerationRewriteStageObservedUnixNano
	if !stagePending {
		stageObservedAt = 0
	}
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), remainingIDs, nil, remainingChunks, db.vlogGenerationRewriteChunkBytes, retainVlogGenerationRewriteHistory(remainingIDs, db.vlogGenerationRewriteHistory), db.vlogGenerationRewritePenalties, stagePending, stageObservedAt); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = remainingIDs
	db.vlogGenerationRewriteLedger = nil
	db.vlogGenerationRewriteChunkLedger = remainingChunks
	db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteChunkLedgerByFileID(remainingChunks)
	db.vlogGenerationRewriteStagePending = stagePending
	db.vlogGenerationRewriteStageObservedUnixNano = stageObservedAt
	if stagePending && stageObservedAt > 0 {
		db.scheduleVlogGenerationRewriteStageConfirmation(stageObservedAt)
	} else {
		db.clearVlogGenerationRewriteStageConfirmation(true)
	}
	return nil
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
	if len(db.vlogGenerationRewriteChunkLedger) > 0 {
		return nil
	}
	remaining := db.vlogGenerationRewriteQueue
	remainingLedger := db.vlogGenerationRewriteLedger
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
			ledgerPrefixMatch := len(remainingLedger) >= len(processed)
			for i := range processed {
				if !ledgerPrefixMatch || remainingLedger[i].FileID != processed[i] {
					ledgerPrefixMatch = false
					break
				}
			}
			if ledgerPrefixMatch {
				remainingLedger = append([]backenddb.ValueLogRewritePlanSegment(nil), remainingLedger[len(processed):]...)
			} else if len(remainingLedger) > 0 {
				processedSet := make(map[uint32]struct{}, len(processed))
				for _, id := range processed {
					processedSet[id] = struct{}{}
				}
				filteredLedger := make([]backenddb.ValueLogRewritePlanSegment, 0, len(remainingLedger))
				for _, seg := range remainingLedger {
					if _, ok := processedSet[seg.FileID]; ok {
						continue
					}
					filteredLedger = append(filteredLedger, seg)
				}
				remainingLedger = filteredLedger
			}
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
			if len(remainingLedger) > 0 {
				filteredLedger := make([]backenddb.ValueLogRewritePlanSegment, 0, len(remainingLedger))
				for _, seg := range remainingLedger {
					if _, ok := processedSet[seg.FileID]; ok {
						continue
					}
					filteredLedger = append(filteredLedger, seg)
				}
				remainingLedger = filteredLedger
			}
		}
	}
	stagePending := db.vlogGenerationRewriteStagePending && len(remainingLedger) > 0
	stageObservedAt := db.vlogGenerationRewriteStageObservedUnixNano
	if !stagePending {
		stageObservedAt = 0
	}
	nextHistory := retainVlogGenerationRewriteHistory(remaining, db.vlogGenerationRewriteHistory)
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), remaining, remainingLedger, nil, 0, nextHistory, db.vlogGenerationRewritePenalties, stagePending, stageObservedAt); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = remaining
	db.vlogGenerationRewriteLedger = remainingLedger
	db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteLedgerByFileID(remainingLedger)
	db.vlogGenerationRewriteHistory = nextHistory
	db.vlogGenerationRewriteStagePending = stagePending
	db.vlogGenerationRewriteStageObservedUnixNano = stageObservedAt
	if stagePending && stageObservedAt > 0 {
		db.scheduleVlogGenerationRewriteStageConfirmation(stageObservedAt)
	} else {
		db.clearVlogGenerationRewriteStageConfirmation(true)
	}
	return nil
}

func (db *DB) restageVlogGenerationRewriteQueueRemaining(observedAt int64) (int, bool, error) {
	if db == nil {
		return 0, false, nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return 0, false, err
	}
	if len(db.vlogGenerationRewriteChunkLedger) > 0 && len(db.vlogGenerationRewriteQueue) > 0 {
		if observedAt <= 0 {
			observedAt = time.Now().UnixNano()
		}
		if err := saveValueLogGenerationRewriteState(
			db.valueLogGenerationStatePath(),
			append([]uint32(nil), db.vlogGenerationRewriteQueue...),
			nil,
			append([]backenddb.ValueLogRewritePlanChunk(nil), db.vlogGenerationRewriteChunkLedger...),
			db.vlogGenerationRewriteChunkBytes,
			db.vlogGenerationRewriteHistory,
			db.vlogGenerationRewritePenalties,
			true,
			observedAt,
		); err != nil {
			return 0, false, err
		}
		db.vlogGenerationRewriteStagePending = true
		db.vlogGenerationRewriteStageObservedUnixNano = observedAt
		db.scheduleVlogGenerationRewriteStageConfirmation(observedAt)
		return len(db.vlogGenerationRewriteQueue), true, nil
	}
	if len(db.vlogGenerationRewriteLedger) == 0 || len(db.vlogGenerationRewriteQueue) == 0 {
		return len(db.vlogGenerationRewriteQueue), false, nil
	}
	if observedAt <= 0 {
		observedAt = time.Now().UnixNano()
	}
	if err := saveValueLogGenerationRewriteState(
		db.valueLogGenerationStatePath(),
		append([]uint32(nil), db.vlogGenerationRewriteQueue...),
		append([]backenddb.ValueLogRewritePlanSegment(nil), db.vlogGenerationRewriteLedger...),
		nil,
		0,
		db.vlogGenerationRewriteHistory,
		db.vlogGenerationRewritePenalties,
		true,
		observedAt,
	); err != nil {
		return 0, false, err
	}
	db.vlogGenerationRewriteStagePending = true
	db.vlogGenerationRewriteStageObservedUnixNano = observedAt
	db.scheduleVlogGenerationRewriteStageConfirmation(observedAt)
	return len(db.vlogGenerationRewriteQueue), true, nil
}

func (db *DB) recordVlogGenerationRewritePenalty(ids []uint32, cooldownUntil time.Time, growth int64) error {
	return db.recordVlogGenerationRewritePenaltyWithLedger(ids, nil, cooldownUntil, growth)
}

func (db *DB) recordVlogGenerationRewritePenaltyWithLedger(ids []uint32, ledger []backenddb.ValueLogRewritePlanSegment, cooldownUntil time.Time, growth int64) error {
	if db == nil || len(ids) == 0 {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	if db.vlogGenerationRewritePenalties == nil {
		db.vlogGenerationRewritePenalties = make(map[uint32]valueLogGenerationRewritePenalty, len(ids))
	}
	ledgerByFileID := buildVlogGenerationRewriteLedgerByFileID(ledger)
	untilNS := cooldownUntil.UnixNano()
	for _, id := range ids {
		if id == 0 {
			continue
		}
		penalty := db.vlogGenerationRewritePenalties[id]
		penalty.Attempts++
		if untilNS > penalty.CooldownUntilUnixNano {
			penalty.CooldownUntilUnixNano = untilNS
		}
		penalty.LastGrowthBytes = growth
		if seg, ok := ledgerByFileID[id]; ok && seg.BytesStale > 0 {
			penalty.LastStaleBytes = seg.BytesStale
		}
		db.vlogGenerationRewritePenalties[id] = penalty
	}
	return saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), db.vlogGenerationRewriteQueue, db.vlogGenerationRewriteLedger, db.vlogGenerationRewriteChunkLedger, db.vlogGenerationRewriteChunkBytes, db.vlogGenerationRewriteHistory, db.vlogGenerationRewritePenalties, db.vlogGenerationRewriteStagePending, db.vlogGenerationRewriteStageObservedUnixNano)
}

func (db *DB) clearVlogGenerationRewritePenalty(ids []uint32) error {
	if db == nil || len(ids) == 0 {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	if len(db.vlogGenerationRewritePenalties) == 0 {
		return nil
	}
	changed := false
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := db.vlogGenerationRewritePenalties[id]; ok {
			delete(db.vlogGenerationRewritePenalties, id)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), db.vlogGenerationRewriteQueue, db.vlogGenerationRewriteLedger, db.vlogGenerationRewriteChunkLedger, db.vlogGenerationRewriteChunkBytes, db.vlogGenerationRewriteHistory, db.vlogGenerationRewritePenalties, db.vlogGenerationRewriteStagePending, db.vlogGenerationRewriteStageObservedUnixNano)
}

func (db *DB) currentVlogGenerationRewritePenalties() (map[uint32]valueLogGenerationRewritePenalty, error) {
	if db == nil {
		return nil, nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return nil, err
	}
	out := make(map[uint32]valueLogGenerationRewritePenalty, len(db.vlogGenerationRewritePenalties))
	for id, penalty := range db.vlogGenerationRewritePenalties {
		out[id] = penalty
	}
	return out, nil
}

func (db *DB) recordVlogGenerationRewriteHistoryWithLedger(ids []uint32, ledger []backenddb.ValueLogRewritePlanSegment, sourceBytesUnreferenced int64, reclaimedBytes int64, attemptedAt time.Time) error {
	if db == nil || len(ids) == 0 {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	if db.vlogGenerationRewriteHistory == nil {
		db.vlogGenerationRewriteHistory = make(map[uint32]valueLogGenerationRewriteHistory, len(ids))
	}
	ledgerByFileID := buildVlogGenerationRewriteLedgerByFileID(ledger)
	allocOrder := make([]uint32, 0, len(ids))
	totalLive := int64(0)
	for _, id := range ids {
		if id == 0 {
			continue
		}
		allocOrder = append(allocOrder, id)
		if seg, ok := ledgerByFileID[id]; ok && seg.BytesLive > 0 {
			totalLive += seg.BytesLive
		}
	}
	allocUsefulRemaining := sourceBytesUnreferenced
	allocReclaimedRemaining := reclaimedBytes
	for i, id := range allocOrder {
		entry := db.vlogGenerationRewriteHistory[id]
		entry.LastAttemptUnixNano = attemptedAt.UnixNano()
		if seg, ok := ledgerByFileID[id]; ok {
			if seg.BytesLive > 0 {
				entry.LastProcessedLiveBytes = seg.BytesLive
			}
			if seg.BytesStale > 0 {
				entry.LastStaleBytes = seg.BytesStale
			}
		}
		usefulShare := int64(0)
		reclaimedShare := int64(0)
		if totalLive > 0 {
			live := int64(0)
			if seg, ok := ledgerByFileID[id]; ok && seg.BytesLive > 0 {
				live = seg.BytesLive
			}
			if i == len(allocOrder)-1 {
				usefulShare = allocUsefulRemaining
				reclaimedShare = allocReclaimedRemaining
			} else if live > 0 {
				usefulShare = sourceBytesUnreferenced * live / totalLive
				reclaimedShare = reclaimedBytes * live / totalLive
				if usefulShare > allocUsefulRemaining {
					usefulShare = allocUsefulRemaining
				}
				if reclaimedShare > allocReclaimedRemaining {
					reclaimedShare = allocReclaimedRemaining
				}
			}
		}
		if usefulShare < 0 {
			usefulShare = 0
		}
		if reclaimedShare < 0 {
			reclaimedShare = 0
		}
		allocUsefulRemaining -= usefulShare
		allocReclaimedRemaining -= reclaimedShare
		entry.LastSourceBytesUnreferenced = usefulShare
		entry.LastReclaimedBytes = reclaimedShare
		db.vlogGenerationRewriteHistory[id] = entry
	}
	return saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), db.vlogGenerationRewriteQueue, db.vlogGenerationRewriteLedger, db.vlogGenerationRewriteChunkLedger, db.vlogGenerationRewriteChunkBytes, db.vlogGenerationRewriteHistory, db.vlogGenerationRewritePenalties, db.vlogGenerationRewriteStagePending, db.vlogGenerationRewriteStageObservedUnixNano)
}
