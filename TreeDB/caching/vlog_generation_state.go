package caching

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
)

const valueLogGenerationStateFileName = "vlog_generation_state.json"

const vlogGenerationRewriteResumeMaxSegments = 1

const vlogGenerationRewriteResumeMinInterval = 1 * time.Second

type valueLogGenerationStateFile struct {
	RewriteSourceFileIDs []string                                   `json:"rewrite_source_file_ids,omitempty"`
	RewriteDebtLedger    []valueLogGenerationRewriteDebtLedgerEntry `json:"rewrite_debt_ledger,omitempty"`
	RewritePenalties     []valueLogGenerationRewritePenaltyEntry    `json:"rewrite_penalties,omitempty"`
	RewriteStagePending  bool                                       `json:"rewrite_stage_pending,omitempty"`
	RewriteStageObserved int64                                      `json:"rewrite_stage_observed_unix_nano,omitempty"`
}

type valueLogGenerationRewriteDebtLedgerEntry struct {
	FileID     string  `json:"file_id,omitempty"`
	BytesTotal int64   `json:"bytes_total,omitempty"`
	BytesLive  int64   `json:"bytes_live,omitempty"`
	BytesStale int64   `json:"bytes_stale,omitempty"`
	StaleRatio float64 `json:"stale_ratio,omitempty"`
}

type valueLogGenerationRewritePenaltyEntry struct {
	FileID                string `json:"file_id,omitempty"`
	Attempts              int    `json:"attempts,omitempty"`
	CooldownUntilUnixNano int64  `json:"cooldown_until_unix_nano,omitempty"`
	LastGrowthBytes       int64  `json:"last_growth_bytes,omitempty"`
}

type valueLogGenerationRewritePenalty struct {
	Attempts              int
	CooldownUntilUnixNano int64
	LastGrowthBytes       int64
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
	if len(raw.RewriteSourceFileIDs) == 0 && len(raw.RewriteDebtLedger) == 0 && len(raw.RewritePenalties) == 0 {
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

func loadValueLogGenerationRewriteState(path string) ([]uint32, []backenddb.ValueLogRewritePlanSegment, map[uint32]valueLogGenerationRewritePenalty, bool, int64, error) {
	raw, err := loadValueLogGenerationState(path)
	if err != nil {
		return nil, nil, nil, false, 0, err
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
		if e.Attempts <= 0 && e.CooldownUntilUnixNano <= 0 && e.LastGrowthBytes == 0 {
			continue
		}
		penalties[uint32(id64)] = valueLogGenerationRewritePenalty{
			Attempts:              e.Attempts,
			CooldownUntilUnixNano: e.CooldownUntilUnixNano,
			LastGrowthBytes:       e.LastGrowthBytes,
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

	if len(ledger) > 0 {
		ids := make([]uint32, 0, len(ledger))
		for _, seg := range ledger {
			ids = append(ids, seg.FileID)
		}
		return ids, ledger, penalties, raw.RewriteStagePending, raw.RewriteStageObserved, nil
	}

	if len(raw.RewriteSourceFileIDs) == 0 {
		return nil, nil, penalties, false, 0, nil
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
		return nil, nil, penalties, false, 0, nil
	}
	return out, nil, penalties, false, 0, nil
}

func saveValueLogGenerationRewriteState(path string, ids []uint32, ledger []backenddb.ValueLogRewritePlanSegment, penalties map[uint32]valueLogGenerationRewritePenalty, stagePending bool, stageObservedAt int64) error {
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
	raw.RewritePenalties = raw.RewritePenalties[:0]
	if len(penalties) > 0 {
		keys := make([]uint32, 0, len(penalties))
		for id, penalty := range penalties {
			if id == 0 {
				continue
			}
			if penalty.Attempts <= 0 && penalty.CooldownUntilUnixNano <= 0 && penalty.LastGrowthBytes == 0 {
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
	ids, ledger, penalties, stagePending, stageObservedAt, err := loadValueLogGenerationRewriteState(db.valueLogGenerationStatePath())
	if err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = ids
	db.vlogGenerationRewriteLedger = ledger
	db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteLedgerByFileID(ledger)
	db.vlogGenerationRewritePenalties = penalties
	db.vlogGenerationRewriteStagePending = stagePending
	db.vlogGenerationRewriteStageObservedUnixNano = stageObservedAt
	db.vlogGenerationRewriteQueueLoaded = true
	if stagePending && stageObservedAt > 0 {
		db.scheduleVlogGenerationRewriteStageConfirmation(stageObservedAt)
	} else {
		db.clearVlogGenerationRewriteStageConfirmation()
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
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), next, nil, db.vlogGenerationRewritePenalties, false, 0); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = next
	db.vlogGenerationRewriteLedger = nil
	db.vlogGenerationRewriteLedgerByFileID = nil
	db.vlogGenerationRewriteStagePending = false
	db.vlogGenerationRewriteStageObservedUnixNano = 0
	db.clearVlogGenerationRewriteStageConfirmation()
	return nil
}

func (db *DB) setVlogGenerationRewriteLedger(segments []backenddb.ValueLogRewritePlanSegment) error {
	return db.setVlogGenerationRewriteLedgerWithStage(segments, false, 0)
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
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), nextIDs, nextLedger, db.vlogGenerationRewritePenalties, stagePending, stageObservedAt); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = nextIDs
	db.vlogGenerationRewriteLedger = nextLedger
	db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteLedgerByFileID(nextLedger)
	db.vlogGenerationRewriteStagePending = stagePending
	db.vlogGenerationRewriteStageObservedUnixNano = stageObservedAt
	if stagePending && stageObservedAt > 0 {
		db.scheduleVlogGenerationRewriteStageConfirmation(stageObservedAt)
	} else {
		db.clearVlogGenerationRewriteStageConfirmation()
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
	return append([]backenddb.ValueLogRewritePlanSegment(nil), db.vlogGenerationRewriteLedger...), nil
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

func (db *DB) refreshVlogGenerationRewriteStageConfirmation(now time.Time) error {
	if db == nil {
		return nil
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		return err
	}
	if !db.vlogGenerationRewriteStagePending || db.vlogGenerationRewriteStageObservedUnixNano <= 0 || len(db.vlogGenerationRewriteLedger) == 0 {
		return nil
	}
	observedAt := now.UnixNano()
	if observedAt <= db.vlogGenerationRewriteStageObservedUnixNano {
		observedAt = db.vlogGenerationRewriteStageObservedUnixNano + 1
	}
	if err := saveValueLogGenerationRewriteState(
		db.valueLogGenerationStatePath(),
		db.vlogGenerationRewriteQueue,
		db.vlogGenerationRewriteLedger,
		db.vlogGenerationRewritePenalties,
		true,
		observedAt,
	); err != nil {
		return err
	}
	db.vlogGenerationRewriteStageObservedUnixNano = observedAt
	db.scheduleVlogGenerationRewriteStageConfirmation(observedAt)
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
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), filteredIDs, filteredLedger, db.vlogGenerationRewritePenalties, stagePending, stageObservedAt); err != nil {
		return nil, 0, err
	}
	db.vlogGenerationRewriteQueue = filteredIDs
	db.vlogGenerationRewriteLedger = filteredLedger
	db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteLedgerByFileID(filteredLedger)
	db.vlogGenerationRewriteStagePending = stagePending
	db.vlogGenerationRewriteStageObservedUnixNano = stageObservedAt
	if stagePending && stageObservedAt > 0 {
		db.scheduleVlogGenerationRewriteStageConfirmation(stageObservedAt)
	} else {
		db.clearVlogGenerationRewriteStageConfirmation()
	}
	return append([]uint32(nil), filteredIDs...), dropped, nil
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

func vlogGenerationRewriteLedgerChunk(ledger []backenddb.ValueLogRewritePlanSegment, maxSegments int, budgetLiveBytes int64) []uint32 {
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
	sort.SliceStable(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
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
			if len(remainingLedger) >= len(processed) {
				remainingLedger = append([]backenddb.ValueLogRewritePlanSegment(nil), remainingLedger[len(processed):]...)
			} else {
				remainingLedger = nil
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
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), remaining, remainingLedger, db.vlogGenerationRewritePenalties, stagePending, stageObservedAt); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = remaining
	db.vlogGenerationRewriteLedger = remainingLedger
	db.vlogGenerationRewriteLedgerByFileID = buildVlogGenerationRewriteLedgerByFileID(remainingLedger)
	db.vlogGenerationRewriteStagePending = stagePending
	db.vlogGenerationRewriteStageObservedUnixNano = stageObservedAt
	if stagePending && stageObservedAt > 0 {
		db.scheduleVlogGenerationRewriteStageConfirmation(stageObservedAt)
	} else {
		db.clearVlogGenerationRewriteStageConfirmation()
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
		db.vlogGenerationRewritePenalties[id] = penalty
	}
	return saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), db.vlogGenerationRewriteQueue, db.vlogGenerationRewriteLedger, db.vlogGenerationRewritePenalties, db.vlogGenerationRewriteStagePending, db.vlogGenerationRewriteStageObservedUnixNano)
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
