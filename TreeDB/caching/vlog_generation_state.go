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
}

type valueLogGenerationRewriteDebtLedgerEntry struct {
	FileID     string  `json:"file_id,omitempty"`
	BytesTotal int64   `json:"bytes_total,omitempty"`
	BytesLive  int64   `json:"bytes_live,omitempty"`
	BytesStale int64   `json:"bytes_stale,omitempty"`
	StaleRatio float64 `json:"stale_ratio,omitempty"`
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
	if len(raw.RewriteSourceFileIDs) == 0 && len(raw.RewriteDebtLedger) == 0 {
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

func loadValueLogGenerationRewriteState(path string) ([]uint32, []backenddb.ValueLogRewritePlanSegment, error) {
	raw, err := loadValueLogGenerationState(path)
	if err != nil {
		return nil, nil, err
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
		return ids, ledger, nil
	}

	if len(raw.RewriteSourceFileIDs) == 0 {
		return nil, nil, nil
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
		return nil, nil, nil
	}
	return out, nil, nil
}

func saveValueLogGenerationRewriteState(path string, ids []uint32, ledger []backenddb.ValueLogRewritePlanSegment) error {
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
	return saveValueLogGenerationState(path, raw)
}

func (db *DB) loadVlogGenerationRewriteQueueLocked() error {
	if db == nil {
		return nil
	}
	if db.vlogGenerationRewriteQueueLoaded {
		return nil
	}
	ids, ledger, err := loadValueLogGenerationRewriteState(db.valueLogGenerationStatePath())
	if err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = ids
	db.vlogGenerationRewriteLedger = ledger
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
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), next, nil); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = next
	db.vlogGenerationRewriteLedger = nil
	return nil
}

func (db *DB) setVlogGenerationRewriteLedger(segments []backenddb.ValueLogRewritePlanSegment) error {
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
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), nextIDs, nextLedger); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = nextIDs
	db.vlogGenerationRewriteLedger = nextLedger
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
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), filteredIDs, filteredLedger); err != nil {
		return nil, 0, err
	}
	db.vlogGenerationRewriteQueue = filteredIDs
	db.vlogGenerationRewriteLedger = filteredLedger
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
	if err := saveValueLogGenerationRewriteState(db.valueLogGenerationStatePath(), remaining, remainingLedger); err != nil {
		return err
	}
	db.vlogGenerationRewriteQueue = remaining
	db.vlogGenerationRewriteLedger = remainingLedger
	return nil
}
