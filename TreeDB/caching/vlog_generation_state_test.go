package caching

import (
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestLoadValueLogGenerationRewriteState_SkipsZeroFileID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vlog_generation_state.json")

	// FileID==0 is not a valid segment ID. Ensure the loader filters it out so a
	// persisted state cannot force the scheduler into an inconsistent queue.
	data := []byte(`{
  "rewrite_source_file_ids": ["0", "1"],
  "rewrite_debt_ledger": [
    {"file_id": "0", "bytes_total": 1, "bytes_live": 1, "bytes_stale": 0, "stale_ratio": 0.0},
    {"file_id": "1", "bytes_total": 128, "bytes_live": 64, "bytes_stale": 64, "stale_ratio": 0.5}
  ]
}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	ids, ledger, chunkLedger, chunkBytes, history, penalties, stagePending, stageObservedAt, err := loadValueLogGenerationRewriteState(path)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("ids=%v want=[1]", ids)
	}
	if len(ledger) != 1 || ledger[0].FileID != 1 {
		t.Fatalf("ledger=%v want single FileID=1", ledger)
	}
	if len(chunkLedger) != 0 || chunkBytes != 0 {
		t.Fatalf("chunkLedger=%v chunkBytes=%d want empty/zero", chunkLedger, chunkBytes)
	}
	if len(history) != 0 {
		t.Fatalf("history=%v want empty", history)
	}
	if len(penalties) != 0 {
		t.Fatalf("penalties=%v want empty", penalties)
	}
	if stagePending || stageObservedAt != 0 {
		t.Fatalf("stagePending=%t stageObservedAt=%d want zero values", stagePending, stageObservedAt)
	}
}

func TestLoadValueLogGenerationRewriteState_PreservesHistoryWithoutQueue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vlog_generation_state.json")

	data := []byte(`{
  "rewrite_history": [
    {"file_id": "0", "last_attempt_unix_nano": 1, "last_processed_live_bytes": 1},
    {"file_id": "7", "last_attempt_unix_nano": 1234, "last_processed_live_bytes": 64, "last_source_bytes_unreferenced": 32, "last_reclaimed_bytes": 16, "last_stale_bytes": 48}
  ]
}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	ids, ledger, chunkLedger, chunkBytes, history, penalties, stagePending, stageObservedAt, err := loadValueLogGenerationRewriteState(path)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("ids=%v want empty", ids)
	}
	if len(ledger) != 0 {
		t.Fatalf("ledger=%v want empty", ledger)
	}
	if len(chunkLedger) != 0 || chunkBytes != 0 {
		t.Fatalf("chunkLedger=%v chunkBytes=%d want empty/zero", chunkLedger, chunkBytes)
	}
	if len(history) != 1 {
		t.Fatalf("history=%v want one valid entry", history)
	}
	entry, ok := history[7]
	if !ok {
		t.Fatalf("history=%v want file 7", history)
	}
	if entry.LastAttemptUnixNano != 1234 || entry.LastProcessedLiveBytes != 64 || entry.LastSourceBytesUnreferenced != 32 || entry.LastReclaimedBytes != 16 || entry.LastStaleBytes != 48 {
		t.Fatalf("history entry=%+v want attempt=1234 processed=64 useful=32 reclaimed=16 stale=48", entry)
	}
	if len(penalties) != 0 {
		t.Fatalf("penalties=%v want empty", penalties)
	}
	if stagePending || stageObservedAt != 0 {
		t.Fatalf("stagePending=%t stageObservedAt=%d want zero values", stagePending, stageObservedAt)
	}
}

func TestLoadValueLogGenerationRewriteState_PreservesPenaltiesWithoutQueue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vlog_generation_state.json")

	data := []byte(`{
  "rewrite_penalties": [
    {"file_id": "0", "attempts": 1, "cooldown_until_unix_nano": 1, "last_growth_bytes": 1},
    {"file_id": "7", "attempts": 2, "cooldown_until_unix_nano": 1234, "last_growth_bytes": 5678}
  ]
}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	ids, ledger, chunkLedger, chunkBytes, history, penalties, stagePending, stageObservedAt, err := loadValueLogGenerationRewriteState(path)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("ids=%v want empty", ids)
	}
	if len(ledger) != 0 {
		t.Fatalf("ledger=%v want empty", ledger)
	}
	if len(chunkLedger) != 0 || chunkBytes != 0 {
		t.Fatalf("chunkLedger=%v chunkBytes=%d want empty/zero", chunkLedger, chunkBytes)
	}
	if len(history) != 0 {
		t.Fatalf("history=%v want empty", history)
	}
	if len(penalties) != 1 {
		t.Fatalf("penalties=%v want one valid entry", penalties)
	}
	penalty, ok := penalties[7]
	if !ok {
		t.Fatalf("penalties=%v want file 7", penalties)
	}
	if penalty.Attempts != 2 || penalty.CooldownUntilUnixNano != 1234 || penalty.LastGrowthBytes != 5678 {
		t.Fatalf("penalty=%+v want attempts=2 cooldown=1234 growth=5678", penalty)
	}
	if stagePending || stageObservedAt != 0 {
		t.Fatalf("stagePending=%t stageObservedAt=%d want zero values", stagePending, stageObservedAt)
	}
}

func TestConsumeVlogGenerationRewriteQueueChunkPreservesMismatchedLedger(t *testing.T) {
	dir := t.TempDir()
	db := &DB{dir: filepath.Join(dir, "db")}

	// Older states can have a queue prefix that is not represented in the
	// ledger. Consuming that prefix must remove by file ID, not by count, or the
	// remaining queued debt loses the ledger quality data used for prioritizing
	// retries.
	if err := saveValueLogGenerationRewriteState(
		db.valueLogGenerationStatePath(),
		[]uint32{33, 11, 22},
		[]backenddb.ValueLogRewritePlanSegment{
			{FileID: 11, BytesTotal: 128, BytesLive: 64, BytesStale: 64, StaleRatio: 0.5},
			{FileID: 22, BytesTotal: 128, BytesLive: 64, BytesStale: 64, StaleRatio: 0.5},
		},
		nil,
		0,
		nil,
		nil,
		false,
		0,
	); err != nil {
		t.Fatalf("save state: %v", err)
	}

	if err := db.consumeVlogGenerationRewriteQueueChunk([]uint32{33}); err != nil {
		t.Fatalf("consume queue chunk: %v", err)
	}

	ids, ledger, chunkLedger, chunkBytes, _, _, stagePending, stageObservedAt, err := loadValueLogGenerationRewriteState(db.valueLogGenerationStatePath())
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(ids) != 2 || ids[0] != 11 || ids[1] != 22 {
		t.Fatalf("ids=%v want [11 22]", ids)
	}
	if len(ledger) != 2 || ledger[0].FileID != 11 || ledger[1].FileID != 22 {
		t.Fatalf("ledger=%v want file IDs [11 22]", ledger)
	}
	if len(chunkLedger) != 0 || chunkBytes != 0 {
		t.Fatalf("chunkLedger=%v chunkBytes=%d want empty/zero", chunkLedger, chunkBytes)
	}
	if stagePending || stageObservedAt != 0 {
		t.Fatalf("stagePending=%t stageObservedAt=%d want zero values", stagePending, stageObservedAt)
	}
}

func TestLoadValueLogGenerationRewriteState_LoadsChunkLedger(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vlog_generation_state.json")

	data := []byte(`{
  "rewrite_source_file_ids": ["1", "2"],
  "rewrite_debt_chunks": [
    {"file_id": "1", "chunk_offset": 0, "bytes_total": 128, "bytes_live": 64, "bytes_stale": 64, "stale_ratio": 0.5},
    {"file_id": "2", "chunk_offset": 16777216, "bytes_total": 256, "bytes_live": 96, "bytes_stale": 160, "stale_ratio": 0.625}
  ],
  "rewrite_debt_chunk_bytes": 16777216,
  "rewrite_stage_pending": true,
  "rewrite_stage_observed_unix_nano": 12345
}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	ids, ledger, chunkLedger, chunkBytes, history, penalties, stagePending, stageObservedAt, err := loadValueLogGenerationRewriteState(path)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("ids=%v want=[1 2]", ids)
	}
	if len(ledger) != 0 {
		t.Fatalf("ledger=%v want empty", ledger)
	}
	if len(chunkLedger) != 2 {
		t.Fatalf("chunkLedger=%v want two entries", chunkLedger)
	}
	if chunkBytes != 16777216 {
		t.Fatalf("chunkBytes=%d want 16777216", chunkBytes)
	}
	if len(history) != 0 {
		t.Fatalf("history=%v want empty", history)
	}
	if len(penalties) != 0 {
		t.Fatalf("penalties=%v want empty", penalties)
	}
	if !stagePending || stageObservedAt != 12345 {
		t.Fatalf("stagePending=%t stageObservedAt=%d want true/12345", stagePending, stageObservedAt)
	}
}

func TestPrioritizeVlogGenerationRewriteIDs_PrefersUsefulThenUnknownThenZeroYield(t *testing.T) {
	ids := []uint32{11, 22, 33}
	history := map[uint32]valueLogGenerationRewriteHistory{
		11: {LastAttemptUnixNano: 1, LastProcessedLiveBytes: 64, LastStaleBytes: 64},
		22: {LastAttemptUnixNano: 2, LastProcessedLiveBytes: 64, LastSourceBytesUnreferenced: 32, LastReclaimedBytes: 16, LastStaleBytes: 64},
	}

	got := prioritizeVlogGenerationRewriteIDs(ids, history, nil)
	want := []uint32{22, 33, 11}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("prioritized ids=%v want=%v", got, want)
	}
}

func TestPrioritizeVlogGenerationRewriteLedger_PrefersUsefulThenImprovedThenUnknownThenZeroYield(t *testing.T) {
	ledger := []backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.5},
		{FileID: 22, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.5},
		{FileID: 33, BytesLive: 64, BytesTotal: 128, BytesStale: 96, StaleRatio: 0.75},
		{FileID: 44, BytesLive: 64, BytesTotal: 128, BytesStale: 80, StaleRatio: 0.625},
	}
	history := map[uint32]valueLogGenerationRewriteHistory{
		11: {LastAttemptUnixNano: 1, LastProcessedLiveBytes: 64, LastStaleBytes: 64},
		22: {LastAttemptUnixNano: 2, LastProcessedLiveBytes: 64, LastSourceBytesUnreferenced: 32, LastReclaimedBytes: 16, LastStaleBytes: 64},
		33: {LastAttemptUnixNano: 3, LastProcessedLiveBytes: 64, LastStaleBytes: 48},
	}

	got := prioritizeVlogGenerationRewriteLedger(ledger, history, nil)
	gotIDs := vlogGenerationRewriteLedgerIDs(got)
	want := []uint32{22, 33, 44, 11}
	if len(gotIDs) != len(want) || gotIDs[0] != want[0] || gotIDs[1] != want[1] || gotIDs[2] != want[2] || gotIDs[3] != want[3] {
		t.Fatalf("prioritized ledger ids=%v want=%v", gotIDs, want)
	}
}
