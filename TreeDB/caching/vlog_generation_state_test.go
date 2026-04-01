package caching

import (
	"os"
	"path/filepath"
	"testing"
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

	ids, ledger, chunkLedger, chunkBytes, penalties, stagePending, stageObservedAt, err := loadValueLogGenerationRewriteState(path)
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

	ids, ledger, chunkLedger, chunkBytes, penalties, stagePending, stageObservedAt, err := loadValueLogGenerationRewriteState(path)
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

	ids, ledger, chunkLedger, chunkBytes, penalties, stagePending, stageObservedAt, err := loadValueLogGenerationRewriteState(path)
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
	if len(penalties) != 0 {
		t.Fatalf("penalties=%v want empty", penalties)
	}
	if !stagePending || stageObservedAt != 12345 {
		t.Fatalf("stagePending=%t stageObservedAt=%d want true/12345", stagePending, stageObservedAt)
	}
}
