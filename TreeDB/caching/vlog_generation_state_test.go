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

	ids, ledger, penalties, err := loadValueLogGenerationRewriteState(path)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("ids=%v want=[1]", ids)
	}
	if len(ledger) != 1 || ledger[0].FileID != 1 {
		t.Fatalf("ledger=%v want single FileID=1", ledger)
	}
	if len(penalties) != 0 {
		t.Fatalf("penalties=%v want empty", penalties)
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

	ids, ledger, penalties, err := loadValueLogGenerationRewriteState(path)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("ids=%v want empty", ids)
	}
	if len(ledger) != 0 {
		t.Fatalf("ledger=%v want empty", ledger)
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
}
