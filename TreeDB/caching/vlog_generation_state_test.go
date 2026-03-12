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

	ids, ledger, err := loadValueLogGenerationRewriteState(path)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("ids=%v want=[1]", ids)
	}
	if len(ledger) != 1 || ledger[0].FileID != 1 {
		t.Fatalf("ledger=%v want single FileID=1", ledger)
	}
}
