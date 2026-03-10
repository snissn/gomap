package caching

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadValueLogGenerationRewriteDebt_LegacyQueueOnlyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, valueLogGenerationStateFileName)
	data := []byte(`{"rewrite_source_file_ids":["11","22"]}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}

	debt, err := loadValueLogGenerationRewriteDebt(path)
	if err != nil {
		t.Fatalf("load debt: %v", err)
	}
	if len(debt) != 2 {
		t.Fatalf("debt len=%d want=2", len(debt))
	}
	if debt[0].FileID != 11 || debt[1].FileID != 22 {
		t.Fatalf("debt file ids=%v want [11 22]", vlogGenerationRewriteDebtFileIDs(debt))
	}
	if debt[0].BytesLive != 0 || debt[1].BytesLive != 0 {
		t.Fatalf("legacy queue should not synthesize debt bytes: %+v", debt)
	}
}
