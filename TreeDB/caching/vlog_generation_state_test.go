package caching

import (
	"os"
	"path/filepath"
	"testing"
)

func TestVlogGenerationRewriteQueueChunk_UsesEstimatedLiveBytesBudget(t *testing.T) {
	entries := []valueLogGenerationRewriteQueueEntry{
		{FileID: 11, EstLiveBytes: 40},
		{FileID: 22, EstLiveBytes: 50},
		{FileID: 33, EstLiveBytes: 60},
	}

	if got, want := vlogGenerationRewriteQueueChunk(entries, 90), []valueLogGenerationRewriteQueueEntry{
		{FileID: 11, EstLiveBytes: 40},
		{FileID: 22, EstLiveBytes: 50},
	}; !equalRewriteQueueEntries(got, want) {
		t.Fatalf("chunk(90)=%v want=%v", got, want)
	}

	if got, want := vlogGenerationRewriteQueueChunk(entries, 89), []valueLogGenerationRewriteQueueEntry{
		{FileID: 11, EstLiveBytes: 40},
	}; !equalRewriteQueueEntries(got, want) {
		t.Fatalf("chunk(89)=%v want=%v", got, want)
	}

	if got := vlogGenerationRewriteQueueChunk(entries, 0); len(got) != 0 {
		t.Fatalf("chunk(0)=%v want empty", got)
	}

	if got, want := vlogGenerationRewriteQueueChunk([]valueLogGenerationRewriteQueueEntry{
		{FileID: 44, EstLiveBytes: 40},
		{FileID: 55, EstLiveBytes: 50},
	}, 1), []valueLogGenerationRewriteQueueEntry{
		{FileID: 44, EstLiveBytes: 40},
	}; !equalRewriteQueueEntries(got, want) {
		t.Fatalf("chunk(1)=%v want=%v", got, want)
	}

	if got, want := vlogGenerationRewriteQueueChunk([]valueLogGenerationRewriteQueueEntry{
		{FileID: 66},
		{FileID: 77, EstLiveBytes: 50},
	}, 90), []valueLogGenerationRewriteQueueEntry{
		{FileID: 66},
	}; !equalRewriteQueueEntries(got, want) {
		t.Fatalf("chunk(legacy)=%v want=%v", got, want)
	}
}

func TestLoadValueLogGenerationRewriteQueue_LegacySourceFileIDsFallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte(`{"rewrite_source_file_ids":["11","22"]}`), 0o644); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}

	queue, err := loadValueLogGenerationRewriteQueue(path)
	if err != nil {
		t.Fatalf("load legacy state: %v", err)
	}

	want := []valueLogGenerationRewriteQueueEntry{
		{FileID: 11},
		{FileID: 22},
	}
	if !equalRewriteQueueEntries(queue, want) {
		t.Fatalf("legacy queue=%v want=%v", queue, want)
	}
}
