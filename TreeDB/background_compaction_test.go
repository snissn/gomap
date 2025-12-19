package treedb

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestBackgroundCompactionRunsAndStops(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                                   dir,
		KeepRecent:                            1,
		BackgroundCompactionInterval:          5 * time.Millisecond,
		BackgroundCompactionMaxSlabs:          1,
		BackgroundCompactionDeadRatio:         0.0,
		BackgroundCompactionMinBytes:          1,
		BackgroundCompactionMicroBatch:        32,
		BackgroundCompactionRotateBeforeWrite: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Create some slab bytes so candidates exist once we rotate.
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		val := make([]byte, 2048)
		if err := d.Set(key, val); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats := d.Stats()
		runsStr := stats["treedb.bg_compaction.runs"]
		if runsStr != "" {
			runs, _ := strconv.ParseUint(runsStr, 10, 64)
			if runs > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
