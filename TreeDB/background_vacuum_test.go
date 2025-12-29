package treedb

import (
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestBackgroundIndexVacuumRunsAndStops(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                               dir,
		KeepRecent:                        1,
		BackgroundIndexVacuumInterval:     5 * time.Millisecond,
		BackgroundIndexVacuumSpanRatioPPM: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		val := []byte("v")
		if err := d.Set(key, val); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	var vacuums uint64
	for time.Now().Before(deadline) {
		stats := d.Stats()
		vacuumsStr := stats["treedb.bg_vacuum.vacuums"]
		if vacuumsStr != "" {
			vacuums, _ = strconv.ParseUint(vacuumsStr, 10, 64)
			if vacuums > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if vacuums == 0 {
		t.Fatalf("expected background vacuum to run")
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
