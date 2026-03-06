package treedb_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

func TestOpenBackend_WALOffReopen_AllowsOfflineValueLogMaintenance(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.Options{
		Dir:               dir,
		DisableSideStores: true,
		Durability:        treedb.DurabilityWALOffRelaxed,
		ValueLog: treedb.ValueLogOptions{
			ForcePointers:    true,
			PointerThreshold: 1,
		},
	}

	writeSession := func(prefix string, start, count int) {
		db, err := treedb.Open(opts)
		if err != nil {
			t.Fatalf("open(%s): %v", prefix, err)
		}
		for i := 0; i < count; i++ {
			key := []byte(fmt.Sprintf("%s-%04d", prefix, i))
			val := bytes.Repeat([]byte{byte(start + i)}, 4096)
			if err := db.Set(key, val); err != nil {
				_ = db.Close()
				t.Fatalf("set(%s,%d): %v", prefix, i, err)
			}
		}
		if err := db.Checkpoint(); err != nil {
			_ = db.Close()
			t.Fatalf("checkpoint(%s): %v", prefix, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close(%s): %v", prefix, err)
		}
	}

	writeSession("first", 1, 8)
	writeSession("second", 16, 8)

	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dir, DisableSideStores: true})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() {
		if err := cleanup(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})

	stats, err := backend.ValueLogGC(context.Background(), treedbdb.ValueLogGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("ValueLogGC(DryRun): %v", err)
	}
	if stats.SegmentsTotal == 0 {
		t.Fatalf("expected value-log segments in GC stats: %+v", stats)
	}
}
