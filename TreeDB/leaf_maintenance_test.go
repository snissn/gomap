package treedb

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func leafMaintenanceOptions(dir string) Options {
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.MemtableMode = "cow_ordered"
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.FlushThreshold = 64 << 20
	opts.IndexOuterLeavesInValueLog = true
	opts.FlushAdmissionPolicy = FlushAdmissionPolicyExplicit
	opts.FlushApplyConcurrency = 3
	opts.FlushApplyMinEntries = 1
	opts.FlushApplyMinSpans = 1
	opts.FlushApplyMinBytes = 1
	opts.FlushApplySpanNative = true
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	return opts
}

func leafMaintenanceWrite(t *testing.T, db *DB, round int) {
	t.Helper()
	b := db.NewBatch()
	defer b.Close()
	for i := 0; i < 4096; i++ {
		if err := b.Set([]byte(fmt.Sprintf("leaf-maintenance-%08d", i)), bytes.Repeat([]byte{byte(round + i%251)}, 128)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
}

func leafMaintenanceFiles(t *testing.T, dir string) map[string]os.FileInfo {
	t.Helper()
	out := make(map[string]os.FileInfo)
	err := filepath.WalkDir(dir, func(path string, ent os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !ent.IsDir() && strings.HasSuffix(ent.Name(), ".log") {
			info, err := ent.Info()
			if err != nil {
				return err
			}
			out[path] = info
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// The seed uses public mutation and checkpoint APIs to populate several leaf
// writers. GC must not mistake one cached writer's sequence for external output.
func TestValueLogGCNoopPreservesIndependentLeafWriters(t *testing.T) {
	opts := leafMaintenanceOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	leafMaintenanceWrite(t, db, 0)
	stats := db.Stats()
	active, err := strconv.Atoi(stats["treedb.cache.leaf_log_lanes.active"])
	if err != nil || active < 2 {
		t.Fatalf("fixture requires independent active leaf writers: active=%q err=%v", stats["treedb.cache.leaf_log_lanes.active"], err)
	}
	before := leafMaintenanceFiles(t, backenddb.LeafLogDirPath(opts.Dir))
	rotations := stats["treedb.cache.leaf_log_lanes.segment_rotations_total"]
	for i := 0; i < 3; i++ {
		gc, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if gc.SegmentsDeleted != 0 {
			t.Fatalf("expected value GC no-op, got %+v", gc)
		}
		after := leafMaintenanceFiles(t, backenddb.LeafLogDirPath(opts.Dir))
		if len(after) != len(before) {
			t.Fatalf("no-op GC created leaf segments: before=%d after=%d", len(before), len(after))
		}
		for path, info := range before {
			if now := after[path]; now == nil || !os.SameFile(info, now) {
				t.Fatalf("no-op GC replaced leaf writer %s", path)
			}
		}
		if got := db.Stats()["treedb.cache.leaf_log_lanes.segment_rotations_total"]; got != rotations {
			t.Fatalf("no-op GC rotated leaf writers: before=%s after=%s", rotations, got)
		}
	}
	leafMaintenanceWrite(t, db, 1)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 2048, 4095} {
		got, err := db.Get([]byte(fmt.Sprintf("leaf-maintenance-%08d", i)))
		want := bytes.Repeat([]byte{byte(1 + i%251)}, 128)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reopen key%d: got=%x err=%v", i, got, err)
		}
	}
}
