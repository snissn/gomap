package redisserver

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestOpenEngine_TreeDB_JournalLanesCreatesSegments(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Dir:                dir,
		Engine:             "treedb",
		TreeDBJournalLanes: 4,
	}
	db, err := OpenEngine(cfg)
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	// NOTE: TreeDB stores its main DB in Dir/maindb/, so the caching WAL dir is
	// Dir/maindb/wal/.
	walDir := filepath.Join(dir, "maindb", "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		_ = db.Close()
		t.Fatalf("ReadDir(%s): %v", walDir, err)
	}
	re := regexp.MustCompile(`^commit-l([0-9]+)-`)
	lanes := make(map[int]struct{})
	for _, e := range entries {
		m := re.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		lane, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatalf("parse lane %q: %v", m[1], err)
		}
		lanes[lane] = struct{}{}
	}
	for i := 0; i < 4; i++ {
		if _, ok := lanes[i]; !ok {
			_ = db.Close()
			t.Fatalf("expected commit log segment for lane %d; got lanes=%v", i, lanes)
		}
	}
	_ = db.Close()
}

func TestOpenEngine_TreeDB_MemtableVlogPointersRequireAllowUnsafe(t *testing.T) {
	dir := t.TempDir()
	_, err := OpenEngine(Config{
		Dir:                    dir,
		Engine:                 "treedb",
		TreeDBMemtableVlogPtrs: true,
	})
	if !errors.Is(err, treedb.ErrUnsafeOptions) {
		t.Fatalf("expected ErrUnsafeOptions, got %v", err)
	}

	db, err := OpenEngine(Config{
		Dir:                    t.TempDir(),
		Engine:                 "treedb",
		TreeDBAllowUnsafe:      true,
		TreeDBMemtableVlogPtrs: true,
	})
	if err != nil {
		t.Fatalf("OpenEngine with AllowUnsafe: %v", err)
	}
	_ = db.Close()
}
