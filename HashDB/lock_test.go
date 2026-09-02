package hashdb

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestOpenWithShards_ExclusiveLockInProcess(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenWithShards(dir, 2)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	db2, err := OpenWithShards(dir, 2)
	if err == nil {
		_ = db2.Close()
		_ = db.Close()
		t.Fatalf("expected ErrLocked, got nil")
	}
	if !errors.Is(err, ErrLocked) {
		_ = db.Close()
		t.Fatalf("expected ErrLocked, got %v", err)
	}

	partition0 := filepath.Join(dir, "partition-0")
	single, err := OpenSingle(partition0)
	if err == nil {
		_ = single.Close()
		_ = db.Close()
		t.Fatalf("expected ErrLocked when opening shard directly, got nil")
	}
	if !errors.Is(err, ErrLocked) {
		_ = db.Close()
		t.Fatalf("expected ErrLocked when opening shard directly, got %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db3, err := OpenWithShards(dir, 2)
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	_ = db3.Close()
}

func TestOpenWithShards_ExclusiveLockCrossProcess(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenWithShards(dir, 2)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	mustHelperOpen(t, dir, "sharded", "locked")

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mustHelperOpen(t, dir, "sharded", "unlocked")
}

func TestOpenSingle_ExclusiveLockCrossProcess(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenSingle(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	mustHelperOpen(t, dir, "single", "locked")

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mustHelperOpen(t, dir, "single", "unlocked")
}

func mustHelperOpen(t *testing.T, dir, mode, expect string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperOpenLock$", "-test.v")
	cmd.Env = append(os.Environ(),
		"HASHDB_LOCK_HELPER=1",
		"HASHDB_LOCK_DIR="+dir,
		"HASHDB_LOCK_MODE="+mode,
		"HASHDB_LOCK_EXPECT="+expect,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper failed: %v\n%s", err, string(out))
	}
}

func TestHelperOpenLock(t *testing.T) {
	if os.Getenv("HASHDB_LOCK_HELPER") != "1" {
		t.Skip("helper")
	}

	dir := os.Getenv("HASHDB_LOCK_DIR")
	mode := os.Getenv("HASHDB_LOCK_MODE")
	expect := os.Getenv("HASHDB_LOCK_EXPECT")
	if dir == "" || (mode != "single" && mode != "sharded") || (expect != "locked" && expect != "unlocked") {
		t.Fatalf("bad helper env: dir=%q mode=%q expect=%q", dir, mode, expect)
	}

	var closeFn func() error
	var err error

	switch mode {
	case "single":
		var db *DB
		db, err = OpenSingle(dir)
		closeFn = db.Close
	case "sharded":
		var db *HashDB
		db, err = OpenWithShards(dir, 2)
		closeFn = db.Close
	}

	switch expect {
	case "locked":
		if err == nil {
			_ = closeFn()
			t.Fatalf("expected ErrLocked, got nil")
		}
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("expected ErrLocked, got %v", err)
		}
	case "unlocked":
		if err != nil {
			t.Fatalf("expected open success, got %v", err)
		}
		_ = closeFn()
	}
}
