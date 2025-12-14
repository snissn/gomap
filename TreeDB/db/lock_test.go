package db

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

func TestOpen_ExclusiveLockInProcess(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db2, err := Open(Options{Dir: dir})
	if err == nil {
		_ = db2.Close()
		t.Fatalf("expected ErrLocked, got nil")
	}
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("expected ErrLocked, got %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db3, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	_ = db3.Close()
}

func TestOpen_ExclusiveLockCrossProcess(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	mustHelperOpen(t, dir, "locked")

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mustHelperOpen(t, dir, "unlocked")
}

func mustHelperOpen(t *testing.T, dir, expect string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperOpenLock$", "-test.v")
	cmd.Env = append(os.Environ(),
		"TREEDB_LOCK_HELPER=1",
		"TREEDB_LOCK_DIR="+dir,
		"TREEDB_LOCK_EXPECT="+expect,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper failed: %v\n%s", err, string(out))
	}
}

func TestHelperOpenLock(t *testing.T) {
	if os.Getenv("TREEDB_LOCK_HELPER") != "1" {
		t.Skip("helper")
	}

	dir := os.Getenv("TREEDB_LOCK_DIR")
	expect := os.Getenv("TREEDB_LOCK_EXPECT")
	if dir == "" || (expect != "locked" && expect != "unlocked") {
		t.Fatalf("bad helper env: dir=%q expect=%q", dir, expect)
	}

	db, err := Open(Options{Dir: dir})
	switch expect {
	case "locked":
		if err == nil {
			_ = db.Close()
			t.Fatalf("expected ErrLocked, got nil")
		}
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("expected ErrLocked, got %v", err)
		}
	case "unlocked":
		if err != nil {
			t.Fatalf("expected open success, got %v", err)
		}
		_ = db.Close()
	}
}
