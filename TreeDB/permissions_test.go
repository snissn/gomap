package treedb_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestDefaultPermissions_AreNotWorldReadable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permissions are not enforced on windows")
	}

	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	value := make([]byte, 1024)
	if err := db.Set([]byte("perm_check"), value); err != nil {
		t.Fatalf("Set: %v", err)
	}

	checkFilePerms := func(path string) {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat(%s): %v", path, err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("expected %s to be owner-only, got perms %o", path, info.Mode().Perm())
		}
	}
	checkDirPerms := func(path string) {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat(%s): %v", path, err)
		}
		if !info.IsDir() {
			t.Fatalf("expected %s to be dir", path)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("expected %s to be owner-only dir, got perms %o", path, info.Mode().Perm())
		}
	}
	checkDirPermsIfExists := func(path string) {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			t.Fatalf("Stat(%s): %v", path, err)
		}
		if !info.IsDir() {
			t.Fatalf("expected %s to be dir", path)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("expected %s to be owner-only dir, got perms %o", path, info.Mode().Perm())
		}
	}

	checkFilePerms(filepath.Join(dir, "maindb", "index.db"))
	checkFilePerms(filepath.Join(dir, "maindb", "LOCK"))
	checkDirPerms(filepath.Join(dir, "maindb", "wal"))
	checkDirPerms(filepath.Join(dir, "maindb", "value_vlog"))
	checkDirPerms(filepath.Join(dir, "maindb", "leaf_vlog"))
	valueFiles, err := filepath.Glob(filepath.Join(dir, "maindb", "value_vlog", "value-*.log"))
	if err != nil {
		t.Fatalf("Glob value_vlog value logs: %v", err)
	}
	if len(valueFiles) == 0 {
		t.Fatalf("expected value-log segment in value_vlog dir")
	}
	for _, path := range valueFiles {
		checkFilePerms(path)
	}

	checkFilePerms(filepath.Join(dir, "dictdb", "index.db"))
	checkFilePerms(filepath.Join(dir, "dictdb", "LOCK"))
	checkDirPermsIfExists(filepath.Join(dir, "dictdb", "wal"))
}
