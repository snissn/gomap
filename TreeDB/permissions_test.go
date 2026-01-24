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
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

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
	checkFilePerms(filepath.Join(dir, "maindb", "data-0000.slab"))
	checkDirPerms(filepath.Join(dir, "maindb", "wal"))

	checkFilePerms(filepath.Join(dir, "dictdb", "index.db"))
	checkFilePerms(filepath.Join(dir, "dictdb", "LOCK"))
	checkFilePerms(filepath.Join(dir, "dictdb", "data-0000.slab"))
	checkDirPermsIfExists(filepath.Join(dir, "dictdb", "wal"))
}
