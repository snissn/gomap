package db

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLayoutPaths_DefaultSplitDirs(t *testing.T) {
	dir := t.TempDir()

	layout := resolveStorageLayout(dir)
	if got, want := layout.rootDir, dir; got != want {
		t.Fatalf("rootDir=%q, want %q", got, want)
	}
	if got, want := layout.walDir, filepath.Join(dir, walDirName); got != want {
		t.Fatalf("walDir=%q, want %q", got, want)
	}
	if got, want := layout.valueVLogDir, filepath.Join(dir, valueVLogDirName); got != want {
		t.Fatalf("valueVLogDir=%q, want %q", got, want)
	}
	if got, want := layout.leafVLogDir, filepath.Join(dir, leafVLogDirName); got != want {
		t.Fatalf("leafVLogDir=%q, want %q", got, want)
	}
	if got, want := layout.columnAssetDir, filepath.Join(dir, columnAssetDirName); got != want {
		t.Fatalf("columnAssetDir=%q, want %q", got, want)
	}
}

func TestOpen_FreshDBCreatesSplitStorageDirs(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	layout := resolveStorageLayout(dir)
	for _, path := range []string{layout.walDir, layout.valueVLogDir, layout.leafVLogDir, layout.columnAssetDir} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat(%s): %v", path, err)
		}
		if !info.IsDir() {
			t.Fatalf("expected %s to be a directory", path)
		}
	}
}

func TestDBColumnAssetRootDirDoesNotAllocateM1634(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	want := ColumnAssetRootDirPath(dir)
	if got := db.ColumnAssetRootDir(); got != want {
		t.Fatalf("ColumnAssetRootDir=%q, want %q", got, want)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_ = db.ColumnAssetRootDir()
	}); got != 0 {
		t.Fatalf("ColumnAssetRootDir allocated %.2f times per call", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	readonly, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open readonly: %v", err)
	}
	defer readonly.Close()
	if got := readonly.ColumnAssetRootDir(); got != want {
		t.Fatalf("readonly ColumnAssetRootDir=%q, want %q", got, want)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_ = readonly.ColumnAssetRootDir()
	}); got != 0 {
		t.Fatalf("readonly ColumnAssetRootDir allocated %.2f times per call", got)
	}
}

func TestEnsureStorageLayoutDirsSyncsRootForColumnAssetsM12A(t *testing.T) {
	dir := t.TempDir()
	var synced []string
	previous := syncStorageLayoutDir
	syncStorageLayoutDir = func(path string) error {
		synced = append(synced, path)
		return nil
	}
	t.Cleanup(func() {
		syncStorageLayoutDir = previous
	})

	if err := ensureStorageLayoutDirs(dir); err != nil {
		t.Fatalf("ensureStorageLayoutDirs: %v", err)
	}
	if _, err := os.Stat(ColumnAssetRootDirPath(dir)); err != nil {
		t.Fatalf("Stat(column_assets): %v", err)
	}
	for _, path := range synced {
		if path == dir {
			return
		}
	}
	t.Fatalf("new storage layout dirs did not sync DB root %q; synced=%v", dir, synced)
}

func TestHasLegacyMixedWALValueSegments_DetectsValueLogsInWAL(t *testing.T) {
	for _, name := range []string{"value-l0-000001.log", "vlog-l0-000001.log"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			layout := resolveStorageLayout(dir)
			if err := os.MkdirAll(layout.walDir, 0o700); err != nil {
				t.Fatalf("MkdirAll(wal): %v", err)
			}
			if err := os.WriteFile(filepath.Join(layout.walDir, name), []byte("x"), 0o600); err != nil {
				t.Fatalf("WriteFile(value log): %v", err)
			}

			ok, err := hasLegacyMixedWALValueSegments(dir)
			if err != nil {
				t.Fatalf("hasLegacyMixedWALValueSegments: %v", err)
			}
			if !ok {
				t.Fatalf("expected legacy mixed WAL/value layout to be detected")
			}
		})
	}
}

func TestHasLegacyMixedWALValueSegments_IgnoresCommitOnlyWAL(t *testing.T) {
	dir := t.TempDir()
	layout := resolveStorageLayout(dir)
	if err := os.MkdirAll(layout.walDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	if err := os.WriteFile(filepath.Join(layout.walDir, "commit-l0-000001.log"), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile(commit log): %v", err)
	}

	ok, err := hasLegacyMixedWALValueSegments(dir)
	if err != nil {
		t.Fatalf("hasLegacyMixedWALValueSegments: %v", err)
	}
	if ok {
		t.Fatalf("unexpected legacy mixed WAL/value layout detection")
	}
}
