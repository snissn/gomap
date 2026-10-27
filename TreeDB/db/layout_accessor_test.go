package db

import (
	"path/filepath"
	"strings"
	"testing"
)

var layoutAccessorResult string

func TestLayoutAccessorsMatchFullLayout(t *testing.T) {
	for _, dir := range []string{"", ".", "..", string(filepath.Separator), "db", "db/", "db/../other", "db//nested/./", "a path/with spaces", `C:\db\nested`, strings.Repeat("segment/", 40)} {
		layout := resolveStorageLayout(dir)
		for _, c := range []struct {
			name string
			get  func(string) string
			want string
		}{
			{"wal", WALDirPath, layout.walDir}, {"value", ValueLogDirPath, layout.valueVLogDir},
			{"leaf", LeafLogDirPath, layout.leafVLogDir}, {"column", ColumnAssetRootDirPath, layout.columnAssetDir},
		} {
			if got := c.get(dir); got != c.want {
				t.Errorf("%s(%q)=%q, want %q", c.name, dir, got, c.want)
			}
		}
	}
}

func TestLayoutAccessorsAllocateOnlyRequestedPath(t *testing.T) {
	dir := filepath.Join("database", "nested")
	for _, c := range []struct {
		name  string
		get   func(string) string
		child string
	}{
		{"wal", WALDirPath, walDirName}, {"value", ValueLogDirPath, valueVLogDirName},
		{"leaf", LeafLogDirPath, leafVLogDirName}, {"column", ColumnAssetRootDirPath, columnAssetDirName},
	} {
		t.Run(c.name, func(t *testing.T) {
			direct := testing.AllocsPerRun(100, func() { layoutAccessorResult = filepath.Join(dir, c.child) })
			got := testing.AllocsPerRun(100, func() { layoutAccessorResult = c.get(dir) })
			if got != direct {
				t.Fatalf("accessor allocs=%g, requested path allocs=%g", got, direct)
			}
		})
	}
}
