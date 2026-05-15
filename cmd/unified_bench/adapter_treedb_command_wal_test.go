package main

import (
	"testing"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

func TestTreeDBBackendCommandWALVariantPersistsFeatureAndAppendsTypedFrames(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()

	dir := t.TempDir()
	db, err := NewTreeDBBackendCommandWAL(dir)
	if err != nil {
		t.Fatalf("NewTreeDBBackendCommandWAL: %v", err)
	}
	if got := db.Name(); got != "TreeDB (backend command_wal_v1)" {
		t.Fatalf("Name=%q, want explicit command WAL variant name", got)
	}
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	sp, ok := db.(interface{ Stats() map[string]string })
	if !ok {
		t.Fatalf("%T does not expose Stats", db)
	}
	stats := sp.Stats()
	for key, want := range map[string]string{
		"treedb.command_wal.enabled":          "true",
		"treedb.command_wal.required_feature": "true",
		"treedb.applied_command_lsn":          "1",
		"treedb.command_wal.frames":           "1",
		"treedb.command_wal.typed_segments":   "1",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("stats[%q]=%q, want %q (stats=%#v)", key, got, want, stats)
		}
	}
	required, err := treedbdb.CommandWALRequiredFeatureEnabled(dir)
	if err != nil {
		t.Fatalf("CommandWALRequiredFeatureEnabled: %v", err)
	}
	if !required {
		t.Fatal("command WAL variant did not persist command_wal_v1 required feature")
	}
	cfg, ok, err := treedbdb.LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if !ok {
		t.Fatal("format config missing")
	}
	if !cfg.IndexOuterLeavesInValueLog {
		t.Fatalf("command WAL backend disabled requested IndexOuterLeavesInValueLog")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closedStats := sp.Stats()
	if got := closedStats["treedb.command_wal.frames"]; got != "1" {
		t.Fatalf("closed stats command WAL frames=%q, want cached final stats (stats=%#v)", got, closedStats)
	}
}

func TestTreeDBBackendPreservesOuterLeavesFlag(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeavesInVlog = true

	dir := t.TempDir()
	db, err := NewTreeDBBackend(dir)
	if err != nil {
		t.Fatalf("NewTreeDBBackend: %v", err)
	}
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set with outer leaves enabled: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close backend: %v", err)
	}
	cfg, ok, err := treedbdb.LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if !ok {
		t.Fatal("format config missing")
	}
	if !cfg.IndexOuterLeavesInValueLog {
		t.Fatalf("legacy backend disabled IndexOuterLeavesInValueLog")
	}
}

func TestTreeDBBackendCommandWALForcesWALOnWhenProfileDisablesWAL(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	*treedbAllowUnsafe = true
	*treedbDisableWAL = true

	db, err := NewTreeDBBackendCommandWAL(t.TempDir())
	if err != nil {
		t.Fatalf("NewTreeDBBackendCommandWAL with disabled WAL flag: %v", err)
	}
	defer db.Close()
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestTreeDBCommandWALAliasResolvesToBackendVariant(t *testing.T) {
	factory, err := GetDBFactory("treedb_command_wal")
	if err != nil {
		t.Fatalf("GetDBFactory alias: %v", err)
	}
	if factory == nil {
		t.Fatal("GetDBFactory returned nil factory")
	}
}
