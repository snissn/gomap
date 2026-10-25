package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

func TestPublicCommandWALArtifactPreservesOpenedRuntimeConfiguration(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	savedMemtable := *treedbMemtableMode
	defer func() { *treedbMemtableMode = savedMemtable }()
	resetTreeDBIndexFlagsForTest()
	*treedbRelaxedSync = true
	*treedbAllowUnsafe = true // The CLI requires this opt-in for its relaxed flag.
	*treedbDisableReadChecksum = false
	*treedbMemtableMode = "cow_ordered"

	db, err := NewTreeDBPublicCommandWAL(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := db.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	opened := db.(*treedbadapter.DB).DB
	stats := opened.Stats()
	want := map[string]string{
		"treedb.profile.resolved":     string(opened.ResolvedProfile()),
		"treedb.durability_mode":      opened.DurabilityMode(),
		"treedb.vlog.read_integrity":  "verify",
		"treedb.profile.bench_unsafe": "false",
		"treedb.cache.memtable_mode":  "cow_ordered",
	}
	for key, value := range want {
		if stats[key] != value || value == "" {
			t.Fatalf("actual opened Stats[%q]=%q want %q", key, stats[key], value)
		}
	}
	if want["treedb.profile.resolved"] != "command_wal_relaxed" || want["treedb.durability_mode"] != "wal_on_relaxed_sync" {
		t.Fatalf("opened the wrong profile: %v", want)
	}

	// Report intent and later global changes cannot replace the opened facts.
	*treedbRelaxedSync = false
	*treedbMemtableMode = "skiplist"
	const other = "TreeDB (other instance)"
	const absent = "TreeDB (missing configuration)"
	run := BenchRun{
		Config: BenchConfig{Keys: 1, Profile: "wal_on_sync"},
		TreeDBStats: map[string]map[string]string{
			db.Name(): db.(*treedbadapter.DB).Stats(),
			other:     {"treedb.profile.resolved": "no_wal_fast", "treedb.cache.memtable_mode": "skiplist"},
			absent:    {"treedb.commit_seq": "0"},
		},
	}
	dir := t.TempDir()
	if err := writeBenchprofArtifacts(dir, "native-fastpath", []BenchRun{run}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "benchprof_results.json"))
	if err != nil {
		t.Fatal(err)
	}
	var output benchprofExport
	if err := json.Unmarshal(raw, &output); err != nil {
		t.Fatal(err)
	}
	if len(output.Runs) != 1 {
		t.Fatalf("runs=%d want1", len(output.Runs))
	}
	perEngine := output.Runs[0].TreeDBStats
	for key, value := range want {
		if got := perEngine[db.Name()][key]; got != value {
			t.Fatalf("exported opened instance[%q]=%q want %q", key, got, value)
		}
		if _, ok := perEngine[absent][key]; ok {
			t.Fatalf("manufactured missing configuration %q", key)
		}
	}
	if perEngine[other]["treedb.profile.resolved"] != "no_wal_fast" || perEngine[other]["treedb.cache.memtable_mode"] != "skiplist" {
		t.Fatalf("mixed engine configuration: %v", perEngine)
	}
}
