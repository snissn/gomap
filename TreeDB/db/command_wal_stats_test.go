package db

import "testing"

func TestCommandWALStatsProveModeAndFrames(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.command_wal.enabled"]; got != "true" {
		t.Fatalf("command WAL enabled stat=%q, want true (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.required_feature"]; got != "true" {
		t.Fatalf("command WAL required feature stat=%q, want true (stats=%#v)", got, stats)
	}
	if got := stats["treedb.applied_command_lsn"]; got != "1" {
		t.Fatalf("applied command LSN stat=%q, want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.frames"]; got != "1" {
		t.Fatalf("command WAL frame stat=%q, want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.typed_segments"]; got != "1" {
		t.Fatalf("command WAL typed segment stat=%q, want 1 (stats=%#v)", got, stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
