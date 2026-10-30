package db

import "testing"

func TestStatsCapacityPreservesSnapshot(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	ordinary := db.Stats()
	for _, hint := range []int{-1, 1, 4608} {
		got := db.StatsWithCapacity(hint)
		if len(got) != len(ordinary) {
			t.Fatalf("hint %d fields=%d want %d", hint, len(got), len(ordinary))
		}
		for key := range ordinary {
			if _, ok := got[key]; !ok {
				t.Fatalf("hint %d missing %s", hint, key)
			}
		}
		for _, key := range []string{"cosmos.db.type", "treedb.commit_seq", "treedb.profile.resolved"} {
			if got[key] != ordinary[key] {
				t.Fatalf("hint %d %s=%q want %q", hint, key, got[key], ordinary[key])
			}
		}
		got["caller-owned"] = "sentinel"
		delete(got, "cosmos.db.type")
	}
	if got := db.Stats(); got["caller-owned"] != "" || got["cosmos.db.type"] != "treedb" {
		t.Fatal("snapshot mutations escaped")
	}
}
