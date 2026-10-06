package mvcc

import (
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func TestCOWPublicACKAdmission(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		enabled, mode := "true", "external_command_wal"
		if profile == treedb.ProfileNoWALFast {
			enabled, mode = "false", "disabled_unsafe"
		}
		stats := map[string]string{"treedb.command_wal.enabled": enabled, "treedb.cache.command_wal.external_durability": enabled,
			"treedb.cache.redo_log.enabled": "false", "treedb.cache.redo_log.mode": mode}
		if err := cowPublicACKRouting(profile, stats); err != nil {
			t.Fatal(err)
		}
		for key, value := range stats {
			delete(stats, key)
			if err := cowPublicACKRouting(profile, stats); err == nil {
				t.Fatalf("accepted missing route %s", key)
			}
			stats[key] = "wrong"
			if err := cowPublicACKRouting(profile, stats); err == nil {
				t.Fatalf("accepted wrong route %s", key)
			}
			stats[key] = value
		}
	}
	const key = "actual.counter"
	for _, test := range []struct {
		name, before, after string
		missing, accepted   bool
		profile             treedb.Profile
	}{
		{name: "durable", before: "1", after: "2", accepted: true, profile: treedb.ProfileCommandWALDurable},
		{name: "relaxed", before: "1", after: "2", accepted: true, profile: treedb.ProfileCommandWALRelaxed},
		{name: "nowal", before: "0", after: "0", accepted: true, profile: treedb.ProfileNoWALFast},
		{name: "missing", after: "0", missing: true, profile: treedb.ProfileNoWALFast},
		{name: "malformed", before: "x", after: "0", profile: treedb.ProfileNoWALFast},
		{name: "regression", before: "2", after: "1", profile: treedb.ProfileCommandWALDurable},
		{name: "nowal-nonzero-zero-delta", before: "1", after: "1", profile: treedb.ProfileNoWALFast},
		{name: "nowal-activity", before: "0", after: "1", profile: treedb.ProfileNoWALFast},
	} {
		t.Run(test.name, func(t *testing.T) {
			before, after := map[string]string{key: test.before}, map[string]string{key: test.after}
			if test.missing {
				delete(before, key)
			}
			_, _, err := cowPublicWALCounters(test.profile, before, after, key)
			if (err == nil) != test.accepted {
				t.Fatalf("accepted=%v, error=%v", test.accepted, err)
			}
		})
	}
}

func TestCOWPublicPhysicalValueLayout(t *testing.T) {
	ptr := page.ValuePtr{FileID: page.ValueLogFileID(1), Offset: 17} // Length=0 is a valid omitted hint.
	for _, test := range []struct {
		name               string
		entry              node.LeafEntry
		pointers, accepted bool
	}{
		{name: "inline", entry: node.LeafEntry{}, accepted: true},
		{name: "persistent-pointer-no-length-hint", entry: node.LeafEntry{Flags: node.FlagPointer, ValuePtr: ptr}, pointers: true, accepted: true},
		{name: "pointer-request-inline-entry", entry: node.LeafEntry{}, pointers: true},
		{name: "inline-request-pointer-entry", entry: node.LeafEntry{Flags: node.FlagPointer, ValuePtr: ptr}},
		{name: "zero-pointer", entry: node.LeafEntry{Flags: node.FlagPointer}, pointers: true},
		{name: "wrong-file-id", entry: node.LeafEntry{Flags: node.FlagPointer, ValuePtr: page.ValuePtr{FileID: 1}}, pointers: true},
		{name: "inline-nonzero-pointer", entry: node.LeafEntry{ValuePtr: ptr}},
		{name: "physical-tombstone", entry: node.LeafEntry{Flags: node.FlagTombstone}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := cowPublicValueLayout(test.entry, test.pointers)
			if (err == nil) != test.accepted {
				t.Fatalf("accepted=%v, error=%v", test.accepted, err)
			}
		})
	}
}

// Actual failure paths must release the public snapshot's leases and cuts.
func TestCOWPublicPhysicalProbeClosesOnRefusal(t *testing.T) {
	for _, missing := range []bool{false, true} {
		opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
		opts.MemtableMode = "cow_btree"
		opts.DisableSideStores = true
		opts.BackgroundCheckpointInterval = -1
		opts.ValueLog.PointerThreshold = 1 << 30
		db, err := treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		store := New(db)
		groups := []CommitGroup{{Timestamp: 10, Mutations: []Mutation{{Key: []byte("probe"), Value: []byte("inline")}}}}
		if err := store.CommitGroupAt(groups, CommitRelaxed); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		if missing {
			groups[0].Mutations[0].Key = []byte("missing")
		}
		before := db.Stats()
		_, _, err = cowC3ValueLayout(db, groups, true, true)
		if err == nil {
			_ = db.Close()
			t.Fatal("accepted missing/wrong-layout physical entry")
		}
		after := db.Stats()
		for _, key := range []string{"external_leases", "views", "active_cuts"} {
			name := "treedb.cache.cow." + key
			if before[name] == "" || before[name] != after[name] {
				t.Errorf("refusal leaked %s: %s -> %s", name, before[name], after[name])
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
