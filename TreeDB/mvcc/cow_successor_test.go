package mvcc

import (
	treedb "github.com/snissn/gomap/TreeDB"
	"testing"
)

func TestCOWSuccessorLogicalTombstoneAndEmptyValue(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.MemtableShards = 4
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			store := New(db)
			key := []byte("logical")
			for _, commit := range []struct {
				ts       uint64
				mutation Mutation
			}{{10, Mutation{Key: key, Value: []byte("old")}}, {20, Mutation{Key: key, Delete: true}}, {30, Mutation{Key: key, Value: nil}}} {
				if err := store.CommitAt(commit.ts, []Mutation{commit.mutation}, CommitRelaxed); err != nil {
					t.Fatal(err)
				}
			}
			// These actual Store reads exercise the qualified successor capability;
			// logical deletion remains a value record, and empty present is distinct.
			for _, afterCheckpoint := range []bool{false, true} {
				if afterCheckpoint {
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				requireResult(t, store, key, 10, Present, 10, []byte("old"))
				requireResult(t, store, key, 20, Tombstone, 20, nil)
				requireResult(t, store, key, 30, Present, 30, nil)
				requireResult(t, store, key, 9, Absent, 0, nil)
			}
		})
	}
}

func TestCOWSuccessorPublicEmptyKeyValue(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Set(nil, nil); err != nil {
				t.Fatal(err)
			}
			for _, disk := range []bool{false, true} {
				if disk {
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				key, value, found, err := db.SeekGE(nil, nil)
				if err != nil || !found || len(key) != 0 || len(value) != 0 {
					t.Fatalf("empty successor=%#v/%#v found%t err%v", key, value, found, err)
				}
				if found, err := db.Has(nil); err != nil || !found {
					t.Fatalf("empty point Has=%t err%v", found, err)
				}
			}
			if err := db.Delete(nil); err != nil {
				t.Fatal(err)
			}
			if _, _, found, err := db.SeekGE(nil, nil); err != nil || found {
				t.Fatalf("deleted empty successor found%t err%v", found, err)
			}
		})
	}
}
