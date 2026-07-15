package treedb

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"strconv"
	"testing"
)

func profileFastKey(i int) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(i))
	return k
}

func runCheckpointConsistencyWorkload(t *testing.T, db *DB, seed int64) map[int][]byte {
	t.Helper()

	r := rand.New(rand.NewSource(seed))
	live := make(map[int][]byte, 1024)

	const (
		ops       = 500
		keySpace  = 2000
		checkEach = 100
	)

	for i := 0; i < ops; i++ {
		k := r.Intn(keySpace)
		key := profileFastKey(k)

		if r.Intn(100) < 70 {
			n := r.Intn(300)
			value := make([]byte, n)
			for j := range value {
				value[j] = byte(r.Intn(256))
			}
			if err := db.Set(key, value); err != nil {
				t.Fatalf("set i=%d k=%d: %v", i, k, err)
			}
			live[k] = append([]byte(nil), value...)
		} else {
			if err := db.Delete(key); err != nil {
				t.Fatalf("delete i=%d k=%d: %v", i, k, err)
			}
			delete(live, k)
		}

		if i > 0 && i%checkEach == 0 {
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("checkpoint i=%d: %v", i, err)
			}
		}
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("final checkpoint: %v", err)
	}
	return live
}

func assertCheckpointConsistencyLiveSet(t *testing.T, db *DB, live map[int][]byte) {
	t.Helper()
	for k, want := range live {
		got, err := db.Get(profileFastKey(k))
		if err != nil {
			t.Fatalf("get k=%d: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch k=%d want_len=%d got_len=%d", k, len(want), len(got))
		}
	}
}

// Regression: repeated checkpoint boundaries must not lose last-write-wins visibility.
func TestProfileFast_CheckpointMaintainsLatestValues(t *testing.T) {
	profiles := []Profile{ProfileFast, ProfileWALOnFast}
	for _, profile := range profiles {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			live := runCheckpointConsistencyWorkload(t, db, 2)
			assertCheckpointConsistencyLiveSet(t, db, live)
		})
	}
}

func TestProfileFast_SetSyncRemainsVisibleAndPersistsAfterCheckpoint(t *testing.T) {
	opts := OptionsFor(ProfileFast, t.TempDir())
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1
	opts.FlushThreshold = 64 << 20
	opts.BackgroundIndexVacuumInterval = -1
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.DisableBackgroundPrune = true
	opts.MaxQueuedMemtables = -1
	opts.WriterFlushMaxMemtables = 0
	opts.WriterFlushMaxDuration = 0
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	live := make(map[int][]byte, 256)
	for i := 0; i < 256; i++ {
		key := profileFastKey(i)
		value := bytes.Repeat([]byte{byte(i), byte(i >> 3), 0x5a, 0xc3}, 256)
		if err := db.SetSync(key, value); err != nil {
			t.Fatalf("SetSync key=%d: %v", i, err)
		}
		live[i] = append([]byte(nil), value...)
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("immediate get key=%d: %v", i, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("immediate get key=%d mismatch got_len=%d want_len=%d", i, len(got), len(value))
		}
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	assertCheckpointConsistencyLiveSet(t, db, live)
}

func TestProfileFast_SetSyncPublishesDurableRootBeforeReturn(t *testing.T) {
	opts := OptionsFor(ProfileFast, t.TempDir())
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1
	opts.FlushThreshold = 64 << 20
	opts.BackgroundIndexVacuumInterval = -1
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.DisableBackgroundPrune = true
	opts.MaxQueuedMemtables = -1
	opts.WriterFlushMaxMemtables = 0
	opts.WriterFlushMaxDuration = 0
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	commitSeqBefore, err := strconv.ParseUint(db.Stats()["treedb.commit_seq"], 10, 64)
	if err != nil {
		t.Fatalf("parse commit_seq before: %v", err)
	}

	key := profileFastKey(42)
	value := bytes.Repeat([]byte("fast-sync-visible"), 256)
	if err := db.SetSync(key, value); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("immediate get: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("immediate get mismatch got_len=%d want_len=%d", len(got), len(value))
	}

	commitSeqAfterSync, err := strconv.ParseUint(db.Stats()["treedb.commit_seq"], 10, 64)
	if err != nil {
		t.Fatalf("parse commit_seq after sync: %v", err)
	}
	if commitSeqAfterSync <= commitSeqBefore {
		t.Fatalf("commit_seq after SetSync=%d want > %d", commitSeqAfterSync, commitSeqBefore)
	}
	durableCommitSeqAfterSync, err := strconv.ParseUint(db.Stats()["treedb.durable_root.commit_seq"], 10, 64)
	if err != nil {
		t.Fatalf("parse durable_root.commit_seq after sync: %v", err)
	}
	if durableCommitSeqAfterSync != commitSeqAfterSync {
		t.Fatalf("durable_root.commit_seq after SetSync=%d want visible commit_seq %d", durableCommitSeqAfterSync, commitSeqAfterSync)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	got, err = db.Get(key)
	if err != nil {
		t.Fatalf("reopen get: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("reopen get mismatch got_len=%d want_len=%d", len(got), len(value))
	}
}
