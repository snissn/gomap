package treedb

import (
	"bytes"
	"encoding/binary"
	"math/rand"
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

// Regression: repeated checkpoint boundaries must not lose last-write-wins
// visibility.
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
