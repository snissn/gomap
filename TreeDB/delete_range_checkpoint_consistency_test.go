package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func deleteRangeConsistencyKey(i int) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(i))
	return k
}

func deleteRangeConsistencyValue(i int) []byte {
	body := bytes.Repeat([]byte{byte(i % 251)}, 384)
	return append([]byte(fmt.Sprintf("v-%04d-", i)), body...)
}

func collectIteratorInts(t *testing.T, it Iterator) []int {
	t.Helper()
	var out []int
	for it.Valid() {
		k := it.Key()
		if len(k) != 8 {
			t.Fatalf("unexpected iterator key len=%d", len(k))
		}
		out = append(out, int(binary.BigEndian.Uint64(k)))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}
	return out
}

func runDeleteRangeCheckpointConsistencyProfileMatrix(t *testing.T) {
	t.Helper()
	profiles := []Profile{ProfileDurable, ProfileFast, ProfileWALOnFast}
	for _, profile := range profiles {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			opts.ValueLog.PointerThreshold = 1
			opts.ValueLog.ForcePointers = true

			db, err := Open(opts)
			if err != nil {
				t.Fatalf("open: %v", err)
			}

			const total = 220
			const delStart = 60
			const delEnd = 170
			live := make(map[int][]byte, total)

			for i := 0; i < total; i++ {
				v := deleteRangeConsistencyValue(i)
				if err := db.Set(deleteRangeConsistencyKey(i), v); err != nil {
					t.Fatalf("set %d: %v", i, err)
				}
				live[i] = v
			}

			if err := db.DeleteRange(deleteRangeConsistencyKey(delStart), deleteRangeConsistencyKey(delEnd)); err != nil {
				t.Fatalf("DeleteRange: %v", err)
			}
			for i := delStart; i < delEnd; i++ {
				delete(live, i)
			}

			if err := db.Checkpoint(); err != nil {
				t.Fatalf("checkpoint: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()

			for i := 0; i < total; i++ {
				got, err := reopened.Get(deleteRangeConsistencyKey(i))
				if err != nil {
					t.Fatalf("get %d: %v", i, err)
				}
				if i >= delStart && i < delEnd {
					if got != nil {
						t.Fatalf("expected deleted key %d absent, got len=%d", i, len(got))
					}
					continue
				}
				want := live[i]
				if !bytes.Equal(got, want) {
					t.Fatalf("value mismatch key=%d got_len=%d want_len=%d", i, len(got), len(want))
				}
			}

			it, err := reopened.Iterator(nil, nil)
			if err != nil {
				t.Fatalf("iterator: %v", err)
			}
			forward := collectIteratorInts(t, it)
			if len(forward) != len(live) {
				t.Fatalf("forward live key count=%d want=%d", len(forward), len(live))
			}
			for _, k := range forward {
				if k >= delStart && k < delEnd {
					t.Fatalf("forward iterator surfaced deleted key %d", k)
				}
			}

			rit, err := reopened.ReverseIterator(nil, nil)
			if err != nil {
				t.Fatalf("reverse iterator: %v", err)
			}
			reverse := collectIteratorInts(t, rit)
			if len(reverse) != len(live) {
				t.Fatalf("reverse live key count=%d want=%d", len(reverse), len(live))
			}
			for _, k := range reverse {
				if k >= delStart && k < delEnd {
					t.Fatalf("reverse iterator surfaced deleted key %d", k)
				}
			}
		})
	}
}

func TestDeleteRange_CheckpointConsistency_ProfileMatrix(t *testing.T) {
	runDeleteRangeCheckpointConsistencyProfileMatrix(t)
}
