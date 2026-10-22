package caching

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Observe the actual write boundary rather than an isolated planner: inline
// publication must avoid pointer-only backing, while mixed publication still
// owns every produced frame and canonical resource before visibility.
func TestCOWInlinePreparationAndMixedPointerBatchReuse(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend")})
			if err != nil {
				t.Fatal(err)
			}
			db, err := Open(dir, backend, Options{
				DisableWAL: true, AllowUnsafe: true, MemtableMode: mode,
				FlushThreshold: 1 << 30, MemtableShards: 4,
				ValueLogPointerThreshold: 64, ValueLogCompression: uint8(vlogCompressionOff),
			})
			if err != nil {
				_ = backend.Close()
				t.Fatal(err)
			}
			defer db.Close()
			owner := backend.NewMVCCAdmission()
			defer owner.Close()
			old := db.AcquireSnapshot()
			defer old.Close()
			b := db.NewBatchWithSize(4)
			defer b.Close()
			type operand struct {
				logical string
				ts      uint64
				value   []byte
			}
			rounds := [][]operand{
				{{"a\x00binary", 31, []byte{1, 'a'}}, {"a\x00binary", 17, []byte{1, 'b'}}, {"other", 9, []byte{1, 'c'}}},
				{{"a\x00binary", 32, []byte{1, 'd'}}, {"other", 12, bytes.Repeat([]byte{1, 'p'}, 1024)}, {"a\x00binary", 17, []byte{2}}},
				{{"pointer", 44, bytes.Repeat([]byte{1, 'q'}, 1024)}},
				{{"inline-again", 55, []byte{1, 'z'}}},
			}
			want := make(map[string][]byte)
			seen := 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Root != db.dir || event.Point != durabilitycut.AfterCOWCanonicalPreparation {
					return nil
				}
				seen++
				pointers := 0
				counts := make([]int, len(db.mutableShards))
				for i, entry := range b.entries {
					shard := db.shardIndex(entry.Key)
					counts[shard]++
					if b.shardIdxs[i] != shard {
						return fmt.Errorf("canonical shard changed for operand %d", i)
					}
					if entry.IsPtr {
						pointers++
					}
				}
				if !b.cowState.shardAssignmentsReady {
					return fmt.Errorf("actual write did not use stable shard assignments")
				}
				for i, count := range counts {
					if b.shardCnts[i] != count || len(b.cowState.predictionGroups[i]) != count {
						return fmt.Errorf("canonical shard %d count=%d group=%d want=%d", i, b.shardCnts[i], len(b.cowState.predictionGroups[i]), count)
					}
				}
				p := b.cowState.prepared
				if pointers == 0 {
					if b.cowState.producer != nil || p.files != nil || p.dictionaries != nil || p.resources != nil {
						return fmt.Errorf("inline write allocated pointer-only metadata")
					}
				} else if b.cowState.producer == nil || cap(b.cowState.producer.frames) != pointers || p.files == nil || p.dictionaries == nil || cap(p.resources) != 2*pointers {
					return fmt.Errorf("mixed write lost its exact pointer-only admission")
				}
				return nil
			})
			defer restore()
			for round, operands := range rounds {
				if round == 2 {
					// Explicit reset and empty writes must invalidate all previous
					// key assignments without leaving a producer from the mixed batch.
					if err := b.Set([]byte("discarded"), []byte("discarded")); err != nil {
						t.Fatal(err)
					}
					b.Reset()
					if err := b.Write(); err != nil {
						t.Fatal(err)
					}
					if b.cowState.shardAssignmentsReady || b.cowState.producer != nil {
						t.Fatal("reset retained an assignment or producer")
					}
				}
				for _, op := range operands {
					key, err := mvcckey.Encode([]byte(op.logical), op.ts)
					if err != nil {
						t.Fatal(err)
					}
					if err := b.SetWithMVCCInput(key, op.value, owner.Input(key, op.ts, 0)); err != nil {
						t.Fatal(err)
					}
					want[string(key)] = bytes.Clone(op.value)
				}
				if err := b.Write(); err != nil {
					t.Fatalf("round %d: %v", round, err)
				}
				if b.cowState.shardAssignmentsReady || b.cowState.producer != nil {
					t.Fatal("completed write retained an assignment or producer")
				}
				for key, expected := range want {
					got, err := db.Get([]byte(key))
					if err != nil || !bytes.Equal(got, expected) {
						t.Fatalf("round %d value=%x want=%x error=%v", round, got, expected, err)
					}
					previous, err := old.Get([]byte(key))
					if err != nil && !errors.Is(err, tree.ErrKeyNotFound) || previous != nil {
						t.Fatalf("retained old cut observed new value: value=%x error=%v", previous, err)
					}
				}
			}
			if seen != len(rounds) {
				t.Fatalf("observed %d actual canonical writes, want %d", seen, len(rounds))
			}
		})
	}
}
