package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

type snapshotReader interface {
	GetUnsafe([]byte) ([]byte, error)
	Close() error
}

type snapshotVisibilityCase struct {
	key       string
	wantValue []byte
	missing   bool
}

func snapshotVisibilityModes() []string {
	return []string{
		"skiplist",
		"hash_sorted",
		"btree",
		"append_only",
		"adaptive",
		"auto",
	}
}

func snapshotVisibilityAdaptivePrefixModes() []string {
	return []string{
		"adaptive:skiplist",
		"adaptive:hash_sorted",
		"adaptive:btree",
		"adaptive:append_only",
	}
}

func snapshotVisibilityModesForCheckpoint(checkpoint bool) []string {
	if !checkpoint {
		return snapshotVisibilityModes()
	}
	return append(snapshotVisibilityModes(), snapshotVisibilityAdaptivePrefixModes()...)
}

func snapshotVisibilityShardsForCheckpoint(checkpoint bool) []int {
	if !checkpoint {
		return []int{1}
	}
	return []int{2, 4, 8}
}

func snapshotVisibilityCheckpointModes() []bool {
	return []bool{false, true}
}

func snapshotVisibilityCheckpointLabel(checkpoint bool) string {
	if checkpoint {
		return "with_checkpoint"
	}
	return "without_checkpoint"
}

func baseSnapshotVisibilityOptions(t *testing.T, mode string, mutator func(*Options)) Options {
	t.Helper()
	opts := OptionsFor(ProfileBench, t.TempDir())
	opts.MemtableMode = mode
	opts.MemtableShards = 1
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	if mutator != nil {
		mutator(&opts)
	}
	return opts
}

func assertSnapshotChecks(t *testing.T, snap snapshotReader, cases []snapshotVisibilityCase) {
	t.Helper()
	for _, tc := range cases {
		got, err := snap.GetUnsafe([]byte(tc.key))
		if tc.missing {
			if !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("snapshot.GetUnsafe(%q): want ErrKeyNotFound, got err=%v, val=%q", tc.key, err, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("snapshot.GetUnsafe(%q): %v", tc.key, err)
		}
		if !bytes.Equal(got, tc.wantValue) {
			t.Fatalf("snapshot.GetUnsafe(%q): got=%q want=%q", tc.key, got, tc.wantValue)
		}
	}
}

func runVisibilitySnapshotCheck(t *testing.T, db *DB, cases []snapshotVisibilityCase, checkpoint bool) {
	t.Helper()
	if checkpoint {
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() {
		if err := snap.Close(); err != nil {
			t.Fatalf("snapshot.Close: %v", err)
		}
	}()
	assertSnapshotChecks(t, snap, cases)
}

func TestAcquireSnapshot_SeesJustWrittenBatch(t *testing.T) {
	for _, checkpoint := range snapshotVisibilityCheckpointModes() {
		checkpoint := checkpoint
		t.Run(snapshotVisibilityCheckpointLabel(checkpoint), func(t *testing.T) {
			for _, mode := range snapshotVisibilityModesForCheckpoint(checkpoint) {
				mode := mode
				t.Run(mode, func(t *testing.T) {
					for _, shards := range snapshotVisibilityShardsForCheckpoint(checkpoint) {
						shards := shards
						t.Run(fmt.Sprintf("memtable_shards_%d", shards), func(t *testing.T) {
							opts := baseSnapshotVisibilityOptions(t, mode, func(mut *Options) {
								mut.MemtableShards = shards
							})
							db, err := Open(opts)
							if err != nil {
								t.Fatalf("Open: %v", err)
							}
							defer db.Close()

							batch := db.NewBatch()
							if batch == nil {
								t.Fatal("NewBatch returned nil")
							}
							if err := batch.Set([]byte("k"), []byte("v")); err != nil {
								t.Fatalf("batch.Set: %v", err)
							}
							if err := batch.Write(); err != nil {
								t.Fatalf("batch.Write: %v", err)
							}
							if err := batch.Close(); err != nil {
								t.Fatalf("batch.Close: %v", err)
							}

							runVisibilitySnapshotCheck(t, db, []snapshotVisibilityCase{
								{key: "k", wantValue: []byte("v")},
							}, checkpoint)
						})
					}
				})
			}
		})
	}
}

func TestAcquireSnapshot_SeesJustWrittenBatch_AfterCheckpoint(t *testing.T) {
	for _, checkpoint := range snapshotVisibilityCheckpointModes() {
		checkpoint := checkpoint
		t.Run(snapshotVisibilityCheckpointLabel(checkpoint), func(t *testing.T) {
			for _, mode := range snapshotVisibilityModesForCheckpoint(checkpoint) {
				mode := mode
				t.Run(mode, func(t *testing.T) {
					for _, shards := range snapshotVisibilityShardsForCheckpoint(checkpoint) {
						shards := shards
						t.Run(fmt.Sprintf("memtable_shards_%d", shards), func(t *testing.T) {
							opts := baseSnapshotVisibilityOptions(t, mode, func(mut *Options) {
								mut.MemtableShards = shards
							})
							db, err := Open(opts)
							if err != nil {
								t.Fatalf("Open: %v", err)
							}
							defer db.Close()

							batch := db.NewBatch()
							if batch == nil {
								t.Fatal("NewBatch returned nil")
							}
							if err := batch.Set([]byte("k"), []byte("v")); err != nil {
								t.Fatalf("batch.Set: %v", err)
							}
							if err := batch.Write(); err != nil {
								t.Fatalf("batch.Write: %v", err)
							}
							if err := batch.Close(); err != nil {
								t.Fatalf("batch.Close: %v", err)
							}

							runVisibilitySnapshotCheck(t, db, []snapshotVisibilityCase{
								{key: "k", wantValue: []byte("v")},
							}, checkpoint)
						})
					}
				})
			}
		})
	}
}

func TestAcquireSnapshot_SeesDirectSetWritesAcrossMemtableModes(t *testing.T) {
	for _, checkpoint := range snapshotVisibilityCheckpointModes() {
		checkpoint := checkpoint
		t.Run(snapshotVisibilityCheckpointLabel(checkpoint), func(t *testing.T) {
			for _, mode := range snapshotVisibilityModesForCheckpoint(checkpoint) {
				mode := mode
				t.Run(mode, func(t *testing.T) {
					for _, shards := range snapshotVisibilityShardsForCheckpoint(checkpoint) {
						shards := shards
						t.Run(fmt.Sprintf("memtable_shards_%d", shards), func(t *testing.T) {
							opts := baseSnapshotVisibilityOptions(t, mode, func(mut *Options) {
								mut.MemtableShards = shards
							})
							db, err := Open(opts)
							if err != nil {
								t.Fatalf("Open: %v", err)
							}
							defer db.Close()

							if err := db.Set([]byte("a"), []byte("alpha")); err != nil {
								t.Fatalf("Set(a): %v", err)
							}
							if err := db.Set([]byte("b"), []byte("beta")); err != nil {
								t.Fatalf("Set(b): %v", err)
							}
							if err := db.Set([]byte("c"), []byte("gamma")); err != nil {
								t.Fatalf("Set(c): %v", err)
							}

							runVisibilitySnapshotCheck(t, db, []snapshotVisibilityCase{
								{key: "a", wantValue: []byte("alpha")},
								{key: "b", wantValue: []byte("beta")},
								{key: "c", wantValue: []byte("gamma")},
							}, checkpoint)
						})
					}
				})
			}
		})
	}
}

func TestAcquireSnapshot_SeesOverwriteAndDeleteAcrossMemtableModes(t *testing.T) {
	for _, checkpoint := range snapshotVisibilityCheckpointModes() {
		checkpoint := checkpoint
		t.Run(snapshotVisibilityCheckpointLabel(checkpoint), func(t *testing.T) {
			for _, mode := range snapshotVisibilityModesForCheckpoint(checkpoint) {
				mode := mode
				t.Run(mode, func(t *testing.T) {
					for _, shards := range snapshotVisibilityShardsForCheckpoint(checkpoint) {
						shards := shards
						t.Run(fmt.Sprintf("memtable_shards_%d", shards), func(t *testing.T) {
							opts := baseSnapshotVisibilityOptions(t, mode, func(mut *Options) {
								mut.MemtableShards = shards
							})
							db, err := Open(opts)
							if err != nil {
								t.Fatalf("Open: %v", err)
							}
							defer db.Close()

							if err := db.Set([]byte("k"), []byte("v1")); err != nil {
								t.Fatalf("Set(v1): %v", err)
							}
							runVisibilitySnapshotCheck(t, db, []snapshotVisibilityCase{
								{key: "k", wantValue: []byte("v1")},
							}, checkpoint)

							if err := db.Set([]byte("k"), []byte("v2")); err != nil {
								t.Fatalf("Set(v2): %v", err)
							}
							runVisibilitySnapshotCheck(t, db, []snapshotVisibilityCase{
								{key: "k", wantValue: []byte("v2")},
							}, checkpoint)

							if err := db.Delete([]byte("k")); err != nil {
								t.Fatalf("Delete: %v", err)
							}
							runVisibilitySnapshotCheck(t, db, []snapshotVisibilityCase{
								{key: "k", missing: true},
							}, checkpoint)
						})
					}
				})
			}
		})
	}
}

func TestAcquireSnapshot_SeesLargePointerValuesAcrossMemtableModes(t *testing.T) {
	for _, checkpoint := range snapshotVisibilityCheckpointModes() {
		checkpoint := checkpoint
		t.Run(snapshotVisibilityCheckpointLabel(checkpoint), func(t *testing.T) {
			for _, mode := range snapshotVisibilityModesForCheckpoint(checkpoint) {
				mode := mode
				t.Run(mode, func(t *testing.T) {
					for _, shards := range snapshotVisibilityShardsForCheckpoint(checkpoint) {
						shards := shards
						t.Run(fmt.Sprintf("memtable_shards_%d", shards), func(t *testing.T) {
							opts := baseSnapshotVisibilityOptions(t, mode, func(mut *Options) {
								mut.MemtableShards = shards
								mut.ValueLog.ForcePointers = true
								mut.ValueLog.PointerThreshold = 1
							})
							db, err := Open(opts)
							if err != nil {
								t.Fatalf("Open: %v", err)
							}
							defer db.Close()

							largeValue := bytes.Repeat([]byte("x"), 2048)
							batch := db.NewBatch()
							if batch == nil {
								t.Fatal("NewBatch returned nil")
							}
							if err := batch.Set([]byte("small"), []byte("inline")); err != nil {
								t.Fatalf("batch.Set(small): %v", err)
							}
							if err := batch.Set([]byte("large"), largeValue); err != nil {
								t.Fatalf("batch.Set(large): %v", err)
							}
							if err := batch.Write(); err != nil {
								t.Fatalf("batch.Write: %v", err)
							}
							if err := batch.Close(); err != nil {
								t.Fatalf("batch.Close: %v", err)
							}

							runVisibilitySnapshotCheck(t, db, []snapshotVisibilityCase{
								{key: "small", wantValue: []byte("inline")},
								{key: "large", wantValue: largeValue},
							}, checkpoint)
						})
					}
				})
			}
		})
	}
}

func TestAcquireSnapshot_SeesWritesWithDurableModeAcrossMemtableModes(t *testing.T) {
	for _, checkpoint := range snapshotVisibilityCheckpointModes() {
		checkpoint := checkpoint
		t.Run(snapshotVisibilityCheckpointLabel(checkpoint), func(t *testing.T) {
			for _, mode := range snapshotVisibilityModesForCheckpoint(checkpoint) {
				mode := mode
				t.Run(mode, func(t *testing.T) {
					for _, shards := range snapshotVisibilityShardsForCheckpoint(checkpoint) {
						shards := shards
						t.Run(fmt.Sprintf("memtable_shards_%d", shards), func(t *testing.T) {
							opts := baseSnapshotVisibilityOptions(t, mode, func(mut *Options) {
								mut.MemtableShards = shards
								mut.Durability = DurabilityDurable
							})
							db, err := Open(opts)
							if err != nil {
								t.Fatalf("Open: %v", err)
							}
							defer db.Close()

							batch := db.NewBatch()
							if batch == nil {
								t.Fatal("NewBatch returned nil")
							}
							if err := batch.Set([]byte("k"), []byte("v")); err != nil {
								t.Fatalf("batch.Set: %v", err)
							}
							if err := batch.Write(); err != nil {
								t.Fatalf("batch.Write: %v", err)
							}
							if err := batch.Close(); err != nil {
								t.Fatalf("batch.Close: %v", err)
							}

							runVisibilitySnapshotCheck(t, db, []snapshotVisibilityCase{
								{key: "k", wantValue: []byte("v")},
							}, checkpoint)
						})
					}
				})
			}
		})
	}
}
