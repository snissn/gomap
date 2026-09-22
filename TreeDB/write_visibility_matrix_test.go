package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func visibilityIAVLStoreNodeKey(version uint64, nonce uint32) []byte {
	key := make([]byte, 13)
	key[0] = 's'
	binary.BigEndian.PutUint64(key[1:9], version)
	binary.BigEndian.PutUint32(key[9:13], nonce)
	return key
}

func visibilityIAVLVersionScanBounds() ([]byte, []byte) {
	start := make([]byte, 9)
	start[0] = 's'
	binary.BigEndian.PutUint64(start[1:9], uint64(1))
	end := make([]byte, 9)
	end[0] = 's'
	binary.BigEndian.PutUint64(end[1:9], ^uint64(0))
	return start, end
}

func visibilityVersionFromKey(t *testing.T, key []byte) uint64 {
	t.Helper()
	if len(key) < 9 || key[0] != 's' {
		t.Fatalf("invalid IAVL-style key %x", key)
	}
	return binary.BigEndian.Uint64(key[1:9])
}

func visibilityKey(i int) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(i))
	return key
}

func visibilityValue(tag string, i int, size int) []byte {
	if size < 16 {
		size = 16
	}
	out := make([]byte, size)
	copy(out, []byte(tag))
	for j := len(tag); j < len(out); j++ {
		out[j] = byte(i>>uint(j%8)) ^ byte(j*17+3)
	}
	return out
}

func TestWriteReadVisibilityMatrix(t *testing.T) {
	t.Parallel()

	type tc struct {
		name              string
		profile           Profile
		forcePointers     bool
		outerLeavesInVlog bool
		valueSize         int
	}

	cases := []tc{
		{name: "durable_inline", profile: ProfileDurable, forcePointers: false, outerLeavesInVlog: false, valueSize: 96},
		{name: "durable_pointer", profile: ProfileDurable, forcePointers: true, outerLeavesInVlog: false, valueSize: 1024},
		{name: "wal_on_fast_inline", profile: ProfileWALOnFast, forcePointers: false, outerLeavesInVlog: true, valueSize: 96},
		{name: "wal_on_fast_pointer", profile: ProfileWALOnFast, forcePointers: true, outerLeavesInVlog: true, valueSize: 1024},
		{name: "fast_inline", profile: ProfileFast, forcePointers: false, outerLeavesInVlog: true, valueSize: 96},
		{name: "fast_pointer", profile: ProfileFast, forcePointers: true, outerLeavesInVlog: true, valueSize: 1024},
	}

	const (
		batches        = 6
		keysPerBatch   = 192
		flushThreshold = 64 << 20
	)

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			opts := OptionsFor(tc.profile, t.TempDir())
			opts.FlushThreshold = flushThreshold
			opts.BackgroundIndexVacuumInterval = -1
			opts.DisableBackgroundPrune = true
			opts.MaxQueuedMemtables = -1
			opts.WriterFlushMaxMemtables = 0
			opts.WriterFlushMaxDuration = 0
			opts.ValueLog.Generational.Policy = ValueLogGenerationOff
			opts.IndexOuterLeavesInValueLog = tc.outerLeavesInVlog
			opts.ValueLog.ForcePointers = tc.forcePointers
			if tc.forcePointers {
				opts.ValueLog.PointerThreshold = 1
			}

			db, err := Open(opts)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()

			live := make(map[int][]byte, batches*keysPerBatch)
			next := 0

			for batchIdx := 0; batchIdx < batches; batchIdx++ {
				b := db.NewBatch()
				if b == nil {
					t.Fatalf("NewBatch returned nil")
				}
				for j := 0; j < keysPerBatch; j++ {
					keyIdx := next
					next++
					val := visibilityValue(fmt.Sprintf("%s-b%d", tc.name, batchIdx), keyIdx, tc.valueSize)
					if err := b.Set(visibilityKey(keyIdx), val); err != nil {
						_ = b.Close()
						t.Fatalf("batch %d set key=%d: %v", batchIdx, keyIdx, err)
					}
					live[keyIdx] = val
				}
				if err := b.Write(); err != nil {
					_ = b.Close()
					t.Fatalf("batch %d write: %v", batchIdx, err)
				}
				if err := b.Close(); err != nil {
					t.Fatalf("batch %d close: %v", batchIdx, err)
				}

				// Immediate read-after-Write contract: values must be visible before
				// any checkpoint/flush boundary.
				start := batchIdx * keysPerBatch
				for keyIdx := start; keyIdx < next; keyIdx++ {
					got, err := db.Get(visibilityKey(keyIdx))
					if err != nil {
						t.Fatalf("immediate get key=%d: %v", keyIdx, err)
					}
					if !bytes.Equal(got, live[keyIdx]) {
						t.Fatalf("immediate get key=%d mismatch got_len=%d want_len=%d", keyIdx, len(got), len(live[keyIdx]))
					}
				}
			}

			if err := db.Checkpoint(); err != nil {
				t.Fatalf("checkpoint: %v", err)
			}

			for keyIdx, want := range live {
				got, err := db.Get(visibilityKey(keyIdx))
				if err != nil {
					t.Fatalf("post-checkpoint get key=%d: %v", keyIdx, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("post-checkpoint get key=%d mismatch got_len=%d want_len=%d", keyIdx, len(got), len(want))
				}
			}

			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			db = nil

			db, err = Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}

			for keyIdx, want := range live {
				got, err := db.Get(visibilityKey(keyIdx))
				if err != nil {
					t.Fatalf("reopen get key=%d: %v", keyIdx, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("reopen get key=%d mismatch got_len=%d want_len=%d", keyIdx, len(got), len(want))
				}
			}
		})
	}
}

func TestWriteReadVisibilityLargeBatchPaths(t *testing.T) {
	t.Parallel()

	type tc struct {
		name              string
		profile           Profile
		forcePointers     bool
		outerLeavesInVlog bool
		flushThreshold    int64
		keys              int
		valueSize         int
		descending        bool
	}

	cases := []tc{
		{
			name:              "fast_stream_inline",
			profile:           ProfileFast,
			forcePointers:     false,
			outerLeavesInVlog: true,
			flushThreshold:    64 << 20,
			keys:              4608,
			valueSize:         320,
		},
		{
			name:              "fast_stream_pointer",
			profile:           ProfileFast,
			forcePointers:     true,
			outerLeavesInVlog: true,
			flushThreshold:    64 << 20,
			keys:              4608,
			valueSize:         1024,
		},
		{
			name:              "wal_on_fast_stream_inline",
			profile:           ProfileWALOnFast,
			forcePointers:     false,
			outerLeavesInVlog: true,
			flushThreshold:    64 << 20,
			keys:              4608,
			valueSize:         320,
		},
		{
			name:              "wal_on_fast_stream_pointer",
			profile:           ProfileWALOnFast,
			forcePointers:     true,
			outerLeavesInVlog: true,
			flushThreshold:    64 << 20,
			keys:              4608,
			valueSize:         1024,
		},
		{
			name:              "fast_bypass_inline",
			profile:           ProfileFast,
			forcePointers:     false,
			outerLeavesInVlog: true,
			flushThreshold:    1 << 20,
			keys:              4096,
			valueSize:         320,
			descending:        true,
		},
		{
			name:              "fast_bypass_pointer",
			profile:           ProfileFast,
			forcePointers:     true,
			outerLeavesInVlog: true,
			flushThreshold:    1 << 20,
			keys:              1536,
			valueSize:         1024,
			descending:        true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			opts := OptionsFor(tc.profile, t.TempDir())
			opts.FlushThreshold = tc.flushThreshold
			opts.BackgroundIndexVacuumInterval = -1
			opts.DisableBackgroundPrune = true
			opts.MaxQueuedMemtables = -1
			opts.WriterFlushMaxMemtables = 0
			opts.WriterFlushMaxDuration = 0
			opts.ValueLog.Generational.Policy = ValueLogGenerationOff
			opts.IndexOuterLeavesInValueLog = tc.outerLeavesInVlog
			opts.ValueLog.ForcePointers = tc.forcePointers
			if tc.forcePointers {
				opts.ValueLog.PointerThreshold = 1
			}

			db, err := Open(opts)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()

			b := db.NewBatch()
			if b == nil {
				t.Fatalf("NewBatch returned nil")
			}

			live := make(map[int][]byte, tc.keys)
			for i := 0; i < tc.keys; i++ {
				keyIdx := i
				if tc.descending {
					keyIdx = tc.keys - 1 - i
				}
				val := visibilityValue(tc.name, keyIdx, tc.valueSize)
				if err := b.Set(visibilityKey(keyIdx), val); err != nil {
					_ = b.Close()
					t.Fatalf("set key=%d: %v", keyIdx, err)
				}
				live[keyIdx] = val
			}

			if err := b.Write(); err != nil {
				_ = b.Close()
				t.Fatalf("write: %v", err)
			}
			if err := b.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			for keyIdx, want := range live {
				got, err := db.Get(visibilityKey(keyIdx))
				if err != nil {
					t.Fatalf("immediate get key=%d: %v", keyIdx, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("immediate get key=%d mismatch got_len=%d want_len=%d", keyIdx, len(got), len(want))
				}
			}

			if err := db.Checkpoint(); err != nil {
				t.Fatalf("checkpoint: %v", err)
			}

			for keyIdx, want := range live {
				got, err := db.Get(visibilityKey(keyIdx))
				if err != nil {
					t.Fatalf("post-checkpoint get key=%d: %v", keyIdx, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("post-checkpoint get key=%d mismatch got_len=%d want_len=%d", keyIdx, len(got), len(want))
				}
			}

			if err := db.Close(); err != nil {
				t.Fatalf("close db: %v", err)
			}
			db = nil

			db, err = Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}

			for keyIdx, want := range live {
				got, err := db.Get(visibilityKey(keyIdx))
				if err != nil {
					t.Fatalf("reopen get key=%d: %v", keyIdx, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("reopen get key=%d mismatch got_len=%d want_len=%d", keyIdx, len(got), len(want))
				}
			}
		})
	}
}

func TestFastWriteSyncRepeatedOverwriteLargeValue(t *testing.T) {
	t.Parallel()

	opts := OptionsFor(ProfileFast, t.TempDir())
	opts.FlushThreshold = 64 << 20
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.MaxQueuedMemtables = -1
	opts.WriterFlushMaxMemtables = 0
	opts.WriterFlushMaxDuration = 0
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	const rounds = 48
	stateKey := []byte("stateKey")
	latestState := []byte(nil)
	auxLive := make(map[string][]byte, rounds*2)

	for round := 1; round <= rounds; round++ {
		b := db.NewBatch()
		if b == nil {
			t.Fatalf("NewBatch returned nil")
		}

		valsKey := []byte(fmt.Sprintf("validatorsKey:%08d", round+1))
		valsVal := visibilityValue("validators", round, 768)
		if err := b.Set(valsKey, valsVal); err != nil {
			_ = b.Close()
			t.Fatalf("round %d set validators: %v", round, err)
		}
		auxLive[string(valsKey)] = valsVal

		paramsKey := []byte(fmt.Sprintf("consensusParamsKey:%08d", round))
		paramsVal := visibilityValue("params", round, 384)
		if err := b.Set(paramsKey, paramsVal); err != nil {
			_ = b.Close()
			t.Fatalf("round %d set params: %v", round, err)
		}
		auxLive[string(paramsKey)] = paramsVal

		stateVal := visibilityValue("state", round, 22*1024+round%257)
		if err := b.Set(stateKey, stateVal); err != nil {
			_ = b.Close()
			t.Fatalf("round %d set stateKey: %v", round, err)
		}
		latestState = stateVal

		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("round %d WriteSync: %v", round, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("round %d Close: %v", round, err)
		}

		got, err := db.Get(stateKey)
		if err != nil {
			t.Fatalf("round %d get stateKey: %v", round, err)
		}
		if !bytes.Equal(got, latestState) {
			t.Fatalf("round %d stateKey mismatch got_len=%d want_len=%d", round, len(got), len(latestState))
		}

		gotVals, err := db.Get(valsKey)
		if err != nil {
			t.Fatalf("round %d get validators: %v", round, err)
		}
		if !bytes.Equal(gotVals, valsVal) {
			t.Fatalf("round %d validators mismatch got_len=%d want_len=%d", round, len(gotVals), len(valsVal))
		}

		if round%8 == 0 {
			if err := db.Close(); err != nil {
				t.Fatalf("round %d close for reopen: %v", round, err)
			}
			db = nil
			db, err = Open(opts)
			if err != nil {
				t.Fatalf("round %d reopen: %v", round, err)
			}
			got, err = db.Get(stateKey)
			if err != nil {
				t.Fatalf("round %d reopen get stateKey: %v", round, err)
			}
			if !bytes.Equal(got, latestState) {
				t.Fatalf("round %d reopen stateKey mismatch got_len=%d want_len=%d", round, len(got), len(latestState))
			}
		}
	}

	for key, want := range auxLive {
		got, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("final get %q: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("final get %q mismatch got_len=%d want_len=%d", key, len(got), len(want))
		}
	}
}

func TestFastWriteSyncPrefixedReverseIteratorVisibility(t *testing.T) {
	t.Parallel()

	opts := OptionsFor(ProfileFast, t.TempDir())
	opts.FlushThreshold = 64 << 20
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.MaxQueuedMemtables = -1
	opts.WriterFlushMaxMemtables = 0
	opts.WriterFlushMaxDuration = 0
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	stores := []string{"acc", "gov", "staking", "warp"}
	const rounds = 20
	const keysPerStore = 192

	for round := 1; round <= rounds; round++ {
		version := uint64(9_000_000 + round)
		for si, storeName := range stores {
			prefix := []byte("s/k:" + storeName + "/")
			b := db.NewBatch()
			if b == nil {
				t.Fatalf("round %d store=%s NewBatch returned nil", round, storeName)
			}
			for i := 0; i < keysPerStore; i++ {
				nonce := uint32(i + 2)
				key := append(append([]byte(nil), prefix...), visibilityIAVLStoreNodeKey(version, nonce)...)
				val := visibilityValue(fmt.Sprintf("%s-r%d", storeName, round), si*keysPerStore+i, 2048+(i%37))
				if err := b.Set(key, val); err != nil {
					_ = b.Close()
					t.Fatalf("round %d store=%s set nonce=%d: %v", round, storeName, nonce, err)
				}
			}
			rootKey := append(append([]byte(nil), prefix...), visibilityIAVLStoreNodeKey(version, 1)...)
			rootVal := visibilityValue(fmt.Sprintf("%s-root-r%d", storeName, round), si, 3072+round%97)
			if err := b.Set(rootKey, rootVal); err != nil {
				_ = b.Close()
				t.Fatalf("round %d store=%s set root: %v", round, storeName, err)
			}
			if err := b.WriteSync(); err != nil {
				_ = b.Close()
				t.Fatalf("round %d store=%s WriteSync: %v", round, storeName, err)
			}
			if err := b.Close(); err != nil {
				t.Fatalf("round %d store=%s Close: %v", round, storeName, err)
			}

			gotRoot, err := db.Get(rootKey)
			if err != nil {
				t.Fatalf("round %d store=%s immediate root get: %v", round, storeName, err)
			}
			if !bytes.Equal(gotRoot, rootVal) {
				t.Fatalf("round %d store=%s immediate root mismatch got_len=%d want_len=%d", round, storeName, len(gotRoot), len(rootVal))
			}

			start, end := visibilityIAVLVersionScanBounds()
			start = append(append([]byte(nil), prefix...), start...)
			end = append(append([]byte(nil), prefix...), end...)
			rit, err := db.ReverseIterator(start, end)
			if err != nil {
				t.Fatalf("round %d store=%s ReverseIterator: %v", round, storeName, err)
			}
			if !rit.Valid() {
				_ = rit.Close()
				t.Fatalf("round %d store=%s ReverseIterator invalid", round, storeName)
			}
			gotVersion := visibilityVersionFromKey(t, rit.Key()[len(prefix):])
			if gotVersion != version {
				_ = rit.Close()
				t.Fatalf("round %d store=%s latest version got=%d want=%d", round, storeName, gotVersion, version)
			}
			if err := rit.Error(); err != nil {
				_ = rit.Close()
				t.Fatalf("round %d store=%s ReverseIterator error: %v", round, storeName, err)
			}
			if err := rit.Close(); err != nil {
				t.Fatalf("round %d store=%s ReverseIterator close: %v", round, storeName, err)
			}
		}

		meta := db.NewBatch()
		if meta == nil {
			t.Fatalf("round %d metadata NewBatch returned nil", round)
		}
		latestValue := make([]byte, 8)
		binary.BigEndian.PutUint64(latestValue, uint64(version))
		if err := meta.Set([]byte("s/latest"), latestValue); err != nil {
			_ = meta.Close()
			t.Fatalf("round %d metadata s/latest: %v", round, err)
		}
		commitInfoKey := []byte(fmt.Sprintf("s/%d", version))
		commitInfoVal := visibilityValue("commitinfo", round, 1536+round%53)
		if err := meta.Set(commitInfoKey, commitInfoVal); err != nil {
			_ = meta.Close()
			t.Fatalf("round %d metadata commit info: %v", round, err)
		}
		if err := meta.WriteSync(); err != nil {
			_ = meta.Close()
			t.Fatalf("round %d metadata WriteSync: %v", round, err)
		}
		if err := meta.Close(); err != nil {
			t.Fatalf("round %d metadata Close: %v", round, err)
		}

		gotLatest, err := db.Get([]byte("s/latest"))
		if err != nil {
			t.Fatalf("round %d metadata get latest: %v", round, err)
		}
		if !bytes.Equal(gotLatest, latestValue) {
			t.Fatalf("round %d metadata latest mismatch got=%x want=%x", round, gotLatest, latestValue)
		}

		if round%5 == 0 {
			if err := db.Close(); err != nil {
				t.Fatalf("round %d close for reopen: %v", round, err)
			}
			db = nil
			db, err = Open(opts)
			if err != nil {
				t.Fatalf("round %d reopen: %v", round, err)
			}
			gotLatest, err = db.Get([]byte("s/latest"))
			if err != nil {
				t.Fatalf("round %d reopen get latest: %v", round, err)
			}
			if !bytes.Equal(gotLatest, latestValue) {
				t.Fatalf("round %d reopen latest mismatch got=%x want=%x", round, gotLatest, latestValue)
			}
		}
	}
}
