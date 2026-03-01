package caching_test

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sort"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func be8Key(v uint64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], v)
	return append([]byte(nil), k[:]...)
}

func phaseValue(tag byte) []byte {
	return bytes.Repeat([]byte{tag}, 100)
}

func bankLikeRouteKey(version uint64, seq uint32) []byte {
	const prefix = "s/k:bank/s"
	key := make([]byte, len(prefix)+8+4)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):len(prefix)+8], version)
	binary.BigEndian.PutUint32(key[len(prefix)+8:], seq)
	return key
}

func putExpected(expected map[string][]byte, key, value []byte) {
	expected[string(key)] = append([]byte(nil), value...)
}

func deterministicDatasetKeys32(count int, seed int64, sortedOrder bool) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	out := make([][]byte, 0, count)
	seen := make(map[string]struct{}, count)
	for len(out) < count {
		k := make([]byte, 32)
		_, _ = rng.Read(k)
		if _, ok := seen[string(k)]; ok {
			continue
		}
		seen[string(k)] = struct{}{}
		out = append(out, k)
	}
	if sortedOrder {
		sort.Slice(out, func(i, j int) bool {
			return bytes.Compare(out[i], out[j]) < 0
		})
	}
	return out
}

func commitBatch(t *testing.T, b treedb.Batch, label string) {
	t.Helper()
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("%s write: %v", label, err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("%s close: %v", label, err)
	}
}

// Regression: route mode must preserve exact state parity after
// dataset_write_sorted-style 32B-key writes.
func TestRegression_RouteMode_DatasetWriteSortedParity(t *testing.T) {
	const keys = 5000
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.IndexOuterLeafMode = treedb.IndexOuterLeafModeV1
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 512

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	expected := make(map[string][]byte, keys*3)

	seqVal := phaseValue('s')
	for i := 0; i < keys; i++ {
		k := be8Key(uint64(i))
		if err := db.Set(k, seqVal); err != nil {
			t.Fatalf("sequential set %d: %v", i, err)
		}
		putExpected(expected, k, seqVal)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after sequential_write: %v", err)
	}

	rng := rand.New(rand.NewSource(1))
	randVal := phaseValue('r')
	for i := 0; i < keys; i++ {
		k := be8Key(uint64(rng.Intn(keys * 10)))
		if err := db.Set(k, randVal); err != nil {
			t.Fatalf("random set %d: %v", i, err)
		}
		putExpected(expected, k, randVal)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after random_write: %v", err)
	}

	dsRand := deterministicDatasetKeys32(keys, 11, false)
	dsRandVal := phaseValue('d')
	for i := range dsRand {
		if err := db.Set(dsRand[i], dsRandVal); err != nil {
			t.Fatalf("dataset random set %d: %v", i, err)
		}
		putExpected(expected, dsRand[i], dsRandVal)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after dataset_write_random: %v", err)
	}

	dsSorted := deterministicDatasetKeys32(keys, 29, true)
	dsSortedVal := phaseValue('q')
	for i := range dsSorted {
		if err := db.Set(dsSorted[i], dsSortedVal); err != nil {
			t.Fatalf("dataset sorted set %d: %v", i, err)
		}
		putExpected(expected, dsSorted[i], dsSortedVal)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after dataset_write_sorted: %v", err)
	}

	assertRouteParityState(t, db, expected)
}

// Regression: route mode must preserve exact state parity after
// batch_random-style 8B-key churn.
func TestRegression_RouteMode_BatchRandom8BParity(t *testing.T) {
	const (
		keys      = 5000
		batchSize = 8000
	)
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.IndexOuterLeafMode = treedb.IndexOuterLeafModeV1
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 512

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	expected := make(map[string][]byte, keys*12)

	seqVal := phaseValue('s')
	for i := 0; i < keys; i++ {
		k := be8Key(uint64(i))
		if err := db.Set(k, seqVal); err != nil {
			t.Fatalf("sequential set %d: %v", i, err)
		}
		putExpected(expected, k, seqVal)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after sequential_write: %v", err)
	}

	rngWrite := rand.New(rand.NewSource(1))
	randVal := phaseValue('r')
	for i := 0; i < keys; i++ {
		k := be8Key(uint64(rngWrite.Intn(keys * 10)))
		if err := db.Set(k, randVal); err != nil {
			t.Fatalf("random set %d: %v", i, err)
		}
		putExpected(expected, k, randVal)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after random_write: %v", err)
	}

	batchWriteVal := phaseValue('b')
	for i := 0; i < keys; i += batchSize {
		b := db.NewBatch()
		end := i + batchSize
		if end > keys {
			end = keys
		}
		for j := i; j < end; j++ {
			k := be8Key(uint64(j + keys))
			if err := b.Set(k, batchWriteVal); err != nil {
				_ = b.Close()
				t.Fatalf("batch_write set %d: %v", j, err)
			}
			putExpected(expected, k, batchWriteVal)
		}
		commitBatch(t, b, "batch_write")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after batch_write: %v", err)
	}

	batchSteadyVal := phaseValue('t')
	for i := 0; i < keys; i += batchSize {
		b := db.NewBatch()
		end := i + batchSize
		if end > keys {
			end = keys
		}
		for j := i; j < end; j++ {
			k := be8Key(uint64(j + keys))
			if err := b.Set(k, batchSteadyVal); err != nil {
				_ = b.Close()
				t.Fatalf("batch_write_steady set %d: %v", j, err)
			}
			putExpected(expected, k, batchSteadyVal)
		}
		commitBatch(t, b, "batch_write_steady")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after batch_write_steady: %v", err)
	}

	rngBatch := rand.New(rand.NewSource(3))
	batchRandomVal := phaseValue('n')
	for i := 0; i < keys; i += batchSize {
		b := db.NewBatch()
		end := i + batchSize
		if end > keys {
			end = keys
		}
		for j := i; j < end; j++ {
			k := be8Key(uint64(rngBatch.Intn(keys * 10)))
			if err := b.Set(k, batchRandomVal); err != nil {
				_ = b.Close()
				t.Fatalf("batch_random set %d: %v", j, err)
			}
			putExpected(expected, k, batchRandomVal)
		}
		commitBatch(t, b, "batch_random")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after batch_random: %v", err)
	}

	assertRouteParityState(t, db, expected)
}

// Regression: route mode WriteSync with unsorted bank-like keys must keep
// Get/Has/Iterator/ReverseIterator parity (including after reopen).
func TestRegression_RouteMode_UnsortedWriteSyncBankLikeParityAfterReopen(t *testing.T) {
	const total = 2048
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.IndexOuterLeafMode = treedb.IndexOuterLeafModeV1
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 512

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	expected := make(map[string][]byte, total)
	perm := rand.New(rand.NewSource(911))
	order := perm.Perm(total)

	b := db.NewBatch()
	for i := 0; i < total; i++ {
		seq := uint32(order[i])
		key := bankLikeRouteKey(10014000, seq)
		value := bytes.Repeat([]byte{byte(seq % 251)}, 64)
		if err := b.Set(key, value); err != nil {
			_ = b.Close()
			t.Fatalf("set seq=%d: %v", seq, err)
		}
		putExpected(expected, key, value)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("writesync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch close: %v", err)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	assertRouteParityState(t, db, expected)

	if err := db.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	assertRouteParityState(t, reopened, expected)
}
