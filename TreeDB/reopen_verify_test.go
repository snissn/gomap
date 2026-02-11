package treedb_test

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"math/rand"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

const (
	verifyBatchSize   = 200
	verifyLargeValLen = 16 * 1024
)

func buildVerifyDataset(n int) ([][]byte, map[string][]byte, uint64) {
	keys := make([][]byte, 0, n)
	values := make(map[string][]byte, n)
	hasher := fnv.New64a()

	for i := 0; i < n; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		var val []byte
		if i%2 == 0 {
			val = bytes.Repeat([]byte{byte(i)}, 32)
		} else {
			val = bytes.Repeat([]byte{byte(i)}, verifyLargeValLen)
		}
		keys = append(keys, key)
		values[string(key)] = val
		_, _ = hasher.Write(key)
		_, _ = hasher.Write(val)
	}

	return keys, values, hasher.Sum64()
}

func writeDataset(t *testing.T, db *treedb.DB, keys [][]byte, values map[string][]byte, sync bool) {
	t.Helper()

	var batch treedb.Batch
	batch = db.NewBatch()
	defer batch.Close()

	for i, key := range keys {
		val := values[string(key)]
		if err := batch.Set(key, val); err != nil {
			t.Fatalf("batch set: %v", err)
		}
		if (i+1)%verifyBatchSize == 0 {
			var err error
			if sync {
				err = batch.WriteSync()
			} else {
				err = batch.Write()
			}
			if err != nil {
				t.Fatalf("batch write: %v", err)
			}
			if err := batch.Close(); err != nil {
				t.Fatalf("batch close: %v", err)
			}
			batch = db.NewBatch()
		}
	}

	if batch != nil {
		var err error
		if sync {
			err = batch.WriteSync()
		} else {
			err = batch.Write()
		}
		if err != nil {
			t.Fatalf("batch write final: %v", err)
		}
		if err := batch.Close(); err != nil {
			t.Fatalf("batch close final: %v", err)
		}
	}
}

func checkGets(t *testing.T, db *treedb.DB, keys [][]byte, values map[string][]byte, allowMissing bool) {
	t.Helper()

	rng := rand.New(rand.NewSource(1))
	samples := 50
	if len(keys) < samples {
		samples = len(keys)
	}

	for i := 0; i < samples; i++ {
		key := keys[rng.Intn(len(keys))]
		got, err := db.Get(key)
		if err != nil {
			if allowMissing && err == treedb.ErrKeyNotFound {
				continue
			}
			t.Fatalf("get: %v", err)
		}
		want := values[string(key)]
		if !bytes.Equal(got, want) {
			t.Fatalf("get mismatch: got %d bytes, want %d", len(got), len(want))
		}
	}
}

func scanAndCheck(t *testing.T, db *treedb.DB, values map[string][]byte, allowMissing bool, wantHash uint64) {
	t.Helper()

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()

	hasher := fnv.New64a()
	count := 0
	for ; it.Valid(); it.Next() {
		key := it.KeyCopy(nil)
		val := it.ValueCopy(nil)
		want, ok := values[string(key)]
		if !ok {
			t.Fatalf("scan unexpected key: %x", key)
		}
		if !bytes.Equal(val, want) {
			t.Fatalf("scan value mismatch for key %x", key)
		}
		_, _ = hasher.Write(key)
		_, _ = hasher.Write(val)
		count++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	if !allowMissing && count != len(values) {
		t.Fatalf("scan count mismatch: got %d want %d", count, len(values))
	}
	if !allowMissing && hasher.Sum64() != wantHash {
		t.Fatalf("scan hash mismatch: got %d want %d", hasher.Sum64(), wantHash)
	}
	if allowMissing && count == 0 {
		t.Fatalf("scan produced no keys")
	}
}

func TestReopenVerify_WALOn_Checkpoint(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	writeDataset(t, db, keys, values, false)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	checkGets(t, reopen, keys, values, false)
	scanAndCheck(t, reopen, values, false, hash)
}

func TestReopenVerify_CurrentSetPublishedOnCheckpoint(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	key := []byte("checkpoint-pointer-key")
	want := bytes.Repeat([]byte("z"), 32*1024)
	if err := db.Set(key, want); err != nil {
		_ = db.Close()
		t.Fatalf("set: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get(key)
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("get mismatch after reopen: got %d bytes want %d", len(got), len(want))
	}
}

func TestReopenVerify_InternalBaseDelta_WALOn_Checkpoint(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir:                    dir,
		IndexInternalBaseDelta: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	writeDataset(t, db, keys, values, false)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	checkGets(t, reopen, keys, values, false)
	scanAndCheck(t, reopen, values, false, hash)
}

func TestReopenVerify_WALOn_WriteSync(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	writeDataset(t, db, keys, values, true)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	checkGets(t, reopen, keys, values, false)
	scanAndCheck(t, reopen, values, false, hash)
}

func TestIterator_GroupedPointerBatching_ReopenDurability(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	writeDataset(t, db, keys, values, false)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	checkGets(t, reopen, keys, values, false)
	scanAndCheck(t, reopen, values, false, hash)

	rit, err := reopen.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	defer rit.Close()

	count := 0
	for idx := len(keys) - 1; rit.Valid(); rit.Next() {
		key := rit.KeyCopy(nil)
		if !bytes.Equal(key, keys[idx]) {
			t.Fatalf("reverse key mismatch at %d: got %x want %x", idx, key, keys[idx])
		}
		val := rit.ValueCopy(nil)
		want := values[string(keys[idx])]
		if !bytes.Equal(val, want) {
			t.Fatalf("reverse value mismatch for key %x", key)
		}
		idx--
		count++
	}
	if err := rit.Error(); err != nil {
		t.Fatalf("reverse iterator error: %v", err)
	}
	if count != len(keys) {
		t.Fatalf("reverse scan count mismatch: got %d want %d", count, len(keys))
	}
}

func TestReopenVerify_WALOn_Checkpoint_CompressionModes(t *testing.T) {
	cases := []struct {
		name        string
		compression treedb.ValueLogCompressionMode
		codec       treedb.ValueLogBlockCodec
	}{
		{name: "off", compression: treedb.ValueLogCompressionOff},
		{name: "block_snappy", compression: treedb.ValueLogCompressionBlock, codec: treedb.ValueLogBlockSnappy},
		{name: "block_lz4", compression: treedb.ValueLogCompressionBlock, codec: treedb.ValueLogBlockLZ4},
		{name: "dict", compression: treedb.ValueLogCompressionDict},
		{name: "auto", compression: treedb.ValueLogCompressionAuto},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			keys, values, hash := buildVerifyDataset(1500)

			opts := treedb.Options{
				Dir: dir,
				ValueLog: treedb.ValueLogOptions{
					PointerThreshold: 1,
					Compression:      tc.compression,
					BlockCodec:       tc.codec,
				},
			}
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			writeDataset(t, db, keys, values, false)
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("checkpoint: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			reopen, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopen.Close()

			checkGets(t, reopen, keys, values, false)
			scanAndCheck(t, reopen, values, false, hash)
		})
	}
}

func TestReopenVerify_WALOff_NoJournal(t *testing.T) {
	dir := t.TempDir()
	keys, values, _ := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir:        dir,
		Durability: treedb.DurabilityWALOffRelaxed,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	writeDataset(t, db, keys, values, false)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	checkGets(t, reopen, keys, values, true)
	scanAndCheck(t, reopen, values, true, 0)
}

func TestReopenVerify_IndexColumnarLeaves(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(4000)

	opts := treedb.Options{
		Dir:                 dir,
		IndexColumnarLeaves: true,
		ChunkSize:           64 * 1024,
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	writeDataset(t, db, keys, values, true)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	checkGets(t, reopen, keys, values, false)
	scanAndCheck(t, reopen, values, false, hash)
}
