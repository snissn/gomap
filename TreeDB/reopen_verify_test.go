package treedb_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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

func countValueLogRecords(t *testing.T, rootDir string) int {
	t.Helper()
	walDir := filepath.Join(rootDir, "maindb", "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("read wal dir: %v", err)
	}
	total := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		var lane int
		var seq int
		if _, err := fmt.Sscanf(name, "value-l%d-%d.log", &lane, &seq); err != nil {
			continue
		}
		fileID, err := valuelog.EncodeFileID(uint32(lane), uint32(seq))
		if err != nil {
			t.Fatalf("encode file id for %q: %v", name, err)
		}
		path := filepath.Join(walDir, name)
		reader, err := valuelog.NewReader(path, fileID)
		if err != nil {
			t.Fatalf("new reader %q: %v", name, err)
		}
		reader.DisableValueDecode()
		for {
			if _, _, _, err := reader.ReadNext(); err == nil {
				total++
				continue
			} else if err == io.EOF {
				break
			} else {
				_ = reader.Close()
				t.Fatalf("read %q: %v", name, err)
			}
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("close reader %q: %v", name, err)
		}
	}
	return total
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

func TestReopenVerify_LeafBlockHitMiss_ReopenParity(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:         1,
			LeafBlockCodec:           treedb.ValueLogBlockLZ4,
			LeafBlockTargetBytes:     4 << 10,
			LeafBlockRestartInterval: 16,
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

	hitKey := keys[len(keys)/3]
	missingKey := make([]byte, 8)
	binary.BigEndian.PutUint64(missingKey, uint64(len(keys))+111)

	got, err := db.Get(hitKey)
	if err != nil {
		t.Fatalf("get hit before reopen: %v", err)
	}
	if want := values[string(hitKey)]; !bytes.Equal(got, want) {
		t.Fatalf("get hit before reopen mismatch")
	}
	if got, err := db.Get(missingKey); err != nil || got != nil {
		t.Fatalf("get miss before reopen got=%v err=%v, want nil,nil", got, err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot before reopen: nil")
	}
	if _, err := snap.GetEntry(hitKey); err != nil {
		_ = snap.Close()
		t.Fatalf("snapshot GetEntry hit before reopen: %v", err)
	}
	if _, err := snap.GetEntry(missingKey); !errors.Is(err, treedb.ErrKeyNotFound) {
		_ = snap.Close()
		t.Fatalf("snapshot GetEntry miss before reopen err=%v, want ErrKeyNotFound", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close before reopen: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	got, err = reopen.Get(hitKey)
	if err != nil {
		t.Fatalf("get hit after reopen: %v", err)
	}
	if want := values[string(hitKey)]; !bytes.Equal(got, want) {
		t.Fatalf("get hit after reopen mismatch")
	}
	if got, err := reopen.Get(missingKey); err != nil || got != nil {
		t.Fatalf("get miss after reopen got=%v err=%v, want nil,nil", got, err)
	}
	scanAndCheck(t, reopen, values, false, hash)
}
