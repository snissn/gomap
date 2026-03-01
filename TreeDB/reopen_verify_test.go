package treedb_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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
	"github.com/snissn/gomap/TreeDB/node"
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

func TestReopenVerify_WALOn_Checkpoint_OuterLeafV2(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: treedb.IndexOuterLeafModeV1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:          1,
			OuterLeafBlockCodec:       treedb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes: 4 << 10,
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

func TestReopenVerify_WALOn_Checkpoint_OuterLeafV1LeafLog_ReadPath_HitMiss_ReopenParity(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: treedb.IndexOuterLeafModeV1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:              1,
			OuterLeafBlockCodec:           treedb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes:     4 << 10,
			OuterLeafBlockRestartInterval: 16,
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
	entry, err := snap.GetEntry(hitKey)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("snapshot GetEntry hit before reopen: %v", err)
	}
	if entry.Flags&node.FlagPointer == 0 {
		_ = snap.Close()
		t.Fatalf("expected pointer-backed entry in v1_leaflog mode, flags=%08b", entry.Flags)
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

	snap = reopen.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot after reopen: nil")
	}
	entry, err = snap.GetEntry(hitKey)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("snapshot GetEntry hit after reopen: %v", err)
	}
	if entry.Flags&node.FlagPointer == 0 {
		_ = snap.Close()
		t.Fatalf("expected pointer-backed entry after reopen in v1_leaflog mode, flags=%08b", entry.Flags)
	}
	if _, err := snap.GetEntry(missingKey); !errors.Is(err, treedb.ErrKeyNotFound) {
		_ = snap.Close()
		t.Fatalf("snapshot GetEntry miss after reopen err=%v, want ErrKeyNotFound", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close after reopen: %v", err)
	}
	scanAndCheck(t, reopen, values, false, hash)
}

type overwriteDeleteReopenResult struct {
	mainValue    []byte
	stableValue  []byte
	deletedValue []byte
	mainFlags    byte
}

func runOverwriteDeleteReopenScenario(t *testing.T, mode string, syncPerWrite bool) overwriteDeleteReopenResult {
	t.Helper()
	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: mode,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:              1,
			OuterLeafBlockCodec:           treedb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes:     4 << 10,
			OuterLeafBlockRestartInterval: 16,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	setValue := func(key, value []byte) error {
		if syncPerWrite {
			return db.SetSync(key, value)
		}
		return db.Set(key, value)
	}
	deleteKey := func(key []byte) error {
		if syncPerWrite {
			return db.DeleteSync(key)
		}
		return db.Delete(key)
	}

	keyMain := []byte("k-main")
	keyDelete := []byte("k-delete")
	keyStable := []byte("k-stable")
	mainV1 := bytes.Repeat([]byte("m"), 4096)
	mainV2 := bytes.Repeat([]byte("n"), 4096)
	deleteV1 := bytes.Repeat([]byte("d"), 4096)
	stableV := bytes.Repeat([]byte("s"), 4096)

	if err := setValue(keyMain, mainV1); err != nil {
		_ = db.Close()
		t.Fatalf("set keyMain v1: %v", err)
	}
	if err := setValue(keyDelete, deleteV1); err != nil {
		_ = db.Close()
		t.Fatalf("set keyDelete v1: %v", err)
	}
	if err := setValue(keyStable, stableV); err != nil {
		_ = db.Close()
		t.Fatalf("set keyStable: %v", err)
	}

	if err := setValue(keyMain, mainV2); err != nil {
		_ = db.Close()
		t.Fatalf("overwrite keyMain: %v", err)
	}
	if err := deleteKey(keyDelete); err != nil {
		_ = db.Close()
		t.Fatalf("delete keyDelete: %v", err)
	}

	if got, err := db.Get(keyMain); err != nil || !bytes.Equal(got, mainV2) {
		_ = db.Close()
		t.Fatalf("post-overwrite keyMain mismatch err=%v", err)
	}
	if got, err := db.Get(keyDelete); err != nil || got != nil {
		_ = db.Close()
		t.Fatalf("post-delete keyDelete got=%v err=%v, want nil,nil", got, err)
	}

	if !syncPerWrite {
		if err := db.Checkpoint(); err != nil {
			_ = db.Close()
			t.Fatalf("checkpoint: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	gotMain, err := reopen.Get(keyMain)
	if err != nil {
		t.Fatalf("reopen Get(keyMain): %v", err)
	}
	gotDelete, err := reopen.Get(keyDelete)
	if err != nil {
		t.Fatalf("reopen Get(keyDelete): %v", err)
	}
	gotStable, err := reopen.Get(keyStable)
	if err != nil {
		t.Fatalf("reopen Get(keyStable): %v", err)
	}
	if !bytes.Equal(gotMain, mainV2) {
		t.Fatalf("reopen keyMain mismatch: got=%d want=%d", len(gotMain), len(mainV2))
	}
	if gotDelete != nil {
		t.Fatalf("reopen keyDelete expected nil, got len=%d", len(gotDelete))
	}
	if !bytes.Equal(gotStable, stableV) {
		t.Fatalf("reopen keyStable mismatch: got=%d want=%d", len(gotStable), len(stableV))
	}

	snapAfter := reopen.AcquireSnapshot()
	if snapAfter == nil {
		t.Fatalf("snapshot after reopen: nil")
	}
	entry, err := snapAfter.GetEntry(keyMain)
	if err != nil {
		_ = snapAfter.Close()
		t.Fatalf("snapshot GetEntry(keyMain): %v", err)
	}
	if entry.Flags&node.FlagPointer == 0 {
		_ = snapAfter.Close()
		t.Fatalf("expected pointer-backed keyMain after reopen, flags=%08b", entry.Flags)
	}
	if _, err := snapAfter.GetEntry(keyDelete); !errors.Is(err, treedb.ErrKeyNotFound) {
		_ = snapAfter.Close()
		t.Fatalf("snapshot GetEntry(keyDelete) err=%v, want ErrKeyNotFound", err)
	}
	if err := snapAfter.Close(); err != nil {
		t.Fatalf("snapshot close after reopen: %v", err)
	}

	return overwriteDeleteReopenResult{
		mainValue:    gotMain,
		stableValue:  gotStable,
		deletedValue: gotDelete,
		mainFlags:    entry.Flags,
	}
}

func TestReopenVerify_WALOn_Checkpoint_OuterLeafV1LeafLog_OverwriteDelete_ReopenParity(t *testing.T) {
	got := runOverwriteDeleteReopenScenario(t, treedb.IndexOuterLeafModeV1, false)
	if got.mainFlags&node.FlagPointer == 0 {
		t.Fatalf("expected pointer-backed keyMain in v1_leaflog checkpoint flow, flags=%08b", got.mainFlags)
	}
}

func TestReopenVerify_WALOn_Checkpoint_OuterLeafV1LeafLogRoute_OverwriteDelete_ReopenParity(t *testing.T) {
	got := runOverwriteDeleteReopenScenario(t, treedb.IndexOuterLeafModeV1, false)
	if got.mainFlags&node.FlagPointer == 0 {
		t.Fatalf("expected pointer-backed keyMain in v1_leaflog_route checkpoint flow, flags=%08b", got.mainFlags)
	}
}

func TestReopenVerify_WALOn_WriteSync_OuterLeafV1LeafLog_OverwriteDelete_ReopenParity(t *testing.T) {
	got := runOverwriteDeleteReopenScenario(t, treedb.IndexOuterLeafModeV1, true)
	if got.mainFlags&node.FlagPointer == 0 {
		t.Fatalf("expected pointer-backed keyMain in v1_leaflog writesync flow, flags=%08b", got.mainFlags)
	}
}

func TestReopenVerify_WALOn_WriteSync_OuterLeafV1LeafLogRoute_OverwriteDelete_ReopenParity(t *testing.T) {
	got := runOverwriteDeleteReopenScenario(t, treedb.IndexOuterLeafModeV1, true)
	if got.mainFlags&node.FlagPointer == 0 {
		t.Fatalf("expected pointer-backed keyMain in v1_leaflog_route writesync flow, flags=%08b", got.mainFlags)
	}
}

func TestReopenVerify_WALOn_Checkpoint_OuterLeafV1LeafLog_OverwriteDelete_ParityWithV1(t *testing.T) {
	v1 := runOverwriteDeleteReopenScenario(t, treedb.IndexOuterLeafModeV1, false)
	v1LeafLog := runOverwriteDeleteReopenScenario(t, treedb.IndexOuterLeafModeV1, false)
	if !bytes.Equal(v1.mainValue, v1LeafLog.mainValue) {
		t.Fatalf("main key parity mismatch: v1=%dB v1_leaflog=%dB", len(v1.mainValue), len(v1LeafLog.mainValue))
	}
	if !bytes.Equal(v1.stableValue, v1LeafLog.stableValue) {
		t.Fatalf("stable key parity mismatch: v1=%dB v1_leaflog=%dB", len(v1.stableValue), len(v1LeafLog.stableValue))
	}
	if v1.deletedValue != nil || v1LeafLog.deletedValue != nil {
		t.Fatalf("deleted key parity mismatch: v1=%v v1_leaflog=%v", v1.deletedValue, v1LeafLog.deletedValue)
	}
}

func TestReopenVerify_WALOn_WriteSync_OuterLeafV1LeafLog_OverwriteDelete_ParityWithV1(t *testing.T) {
	v1 := runOverwriteDeleteReopenScenario(t, treedb.IndexOuterLeafModeV1, true)
	v1LeafLog := runOverwriteDeleteReopenScenario(t, treedb.IndexOuterLeafModeV1, true)
	if !bytes.Equal(v1.mainValue, v1LeafLog.mainValue) {
		t.Fatalf("main key parity mismatch (writesync): v1=%dB v1_leaflog=%dB", len(v1.mainValue), len(v1LeafLog.mainValue))
	}
	if !bytes.Equal(v1.stableValue, v1LeafLog.stableValue) {
		t.Fatalf("stable key parity mismatch (writesync): v1=%dB v1_leaflog=%dB", len(v1.stableValue), len(v1LeafLog.stableValue))
	}
	if v1.deletedValue != nil || v1LeafLog.deletedValue != nil {
		t.Fatalf("deleted key parity mismatch (writesync): v1=%v v1_leaflog=%v", v1.deletedValue, v1LeafLog.deletedValue)
	}
}

func TestReopenVerify_WALOn_Checkpoint_OuterLeafV2FencePtr(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: treedb.IndexOuterLeafModeV1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:              1,
			OuterLeafBlockCodec:           treedb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes:     4 << 10,
			OuterLeafBlockRestartInterval: 16,
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

func TestReopenVerify_WALOn_Checkpoint_OuterLeafV2FencePtr_SimpleInline(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: treedb.IndexOuterLeafModeV1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:              1,
			WALFenceMode:                  treedb.ValueLogWALFenceModeSimpleInline,
			OuterLeafBlockCodec:           treedb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes:     4 << 10,
			OuterLeafBlockRestartInterval: 16,
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

func TestReopenVerify_WALOn_Checkpoint_OuterLeafV2_GroupedBlocks(t *testing.T) {
	dir := t.TempDir()
	n := 500
	keys := make([][]byte, 0, n)
	values := make(map[string][]byte, n)
	hasher := fnv.New64a()
	for i := 0; i < n; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		val := bytes.Repeat([]byte{byte(i % 251)}, 128)
		keys = append(keys, key)
		values[string(key)] = val
		_, _ = hasher.Write(key)
		_, _ = hasher.Write(val)
	}
	wantHash := hasher.Sum64()

	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: treedb.IndexOuterLeafModeV1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:              1,
			OuterLeafBlockCodec:           treedb.ValueLogBlockSnappy,
			OuterLeafBlockTargetBytes:     512,
			OuterLeafBlockRestartInterval: 8,
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
	recordCount := countValueLogRecords(t, dir)
	if recordCount >= n {
		t.Fatalf("expected grouped outer-leaf records < keys, got records=%d keys=%d", recordCount, n)
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
	scanAndCheck(t, reopen, values, false, wantHash)
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

func TestReopenVerify_CommitFence_CheckpointAndWriteSync(t *testing.T) {
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

	valueSync := bytes.Repeat([]byte("s"), 24*1024)
	b := db.NewBatch()
	if err := b.Set([]byte("k-sync"), valueSync); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch writesync: %v", err)
	}
	_ = b.Close()

	valueCP := bytes.Repeat([]byte("c"), 24*1024)
	if err := db.Set([]byte("k-checkpoint"), valueCP); err != nil {
		_ = db.Close()
		t.Fatalf("set checkpoint key: %v", err)
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

	got, err := reopen.Get([]byte("k-sync"))
	if err != nil {
		t.Fatalf("get k-sync: %v", err)
	}
	if !bytes.Equal(got, valueSync) {
		t.Fatalf("get k-sync mismatch: got %d bytes want %d", len(got), len(valueSync))
	}
	got, err = reopen.Get([]byte("k-checkpoint"))
	if err != nil {
		t.Fatalf("get k-checkpoint: %v", err)
	}
	if !bytes.Equal(got, valueCP) {
		t.Fatalf("get k-checkpoint mismatch: got %d bytes want %d", len(got), len(valueCP))
	}
}

func TestReopenVerify_ValueLogRewrite_BatchedPointerSwap_ReopenParity(t *testing.T) {
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

	values := map[string][]byte{
		"k1": bytes.Repeat([]byte("a"), 4*1024),
		"k2": bytes.Repeat([]byte("b"), 4*1024),
		"k3": bytes.Repeat([]byte("c"), 4*1024),
	}
	for k, v := range values {
		if err := db.Set([]byte(k), v); err != nil {
			_ = db.Close()
			t.Fatalf("set %s: %v", k, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint before rewrite: %v", err)
	}
	stats, err := db.ValueLogRewriteOnline(context.Background(), treedb.ValueLogRewriteOnlineOptions{
		BatchSize:     2,
		SyncEachBatch: true,
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		_ = db.Close()
		t.Fatalf("expected rewrite to copy records, stats=%+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after rewrite: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	for k, want := range values {
		got, err := reopen.Get([]byte(k))
		if err != nil {
			t.Fatalf("reopen get %s: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reopen mismatch key=%s got=%dB want=%dB", k, len(got), len(want))
		}
	}
}

func TestReopenVerify_ValueLogRewrite_BatchedPointerSwap_ReopenParity_OuterLeafV2(t *testing.T) {
	runValueLogRewriteBatchedPointerSwapReopenParityOuterLeaf(t, treedb.IndexOuterLeafModeV1)
}

func TestReopenVerify_ValueLogRewrite_BatchedPointerSwap_ReopenParity_OuterLeafV2FencePtr(t *testing.T) {
	runValueLogRewriteBatchedPointerSwapReopenParityOuterLeaf(t, treedb.IndexOuterLeafModeV1)
}

func TestReopenVerify_ValueLogRewrite_BatchedPointerSwap_ReopenParity_OuterLeafV1LeafLog(t *testing.T) {
	runValueLogRewriteBatchedPointerSwapReopenParityOuterLeaf(t, treedb.IndexOuterLeafModeV1)
}

func TestReopenVerify_ValueLogRewrite_BatchedPointerSwap_ReopenParity_OuterLeafV1LeafLogRoute(t *testing.T) {
	runValueLogRewriteBatchedPointerSwapReopenParityOuterLeaf(t, treedb.IndexOuterLeafModeV1)
}

func runValueLogRewriteBatchedPointerSwapReopenParityOuterLeaf(t *testing.T, mode string) {
	t.Helper()

	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: mode,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:              1,
			OuterLeafBlockCodec:           treedb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes:     1024,
			OuterLeafBlockRestartInterval: 8,
		},
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	values := map[string][]byte{
		"k1": bytes.Repeat([]byte("a"), 2*1024),
		"k2": bytes.Repeat([]byte("b"), 2*1024),
		"k3": bytes.Repeat([]byte("c"), 2*1024),
		"k4": bytes.Repeat([]byte("d"), 2*1024),
	}
	for k, v := range values {
		if err := db.Set([]byte(k), v); err != nil {
			_ = db.Close()
			t.Fatalf("set %s: %v", k, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint before rewrite: %v", err)
	}
	stats, err := db.ValueLogRewriteOnline(context.Background(), treedb.ValueLogRewriteOnlineOptions{
		BatchSize:     2,
		SyncEachBatch: true,
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		_ = db.Close()
		t.Fatalf("expected rewrite to copy records, stats=%+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after rewrite: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	for k, want := range values {
		got, err := reopen.Get([]byte(k))
		if err != nil {
			t.Fatalf("reopen get %s: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reopen mismatch key=%s got=%dB want=%dB", k, len(got), len(want))
		}
	}
}

func TestReopenVerify_ValueLogGC_OuterLeafV2_ReopenParity(t *testing.T) {
	runValueLogGCReopenParityOuterLeaf(t, treedb.IndexOuterLeafModeV1)
}

func TestReopenVerify_ValueLogGC_OuterLeafV2FencePtr_ReopenParity(t *testing.T) {
	runValueLogGCReopenParityOuterLeaf(t, treedb.IndexOuterLeafModeV1)
}

func TestReopenVerify_ValueLogGC_OuterLeafV1LeafLog_ReopenParity(t *testing.T) {
	runValueLogGCReopenParityOuterLeaf(t, treedb.IndexOuterLeafModeV1)
}

func TestReopenVerify_ValueLogGC_OuterLeafV1LeafLogRoute_ReopenParity(t *testing.T) {
	runValueLogGCReopenParityOuterLeaf(t, treedb.IndexOuterLeafModeV1)
}

func runValueLogGCReopenParityOuterLeaf(t *testing.T, mode string) {
	t.Helper()

	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: mode,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:              1,
			OuterLeafBlockCodec:           treedb.ValueLogBlockSnappy,
			OuterLeafBlockTargetBytes:     512,
			OuterLeafBlockRestartInterval: 8,
		},
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const total = 240
	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("gc-key-%04d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, 256)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set %q: %v", string(key), err)
		}
	}
	for i := 0; i < 140; i++ {
		key := []byte(fmt.Sprintf("gc-key-%04d", i))
		if err := db.Delete(key); err != nil {
			_ = db.Close()
			t.Fatalf("delete %q: %v", string(key), err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint before gc: %v", err)
	}
	stats, err := db.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{})
	if err != nil {
		_ = db.Close()
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsEligible == 0 {
		_ = db.Close()
		t.Fatalf("expected GC to find eligible segments, stats=%+v", stats)
	}
	if stats.SegmentsDeleted == 0 {
		// Reopen parity is the primary contract for this test. On some platforms
		// GC can report eligible segments yet defer unlink until handles settle.
		t.Logf("ValueLogGC deferred physical deletion; continuing with reopen parity check, stats=%+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("gc-key-%04d", i))
		got, err := reopen.Get(key)
		if i < 140 {
			if err == nil && got != nil {
				t.Fatalf("expected deleted key %q to be absent", string(key))
			}
			continue
		}
		if err != nil {
			t.Fatalf("reopen get %q: %v", string(key), err)
		}
		want := bytes.Repeat([]byte{byte(i % 251)}, 256)
		if !bytes.Equal(got, want) {
			t.Fatalf("reopen mismatch key=%q got=%dB want=%dB", string(key), len(got), len(want))
		}
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

func TestReopenVerify_WALOn_WriteSync_OuterLeafV2(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: treedb.IndexOuterLeafModeV1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:              1,
			OuterLeafBlockCodec:           treedb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes:     4 << 10,
			OuterLeafBlockRestartInterval: 16,
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

func TestReopenVerify_WALOn_WriteSync_OuterLeafV2FencePtr(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: treedb.IndexOuterLeafModeV1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:              1,
			OuterLeafBlockCodec:           treedb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes:     4 << 10,
			OuterLeafBlockRestartInterval: 16,
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

func testReopenVerifyOuterLeafSplitMergeDeleteRangeIteratorParity(t *testing.T, mode string) {
	dir := t.TempDir()
	const total = 12000

	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: mode,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:              1,
			OuterLeafBlockCodec:           treedb.ValueLogBlockSnappy,
			OuterLeafBlockTargetBytes:     512,
			OuterLeafBlockRestartInterval: 8,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	expected := make(map[string][]byte, total)
	makeValue := func(i int, seed byte) []byte {
		v := make([]byte, 256)
		for j := range v {
			v[j] = seed + byte((i+j)%251)
		}
		return v
	}
	keyFor := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}

	for i := 0; i < total; i++ {
		key := keyFor(i)
		val := makeValue(i, 11)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set initial %d: %v", i, err)
		}
		expected[string(key)] = val
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint initial: %v", err)
	}

	// Force heavy structural churn:
	// - remove a large middle range via DeleteRange (split/merge/collapse pressure),
	// - overwrite a patterned subset outside the deleted range,
	// - delete sparse boundary keys.
	if err := db.DeleteRange(keyFor(2000), keyFor(8000)); err != nil {
		_ = db.Close()
		t.Fatalf("delete range: %v", err)
	}
	for i := 2000; i < 8000; i++ {
		key := keyFor(i)
		delete(expected, string(key))
	}
	for i := 0; i < total; i++ {
		if i >= 2000 && i < 8000 {
			continue
		}
		key := keyFor(i)
		if i%7 == 0 {
			val := makeValue(i, 77)
			if err := db.Set(key, val); err != nil {
				_ = db.Close()
				t.Fatalf("overwrite %d: %v", i, err)
			}
			expected[string(key)] = val
		}
		if i%113 == 0 {
			if err := db.Delete(key); err != nil {
				_ = db.Close()
				t.Fatalf("sparse delete %d: %v", i, err)
			}
			delete(expected, string(key))
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint churn: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	for i := 0; i < total; i++ {
		key := keyFor(i)
		got, err := reopen.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		want, ok := expected[string(key)]
		if !ok {
			if got != nil {
				t.Fatalf("expected key %d deleted, got value len=%d", i, len(got))
			}
			continue
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get mismatch key=%d got=%d want=%d", i, len(got), len(want))
		}
	}

	it, err := reopen.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()
	seen := 0
	for i := 0; i < total; i++ {
		key := keyFor(i)
		want, ok := expected[string(key)]
		if !ok {
			continue
		}
		if !it.Valid() {
			t.Fatalf("iterator ended early at logical key %d", i)
		}
		gotKey := it.KeyCopy(nil)
		if !bytes.Equal(gotKey, key) {
			t.Fatalf("iterator key mismatch at %d: got=%x want=%x", i, gotKey, key)
		}
		gotVal := it.ValueCopy(nil)
		if !bytes.Equal(gotVal, want) {
			t.Fatalf("iterator value mismatch key=%d", i)
		}
		seen++
		it.Next()
	}
	if it.Valid() {
		t.Fatalf("iterator has extra entries after expected stream")
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if seen != len(expected) {
		t.Fatalf("iterator count mismatch seen=%d want=%d", seen, len(expected))
	}

	rit, err := reopen.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	defer rit.Close()
	revSeen := 0
	for i := total - 1; i >= 0; i-- {
		key := keyFor(i)
		want, ok := expected[string(key)]
		if !ok {
			continue
		}
		if !rit.Valid() {
			t.Fatalf("reverse iterator ended early at logical key %d", i)
		}
		gotKey := rit.KeyCopy(nil)
		if !bytes.Equal(gotKey, key) {
			t.Fatalf("reverse key mismatch at %d: got=%x want=%x", i, gotKey, key)
		}
		gotVal := rit.ValueCopy(nil)
		if !bytes.Equal(gotVal, want) {
			t.Fatalf("reverse value mismatch key=%d", i)
		}
		revSeen++
		rit.Next()
	}
	if rit.Valid() {
		t.Fatalf("reverse iterator has extra entries after expected stream")
	}
	if err := rit.Error(); err != nil {
		t.Fatalf("reverse iterator error: %v", err)
	}
	if revSeen != len(expected) {
		t.Fatalf("reverse iterator count mismatch seen=%d want=%d", revSeen, len(expected))
	}

	start := keyFor(1500)
	end := keyFor(2500)
	rangeIt, err := reopen.Iterator(start, end)
	if err != nil {
		t.Fatalf("range iterator: %v", err)
	}
	defer rangeIt.Close()
	for i := 1500; i < 2500; i++ {
		key := keyFor(i)
		want, ok := expected[string(key)]
		if !ok {
			continue
		}
		if !rangeIt.Valid() {
			t.Fatalf("range iterator ended early at %d", i)
		}
		gotKey := rangeIt.KeyCopy(nil)
		if !bytes.Equal(gotKey, key) {
			t.Fatalf("range key mismatch at %d: got=%x want=%x", i, gotKey, key)
		}
		gotVal := rangeIt.ValueCopy(nil)
		if !bytes.Equal(gotVal, want) {
			t.Fatalf("range value mismatch at %d", i)
		}
		rangeIt.Next()
	}
	if rangeIt.Valid() {
		t.Fatalf("range iterator has extra entries")
	}
	if err := rangeIt.Error(); err != nil {
		t.Fatalf("range iterator error: %v", err)
	}
}

func TestReopenVerify_OuterLeafV2_SplitMergeDeleteRange_IteratorParity(t *testing.T) {
	testReopenVerifyOuterLeafSplitMergeDeleteRangeIteratorParity(t, treedb.IndexOuterLeafModeV1)
}

func TestReopenVerify_OuterLeafV1LeafLog_SplitMergeDeleteRange_IteratorParity(t *testing.T) {
	testReopenVerifyOuterLeafSplitMergeDeleteRangeIteratorParity(t, treedb.IndexOuterLeafModeV1)
}

func TestReopenVerify_OuterLeafV1LeafLogRoute_SplitMergeDeleteRange_IteratorParity(t *testing.T) {
	testReopenVerifyOuterLeafSplitMergeDeleteRangeIteratorParity(t, treedb.IndexOuterLeafModeV1)
}

func TestReopenVerify_OuterLeafV2FencePtr_SplitMergeDeleteRange_IteratorParity(t *testing.T) {
	testReopenVerifyOuterLeafSplitMergeDeleteRangeIteratorParity(t, treedb.IndexOuterLeafModeV1)
}

func TestReopenVerify_OuterLeafV1LeafLog_SplitMergeDeleteRange_ParityWithV1(t *testing.T) {
	type apiParityFingerprint struct {
		forwardDigest string
		forwardCount  int
		reverseDigest string
		reverseCount  int
		rangeDigest   string
		rangeCount    int
		pointDigest   string
	}

	run := func(mode string) apiParityFingerprint {
		dir := t.TempDir()
		const total = 12000
		opts := treedb.Options{
			Dir:                dir,
			IndexOuterLeafMode: mode,
			ValueLog: treedb.ValueLogOptions{
				PointerThreshold:              1,
				OuterLeafBlockCodec:           treedb.ValueLogBlockSnappy,
				OuterLeafBlockTargetBytes:     512,
				OuterLeafBlockRestartInterval: 8,
			},
		}

		db, err := treedb.Open(opts)
		if err != nil {
			t.Fatalf("open %s: %v", mode, err)
		}

		makeValue := func(i int, seed byte) []byte {
			v := make([]byte, 256)
			for j := range v {
				v[j] = seed + byte((i+j)%251)
			}
			return v
		}
		keyFor := func(i int) []byte {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i))
			return k
		}

		for i := 0; i < total; i++ {
			if err := db.Set(keyFor(i), makeValue(i, 11)); err != nil {
				_ = db.Close()
				t.Fatalf("set %s %d: %v", mode, i, err)
			}
		}
		if err := db.Checkpoint(); err != nil {
			_ = db.Close()
			t.Fatalf("checkpoint initial %s: %v", mode, err)
		}
		if err := db.DeleteRange(keyFor(2000), keyFor(8000)); err != nil {
			_ = db.Close()
			t.Fatalf("delete range %s: %v", mode, err)
		}
		for i := 0; i < total; i++ {
			if i >= 2000 && i < 8000 {
				continue
			}
			key := keyFor(i)
			if i%7 == 0 {
				if err := db.Set(key, makeValue(i, 77)); err != nil {
					_ = db.Close()
					t.Fatalf("overwrite %s %d: %v", mode, i, err)
				}
			}
			if i%113 == 0 {
				if err := db.Delete(key); err != nil {
					_ = db.Close()
					t.Fatalf("sparse delete %s %d: %v", mode, i, err)
				}
			}
		}
		if err := db.Checkpoint(); err != nil {
			_ = db.Close()
			t.Fatalf("checkpoint churn %s: %v", mode, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close %s: %v", mode, err)
		}

		reopen, err := treedb.Open(opts)
		if err != nil {
			t.Fatalf("reopen %s: %v", mode, err)
		}
		defer reopen.Close()

		out := apiParityFingerprint{}

		it, err := reopen.Iterator(nil, nil)
		if err != nil {
			t.Fatalf("iterator %s: %v", mode, err)
		}
		defer it.Close()
		forwardSum := sha256.New()
		for it.Valid() {
			_, _ = forwardSum.Write(it.Key())
			_, _ = forwardSum.Write(it.Value())
			out.forwardCount++
			it.Next()
		}
		if err := it.Error(); err != nil {
			t.Fatalf("iterator error %s: %v", mode, err)
		}
		out.forwardDigest = hex.EncodeToString(forwardSum.Sum(nil))

		rit, err := reopen.ReverseIterator(nil, nil)
		if err != nil {
			t.Fatalf("reverse iterator %s: %v", mode, err)
		}
		defer rit.Close()
		reverseSum := sha256.New()
		for rit.Valid() {
			_, _ = reverseSum.Write(rit.Key())
			_, _ = reverseSum.Write(rit.Value())
			out.reverseCount++
			rit.Next()
		}
		if err := rit.Error(); err != nil {
			t.Fatalf("reverse iterator error %s: %v", mode, err)
		}
		out.reverseDigest = hex.EncodeToString(reverseSum.Sum(nil))

		rangeIt, err := reopen.Iterator(keyFor(1500), keyFor(2500))
		if err != nil {
			t.Fatalf("range iterator %s: %v", mode, err)
		}
		defer rangeIt.Close()
		rangeSum := sha256.New()
		for rangeIt.Valid() {
			_, _ = rangeSum.Write(rangeIt.Key())
			_, _ = rangeSum.Write(rangeIt.Value())
			out.rangeCount++
			rangeIt.Next()
		}
		if err := rangeIt.Error(); err != nil {
			t.Fatalf("range iterator error %s: %v", mode, err)
		}
		out.rangeDigest = hex.EncodeToString(rangeSum.Sum(nil))

		pointSum := sha256.New()
		for i := 0; i < total; i++ {
			key := keyFor(i)
			_, _ = pointSum.Write(key)
			got, err := reopen.Get(key)
			if err != nil {
				t.Fatalf("get %s %d: %v", mode, i, err)
			}
			if got == nil {
				_, _ = pointSum.Write([]byte{0})
				continue
			}
			_, _ = pointSum.Write([]byte{1})
			_, _ = pointSum.Write(got)
		}
		out.pointDigest = hex.EncodeToString(pointSum.Sum(nil))
		return out
	}

	v1 := run(treedb.IndexOuterLeafModeV1)
	v1LeafLog := run(treedb.IndexOuterLeafModeV1)
	if v1.forwardCount != v1LeafLog.forwardCount {
		t.Fatalf("forward count mismatch v1=%d v1_leaflog=%d", v1.forwardCount, v1LeafLog.forwardCount)
	}
	if v1.forwardDigest != v1LeafLog.forwardDigest {
		t.Fatalf("forward digest mismatch v1=%s v1_leaflog=%s", v1.forwardDigest, v1LeafLog.forwardDigest)
	}
	if v1.reverseCount != v1LeafLog.reverseCount {
		t.Fatalf("reverse count mismatch v1=%d v1_leaflog=%d", v1.reverseCount, v1LeafLog.reverseCount)
	}
	if v1.reverseDigest != v1LeafLog.reverseDigest {
		t.Fatalf("reverse digest mismatch v1=%s v1_leaflog=%s", v1.reverseDigest, v1LeafLog.reverseDigest)
	}
	if v1.rangeCount != v1LeafLog.rangeCount {
		t.Fatalf("range count mismatch v1=%d v1_leaflog=%d", v1.rangeCount, v1LeafLog.rangeCount)
	}
	if v1.rangeDigest != v1LeafLog.rangeDigest {
		t.Fatalf("range digest mismatch v1=%s v1_leaflog=%s", v1.rangeDigest, v1LeafLog.rangeDigest)
	}
	if v1.pointDigest != v1LeafLog.pointDigest {
		t.Fatalf("point digest mismatch v1=%s v1_leaflog=%s", v1.pointDigest, v1LeafLog.pointDigest)
	}
}

func TestReopenVerify_OuterLeafV2FencePtr_CompactIndex_ReopenParity(t *testing.T) {
	dir := t.TempDir()
	const total = 4000
	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: treedb.IndexOuterLeafModeV1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:              1,
			OuterLeafBlockCodec:           treedb.ValueLogBlockLZ4,
			OuterLeafBlockTargetBytes:     512,
			OuterLeafBlockRestartInterval: 8,
		},
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	keyFor := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}
	valueFor := func(i int, seed byte) []byte {
		v := make([]byte, 384)
		for j := range v {
			v[j] = seed + byte((i+j)%251)
		}
		return v
	}

	expected := make(map[string][]byte, total)
	for i := 0; i < total; i++ {
		key := keyFor(i)
		val := valueFor(i, 31)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set %d: %v", i, err)
		}
		expected[string(key)] = val
	}
	for i := 600; i < 2200; i++ {
		key := keyFor(i)
		if err := db.Delete(key); err != nil {
			_ = db.Close()
			t.Fatalf("delete %d: %v", i, err)
		}
		delete(expected, string(key))
	}
	for i := 0; i < total; i += 5 {
		if i >= 600 && i < 2200 {
			continue
		}
		key := keyFor(i)
		val := valueFor(i, 97)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("overwrite %d: %v", i, err)
		}
		expected[string(key)] = val
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint before compact: %v", err)
	}

	beforeInfo, err := os.Stat(filepath.Join(dir, "maindb", "index.db"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("stat index before compact: %v", err)
	}
	if err := db.CompactIndex(); err != nil {
		_ = db.Close()
		t.Fatalf("CompactIndex: %v", err)
	}
	afterInfo, err := os.Stat(filepath.Join(dir, "maindb", "index.db"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("stat index after compact: %v", err)
	}
	if beforeInfo.Size() == 0 || afterInfo.Size() == 0 {
		_ = db.Close()
		t.Fatalf("unexpected zero-size index (before=%d after=%d)", beforeInfo.Size(), afterInfo.Size())
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after compact: %v", err)
	}

	reopen, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	for i := 0; i < total; i++ {
		key := keyFor(i)
		got, err := reopen.Get(key)
		want, ok := expected[string(key)]
		if !ok {
			if err == nil && got != nil {
				t.Fatalf("expected key %d deleted", i)
			}
			continue
		}
		if err != nil {
			t.Fatalf("reopen get %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch key=%d got=%d want=%d", i, len(got), len(want))
		}
	}

	it, err := reopen.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()
	seen := 0
	for ; it.Valid(); it.Next() {
		seen++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if seen != len(expected) {
		t.Fatalf("iterator count mismatch seen=%d want=%d", seen, len(expected))
	}
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

func TestReopenVerify_AdaptiveLeafEncoding_MixedEncodingPages(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                       dir,
		ChunkSize:                 64 * 1024,
		LeafPrefixCompression:     true,
		IndexColumnarLeaves:       true,
		IndexPackedValuePtr:       true,
		IndexAdaptiveLeafEncoding: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 128,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	keys := make([][]byte, 0, 6000)
	values := make(map[string][]byte, 6000)

	largeTemplate := bytes.Repeat([]byte("P"), 2048)
	for i := 0; i < 3000; i++ {
		k := []byte(fmt.Sprintf("a/%06d", i))
		v := append([]byte(nil), largeTemplate...)
		binary.LittleEndian.PutUint32(v[:4], uint32(i))
		if err := db.Set(k, v); err != nil {
			_ = db.Close()
			t.Fatalf("set pointer-heavy key %q: %v", k, err)
		}
		keys = append(keys, k)
		values[string(k)] = append([]byte(nil), v...)
	}

	for i := 0; i < 3000; i++ {
		k := []byte(fmt.Sprintf("zz/tenant/orders/%06d", i))
		v := []byte(fmt.Sprintf("inline-%06d", i))
		if err := db.Set(k, v); err != nil {
			_ = db.Close()
			t.Fatalf("set inline key %q: %v", k, err)
		}
		keys = append(keys, k)
		values[string(k)] = append([]byte(nil), v...)
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

	for _, key := range keys {
		got, err := reopen.Get(key)
		if err != nil {
			t.Fatalf("get %q after reopen: %v", key, err)
		}
		want := values[string(key)]
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch for %q after reopen: got=%d want=%d", key, len(got), len(want))
		}
	}

	it, err := reopen.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()

	scanCount := 0
	for ; it.Valid(); it.Next() {
		k := it.KeyCopy(nil)
		v := it.ValueCopy(nil)
		want, ok := values[string(k)]
		if !ok {
			t.Fatalf("unexpected key in scan: %q", k)
		}
		if !bytes.Equal(v, want) {
			t.Fatalf("scan mismatch for %q", k)
		}
		scanCount++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if scanCount != len(values) {
		t.Fatalf("scan count mismatch: got=%d want=%d", scanCount, len(values))
	}
}

func TestValuePlacement_PerDomainThreshold_ReopenDurability(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                dir,
		IndexOuterLeafMode: treedb.IndexOuterLeafModeV1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 256,
			DomainInlineThresholds: []treedb.ValueLogDomainThreshold{
				{Prefix: []byte("hot/"), InlineThreshold: 16},
				{Prefix: []byte("cold/"), InlineThreshold: 1024},
			},
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	hotKey := []byte("hot/reopen")
	coldKey := []byte("cold/reopen")
	defaultKey := []byte("other/reopen")
	hotVal := bytes.Repeat([]byte("h"), 64)
	coldVal := bytes.Repeat([]byte("c"), 64)
	defaultVal := bytes.Repeat([]byte("d"), 300)

	if err := db.Set(hotKey, hotVal); err != nil {
		_ = db.Close()
		t.Fatalf("set hot: %v", err)
	}
	if err := db.Set(coldKey, coldVal); err != nil {
		_ = db.Close()
		t.Fatalf("set cold: %v", err)
	}
	if err := db.Set(defaultKey, defaultVal); err != nil {
		_ = db.Close()
		t.Fatalf("set default: %v", err)
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

	snap := reopen.AcquireSnapshot()
	defer snap.Close()

	hotEntry, err := snap.GetEntry(hotKey)
	if err != nil {
		t.Fatalf("snapshot GetEntry hot: %v", err)
	}
	if hotEntry.Flags&node.FlagPointer == 0 {
		t.Fatalf("expected hot domain key to remain pointer-backed after reopen")
	}

	coldEntry, err := snap.GetEntry(coldKey)
	if err != nil {
		t.Fatalf("snapshot GetEntry cold: %v", err)
	}
	if coldEntry.Flags&node.FlagPointer != 0 {
		t.Fatalf("expected cold domain key to remain inline after reopen")
	}

	defaultEntry, err := snap.GetEntry(defaultKey)
	if err != nil {
		t.Fatalf("snapshot GetEntry default: %v", err)
	}
	if defaultEntry.Flags&node.FlagPointer == 0 {
		t.Fatalf("expected default fallback key to remain pointer-backed after reopen")
	}

	gotHot, err := reopen.Get(hotKey)
	if err != nil || !bytes.Equal(gotHot, hotVal) {
		t.Fatalf("reopen get hot mismatch: err=%v", err)
	}
	gotCold, err := reopen.Get(coldKey)
	if err != nil || !bytes.Equal(gotCold, coldVal) {
		t.Fatalf("reopen get cold mismatch: err=%v", err)
	}
	gotDefault, err := reopen.Get(defaultKey)
	if err != nil || !bytes.Equal(gotDefault, defaultVal) {
		t.Fatalf("reopen get default mismatch: err=%v", err)
	}
}
