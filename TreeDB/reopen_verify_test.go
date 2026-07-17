package treedb_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
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

func writeDatasetOneBatch(t *testing.T, db *treedb.DB, keys [][]byte, values map[string][]byte) {
	t.Helper()
	batch := db.NewBatch()
	defer batch.Close()
	for _, key := range keys {
		if err := batch.Set(key, values[string(key)]); err != nil {
			t.Fatalf("batch set: %v", err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("batch write: %v", err)
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

func leafVLogFrameStats(t *testing.T, dir string) (frames int, maxK int) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "maindb", "leaf_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob leaf_vlog: %v", err)
	}
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open leaf_vlog segment %q: %v", path, err)
		}
		func() {
			defer f.Close()
			var header [valuelog.HeaderSize]byte
			for {
				n, err := io.ReadFull(f, header[:])
				if err != nil {
					if err == io.EOF || (err == io.ErrUnexpectedEOF && n == 0) {
						return
					}
					t.Fatalf("read leaf_vlog header %q: %v", path, err)
				}
				if header[4] != valuelog.Version {
					t.Fatalf("leaf_vlog segment %q has version %d want %d", path, header[4], valuelog.Version)
				}
				bodyLen := int64(binary.LittleEndian.Uint32(header[16:20]))
				if header[5]&1 == 0 {
					if _, err := f.Seek(bodyLen, io.SeekCurrent); err != nil {
						t.Fatalf("seek leaf_vlog body %q: %v", path, err)
					}
					continue
				}
				if bodyLen < valuelog.FrameHeaderSize {
					t.Fatalf("leaf_vlog grouped body too short in %q: %d", path, bodyLen)
				}
				var frameHeader [valuelog.FrameHeaderSize]byte
				if _, err := io.ReadFull(f, frameHeader[:]); err != nil {
					t.Fatalf("read leaf_vlog frame header %q: %v", path, err)
				}
				if frameHeader[0] != valuelog.FrameVersion {
					t.Fatalf("leaf_vlog frame version %d want %d", frameHeader[0], valuelog.FrameVersion)
				}
				k := int(frameHeader[2])
				frames++
				if k > maxK {
					maxK = k
				}
				if _, err := f.Seek(bodyLen-valuelog.FrameHeaderSize, io.SeekCurrent); err != nil {
					t.Fatalf("seek leaf_vlog frame payload %q: %v", path, err)
				}
			}
		}()
	}
	return frames, maxK
}

func corruptFirstLeafVLogCRC(t *testing.T, dir string) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "maindb", "leaf_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob leaf_vlog: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no leaf_vlog segments found")
	}
	f, err := os.OpenFile(paths[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open leaf_vlog segment for corruption: %v", err)
	}
	defer f.Close()
	var crcBytes [4]byte
	if _, err := f.ReadAt(crcBytes[:], 0); err != nil {
		t.Fatalf("read leaf_vlog crc: %v", err)
	}
	crcBytes[0] ^= 0xff
	if _, err := f.WriteAt(crcBytes[:], 0); err != nil {
		t.Fatalf("write corrupted leaf_vlog crc: %v", err)
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

func TestReopenVerify_WALOn_Checkpoint_LeafPagesInValueLog(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(2000)

	opts := treedb.Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	writeDatasetOneBatch(t, db, keys, values)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	frames, maxK := leafVLogFrameStats(t, dir)
	if frames == 0 || maxK <= 1 {
		t.Fatalf("leaf_vlog frame grouping frames=%d maxK=%d, want grouped live frames", frames, maxK)
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

func TestReopenVerify_ParallelFlushLeafLogOutput(t *testing.T) {
	dir := t.TempDir()
	keys, values, hash := buildVerifyDataset(6000)

	opts := treedb.Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		FlushThreshold:             64 * 1024,
		FlushAdmissionPolicy:       treedb.FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:      4,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
			Compression:      treedb.ValueLogCompressionBlock,
			BlockCodec:       treedb.ValueLogBlockLZ4,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	writeDatasetOneBatch(t, db, keys, values)
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	stats := db.Stats()
	if got := stats["treedb.flush_apply.read_only_prepare.calls_total"]; got == "" || got == "0" {
		_ = db.Close()
		t.Fatalf("read-only prepare calls stat=%q want >0", got)
	}
	if got := stats["treedb.flush_apply.prepared_output.leaf_log_pages_installed_total"]; got == "" || got == "0" {
		_ = db.Close()
		t.Fatalf("installed leaf-log output stat=%q want >0", got)
	}
	if _, err := db.LeafGenerationGC(context.Background(), treedb.LeafGenerationGCOptions{DryRun: true}); err != nil {
		_ = db.Close()
		t.Fatalf("LeafGenerationGC dry-run: %v", err)
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

func TestReopenVerify_LeafPageLogGroupedFrameCRCIntegrityModes(t *testing.T) {
	dir := t.TempDir()
	keys, values, _ := buildVerifyDataset(2000)

	opts := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ReadIntegrity = treedb.IntegrityVerify
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	writeDatasetOneBatch(t, db, keys, values)
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	frames, maxK := leafVLogFrameStats(t, dir)
	if frames == 0 || maxK <= 1 {
		_ = db.Close()
		t.Fatalf("leaf_vlog frame grouping frames=%d maxK=%d, want grouped live frames", frames, maxK)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	verifyDB, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen with checksum verification: %v", err)
	}
	corruptFirstLeafVLogCRC(t, dir)
	_, getErr := verifyDB.Get(keys[0])
	if closeErr := verifyDB.Close(); closeErr != nil {
		t.Fatalf("close verify reopen: %v", closeErr)
	}
	corruptFirstLeafVLogCRC(t, dir) // restore the CRC before the skip-checksum reopen.
	if getErr == nil {
		t.Fatalf("Get with checksum verification succeeded after leaf_vlog CRC corruption")
	}

	skipOpts := opts
	skipOpts.ValueLog.ReadIntegrity = treedb.IntegritySkipChecksums
	skipDB, err := treedb.Open(skipOpts)
	if err != nil {
		t.Fatalf("reopen with skipped checksums: %v", err)
	}
	defer skipDB.Close()
	corruptFirstLeafVLogCRC(t, dir)
	got, err := skipDB.Get(keys[0])
	if err != nil {
		t.Fatalf("Get with skipped checksums: %v", err)
	}
	if want := values[string(keys[0])]; !bytes.Equal(got, want) {
		t.Fatalf("Get with skipped checksums mismatch: got %d bytes want %d", len(got), len(want))
	}
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

func TestReopenVerify_ValueLogGC_LeafPagesInValueLog_ReopenParity(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1 << 20,
		},
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const total = 240
	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("leafgc-key-%04d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, 32)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set %q: %v", string(key), err)
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
	if stats.SegmentsDeleted != 0 {
		t.Fatalf("value-log GC deleted segments in leaf-only split-log workload, stats=%+v", stats)
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
		key := []byte(fmt.Sprintf("leafgc-key-%04d", i))
		got, err := reopen.Get(key)
		if err != nil {
			t.Fatalf("reopen get %q: %v", string(key), err)
		}
		want := bytes.Repeat([]byte{byte(i % 251)}, 32)
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

func TestReopenVerify_OuterLeavesExplicitInternalBaseDeltaDisabled_Churn(t *testing.T) {
	dir := t.TempDir()
	const total = 4096

	type formatConfigView struct {
		IndexOuterLeavesInValueLog bool `json:"index_outer_leaves_in_vlog"`
		IndexInternalBaseDelta     bool `json:"index_internal_base_delta"`
	}
	readFormat := func() formatConfigView {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(dir, "maindb", "format.json"))
		if err != nil {
			t.Fatalf("read format config: %v", err)
		}
		var cfg formatConfigView
		if err := json.Unmarshal(data, &cfg); err != nil {
			t.Fatalf("decode format config: %v", err)
		}
		return cfg
	}
	keyFor := func(i int) []byte {
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], uint64(i))
		return append([]byte(nil), k[:]...)
	}
	valueFor := func(i int, seed byte) []byte {
		v := make([]byte, 192)
		for j := range v {
			v[j] = seed + byte((i+j)%251)
		}
		return v
	}
	dirSize := func(path string) (int64, error) {
		var total int64
		err := filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
			return nil
		})
		return total, err
	}

	opts := treedb.Options{
		Dir:                        dir,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		IndexInternalBaseDelta:     true, // Must resolve false with outer leaf-log refs.
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	expected := make(map[string][]byte, total)
	for i := 0; i < total; i++ {
		key := keyFor(i)
		val := valueFor(i, 17)
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

	if cfg := readFormat(); !cfg.IndexOuterLeavesInValueLog || cfg.IndexInternalBaseDelta {
		_ = db.Close()
		t.Fatalf("unexpected persisted index format after open: %+v", cfg)
	}

	if err := db.DeleteRange(keyFor(700), keyFor(1700)); err != nil {
		_ = db.Close()
		t.Fatalf("delete range: %v", err)
	}
	for i := 700; i < 1700; i++ {
		delete(expected, string(keyFor(i)))
	}
	for i := 0; i < total; i++ {
		if i >= 700 && i < 1700 {
			continue
		}
		key := keyFor(i)
		if i%11 == 0 {
			val := valueFor(i, 83)
			if err := db.Set(key, val); err != nil {
				_ = db.Close()
				t.Fatalf("overwrite %d: %v", i, err)
			}
			expected[string(key)] = val
		}
		if i%257 == 0 {
			if err := db.Delete(key); err != nil {
				_ = db.Close()
				t.Fatalf("delete %d: %v", i, err)
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

	if cfg := readFormat(); !cfg.IndexOuterLeavesInValueLog || cfg.IndexInternalBaseDelta {
		t.Fatalf("unexpected persisted index format after close: %+v", cfg)
	}
	if size, err := dirSize(filepath.Join(dir, "maindb", "leaf_vlog")); err != nil || size == 0 {
		t.Fatalf("expected outer leaf log bytes, size=%d err=%v", size, err)
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
			if err != nil && err != treedb.ErrKeyNotFound {
				t.Fatalf("get deleted %d: %v", i, err)
			}
			if got != nil {
				t.Fatalf("expected key %d deleted, got %d bytes", i, len(got))
			}
			continue
		}
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get mismatch key=%d got=%d bytes want=%d bytes", i, len(got), len(want))
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
			t.Fatalf("iterator ended early at %d", i)
		}
		if gotKey := it.KeyCopy(nil); !bytes.Equal(gotKey, key) {
			t.Fatalf("iterator key mismatch at %d: got=%x want=%x", i, gotKey, key)
		}
		if gotVal := it.ValueCopy(nil); !bytes.Equal(gotVal, want) {
			t.Fatalf("iterator value mismatch at %d", i)
		}
		seen++
		it.Next()
	}
	if it.Valid() {
		t.Fatalf("iterator has extra entries")
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if seen != len(expected) {
		t.Fatalf("iterator count mismatch seen=%d want=%d", seen, len(expected))
	}
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

func TestReopenVerify_SplitMergeDeleteRange_IteratorParity(t *testing.T) {
	dir := t.TempDir()
	const total = 12000

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

func TestReopenVerify_CompactIndex_ReopenParity(t *testing.T) {
	dir := t.TempDir()
	const total = 4000
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.ValueLog.PointerThreshold = 1
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
		{name: "block_zstd", compression: treedb.ValueLogCompressionBlock, codec: treedb.ValueLogBlockZSTD},
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
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff

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
		Dir: dir,
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
