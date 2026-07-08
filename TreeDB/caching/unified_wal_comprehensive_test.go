package caching

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// TestUnifiedWAL_ValueLogFlow verifies that large values land in the value log
// and remain readable after a checkpoint.
func TestUnifiedWAL_ValueLogFlow(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	opts := Options{
		FlushThreshold:           4 * 1024 * 1024,
		ValueLogPointerThreshold: 100,
		DisableWAL:               false,
		AllowUnsafe:              true,
	}

	cached, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()

	valSize := 1000
	key := []byte("large-key")
	val := bytes.Repeat([]byte{0xAA}, valSize)
	if err := cached.Set(key, val); err != nil {
		t.Fatal(err)
	}
	if err := cached.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	commitSize, valueSize := getLogSizes(t, dir)
	t.Logf("CommitLog Size: %d, ValueLog Size: %d", commitSize, valueSize)

	got, err := backend.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, val) {
		t.Fatal("data mismatch after checkpoint")
	}
}

// TestUnifiedWAL_CrashRecoveryMissingCommit ensures payloads without commit intent are ignored.
func TestUnifiedWAL_CrashRecoveryMissingCommit(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	valueLogDir := filepath.Join(dir, "value_vlog")
	for _, path := range []string{walDir, valueLogDir} {
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatalf("mkdir log dir %s: %v", path, err)
		}
	}

	valuePath := filepath.Join(valueLogDir, "value-l0-000001.log")
	writer, err := valuelog.NewWriter(valuePath, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("valuelog.NewWriter: %v", err)
	}
	key := []byte("k1")
	val := bytes.Repeat([]byte{0xAB}, 512)
	if _, err := writer.Append(0, nil, 1, val); err != nil {
		_ = writer.Close()
		t.Fatalf("valuelog.Append: %v", err)
	}
	if err := writer.Sync(); err != nil {
		_ = writer.Close()
		t.Fatalf("valuelog.Sync: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("valuelog.Close: %v", err)
	}

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	got, err := backend.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil {
		t.Fatalf("expected missing commit to skip payload, got %q", string(got))
	}
}

// TestUnifiedWAL_CrashRecoveryMissingPayloadSkipped ensures missing RID payloads
// are skipped during replay instead of surfacing as hard open failures.
func TestUnifiedWAL_CrashRecoveryMissingPayloadSkipped(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	commitPath := filepath.Join(walDir, "commit-l0-000001.log")
	writer, err := commitlog.NewWriter(commitPath)
	if err != nil {
		t.Fatalf("commitlog.NewWriter: %v", err)
	}
	rec := commitlog.Record{Op: commitlog.OpSetRID, Key: []byte("k2"), RID: 1, Seq: 1}
	if err := writer.AppendBatch([]commitlog.Record{rec}); err != nil {
		_ = writer.Close()
		t.Fatalf("commitlog.AppendBatch: %v", err)
	}
	if err := writer.Sync(); err != nil {
		_ = writer.Close()
		t.Fatalf("commitlog.Sync: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("commitlog.Close: %v", err)
	}

	opened, err := db.Open(db.Options{Dir: dir, AllowLegacyCachedRedoJournalReplay: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer opened.Close()

	got, err := opened.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("get k2: %v", err)
	}
	if got != nil {
		t.Fatalf("expected missing payload commit to be skipped, got %q", string(got))
	}
}

func TestUnifiedWAL_CrashRecoveryCompactZeroInlineBatch(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	commitPath := filepath.Join(walDir, "commit-l0-000001.log")
	writer, err := commitlog.NewWriter(commitPath)
	if err != nil {
		t.Fatalf("commitlog.NewWriter: %v", err)
	}
	want := make([]byte, 64)
	records := []commitlog.Record{
		{Op: commitlog.OpSetInline, Key: []byte("zero-a"), Value: want, Seq: 1},
		{Op: commitlog.OpSetInline, Key: []byte("zero-b"), Value: want, Seq: 1},
	}
	if err := writer.AppendBatch(records); err != nil {
		_ = writer.Close()
		t.Fatalf("commitlog.AppendBatch: %v", err)
	}
	if err := writer.Sync(); err != nil {
		_ = writer.Close()
		t.Fatalf("commitlog.Sync: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("commitlog.Close: %v", err)
	}

	opened, err := db.Open(db.Options{Dir: dir, AllowLegacyCachedRedoJournalReplay: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer opened.Close()

	for _, key := range [][]byte{[]byte("zero-a"), []byte("zero-b")} {
		got, err := opened.Get(key)
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch for %q: got %x want %x", key, got, want)
		}
	}
}

func runUnifiedWALRevisionCrashWriter(t *testing.T, dir string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperUnifiedWALRevisionCrashWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		"TREEDB_CACHING_WAL_REVISION_CRASH_HELPER=1",
		"TREEDB_CACHING_WAL_REVISION_CRASH_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("crash writer helper failed: %v\n%s", err, string(out))
	}
}

func TestHelperUnifiedWALRevisionCrashWriter(t *testing.T) {
	if os.Getenv("TREEDB_CACHING_WAL_REVISION_CRASH_HELPER") != "1" {
		t.Skip("helper")
	}

	dir := os.Getenv("TREEDB_CACHING_WAL_REVISION_CRASH_DIR")
	if dir == "" {
		t.Fatalf("missing TREEDB_CACHING_WAL_REVISION_CRASH_DIR")
	}

	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	cache, err := Open(dir, backend, Options{
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "skiplist",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}

	b := cache.NewBatch()
	if err := b.SetWithRevision([]byte("explicit"), []byte("value"), page.EntryRevision(41)); err != nil {
		t.Fatalf("SetWithRevision explicit: %v", err)
	}
	if err := b.Set([]byte("seq-backed"), []byte("value")); err != nil {
		t.Fatalf("Set seq-backed: %v", err)
	}
	if err := b.DeleteWithRevision([]byte("deleted"), page.EntryRevision(43)); err != nil {
		t.Fatalf("DeleteWithRevision deleted: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	_ = b.Close()

	// Simulate a crash: do not close the cache or backend, because Close drains
	// memtables and can avoid replaying the cached redo WAL.
	os.Exit(0)
}

func TestUnifiedWAL_CrashRecoveryPreservesEntryRevision(t *testing.T) {
	dir := t.TempDir()
	runUnifiedWALRevisionCrashWriter(t, dir)

	reader, err := commitlog.NewReader(filepath.Join(dir, "wal", "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("commitlog.NewReader: %v", err)
	}
	records, err := reader.ReadBatch()
	if err != nil {
		_ = reader.Close()
		t.Fatalf("ReadBatch: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("reader Close: %v", err)
	}
	byKey := make(map[string]commitlog.Record, len(records))
	for _, rec := range records {
		byKey[string(rec.Key)] = rec
	}
	seqRecord, ok := byKey["seq-backed"]
	if !ok {
		t.Fatalf("missing seq-backed record: %+v", records)
	}
	if seqRecord.Seq == 0 || seqRecord.Revision != 0 {
		t.Fatalf("seq-backed record seq=%d revision=%d, want nonzero seq and zero record revision", seqRecord.Seq, seqRecord.Revision)
	}
	explicitRecord, ok := byKey["explicit"]
	if !ok {
		t.Fatalf("missing explicit record: %+v", records)
	}
	if explicitRecord.Seq != seqRecord.Seq || explicitRecord.Revision != 41 {
		t.Fatalf("explicit record seq=%d revision=%d, want seq %d revision 41", explicitRecord.Seq, explicitRecord.Revision, seqRecord.Seq)
	}
	deletedRecord, ok := byKey["deleted"]
	if !ok {
		t.Fatalf("missing deleted record: %+v", records)
	}
	if deletedRecord.Op != commitlog.OpDelete || deletedRecord.Seq != seqRecord.Seq || deletedRecord.Revision != 43 {
		t.Fatalf("deleted record op=%d seq=%d revision=%d, want delete seq %d revision 43", deletedRecord.Op, deletedRecord.Seq, deletedRecord.Revision, seqRecord.Seq)
	}

	opened, err := db.Open(db.Options{Dir: dir, AllowLegacyCachedRedoJournalReplay: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer opened.Close()

	explicitValue, explicitRevision, err := opened.GetVersioned([]byte("explicit"))
	if err != nil {
		t.Fatalf("GetVersioned explicit: %v", err)
	}
	if !bytes.Equal(explicitValue, []byte("value")) || explicitRevision != page.EntryRevision(41) {
		t.Fatalf("GetVersioned explicit=(%q,%d), want (value,41)", explicitValue, explicitRevision)
	}
	seqValue, seqRevision, err := opened.GetVersioned([]byte("seq-backed"))
	if err != nil {
		t.Fatalf("GetVersioned seq-backed: %v", err)
	}
	if !bytes.Equal(seqValue, []byte("value")) || seqRevision != page.EntryRevision(seqRecord.Seq) {
		t.Fatalf("GetVersioned seq-backed=(%q,%d), want (value,%d)", seqValue, seqRevision, seqRecord.Seq)
	}
	deletedValue, deletedRevision, err := opened.GetVersioned([]byte("deleted"))
	if err != nil {
		t.Fatalf("GetVersioned deleted: %v", err)
	}
	if deletedValue != nil {
		t.Fatalf("GetVersioned deleted=(%q,%d), want missing after replayed delete", deletedValue, deletedRevision)
	}
	if got := opened.State().MaxEntryRevision; got < page.EntryRevision(43) {
		t.Fatalf("MaxEntryRevision=%d, want >= 43", got)
	}
}

func TestUnifiedWAL_LargeBatch(t *testing.T) {
	dir := t.TempDir()
	backend, _ := db.Open(db.Options{Dir: dir})
	defer backend.Close()

	opts := Options{
		FlushThreshold:           4 * 1024 * 1024,
		ValueLogPointerThreshold: 100,
		// Small commitlog segment to force multiple files
		WALMaxSegmentBytes: 1024 * 1024,
		AllowUnsafe:        true,
	}
	cached, _ := Open(dir, backend, opts)
	defer cached.Close()

	// Write 5MB of data (larger than FlushThreshold and CommitLog segment)
	blob := bytes.Repeat([]byte{0xCC}, 10000) // 10KB
	count := 500
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("k-%d", i))
		if err := cached.Set(key, blob); err != nil {
			t.Fatal(err)
		}
	}

	// Flush
	if err := cached.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Verify Backend
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("k-%d", i))
		val, err := backend.Get(key)
		if err != nil || len(val) != 10000 {
			t.Fatalf("Backend Read Failed at %d: %v", i, err)
		}
	}
}

func TestUnifiedWAL_InterleavedWrites(t *testing.T) {
	dir := t.TempDir()
	backend, _ := db.Open(db.Options{Dir: dir})
	defer backend.Close()

	opts := Options{
		FlushThreshold:           4 * 1024 * 1024,
		ValueLogPointerThreshold: 100, // Small threshold
		DisableWAL:               false,
		AllowUnsafe:              true,
	}
	cached, _ := Open(dir, backend, opts)
	defer cached.Close()

	// Mix small and large
	for i := 0; i < 100; i++ {
		// Small (50 bytes) -> Inline / CommitLog
		if err := cached.Set([]byte(fmt.Sprintf("small-%d", i)), bytes.Repeat([]byte{1}, 50)); err != nil {
			t.Fatal(err)
		}
		// Large (500 bytes) -> ValueLog
		if err := cached.Set([]byte(fmt.Sprintf("large-%d", i)), bytes.Repeat([]byte{2}, 500)); err != nil {
			t.Fatal(err)
		}
	}

	if err := cached.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Verify
	for i := 0; i < 100; i++ {
		sVal, err := backend.Get([]byte(fmt.Sprintf("small-%d", i)))
		if err != nil || len(sVal) != 50 {
			t.Errorf("Small read failed at %d", i)
		}
		lVal, err := backend.Get([]byte(fmt.Sprintf("large-%d", i)))
		if err != nil || len(lVal) != 500 {
			t.Errorf("Large read failed at %d", i)
		}
	}
}

func getLogSizes(t *testing.T, dir string) (commitSize, valueSize int64) {
	walDir := filepath.Join(dir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		t.Logf("ReadDir(%q) error: %v", walDir, err)
		return 0, 0
	}
	for _, e := range entries {
		info, _ := e.Info()
		if len(e.Name()) > 6 && e.Name()[:6] == "value-" {
			valueSize += info.Size()
		} else if len(e.Name()) > 7 && e.Name()[:7] == "commit-" {
			commitSize += info.Size()
		}
	}
	return
}
