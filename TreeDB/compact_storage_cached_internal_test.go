package treedb

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCompactStorageCachedAdvancesWritersPastBackendSegments(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.DisableSideStores = true
	opts.JournalLanes = 1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	valueLogDir := backenddb.ValueLogDirPath(dir)
	seededPath := filepath.Join(valueLogDir, "value-l0-000001.log")
	writeTestValueLogSegment(t, seededPath, 0, 1, []byte("backend-created-segment"))
	seededSize := testFileSize(t, seededPath)

	if _, err := db.CompactStorage(context.Background(), CompactStorageOptions{
		ValueLogProtectedPaths: []string{seededPath},
	}); err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}

	if err := db.SetSync([]byte("after-compact"), bytes.Repeat([]byte("v"), 512)); err != nil {
		t.Fatalf("SetSync after CompactStorage: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after CompactStorage write: %v", err)
	}

	if got := testFileSize(t, seededPath); got != seededSize {
		t.Fatalf("backend-created segment was reused: size=%d want %d", got, seededSize)
	}
	nextPath := filepath.Join(valueLogDir, "value-l0-000002.log")
	if got := testFileSize(t, nextPath); got == 0 {
		t.Fatalf("next cached value-log segment was not written: %s", nextPath)
	}
}

func TestCompactStorageDefaultPathReclaimsDeadTopSegment(t *testing.T) {
	dir := t.TempDir()
	opts := cachedRewriteReclaimTestOptions(dir)
	opts.IndexOuterLeavesInValueLog = false

	openBackend := func() (*backenddb.DB, func() error, error) {
		db, cleanup, err := OpenBackend(opts)
		if err != nil {
			return nil, nil, err
		}
		var closeOnce bool
		closeBackend := func() error {
			if closeOnce {
				return nil
			}
			closeOnce = true
			if db != nil {
				if err := db.Close(); err != nil {
					return err
				}
			}
			if cleanup != nil {
				return cleanup()
			}
			return nil
		}
		return db, closeBackend, nil
	}

	backend, cleanupBackend, err := openBackend()
	if err != nil {
		t.Fatalf("open backend (for write pointers): %v", err)
	}

	valueLogDir := backenddb.ValueLogDirPath(dir)
	segment1 := filepath.Join(valueLogDir, "value-l0-000001.log")
	segment2 := filepath.Join(valueLogDir, "value-l0-000002.log")

	deadPtr := writeTestValueLogSegmentWithPointer(t, segment1, 0, 1, bytes.Repeat([]byte("dead"), 512))
	livePtr := writeTestValueLogSegmentWithPointer(t, segment2, 0, 2, bytes.Repeat([]byte("live"), 1024))

	backendBatch := backend.NewBatch()
	firstBatch, ok := backendBatch.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
		Write() error
		Close() error
	})
	if !ok {
		t.Fatalf("missing pointer batch on backend %T", backendBatch)
	}
	if err := firstBatch.SetPointer([]byte("dead"), deadPtr); err != nil {
		t.Fatalf("set dead pointer: %v", err)
	}
	if err := firstBatch.Write(); err != nil {
		t.Fatalf("batch write (dead pointer): %v", err)
	}
	if err := firstBatch.Close(); err != nil {
		t.Fatalf("batch close (dead pointer): %v", err)
	}

	backendBatch = backend.NewBatch()
	secondBatch, ok := backendBatch.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
		Write() error
		Close() error
	})
	if !ok {
		t.Fatalf("missing pointer batch for live pointer %T", backendBatch)
	}
	if err := secondBatch.SetPointer([]byte("dead"), livePtr); err != nil {
		t.Fatalf("set updated dead pointer: %v", err)
	}
	if err := secondBatch.SetPointer([]byte("live"), livePtr); err != nil {
		t.Fatalf("set live pointer: %v", err)
	}
	if err := secondBatch.Write(); err != nil {
		t.Fatalf("batch write (live pointer): %v", err)
	}
	if err := secondBatch.Close(); err != nil {
		t.Fatalf("batch close (live pointer): %v", err)
	}
	if err := cleanupBackend(); err != nil {
		t.Fatalf("close backend (write pointers): %v", err)
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close cached db: %v", err)
		}
	})
	if _, err := db.Get([]byte("live")); err != nil {
		t.Fatalf("expected live key present before compact: %v", err)
	}

	if _, err := os.Stat(segment1); err != nil {
		t.Fatalf("expected initial dead segment to exist: %v", err)
	}
	if _, err := os.Stat(segment2); err != nil {
		t.Fatalf("expected initial live segment to exist: %v", err)
	}

	stats, err := db.CompactStorage(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}

	if _, err := os.Stat(segment1); !os.IsNotExist(err) {
		t.Fatalf("compact did not remove dead top segment %s (err=%v), stats=%+v", segment1, err, stats)
	}
	if _, err := os.Stat(segment2); err != nil {
		t.Fatalf("expected live segment retained: %v", err)
	}
	if got, err := db.Get([]byte("live")); err != nil {
		t.Fatalf("live key missing after compact: %v", err)
	} else if !bytes.Equal(got, bytes.Repeat([]byte("live"), 1024)) {
		t.Fatalf("live key value changed after compact")
	}
	if got, err := db.Get([]byte("dead")); err != nil {
		t.Fatalf("dead key missing after compact: %v", err)
	} else if !bytes.Equal(got, bytes.Repeat([]byte("live"), 1024)) {
		t.Fatalf("dead key value changed after compact")
	}
}

func TestCachedValueLogRewriteOnlinePreservesRetainedObservedSourcesAndReclaimsLeafDebt(t *testing.T) {
	dir := t.TempDir()
	opts := cachedRewriteReclaimTestOptions(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	expected := make(map[string][]byte)
	for i := 0; i < 256; i++ {
		key := cachedRewriteReclaimKey(i)
		value := cachedRewriteReclaimValue(i)
		if err := db.Set(key, value); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
		expected[string(key)] = value
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	source := listValueLogSegmentFiles(t, backenddb.ValueLogDirPath(dir))
	if len(source) == 0 {
		t.Fatalf("expected value-log source segments")
	}
	sourceIDs := make([]uint32, 0, len(source))
	for _, segment := range source {
		sourceIDs = append(sourceIDs, segment.id)
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceFileIDs:  sourceIDs,
		BatchSize:      64,
		SyncEachBatch:  true,
		LocalityPolicy: ValueLogRewriteLocalityGrouped,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if len(stats.SourceFileIDsUnreferenced) == 0 {
		t.Fatalf("rewrite reported no unreferenced source IDs")
	}
	if stats.SourceSegmentsReclaimed == 0 || stats.SourceBytesReclaimed == 0 {
		t.Fatalf("cached rewrite did not report reclaimed observed sources: %+v", stats)
	}

	retained := make(map[string]struct{})
	for _, path := range db.cached.ValueLogRetainedPaths() {
		retained[path] = struct{}{}
	}
	for _, segment := range source {
		if _, ok := retained[segment.path]; ok {
			if _, err := os.Stat(segment.path); err != nil {
				t.Fatalf("retained observed source %s was not preserved: %v", segment.path, err)
			}
			continue
		}
		if _, err := os.Stat(segment.path); !os.IsNotExist(err) {
			t.Fatalf("unretained source segment %s retained after observed-source reclaim: err=%v", segment.path, err)
		}
	}
	leafGC, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC dry-run after rewrite: %v", err)
	}
	if leafGC.GenerationsEligible != 0 || leafGC.FilesDeleted != 0 {
		t.Fatalf("rewrite left eligible leaf generations: %+v", leafGC)
	}
	for key, want := range expected {
		got, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %x after rewrite: %v", []byte(key), err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch for %x: got %dB want %dB", []byte(key), len(got), len(want))
		}
	}
}

func TestBackendValueLogRewriteReclaimsLeafGenerationDebt(t *testing.T) {
	dir := t.TempDir()
	opts := cachedRewriteReclaimTestOptions(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	expected := make(map[string][]byte)
	for i := 0; i < 256; i++ {
		key := cachedRewriteReclaimKey(i)
		value := cachedRewriteReclaimValue(i)
		if err := db.Set(key, value); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
		expected[string(key)] = value
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after load: %v", err)
	}

	source := listValueLogSegmentFiles(t, backenddb.ValueLogDirPath(dir))
	if len(source) == 0 {
		t.Fatalf("expected value-log source segments")
	}
	sourceIDs := make([]uint32, 0, len(source))
	for _, segment := range source {
		sourceIDs = append(sourceIDs, segment.id)
	}

	backend, cleanup, err := OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
	}
	stats, rewriteErr := backend.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
		SourceFileIDs: sourceIDs,
		BatchSize:     64,
		SyncEachBatch: true,
	})
	cleanupErr := cleanup()
	if rewriteErr != nil {
		t.Fatalf("backend ValueLogRewriteOnline: %v", rewriteErr)
	}
	if cleanupErr != nil {
		t.Fatalf("cleanup after backend rewrite: %v", cleanupErr)
	}
	if len(stats.SourceFileIDsUnreferenced) == 0 {
		t.Fatalf("backend rewrite reported no unreferenced source IDs")
	}

	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after backend rewrite: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	leafGC, err := reopen.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC dry-run after backend rewrite: %v", err)
	}
	if leafGC.GenerationsEligible != 0 {
		t.Fatalf("backend rewrite left eligible leaf generations: %+v", leafGC)
	}
	for key, want := range expected {
		got, err := reopen.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %x after backend rewrite: %v", []byte(key), err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch for %x: got %dB want %dB", []byte(key), len(got), len(want))
		}
	}
}

func TestCompactStorageClearsPublicRewriteSourceGCBehindActiveWriters(t *testing.T) {
	dir := t.TempDir()
	opts := cachedRewriteReclaimTestOptions(dir)
	opts.JournalLanes = 2

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	const rows = 20_000
	const batchSize = 16_000
	batch := db.NewBatchWithSize(batchSize)
	type expectedValue struct {
		size int
		sum  uint32
	}
	expected := make(map[string]expectedValue)
	for i := 0; i < rows; i++ {
		key := cachedRewriteReclaimKey(i)
		value := cachedRewriteReclaimValue(i)
		expected[string(key)] = expectedValue{size: len(value), sum: crc.Checksum(value)}
		if err := batch.Set(key, value); err != nil {
			t.Fatalf("batch set %d: %v", i, err)
		}
		if (i+1)%batchSize == 0 {
			if err := batch.Write(); err != nil {
				t.Fatalf("batch write %d: %v", i, err)
			}
			if err := batch.Close(); err != nil {
				t.Fatalf("batch close %d: %v", i, err)
			}
			batch = db.NewBatchWithSize(batchSize)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("final batch write: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("final batch close: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	source := listValueLogSegmentFiles(t, backenddb.ValueLogDirPath(dir))
	if len(source) == 0 {
		t.Fatalf("expected value-log source segments")
	}
	sourceIDs := make([]uint32, 0, len(source))
	for _, segment := range source {
		sourceIDs = append(sourceIDs, segment.id)
	}
	rewrite, err := db.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
		SourceFileIDs: sourceIDs,
		BatchSize:     batchSize,
		SyncEachBatch: true,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if rewrite.ValueRecordsCopied == 0 {
		t.Fatalf("rewrite copied no value records: %+v", rewrite)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after rewrite: %v", err)
	}

	compactor, err := Open(opts)
	if err != nil {
		t.Fatalf("open compactor: %v", err)
	}
	compact, err := compactor.CompactStorage(context.Background(), CompactStorageOptions{
		ValueLogRewriteBatchSize:        batchSize,
		LeafPackMinExpectedReclaimBytes: 1,
		LeafPackMinReclaimPerCopyPPM:    1,
	})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if !compact.FullyCompacted {
		t.Fatalf("CompactStorage reported remaining debt before close: %+v", compact.RemainingDebt)
	}
	if err := compactor.Close(); err != nil {
		t.Fatalf("close compactor: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after compaction: %v", err)
	}
	gc, err := reopened.ValueLogGC(context.Background(), ValueLogGCOptions{DryRun: true})
	if err != nil {
		_ = reopened.Close()
		t.Fatalf("ValueLogGC dry-run after compaction: %v", err)
	}
	if gc.SegmentsEligible != 0 || gc.BytesEligible != 0 {
		_ = reopened.Close()
		t.Fatalf("CompactStorage left post-reopen GC debt: segments=%d bytes=%d stats=%+v", gc.SegmentsEligible, gc.BytesEligible, gc)
	}
	for key, want := range expected {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			_ = reopened.Close()
			t.Fatalf("get %x after CompactStorage: %v", []byte(key), err)
		}
		if len(got) != want.size || crc.Checksum(got) != want.sum {
			_ = reopened.Close()
			t.Fatalf("value mismatch for %x: got len=%d crc=%08x want len=%d crc=%08x", []byte(key), len(got), crc.Checksum(got), want.size, want.sum)
		}
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened: %v", err)
	}
}

func writeTestValueLogSegment(t *testing.T, path string, lane, seq uint32, value []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir value-log dir: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, value); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
}

func writeTestValueLogSegmentWithPointer(t *testing.T, path string, lane, seq uint32, value []byte) page.ValuePtr {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir value-log dir: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptr, err := writer.Append(0, nil, uint64(seq), value)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return ptr
}

func testFileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

type valueLogSegmentFile struct {
	id   uint32
	path string
}

func cachedRewriteReclaimTestOptions(dir string) Options {
	opts := OptionsFor(ProfileFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.DisableSideStores = true
	opts.JournalLanes = 1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	return opts
}

func cachedRewriteReclaimKey(i int) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(i))
	return key
}

func cachedRewriteReclaimValue(i int) []byte {
	prefix := []byte(`{"kind":"PushEvent","commit":{"operation":"append","collection":"events"},"payload":"`)
	value := make([]byte, 0, len(prefix)+2048)
	value = append(value, prefix...)
	for len(value) < cap(value)-2 {
		value = append(value, byte('a'+i%26))
	}
	value = append(value, '"', '}')
	return value
}

func listValueLogSegmentFiles(t *testing.T, dir string) []valueLogSegmentFile {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read value-log dir: %v", err)
	}
	var out []valueLogSegmentFile
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		id, ok := parseValueLogSegmentFileID(entry.Name())
		if !ok {
			continue
		}
		out = append(out, valueLogSegmentFile{
			id:   id,
			path: filepath.Join(dir, entry.Name()),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].id < out[j].id
	})
	return out
}

func parseValueLogSegmentFileID(name string) (uint32, bool) {
	if !strings.HasPrefix(name, "value-l") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(name, "value-l"), ".log")
	parts := strings.Split(body, "-")
	if len(parts) != 2 {
		return 0, false
	}
	lane, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, false
	}
	seq, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, false
	}
	id, err := valuelog.EncodeFileID(uint32(lane), uint32(seq))
	if err != nil {
		return 0, false
	}
	return id, true
}
