package caching

import (
	"bytes"
	"fmt"
	"os"
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

	opened, err := db.Open(db.Options{Dir: dir})
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
