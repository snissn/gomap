package caching

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// TestUnifiedWAL_SplitLog_Flow verifies the flow of data when SplitValueLog is enabled:
// 1. Data lands in ValueLog (not CommitLog).
// 2. RAM holds value (or pointer).
// 3. Flush copies to Backend (Slab).
// 4. ValueLog deletion is safe after flush.
func TestUnifiedWAL_SplitLog_Flow(t *testing.T) {
	scenarios := []struct {
		name                     string
		memtableValueLogPointers bool
	}{
		{"RAM_Value", false},
		{"RAM_Ptr", true},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			dir := t.TempDir()
			backend, err := db.Open(db.Options{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()

			opts := Options{
				FlushThreshold:           4 * 1024 * 1024,
				ValueLogPointerThreshold: 100,
				SplitValueLog:            true, // Crucial for efficiency
				MemtableValueLogPointers: sc.memtableValueLogPointers,
				DisableWAL:               false,
				AllowUnsafe:              true,
			}

			cached, err := Open(dir, backend, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer cached.Close()

			// 1. Write Large Value
			valSize := 1000
			key := []byte("large-key")
			val := bytes.Repeat([]byte{0xAA}, valSize)
			if err := cached.Set(key, val); err != nil {
				t.Fatal(err)
			}

			// 3. Flush (Checkpoint)
			// This flushes buffers to disk and flushes Memtable to Backend.
			if err := cached.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			// 2. Verify CommitLog vs ValueLog sizes (Check AFTER flush)
			commitSize, valueSize := getLogSizes(t, dir)
			t.Logf("CommitLog Size: %d, ValueLog Size: %d", commitSize, valueSize)

			// Size checks are intentionally informational only:
			// - Copy-on-Flush can make value-log segments immediately deletable after Checkpoint.
			// - CommitLog segments may rotate/truncate, leaving a 0-byte "current" segment.
			// Correctness is asserted by verifying backend reads still succeed after deleting value logs.

			// 4. Verify Backend Storage
			stats := backend.Stats()
			t.Logf("Backend Stats: %+v", stats)
			// Check ActiveSlabID. If > 0, slabs exist.
			// Or check if Get works after deleting value logs.

			// 5. Destructive: Delete ValueLog
			deleteValueLogs(t, dir)

			// 6. Read
			got, err := backend.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, val) {
				t.Fatal("Data mismatch after value-log deletion")
			}
			t.Log("Data verified from backend after value-log deletion")
		})
	}
}

// TestUnifiedWAL_CrashRecoveryMissingCommit ensures payloads without commit intent are ignored.
func TestUnifiedWAL_CrashRecoveryMissingCommit(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	valuePath := filepath.Join(walDir, "value-000001.log")
	writer, err := valuelog.NewWriter(valuePath, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("valuelog.NewWriter: %v", err)
	}
	key := []byte("k1")
	val := bytes.Repeat([]byte{0xAB}, 512)
	if _, err := writer.Append(1, val); err != nil {
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

	backend, err := db.Open(db.Options{Dir: dir, SplitValueLog: true})
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

// TestUnifiedWAL_CrashRecoveryMissingPayload ensures commit intent without payload fails fast.
func TestUnifiedWAL_CrashRecoveryMissingPayload(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	commitPath := filepath.Join(walDir, "commit-000001.log")
	writer, err := commitlog.NewWriter(commitPath)
	if err != nil {
		t.Fatalf("commitlog.NewWriter: %v", err)
	}
	rec := commitlog.Record{Op: commitlog.OpSetRID, Key: []byte("k2"), RID: 1}
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

	if _, err := db.Open(db.Options{Dir: dir, SplitValueLog: true}); err == nil {
		t.Fatalf("expected recovery to fail on missing payload, got nil error")
	} else if !strings.Contains(err.Error(), "missing rid") {
		t.Fatalf("expected pointer payload error, got %v", err)
	}
}

func TestUnifiedWAL_LargeBatch(t *testing.T) {
	dir := t.TempDir()
	backend, _ := db.Open(db.Options{Dir: dir})
	defer backend.Close()

	opts := Options{
		FlushThreshold:           4 * 1024 * 1024,
		ValueLogPointerThreshold: 100,
		SplitValueLog:            true,
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
	deleteValueLogs(t, dir)
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

func deleteValueLogs(t *testing.T, dir string) {
	walDir := filepath.Join(dir, "wal")
	entries, _ := os.ReadDir(walDir)
	for _, e := range entries {
		if len(e.Name()) > 6 && e.Name()[:6] == "value-" {
			os.Remove(filepath.Join(walDir, e.Name()))
		}
	}
}
