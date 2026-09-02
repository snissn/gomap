package treedb_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestValueLogGCOnline_ProtectedSetSafety(t *testing.T) {
	dir := t.TempDir()

	db, err := treedb.Open(treedb.Options{
		Dir:                           dir,
		BackgroundIndexVacuumInterval: -1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
			Generational: treedb.ValueLogGenerationConfig{
				Policy: treedb.ValueLogGenerationOff,
			},
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	expected := make(map[string][]byte)
	inOrderKeys := make([][]byte, 0, 4096)

	for round := 0; round < 4; round++ {
		for i := 0; i < 180; i++ {
			key := make([]byte, 16)
			binary.BigEndian.PutUint64(key[:8], uint64(round))
			binary.BigEndian.PutUint64(key[8:], uint64(i))
			val := bytes.Repeat([]byte{byte((round + i) % 251)}, 1024)
			if err := db.Set(key, val); err != nil {
				t.Fatalf("set round=%d i=%d: %v", round, i, err)
			}
			expected[string(key)] = append([]byte(nil), val...)
			inOrderKeys = append(inOrderKeys, append([]byte(nil), key...))
		}

		// Create churn and allow segments to become cold while writes continue.
		deletes := 60
		if deletes > len(inOrderKeys) {
			deletes = len(inOrderKeys)
		}
		for i := 0; i < deletes; i++ {
			key := inOrderKeys[0]
			inOrderKeys = inOrderKeys[1:]
			if err := db.Delete(key); err != nil {
				t.Fatalf("delete round=%d i=%d: %v", round, i, err)
			}
			delete(expected, string(key))
		}

		stats, err := db.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{Mode: treedb.ValueLogGCModeOnline})
		if err != nil {
			t.Fatalf("ValueLogGC online round=%d: %v", round, err)
		}
		if stats.FailClosedToDryRun && stats.SegmentsDeleted > 0 {
			t.Fatalf("ValueLogGC online round=%d: dry-run reported deletes", round)
		}
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	for key, want := range expected {
		got, err := reopen.Get([]byte(key))
		if err != nil {
			t.Fatalf("reopen get %x: %v", []byte(key), err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reopen mismatch key=%s got=%dB want=%dB", fmt.Sprintf("%x", []byte(key)), len(got), len(want))
		}
	}
}

func TestValueLogGC_OnlineMode_HealthStateNoUnsafeDeletes(t *testing.T) {
	dir := t.TempDir()

	db, err := treedb.Open(treedb.Options{
		Dir:                           dir,
		BackgroundIndexVacuumInterval: -1,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
			Generational: treedb.ValueLogGenerationConfig{
				Policy: treedb.ValueLogGenerationOff,
			},
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 240; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		val := bytes.Repeat([]byte{byte(i % 251)}, 1024)
		if err := db.Set(key, val); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	for i := 0; i < 80; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := db.Delete(key); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}

	stats, err := db.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{Mode: treedb.ValueLogGCModeOnline})
	if err != nil {
		t.Fatalf("ValueLogGC online: %v", err)
	}
	if stats.FailClosedToDryRun && stats.SegmentsDeleted > 0 {
		t.Fatalf("online fail-closed path must not delete segments")
	}
	if stats.FailClosedToDryRun {
		if _, err := db.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{Mode: treedb.ValueLogGCModeStrict}); err != nil {
			t.Fatalf("ValueLogGC strict fallback: %v", err)
		}
	}

	healthPath := filepath.Join(dir, "maindb", "vlog_health.json")
	data, err := os.ReadFile(healthPath)
	if err != nil {
		t.Fatalf("read health metadata: %v", err)
	}
	var decoded struct {
		Segments map[string]struct {
			SegmentBytes int64 `json:"segment_bytes"`
			LiveBytes    int64 `json:"live_bytes"`
		} `json:"segments"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode health metadata: %v", err)
	}
	if len(decoded.Segments) == 0 {
		t.Fatalf("expected at least one health metadata segment")
	}
	for id, seg := range decoded.Segments {
		if seg.SegmentBytes < 0 || seg.LiveBytes < 0 {
			t.Fatalf("invalid negative bytes in segment %s: %+v", id, seg)
		}
		if seg.LiveBytes > seg.SegmentBytes {
			t.Fatalf("live bytes exceed segment bytes in segment %s: %+v", id, seg)
		}
	}
}
