package treedb_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestValueLogGCOnline_ProtectedSetSafety(t *testing.T) {
	dir := t.TempDir()

	db, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	expected := make(map[string][]byte)
	inOrderKeys := make([][]byte, 0, 4096)

	for round := 0; round < 6; round++ {
		for i := 0; i < 400; i++ {
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
		deletes := 120
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
