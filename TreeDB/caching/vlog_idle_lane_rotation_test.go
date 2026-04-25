package caching

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCheckpoint_DoesNotRotateIdleValueLogLanes(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	// Use 3 lanes so warm/cold lanes exist. We will only write keys that map to the
	// first hot lane and then checkpoint repeatedly; idle lanes should not churn.
	db, err := Open(dir, backend, Options{
		AllowUnsafe:                      true,
		JournalLanes:                     3,
		MemtableShards:                   3,
		ForceValueLogPointers:            true,
		ValueLogMaxSegmentBytes:          1 << 20, // keep rotations cheap if they happen
		ValueLogRewriteTriggerTotalBytes: 1 << 60, // keep rewrite out of the way
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Seed some pointer-backed data so at least one lane's value-log writer becomes dirty
	// (durability=none). Checkpoint should clear dirty state without rotating lanes.
	value := bytes.Repeat([]byte("v"), 64<<10)
	for i := 0; i < 8; i++ {
		b := db.NewBatch()
		k := []byte("k")
		k = append(k, byte(i>>16), byte(i>>8), byte(i))
		if err := b.Set(k, value); err != nil {
			_ = b.Close()
			t.Fatalf("set: %v", err)
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	if !db.hasDirtyValueLogLanes() {
		t.Fatalf("expected dirty value-log lanes after unsynced pointer writes")
	}

	walDir := filepath.Join(dir, "wal")
	countSegments := func() map[string]int {
		ents, err := os.ReadDir(walDir)
		if err != nil {
			t.Fatalf("readdir wal: %v", err)
		}
		countByLane := map[string]int{}
		for _, ent := range ents {
			if ent.IsDir() {
				continue
			}
			name := ent.Name()
			if !strings.HasPrefix(name, "value-l") || !strings.HasSuffix(name, ".log") {
				continue
			}
			// value-l<lane>-<seq>.log
			parts := strings.Split(name, "-")
			if len(parts) < 3 {
				continue
			}
			lane := parts[1] // "l0", "l1", ...
			countByLane[lane]++
		}
		return countByLane
	}

	before := countSegments()

	// Repeated checkpoints should not rotate value-log lanes that are idle (no writes).
	for i := 0; i < 50; i++ {
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("checkpoint %d: %v", i, err)
		}
	}

	after := countSegments()
	if db.hasDirtyValueLogLanes() {
		t.Fatalf("expected checkpoint to clear dirty value-log lanes")
	}

	// Checkpoint should establish a durability boundary without rotating/creating new
	// value-log segments.
	for lane, got := range after {
		want := before[lane]
		if got != want {
			t.Fatalf("lane %s rotated value-log segments: before=%d after=%d", lane, want, got)
		}
	}
}
