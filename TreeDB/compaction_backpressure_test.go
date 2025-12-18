package treedb

import (
	"bytes"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/compaction"
)

func TestCompactCandidates_CachedMode_AssistsFlushBackpressure(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                  dir,
		Mode:                 ModeCached,
		FlushThreshold:        64 * 1024,
		MaxQueuedMemtables:    1,
		WriterFlushMaxMemtables: 100,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	// Create a non-active slab with dead bytes so compaction has real work.
	valA := bytes.Repeat([]byte("a"), 2048)
	valB := bytes.Repeat([]byte("b"), 2048)
	for i := 0; i < 200; i++ {
		k := []byte{0x63, 0x2f, byte(i >> 8), byte(i)} // "c/" + u16
		if err := d.backend.SetSync(k, valA); err != nil {
			t.Fatalf("seed set: %v", err)
		}
		if err := d.backend.SetSync(k, valB); err != nil {
			t.Fatalf("seed overwrite: %v", err)
		}
	}
	if _, err := d.backend.SlabManager().Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if err := d.backend.SetSync([]byte("touch"), valA); err != nil {
		t.Fatalf("touch: %v", err)
	}

	c := compaction.New(d.backend)
	cands, err := c.Candidates(compaction.Options{
		DeadRatioThreshold: 0.10,
		MinTotalBytes:      1,
		MaxSlabs:           1,
	})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(cands) == 0 {
		t.Fatalf("expected at least one compaction candidate")
	}

	// Build cached backlog without triggering background flush:
	// Iterator rotates the mutable memtable into the queue with triggerFlush=false.
	for i := 0; i < 8; i++ {
		if err := d.Set([]byte{0x71, byte(i)}, bytes.Repeat([]byte("x"), 8*1024)); err != nil {
			t.Fatalf("cached set: %v", err)
		}
		it, err := d.Iterator(nil, nil)
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		_ = it.Close()
	}

	startBacklog := d.cached.QueueBacklogBytes()
	if startBacklog <= 0 {
		t.Fatalf("expected backlog > 0")
	}

	if err := d.CompactCandidates(compaction.Options{
		DeadRatioThreshold: 0.10,
		MinTotalBytes:      1,
		MaxSlabs:           1,
		MicroBatchSize:     32,
	}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Compaction should have triggered bounded flush assist, so the backlog should
	// drain even without additional foreground writes.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if d.cached.QueueBacklogBytes() == 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("expected backlog to drain, start=%d now=%d", startBacklog, d.cached.QueueBacklogBytes())
}

