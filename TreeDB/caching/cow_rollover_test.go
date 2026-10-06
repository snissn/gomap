package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCOWRolloverPreservesFrozenPrefixAndLateWrites(t *testing.T) {
	db, c := cowCutFixture(t)
	c.writerMu.Lock()
	old := cowCutPrepare(t, c, "prefix").publish()
	c.writerMu.Unlock()
	if old != nil {
		old.drain()
	}
	prefix := db.AcquireSnapshot()
	defer prefix.Close()
	c.writerMu.Lock()
	rollover, err := c.prepareRollover()
	if err != nil {
		c.writerMu.Unlock()
		if rollover != nil {
			rollover.drain()
		}
		t.Fatal(err)
	}
	// Fully constructed successor is still private until the short cut swap.
	paused := db.AcquireSnapshot()
	if paused.cowCut != prefix.cowCut {
		t.Fatal("rollover exposed private successor")
	}
	rollover.install()
	c.writerMu.Unlock()
	rollover.drain()
	paused.Close()
	if len(c.cut.frozen) != 2 {
		t.Fatalf("frozen count=%d", len(c.cut.frozen))
	}
	if c.cut.basis != prefix.cowCut.basis {
		t.Fatal("rollover captured a second backend basis")
	}
	for _, table := range c.cut.shards {
		if table.Len() != 0 || table.Size() != 0 {
			t.Fatal("successor mutable not empty")
		}
	}
	c.writerMu.Lock()
	old = cowCutPrepare(t, c, "late").publish()
	c.writerMu.Unlock()
	if old != nil {
		old.drain()
	}
	current := db.AcquireSnapshot()
	defer current.Close()
	for _, key := range []string{"a", "d"} {
		value, err := prefix.Get([]byte(key))
		if err != nil || string(value) != "prefix" {
			t.Fatalf("prefix %s=%q err=%v", key, value, err)
		}
		value, err = current.Get([]byte(key))
		if err != nil || string(value) != "late" {
			t.Fatalf("current %s=%q err=%v", key, value, err)
		}
	}
	if sources := c.budget.Stats().Sources; sources != 2 {
		t.Fatalf("sources=%d", sources)
	}
}

func TestCOWRolloverRefusesBeforeFreeze(t *testing.T) {
	_, c := cowCutFixture(t)
	c.writerMu.Lock()
	old := cowCutPrepare(t, c, "prefix").publish()
	c.writerMu.Unlock()
	if old != nil {
		old.drain()
	}
	// Fill the finite generation slots with independent empty writers. Rollover
	// must fail during successor admission before marking a current source frozen.
	var extra []*memtable.COWWriter
	for {
		w, err := memtable.NewCOWWriter(c.budget)
		if err != nil {
			break
		}
		extra = append(extra, w)
	}
	c.writerMu.Lock()
	before := c.cut
	p, err := c.prepareRollover()
	c.writerMu.Unlock()
	if err != memtable.ErrCOWCapacity {
		t.Fatalf("rollover err=%v", err)
	}
	p.drain()
	if c.cut != before || c.budget.Stats().Sources != 0 {
		t.Fatal("refused rollover changed cut/froze current")
	}
	for _, w := range extra {
		r := w.Close()
		r.Drain()
	}
	c.writerMu.Lock()
	// Current writers remain usable after refusal.
	old = cowCutPrepare(t, c, "after refusal").publish()
	c.writerMu.Unlock()
	if old != nil {
		old.drain()
	}
}
