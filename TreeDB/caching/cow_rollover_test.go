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
	if sources := c.budget.Stats().Sources; sources != 4 {
		t.Fatalf("sources=%d", sources)
	}
}

func TestCOWRolloverUsesParkedSuccessorAtGenerationLimit(t *testing.T) {
	_, c := cowCutFixture(t)
	c.writerMu.Lock()
	old := cowCutPrepare(t, c, "prefix").publish()
	c.writerMu.Unlock()
	if old != nil {
		old.drain()
	}
	parked := append([]cowRawSuccessor(nil), c.successors...)
	var extra []*memtable.COWWriter
	for {
		w, err := memtable.NewCOWWriter(c.budget)
		if err != nil {
			break
		}
		extra = append(extra, w)
	}
	defer func() {
		for _, w := range extra {
			r := w.Close()
			r.Drain()
		}
	}()
	c.writerMu.Lock()
	p, err := c.prepareRollover()
	if err == nil {
		p.install()
	}
	c.writerMu.Unlock()
	if p != nil {
		p.drain()
	}
	if err != nil {
		t.Fatalf("preadmitted rollover: %v", err)
	}
	for i, s := range parked {
		if c.writers[i] != s.writer || c.cut.shards[i].root != s.root || c.successors[i].writer != nil {
			t.Fatal("did not adopt exact parked successor")
		}
	}
}

func TestCOWFirstDirtyCancellationReturnsJointProvision(t *testing.T) {
	_, c := cowCutFixture(t)
	before, maintenance := c.budget.Stats(), c.maintenance.Stats()
	c.writerMu.Lock()
	p := cowCutPrepare(t, c, "private")
	if c.budget.Stats().Sources != 2 || c.budget.Stats().Generations != before.Generations+2 {
		t.Fatal("missing actual joint provision")
	}
	p.cancel()
	c.writerMu.Unlock()
	if err := p.drainCancelled(); err != nil {
		t.Fatal(err)
	}
	after, mAfter := c.budget.Stats(), c.maintenance.Stats()
	if after.TotalBytes != before.TotalBytes || after.Sources != before.Sources || after.Generations != before.Generations || mAfter.TotalBytes != maintenance.TotalBytes {
		t.Fatalf("cancel retained ownership: before=%+v after=%+v maintenance=%+v/%+v", before, after, maintenance, mAfter)
	}
	for _, s := range c.successors {
		if s.writer != nil {
			t.Fatal("private provision escaped")
		}
	}
}

func TestCOWDirtyReplacementReusesParkedProvision(t *testing.T) {
	_, c := cowCutFixture(t)
	c.writerMu.Lock()
	old := cowCutPrepare(t, c, "first").publish()
	c.writerMu.Unlock()
	if old != nil {
		old.drain()
	}
	parked := append([]cowRawSuccessor(nil), c.successors...)
	before := c.budget.Stats()
	c.writerMu.Lock()
	p := cowCutPrepare(t, c, "replacement")
	if p.successors != nil {
		t.Fatal("repeat dirty write allocated successor vector")
	}
	old = p.publish()
	c.writerMu.Unlock()
	if old != nil {
		old.drain()
	}
	for i, s := range parked {
		if c.successors[i].writer != s.writer || c.successors[i].root != s.root {
			t.Fatal("replacement reconstructed provision")
		}
	}
	if stats := c.budget.Stats(); stats.Generations != before.Generations || stats.Sources != before.Sources {
		t.Fatal("replacement charged another provision")
	}
}

func TestCOWParkedSuccessorNotPinnedByViewAtClose(t *testing.T) {
	db, c := cowCutFixture(t)
	c.writerMu.Lock()
	old := cowCutPrepare(t, c, "held").publish()
	c.writerMu.Unlock()
	if old != nil {
		old.drain()
	}
	view := db.AcquireSnapshot()
	if c.budget.Stats().Generations != 4 {
		t.Fatal("missing parked generations")
	}
	if got, err := view.Get([]byte("a")); err != nil || string(got) != "held" {
		t.Fatalf("held view before Close: %q %v", got, err)
	}
	if err := c.close(); err != nil {
		t.Fatal(err)
	}
	if stats := c.budget.Stats(); stats.Generations != 2 || stats.Sources != 2 {
		t.Fatalf("view pinned parked ownership: %+v", stats)
	}
	view.Close()
	if stats := c.budget.Stats(); stats.TotalBytes != 0 || stats.Generations != 0 || stats.Sources != 0 {
		t.Fatalf("final ownership: %+v", stats)
	}
}
