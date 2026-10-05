package caching

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestCOWBatchCanonicalPreparationPublishesFinalRevisions(t *testing.T) {
	db, c := cowCutFixture(t)
	b := &Batch{db: db, entries: []batch.Entry{
		{Type: batch.OpPut, Key: []byte("a"), Value: []byte("new"), Revision: 41},
		{Type: batch.OpDelete, Key: []byte("b"), Revision: 42},
	}}
	c.writerMu.Lock()
	backend := db.backend.(*backenddb.DB)
	err := backend.FinalizeRawKVEntryScanForCachedPublication(b.PrepareExternalCommandWALPublication, b.finalizeCOWPublication, b.Replay, 2)
	if err != nil {
		c.writerMu.Unlock()
		t.Fatal(err)
	}
	old := db.AcquireSnapshot()
	value, err := old.Get([]byte("a"))
	if (err != nil && !errors.Is(err, tree.ErrKeyNotFound)) || value != nil {
		c.writerMu.Unlock()
		t.Fatalf("staged value=%q err=%v", value, err)
	}
	retired := b.cowPrepared.cut.publish()
	p := b.cowPrepared
	b.cowPrepared = nil
	c.writerMu.Unlock()
	if retired != nil {
		retired.drain()
	}
	p.scratch.Close()
	defer old.Close()
	s := db.AcquireSnapshot()
	defer s.Close()
	value, revision, err := s.GetVersioned([]byte("a"))
	if err != nil || string(value) != "new" || revision != page.EntryRevision(41) {
		t.Fatalf("value=%q revision=%d err=%v", value, revision, err)
	}
	entry, err := s.GetEntry([]byte("b"))
	if err != nil || entry.Revision != 42 {
		t.Fatalf("tombstone=%+v err=%v", entry, err)
	}
}

func TestCOWBatchFinalizerRefusalCancelsPrivateRoots(t *testing.T) {
	db, c := cowCutFixture(t)
	b := &Batch{db: db, entries: []batch.Entry{{Type: batch.OpPut, Key: []byte("a"), Value: []byte("private"), Revision: 23}}}
	before := c.budget.Stats().TotalBytes
	c.writerMu.Lock()
	backend := db.backend.(*backenddb.DB)
	err := backend.FinalizeRawKVEntryScanForCachedPublication(b.PrepareExternalCommandWALPublication, func(payload []byte, lookup func(page.ValuePtr) (uint64, bool)) error {
		b.entries[0].Revision++ // canonical frame and intended live state diverge
		return b.finalizeCOWPublication(payload, lookup)
	}, b.Replay, 1)
	if err == nil {
		c.writerMu.Unlock()
		t.Fatal("mismatched revision accepted")
	}
	p := b.cancelCOWPublication()
	c.writerMu.Unlock()
	p.drainCancelled()
	s := db.AcquireSnapshot()
	defer s.Close()
	v, err := s.Get([]byte("a"))
	if (err != nil && !errors.Is(err, tree.ErrKeyNotFound)) || v != nil {
		t.Fatalf("cancelled value=%q err=%v", v, err)
	}
	// Private copied nodes remain conservatively charged to the live writer
	// generation, while caller scratch/cut leases refund immediately.
	if got := c.budget.Stats().ExternalLeases; got != 3 {
		t.Fatalf("external leases=%d before bytes=%d", got, before)
	}
}

func TestCOWLogicalSizeChangedKeysAndDuplicates(t *testing.T) {
	db, c := cowCutFixture(t)
	publish := func(entries []batch.Entry) {
		t.Helper()
		b := &Batch{db: db, entries: entries}
		c.writerMu.Lock()
		err := db.backend.(*backenddb.DB).FinalizeRawKVEntryScanForCachedPublication(b.PrepareExternalCommandWALPublication, b.finalizeCOWPublication, b.Replay, len(entries))
		if err != nil {
			p := b.cancelCOWPublication()
			c.writerMu.Unlock()
			if p != nil {
				p.drainCancelled()
			}
			t.Fatal(err)
		}
		p := b.cowPrepared
		old := p.cut.publish()
		c.writerMu.Unlock()
		if old != nil {
			old.drain()
		}
		p.scratch.Close()
	}
	publish([]batch.Entry{{Type: batch.OpPut, Key: []byte("a"), Value: []byte("initial"), Revision: 1}, {Type: batch.OpPut, Key: []byte("a"), Value: []byte("final"), Revision: 2}})
	var size int64
	for _, table := range c.cut.shards {
		size += table.Size()
	}
	if size != 6 {
		t.Fatalf("duplicate size=%d want6", size)
	}
	publish([]batch.Entry{{Type: batch.OpDelete, Key: []byte("a"), Revision: 3}, {Type: batch.OpPut, Key: []byte("b"), Value: []byte("new"), Revision: 4}})
	size = 0
	for _, table := range c.cut.shards {
		size += table.Size()
	}
	if size != 5 {
		t.Fatalf("replacement size=%d want5", size)
	}
}

func TestCOWUnsupportedSurfacesRefuseBeforeEffects(t *testing.T) {
	db, c := cowCutFixture(t)
	called := false
	if err := db.Update([]byte("a"), func([]byte) (backenddb.UpdateResult, error) { called = true; return backenddb.UpdateResult{}, nil }); err != ErrCOWUnsupported {
		t.Fatalf("update err=%v", err)
	}
	if called {
		t.Fatal("unsupported callback invoked")
	}
	if err := db.DeleteRange(nil, nil); err != ErrCOWUnsupported {
		t.Fatalf("range err=%v", err)
	}
	b := db.NewBatch()
	defer b.Close()
	if err := b.DeleteRange(nil, nil); err != ErrCOWUnsupported {
		t.Fatalf("batch range err=%v", err)
	}
	b.maybeSwitchToStreaming()
	if b.backend != nil {
		t.Fatal("COW batch acquired streaming backend")
	}
	var tx ConditionalTxn
	if err := db.InitConditionalTxn(&tx); err != backenddb.ErrConditionalTxnUnsupported {
		t.Fatalf("conditional err=%v", err)
	}
	if c.cut.shards[0].Len()+c.cut.shards[1].Len() != 0 {
		t.Fatal("refusal changed roots")
	}
}
