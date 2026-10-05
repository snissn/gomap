package caching

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func cowCutFixture(t *testing.T) (*DB, *cowCache) {
	t.Helper()
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Set([]byte("disk"), []byte("basis")); err != nil {
		t.Fatal(err)
	}
	c, err := newCOWCache(backend.AcquireSnapshot(), 2, memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	db := &DB{backend: backend, cow: c, mutableShards: make([]memShard, 2), mutableShardMask: 1}
	t.Cleanup(func() { c.close(); _ = backend.Close() })
	return db, c
}
func cowCutPrepare(t *testing.T, c *cowCache, value string) *cowPreparedCut {
	t.Helper()
	groups := make([][]memtable.COWMutation, 2)
	selector := &DB{mutableShardMask: 1, mutableShards: make([]memShard, 2)}
	for i, key := range []string{"a", "b"} {
		shard := selector.shardIndex([]byte(key))
		groups[shard] = append(groups[shard], memtable.COWMutation{Key: []byte(key), Value: []byte(value), Flags: node.FlagInline, Revision: page.EntryRevision(11 + i)})
	}
	p, err := c.prepare(groups, make([]memtable.COWPrepareOptions, 2))
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestCOWCutPrivatePrepareDoesNotBlockOrMixReaders(t *testing.T) {
	db, c := cowCutFixture(t)
	c.writerMu.Lock()
	p := cowCutPrepare(t, c, "old")
	retired := p.publish()
	c.writerMu.Unlock()
	if retired != nil {
		retired.drain()
	}
	old := db.AcquireSnapshot()
	defer old.Close()
	c.writerMu.Lock()
	p = cowCutPrepare(t, c, "new")
	// Writer remains paused after both shard roots are prepared. Capture cannot
	// need its owner, and readers must see the complete previously published cut.
	done := make(chan error, 1)
	go func() {
		s := db.AcquireSnapshot()
		if s == nil {
			done <- errors.New("capture nil")
			return
		}
		defer s.Close()
		for _, key := range []string{"a", "b"} {
			v, err := s.Get([]byte(key))
			if err != nil || string(v) != "old" {
				done <- errors.New("private shard exposed")
				return
			}
		}
		done <- nil
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	retired = p.publish()
	c.writerMu.Unlock()
	if retired != nil {
		retired.drain()
	}
	current := db.AcquireSnapshot()
	defer current.Close()
	for _, test := range []struct {
		s     *Snapshot
		value string
	}{{old, "old"}, {current, "new"}} {
		for _, key := range []string{"a", "b"} {
			v, err := test.s.Get([]byte(key))
			if err != nil || string(v) != test.value {
				t.Fatalf("key=%s got=%q err=%v expected=%s", key, v, err, test.value)
			}
		}
		v, err := test.s.Get([]byte("disk"))
		if err != nil || string(v) != "basis" {
			t.Fatalf("basis got=%q err=%v", v, err)
		}
		it, err := test.s.Iterator(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		var keys [][]byte
		for it.Valid() {
			keys = append(keys, append([]byte(nil), it.Key()...))
			it.Next()
		}
		if err := it.Error(); err != nil {
			t.Fatal(err)
		}
		_ = it.Close()
		if len(keys) != 3 || !bytes.Equal(keys[0], []byte("a")) || !bytes.Equal(keys[2], []byte("disk")) {
			t.Fatalf("iterator keys=%q", keys)
		}
	}
}
func TestCOWCutCancelledResourcesDrainOutsideWriter(t *testing.T) {
	_, c := cowCutFixture(t)
	c.writerMu.Lock()
	groups := [][]memtable.COWMutation{{{Key: []byte("a"), Value: []byte("never")}}, nil}
	opts := []memtable.COWPrepareOptions{{ResourceSlots: 1, ResourceBytes: 128}, {}}
	p, err := c.prepare(groups, opts)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err := p.prepared[0].AttachResources([]memtable.COWResourceID{{Kind: 1, ID: 77}}, func() {
		c.writerMu.Lock()
		defer c.writerMu.Unlock()
		called = true
	}); err != nil {
		t.Fatal(err)
	}
	p.cancel()
	if called {
		t.Fatal("resource retired under writer owner")
	}
	c.writerMu.Unlock()
	p.drainCancelled()
	if !called {
		t.Fatal("resource not retired")
	}
}
func TestCOWCutPublicationCaptureCloseRace(t *testing.T) {
	db, c := cowCutFixture(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 50 {
			s := db.AcquireSnapshot()
			if s != nil {
				_, _ = s.Get([]byte("a"))
				_ = s.Close()
			}
		}
	}()
	for range 50 {
		c.writerMu.Lock()
		p := cowCutPrepare(t, c, "same")
		retired := p.publish()
		c.writerMu.Unlock()
		if retired != nil {
			retired.drain()
		}
	}
	wg.Wait()
	if views := c.budget.Stats().Views; views != 0 {
		t.Fatalf("views leaked: %d", views)
	}
}

func TestCOWCutDBCloseExcludesAdmittedReadAndAllowsDelayedRelease(t *testing.T) {
	db, c := cowCutFixture(t)
	c.writerMu.Lock()
	p := cowCutPrepare(t, c, "old")
	retired := p.publish()
	c.writerMu.Unlock()
	if retired != nil {
		retired.drain()
	}
	snap := db.AcquireSnapshot()
	it, err := snap.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.beginRead(); err != nil {
		t.Fatal(err)
	}
	started, finished := make(chan struct{}), make(chan struct{})
	go func() { close(started); c.close(); close(finished) }()
	<-started
	select {
	case <-finished:
		t.Fatal("DB close overtook admitted storage read")
	default:
	}
	// Complete an already-admitted backend access before allowing teardown.
	got, err := snap.backend.Get([]byte("disk"))
	if err != nil || string(got) != "basis" {
		t.Fatalf("admitted backend got=%q err=%v", got, err)
	}
	snap.endRead()
	<-finished
	if _, err := snap.Get([]byte("a")); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("post-close read err=%v", err)
	}
	it.Next()
	it.Seek([]byte("a"))
	if it.Valid() || !errors.Is(it.Error(), backenddb.ErrClosed) {
		t.Fatal("post-close iterator remained readable")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	stats := c.budget.Stats()
	if stats.TotalBytes != 0 || stats.Views != 0 {
		t.Fatalf("delayed close leaked charge: %+v", stats)
	}
}
