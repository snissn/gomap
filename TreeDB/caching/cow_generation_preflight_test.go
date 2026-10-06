package caching

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func cowFiniteGenerationFixture(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend")})
	if err != nil {
		t.Fatal(err)
	}
	limits := memtable.DefaultCOWLimits()
	limits.MaxGenerationBytes = 256 << 10
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_btree", MemtableShards: 1, COWMemtableLimits: limits, FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1 << 30, ValueLogCompression: uint8(vlogCompressionOff)})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestCOWOrdinaryOverwriteAutomaticallyRollsBoundedGenerations(t *testing.T) {
	db := cowFiniteGenerationFixture(t)
	value := bytes.Repeat([]byte("a"), 1024)
	if err := db.Set([]byte("key"), value); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	generations := 1
	previous := db.cow.writers[0]
	for i := 0; i < 160; i++ {
		value[0] = byte(i)
		if err := db.Set([]byte("key"), value); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if db.cow.writers[0] != previous {
			generations++
			previous = db.cow.writers[0]
		}
		// Give the independent flush coordinator time to retire unpinned prior
		// generations. No checkpoint or synchronous backend flush is hidden in Set.
		time.Sleep(time.Millisecond)
	}
	if generations < 3 {
		t.Fatalf("generations=%d", generations)
	}
	got, err := old.Get([]byte("key"))
	if err != nil || len(got) != 1024 || got[0] != 'a' {
		t.Fatalf("old cut=(%v,%v)", got, err)
	}
	got, err = db.Get([]byte("key"))
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("current cut=(%v,%v)", got, err)
	}
}

func TestCOWPredictedRotationRefusesBeforeAppendAndResumesAfterDrain(t *testing.T) {
	db := cowFiniteGenerationFixture(t)
	if err := db.Set([]byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	c := db.cow
	// Fill admission with real independent writer owners. The history scalar
	// only triggers rollover; the C1 authority must refuse successor admission.
	var extras []*memtable.COWWriter
	for {
		w, err := memtable.NewCOWWriter(c.budget)
		if err != nil {
			break
		}
		extras = append(extras, w)
	}
	c.writerMu.Lock()
	c.cut.shards[0].history = c.budget.Limits().MaxGenerationBytes
	before := c.cut
	c.writerMu.Unlock()
	db.externalCommandWAL = true
	b := db.NewBatchWithSize(1)
	defer b.Close()
	if err := b.Set([]byte("key"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	appended := false
	err := b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error { appended = true; return errors.New("unexpected append") })
	if !errors.Is(err, memtable.ErrCOWCapacity) || appended {
		t.Fatalf("refusal=%v appended=%v", err, appended)
	}
	if c.cut != before || c.budget.Stats().Sources != 0 {
		t.Fatal("refused rotation changed cut or froze source")
	}
	for _, w := range extras {
		r := w.Close()
		r.Drain()
	}
	db.externalCommandWAL = false
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("key")); err != nil || string(got) != "new" {
		t.Fatalf("resumed=(%q,%v)", got, err)
	}
}
