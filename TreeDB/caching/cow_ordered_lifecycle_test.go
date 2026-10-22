package caching

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Use the production owner, including value-log custody and the existing
// cleanup worker. Selecting the mode alone is insufficient evidence that the
// ordered implementation, rather than the compatibility tree, is active.
func cowOrderedLifecycleFixture(t *testing.T) (*DB, *mvccadmission.Capability) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend")})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered",
		FlushThreshold: 1 << 30, MemtableShards: 2,
		ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closing.Load() {
			_ = db.Close()
		}
	})
	if db.cow == nil || !db.SupportsLogicalMVCC() {
		t.Fatal("cow_ordered did not activate the ordered logical engine")
	}
	owner := backend.NewMVCCAdmission()
	if owner == nil {
		t.Fatal("backend did not issue the original Store admission owner")
	}
	t.Cleanup(owner.Close)
	return db, owner
}

func cowOrderedLifecycleKey(t *testing.T, logical string, timestamp uint64) []byte {
	t.Helper()
	key, err := mvcckey.Encode([]byte(logical), timestamp)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func cowOrderedLifecycleSet(t *testing.T, db *DB, owner *mvccadmission.Capability, key, value []byte) {
	t.Helper()
	_, timestamp, err := mvcckey.Decode(key)
	if err != nil {
		t.Fatal(err)
	}
	// Physical key shape is deliberately insufficient to select the ordered
	// history. Carry the same live Store input through its real producer cut.
	batch := db.NewBatchWithSize(1)
	if err := batch.SetWithMVCCInput(key, value, owner.Input(key, timestamp, 0)); err != nil {
		_ = batch.Close()
		t.Fatal(err)
	}
	err = batch.Write()
	if err = errors.Join(err, batch.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestCOWOrderedSnapshotCloseDefersCutUntilReadAndIteratorFinish(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	key := cowOrderedLifecycleKey(t, "retained", 100)
	want := bytes.Repeat([]byte("retained-value"), 1024)
	cowOrderedLifecycleSet(t, db, owner, key, want)
	snapshot, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	iterator, err := snapshot.Iterator(nil, nil)
	if err != nil {
		_ = snapshot.Close()
		t.Fatal(err)
	}
	if !iterator.Valid() || !bytes.Equal(iterator.Key(), key) || !bytes.Equal(iterator.Value(), want) {
		t.Fatal("opening iterator did not resolve the retained value-log pointer")
	}
	if err := snapshot.beginRead(); err != nil {
		_ = iterator.Close()
		_ = snapshot.Close()
		t.Fatal(err)
	}
	reading := true
	t.Cleanup(func() {
		if reading {
			_ = snapshot.Close()
			_ = iterator.Close()
			snapshot.endRead()
		}
	})
	// Keep identities before finalization: snapshot wrappers are pooled after
	// their final read/iterator, and must not be inspected after that point.
	cut := snapshot.cowCut
	root := cut.shards[db.shardIndex(key)].root
	borrowed, found := root.Get(key)
	if !found {
		t.Fatal("opening physical version missing")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if db.cow.cut == cut || db.cow.cut.shards[db.shardIndex(key)].root == root {
		t.Fatal("checkpoint did not retire the captured mutable source")
	}
	if !db.SupportsLogicalMVCC() {
		t.Fatal("checkpoint successor lost the ordered engine")
	}
	viewsBeforeClose := db.cow.budget.Stats().Views
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot.finalized.Load() || cut.shards == nil || viewsBeforeClose == 0 || db.cow.budget.Stats().Views != viewsBeforeClose {
		t.Fatalf("Close released a cut with an admitted read and iterator: finalized=%t cutDrained=%t views=%d want=%d", snapshot.finalized.Load(), cut.shards == nil, db.cow.budget.Stats().Views, viewsBeforeClose)
	}
	if iterator.Valid() || !errors.Is(iterator.Error(), backenddb.ErrClosed) {
		t.Fatal("closed snapshot iterator remained readable")
	}
	if err := iterator.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot.finalized.Load() || cut.shards == nil || db.cow.budget.Stats().Views != 1 {
		t.Fatal("iterator cleanup overtook the admitted read")
	}
	// These aliases belong to the still-admitted source, not a projection
	// scratch buffer that checkpoint or another cursor may overwrite.
	again, found := root.Get(key)
	if !found || again.Key != borrowed.Key || again.Ptr != borrowed.Ptr || again.Revision != borrowed.Revision {
		t.Fatal("retired physical key or value-pointer identity changed")
	}
	snapshot.endRead()
	reading = false
	if cut.shards != nil || db.cow.budget.Stats().Views != 0 {
		t.Fatal("final admitted read did not release its captured cut")
	}
	if got, err := db.Get(key); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("durable successor value=(%q,%v)", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := db.cow.budget.Stats(); stats.TotalBytes != 0 || stats.Views != 0 || stats.Sources != 0 {
		t.Fatalf("last owner left admission charged: %+v", stats)
	}
}

func TestCOWOrderedAcceptedErrorPreservesVersionPrefixAndNewTail(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	backend := &cowAcceptedReportBackend{DB: db.backend.(*backenddb.DB)}
	db.backend = backend
	db.flushBackendMaxEntries = 1
	k50 := cowOrderedLifecycleKey(t, "shared", 50)
	k100 := cowOrderedLifecycleKey(t, "shared", 100)
	other := cowOrderedLifecycleKey(t, "other", 70)
	oldValue := bytes.Repeat([]byte("old"), 4096)
	wants := map[string][]byte{string(k50): []byte("history"), string(k100): oldValue, string(other): []byte("other")}
	for _, key := range [][]byte{k50, k100, other} {
		cowOrderedLifecycleSet(t, db, owner, key, wants[string(key)])
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	opening := old.cowCut.basis
	backend.inject.Store(true)
	if err := db.Checkpoint(); !errors.Is(err, errCOWAcceptedReport) {
		t.Fatalf("expected real accepted report error, got %v", err)
	}
	db.flushMu.Lock()
	handoff := db.cow.handoff
	if handoff == nil || !handoff.accepted || handoff.cut.basis != opening || len(handoff.cut.frozen) == 0 {
		db.flushMu.Unlock()
		t.Fatal("accepted version prefix lost its opening basis or custody")
	}
	for i, table := range handoff.cut.frozen {
		if db.cow.cut.frozen[i].root != table.root {
			db.flushMu.Unlock()
			t.Fatal("accepted source was removed before exact basis handoff")
		}
	}
	acceptedSequence := backend.State().CommitSeq
	db.flushMu.Unlock()
	if backend.publications.Load() != 1 {
		t.Fatal("physical version chunks did not publish as one accepted group")
	}
	for _, key := range [][]byte{k50, k100, other} {
		if got, err := backend.Get(key); err != nil || !bytes.Equal(got, wants[string(key)]) {
			t.Fatalf("accepted physical version %x=(%q,%v)", key, got, err)
		}
	}
	k150 := cowOrderedLifecycleKey(t, "shared", 150)
	late := cowOrderedLifecycleKey(t, "late", 1)
	newValue := bytes.Repeat([]byte("new"), 4096)
	for _, entry := range []struct{ key, value []byte }{{k100, newValue}, {k150, []byte("future version")}, {late, []byte("late tail")}} {
		cowOrderedLifecycleSet(t, db, owner, entry.key, entry.value)
	}
	if !db.SupportsLogicalMVCC() {
		t.Fatal("accepted-error rollover lost the ordered engine")
	}
	backend.refuseCapture.Store(false)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if backend.publications.Load() != 2 || backend.State().CommitSeq != acceptedSequence+1 {
		t.Fatal("retry replayed the accepted version prefix")
	}
	if db.cow.handoff != nil || len(db.cow.cut.frozen) != 0 {
		t.Fatal("completed handoff retained a covered source")
	}
	for _, key := range [][]byte{k50, k100, other} {
		if got, err := old.Get(key); err != nil || !bytes.Equal(got, wants[string(key)]) {
			t.Fatalf("retained old version %x=(%q,%v)", key, got, err)
		}
	}
	for _, key := range [][]byte{k150, late} {
		if got, err := old.Get(key); !errors.Is(err, tree.ErrKeyNotFound) || got != nil {
			t.Fatalf("old cut acquired tail version %x=(%q,%v)", key, got, err)
		}
	}
	wants[string(k100)], wants[string(k150)], wants[string(late)] = newValue, []byte("future version"), []byte("late tail")
	for key, want := range wants {
		if got, err := db.Get([]byte(key)); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("new physical version %x=(%q,%v)", key, got, err)
		}
	}
}
