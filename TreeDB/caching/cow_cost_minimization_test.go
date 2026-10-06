package caching

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWEmptyBasisIteratorNeedsNoReadWorkspace(t *testing.T) {
	db := cowPointerFixture(t)
	db.valueLogThreshold = 64
	if err := db.Set([]byte("a"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if empty, err := old.cowCut.basis.snapshot.OwnedUserRootEmpty(); err != nil || !empty {
		t.Fatalf("empty basis=%t err=%v", empty, err)
	}
	// Leave enough for owning cached cursors, but not the former eager disk/page
	// workspace. This is real shared-budget pressure, not mocked allocation.
	before := db.cow.budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead - (32 << 10))
	if err != nil {
		t.Fatal(err)
	}
	it, err := old.Iterator(nil, nil)
	if err != nil {
		pressure.Close()
		t.Fatal(err)
	}
	inner := it.(*snapshotBoundIterator).inner.(*cowMergeIterator)
	if inner.workspace != nil {
		t.Fatal("inline iterator created read workspace")
	}
	if !it.Valid() || string(it.Key()) != "a" || string(it.Value()) != "old" {
		t.Fatalf("old entry key=%q value=%q err=%v", it.Key(), it.Value(), it.Error())
	}
	if inner.workspace != nil {
		t.Fatal("inline value created read workspace")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	pressure.Close()
	if err := db.Set([]byte("a"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("d"), []byte("disk")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	current, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	if empty, err := current.cowCut.basis.snapshot.OwnedUserRootEmpty(); err != nil || empty {
		t.Fatalf("nonempty basis=%t err=%v", empty, err)
	}
	if empty, err := old.cowCut.basis.snapshot.OwnedUserRootEmpty(); err != nil || !empty {
		t.Fatalf("old basis changed: empty=%t err=%v", empty, err)
	}
	for _, s := range []*Snapshot{old, current} {
		it, err := s.Iterator(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := "old"
		if s == current {
			want = "new"
		}
		if !it.Valid() || string(it.Key()) != "a" || string(it.Value()) != want {
			t.Fatalf("captured winner=%q err=%v", it.Value(), it.Error())
		}
		if s == current {
			it.Next()
			if !it.Valid() || string(it.Key()) != "d" || string(it.Value()) != "disk" {
				t.Fatalf("disk source missing: key=%q err=%v", it.Key(), it.Error())
			}
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// A retained nonempty backend basis also remains exact across a later
	// accepted checkpoint; skipping a current empty/dirty state cannot replace it.
	if err := db.Set([]byte("a"), []byte("later")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, err := current.Get([]byte("a")); err != nil || string(got) != "new" {
		t.Fatalf("old disk basis=%q err=%v", got, err)
	}
	if got, err := db.Get([]byte("a")); err != nil || string(got) != "later" {
		t.Fatalf("current basis=%q err=%v", got, err)
	}

}

func TestCOWCodecSlotsGrowWithinActualAdmission(t *testing.T) {
	db := cowPointerFixture(t)
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w, err := newCOWReadWorkspace(s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	if len(w.codecs) != 0 || cap(w.codecs) != 0 || w.point != nil {
		t.Fatal("new workspace eagerly allocated decoder/page backing")
	}
	limits := db.cow.budget.Limits()
	// Every newly grown backing is preadmitted. At full pressure it refuses,
	// leaving both length and capacity unchanged; closing pressure permits retry.
	before := db.cow.budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := db.cow.budget.AcquireExternal(limits.MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.addCodecSlot(); !errors.Is(err, memtable.ErrCOWCapacity) || cap(w.codecs) != 0 {
		t.Fatalf("slot refused err=%v capacity=%d", err, cap(w.codecs))
	}
	pressure.Close()
	for i := 0; i < limits.MaxResources; i++ {
		slot, err := w.addCodecSlot()
		if err != nil || slot != i || cap(w.codecs) > limits.MaxResources {
			t.Fatalf("slot=%d want=%d cap=%d err=%v", slot, i, cap(w.codecs), err)
		}
	}
	if _, err := w.addCodecSlot(); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("finite codec slots: %v", err)
	}
}

// Real configured Open covers empty shard roots alongside an exact nonempty disk
// basis. The C1 view count witnesses which cached cursors were actually acquired.
func TestCOWEmptyShardIteratorDiskTombstoneAndOldCut(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend")})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true,
		MemtableMode: "cow_btree", MemtableShards: 4, FlushThreshold: 1 << 30,
		ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionOff)})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closing.Load() {
			_ = db.Close()
		}
	})
	capture := func() *Snapshot {
		t.Helper()
		s, e := db.acquireCOWSnapshotWithError()
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	check := func(s *Snapshot, want map[string]string) {
		t.Helper()
		before := db.cow.budget.Stats()
		nonempty := 0
		for _, source := range s.rootIterator.immutables {
			if source.Len() != 0 {
				nonempty++
			}
		}
		it, e := s.Iterator(nil, nil)
		if e != nil {
			t.Fatal(e)
		}
		if got := db.cow.budget.Stats().Views - before.Views; got != nonempty {
			t.Fatalf("cached cursor pins=%d want=%d (empty roots must acquire none)", got, nonempty)
		}
		seen := map[string]string{}
		for ; it.Valid(); it.Next() {
			seen[string(it.Key())] = string(it.Value())
		}
		if e := it.Error(); e != nil {
			t.Fatal(e)
		}
		if len(seen) != len(want) {
			t.Fatalf("entries=%v want=%v", seen, want)
		}
		for key, value := range want {
			if seen[key] != value {
				t.Fatalf("entry %q=%q want=%q", key, seen[key], value)
			}
		}
		if e := it.Close(); e != nil {
			t.Fatal(e)
		}
		after := db.cow.budget.Stats()
		if after.Views != before.Views || after.ExternalBytes != before.ExternalBytes {
			t.Fatalf("cursor drain before=%+v after=%+v", before, after)
		}
	}
	empty := capture()
	check(empty, map[string]string{})
	oldValue, newValue := bytes.Repeat([]byte("old"), 128), bytes.Repeat([]byte("new"), 128)
	if err := db.Set([]byte("a"), oldValue); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("d"), []byte("disk")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	disk := capture()
	check(disk, map[string]string{"a": string(oldValue), "d": "disk"})
	if isEmpty, e := disk.cowCut.basis.snapshot.OwnedUserRootEmpty(); e != nil || isEmpty {
		t.Fatalf("disk emptiness=%t err=%v", isEmpty, e)
	}
	if err := db.Set([]byte("a"), newValue); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("d")); err != nil {
		t.Fatal(err)
	}
	current := capture()
	deleted, found := current.cowCut.shards[db.shardIndex([]byte("d"))].root.Get([]byte("d"))
	if !found || deleted.Flags&node.FlagTombstone == 0 {
		t.Fatal("fixture lacks nonempty tombstone root")
	}
	check(current, map[string]string{"a": string(newValue)})
	check(disk, map[string]string{"a": string(oldValue), "d": "disk"})
	check(empty, map[string]string{})
	before := db.cow.budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	if it, e := current.Iterator(nil, nil); !errors.Is(e, memtable.ErrCOWCapacity) || it != nil {
		pressure.Close()
		t.Fatalf("full budget iterator=%v err=%v", it, e)
	}
	if got := db.cow.budget.Stats().Views; got != before.Views {
		t.Fatalf("refusal leaked cursor pins=%d want=%d", got, before.Views)
	}
	pressure.Close()
	check(current, map[string]string{"a": string(newValue)})
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check(current, map[string]string{"a": string(newValue)})
	check(disk, map[string]string{"a": string(oldValue), "d": "disk"})
	for _, s := range []*Snapshot{empty, disk, current} {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	cache := db.cow
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	stats := cache.budget.Stats()
	if stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.Views != 0 || stats.ExternalLeases != 0 || stats.Generations != 0 || stats.PeakBytes == 0 {
		t.Fatalf("close did not drain real owners: %+v", stats)
	}
}

func TestCOWChangedSizeSingleAndDuplicateFinalRecord(t *testing.T) {
	db := cowPointerFixture(t)
	key := []byte("a")
	if err := db.Set(key, bytes.Repeat([]byte("p"), 128)); err != nil {
		t.Fatal(err)
	}
	pointer, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer pointer.Close()
	shard := db.shardIndex(key)
	old := pointer.cowCut.shards[shard]
	r, found := old.root.Get(key)
	if !found || r.Flags&node.FlagPointer == 0 || old.size != int64(len(key)+page.ValuePtrSize) {
		t.Fatalf("pointer record=%+v size=%d", r, old.size)
	}
	db.valueLogThreshold = 64
	if err := db.Set(key, []byte("tiny")); err != nil {
		t.Fatal(err)
	}
	inline, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer inline.Close()
	one := []memtable.COWMutation{{Key: key}}
	if got := cowChangedSize(old, inline.cowCut.shards[shard].root, one); got != 5 || inline.cowCut.shards[shard].size != 5 {
		t.Fatalf("pointer-to-inline size=%d", got)
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = cowChangedSize(old, inline.cowCut.shards[shard].root, one) }); allocs != 0 {
		t.Fatalf("single changed key allocated %g", allocs)
	}
	if err := db.Delete(key); err != nil {
		t.Fatal(err)
	}
	deleted, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer deleted.Close()
	if got := deleted.cowCut.shards[shard].size; got != 1 {
		t.Fatalf("inline-to-tombstone size=%d", got)
	}
	b := db.NewBatch()
	defer b.Close()
	for _, value := range []string{"first", "longer", "final"} {
		if err := b.Set(key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	final, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer final.Close()
	duplicates := []memtable.COWMutation{{Key: key}, {Key: key}, {Key: key}}
	if got := cowChangedSize(deleted.cowCut.shards[shard], final.cowCut.shards[shard].root, duplicates); got != 6 || final.cowCut.shards[shard].size != 6 {
		t.Fatalf("duplicate final-record size=%d", got)
	}
	if value, err := final.Get(key); err != nil || string(value) != "final" {
		t.Fatalf("final duplicate value=%q err=%v", value, err)
	}
	if value, err := pointer.Get(key); err != nil || !bytes.Equal(value, bytes.Repeat([]byte("p"), 128)) {
		t.Fatalf("old pointer changed=%q err=%v", value, err)
	}
}
