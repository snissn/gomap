package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func seedBatchDeleteRangeDB(t *testing.T, d *DB, n int) {
	t.Helper()
	b := d.NewBatch()
	defer b.Close()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		val := []byte(fmt.Sprintf("v%03d", i))
		if err := b.Set(key, val); err != nil {
			t.Fatalf("seed Set(%q): %v", key, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed Write: %v", err)
	}
}

func assertHasValue(t *testing.T, d *DB, key, want string) {
	t.Helper()
	got, err := d.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("Get(%s)=%q want %q", key, got, want)
	}
}

func assertMissing(t *testing.T, d *DB, key string) {
	t.Helper()
	ok, err := d.Has([]byte(key))
	if err != nil {
		t.Fatalf("Has(%s): %v", key, err)
	}
	if ok {
		t.Fatalf("Has(%s)=true, want false", key)
	}
}

func assertIteratorKVs(t *testing.T, it iterator.UnsafeIterator, want []string) {
	t.Helper()
	defer func() { _ = it.Close() }()
	var got []string
	for it.Valid() {
		got = append(got, fmt.Sprintf("%s=%s", it.Key(), it.Value()))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("iterator KVs=%v want %v", got, want)
	}
}

func TestBatchDeleteRangeMixedOrderedSemantics(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()
	seedBatchDeleteRangeDB(t, d, 8)

	snap := d.AcquireSnapshot()
	defer snap.Close()

	b := d.NewBatch()
	defer b.Close()
	if err := b.Set([]byte("k002"), []byte("shadowed")); err != nil {
		t.Fatalf("Set shadowed: %v", err)
	}
	if err := b.DeleteRange([]byte("k001"), []byte("k006")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := b.Set([]byte("k003"), []byte("after-range")); err != nil {
		t.Fatalf("Set after range: %v", err)
	}
	if err := b.Delete([]byte("k007")); err != nil {
		t.Fatalf("Delete point: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Snapshot acquired before the batch sees the old state.
	if got, err := snap.Get([]byte("k002")); err != nil || !bytes.Equal(got, []byte("v002")) {
		t.Fatalf("snapshot k002=(%q,%v), want old v002", got, err)
	}

	assertHasValue(t, d, "k000", "v000")
	assertMissing(t, d, "k001")
	assertMissing(t, d, "k002")
	assertHasValue(t, d, "k003", "after-range")
	assertMissing(t, d, "k004")
	assertMissing(t, d, "k005")
	assertHasValue(t, d, "k006", "v006")
	assertMissing(t, d, "k007")
}

func TestBatchDeleteRangeIteratorVisibilityMixedBatch(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()
	seedBatchDeleteRangeDB(t, d, 6)

	snap := d.AcquireSnapshot()
	defer snap.Close()

	b := d.NewBatch()
	defer b.Close()
	if err := b.Set([]byte("k002"), []byte("shadowed")); err != nil {
		t.Fatalf("Set shadowed: %v", err)
	}
	if err := b.DeleteRange([]byte("k001"), []byte("k005")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := b.Set([]byte("k003"), []byte("after")); err != nil {
		t.Fatalf("Set after: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	it, err := d.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	assertIteratorKVs(t, it, []string{"k000=v000", "k003=after", "k005=v005"})

	snapIt, err := snap.IteratorWithOptions(nil, nil, IteratorOptions{})
	if err != nil {
		t.Fatalf("snapshot Iterator: %v", err)
	}
	assertIteratorKVs(t, snapIt, []string{"k000=v000", "k001=v001", "k002=v002", "k003=v003", "k004=v004", "k005=v005"})
}

func TestBatchDeleteRangeValueLogPointerRefsAndGC(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	valueDrop := bytes.Repeat([]byte("drop-one|"), 32)
	valueOldReplace := bytes.Repeat([]byte("old-replace|"), 32)
	valueKeep := bytes.Repeat([]byte("keep|"), 32)
	valueNewReplace := bytes.Repeat([]byte("new-replace|"), 32)
	fileDrop, ptrDrop := writeValueLogRecord(t, dir, 0, 1, valueDrop, 1)
	fileOldReplace, ptrOldReplace := writeValueLogRecord(t, dir, 0, 2, valueOldReplace, 2)
	fileKeep, ptrKeep := writeValueLogRecord(t, dir, 0, 3, valueKeep, 3)
	fileNewReplace, ptrNewReplace := writeValueLogRecord(t, dir, 0, 4, valueNewReplace, 4)

	seed := d.NewBatch().(*Batch)
	if err := seed.SetPointer([]byte("k1"), ptrDrop); err != nil {
		t.Fatalf("seed k1: %v", err)
	}
	if err := seed.SetPointer([]byte("k2"), ptrOldReplace); err != nil {
		t.Fatalf("seed k2: %v", err)
	}
	if err := seed.SetPointer([]byte("k3"), ptrKeep); err != nil {
		t.Fatalf("seed k3: %v", err)
	}
	if err := seed.Write(); err != nil {
		t.Fatalf("seed Write: %v", err)
	}
	_ = seed.Close()

	b := d.NewBatch().(*Batch)
	if err := b.DeleteRange([]byte("k1"), []byte("k3")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrNewReplace); err != nil {
		t.Fatalf("SetPointer replacement: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = b.Close()

	assertMissing(t, d, "k1")
	if got, err := d.Get([]byte("k2")); err != nil || !bytes.Equal(got, valueNewReplace) {
		t.Fatalf("Get(k2)=(%d bytes,%v), want replacement", len(got), err)
	}
	if got, err := d.Get([]byte("k3")); err != nil || !bytes.Equal(got, valueKeep) {
		t.Fatalf("Get(k3)=(%d bytes,%v), want keep", len(got), err)
	}

	// The prior durable slot still names the seed generation, so recoverable-root
	// projection must classify the deleted pointer segments as referenced until
	// normal slot alternation replaces that generation.
	pinned, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("ValueLogGC dry run: %v", err)
	}
	if pinned.SegmentsReferenced < 2 || pinned.SegmentsEligible != 0 || pinned.SegmentsPending != 0 {
		t.Fatalf("ValueLogGC stats while older durable slot is recoverable=%+v, want at least 2 referenced and no eligible/pending segments", pinned)
	}
	if err := d.SetSync([]byte("slot-advance"), []byte("inline")); err != nil {
		t.Fatalf("advance durable slot: %v", err)
	}

	stats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted < 2 {
		t.Fatalf("ValueLogGC deleted %d segments, want at least 2: %+v", stats.SegmentsDeleted, stats)
	}

	pathForFile := func(seq uint32) string {
		return filepath.Join(dir, "value_vlog", fmt.Sprintf("value-l0-%06d.log", seq))
	}
	for _, tc := range []struct {
		name   string
		fileID uint32
		seq    uint32
		dead   bool
	}{
		{"range-deleted", fileDrop, 1, true},
		{"range-deleted-before-replace", fileOldReplace, 2, true},
		{"retained-outside-range", fileKeep, 3, false},
		{"replacement-after-range", fileNewReplace, 4, false},
	} {
		_, err := os.Stat(pathForFile(tc.seq))
		if tc.dead {
			if !os.IsNotExist(err) {
				t.Fatalf("%s segment %d stat err=%v, want removed", tc.name, tc.fileID, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s segment %d stat err=%v, want retained", tc.name, tc.fileID, err)
		}
	}
}

func TestBatchDeleteRangeNilEndReopenCommandWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	seedBatchDeleteRangeDB(t, d, 5)
	b := d.NewBatch()
	if err := b.DeleteRange([]byte("k002"), nil); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := b.Set([]byte("k003"), []byte("later")); err != nil {
		t.Fatalf("Set later: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	_ = b.Close()
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopen.Close()
	assertHasValue(t, reopen, "k000", "v000")
	assertHasValue(t, reopen, "k001", "v001")
	assertMissing(t, reopen, "k002")
	assertHasValue(t, reopen, "k003", "later")
	assertMissing(t, reopen, "k004")
}

func TestCommandWALRawDeleteRangeReplayFrame(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open bootstrap: %v", err)
	}
	seedBatchDeleteRangeDB(t, d, 5)
	if err := d.Close(); err != nil {
		t.Fatalf("Close bootstrap: %v", err)
	}
	enableCommandWALFormat(t, dir)
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpDeleteRange, Key: []byte("k001"), Value: []byte("k004")},
		{Op: commitlog.RawKVOpSet, Key: []byte("k002"), Value: []byte("after")},
	})
	reopen, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open replay: %v", err)
	}
	defer reopen.Close()
	assertHasValue(t, reopen, "k000", "v000")
	assertMissing(t, reopen, "k001")
	assertHasValue(t, reopen, "k002", "after")
	assertMissing(t, reopen, "k003")
	assertHasValue(t, reopen, "k004", "v004")
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d want 1", got)
	}
}
