package treedb

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func requireRawKVMissing(t *testing.T, db *DB, key []byte) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if got != nil {
		t.Fatalf("Get(%q)=%q want missing", key, got)
	}
	has, err := db.Has(key)
	if err != nil {
		t.Fatalf("Has(%q): %v", key, err)
	}
	if has {
		t.Fatalf("Has(%q)=true, want false", key)
	}
}

func requireRawKVValue(t *testing.T, db *DB, key []byte, want []byte) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q)=%q want %q", key, got, want)
	}
	if len(want) == 0 && got == nil {
		t.Fatalf("Get(%q) returned nil for present zero-length value", key)
	}
	has, err := db.Has(key)
	if err != nil {
		t.Fatalf("Has(%q): %v", key, err)
	}
	if !has {
		t.Fatalf("Has(%q)=false, want true", key)
	}

	many, err := db.GetMany([][]byte{key})
	if err != nil {
		t.Fatalf("GetMany(%q): %v", key, err)
	}
	if len(many) != 1 || !bytes.Equal(many[0], want) || (len(want) == 0 && many[0] == nil) {
		t.Fatalf("GetMany(%q)=%#v want one non-nil value %q", key, many, want)
	}
	hasMany, err := db.HasMany([][]byte{key})
	if err != nil {
		t.Fatalf("HasMany(%q): %v", key, err)
	}
	if len(hasMany) != 1 || !hasMany[0] {
		t.Fatalf("HasMany(%q)=%v want [true]", key, hasMany)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	snapGot, err := snap.Get(key)
	if err != nil {
		t.Fatalf("snapshot Get(%q): %v", key, err)
	}
	if !bytes.Equal(snapGot, want) || (len(want) == 0 && snapGot == nil) {
		t.Fatalf("snapshot Get(%q)=%#v want non-nil value %q", key, snapGot, want)
	}
	snapHas, err := snap.Has(key)
	if err != nil {
		t.Fatalf("snapshot Has(%q): %v", key, err)
	}
	if !snapHas {
		t.Fatalf("snapshot Has(%q)=false, want true", key)
	}
}

func TestRawKVParityEmptyPointKeyNilValueCached(t *testing.T) {
	db, err := Open(OptionsFor(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte{}, nil); err != nil {
		t.Fatalf("Set(empty,nil): %v", err)
	}
	requireRawKVValue(t, db, []byte{}, []byte{})
	requireRawKVValue(t, db, nil, []byte{})

	if err := db.Set(nil, []byte("nil-key-value")); err != nil {
		t.Fatalf("Set(nil,value): %v", err)
	}
	requireRawKVValue(t, db, []byte{}, []byte("nil-key-value"))
	requireRawKVValue(t, db, nil, []byte("nil-key-value"))

	if err := db.Set([]byte("nil-value"), nil); err != nil {
		t.Fatalf("Set(key,nil): %v", err)
	}
	requireRawKVValue(t, db, []byte("nil-value"), []byte{})

	if err := db.Update(nil, func(old []byte) (UpdateResult, error) {
		if !bytes.Equal(old, []byte("nil-key-value")) {
			t.Fatalf("Update(nil) old=%q want nil-key-value", old)
		}
		return SetUpdate(nil), nil
	}); err != nil {
		t.Fatalf("Update(nil -> nil value): %v", err)
	}
	requireRawKVValue(t, db, []byte{}, []byte{})

	if err := db.UpdateSync([]byte{}, func(old []byte) (UpdateResult, error) {
		if old == nil || len(old) != 0 {
			t.Fatalf("UpdateSync(empty) old=%#v want non-nil zero-length", old)
		}
		return SetUpdate([]byte("updated-empty")), nil
	}); err != nil {
		t.Fatalf("UpdateSync(empty): %v", err)
	}
	requireRawKVValue(t, db, nil, []byte("updated-empty"))

	if err := db.Delete(nil); err != nil {
		t.Fatalf("Delete(nil): %v", err)
	}
	if has, err := db.Has([]byte{}); err != nil {
		t.Fatalf("Has(empty after delete): %v", err)
	} else if has {
		t.Fatal("empty key still present after Delete(nil)")
	}
	if err := db.Delete([]byte{}); err != nil {
		t.Fatalf("Delete(empty missing): %v", err)
	}
}

func TestRawKVParityUpdateEmptyKeyNilValueCached(t *testing.T) {
	db, err := Open(OptionsFor(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Update(nil, func(old []byte) (UpdateResult, error) {
		if old != nil {
			t.Fatalf("initial old=%#v want nil missing", old)
		}
		return SetUpdate(nil), nil
	}); err != nil {
		t.Fatalf("Update(nil -> nil value): %v", err)
	}
	requireRawKVValue(t, db, []byte{}, []byte{})

	if err := db.UpdateSync([]byte{}, func(old []byte) (UpdateResult, error) {
		if old == nil || len(old) != 0 {
			t.Fatalf("old=%#v want non-nil empty", old)
		}
		return SetUpdate([]byte("updated")), nil
	}); err != nil {
		t.Fatalf("UpdateSync(empty): %v", err)
	}
	requireRawKVValue(t, db, nil, []byte("updated"))

	if err := db.Update([]byte{}, func(old []byte) (UpdateResult, error) {
		if !bytes.Equal(old, []byte("updated")) {
			t.Fatalf("old=%q want updated", old)
		}
		return DeleteUpdate(), nil
	}); err != nil {
		t.Fatalf("Update(empty delete): %v", err)
	}
	if has, err := db.Has(nil); err != nil {
		t.Fatalf("Has(nil after Update delete): %v", err)
	} else if has {
		t.Fatal("empty key still present after Update delete")
	}
}

func TestRawKVParityIteratorRangeEmptyKeyCached(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, kv := range []struct{ key, value []byte }{
		{[]byte{}, []byte("empty")},
		{[]byte("a"), []byte("A")},
		{[]byte("b"), []byte("B")},
	} {
		if err := db.Set(kv.key, kv.value); err != nil {
			t.Fatalf("Set(%q): %v", kv.key, err)
		}
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator(nil,nil): %v", err)
	}
	if !it.Valid() {
		t.Fatal("Iterator(nil,nil) invalid, want empty key first")
	}
	if got := it.Key(); len(got) != 0 {
		t.Fatalf("first key=%q want empty", got)
	}
	_ = it.Close()

	emptyEnd, err := db.Iterator(nil, []byte{})
	if err != nil {
		t.Fatalf("Iterator(nil,empty): %v", err)
	}
	if emptyEnd.Valid() {
		t.Fatalf("Iterator(nil,empty) valid at key %q, want empty range", emptyEnd.Key())
	}
	_ = emptyEnd.Close()

	rev, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("ReverseIterator(nil,nil): %v", err)
	}
	for rev.Valid() && string(rev.Key()) != "a" && string(rev.Key()) != "" {
		rev.Next()
	}
	if !rev.Valid() {
		t.Fatal("ReverseIterator(nil,nil) did not include empty key")
	}
	for rev.Valid() && len(rev.Key()) != 0 {
		rev.Next()
	}
	if !rev.Valid() || len(rev.Key()) != 0 {
		t.Fatal("ReverseIterator(nil,nil) did not reach empty key as lowest key")
	}
	_ = rev.Close()

	if err := db.DeleteRange(nil, []byte{}); err != nil {
		t.Fatalf("DeleteRange(nil,empty): %v", err)
	}
	requireRawKVValue(t, db, []byte{}, []byte("empty"))

	if err := db.DeleteRange([]byte{}, []byte("a")); err != nil {
		t.Fatalf("DeleteRange(empty,a): %v", err)
	}
	if has, err := db.Has([]byte{}); err != nil {
		t.Fatalf("Has(empty after DeleteRange): %v", err)
	} else if has {
		t.Fatal("empty key still present after DeleteRange(empty,a)")
	}
	requireRawKVValue(t, db, []byte("a"), []byte("A"))
}

func TestReopenEmptyKeyNilValueValueLogPointer(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	opts.ValueLog.PointerThreshold = 1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Set(nil, []byte("pointer-value")); err != nil {
		_ = db.Close()
		t.Fatalf("Set(nil,pointer): %v", err)
	}
	if err := db.Set([]byte("zero"), nil); err != nil {
		_ = db.Close()
		t.Fatalf("Set(zero,nil): %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	requireRawKVValue(t, reopen, []byte{}, []byte("pointer-value"))
	requireRawKVValue(t, reopen, nil, []byte("pointer-value"))
	requireRawKVValue(t, reopen, []byte("zero"), []byte{})
}

func TestCommandWALRawKVDeleteRangeSpanReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"z", "vz"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			_ = db.Close()
			t.Fatalf("Set(%s): %v", kv.k, err)
		}
	}
	if err := db.DeleteRange([]byte("a"), []byte("c")); err != nil {
		_ = db.Close()
		t.Fatalf("DeleteRange: %v", err)
	}
	requireRawKVMissing(t, db, []byte("a"))
	requireRawKVMissing(t, db, []byte("b"))
	requireRawKVValue(t, db, []byte("z"), []byte("vz"))
	stats := db.Stats()
	if got := stats["treedb.cache.delete_range.materialized_keys_total"]; got != "0" {
		_ = db.Close()
		t.Fatalf("materialized_keys_total=%s want 0", got)
	}
	if got := stats["treedb.cache.range_span.active_spans"]; got != "1" {
		_ = db.Close()
		t.Fatalf("active_spans=%s want 1", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true})
	if err != nil {
		t.Fatalf("Reopen command WAL: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	requireRawKVMissing(t, reopen, []byte("a"))
	requireRawKVMissing(t, reopen, []byte("b"))
	requireRawKVValue(t, reopen, []byte("z"), []byte("vz"))
}

func TestCommandWALRawKVParityEmptyKeyNilValueReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}

	var sawPoint bool
	prevHook := testAfterPublicCommandWALPointAppend
	testAfterPublicCommandWALPointAppend = func(op commitlog.RawKVOperation) {
		if op.Op == commitlog.RawKVOpSet && len(op.Key) == 0 && op.Key != nil && len(op.Value) == 0 && op.Value != nil {
			sawPoint = true
		}
	}
	defer func() { testAfterPublicCommandWALPointAppend = prevHook }()

	if err := db.Set(nil, nil); err != nil {
		_ = db.Close()
		t.Fatalf("command WAL Set(nil,nil): %v", err)
	}
	if !sawPoint {
		_ = db.Close()
		t.Fatal("command WAL point hook did not observe non-nil empty key/value payload")
	}

	b := db.NewBatch()
	if err := b.Set([]byte{}, []byte("batch-empty")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set(empty): %v", err)
	}
	if err := b.Set([]byte("zero"), nil); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set(zero,nil): %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	assertPublicCommandWALFrames(t, db, 2)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true})
	if err != nil {
		t.Fatalf("Reopen command WAL: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	requireRawKVValue(t, reopen, []byte{}, []byte("batch-empty"))
	requireRawKVValue(t, reopen, nil, []byte("batch-empty"))
	requireRawKVValue(t, reopen, []byte("zero"), []byte{})
}
