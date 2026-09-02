package caching

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	publiciterator "github.com/snissn/gomap/TreeDB/iterator"
)

func TestCachedSnapshotClosedSurfaceRejectsReadsAndInvalidatesAllIterators(t *testing.T) {
	cached, backend := newCachedSnapshotTestDB(t)
	defer func() {
		_ = cached.Close()
		_ = backend.Close()
	}()
	if err := cached.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	snap := cached.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	constructors := []struct {
		name string
		fn   func() (publiciterator.Iterator, error)
	}{
		{"iterator", func() (publiciterator.Iterator, error) { return snap.Iterator(nil, nil) }},
		{"reverse_iterator", func() (publiciterator.Iterator, error) { return snap.ReverseIterator(nil, nil) }},
	}
	iters := make([]publiciterator.Iterator, 0, len(constructors))
	for _, ctor := range constructors {
		it, err := ctor.fn()
		if err != nil {
			t.Fatalf("%s: %v", ctor.name, err)
		}
		if !it.Valid() {
			t.Fatalf("%s initially invalid: %v", ctor.name, it.Error())
		}
		iters = append(iters, it)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("Snapshot.Close: %v", err)
	}
	if snap.db == nil {
		t.Fatal("cached snapshot released pinned state while iterators remained open")
	}
	for i, it := range iters {
		if it.Valid() {
			t.Fatalf("iterator %d remained valid after Snapshot.Close", i)
		}
		if err := it.Error(); !errors.Is(err, backenddb.ErrClosed) {
			t.Fatalf("iterator %d error=%v want ErrClosed", i, err)
		}
	}

	called := false
	checks := []struct {
		name string
		fn   func() error
	}{
		{"get", func() error { _, err := snap.Get([]byte("key")); return err }},
		{"get_append", func() error {
			dst := []byte("prefix")
			out, err := snap.GetAppend([]byte("key"), dst)
			if string(out) != "prefix" {
				t.Fatalf("GetAppend mutated dst: %q", out)
			}
			return err
		}},
		{"get_versioned", func() error { _, _, err := snap.GetVersioned([]byte("key")); return err }},
		{"get_versioned_append", func() error { _, _, err := snap.GetVersionedAppend([]byte("key"), nil); return err }},
		{"get_many_empty", func() error { return snap.GetManyView(nil, nil) }},
		{"get_many", func() error {
			return snap.GetManyView([][]byte{[]byte("key")}, func(int, []byte, []byte, bool) error {
				called = true
				return nil
			})
		}},
		{"get_unsafe", func() error { _, err := snap.GetUnsafe([]byte("key")); return err }},
		{"has", func() error { _, err := snap.Has([]byte("key")); return err }},
		{"has_many_empty", func() error { _, err := snap.HasMany(nil); return err }},
		{"has_many", func() error { _, err := snap.HasMany([][]byte{[]byte("key")}); return err }},
		{"has_prefixes_empty", func() error { _, err := snap.HasPrefixes(nil); return err }},
		{"has_prefixes", func() error { _, err := snap.HasPrefixes([][]byte{[]byte("k")}); return err }},
		{"get_entry", func() error { _, err := snap.GetEntry([]byte("key")); return err }},
		{"get_entry_exact", func() error { _, err := snap.GetEntryExact([]byte("key")); return err }},
		{"iterate", func() error { return snap.Iterate(nil, nil, func([]byte, []byte) error { called = true; return nil }) }},
		{"reverse_iterate", func() error {
			return snap.ReverseIterate(nil, nil, func([]byte, []byte) error { called = true; return nil })
		}},
	}
	for _, check := range checks {
		if err := check.fn(); !errors.Is(err, backenddb.ErrClosed) {
			t.Errorf("%s error=%v want ErrClosed", check.name, err)
		}
	}
	if called {
		t.Fatal("closed cached snapshot invoked a read callback")
	}
	if snap.Pager() != nil || snap.State() != nil {
		t.Fatal("closed cached snapshot exposed Pager or State")
	}
	if _, ok := snap.StateToken(); ok {
		t.Fatal("closed cached snapshot exposed StateToken")
	}

	for i, it := range iters {
		if err := it.Close(); err != nil {
			t.Fatalf("iterator %d Close: %v", i, err)
		}
		if i+1 < len(iters) && snap.db == nil {
			t.Fatalf("cached snapshot released state with %d iterators still open", len(iters)-i-1)
		}
	}
	if snap.db != nil {
		t.Fatal("cached snapshot retained state after final iterator close")
	}
}

func TestCachedSnapshotReadCallbackCanCloseOwner(t *testing.T) {
	cached, backend := newCachedSnapshotTestDB(t)
	defer func() {
		_ = cached.Close()
		_ = backend.Close()
	}()
	if err := cached.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	snap := cached.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	if err := snap.GetManyView([][]byte{[]byte("key")}, func(int, []byte, []byte, bool) error {
		return snap.Close()
	}); err != nil {
		t.Fatalf("GetManyView: %v", err)
	}
	if snap.db != nil {
		t.Fatal("cached snapshot retained pinned state after callback read completed")
	}
	if _, err := snap.Get([]byte("key")); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("post-callback Get error=%v want ErrClosed", err)
	}
}
