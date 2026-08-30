package db

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestSnapshotClosedSurfaceRejectsReadsAndInvalidatesAllIterators(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	token, ok := snap.StateToken()
	if !ok || token.RootPageID == 0 {
		t.Fatalf("StateToken=%+v ok=%v", token, ok)
	}
	rootID := token.RootPageID
	reader, err := snap.ReaderAtRoot(rootID)
	if err != nil {
		t.Fatalf("ReaderAtRoot: %v", err)
	}

	constructors := []struct {
		name string
		fn   func() (iterator.UnsafeIterator, error)
	}{
		{"public_iterator", func() (iterator.UnsafeIterator, error) {
			it, err := snap.Iterator(nil, nil)
			if err != nil {
				return nil, err
			}
			return it.(iterator.UnsafeIterator), nil
		}},
		{"iterator", func() (iterator.UnsafeIterator, error) { return snap.IteratorWithOptions(nil, nil, IteratorOptions{}) }},
		{"public_reverse_iterator", func() (iterator.UnsafeIterator, error) {
			it, err := snap.ReverseIterator(nil, nil)
			if err != nil {
				return nil, err
			}
			return it.(iterator.UnsafeIterator), nil
		}},
		{"reverse_iterator", func() (iterator.UnsafeIterator, error) {
			return snap.ReverseIteratorWithOptions(nil, nil, IteratorOptions{})
		}},
		{"root_iterator", func() (iterator.UnsafeIterator, error) { return snap.IteratorAtRoot(rootID, nil, nil) }},
		{"root_iterator_options", func() (iterator.UnsafeIterator, error) {
			return snap.IteratorAtRootWithOptions(rootID, nil, nil, IteratorOptions{})
		}},
		{"root_reverse_iterator", func() (iterator.UnsafeIterator, error) { return snap.ReverseIteratorAtRoot(rootID, nil, nil) }},
		{"root_reverse_iterator_options", func() (iterator.UnsafeIterator, error) {
			return snap.ReverseIteratorAtRootWithOptions(rootID, nil, nil, IteratorOptions{})
		}},
	}
	iters := make([]iterator.UnsafeIterator, 0, len(constructors))
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
		t.Fatal("snapshot released pinned state while iterators remained open")
	}
	for i, it := range iters {
		if it.Valid() {
			t.Fatalf("iterator %d remained valid after Snapshot.Close", i)
		}
		if err := it.Error(); !errors.Is(err, ErrClosed) {
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
		{"reader_at_root", func() error { _, err := snap.ReaderAtRoot(rootID); return err }},
		{"root_get", func() error { _, err := snap.GetAtRoot(rootID, []byte("key")); return err }},
		{"root_get_append", func() error { _, err := snap.GetAppendAtRoot(rootID, []byte("key"), nil); return err }},
		{"root_get_many_empty", func() error { return snap.GetManyViewAtRoot(rootID, nil, nil) }},
		{"root_get_unsafe", func() error { _, err := snap.GetUnsafeAtRoot(rootID, []byte("key")); return err }},
		{"root_get_entry", func() error { _, err := snap.GetEntryAtRoot(rootID, []byte("key")); return err }},
		{"root_has_many_empty", func() error { _, err := snap.HasManyAtRoot(rootID, nil); return err }},
		{"root_has_any_empty", func() error { _, err := snap.HasAnySortedAtRoot(rootID, nil); return err }},
		{"root_has_prefixes_empty", func() error { _, err := snap.HasPrefixesAtRoot(rootID, nil); return err }},
		{"root_reader_get", func() error { _, err := reader.GetAppend([]byte("key"), nil); return err }},
		{"root_reader_get_many_empty", func() error { return reader.GetManyView(nil, nil) }},
	}
	for _, check := range checks {
		if err := check.fn(); !errors.Is(err, ErrClosed) {
			t.Errorf("%s error=%v want ErrClosed", check.name, err)
		}
	}
	if called {
		t.Fatal("closed snapshot invoked a read callback")
	}
	if snap.Pager() != nil || snap.State() != nil {
		t.Fatal("closed snapshot exposed Pager or State")
	}
	if _, ok := snap.StateToken(); ok {
		t.Fatal("closed snapshot exposed StateToken")
	}

	for i, it := range iters {
		if err := it.Close(); err != nil {
			t.Fatalf("iterator %d Close: %v", i, err)
		}
		if i+1 < len(iters) && snap.db == nil {
			t.Fatalf("snapshot released state with %d iterators still open", len(iters)-i-1)
		}
	}
	if snap.db != nil {
		t.Fatal("snapshot retained state after final iterator close")
	}
}

func TestSnapshotForegroundReadLifetimeFollowsBoundIterator(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	var begins atomic.Int64
	var ends atomic.Int64
	var active atomic.Int64
	unregister := d.RegisterForegroundReadObserver(func() {}, func() func() {
		begins.Add(1)
		active.Add(1)
		return func() {
			ends.Add(1)
			active.Add(-1)
		}
	})
	defer unregister()

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	snap.MarkForegroundRead()
	snap.MarkForegroundRead()
	if got := begins.Load(); got != 1 {
		t.Fatalf("foreground begins=%d want 1", got)
	}
	if got := active.Load(); got != 1 {
		t.Fatalf("active foreground reads=%d want 1", got)
	}
	it, err := snap.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("Snapshot.Close: %v", err)
	}
	if got := ends.Load(); got != 0 {
		t.Fatalf("foreground ends after Snapshot.Close=%d want 0 while iterator retains snapshot", got)
	}
	if got := active.Load(); got != 1 {
		t.Fatalf("active foreground reads after Snapshot.Close=%d want 1", got)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Iterator.Close: %v", err)
	}
	if got := ends.Load(); got != 1 {
		t.Fatalf("foreground ends after Iterator.Close=%d want 1", got)
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("active foreground reads after Iterator.Close=%d want 0", got)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("second Snapshot.Close: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("second Iterator.Close: %v", err)
	}
	if got := ends.Load(); got != 1 {
		t.Fatalf("foreground ends after repeated closes=%d want 1", got)
	}
}

func TestSnapshotForegroundReadConcurrentCloseEndsOnce(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	var ends atomic.Int64
	unregister := d.RegisterForegroundReadObserver(func() {}, func() func() {
		return func() { ends.Add(1) }
	})
	defer unregister()

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	snap.MarkForegroundRead()
	it, err := snap.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_ = snap.Close()
	}()
	go func() {
		defer wg.Done()
		<-start
		_ = it.Close()
	}()
	close(start)
	wg.Wait()

	if got := ends.Load(); got != 1 {
		t.Fatalf("foreground ends after concurrent close=%d want 1", got)
	}
}

func TestSnapshotReadCallbackCanCloseOwner(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	if err := snap.GetManyView([][]byte{[]byte("key")}, func(int, []byte, []byte, bool) error {
		return snap.Close()
	}); err != nil {
		t.Fatalf("GetManyView: %v", err)
	}
	if snap.db != nil {
		t.Fatal("snapshot retained pinned state after callback read completed")
	}
	if _, err := snap.Get([]byte("key")); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-callback Get error=%v want ErrClosed", err)
	}
}

func TestSnapshotRootReaderCallbackCanCloseOwner(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	token, ok := snap.StateToken()
	if !ok {
		t.Fatal("StateToken unavailable")
	}
	reader, err := snap.ReaderAtRoot(token.RootPageID)
	if err != nil {
		t.Fatalf("ReaderAtRoot: %v", err)
	}
	if err := reader.GetManyView([][]byte{[]byte("key")}, func(int, []byte, []byte, bool) error {
		return snap.Close()
	}); err != nil {
		t.Fatalf("SnapshotRootReader.GetManyView: %v", err)
	}
	if snap.db != nil {
		t.Fatal("snapshot retained pinned state after root-reader callback completed")
	}
	if _, err := reader.GetAppend([]byte("key"), nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-callback GetAppend error=%v want ErrClosed", err)
	}
}
