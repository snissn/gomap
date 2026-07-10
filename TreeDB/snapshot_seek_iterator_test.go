package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

func TestSnapshotIteratorSeekContract(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	entries := []struct{ key, value []byte }{
		{[]byte{}, []byte("empty")},
		{[]byte{0x00, 0xff}, []byte("binary-low")},
		{[]byte("a"), []byte("a-value")},
		{[]byte("c"), []byte("c-value")},
		{[]byte("e"), []byte("e-value")},
		{[]byte("p/0"), []byte("prefix-0")},
		{[]byte{'p', '/', 0xff}, []byte("prefix-ff")},
	}
	for _, entry := range entries {
		if err := d.Set(entry.key, entry.value); err != nil {
			t.Fatalf("Set(%x): %v", entry.key, err)
		}
	}
	if err := d.Set([]byte("deleted"), []byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("c"), []byte("c-new")); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete([]byte("deleted")); err != nil {
		t.Fatal(err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()
	if err := d.Set([]byte("b"), []byte("post-snapshot")); err != nil {
		t.Fatal(err)
	}

	forward, err := snap.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer forward.Close()
	assertSeekKey(t, forward, nil, []byte{})
	assertSeekKey(t, forward, []byte("c"), []byte("c"))
	assertSeekKey(t, forward, []byte("d"), []byte("e"))
	assertSeekKey(t, forward, []byte{0xff, 0xff}, nil)
	forward.Seek([]byte("a"))
	owned := forward.KeyCopy(nil)
	forward.Next()
	if !bytes.Equal(owned, []byte("a")) {
		t.Fatalf("KeyCopy changed after Next: %x", owned)
	}
	if forward.Valid() && bytes.Equal(forward.Key(), []byte("b")) {
		t.Fatal("snapshot exposed post-acquire write")
	}

	reverse, err := snap.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("ReverseIterator: %v", err)
	}
	defer reverse.Close()
	assertSeekKey(t, reverse, nil, []byte{'p', '/', 0xff})
	assertSeekKey(t, reverse, []byte("c"), []byte("c"))
	assertSeekKey(t, reverse, []byte("d"), []byte("c"))
	assertSeekKey(t, reverse, []byte{0x00}, []byte{})
	assertSeekKey(t, reverse, []byte{}, []byte{})

	prefix, err := snap.Iterator([]byte("p/"), []byte("p0"))
	if err != nil {
		t.Fatalf("prefix Iterator: %v", err)
	}
	defer prefix.Close()
	assertSeekKey(t, prefix, []byte("a"), []byte("p/0"))
	assertSeekKey(t, prefix, []byte("p/1"), []byte{'p', '/', 0xff})
	assertSeekKey(t, prefix, []byte("p0"), nil)
	prefixReverse, err := snap.ReverseIterator([]byte("p/"), []byte("p0"))
	if err != nil {
		t.Fatalf("prefix ReverseIterator: %v", err)
	}
	defer prefixReverse.Close()
	assertSeekKey(t, prefixReverse, []byte("z"), []byte{'p', '/', 0xff})
	assertSeekKey(t, prefixReverse, []byte("a"), nil)
	emptyReverse, err := snap.ReverseIterator([]byte("q"), []byte("r"))
	if err != nil {
		t.Fatalf("empty ReverseIterator: %v", err)
	}
	defer emptyReverse.Close()
	assertSeekKey(t, emptyReverse, nil, nil)
}

func assertSeekKey(t *testing.T, it Iterator, target, want []byte) {
	t.Helper()
	it.Seek(target)
	if want == nil {
		if it.Valid() {
			t.Fatalf("Seek(%x) key=%x want invalid", target, it.Key())
		}
		if err := it.Error(); err != nil {
			t.Fatalf("Seek(%x) error=%v", target, err)
		}
		return
	}
	if !it.Valid() {
		t.Fatalf("Seek(%x) invalid, error=%v want=%x", target, it.Error(), want)
	}
	if got := it.Key(); !bytes.Equal(got, want) {
		t.Fatalf("Seek(%x) key=%x want=%x", target, got, want)
	}
}

func TestSnapshotIteratorSeekRandomizedOracle(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	rng := rand.New(rand.NewSource(3671))
	set := make(map[string]struct{})
	for len(set) < 256 {
		key := make([]byte, 1+rng.Intn(8))
		_, _ = rng.Read(key)
		set[string(key)] = struct{}{}
	}
	keys := make([][]byte, 0, len(set))
	for key := range set {
		b := []byte(key)
		keys = append(keys, b)
		if err := d.Set(b, b); err != nil {
			t.Fatal(err)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()
	fwd, err := snap.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fwd.Close()
	rev, err := snap.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rev.Close()
	for i := 0; i < 512; i++ {
		target := make([]byte, rng.Intn(9))
		_, _ = rng.Read(target)
		fi := sort.Search(len(keys), func(j int) bool { return bytes.Compare(keys[j], target) >= 0 })
		if fi == len(keys) {
			assertSeekKey(t, fwd, target, nil)
		} else {
			assertSeekKey(t, fwd, target, keys[fi])
		}
		ri := sort.Search(len(keys), func(j int) bool { return bytes.Compare(keys[j], target) > 0 }) - 1
		if ri < 0 {
			assertSeekKey(t, rev, target, nil)
		} else {
			assertSeekKey(t, rev, target, keys[ri])
		}
	}
}

func TestSnapshotCloseInvalidatesOutstandingIterator(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 128; i++ {
		key := []byte(fmt.Sprintf("key/%04d", i))
		if err := d.Set(key, key); err != nil {
			t.Fatal(err)
		}
	}
	snap := d.AcquireSnapshot()
	it, err := snap.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	started := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(started)
		for i := 0; i < 10_000; i++ {
			it.Seek([]byte("key/0064"))
			if !it.Valid() {
				return
			}
			_ = it.Key()
			it.Next()
		}
	}()
	<-started
	if err := snap.Close(); err != nil {
		t.Fatalf("Snapshot.Close: %v", err)
	}
	wg.Wait()
	if it.Valid() {
		t.Fatal("iterator remained valid after snapshot close")
	}
	if got := it.Key(); got != nil {
		t.Fatalf("Key after snapshot close=%x want nil", got)
	}
	if err := it.Error(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Error after snapshot close=%v want ErrClosed", err)
	}
	it.Seek(nil)
	it.Next()
	if err := it.Close(); err != nil {
		t.Fatalf("Iterator.Close after snapshot close: %v", err)
	}
}

func TestSnapshotIteratorSeekAfterReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"persist/a", "persist/c", "persist/e"} {
		if err := d.SetSync([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()
	it, err := snap.Iterator([]byte("persist/"), []byte("persist0"))
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	assertSeekKey(t, it, []byte("persist/b"), []byte("persist/c"))
	rev, err := snap.ReverseIterator([]byte("persist/"), []byte("persist0"))
	if err != nil {
		t.Fatal(err)
	}
	defer rev.Close()
	assertSeekKey(t, rev, []byte("persist/d"), []byte("persist/c"))
}

func TestSnapshotIteratorEmptyDomainSeekRemainsEmpty(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, key := range []string{"a", "m", "z"} {
		if err := d.Set([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("cached"), []byte("cached")); err != nil {
		t.Fatal(err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()

	for _, bounds := range []struct {
		name       string
		start, end []byte
	}{
		{name: "equal", start: []byte("m"), end: []byte("m")},
		{name: "inverted", start: []byte("z"), end: []byte("a")},
	} {
		for _, reverse := range []bool{false, true} {
			name := bounds.name + map[bool]string{false: "/forward", true: "/reverse"}[reverse]
			t.Run(name, func(t *testing.T) {
				var it Iterator
				var err error
				if reverse {
					it, err = snap.ReverseIterator(bounds.start, bounds.end)
				} else {
					it, err = snap.Iterator(bounds.start, bounds.end)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer it.Close()
				for _, target := range [][]byte{nil, []byte("a"), []byte("m"), []byte("z"), {0xff}} {
					assertSeekKey(t, it, target, nil)
				}
			})
		}
	}
}

func TestSnapshotIteratorOpenEndedLowerBoundSeekRemainsEmpty(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.Set([]byte("a"), []byte("backend")); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("c"), []byte("cached")); err != nil {
		t.Fatal(err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()

	start := []byte("m")
	targets := []struct {
		name string
		key  []byte
	}{
		{name: "nil", key: nil},
		{name: "below", key: []byte("a")},
		{name: "inside", key: []byte("m")},
		{name: "above", key: []byte{0xff}},
	}
	for _, reverse := range []bool{false, true} {
		name := map[bool]string{false: "forward", true: "reverse"}[reverse]
		t.Run(name, func(t *testing.T) {
			var it Iterator
			var err error
			if reverse {
				it, err = snap.ReverseIterator(start, nil)
			} else {
				it, err = snap.Iterator(start, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer it.Close()
			if it.Valid() {
				t.Fatalf("iterator construction exposed key %q below start %q", it.Key(), start)
			}
			for _, target := range targets {
				t.Run(target.name, func(t *testing.T) {
					assertSeekKey(t, it, target.key, nil)
				})
			}
		})
	}
}

func TestSnapshotIteratorOpenEndedLowerBoundSeekClampsToDomain(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, key := range []string{"a", "m"} {
		if err := d.Set([]byte(key), []byte("backend/"+key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"c", "z"} {
		if err := d.Set([]byte(key), []byte("cached/"+key)); err != nil {
			t.Fatal(err)
		}
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()

	start := []byte("m")
	tests := []struct {
		name    string
		target  []byte
		forward []byte
		reverse []byte
	}{
		{name: "nil", target: nil, forward: []byte("m"), reverse: []byte("z")},
		{name: "below", target: []byte("a"), forward: []byte("m")},
		{name: "inside", target: []byte("n"), forward: []byte("z"), reverse: []byte("m")},
		{name: "above", target: []byte{0xff}, reverse: []byte("z")},
	}
	for _, reverse := range []bool{false, true} {
		name := map[bool]string{false: "forward", true: "reverse"}[reverse]
		t.Run(name, func(t *testing.T) {
			var it Iterator
			var err error
			if reverse {
				it, err = snap.ReverseIterator(start, nil)
			} else {
				it, err = snap.Iterator(start, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer it.Close()
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					want := test.forward
					if reverse {
						want = test.reverse
					}
					assertSeekKey(t, it, test.target, want)
				})
			}
		})
	}
}
