package db

import (
	"bytes"
	"errors"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	publiciterator "github.com/snissn/gomap/TreeDB/iterator"
)

func TestSnapshotIteratorCreationSerializesCloseAndRelease(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.SetSync([]byte("old"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "forward", true: "reverse"}[reverse], func(t *testing.T) {
			snap := d.AcquireSnapshot()
			if snap == nil {
				t.Fatal("AcquireSnapshot=nil")
			}

			enteredCreate := make(chan struct{})
			releaseCreate := make(chan struct{})
			type iteratorResult struct {
				it  iterator.UnsafeIterator
				err error
			}
			iteratorDone := make(chan iteratorResult, 1)
			go func() {
				it, err := snap.bindNewIterator(func() iterator.UnsafeIterator {
					close(enteredCreate)
					<-releaseCreate
					return snap.buildIteratorLocked(nil, nil, IteratorOptions{}, reverse)
				})
				iteratorDone <- iteratorResult{it: it, err: err}
			}()
			<-enteredCreate

			closeStarted := make(chan struct{})
			closeDone := make(chan error, 1)
			go func() {
				close(closeStarted)
				closeDone <- snap.Close()
			}()
			<-closeStarted
			for i := 0; i < 100; i++ {
				runtime.Gosched()
			}
			if snap.closed.Load() {
				t.Fatal("Close changed snapshot generation while iterator creation held iteratorMu")
			}

			other := d.AcquireSnapshot()
			if other == nil {
				t.Fatal("second AcquireSnapshot=nil")
			}
			if other == snap {
				t.Fatal("AcquireSnapshot reused a snapshot handle during iterator registration")
			}
			if err := other.Close(); err != nil {
				t.Fatalf("second Snapshot.Close: %v", err)
			}

			close(releaseCreate)
			result := <-iteratorDone
			if result.err != nil {
				t.Fatalf("iterator creation: %v", result.err)
			}
			if err := <-closeDone; err != nil {
				t.Fatalf("Snapshot.Close: %v", err)
			}
			if result.it.Valid() {
				t.Fatal("iterator remained valid after serialized snapshot close")
			}
			if err := result.it.Error(); !errors.Is(err, ErrClosed) {
				t.Fatalf("iterator Error=%v want ErrClosed", err)
			}
			if err := result.it.Close(); err != nil {
				t.Fatalf("Iterator.Close: %v", err)
			}
		})
	}
}

func TestSnapshotIteratorCreationRejectsStaleGeneration(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	defer snap.Close()

	staleGeneration := snap.generation.Load()
	snap.iteratorMu.Lock()
	snap.generation.Add(1)
	snap.closed.Store(false)
	snap.iteratorMu.Unlock()
	createCalled := false
	it, err := snap.bindNewIteratorAtGeneration(staleGeneration, func() iterator.UnsafeIterator {
		createCalled = true
		return snap.buildIteratorLocked(nil, nil, IteratorOptions{}, false)
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("bind stale generation error=%v want ErrClosed", err)
	}
	if it != nil {
		t.Fatal("stale generation returned an iterator")
	}
	if createCalled {
		t.Fatal("stale generation accessed snapshot fields")
	}
}

func TestSnapshotIteratorCreationRejectsClosedHandleAfterNewAcquisition(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	stale := d.AcquireSnapshot()
	if stale == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	if err := stale.Close(); err != nil {
		t.Fatalf("stale Snapshot.Close: %v", err)
	}
	fresh := d.AcquireSnapshot()
	if fresh == nil {
		t.Fatal("fresh AcquireSnapshot=nil")
	}
	defer fresh.Close()
	if fresh == stale {
		t.Fatal("AcquireSnapshot reused an exported snapshot handle")
	}

	for _, reverse := range []bool{false, true} {
		var it iterator.UnsafeIterator
		if reverse {
			it, err = stale.ReverseIteratorWithOptions(nil, nil, IteratorOptions{})
		} else {
			it, err = stale.IteratorWithOptions(nil, nil, IteratorOptions{})
		}
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("reverse=%v stale iterator error=%v want ErrClosed", reverse, err)
		}
		if it != nil {
			t.Fatalf("reverse=%v stale handle returned an iterator", reverse)
		}
	}
}

func TestSnapshotIteratorDoubleCloseCannotMutateDetachedOwner(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	it, err := snap.IteratorWithOptions(nil, nil, IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bound := it.(*snapshotBoundIterator)
	keeper, err := snap.IteratorWithOptions(nil, nil, IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer keeper.Close()
	if err := snap.Close(); err != nil {
		t.Fatalf("Snapshot.Close: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Iterator.Close: %v", err)
	}
	if bound.owner != nil {
		t.Fatal("closed iterator retained snapshot owner")
	}

	generation := snap.generation.Load()
	if snap.finalized.Load() {
		t.Fatal("keeper did not pin closed snapshot owner")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("second Iterator.Close: %v", err)
	}
	if snap.generation.Load() != generation || snap.finalized.Load() {
		t.Fatal("double iterator close mutated detached snapshot owner")
	}
	if err := keeper.Close(); err != nil {
		t.Fatalf("keeper Iterator.Close: %v", err)
	}
}

func TestBackendSnapshotIteratorEmptyDomainSeekRemainsEmpty(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, key := range []string{"a", "m", "z"} {
		if err := d.SetSync([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
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
				var it iterator.UnsafeIterator
				var err error
				if reverse {
					it, err = snap.ReverseIteratorWithOptions(bounds.start, bounds.end, IteratorOptions{})
				} else {
					it, err = snap.IteratorWithOptions(bounds.start, bounds.end, IteratorOptions{})
				}
				if err != nil {
					t.Fatal(err)
				}
				defer it.Close()
				for _, target := range [][]byte{nil, []byte("a"), []byte("m"), []byte("z"), {0xff}} {
					it.Seek(target)
					if it.Valid() {
						t.Fatalf("Seek(%x) exposed key %x from empty domain", target, it.UnsafeKey())
					}
					if err := it.Error(); err != nil {
						t.Fatalf("Seek(%x) error=%v", target, err)
					}
				}
			})
		}
	}
}

func TestBackendSnapshotIteratorOpenEndedLowerBoundSeekRemainsEmpty(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, key := range []string{"a", "c"} {
		if err := d.SetSync([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
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
			var it publiciterator.Iterator
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
					it.Seek(target.key)
					if it.Valid() {
						t.Fatalf("Seek(%x) exposed key %q below start %q", target.key, it.Key(), start)
					}
					if err := it.Error(); err != nil {
						t.Fatalf("Seek(%x) error=%v", target.key, err)
					}
				})
			}
		})
	}
}

func TestBackendSnapshotIteratorOpenEndedLowerBoundSeekClampsToDomain(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, key := range []string{"a", "c", "m", "z"} {
		if err := d.SetSync([]byte(key), []byte(key)); err != nil {
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
			var it publiciterator.Iterator
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
					it.Seek(test.target)
					want := test.forward
					if reverse {
						want = test.reverse
					}
					if want == nil {
						if it.Valid() {
							t.Fatalf("Seek(%x) key=%q want invalid", test.target, it.Key())
						}
					} else if !it.Valid() || !bytes.Equal(it.Key(), want) {
						t.Fatalf("Seek(%x) valid=%v key=%q want %q", test.target, it.Valid(), it.Key(), want)
					}
					if err := it.Error(); err != nil {
						t.Fatalf("Seek(%x) error=%v", test.target, err)
					}
				})
			}
		})
	}
}
