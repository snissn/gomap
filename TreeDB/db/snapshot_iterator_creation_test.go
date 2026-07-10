package db

import (
	"errors"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestSnapshotIteratorCreationSerializesCloseAndPoolReuse(t *testing.T) {
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
				t.Fatal("snapshot was returned to pool before iterator registration completed")
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
