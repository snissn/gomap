package caching

import (
	"errors"
	"runtime"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	publiciterator "github.com/snissn/gomap/TreeDB/iterator"
)

func TestCachedSnapshotIteratorCreationSerializesCloseAndRelease(t *testing.T) {
	cached, backend := newCachedSnapshotTestDB(t)
	defer func() {
		_ = cached.Close()
		_ = backend.Close()
	}()
	if err := cached.Set([]byte("old"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "forward", true: "reverse"}[reverse], func(t *testing.T) {
			snap := cached.AcquireSnapshot()
			if snap == nil {
				t.Fatal("AcquireSnapshot=nil")
			}

			enteredCreate := make(chan struct{})
			releaseCreate := make(chan struct{})
			type iteratorResult struct {
				it  merging.Iterator
				err error
			}
			iteratorDone := make(chan iteratorResult, 1)
			go func() {
				it, err := snap.bindNewIterator(func() (merging.Iterator, error) {
					close(enteredCreate)
					<-releaseCreate
					return snap.buildIteratorLocked(nil, nil, reverse)
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
				t.Fatal("Close changed cached snapshot generation while iterator creation held iteratorMu")
			}

			other := cached.AcquireSnapshot()
			if other == nil {
				t.Fatal("second AcquireSnapshot=nil")
			}
			if other == snap {
				t.Fatal("AcquireSnapshot reused a cached snapshot handle during iterator registration")
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
			if err := result.it.Error(); !errors.Is(err, backenddb.ErrClosed) {
				t.Fatalf("iterator Error=%v want ErrClosed", err)
			}
			if err := result.it.Close(); err != nil {
				t.Fatalf("Iterator.Close: %v", err)
			}
		})
	}
}

func TestCachedSnapshotIteratorCreationRejectsStaleGeneration(t *testing.T) {
	cached, backend := newCachedSnapshotTestDB(t)
	defer func() {
		_ = cached.Close()
		_ = backend.Close()
	}()
	if err := cached.Set([]byte("old"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	snap := cached.AcquireSnapshot()
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
	it, err := snap.bindNewIteratorAtGeneration(staleGeneration, func() (merging.Iterator, error) {
		createCalled = true
		return snap.buildIteratorLocked(nil, nil, false)
	})
	if !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("bind stale generation error=%v want ErrClosed", err)
	}
	if it != nil {
		t.Fatal("stale generation returned an iterator")
	}
	if createCalled {
		t.Fatal("stale generation accessed cached snapshot fields")
	}
}

func TestCachedSnapshotIteratorCreationRejectsClosedHandleAfterNewAcquisition(t *testing.T) {
	cached, backend := newCachedSnapshotTestDB(t)
	defer func() {
		_ = cached.Close()
		_ = backend.Close()
	}()
	if err := cached.Set([]byte("old"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	stale := cached.AcquireSnapshot()
	if stale == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	if err := stale.Close(); err != nil {
		t.Fatalf("stale Snapshot.Close: %v", err)
	}
	fresh := cached.AcquireSnapshot()
	if fresh == nil {
		t.Fatal("fresh AcquireSnapshot=nil")
	}
	defer fresh.Close()
	if fresh == stale {
		t.Fatal("AcquireSnapshot reused an exported cached snapshot handle")
	}

	for _, reverse := range []bool{false, true} {
		var it publiciterator.Iterator
		var err error
		if reverse {
			it, err = stale.ReverseIterator(nil, nil)
		} else {
			it, err = stale.Iterator(nil, nil)
		}
		if !errors.Is(err, backenddb.ErrClosed) {
			t.Fatalf("reverse=%v stale iterator error=%v want ErrClosed", reverse, err)
		}
		if it != nil {
			t.Fatalf("reverse=%v stale handle returned an iterator", reverse)
		}
	}
}

func TestCachedSnapshotIteratorDoubleCloseCannotMutateDetachedOwner(t *testing.T) {
	cached, backend := newCachedSnapshotTestDB(t)
	defer func() {
		_ = cached.Close()
		_ = backend.Close()
	}()

	snap := cached.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot=nil")
	}
	it, err := snap.bindNewIterator(func() (merging.Iterator, error) {
		return snap.buildIteratorLocked(nil, nil, false)
	})
	if err != nil {
		t.Fatal(err)
	}
	bound := it.(*snapshotBoundIterator)
	keeper, err := snap.bindNewIterator(func() (merging.Iterator, error) {
		return snap.buildIteratorLocked(nil, nil, false)
	})
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
		t.Fatal("closed iterator retained cached snapshot owner")
	}

	generation := snap.generation.Load()
	if snap.finalized.Load() {
		t.Fatal("keeper did not pin closed cached snapshot owner")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("second Iterator.Close: %v", err)
	}
	if snap.generation.Load() != generation || snap.finalized.Load() {
		t.Fatal("double iterator close mutated detached cached snapshot owner")
	}
	if err := keeper.Close(); err != nil {
		t.Fatalf("keeper Iterator.Close: %v", err)
	}
}
