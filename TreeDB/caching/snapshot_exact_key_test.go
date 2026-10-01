package caching

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestSnapshotExactVersionSourcesFallbackAndPrecedence(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	cached, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true, FlushThreshold: 1 << 30, MemtableShards: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()
	key, err := mvcckey.Encode([]byte("target"), 9)
	if err != nil {
		t.Fatal(err)
	}
	var other []byte
	for i := 0; ; i++ {
		other, err = mvcckey.Encode([]byte(fmt.Sprint(i)), 9)
		if err != nil {
			t.Fatal(err)
		}
		if cached.shardIndex(other) != cached.shardIndex(key) {
			break
		}
	}
	for _, entry := range []struct{ key, value []byte }{{key, []byte("old")}, {other, []byte("unrelated")}} {
		if err := cached.Set(entry.key, entry.value); err != nil {
			t.Fatal(err)
		}
	}
	first := cached.AcquireSnapshot()
	if first == nil {
		t.Fatal("first snapshot unavailable")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cached.Set(key, []byte("new")); err != nil {
		t.Fatal(err)
	}
	snap := cached.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot unavailable")
	}
	defer snap.Close()
	lower, _ := mvcckey.AppendKeyVersionsLower(nil, []byte("target"))
	upper, _ := mvcckey.AppendKeyVersionsUpper(nil, []byte("target"))
	genericCount := len(snap.rootIterator.immutables) + 1
	relevantCount := len(snap.rootPointShards[cached.shardIndex(key)].immutables) + 1
	if relevantCount >= genericCount {
		t.Fatal("fixture failed to retain an unrelated shard")
	}
	for _, reverse := range []bool{false, true} {
		for _, mode := range []string{"eligible", "missing point roots", "missing queue IDs", "global upgrade", "range spans"} {
			t.Run(fmt.Sprintf("%t/%s", reverse, mode), func(t *testing.T) {
				view := &memtableView{queue: snap.view.queue, queueShardIDs: snap.view.queueShardIDs, queueRangeSpans: snap.view.queueRangeSpans}
				// Borrow immutable state under the real snapshot's lease. Do not copy its
				// atomics or close this synthetic handle, which owns no independent lease.
				candidate := &Snapshot{db: snap.db, view: view, backend: snap.backend, backendRootID: snap.backendRootID, backendFallback: snap.backendFallback, rootPointShards: snap.rootPointShards, rootIterator: snap.rootIterator, publishedRoots: snap.publishedRoots}
				wantCount := genericCount
				switch mode {
				case "eligible":
					wantCount = relevantCount
				case "missing point roots":
					candidate.rootPointShards = nil
				case "missing queue IDs":
					view.queueShardIDs = nil
				case "global upgrade":
					candidate.publishedRoots = &publishedRootSet{pointShards: []publishedRootRef{{}}}
				case "range spans":
					view.queueRangeSpans = make([][]batch.DeleteRange, len(view.queue))
					// An unrelated span still forbids filtering; all positional metadata
					// remains attached to the original full queue.
					view.queueRangeSpans[len(view.queue)-1] = []batch.DeleteRange{{Start: []byte("x"), End: []byte("y")}}
				}
				sources, err := candidate.iteratorSources(lower, upper, reverse)
				if err != nil {
					t.Fatal(err)
				}
				if len(sources) != wantCount {
					closeExactSources(sources)
					t.Fatalf("sources=%d want=%d", len(sources), wantCount)
				}
				var it merging.Iterator
				if reverse {
					it = merging.NewReverseMergingIterator(sources, lower, upper)
				} else {
					it = merging.NewMergingIterator(sources, lower, upper)
				}
				var got []string
				for it.Valid() {
					got = append(got, fmt.Sprintf("%x:%s", it.Key(), it.Value()))
					it.Next()
				}
				if err := it.Error(); err != nil {
					t.Fatal(err)
				}
				if err := it.Close(); err != nil {
					t.Fatal(err)
				}
				want := []string{fmt.Sprintf("%x:new", key)}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("duplicate priority: got=%v want=%v", got, want)
				}
			})
		}
	}

	// A failed published-root open must close precisely the selected queue
	// cursors; no unrelated shard cursor was opened to be cleaned up.
	for _, reverse := range []bool{false, true} {
		closed := 0
		injected := errors.New("published exact iterator open failure")
		shard := cached.shardIndex(key)
		points := append([]rootDomainSnapshot(nil), snap.rootPointShards...)
		points[shard].immutables = make([]memtable.Table, len(snap.rootPointShards[shard].immutables))
		for i, table := range snap.rootPointShards[shard].immutables {
			points[shard].immutables[i] = exactCloseTable{Table: table, closed: &closed}
		}
		refs := make([]publishedRootRef, len(points))
		refs[shard].lookup = exactOpenErrorRoot{err: injected}
		candidate := &Snapshot{db: snap.db, view: snap.view, backend: snap.backend, backendRootID: snap.backendRootID, backendFallback: snap.backendFallback, rootPointShards: points, rootIterator: snap.rootIterator, publishedRoots: &publishedRootSet{pointShards: refs}}
		sources, err := candidate.iteratorSources(lower, upper, reverse)
		if !errors.Is(err, injected) || len(sources) != 0 || closed != relevantCount-1 {
			t.Fatalf("error cleanup: error=%v sources=%d closed=%d want=%d", err, len(sources), closed, relevantCount-1)
		}
	}
	// A physical delete in the selected shard must hide its older copy.
	if err := cached.Delete(key); err != nil {
		t.Fatal(err)
	}
	tomb := cached.AcquireSnapshot()
	if tomb == nil {
		t.Fatal("tomb snapshot unavailable")
	}
	defer tomb.Close()
	for _, reverse := range []bool{false, true} {
		var it interface {
			Valid() bool
			Error() error
			Close() error
		}
		if reverse {
			it, err = tomb.ReverseIterator(lower, upper)
		} else {
			it, err = tomb.Iterator(lower, upper)
		}
		if err != nil {
			t.Fatal(err)
		}
		if it.Valid() || it.Error() != nil {
			t.Fatalf("physical tombstone exposed older value: valid=%t err=%v", it.Valid(), it.Error())
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(key[:len(lower)], lower) {
		t.Fatal("fixture lost canonical affinity")
	}
}

func closeExactSources(sources []merging.IteratorSource) {
	for _, source := range sources {
		if source.Iter != nil {
			_ = source.Iter.Close()
		}
	}
}

type exactCloseTable struct {
	memtable.Table
	closed *int
}

func (table exactCloseTable) NewIterator(start, end []byte) iterator.UnsafeIterator {
	return &exactCloseCursor{UnsafeIterator: table.Table.NewIterator(start, end), closed: table.closed}
}
func (table exactCloseTable) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	return &exactCloseCursor{UnsafeIterator: table.Table.NewReverseIterator(start, end), closed: table.closed}
}

type exactCloseCursor struct {
	iterator.UnsafeIterator
	closed *int
}

func (cursor *exactCloseCursor) Close() error {
	(*cursor.closed)++
	return cursor.UnsafeIterator.Close()
}

type exactOpenErrorRoot struct{ err error }

func (root exactOpenErrorRoot) GetEntry([]byte) ([]byte, page.ValuePtr, byte, bool) {
	return nil, page.ValuePtr{}, 0, false
}
func (root exactOpenErrorRoot) Iterator([]byte, []byte) (iterator.UnsafeIterator, error) {
	return nil, root.err
}
func (root exactOpenErrorRoot) ReverseIterator([]byte, []byte) (iterator.UnsafeIterator, error) {
	return nil, root.err
}
