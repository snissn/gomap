package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// Canonical exact-version bounds must not open frozen tables from other shards.
// This uses the public snapshot boundary so source accounting measures opens,
// including empty iterators, rather than just the records returned.
func TestExactVersionSnapshotSources(t *testing.T) {
	caching.SetIteratorDebug(true)
	t.Cleanup(func() { caching.SetIteratorDebug(false) })
	for _, depth := range []int{1, 8, 64} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("depth=%d/reverse=%t", depth, reverse), func(t *testing.T) {
				db, _, _ := prepareExactKeyIteration(t, depth, "FrozenQueue")
				lower, err := mvcckey.AppendKeyVersionsLower(nil, exactKeyIterationTarget())
				if err != nil {
					t.Fatal(err)
				}
				upper, err := mvcckey.AppendKeyVersionsUpper(nil, exactKeyIterationTarget())
				if err != nil {
					t.Fatal(err)
				}
				snap := db.AcquireSnapshot()
				if snap == nil {
					t.Fatal("snapshot unavailable")
				}
				defer snap.Close()
				before := exactSourceStat(t, db.Stats())
				var it interface {
					Valid() bool
					Next()
					Error() error
					Close() error
				}
				if reverse {
					it, err = snap.ReverseIterator(lower, upper)
				} else {
					it, err = snap.Iterator(lower, upper)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer it.Close()
				seen := 0
				for ; it.Valid(); it.Next() {
					seen++
				}
				if err := it.Error(); err != nil {
					t.Fatal(err)
				}
				if seen != depth {
					t.Fatalf("versions=%d, want %d", seen, depth)
				}
				after := exactSourceStat(t, db.Stats())
				if got := after - before; got != 2 {
					t.Fatalf("sources opened=%d, want relevant shard + published root (2)", got)
				}
			})
		}
	}
}

func exactSourceStat(t testing.TB, stats map[string]string) uint64 {
	t.Helper()
	n, err := strconv.ParseUint(stats["treedb.cache.iterator.sources_total"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// The oracle opens the unchanged generic path and filters owned records after
// iteration, rather than sharing exact physical bounds with the candidate.
func exactGenericOracle(t testing.TB, store *Store, opts VersionIteratorOptions, seek bool, key []byte, ts uint64) []Version {
	t.Helper()
	wanted := opts.ExactKey
	opts.ExactKey = nil
	it, err := store.IterateVersions(opts)
	if err != nil {
		t.Fatal(err)
	}
	if seek {
		it.Seek(key, ts)
	}
	var result []Version
	for _, v := range collectVersions(t, it) {
		if bytes.Equal(v.Key, wanted) {
			result = append(result, v)
		}
	}
	return result
}

func TestVersionIteratorExactKeyGenericOracle(t *testing.T) {
	db, store, _ := prepareExactKeyIteration(t, 8, "FrozenQueue")
	keys := [][]byte{{}, {'p', 0, 0xff, 'o', 's', 't'}, {'p', 0, 0xff, 'o', 's', 't', 0}, {0xff, 0, 0xff}}
	for _, key := range keys {
		for _, mutation := range []struct {
			ts      uint64
			value   []byte
			deleted bool
		}{{21, []byte{}, false}, {24, nil, true}, {28, []byte("new"), false}} {
			if err := store.CommitAt(mutation.ts, []Mutation{{Key: key, Value: mutation.value, Delete: mutation.deleted}}, CommitRelaxed); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, key := range append(keys, []byte("absent")) {
		for _, reverse := range []bool{false, true} {
			for _, ts := range []uint64{0, 23, 27, 29} {
				for _, seek := range []bool{false, true} {
					opts := VersionIteratorOptions{ExactKey: key, Reverse: reverse, ReadTimestamp: ts}
					want := exactGenericOracle(t, store, opts, seek, key, 25)
					it, err := store.IterateVersions(opts)
					if err != nil {
						t.Fatal(err)
					}
					if seek {
						it.Seek(key, 25)
					}
					got := collectVersions(t, it)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("key=%x reverse=%t ceiling=%d seek=%t\ngot=%+v\nwant=%+v", key, reverse, ts, seek, got, want)
					}
				}
			}
		}
	}
	// Options are intersected; excluding the exact key produces an empty scan.
	for _, opts := range []VersionIteratorOptions{
		{ExactKey: keys[1], Prefix: []byte("other")},
		{ExactKey: keys[1], LowerBound: []byte("z")},
		{ExactKey: keys[1], UpperBound: keys[1]},
	} {
		it, err := store.IterateVersions(opts)
		if err != nil {
			t.Fatal(err)
		}
		if got := collectVersions(t, it); len(got) != 0 {
			t.Fatalf("excluded exact key returned %+v", got)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Maximum-size keys do not require an impossible key+NUL upper bound.
	maxKey := bytes.Repeat([]byte{'z'}, mvcckey.MaxEncodedKeySize-19)
	lower, upper, err := versionPhysicalBounds(VersionIteratorOptions{ExactKey: maxKey})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := mvcckey.ExactVersionRange(lower, upper); !ok {
		t.Fatal("maximum-key bounds are not canonical")
	}
	it, err := store.IterateVersions(VersionIteratorOptions{ExactKey: maxKey})
	if err != nil {
		t.Fatal(err)
	}
	if got := collectVersions(t, it); len(got) != 0 {
		t.Fatalf("missing maximum key returned %d records", len(got))
	}
	if _, err := store.IterateVersions(VersionIteratorOptions{ExactKey: append(maxKey, 'z')}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("oversized exact key error=%v", err)
	}
}

func TestVersionIteratorExactKeyPinnedFloorAndOwnership(t *testing.T) {
	db, store, _ := prepareExactKeyIteration(t, 8, "FrozenQueue")
	key := exactKeyIterationTarget()
	it, err := store.IterateVersions(VersionIteratorOptions{ExactKey: key, ReadTimestamp: 17})
	if err != nil {
		t.Fatal(err)
	}
	want := it.Entry()
	key[0] = 'x' // The copied option must keep its original domain.
	if err := store.CommitAt(20, []Mutation{{Key: exactKeyIterationTarget(), Value: []byte("later")}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceDiscardFloor(18, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	got := collectVersions(t, it)
	if len(got) != 8 || !reflect.DeepEqual(got[0], want) {
		t.Fatalf("pinned result changed: %+v", got)
	}
	if !bytes.Equal(want.Value, []byte("posting-list-delta")) {
		t.Fatal("owned result changed after close")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.IterateVersions(VersionIteratorOptions{ExactKey: exactKeyIterationTarget(), ReadTimestamp: 17}); !errors.Is(err, ErrReadBeforeDiscardFloor) {
		t.Fatalf("floor error=%v", err)
	}
}

func TestVersionIteratorExactKeyMalformedAffinity(t *testing.T) {
	for _, suffix := range [][]byte{{}, {1, 2, 3}, bytes.Repeat([]byte{0xa5}, mvcckey.MaxEncodedKeySize)} {
		t.Run(fmt.Sprint(len(suffix)), func(t *testing.T) {
			db, store, _ := prepareExactKeyIteration(t, 1, "FrozenQueue")
			key := exactKeyIterationTarget()
			lower, err := mvcckey.AppendKeyVersionsLower(nil, key)
			if err != nil {
				t.Fatal(err)
			}
			malformed := append(append([]byte(nil), lower...), suffix...)
			// Oversized physical records cannot be inserted through TreeDB's public
			// key-size gate; the allocation-free affinity guarantee is still checked.
			prefix, ok := mvcckey.VersionAffinityPrefix(malformed)
			if !ok || !bytes.Equal(prefix, lower) {
				t.Fatal("malformed suffix lost affinity")
			}
			if len(malformed) > mvcckey.MaxEncodedKeySize {
				return
			}
			if err := db.Set(malformed, []byte{1}); err != nil {
				t.Fatal(err)
			}
			for _, opts := range []VersionIteratorOptions{{ExactKey: key}, {Prefix: key}} {
				it, err := store.IterateVersions(opts)
				if err != nil {
					t.Fatal(err)
				}
				for it.Valid() {
					it.Next()
				}
				if !errors.Is(it.Error(), ErrMalformedRecord) {
					t.Fatalf("malformed record error=%v", it.Error())
				}
				if err := it.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestVersionIteratorExactKeyStorageError(t *testing.T) {
	db := openTestDB(t, t.TempDir(), treedb.DurabilityDurable)
	defer db.Close()
	injected := errors.New("exact value failure")
	store := newStore(&snapshotValueErrorDB{DB: db, err: injected})
	if err := store.CommitAt(10, []Mutation{{Key: []byte("k"), Value: []byte("v")}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	for _, reverse := range []bool{false, true} {
		it, err := store.IterateVersions(VersionIteratorOptions{ExactKey: []byte("k"), Reverse: reverse})
		if err != nil {
			t.Fatal(err)
		}
		assertStorageValueError(t, it.Error(), injected, "read version iterator value")
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestVersionIteratorExactKeyRangeDeleteFallback(t *testing.T) {
	db, store, _ := prepareExactKeyIteration(t, 8, "FrozenQueue")
	key := exactKeyIterationTarget()
	lower, _ := mvcckey.AppendKeyVersionsLower(nil, key)
	upper, _ := mvcckey.AppendKeyVersionsUpper(nil, key)
	// Exercise the raw layer's retained range barrier intentionally. Production
	// Store ownership forbids arbitrary raw writes in this reserved namespace.
	if err := db.DeleteRange(lower, upper); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitAt(20, []Mutation{{Key: key, Value: []byte("after range")}}, CommitRelaxed); err != nil {
		t.Fatal(err)
	}
	for _, reverse := range []bool{false, true} {
		opts := VersionIteratorOptions{ExactKey: key, Reverse: reverse}
		want := exactGenericOracle(t, store, opts, false, nil, 0)
		it, err := store.IterateVersions(opts)
		if err != nil {
			t.Fatal(err)
		}
		got := collectVersions(t, it)
		if !reflect.DeepEqual(got, want) || len(got) != 1 || got[0].Timestamp != 20 {
			t.Fatalf("range delete result=%+v oracle=%+v", got, want)
		}
		// The flusher can retire live queue entries after this snapshot opens.
		// Do not compare captured source opens with a later live queue gauge.
		// Exact full-queue fallback counts are checked on the pinned caching view.
	}
}

func TestExactVersionSnapshotCloseInvalidatesIterator(t *testing.T) {
	db, _, _ := prepareExactKeyIteration(t, 1, "FrozenQueue")
	lower, _ := mvcckey.AppendKeyVersionsLower(nil, exactKeyIterationTarget())
	upper, _ := mvcckey.AppendKeyVersionsUpper(nil, exactKeyIterationTarget())
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot unavailable")
	}
	it, err := snap.Iterator(lower, upper)
	if err != nil {
		snap.Close()
		t.Fatal(err)
	}
	if !it.Valid() {
		t.Fatal("fixture iterator is empty")
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if it.Valid() || !errors.Is(it.Error(), treedb.ErrClosed) {
		t.Fatalf("iterator survived snapshot close: valid=%t error=%v", it.Valid(), it.Error())
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
}
