package caching

import (
	"bytes"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/page"
)

// Count exact-key re-resolution independently of the successor stream. The
// wrapped tables still delegate every operation to their original pinned root.
type cowInlineSuccessorLookup struct {
	memtable.Table
	exact *int
	seeks *int
}

func (s *cowInlineSuccessorLookup) SeekGE(start, end []byte) ([]byte, []byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	if s.seeks != nil {
		*s.seeks++
	}
	return s.Table.(memtable.SuccessorTable).SeekGE(start, end)
}

func (s *cowInlineSuccessorLookup) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	*s.exact++
	return s.Table.GetEntry(key)
}
func (s *cowInlineSuccessorLookup) GetEntryWithRevision(key []byte) ([]byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	*s.exact++
	if table, ok := s.Table.(memtable.RevisionTable); ok {
		return table.GetEntryWithRevision(key)
	}
	value, ptr, flags, found := s.Table.GetEntry(key)
	return value, ptr, flags, page.LegacyEntryRevision, found
}

func cowInlineSuccessorCountExact(snapshot *Snapshot, count *int) {
	shards := append([]rootDomainSnapshot(nil), snapshot.rootPointShards...)
	for i := range shards {
		if shards[i].mutable != nil {
			shards[i].mutable = &cowInlineSuccessorLookup{Table: shards[i].mutable, exact: count}
		}
		shards[i].immutables = append([]memtable.Table(nil), shards[i].immutables...)
		for j, table := range shards[i].immutables {
			shards[i].immutables[j] = &cowInlineSuccessorLookup{Table: table, exact: count}
		}
	}
	snapshot.rootPointShards = shards
}

func TestCOWInlineSuccessorUsesOwnedWinnerAndPreservesPointerFallback(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir + "/backend"})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered",
		FlushThreshold: 1 << 30, MemtableShards: 2,
		ValueLogPointerThreshold: 1024, ValueLogCompression: uint8(vlogCompressionOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	defer db.Close()
	owner := backend.NewMVCCAdmission()
	defer owner.Close()
	keys := [][]byte{{}, {0}, {'a'}, {'a', 0}, {'a', 0, 'b'}, {255}}
	keys = append(keys, cowOrderedLifecycleKey(t, "logical\x00", 19))
	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = append([]byte("inline-"), key...)
		if i == len(keys)-1 {
			cowOrderedLifecycleSet(t, db, owner, key, values[i])
		} else if err := db.Set(key, values[i]); err != nil {
			t.Fatal(err)
		}
	}
	pointerKey := cowOrderedLifecycleKey(t, "pointer\x00", 19)
	pointerValue := bytes.Repeat([]byte("pointer"), 2048)
	cowOrderedLifecycleSet(t, db, owner, pointerKey, pointerValue)
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	exact := 0
	cowInlineSuccessorCountExact(old, &exact)
	check := func(snapshot *Snapshot, key, want []byte) {
		t.Helper()
		end := append(append([]byte(nil), key...), 0)
		gotKey, value, found, err := snapshot.SeekGEVersionRange(key, end)
		if err != nil || !found || !bytes.Equal(gotKey, key) || !bytes.Equal(value, want) {
			t.Fatalf("key=%x found=%t value=%x err=%v", key, found, value, err)
		}
		// Each returned record is owned even though selection borrowed the
		// immutable candidate; mutation cannot poison a subsequent read.
		clear(gotKey)
		clear(value)
	}
	for i, key := range keys {
		check(old, key, values[i])
		check(old, key, values[i])
	}
	if exact != 0 {
		t.Fatalf("inline winners repeated %d exact lookups", exact)
	}
	check(old, pointerKey, pointerValue)
	if exact == 0 {
		t.Fatal("pointer winner bypassed original materialization path")
	}

	// Retained old-cut winners stay owned across replacements and a backend
	// basis change. A physical tombstone must still mask its older record.
	if err := db.Set(keys[2], []byte("updated")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(keys[3]); err != nil {
		t.Fatal(err)
	}
	current, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	check(current, keys[2], []byte("updated"))
	end := append(append([]byte(nil), keys[3]...), 0)
	if _, _, found, err := current.SeekGEVersionRange(keys[3], end); err != nil || found {
		t.Fatalf("physical tombstone resurrected: found=%t err=%v", found, err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check(old, keys[2], values[2])
	check(old, keys[3], values[3])
	check(old, pointerKey, pointerValue)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	latest, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer latest.Close()
	check(latest, keys[2], []byte("updated"))
	check(latest, pointerKey, pointerValue)
}

func TestCOWExactVersionSuccessorUsesRetainedPointShard(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	logical := []byte("target\x00")
	key := cowOrderedLifecycleKey(t, string(logical), 19)
	cowOrderedLifecycleSet(t, db, owner, key, []byte("retained"))
	// Populate every sibling shard so a full iterator-domain search really
	// would do unrelated work rather than only finding empty adapters.
	for i := 0; i < 64; i++ {
		other := cowOrderedLifecycleKey(t, fmt.Sprintf("other-%02d", i), 19)
		cowOrderedLifecycleSet(t, db, owner, other, []byte("sibling"))
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	pointSeeks, genericSeeks, exact := 0, 0, 0
	cowInlineSuccessorCountExact(old, &exact)
	for i := range old.rootPointShards {
		for _, table := range old.rootPointShards[i].immutables {
			table.(*cowInlineSuccessorLookup).seeks = &pointSeeks
		}
	}
	old.rootIterator.immutables = append([]memtable.Table(nil), old.rootIterator.immutables...)
	for i, table := range old.rootIterator.immutables {
		old.rootIterator.immutables[i] = &cowInlineSuccessorLookup{Table: table, exact: &exact, seeks: &genericSeeks}
	}
	lower, _ := mvcckey.AppendKeyVersionsLower(nil, logical)
	upper, _ := mvcckey.AppendKeyVersionsUpper(nil, logical)
	check := func(start, end, want []byte) {
		t.Helper()
		got, _, found, err := old.SeekGEVersionRange(start, end)
		if err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("range [%x,%x) got=%x found=%t err=%v", start, end, got, found, err)
		}
	}
	check(lower, upper, key)
	if genericSeeks != 0 || pointSeeks != len(old.rootPointShards[db.shardIndex(key)].immutables) {
		t.Fatalf("qualified routing: point=%d generic=%d", pointSeeks, genericSeeks)
	}
	pointSeeks, genericSeeks = 0, 0
	check(key, append(append([]byte(nil), key...), 0), key)
	if pointSeeks != 0 || genericSeeks != len(old.rootIterator.immutables) {
		t.Fatalf("noncanonical range incorrectly filtered: point=%d generic=%d", pointSeeks, genericSeeks)
	}
	// Malformed suffixes sort in the same exact range and MUST retain its
	// affinity. Returning the malformed record lets the MVCC decoder reject
	// it instead of silently choosing a later canonical version.
	malformed := append(append([]byte(nil), lower...), 0x80)
	if err := db.Set(malformed, []byte("malformed")); err != nil {
		t.Fatal(err)
	}
	current, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	got, _, found, err := current.SeekGEVersionRange(lower, upper)
	if err != nil || !found || !bytes.Equal(got, malformed) {
		t.Fatalf("malformed suffix hidden by shard route: got=%x found=%t err=%v", got, found, err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check(lower, upper, key) // the old cut excludes the later malformed tail
}
