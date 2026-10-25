package collections

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// This exercises the real public owner with no canonical graph command. It is
// a named-domain seam discriminator, not vector API or recovery qualification.
func TestNativeNamedDomainReplacement(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(handle)
	defer func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	document, err := bson.Marshal(bson.D{{Key: "n", Value: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Insert([]byte("one"), document); err != nil {
		t.Fatal(err)
	}
	var reads []*NativeJSONRead
	defer func() {
		for _, read := range reads {
			if err := read.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	capture := func() *NativeJSONRead {
		t.Helper()
		read, err := collection.nativeJSON.Capture("events")
		if err != nil {
			t.Fatal(err)
		}
		reads = append(reads, read)
		return read
	}
	save := func(expected *NativeJSONRead, replace bool, rows []memtable.COWMutation) (backenddb.NativePublicationResult, error) {
		t.Helper()
		tx, err := handle.BeginNativeConditionalTxn()
		if err != nil {
			return backenddb.NativePublicationResult{}, err
		}
		defer tx.Close()
		base := tx.State().(*nativeEngineVersion)
		c := expected.collection
		interpretation := uint64(0)
		if old, ok := c.domains.Get("vector/graph"); ok {
			interpretation = old.interpretation
		}
		next, err := prepareNativeNamedDomains(base, []nativeNamedCollectionChange{{collection: c, sourceGeneration: c.documentGeneration, domains: []nativeNamedDomainChange{{name: "vector/graph", entries: rows, replace: replace, expectedInterpretation: interpretation}}}})
		if err != nil {
			if next != nil {
				err = errors.Join(err, finishPrivateNative(next.Release()))
			}
			return backenddb.NativePublicationResult{}, err
		}
		return tx.CommitNative(&backenddb.NativePreparedPublication{Basis: base, State: next, StartSequence: tx.LogicalSequence()}, nil, false)
	}
	rows := func(keys ...string) []memtable.COWMutation {
		result := make([]memtable.COWMutation, len(keys))
		for i, key := range keys {
			result[i] = memtable.COWMutation{Key: []byte(key), Value: []byte("node:" + key), Flags: node.FlagInline}
		}
		return result
	}
	accepted := func(expected *NativeJSONRead, replace bool, changes []memtable.COWMutation) {
		t.Helper()
		result, err := save(expected, replace, changes)
		if err != nil || !result.LogicalInstalled {
			t.Fatalf("save: %+v %v", result, err)
		}
	}
	check := func(read *NativeJSONRead, want ...string) {
		t.Helper()
		for _, reverse := range []bool{false, true} {
			var got []string
			var start, end []byte
			for {
				key, value, ok, err := read.namedDomainSeek("vector/graph", start, end, reverse)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				if !bytes.Equal(value, []byte("node:"+string(key))) {
					t.Fatalf("key=%q value=%q", key, value)
				}
				got = append(got, string(key))
				if reverse {
					end = key
				} else {
					start = append(key, 0)
				}
			}
			expected := append([]string(nil), want...)
			if reverse {
				for i, j := 0, len(expected)-1; i < j; i, j = i+1, j-1 {
					expected[i], expected[j] = expected[j], expected[i]
				}
			}
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("reverse=%v keys=%q want=%q", reverse, got, expected)
			}
		}
		for _, key := range want {
			value, err := read.namedDomainGet("vector/graph", []byte(key))
			if err != nil || string(value) != "node:"+key {
				t.Fatalf("Get %s: %q %v", key, value, err)
			}
		}
	}
	domain := func(read *NativeJSONRead) *nativeDomainState {
		t.Helper()
		d, ok := read.collection.domains.Get("vector/graph")
		if !ok {
			t.Fatal("missing named domain")
		}
		return d
	}
	initial := capture()
	if initial.collection.documentGeneration != 1 {
		t.Fatalf("initial generation=%d", initial.collection.documentGeneration)
	}
	accepted(initial, true, rows("a", "b", "c"))
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	old := capture()
	if d := domain(old); d.baseRoot == 0 || d.interpretation != 1 || d.sourceGeneration != 1 {
		t.Fatalf("physical initial domain=%+v", d)
	}
	check(old, "a", "b", "c")
	tokenBefore, ok := handle.StateToken()
	if !ok {
		t.Fatal("no physical token")
	}
	accepted(old, true, rows("b"))
	replacement := capture()
	tokenAfter, ok := handle.StateToken()
	if !ok || !reflect.DeepEqual(tokenBefore, tokenAfter) {
		t.Fatalf("replacement forced physical publication: before=%+v after=%+v", tokenBefore, tokenAfter)
	}
	if replacement.capture.LogicalSequence() != old.capture.LogicalSequence()+1 {
		t.Fatalf("logical sequence before=%d after=%d", old.capture.LogicalSequence(), replacement.capture.LogicalSequence())
	}
	d := domain(replacement)
	if d.baseRoot != 0 || d.interpretation != 2 || d.sourceGeneration != 1 || replacement.collection.documentGeneration != 1 {
		t.Fatalf("replacement identity=%+v generation=%d", d, replacement.collection.documentGeneration)
	}
	if _, err := replacement.namedDomainGet("vector/graph", []byte("a")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("replacement resurrected omitted graph node a: %v", err)
	}
	check(old, "a", "b", "c")
	check(replacement, "b")
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	built := capture()
	if d := domain(built); d.baseRoot == 0 || d.interpretation != 2 || d.sourceGeneration != 1 {
		t.Fatalf("handoff changed replacement identity: %+v", d)
	}
	check(old, "a", "b", "c")
	check(replacement, "b")
	check(built, "b")
	if physical, err := vectorIndexDocumentGenerationForCollection(built.capture.Snapshot(), "events"); err != nil || physical != 1 {
		t.Fatalf("physical generation=%d err=%v", physical, err)
	}
	// A delta preserves interpretation and source coverage and uses the same
	// newest-first tombstone merge in forward and reverse traversal.
	delta := rows("d")
	delta = append(delta, memtable.COWMutation{Key: []byte("b"), Flags: node.FlagTombstone})
	accepted(built, false, delta)
	changed := capture()
	if d := domain(changed); d.interpretation != 2 || d.sourceGeneration != 1 {
		t.Fatalf("delta changed identity: %+v", d)
	}
	check(changed, "d")
	check(built, "b")
	// Same source generation, stale graph interpretation: rejected before install.
	beforeStale := changed.capture.LogicalSequence()
	if result, err := save(old, true, rows("stale")); !errors.Is(err, backenddb.ErrConcurrentModification) || result.Assigned || result.LogicalInstalled {
		t.Fatalf("stale graph save: %+v %v", result, err)
	}
	if current := capture(); current.capture.LogicalSequence() != beforeStale {
		t.Fatal("rejected graph save advanced logical sequence")
	}
	// Accepted primary mutation advances the immutable generation once. A stale
	// saved graph cannot claim coverage of that newer primary, even on the latest
	// transaction basis. Its physical marker advances only with the primary build.
	if _, err := collection.Insert([]byte("two"), document); err != nil {
		t.Fatal(err)
	}
	newer := capture()
	if newer.collection.documentGeneration != 2 || domain(newer).sourceGeneration != 1 {
		t.Fatalf("primary=%d graph coverage=%d", newer.collection.documentGeneration, domain(newer).sourceGeneration)
	}
	physical, err := vectorIndexDocumentGenerationForCollection(newer.capture.Snapshot(), "events")
	if err != nil || physical != 1 {
		t.Fatalf("unbuilt physical generation=%d err=%v", physical, err)
	}
	beforeStale = newer.capture.LogicalSequence()
	if result, err := save(changed, true, rows("stale")); !errors.Is(err, backenddb.ErrConcurrentModification) || result.Assigned || result.LogicalInstalled {
		t.Fatalf("stale primary save: %+v %v", result, err)
	}
	if current := capture(); current.capture.LogicalSequence() != beforeStale {
		t.Fatal("rejected primary save advanced logical sequence")
	}
	// A full empty image is a replacement, not an omitted delta. Both old cuts
	// retain their images while the new cut has no visible graph rows.
	accepted(newer, true, nil)
	empty := capture()
	check(empty)
	check(changed, "d")
	if d := domain(empty); d.baseRoot != 0 || d.interpretation != 3 || d.sourceGeneration != 2 {
		t.Fatalf("empty replacement=%+v", d)
	}
	if _, err := empty.namedDomainGet("vector/graph", []byte("d")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("empty image retained node: %v", err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	final := capture()
	check(final)
	check(old, "a", "b", "c")
	if d := domain(final); d.baseRoot != 0 || d.interpretation != 3 || d.sourceGeneration != 2 || final.collection.documentGeneration != 2 {
		t.Fatalf("empty handoff=%+v generation=%d", d, final.collection.documentGeneration)
	}
	if physical, err := vectorIndexDocumentGenerationForCollection(final.capture.Snapshot(), "events"); err != nil || physical != 2 {
		t.Fatalf("final physical generation=%d err=%v", physical, err)
	}
	// Import consumes the existing persisted marker; named graph encoding/import
	// itself is intentionally outside this discriminator's public capability.
	importBase, err := newNativeEmptyEngine(final.capture.NativeAllocator(), final.capture.NativeMaintenanceAllocator())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if importBase != nil {
			if err := finishPrivateNative(importBase.Release()); err != nil {
				t.Error(err)
			}
		}
	}()
	get := func(key string) ([]byte, error) {
		value, found, err := getSystemValue(final.capture.Snapshot(), key)
		if err == nil && !found {
			err = tree.ErrKeyNotFound
		}
		return value, err
	}
	imported, err := importNativeJSONCollection(importBase, *final.collection.schema.meta, final.collection.incarnation, get)
	if imported != nil {
		defer func() {
			if imported != nil {
				if err := finishPrivateNative(imported.Release()); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	if err != nil {
		t.Fatal(err)
	}
	importedCollection, ok := imported.names.Get("events")
	if !ok || importedCollection.documentGeneration != 2 {
		t.Fatalf("imported generation=%v", importedCollection)
	}
	// End every retained old/new scope and private import before checking the
	// actual finite allocator, including the named reader's scratch receipts.
	type memoryStats interface{ Stats() memtable.COWStats }
	budget := final.capture.NativeAllocator().(memoryStats)
	importedCollection = nil
	if err := finishPrivateNative(imported.Release()); err != nil {
		t.Fatal(err)
	}
	imported = nil
	if err := finishPrivateNative(importBase.Release()); err != nil {
		t.Fatal(err)
	}
	importBase = nil
	for _, read := range reads {
		if err := read.Close(); err != nil {
			t.Fatal(err)
		}
	}
	reads = nil
	d = nil
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	if stats := budget.Stats(); stats.TotalBytes != 0 {
		t.Fatalf("closed retained scopes leaked finite budget: %+v", stats)
	}

}
