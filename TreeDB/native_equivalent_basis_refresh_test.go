package treedb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// CRUD uses the ordinary activated public frontend. The existing fenced
// maintenance seam schedules a real rewrite without draining the newer suffix;
// ValueLogRewriteOnline's public wrapper normally checkpoints that suffix first.
func TestNativeExistingCollectionEquivalentBasisRewrite(t *testing.T) {
	opts := OptionsFor(ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	seed, seedClose, _, err := OpenBackendWithCachedLeafLogStats(opts)
	if err != nil {
		t.Fatal(err)
	}
	seedClosed := false
	t.Cleanup(func() {
		if !seedClosed {
			if e := seedClose(); e != nil {
				t.Errorf("seed Close: %v", e)
			}
		}
	})
	manager := collections.NewCollectionManager(seed)
	for _, name := range []string{"rows", "alias"} {
		if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	// Maintained physical setup: one persistent pointer root is named by two
	// canonical collection descriptors. Apply a system delta so the original
	// manager-created metadata and incarnation records remain eligible for import.
	document := []byte(`{"body":"` + string(bytes.Repeat([]byte("persistent"), 512)) + `"}`)
	ptrs, err := seed.AppendValueLogValues([][]byte{document})
	if err != nil {
		t.Fatal(err)
	}
	rootTable, err := memtable.NewWithCapacityMode(1, memtable.ModeHashSorted)
	if err != nil {
		t.Fatal(err)
	}
	rootTable.SetEntry([]byte("old"), nil, ptrs[0], node.FlagPointer)
	rootTable.Freeze()
	_, roots, err := seed.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		Iter: rootTable.NewIterator(nil, nil), StoragePolicy: backenddb.OrderedRootStoragePagerLeaves,
	}}, func(roots []uint64) (iterator.UnsafeIterator, error) {
		descriptors, e := memtable.NewWithCapacityMode(2, memtable.ModeHashSorted)
		if e != nil {
			return nil, e
		}
		for _, name := range []string{"rows", "alias"} {
			var raw [8]byte
			binary.BigEndian.PutUint64(raw[:], roots[0])
			descriptors.Set([]byte("collections/root/"+name+"/primary"), raw[:])
		}
		descriptors.Freeze()
		return descriptors.NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	seed.ReleaseValueLogValues(ptrs)
	if len(roots) != 1 || roots[0] == 0 {
		t.Fatalf("seed roots: %v", roots)
	}
	if err = seedClose(); err != nil {
		t.Fatal(err)
	}
	seedClosed = true

	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if e := database.Close(); e != nil {
				t.Errorf("cleanup Close: %v", e)
			}
		}
	})
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	// Import the complete physical catalog and seal a native engine while leaving
	// the aliased physical roots untouched. The following row remains unsealed.
	if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "spare"}); err != nil {
		t.Fatal(err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	rows, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	alias, err := manager.OpenCollection("alias")
	if err != nil {
		t.Fatal(err)
	}
	held, err := rows.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if e := held.Close(); e != nil {
			t.Errorf("held Close: %v", e)
		}
	})
	heldAlias, err := alias.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if e := heldAlias.Close(); e != nil {
			t.Errorf("held alias Close: %v", e)
		}
	})
	newer := []byte(`{"n":2}`)
	if _, err = rows.InsertBatch([][]byte{[]byte("new")}, [][]byte{newer}); err != nil {
		t.Fatal(err)
	}
	before, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	sequence, lsn := before.LogicalSequence(), backend.CommandWALNextLSN()
	if err = before.Close(); err != nil {
		t.Fatal(err)
	}
	physical := func() (uint64, page.ValuePtr) {
		s := backend.AcquireSnapshot()
		if s == nil {
			t.Fatal("nil physical snapshot")
		}
		defer func() {
			if e := s.Close(); e != nil {
				t.Errorf("physical Close: %v", e)
			}
		}()
		token, ok := s.StateToken()
		if !ok {
			t.Fatal("physical token")
		}
		var root uint64
		for _, name := range []string{"rows", "alias"} {
			raw, e := s.GetAtRoot(token.SystemRootPageID, []byte("collections/root/"+name+"/primary"))
			if e != nil || len(raw) != 8 {
				t.Fatalf("descriptor %s: %x %v", name, raw, e)
			}
			r := binary.BigEndian.Uint64(raw)
			if root != 0 && root != r {
				t.Fatalf("qualified alias diverged: %d %d", root, r)
			}
			root = r
		}
		entry, e := s.GetEntryAtRoot(root, []byte("old"))
		if e != nil || entry.Flags&node.FlagPointer == 0 {
			t.Fatalf("actual persistent pointer: %+v %v", entry, e)
		}
		return root, entry.ValuePtr
	}
	oldRoot, oldPointer := physical()
	protection := backenddb.ValueLogRewriteOnlineOptions{SourceFileIDs: []uint32{oldPointer.FileID}, BatchSize: 1}
	protection.ProtectedPaths = database.cached.ValueLogProtectedPaths()
	if len(protection.ProtectedPaths) == 0 {
		protection.ProtectedPaths = []string{""}
	}
	protection.LeafGenerationProtectedRootIDs, protection.LeafGenerationProtectedSystemRootIDs = database.cached.ProtectedLeafGenerationRootIDPair()
	protection.ReserveRIDs = database.cached.ReserveValueLogRIDs
	var stats backenddb.ValueLogRewriteStats
	err = database.runCachedBackendMaintenance(func() error {
		var e error
		stats, e = backend.ValueLogRewriteOnline(context.Background(), protection)
		return e
	})
	if err != nil {
		t.Fatalf("equivalent physical rewrite: %v", err)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("real aliased pointer rewrite: %+v", stats)
	}
	newRoot, newPointer := physical()
	if oldRoot == newRoot || oldPointer == newPointer {
		t.Fatalf("rewrite did not move root/pointer: %d/%+v -> %d/%+v", oldRoot, oldPointer, newRoot, newPointer)
	}
	after, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	if after.LogicalSequence() != sequence || backend.CommandWALNextLSN() != lsn {
		t.Fatal("physical maintenance changed logical sequence/LSN")
	}
	if err = after.Close(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []*collections.Collection{rows, alias} {
		got, e := c.Get([]byte("old"))
		if e != nil || !bytes.Equal(got, document) {
			t.Fatalf("current rewritten value: %q %v", got, e)
		}
	}
	got, err := rows.Get([]byte("new"))
	if err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("unsealed suffix: %q %v", got, err)
	}
	if _, err = alias.Get([]byte("new")); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatalf("qualified alias absence: %v", err)
	}
	for _, v := range []*collections.CollectionReadView{held, heldAlias} {
		result, e := v.FetchDocumentsByID([][]byte{[]byte("old"), []byte("new")}, collections.DocumentFetchOptions{})
		if e != nil || len(result.Results) != 2 || !result.Results[0].Found || !bytes.Equal(result.Results[0].Document, document) || result.Results[1].Found {
			t.Fatalf("old complete view: %+v %v", result, e)
		}
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	if err = heldAlias.Close(); err != nil {
		t.Fatal(err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	registry, cache := backend.ValueLogIdentityPinRegistry(), database.cached
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("original pins: %+v", s)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed = false
	backend, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	for _, name := range []string{"rows", "alias"} {
		c, e := manager.OpenCollection(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.Get([]byte("old"))
		if e != nil || !bytes.Equal(got, document) {
			t.Fatalf("reopen %s: %q %v", name, got, e)
		}
		got, e = c.Get([]byte("new"))
		if name == "rows" {
			if e != nil || !bytes.Equal(got, newer) {
				t.Fatalf("reopen suffix: %q %v", got, e)
			}
		} else if !errors.Is(e, tree.ErrKeyNotFound) {
			t.Fatalf("reopen alias absence: %v", e)
		}
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	registry, cache = backend.ValueLogIdentityPinRegistry(), database.cached
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("reopen pins: %+v", s)
	}
}
