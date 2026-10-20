package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestVacuumSystemLeafPolicyWarmWriteAndRecovery(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact manifest namespace unsupported")
	}
	for _, mode := range []string{"online", "offline", string(vacuumFailAfterNewSync), string(vacuumFailAfterReady), string(vacuumFailAfterRenameOld), string(vacuumFailAfterRenameNew)} {
		t.Run(mode, func(t *testing.T) {
			opts := Options{Dir: t.TempDir(), ChunkSize: 64 << 10, IndexOuterLeavesInValueLog: true}
			var d *DB
			var log LeafPageLogCloser
			closeDB := func() {
				t.Helper()
				if d != nil {
					err := d.Close()
					d = nil
					if err != nil {
						t.Errorf("close DB: %v", err)
					}
				}
				if log != nil {
					err := log.Close()
					log = nil
					if err != nil {
						t.Errorf("close leaf log: %v", err)
					}
				}
			}
			defer closeDB()
			openDB := func() {
				t.Helper()
				var err error
				d, err = Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				log, err = NewStandaloneLeafPageLog(opts.Dir, StandaloneLeafPageLogOptions{})
				if err != nil {
					t.Fatal(err)
				}
				d.SetLeafPageLog(log)
			}
			openDB()
			// SYSTEM and the collection roots below use pager leaves. Keep a real
			// USER DATA root in the log so both recovery slots must retain the
			// original exact outer-leaf manifest as well as SYSTEM ValuePtrs.
			writeLeafGenerationKeys(t, d, "user-data", 256, 'u')
			if len(collectLeafRefIDsFromRoot(t, d, d.State().RootPageID)) == 0 {
				t.Fatal("USER seed did not activate DATA log leaves")
			}
			// Publish the small target before a multi-page collection whose catalog
			// key sorts first. Vacuum therefore rebuilds the padding collection first
			// and cannot reuse the target's lower source page for its destination.
			const paddingCollectionRootKey = "collections/root/aaa-padding/primary"
			_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{
				{Iter: mustFrozenSystemMemtable(t, vacuumTestDocumentKey, vacuumTestDocumentValue).NewIterator(nil, nil), StoragePolicy: OrderedRootStoragePagerLeaves},
				{Iter: mustFrozenSystemMemtable(t, systemRangeKVs(1024, nil)...).NewIterator(nil, nil), StoragePolicy: OrderedRootStoragePagerLeaves},
			}, func(ids []uint64) (iterator.UnsafeIterator, error) {
				mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
				if err != nil {
					return nil, err
				}
				kvs := systemRangeKVs(2048, nil)
				for i := 0; i < len(kvs); i += 2 {
					mt.Set([]byte(kvs[i]), []byte(kvs[i+1]))
				}
				targetPtr := appendCollectionRootDescriptorPointer(t, opts.Dir, ids[0])
				paddingPtr := appendCollectionRootDescriptorPointer(t, opts.Dir, ids[1])
				mt.SetEntry([]byte(vacuumTestCollectionRootKey), nil, targetPtr, node.FlagPointer)
				mt.SetEntry([]byte(paddingCollectionRootKey), nil, paddingPtr, node.FlagPointer)
				mt.Freeze()
				return mt.NewIterator(nil, nil), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			// Move the collection root beyond the initial system allocation so
			// vacuum must relocate its descriptor and rebuild the system tree.
			moveCollection := func(baseRoot uint64, descriptorKey string) uint64 {
				deltaIter := mustFrozenSystemMemtable(t, "doc/u2", fmt.Sprintf("updated from root %d", baseRoot)).NewIterator(nil, nil)
				delta, err := OrderedRootDeltaBatchFromIterator(deltaIter)
				_ = deltaIter.Close()
				if err != nil {
					t.Fatal(err)
				}
				_, movedRoots, err := d.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
					BaseRoot: baseRoot, Delta: delta, StoragePolicy: OrderedRootStoragePagerLeaves,
				}}, func(ids []uint64) (iterator.UnsafeIterator, error) {
					ptr := appendCollectionRootDescriptorPointer(t, opts.Dir, ids[0])
					return mustFrozenSystemPointerMemtable(t, descriptorKey, ptr).NewIterator(nil, nil), nil
				})
				_ = delta.Close()
				if err != nil {
					t.Fatal(err)
				}
				if movedRoots[0] == baseRoot {
					t.Fatal("collection delta did not replace its root")
				}
				return movedRoots[0]
			}
			// Make the initial application state durable before publishing its
			// successor; the coordinator may otherwise coalesce both publications.
			if err := d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			// Give both recovery slots an application system tree while keeping
			// the target below the padding tree in the source allocation order.
			moveCollection(rootIDs[1], paddingCollectionRootKey)
			seedCollectionRoot := rootIDs[0]
			if err := d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			// Both original recovery slots must hold a multi-page pager SYSTEM
			// tree, while pointer-backed descriptor values retain vlog authority.
			func() {
				d.durablePublishMu.Lock()
				defer d.durablePublishMu.Unlock()
				for slot, record := range d.durableRoot.slotRecord {
					if record.CommitSeq <= 1 {
						t.Fatalf("seed slot %d is not a durable application system tree: %+v", slot, record)
					}
					if len(vacuumSystemPagerPointers(t, d, record.SystemRootPageID)) == 0 {
						t.Fatalf("seed slot %d did not activate pointer-backed SYSTEM values", slot)
					}
				}
			}()
			vacuumSystemManifestFiles(t, d, true)
			vacuumOnline := func() bool {
				t.Helper()
				before, index := d.state.Load(), d.idx.Load()
				err := d.VacuumIndexOnline(context.Background())
				if runtime.GOOS == "windows" {
					if !errors.Is(err, ErrVacuumUnsupported) || d.state.Load() != before || d.idx.Load() != index {
						t.Fatalf("unsupported online vacuum changed state or returned %v", err)
					}
					return false
				}
				if err != nil {
					t.Fatal(err)
				}
				if d.idx.Load() == index {
					t.Fatal("vacuum did not replace pager generation")
				}
				return true
			}
			seedReplaced := vacuumOnline()
			seedSnap := d.AcquireSnapshot()
			seedDescriptor, err := seedSnap.GetAtRoot(seedSnap.state.SystemRootPageID, []byte(vacuumTestCollectionRootKey))
			seedDescriptor = append([]byte(nil), seedDescriptor...)
			_ = seedSnap.Close()
			if err != nil || len(seedDescriptor) != 8 {
				t.Fatalf("vacuum seed descriptor=%x err=%v", seedDescriptor, err)
			}
			if got := binary.BigEndian.Uint64(seedDescriptor); seedReplaced && got == seedCollectionRoot {
				t.Fatalf("seed vacuum did not relocate collection root %d", got)
			}
			vacuumSystemManifestFiles(t, d, true)
			// Force the subject online vacuum to relocate a collection too,
			// before reopening the original source and its exact dependencies.
			snap := d.AcquireSnapshot()
			descriptor, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(vacuumTestCollectionRootKey))
			descriptor = append([]byte(nil), descriptor...)
			_ = snap.Close()
			if err != nil || len(descriptor) != 8 {
				t.Fatalf("seed descriptor=%x err=%v", descriptor, err)
			}
			subjectCollectionRoot := moveCollection(binary.BigEndian.Uint64(descriptor), vacuumTestCollectionRootKey)
			closeDB()
			if t.Failed() {
				return
			}
			openDB()
			vacuumSystemManifestFiles(t, d, false)
			subjectReplaced := false
			if mode == "online" {
				// The base interface does not require optional segment reporting.
				d.SetLeafPageLog(&countingLeafPageLog{inner: log})
				subjectReplaced = vacuumOnline()
				d.SetLeafPageLog(log)
			} else {
				closeDB()
				if t.Failed() {
					return
				}
				fp := vacuumFailpoint(mode)
				if mode == "offline" {
					fp = vacuumFailNone
				}
				err := vacuumIndexOffline(opts, fp)
				if fp == vacuumFailNone {
					if err != nil {
						t.Fatal(err)
					}
				} else if !errors.Is(err, errVacuumFailpoint) {
					t.Fatalf("failpoint=%s error=%v", fp, err)
				}
				openDB()
				subjectReplaced = mode == "offline" || mode == string(vacuumFailAfterRenameOld) || mode == string(vacuumFailAfterRenameNew)
			}
			vacuumSystemPagerPointers(t, d, d.State().SystemRootPageID)
			vacuumSystemManifestFiles(t, d, mode == "online")
			verify := func(updated bool) string {
				t.Helper()
				vacuumSystemPagerPointers(t, d, d.State().SystemRootPageID)
				verifyVacuumCollectionRootDescriptor(t, d, vacuumTestCollectionRootKey)
				snap := d.AcquireSnapshot()
				if snap == nil {
					t.Fatal("missing snapshot")
				}
				defer snap.Close()
				if len(collectLeafRefIDsFromRoot(t, d, snap.state.RootPageID)) == 0 {
					t.Fatal("USER root lost DATA log-leaf activation")
				}
				for i := 0; i < 256; i++ {
					key := []byte(fmt.Sprintf("user-data-%04d", i))
					got, err := snap.Get(key)
					if err != nil || string(got) != "uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu" {
						t.Fatalf("USER key=%q value=%q err=%v", key, got, err)
					}
				}
				for i := 0; i < 2048; i++ {
					want := fmt.Sprintf("value-%04d", i)
					if updated && i == 1024 {
						want = "updated"
					}
					got, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(fmt.Sprintf("sys/%04d", i)))
					if err != nil || string(got) != want {
						t.Fatalf("catalog key=%d got=%q want=%q err=%v", i, got, want, err)
					}
				}
				descriptor, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(vacuumTestCollectionRootKey))
				if err != nil {
					t.Fatal(err)
				}
				return string(descriptor)
			}
			descriptorText := verify(false)
			if subjectReplaced {
				if got := binary.BigEndian.Uint64([]byte(descriptorText)); got == subjectCollectionRoot {
					t.Fatalf("subject vacuum did not relocate collection root %d", got)
				}
			}
			held := d.AcquireSnapshot()
			if held == nil {
				t.Fatal("missing held pre-update snapshot")
			}
			defer func() {
				if held != nil {
					_ = held.Close()
				}
			}()
			kvs := append(systemRangeKVs(2048, map[int]string{1024: "updated"}), vacuumTestCollectionRootKey, descriptorText)
			before := d.systemRootPublishStatsSnapshot()
			if _, err := d.PublishSystemRootIterator(mustFrozenSystemMemtable(t, kvs...).NewIterator(nil, nil)); err != nil {
				t.Fatalf("warm system update: %v", err)
			}
			after := d.systemRootPublishStatsSnapshot()
			if after.warmNativeApplyAttempts != before.warmNativeApplyAttempts+1 || after.warmRebuildFallbacks != before.warmRebuildFallbacks {
				t.Fatalf("warm publication before=%+v after=%+v", before, after)
			}
			verify(true)
			old, err := held.GetAtRoot(held.state.SystemRootPageID, []byte("sys/1024"))
			if err != nil || string(old) != "value-1024" {
				t.Fatalf("held pre-update value=%q err=%v", old, err)
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			held = nil
			closeDB()
			if t.Failed() {
				return
			}
			openDB()
			verify(true)
		})
	}
}

// Check the actual immutable manifest tokens, not the compatibility view.
func vacuumSystemManifestFiles(t *testing.T, d *DB, allSlots bool) {
	t.Helper()
	// Keep each record and its original resource owner paired while borrowing
	// tokens; publication swaps the slot and releases the previous set here.
	d.durablePublishMu.Lock()
	defer d.durablePublishMu.Unlock()
	for slot, record := range d.durableRoot.slotRecord {
		if record.CommitSeq == 0 || (!allSlots && uint64(slot) != d.durableRoot.slot) {
			continue
		}
		pointers := vacuumSystemPagerPointers(t, d, record.SystemRootPageID)
		var manifest *leafGenerationManifest
		var tokenContext []string
		resources := d.durableRoot.slotResources[slot]
		err := resources.WithScopedTokens(func(tokens []*rootpublication.StableResourceToken) error {
			for _, token := range tokens {
				tokenContext = append(tokenContext, fmt.Sprintf("%s/%s/generation=%d", token.Kind(), token.ResourceID(), token.Generation()))
				if token.Kind() == rootpublication.ResourceValueLog && token.Reachability() == rootpublication.ReachabilityValueLogPointer {
					for id := range pointers {
						if token.ResourceID() == fmt.Sprint(id) {
							delete(pointers, id)
						}
					}
				}
			}
			var err error
			manifest, _, err = loadSelectedDurableLeafGenerationManifestTokens(tokens)
			return err
		})
		if err != nil {
			t.Fatalf("slot %d scoped manifest read: owner=%v tokens=%v: %v", slot, resources.Owner(), tokenContext, err)
		}
		if manifest == nil {
			t.Fatalf("slot %d has no exact manifest: selected=%d record=%+v owner=%v tokens=%v manifest_mode=%v", slot, d.durableRoot.slot, record, resources.Owner(), tokenContext, d.leafGenerationManifestStore.mode)
		}
		userLeaves := collectLeafRefIDsFromRoot(t, d, record.UserRootPageID)
		if len(userLeaves) == 0 {
			t.Fatalf("slot %d has no USER DATA log leaves", slot)
		}
		for ptr := range userLeaves {
			id := page.ValueLogSegmentID(ptr.FileID)
			if !manifest.hasNonDeletedFileID(id) {
				t.Fatalf("slot %d manifest %d omits USER DATA leaf file %d", slot, manifest.ManifestRevision, id)
			}
		}
		if len(pointers) != 0 {
			t.Fatalf("slot %d omits original SYSTEM value-pointer dependencies: %v", slot, pointers)
		}
	}
}

// Pager child closure is independent of persistent pointer values in its leaves.
func vacuumSystemPagerPointers(t *testing.T, d *DB, rootID uint64) map[uint32]struct{} {
	t.Helper()
	files := make(map[uint32]struct{})
	leaves := 0
	err := leafrefscan.Walk(context.Background(), rootID, d.Pager().Get, func(id uint64, n node.Node) error {
		if !n.VerifyChecksum() {
			return fmt.Errorf("SYSTEM page %d checksum mismatch", id)
		}
		if n.Type() != page.PageTypeLeaf {
			return nil
		}
		leaves++
		for i := uint16(0); i < n.Count(); i++ {
			_, _, ptr, flags, err := n.GetLeafEntryView(i)
			if err != nil {
				return err
			}
			if flags&node.FlagPointer != 0 {
				files[page.ValueLogSegmentID(ptr.FileID)] = struct{}{}
			}
		}
		return nil
	}, func(ptr page.LeafLogPtr) error {
		return fmt.Errorf("SYSTEM child points into leaf log: %+v", ptr)
	})
	if err != nil || leaves < 2 {
		t.Fatalf("SYSTEM pager closure leaves=%d want multiple: %v", leaves, err)
	}
	return files
}
