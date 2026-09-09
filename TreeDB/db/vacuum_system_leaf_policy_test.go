package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
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
			if mode == "online" && runtime.GOOS == "windows" {
				t.Skip("online vacuum unsupported on windows")
			}
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
			// Give both recovery slots an application system tree while keeping
			// the target below the padding tree in the source allocation order.
			moveCollection(rootIDs[1], paddingCollectionRootKey)
			seedCollectionRoot := rootIDs[0]
			if err := d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if n := len(collectLeafRefIDsFromRoot(t, d, d.State().SystemRootPageID)); n < 2 {
				t.Fatalf("system leaf refs=%d want multiple", n)
			}
			if all, err := vacuumTreeAllLeafRefsIfComplete(d.Pager(), d.State().SystemRootPageID); err != nil || !all {
				t.Fatalf("initial system leaf policy: all=%v err=%v", all, err)
			}
			seedLeafFiles := make(map[uint32]bool)
			for ptr := range collectLeafRefIDsFromRoot(t, d, d.State().SystemRootPageID) {
				seedLeafFiles[ptr.FileID] = true
			}
			// Seed both recovery slots through the production vacuum path. Its
			// appends stay in the already registered current leaf segment.
			if err := d.VacuumIndexOnline(context.Background()); err != nil {
				t.Fatal(err)
			}
			seedSnap := d.AcquireSnapshot()
			seedDescriptor, err := seedSnap.GetAtRoot(seedSnap.state.SystemRootPageID, []byte(vacuumTestCollectionRootKey))
			_ = seedSnap.Close()
			if err != nil || len(seedDescriptor) != 8 {
				t.Fatalf("vacuum seed descriptor=%x err=%v", seedDescriptor, err)
			}
			if got := binary.BigEndian.Uint64(seedDescriptor); got == seedCollectionRoot {
				t.Fatalf("seed vacuum did not relocate collection root %d", got)
			}
			for ptr := range collectLeafRefIDsFromRoot(t, d, d.State().SystemRootPageID) {
				if !seedLeafFiles[ptr.FileID] {
					t.Fatal("seed vacuum unexpectedly rotated the leaf segment")
				}
			}
			vacuumSystemManifestFiles(t, d, true)
			// Force the subject online vacuum to relocate a collection too,
			// before reopening rotates the leaf writer to a fresh file.
			snap := d.AcquireSnapshot()
			descriptor, err := snap.GetAtRoot(snap.state.SystemRootPageID, []byte(vacuumTestCollectionRootKey))
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
			seedFiles := vacuumSystemManifestFiles(t, d, false)
			if mode == "online" {
				before := d.Pager()
				if err := d.VacuumIndexOnline(context.Background()); err != nil {
					t.Fatal(err)
				}
				if d.Pager() == before {
					t.Fatal("vacuum did not replace pager")
				}
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
			}
			if n := len(collectLeafRefIDsFromRoot(t, d, d.State().SystemRootPageID)); n < 2 {
				t.Fatalf("recovered system leaf refs=%d want multiple", n)
			}
			vacuumSystemManifestFiles(t, d, mode == "online")
			if mode == "online" || mode == "offline" || mode == string(vacuumFailAfterRenameOld) || mode == string(vacuumFailAfterRenameNew) {
				newFile := false
				for ptr := range collectLeafRefIDsFromRoot(t, d, d.State().SystemRootPageID) {
					if _, old := seedFiles[page.ValueLogSegmentID(ptr.FileID)]; !old {
						newFile = true
					}
				}
				if !newFile {
					t.Fatal("subject vacuum did not create a new system leaf segment")
				}
			}
			verify := func(updated bool) string {
				t.Helper()
				if all, err := vacuumTreeAllLeafRefsIfComplete(d.Pager(), d.State().SystemRootPageID); err != nil || !all {
					t.Fatalf("system leaf policy: all=%v err=%v", all, err)
				}
				verifyVacuumCollectionRootDescriptor(t, d, vacuumTestCollectionRootKey)
				snap := d.AcquireSnapshot()
				if snap == nil {
					t.Fatal("missing snapshot")
				}
				defer snap.Close()
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
			if mode == "online" || mode == "offline" || mode == string(vacuumFailAfterRenameOld) || mode == string(vacuumFailAfterRenameNew) {
				if got := binary.BigEndian.Uint64([]byte(descriptorText)); got == subjectCollectionRoot {
					t.Fatalf("subject vacuum did not relocate collection root %d", got)
				}
			}
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
func vacuumSystemManifestFiles(t *testing.T, d *DB, allSlots bool) map[uint32]struct{} {
	t.Helper()
	files := make(map[uint32]struct{})
	for slot, record := range d.durableRoot.slotRecord {
		if record.CommitSeq == 0 || (!allSlots && uint64(slot) != d.durableRoot.slot) {
			continue
		}
		var manifest *leafGenerationManifest
		for _, token := range d.durableRoot.slotResources[slot].Tokens() {
			if token.Kind() != rootpublication.ResourceOuterLeafManifest {
				continue
			}
			if manifest != nil {
				t.Fatalf("slot %d has multiple manifests", slot)
			}
			err := token.WithPinnedFile(func(f *os.File) error {
				data, err := io.ReadAll(io.NewSectionReader(f, 0, int64(token.Frontier().Bytes)))
				if err != nil {
					return err
				}
				manifest, err = decodeLeafGenerationManifest(data, token.ResourceID())
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if manifest.ManifestRevision != token.Generation() {
				t.Fatal("manifest revision mismatch")
			}
		}
		if manifest == nil {
			t.Fatalf("slot %d has no exact manifest", slot)
		}
		for ptr := range collectLeafRefIDsFromRoot(t, d, record.SystemRootPageID) {
			id := page.ValueLogSegmentID(ptr.FileID)
			if !manifest.hasNonDeletedFileID(id) {
				t.Fatalf("slot %d manifest %d omits system leaf file %d", slot, manifest.ManifestRevision, id)
			}
		}
		if uint64(slot) == d.durableRoot.slot {
			for _, gen := range manifest.Generations {
				for _, id := range gen.FileIDs {
					files[id] = struct{}{}
				}
			}
		}
	}
	return files
}
