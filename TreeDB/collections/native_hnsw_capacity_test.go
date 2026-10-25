package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Actual public owners exercise descriptor growth, deterministic upper layers,
// a degree beyond the legacy eager cap, mutation, and lazy immutable images.
func TestNativeHNSWRetainedCapacity(t *testing.T) {
	for _, m := range []int{4, 16, 65} {
		t.Run(fmt.Sprintf("M%d", m), func(t *testing.T) {
			c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 257, 8)
			defer closeAll()
			if _, err := c.DropVectorIndex(def.Name); err != nil {
				t.Fatal(err)
			}
			def.M, def.EfConstruction, def.EfSearch = m, 137, 93
			if _, err := c.CreateVectorIndex(def); err != nil {
				t.Fatal(err)
			}
			idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
			if err != nil {
				t.Fatal(err)
			}
			witness := func() {
				t.Helper()
				idx.mu.RLock()
				defer idx.mu.RUnlock()
				var concrete uint64
				add := func(n uint64) { concrete += memtable.COWAllocationCharge(n) }
				add(uint64(cap(idx.nodes)) * uint64(unsafe.Sizeof(vectorIndexNode{})))
				upper := false
				for _, plane := range []*VectorIndex{idx, idx.liveDelta} {
					if plane == nil {
						continue
					}
					if plane.vectorRows != nil {
						t.Fatal("native graph retained optional matrix")
					}
					for _, row := range plane.nodes {
						upper = upper || row.level > 0
						add(uint64(cap(row.documentID)))
						add(uint64(cap(row.vector)) * 4)
						add(uint64(cap(row.neighbors)) * uint64(unsafe.Sizeof([]vectorIndexNeighbor{})))
						for layer, neighbors := range row.neighbors {
							if cap(neighbors) != plane.maxNeighborsForLayer(layer)+1 {
								t.Fatalf("layer%d capacity=%d limit=%d", layer, cap(neighbors), idx.maxNeighborsForLayer(layer))
							}
							add(uint64(cap(neighbors)) * uint64(unsafe.Sizeof(vectorIndexNeighbor{})))
						}
					}
				}
				if !upper {
					t.Fatal("fixture did not reach an upper layer")
				}
				if idx.searchViewSpare.Load() != nil {
					t.Fatal("native runtime retained spare graph image")
				}
				if view := idx.searchView.Load(); view != nil {
					add(uint64(cap(view.nodes)) * uint64(unsafe.Sizeof(vectorIndexNode{})))
					for _, row := range view.nodes {
						add(uint64(cap(row.neighbors)) * uint64(unsafe.Sizeof([]vectorIndexNeighbor{})))
						for _, neighbors := range row.neighbors {
							add(uint64(cap(neighbors)) * uint64(unsafe.Sizeof(vectorIndexNeighbor{})))
						}
					}
				}
				scratch := &idx.insertScratch
				scratchBytes := uint64(cap(scratch.visitedEpochs)) * 4
				for _, n := range []int{cap(scratch.queue), cap(scratch.best), cap(scratch.liveBest), cap(scratch.out), cap(scratch.resumeCandidates), cap(scratch.resumeDeferred)} {
					scratchBytes += uint64(n) * uint64(unsafe.Sizeof(vectorIndexCandidate{}))
				}
				if scratchBytes > idx.nativeHNSWConstructionPeak {
					t.Fatalf("retained scratch=%d peak=%d", scratchBytes, idx.nativeHNSWConstructionPeak)
				}
				if concrete+scratchBytes > idx.nativeHNSWCharge {
					t.Fatalf("concrete retained backings=%d scratch=%d charge=%d", concrete, scratchBytes, idx.nativeHNSWCharge)
				}
			}
			witness()
			query := []float32{1, 2, 3, 4, 5, 6, 7, 8}
			if _, _, err = idx.Search(query, VectorIndexSearchOptions{TopK: 7, DisableExactFallback: true}); err != nil {
				t.Fatal(err)
			}
			witness()
			doc, _ := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{8., 7., 6., 5., 4., 3., 2., 1.}}})
			if _, err = c.Insert([]byte("additional"), doc); err != nil {
				t.Fatal(err)
			}
			if idx.searchViewSpare.Load() != nil || idx.searchViewCurrent.Load() {
				t.Fatal("native mutation retained spare or marked old image current")
			}
			if _, _, err = idx.Search(query, VectorIndexSearchOptions{TopK: 7, DisableExactFallback: true}); err != nil {
				t.Fatal(err)
			}
			witness()
			ids, docs := make([][]byte, 32), make([][]byte, 32)
			for i := range ids {
				ids[i] = []byte(fmt.Sprintf("fold-%02d", i))
				docs[i] = doc
			}
			if _, err = c.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			if _, err = idx.SaveSnapshot(); err != nil {
				t.Fatal(err)
			}
			if idx.liveDelta != nil {
				t.Fatal("save did not fold live delta")
			}
			witness()
		})
	}
}

// The direct admitted image must remain byte-compatible with the maintained
// full snapshot codec, including tombstones, edge normalization and doc-map keys.
func TestNativeHNSWEncodedImageCodec(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 257, 8)
	defer closeAll()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	idx.TombstoneDocumentID([]byte("row-00000"))
	status, err := idx.SaveSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := idx.nativeHNSWImagePlan()
	if err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	allocator := read.capture.NativeAllocator()
	imageLease, err := allocator.AdmitNativeBytes(plan.charge)
	if err != nil {
		t.Fatal(err)
	}
	defer imageLease.Close()
	rows, _, _, err := idx.encodeNativeHNSWImage(status.LogicalVersion, plan)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := nativeHNSWImageRetainedCharge(rows)
	if err != nil {
		t.Fatal(err)
	}
	// An independent concrete witness includes the unused original row slots.
	witness := memtable.COWAllocationCharge(uint64(cap(rows)) * uint64(unsafe.Sizeof(memtable.COWMutation{})))
	for _, row := range rows {
		witness += memtable.COWAllocationCharge(uint64(cap(row.Key)))
		witness += memtable.COWAllocationCharge(uint64(cap(row.Value)))
	}
	admitted := memtable.COWAllocationCharge(plan.charge)
	if retained != witness || retained >= admitted {
		t.Fatalf("retained=%d witness=%d admitted=%d", retained, witness, admitted)
	}
	// Encoding has ended, but publication controls and their variable names/
	// canonical command payload remain owned on this same original receipt.
	controlWitness := uint64(64 << 10)
	for _, n := range []int{len("vector-index/") + len(idx.name), len(c.name), 10 + len(c.name) + len(idx.name), len(c.name) + len("/vector-index/") + len(idx.name)} {
		controlWitness += memtable.COWAllocationCharge(uint64(n))
	}
	if plan.control != controlWitness {
		t.Fatalf("publication controls=%d witness=%d", plan.control, controlWitness)
	}
	retained += plan.control
	budget := allocator.(interface{ Stats() memtable.COWStats })
	before := budget.Stats()
	credit := admitted - retained
	if err = imageLease.ShrinkNativeBytes(credit, retained); err != nil {
		t.Fatal(err)
	}
	after := budget.Stats()
	if after.ExternalLeases > before.ExternalLeases || after.ReservedBytes > before.ReservedBytes-credit {
		t.Fatalf("phase refund=%d before=%+v after=%+v", credit, before, after)
	}
	// This is the original receipt, now exactly at its retained floor (plus
	// its original header): no additional byte can be refunded at that floor.
	if err = imageLease.ShrinkNativeBytes(1, retained); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("retained floor accepted another refund: %v", err)
	}
	image := make(map[string][]byte, len(rows))
	for _, row := range rows {
		image[string(row.Key)] = row.Value
	}
	charge, err := idx.nativeHNSWPersistenceCharge()
	if err != nil {
		t.Fatal(err)
	}
	// Test-only reference image uses the unchanged maintained snapshot codec.
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	snapshot, _ := idx.persistSnapshot()
	snapshot.Meta.NativeLogicalVersion = status.LogicalVersion
	table, _, err := buildVectorIndexNativeSnapshotTable(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer resetCollectionRunTable(table)
	table.Freeze()
	it := table.NewIterator(nil, nil)
	defer it.Close()
	count := 0
	for ; it.Valid(); it.Next() {
		got, e := read.namedDomainGet("vector-index/"+def.Name, it.UnsafeKey())
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(got, it.UnsafeValue()) || !bytes.Equal(image[string(it.UnsafeKey())], it.UnsafeValue()) {
			t.Fatalf("record %q differs from maintained codec", it.UnsafeKey())
		}
		if uint64(len(got)) > plan.maxRecord {
			t.Fatalf("record %q exceeds admitted maximum", it.UnsafeKey())
		}
		count++
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if count != table.Len() || count > plan.entries {
		t.Fatalf("records=%d table=%d plan=%d", count, table.Len(), plan.entries)
	}
	t.Logf("direct image admitted=%d retained=%d ended credit=%d max_record=%d records=%d; reference snapshot charge=%d", admitted, retained, credit, plan.maxRecord, count, charge)
}

// Rebuild returns the exact newly saved runtime's public status. An explicit
// later status still inspects saved coverage rather than blessing dirty data.
func TestNativeHNSWRebuildReturnedStatus(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 33, 8)
	defer closeAll()
	status, err := c.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	if status.Definition.Name != def.Name || status.Definition.Field != def.Field || !status.Registered ||
		!status.Loaded || !status.NativeRootLoaded || status.RebuildNeeded || status.Stats.SnapshotDirty ||
		status.Stats.LiveDocs != 33 || status.NativeRootBytes <= 0 || status.NativeRootBytes != status.Stats.BytesDisk ||
		status.RootID != status.Stats.Epoch || status.Duration <= 0 {
		t.Fatalf("rebuilt public status=%+v", status)
	}
	inspected, err := c.VectorIndexStatus(def.Name)
	if err != nil || inspected.NativeRootBytes != status.NativeRootBytes || inspected.RootID != status.RootID ||
		!inspected.NativeRootLoaded || inspected.RebuildNeeded {
		t.Fatalf("explicit persisted inspection=%+v err=%v", inspected, err)
	}
	idx := c.registeredVectorIndex(def.Name)
	if status.RootID != 0 || idx == nil {
		t.Fatalf("fixture must exercise logical Save before physical projection: %+v", status)
	}
	// A non-entry tombstone changes graph rows without changing graph metadata.
	// It must become dirty even though its last successful Save has root zero.
	var nonEntry []byte
	idx.mu.RLock()
	for ordinal, n := range idx.nodes {
		if ordinal != idx.entry && !n.deleted {
			nonEntry = []byte(n.documentID)
			break
		}
	}
	idx.mu.RUnlock()
	if len(nonEntry) == 0 {
		t.Fatal("no non-entry graph row")
	}
	idx.TombstoneDocumentID(nonEntry)
	if stats := idx.Stats(); !stats.SnapshotDirty || stats.LiveDocs != 32 || stats.Epoch != 0 {
		t.Fatalf("root-zero manual tombstone lost dirty state: %+v", stats)
	}
	if saved, e := idx.SaveSnapshot(); e != nil || !saved.Loaded || saved.RootID != 0 {
		t.Fatalf("save manual root-zero mutation: %+v err=%v", saved, e)
	}
	if stats := idx.Stats(); stats.SnapshotDirty || stats.LiveDocs != 32 || stats.Epoch != 0 {
		t.Fatalf("root-zero successful Save did not become clean: %+v", stats)
	}
	doc, err := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{8., 7., 6., 5., 4., 3., 2., 1.}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Insert([]byte("after-rebuild"), doc); err != nil {
		t.Fatal(err)
	}
	inspected, err = c.VectorIndexStatus(def.Name)
	if err != nil || !inspected.Loaded || !inspected.RebuildNeeded || inspected.NativeRootLoaded ||
		inspected.ExactFallbackReason != vectorIndexFallbackStaleDocumentRoot {
		t.Fatalf("explicit inspection blessed unsaved coverage: %+v err=%v", inspected, err)
	}
}
