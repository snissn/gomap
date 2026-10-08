package db

import (
	"bytes"
	"context"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func TestPrimaryLeafPackRetainsComponentsSnapshotAndReopens(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	dir := t.TempDir()
	opts := Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true, IndexPrimaryDirectory: true, IndexOuterLeavesInValueLog: true, LeafPrefixCompression: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true, ValueLog: ValueLogOptions{PointerThreshold: 128}}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			_ = database.Close()
		}
	}()
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	database.SetLeafPageLog(leafLog)
	defer leafLog.Close()
	const count = 192
	oldPointers := appendPointersInNewSegment(t, dir, 0, 1, 1000000, count, func(i int) []byte { return bytes.Repeat([]byte{byte(1 + i%251)}, 256) })
	if err = database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	writeLeafGenerationPointerBatch(t, database, oldPointers, 0)
	_, sourceID := currentLeafSegmentOrFatal(t, leafLog)
	if err = leafLog.rotateLeaf(); err != nil {
		t.Fatal(err)
	}
	newPointers := appendPointersInNewSegment(t, dir, 0, 2, 2000000, count/2, func(i int) []byte { return bytes.Repeat([]byte{byte(252 - i%251)}, 384) })
	if err = database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	writeLeafGenerationPointerBatch(t, database, newPointers, 0)
	// Keep a nonempty exact-key component while genuine leaf-log DATA base pages
	// are rewritten. The component must stay in the primary namespace.
	if err = database.SetSync([]byte("overlay-retained"), []byte("component")); err != nil {
		t.Fatal(err)
	}
	if err = database.DeleteSync([]byte("overlay-absent")); err != nil {
		t.Fatal(err)
	}
	held := database.AcquireSnapshot()
	defer held.Close()
	before := database.State()
	if !primaryarena.IsPage(before.RootPageID) {
		t.Fatalf("not primary: %d", before.RootPageID)
	}
	generation := findLeafGenerationByFileID(t, loadLeafGenerationManifestOrFatal(t, dir), page.ValueLogSegmentID(sourceID))
	stats, err := database.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{GenerationIDs: []uint64{generation.GenerationID}, Force: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.LeafPagesCopied == 0 {
		t.Fatalf("no genuine leaf pack: %+v", stats)
	}
	after := database.State()
	if !primaryarena.IsPage(after.RootPageID) || after.RootPageID == before.RootPageID {
		t.Fatalf("primary pack roots %d -> %d", before.RootPageID, after.RootPageID)
	}
	got, err := held.Get([]byte("overlay-retained"))
	if err != nil || !bytes.Equal(got, []byte("component")) {
		t.Fatalf("held component %q %v", got, err)
	}
	verifyLeafGenerationPointerValues(t, database, oldPointers, newPointers)
	if has, err := held.Has([]byte("overlay-absent")); err != nil || has {
		t.Fatalf("held inline absence: %v %v", has, err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	if err = leafLog.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	verifyLeafGenerationPointerValues(t, database, oldPointers, newPointers)
	if has, err := database.Has([]byte("overlay-absent")); err != nil || has {
		t.Fatalf("reopened inline absence: %v %v", has, err)
	}
	got, err = database.Get([]byte("overlay-retained"))
	if err != nil || !bytes.Equal(got, []byte("component")) {
		t.Fatalf("reopened component %q %v", got, err)
	}
	if err = database.SetSync([]byte("after-pack"), []byte("write")); err != nil {
		t.Fatal(err)
	}
}
