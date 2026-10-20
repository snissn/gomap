package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// Keep an older outer scan alive, then select a different exact current Set.
// Its only initial reference is publication, so the interleaved refresh really
// exercises the consumer's own borrow rather than the outer scan's Set.
func TestCurrentStateValueLogConsumersRetainExactSet(t *testing.T) {
	for _, stage := range []string{"rewrite-user-optimistic", "rewrite-user-serialized", "rewrite-system", "rewrite-collection", "compact-index", "ordered-root", "dictionary", "audit-topology", "compact-carry-plan", "compact-carry-advance"} {
		t.Run(stage, func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true})
			if err != nil {
				t.Fatal(err)
			}
			defer closeNoErr(t, d)
			leaf := &rewriteTestLeafPageLog{db: d, dir: d.dir}
			d.SetLeafPageLog(leaf)
			defer func() {
				if err := leaf.Close(); err != nil {
					t.Error(err)
				}
			}()
			b := d.NewBatch().(*Batch)
			for i := 0; i < 128; i++ {
				if err = b.Set([]byte(fmt.Sprintf("key/%03d", i)), bytes.Repeat([]byte("v"), 64)); err != nil {
					t.Fatal(err)
				}
			}
			if err = errors.Join(b.WriteSync(), b.Close()); err != nil {
				t.Fatal(err)
			}
			table := mustFrozenSystemMemtable(t, "key/000", "value")
			collectionRoot, err := d.PublishOrderedRootIterator(0, table.NewIterator(nil, nil))
			if err != nil {
				t.Fatal(err)
			}
			descriptor := mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(collectionRoot))
			if _, err = d.PublishSystemRootIterator(descriptor.NewIterator(nil, nil)); err != nil {
				t.Fatal(err)
			}
			outer := d.AcquireSnapshot()
			if outer == nil {
				t.Fatal("missing outer scan")
			}
			defer outer.Close()
			leaves := 0
			if err = leafrefscan.WalkRoots(context.Background(), []uint64{outer.state.RootPageID}, outer.idx.pager.Get, nil, func(page.LeafLogPtr) error { leaves++; return nil }); err != nil || leaves == 0 {
				t.Fatalf("fixture requires actual outer leaves: leaves=%d err=%v", leaves, err)
			}
			if err = d.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			selected := d.state.Load().ValueLogSet
			if selected == nil || selected == outer.state.ValueLogSet || selected.RefCount.Load() != 1 {
				t.Fatal("current Set is not independently publication-owned")
			}
			var consumed *valuelog.Set
			observed := false
			d.testValueLogStateCapturedHook = func(at string, set *valuelog.Set, root uint64) {
				if at != stage {
					return
				}
				if observed {
					t.Fatal("unexpected repeated capture")
				}
				observed = true
				consumed = set
				if consumed != selected || consumed.RefCount.Load() != 2 {
					t.Fatalf("capture did not pin selected Set before unlock: refs=%d", consumed.RefCount.Load())
				}
				if err := d.RefreshValueLogSet(); err != nil {
					t.Fatal(err)
				}
				if consumed.RefCount.Load() != 1 || len(consumed.Files) == 0 {
					t.Fatal("refresh consumed a live reader's exact Set")
				}
				count := 0
				if err := leafrefscan.WalkRoots(context.Background(), []uint64{root}, outer.idx.pager.Get, nil, func(ptr page.LeafLogPtr) error { count++; _, err := consumed.Read(ptr.ValuePtr()); return err }); err != nil || count == 0 {
					t.Fatalf("held outer leaves unreadable: count=%d err=%v", count, err)
				}
			}
			defer func() { d.testValueLogStateCapturedHook = nil }()
			swaps := []rewriteSwap{{key: []byte("key/000")}}
			switch stage {
			case "rewrite-user-optimistic":
				_, err = d.applyRewriteSwapBatchOptimistic(swaps, false)
			case "rewrite-user-serialized":
				err = d.applyRewriteSwapBatchSerialized(swaps, false)
			case "rewrite-system":
				err = d.applyRewriteSwapBatchToSystemRoot([]rewriteSwap{{key: []byte(maintenanceTestCollectionRootKey)}}, false)
			case "rewrite-collection":
				err = d.applyRewriteSwapBatchToCollectionRoot(&collectionRewriteRootState{descriptorKey: []byte(maintenanceTestCollectionRootKey)}, swaps, false)
			case "compact-index":
				err = d.CompactIndex()
			case "ordered-root":
				_, err = d.PublishOrderedRootIterator(collectionRoot, table.NewIterator(nil, nil))
			case "dictionary":
				d.valueLogDictLookup = func(uint64) ([]byte, error) { return nil, valuelog.ErrMissingDict }
				d.valueLogDictPut = func(context.Context, []byte) (uint64, error) { return 1, nil }
				_, _, _, err = d.prepareCurrentRewriteLeafDict()
			case "audit-topology":
				err = d.prepareCompactStorageAuditTopology()
			case "compact-carry-plan", "compact-carry-advance":
				state := d.state.Load()
				key, ok := leafGenerationPlanStateKey(state, LeafGenerationPlanOptions{})
				if !ok || state.LeafGenerations == nil {
					t.Fatal("missing plan state")
				}
				carry := &compactStorageLeafPackPlanState{key: key, valid: true, live: leafGenerationLiveScanStats{Generations: make(map[uint64]leafGenerationLiveTotals)}}
				if stage == "compact-carry-plan" {
					_, err = d.compactStorageLeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}, carry)
				} else {
					err = carry.advance(d, LeafGenerationPackStats{}, leafGenerationPackCarryResult{sourceStateKey: key, publishedState: state})
				}
			}
			d.testValueLogStateCapturedHook = nil
			if err != nil {
				t.Fatal(err)
			}
			if !observed || consumed == nil {
				t.Fatal("actual consumer was not reached")
			}
			if consumed.RefCount.Load() != 0 || consumed.Files != nil {
				t.Fatalf("actual last consumer did not scrub retired Set: refs=%d files=%d", consumed.RefCount.Load(), len(consumed.Files))
			}
			if _, err = outer.Get([]byte("key/000")); err != nil {
				t.Fatal("independent older scan lost its own Set", err)
			}
		})
	}
}

func TestTopologyPublicationPreservesPrimaryRootLifetime(t *testing.T) {
	for _, route := range []string{"audit", "leaf-same", "leaf-refresh"} {
		t.Run(route, func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if err = d.SetSync([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			before := d.state.Load()
			root := before.primaryRoot
			if root == nil {
				t.Fatal("missing actual PRIMARY root")
			}
			arena := root.arena
			switch route {
			case "audit":
				appendPointersInNewSegment(t, d.dir, 0, 1, 700001, 1, func(int) []byte { return []byte("registered") })
				if err = d.valueLogManager.Refresh(); err != nil {
					t.Fatal(err)
				}
				err = d.publishCompactStorageValueLogSet(d.valueLogManager.CurrentSetNoRefresh())
			case "leaf-same":
				err = d.publishLeafGenerationState(false)
			case "leaf-refresh":
				err = d.publishLeafGenerationState(true)
			}
			if err != nil {
				t.Fatal(err)
			}
			if d.state.Load() == before || d.state.Load().primaryRoot != root {
				t.Fatal("topology copy lost or replaced SAME current-root edge")
			}
			snap, err := d.AcquireSnapshotWithAllocationAdmission(func(s SnapshotAllocationSizes) error {
				if s.PrimaryMetadata != arena.MetadataOwner() || s.PrimaryRoot == 0 {
					t.Fatal("capture omitted original PRIMARY admission")
				}
				return nil
			})
			if err != nil || snap == nil {
				t.Fatal(err)
			}
			budget, e := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if e != nil {
				t.Fatal(e)
			}
			defer budget.Close()
			enrollment, e := retainedalloc.EnrollPair(arena.MetadataOwner(), d.valueLogIdentityPins.MetadataOwner(), budget)
			if e != nil {
				t.Fatal(e)
			}
			if e = snap.AdoptPrimaryMetadataEnrollment(enrollment); e != nil {
				t.Fatal(e)
			}
			if budget.Stats().ExternalBytes == 0 {
				t.Fatal("actual PRIMARY enrollment has no charge")
			}
			if snap.primaryRoot == nil || snap.primaryRoot.ref != root.ref {
				t.Fatal("snapshot did not acquire original current root")
			}
			it, err := snap.Iterator(nil, nil)
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < 6; i++ {
				if err = d.SetSync([]byte(fmt.Sprintf("advance/%d", i)), []byte("v")); err != nil {
					t.Fatal(err)
				}
			}
			w := &iterator.OrdinalScanWork{RecordLimit: 1000, ByteLimit: 1 << 24}
			if _, ready, err := arena.BorrowReadRootCertificateV6(root.ref, w); err != nil || !ready {
				t.Fatal("live iterator lost original root after slot/parent advancement", err)
			}
			if !it.Valid() || string(it.Value()) != "value" {
				t.Fatalf("retained iterator unreadable: valid=%v key=%q value=%q err=%v", it.Valid(), it.Key(), it.Value(), it.Error())
			}
			if err = snap.Close(); err != nil {
				t.Fatal(err)
			}
			if it.Valid() || !errors.Is(it.Error(), ErrClosed) {
				t.Fatal("Snapshot.Close did not invalidate public iterator")
			}
			w = &iterator.OrdinalScanWork{RecordLimit: 1000, ByteLimit: 1 << 24}
			if _, ready, e := arena.BorrowReadRootCertificateV6(root.ref, w); e != nil || !ready {
				t.Fatal("invalidated retained iterator lost original ownership before actual Close", e)
			}
			if budget.Stats().ExternalBytes == 0 {
				t.Fatal("invalidated iterator refunded live PRIMARY enrollment")
			}
			if err = it.Close(); err != nil {
				t.Fatal(err)
			}
			for {
				ready, progress, e := arena.ReleaseStep(nil)
				if e != nil {
					t.Fatal(e)
				}
				if ready && !progress {
					break
				}
			}
			w = &iterator.OrdinalScanWork{RecordLimit: 1000, ByteLimit: 1 << 24}
			if _, ready, e := arena.BorrowReadRootCertificateV6(root.ref, w); ready || e == nil {
				t.Fatal("superseded original root did not drain")
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			if budget.Stats().ExternalBytes != 0 || budget.Stats().ExternalLeases != 0 {
				t.Fatalf("final governor did not drain: %+v", budget.Stats())
			}
			if arena.MetadataOwner().Bytes() != 0 {
				t.Fatalf("final original PRIMARY owner retained %d bytes", arena.MetadataOwner().Bytes())
			}
		})
	}
}

// Both ordinary and admitted captures use rootReuseMu without db.mu. Each
// formerly omitted topology publisher must wait until that real capture pins.
func TestTopologyPublishersWaitForSnapshotSetCapture(t *testing.T) {
	for _, route := range []string{"audit", "leaf-same", "leaf-refresh"} {
		t.Run(route, func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			ptrs := appendPointersInNewSegment(t, d.dir, 0, 1, 800001, 1, func(int) []byte { return []byte("pinned-persistent") })
			if err = d.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			b := d.NewBatch().(*Batch)
			if err = b.SetPointer([]byte("key"), ptrs[0]); err != nil {
				t.Fatal(err)
			}
			if err = errors.Join(b.WriteSync(), b.Close()); err != nil {
				t.Fatal(err)
			}
			beforeState := d.state.Load()
			before := beforeState.ValueLogSet
			if route == "audit" {
				appendPointersInNewSegment(t, d.dir, 0, 2, 800002, 1, func(int) []byte { return []byte("new-topology") })
				if err = d.valueLogManager.Refresh(); err != nil {
					t.Fatal(err)
				}
			}
			ordinary := d.AcquireSnapshot()
			if ordinary == nil || ordinary.state.ValueLogSet != before {
				t.Fatal("ordinary capture did not select exact Set")
			}

			captured := make(chan *Snapshot, 1)
			captureErr := make(chan error, 1)
			selected := make(chan struct{})
			resume := make(chan struct{})
			go func() {
				snap, e := d.AcquireSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error {
					close(selected)
					<-resume
					return nil
				})
				captured <- snap
				captureErr <- e
			}()
			<-selected // actual capture selected its view while holding rootReuseMu.
			done := make(chan error, 1)
			started := make(chan struct{})
			go func() {
				close(started)
				switch route {
				case "audit":
					done <- d.publishCompactStorageValueLogSet(d.valueLogManager.CurrentSetNoRefresh())
				case "leaf-same":
					done <- d.publishLeafGenerationState(false)
				case "leaf-refresh":
					done <- d.publishLeafGenerationState(true)
				}
			}()
			<-started
			select {
			case e := <-done:
				close(resume)
				t.Fatalf("publisher crossed active capture: %v", e)
			case <-time.After(20 * time.Millisecond):
			}
			close(resume)
			snap := <-captured
			if err = <-captureErr; err != nil || snap == nil {
				t.Fatalf("actual admitted capture: %v", err)
			}
			if snap.state.ValueLogSet != before {
				t.Fatal("capture selected a different Set")
			}
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			if d.state.Load() == beforeState {
				t.Fatal("actual topology publisher did not replace state")
			}
			if value, e := ordinary.Get([]byte("key")); e != nil || string(value) != "pinned-persistent" {
				t.Fatalf("public Snapshot lost selected pointer: %q %v", value, e)
			}
			if before.RefCount.Load() < 1 {
				t.Fatal("publisher retired captured Set")
			}
			if err = snap.Close(); err != nil {
				t.Fatal(err)
			}
			if value, e := ordinary.Get([]byte("key")); e != nil || string(value) != "pinned-persistent" {
				t.Fatalf("ordinary last reader lost Set: %q %v", value, e)
			}
			if err = ordinary.Close(); err != nil {
				t.Fatal(err)
			}
			// On leaf-same the publication intentionally retains the SAME Set.
			if route != "leaf-same" && (before.RefCount.Load() != 0 || before.Files != nil) {
				t.Fatal("replaced nonempty Set did not finally retire")
			}
		})
	}
}
