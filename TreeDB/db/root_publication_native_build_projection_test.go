package db

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/tree"
)

func openNativeBuildProjectionTestDB(t *testing.T, dir string) (*DB, *registeredLeafPageLog) {
	t.Helper()
	opts := Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		DisableBackgroundPrune:     true,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	log := &registeredLeafPageLog{db: d, dir: d.dir}
	if err := log.ensureWriter(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	d.SetLeafPageLog(log)
	return d, log
}

func rotateProjectionTestLeafLog(t *testing.T, d *DB, log *registeredLeafPageLog, seq uint32) uint32 {
	t.Helper()
	prev := log.fileID
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, seq)
	if err != nil {
		t.Fatal(err)
	}
	log.fileID = fileID
	log.path = filepath.Join(LeafLogDirPath(d.dir), fmt.Sprintf("value-l%d-%06d.log", rewriteLeafLogLaneID, seq))
	log.w, err = valuelog.NewWriter(log.path, log.fileID)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.RegisterValueLogSegment(log.path, log.fileID); err != nil {
		t.Fatal(err)
	}
	return prev
}

func finalizeNativeBuildGroupEmptyCarrier(t *testing.T, d *DB, group *RootPublicationBuildGroup) {
	t.Helper()
	carrier := d.NewPhysicalBatch().(*Batch)
	defer carrier.Close()
	if err := carrier.SetRootPublicationBuildGroup(group, true); err != nil {
		t.Fatal(err)
	}
	if err := carrier.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRootPublicationNativeBuildGroupAvoidsCandidateScan(t *testing.T) {
	dir := t.TempDir()
	d, log := openNativeBuildProjectionTestDB(t, dir)
	closed := false
	t.Cleanup(func() {
		if !closed {
			closeVacuumTestLeafPageLog(t, d, log)
			_ = d.Close()
		}
	})

	// Seed 6 (>4!) collection domains so we exceed the fixed [4]uint64 nativeBuiltRoots ring.
	const domainCount = 6
	const keysPerDomain = 32
	roots := make([]uint64, domainCount)
	descriptorKey := func(domain int) []byte {
		return []byte(fmt.Sprintf("%sdomain-%02d", collectionRootDescriptorPrefix, domain))
	}

	applyRound := func(round int) {
		t.Helper()
		group, err := d.BeginRootPublicationBuildGroup()
		if err != nil {
			t.Fatal(err)
		}
		defer group.Close()

		nextRoots := make([]uint64, domainCount)
		for dom := 0; dom < domainCount; dom++ {
			delta := batch.New(nil, 1024)
			for k := 0; k < keysPerDomain; k++ {
				key := []byte(fmt.Sprintf("d%02d/k%04d", dom, k))
				val := bytes.Repeat([]byte{byte('a' + (round+dom)%26)}, 96)
				if err := delta.Set(key, val); err != nil {
					_ = delta.Close()
					t.Fatal(err)
				}
			}
			newRoot, err := group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{
				BaseRoot: roots[dom],
				Delta:    delta,
			})
			_ = delta.Close()
			if err != nil {
				t.Fatal(err)
			}
			nextRoots[dom] = newRoot
		}

		sysDelta := batch.New(nil, 1024)
		for dom := 0; dom < domainCount; dom++ {
			if err := sysDelta.Set(descriptorKey(dom), encodeMaintenanceRootID(nextRoots[dom])); err != nil {
				_ = sysDelta.Close()
				t.Fatal(err)
			}
		}
		err = group.BuildNativeSystemDelta(sysDelta)
		_ = sysDelta.Close()
		if err != nil {
			t.Fatal(err)
		}

		finalizeNativeBuildGroupEmptyCarrier(t, d, group)
		copy(roots, nextRoots)
	}

	// Warmup round 0 creates initial roots and descriptors from empty base.
	applyRound(0)
	// Warmup round 1 establishes steady-state base closure.
	applyRound(1)
	assertCandidateTrackerMatchesFullScan(t, d)

	var scans atomic.Int64
	restore := d.SetScanCandidateExternalReferencesHookForTesting(func() {
		scans.Add(1)
	})
	defer restore()

	// Steady-state updates across all 6 (>4) domains with empty raw carrier batch.
	applyRound(2)
	applyRound(3)
	if got := scans.Load(); got != 0 {
		t.Fatalf("steady-state 6-domain native build group triggered %d candidate scans, want 0", got)
	}
	assertCandidateTrackerMatchesFullScan(t, d)

	// Rotate the leaf log segment and run another native build group;
	// finalizeLocked must admit the new outer-leaf segment without falling back to a scan.
	rotateProjectionTestLeafLog(t, d, log, 2)
	applyRound(4)
	if got := scans.Load(); got != 0 {
		t.Fatalf("post-rotation 6-domain native build group triggered %d candidate scans, want 0", got)
	}
	assertCandidateTrackerMatchesFullScan(t, d)

	// Run LeafGenerationGC and verify all keys in all 6 roots survive GC + reopen.
	if _, err := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}

	closeVacuumTestLeafPageLog(t, d, log)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	reopened, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		DisableBackgroundPrune:     true,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	snap := reopened.AcquireSnapshot()
	defer snap.Close()
	for dom := 0; dom < domainCount; dom++ {
		wantVal := bytes.Repeat([]byte{byte('a' + (4+dom)%26)}, 96)
		tr := tree.New(snap.idx.pager, &snap.reader, roots[dom])
		for k := 0; k < keysPerDomain; k++ {
			key := []byte(fmt.Sprintf("d%02d/k%04d", dom, k))
			got, err := tr.Get(key)
			if err != nil || !bytes.Equal(got, wantVal) {
				t.Fatalf("reopened dom=%d key=%q: got=%x err=%v", dom, key, got, err)
			}
		}
	}
}

func TestRootPublicationNativeBuildGroupSystemDeltaNoDescriptorChangeAvoidsScan(t *testing.T) {
	d, log := openNativeBuildProjectionTestDB(t, t.TempDir())
	defer func() {
		closeVacuumTestLeafPageLog(t, d, log)
		_ = d.Close()
	}()

	writeMeta := func(val string) {
		t.Helper()
		group, err := d.BeginRootPublicationBuildGroup()
		if err != nil {
			t.Fatal(err)
		}
		defer group.Close()
		sysDelta := batch.New(nil, 1024)
		if err := sysDelta.Set([]byte("catalog/meta/version"), []byte(val)); err != nil {
			_ = sysDelta.Close()
			t.Fatal(err)
		}
		err = group.BuildNativeSystemDelta(sysDelta)
		_ = sysDelta.Close()
		if err != nil {
			t.Fatal(err)
		}
		finalizeNativeBuildGroupEmptyCarrier(t, d, group)
	}

	writeMeta("v1")
	writeMeta("v2")

	var scans atomic.Int64
	restore := d.SetScanCandidateExternalReferencesHookForTesting(func() {
		scans.Add(1)
	})
	defer restore()

	writeMeta("v3")
	writeMeta("v4")
	if got := scans.Load(); got != 0 {
		t.Fatalf("non-descriptor system delta triggered %d candidate scans, want 0", got)
	}
	assertCandidateTrackerMatchesFullScan(t, d)
}

func TestRootPublicationNativeBuildGroupUncoveredDescriptorFailsClosed(t *testing.T) {
	for _, tc := range []string{"delete_descriptor", "uncovered_descriptor_root", "unattached_native_root"} {
		t.Run(tc, func(t *testing.T) {
			d, log := openNativeBuildProjectionTestDB(t, t.TempDir())
			defer func() {
				closeVacuumTestLeafPageLog(t, d, log)
				_ = d.Close()
			}()

			descKeyA := []byte(collectionRootDescriptorPrefix + "col-a")
			descKeyB := []byte(collectionRootDescriptorPrefix + "col-b")

			seedGroup, err := d.BeginRootPublicationBuildGroup()
			if err != nil {
				t.Fatal(err)
			}
			deltaA := batch.New(nil, 1024)
			_ = deltaA.Set([]byte("a/1"), bytes.Repeat([]byte("A"), 128))
			rootA, err := seedGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: deltaA})
			_ = deltaA.Close()
			if err != nil {
				t.Fatal(err)
			}
			deltaB := batch.New(nil, 1024)
			_ = deltaB.Set([]byte("b/1"), bytes.Repeat([]byte("B"), 128))
			rootB, err := seedGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: deltaB})
			_ = deltaB.Close()
			if err != nil {
				t.Fatal(err)
			}
			sysSeed := batch.New(nil, 1024)
			_ = sysSeed.Set(descKeyA, encodeMaintenanceRootID(rootA))
			_ = sysSeed.Set(descKeyB, encodeMaintenanceRootID(rootB))
			if err := seedGroup.BuildNativeSystemDelta(sysSeed); err != nil {
				_ = sysSeed.Close()
				t.Fatal(err)
			}
			_ = sysSeed.Close()
			finalizeNativeBuildGroupEmptyCarrier(t, d, seedGroup)

			var scans atomic.Int64
			restore := d.SetScanCandidateExternalReferencesHookForTesting(func() {
				scans.Add(1)
			})
			defer restore()

			group, err := d.BeginRootPublicationBuildGroup()
			if err != nil {
				t.Fatal(err)
			}
			defer group.Close()

			switch tc {
			case "delete_descriptor":
				sysDelta := batch.New(nil, 1024)
				_ = sysDelta.Delete(descKeyB)
				err = group.BuildNativeSystemDelta(sysDelta)
				_ = sysDelta.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "uncovered_descriptor_root":
				// Point descKeyB to rootA (alias / uncovered transition).
				sysDelta := batch.New(nil, 1024)
				_ = sysDelta.Set(descKeyB, encodeMaintenanceRootID(rootA))
				err = group.BuildNativeSystemDelta(sysDelta)
				_ = sysDelta.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "unattached_native_root":
				// Build a replacement root for rootA, but never attach it in BuildNativeSystemDelta.
				deltaNext := batch.New(nil, 1024)
				_ = deltaNext.Set([]byte("a/1"), bytes.Repeat([]byte("Z"), 128))
				_, err = group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{
					BaseRoot: rootA,
					Delta:    deltaNext,
				})
				_ = deltaNext.Close()
				if err != nil {
					t.Fatal(err)
				}
			}

			finalizeNativeBuildGroupEmptyCarrier(t, d, group)
			if got := scans.Load(); got == 0 {
				t.Fatalf("%s: expected fail-closed candidate scan (> 0), got 0", tc)
			}
			assertCandidateTrackerMatchesFullScan(t, d)
		})
	}
}

func TestRootPublicationNativeBuildGroupNonEmptyRawBatchPreservesOrdinaryProducer(t *testing.T) {
	d, log := openNativeBuildProjectionTestDB(t, t.TempDir())
	defer func() {
		closeVacuumTestLeafPageLog(t, d, log)
		_ = d.Close()
	}()

	writeRaw := func(round int) {
		t.Helper()
		b := d.NewBatch()
		defer b.Close()
		for i := 0; i < 64; i++ {
			if err := b.Set([]byte(fmt.Sprintf("raw/%04d", i)), bytes.Repeat([]byte{byte(round)}, 128)); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatal(err)
		}
	}
	writeRaw(1)
	writeRaw(2)
	assertOrdinaryRawCensus(t, d)

	group, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()

	// Non-empty raw batch inside a build group must preserve ordinaryRawProducer = true
	// and exact raw census tracking.
	rawBatch := d.NewPhysicalBatch().(*Batch)
	for i := 0; i < 64; i++ {
		if err := rawBatch.Set([]byte(fmt.Sprintf("raw/%04d", i)), bytes.Repeat([]byte{3}, 128)); err != nil {
			_ = rawBatch.Close()
			t.Fatal(err)
		}
	}
	if err := rawBatch.SetRootPublicationBuildGroup(group, false); err != nil {
		_ = rawBatch.Close()
		t.Fatal(err)
	}
	if err := rawBatch.Write(); err != nil {
		_ = rawBatch.Close()
		t.Fatal(err)
	}
	_ = rawBatch.Close()

	if group.vlogRefDelta == nil || !group.vlogRefDelta.ordinaryRawProducer {
		t.Fatalf("non-empty raw batch in build group did not set ordinaryRawProducer=true: delta=%+v", group.vlogRefDelta)
	}

	var scans atomic.Int64
	restore := d.SetScanCandidateExternalReferencesHookForTesting(func() {
		scans.Add(1)
	})
	defer restore()

	// Final empty carrier batch must preserve ordinaryRawProducer=true and rawExact=true.
	finalizeNativeBuildGroupEmptyCarrier(t, d, group)
	if got := scans.Load(); got != 0 {
		t.Fatalf("ordinary raw build group + empty carrier triggered %d scans, want 0", got)
	}
	assertOrdinaryRawCensus(t, d)
	assertCandidateTrackerMatchesFullScan(t, d)
}

func TestRootPublicationNativeBuildGroupMidGroupLeafRotationFailsClosed(t *testing.T) {
	d, log := openNativeBuildProjectionTestDB(t, t.TempDir())
	defer func() {
		closeVacuumTestLeafPageLog(t, d, log)
		_ = d.Close()
	}()

	descKeyA := []byte(collectionRootDescriptorPrefix + "rot-a")
	descKeyB := []byte(collectionRootDescriptorPrefix + "rot-b")

	// Seed two collection roots in segment 1.
	seedGroup, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	dA := batch.New(nil, 1024)
	_ = dA.Set([]byte("a/1"), bytes.Repeat([]byte("A"), 128))
	rootA, err := seedGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: dA})
	_ = dA.Close()
	if err != nil {
		t.Fatal(err)
	}
	dB := batch.New(nil, 1024)
	_ = dB.Set([]byte("b/1"), bytes.Repeat([]byte("B"), 128))
	rootB, err := seedGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: dB})
	_ = dB.Close()
	if err != nil {
		t.Fatal(err)
	}
	sysSeed := batch.New(nil, 1024)
	_ = sysSeed.Set(descKeyA, encodeMaintenanceRootID(rootA))
	_ = sysSeed.Set(descKeyB, encodeMaintenanceRootID(rootB))
	if err := seedGroup.BuildNativeSystemDelta(sysSeed); err != nil {
		_ = sysSeed.Close()
		t.Fatal(err)
	}
	_ = sysSeed.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, seedGroup)

	// Mid-group rotation: start on seg 1, rotate to seg 2 for rootA, rotate to seg 3
	// for rootB (mirroring COW mode where intermediate rotations are registered
	// without being retained in CreatedLeafPageLogSegmentsSnapshot).
	var scans atomic.Int64
	restore := d.SetScanCandidateExternalReferencesHookForTesting(func() {
		scans.Add(1)
	})
	defer restore()

	group, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()

	rotateProjectionTestLeafLog(t, d, log, 2)
	seg2 := log.fileID
	dA2 := batch.New(nil, 1024)
	_ = dA2.Set([]byte("a/1"), bytes.Repeat([]byte("C"), 128))
	rootA2, err := group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: rootA, Delta: dA2})
	_ = dA2.Close()
	if err != nil {
		t.Fatal(err)
	}

	rotateProjectionTestLeafLog(t, d, log, 3)
	seg3 := log.fileID
	dB2 := batch.New(nil, 1024)
	_ = dB2.Set([]byte("b/1"), bytes.Repeat([]byte("D"), 128))
	rootB2, err := group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: rootB, Delta: dB2})
	_ = dB2.Close()
	if err != nil {
		t.Fatal(err)
	}

	sys2 := batch.New(nil, 1024)
	_ = sys2.Set(descKeyA, encodeMaintenanceRootID(rootA2))
	_ = sys2.Set(descKeyB, encodeMaintenanceRootID(rootB2))
	if err := group.BuildNativeSystemDelta(sys2); err != nil {
		_ = sys2.Close()
		t.Fatal(err)
	}
	_ = sys2.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, group)

	if got := scans.Load(); got == 0 {
		t.Fatal("mid-group leaf-log rotation bypassed fail-closed candidate scan")
	}

	// Verify BOTH intermediate seg2 AND current seg3 are retained in the visible closure!
	resources, _, err := d.rootPublication.cloneVisibleResourcesWithWorkMax(-1)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	diag, err := resources.AcquirePhysicalDiagnostics()
	if err != nil {
		t.Fatal(err)
	}
	retained := make(map[uint32]struct{})
	for _, desc := range diag.Physical() {
		if desc.Kind == rootpublication.ResourceOuterLeafLog {
			retained[uint32(desc.Generation)] = struct{}{}
		}
	}
	if err := diag.CloseWithError(); err != nil {
		t.Fatal(err)
	}
	if _, ok := retained[seg2]; !ok {
		t.Fatalf("intermediate rotated segment %d missing from visible closure %v", seg2, retained)
	}
	if _, ok := retained[seg3]; !ok {
		t.Fatalf("current rotated segment %d missing from visible closure %v", seg3, retained)
	}

	// Next steady-state build group without mid-group rotation must execute 0 scans.
	scans.Store(0)
	steady, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer steady.Close()
	dA3 := batch.New(nil, 1024)
	_ = dA3.Set([]byte("a/1"), bytes.Repeat([]byte("E"), 128))
	rootA3, err := steady.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: rootA2, Delta: dA3})
	_ = dA3.Close()
	if err != nil {
		t.Fatal(err)
	}
	sys3 := batch.New(nil, 1024)
	_ = sys3.Set(descKeyA, encodeMaintenanceRootID(rootA3))
	if err := steady.BuildNativeSystemDelta(sys3); err != nil {
		_ = sys3.Close()
		t.Fatal(err)
	}
	_ = sys3.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, steady)
	if got := scans.Load(); got != 0 {
		t.Fatalf("steady-state group after rotation triggered %d scans, want 0", got)
	}
	assertCandidateTrackerMatchesFullScan(t, d)
}
