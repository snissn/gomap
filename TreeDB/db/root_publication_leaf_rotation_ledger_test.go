package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/tree"
)

func visibleOuterLeafFileIDs(t *testing.T, d *DB) map[uint32]struct{} {
	t.Helper()
	resources, _, err := d.rootPublication.cloneVisibleResourcesWithWorkMax(-1)
	if err != nil {
		t.Fatalf("cloneVisibleResourcesWithWorkMax: %v", err)
	}
	defer resources.Release()
	diag, err := resources.AcquirePhysicalDiagnostics()
	if err != nil {
		t.Fatalf("AcquirePhysicalDiagnostics: %v", err)
	}
	retained := make(map[uint32]struct{})
	for _, desc := range diag.Physical() {
		if desc.Kind == rootpublication.ResourceOuterLeafLog {
			retained[uint32(desc.Generation)] = struct{}{}
		}
	}
	if err := diag.CloseWithError(); err != nil {
		t.Fatalf("CloseWithError: %v", err)
	}
	return retained
}

func rotateLedgerTestLeafLogReplacing(t *testing.T, d *DB, log *registeredLeafPageLog, seq uint32) (prevID, newID uint32, newPath string) {
	t.Helper()
	prevID = log.fileID
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, seq)
	if err != nil {
		t.Fatal(err)
	}
	log.fileID = fileID
	log.path = filepath.Join(LeafLogDirPath(d.dir), fmt.Sprintf("value-l%d-%06d.log", rewriteLeafLogLaneID, seq))
	if err := os.MkdirAll(filepath.Dir(log.path), 0o755); err != nil {
		t.Fatal(err)
	}
	log.w, err = valuelog.NewWriter(log.path, log.fileID)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.RegisterValueLogSegmentReplacing(log.path, log.fileID, prevID); err != nil {
		t.Fatal(err)
	}
	log.registered = true
	return prevID, log.fileID, log.path
}

// rotateLedgerTestLeafLogBypassingObserver registers the new leaf segment
// directly on valueLogManager without going through DB.RegisterValueLogSegment*
// so the build-group leaf-segment observer does not record it.
func rotateLedgerTestLeafLogBypassingObserver(t *testing.T, d *DB, log *registeredLeafPageLog, seq uint32) uint32 {
	t.Helper()
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, seq)
	if err != nil {
		t.Fatal(err)
	}
	log.fileID = fileID
	log.path = filepath.Join(LeafLogDirPath(d.dir), fmt.Sprintf("value-l%d-%06d.log", rewriteLeafLogLaneID, seq))
	if err := os.MkdirAll(filepath.Dir(log.path), 0o755); err != nil {
		t.Fatal(err)
	}
	log.w, err = valuelog.NewWriter(log.path, log.fileID)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.valueLogManager.RegisterSegment(log.path, log.fileID); err != nil {
		t.Fatal(err)
	}
	if err := d.valueLogManager.PromoteCurrentWritable(log.fileID); err != nil {
		t.Fatal(err)
	}
	return log.fileID
}

type failingStartLeafPageLog struct {
	*registeredLeafPageLog
	failCurrent atomic.Bool
}

func (l *failingStartLeafPageLog) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if l.failCurrent.Load() {
		return nil, errors.New("injected startLeafSegments error")
	}
	if l.path == "" || l.fileID == 0 {
		return nil, nil
	}
	return []LeafPageLogSegment{{Path: l.path, FileID: l.fileID}}, nil
}

type lazyRegisteredLeafPageLog struct {
	*registeredLeafPageLog
}

func (l *lazyRegisteredLeafPageLog) Flush() error {
	if l.w == nil {
		return nil
	}
	return l.w.Flush()
}

func (l *lazyRegisteredLeafPageLog) Sync() error {
	if l.w == nil {
		return nil
	}
	return l.w.Sync()
}

func TestRootPublicationLeafRotationLedgerInitialLoadZeroScans(t *testing.T) {
	dir := t.TempDir()
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
	d.indexSystemLeavesInPager = true
	closed := false
	// Install lazy leaf log WITHOUT opening a segment yet so len(startLeafSegments) == 0.
	inner := &registeredLeafPageLog{db: d, dir: d.dir}
	log := &lazyRegisteredLeafPageLog{registeredLeafPageLog: inner}
	d.SetLeafPageLog(log)
	t.Cleanup(func() {
		if !closed {
			closeVacuumTestLeafPageLog(t, d, inner)
			_ = d.Close()
		}
	})

	// Seed a pager-backed system-root catalog entry first (mirroring
	// CreateCollection where baseSystemRoot != 0 while len(startLeafSegments) == 0).
	catGroup, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	catSys := batch.New(nil, 1024)
	_ = catSys.Set([]byte("catalog/init-col/meta"), []byte("v1"))
	if err := catGroup.BuildNativeSystemDelta(catSys); err != nil {
		_ = catSys.Close()
		_ = catGroup.Close()
		t.Fatal(err)
	}
	_ = catSys.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, catGroup)

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

	if len(group.startLeafSegments) != 0 {
		t.Fatalf("expected empty startLeafSegments on initial load, got %+v", group.startLeafSegments)
	}
	if !group.leafSegmentObservationActive {
		t.Fatal("expected leafSegmentObservationActive=true after BeginRootPublicationBuildGroup")
	}

	// Open and register the first leaf-lane segment mid-group via RegisterValueLogSegmentReplacing.
	_, seg1, _ := rotateLedgerTestLeafLogReplacing(t, d, inner, 1)

	dA := batch.New(nil, 1024)
	_ = dA.Set([]byte("init/k1"), bytes.Repeat([]byte("I"), 128))
	rootA, err := group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: dA})
	_ = dA.Close()
	if err != nil {
		t.Fatal(err)
	}
	sysDelta := batch.New(nil, 1024)
	_ = sysDelta.Set([]byte(collectionRootDescriptorPrefix+"init-col"), encodeMaintenanceRootID(rootA))
	if err := group.BuildNativeSystemDelta(sysDelta); err != nil {
		_ = sysDelta.Close()
		t.Fatal(err)
	}
	_ = sysDelta.Close()

	finalizeNativeBuildGroupEmptyCarrier(t, d, group)

	if got := scans.Load(); got != 0 {
		t.Fatalf("initial load with registered leaf segment triggered %d candidate scans, want 0", got)
	}
	if d.leafSegmentObserverActive {
		t.Fatal("leafSegmentObserverActive leaked true after finalize")
	}

	retained := visibleOuterLeafFileIDs(t, d)
	if _, ok := retained[seg1]; !ok {
		t.Fatalf("initial leaf segment %d missing from visible closure %v", seg1, retained)
	}
	assertCandidateTrackerMatchesFullScan(t, d)

	closeVacuumTestLeafPageLog(t, d, inner)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	snap := reopened.AcquireSnapshot()
	defer snap.Close()
	tr := tree.New(snap.idx.pager, &snap.reader, rootA)
	got, err := tr.Get([]byte("init/k1"))
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("I"), 128)) {
		t.Fatalf("reopened key mismatch: got=%x err=%v", got, err)
	}
}

func TestRootPublicationLeafRotationLedgerMultiRotationGCAndReopen(t *testing.T) {
	dir := t.TempDir()
	d, log := openNativeBuildProjectionTestDB(t, dir)
	closed := false
	t.Cleanup(func() {
		if !closed {
			closeVacuumTestLeafPageLog(t, d, log)
			_ = d.Close()
		}
	})

	descKeyA := []byte(collectionRootDescriptorPrefix + "ledger-a")
	descKeyB := []byte(collectionRootDescriptorPrefix + "ledger-b")
	descKeyC := []byte(collectionRootDescriptorPrefix + "ledger-c")

	// Seed rootA, rootB, rootC in segment A (seq 1).
	segA := log.fileID
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
	dB0 := batch.New(nil, 1024)
	_ = dB0.Set([]byte("b/0"), bytes.Repeat([]byte("b"), 128))
	rootB0, err := seedGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: dB0})
	_ = dB0.Close()
	if err != nil {
		t.Fatal(err)
	}
	dC0 := batch.New(nil, 1024)
	_ = dC0.Set([]byte("c/0"), bytes.Repeat([]byte("c"), 128))
	rootC0, err := seedGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: dC0})
	_ = dC0.Close()
	if err != nil {
		t.Fatal(err)
	}
	sysSeed := batch.New(nil, 1024)
	_ = sysSeed.Set(descKeyA, encodeMaintenanceRootID(rootA))
	_ = sysSeed.Set(descKeyB, encodeMaintenanceRootID(rootB0))
	_ = sysSeed.Set(descKeyC, encodeMaintenanceRootID(rootC0))
	if err := seedGroup.BuildNativeSystemDelta(sysSeed); err != nil {
		_ = sysSeed.Close()
		t.Fatal(err)
	}
	_ = sysSeed.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, seedGroup)

	// Create two unreferenced data value-log segments in lane 0 (seq 98 and 99)
	// so seq 98 is non-active and eligible for ValueLogGC reclamation while seq 99
	// remains the lane's active tail segment, confirming ValueLogGC reclaims only
	// unreferenced segments while retaining every live segment across reopen.
	unrefValPath := filepath.Join(ValueLogDirPath(d.dir), "value-l0-000098.log")
	if err := os.MkdirAll(filepath.Dir(unrefValPath), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, seq := range []uint32{98, 99} {
		fid, err := valuelog.EncodeFileID(0, seq)
		if err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(ValueLogDirPath(d.dir), fmt.Sprintf("value-l0-%06d.log", seq))
		w, err := valuelog.NewWriter(p, fid)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Append(0, nil, uint64(seq), bytes.Repeat([]byte("U"), 256)); err != nil {
			_ = w.Close()
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.valueLogManager.Refresh(); err != nil {
		t.Fatal(err)
	}

	var scans atomic.Int64
	restore := d.SetScanCandidateExternalReferencesHookForTesting(func() {
		scans.Add(1)
	})
	defer restore()

	// Single build group starting at segA, rotating A -> B -> C via RegisterValueLogSegmentReplacing.
	group, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()

	_, segB, _ := rotateLedgerTestLeafLogReplacing(t, d, log, 2)
	dB := batch.New(nil, 1024)
	_ = dB.Set([]byte("b/1"), bytes.Repeat([]byte("B"), 128))
	rootB, err := group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: rootB0, Delta: dB})
	_ = dB.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, segC, _ := rotateLedgerTestLeafLogReplacing(t, d, log, 3)
	dC := batch.New(nil, 1024)
	_ = dC.Set([]byte("c/1"), bytes.Repeat([]byte("C"), 128))
	rootC, err := group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: rootC0, Delta: dC})
	_ = dC.Close()
	if err != nil {
		t.Fatal(err)
	}

	sysNext := batch.New(nil, 1024)
	_ = sysNext.Set(descKeyB, encodeMaintenanceRootID(rootB))
	_ = sysNext.Set(descKeyC, encodeMaintenanceRootID(rootC))
	if err := group.BuildNativeSystemDelta(sysNext); err != nil {
		_ = sysNext.Close()
		t.Fatal(err)
	}
	_ = sysNext.Close()

	finalizeNativeBuildGroupEmptyCarrier(t, d, group)

	if got := scans.Load(); got != 0 {
		t.Fatalf("multi-rotation A->B->C build group triggered %d candidate scans, want 0", got)
	}

	retained := visibleOuterLeafFileIDs(t, d)
	for _, wantID := range []uint32{segA, segB, segC} {
		if _, ok := retained[wantID]; !ok {
			t.Fatalf("expected segment %d in visible closure %v", wantID, retained)
		}
	}
	assertCandidateTrackerMatchesFullScan(t, d)

	// Run ValueLogGC and LeafGenerationGC: unreferenced value segment must be
	// reclaimed while all three referenced leaf segments A, B, C survive.
	gcStats, err := d.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if gcStats.SegmentsDeleted == 0 {
		t.Fatalf("expected ValueLogGC to delete unreferenced segment, got %+v", gcStats)
	}
	if _, err := os.Stat(unrefValPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected unreferenced value segment %q to be deleted, stat err=%v", unrefValPath, err)
	}
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
	checks := []struct {
		root uint64
		key  []byte
		val  []byte
	}{
		{root: rootA, key: []byte("a/1"), val: bytes.Repeat([]byte("A"), 128)},
		{root: rootB, key: []byte("b/1"), val: bytes.Repeat([]byte("B"), 128)},
		{root: rootC, key: []byte("c/1"), val: bytes.Repeat([]byte("C"), 128)},
	}
	for _, tc := range checks {
		tr := tree.New(snap.idx.pager, &snap.reader, tc.root)
		got, err := tr.Get(tc.key)
		if err != nil || !bytes.Equal(got, tc.val) {
			t.Fatalf("reopened root=%d key=%q mismatch: got=%x err=%v", tc.root, tc.key, got, err)
		}
	}
}

func TestRootPublicationLeafRotationLedgerFailClosedUnregisteredEndSegment(t *testing.T) {
	d, log := openNativeBuildProjectionTestDB(t, t.TempDir())
	defer func() {
		closeVacuumTestLeafPageLog(t, d, log)
		_ = d.Close()
	}()

	descKey := []byte(collectionRootDescriptorPrefix + "fc-col")
	seedGroup, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	d1 := batch.New(nil, 1024)
	_ = d1.Set([]byte("k/1"), bytes.Repeat([]byte("A"), 128))
	root1, err := seedGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: d1})
	_ = d1.Close()
	if err != nil {
		t.Fatal(err)
	}
	sys1 := batch.New(nil, 1024)
	_ = sys1.Set(descKey, encodeMaintenanceRootID(root1))
	if err := seedGroup.BuildNativeSystemDelta(sys1); err != nil {
		_ = sys1.Close()
		t.Fatal(err)
	}
	_ = sys1.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, seedGroup)

	// Seed a pager-backed system-root catalog entry first (mirroring
	// CreateCollection before any outer-leaf data segment has been opened, so
	// baseSystemRoot != 0 while len(startLeafSegments) == 0).
	catGroup, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	catSys := batch.New(nil, 1024)
	_ = catSys.Set([]byte("catalog/init-col/meta"), []byte("v1"))
	if err := catGroup.BuildNativeSystemDelta(catSys); err != nil {
		_ = catSys.Close()
		_ = catGroup.Close()
		t.Fatal(err)
	}
	_ = catSys.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, catGroup)

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

	// Bypass RegisterValueLogSegment* so seg2 is NOT in start ∪ observed ∪ created.
	seg2 := rotateLedgerTestLeafLogBypassingObserver(t, d, log, 2)

	d2 := batch.New(nil, 1024)
	_ = d2.Set([]byte("k/2"), bytes.Repeat([]byte("B"), 128))
	root2, err := group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: root1, Delta: d2})
	_ = d2.Close()
	if err != nil {
		t.Fatal(err)
	}
	sys2 := batch.New(nil, 1024)
	_ = sys2.Set(descKey, encodeMaintenanceRootID(root2))
	if err := group.BuildNativeSystemDelta(sys2); err != nil {
		_ = sys2.Close()
		t.Fatal(err)
	}
	_ = sys2.Close()

	finalizeNativeBuildGroupEmptyCarrier(t, d, group)

	if got := scans.Load(); got == 0 {
		t.Fatal("unregistered mid-group endLeafSegments bypassed fail-closed candidate scan")
	}
	retained := visibleOuterLeafFileIDs(t, d)
	if _, ok := retained[seg2]; !ok {
		t.Fatalf("unregistered end segment %d missing from visible closure %v", seg2, retained)
	}
	assertCandidateTrackerMatchesFullScan(t, d)
}

func TestRootPublicationLeafRotationLedgerFailClosedStartError(t *testing.T) {
	opts := Options{
		Dir:                        t.TempDir(),
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		DisableBackgroundPrune:     true,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	inner := &registeredLeafPageLog{db: d, dir: d.dir}
	if err := inner.ensureWriter(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	log := &failingStartLeafPageLog{registeredLeafPageLog: inner}
	d.SetLeafPageLog(log)
	defer func() {
		closeVacuumTestLeafPageLog(t, d, inner)
		_ = d.Close()
	}()

	descKey := []byte(collectionRootDescriptorPrefix + "fc-err")
	seedGroup, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	d1 := batch.New(nil, 1024)
	_ = d1.Set([]byte("k/1"), bytes.Repeat([]byte("A"), 128))
	root1, err := seedGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: d1})
	_ = d1.Close()
	if err != nil {
		t.Fatal(err)
	}
	sys1 := batch.New(nil, 1024)
	_ = sys1.Set(descKey, encodeMaintenanceRootID(root1))
	if err := seedGroup.BuildNativeSystemDelta(sys1); err != nil {
		_ = sys1.Close()
		t.Fatal(err)
	}
	_ = sys1.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, seedGroup)

	var scans atomic.Int64
	restore := d.SetScanCandidateExternalReferencesHookForTesting(func() {
		scans.Add(1)
	})
	defer restore()

	// Inject error during BeginRootPublicationBuildGroup startLeafSegments capture.
	log.failCurrent.Store(true)
	group, err := d.BeginRootPublicationBuildGroup()
	log.failCurrent.Store(false)
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	if group.startLeafSegmentsErr == nil {
		t.Fatal("expected non-nil startLeafSegmentsErr")
	}

	d2 := batch.New(nil, 1024)
	_ = d2.Set([]byte("k/2"), bytes.Repeat([]byte("B"), 128))
	root2, err := group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: root1, Delta: d2})
	_ = d2.Close()
	if err != nil {
		t.Fatal(err)
	}
	sys2 := batch.New(nil, 1024)
	_ = sys2.Set(descKey, encodeMaintenanceRootID(root2))
	if err := group.BuildNativeSystemDelta(sys2); err != nil {
		_ = sys2.Close()
		t.Fatal(err)
	}
	_ = sys2.Close()

	finalizeNativeBuildGroupEmptyCarrier(t, d, group)

	if got := scans.Load(); got == 0 {
		t.Fatal("startLeafSegmentsErr != nil bypassed fail-closed candidate scan")
	}
}

func TestRootPublicationLeafRotationLedgerAbortedGroupClearsObserver(t *testing.T) {
	d, log := openNativeBuildProjectionTestDB(t, t.TempDir())
	defer func() {
		closeVacuumTestLeafPageLog(t, d, log)
		_ = d.Close()
	}()

	descKey := []byte(collectionRootDescriptorPrefix + "abort-col")
	seedGroup, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	d1 := batch.New(nil, 1024)
	_ = d1.Set([]byte("k/1"), bytes.Repeat([]byte("A"), 128))
	root1, err := seedGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: d1})
	_ = d1.Close()
	if err != nil {
		t.Fatal(err)
	}
	sys1 := batch.New(nil, 1024)
	_ = sys1.Set(descKey, encodeMaintenanceRootID(root1))
	if err := seedGroup.BuildNativeSystemDelta(sys1); err != nil {
		_ = sys1.Close()
		t.Fatal(err)
	}
	_ = sys1.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, seedGroup)

	// Start group 1, record a leaf-lane segment and a non-leaf-lane segment, then abort!
	aborted, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	abortedLeafID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 42)
	if err != nil {
		t.Fatal(err)
	}
	abortedLeafPath := filepath.Join(LeafLogDirPath(d.dir), fmt.Sprintf("value-l%d-000042.log", rewriteLeafLogLaneID))
	w42, err := valuelog.NewWriter(abortedLeafPath, abortedLeafID)
	if err != nil {
		t.Fatal(err)
	}
	if err := w42.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.RegisterValueLogSegmentReplacing(abortedLeafPath, abortedLeafID, 0); err != nil {
		t.Fatal(err)
	}
	// Non-leaf-lane segment must not be recorded in the leaf observer.
	nonLeafID, err := valuelog.EncodeFileID(0, 77)
	if err != nil {
		t.Fatal(err)
	}
	nonLeafPath := filepath.Join(ValueLogDirPath(d.dir), "value-l0-000077.log")
	if err := os.MkdirAll(filepath.Dir(nonLeafPath), 0o755); err != nil {
		t.Fatal(err)
	}
	w77, err := valuelog.NewWriter(nonLeafPath, nonLeafID)
	if err != nil {
		t.Fatal(err)
	}
	if err := w77.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.RegisterValueLogSegmentReplacing(nonLeafPath, nonLeafID, 0); err != nil {
		t.Fatal(err)
	}

	d.leafSegmentObserverMu.Lock()
	if !d.leafSegmentObserverActive {
		d.leafSegmentObserverMu.Unlock()
		t.Fatal("expected observer active during group 1")
	}
	if len(d.leafSegmentObserverIDs) != 1 || d.leafSegmentObserverIDs[0] != abortedLeafID {
		ids := append([]uint32(nil), d.leafSegmentObserverIDs...)
		d.leafSegmentObserverMu.Unlock()
		t.Fatalf("observer IDs=%v want [%d] (non-leaf lane must be ignored)", ids, abortedLeafID)
	}
	d.leafSegmentObserverMu.Unlock()

	// Abort group 1 before finalize.
	if err := aborted.Close(); err != nil {
		t.Fatalf("abort group 1: %v", err)
	}

	d.leafSegmentObserverMu.Lock()
	activeAfterAbort := d.leafSegmentObserverActive
	idsAfterAbort := len(d.leafSegmentObserverIDs)
	d.leafSegmentObserverMu.Unlock()
	if activeAfterAbort || idsAfterAbort != 0 {
		t.Fatalf("aborted group leaked observer state: active=%v ids=%d", activeAfterAbort, idsAfterAbort)
	}

	// Start group 2: if abortedLeafID appears unregistered in endLeafSegments,
	// group 2 must NOT see group 1's stale observed ID and must fail closed!
	var scans atomic.Int64
	restore := d.SetScanCandidateExternalReferencesHookForTesting(func() {
		scans.Add(1)
	})
	defer restore()

	group2, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group2.Close()

	// Switch log to seq 42 bypassing DB.RegisterValueLogSegment* (already in manager).
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	log.fileID = abortedLeafID
	log.path = abortedLeafPath
	log.w, err = valuelog.NewWriter(abortedLeafPath, abortedLeafID)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.valueLogManager.PromoteCurrentWritable(abortedLeafID); err != nil {
		t.Fatal(err)
	}

	d2 := batch.New(nil, 1024)
	_ = d2.Set([]byte("k/2"), bytes.Repeat([]byte("B"), 128))
	root2, err := group2.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: root1, Delta: d2})
	_ = d2.Close()
	if err != nil {
		t.Fatal(err)
	}
	sys2 := batch.New(nil, 1024)
	_ = sys2.Set(descKey, encodeMaintenanceRootID(root2))
	if err := group2.BuildNativeSystemDelta(sys2); err != nil {
		_ = sys2.Close()
		t.Fatal(err)
	}
	_ = sys2.Close()

	finalizeNativeBuildGroupEmptyCarrier(t, d, group2)

	if got := scans.Load(); got == 0 {
		t.Fatal("group 2 saw stale aborted segment ID and bypassed fail-closed scan")
	}
}

func TestRootPublicationLeafRotationLedgerPublishAndEnsurePathsRecordLeafLane(t *testing.T) {
	d, log := openNativeBuildProjectionTestDB(t, t.TempDir())
	defer func() {
		closeVacuumTestLeafPageLog(t, d, log)
		_ = d.Close()
	}()

	// 1. ensureLeafPageLogSegmentRegisteredAt records leaf-lane segment during active observation.
	seg2ID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 2)
	if err != nil {
		t.Fatal(err)
	}
	seg2Path := filepath.Join(LeafLogDirPath(d.dir), fmt.Sprintf("value-l%d-000002.log", rewriteLeafLogLaneID))
	w2, err := valuelog.NewWriter(seg2Path, seg2ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	d.beginBuildGroupLeafSegmentObservation()
	ok, err := d.ensureLeafPageLogSegmentRegisteredAt(seg2Path, seg2ID, 0)
	if err != nil || !ok {
		t.Fatalf("ensureLeafPageLogSegmentRegisteredAt(new): ok=%v err=%v", ok, err)
	}
	// Call a second time to exercise the HasSegment fast path too.
	ok, err = d.ensureLeafPageLogSegmentRegisteredAt(seg2Path, seg2ID, 0)
	if err != nil || !ok {
		t.Fatalf("ensureLeafPageLogSegmentRegisteredAt(existing): ok=%v err=%v", ok, err)
	}
	observed := d.finishBuildGroupLeafSegmentObservation()
	if len(observed) != 1 || observed[0] != seg2ID {
		t.Fatalf("ensureLeafPageLogSegmentRegisteredAt observed=%v want [%d]", observed, seg2ID)
	}

	// 2. registerLeafPageLogSegmentsForPublish records current leaf-lane segment.
	d.beginBuildGroupLeafSegmentObservation()
	ok, err = d.registerLeafPageLogSegmentsForPublish()
	if err != nil || !ok {
		t.Fatalf("registerLeafPageLogSegmentsForPublish: ok=%v err=%v", ok, err)
	}
	observed = d.finishBuildGroupLeafSegmentObservation()
	if len(observed) != 1 || observed[0] != log.fileID {
		t.Fatalf("registerLeafPageLogSegmentsForPublish observed=%v want [%d]", observed, log.fileID)
	}
}

func TestRootPublicationLeafRotationLedgerAfterUnreachableGenerationGC(t *testing.T) {
	dir := t.TempDir()
	d, log := openNativeBuildProjectionTestDB(t, dir)
	closed := false
	t.Cleanup(func() {
		if !closed {
			closeVacuumTestLeafPageLog(t, d, log)
			_ = d.Close()
		}
	})

	descKey := []byte(collectionRootDescriptorPrefix + "gc-col")

	// Round 0: seed root in seg1 (seq 1).
	seg1 := log.fileID
	seedGroup, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	d0 := batch.New(nil, 1024)
	_ = d0.Set([]byte("k/1"), bytes.Repeat([]byte("0"), 128))
	root, err := seedGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: d0})
	_ = d0.Close()
	if err != nil {
		t.Fatal(err)
	}
	sys0 := batch.New(nil, 1024)
	_ = sys0.Set(descKey, encodeMaintenanceRootID(root))
	if err := seedGroup.BuildNativeSystemDelta(sys0); err != nil {
		_ = sys0.Close()
		t.Fatal(err)
	}
	_ = sys0.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, seedGroup)

	// Rounds 1..4: rotate to a new segment on each commit and overwrite the sole
	// key so earlier segments (including seg2) become unreachable from both live
	// and recoverable durable slots while remaining in the conservative
	// visibleResources closure via outerLeafDependencyReuse.
	var seg2 uint32
	for seq := uint32(2); seq <= 5; seq++ {
		group, err := d.BeginRootPublicationBuildGroup()
		if err != nil {
			t.Fatal(err)
		}
		_, newID, _ := rotateLedgerTestLeafLogReplacing(t, d, log, seq)
		if seq == 2 {
			seg2 = newID
		}
		delta := batch.New(nil, 1024)
		_ = delta.Set([]byte("k/1"), bytes.Repeat([]byte{byte('0' + seq)}, 128))
		nextRoot, err := group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: root, Delta: delta})
		_ = delta.Close()
		if err != nil {
			_ = group.Close()
			t.Fatal(err)
		}
		sysDelta := batch.New(nil, 1024)
		_ = sysDelta.Set(descKey, encodeMaintenanceRootID(nextRoot))
		if err := group.BuildNativeSystemDelta(sysDelta); err != nil {
			_ = sysDelta.Close()
			_ = group.Close()
			t.Fatal(err)
		}
		_ = sysDelta.Close()
		finalizeNativeBuildGroupEmptyCarrier(t, d, group)
		root = nextRoot
	}

	// Confirm seg1 and seg2 were conservatively retained in visibleResources.
	beforeGC := visibleOuterLeafFileIDs(t, d)
	if _, ok := beforeGC[seg2]; !ok {
		t.Fatalf("expected seg2=%d in visible closure before GC: %v", seg2, beforeGC)
	}

	// Run LeafGenerationGC to retire/delete unreachable seg1/seg2.
	if _, err := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC #1: %v", err)
	}
	if _, err := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC #2: %v", err)
	}
	if d.valueLogManager.HasSegment(seg2) {
		t.Fatalf("expected LeafGenerationGC to unregister unreachable seg2=%d (seg1=%d registered=%v)", seg2, seg1, d.valueLogManager.HasSegment(seg1))
	}

	var scans atomic.Int64
	restore := d.SetScanCandidateExternalReferencesHookForTesting(func() {
		scans.Add(1)
	})
	defer restore()

	// Next commit after GC: must fail closed to the exact candidate scanner
	// instead of trying to recapture unregistered seg2, pruning seg2 cleanly.
	postGCGroup, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	_, seg6, _ := rotateLedgerTestLeafLogReplacing(t, d, log, 6)
	d6 := batch.New(nil, 1024)
	_ = d6.Set([]byte("k/1"), bytes.Repeat([]byte("6"), 128))
	root6, err := postGCGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: root, Delta: d6})
	_ = d6.Close()
	if err != nil {
		_ = postGCGroup.Close()
		t.Fatal(err)
	}
	sys6 := batch.New(nil, 1024)
	_ = sys6.Set(descKey, encodeMaintenanceRootID(root6))
	if err := postGCGroup.BuildNativeSystemDelta(sys6); err != nil {
		_ = sys6.Close()
		_ = postGCGroup.Close()
		t.Fatal(err)
	}
	_ = sys6.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, postGCGroup)

	if got := scans.Load(); got == 0 {
		t.Fatal("post-GC publication did not fail closed to prune unregistered segment from visible closure")
	}
	afterPostGC := visibleOuterLeafFileIDs(t, d)
	if _, stale := afterPostGC[seg2]; stale {
		t.Fatalf("unregistered seg2=%d remained in visible closure after post-GC scan: %v", seg2, afterPostGC)
	}
	if _, ok := afterPostGC[seg6]; !ok {
		t.Fatalf("new seg6=%d missing from visible closure: %v", seg6, afterPostGC)
	}
	assertCandidateTrackerMatchesFullScan(t, d)

	// Subsequent registered rotation must return to 0 candidate scans.
	scans.Store(0)
	steadyGroup, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	_, seg7, _ := rotateLedgerTestLeafLogReplacing(t, d, log, 7)
	d7 := batch.New(nil, 1024)
	_ = d7.Set([]byte("k/1"), bytes.Repeat([]byte("7"), 128))
	root7, err := steadyGroup.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{BaseRoot: root6, Delta: d7})
	_ = d7.Close()
	if err != nil {
		_ = steadyGroup.Close()
		t.Fatal(err)
	}
	sys7 := batch.New(nil, 1024)
	_ = sys7.Set(descKey, encodeMaintenanceRootID(root7))
	if err := steadyGroup.BuildNativeSystemDelta(sys7); err != nil {
		_ = sys7.Close()
		_ = steadyGroup.Close()
		t.Fatal(err)
	}
	_ = sys7.Close()
	finalizeNativeBuildGroupEmptyCarrier(t, d, steadyGroup)

	if got := scans.Load(); got != 0 {
		t.Fatalf("steady-state rotation after post-GC prune triggered %d scans, want 0", got)
	}
	if _, ok := visibleOuterLeafFileIDs(t, d)[seg7]; !ok {
		t.Fatalf("new seg7=%d missing from visible closure", seg7)
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
	tr := tree.New(snap.idx.pager, &snap.reader, root7)
	got, err := tr.Get([]byte("k/1"))
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("7"), 128)) {
		t.Fatalf("reopened key mismatch: got=%x err=%v", got, err)
	}
}
