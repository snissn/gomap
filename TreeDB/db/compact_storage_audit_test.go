package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCompactStorageAudit_DryRunUsesOneSharedWalkAndNoLegacyScanners(t *testing.T) {
	db := openCompactStorageRewritePolicyBenchmarkFixture(t, 128, 16, 256)
	defer closeNoErr(t, db)

	var shared, refScans, liveScans, leafScans atomic.Uint64
	unregisterShared := registerCompactStorageSharedAuditScanHook(func(compactStorageAuditCounters) {
		shared.Add(1)
	})
	unregisterRefs := registerScanValueLogRefCountsHook(func() { refScans.Add(1) })
	unregisterLive := registerRewritePlanLiveEstimateHook(func() { liveScans.Add(1) })
	unregisterLeaf := registerLeafGenerationLiveScanHook(func() { leafScans.Add(1) })
	t.Cleanup(func() {
		unregisterLeaf()
		unregisterLive()
		unregisterRefs()
		unregisterShared()
	})

	stats, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{
		Mode:                           CompactStorageFull,
		DisableZeroByteValueLogCleanup: true,
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := shared.Load(); got != 1 {
		t.Fatalf("shared scans=%d want 1", got)
	}
	if got := refScans.Load(); got != 0 {
		t.Fatalf("scanValueLogRefCounts calls=%d want 0", got)
	}
	if got := liveScans.Load(); got != 0 {
		t.Fatalf("estimateValueLogLiveBytesBySegment calls=%d want 0", got)
	}
	if got := leafScans.Load(); got != 0 {
		t.Fatalf("legacy leaf-generation live scans=%d want 0", got)
	}
	if stats.Audit.SharedScans != 1 || stats.Audit.PagesVisited == 0 || stats.Audit.PointerProjections == 0 {
		t.Fatalf("unexpected audit counters: %+v", stats.Audit)
	}
}

func TestCompactStorageAudit_EmptyRegisteredTopologyRefreshesOutOfProcessSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, db)

	path := filepath.Join(ValueLogDirPath(dir), "value-l0-000001.log")
	if err := os.WriteFile(path, make([]byte, 4<<10), 0o644); err != nil {
		t.Fatalf("write out-of-process value-log segment: %v", err)
	}
	refreshBefore := db.valueLogManager.RefreshScanCount()

	stats, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got, want := db.valueLogManager.RefreshScanCount(), refreshBefore+1; got != want {
		t.Fatalf("refresh scans=%d want %d", got, want)
	}
	var diskBytes int64
	for _, usage := range stats.Before {
		if usage.Name == "value_vlog" {
			diskBytes = usage.Bytes
			break
		}
	}
	if diskBytes != 4<<10 {
		t.Fatalf("value_vlog disk bytes=%d want %d", diskBytes, 4<<10)
	}
	if stats.ValueLogRewritePlan.SegmentsTotal != 1 || stats.ValueLogGC.SegmentsTotal != 1 {
		t.Fatalf("out-of-process segment missing from plans: rewrite=%+v gc=%+v", stats.ValueLogRewritePlan, stats.ValueLogGC)
	}
}

func TestCompactStorageAudit_PublishesLegitimatelyEmptyRefreshedTopology(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(ValueLogDirPath(dir), "value-l0-000001.log")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir value-log dir: %v", err)
	}
	if err := os.WriteFile(path, make([]byte, 4<<10), 0o644); err != nil {
		t.Fatalf("write value-log segment: %v", err)
	}
	db, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, db)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("explicit maintenance refresh: %v", err)
	}

	fileID, ok := compactStorageValueLogFileID(filepath.Base(path))
	if !ok {
		t.Fatalf("parse file ID from %s", path)
	}
	if state := db.State(); state == nil || state.ValueLogSet == nil || len(state.ValueLogSet.Files) != 1 {
		t.Fatalf("initial published topology=%+v want one segment", state)
	}
	if err := db.valueLogManager.MarkZombie(fileID); err != nil {
		t.Fatalf("MarkZombie: %v", err)
	}
	refreshBefore := db.valueLogManager.RefreshScanCount()

	if err := db.prepareCompactStorageAuditTopology(); err != nil {
		t.Fatalf("prepareCompactStorageAuditTopology: %v", err)
	}
	if got, want := db.valueLogManager.RefreshScanCount(), refreshBefore+1; got != want {
		t.Fatalf("refresh scans=%d want exactly one empty-topology refresh (%d)", got, want)
	}
	state := db.State()
	if state == nil || state.ValueLogSet == nil || len(state.ValueLogSet.Files) != 0 {
		t.Fatalf("published topology=%+v want legitimate empty set", state)
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	defer func() { _ = db.valueLogManager.Release(set) }()
	if set == nil || len(set.Files) != 0 {
		t.Fatalf("manager topology=%+v want empty set", set)
	}
}

func TestCompactStorageAuditKey_ExactInvalidation(t *testing.T) {
	setA := &valuelog.Set{}
	setB := &valuelog.Set{}
	base := compactStorageAuditKey{
		CommitSeq:                  1,
		RootPageID:                 2,
		SystemRootPageID:           3,
		LeafGenerationStateVersion: 4,
		ValueLogSetIdentity:        setA,
		ProtectedRootSetHash:       [32]byte{5},
		ProtectedPathSetHash:       [32]byte{6},
	}
	tests := map[string]func(*compactStorageAuditKey){
		"commit sequence":         func(k *compactStorageAuditKey) { k.CommitSeq++ },
		"root":                    func(k *compactStorageAuditKey) { k.RootPageID++ },
		"system root":             func(k *compactStorageAuditKey) { k.SystemRootPageID++ },
		"leaf generation version": func(k *compactStorageAuditKey) { k.LeafGenerationStateVersion++ },
		"value-log set identity":  func(k *compactStorageAuditKey) { k.ValueLogSetIdentity = setB },
		"protected roots":         func(k *compactStorageAuditKey) { k.ProtectedRootSetHash[0]++ },
		"protected paths":         func(k *compactStorageAuditKey) { k.ProtectedPathSetHash[0]++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			got := base
			mutate(&got)
			if got == base {
				t.Fatalf("mutation did not invalidate key: %+v", got)
			}
		})
	}
}

func TestCompactStorageAudit_ValueLogCollectorsMatchStandaloneGroupedAliases(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 240_000, 2, func(int) []byte {
		return bytes.Repeat([]byte("grouped"), 64)
	})
	active := appendPointersInNewSegment(t, dir, 0, 2, 250_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("active"), 64)
	})[0]
	base := ptrs[0]
	recordLen := page.ValuePtrRecordLength(base)
	b := db.NewBatch().(*Batch)
	for i := 0; i < 3; i++ {
		ptr := base
		ptr.Length = page.ValuePtrMarkGrouped(recordLen, uint8(i))
		if err := b.SetPointer([]byte(fmt.Sprintf("grouped-%d", i)), ptr); err != nil {
			t.Fatalf("SetPointer grouped %d: %v", i, err)
		}
	}
	if err := b.SetPointer([]byte("active"), active); err != nil {
		t.Fatalf("SetPointer active: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	closeNoErr(t, b)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	wantRefs, _, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("standalone refs: %v", err)
	}
	wantLive, err := db.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("standalone live bytes: %v", err)
	}
	input, err := db.acquireCompactStorageAuditInput(CompactStorageOptions{})
	if err != nil {
		t.Fatalf("acquire audit input: %v", err)
	}
	defer input.close()
	got, err := db.scanCompactStorageAudit(context.Background(), input)
	if err != nil {
		t.Fatalf("shared audit: %v", err)
	}
	if !reflect.DeepEqual(got.valueLogRefCounts, wantRefs) {
		t.Fatalf("ref counts mismatch: combined=%v standalone=%v", got.valueLogRefCounts, wantRefs)
	}
	if !reflect.DeepEqual(got.valueLogLiveBytesBySegment, wantLive) {
		t.Fatalf("live bytes mismatch: combined=%v standalone=%v", got.valueLogLiveBytesBySegment, wantLive)
	}
	if got.valueLogRefCounts[base.FileID] != 3 {
		t.Fatalf("grouped alias refs=%d want 3", got.valueLogRefCounts[base.FileID])
	}
	if got.valueLogLiveBytesBySegment[base.FileID] != int64(recordLen) {
		t.Fatalf("grouped live bytes=%d want one record=%d", got.valueLogLiveBytesBySegment[base.FileID], recordLen)
	}
	if got.counters.GroupedRecordDedupeHits != 2 {
		t.Fatalf("grouped dedupe hits=%d want 2", got.counters.GroupedRecordDedupeHits)
	}
}

func TestCompactStorageAudit_ProtectedRootsOnlyExtendLeafProjection(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	grouped := appendPointersInNewSegment(t, db.dir, 0, 1, 270_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("grouped-maintenance|"), 32)
	})[0]
	active := appendPointersInNewSegment(t, db.dir, 0, 2, 280_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("active-maintenance|"), 32)
	})[0]
	protectedOrdinary := appendPointersInNewSegment(t, db.dir, 0, 3, 290_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("protected-ordinary|"), 32)
	})[0]
	protectedSystem := appendPointersInNewSegment(t, db.dir, 0, 4, 300_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte(fmt.Sprintf("protected-system-%d|", i)), 32)
	})
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	writeLeafGenerationKeys(t, db, "base", 2048, 'a')
	b := db.NewBatch().(*Batch)
	recordLen := page.ValuePtrRecordLength(grouped)
	for i := 0; i < 3; i++ {
		ptr := grouped
		ptr.Length = page.ValuePtrMarkGrouped(recordLen, uint8(i))
		if err := b.SetPointer([]byte(fmt.Sprintf("grouped/%d", i)), ptr); err != nil {
			t.Fatalf("SetPointer grouped %d: %v", i, err)
		}
	}
	if err := b.SetPointer([]byte("maintenance/active"), active); err != nil {
		t.Fatalf("SetPointer active: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync maintenance pointers: %v", err)
	}
	closeNoErr(t, b)

	maintenanceRoot := db.State().RootPageID
	ordinaryRoot, err := db.PublishOrderedRootIterator(
		maintenanceRoot,
		mustFrozenSystemPointerMemtable(t, "protected/ordinary", protectedOrdinary).NewIterator(nil, nil),
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator ordinary: %v", err)
	}
	collectionRoot, err := db.PublishOrderedRootIterator(
		maintenanceRoot,
		mustFrozenSystemPointerMemtable(t, "protected/collection", protectedSystem[0]).NewIterator(nil, nil),
	)
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator collection: %v", err)
	}
	protectedSystemRoot, err := db.PublishOrderedRootIterator(0, compactStorageAuditMixedTable(
		t,
		map[string][]byte{maintenanceTestCollectionRootKey: encodeMaintenanceRootID(collectionRoot)},
		map[string]page.ValuePtr{"protected/system": protectedSystem[1]},
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator system: %v", err)
	}
	if state := db.State(); state.RootPageID == ordinaryRoot || state.SystemRootPageID == protectedSystemRoot {
		t.Fatalf("protected roots must be detached: state=%+v ordinary=%d system=%d", state, ordinaryRoot, protectedSystemRoot)
	}
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "current", 1, 'z')

	wantRefs, _, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("legacy refs: %v", err)
	}
	wantLive, err := db.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("standalone live bytes: %v", err)
	}
	opts := normalizeCompactStorageOptions(CompactStorageOptions{
		LeafGenerationProtectedRootIDs:       []uint64{ordinaryRoot},
		LeafGenerationProtectedSystemRootIDs: []uint64{protectedSystemRoot},
	})
	input, err := db.acquireCompactStorageAuditInput(opts)
	if err != nil {
		t.Fatalf("acquire protected audit input: %v", err)
	}
	defer input.close()
	got, err := db.scanCompactStorageAudit(context.Background(), input)
	if err != nil {
		t.Fatalf("shared protected audit: %v", err)
	}
	if !reflect.DeepEqual(got.valueLogRefCounts, wantRefs) {
		t.Fatalf("ref counts mismatch: shared=%v legacy=%v", got.valueLogRefCounts, wantRefs)
	}
	if !reflect.DeepEqual(got.valueLogReferencedSegments, valueLogRefSetFromCounts(wantRefs)) {
		t.Fatalf("referenced segments mismatch: shared=%v legacy=%v", got.valueLogReferencedSegments, valueLogRefSetFromCounts(wantRefs))
	}
	if !reflect.DeepEqual(got.valueLogLiveBytesBySegment, wantLive) {
		t.Fatalf("live bytes mismatch: shared=%v legacy=%v", got.valueLogLiveBytesBySegment, wantLive)
	}
	if got.valueLogRefCounts[grouped.FileID] != 3 {
		t.Fatalf("grouped alias refs=%d want 3", got.valueLogRefCounts[grouped.FileID])
	}
	if got.valueLogLiveBytesBySegment[grouped.FileID] != int64(recordLen) {
		t.Fatalf("grouped live bytes=%d want one record=%d", got.valueLogLiveBytesBySegment[grouped.FileID], recordLen)
	}
	if _, ok := got.valueLogRefCounts[protectedOrdinary.FileID]; ok {
		t.Fatalf("protected ordinary segment %d entered maintenance projection: %v", protectedOrdinary.FileID, got.valueLogRefCounts)
	}
	if _, ok := got.valueLogRefCounts[protectedSystem[0].FileID]; ok {
		t.Fatalf("protected system segment %d entered maintenance projection: %v", protectedSystem[0].FileID, got.valueLogRefCounts)
	}
	leafOpts := leafGenerationPackFromPlanPlanOptions(compactStorageLeafPackFromPlanOptions(
		opts,
		input.protectedRootIDs,
		input.protectedSystemRootIDs,
	))
	wantLeaf, err := db.LeafGenerationPlan(context.Background(), leafOpts)
	if err != nil {
		t.Fatalf("legacy leaf plan: %v", err)
	}
	gotLeaf := db.compactStorageLeafGenerationPlanFromAudit(leafOpts, input, got.leafGenerationLive)
	if !reflect.DeepEqual(compactStorageAuditPublicLeafPlan(gotLeaf), compactStorageAuditPublicLeafPlan(wantLeaf)) {
		t.Fatalf("leaf plan mismatch:\nshared=%+v\nlegacy=%+v", gotLeaf, wantLeaf)
	}

	unprotectedInput, err := db.acquireCompactStorageAuditInput(CompactStorageOptions{})
	if err != nil {
		t.Fatalf("acquire unprotected audit input: %v", err)
	}
	defer unprotectedInput.close()
	unprotected, err := db.scanCompactStorageAudit(context.Background(), unprotectedInput)
	if err != nil {
		t.Fatalf("shared unprotected audit: %v", err)
	}
	unprotectedPages := compactStorageAuditLeafLivePages(unprotected.leafGenerationLive)
	if compactStorageAuditLeafLivePages(got.leafGenerationLive) <= unprotectedPages {
		t.Fatalf("protected roots did not extend leaf projection: protected=%v unprotected=%v", got.leafGenerationLive, unprotected.leafGenerationLive)
	}
	for _, projection := range []struct {
		name string
		opts CompactStorageOptions
	}{
		{name: "ordinary", opts: CompactStorageOptions{LeafGenerationProtectedRootIDs: []uint64{ordinaryRoot}}},
		{name: "system", opts: CompactStorageOptions{LeafGenerationProtectedSystemRootIDs: []uint64{protectedSystemRoot}}},
	} {
		protectedInput, err := db.acquireCompactStorageAuditInput(projection.opts)
		if err != nil {
			t.Fatalf("acquire %s-only audit input: %v", projection.name, err)
		}
		protectedOnly, scanErr := db.scanCompactStorageAudit(context.Background(), protectedInput)
		protectedInput.close()
		if scanErr != nil {
			t.Fatalf("shared %s-only audit: %v", projection.name, scanErr)
		}
		if pages := compactStorageAuditLeafLivePages(protectedOnly.leafGenerationLive); pages <= unprotectedPages {
			t.Fatalf("%s protected root did not extend leaf projection: protected=%v unprotected=%v", projection.name, protectedOnly.leafGenerationLive, unprotected.leafGenerationLive)
		}
	}
}

func TestCompactStorageAudit_ProtectedPagerRootsMatchStandalonePlansWithMemoReuse(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, db)

	grouped := appendPointersInNewSegment(t, dir, 0, 1, 320_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("memo-grouped|"), 32)
	})[0]
	protected := appendPointersInNewSegment(t, dir, 0, 2, 330_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte(fmt.Sprintf("memo-protected-%d|", i)), 32)
	})
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	b := db.NewBatch().(*Batch)
	recordLen := page.ValuePtrRecordLength(grouped)
	for i := 0; i < 512; i++ {
		if err := b.Set(
			[]byte(fmt.Sprintf("maintenance/%04d", i)),
			bytes.Repeat([]byte{byte('a' + i%23)}, 96),
		); err != nil {
			t.Fatalf("Set maintenance %d: %v", i, err)
		}
	}
	for i := 0; i < 3; i++ {
		ptr := grouped
		ptr.Length = page.ValuePtrMarkGrouped(recordLen, uint8(i))
		if err := b.SetPointer([]byte(fmt.Sprintf("grouped/%d", i)), ptr); err != nil {
			t.Fatalf("SetPointer grouped %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write grouped maintenance pointers: %v", err)
	}
	closeNoErr(t, b)

	state := db.State()
	pageIDs := collectRootPageIDs(t, db, state.RootPageID)
	systemDescriptors := make(map[string][]byte, len(pageIDs))
	for i, pageID := range pageIDs {
		if pageID == state.RootPageID {
			continue
		}
		systemDescriptors[fmt.Sprintf("%smemo-%04d", collectionRootDescriptorPrefix, i)] = encodeMaintenanceRootID(pageID)
	}
	if len(systemDescriptors) == 0 {
		t.Fatalf("maintenance root %d has no shared child subtrees: pages=%v", state.RootPageID, pageIDs)
	}
	if _, err := db.PublishSystemRootIterator(compactStorageAuditMixedTable(t, systemDescriptors, nil).NewIterator(nil, nil)); err != nil {
		t.Fatalf("PublishSystemRootIterator descriptors: %v", err)
	}
	ordinaryRoot, err := db.PublishOrderedRootIterator(0, compactStorageAuditMixedTable(
		t,
		nil,
		map[string]page.ValuePtr{"detached/pointer": protected[0]},
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator ordinary: %v", err)
	}
	protectedSystemRoot, err := db.PublishOrderedRootIterator(0, compactStorageAuditMixedTable(
		t,
		map[string][]byte{maintenanceTestCollectionRootKey: encodeMaintenanceRootID(ordinaryRoot)},
		map[string]page.ValuePtr{"detached/system": protected[1]},
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator system: %v", err)
	}

	wantRefs, _, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("legacy refs: %v", err)
	}
	wantLive, err := db.estimateValueLogLiveBytesBySegment(context.Background())
	if err != nil {
		t.Fatalf("standalone live bytes: %v", err)
	}
	opts := normalizeCompactStorageOptions(CompactStorageOptions{
		LeafGenerationProtectedRootIDs:       []uint64{ordinaryRoot},
		LeafGenerationProtectedSystemRootIDs: []uint64{protectedSystemRoot},
	})
	input, err := db.acquireCompactStorageAuditInput(opts)
	if err != nil {
		t.Fatalf("acquire audit input: %v", err)
	}
	defer input.close()
	got, err := db.scanCompactStorageAudit(context.Background(), input)
	if err != nil {
		t.Fatalf("shared audit: %v", err)
	}
	if !reflect.DeepEqual(got.valueLogRefCounts, wantRefs) {
		t.Fatalf("ref counts mismatch: shared=%v legacy=%v", got.valueLogRefCounts, wantRefs)
	}
	if !reflect.DeepEqual(got.valueLogLiveBytesBySegment, wantLive) {
		t.Fatalf("live bytes mismatch: shared=%v legacy=%v", got.valueLogLiveBytesBySegment, wantLive)
	}
	if wantRefs[grouped.FileID] <= 3 {
		t.Fatalf("fixture did not create repeated maintenance projections: refs=%d", wantRefs[grouped.FileID])
	}
	if got.valueLogRefCounts[grouped.FileID] != wantRefs[grouped.FileID] || got.valueLogLiveBytesBySegment[grouped.FileID] != int64(recordLen) {
		t.Fatalf("grouped projection mismatch: refs=%d want=%d live=%d record=%d", got.valueLogRefCounts[grouped.FileID], wantRefs[grouped.FileID], got.valueLogLiveBytesBySegment[grouped.FileID], recordLen)
	}
	if _, ok := got.valueLogRefCounts[protected[0].FileID]; ok {
		t.Fatalf("protected-only segment %d entered value-log projection: %v", protected[0].FileID, got.valueLogRefCounts)
	}
	if got.counters.MemoHits == 0 {
		t.Fatalf("expected current collection roots to reuse user-root subtree memo: %+v", got.counters)
	}
	leafOpts := leafGenerationPackFromPlanPlanOptions(compactStorageLeafPackFromPlanOptions(
		opts,
		input.protectedRootIDs,
		input.protectedSystemRootIDs,
	))
	wantLeaf, err := db.LeafGenerationPlan(context.Background(), leafOpts)
	if err != nil {
		t.Fatalf("legacy leaf plan: %v", err)
	}
	gotLeaf := db.compactStorageLeafGenerationPlanFromAudit(leafOpts, input, got.leafGenerationLive)
	if !reflect.DeepEqual(compactStorageAuditPublicLeafPlan(gotLeaf), compactStorageAuditPublicLeafPlan(wantLeaf)) {
		t.Fatalf("leaf plan mismatch:\nshared=%+v\nlegacy=%+v", gotLeaf, wantLeaf)
	}
}

func compactStorageAuditPublicLeafPlan(plan LeafGenerationPlan) LeafGenerationPlan {
	plan.stateKey = treeReachabilityCacheKey{}
	plan.liveStats = leafGenerationLiveScanStats{}
	return plan
}

func TestCompactStorageAudit_ProtectedRootsDoNotPoisonTrackerOrSubsequentGC(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, db)

	protected := appendPointersInNewSegment(t, dir, 0, 1, 300_000, 3, func(i int) []byte {
		return bytes.Repeat([]byte(fmt.Sprintf("detached-%d|", i)), 32)
	})
	maintenance := appendPointersInNewSegment(t, dir, 0, 2, 310_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("maintenance|"), 32)
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("maintenance"), maintenance); err != nil {
		t.Fatalf("SetPointer maintenance: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write maintenance: %v", err)
	}
	closeNoErr(t, b)

	ordinaryRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemPointerMemtable(t, "detached/ordinary", protected[0]).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator ordinary: %v", err)
	}
	collectionRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemPointerMemtable(t, "detached/collection", protected[1]).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator collection: %v", err)
	}
	protectedSystemRoot, err := db.PublishOrderedRootIterator(0, compactStorageAuditMixedTable(
		t,
		map[string][]byte{maintenanceTestCollectionRootKey: encodeMaintenanceRootID(collectionRoot)},
		map[string]page.ValuePtr{"detached/system": protected[2]},
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator system: %v", err)
	}

	_, err = db.CompactStoragePlan(context.Background(), CompactStorageOptions{
		LeafGenerationProtectedRootIDs:       []uint64{ordinaryRoot},
		LeafGenerationProtectedSystemRootIDs: []uint64{protectedSystemRoot},
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan protected: %v", err)
	}
	trackerRefs, ok := db.valueLogRefTracker.referencedSet(db.currentCommitSeq())
	if !ok {
		t.Fatal("protected audit did not publish a current tracker")
	}
	if _, ok := trackerRefs[protected[0].FileID]; ok {
		t.Fatalf("protected-only segment %d entered tracker: %v", protected[0].FileID, trackerRefs)
	}
	if _, ok := trackerRefs[maintenance.FileID]; !ok {
		t.Fatalf("maintenance segment %d missing from tracker: %v", maintenance.FileID, trackerRefs)
	}

	var scans atomic.Uint64
	unregister := registerScanValueLogRefCountsHook(func() { scans.Add(1) })
	t.Cleanup(unregister)
	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC after removing protection: %v", err)
	}
	if got := scans.Load(); got != 0 {
		t.Fatalf("ValueLogGC legacy ref scans=%d want tracker-only resolution", got)
	}
	if stats.SegmentsDeleted != 1 {
		t.Fatalf("SegmentsDeleted=%d want 1: %+v", stats.SegmentsDeleted, stats)
	}
	protectedPath := filepath.Join(ValueLogDirPath(dir), "value-l0-000001.log")
	if _, err := os.Stat(protectedPath); !os.IsNotExist(err) {
		t.Fatalf("protected-only segment retained after protection removal: %v", err)
	}
}

func TestCompactStorageAudit_CountsValuePointersInsideOuterLeafPages(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	const records = 512
	ptrs := appendPointersInNewSegment(t, db.dir, 0, 1, 260_000, records, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i%23)}, 256)
	})
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet before write: %v", err)
	}
	b := db.NewBatch().(*Batch)
	for i, ptr := range ptrs {
		if err := b.SetPointer([]byte(fmt.Sprintf("outer-%06d", i)), ptr); err != nil {
			t.Fatalf("SetPointer %d: %v", i, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	closeNoErr(t, b)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet after write: %v", err)
	}

	wantRefs, _, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("legacy refs: %v", err)
	}
	input, err := db.acquireCompactStorageAuditInput(CompactStorageOptions{})
	if err != nil {
		t.Fatalf("acquire audit input: %v", err)
	}
	defer input.close()
	got, err := db.scanCompactStorageAudit(context.Background(), input)
	if err != nil {
		t.Fatalf("shared audit: %v", err)
	}
	fileID := ptrs[0].FileID
	if wantRefs[fileID] != records {
		t.Fatalf("legacy refs for file %d=%d want %d", fileID, wantRefs[fileID], records)
	}
	if !reflect.DeepEqual(got.valueLogRefCounts, wantRefs) {
		t.Fatalf("outer-leaf ref counts mismatch: shared=%v legacy=%v", got.valueLogRefCounts, wantRefs)
	}
}

func TestCompactStorageAudit_PlannersMatchStandaloneResults(t *testing.T) {
	db := openCompactStorageRewritePolicyBenchmarkFixture(t, 256, 32, 512)
	defer closeNoErr(t, db)

	opts := normalizeCompactStorageOptions(CompactStorageOptions{
		Mode:                           CompactStorageFull,
		DisableZeroByteValueLogCleanup: true,
	})
	protectedPaths := compactStorageFencedValueLogProtectedPaths(opts)
	wantRewrite, err := db.ValueLogRewritePlan(context.Background(), compactStorageRewritePlanOptions(opts, protectedPaths))
	if err != nil {
		t.Fatalf("legacy rewrite plan: %v", err)
	}
	wantGC, err := db.valueLogGC(context.Background(), ValueLogGCOptions{DryRun: true, ProtectedPaths: protectedPaths}, true)
	if err != nil {
		t.Fatalf("legacy value-log GC plan: %v", err)
	}

	got, err := db.CompactStoragePlan(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if !reflect.DeepEqual(got.ValueLogRewritePlan, wantRewrite) {
		t.Fatalf("rewrite plan mismatch:\nshared=%+v\nlegacy=%+v", got.ValueLogRewritePlan, wantRewrite)
	}
	if !reflect.DeepEqual(got.ValueLogGC, wantGC) {
		t.Fatalf("value-log GC plan mismatch:\nshared=%+v\nlegacy=%+v", got.ValueLogGC, wantGC)
	}
}

func TestCompactStorageAudit_ReusesStructureButRefreshesZeroByteDebt(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	session := &compactStorageAuditSession{}
	defer session.close()
	opts := normalizeCompactStorageOptions(CompactStorageOptions{})
	var first CompactStorageStats
	if _, err := db.populateCompactStorageAudit(context.Background(), opts, &first, true, nil, nil, session); err != nil {
		t.Fatalf("first audit: %v", err)
	}
	zeroPath := filepath.Join(ValueLogDirPath(db.dir), "value-l7-000777.log")
	if err := os.MkdirAll(filepath.Dir(zeroPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(zeroPath, nil, 0o644); err != nil {
		t.Fatalf("write zero-byte segment: %v", err)
	}
	var second CompactStorageStats
	debt, err := db.populateCompactStorageAudit(context.Background(), opts, &second, true, nil, nil, session)
	if err != nil {
		t.Fatalf("second audit: %v", err)
	}
	if second.Audit.SharedScans != 0 || second.Audit.StructuralReuseHits != 1 {
		t.Fatalf("expected structural reuse, counters=%+v", second.Audit)
	}
	if debt.ZeroByteValueLogFiles != 1 {
		t.Fatalf("zero-byte debt=%d want 1", debt.ZeroByteValueLogFiles)
	}
}

func TestCompactStorageAudit_ProtectedRootBasisDrift(t *testing.T) {
	tests := []struct {
		name        string
		stage       string
		ordinary    bool
		wantScans   uint64
		wantRetries uint64
	}{
		{
			name:        "ordinary provider changes across initial state capture",
			stage:       "acquire-after-first-protected-basis",
			ordinary:    true,
			wantScans:   1,
			wantRetries: 1,
		},
		{
			name:        "system provider changes across revalidation",
			stage:       "revalidate-after-first-protected-basis",
			wantScans:   2,
			wantRetries: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer closeNoErr(t, db)

			firstRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "protected/a", "a").NewIterator(nil, nil))
			if err != nil {
				t.Fatalf("PublishOrderedRootIterator first: %v", err)
			}
			secondRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "protected/b", "b").NewIterator(nil, nil))
			if err != nil {
				t.Fatalf("PublishOrderedRootIterator second: %v", err)
			}

			var providerMu sync.RWMutex
			providerRoot := firstRoot
			readProvider := func() []uint64 {
				providerMu.RLock()
				defer providerMu.RUnlock()
				return []uint64{providerRoot}
			}
			opts := CompactStorageOptions{}
			if tt.ordinary {
				opts.LeafGenerationProtectedRootIDsFunc = readProvider
			} else {
				opts.LeafGenerationProtectedSystemRootIDsFunc = readProvider
			}

			var advanced atomic.Bool
			db.compactStorageAuditProtectedBasisHook = func(stage string, attempt int) {
				if attempt != 0 || stage != tt.stage || !advanced.CompareAndSwap(false, true) {
					return
				}
				providerMu.Lock()
				providerRoot = secondRoot
				providerMu.Unlock()
			}
			defer func() { db.compactStorageAuditProtectedBasisHook = nil }()

			stats, err := db.CompactStoragePlan(context.Background(), opts)
			if err != nil {
				t.Fatalf("CompactStoragePlan: %v", err)
			}
			if !advanced.Load() {
				t.Fatal("protected-root provider was not advanced")
			}
			if stats.Audit.SharedScans != tt.wantScans || stats.Audit.RevalidationRetries != tt.wantRetries {
				t.Fatalf("audit counters=%+v want scans=%d retries=%d", stats.Audit, tt.wantScans, tt.wantRetries)
			}
		})
	}
}

type compactStorageAuditVersionedLeafLog struct {
	replayInlineLeafPageLog
	mu            sync.RWMutex
	rootIDs       []uint64
	systemRootIDs []uint64
	version       uint64
}

func (l *compactStorageAuditVersionedLeafLog) ProtectedLeafGenerationRootIDPairSnapshot() ([]uint64, []uint64, uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]uint64(nil), l.rootIDs...), append([]uint64(nil), l.systemRootIDs...), l.version
}

func (l *compactStorageAuditVersionedLeafLog) LeafPageLogLane(workerIndex int) (LeafPageLog, bool) {
	return l, workerIndex == 0
}

func (l *compactStorageAuditVersionedLeafLog) advanceVersion() {
	l.mu.Lock()
	l.version++
	l.mu.Unlock()
}

func TestCompactStorageAudit_ProtectedRootProviderVersionRejectsSameIDABA(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, db)

	provider := &compactStorageAuditVersionedLeafLog{
		rootIDs: []uint64{db.State().RootPageID},
		version: 1,
	}
	db.SetLeafPageLog(provider)
	var advanced atomic.Bool
	db.compactStorageAuditProtectedBasisHook = func(stage string, attempt int) {
		if stage == "acquire-after-first-protected-basis" && attempt == 0 && advanced.CompareAndSwap(false, true) {
			provider.advanceVersion()
		}
	}
	defer func() { db.compactStorageAuditProtectedBasisHook = nil }()

	stats, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if !advanced.Load() {
		t.Fatal("versioned provider did not advance")
	}
	if stats.Audit.SharedScans != 1 || stats.Audit.RevalidationRetries != 1 {
		t.Fatalf("same-ID provider ABA reused mixed basis: %+v", stats.Audit)
	}
}

func TestCompactStorageAudit_ProtectedRootProviderVersionInvalidatesStructuralReuse(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, db)
	writeCompactStorageAuditInvalidation(t, db, "provider-version-root")

	provider := &compactStorageAuditVersionedLeafLog{
		rootIDs: []uint64{db.State().RootPageID},
		version: 1,
	}
	db.SetLeafPageLog(provider)
	session := &compactStorageAuditSession{}
	defer session.close()
	if _, err := runCompactStorageAuditTableCase(db, session, CompactStorageOptions{}); err != nil {
		t.Fatalf("first audit: %v", err)
	}
	provider.advanceVersion()

	stats, err := runCompactStorageAuditTableCase(db, session, CompactStorageOptions{})
	if err != nil {
		t.Fatalf("second audit: %v", err)
	}
	if stats.Audit.SharedScans != 1 || stats.Audit.StructuralReuseHits != 0 || stats.Audit.LastStructuralReuseMissReason != "protected_root_set" {
		t.Fatalf("same-ID provider version did not invalidate structural cache: %+v", stats.Audit)
	}
}

func TestCompactStorageAudit_RepeatedProtectedRootDriftReturnsStaleWithoutTrackerPublication(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, db)
	db.valueLogRefTracker.invalidate()

	firstRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "protected/stale-a", "a").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator first: %v", err)
	}
	secondRoot, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, "protected/stale-b", "b").NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator second: %v", err)
	}
	var providerMu sync.RWMutex
	providerRoot := firstRoot
	opts := CompactStorageOptions{
		LeafGenerationProtectedRootIDsFunc: func() []uint64 {
			providerMu.RLock()
			defer providerMu.RUnlock()
			return []uint64{providerRoot}
		},
	}
	db.compactStorageAuditProtectedBasisHook = func(stage string, attempt int) {
		if stage != "revalidate-after-first-protected-basis" {
			return
		}
		providerMu.Lock()
		if attempt == 0 {
			providerRoot = secondRoot
		} else {
			providerRoot = firstRoot
		}
		providerMu.Unlock()
	}
	defer func() { db.compactStorageAuditProtectedBasisHook = nil }()

	_, err = db.CompactStoragePlan(context.Background(), opts)
	if !errors.Is(err, ErrCompactStorageAuditStale) {
		t.Fatalf("CompactStoragePlan error=%v want ErrCompactStorageAuditStale", err)
	}
	if _, ok := db.valueLogRefTracker.referencedSet(db.currentCommitSeq()); ok {
		t.Fatal("repeated protected-root drift published a current tracker")
	}
}

func TestCompactStorageAudit_PendingSegmentExcludedFromFencedDebtAndSettle(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer closeNoErr(t, db)

	pendingID := appendPointersInNewSegment(t, dir, 0, 1, 360_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("pending-fenced|"), 32)
	})[0].FileID
	active := appendPointersInNewSegment(t, dir, 0, 2, 370_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("active-fenced|"), 32)
	})[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	db.pendingValueLogAppendMu.Lock()
	db.pendingValueLogAppendFileIDRefs = map[uint32]int{pendingID: 1}
	db.pendingValueLogAppendMu.Unlock()

	activePath := filepath.Join(ValueLogDirPath(dir), "value-l0-000002.log")
	opts := normalizeCompactStorageOptions(CompactStorageOptions{
		UnsafeValueLogReclaimFencedUnreferenced: true,
		ValueLogFencedProtectedPathsFunc: func() []string {
			return []string{activePath}
		},
	})
	session := &compactStorageAuditSession{}
	defer session.close()
	var stats CompactStorageStats
	var fencedIDs []uint32
	debt, err := db.populateCompactStorageAudit(context.Background(), opts, &stats, true, &fencedIDs, nil, session)
	if err != nil {
		t.Fatalf("populateCompactStorageAudit: %v", err)
	}
	if compactStorageIDListContains(fencedIDs, pendingID) {
		t.Fatalf("pending file %d selected for fenced reclaim: %v", pendingID, fencedIDs)
	}
	if debt.ValueLogGCSegments != 0 || debt.ValueLogGCBytes != 0 {
		t.Fatalf("pending segment produced GC debt: %+v", debt)
	}
	if stats.ValueLogGC.SegmentsActive == 0 || active.FileID == pendingID {
		t.Fatalf("fixture did not classify pending/current segments as active: %+v", stats.ValueLogGC)
	}

	var settleStats CompactStorageStats
	if err := db.settleCompactStorageGC(context.Background(), opts, &settleStats, true, nil, nil, 0, session, nil); err != nil {
		t.Fatalf("settleCompactStorageGC: %v", err)
	}
	if len(settleStats.Phases) != 0 {
		t.Fatalf("pending-only debt started settle phases: %+v", settleStats.Phases)
	}
}

func TestCompactStorageAudit_ScanCountInvalidationTable(t *testing.T) {
	type scanCounts struct {
		shared uint64
		refs   uint64
		live   uint64
		leaf   uint64
	}
	var counts struct {
		shared atomic.Uint64
		refs   atomic.Uint64
		live   atomic.Uint64
		leaf   atomic.Uint64
	}
	unregisterShared := registerCompactStorageSharedAuditScanHook(func(compactStorageAuditCounters) { counts.shared.Add(1) })
	unregisterRefs := registerScanValueLogRefCountsHook(func() { counts.refs.Add(1) })
	unregisterLive := registerRewritePlanLiveEstimateHook(func() { counts.live.Add(1) })
	unregisterLeaf := registerLeafGenerationLiveScanHook(func() { counts.leaf.Add(1) })
	t.Cleanup(func() {
		unregisterLeaf()
		unregisterLive()
		unregisterRefs()
		unregisterShared()
	})
	load := func() scanCounts {
		return scanCounts{
			shared: counts.shared.Load(),
			refs:   counts.refs.Load(),
			live:   counts.live.Load(),
			leaf:   counts.leaf.Load(),
		}
	}
	delta := func(after, before scanCounts) scanCounts {
		return scanCounts{
			shared: after.shared - before.shared,
			refs:   after.refs - before.refs,
			live:   after.live - before.live,
			leaf:   after.leaf - before.leaf,
		}
	}

	tests := []struct {
		name       string
		prepare    func(t *testing.T, db *DB, session *compactStorageAuditSession) error
		run        func(t *testing.T, db *DB, session *compactStorageAuditSession) (CompactStorageStats, error)
		want       scanCounts
		missReason string
	}{
		{
			name: "cold",
			run: func(t *testing.T, db *DB, session *compactStorageAuditSession) (CompactStorageStats, error) {
				return runCompactStorageAuditTableCase(db, session, CompactStorageOptions{})
			},
			want: scanCounts{shared: 1}, missReason: "cold",
		},
		{
			name: "exact reuse",
			prepare: func(t *testing.T, db *DB, session *compactStorageAuditSession) error {
				_, err := runCompactStorageAuditTableCase(db, session, CompactStorageOptions{})
				return err
			},
			run: func(t *testing.T, db *DB, session *compactStorageAuditSession) (CompactStorageStats, error) {
				return runCompactStorageAuditTableCase(db, session, CompactStorageOptions{})
			},
			want: scanCounts{},
		},
		{
			name: "protected-root invalidation",
			prepare: func(t *testing.T, db *DB, session *compactStorageAuditSession) error {
				_, err := runCompactStorageAuditTableCase(db, session, CompactStorageOptions{})
				return err
			},
			run: func(t *testing.T, db *DB, session *compactStorageAuditSession) (CompactStorageStats, error) {
				return runCompactStorageAuditTableCase(db, session, CompactStorageOptions{
					LeafGenerationProtectedRootIDs: []uint64{db.State().RootPageID},
				})
			},
			want:       scanCounts{shared: 1},
			missReason: "protected_root_set",
		},
		{
			name: "protected-path invalidation",
			prepare: func(t *testing.T, db *DB, session *compactStorageAuditSession) error {
				_, err := runCompactStorageAuditTableCase(db, session, CompactStorageOptions{
					LeafGenerationProtectedRootIDs: []uint64{db.State().RootPageID},
				})
				return err
			},
			run: func(t *testing.T, db *DB, session *compactStorageAuditSession) (CompactStorageStats, error) {
				root := db.State().RootPageID
				return runCompactStorageAuditTableCase(db, session, CompactStorageOptions{
					LeafGenerationProtectedRootIDs: []uint64{root},
					ValueLogProtectedPaths:         []string{filepath.Join(ValueLogDirPath(db.dir), "protected.log")},
				})
			},
			want:       scanCounts{shared: 1},
			missReason: "protected_path_set",
		},
		{
			name: "commit invalidation",
			prepare: func(t *testing.T, db *DB, session *compactStorageAuditSession) error {
				if _, err := runCompactStorageAuditTableCase(db, session, CompactStorageOptions{}); err != nil {
					return err
				}
				writeCompactStorageAuditInvalidation(t, db, "table-commit")
				return nil
			},
			run: func(t *testing.T, db *DB, session *compactStorageAuditSession) (CompactStorageStats, error) {
				return runCompactStorageAuditTableCase(db, session, CompactStorageOptions{})
			},
			want:       scanCounts{shared: 1},
			missReason: "commit_seq",
		},
		{
			name: "cold retry",
			run: func(t *testing.T, db *DB, session *compactStorageAuditSession) (CompactStorageStats, error) {
				db.compactStorageAuditBeforeRevalidate = func(attempt int) {
					if attempt == 0 {
						writeCompactStorageAuditInvalidation(t, db, "table-retry")
					}
				}
				return runCompactStorageAuditTableCase(db, session, CompactStorageOptions{})
			},
			want:       scanCounts{shared: 2},
			missReason: "cold",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer closeNoErr(t, db)
			writeCompactStorageAuditInvalidation(t, db, "table-fixture")
			session := &compactStorageAuditSession{}
			defer session.close()
			if tt.prepare != nil {
				if err := tt.prepare(t, db, session); err != nil {
					t.Fatalf("prepare: %v", err)
				}
			}
			before := load()
			stats, err := tt.run(t, db, session)
			db.compactStorageAuditBeforeRevalidate = nil
			if err != nil {
				t.Fatalf("populateCompactStorageAudit: %v", err)
			}
			if got := delta(load(), before); got != tt.want {
				t.Fatalf("scan counts=%+v want %+v", got, tt.want)
			}
			if tt.missReason != "" && stats.Audit.LastStructuralReuseMissReason != tt.missReason {
				t.Fatalf("miss reason=%q want %q: %+v", stats.Audit.LastStructuralReuseMissReason, tt.missReason, stats.Audit)
			}
		})
	}
}

func runCompactStorageAuditTableCase(db *DB, session *compactStorageAuditSession, opts CompactStorageOptions) (CompactStorageStats, error) {
	var stats CompactStorageStats
	_, err := db.populateCompactStorageAudit(
		context.Background(),
		normalizeCompactStorageOptions(opts),
		&stats,
		true,
		nil,
		nil,
		session,
	)
	return stats, err
}

func TestCompactStorageAudit_StaleRevalidationRetriesOnce(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	var calls atomic.Uint64
	db.compactStorageAuditBeforeRevalidate = func(attempt int) {
		if calls.Add(1) != 1 {
			return
		}
		writeCompactStorageAuditInvalidation(t, db, "retry")
	}
	t.Cleanup(func() { db.compactStorageAuditBeforeRevalidate = nil })
	if _, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{}); err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("revalidation calls=%d want 2", got)
	}
	if _, ok := db.valueLogRefTracker.referencedSet(db.currentCommitSeq()); !ok {
		t.Fatal("successful retry did not replace tracker at current sequence")
	}
}

func TestCompactStorageAudit_ProtectedPathChangeRetriesOnce(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	protectedPath := "before"
	var calls atomic.Uint64
	db.compactStorageAuditBeforeRevalidate = func(attempt int) {
		if calls.Add(1) == 1 {
			protectedPath = "after"
		}
	}
	t.Cleanup(func() { db.compactStorageAuditBeforeRevalidate = nil })
	stats, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{
		ValueLogFencedProtectedPathsFunc: func() []string { return []string{protectedPath} },
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("revalidation calls=%d want 2", got)
	}
	if stats.Audit.RevalidationRetries != 1 || stats.Audit.SharedScans != 2 {
		t.Fatalf("unexpected retry counters: %+v", stats.Audit)
	}
}

func TestCompactStorageAudit_RepeatedInvalidationReturnsStaleError(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)
	db.valueLogRefTracker.invalidate()

	db.compactStorageAuditBeforeRevalidate = func(attempt int) {
		writeCompactStorageAuditInvalidation(t, db, fmt.Sprintf("stale-%d", attempt))
	}
	t.Cleanup(func() { db.compactStorageAuditBeforeRevalidate = nil })
	_, err = db.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if !errors.Is(err, ErrCompactStorageAuditStale) {
		t.Fatalf("CompactStoragePlan error=%v want ErrCompactStorageAuditStale", err)
	}
	if _, ok := db.valueLogRefTracker.referencedSet(db.currentCommitSeq()); ok {
		t.Fatal("repeated invalidation published a current tracker")
	}
}

func writeCompactStorageAuditInvalidation(t *testing.T, db *DB, key string) {
	t.Helper()
	b := db.NewBatch().(*Batch)
	if err := b.Set([]byte(key), []byte("value")); err != nil {
		closeNoErr(t, b)
		t.Fatalf("Set invalidation: %v", err)
	}
	if err := b.Write(); err != nil {
		closeNoErr(t, b)
		t.Fatalf("Write invalidation: %v", err)
	}
	closeNoErr(t, b)
}

func compactStorageAuditMixedTable(t *testing.T, inline map[string][]byte, pointers map[string]page.ValuePtr) memtable.Table {
	t.Helper()
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	for key, value := range inline {
		mt.Set([]byte(key), value)
	}
	for key, ptr := range pointers {
		mt.SetEntry([]byte(key), nil, ptr, node.FlagPointer)
	}
	mt.Freeze()
	return mt
}

func compactStorageAuditLeafLivePages(stats leafGenerationLiveScanStats) int {
	total := 0
	for _, generation := range stats.Generations {
		total += generation.LivePages
	}
	return total
}
