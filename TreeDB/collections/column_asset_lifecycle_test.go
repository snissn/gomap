package collections

import (
	"context"
	"os"
	"slices"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestColumnAssetLifecycleReportInventoriesRootsAndStaysReportOnly1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnAssetLifecycleTestCollection1954(t, d)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"ignored"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2","payload":"ignored"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	manifestRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(manifestRefs) == 0 {
		t.Fatal("manifest refs empty, test requires physical column assets")
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	superseded := writeColumnAssetReachabilityCandidateM15A(t, d, col, 4, 100)
	pending := writeColumnAssetReachabilityCandidateM15A(t, d, col, 5, 101)
	prepared := writeColumnAssetReachabilityCandidateM15A(t, d, col, 6, 102)
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("candidate segment path: %v", err)
	}
	beforeInfo, err := os.Stat(candidatePath)
	if err != nil {
		t.Fatalf("stat candidate segment before report: %v", err)
	}

	report, err := col.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{
		Detailed:       true,
		SegmentDetails: true,
		CandidateRefs:  []ColumnAssetRef{candidate},
		SupersededRefs: []ColumnAssetRef{superseded},
		PendingRefs:    []ColumnAssetRef{pending},
		PreparedRefs:   []ColumnAssetRef{prepared},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle: %v", err)
	}
	if !report.DryRun || !report.ReportOnly || report.Actions.DestructiveActionsEnabled {
		t.Fatalf("report action flags dry_run=%v report_only=%v actions=%+v", report.DryRun, report.ReportOnly, report.Actions)
	}
	if report.Complete {
		t.Fatalf("slice-1 lifecycle report complete=true without durable registries: %+v", report.IncompleteReasons)
	}
	for _, reason := range []ColumnAssetLifecycleIncompleteReason{
		ColumnAssetLifecycleIncompletePendingPublishRegistry,
		ColumnAssetLifecycleIncompletePreparedAssetRegistry,
		ColumnAssetLifecycleIncompleteQuarantineRegistry,
	} {
		if !columnAssetLifecycleReasonsContain(report.IncompleteReasons, reason) {
			t.Fatalf("incomplete reasons=%+v missing %q", report.IncompleteReasons, reason)
		}
	}
	if report.Identity.Collection != "events" || report.Identity.Namespace != "events/column-assets" || report.Identity.ManifestRootName == "" || report.Identity.ManifestRootID == 0 || report.Identity.SystemRoot == 0 || report.Identity.PlanCommitSeq == 0 {
		t.Fatalf("unexpected identity: %+v", report.Identity)
	}
	if report.Identity.ActiveManifestGeneration == 0 || report.Identity.ActiveManifestChecksum == 0 || report.Identity.RecoveryManifestGeneration != report.Identity.ActiveManifestGeneration || report.Identity.RecoveryManifestChecksum != report.Identity.ActiveManifestChecksum {
		t.Fatalf("unexpected manifest identity: %+v", report.Identity)
	}
	if report.Roots.ActiveManifestRefs != len(manifestRefs) || report.Roots.RecoveryManifestRefs != len(manifestRefs) {
		t.Fatalf("manifest roots=%+v want active/recovery=%d", report.Roots, len(manifestRefs))
	}
	if report.Roots.CandidateRefs != 1 || report.Roots.SupersededRefs != 1 || report.Roots.PendingPublishRefs != 1 || report.Roots.PreparedAssetRefs != 1 {
		t.Fatalf("supplied root counts=%+v", report.Roots)
	}
	if report.Reachability.Sources.CandidateRefs != 2 || report.Reachability.Sources.PendingRefs != 1 || report.Reachability.Sources.PreparedRefs != 1 {
		t.Fatalf("reachability sources=%+v", report.Reachability.Sources)
	}
	if afterInfo, err := os.Stat(candidatePath); err != nil {
		t.Fatalf("stat candidate segment after report: %v", err)
	} else if afterInfo.Size() != beforeInfo.Size() {
		t.Fatalf("candidate segment size changed by report: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}
}

func TestColumnAssetLifecycleReportExplicitPinSetScaffold1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)
	pin, err := col.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{
		Source: ColumnAssetLifecyclePinSourcePreparedQuery,
		Owner:  "prepared-runner-test",
		Reason: "slice-1 explicit pin scaffold",
		Refs:   []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("AcquireColumnAssetLifecyclePinSet: %v", err)
	}
	if pin.ID() == 0 || pin.Source() != ColumnAssetLifecyclePinSourcePreparedQuery || len(pin.Refs()) != 1 {
		t.Fatalf("unexpected pin id/source/refs: id=%d source=%q refs=%+v", pin.ID(), pin.Source(), pin.Refs())
	}

	sibling, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection sibling: %v", err)
	}
	report, err := sibling.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle with pin: %v", err)
	}
	if report.PinSets.OpenSessions != 1 || report.PinSets.Refs != 1 || report.PinSets.Bytes != candidate.Length {
		t.Fatalf("pin summary=%+v want one pinned ref length=%d", report.PinSets, candidate.Length)
	}
	if report.Roots.LifecyclePinSets != 1 || report.Roots.LifecyclePinnedRefs != 1 || report.Roots.PreparedQueryRefs != 1 || report.Reachability.Sources.PreparedQueryRefs != 1 {
		t.Fatalf("pin root counts=%+v sources=%+v", report.Roots, report.Reachability.Sources)
	}
	entry, ok := columnAssetLifecycleFindEntry(report.Reachability.Entries, candidate)
	if !ok {
		t.Fatalf("missing candidate entry in report")
	}
	if entry.Status != ColumnAssetReachabilityProtected || !slices.Contains(entry.Sources, ColumnAssetReachabilitySourcePreparedQuery) {
		t.Fatalf("pinned entry=%+v want protected prepared_query", entry)
	}

	if err := pin.Close(); err != nil {
		t.Fatalf("pin close: %v", err)
	}
	afterClose, err := sibling.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle after close: %v", err)
	}
	if afterClose.PinSets.OpenSessions != 0 || afterClose.Roots.PreparedQueryRefs != 0 || afterClose.Reachability.Sources.PreparedQueryRefs != 0 {
		t.Fatalf("pin still reported after close: pins=%+v roots=%+v sources=%+v", afterClose.PinSets, afterClose.Roots, afterClose.Reachability.Sources)
	}
	entry, ok = columnAssetLifecycleFindEntry(afterClose.Reachability.Entries, candidate)
	if !ok {
		t.Fatalf("missing candidate entry after close")
	}
	if entry.Status != ColumnAssetReachabilityReclaimable || slices.Contains(entry.Sources, ColumnAssetReachabilitySourcePreparedQuery) {
		t.Fatalf("after close entry=%+v want reclaimable without prepared_query", entry)
	}
}

func TestColumnAssetLifecycleReportPinSetSharedAcrossHandles1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection owner: %v", err)
	}
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)
	pin, err := col.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{
		Source: ColumnAssetLifecyclePinSourcePreparedQuery,
		Owner:  "prepared-runner-owner-handle",
		Refs:   []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("AcquireColumnAssetLifecyclePinSet: %v", err)
	}
	defer func() { _ = pin.Close() }()

	maintenance, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection maintenance: %v", err)
	}
	report, err := maintenance.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle from sibling handle: %v", err)
	}
	if report.PinSets.OpenSessions != 1 || report.Roots.PreparedQueryRefs != 1 || report.Reachability.Sources.PreparedQueryRefs != 1 {
		t.Fatalf("sibling handle missed lifecycle pin: pins=%+v roots=%+v sources=%+v", report.PinSets, report.Roots, report.Reachability.Sources)
	}
	entry, ok := columnAssetLifecycleFindEntry(report.Reachability.Entries, candidate)
	if !ok {
		t.Fatalf("missing candidate entry")
	}
	if entry.Status != ColumnAssetReachabilityProtected || !slices.Contains(entry.Sources, ColumnAssetReachabilitySourcePreparedQuery) {
		t.Fatalf("sibling handle entry=%+v want protected prepared_query", entry)
	}
}

func TestColumnAssetLifecycleReportPinSetSharedAcrossManagers1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	ownerMgr := NewCollectionManager(d)
	owner, err := ownerMgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection owner: %v", err)
	}
	if _, err := owner.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, owner, 2, 99)
	pin, err := owner.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{
		Source: ColumnAssetLifecyclePinSourcePreparedQuery,
		Owner:  "prepared-runner-owner-manager",
		Refs:   []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("AcquireColumnAssetLifecyclePinSet: %v", err)
	}
	defer func() { _ = pin.Close() }()

	maintenanceMgr := NewCollectionManager(d)
	maintenance, err := maintenanceMgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection maintenance: %v", err)
	}
	report, err := maintenance.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle from separate manager: %v", err)
	}
	if report.PinSets.OpenSessions != 1 || report.Roots.PreparedQueryRefs != 1 || report.Reachability.Sources.PreparedQueryRefs != 1 {
		t.Fatalf("separate manager missed lifecycle pin: pins=%+v roots=%+v sources=%+v", report.PinSets, report.Roots, report.Reachability.Sources)
	}
	entry, ok := columnAssetLifecycleFindEntry(report.Reachability.Entries, candidate)
	if !ok {
		t.Fatalf("missing candidate entry")
	}
	if entry.Status != ColumnAssetReachabilityProtected || !slices.Contains(entry.Sources, ColumnAssetReachabilitySourcePreparedQuery) {
		t.Fatalf("separate manager entry=%+v want protected prepared_query", entry)
	}
}

func TestColumnAssetLifecyclePinSetReleasedOnDBClose1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnAssetLifecycleTestCollection1954(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)
	pin, err := col.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{
		Source: ColumnAssetLifecyclePinSourcePreparedQuery,
		Owner:  "prepared-runner-db-close",
		Refs:   []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("AcquireColumnAssetLifecyclePinSet: %v", err)
	}
	if pin.ID() == 0 {
		t.Fatalf("pin id is zero")
	}
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	columnAssetLifecycleProcessPins.Lock()
	defer columnAssetLifecycleProcessPins.Unlock()
	for _, record := range columnAssetLifecycleProcessPins.pins {
		if record.Scope.db == d {
			t.Fatalf("pin record for closed DB leaked: %+v", record)
		}
	}
	if columnAssetLifecycleProcessPins.registeredDBs[d] {
		t.Fatalf("closed DB remained registered for lifecycle pin cleanup")
	}
}

func TestColumnAssetLifecycleReportMappedResourceUnconvertibleIncomplete1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnAssetLifecycleTestCollection1954(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	manifestRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(manifestRefs) == 0 {
		t.Fatal("manifest refs empty, test requires physical column assets")
	}
	namespace := manifestRefs[0].Namespace
	mgr := mappedresource.NewManager()
	key := mappedresource.Key{
		Class:      mappedresource.ClassTypedColumnAsset,
		Namespace:  namespace,
		Kind:       "unexpected_section_only_kind",
		Generation: 1,
		PartID:     1,
		FileID:     1,
		Offset:     0,
		Length:     4,
		Checksum:   7,
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "lifecycle-unconvertible-pin-1954", Namespace: namespace, Collection: "events", Generation: 1}
	handle, err := mgr.AcquireBytes(key, scope, mappedresource.SourceHeapCopy, []byte("pin!"), mappedresource.AcquireOptions{Reason: "lifecycle-unconvertible-pin", ResourceRoot: d.ColumnAssetRootDir()})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	defer func() { _ = handle.Release() }()

	report, err := col.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle: %v", err)
	}
	if report.ReachabilityComplete || report.Reachability.Complete || report.MappedResources.UnconvertiblePins != 1 || report.MappedResources.ActiveHandles == 0 {
		t.Fatalf("mappedresource report complete=%v reachability=%v mapped=%+v", report.Complete, report.Reachability.Complete, report.MappedResources)
	}
	for _, reason := range []ColumnAssetLifecycleIncompleteReason{
		ColumnAssetLifecycleIncompleteReachabilityPlan,
		ColumnAssetLifecycleIncompleteMappedResourcePins,
	} {
		if !columnAssetLifecycleReasonsContain(report.IncompleteReasons, reason) {
			t.Fatalf("incomplete reasons=%+v missing %q", report.IncompleteReasons, reason)
		}
	}
}

func TestColumnAssetLifecycleReportSnapshotFenceFailsClosed1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnAssetLifecycleTestCollection1954(t, d)
	oldSnap := d.AcquireSnapshot()
	if oldSnap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = oldSnap.Close() }()

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	report, err := col.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle: %v", err)
	}
	if !report.SnapshotFence.OlderSnapshotPinned || report.SnapshotFence.ExactSnapshotRootsAvailable || report.SnapshotFence.MinPinnedSnapshotCommitSeq >= report.SnapshotFence.PlanCommitSeq {
		t.Fatalf("snapshot fence=%+v want older pinned without exact roots", report.SnapshotFence)
	}
	if !columnAssetLifecycleReasonsContain(report.IncompleteReasons, ColumnAssetLifecycleIncompletePinnedSnapshotExactRoots) {
		t.Fatalf("incomplete reasons=%+v missing pinned snapshot exact-root reason", report.IncompleteReasons)
	}
}

func TestColumnAssetLifecycleReportByteAccountingUsesIntervalUnion1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnAssetLifecycleTestCollection1954(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)
	if candidate.Length < 8 {
		t.Fatalf("candidate length=%d too small for overlap test", candidate.Length)
	}
	first := candidate
	first.Length = candidate.Length / 2
	second := candidate
	second.Offset += candidate.Length / 4
	second.Length = candidate.Length / 2
	wantUnion := candidate.Length/2 + candidate.Length/4

	report, err := col.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{
		CandidateRefs: []ColumnAssetRef{first, second},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle: %v", err)
	}
	if got, sum := report.Reachability.Segments.BytesReclaimable, first.Length+second.Length; got != wantUnion || got >= sum {
		t.Fatalf("reclaimable bytes=%d want interval union=%d and less than double-counted sum=%d", got, wantUnion, sum)
	}
}

func openColumnAssetLifecycleTestCollection1954(t testing.TB, d *backenddb.DB) *Collection {
	t.Helper()
	col, err := NewCollectionManager(d).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func columnAssetLifecycleReasonsContain(reasons []ColumnAssetLifecycleIncompleteReason, want ColumnAssetLifecycleIncompleteReason) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}

func columnAssetLifecycleFindEntry(entries []ColumnAssetReachabilityRefEntry, ref ColumnAssetRef) (ColumnAssetReachabilityRefEntry, bool) {
	for _, entry := range entries {
		if entry.Ref == ref {
			return entry, true
		}
	}
	return ColumnAssetReachabilityRefEntry{}, false
}
