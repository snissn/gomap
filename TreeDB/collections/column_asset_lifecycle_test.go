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
		t.Fatalf("lifecycle report complete=true without durable registries: %+v", report.IncompleteReasons)
	}
	for _, reason := range []ColumnAssetLifecycleIncompleteReason{
		ColumnAssetLifecycleIncompletePendingPublishProcessLocalOnly,
		ColumnAssetLifecycleIncompletePreparedAssetProcessLocalOnly,
		ColumnAssetLifecycleIncompleteQuarantineProcessLocalOnly,
	} {
		if !columnAssetLifecycleReasonsContain(report.IncompleteReasons, reason) {
			t.Fatalf("incomplete reasons=%+v missing %q", report.IncompleteReasons, reason)
		}
	}
	if !report.PendingPublish.RegistryAvailable || !report.PendingPublish.ProcessLocal || report.PendingPublish.Durable || !report.PreparedAssets.RegistryAvailable || !report.PreparedAssets.ProcessLocal || report.PreparedAssets.Durable || !report.Quarantine.RegistryAvailable || !report.Quarantine.ProcessLocal || report.Quarantine.Durable {
		t.Fatalf("registry availability pending=%+v prepared=%+v quarantine=%+v", report.PendingPublish, report.PreparedAssets, report.Quarantine)
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
	if report.Bytes.ManifestCatalogBytes == 0 || report.Reachability.ManifestCatalogBytes != report.Bytes.ManifestCatalogBytes {
		t.Fatalf("manifest/catalog byte accounting missing: bytes=%+v reachability=%d", report.Bytes, report.Reachability.ManifestCatalogBytes)
	}
	if report.Bytes.ReferencedAssetBytes != report.Reachability.Refs.BytesTotal || report.Bytes.LiveBytes != report.Reachability.Sources.ActiveManifestBytes || report.Bytes.ProtectedBytes != report.Reachability.Refs.BytesProtected {
		t.Fatalf("asset byte classes=%+v refs=%+v sources=%+v", report.Bytes, report.Reachability.Refs, report.Reachability.Sources)
	}
	if report.Bytes.StaleBytes != candidate.Length+superseded.Length || report.Bytes.PendingPublishBytes != pending.Length || report.Bytes.PreparedAssetBytes != prepared.Length || report.Bytes.RewriteDebtBytes != report.Reachability.RewriteDebtBytes {
		t.Fatalf("unexpected lifecycle byte classes=%+v", report.Bytes)
	}
	if afterInfo, err := os.Stat(candidatePath); err != nil {
		t.Fatalf("stat candidate segment after report: %v", err)
	} else if afterInfo.Size() != beforeInfo.Size() {
		t.Fatalf("candidate segment size changed by report: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}
}

func TestColumnAssetLifecycleProcessRegistriesFeedReport1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnAssetLifecycleTestCollection1954(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	pending := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)
	prepared := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 100)
	quarantined := writeColumnAssetReachabilityCandidateM15A(t, d, col, 4, 101)
	quarantinePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), quarantined)
	if err != nil {
		t.Fatalf("quarantine segment path: %v", err)
	}
	beforeInfo, err := os.Stat(quarantinePath)
	if err != nil {
		t.Fatalf("stat quarantined segment before report: %v", err)
	}

	pendingLease, err := col.RegisterColumnAssetPendingPublish(ColumnAssetPendingPublishRegistrationOptions{
		Owner:  "publish-worker-1",
		Source: "column_publish",
		Reason: "ambiguous publish window",
		Refs:   []ColumnAssetRef{pending},
	})
	if err != nil {
		t.Fatalf("RegisterColumnAssetPendingPublish: %v", err)
	}
	defer func() { _ = pendingLease.Close() }()
	preparedLease, err := col.RegisterColumnAssetPreparedAsset(ColumnAssetPreparedAssetRegistrationOptions{
		Owner:  "compaction-worker-1",
		Source: "column_compaction",
		Reason: "prepared before publish",
		Refs:   []ColumnAssetRef{prepared},
	})
	if err != nil {
		t.Fatalf("RegisterColumnAssetPreparedAsset: %v", err)
	}
	defer func() { _ = preparedLease.Close() }()
	quarantineLease, err := col.RegisterColumnAssetQuarantine(ColumnAssetQuarantineRegistrationOptions{
		Owner:  "integrity-checker-1",
		Source: "read_integrity",
		Reason: "checksum mismatch",
		Refs:   []ColumnAssetRef{quarantined},
		Segments: []ColumnAssetQuarantineSegment{{
			FileID: quarantined.FileID,
			Bytes:  beforeInfo.Size(),
			Reason: "logical quarantine only",
		}},
	})
	if err != nil {
		t.Fatalf("RegisterColumnAssetQuarantine: %v", err)
	}
	defer func() { _ = quarantineLease.Close() }()
	if pendingLease.ID() == 0 || pendingLease.Class() != ColumnAssetLifecycleRegistryPendingPublish || pendingLease.Source() != "column_publish" || len(pendingLease.Refs()) != 1 {
		t.Fatalf("unexpected pending lease id/class/source/refs: id=%d class=%q source=%q refs=%+v", pendingLease.ID(), pendingLease.Class(), pendingLease.Source(), pendingLease.Refs())
	}
	if got := quarantineLease.Segments(); len(got) != 1 || got[0].Namespace != "events/column-assets" || got[0].FileID != quarantined.FileID || got[0].Bytes != beforeInfo.Size() {
		t.Fatalf("unexpected quarantine lease segments: %+v", got)
	}

	report, err := col.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle: %v", err)
	}
	if report.Complete {
		t.Fatalf("process-local registry report unexpectedly complete: reasons=%+v", report.IncompleteReasons)
	}
	for _, reason := range []ColumnAssetLifecycleIncompleteReason{
		ColumnAssetLifecycleIncompletePendingPublishProcessLocalOnly,
		ColumnAssetLifecycleIncompletePreparedAssetProcessLocalOnly,
		ColumnAssetLifecycleIncompleteQuarantineProcessLocalOnly,
	} {
		if !columnAssetLifecycleReasonsContain(report.IncompleteReasons, reason) {
			t.Fatalf("incomplete reasons=%+v missing %q", report.IncompleteReasons, reason)
		}
	}
	if report.Roots.PendingPublishRegistryRecords != 1 || report.Roots.PreparedAssetRegistryRecords != 1 || report.Roots.QuarantineRegistryRecords != 1 {
		t.Fatalf("registry root records=%+v", report.Roots)
	}
	if report.Roots.PendingPublishRefs != 1 || report.Roots.PreparedAssetRefs != 1 || report.Roots.QuarantineRefs != 1 || report.Roots.QuarantineSegments != 1 {
		t.Fatalf("registry root refs/segments=%+v", report.Roots)
	}
	if report.PendingPublish.OpenRecords != 1 || report.PendingPublish.Refs != 1 || report.PendingPublish.Bytes != pending.Length || !columnAssetLifecycleRegistrySourcesContain(report.PendingPublish.Sources, "column_publish", 1, pending.Length, 0, 0) {
		t.Fatalf("pending summary=%+v", report.PendingPublish)
	}
	if report.PreparedAssets.OpenRecords != 1 || report.PreparedAssets.Refs != 1 || report.PreparedAssets.Bytes != prepared.Length || !columnAssetLifecycleRegistrySourcesContain(report.PreparedAssets.Sources, "column_compaction", 1, prepared.Length, 0, 0) {
		t.Fatalf("prepared summary=%+v", report.PreparedAssets)
	}
	if report.Quarantine.OpenRecords != 1 || report.Quarantine.Refs != 1 || report.Quarantine.Bytes != quarantined.Length || report.Quarantine.Segments != 1 || report.Quarantine.SegmentBytes != beforeInfo.Size() || !columnAssetLifecycleRegistrySourcesContain(report.Quarantine.Sources, "read_integrity", 1, quarantined.Length, 1, beforeInfo.Size()) {
		t.Fatalf("quarantine summary=%+v", report.Quarantine)
	}
	for _, tc := range []struct {
		ref    ColumnAssetRef
		source ColumnAssetReachabilitySource
	}{
		{pending, ColumnAssetReachabilitySourcePendingPublish},
		{prepared, ColumnAssetReachabilitySourcePreparedAsset},
		{quarantined, ColumnAssetReachabilitySourceQuarantine},
	} {
		entry, ok := columnAssetLifecycleFindEntry(report.Reachability.Entries, tc.ref)
		if !ok {
			t.Fatalf("missing registry ref entry: %+v", tc.ref)
		}
		if entry.Status != ColumnAssetReachabilityProtected || !slices.Contains(entry.Sources, tc.source) {
			t.Fatalf("registry entry=%+v want protected source %q", entry, tc.source)
		}
	}
	if afterInfo, err := os.Stat(quarantinePath); err != nil {
		t.Fatalf("stat quarantined segment after report: %v", err)
	} else if afterInfo.Size() != beforeInfo.Size() {
		t.Fatalf("quarantined segment size changed by report: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}
}

func TestColumnAssetLifecycleRegistryReleaseAndDBCloseCleanup1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnAssetLifecycleTestCollection1954(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	pending := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)
	pendingLease, err := col.RegisterColumnAssetPendingPublish(ColumnAssetPendingPublishRegistrationOptions{
		Owner:  "release-test",
		Source: "publish",
		Refs:   []ColumnAssetRef{pending},
	})
	if err != nil {
		t.Fatalf("RegisterColumnAssetPendingPublish: %v", err)
	}
	if err := pendingLease.Release(); err != nil {
		t.Fatalf("pending release: %v", err)
	}
	if err := pendingLease.Release(); err != nil {
		t.Fatalf("pending release second call: %v", err)
	}
	afterRelease, err := col.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle after release: %v", err)
	}
	if afterRelease.PendingPublish.OpenRecords != 0 || afterRelease.Roots.PendingPublishRefs != 0 || afterRelease.Reachability.Sources.PendingRefs != 0 {
		t.Fatalf("released pending registry still reported: pending=%+v roots=%+v sources=%+v", afterRelease.PendingPublish, afterRelease.Roots, afterRelease.Reachability.Sources)
	}

	quarantined := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 100)
	quarantineLease, err := col.RegisterColumnAssetQuarantine(ColumnAssetQuarantineRegistrationOptions{
		Owner:  "db-close-test",
		Source: "integrity",
		Refs:   []ColumnAssetRef{quarantined},
	})
	if err != nil {
		t.Fatalf("RegisterColumnAssetQuarantine: %v", err)
	}
	quarantineID := quarantineLease.ID()
	if quarantineID == 0 {
		t.Fatalf("quarantine lease id is zero")
	}
	dbID := columnAssetLifecycleRegistryProcessDBID(d)
	if dbID == 0 {
		t.Fatalf("lifecycle registry DB id is zero")
	}
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	columnAssetLifecycleProcessRegistries.Lock()
	defer columnAssetLifecycleProcessRegistries.Unlock()
	if _, ok := columnAssetLifecycleProcessRegistries.records[quarantineID]; ok {
		t.Fatalf("registry record for closed DB leaked under id %d", quarantineID)
	}
	for _, record := range columnAssetLifecycleProcessRegistries.records {
		if record.Scope.dbID == dbID {
			t.Fatalf("registry record for closed DB leaked: %+v", record)
		}
	}
	if _, ok := columnAssetLifecycleProcessRegistries.dbIDs[d]; ok {
		t.Fatalf("closed DB remained registered for lifecycle registry cleanup")
	}
}

func TestColumnAssetLifecycleRegistrySharedAcrossManagersAndProcessLocal1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	ownerMgr := NewCollectionManager(d)
	owner, err := ownerMgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection owner: %v", err)
	}
	if _, err := owner.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	pending := writeColumnAssetReachabilityCandidateM15A(t, d, owner, 2, 99)
	lease, err := owner.RegisterColumnAssetPendingPublish(ColumnAssetPendingPublishRegistrationOptions{
		Owner:  "owner-manager",
		Source: "publish",
		Refs:   []ColumnAssetRef{pending},
	})
	if err != nil {
		t.Fatalf("RegisterColumnAssetPendingPublish: %v", err)
	}

	maintenanceMgr := NewCollectionManager(d)
	maintenance, err := maintenanceMgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection maintenance: %v", err)
	}
	report, err := maintenance.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle from separate manager: %v", err)
	}
	if report.PendingPublish.OpenRecords != 1 || report.Roots.PendingPublishRefs != 1 || report.Reachability.Sources.PendingRefs != 1 {
		t.Fatalf("separate manager missed pending registry: pending=%+v roots=%+v sources=%+v", report.PendingPublish, report.Roots, report.Reachability.Sources)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close original DB: %v", err)
	}
	if lease.ID() == 0 {
		t.Fatalf("lease unexpectedly zeroed by DB close; Close should only clear registry state")
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol := openColumnAssetLifecycleTestCollection1954(t, reopened)
	reopenedReport, err := reopenedCol.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle after reopen: %v", err)
	}
	if reopenedReport.PendingPublish.OpenRecords != 0 || reopenedReport.Roots.PendingPublishRefs != 0 || reopenedReport.Reachability.Sources.PendingRefs != 0 {
		t.Fatalf("process-local pending registry survived reopen: pending=%+v roots=%+v sources=%+v", reopenedReport.PendingPublish, reopenedReport.Roots, reopenedReport.Reachability.Sources)
	}
}

func TestColumnAssetLifecycleRegistryNamespaceScoping1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnAssetLifecycleTestCollection1954(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	ref := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)
	badRef := ref
	badRef.Namespace = "other/column-assets"
	if _, err := col.RegisterColumnAssetPreparedAsset(ColumnAssetPreparedAssetRegistrationOptions{Owner: "bad-ref", Source: "test", Refs: []ColumnAssetRef{badRef}}); err == nil {
		t.Fatalf("RegisterColumnAssetPreparedAsset accepted ref from wrong namespace")
	}
	if _, err := col.RegisterColumnAssetQuarantine(ColumnAssetQuarantineRegistrationOptions{Owner: "bad-segment", Source: "test", Segments: []ColumnAssetQuarantineSegment{{Namespace: "other/column-assets", FileID: ref.FileID}}}); err == nil {
		t.Fatalf("RegisterColumnAssetQuarantine accepted segment from wrong namespace")
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

func TestColumnPhysicalPreparedQueryLifecyclePinsSnapshotRefs1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":10,"kind":"like","did":"did:a"}`),
		[]byte(`{"time_us":20,"kind":"post","did":"did:b"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	runner, err := col.PrepareColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupMinInt64,
		GroupColumn: "did",
		ValueColumn: "time_us",
	})
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	refs := columnPhysicalScanSnapshotViewAssetRefs(runner.view)
	if len(refs) == 0 {
		t.Fatalf("prepared runner snapshot refs empty")
	}
	if runner.lifecyclePin == nil || runner.lifecyclePin.ID() == 0 || runner.lifecyclePin.Source() != ColumnAssetLifecyclePinSourcePreparedQuery {
		t.Fatalf("prepared runner lifecycle pin=%+v", runner.lifecyclePin)
	}
	if pinRefs := runner.lifecyclePin.Refs(); !slices.Equal(pinRefs, refs) {
		t.Fatalf("pin refs=%+v want snapshot refs=%+v", pinRefs, refs)
	}
	if result, err := runner.Run(); err != nil || len(result.Groups) == 0 {
		t.Fatalf("prepared runner Run result=%+v err=%v", result, err)
	}

	report, err := col.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle with open prepared runner: %v", err)
	}
	if report.PinSets.OpenSessions != 1 || report.PinSets.Refs != len(refs) {
		t.Fatalf("pin summary=%+v want one prepared runner with %d refs", report.PinSets, len(refs))
	}
	if report.Roots.LifecyclePinSets != 1 || report.Roots.LifecyclePinnedRefs != len(refs) || report.Roots.PreparedQueryRefs != len(refs) || report.Reachability.Sources.PreparedQueryRefs != len(refs) {
		t.Fatalf("prepared root counts=%+v sources=%+v want %d prepared-query refs", report.Roots, report.Reachability.Sources, len(refs))
	}
	for _, ref := range refs {
		entry, ok := columnAssetLifecycleFindEntry(report.Reachability.Entries, ref)
		if !ok {
			t.Fatalf("missing prepared runner ref entry: %+v", ref)
		}
		if entry.Status != ColumnAssetReachabilityProtected || !slices.Contains(entry.Sources, ColumnAssetReachabilitySourcePreparedQuery) {
			t.Fatalf("prepared runner entry=%+v want protected prepared_query", entry)
		}
	}

	if err := runner.Close(); err != nil {
		t.Fatalf("runner close: %v", err)
	}
	if err := runner.Close(); err != nil {
		t.Fatalf("runner close second call: %v", err)
	}
	if runner.lifecyclePin != nil {
		t.Fatalf("runner retained lifecycle pin after Close")
	}
	afterClose, err := col.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle after prepared runner close: %v", err)
	}
	if afterClose.PinSets.OpenSessions != 0 || afterClose.PinSets.Refs != 0 || afterClose.Roots.PreparedQueryRefs != 0 || afterClose.Reachability.Sources.PreparedQueryRefs != 0 {
		t.Fatalf("prepared pin still reported after Close: pins=%+v roots=%+v sources=%+v", afterClose.PinSets, afterClose.Roots, afterClose.Reachability.Sources)
	}
	for _, ref := range refs {
		entry, ok := columnAssetLifecycleFindEntry(afterClose.Reachability.Entries, ref)
		if !ok {
			t.Fatalf("missing prepared runner ref entry after Close: %+v", ref)
		}
		if slices.Contains(entry.Sources, ColumnAssetReachabilitySourcePreparedQuery) {
			t.Fatalf("entry retained prepared_query after Close: %+v", entry)
		}
	}
}

func TestColumnPhysicalPreparedQueryLifecyclePinReleasedOnDBClose1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnAssetLifecycleTestCollection1954(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	runner, err := col.PrepareColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"})
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	pinID := uint64(0)
	if runner.lifecyclePin != nil {
		pinID = runner.lifecyclePin.ID()
	}
	if pinID == 0 {
		t.Fatalf("prepared runner lifecycle pin id is zero")
	}
	dbID := columnAssetLifecycleProcessDBID(d)
	if dbID == 0 {
		t.Fatalf("lifecycle pin DB id is zero")
	}
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	columnAssetLifecycleProcessPins.Lock()
	if _, ok := columnAssetLifecycleProcessPins.pins[pinID]; ok {
		columnAssetLifecycleProcessPins.Unlock()
		t.Fatalf("prepared runner pin record for closed DB leaked under id %d", pinID)
	}
	for _, record := range columnAssetLifecycleProcessPins.pins {
		if record.Scope.dbID == dbID {
			columnAssetLifecycleProcessPins.Unlock()
			t.Fatalf("prepared runner pin record for closed DB leaked: %+v", record)
		}
	}
	if _, ok := columnAssetLifecycleProcessPins.dbIDs[d]; ok {
		columnAssetLifecycleProcessPins.Unlock()
		t.Fatalf("closed DB remained registered for prepared runner lifecycle pin cleanup")
	}
	columnAssetLifecycleProcessPins.Unlock()
	if err := runner.Close(); err != nil {
		t.Fatalf("runner close after DB close hook cleanup: %v", err)
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
	pinID := pin.ID()
	if pinID == 0 {
		t.Fatalf("pin id is zero")
	}
	dbID := columnAssetLifecycleProcessDBID(d)
	if dbID == 0 {
		t.Fatalf("lifecycle pin DB id is zero")
	}
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	columnAssetLifecycleProcessPins.Lock()
	defer columnAssetLifecycleProcessPins.Unlock()
	if _, ok := columnAssetLifecycleProcessPins.pins[pinID]; ok {
		t.Fatalf("pin record for closed DB leaked under id %d", pinID)
	}
	for _, record := range columnAssetLifecycleProcessPins.pins {
		if record.Scope.dbID == dbID {
			t.Fatalf("pin record for closed DB leaked: %+v", record)
		}
	}
	if _, ok := columnAssetLifecycleProcessPins.dbIDs[d]; ok {
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

func columnAssetLifecycleRegistrySourcesContain(sources []ColumnAssetLifecycleRegistrySourceSummary, source string, refs int, bytes int64, segments int, segmentBytes int64) bool {
	for _, summary := range sources {
		if summary.Source == source && summary.Refs == refs && summary.Bytes == bytes && summary.Segments == segments && summary.SegmentBytes == segmentBytes {
			return true
		}
	}
	return false
}
