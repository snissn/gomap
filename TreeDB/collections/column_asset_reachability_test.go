package collections

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnAssetReachabilityPlanProtectsActiveManifestRefsM15A(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"ignored"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2","payload":"ignored"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(refs) == 0 {
		t.Fatal("manifest refs empty, test requires physical column assets")
	}

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if !plan.ProtectOnly {
		t.Fatalf("ProtectOnly=false, M15A must be dry-run/protect-only")
	}
	if plan.Collection != "events" || plan.Namespace != "events/column-assets" {
		t.Fatalf("unexpected plan identity: %+v", plan)
	}
	if plan.Sources.ActiveManifestRefs != len(refs) || plan.Sources.RecoveryManifestRefs != len(refs) {
		t.Fatalf("source refs=%+v want active/recovery=%d", plan.Sources, len(refs))
	}
	if plan.Refs.Protected != len(refs) || plan.Refs.Reclaimable != 0 || plan.Refs.Uncertain != 0 {
		t.Fatalf("ref stats=%+v want protected=%d only", plan.Refs, len(refs))
	}
	if plan.Segments.Protected == 0 || plan.Segments.Reclaimable != 0 || plan.Segments.Unknown != 0 {
		t.Fatalf("segment stats=%+v want protected segment only", plan.Segments)
	}
	if plan.RewriteDebtBytes != 0 {
		t.Fatalf("rewrite debt=%d want 0 for fully live manifest", plan.RewriteDebtBytes)
	}
	if len(plan.Entries) != len(refs) {
		t.Fatalf("detailed entries=%d want %d", len(plan.Entries), len(refs))
	}
	for _, entry := range plan.Entries {
		if entry.Status != ColumnAssetReachabilityProtected {
			t.Fatalf("entry %+v status=%q want protected", entry.Ref, entry.Status)
		}
	}
}

func TestColumnAssetReachabilityPlanClassifiesCandidateRefsAndPinsM15A(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability candidate: %v", err)
	}
	if plan.Refs.Reclaimable != 1 || plan.Refs.BytesReclaimable != candidate.Length {
		t.Fatalf("candidate ref stats=%+v want one reclaimable candidate of %d bytes", plan.Refs, candidate.Length)
	}
	if plan.Segments.Mixed == 0 || plan.Segments.Reclaimable != 0 {
		t.Fatalf("candidate segment stats=%+v want mixed protected/reclaimable segment", plan.Segments)
	}
	if plan.RewriteDebtBytes != candidate.Length {
		t.Fatalf("rewrite debt=%d want candidate length %d", plan.RewriteDebtBytes, candidate.Length)
	}

	pinned, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
		PinnedRefs:    []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability pinned: %v", err)
	}
	if pinned.Refs.Reclaimable != 0 || pinned.Refs.Protected != plan.Refs.Protected+1 {
		t.Fatalf("pinned ref stats=%+v want candidate moved from reclaimable to protected", pinned.Refs)
	}
	if pinned.RewriteDebtBytes != 0 {
		t.Fatalf("pinned rewrite debt=%d want 0", pinned.RewriteDebtBytes)
	}
}

func TestColumnAssetReachabilityPlanProtectsPendingPreparedRefsM15A(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	pending := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 98)
	prepared := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	reclaimable := writeColumnAssetReachabilityCandidateM15A(t, d, col, 4, 100)

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{pending, prepared, reclaimable},
		PendingRefs:   []ColumnAssetRef{pending},
		PreparedRefs:  []ColumnAssetRef{prepared},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.Sources.PendingRefs != 1 || plan.Sources.PreparedRefs != 1 || plan.Sources.CandidateRefs != 3 {
		t.Fatalf("source stats=%+v want pending/prepared/candidates", plan.Sources)
	}
	if plan.Refs.Reclaimable != 1 || plan.Refs.BytesReclaimable != reclaimable.Length {
		t.Fatalf("ref stats=%+v want only unprotected candidate reclaimable", plan.Refs)
	}
	foundPending := false
	foundPrepared := false
	for _, entry := range plan.Entries {
		if entry.Ref == pending || entry.Ref == prepared {
			if entry.Ref == pending {
				foundPending = true
			}
			if entry.Ref == prepared {
				foundPrepared = true
			}
			if entry.Status != ColumnAssetReachabilityProtected {
				t.Fatalf("pending/prepared entry %+v status=%q want protected", entry.Ref, entry.Status)
			}
		}
	}
	if !foundPending || !foundPrepared {
		t.Fatalf("missing pending/prepared entries in detailed plan: pending=%t prepared=%t", foundPending, foundPrepared)
	}
}

func TestColumnAssetReachabilityPlanCountsUniqueSourceRefsM15A(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate, candidate},
		PendingRefs:   []ColumnAssetRef{candidate, candidate},
		PreparedRefs:  []ColumnAssetRef{candidate, candidate},
		PinnedRefs:    []ColumnAssetRef{candidate, candidate},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.Sources.CandidateRefs != 1 || plan.Sources.PendingRefs != 1 ||
		plan.Sources.PreparedRefs != 1 || plan.Sources.PinnedRefs != 1 {
		t.Fatalf("source stats=%+v want one contribution per duplicated source", plan.Sources)
	}
	if plan.Refs.Reclaimable != 0 || plan.Refs.Protected == 0 {
		t.Fatalf("ref stats=%+v want duplicated pinned candidate protected once", plan.Refs)
	}
}

func TestColumnAssetReachabilityPlanRetainsUnknownSegmentsM15A(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	cfg := col.Meta().Options.ColumnStore
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(99)), []byte("untracked-column-asset-bytes"), 0o600); err != nil {
		t.Fatalf("WriteFile unknown segment: %v", err)
	}

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.Complete {
		t.Fatalf("plan marked complete despite untracked segment: %+v", plan.Segments)
	}
	if plan.Segments.Unknown != 1 || plan.Segments.BytesUnknown == 0 {
		t.Fatalf("segment stats=%+v want one retained unknown segment with bytes", plan.Segments)
	}
	if plan.Segments.Reclaimable != 0 {
		t.Fatalf("unknown segment was treated as reclaimable: %+v", plan.Segments)
	}
}

func TestColumnAssetReachabilityPlanRetainsNonCanonicalSegmentFileM15A(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(refs) == 0 {
		t.Fatal("manifest refs empty")
	}
	assetPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), refs[0])
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	info, err := os.Stat(assetPath)
	if err != nil {
		t.Fatalf("Stat live segment: %v", err)
	}
	cfg := col.Meta().Options.ColumnStore
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	nonCanonicalPath := filepath.Join(namespace.SegmentDir, "segment-1.tca")
	if err := os.WriteFile(nonCanonicalPath, make([]byte, info.Size()), 0o600); err != nil {
		t.Fatalf("WriteFile non-canonical segment: %v", err)
	}

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.Complete {
		t.Fatalf("plan marked complete despite non-canonical segment file: %+v", plan.Segments)
	}
	if plan.Segments.Unknown != 1 || plan.Segments.BytesUnknown != info.Size() {
		t.Fatalf("segment stats=%+v want one unknown non-canonical segment with %d bytes", plan.Segments, info.Size())
	}
	found := false
	for _, entry := range plan.SegmentEntries {
		if entry.Path != nonCanonicalPath {
			continue
		}
		found = true
		if entry.FileID != 0 || entry.Status != ColumnAssetReachabilitySegmentUnknown || entry.UnknownBytes != info.Size() {
			t.Fatalf("non-canonical entry=%+v want fileID=0 unknown bytes=%d", entry, info.Size())
		}
	}
	if !found {
		t.Fatalf("missing non-canonical segment entry for %s in %+v", nonCanonicalPath, plan.SegmentEntries)
	}
}

func TestColumnAssetReachabilityPlanRetainsMissingLiveSegmentM15A(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(refs) == 0 {
		t.Fatal("manifest refs empty")
	}
	assetPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), refs[0])
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	if err := os.Remove(assetPath); err != nil {
		t.Fatalf("Remove live segment: %v", err)
	}

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.Complete {
		t.Fatalf("plan marked complete despite missing live segment: %+v", plan.Segments)
	}
	if plan.Segments.Missing == 0 || plan.Segments.Reclaimable != 0 {
		t.Fatalf("segment stats=%+v want missing retained and no reclaimable segment", plan.Segments)
	}
}

func TestColumnAssetReachabilityPlanOrdersMissingSegmentEntriesM15A(t *testing.T) {
	const namespace = "events/column-assets"
	input := columnAssetReachabilityInput{
		rootDir:     t.TempDir(),
		collection:  "events",
		namespace:   namespace,
		detailed:    true,
		activeGen:   1,
		recoveryGen: 1,
	}
	for _, fileID := range []uint32{3, 1, 2} {
		input.addRef(ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  namespace,
			Generation: 1,
			PartID:     uint64(fileID),
			FileID:     fileID,
			Length:     64,
		}, ColumnAssetReachabilitySourceActiveManifest)
	}

	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if plan.Complete || plan.Segments.Missing != 3 {
		t.Fatalf("plan complete=%t missing=%d want incomplete with three missing segments", plan.Complete, plan.Segments.Missing)
	}
	if len(plan.SegmentEntries) != 3 {
		t.Fatalf("segment entries=%d want 3", len(plan.SegmentEntries))
	}
	for i, want := range []uint32{1, 2, 3} {
		if got := plan.SegmentEntries[i].FileID; got != want {
			t.Fatalf("segment entry %d fileID=%d want %d; entries=%+v", i, got, want, plan.SegmentEntries)
		}
	}
}

func TestColumnAssetReachabilityPlanCountsMissingSegmentsOnceM15A(t *testing.T) {
	const namespace = "events/column-assets"
	input := columnAssetReachabilityInput{
		rootDir:     t.TempDir(),
		collection:  "events",
		namespace:   namespace,
		detailed:    true,
		activeGen:   1,
		recoveryGen: 1,
	}
	for partID := uint64(1); partID <= 2; partID++ {
		input.addRef(ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  namespace,
			Generation: 1,
			PartID:     partID,
			FileID:     7,
			Offset:     int64(partID-1) * 64,
			Length:     64,
		}, ColumnAssetReachabilitySourceActiveManifest)
	}

	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if plan.Complete || plan.Segments.Missing != 1 || plan.Segments.OutOfBoundsRefs != 0 {
		t.Fatalf("segments=%+v complete=%t want one missing segment and no out-of-bounds refs", plan.Segments, plan.Complete)
	}
	if len(plan.SegmentEntries) != 1 || plan.SegmentEntries[0].FileID != 7 || plan.SegmentEntries[0].RefCount != 2 {
		t.Fatalf("segment entries=%+v want one missing segment entry with two refs", plan.SegmentEntries)
	}
}

func TestColumnAssetReachabilityPlanSeparatesOutOfBoundsRefsFromMissingSegmentsM15A(t *testing.T) {
	const namespaceName = "events/column-assets"
	root := t.TempDir()
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(1)), make([]byte, 32), 0o600); err != nil {
		t.Fatalf("WriteFile segment: %v", err)
	}
	input := columnAssetReachabilityInput{
		rootDir:     root,
		collection:  "events",
		namespace:   namespaceName,
		detailed:    true,
		activeGen:   1,
		recoveryGen: 1,
	}
	input.addRef(ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  namespaceName,
		Generation: 1,
		PartID:     1,
		FileID:     1,
		Length:     64,
	}, ColumnAssetReachabilitySourceActiveManifest)

	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if plan.Complete || plan.Segments.Missing != 0 || plan.Segments.OutOfBoundsRefs != 1 {
		t.Fatalf("segments=%+v complete=%t want existing out-of-bounds ref without missing segment", plan.Segments, plan.Complete)
	}
	if plan.Segments.Unknown != 1 || plan.Segments.BytesProtected != 32 || plan.Segments.BytesUnknown != 0 {
		t.Fatalf("segments=%+v want unknown existing segment with clipped protected bytes", plan.Segments)
	}
	if len(plan.SegmentEntries) != 1 || plan.SegmentEntries[0].Status != ColumnAssetReachabilitySegmentUnknown || plan.SegmentEntries[0].RefCount != 1 {
		t.Fatalf("segment entries=%+v want one unknown existing segment entry", plan.SegmentEntries)
	}
}

func TestColumnAssetReachabilitySegmentFileIDRejectsNonCanonicalM15A(t *testing.T) {
	fileID, ok := columnAssetReachabilitySegmentFileID(columnAssetSegmentFileName(1))
	if !ok || fileID != 1 {
		t.Fatalf("canonical segment file parsed as fileID=%d ok=%t, want 1 true", fileID, ok)
	}
	fileID, ok = columnAssetReachabilitySegmentFileID(columnAssetSegmentFileName(1_000_000))
	if !ok || fileID != 1_000_000 {
		t.Fatalf("canonical wide segment file parsed as fileID=%d ok=%t, want 1000000 true", fileID, ok)
	}
	for _, name := range []string{
		"segment-1.tca",
		"segment-000000.tca",
		"segment-000001.extra",
		"segment-000001.tca.bak",
		"segment-0000001.tca",
	} {
		if fileID, ok := columnAssetReachabilitySegmentFileID(name); ok {
			t.Fatalf("non-canonical segment file %q parsed as fileID=%d", name, fileID)
		}
	}
}

func TestColumnAssetReachabilitySegmentAccountingPreservesKnownBytesWhenUnknownM15A(t *testing.T) {
	protected := classifyColumnAssetReachabilitySegment(columnAssetReachabilitySegment{fileID: 1, bytes: 100}, []columnAssetReachabilityRange{{
		start:  0,
		end:    40,
		status: ColumnAssetReachabilityProtected,
	}})
	if protected.status != ColumnAssetReachabilitySegmentUnknown ||
		protected.protectedBytes != 40 ||
		protected.reclaimableBytes != 0 ||
		protected.unknownBytes != 60 {
		t.Fatalf("protected unknown segment plan=%+v want protected=40 unknown=60", protected)
	}

	reclaimable := classifyColumnAssetReachabilitySegment(columnAssetReachabilitySegment{fileID: 1, bytes: 100}, []columnAssetReachabilityRange{{
		start:  10,
		end:    50,
		status: ColumnAssetReachabilityReclaimable,
	}})
	if reclaimable.status != ColumnAssetReachabilitySegmentUnknown ||
		reclaimable.protectedBytes != 0 ||
		reclaimable.reclaimableBytes != 40 ||
		reclaimable.unknownBytes != 60 {
		t.Fatalf("reclaimable unknown segment plan=%+v want reclaimable=40 unknown=60", reclaimable)
	}
}

func prepareColumnAssetReachabilityCommandWALDirM15A(t testing.TB) string {
	t.Helper()
	dir, baseLSN := prepareColumnStoreCommandWALDirM10B(t)
	if baseLSN == 0 {
		t.Fatal("prepareColumnStoreCommandWALDirM10B returned base LSN 0")
	}
	return dir
}

func writeColumnAssetReachabilityCandidateM15A(t testing.TB, d *backenddb.DB, col *Collection, generation, partID uint64) ColumnAssetRef {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		t.Fatalf("missing column store config: %+v", cfg)
	}
	rows := []columnDeclaredRow{{
		ID: []byte("candidate"),
		Values: []columnDeclaredValue{
			{Type: ColumnStoreValueInt64, Present: true, Int64: int64(generation)},
			{Type: ColumnStoreValueString, Present: true, String: "candidate"},
			{Type: ColumnStoreValueString, Present: true, String: "did_candidate"},
		},
	}}
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         cfg.AssetManager.Namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: d.State().AppliedCommandLSN + 1,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        cfg.SchemaHash,
		Columns:           cfg.Columns,
		Rows:              rows,
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	ref, err := writeColumnPhysicalAssetToManager(d.ColumnAssetRootDir(), *cfg, encoded, generation, partID)
	if err != nil {
		t.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}
	if ref.Checksum != page.Checksum(encoded) || ref.Length != int64(len(encoded)) {
		t.Fatalf("unexpected candidate ref=%+v encoded=%d checksum=%d", ref, len(encoded), page.Checksum(encoded))
	}
	return ref
}

func BenchmarkColumnAssetReachabilityPlanSummaryM15A(b *testing.B) {
	const refs = 10_000
	const refBytes = int64(64)
	root := b.TempDir()
	const namespaceName = "events/column-assets"
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		b.Fatal(err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		b.Fatal(err)
	}
	segmentPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(1))
	segment, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	if err := segment.Truncate(refs * refBytes); err != nil {
		_ = segment.Close()
		b.Fatal(err)
	}
	if err := segment.Close(); err != nil {
		b.Fatal(err)
	}

	input := columnAssetReachabilityInput{
		rootDir:      root,
		collection:   "events",
		namespace:    namespaceName,
		activeGen:    1,
		recoveryGen:  1,
		manifestRecs: refs + 1,
		activeRefs:   refs,
		recoveryRefs: refs,
	}
	for i := 0; i < refs; i++ {
		ref := ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  namespaceName,
			Generation: 1,
			PartID:     uint64(i + 1),
			FileID:     1,
			Offset:     int64(i) * refBytes,
			Length:     refBytes,
			Checksum:   uint32(i + 1),
		}
		input.addRef(ref, ColumnAssetReachabilitySourceActiveManifest)
		input.addRef(ref, ColumnAssetReachabilitySourceRecoveryManifest)
	}

	b.ReportAllocs()
	b.SetBytes(refs * refBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
		if !plan.Complete ||
			plan.Refs.Protected != refs ||
			plan.Segments.Protected != 1 ||
			plan.Segments.Unknown != 0 ||
			plan.RewriteDebtBytes != 0 {
			b.Fatalf("unexpected plan: complete=%t refs=%+v segments=%+v debt=%d", plan.Complete, plan.Refs, plan.Segments, plan.RewriteDebtBytes)
		}
	}
}
