package collections

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPrecountColumnAssetReachabilityRangesSkipsMissingSegmentsM15C(t *testing.T) {
	if precountColumnAssetReachabilityRanges(8, 0) {
		t.Fatal("precount=true for refs with no segment files; missing-segment plans should skip the extra pass")
	}
	if precountColumnAssetReachabilityRanges(0, 0) {
		t.Fatal("precount=true for empty refs")
	}
	if !precountColumnAssetReachabilityRanges(10, 2) {
		t.Fatal("precount=false for dense refs with existing segment files")
	}
}

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

func TestMappedResourcePinnedRefsProtectColumnAssetReachability(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)
	basePlan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability unpinned: %v", err)
	}
	if basePlan.Refs.Reclaimable != 1 {
		t.Fatalf("unpinned ref stats=%+v want one reclaimable candidate", basePlan.Refs)
	}

	cfg := col.Meta().Options.ColumnStore
	manager := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	scope := mappedresource.Scope{
		Kind:       mappedresource.ScopeSnapshot,
		ID:         "snapshot-reachability-pin",
		Namespace:  cfg.AssetManager.Namespace,
		Collection: "events",
		Generation: candidate.Generation,
		Reason:     "reachability-test",
	}
	if err := readCache.useMappedResourceManager(manager, scope, "reachability-pinned-read"); err != nil {
		_ = readCache.close()
		t.Fatalf("useMappedResourceManager: %v", err)
	}
	raw, err := readCache.read(candidate, nil)
	if err != nil {
		_ = readCache.close()
		t.Fatalf("read candidate: %v", err)
	}
	if int64(len(raw)) != candidate.Length {
		_ = readCache.close()
		t.Fatalf("candidate read bytes=%d want %d", len(raw), candidate.Length)
	}
	pinnedRefs := readCache.mappedResourcePinnedRefs()
	if len(pinnedRefs) != 1 || pinnedRefs[0] != candidate {
		_ = readCache.close()
		t.Fatalf("mappedresource pinned refs=%+v want candidate %+v", pinnedRefs, candidate)
	}

	pinnedPlan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
		PinnedRefs:    pinnedRefs,
	})
	if err != nil {
		_ = readCache.close()
		t.Fatalf("PlanColumnAssetReachability pinned: %v", err)
	}
	if pinnedPlan.Sources.PinnedRefs != 1 || pinnedPlan.Refs.Reclaimable != 0 || pinnedPlan.RewriteDebtBytes != 0 {
		_ = readCache.close()
		t.Fatalf("pinned plan stats=%+v sources=%+v rewriteDebt=%d", pinnedPlan.Refs, pinnedPlan.Sources, pinnedPlan.RewriteDebtBytes)
	}
	foundPinnedCandidate := false
	for _, entry := range pinnedPlan.Entries {
		if entry.Ref != candidate {
			continue
		}
		foundPinnedCandidate = true
		if entry.Status != ColumnAssetReachabilityProtected || !slices.Contains(entry.Sources, ColumnAssetReachabilitySourcePinnedSnapshot) {
			_ = readCache.close()
			t.Fatalf("pinned candidate entry=%+v want protected with pinned source", entry)
		}
	}
	if !foundPinnedCandidate {
		_ = readCache.close()
		t.Fatal("missing pinned candidate entry")
	}
	if err := readCache.close(); err != nil {
		t.Fatalf("close read cache: %v", err)
	}
	if pins := manager.PinSummary(); len(pins) != 0 {
		t.Fatalf("pins after close=%d want 0", len(pins))
	}
}

func TestColumnAssetReachabilityFailClosedOnUnconvertibleMappedResourcePin1788(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	cfg := col.Meta().Options.ColumnStore
	mgr := mappedresource.NewManager()
	key := mappedresource.Key{
		Class:      mappedresource.ClassTypedColumnAsset,
		Namespace:  cfg.AssetManager.Namespace,
		Kind:       "unexpected_section_only_kind",
		Generation: 1,
		PartID:     1,
		FileID:     1,
		Offset:     0,
		Length:     4,
		Checksum:   7,
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "unconvertible-pin-1788", Namespace: cfg.AssetManager.Namespace, Collection: "events", Generation: 1}
	handle, err := mgr.AcquireBytes(key, scope, mappedresource.SourceHeapCopy, []byte("pin!"), mappedresource.AcquireOptions{Reason: "unconvertible-pin", ResourceRoot: d.ColumnAssetRootDir()})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	defer func() { _ = handle.Release() }()

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.Complete {
		t.Fatalf("plan complete with unconvertible mappedresource pin: %+v", plan.MappedResources)
	}
	if plan.MappedResources.ActiveHandles == 0 || plan.MappedResources.UnconvertiblePins != 1 || plan.MappedResources.PinnedRefs != 0 {
		t.Fatalf("mappedresource stats=%+v want one unconvertible active pin", plan.MappedResources)
	}
	gcStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{})
	if !errors.Is(err, ErrColumnAssetReachabilityIncomplete) {
		t.Fatalf("ColumnAssetGC error=%v want ErrColumnAssetReachabilityIncomplete", err)
	}
	if gcStats.SegmentsDeleted != 0 || gcStats.Plan.MappedResources.UnconvertiblePins != 1 {
		t.Fatalf("GC stats=%+v plan mapped=%+v want fail-closed unconvertible pin", gcStats, gcStats.Plan.MappedResources)
	}
}

func TestColumnAssetReachabilityIgnoresMappedResourcePinsFromOtherDBRoots1788(t *testing.T) {
	dirA := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	dbA := openCollectionCommandWALDB(t, dirA)
	defer func() { _ = dbA.Close() }()
	colA := openColumnStoreCollectionM10B(t, dbA)
	if _, err := colA.Insert([]byte("a1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert A: %v", err)
	}

	dirB := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	dbB := openCollectionCommandWALDB(t, dirB)
	defer func() { _ = dbB.Close() }()
	colB := openColumnStoreCollectionM10B(t, dbB)
	if _, err := colB.Insert([]byte("b1"), []byte(`{"time_us":2,"kind":"post","did":"d2"}`)); err != nil {
		t.Fatalf("Insert B: %v", err)
	}

	refsA := columnManifestAssetRefsForCollectionM12A(t, dbA, colA)
	if len(refsA) == 0 {
		t.Fatal("manifest refs empty for DB A")
	}
	refA := refsA[0]
	mgr := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(dbA.ColumnAssetRootDir(), refA.Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("new read cache A: %v", err)
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeSnapshot, ID: "foreign-root-pin-1788", Namespace: refA.Namespace, Collection: "events", Generation: refA.Generation, Reason: "foreign root pin"}
	if err := readCache.useMappedResourceManager(mgr, scope, "foreign-root-pin"); err != nil {
		_ = readCache.close()
		t.Fatalf("useMappedResourceManager A: %v", err)
	}
	if _, err := readCache.read(refA, nil); err != nil {
		_ = readCache.close()
		t.Fatalf("read foreign ref: %v", err)
	}
	defer func() { _ = readCache.close() }()

	plan, err := colB.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability B: %v", err)
	}
	if !plan.Complete || plan.Sources.MappedResourcePins != 0 || plan.MappedResources.ActiveHandles != 0 || plan.MappedResources.UnconvertiblePins != 0 {
		t.Fatalf("plan for DB B imported DB A pin: complete=%v sources=%+v mapped=%+v", plan.Complete, plan.Sources, plan.MappedResources)
	}
}

func TestColumnAssetReachabilityMatchesMappedResourcePinsThroughSymlinkRoot1788(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)
	basePlan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true, CandidateRefs: []ColumnAssetRef{candidate}})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability unpinned: %v", err)
	}
	if basePlan.Refs.Reclaimable != 1 {
		t.Fatalf("unpinned ref stats=%+v want one reclaimable candidate", basePlan.Refs)
	}

	linkedRoot := filepath.Join(t.TempDir(), "column-assets-link")
	if err := os.Symlink(d.ColumnAssetRootDir(), linkedRoot); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	mgr := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(linkedRoot, candidate.Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("new symlink read cache: %v", err)
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeSnapshot, ID: "symlink-root-pin-1788", Namespace: candidate.Namespace, Collection: "events", Generation: candidate.Generation, Reason: "symlink root pin"}
	if err := readCache.useMappedResourceManager(mgr, scope, "symlink-root-pin"); err != nil {
		_ = readCache.close()
		t.Fatalf("useMappedResourceManager symlink: %v", err)
	}
	if _, err := readCache.read(candidate, nil); err != nil {
		_ = readCache.close()
		t.Fatalf("read symlink candidate: %v", err)
	}
	defer func() { _ = readCache.close() }()

	assertColumnAssetReachabilityMappedResourcePinProtectsCandidate1788(t, col, candidate, "symlink root")
}

func TestColumnAssetReachabilityMatchesMappedResourcePinsThroughSymlinkPath1788(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)
	linkedRoot := filepath.Join(t.TempDir(), "column-assets-link")
	if err := os.Symlink(d.ColumnAssetRootDir(), linkedRoot); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	linkedNamespace, err := columnAssetManagerNamespaceForRoot(linkedRoot, candidate.Namespace)
	if err != nil {
		t.Fatalf("linked namespace: %v", err)
	}
	segmentPath := filepath.Join(linkedNamespace.SegmentDir, columnAssetSegmentFileName(candidate.FileID))
	if _, err := os.Stat(segmentPath); err != nil {
		t.Fatalf("stat symlink segment path: %v", err)
	}
	mgr := mappedresource.NewManager()
	scope := mappedresource.Scope{Kind: mappedresource.ScopeSnapshot, ID: "symlink-path-pin-1788", Namespace: candidate.Namespace, Collection: "events", Generation: candidate.Generation, Reason: "symlink path pin"}
	handle, err := mgr.AcquireBytes(mappedResourceKeyForColumnAssetRef(candidate), scope, mappedresource.SourceHeapCopy, make([]byte, int(candidate.Length)), mappedresource.AcquireOptions{Reason: "symlink-path-pin", ResourcePath: segmentPath})
	if err != nil {
		t.Fatalf("AcquireBytes symlink path pin: %v", err)
	}
	defer func() { _ = handle.Release() }()

	assertColumnAssetReachabilityMappedResourcePinProtectsCandidate1788(t, col, candidate, "symlink path")
}

func assertColumnAssetReachabilityMappedResourcePinProtectsCandidate1788(t *testing.T, col *Collection, candidate ColumnAssetRef, label string) {
	t.Helper()
	pinnedPlan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true, CandidateRefs: []ColumnAssetRef{candidate}})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability %s pinned: %v", label, err)
	}
	if !pinnedPlan.Complete || pinnedPlan.Sources.MappedResourcePins != 1 || pinnedPlan.MappedResources.ActiveHandles != 1 || pinnedPlan.MappedResources.PinnedRefs != 1 {
		t.Fatalf("%s pin stats: complete=%v sources=%+v mapped=%+v", label, pinnedPlan.Complete, pinnedPlan.Sources, pinnedPlan.MappedResources)
	}
	if pinnedPlan.Refs.Reclaimable != 0 || pinnedPlan.RewriteDebtBytes != 0 {
		t.Fatalf("%s pinned ref stats=%+v rewriteDebt=%d want protected candidate", label, pinnedPlan.Refs, pinnedPlan.RewriteDebtBytes)
	}
	foundPinnedCandidate := false
	for _, entry := range pinnedPlan.Entries {
		if entry.Ref != candidate {
			continue
		}
		foundPinnedCandidate = true
		if entry.Status != ColumnAssetReachabilityProtected || !slices.Contains(entry.Sources, ColumnAssetReachabilitySourceMappedResourcePin) {
			t.Fatalf("%s pinned candidate entry=%+v want mappedresource protected", label, entry)
		}
	}
	if !foundPinnedCandidate {
		t.Fatalf("missing %s pinned candidate entry", label)
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

func TestColumnAssetReachabilityPlanClassifiesUnreferencedCanonicalSegmentReclaimableM15A(t *testing.T) {
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
	segmentPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(99))
	if err := os.WriteFile(segmentPath, []byte("untracked-column-asset-bytes"), 0o600); err != nil {
		t.Fatalf("WriteFile unreferenced segment: %v", err)
	}

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if !plan.Complete {
		t.Fatalf("plan marked incomplete despite unreferenced canonical segment: %+v", plan.Segments)
	}
	if plan.Segments.Reclaimable != 1 || plan.Segments.BytesReclaimable == 0 || plan.Segments.Unknown != 0 {
		t.Fatalf("segment stats=%+v want one reclaimable unreferenced canonical segment", plan.Segments)
	}
	found := false
	for _, entry := range plan.SegmentEntries {
		if entry.Path != segmentPath {
			continue
		}
		found = true
		if entry.FileID != 99 || entry.Status != ColumnAssetReachabilitySegmentReclaimable || entry.ReclaimableBytes == 0 || entry.UnknownBytes != 0 {
			t.Fatalf("unreferenced canonical entry=%+v want reclaimable bytes and no unknown bytes", entry)
		}
	}
	if !found {
		t.Fatalf("missing unreferenced segment entry for %s in %+v", segmentPath, plan.SegmentEntries)
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

func TestColumnAssetReachabilityPlanRetainsCanonicalSymlinkAsUnknownM15A(t *testing.T) {
	root := t.TempDir()
	const namespaceName = "events/column-assets"
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	targetPath := filepath.Join(root, "outside-target")
	if err := os.WriteFile(targetPath, []byte("not-a-column-segment"), 0o600); err != nil {
		t.Fatalf("WriteFile symlink target: %v", err)
	}
	linkPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(7))
	if err := os.Symlink(targetPath, linkPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	input := columnAssetReachabilityInput{
		rootDir:    root,
		collection: "events",
		namespace:  namespaceName,
		detailed:   true,
	}
	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if plan.Complete || plan.Segments.Unknown != 1 || plan.Segments.Reclaimable != 0 {
		t.Fatalf("segments=%+v complete=%t want symlink retained as unknown", plan.Segments, plan.Complete)
	}
	if plan.Segments.BytesUnknown != 0 {
		t.Fatalf("segments=%+v want non-regular symlink bytes omitted from unknown byte totals", plan.Segments)
	}
	foundSymlink := false
	for _, entry := range plan.SegmentEntries {
		if entry.Path == linkPath {
			foundSymlink = true
			if entry.FileID != 7 || entry.Status != ColumnAssetReachabilitySegmentUnknown || entry.Bytes != 0 || entry.UnknownBytes != 0 {
				t.Fatalf("symlink entry=%+v want canonical fileID with zero-byte unknown diagnostics", entry)
			}
		}
	}
	if !foundSymlink {
		t.Fatalf("missing symlink entry for %s in %+v", linkPath, plan.SegmentEntries)
	}
}

func TestColumnAssetReachabilityPlanRetainsCanonicalDirectoryAsUnknownM15A(t *testing.T) {
	root := t.TempDir()
	const namespaceName = "events/column-assets"
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	dirPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(9))
	if err := os.Mkdir(dirPath, 0o700); err != nil {
		t.Fatalf("Mkdir canonical segment directory: %v", err)
	}

	input := columnAssetReachabilityInput{
		rootDir:    root,
		collection: "events",
		namespace:  namespaceName,
		detailed:   true,
	}
	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if plan.Complete || plan.Segments.Unknown != 1 || plan.Segments.Reclaimable != 0 {
		t.Fatalf("segments=%+v complete=%t want directory retained as unknown", plan.Segments, plan.Complete)
	}
	if plan.Segments.BytesUnknown != 0 {
		t.Fatalf("segments=%+v want directory bytes omitted from unknown byte totals", plan.Segments)
	}
	foundDir := false
	for _, entry := range plan.SegmentEntries {
		if entry.Path == dirPath {
			foundDir = true
			if entry.FileID != 9 || entry.Status != ColumnAssetReachabilitySegmentUnknown || entry.Bytes != 0 || entry.UnknownBytes != 0 {
				t.Fatalf("directory entry=%+v want canonical fileID with zero-byte unknown diagnostics", entry)
			}
		}
	}
	if !foundDir {
		t.Fatalf("missing directory entry for %s in %+v", dirPath, plan.SegmentEntries)
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
	if plan.Segments.Total == 0 || plan.Segments.Missing == 0 || plan.Segments.Reclaimable != 0 {
		t.Fatalf("segment stats=%+v want missing retained and no reclaimable segment", plan.Segments)
	}
	if len(plan.SegmentEntries) != 1 || plan.SegmentEntries[0].Status != ColumnAssetReachabilitySegmentMissing {
		t.Fatalf("segment entries=%+v want explicit missing status", plan.SegmentEntries)
	}
}

func TestColumnAssetReachabilityPlanOrdersMissingSegmentEntriesM15A(t *testing.T) {
	const namespaceName = "events/column-assets"
	root := t.TempDir()
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	input := columnAssetReachabilityInput{
		rootDir:     root,
		collection:  "events",
		namespace:   namespaceName,
		detailed:    true,
		activeGen:   1,
		recoveryGen: 1,
	}
	for _, fileID := range []uint32{3, 1, 2} {
		input.addRef(ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  namespaceName,
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
		if got, wantPath := plan.SegmentEntries[i].Path, columnAssetReachabilitySegmentPath(namespace.SegmentDir, columnAssetSegmentFileName(want)); got != wantPath {
			t.Fatalf("segment entry %d path=%q want %q; entries=%+v", i, got, wantPath, plan.SegmentEntries)
		}
		if got := plan.SegmentEntries[i].Status; got != ColumnAssetReachabilitySegmentMissing {
			t.Fatalf("segment entry %d status=%q want missing; entries=%+v", i, got, plan.SegmentEntries)
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
	if plan.Complete || plan.Segments.Total != 1 || plan.Segments.Missing != 1 || plan.Segments.OutOfBoundsRefs != 0 {
		t.Fatalf("segments=%+v complete=%t want one total missing segment and no out-of-bounds refs", plan.Segments, plan.Complete)
	}
	if len(plan.SegmentEntries) != 1 || plan.SegmentEntries[0].FileID != 7 || plan.SegmentEntries[0].RefCount != 2 {
		t.Fatalf("segment entries=%+v want one missing segment entry with two refs", plan.SegmentEntries)
	}
	if plan.SegmentEntries[0].Status != ColumnAssetReachabilitySegmentMissing {
		t.Fatalf("segment entry status=%q want missing", plan.SegmentEntries[0].Status)
	}
}

func TestColumnAssetReachabilityInvalidRefDoesNotContributeSegmentRangeM15A(t *testing.T) {
	const namespace = "events/column-assets"
	input := columnAssetReachabilityInput{
		rootDir:     t.TempDir(),
		collection:  "events",
		namespace:   namespace,
		detailed:    true,
		activeGen:   1,
		recoveryGen: 1,
	}
	input.addRef(ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  namespace,
		Generation: 1,
		PartID:     1,
		FileID:     7,
		Offset:     -1,
		Length:     64,
	}, ColumnAssetReachabilitySourceActiveManifest)

	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if plan.Complete || plan.Refs.Uncertain != 1 {
		t.Fatalf("plan complete=%t refs=%+v want one uncertain invalid ref", plan.Complete, plan.Refs)
	}
	if plan.Segments.Missing != 0 || plan.Segments.OutOfBoundsRefs != 0 || len(plan.SegmentEntries) != 0 {
		t.Fatalf("invalid ref contributed segment work: segments=%+v entries=%+v", plan.Segments, plan.SegmentEntries)
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

func TestColumnAssetReachabilityUnknownSegmentCountsKnownRewriteDebtM15A(t *testing.T) {
	const namespaceName = "events/column-assets"
	root := t.TempDir()
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(1)), make([]byte, 64), 0o600); err != nil {
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
		Offset:     0,
		Length:     16,
	}, ColumnAssetReachabilitySourceActiveManifest)
	input.addRef(ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  namespaceName,
		Generation: 1,
		PartID:     2,
		FileID:     1,
		Offset:     16,
		Length:     16,
	}, ColumnAssetReachabilitySourceCandidate)
	input.addRef(ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  namespaceName,
		Generation: 1,
		PartID:     3,
		FileID:     1,
		Offset:     60,
		Length:     8,
	}, ColumnAssetReachabilitySourceActiveManifest)

	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if plan.Complete || plan.Segments.Unknown != 1 || plan.Segments.OutOfBoundsRefs != 1 {
		t.Fatalf("segments=%+v complete=%t want one unknown out-of-bounds segment", plan.Segments, plan.Complete)
	}
	if plan.RewriteDebtBytes != 16 {
		t.Fatalf("rewrite debt=%d want known reclaimable bytes preserved", plan.RewriteDebtBytes)
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

func TestColumnAssetReachabilityKnownSourcesHaveMasksM15B(t *testing.T) {
	if len(columnAssetReachabilitySourceBits) > 64 {
		t.Fatalf("source mask table has %d entries; uint64 mask needs an explicit overflow strategy", len(columnAssetReachabilitySourceBits))
	}
	seen := make(map[columnAssetReachabilitySourceMask]ColumnAssetReachabilitySource, len(columnAssetReachabilitySourceBits))
	for _, entry := range columnAssetReachabilitySourceBits {
		mask, ok := columnAssetReachabilitySourceBit(entry.source)
		if entry.source == columnAssetReachabilitySourceUnknown {
			if ok || mask != columnAssetReachabilitySourceUnknownMask {
				t.Fatalf("unknown table source mask=%b ok=%t, want unknown mask and ok=false", mask, ok)
			}
			continue
		}
		if prev, dup := seen[entry.mask]; dup {
			t.Fatalf("source %q duplicates mask %b already used by %q", entry.source, entry.mask, prev)
		}
		seen[entry.mask] = entry.source
		if !ok || mask == 0 || entry.mask == 0 || mask != entry.mask || mask == columnAssetReachabilitySourceUnknownMask {
			t.Fatalf("source %q mask=%b ok=%t, want table mask %b", entry.source, mask, ok, entry.mask)
		}
	}
	mask, ok := columnAssetReachabilitySourceBit(ColumnAssetReachabilitySource("future_source"))
	if ok || mask != columnAssetReachabilitySourceUnknownMask {
		t.Fatalf("unknown source mask=%b ok=%t, want unknown mask and ok=false", mask, ok)
	}
	knownSources := columnAssetReachabilitySourcesForMaskWithUnknown(
		columnAssetReachabilitySourceCandidateMask,
		[]ColumnAssetReachabilitySource{"future_source"},
	)
	if !reflect.DeepEqual(knownSources, []ColumnAssetReachabilitySource{ColumnAssetReachabilitySourceCandidate}) {
		t.Fatalf("known source list=%v want candidate only without stray unknown sources", knownSources)
	}
	unknownSources := columnAssetReachabilitySourcesForMaskWithUnknown(
		columnAssetReachabilitySourceCandidateMask|columnAssetReachabilitySourceUnknownMask,
		[]ColumnAssetReachabilitySource{"future_source"},
	)
	if !reflect.DeepEqual(unknownSources, []ColumnAssetReachabilitySource{ColumnAssetReachabilitySourceCandidate, "future_source"}) {
		t.Fatalf("unknown source list=%v want candidate plus original unknown source", unknownSources)
	}
}

func TestColumnAssetReachabilityCanElideDiagnosticSourcesForRewriteM15C(t *testing.T) {
	const namespaceName = "events/column-assets"
	root := t.TempDir()
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	payload := []byte("rewrite-source-elision-column-asset")
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(1)), payload, 0o600); err != nil {
		t.Fatalf("WriteFile segment: %v", err)
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  namespaceName,
		Generation: 1,
		PartID:     1,
		FileID:     1,
		Length:     int64(len(payload)),
		Checksum:   page.Checksum(payload),
	}
	input := columnAssetReachabilityInput{
		rootDir:      root,
		collection:   "events",
		namespace:    namespaceName,
		detailed:     true,
		omitSources:  true,
		omitSort:     true,
		activeGen:    1,
		recoveryGen:  1,
		manifestRecs: 1,
	}
	input.addRef(ref, ColumnAssetReachabilitySourceActiveManifest)
	input.addRef(ref, ColumnAssetReachabilitySourceRecoveryManifest)

	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if !plan.Complete || len(plan.Entries) != 1 {
		t.Fatalf("plan complete=%t entries=%d want one complete entry", plan.Complete, len(plan.Entries))
	}
	if len(plan.Entries[0].Sources) != 0 {
		t.Fatalf("diagnostic sources=%v want elided", plan.Entries[0].Sources)
	}
	if !columnAssetRewriteSourceMaskIncludesManifest(input.refs[plan.Entries[0].Ref]) {
		t.Fatalf("entry source mask did not preserve manifest-source classification: %+v", plan.Entries[0])
	}
}

func TestColumnAssetReachabilityUnknownSourceFailsClosedM15B(t *testing.T) {
	const namespaceName = "events/column-assets"
	root := t.TempDir()
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	payload := []byte("unknown-source-column-asset")
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(1)), payload, 0o600); err != nil {
		t.Fatalf("WriteFile segment: %v", err)
	}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  namespaceName,
		Generation: 1,
		PartID:     1,
		FileID:     1,
		Length:     int64(len(payload)),
		Checksum:   page.Checksum(payload),
	}
	input := columnAssetReachabilityInput{
		rootDir:        root,
		collection:     "events",
		namespace:      namespaceName,
		detailed:       true,
		segmentDetails: true,
		activeGen:      1,
		recoveryGen:    1,
	}
	if !input.addRef(ref, ColumnAssetReachabilitySource("future_source")) {
		t.Fatal("unknown source ref was dropped, want fail-closed retention")
	}
	if !input.addRef(ref, ColumnAssetReachabilitySource("future_source_2")) {
		t.Fatal("second unknown source ref was dropped, want diagnostics to retain distinct sources")
	}

	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if plan.Complete {
		t.Fatalf("plan marked complete with unknown source: %+v", plan)
	}
	if plan.Refs.Total != 1 || plan.Refs.Uncertain != 1 || plan.Refs.Protected != 0 || plan.Refs.Reclaimable != 0 {
		t.Fatalf("ref stats=%+v want one uncertain unknown-source ref", plan.Refs)
	}
	if len(plan.Entries) != 1 || plan.Entries[0].Status != ColumnAssetReachabilityUncertain {
		t.Fatalf("entries=%+v want one uncertain ref entry", plan.Entries)
	}
	if len(plan.Entries[0].Sources) != 2 ||
		plan.Entries[0].Sources[0] != ColumnAssetReachabilitySource("future_source") ||
		plan.Entries[0].Sources[1] != ColumnAssetReachabilitySource("future_source_2") {
		t.Fatalf("entry sources=%v want original unknown source values", plan.Entries[0].Sources)
	}
	if plan.Segments.Unknown != 1 || plan.Segments.Protected != 0 || plan.Segments.Reclaimable != 0 {
		t.Fatalf("segment stats=%+v want existing unknown-source segment retained as unknown", plan.Segments)
	}
	if len(plan.SegmentEntries) != 1 || plan.SegmentEntries[0].Status != ColumnAssetReachabilitySegmentUnknown {
		t.Fatalf("segment entries=%+v want unknown segment entry", plan.SegmentEntries)
	}
}

func TestColumnAssetReachabilityInputSkipsUnknownSourceDetailsWhenNotDetailedM15B(t *testing.T) {
	ref := ColumnAssetRef{
		Kind:      ColumnAssetKindTCS1PartImage,
		FileID:    1,
		Offset:    8,
		Length:    64,
		Checksum:  99,
		Namespace: "events/column-assets",
	}
	input := columnAssetReachabilityInput{segmentDetails: true}
	if !input.addRef(ref, ColumnAssetReachabilitySource("future_source")) {
		t.Fatal("first unknown source ref was dropped")
	}
	if input.addRef(ref, ColumnAssetReachabilitySource("future_source_2")) {
		t.Fatal("second unknown source ref should be collapsed when detailed diagnostics are disabled")
	}
	if input.unknownSources != nil {
		t.Fatalf("unknownSources allocated in non-detailed mode: %+v", input.unknownSources)
	}
	if got := input.refs[ref]; got&columnAssetReachabilitySourceUnknownMask == 0 {
		t.Fatalf("ref source mask=%b want unknown bit", got)
	}
}

func TestColumnAssetReachabilityPlanMarksZeroByteUnknownSegmentIncompleteM15A(t *testing.T) {
	const namespaceName = "events/column-assets"
	root := t.TempDir()
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	unknownPath := filepath.Join(namespace.SegmentDir, "segment-garbage.tca")
	if err := os.WriteFile(unknownPath, nil, 0o600); err != nil {
		t.Fatalf("WriteFile unknown segment: %v", err)
	}
	input := columnAssetReachabilityInput{
		rootDir:     root,
		collection:  "events",
		namespace:   namespaceName,
		detailed:    true,
		activeGen:   1,
		recoveryGen: 1,
	}

	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if plan.Complete || plan.Segments.Unknown != 1 || plan.Segments.BytesUnknown != 0 {
		t.Fatalf("plan complete=%t segments=%+v want incomplete zero-byte unknown segment", plan.Complete, plan.Segments)
	}
	if len(plan.SegmentEntries) != 1 || plan.SegmentEntries[0].Path != unknownPath || plan.SegmentEntries[0].Status != ColumnAssetReachabilitySegmentUnknown {
		t.Fatalf("segment entries=%+v want zero-byte unknown segment entry", plan.SegmentEntries)
	}
}

func TestColumnAssetReachabilityCancelledBuildReturnsIdentityOnlyM15A(t *testing.T) {
	const namespaceName = "events/column-assets"
	input := columnAssetReachabilityInput{
		rootDir:      t.TempDir(),
		collection:   "events",
		namespace:    namespaceName,
		activeGen:    7,
		recoveryGen:  7,
		manifestRecs: 9,
		activeRefs:   1,
		recoveryRefs: 1,
	}
	input.addRef(ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  namespaceName,
		Generation: 7,
		PartID:     1,
		FileID:     1,
		Length:     64,
	}, ColumnAssetReachabilitySourceActiveManifest)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	plan, err := buildColumnAssetReachabilityPlan(ctx, input)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if !plan.ProtectOnly || plan.Complete || plan.Collection != "events" || plan.Namespace != namespaceName ||
		plan.ActiveManifestGeneration != 7 || plan.RecoveryManifestGeneration != 7 {
		t.Fatalf("canceled plan identity=%+v", plan)
	}
	if plan.Sources.ManifestRecords != 0 || plan.Refs.Total != 0 || plan.Segments.Total != 0 || len(plan.Entries) != 0 || len(plan.SegmentEntries) != 0 {
		t.Fatalf("canceled plan should not expose partial stats: %+v", plan)
	}
}

func TestColumnAssetReachabilityPlanCancellationBeforeSnapshotM15A(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	col := &Collection{}
	plan, err := col.PlanColumnAssetReachability(ctx, ColumnAssetReachabilityOptions{Detailed: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if !plan.ProtectOnly || plan.Complete || plan.Collection != "" || plan.Namespace != "" {
		t.Fatalf("canceled plan=%+v want protect-only identity", plan)
	}
	if plan.Sources.ManifestRecords != 0 || plan.Refs.Total != 0 || plan.Segments.Total != 0 || len(plan.Entries) != 0 || len(plan.SegmentEntries) != 0 {
		t.Fatalf("canceled plan should not expose partial stats: %+v", plan)
	}
}

func TestColumnAssetReachabilityPlanPreservesIdentityOnSnapshotViewErrorM15A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	active := ColumnManifestIdentity{
		Generation: 7,
		Format:     columnManifestFormatTCS1,
		Version:    columnManifestIdentityVersion,
		Checksum:   0xabcddcbe,
	}
	meta, err := normalizeCollectionMeta(CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: testColumnStoreConfig(&active)}})
	if err != nil {
		t.Fatalf("normalizeCollectionMeta: %v", err)
	}
	cfg := meta.Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil || cfg.AssetManager == nil {
		t.Fatalf("missing column store metadata: %+v", cfg)
	}
	publishColumnStoreCatalogForTest(t, d, meta, active)

	col := &Collection{db: d, meta: CollectionMeta{Name: "events"}}
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err == nil {
		t.Fatal("PlanColumnAssetReachability err=nil, want fail-closed incomplete manifest error")
	}
	if !plan.ProtectOnly || plan.Complete {
		t.Fatalf("plan=%+v want protect-only incomplete identity", plan)
	}
	if plan.Collection != "events" || plan.Namespace != cfg.AssetManager.Namespace ||
		plan.ActiveManifestGeneration != active.Generation || plan.RecoveryManifestGeneration != active.Generation {
		t.Fatalf("plan identity=%+v want collection/namespace/generation preserved", plan)
	}
	if plan.Sources.ManifestRecords != 0 || plan.Refs.Total != 0 || plan.Segments.Total != 0 || len(plan.Entries) != 0 || len(plan.SegmentEntries) != 0 {
		t.Fatalf("failed plan should not expose partial stats: %+v", plan)
	}
}

func TestColumnAssetReachabilityListSegmentsCancellationM15A(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	segments, err := listColumnAssetReachabilitySegments(ctx, t.TempDir())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if len(segments) != 0 {
		t.Fatalf("segments=%+v want none after cancellation", segments)
	}
}

func TestColumnAssetReachabilityAddRefsCancellationDoesNotPartiallyCountM15A(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	input := columnAssetReachabilityInput{}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  "events/column-assets",
		Generation: 1,
		PartID:     1,
		FileID:     1,
		Length:     64,
	}
	err := input.addRefs(ctx, []ColumnAssetRef{ref}, ColumnAssetReachabilitySourceCandidate)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if len(input.refs) != 0 || input.sourceCounts.CandidateRefs != 0 {
		t.Fatalf("input mutated despite cancellation: refs=%d sources=%+v", len(input.refs), input.sourceCounts)
	}
}

func TestColumnAssetReachabilityAddRefsCancellationKeepsPartialCountsConsistentM15A(t *testing.T) {
	ctx := &columnAssetReachabilityCancelAfterErrContext{Context: context.Background(), cancelAfterErrCalls: 1}
	ref1 := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  "events/column-assets",
		Generation: 1,
		PartID:     1,
		FileID:     1,
		Length:     64,
	}
	refs := make([]ColumnAssetRef, columnAssetReachabilityContextCheckInterval+1)
	for i := range refs {
		refs[i] = ref1
		refs[i].PartID = uint64(i + 1)
	}
	input := columnAssetReachabilityInput{}

	err := input.addRefs(ctx, refs, ColumnAssetReachabilitySourceCandidate)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if len(input.refs) != columnAssetReachabilityContextCheckInterval || input.sourceCounts.CandidateRefs != columnAssetReachabilityContextCheckInterval {
		t.Fatalf("partial input inconsistent: refs=%d sources=%+v", len(input.refs), input.sourceCounts)
	}
	sourceMask := input.refs[ref1]
	if sourceMask != columnAssetReachabilitySourceCandidateMask {
		t.Fatalf("partial ref sourceMask=%b want candidate", sourceMask)
	}
}

type columnAssetReachabilityCancelAfterErrContext struct {
	context.Context
	cancelAfterErrCalls int
	errCalls            int
}

func (c *columnAssetReachabilityCancelAfterErrContext) Err() error {
	c.errCalls++
	if c.errCalls > c.cancelAfterErrCalls {
		return context.Canceled
	}
	return nil
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

func TestColumnAssetReachabilityPlanClassifiesFullCandidateSegmentReclaimableM15A(t *testing.T) {
	root := t.TempDir()
	namespace, err := columnAssetManagerNamespaceForRoot(root, "events/column-assets")
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := os.MkdirAll(namespace.SegmentDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segment dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(22)), make([]byte, 100), 0o600); err != nil {
		t.Fatalf("WriteFile segment: %v", err)
	}
	ref := ColumnAssetRef{
		Namespace:  "events/column-assets",
		Kind:       ColumnAssetKindTCS1PartImage,
		Generation: 1,
		PartID:     1,
		FileID:     22,
		Offset:     0,
		Length:     100,
	}
	input := columnAssetReachabilityInput{
		rootDir:    root,
		collection: "events",
		namespace:  "events/column-assets",
		detailed:   true,
	}
	if err := input.addRefs(context.Background(), []ColumnAssetRef{ref}, ColumnAssetReachabilitySourceCandidate); err != nil {
		t.Fatalf("addRefs: %v", err)
	}
	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if !plan.Complete || plan.Segments.Reclaimable != 1 || plan.Segments.Unknown != 0 || plan.Segments.Mixed != 0 {
		t.Fatalf("segment stats=%+v complete=%t want one fully reclaimable segment", plan.Segments, plan.Complete)
	}
	if plan.RewriteDebtBytes != 0 {
		t.Fatalf("rewrite debt=%d want 0 for fully reclaimable segment", plan.RewriteDebtBytes)
	}
	if len(plan.SegmentEntries) != 1 || plan.SegmentEntries[0].Status != ColumnAssetReachabilitySegmentReclaimable {
		t.Fatalf("segment entries=%+v want reclaimable", plan.SegmentEntries)
	}
}

func TestColumnAssetReachabilityDirectViewPrefixPaddingIsKnown1895(t *testing.T) {
	root := t.TempDir()
	const namespaceName = "events/column-assets"
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := os.MkdirAll(namespace.SegmentDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segment dir: %v", err)
	}
	fileID, err := directViewTypedColumnSegmentFileID(41)
	if err != nil {
		t.Fatalf("directViewTypedColumnSegmentFileID: %v", err)
	}
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(fileID)), make([]byte, 49), 0o600); err != nil {
		t.Fatalf("WriteFile segment: %v", err)
	}
	seed := ColumnAssetRef{Kind: ColumnAssetKindTCS1DictionaryCodes, Namespace: namespaceName, Generation: 41, PartID: 90, FileID: fileID, Offset: 0, Length: 1}
	first := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: namespaceName, Generation: 41, PartID: 1, FileID: fileID, Offset: 8, Length: 25}
	second := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: namespaceName, Generation: 41, PartID: 2, FileID: fileID, Offset: 40, Length: 9}
	input := columnAssetReachabilityInput{rootDir: root, collection: "events", namespace: namespaceName, detailed: true, segmentDetails: true}
	if err := input.addRefs(context.Background(), []ColumnAssetRef{seed, first}, ColumnAssetReachabilitySourceActiveManifest); err != nil {
		t.Fatalf("add protected refs: %v", err)
	}
	if err := input.addRefs(context.Background(), []ColumnAssetRef{second}, ColumnAssetReachabilitySourceCandidate); err != nil {
		t.Fatalf("add candidate ref: %v", err)
	}
	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if !plan.Complete || plan.Segments.Mixed != 1 || plan.Segments.Unknown != 0 || plan.Segments.BytesUnknown != 0 {
		t.Fatalf("plan complete=%t segments=%+v want one complete mixed segment with deterministic padding known", plan.Complete, plan.Segments)
	}
	if plan.Segments.BytesProtected != 33 || plan.Segments.BytesReclaimable != 16 || plan.RewriteDebtBytes != 16 {
		t.Fatalf("segment bytes protected=%d reclaimable=%d rewrite=%d want protected=33 reclaimable=16 rewrite=16", plan.Segments.BytesProtected, plan.Segments.BytesReclaimable, plan.RewriteDebtBytes)
	}
	if len(plan.SegmentEntries) != 1 || plan.SegmentEntries[0].UnknownBytes != 0 || plan.SegmentEntries[0].Status != ColumnAssetReachabilitySegmentMixed {
		t.Fatalf("segment entries=%+v want mixed with no unknown padding bytes", plan.SegmentEntries)
	}
	if plan.Refs.BytesProtected != seed.Length+first.Length || plan.Refs.BytesReclaimable != second.Length {
		t.Fatalf("ref bytes=%+v want payload-only ref accounting", plan.Refs)
	}
}

func TestColumnAssetReachabilityScalarU8PrefixPaddingIsKnown4234(t *testing.T) {
	root := t.TempDir()
	const namespaceName = "events/column-assets"
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := os.MkdirAll(namespace.SegmentDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segment dir: %v", err)
	}
	const fileID = uint32(73)
	segmentBytes := int64(columnVectorGraphScalarU8CodesAlignment + 9)
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(fileID)), make([]byte, segmentBytes), 0o600); err != nil {
		t.Fatalf("WriteFile segment: %v", err)
	}
	seed := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: namespaceName, Generation: 73, PartID: 1, FileID: fileID, Offset: 0, Length: 13}
	aligned := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: namespaceName, Generation: 73, PartID: 2, FileID: fileID, Offset: columnVectorGraphScalarU8CodesAlignment, Length: 9}
	input := columnAssetReachabilityInput{rootDir: root, collection: "events", namespace: namespaceName, detailed: true, segmentDetails: true}
	if err := input.addRefs(context.Background(), []ColumnAssetRef{seed, aligned}, ColumnAssetReachabilitySourceActiveManifest); err != nil {
		t.Fatalf("add protected refs: %v", err)
	}
	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if !plan.Complete || plan.Segments.Protected != 1 || plan.Segments.Unknown != 0 || plan.Segments.BytesUnknown != 0 {
		t.Fatalf("plan complete=%t segments=%+v want 64-byte scalar_u8 padding known/protected", plan.Complete, plan.Segments)
	}
	if plan.Segments.BytesProtected != segmentBytes {
		t.Fatalf("protected bytes=%d want %d", plan.Segments.BytesProtected, segmentBytes)
	}
}

func TestColumnAssetReachabilityHNSWSearchPackRefAndPaddingAreKnown2312(t *testing.T) {
	root := t.TempDir()
	const namespaceName = "events/column-assets"
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := os.MkdirAll(namespace.SegmentDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segment dir: %v", err)
	}
	const fileID = uint32(72)
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(fileID)), make([]byte, 48), 0o600); err != nil {
		t.Fatalf("WriteFile segment: %v", err)
	}
	seed := ColumnAssetRef{Kind: ColumnAssetKindTCS1DictionaryCodes, Namespace: namespaceName, Generation: 72, PartID: 90, FileID: fileID, Offset: 0, Length: 1}
	pack := ColumnAssetRef{Kind: ColumnAssetKindTCS1HNSWSearchPack, Namespace: namespaceName, Generation: 72, PartID: 91, FileID: fileID, Offset: 16, Length: 32}
	input := columnAssetReachabilityInput{rootDir: root, collection: "events", namespace: namespaceName, detailed: true, segmentDetails: true}
	if err := input.addRefs(context.Background(), []ColumnAssetRef{seed, pack}, ColumnAssetReachabilitySourceActiveManifest); err != nil {
		t.Fatalf("add protected refs: %v", err)
	}
	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if !plan.Complete || plan.Segments.Protected != 1 || plan.Segments.Unknown != 0 || plan.Segments.BytesUnknown != 0 {
		t.Fatalf("plan complete=%t segments=%+v want HNSW pack ref and 16-byte prefix padding known/protected", plan.Complete, plan.Segments)
	}
	if plan.Segments.BytesProtected != 48 || plan.Refs.BytesProtected != seed.Length+pack.Length {
		t.Fatalf("protected segment bytes=%d ref bytes=%d want segment=48 refs=%d", plan.Segments.BytesProtected, plan.Refs.BytesProtected, seed.Length+pack.Length)
	}
}

func TestColumnAssetReachabilityRewriteSegmentPrefixPaddingIsKnown1895(t *testing.T) {
	root := t.TempDir()
	const namespaceName = "events/column-assets"
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := os.MkdirAll(namespace.SegmentDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segment dir: %v", err)
	}
	const fileID = uint32(22)
	if fileID >= columnAssetDirectViewSegmentFileIDBase {
		t.Fatalf("test file_id=%d unexpectedly in direct-view reserved band", fileID)
	}
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(fileID)), make([]byte, 49), 0o600); err != nil {
		t.Fatalf("WriteFile segment: %v", err)
	}
	seed := ColumnAssetRef{Kind: ColumnAssetKindTCS1DictionaryCodes, Namespace: namespaceName, Generation: 51, PartID: 90, FileID: fileID, Offset: 0, Length: 1}
	first := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: namespaceName, Generation: 51, PartID: 1, FileID: fileID, Offset: 8, Length: 25}
	second := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: namespaceName, Generation: 51, PartID: 2, FileID: fileID, Offset: 40, Length: 9}
	input := columnAssetReachabilityInput{rootDir: root, collection: "events", namespace: namespaceName, detailed: true, segmentDetails: true}
	if err := input.addRefs(context.Background(), []ColumnAssetRef{seed, first}, ColumnAssetReachabilitySourceActiveManifest); err != nil {
		t.Fatalf("add protected refs: %v", err)
	}
	if err := input.addRefs(context.Background(), []ColumnAssetRef{second}, ColumnAssetReachabilitySourceCandidate); err != nil {
		t.Fatalf("add candidate ref: %v", err)
	}
	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if !plan.Complete || plan.Segments.Mixed != 1 || plan.Segments.Unknown != 0 || plan.Segments.BytesUnknown != 0 {
		t.Fatalf("plan complete=%t segments=%+v want regular rewrite segment padding known", plan.Complete, plan.Segments)
	}
	if plan.Segments.BytesProtected != 33 || plan.Segments.BytesReclaimable != 16 || plan.RewriteDebtBytes != 16 {
		t.Fatalf("segment bytes protected=%d reclaimable=%d rewrite=%d want protected=33 reclaimable=16 rewrite=16", plan.Segments.BytesProtected, plan.Segments.BytesReclaimable, plan.RewriteDebtBytes)
	}
	if len(plan.SegmentEntries) != 1 || plan.SegmentEntries[0].UnknownBytes != 0 || plan.SegmentEntries[0].Status != ColumnAssetReachabilitySegmentMixed {
		t.Fatalf("segment entries=%+v want mixed with no unknown padding bytes", plan.SegmentEntries)
	}
	if plan.Refs.BytesProtected != seed.Length+first.Length || plan.Refs.BytesReclaimable != second.Length {
		t.Fatalf("ref bytes=%+v want payload-only ref accounting", plan.Refs)
	}
}

func TestColumnAssetReachabilityNonZeroPrefixPaddingGapStaysUnknown1895(t *testing.T) {
	root := t.TempDir()
	const namespaceName = "events/column-assets"
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := os.MkdirAll(namespace.SegmentDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segment dir: %v", err)
	}
	fileID, err := directViewTypedColumnSegmentFileID(52)
	if err != nil {
		t.Fatalf("directViewTypedColumnSegmentFileID: %v", err)
	}
	segment := make([]byte, 33)
	for i := 1; i < 8; i++ {
		segment[i] = 0xa5
	}
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(fileID)), segment, 0o600); err != nil {
		t.Fatalf("WriteFile segment: %v", err)
	}
	seed := ColumnAssetRef{Kind: ColumnAssetKindTCS1DictionaryCodes, Namespace: namespaceName, Generation: 52, PartID: 90, FileID: fileID, Offset: 0, Length: 1}
	typed := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: namespaceName, Generation: 52, PartID: 1, FileID: fileID, Offset: 8, Length: 25}
	input := columnAssetReachabilityInput{rootDir: root, collection: "events", namespace: namespaceName, detailed: true, segmentDetails: true}
	if err := input.addRefs(context.Background(), []ColumnAssetRef{seed, typed}, ColumnAssetReachabilitySourceActiveManifest); err != nil {
		t.Fatalf("add refs: %v", err)
	}
	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if plan.Complete || plan.Segments.Unknown != 1 || plan.Segments.BytesUnknown != 7 {
		t.Fatalf("plan complete=%t segments=%+v want non-zero padding-sized gap unknown", plan.Complete, plan.Segments)
	}
}

func TestColumnAssetReachabilityDirectViewPaddingWindowDoesNotHideLargeGaps1895(t *testing.T) {
	root := t.TempDir()
	const namespaceName = "events/column-assets"
	namespace, err := columnAssetManagerNamespaceForRoot(root, namespaceName)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := os.MkdirAll(namespace.SegmentDir, 0o755); err != nil {
		t.Fatalf("MkdirAll segment dir: %v", err)
	}
	fileID, err := directViewTypedColumnSegmentFileID(42)
	if err != nil {
		t.Fatalf("directViewTypedColumnSegmentFileID: %v", err)
	}
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(fileID)), make([]byte, 41), 0o600); err != nil {
		t.Fatalf("WriteFile segment: %v", err)
	}
	seed := ColumnAssetRef{Kind: ColumnAssetKindTCS1DictionaryCodes, Namespace: namespaceName, Generation: 42, PartID: 90, FileID: fileID, Offset: 0, Length: 1}
	typed := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: namespaceName, Generation: 42, PartID: 1, FileID: fileID, Offset: 16, Length: 25}
	input := columnAssetReachabilityInput{rootDir: root, collection: "events", namespace: namespaceName, detailed: true, segmentDetails: true}
	if err := input.addRefs(context.Background(), []ColumnAssetRef{seed, typed}, ColumnAssetReachabilitySourceActiveManifest); err != nil {
		t.Fatalf("add refs: %v", err)
	}
	plan, err := buildColumnAssetReachabilityPlan(context.Background(), input)
	if err != nil {
		t.Fatalf("buildColumnAssetReachabilityPlan: %v", err)
	}
	if plan.Complete || plan.Segments.Unknown != 1 || plan.Segments.BytesUnknown != 15 {
		t.Fatalf("plan complete=%t segments=%+v want full 15-byte non-padding gap unknown", plan.Complete, plan.Segments)
	}
	if len(plan.SegmentEntries) != 1 || plan.SegmentEntries[0].UnknownBytes != 15 || plan.SegmentEntries[0].Status != ColumnAssetReachabilitySegmentUnknown {
		t.Fatalf("segment entries=%+v want unknown large gap", plan.SegmentEntries)
	}
}

func TestColumnAssetReachabilityByteAccountingSaturatesM15A(t *testing.T) {
	if got := addColumnAssetReachabilityBytes(math.MaxInt64-3, 2); got != math.MaxInt64-1 {
		t.Fatalf("non-overflow add=%d want %d", got, int64(math.MaxInt64-1))
	}
	if got := addColumnAssetReachabilityBytes(math.MaxInt64-3, 4); got != math.MaxInt64 {
		t.Fatalf("overflow add=%d want MaxInt64", got)
	}
	if got := addColumnAssetReachabilityBytes(7, -1); got != 7 {
		t.Fatalf("negative add=%d want unchanged", got)
	}
	if got := columnAssetReachabilityIntervalsLength([]columnAssetReachabilityInterval{
		{start: 0, end: math.MaxInt64},
		{start: 0, end: 1},
	}); got != math.MaxInt64 {
		t.Fatalf("interval length=%d want saturated MaxInt64", got)
	}
}

func TestColumnAssetReachabilitySubtractIntervalsInterleavedM15A(t *testing.T) {
	got := subtractColumnAssetReachabilityIntervals(
		[]columnAssetReachabilityInterval{
			{start: 0, end: 10},
			{start: 20, end: 30},
			{start: 40, end: 50},
		},
		[]columnAssetReachabilityInterval{
			{start: 2, end: 4},
			{start: 6, end: 22},
			{start: 25, end: 45},
		},
	)
	want := []columnAssetReachabilityInterval{
		{start: 0, end: 2},
		{start: 4, end: 6},
		{start: 22, end: 25},
		{start: 45, end: 50},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subtract intervals=%+v want %+v", got, want)
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
