package collections

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestCleanColumnAssetNamespaceRejectsEmptyM15B verifies that an empty string
// is rejected as a namespace.
func TestCleanColumnAssetNamespaceRejectsEmptyM15B(t *testing.T) {
	_, err := cleanColumnAssetNamespace("")
	if err == nil {
		t.Fatal("cleanColumnAssetNamespace accepted empty namespace")
	}
}

// TestCleanColumnAssetNamespaceRejectsLeadingSlashM15B verifies that a
// namespace starting with "/" is rejected.
func TestCleanColumnAssetNamespaceRejectsLeadingSlashM15B(t *testing.T) {
	_, err := cleanColumnAssetNamespace("/events/column-assets")
	if err == nil {
		t.Fatal("cleanColumnAssetNamespace accepted leading-slash namespace")
	}
}

// TestCleanColumnAssetNamespaceRejectsLeadingSpaceM15B verifies that
// leading/trailing whitespace is rejected.
func TestCleanColumnAssetNamespaceRejectsLeadingSpaceM15B(t *testing.T) {
	for _, ns := range []string{" events", "events ", "\tevents", "events\n"} {
		if _, err := cleanColumnAssetNamespace(ns); err == nil {
			t.Fatalf("cleanColumnAssetNamespace accepted whitespace namespace %q", ns)
		}
	}
}

// TestCleanColumnAssetNamespaceRejectsBackslashM15B verifies that backslash
// characters are rejected to prevent Windows path separator injection.
func TestCleanColumnAssetNamespaceRejectsBackslashM15B(t *testing.T) {
	_, err := cleanColumnAssetNamespace(`events\column-assets`)
	if err == nil {
		t.Fatal("cleanColumnAssetNamespace accepted backslash namespace")
	}
}

// TestCleanColumnAssetNamespaceRejectsColonM15B verifies that colon characters
// are rejected to prevent drive-letter paths on Windows.
func TestCleanColumnAssetNamespaceRejectsColonM15B(t *testing.T) {
	_, err := cleanColumnAssetNamespace("C:events")
	if err == nil {
		t.Fatal("cleanColumnAssetNamespace accepted colon namespace")
	}
}

// TestCleanColumnAssetNamespaceRejectsNullByteM15B verifies that null bytes
// are rejected to prevent path truncation attacks.
func TestCleanColumnAssetNamespaceRejectsNullByteM15B(t *testing.T) {
	_, err := cleanColumnAssetNamespace("events\x00/column-assets")
	if err == nil {
		t.Fatal("cleanColumnAssetNamespace accepted null-byte namespace")
	}
}

// TestCleanColumnAssetNamespaceRejectsDotAloneM15B verifies that a bare "."
// is rejected because path.Clean(".")="." matches the clean=="." guard.
func TestCleanColumnAssetNamespaceRejectsDotAloneM15B(t *testing.T) {
	_, err := cleanColumnAssetNamespace(".")
	if err == nil {
		t.Fatal("cleanColumnAssetNamespace accepted bare dot namespace")
	}
}

// TestCleanColumnAssetNamespaceRejectsDotDotAloneM15B verifies that ".." is
// rejected (path.Clean("..") stays ".." and the ".." part check fires).
func TestCleanColumnAssetNamespaceRejectsDotDotAloneM15B(t *testing.T) {
	_, err := cleanColumnAssetNamespace("..")
	if err == nil {
		t.Fatal("cleanColumnAssetNamespace accepted dotdot namespace")
	}
}

// TestCleanColumnAssetNamespaceRejectsDoubleDotSegmentM15B verifies that
// ".." segments in the middle of a namespace are rejected because path.Clean
// collapses them and the clean != namespace check fires.
func TestCleanColumnAssetNamespaceRejectsDoubleDotSegmentM15B(t *testing.T) {
	_, err := cleanColumnAssetNamespace("events/../other")
	if err == nil {
		t.Fatal("cleanColumnAssetNamespace accepted dotdot segment in namespace")
	}
}

// TestCleanColumnAssetNamespaceRejectsUnnormalizedPathsM15B verifies that any
// namespace that changes after path.Clean normalization is rejected.
func TestCleanColumnAssetNamespaceRejectsUnnormalizedPathsM15B(t *testing.T) {
	for _, ns := range []string{
		"events//column-assets",
		"events/./column-assets",
		"a/b//c",
	} {
		if _, err := cleanColumnAssetNamespace(ns); err == nil {
			t.Fatalf("cleanColumnAssetNamespace accepted unnormalized namespace %q", ns)
		}
	}
}

// TestCleanColumnAssetNamespaceAcceptsValidNamespacesM15B verifies that
// well-formed single-level and multi-level namespaces are accepted and returned
// unchanged.
func TestCleanColumnAssetNamespaceAcceptsValidNamespacesM15B(t *testing.T) {
	valid := []string{
		"events",
		"events/column-assets",
		"a/b/c",
		"my-collection/nested/column-assets",
		"events/column-assets/v2",
	}
	for _, ns := range valid {
		got, err := cleanColumnAssetNamespace(ns)
		if err != nil {
			t.Fatalf("cleanColumnAssetNamespace(%q) error=%v want nil", ns, err)
		}
		if got != ns {
			t.Fatalf("cleanColumnAssetNamespace(%q)=%q want identity", ns, got)
		}
	}
}

// TestColumnAssetManagerNamespaceForRootRejectsEmptyRootDirM15B verifies that
// an empty rootDir causes an immediate error.
func TestColumnAssetManagerNamespaceForRootRejectsEmptyRootDirM15B(t *testing.T) {
	_, err := columnAssetManagerNamespaceForRoot("", "events/column-assets")
	if err == nil {
		t.Fatal("columnAssetManagerNamespaceForRoot accepted empty rootDir")
	}
}

// TestColumnAssetManagerNamespaceForRootPropagatesNamespaceErrorM15B verifies
// that invalid namespace errors from cleanColumnAssetNamespace propagate.
func TestColumnAssetManagerNamespaceForRootPropagatesNamespaceErrorM15B(t *testing.T) {
	_, err := columnAssetManagerNamespaceForRoot("/some/root", "/invalid/leading-slash")
	if err == nil {
		t.Fatal("columnAssetManagerNamespaceForRoot accepted invalid namespace")
	}
}

// TestColumnAssetManagerNamespaceForRootBuildsCorrectPathsM15B verifies that
// the returned namespace struct has the expected directory layout.
func TestColumnAssetManagerNamespaceForRootBuildsCorrectPathsM15B(t *testing.T) {
	root := t.TempDir()
	ns, err := columnAssetManagerNamespaceForRoot(root, "events/column-assets")
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if ns.ManagerRootDir != root {
		t.Fatalf("ManagerRootDir=%q want %q", ns.ManagerRootDir, root)
	}
	wantRootDir := filepath.Join(root, "events", "column-assets")
	if ns.RootDir != wantRootDir {
		t.Fatalf("RootDir=%q want %q", ns.RootDir, wantRootDir)
	}
	wantAssetDir := filepath.Join(wantRootDir, "assets")
	if ns.AssetDir != wantAssetDir {
		t.Fatalf("AssetDir=%q want %q", ns.AssetDir, wantAssetDir)
	}
	if ns.SegmentDir != filepath.Join(wantAssetDir, "segments") {
		t.Fatalf("SegmentDir=%q want %q", ns.SegmentDir, filepath.Join(wantAssetDir, "segments"))
	}
	if ns.IndexDir != filepath.Join(wantAssetDir, "indexes") {
		t.Fatalf("IndexDir=%q want %q", ns.IndexDir, filepath.Join(wantAssetDir, "indexes"))
	}
	if ns.PreparedDir != filepath.Join(wantRootDir, "prepared") {
		t.Fatalf("PreparedDir=%q want %q", ns.PreparedDir, filepath.Join(wantRootDir, "prepared"))
	}
	if ns.QuarantineDir != filepath.Join(wantRootDir, "quarantine") {
		t.Fatalf("QuarantineDir=%q want %q", ns.QuarantineDir, filepath.Join(wantRootDir, "quarantine"))
	}
	if ns.TempDir != filepath.Join(wantRootDir, "tmp") {
		t.Fatalf("TempDir=%q want %q", ns.TempDir, filepath.Join(wantRootDir, "tmp"))
	}
}

// TestColumnAssetSegmentFileNameFormatM15B verifies the segment file name
// format uses zero-padded six-digit file IDs with the expected prefix/suffix.
func TestColumnAssetSegmentFileNameFormatM15B(t *testing.T) {
	cases := []struct {
		fileID uint32
		want   string
	}{
		{1, "segment-000001.tca"},
		{2, "segment-000002.tca"},
		{99, "segment-000099.tca"},
		{999999, "segment-999999.tca"},
		{1000000, "segment-1000000.tca"},
	}
	for _, tc := range cases {
		got := columnAssetSegmentFileName(tc.fileID)
		if got != tc.want {
			t.Errorf("columnAssetSegmentFileName(%d)=%q want %q", tc.fileID, got, tc.want)
		}
	}
}

// TestColumnAssetGCPlanForDetailPreservesEntriesWhenDetailedM15B verifies that
// columnAssetGCPlanForDetail with detailed=true returns entries and segment
// entries unchanged.
func TestColumnAssetGCPlanForDetailPreservesEntriesWhenDetailedM15B(t *testing.T) {
	plan := ColumnAssetReachabilityPlan{
		Collection: "events",
		Namespace:  "events/column-assets",
		Entries: []ColumnAssetReachabilityRefEntry{
			{Ref: ColumnAssetRef{FileID: 1, Length: 64}, Status: ColumnAssetReachabilityProtected},
		},
		SegmentEntries: []ColumnAssetReachabilitySegmentEntry{
			{FileID: 1, Bytes: 64, Status: ColumnAssetReachabilitySegmentProtected},
		},
	}
	result := columnAssetGCPlanForDetail(plan, true)
	if len(result.Entries) != 1 {
		t.Fatalf("detailed=true entries=%d want 1", len(result.Entries))
	}
	if len(result.SegmentEntries) != 1 {
		t.Fatalf("detailed=true segmentEntries=%d want 1", len(result.SegmentEntries))
	}
	if result.Collection != "events" || result.Namespace != "events/column-assets" {
		t.Fatalf("detailed=true other fields modified: collection=%q namespace=%q",
			result.Collection, result.Namespace)
	}
}

// TestColumnAssetGCPlanForDetailClearsEntriesWhenNotDetailedM15B verifies
// that columnAssetGCPlanForDetail with detailed=false clears entries and
// segment entries while preserving other fields.
func TestColumnAssetGCPlanForDetailClearsEntriesWhenNotDetailedM15B(t *testing.T) {
	plan := ColumnAssetReachabilityPlan{
		Collection: "events",
		Namespace:  "events/column-assets",
		Complete:   true,
		Entries: []ColumnAssetReachabilityRefEntry{
			{Ref: ColumnAssetRef{FileID: 1, Length: 64}, Status: ColumnAssetReachabilityProtected},
			{Ref: ColumnAssetRef{FileID: 2, Length: 32}, Status: ColumnAssetReachabilityReclaimable},
		},
		SegmentEntries: []ColumnAssetReachabilitySegmentEntry{
			{FileID: 1, Bytes: 64, Status: ColumnAssetReachabilitySegmentProtected},
		},
	}
	result := columnAssetGCPlanForDetail(plan, false)
	if result.Entries != nil {
		t.Fatalf("detailed=false entries=%v want nil", result.Entries)
	}
	if result.SegmentEntries != nil {
		t.Fatalf("detailed=false segmentEntries=%v want nil", result.SegmentEntries)
	}
	// Other fields must be preserved.
	if result.Collection != "events" || result.Namespace != "events/column-assets" || !result.Complete {
		t.Fatalf("detailed=false modified non-entry fields: collection=%q namespace=%q complete=%t",
			result.Collection, result.Namespace, result.Complete)
	}
}

// TestColumnAssetGCRejectsNilCollectionM15B verifies that calling ColumnAssetGC
// on a nil *Collection returns errCollectionNil immediately.
func TestColumnAssetGCRejectsNilCollectionM15B(t *testing.T) {
	var col *Collection
	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{DryRun: true})
	if !errors.Is(err, errCollectionNil) {
		t.Fatalf("nil collection error=%v want errCollectionNil", err)
	}
	if stats.SegmentsEligible != 0 || stats.SegmentsDeleted != 0 {
		t.Fatalf("nil collection stats=%+v want zero", stats)
	}
}

// TestColumnAssetGCRejectsCancelledContextBeforeStartM15B verifies that
// ColumnAssetGC returns the context error immediately when the context is
// already cancelled before the call.
func TestColumnAssetGCRejectsCancelledContextBeforeStartM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stats, err := col.ColumnAssetGC(ctx, ColumnAssetGCOptions{DryRun: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context error=%v want context.Canceled", err)
	}
	if stats.SegmentsEligible != 0 || stats.SegmentsDeleted != 0 {
		t.Fatalf("cancelled context stats=%+v want zero", stats)
	}
}

// TestColumnAssetGCDryRunIncompletePlanReturnsNoErrorM15B verifies that when
// the reachability plan is incomplete (unknown segments present), a dry-run
// GC returns stats without error, while a real GC fails closed with
// ErrColumnAssetReachabilityIncomplete.
func TestColumnAssetGCDryRunIncompletePlanReturnsNoErrorM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// Plant an unknown segment file to make the plan incomplete.
	cfg := col.Meta().Options.ColumnStore
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	unknownPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(99))
	if err := os.WriteFile(unknownPath, []byte("unknown-bytes"), 0o600); err != nil {
		t.Fatalf("write unknown segment: %v", err)
	}

	// Dry-run: incomplete plan should return nil error.
	dryStats, dryErr := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{DryRun: true})
	if dryErr != nil {
		t.Fatalf("dry-run incomplete plan error=%v want nil", dryErr)
	}
	if dryStats.Plan.Complete {
		t.Fatalf("dry-run incomplete plan marked complete")
	}
	if dryStats.Plan.Segments.Unknown == 0 {
		t.Fatalf("dry-run incomplete plan unknown segments=%d want > 0", dryStats.Plan.Segments.Unknown)
	}
	if dryStats.SegmentsDeleted != 0 {
		t.Fatalf("dry-run deleted=%d want 0", dryStats.SegmentsDeleted)
	}

	// Real GC: incomplete plan should fail closed with ErrColumnAssetReachabilityIncomplete.
	realStats, realErr := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{})
	if !errors.Is(realErr, ErrColumnAssetReachabilityIncomplete) {
		t.Fatalf("real GC incomplete plan error=%v want ErrColumnAssetReachabilityIncomplete", realErr)
	}
	if realStats.SegmentsDeleted != 0 {
		t.Fatalf("real GC incomplete plan deleted=%d want 0", realStats.SegmentsDeleted)
	}
}

// TestErrColumnAssetReachabilityIncompleteIsWrappableM15B verifies that
// ErrColumnAssetReachabilityIncomplete can be compared with errors.Is after
// wrapping, matching the convention used in columnAssetGC where the error is
// returned via fmt.Errorf("%w", ...).
func TestErrColumnAssetReachabilityIncompleteIsWrappableM15B(t *testing.T) {
	wrapped := errors.Join(ErrColumnAssetReachabilityIncomplete, errors.New("extra context"))
	if !errors.Is(wrapped, ErrColumnAssetReachabilityIncomplete) {
		t.Fatal("errors.Is failed for wrapped ErrColumnAssetReachabilityIncomplete")
	}
	unrelated := errors.New("other error")
	if errors.Is(unrelated, ErrColumnAssetReachabilityIncomplete) {
		t.Fatal("errors.Is matched unrelated error against ErrColumnAssetReachabilityIncomplete")
	}
}

// TestColumnAssetGCSegmentEligibleRequiresAllConditionsM15B is an additional
// boundary test verifying that each required condition for segment eligibility
// is individually necessary. Mutating any single condition must cause rejection.
func TestColumnAssetGCSegmentEligibleRequiresAllConditionsM15B(t *testing.T) {
	segmentDir := t.TempDir()
	fileID := uint32(5)
	base := ColumnAssetReachabilitySegmentEntry{
		FileID:           fileID,
		Path:             filepath.Join(segmentDir, columnAssetSegmentFileName(fileID)),
		Bytes:            128,
		Status:           ColumnAssetReachabilitySegmentReclaimable,
		RefCount:         2,
		ReclaimableBytes: 128,
		ProtectedBytes:   0,
		UnknownBytes:     0,
	}
	if !columnAssetGCSegmentEligibleForDelete(segmentDir, base) {
		t.Fatalf("base eligible entry was rejected: %+v", base)
	}

	cases := []struct {
		name  string
		mutFn func(e ColumnAssetReachabilitySegmentEntry) ColumnAssetReachabilitySegmentEntry
	}{
		{"wrong_status", func(e ColumnAssetReachabilitySegmentEntry) ColumnAssetReachabilitySegmentEntry {
			e.Status = ColumnAssetReachabilitySegmentMixed
			return e
		}},
		{"zero_file_id", func(e ColumnAssetReachabilitySegmentEntry) ColumnAssetReachabilitySegmentEntry {
			e.FileID = 0
			return e
		}},
		{"empty_path", func(e ColumnAssetReachabilitySegmentEntry) ColumnAssetReachabilitySegmentEntry {
			e.Path = ""
			return e
		}},
		{"zero_bytes", func(e ColumnAssetReachabilitySegmentEntry) ColumnAssetReachabilitySegmentEntry {
			e.Bytes = 0
			return e
		}},
		{"negative_bytes", func(e ColumnAssetReachabilitySegmentEntry) ColumnAssetReachabilitySegmentEntry {
			e.Bytes = -1
			return e
		}},
		{"zero_ref_count", func(e ColumnAssetReachabilitySegmentEntry) ColumnAssetReachabilitySegmentEntry {
			e.RefCount = 0
			return e
		}},
		{"protected_bytes_nonzero", func(e ColumnAssetReachabilitySegmentEntry) ColumnAssetReachabilitySegmentEntry {
			e.ProtectedBytes = 1
			return e
		}},
		{"unknown_bytes_nonzero", func(e ColumnAssetReachabilitySegmentEntry) ColumnAssetReachabilitySegmentEntry {
			e.UnknownBytes = 1
			return e
		}},
		{"reclaimable_not_equal_bytes", func(e ColumnAssetReachabilitySegmentEntry) ColumnAssetReachabilitySegmentEntry {
			e.ReclaimableBytes = e.Bytes - 1
			return e
		}},
		{"wrong_path_for_file_id", func(e ColumnAssetReachabilitySegmentEntry) ColumnAssetReachabilitySegmentEntry {
			e.Path = filepath.Join(segmentDir, columnAssetSegmentFileName(fileID+1))
			return e
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			modified := tc.mutFn(base)
			if columnAssetGCSegmentEligibleForDelete(segmentDir, modified) {
				t.Fatalf("modified entry %q was incorrectly accepted: %+v", tc.name, modified)
			}
		})
	}
}

// TestColumnAssetGCNilContextFallsBackToBackgroundM15B verifies that passing
// a nil context to ColumnAssetGC is treated as context.Background() (no panic,
// no immediate error from ctx.Err()).
func TestColumnAssetGCNilContextFallsBackToBackgroundM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// A nil context must not panic. No candidates means no eligible, no deletion.
	//nolint:staticcheck // intentionally passing nil context to test nil guard
	stats, err := col.ColumnAssetGC(nil, ColumnAssetGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("nil context dry-run error=%v want nil", err)
	}
	if stats.SegmentsDeleted != 0 {
		t.Fatalf("nil context deleted=%d want 0", stats.SegmentsDeleted)
	}
}

// TestColumnAssetGCInitialRetainedStatsFromPlanM15B verifies that the initial
// SegmentsRetained and BytesRetained values are taken from the reachability
// plan totals before any deletions.
func TestColumnAssetGCInitialRetainedStatsFromPlanM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 77, []byte("reclaimable-segment"))

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		DryRun:        true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC dry-run: %v", err)
	}
	// Retained should start at plan total (live segments + candidate) before any deletion.
	if stats.SegmentsRetained != stats.Plan.Segments.Total {
		t.Fatalf("SegmentsRetained=%d want Plan.Segments.Total=%d", stats.SegmentsRetained, stats.Plan.Segments.Total)
	}
	if stats.BytesRetained != stats.Plan.Segments.BytesTotal {
		t.Fatalf("BytesRetained=%d want Plan.Segments.BytesTotal=%d", stats.BytesRetained, stats.Plan.Segments.BytesTotal)
	}
}
