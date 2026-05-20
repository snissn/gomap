package collections

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnAssetGCDryRunReportsReclaimableButDoesNotDeleteM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 77, []byte("reclaimable-column-asset-segment"))
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		DryRun:        true,
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC dry-run: %v", err)
	}
	if !stats.DryRun || stats.SegmentsEligible != 1 || stats.BytesEligible != candidate.Length {
		t.Fatalf("stats=%+v want one dry-run eligible segment of %d bytes", stats, candidate.Length)
	}
	if stats.SegmentsDeleted != 0 || stats.BytesDeleted != 0 {
		t.Fatalf("dry-run deleted stats=%+v", stats)
	}
	if stats.Plan.Segments.Reclaimable != 1 || stats.Plan.Segments.Unknown != 0 || !stats.Plan.Complete {
		t.Fatalf("plan segments=%+v complete=%t want one complete reclaimable segment", stats.Plan.Segments, stats.Plan.Complete)
	}
	if _, err := os.Stat(candidatePath); err != nil {
		t.Fatalf("dry-run removed candidate segment %q: %v", candidatePath, err)
	}
}

func TestColumnAssetGCDryRunCanReturnSegmentDetailsWithoutRefEntriesM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 78, []byte("segment-detail-without-ref-detail"))

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		DryRun:         true,
		SegmentDetails: true,
		CandidateRefs:  []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC dry-run: %v", err)
	}
	if len(stats.Plan.Entries) != 0 {
		t.Fatalf("plan kept %d ref entries without Detailed=true", len(stats.Plan.Entries))
	}
	if len(stats.Plan.SegmentEntries) == 0 {
		t.Fatalf("plan omitted segment entries despite SegmentDetails=true: %+v", stats.Plan.Segments)
	}
	if stats.SegmentsEligible != 1 || stats.BytesEligible != candidate.Length {
		t.Fatalf("stats=%+v want one eligible segment of %d bytes", stats, candidate.Length)
	}
}

func TestColumnAssetGCDryRunSummaryOmitsSegmentEntriesAndMissingRetainedM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	cfg := col.Meta().Options.ColumnStore

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		DryRun: true,
		CandidateRefs: []ColumnAssetRef{{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  cfg.AssetManager.Namespace,
			Generation: 1,
			PartID:     1,
			FileID:     99,
			Length:     64,
			Checksum:   page.Checksum([]byte("missing")),
		}},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC dry-run: %v", err)
	}
	if len(stats.Plan.Entries) != 0 || len(stats.Plan.SegmentEntries) != 0 {
		t.Fatalf("summary dry-run retained detail entries: refs=%d segments=%d", len(stats.Plan.Entries), len(stats.Plan.SegmentEntries))
	}
	if stats.Plan.Segments.Missing != 1 {
		t.Fatalf("segments=%+v want one missing segment in summary plan", stats.Plan.Segments)
	}
	if stats.SegmentsRetained != stats.Plan.Segments.Total-stats.Plan.Segments.Missing || stats.BytesRetained != stats.Plan.Segments.BytesTotal {
		t.Fatalf("stats=%+v segments=%+v want missing segment excluded from retained physical segment count", stats, stats.Plan.Segments)
	}
}

func TestColumnAssetGCDeletesCompleteReclaimableSegmentM15B(t *testing.T) {
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
	liveRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(liveRefs) == 0 {
		t.Fatal("manifest refs empty, test requires live physical assets")
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 88, []byte("fully-reclaimable-segment"))
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC: %v", err)
	}
	if stats.DryRun || stats.SegmentsEligible != 1 || stats.SegmentsDeleted != 1 || stats.BytesDeleted != candidate.Length {
		t.Fatalf("stats=%+v want one deleted segment of %d bytes", stats, candidate.Length)
	}
	if _, err := os.Stat(candidatePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate segment still exists or unexpected stat error: %v", err)
	}
	for _, ref := range liveRefs {
		if _, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref); err != nil {
			t.Fatalf("live ref %+v unreadable after GC: %v", ref, err)
		}
	}
}

func TestColumnAssetGCRetainedStatsUpdateOnPartialContextCancelM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	first := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 101, []byte("first-reclaimable-segment"))
	second := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 102, []byte("second-reclaimable-segment"))
	firstPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), first)
	if err != nil {
		t.Fatalf("first columnAssetSegmentPath: %v", err)
	}
	secondPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), second)
	if err != nil {
		t.Fatalf("second columnAssetSegmentPath: %v", err)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), first.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	var syncedDirs []string
	gcCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	restoreHooks := setColumnAssetGCTestHooks(func(path string) error {
		err := os.Remove(path)
		if err == nil && path == firstPath {
			cancel()
		}
		return err
	}, func(dir string) error {
		syncedDirs = append(syncedDirs, dir)
		return nil
	})
	defer restoreHooks()

	stats, err := col.ColumnAssetGC(gcCtx, ColumnAssetGCOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{first, second},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ColumnAssetGC error=%v want context.Canceled", err)
	}
	if stats.SegmentsDeleted != 1 || stats.BytesDeleted != first.Length {
		t.Fatalf("deleted stats=%+v want first segment deleted", stats)
	}
	if stats.SegmentsRetained != columnAssetGCExistingSegmentCount(stats.Plan)-stats.SegmentsDeleted ||
		stats.BytesRetained != stats.Plan.Segments.BytesTotal-stats.BytesDeleted {
		t.Fatalf("retained stats=%+v planSegments=%+v want retained decremented after partial delete", stats, stats.Plan.Segments)
	}
	if _, err := os.Stat(firstPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("first candidate still exists or unexpected stat error: %v", err)
	}
	if _, err := os.Stat(secondPath); err != nil {
		t.Fatalf("second candidate removed despite context cancellation: %v", err)
	}
	if len(syncedDirs) != 1 || syncedDirs[0] != namespace.SegmentDir {
		t.Fatalf("sync dirs=%v want one sync of %q after partial delete", syncedDirs, namespace.SegmentDir)
	}
}

func TestColumnAssetGCRetainsMixedSegmentM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 2, 99)
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC: %v", err)
	}
	if stats.SegmentsEligible != 0 || stats.SegmentsDeleted != 0 {
		t.Fatalf("stats=%+v want no deletion for mixed protected/reclaimable segment", stats)
	}
	if stats.Plan.Segments.Mixed != 1 || stats.Plan.RewriteDebtBytes != candidate.Length {
		t.Fatalf("plan segments=%+v debt=%d want mixed rewrite debt=%d", stats.Plan.Segments, stats.Plan.RewriteDebtBytes, candidate.Length)
	}
	if _, err := os.Stat(candidatePath); err != nil {
		t.Fatalf("mixed candidate segment was removed: %v", err)
	}
}

func TestColumnAssetGCProtectsPinnedCandidateM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 89, []byte("pinned-candidate-segment"))
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
		PinnedRefs:    []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC: %v", err)
	}
	if stats.SegmentsEligible != 0 || stats.SegmentsDeleted != 0 || stats.Plan.Refs.Reclaimable != 0 {
		t.Fatalf("stats=%+v refs=%+v want pinned candidate protected", stats, stats.Plan.Refs)
	}
	if _, err := os.Stat(candidatePath); err != nil {
		t.Fatalf("pinned candidate segment was removed: %v", err)
	}
}

func TestColumnAssetGCFailClosedOnIncompletePlanM15B(t *testing.T) {
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
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(99)), []byte("unknown-bytes"), 0o600); err != nil {
		t.Fatalf("WriteFile unknown segment: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 90, []byte("complete-but-plan-is-incomplete"))
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, ErrColumnAssetReachabilityIncomplete) {
		t.Fatalf("ColumnAssetGC error=%v want ErrColumnAssetReachabilityIncomplete", err)
	}
	if stats.SegmentsDeleted != 0 || stats.BytesDeleted != 0 {
		t.Fatalf("incomplete plan deleted stats=%+v", stats)
	}
	if stats.Plan.Complete || stats.Plan.Segments.Unknown == 0 {
		t.Fatalf("plan=%+v want incomplete unknown retained", stats.Plan)
	}
	if _, err := os.Stat(candidatePath); err != nil {
		t.Fatalf("candidate segment was removed after incomplete plan: %v", err)
	}
}

func TestColumnAssetGCIncompletePlanReportsUncertainRefsM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	cfg := col.Meta().Options.ColumnStore
	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: []ColumnAssetRef{{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  cfg.AssetManager.Namespace,
			Generation: 1,
			PartID:     1,
			FileID:     1,
			Offset:     -1,
			Length:     64,
		}},
	})
	if !errors.Is(err, ErrColumnAssetReachabilityIncomplete) || !strings.Contains(err.Error(), "uncertain_refs=1") {
		t.Fatalf("ColumnAssetGC error=%v want incomplete error with uncertain_refs=1", err)
	}
	if stats.Plan.Complete || stats.Plan.Refs.Uncertain != 1 {
		t.Fatalf("plan complete=%t refs=%+v want one uncertain ref", stats.Plan.Complete, stats.Plan.Refs)
	}
	if stats.Plan.Segments.Unknown != 0 || stats.Plan.Segments.Missing != 0 || stats.Plan.Segments.OutOfBoundsRefs != 0 {
		t.Fatalf("invalid ref produced segment counters: %+v", stats.Plan.Segments)
	}
}

func TestColumnAssetGCRejectsReadOnlyNoEligibleDestructiveMaintenanceM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		_ = d.Close()
		t.Fatalf("Insert: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}

	readonly, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open readonly: %v", err)
	}
	defer func() { _ = readonly.Close() }()
	readonlyCol := openColumnStoreCollectionM10B(t, readonly)

	stats, err := readonlyCol.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{})
	if !errors.Is(err, backenddb.ErrReadOnly) {
		t.Fatalf("ColumnAssetGC readonly no-eligible error=%v want ErrReadOnly", err)
	}
	if stats.SegmentsEligible != 0 || stats.SegmentsDeleted != 0 {
		t.Fatalf("stats=%+v want no eligible deletion on readonly handle", stats)
	}
}

func TestColumnAssetGCRejectsReadOnlyDestructiveMaintenanceM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		_ = d.Close()
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 91, []byte("readonly-candidate-segment"))
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		_ = d.Close()
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}

	readonly, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open readonly: %v", err)
	}
	defer func() { _ = readonly.Close() }()
	readonlyCol := openColumnStoreCollectionM10B(t, readonly)

	stats, err := readonlyCol.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, backenddb.ErrReadOnly) {
		t.Fatalf("ColumnAssetGC readonly error=%v want ErrReadOnly", err)
	}
	if stats.SegmentsEligible != 0 || stats.SegmentsDeleted != 0 || stats.Plan.ProtectOnly {
		t.Fatalf("stats=%+v want readiness rejection before reachability planning", stats)
	}
	if _, err := os.Stat(candidatePath); err != nil {
		t.Fatalf("readonly destructive GC removed candidate segment: %v", err)
	}
}

func TestColumnAssetGCRejectsClosedDestructiveMaintenanceBeforePlanningM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		_ = d.Close()
		t.Fatalf("Insert: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{})
	if !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("ColumnAssetGC closed error=%v want ErrClosed", err)
	}
	if stats.Plan.ProtectOnly || stats.Plan.Collection != "" || stats.SegmentsEligible != 0 || stats.SegmentsDeleted != 0 {
		t.Fatalf("stats=%+v want readiness rejection before reachability planning", stats)
	}
}

func TestColumnAssetGCSegmentEligibleRequiresExactCanonicalPathM15B(t *testing.T) {
	baseDir := t.TempDir()
	segmentDir := filepath.Join(baseDir, "segments")
	entry := ColumnAssetReachabilitySegmentEntry{
		FileID:           7,
		Path:             filepath.Join(segmentDir, columnAssetSegmentFileName(7)),
		Bytes:            64,
		Status:           ColumnAssetReachabilitySegmentReclaimable,
		ReclaimableBytes: 64,
		RefCount:         1,
	}
	if !columnAssetGCSegmentEligibleForDelete(segmentDir, entry) {
		t.Fatalf("canonical entry was rejected: %+v", entry)
	}
	entry.Path = filepath.Join(segmentDir, "nested", columnAssetSegmentFileName(7))
	if columnAssetGCSegmentEligibleForDelete(segmentDir, entry) {
		t.Fatalf("nested canonical basename was accepted: %+v", entry)
	}
	entry.Path = segmentDir + string(os.PathSeparator) + ".." + string(os.PathSeparator) + filepath.Base(segmentDir) + string(os.PathSeparator) + columnAssetSegmentFileName(7)
	if columnAssetGCSegmentEligibleForDelete(segmentDir, entry) {
		t.Fatalf("non-canonical equivalent path was accepted: %+v", entry)
	}
	entry.Path = filepath.Join(segmentDir, columnAssetSegmentFileName(8))
	if columnAssetGCSegmentEligibleForDelete(segmentDir, entry) {
		t.Fatalf("wrong canonical file id was accepted: %+v", entry)
	}
}

func TestColumnAssetGCByteAccountingSaturatesM15B(t *testing.T) {
	stats := ColumnAssetGCStats{BytesRetained: 3}
	stats.BytesEligible = addColumnAssetReachabilityBytes(math.MaxInt64-1, 2)
	stats.BytesDeleted = addColumnAssetReachabilityBytes(math.MaxInt64-2, 3)
	stats.BytesRetained = subColumnAssetReachabilityBytesFloor(stats.BytesRetained, 4)
	if stats.BytesEligible != math.MaxInt64 || stats.BytesDeleted != math.MaxInt64 {
		t.Fatalf("saturating add failed: %+v", stats)
	}
	if stats.BytesRetained != 0 {
		t.Fatalf("retained underflowed to %d, want 0", stats.BytesRetained)
	}
}

func TestColumnAssetGCClosedDuringPlanningReturnsErrClosedM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err := col.columnAssetGC(context.Background(), ColumnAssetGCOptions{})
	if !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("columnAssetGC err=%v want ErrClosed", err)
	}
}

func writeColumnAssetGCCandidateSegmentM15B(t testing.TB, rootDir string, col *Collection, fileID uint32, payload []byte) ColumnAssetRef {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		t.Fatalf("missing column store config: %+v", cfg)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	segmentPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(fileID))
	if err := os.WriteFile(segmentPath, payload, 0o600); err != nil {
		t.Fatalf("WriteFile segment: %v", err)
	}
	return ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  cfg.AssetManager.Namespace,
		Generation: 2,
		PartID:     uint64(fileID),
		FileID:     fileID,
		Length:     int64(len(payload)),
		Checksum:   page.Checksum(payload),
	}
}

func BenchmarkColumnAssetGCDryRunTenKCandidatesM15B(b *testing.B) {
	const refs = 10_000
	const refBytes = 64
	dir := b.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		b.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		b.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		b.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("live"), []byte(`{"time_us":1,"kind":"like","did":"did_live"}`)); err != nil {
		b.Fatalf("Insert: %v", err)
	}
	payload := make([]byte, refBytes)
	candidates := make([]ColumnAssetRef, 0, refs)
	for i := 0; i < refs; i++ {
		payload[0] = byte(i)
		candidates = append(candidates, writeColumnAssetGCCandidateSegmentM15B(b, d.ColumnAssetRootDir(), col, uint32(i+2), payload))
	}

	b.ReportAllocs()
	b.SetBytes(refs * refBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
			DryRun:        true,
			CandidateRefs: candidates,
		})
		if err != nil {
			b.Fatal(err)
		}
		if !stats.Plan.Complete || stats.SegmentsEligible != refs || stats.SegmentsDeleted != 0 {
			b.Fatalf("unexpected stats: complete=%t eligible=%d deleted=%d plan=%+v", stats.Plan.Complete, stats.SegmentsEligible, stats.SegmentsDeleted, stats.Plan.Segments)
		}
	}
}

func BenchmarkColumnAssetGCDryRunTenKRefsOneSegmentM15B(b *testing.B) {
	const refs = 10_000
	const refBytes = int64(64)
	d, col := prepareColumnAssetGCBenchmarkCollectionM15B(b)
	defer func() { _ = d.Close() }()
	cfg := col.Meta().Options.ColumnStore
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
	if err != nil {
		b.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	const fileID = uint32(7)
	segmentPath := filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(fileID))
	segment, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		b.Fatalf("OpenFile segment: %v", err)
	}
	if err := segment.Truncate(refs * refBytes); err != nil {
		_ = segment.Close()
		b.Fatalf("Truncate segment: %v", err)
	}
	if err := segment.Close(); err != nil {
		b.Fatalf("Close segment: %v", err)
	}
	candidates := make([]ColumnAssetRef, 0, refs)
	for i := 0; i < refs; i++ {
		candidates = append(candidates, ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  cfg.AssetManager.Namespace,
			Generation: 2,
			PartID:     uint64(i + 1),
			FileID:     fileID,
			Offset:     int64(i) * refBytes,
			Length:     refBytes,
			Checksum:   uint32(i + 1),
		})
	}

	b.ReportAllocs()
	b.SetBytes(refs * refBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
			DryRun:        true,
			CandidateRefs: candidates,
		})
		if err != nil {
			b.Fatal(err)
		}
		if !stats.Plan.Complete || stats.SegmentsEligible != 1 || stats.BytesEligible != refs*refBytes {
			b.Fatalf("unexpected stats: complete=%t eligible=%d bytes=%d plan=%+v", stats.Plan.Complete, stats.SegmentsEligible, stats.BytesEligible, stats.Plan.Segments)
		}
	}
}

func prepareColumnAssetGCBenchmarkCollectionM15B(b *testing.B) (*backenddb.DB, *Collection) {
	b.Helper()
	dir := b.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		b.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		_ = d.Close()
		b.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		b.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("live"), []byte(`{"time_us":1,"kind":"like","did":"did_live"}`)); err != nil {
		_ = d.Close()
		b.Fatalf("Insert: %v", err)
	}
	return d, col
}
