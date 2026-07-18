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
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func requireColumnAssetExactDestructiveGCTest(tb testing.TB) {
	tb.Helper()
	requireStandaloneColumnProductionAuthorityTest(tb)
}

func requireStandaloneColumnProductionAuthorityTest(tb testing.TB) {
	tb.Helper()
	if !rootpublication.StableRelativeNamespaceSupported() {
		tb.Skip("standalone column production authority requires exact relative namespace support")
	}
}

func TestRecoverableColumnAssetReplayRefsPreservePairedVisibleBasis(t *testing.T) {
	candidate := ColumnAssetRef{Generation: 8, FileID: 7}
	bases := []recoverableColumnAssetReplayBasis{
		{
			appliedCommandLSN:  30,
			manifestGeneration: 5,
			collection:         "newer-lsn",
			config:             ColumnStoreConfig{AssetManager: &ColumnAssetManagerConfig{Namespace: "newer-lsn"}},
		},
		{
			appliedCommandLSN:  20,
			manifestGeneration: 9,
			collection:         "older-lsn-higher-generation",
			config:             ColumnStoreConfig{AssetManager: &ColumnAssetManagerConfig{Namespace: "paired-basis"}},
		},
	}
	refs, err := recoverableColumnAssetReplayRefs(
		[]ColumnAssetRef{candidate},
		bases,
		func(uint64) bool { return true },
		func(ref ColumnAssetRef, basis recoverableColumnAssetReplayBasis) (bool, error) {
			if ref != candidate {
				t.Fatalf("classifier ref=%+v want %+v", ref, candidate)
			}
			if basis.appliedCommandLSN != 20 || basis.manifestGeneration != 9 ||
				basis.collection != "older-lsn-higher-generation" || basis.config.AssetManager == nil ||
				basis.config.AssetManager.Namespace != "paired-basis" {
				t.Fatalf("classifier received mixed replay basis: %+v", basis)
			}
			return true, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0] != candidate {
		t.Fatalf("replay refs=%+v want paired-basis candidate %+v", refs, candidate)
	}
}

func TestRecoverableColumnAssetReplayCandidateFailsClosedOnUnreadableAsset(t *testing.T) {
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  "missing-replay-candidate",
		Generation: 1,
		PartID:     1,
		FileID:     1,
		Length:     1,
	}
	replayable, err := recoverableColumnAssetReplayCandidate(t.TempDir(), ref, "events", ColumnStoreConfig{}, 1)
	if err == nil {
		t.Fatalf("unreadable replay candidate returned replayable=%t without error", replayable)
	}
}

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

	summary, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		DryRun:        true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC summary dry-run: %v", err)
	}
	if !summary.DryRun || summary.SegmentsEligible != 1 || summary.BytesEligible != candidate.Length {
		t.Fatalf("summary stats=%+v want one dry-run eligible segment of %d bytes", summary, candidate.Length)
	}
	if len(summary.Plan.SegmentEntries) != 0 {
		t.Fatalf("summary segment entries=%d want non-detailed summary plan", len(summary.Plan.SegmentEntries))
	}
	if summary.Plan.Segments.BytesWholeReclaimable != candidate.Length {
		t.Fatalf("summary segment stats=%+v want whole reclaimable bytes %d", summary.Plan.Segments, candidate.Length)
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
	foundCandidateEntry := false
	for _, entry := range stats.Plan.SegmentEntries {
		if entry.FileID == candidate.FileID && entry.Status == ColumnAssetReachabilitySegmentReclaimable {
			foundCandidateEntry = true
			break
		}
	}
	if !foundCandidateEntry {
		t.Fatalf("dry-run detailed segment entries=%+v want reclaimable candidate file %d", stats.Plan.SegmentEntries, candidate.FileID)
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

func TestColumnAssetGCDryRunSummaryReportsEligibleWithoutSegmentEntriesM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 79, []byte("summary-eligible-without-segment-entries"))

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		DryRun:        true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC dry-run: %v", err)
	}
	if len(stats.Plan.Entries) != 0 || len(stats.Plan.SegmentEntries) != 0 {
		t.Fatalf("summary dry-run retained detail entries: refs=%d segments=%d", len(stats.Plan.Entries), len(stats.Plan.SegmentEntries))
	}
	if stats.SegmentsEligible != 1 || stats.BytesEligible != candidate.Length {
		t.Fatalf("stats=%+v want one summary eligible segment of %d bytes", stats, candidate.Length)
	}
	if stats.SegmentsDeleted != 0 || stats.BytesDeleted != 0 {
		t.Fatalf("dry-run deleted stats=%+v", stats)
	}
}

func TestColumnAssetGCDeletesCompleteReclaimableSegmentM15B(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
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

func TestColumnAssetGCRetainsPreparedUnpublishedSegmentAfterReopenM15C(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	closed := false
	defer func() {
		if !closed {
			_ = d.Close()
		}
	}()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	prepared := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 87, []byte("prepared-before-manifest-publish"))
	preparedPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), prepared)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{prepared},
		PreparedRefs:  []ColumnAssetRef{prepared},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC with prepared ref: %v", err)
	}
	if stats.SegmentsEligible != 0 || stats.SegmentsDeleted != 0 || stats.Plan.Sources.PreparedRefs != 1 {
		t.Fatalf("stats=%+v plan=%+v want prepared ref retained and not deleted", stats, stats.Plan)
	}
	if _, err := os.Stat(preparedPath); err != nil {
		t.Fatalf("prepared segment removed before reopen: %v", err)
	}
	if raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), prepared); err != nil || string(raw) != "prepared-before-manifest-publish" {
		t.Fatalf("prepared asset read before reopen raw=%q err=%v", raw, err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}
	closed = true

	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	reopenedStats, err := reopened.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{prepared},
		PreparedRefs:  []ColumnAssetRef{prepared},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after reopen with prepared ref: %v", err)
	}
	if reopenedStats.SegmentsEligible != 0 || reopenedStats.SegmentsDeleted != 0 || reopenedStats.Plan.Sources.PreparedRefs != 1 {
		t.Fatalf("reopened stats=%+v plan=%+v want prepared ref retained and not deleted", reopenedStats, reopenedStats.Plan)
	}
	if raw, err := readColumnPhysicalAssetFromManager(reopen.ColumnAssetRootDir(), prepared); err != nil || string(raw) != "prepared-before-manifest-publish" {
		t.Fatalf("prepared asset read after reopen raw=%q err=%v", raw, err)
	}
}

func TestColumnAssetGCTreatsMissingEligibleSegmentAsDeletedM15B(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 89, []byte("missing-after-plan"))
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), candidate.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	var syncedDirs []string
	restoreHooks := setColumnAssetGCTestHooks(func(path string) error {
		if path != candidatePath {
			t.Fatalf("remove path=%q want %q", path, candidatePath)
		}
		if err := os.Remove(path); err != nil {
			t.Fatalf("pre-remove candidate: %v", err)
		}
		return os.ErrNotExist
	}, func(dir string) error {
		syncedDirs = append(syncedDirs, dir)
		return nil
	})
	defer restoreHooks()

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC: %v", err)
	}
	if stats.SegmentsEligible != 1 || stats.BytesEligible != candidate.Length ||
		stats.SegmentsDeleted != 1 || stats.BytesDeleted != candidate.Length {
		t.Fatalf("stats=%+v want missing eligible segment counted deleted with %d bytes", stats, candidate.Length)
	}
	if stats.SegmentsRetained != columnAssetGCExistingSegmentCount(stats.Plan)-stats.SegmentsDeleted ||
		stats.BytesRetained != stats.Plan.Segments.BytesTotal-stats.BytesDeleted {
		t.Fatalf("retained stats=%+v planSegments=%+v want retained decremented after missing delete", stats, stats.Plan.Segments)
	}
	if len(syncedDirs) != 1 || syncedDirs[0] != namespace.SegmentDir {
		t.Fatalf("sync dirs=%v want one sync of %q after missing delete", syncedDirs, namespace.SegmentDir)
	}
	if _, err := os.Stat(candidatePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate segment still exists or unexpected stat error: %v", err)
	}
}

func TestColumnAssetGCRetainedStatsUpdateOnPartialContextCancelM15B(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
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

	dry, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		DryRun:        true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC dry-run: %v", err)
	}
	if dry.SegmentsEligible != 0 || dry.BytesEligible != 0 {
		t.Fatalf("dry-run stats=%+v want no whole-segment eligibility for mixed protected/reclaimable segment", dry)
	}
	if len(dry.Plan.SegmentEntries) != 0 {
		t.Fatalf("dry-run segment entries=%d want non-detailed summary plan", len(dry.Plan.SegmentEntries))
	}
	if dry.Plan.Segments.Mixed != 1 || dry.Plan.RewriteDebtBytes != candidate.Length {
		t.Fatalf("dry-run plan segments=%+v debt=%d want mixed rewrite debt=%d", dry.Plan.Segments, dry.Plan.RewriteDebtBytes, candidate.Length)
	}
	if dry.Plan.Segments.BytesReclaimable != candidate.Length || dry.Plan.Segments.BytesWholeReclaimable != 0 {
		t.Fatalf("dry-run segment bytes=%+v want reclaimable range bytes %d but zero whole reclaimable bytes", dry.Plan.Segments, candidate.Length)
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

func TestColumnAssetGCRetainsStablePublicationPinThenDeletesM15B(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 97, []byte("stable-publication-pinned-candidate"))
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(candidatePath)
	if err != nil {
		t.Fatal(err)
	}
	registry := d.StableResourceIdentityPinRegistry()
	if err := d.Checkpoint(); err != nil {
		_ = file.Close()
		t.Fatalf("settle initial publication before pin baseline: %v", err)
	}
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	token, err := stableColumnAssetResourceTokenWithRegistry(file, candidate, nil, registry)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC with stable publication pin: %v", err)
	}
	if stats.SegmentsEligible != 1 || stats.SegmentsDeleted != 0 {
		t.Fatalf("pinned GC stats=%+v want eligible retained segment", stats)
	}
	if _, err := os.Stat(candidatePath); err != nil {
		t.Fatalf("stable publication pin did not retain segment: %v", err)
	}
	token.Release()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("active stable publication pins after release=%d want baseline %d", got, baselinePins)
	}

	stats, err = col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after stable publication release: %v", err)
	}
	if stats.SegmentsDeleted != 1 {
		t.Fatalf("released GC stats=%+v want one deleted segment", stats)
	}
	if _, err := os.Stat(candidatePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released stable publication segment stat=%v want not-exist", err)
	}
	if got := registry.ActiveIdentities(); got != baselineIdentities {
		t.Fatalf("active stable identities after GC=%d want baseline %d", got, baselineIdentities)
	}
}

func TestColumnAssetGCConsumesLifecyclePinSet1954(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 94, []byte("lifecycle-pinned-candidate-segment"))
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	pin, err := col.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{
		Source: ColumnAssetLifecyclePinSourcePreparedQuery,
		Owner:  "gc-lifecycle-pin-test",
		Reason: "protect candidate from destructive GC",
		Refs:   []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("AcquireColumnAssetLifecyclePinSet: %v", err)
	}

	pinnedStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		_ = pin.Close()
		t.Fatalf("ColumnAssetGC while lifecycle pin active: %v", err)
	}
	if pinnedStats.SegmentsEligible != 0 || pinnedStats.SegmentsDeleted != 0 || pinnedStats.Plan.Sources.PreparedQueryRefs != 1 || pinnedStats.Plan.Refs.Reclaimable != 0 {
		_ = pin.Close()
		t.Fatalf("pinned GC stats=%+v plan=%+v want lifecycle pin protection", pinnedStats, pinnedStats.Plan)
	}
	if _, err := os.Stat(candidatePath); err != nil {
		_ = pin.Close()
		t.Fatalf("candidate segment removed while lifecycle-pinned: %v", err)
	}
	if err := pin.Close(); err != nil {
		t.Fatalf("pin close: %v", err)
	}

	drainedStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{CandidateRefs: []ColumnAssetRef{candidate}})
	if err != nil {
		t.Fatalf("ColumnAssetGC after lifecycle pin release: %v", err)
	}
	if drainedStats.SegmentsDeleted != 1 || drainedStats.BytesDeleted != candidate.Length {
		t.Fatalf("drained GC stats=%+v want candidate deleted", drainedStats)
	}
	if _, err := os.Stat(candidatePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate still exists after pin release or unexpected stat error: %v", err)
	}
}

func TestColumnAssetGCQuarantineRegistrySegmentsFailClosed1954(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 95, []byte("quarantined-candidate-segment"))
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	lease, err := col.RegisterColumnAssetQuarantine(ColumnAssetQuarantineRegistrationOptions{
		Owner:  "gc-quarantine-test",
		Source: "integrity",
		Reason: "segment quarantine blocks destructive GC",
		Segments: []ColumnAssetQuarantineSegment{{
			FileID: candidate.FileID,
			Bytes:  candidate.Length + 1,
		}},
	})
	if err != nil {
		t.Fatalf("RegisterColumnAssetQuarantine: %v", err)
	}

	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, ErrColumnAssetReachabilityIncomplete) || !strings.Contains(err.Error(), "quarantine_segment_mismatches=1") {
		_ = lease.Close()
		t.Fatalf("ColumnAssetGC error=%v want incomplete quarantine mismatch", err)
	}
	if stats.SegmentsDeleted != 0 || stats.Plan.Segments.QuarantineSegments != 1 || stats.Plan.Segments.QuarantineSegmentMismatches != 1 || stats.Plan.Complete {
		_ = lease.Close()
		t.Fatalf("quarantine GC stats=%+v plan=%+v", stats, stats.Plan)
	}
	if _, err := os.Stat(candidatePath); err != nil {
		_ = lease.Close()
		t.Fatalf("candidate segment removed while quarantined: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("quarantine release: %v", err)
	}

	drainedStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{CandidateRefs: []ColumnAssetRef{candidate}})
	if err != nil {
		t.Fatalf("ColumnAssetGC after quarantine release: %v", err)
	}
	if drainedStats.SegmentsDeleted != 1 || drainedStats.BytesDeleted != candidate.Length {
		t.Fatalf("drained GC stats=%+v want candidate deleted", drainedStats)
	}
}

func TestColumnAssetGCAutomaticMappedResourcePinBlocksDelete1788(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 93, []byte("auto-pinned-candidate-segment"))
	candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	mgr := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(d.ColumnAssetRootDir(), candidate.Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeSnapshot, ID: "auto-gc-pin-1788", Namespace: candidate.Namespace, Collection: "events", Generation: candidate.Generation, Reason: "gc auto pin test"}
	if err := readCache.useMappedResourceManager(mgr, scope, "gc-auto-pin"); err != nil {
		_ = readCache.close()
		t.Fatalf("useMappedResourceManager: %v", err)
	}
	if raw, err := readCache.read(candidate, nil); err != nil || string(raw) != "auto-pinned-candidate-segment" {
		_ = readCache.close()
		t.Fatalf("read pinned candidate raw=%q err=%v", raw, err)
	}

	pinnedStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		_ = readCache.close()
		t.Fatalf("ColumnAssetGC while mappedresource pin active: %v", err)
	}
	if pinnedStats.SegmentsEligible != 0 || pinnedStats.SegmentsDeleted != 0 || pinnedStats.Plan.Refs.Reclaimable != 0 {
		_ = readCache.close()
		t.Fatalf("pinned GC stats=%+v refs=%+v want automatic active pin retained", pinnedStats, pinnedStats.Plan.Refs)
	}
	if pinnedStats.Plan.MappedResources.ActiveHandles == 0 || pinnedStats.Plan.MappedResources.PinnedRefs == 0 || pinnedStats.Plan.MappedResources.PinnedBytes < candidate.Length || pinnedStats.Plan.MappedResources.ActiveHeapCopyBytes < candidate.Length || pinnedStats.Plan.Sources.MappedResourcePins == 0 {
		_ = readCache.close()
		t.Fatalf("mappedresource stats=%+v sources=%+v want active pinned candidate bytes", pinnedStats.Plan.MappedResources, pinnedStats.Plan.Sources)
	}
	if _, err := os.Stat(candidatePath); err != nil {
		_ = readCache.close()
		t.Fatalf("candidate segment removed while pinned: %v", err)
	}
	if err := readCache.close(); err != nil {
		t.Fatalf("close read cache: %v", err)
	}

	drainedStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after pin release: %v", err)
	}
	if drainedStats.SegmentsDeleted != 1 || drainedStats.BytesDeleted != candidate.Length {
		t.Fatalf("drained GC stats=%+v want candidate deleted", drainedStats)
	}
	if _, err := os.Stat(candidatePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate still exists after pin release or unexpected stat error: %v", err)
	}
}

func TestColumnAssetGCRetainsSupersededSegmentWhileOlderSnapshotPinnedM15C(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
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
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(beforeRefs) == 0 {
		t.Fatal("manifest refs empty, test requires live physical assets")
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	if candidate.FileID != beforeRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires mixed segment", candidate.FileID, beforeRefs[0].FileID)
	}
	oldSegmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

	pinned := d.AcquireSnapshot()
	if pinned == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	pinnedClosed := false
	defer func() {
		if !pinnedClosed {
			_ = pinned.Close()
		}
	}()
	pinnedCatalog, err := col.catalogForSnapshot(pinned)
	if err != nil {
		t.Fatalf("catalogForSnapshot pinned: %v", err)
	}
	if pinnedCatalog == nil || pinnedCatalog.meta.Options.ColumnStore == nil {
		t.Fatalf("pinned catalog missing column store metadata: %+v", pinnedCatalog)
	}
	pinnedCollection := pinnedCatalog.meta.Name
	pinnedRootID := pinnedCatalog.rootID(collectionColumnManifestRootName(pinnedCollection))
	pinnedCfg := pinnedCatalog.meta.Options.ColumnStore.copy()

	rewrite, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite: %v", err)
	}
	if rewrite.SegmentsRewritten != 1 || rewrite.RefsRemapped != len(beforeRefs) {
		t.Fatalf("rewrite stats=%+v want one rewritten segment and %d remapped refs", rewrite, len(beforeRefs))
	}
	candidates := append(append([]ColumnAssetRef(nil), rewrite.SupersededRefs...), candidate)

	pinnedStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		Detailed:      true,
		CandidateRefs: candidates,
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC while older snapshot pinned: %v", err)
	}
	if pinnedStats.SegmentsEligible != 0 || pinnedStats.SegmentsDeleted != 0 || pinnedStats.Plan.Refs.Reclaimable != 0 {
		t.Fatalf("pinned GC stats=%+v refs=%+v want no eligible/reclaimable refs", pinnedStats, pinnedStats.Plan.Refs)
	}
	if pinnedStats.Plan.Sources.PinnedRefs != len(candidates) {
		t.Fatalf("pinned refs=%d want %d in plan sources", pinnedStats.Plan.Sources.PinnedRefs, len(candidates))
	}
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("old segment removed while older snapshot pinned: %v", err)
	}
	diag, err := col.scanColumnPhysicalRowsAtSnapshot(
		pinned,
		pinnedCatalog,
		pinnedCollection,
		pinnedRootID,
		pinnedCfg,
		true,
		columnPhysicalScanRequest{},
	)
	if err != nil {
		t.Fatalf("scanColumnPhysicalRowsAtSnapshot pinned: %v", err)
	}
	beforePhysicalRefs := columnManifestPhysicalAssetRefsForTestM1634(beforeRefs)
	if diag.RowsScanned != 2 || diag.AssetRefs != len(beforePhysicalRefs) {
		t.Fatalf("pinned diag=%+v want 2 rows and %d old physical refs", diag, len(beforePhysicalRefs))
	}
	if err := pinned.Close(); err != nil {
		t.Fatalf("Close pinned snapshot: %v", err)
	}
	pinnedClosed = true
	// Closing the explicit snapshot releases its history pin, but the rewrite's
	// visible root may still be crossing the asynchronous publication boundary.
	// Settle that root before capturing destructive-maintenance authority so the
	// test isolates fallback-slot retention instead of racing coordinator epoch
	// advancement during RecoverableRootSet revalidation.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("settle rewrite publication after snapshot drain: %v", err)
	}

	drainedStats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: candidates,
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after snapshot drain: %v", err)
	}
	if drainedStats.SegmentsEligible != 1 || drainedStats.SegmentsDeleted != 0 {
		t.Fatalf("drained GC stats=%+v want old mixed segment retained by fallback durable generation", drainedStats)
	}
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("old segment removed before fallback generation advance: %v", err)
	}
	advanceColumnAssetDurableFallbackM15C(t, d)
	drainedStats, err = col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: candidates,
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after fallback advance: %v", err)
	}
	if drainedStats.SegmentsDeleted != 1 || drainedStats.BytesDeleted == 0 {
		t.Fatalf("drained GC stats=%+v want old mixed segment deleted after fallback advance", drainedStats)
	}
	if _, err := os.Stat(oldSegmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old segment still exists after snapshot drain or unexpected stat error: %v", err)
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
	if err := os.Mkdir(filepath.Join(namespace.SegmentDir, columnAssetSegmentFileName(99)), 0o700); err != nil {
		t.Fatalf("Mkdir unknown segment: %v", err)
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

func TestColumnAssetGCSegmentEligibleAcceptsUnreferencedCanonicalSegmentM15B(t *testing.T) {
	segmentDir := filepath.Join(t.TempDir(), "segments")
	entry := ColumnAssetReachabilitySegmentEntry{
		FileID:           9,
		Path:             filepath.Join(segmentDir, columnAssetSegmentFileName(9)),
		Bytes:            96,
		Status:           ColumnAssetReachabilitySegmentReclaimable,
		ReclaimableBytes: 96,
	}
	if !columnAssetGCSegmentEligibleForDelete(segmentDir, entry) {
		t.Fatalf("unreferenced canonical reclaimable segment was rejected: %+v", entry)
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

func TestColumnAssetGCNormalizeMaintenanceRaceErrorPreservesPlanningErrorM15B(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	planningErr := errors.New("planning failed")
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := col.columnAssetGCNormalizeMaintenanceRaceError(planningErr)
	if !errors.Is(err, planningErr) {
		t.Fatalf("normalized err=%v want planning error", err)
	}
	if !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("normalized err=%v want maintenance ErrClosed", err)
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

func BenchmarkColumnAssetGCDryRunTenKCandidatesOlderSnapshotM15C(b *testing.B) {
	const refs = 10_000
	const refBytes = 64
	d, col := prepareColumnAssetGCBenchmarkCollectionM15B(b)
	defer func() { _ = d.Close() }()
	payload := make([]byte, refBytes)
	candidates := make([]ColumnAssetRef, 0, refs)
	for i := 0; i < refs; i++ {
		payload[0] = byte(i)
		candidates = append(candidates, writeColumnAssetGCCandidateSegmentM15B(b, d.ColumnAssetRootDir(), col, uint32(i+2), payload))
	}
	pinned := d.AcquireSnapshot()
	if pinned == nil {
		b.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = pinned.Close() }()
	if _, err := col.Insert([]byte("live-after-pinned-snapshot"), []byte(`{"time_us":2,"kind":"post","did":"did_after"}`)); err != nil {
		b.Fatalf("Insert after pinned snapshot: %v", err)
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
		if !stats.Plan.Complete || stats.SegmentsEligible != 0 || stats.Plan.Refs.Reclaimable != 0 || stats.Plan.Sources.PinnedRefs != refs {
			b.Fatalf("unexpected pinned stats: complete=%t eligible=%d reclaimable_refs=%d pinned_refs=%d plan=%+v",
				stats.Plan.Complete,
				stats.SegmentsEligible,
				stats.Plan.Refs.Reclaimable,
				stats.Plan.Sources.PinnedRefs,
				stats.Plan.Segments,
			)
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
