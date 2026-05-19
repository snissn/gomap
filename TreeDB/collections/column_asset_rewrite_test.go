package collections

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestColumnAssetRewriteRemapsManifestRefsOutOfMixedSegmentM15C(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	dClosed := false
	defer func() {
		if !dClosed {
			_ = d.Close()
		}
	}()
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

	dry, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		DryRun:        true,
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite dry-run: %v", err)
	}
	if !dry.DryRun || dry.SegmentsEligible != 1 || dry.SegmentsRewritten != 0 || dry.RefsEligible != len(beforeRefs) {
		t.Fatalf("dry-run stats=%+v want one eligible mixed segment and %d eligible refs", dry, len(beforeRefs))
	}
	if dry.Plan.Segments.Mixed != 1 || dry.Plan.RewriteDebtBytes != candidate.Length {
		t.Fatalf("dry-run plan segments=%+v debt=%d want one mixed segment with candidate debt %d", dry.Plan.Segments, dry.Plan.RewriteDebtBytes, candidate.Length)
	}

	stats, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite: %v", err)
	}
	if stats.DryRun || stats.SegmentsEligible != 1 || stats.SegmentsRewritten != 1 || stats.RefsRemapped != len(beforeRefs) {
		t.Fatalf("stats=%+v want one rewritten segment and %d remapped refs", stats, len(beforeRefs))
	}
	if stats.BytesCopied <= 0 || stats.BytesReclaimable < candidate.Length {
		t.Fatalf("stats=%+v want copied live bytes and reclaimable candidate bytes >= %d", stats, candidate.Length)
	}
	if len(stats.SupersededRefs) != len(beforeRefs) || len(stats.RemappedRefs) != len(beforeRefs) {
		t.Fatalf("stats superseded=%d remapped=%d want %d", len(stats.SupersededRefs), len(stats.RemappedRefs), len(beforeRefs))
	}

	afterRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	assertColumnAssetRefsRemappedM15C(t, beforeRefs, afterRefs)
	for _, ref := range afterRefs {
		if _, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref); err != nil {
			t.Fatalf("remapped ref %+v unreadable: %v", ref, err)
		}
	}
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("rewrite removed old mixed segment before GC: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close after rewrite: %v", err)
	}
	dClosed = true
	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	reopenRefs := columnManifestAssetRefsForCollectionM12A(t, reopen, reopened)
	assertColumnAssetRefsEqualM15C(t, afterRefs, reopenRefs)
	diag, err := reopened.scanColumnPhysicalRows(columnPhysicalScanRequest{})
	if err != nil {
		t.Fatalf("scanColumnPhysicalRows after reopen: %v", err)
	}
	if diag.RowsScanned != 2 || diag.AssetRefs != len(afterRefs) {
		t.Fatalf("diag=%+v want 2 rows and %d remapped refs", diag, len(afterRefs))
	}

	gcStats, err := reopened.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: append(append([]ColumnAssetRef(nil), stats.SupersededRefs...), candidate),
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after rewrite: %v", err)
	}
	if gcStats.SegmentsDeleted != 1 || gcStats.BytesDeleted == 0 {
		t.Fatalf("gc stats=%+v want old mixed segment deleted after remap", gcStats)
	}
	if _, err := os.Stat(oldSegmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old segment still exists or unexpected stat error: %v", err)
	}
	for _, ref := range reopenRefs {
		if _, err := readColumnPhysicalAssetFromManager(reopen.ColumnAssetRootDir(), ref); err != nil {
			t.Fatalf("live remapped ref %+v unreadable after GC: %v", ref, err)
		}
	}
}

func TestColumnAssetRewriteLifecycleSmokeWithMutationsM15C(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	dClosed := false
	defer func() {
		if !dClosed {
			_ = d.Close()
		}
	}()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2","payload":"beta"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, modified, err := col.Update([]byte("e1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"time_us":3,"kind":"share","did":"d1","payload":"alpha2"}`), true, nil
	}); err != nil || !modified {
		t.Fatalf("Update modified=%t err=%v, want modified update", modified, err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("e2")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch deleted=%d err=%v, want one delete", deleted, err)
	}
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(beforeRefs) < 3 {
		t.Fatalf("manifest refs=%d want insert/update/delete assets", len(beforeRefs))
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 4, 99)
	if candidate.FileID != beforeRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires mixed segment", candidate.FileID, beforeRefs[0].FileID)
	}
	oldSegmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

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
	afterRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	assertColumnAssetRefsRemappedM15C(t, beforeRefs, afterRefs)
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("rewrite removed old mixed segment before GC: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close after rewrite: %v", err)
	}
	dClosed = true
	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	result, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupCount,
		GroupColumn: "kind",
	})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery after reopen: %v", err)
	}
	if got, want := columnPhysicalQueryLinesM13B("q1", result.Groups), []string{"q1:share=1"}; !equalStringSets(got, want) {
		t.Fatalf("query lines=%v want %v diagnostics=%+v", got, want, result.Diagnostics)
	}
	if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReconstructionRows != 0 {
		t.Fatalf("aggregate diagnostics materialized/reconstructed rows: %+v", result.Diagnostics)
	}
	if result.Diagnostics.ReduceRows != 1 || result.Diagnostics.DeletedRows != 1 {
		t.Fatalf("visibility diagnostics=%+v want one reduced live row and one tombstone", result.Diagnostics)
	}

	gcStats, err := reopened.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: append(append([]ColumnAssetRef(nil), rewrite.SupersededRefs...), candidate),
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after lifecycle rewrite: %v", err)
	}
	if gcStats.SegmentsDeleted != 1 || gcStats.BytesDeleted == 0 {
		t.Fatalf("gc stats=%+v want old mixed segment deleted after remap", gcStats)
	}
	if _, err := os.Stat(oldSegmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old segment still exists or unexpected stat error: %v", err)
	}
	afterGC, err := reopened.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind:        ColumnPhysicalQueryGroupCount,
		GroupColumn: "kind",
	})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery after GC: %v", err)
	}
	if got, want := columnPhysicalQueryLinesM13B("q1", afterGC.Groups), []string{"q1:share=1"}; !equalStringSets(got, want) {
		t.Fatalf("post-GC query lines=%v want %v diagnostics=%+v", got, want, afterGC.Diagnostics)
	}
}

func TestColumnAssetRewriteFailClosedOnIncompletePlanM15C(t *testing.T) {
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
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)

	dry, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		DryRun:        true,
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite dry-run: %v", err)
	}
	if !dry.DryRun || dry.Plan.Complete || dry.Plan.Segments.Unknown == 0 || dry.SegmentsRewritten != 0 {
		t.Fatalf("dry-run stats=%+v want incomplete plan and no rewrite", dry)
	}

	stats, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, ErrColumnAssetReachabilityIncomplete) {
		t.Fatalf("ColumnAssetRewrite error=%v want ErrColumnAssetReachabilityIncomplete", err)
	}
	if stats.SegmentsRewritten != 0 || stats.RefsRemapped != 0 || len(stats.SupersededRefs) != 0 {
		t.Fatalf("incomplete plan rewrote stats=%+v", stats)
	}
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	for _, ref := range refs {
		if ref.FileID != columnAssetM12ASegmentFileID {
			t.Fatalf("manifest ref %+v changed despite incomplete rewrite", ref)
		}
	}
}

func TestColumnAssetRewriteCleansCopiedSegmentOnStalePublishPreflightM15C(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	col := openColumnStoreCollectionM10B(t, d, mgr)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	beforeSegments := columnAssetSegmentNamesM15C(t, d, col)

	stats, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
		afterCopyHookForTest: func() error {
			_, _, err := d.PublishOrderedRootDeltaGroupWithPreflightMaintenanceSystemDeltaBuilder(nil, nil, func([]uint64) (iterator.UnsafeIterator, error) {
				current := d.AcquireSnapshot()
				if current == nil {
					return nil, backenddb.ErrClosed
				}
				defer func() { _ = current.Close() }()
				return buildSystemTargetIterator(current, map[string][]byte{
					systemCollectionRootKey(collectionColumnManifestRootName("events")): encodeRootID(0),
				})
			})
			return err
		},
	})
	if err == nil {
		t.Fatal("ColumnAssetRewrite stale publish unexpectedly succeeded")
	}
	if stats.SegmentsRewritten != 0 || stats.RefsRemapped != 0 || len(stats.SupersededRefs) != 0 {
		t.Fatalf("stale publish reported successful rewrite stats=%+v", stats)
	}
	if stats.BytesCopied != 0 {
		t.Fatalf("stale publish reported copied bytes stats=%+v", stats)
	}
	assertStringSlicesEqualM15C(t, beforeSegments, columnAssetSegmentNamesM15C(t, d, col))
}

func TestColumnAssetRewriteRejectsReadOnlyM15C(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	readonly, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open readonly: %v", err)
	}
	defer func() { _ = readonly.Close() }()
	readonlyCol := openColumnStoreCollectionM10B(t, readonly)
	stats, err := readonlyCol.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, backenddb.ErrReadOnly) {
		t.Fatalf("ColumnAssetRewrite read-only error=%v want ErrReadOnly", err)
	}
	if stats.SegmentsRewritten != 0 || stats.RefsRemapped != 0 {
		t.Fatalf("read-only rewrite stats=%+v", stats)
	}
	afterRefs := columnManifestAssetRefsForCollectionM12A(t, readonly, readonlyCol)
	assertColumnAssetRefsEqualM15C(t, beforeRefs, afterRefs)
}

func columnAssetSegmentNamesM15C(t testing.TB, d *backenddb.DB, col *Collection) []string {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		t.Fatalf("missing column-store asset manager config")
	}
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	entries, err := os.ReadDir(namespace.SegmentDir)
	if err != nil {
		t.Fatalf("ReadDir segment dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

func assertStringSlicesEqualM15C(t testing.TB, before, after []string) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("slice length after=%d want %d: after=%v before=%v", len(after), len(before), after, before)
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("slice[%d]=%q want %q: after=%v before=%v", i, after[i], before[i], after, before)
		}
	}
}

func assertColumnAssetRefsRemappedM15C(t testing.TB, before, after []ColumnAssetRef) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("ref count after=%d want before=%d", len(after), len(before))
	}
	matched := make([]bool, len(after))
	for _, oldRef := range before {
		found := false
		for i, newRef := range after {
			if matched[i] || !columnAssetRefsSameLogicalAssetM15C(oldRef, newRef) {
				continue
			}
			if oldRef.FileID == newRef.FileID && oldRef.Offset == newRef.Offset {
				t.Fatalf("ref %+v was not remapped", oldRef)
			}
			matched[i] = true
			found = true
			break
		}
		if !found {
			t.Fatalf("old ref %+v has no logically equivalent remapped ref in %+v", oldRef, after)
		}
	}
}

func assertColumnAssetRefsEqualM15C(t testing.TB, before, after []ColumnAssetRef) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("ref count after=%d want before=%d", len(after), len(before))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("ref[%d]=%+v want %+v", i, after[i], before[i])
		}
	}
}

func columnAssetRefsSameLogicalAssetM15C(left, right ColumnAssetRef) bool {
	return left.Kind == right.Kind &&
		left.Namespace == right.Namespace &&
		left.Generation == right.Generation &&
		left.PartID == right.PartID &&
		left.Length == right.Length &&
		left.Checksum == right.Checksum
}

func BenchmarkColumnAssetRewriteMixedSegmentM15C(b *testing.B) {
	for _, refs := range []int{1, 128} {
		b.Run(fmt.Sprintf("refs_%d", refs), func(b *testing.B) {
			bytesCopiedPerRun := columnAssetRewriteBenchmarkBytesPerRunM15C(b, refs)
			b.SetBytes(bytesCopiedPerRun)
			b.ReportAllocs()
			ctx := context.Background()
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				tc := prepareColumnAssetRewriteBenchmarkCaseM15C(b, refs)
				if tc.bytesCopied != bytesCopiedPerRun {
					if err := closeColumnAssetRewriteBenchmarkCaseM15C(tc); err != nil {
						b.Fatalf("close benchmark case after unstable bytes: %v", err)
					}
					b.Fatalf("bytesCopied=%d want stable benchmark bytes=%d", tc.bytesCopied, bytesCopiedPerRun)
				}
				b.StartTimer()
				stats, err := tc.col.ColumnAssetRewrite(ctx, ColumnAssetRewriteOptions{
					CandidateRefs: []ColumnAssetRef{tc.candidate},
				})
				b.StopTimer()
				if err != nil {
					b.Fatalf("ColumnAssetRewrite refs=%d: %v", refs, err)
				}
				if stats.RefsRemapped != len(tc.liveRefs) || stats.SegmentsRewritten != 1 || stats.BytesCopied != bytesCopiedPerRun {
					b.Fatalf("stats=%+v liveRefs=%d bytesCopied=%d", stats, len(tc.liveRefs), bytesCopiedPerRun)
				}
				if err := closeColumnAssetRewriteBenchmarkCaseM15C(tc); err != nil {
					b.Fatalf("close benchmark case: %v", err)
				}
			}
		})
	}
}

type columnAssetRewriteBenchmarkCaseM15C struct {
	d           *backenddb.DB
	col         *Collection
	candidate   ColumnAssetRef
	liveRefs    []ColumnAssetRef
	bytesCopied int64
}

func columnAssetRewriteBenchmarkBytesPerRunM15C(b *testing.B, refs int) int64 {
	b.Helper()
	tc := prepareColumnAssetRewriteBenchmarkCaseM15C(b, refs)
	defer func() {
		if err := closeColumnAssetRewriteBenchmarkCaseM15C(tc); err != nil {
			b.Fatalf("close benchmark probe: %v", err)
		}
	}()
	return tc.bytesCopied
}

func prepareColumnAssetRewriteBenchmarkCaseM15C(b testing.TB, refs int) columnAssetRewriteBenchmarkCaseM15C {
	b.Helper()
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(b)
	d := openCollectionCommandWALDB(b, dir)
	col := openColumnStoreCollectionM10B(b, d)
	for refIdx := 0; refIdx < refs; refIdx++ {
		id := []byte(fmt.Sprintf("e%06d", refIdx))
		doc := []byte(fmt.Sprintf(`{"time_us":%d,"kind":"like","did":"d%d"}`, refIdx, refIdx))
		if _, err := col.Insert(id, doc); err != nil {
			_ = d.Close()
			b.Fatalf("Insert ref=%d: %v", refIdx, err)
		}
	}
	liveRefs := columnManifestAssetRefsForCollectionM12A(b, d, col)
	candidate := writeColumnAssetReachabilityCandidateM15A(b, d, col, uint64(refs+2), 99)
	var bytesCopied int64
	for _, ref := range liveRefs {
		bytesCopied += ref.Length
	}
	if bytesCopied <= 0 {
		_ = d.Close()
		b.Fatalf("benchmark bytesCopied=%d for refs=%d", bytesCopied, refs)
	}
	return columnAssetRewriteBenchmarkCaseM15C{
		d:           d,
		col:         col,
		candidate:   candidate,
		liveRefs:    liveRefs,
		bytesCopied: bytesCopied,
	}
}

func closeColumnAssetRewriteBenchmarkCaseM15C(tc columnAssetRewriteBenchmarkCaseM15C) error {
	if tc.d == nil {
		return nil
	}
	return tc.d.Close()
}

func closeColumnAssetRewriteBenchmarkCasesM15C(b testing.TB, cases []columnAssetRewriteBenchmarkCaseM15C) {
	b.Helper()
	for i := range cases {
		if err := closeColumnAssetRewriteBenchmarkCaseM15C(cases[i]); err != nil {
			b.Fatalf("Close benchmark DB: %v", err)
		}
		cases[i].d = nil
	}
}
