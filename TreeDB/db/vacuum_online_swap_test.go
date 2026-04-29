package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func collectLeafRefIDsFromRoot(t *testing.T, d *DB, rootID uint64) map[page.LeafLogPtr]struct{} {
	t.Helper()
	out := make(map[page.LeafLogPtr]struct{})
	if d == nil || rootID == 0 {
		return out
	}
	err := leafrefscan.Walk(context.Background(), rootID, d.Pager().Get, func(pageID uint64, n node.Node) error {
		if !n.VerifyChecksum() {
			return fmt.Errorf("checksum mismatch on page %d", pageID)
		}
		return nil
	}, func(ptr page.LeafLogPtr) error {
		out[ptr] = struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("collect leaf refs: %v", err)
	}
	return out
}

type countingLeafPageLog struct {
	inner   LeafPageLog
	appends atomic.Uint64
}

func (l *countingLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	l.appends.Add(1)
	return l.inner.AppendLeafPage(leafPage)
}

func (l *countingLeafPageLog) Flush() error { return l.inner.Flush() }
func (l *countingLeafPageLog) Sync() error  { return l.inner.Sync() }

func TestVacuumIndexOnline_ShrinksAndPreservesData(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:               dir,
		ChunkSize:         chunkSize,
		KeepRecent:        1,
		PreferAppendAlloc: true, // intentionally bloat index.db under churn
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	value := bytes.Repeat([]byte("v"), 200) // inline-ish to force page pressure
	for round := 0; round < 6; round++ {
		b := d.NewBatch()
		for i := 0; i < 4000; i++ {
			k := []byte(fmt.Sprintf("k%06d", i))
			if err := b.Set(k, value); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	indexPath := filepath.Join(dir, indexFileName)
	beforeInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	afterInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if afterInfo.Size() >= beforeInfo.Size() {
		t.Fatalf("expected vacuum to shrink index.db: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}

	got, err := d.Get([]byte("k000010"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch")
	}
}

func TestVacuumIndexOnline_OuterLeavesInValueLog_DoesNotRewriteLeafPages(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                        dir,
		KeepRecent:                 1,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		PreferAppendAlloc:          true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	baseLeafLog := &registeredLeafPageLog{db: d, dir: dir}
	if err := baseLeafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	leafLog := &countingLeafPageLog{inner: baseLeafLog}
	d.SetLeafPageLog(leafLog)

	val := bytes.Repeat([]byte("v"), 64)
	for version := 1; version <= 24; version++ {
		b := d.NewBatch()
		for i := 0; i < 256; i++ {
			key := []byte(fmt.Sprintf("s/k:store/n/%08d/%08d", version, i))
			val[0] = byte(version)
			if err := b.Set(key, val); err != nil {
				t.Fatalf("set version=%d key=%d: %v", version, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("writesync version=%d: %v", version, err)
		}
		_ = b.Close()
	}

	stBefore := d.State()
	if stBefore == nil {
		t.Fatalf("missing state before vacuum")
	}
	beforeRefs := collectLeafRefIDsFromRoot(t, d, stBefore.RootPageID)
	if len(beforeRefs) == 0 {
		t.Fatalf("expected outer-leaf refs before vacuum")
	}

	// Reset after initial population; we only want to observe what vacuum does.
	leafLog.appends.Store(0)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if got := leafLog.appends.Load(); got != 0 {
		t.Fatalf("vacuum rewrote outer leaf pages: leaf_page_appends=%d want 0", got)
	}

	stAfter := d.State()
	if stAfter == nil {
		t.Fatalf("missing state after vacuum")
	}
	afterRefs := collectLeafRefIDsFromRoot(t, d, stAfter.RootPageID)
	if len(afterRefs) == 0 {
		t.Fatalf("expected outer-leaf refs after vacuum")
	}
	if len(afterRefs) != len(beforeRefs) {
		t.Fatalf("leafref count changed across vacuum: before=%d after=%d", len(beforeRefs), len(afterRefs))
	}

	for _, version := range []int{1, 12, 24} {
		for _, idx := range []int{0, 127, 255} {
			key := []byte(fmt.Sprintf("s/k:store/n/%08d/%08d", version, idx))
			got, err := d.Get(key)
			if err != nil {
				t.Fatalf("get version=%d key=%d after vacuum: %v", version, idx, err)
			}
			if len(got) != len(val) {
				t.Fatalf("value length mismatch version=%d key=%d: got=%d want=%d", version, idx, len(got), len(val))
			}
			if got[0] != byte(version) {
				t.Fatalf("value content mismatch version=%d key=%d: got[0]=%d want=%d", version, idx, got[0], byte(version))
			}
		}
	}
}

func TestVacuumIndexOnline_AllowsSnapshotAcrossSwap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:        dir,
		ChunkSize:  chunkSize,
		KeepRecent: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.SetSync([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("set v1: %v", err)
	}

	snap := d.AcquireSnapshot()

	if err := d.SetSync([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("set v2: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	// DB reads see the latest value.
	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("expected v2, got %q", got)
	}

	// Old snapshot remains valid and sees the older value.
	old, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snap get: %v", err)
	}
	if string(old) != "v1" {
		t.Fatalf("expected v1 from snapshot, got %q", old)
	}

	d.idxMu.Lock()
	genCount := len(d.idxAll)
	d.idxMu.Unlock()
	if genCount < 2 {
		t.Fatalf("expected at least 2 index generations after vacuum, got %d", genCount)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("snap close: %v", err)
	}

	d.idxMu.Lock()
	genCountAfter := len(d.idxAll)
	d.idxMu.Unlock()
	if genCountAfter != 1 {
		t.Fatalf("expected old index generation to be released after snapshot close; gens=%d", genCountAfter)
	}
}

func TestVacuumIndexOnline_RepeatWhileSnapshotPinned(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:        dir,
		ChunkSize:  chunkSize,
		KeepRecent: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.SetSync([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("set v1: %v", err)
	}

	snap := d.AcquireSnapshot()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := d.SetSync([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("set v2: %v", err)
	}
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum 1: %v", err)
	}

	d.idxMu.Lock()
	genCount1 := len(d.idxAll)
	d.idxMu.Unlock()
	if genCount1 != 2 {
		t.Fatalf("expected 2 index generations after first vacuum, got %d", genCount1)
	}

	if err := d.SetSync([]byte("k"), []byte("v3")); err != nil {
		t.Fatalf("set v3: %v", err)
	}
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum 2: %v", err)
	}

	// DB reads see the latest value.
	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "v3" {
		t.Fatalf("expected v3, got %q", got)
	}

	// Old snapshot remains valid and sees the older value.
	old, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snap get: %v", err)
	}
	if string(old) != "v1" {
		t.Fatalf("expected v1 from snapshot, got %q", old)
	}

	d.idxMu.Lock()
	genCount2 := len(d.idxAll)
	d.idxMu.Unlock()
	if genCount2 < 2 {
		t.Fatalf("expected at least 2 index generations after second vacuum, got %d", genCount2)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("snap close: %v", err)
	}

	d.idxMu.Lock()
	genCountAfter := len(d.idxAll)
	d.idxMu.Unlock()
	if genCountAfter != 1 {
		t.Fatalf("expected old index generations to be released after snapshot close; gens=%d", genCountAfter)
	}
}

func TestVacuumIndexOnline_RebuildsPackedInternalTreeForLeafRefs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                        dir,
		KeepRecent:                 1,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		PreferAppendAlloc:          true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()
	leafLog := &registeredLeafPageLog{db: d, dir: dir}
	if err := leafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	d.SetLeafPageLog(leafLog)

	val := bytes.Repeat([]byte("v"), 64)
	const (
		stores   = 12
		versions = 120
		keys     = 48
	)

	for version := 1; version <= versions; version++ {
		for store := 0; store < stores; store++ {
			b := d.NewBatch()
			for i := 0; i < keys; i++ {
				key := []byte(fmt.Sprintf("s/k:store%02d/n/%08d/%08d", store, version, i))
				val[0] = byte(version)
				val[1] = byte(store)
				if err := b.Set(key, val); err != nil {
					t.Fatalf("set version=%d store=%d key=%d: %v", version, store, i, err)
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatalf("writesync version=%d store=%d: %v", version, store, err)
			}
			_ = b.Close()
		}
	}

	parse := func(report map[string]string, key string) uint64 {
		t.Helper()
		v, err := strconv.ParseUint(report[key], 10, 64)
		if err != nil {
			t.Fatalf("parse %s=%q: %v", key, report[key], err)
		}
		return v
	}

	before, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport(before): %v", err)
	}
	beforeP50 := parse(before, "treedb.user.internal_fill_ppm_p50")
	beforeAvg := parse(before, "treedb.user.internal_fill_ppm_avg")
	beforeSpanRatio := parse(before, "treedb.user.pages.span_ratio_ppm")
	beforePagesTotal := parse(before, "treedb.pages.total")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	after, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport(after): %v", err)
	}
	afterP50 := parse(after, "treedb.user.internal_fill_ppm_p50")
	afterAvg := parse(after, "treedb.user.internal_fill_ppm_avg")

	afterSpanRatio := parse(after, "treedb.user.pages.span_ratio_ppm")
	afterPagesTotal := parse(after, "treedb.pages.total")
	if afterSpanRatio >= beforeSpanRatio {
		t.Fatalf("expected vacuum to compact internal page span: before=%d after=%d before=%v after=%v", beforeSpanRatio, afterSpanRatio, before, after)
	}
	if afterPagesTotal >= beforePagesTotal {
		t.Fatalf("expected vacuum to reduce total page count: before=%d after=%d before=%v after=%v", beforePagesTotal, afterPagesTotal, before, after)
	}
	if afterP50 < beforeP50 || afterAvg < beforeAvg {
		t.Fatalf("expected vacuum to preserve internal fill while compacting span: beforeP50=%d afterP50=%d beforeAvg=%d afterAvg=%d before=%v after=%v", beforeP50, afterP50, beforeAvg, afterAvg, before, after)
	}
}

func TestVacuumIndexOnline_PreservesOuterLeafRefsAndDataWhenOuterLeavesInValueLog(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                        dir,
		KeepRecent:                 1,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		PreferAppendAlloc:          true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()
	leafLog := &registeredLeafPageLog{db: d, dir: dir}
	if err := leafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	d.SetLeafPageLog(leafLog)

	val := bytes.Repeat([]byte("v"), 64)
	for version := 1; version <= 48; version++ {
		b := d.NewBatch()
		for i := 0; i < 512; i++ {
			key := []byte(fmt.Sprintf("s/k:store/n/%08d/%08d", version, i))
			val[0] = byte(version)
			if err := b.Set(key, val); err != nil {
				t.Fatalf("set version=%d key=%d: %v", version, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("writesync version=%d: %v", version, err)
		}
		_ = b.Close()
	}

	stateBefore := d.State()
	if stateBefore == nil {
		t.Fatalf("missing state before vacuum")
	}
	beforeRefs := collectLeafRefIDsFromRoot(t, d, stateBefore.RootPageID)
	if len(beforeRefs) == 0 {
		t.Fatalf("expected outer-leaf refs before vacuum")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	stateAfter := d.State()
	if stateAfter == nil {
		t.Fatalf("missing state after vacuum")
	}
	afterRefs := collectLeafRefIDsFromRoot(t, d, stateAfter.RootPageID)
	if len(afterRefs) == 0 {
		t.Fatalf("expected outer-leaf refs after vacuum")
	}
	if len(afterRefs) != len(beforeRefs) {
		t.Fatalf("leafref count changed across vacuum: before=%d after=%d", len(beforeRefs), len(afterRefs))
	}

	for _, version := range []int{1, 24, 48} {
		for _, idx := range []int{0, 127, 511} {
			key := []byte(fmt.Sprintf("s/k:store/n/%08d/%08d", version, idx))
			got, err := d.Get(key)
			if err != nil {
				t.Fatalf("get version=%d key=%d after vacuum: %v", version, idx, err)
			}
			if len(got) != len(val) {
				t.Fatalf("value length mismatch version=%d key=%d: got=%d want=%d", version, idx, len(got), len(val))
			}
			if got[0] != byte(version) {
				t.Fatalf("value content mismatch version=%d key=%d: got[0]=%d want=%d", version, idx, got[0], byte(version))
			}
		}
	}
}
