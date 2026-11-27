package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"os"
	"strconv"
	"testing"
)

func TestIndexVacuumTriggerReportContextCanceledBeforeProbe(t *testing.T) {
	d := openFragmentationTriggerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.IndexVacuumTriggerReportContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("trigger report error=%v want context canceled", err)
	}
}

func TestIndexVacuumTriggerReportContextCancelsCollectionWalk(t *testing.T) {
	d := openFragmentationTriggerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	restore := SetFragmentationProbeHookForTest(func(event FragmentationProbeEvent) {
		if event == FragmentationProbeEventTriggerCollectionRootWalk {
			cancel()
		}
	})
	defer restore()
	if _, err := d.IndexVacuumTriggerReportContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("trigger report error=%v want context canceled", err)
	}
}

func TestIndexVacuumTriggerReportMatchesFragmentationSpan(t *testing.T) {
	d := openFragmentationTriggerFixture(t)

	compareTriggerToFullFragmentationReport(t, d, "initial")

	for i := 0; i < 512; i++ {
		key := []byte(fmt.Sprintf("churn-%04d", i))
		if err := d.Set(key, bytes.Repeat([]byte{byte(i)}, 32)); err != nil {
			t.Fatalf("set churn: %v", err)
		}
	}
	for i := 0; i < 512; i += 2 {
		key := []byte(fmt.Sprintf("churn-%04d", i))
		if err := d.Delete(key); err != nil {
			t.Fatalf("delete churn: %v", err)
		}
	}
	for i := 0; i < 512; i += 2 {
		key := []byte(fmt.Sprintf("churn-%04d", i))
		if err := d.Set(key, bytes.Repeat([]byte("z"), 32)); err != nil {
			t.Fatalf("set churn2: %v", err)
		}
	}

	compareTriggerToFullFragmentationReport(t, d, "after churn")
}

func compareTriggerToFullFragmentationReport(t *testing.T, d *DB, label string) {
	t.Helper()

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("%s checkpoint: %v", label, err)
	}
	settled := d.State()
	if settled == nil {
		t.Fatalf("%s missing settled state", label)
	}
	settledCommitSeq := settled.CommitSeq

	full, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("%s FragmentationReport: %v", label, err)
	}

	counts := make(map[FragmentationProbeEvent]int)
	restore := SetFragmentationProbeHookForTest(func(event FragmentationProbeEvent) {
		counts[event]++
	})
	trigger, err := d.IndexVacuumTriggerReport()
	restore()
	if err != nil {
		t.Fatalf("%s IndexVacuumTriggerReport: %v", label, err)
	}

	if got := counts[FragmentationProbeEventTriggerReport]; got != 1 {
		t.Fatalf("%s trigger reports=%d want 1", label, got)
	}
	if got := counts[FragmentationProbeEventTriggerUserTreeWalk]; got != 1 {
		t.Fatalf("%s trigger user-tree walks=%d want 1", label, got)
	}
	if got := counts[FragmentationProbeEventTriggerFreelistCounters]; got != 1 {
		t.Fatalf("%s trigger freelist counter reads=%d want 1", label, got)
	}
	if got := counts[FragmentationProbeEventTriggerCollectionRootWalk]; got != 1 {
		t.Fatalf("%s trigger collection-root walks=%d want 1", label, got)
	}
	if got := counts[FragmentationProbeEventFullReport]; got != 0 {
		t.Fatalf("%s full fragmentation reports during trigger=%d want 0", label, got)
	}
	if got := counts[FragmentationProbeEventFullUserTreeWalk]; got != 0 {
		t.Fatalf("%s full user-tree walks during trigger=%d want 0", label, got)
	}
	if got := counts[FragmentationProbeEventFullFreelistChainWalk]; got != 0 {
		t.Fatalf("%s full freelist-chain walks during trigger=%d want 0", label, got)
	}
	if got := counts[FragmentationProbeEventFullCollectionRootWalk]; got != 0 {
		t.Fatalf("%s full collection-root walks during trigger=%d want 0", label, got)
	}

	state := d.State()
	if state == nil {
		t.Fatalf("%s missing state", label)
	}
	if state.CommitSeq != settledCommitSeq {
		t.Fatalf("%s state CommitSeq=%d want settled %d", label, state.CommitSeq, settledCommitSeq)
	}
	if trigger.CommitSeq != state.CommitSeq {
		t.Fatalf("%s CommitSeq=%d want %d", label, trigger.CommitSeq, state.CommitSeq)
	}

	assertTriggerFieldMatchesFullReport(t, label, "treedb.pages.total", trigger.TotalPages, full)
	assertTriggerFieldMatchesFullReport(t, label, "treedb.user.pages", trigger.UserPages, full)
	if trigger.UserPages > 0 {
		assertTriggerFieldMatchesFullReport(t, label, "treedb.user.pages.min", trigger.UserMinPageID, full)
		assertTriggerFieldMatchesFullReport(t, label, "treedb.user.pages.max", trigger.UserMaxPageID, full)
		assertTriggerFieldMatchesFullReport(t, label, "treedb.user.pages.span", trigger.UserSpan, full)
		assertTriggerFieldMatchesFullReport(t, label, "treedb.user.pages.span_ratio_ppm", trigger.UserSpanRatioPPM, full)
	}

	idx := d.idx.Load()
	if idx == nil || idx.allocator == nil {
		t.Fatalf("%s missing allocator", label)
	}
	counters := idx.allocator.Counters()
	if trigger.FreelistHead != counters.Head ||
		trigger.FreelistPages != counters.Pages ||
		trigger.FreelistFreeIDs != counters.FreeIDs ||
		trigger.FreelistAllocPagesTotal != counters.AllocPages ||
		trigger.FreelistAppendAllocPagesTotal != counters.AppendAllocPages ||
		trigger.FreelistReuseAllocPagesTotal != counters.ReuseAllocPages ||
		trigger.FreelistFreePagesTotal != counters.FreePages {
		t.Fatalf("%s freelist counters mismatch: trigger={head:%d pages:%d free_ids:%d alloc:%d append:%d reuse:%d free:%d} counters={head:%d pages:%d free_ids:%d alloc:%d append:%d reuse:%d free:%d}",
			label,
			trigger.FreelistHead, trigger.FreelistPages, trigger.FreelistFreeIDs, trigger.FreelistAllocPagesTotal, trigger.FreelistAppendAllocPagesTotal, trigger.FreelistReuseAllocPagesTotal, trigger.FreelistFreePagesTotal,
			counters.Head, counters.Pages, counters.FreeIDs, counters.AllocPages, counters.AppendAllocPages, counters.ReuseAllocPages, counters.FreePages)
	}
	if trigger.FreelistReclaimableValid {
		if got, want := trigger.FreelistReclaimablePages, counters.ReclaimablePages(); got != want {
			t.Fatalf("%s trigger freelist reclaimable pages=%d want allocator counters %d", label, got, want)
		}
		if trigger.TotalPages > 0 {
			wantRatio := (trigger.FreelistReclaimablePages * 1_000_000) / trigger.TotalPages
			if got := trigger.FreelistReclaimableRatioPPM; got != wantRatio {
				t.Fatalf("%s trigger freelist reclaimable ratio=%d want %d", label, got, wantRatio)
			}
		}
	}
	if trigger.CollectionRootSpanRatioValid {
		assertTriggerFieldMatchesFullReport(t, label, "treedb.collection_roots.pages", trigger.CollectionRootPages, full)
		assertTriggerFieldMatchesFullReport(t, label, "treedb.collection_roots.pages.span", trigger.CollectionRootSpan, full)
		assertTriggerFieldMatchesFullReport(t, label, "treedb.collection_roots.pages.span_ratio_ppm", trigger.CollectionRootSpanRatioPPM, full)
	}
}

func assertTriggerFieldMatchesFullReport(t *testing.T, label, key string, got uint64, full map[string]string) {
	t.Helper()
	want, err := strconv.ParseUint(full[key], 10, 64)
	if err != nil {
		t.Fatalf("%s parse %s=%q: %v", label, key, full[key], err)
	}
	if got != want {
		t.Fatalf("%s %s=%d want %d", label, key, got, want)
	}
}

func BenchmarkBackgroundIndexVacuumTrigger(b *testing.B) {
	d := openFragmentationTriggerFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.IndexVacuumTriggerReport(); err != nil {
			b.Fatalf("IndexVacuumTriggerReport: %v", err)
		}
	}
}

func BenchmarkFragmentationReport(b *testing.B) {
	d := openFragmentationTriggerFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.FragmentationReport(); err != nil {
			b.Fatalf("FragmentationReport: %v", err)
		}
	}
}

func openFragmentationTriggerFixture(tb testing.TB) *DB {
	tb.Helper()
	d, err := Open(Options{Dir: tb.TempDir(), KeepRecent: 1})
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	tb.Cleanup(func() { _ = d.Close() })

	value := bytes.Repeat([]byte("v"), 64)
	for i := 0; i < 2048; i++ {
		key := []byte(fmt.Sprintf("bench-%04d", i))
		if err := d.Set(key, value); err != nil {
			tb.Fatalf("set fixture: %v", err)
		}
	}
	for i := 0; i < 2048; i += 3 {
		key := []byte(fmt.Sprintf("bench-%04d", i))
		if err := d.Delete(key); err != nil {
			tb.Fatalf("delete fixture: %v", err)
		}
	}
	for i := 0; i < 2048; i += 5 {
		key := []byte(fmt.Sprintf("bench-%04d", i))
		if err := d.Set(key, bytes.Repeat([]byte("w"), 64)); err != nil {
			tb.Fatalf("set fixture2: %v", err)
		}
	}
	return d
}

func TestPrimaryFragmentationCensusKeepsPhysicalDomainsDistinct(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, IndexPrimaryDirectory: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, key := range []string{"a", "b", "c"} {
		if err := d.SetSync([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	snap := d.AcquireSnapshot()
	defer snap.Close()
	var dataPages, dataMin, dataMax, primaryPages, readRoots uint64
	if err := snap.tree.WalkPages(func(id uint64, _ node.Node) error {
		if id >= page.PrimaryBankNamespace {
			local := id - page.PrimaryBankNamespace
			if local >= pager.PrimaryReadRootLocalBaseV6 {
				readRoots++
			} else {
				primaryPages++
			}
			return nil
		}
		if dataPages == 0 {
			dataMin, dataMax = id, id
		}
		if id < dataMin {
			dataMin = id
		}
		if id > dataMax {
			dataMax = id
		}
		dataPages++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if dataPages == 0 || primaryPages == 0 || readRoots == 0 {
		t.Fatalf("fixture must traverse DATA, physical PRIMARY and private root: data=%d primary=%d roots=%d", dataPages, primaryPages, readRoots)
	}
	report, err := d.IndexVacuumTriggerReport()
	if err != nil {
		t.Fatal(err)
	}
	if report.UserPages != dataPages || report.UserMinPageID != dataMin || report.UserMaxPageID != dataMax || report.UserSpan != dataMax-dataMin+1 || report.TotalPages != snap.idx.pager.PageCount() {
		t.Fatalf("DATA debt mixed domains: %+v; expected pages=%d bounds=[%d,%d]", report, dataPages, dataMin, dataMax)
	}
	full, err := d.FragmentationReport()
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateFragmentationReport(full); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]uint64{"treedb.user.primary.pages": primaryPages, "treedb.user.primary.read_roots": readRoots, "treedb.primary.pages.total": snap.idx.pager.PrimaryBankPageCount()} {
		if full[key] != strconv.FormatUint(want, 10) {
			t.Fatalf("%s=%q expected %d", key, full[key], want)
		}
	}
	for key, p := range map[string]*pager.Pager{"treedb.index.file.bytes": snap.idx.pager, "treedb.primary.file.bytes": snap.idx.primary.Pager()} {
		if err := p.WithStableResourceFile(func(file *os.File) error {
			info, err := file.Stat()
			if err != nil {
				return err
			}
			if full[key] != strconv.FormatInt(info.Size(), 10) {
				t.Errorf("%s=%q expected actual original file bytes%d", key, full[key], info.Size())
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	compareTriggerToFullFragmentationReport(t, d, "mixed physical domains")
}

func TestFragmentationSpanRatioRejectsInvalidAndPreservesWideArithmetic(t *testing.T) {
	if value, err := fragmentationSpanRatio(^uint64(0), 1_000_000); err != nil || value != ^uint64(0) {
		t.Fatalf("wide ratio=%d err=%v", value, err)
	}
	for _, pair := range [][2]uint64{{1, 0}, {1, 2}, {^uint64(0), 1}} {
		if _, err := fragmentationSpanRatio(pair[0], pair[1]); err == nil {
			t.Fatalf("invalid ratio accepted: %v", pair)
		}
	}
	if value, err := fragmentationSpanRatio(0, 0); err != nil || value != 0 {
		t.Fatalf("empty ratio=%d err=%v", value, err)
	}
}

func TestIndexVacuumTriggerReportPropagatesSystemTreeWalkError(t *testing.T) {
	d, err := Open(Options{
		Dir:                        t.TempDir(),
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: false,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	// Build a multi-leaf system tree via real publication containing:
	//  - a valid collection root descriptor under "collections/root/..."
	//  - enough keys after "collections/root0" (outside the descriptor prefix)
	//    so the system tree spans multiple leaves and has a trailing leaf
	//    ("zzz/...") that descriptor scans never touch.
	gen := d.idx.Load()
	p := gen.pager

	colLeafID, err := gen.allocator.Alloc(0)
	if err != nil {
		t.Fatalf("alloc col leaf: %v", err)
	}
	colLeafBuf, err := p.GetForWrite(colLeafID)
	if err != nil {
		t.Fatalf("get col leaf: %v", err)
	}
	cb := node.NewBuilder(colLeafBuf, page.PageTypeLeaf)
	cb.SetPageID(colLeafID)
	if err := cb.AddLeafEntry([]byte("k1"), []byte("v1"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("add col leaf entry: %v", err)
	}
	cb.Finish()

	if _, _, err := d.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		mt, err := memtable.NewWithCapacityMode(64*1024, memtable.ModeHashSorted)
		if err != nil {
			return nil, err
		}
		mt.Set([]byte("collections/root/corrupt_sys_test/primary"), encodeCollectionRootDescriptorRootID(colLeafID))
		filler := bytes.Repeat([]byte("X"), 500)
		for i := 0; i < 32; i++ {
			mt.Set([]byte(fmt.Sprintf("collections/zzzz/%03d", i)), filler)
		}
		for i := 0; i < 32; i++ {
			mt.Set([]byte(fmt.Sprintf("zzz/non_descriptor/%03d", i)), filler)
		}
		mt.Freeze()
		return mt.NewIterator(nil, nil), nil
	}); err != nil {
		t.Fatalf("PublishOrderedRootGroupWithSystemBuilder: %v", err)
	}

	beforeRep, err := d.IndexVacuumTriggerReportContext(context.Background())
	if err != nil {
		t.Fatalf("healthy IndexVacuumTriggerReportContext: %v", err)
	}
	if beforeRep.SystemPages < 4 || beforeRep.CollectionRootCount != 1 {
		t.Fatalf("expected multi-leaf healthy system tree: SystemPages=%d CollectionRootCount=%d",
			beforeRep.SystemPages, beforeRep.CollectionRootCount)
	}

	// Find the trailing non-descriptor leaf ("zzz/...") in the published system
	// tree and corrupt its page type in place.
	state := d.State()
	sysRootID := state.SystemRootPageID
	var badLeafID uint64
	if err := tree.New(p, d.valueLogManager, sysRootID).WalkPages(func(pageID uint64, n node.Node) error {
		if n.Type() != page.PageTypeLeaf || n.Count() == 0 {
			return nil
		}
		k, _, _, _, err := n.GetLeafEntryView(0)
		if err != nil {
			return err
		}
		if bytes.HasPrefix(k, []byte("zzz/")) {
			badLeafID = pageID
		}
		return nil
	}); err != nil {
		t.Fatalf("walk healthy system tree: %v", err)
	}
	if badLeafID == 0 {
		t.Fatalf("failed to locate trailing non-descriptor leaf in system tree")
	}

	badLeafBuf, err := p.GetForWrite(badLeafID)
	if err != nil {
		t.Fatalf("get bad leaf %d: %v", badLeafID, err)
	}
	badNode := node.NewNode(badLeafBuf)
	badNode.SetType(page.PageType(0xff))
	badNode.UpdateChecksum()

	// Sanity check: descriptor scan alone still succeeds because it only scans
	// collections/root* prefixes and never loads badLeafID.
	descs, err := vacuumCollectCollectionRootDescriptors(p, d.valueLogManager, sysRootID)
	if err != nil || len(descs) != 1 {
		t.Fatalf("expected descriptor scan to succeed before full walk: descs=%+v err=%v", descs, err)
	}

	// IndexVacuumTriggerReportContext MUST propagate the system-tree walk error
	// instead of succeeding with zeroed SystemPages and collection counts.
	rep, err := d.IndexVacuumTriggerReportContext(context.Background())
	if err == nil {
		t.Fatalf("IndexVacuumTriggerReportContext unexpectedly succeeded on degraded system tree: report=%+v", rep)
	}
}
