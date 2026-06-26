package db

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func readOnlyPrepareDirSize(tb testing.TB, path string) int64 {
	tb.Helper()
	var total int64
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		// Missing dirs are equivalent to zero bytes for this side-effect check.
		if errors.Is(err, fs.ErrNotExist) {
			return 0
		}
		tb.Fatalf("walk dir %q: %v", path, err)
	}
	return total
}

func readOnlyPrepareRootSeq(d *DB) (root, seq uint64) {
	if d == nil {
		return 0, 0
	}
	d.mu.RLock()
	root = d.meta.UserRootPageID
	seq = d.meta.CommitSeq
	d.mu.RUnlock()
	return root, seq
}

func TestDBPrepareReadOnlyApplyPlanSideEffectFreeAndStats(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, FlushAdmissionPolicy: FlushAdmissionPolicyExplicit})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	seed := d.NewBatch().(*Batch)
	for i := 0; i < 384; i++ {
		if err := seed.Set([]byte(fmt.Sprintf("key-%06d", i)), []byte("value")); err != nil {
			t.Fatalf("seed set: %v", err)
		}
	}
	if err := seed.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = seed.Close()

	idx := d.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}
	beforeRoot, beforeSeq := readOnlyPrepareRootSeq(d)
	beforePages := idx.pager.PageCount()
	beforeGraveyard := idx.graveyard.Stats()
	beforeLeafBytes := readOnlyPrepareDirSize(t, LeafLogDirPath(dir))
	beforeValueBytes := readOnlyPrepareDirSize(t, ValueLogDirPath(dir))

	delta := d.NewBatch().(*Batch)
	defer func() { _ = delta.Close() }()
	if err := delta.Set([]byte("key-000001"), []byte("new")); err != nil {
		t.Fatalf("delta set: %v", err)
	}
	if err := delta.DeleteRange([]byte("key-000120"), []byte("key-000220")); err != nil {
		t.Fatalf("delta range: %v", err)
	}
	plan, err := d.PrepareReadOnlyApplyPlan(delta, ReadOnlyApplyPlanOptions{Workers: 4})
	if err != nil {
		t.Fatalf("PrepareReadOnlyApplyPlan: %v", err)
	}
	if err := plan.Prepare.ValidateLeafSpans(); err != nil {
		t.Fatalf("ValidateLeafSpans: %v", err)
	}
	if plan.Prepare.RootID != beforeRoot || plan.Prepare.Ops != 2 || len(plan.Prepare.LeafSpans) == 0 || len(plan.WorkerRanges) == 0 {
		t.Fatalf("plan root/ops/spans/ranges=%d/%d/%d/%d want %d/2/>0/>0", plan.Prepare.RootID, plan.Prepare.Ops, len(plan.Prepare.LeafSpans), len(plan.WorkerRanges), beforeRoot)
	}
	afterRoot, afterSeq := readOnlyPrepareRootSeq(d)
	if afterRoot != beforeRoot || afterSeq != beforeSeq {
		t.Fatalf("read-only prepare changed root/seq: before %d/%d after %d/%d", beforeRoot, beforeSeq, afterRoot, afterSeq)
	}
	if got := idx.pager.PageCount(); got != beforePages {
		t.Fatalf("read-only prepare allocated pages: got %d want %d", got, beforePages)
	}
	if after := idx.graveyard.Stats(); after.Pages != beforeGraveyard.Pages || after.Batches != beforeGraveyard.Batches {
		t.Fatalf("read-only prepare changed graveyard: before=%+v after=%+v", beforeGraveyard, after)
	}
	if got := readOnlyPrepareDirSize(t, LeafLogDirPath(dir)); got != beforeLeafBytes {
		t.Fatalf("read-only prepare changed leaf_vlog bytes: got %d want %d", got, beforeLeafBytes)
	}
	if got := readOnlyPrepareDirSize(t, ValueLogDirPath(dir)); got != beforeValueBytes {
		t.Fatalf("read-only prepare changed value_vlog bytes: got %d want %d", got, beforeValueBytes)
	}

	stats := d.Stats()
	if got := stats["treedb.flush_apply.read_only_prepare.calls_total"]; got != "1" {
		t.Fatalf("calls stat=%q want 1", got)
	}
	if got := stats["treedb.flush_apply.read_only_prepare.requested_workers_total"]; got != "4" {
		t.Fatalf("requested workers stat=%q want 4", got)
	}
	if got := stats["treedb.flush_apply.read_only_prepare.validation_failures_total"]; got != "0" {
		t.Fatalf("validation failures stat=%q want 0", got)
	}
	if got := stats["treedb.flush_apply.read_only_prepare.spans_total"]; got == "" || got == "0" {
		t.Fatalf("spans stat=%q want non-zero", got)
	}
	if got := stats["treedb.flush_apply.read_only_prepare.span_ops_total"]; got == "" || got == "0" {
		t.Fatalf("span ops stat=%q want non-zero", got)
	}
	if got := stats["treedb.flush_apply.read_only_prepare.span_bytes_total"]; got == "" || got == "0" {
		t.Fatalf("span bytes stat=%q want non-zero", got)
	}
}

func TestOrderedRootDeltaBatchPrepareReadOnlyStats(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, FlushAdmissionPolicy: FlushAdmissionPolicyExplicit})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	baseRoot, err := d.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t,
		"root/a", "va",
		"root/b", "vb",
	).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("publish base root: %v", err)
	}

	deltaIter := mustFrozenSystemMemtable(t,
		"root/b", "vb2",
		"root/c", "vc",
	).NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(deltaIter)
	_ = deltaIter.Close()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchFromIterator: %v", err)
	}
	defer func() { _ = delta.Close() }()

	_, rootIDs, err := d.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
		BaseRoot:               baseRoot,
		Delta:                  delta,
		PrepareReadOnly:        true,
		ReadOnlyPrepareWorkers: 2,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
		}
		return mustFrozenSystemMemtable(t, "sys/collections/users/primary", strconv.FormatUint(rootIDs[0], 10)).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
	}

	stats := d.Stats()
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_calls_total"]; got != "1" {
		t.Fatalf("ordered read-only calls stat=%q want 1", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_requested_workers_total"]; got != "2" {
		t.Fatalf("ordered requested workers stat=%q want 2", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_validation_failures_total"]; got != "0" {
		t.Fatalf("ordered validation failures stat=%q want 0", got)
	}
	if got := stats["treedb.publish.ordered_root_delta_group.root_apply_read_only_prepare_spans_total"]; got == "" || got == "0" {
		t.Fatalf("ordered spans stat=%q want non-zero", got)
	}
	if got := stats["treedb.flush_apply.read_only_prepare.calls_total"]; got != "1" {
		t.Fatalf("generic read-only calls stat=%q want 1", got)
	}
	readOnlyPrepareRoutePrefix := "treedb.publish.ordered_root_delta_group.span_native.route.read_only_prepare."
	requireOrderedRootStatCounterPositive(t, stats, readOnlyPrepareRoutePrefix+"observations_total")
	requireOrderedRootStatCounterPositive(t, stats, readOnlyPrepareRoutePrefix+"candidate_ops_total")
	requireOrderedRootStatCounterPositive(t, stats, readOnlyPrepareRoutePrefix+"fallbacks_total")
	requireOrderedRootStatCounterPositive(t, stats, readOnlyPrepareRoutePrefix+"fallback.reason."+FlushSpanRunFallbackAdmissionPolicyDecline.String()+".ops_total")
	requireOrderedRootStatCounterZero(t, stats, readOnlyPrepareRoutePrefix+"fallback.reason."+FlushSpanRunFallbackUnknown.String()+".ops_total")
}
