package db

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

type lockedRewriteLeafPageLog struct {
	mu    sync.Mutex
	inner *rewriteWriter
}

func (l *lockedRewriteLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.AppendLeafPage(leafPage)
}

func (l *lockedRewriteLeafPageLog) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.AppendLeafPages(leafPages)
}

func (l *lockedRewriteLeafPageLog) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.Flush()
}

func (l *lockedRewriteLeafPageLog) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.Sync()
}

func (l *lockedRewriteLeafPageLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.Close()
}

func (l *lockedRewriteLeafPageLog) LastLeafPageRecordLength() uint32 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.LastLeafPageRecordLength()
}

func (l *lockedRewriteLeafPageLog) CreatedLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.CreatedLeafPageLogSegmentsSnapshot()
}

func (l *lockedRewriteLeafPageLog) MarkLeafPageLogSegmentsRegistered(segments []LeafPageLogSegment) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inner.MarkLeafPageLogSegmentsRegistered(segments)
}

func (l *lockedRewriteLeafPageLog) CurrentValueLogSegment() (string, uint32, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.CurrentValueLogSegment()
}

func openFlushApplyTestDB(t *testing.T, concurrency int) *DB {
	return openFlushApplyTestDBWithSpanNative(t, concurrency, false)
}

func openFlushApplyTestDBWithSpanNative(t *testing.T, concurrency int, spanNative bool) *DB {
	t.Helper()
	d, err := Open(Options{
		Dir:                   t.TempDir(),
		ChunkSize:             64 * 1024,
		FlushAdmissionPolicy:  FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency: concurrency,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
		FlushApplySpanNative:  spanNative,
	})
	if err != nil {
		t.Fatalf("Open(concurrency=%d spanNative=%v): %v", concurrency, spanNative, err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func openFlushApplyLeafLogTestDB(t *testing.T, concurrency int) *DB {
	return openFlushApplyLeafLogTestDBWithSpanNative(t, concurrency, false)
}

func openFlushApplyLeafLogTestDBWithSpanNative(t *testing.T, concurrency int, spanNative bool) *DB {
	t.Helper()
	d, err := Open(Options{
		Dir:                        t.TempDir(),
		ChunkSize:                  64 * 1024,
		IndexOuterLeavesInValueLog: true,
		FlushAdmissionPolicy:       FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:      concurrency,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		FlushApplySpanNative:       spanNative,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionBlock,
			BlockCodec:  ValueLogBlockLZ4,
		},
	})
	if err != nil {
		t.Fatalf("Open leaf-log(concurrency=%d): %v", concurrency, err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(d.dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(d.dir), rewriteLeafLogLaneID, 0)
	lockedLeafLog := &lockedRewriteLeafPageLog{inner: leafLog}
	d.SetLeafPageLog(lockedLeafLog)
	t.Cleanup(func() { closeNoErr(t, lockedLeafLog) })
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func requireDBStatUint64(t *testing.T, d *DB, key string) uint64 {
	t.Helper()
	stats := d.Stats()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %s", key)
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %s=%q: %v", key, raw, err)
	}
	return v
}

func putBatch(t *testing.T, d *DB, start, count int, valuePrefix string) {
	t.Helper()
	b := d.NewBatch()
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%06d", start+i))
		val := []byte(fmt.Sprintf("%s-%06d", valuePrefix, start+i))
		if err := b.Set(key, val); err != nil {
			t.Fatalf("Set seed %d: %v", start+i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write seed: %v", err)
	}
}

func applyMixedFlushApplyBatch(t *testing.T, d *DB) {
	t.Helper()
	b := d.NewBatch()
	for i := 0; i < 6400; i += 2 {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("upd-%06d", i))); err != nil {
			t.Fatalf("Set update %d: %v", i, err)
		}
	}
	// Duplicate point op proves canonical newest-wins survives the opt-in path.
	if err := b.Set([]byte("key-000200"), []byte("first")); err != nil {
		t.Fatalf("duplicate set first: %v", err)
	}
	if err := b.Set([]byte("key-000200"), []byte("second")); err != nil {
		t.Fatalf("duplicate set second: %v", err)
	}
	for i := 101; i < 900; i += 4 {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Delete(key); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}
	if err := b.DeleteRange([]byte("key-001500"), []byte("key-001650")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	for i := 7000; i < 7600; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("new-%06d", i))); err != nil {
			t.Fatalf("Set new %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write mixed: %v", err)
	}
}

func assertDBsEqualOnRange(t *testing.T, serial, parallel *DB, maxKey int) {
	t.Helper()
	for i := 0; i < maxKey; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		want, err := serial.Get(key)
		if err != nil {
			t.Fatalf("serial Get %q: %v", key, err)
		}
		got, err := parallel.Get(key)
		if err != nil {
			t.Fatalf("parallel Get %q: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch for %q: got %q want %q", key, got, want)
		}
	}
}

func TestFlushApplyConcurrencySerialParallelEquivalenceMixedOps(t *testing.T) {
	serial := openFlushApplyTestDB(t, 0)
	parallel := openFlushApplyTestDB(t, 4)
	putBatch(t, serial, 0, 7000, "base")
	putBatch(t, parallel, 0, 7000, "base")

	applyMixedFlushApplyBatch(t, serial)
	applyMixedFlushApplyBatch(t, parallel)

	assertDBsEqualOnRange(t, serial, parallel, 7800)
	if got, _ := parallel.Get([]byte("key-000200")); string(got) != "second" {
		t.Fatalf("newest duplicate value=%q want second", got)
	}
	if got, _ := parallel.Get([]byte("key-001550")); got != nil {
		t.Fatalf("range-deleted value=%q want nil", got)
	}
	stats := parallel.Stats()
	if got := stats["treedb.flush_apply.read_only_prepare.calls_total"]; got == "" || got == "0" {
		t.Fatalf("read-only prepare calls stat=%q want >0", got)
	}
}

func TestFlushApplyConcurrencyUsesBoundedWorkerPoolForPointSpans(t *testing.T) {
	d := openFlushApplyTestDB(t, 4)
	putBatch(t, d, 0, 12000, "base")

	b := d.NewBatch()
	for i := 0; i < 9000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("p-%06d", i))); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write point update: %v", err)
	}
	stats := d.Stats()
	if got := stats["treedb.flush_apply.merge_build.internal_parallel_merges_total"]; got == "" || got == "0" {
		t.Fatalf("internal parallel merge stat=%q want >0", got)
	}
	if got := stats["treedb.flush_apply.merge_build.internal_parallel_workers_total"]; got == "" || got == "0" {
		t.Fatalf("internal parallel workers stat=%q want >0", got)
	}
	for _, key := range []string{
		"treedb.flush_apply.old_leaf_read_decode.bytes_per_op",
		"treedb.flush_apply.merge_build.leaf_merges_per_op",
		"treedb.flush_apply.merge_build.replacement_leaf_pages_per_op",
		"treedb.flush_apply.root_reduce.ns_per_op",
		"treedb.flush_apply.publish_prepare.ns_per_op",
		"treedb.flush_apply.guarded_publish.ns_per_op",
		"treedb.flush_apply.publish_final_install.ns_per_op",
		"treedb.flush_apply.publish_total.ns_per_op",
		"treedb.flush_apply.span_run.target_leaf_spans_total",
		"treedb.flush_apply.span_run.span_ops_total",
		"treedb.flush_apply.span_run.ops_per_span",
		"treedb.flush_apply.span_native.candidate_ops_total",
		"treedb.flush_apply.span_native.ineligible_ops_total",
	} {
		if got := stats[key]; got == "" {
			t.Fatalf("missing proof counter %s", key)
		}
	}
	if got := stats["treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total"]; got == "" || got == "0" {
		t.Fatalf("span-native not-implemented fallback ops=%q want >0", got)
	}
	if got := stats["treedb.flush_apply.span_native.eligible_ops_total"]; got != "0" {
		t.Fatalf("span-native eligible ops=%q want 0 before M10", got)
	}
}

func TestFlushApplyRootMismatchRetriesWithoutPublishingAbandonedWork(t *testing.T) {
	d := openFlushApplyTestDB(t, 4)
	putBatch(t, d, 0, 9000, "base")

	var fired atomic.Bool
	d.testAfterOptimisticApplyHook = func() {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		other := d.NewBatch()
		if err := other.Set([]byte("key-concurrent"), []byte("concurrent")); err != nil {
			t.Fatalf("concurrent Set: %v", err)
		}
		if err := other.Write(); err != nil {
			t.Fatalf("concurrent Write: %v", err)
		}
	}
	defer func() { d.testAfterOptimisticApplyHook = nil }()

	b := d.NewBatch()
	for i := 0; i < 7000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("retry-%06d", i))); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write with retry: %v", err)
	}
	if got, err := d.Get([]byte("key-concurrent")); err != nil || string(got) != "concurrent" {
		t.Fatalf("concurrent value got=%q err=%v", got, err)
	}
	if got, err := d.Get([]byte("key-000123")); err != nil || string(got) != "retry-000123" {
		t.Fatalf("retried value got=%q err=%v", got, err)
	}
	stats := d.Stats()
	if got := stats["treedb.flush_apply.mismatch_total"]; got == "" || got == "0" {
		t.Fatalf("mismatch stat=%q want >0", got)
	}
	if got := stats["treedb.flush_apply.retry_total"]; got == "" || got == "0" {
		t.Fatalf("retry stat=%q want >0", got)
	}
}

func TestFlushApplyPreparedPublishMismatchRetriesWithoutPublishingPreparedRoot(t *testing.T) {
	d := openFlushApplyTestDB(t, 4)
	putBatch(t, d, 0, 9000, "base")
	prepareBefore := requireDBStatUint64(t, d, "treedb.flush_apply.publish_prepare.calls_total")

	var fired atomic.Bool
	d.testAfterOptimisticPublishPrepareHook = func() {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		other := d.NewBatch()
		if err := other.Set([]byte("key-concurrent"), []byte("concurrent")); err != nil {
			t.Fatalf("concurrent Set: %v", err)
		}
		if err := other.Write(); err != nil {
			t.Fatalf("concurrent Write: %v", err)
		}
	}
	defer func() { d.testAfterOptimisticPublishPrepareHook = nil }()

	b := d.NewBatch()
	for i := 0; i < 7000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("prepared-retry-%06d", i))); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write with prepared-publish retry: %v", err)
	}
	if got, err := d.Get([]byte("key-concurrent")); err != nil || string(got) != "concurrent" {
		t.Fatalf("concurrent value got=%q err=%v", got, err)
	}
	if got, err := d.Get([]byte("key-000123")); err != nil || string(got) != "prepared-retry-000123" {
		t.Fatalf("retried value got=%q err=%v", got, err)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.mismatch_total"); got == 0 {
		t.Fatalf("mismatch stat=0 want >0")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.retry_total"); got == 0 {
		t.Fatalf("retry stat=0 want >0")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.publish_prepare.calls_total"); got < prepareBefore+2 {
		t.Fatalf("publish prepare calls delta=%d want >=2 for stale prepared attempt plus retry", got-prepareBefore)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.publish_final_install.calls_total"); got == 0 {
		t.Fatalf("publish final install calls=0 want >0")
	}
}

func TestFlushApplyPreparedPublishWriteSyncReopens(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                   dir,
		ChunkSize:             64 * 1024,
		FlushAdmissionPolicy:  FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b := d.NewBatch()
	for i := 0; i < 1024; i++ {
		key := []byte(fmt.Sprintf("reopen-key-%06d", i))
		val := []byte(fmt.Sprintf("reopen-value-%06d", i))
		if err := b.Set(key, val); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.publish_prepare.calls_total"); got == 0 {
		t.Fatalf("publish prepare calls=0 want >0")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.publish_final_install.calls_total"); got == 0 {
		t.Fatalf("publish final install calls=0 want >0")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	for _, i := range []int{0, 123, 1023} {
		key := []byte(fmt.Sprintf("reopen-key-%06d", i))
		want := fmt.Sprintf("reopen-value-%06d", i)
		got, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("reopen Get %q: %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("reopen Get %q=%q want %q", key, got, want)
		}
	}
}

func TestFlushApplyRootMismatchTracksAbandonedLeafLogOutput(t *testing.T) {
	d := openFlushApplyLeafLogTestDB(t, 4)
	putBatch(t, d, 0, 9000, "base")

	var fired atomic.Bool
	d.testAfterOptimisticApplyHook = func() {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		other := d.NewBatch()
		if err := other.Set([]byte("key-concurrent"), []byte("concurrent")); err != nil {
			t.Fatalf("concurrent Set: %v", err)
		}
		if err := other.Write(); err != nil {
			t.Fatalf("concurrent Write: %v", err)
		}
	}
	defer func() { d.testAfterOptimisticApplyHook = nil }()

	b := d.NewBatch()
	for i := 0; i < 7000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("retry-%06d", i))); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write with retry: %v", err)
	}
	if got, err := d.Get([]byte("key-concurrent")); err != nil || string(got) != "concurrent" {
		t.Fatalf("concurrent value got=%q err=%v", got, err)
	}
	if got, err := d.Get([]byte("key-000123")); err != nil || string(got) != "retry-000123" {
		t.Fatalf("retried value got=%q err=%v", got, err)
	}

	prepared := requireDBStatUint64(t, d, "treedb.flush_apply.prepared_output.leaf_log_pages_prepared_total")
	installed := requireDBStatUint64(t, d, "treedb.flush_apply.prepared_output.leaf_log_pages_installed_total")
	abandoned := requireDBStatUint64(t, d, "treedb.flush_apply.prepared_output.leaf_log_pages_abandoned_total")
	if abandoned == 0 {
		t.Fatalf("abandoned leaf-log output = 0, want retry orphan output counted")
	}
	if installed == 0 {
		t.Fatalf("installed leaf-log output = 0, want published output counted")
	}
	if prepared < installed+abandoned {
		t.Fatalf("prepared leaf-log output=%d < installed+abandoned=%d+%d", prepared, installed, abandoned)
	}
}

func TestFlushApplySpanNativePartialMultiLeafParentStitchWithStats(t *testing.T) {
	d := openFlushApplyTestDBWithSpanNative(t, 4, true)
	putBatch(t, d, 0, 9000, "base")
	usedBefore := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.used_ops_total")
	fallbackBefore := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total")

	b := d.NewBatch()
	for i := 2000; i < 7000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("partial-%06d", i))); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("partial Write: %v", err)
	}
	if got, err := d.Get([]byte("key-000123")); err != nil || string(got) != "base-000123" {
		t.Fatalf("untouched left key got=%q err=%v", got, err)
	}
	if got, err := d.Get([]byte("key-003456")); err != nil || string(got) != "partial-003456" {
		t.Fatalf("updated key got=%q err=%v", got, err)
	}
	if got, err := d.Get([]byte("key-008000")); err != nil || string(got) != "base-008000" {
		t.Fatalf("untouched right key got=%q err=%v", got, err)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.eligible_ops_total"); got == 0 {
		t.Fatalf("span-native eligible ops = 0, want partial run classified before fallback")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.used_ops_total"); got <= usedBefore {
		t.Fatalf("span-native used ops delta=%d want >0 for partial parent-stitch path", got-usedBefore)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total"); got != fallbackBefore {
		t.Fatalf("span-native not-implemented fallback ops delta=%d want 0 for partial parent-stitch path", got-fallbackBefore)
	}
}

func TestFlushApplySpanNativeSparsePointSpansWithStats(t *testing.T) {
	d := openFlushApplyTestDBWithSpanNative(t, 4, true)
	putBatch(t, d, 0, 12000, "base")
	usedBefore := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.used_ops_total")
	fallbackBefore := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total")

	b := d.NewBatch()
	updated := []int{17, 997, 2049, 4097, 6143, 8191, 11003}
	for _, i := range updated {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("sparse-%06d", i))); err != nil {
			t.Fatalf("Set sparse %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("sparse Write: %v", err)
	}
	for _, i := range updated {
		key := []byte(fmt.Sprintf("key-%06d", i))
		want := fmt.Sprintf("sparse-%06d", i)
		if got, err := d.Get(key); err != nil || string(got) != want {
			t.Fatalf("updated key %q got=%q err=%v want %q", key, got, err, want)
		}
	}
	if got, err := d.Get([]byte("key-000123")); err != nil || string(got) != "base-000123" {
		t.Fatalf("untouched key got=%q err=%v", got, err)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.used_ops_total"); got <= usedBefore {
		t.Fatalf("span-native used ops delta=%d want >0 for sparse point spans", got-usedBefore)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total"); got != fallbackBefore {
		t.Fatalf("span-native not-implemented fallback ops delta=%d want 0 for sparse point spans", got-fallbackBefore)
	}
}

func TestFlushApplySpanNativeSchedulerWorkerStats(t *testing.T) {
	// This test asserts multi-worker scheduler telemetry; force enough runnable
	// workers even when the test process is launched with GOMAXPROCS=1.
	prevProcs := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(prevProcs) })

	d := openFlushApplyTestDBWithSpanNative(t, 4, true)
	putBatch(t, d, 0, 12000, "base")

	beforeBusy := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.worker_busy_ns_total")
	beforeIdle := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.worker_idle_ns_total")
	beforeWait := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.worker_wait_ns_total")
	beforeReady := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.ready_tasks_total")
	beforeDispatched := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.dispatched_tasks_total")
	beforeCompleted := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.completed_tasks_total")
	beforeTaskSpans := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.task_spans_total")
	beforeTaskOps := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.task_ops_total")
	beforeSingleSpanTasks := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.single_span_tasks_total")

	b := d.NewBatch()
	for _, i := range []int{17, 997, 2049, 4097, 6143, 8191, 11003} {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("sched-%06d", i))); err != nil {
			t.Fatalf("Set scheduler %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("scheduler Write: %v", err)
	}

	busy := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.worker_busy_ns_total") - beforeBusy
	idle := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.worker_idle_ns_total") - beforeIdle
	wait := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.worker_wait_ns_total") - beforeWait
	ready := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.ready_tasks_total") - beforeReady
	dispatched := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.dispatched_tasks_total") - beforeDispatched
	completed := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.completed_tasks_total") - beforeCompleted
	if busy == 0 {
		t.Fatalf("span-native worker busy ns delta=0, want >0")
	}
	if idle > wait*4 {
		t.Fatalf("span-native worker idle ns delta=%d exceeds plausible test capacity wait=%d", idle, wait)
	}
	if wait == 0 {
		t.Fatalf("span-native worker wait ns delta=0, want >0")
	}
	if ready == 0 {
		t.Fatalf("span-native ready tasks delta=0, want >0")
	}
	if dispatched != ready || completed != dispatched {
		t.Fatalf("span-native scheduler ready/dispatched/completed=%d/%d/%d, want all equal", ready, dispatched, completed)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.queue_depth_max"); got == 0 {
		t.Fatalf("span-native queue_depth_max=0, want >0")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.scheduled_workers_max"); got < 2 {
		t.Fatalf("span-native scheduled_workers_max=%d, want >=2", got)
	}
	taskSpans := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.task_spans_total") - beforeTaskSpans
	taskOps := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.task_ops_total") - beforeTaskOps
	singleSpanTasks := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.single_span_tasks_total") - beforeSingleSpanTasks
	if taskSpans < ready || taskOps == 0 {
		t.Fatalf("span-native task distribution spans/ops=%d/%d ready=%d, want populated work-unit sizes", taskSpans, taskOps, ready)
	}
	if singleSpanTasks > ready {
		t.Fatalf("span-native single-span tasks=%d exceeds ready tasks=%d", singleSpanTasks, ready)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.task_spans_max"); got == 0 {
		t.Fatalf("span-native task_spans_max=0, want >0")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.task_ops_max"); got == 0 {
		t.Fatalf("span-native task_ops_max=0, want >0")
	}
}

func TestFlushApplySpanNativeRangeDeleteBarrierDoesNotScheduleWorkUnits(t *testing.T) {
	d := openFlushApplyTestDBWithSpanNative(t, 4, true)
	putBatch(t, d, 0, 2048, "base")

	beforeReady := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.ready_tasks_total")
	beforeTaskSpans := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.task_spans_total")
	beforeFallback := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.fallback.reason.range_delete_barrier.ops_total")

	b := d.NewBatch()
	if err := b.DeleteRange([]byte("key-000100"), []byte("key-000300")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("range barrier Write: %v", err)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.fallback.reason.range_delete_barrier.ops_total"); got <= beforeFallback {
		t.Fatalf("range_delete_barrier fallback delta=%d want >0", got-beforeFallback)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.ready_tasks_total"); got != beforeReady {
		t.Fatalf("span-native scheduler ready tasks changed on range fallback: before=%d after=%d", beforeReady, got)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.scheduler.task_spans_total"); got != beforeTaskSpans {
		t.Fatalf("span-native task spans changed on range fallback: before=%d after=%d", beforeTaskSpans, got)
	}
}

func TestFlushApplyLeafLogOutputAppendWaitStats(t *testing.T) {
	d := openFlushApplyLeafLogTestDBWithSpanNative(t, 4, true)
	putBatch(t, d, 0, 9000, "base")

	beforeWait := requireDBStatUint64(t, d, "treedb.flush_apply.leaf_log_output.append_wait_ns_total")
	beforeCalls := requireDBStatUint64(t, d, "treedb.flush_apply.leaf_log_output.append_calls_total")
	beforePages := requireDBStatUint64(t, d, "treedb.flush_apply.leaf_log_output.append_pages_total")

	b := d.NewBatch()
	for i := 2000; i < 7000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("leaflog-%06d", i))); err != nil {
			t.Fatalf("Set leaf-log %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("leaf-log Write: %v", err)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.leaf_log_output.append_wait_ns_total"); got <= beforeWait {
		t.Fatalf("leaf-log append wait ns delta=%d, want >0", got-beforeWait)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.leaf_log_output.append_calls_total"); got <= beforeCalls {
		t.Fatalf("leaf-log append calls delta=%d, want >0", got-beforeCalls)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.leaf_log_output.append_pages_total"); got <= beforePages {
		t.Fatalf("leaf-log append pages delta=%d, want >0", got-beforePages)
	}
}

func TestFlushApplySpanNativeSingleWorkerUsesApplyWithOptionsStats(t *testing.T) {
	d := openFlushApplyTestDBWithSpanNative(t, 1, true)
	putBatch(t, d, 0, 4096, "base")
	prepareBefore := requireDBStatUint64(t, d, "treedb.flush_apply.read_only_prepare.calls_total")
	usedBefore := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.used_ops_total")
	fallbackBefore := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total")

	b := d.NewBatch()
	for i := 0; i < 4096; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("single-%06d", i))); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("single-worker span-native Write: %v", err)
	}
	if got, err := d.Get([]byte("key-000123")); err != nil || string(got) != "single-000123" {
		t.Fatalf("updated key got=%q err=%v", got, err)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.read_only_prepare.calls_total"); got <= prepareBefore {
		t.Fatalf("read-only prepare calls delta=%d want >0 for span-native single-worker path", got-prepareBefore)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.used_ops_total"); got <= usedBefore {
		t.Fatalf("span-native used ops delta=%d want >0 with FlushApplySpanNative and concurrency=1", got-usedBefore)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.fallback.reason.span_native_not_implemented.ops_total"); got != fallbackBefore {
		t.Fatalf("span-native not-implemented fallback ops delta=%d want 0 for single-worker path", got-fallbackBefore)
	}
}

func TestFlushApplySpanNativePreparedLeafLogOutputReopens(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		ChunkSize:                  64 * 1024,
		IndexOuterLeavesInValueLog: true,
		FlushAdmissionPolicy:       FlushAdmissionPolicyExplicit,
		FlushApplyConcurrency:      4,
		FlushApplyMinEntries:       1,
		FlushApplyMinSpans:         1,
		FlushApplyMinBytes:         1,
		FlushApplySpanNative:       true,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionBlock,
			BlockCodec:  ValueLogBlockLZ4,
		},
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{
		Compression: ValueLogCompressionBlock,
		BlockCodec:  ValueLogBlockLZ4,
	})
	if err != nil {
		_ = d.Close()
		t.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	d.SetLeafPageLog(leafLog)

	putBatch(t, d, 0, 4096, "base")
	updates := d.NewBatch()
	for _, anchor := range []int{17, 1029, 2053, 3079} {
		for j := 0; j < 48; j++ {
			key := []byte(fmt.Sprintf("key-%06d-%03d", anchor, j))
			val := bytes.Repeat([]byte{byte(1 + (anchor+j)%251)}, 180)
			if err := updates.Set(key, val); err != nil {
				_ = updates.Close()
				_ = leafLog.Close()
				_ = d.Close()
				t.Fatalf("Set update anchor=%d j=%d: %v", anchor, j, err)
			}
		}
	}
	if err := updates.Write(); err != nil {
		_ = updates.Close()
		_ = leafLog.Close()
		_ = d.Close()
		t.Fatalf("Write updates: %v", err)
	}
	_ = updates.Close()
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.used_ops_total"); got == 0 {
		_ = leafLog.Close()
		_ = d.Close()
		t.Fatalf("span-native used ops = 0, want prepared leaf-log output path")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.leaf_log_output.append_calls_total"); got == 0 {
		_ = leafLog.Close()
		_ = d.Close()
		t.Fatalf("leaf-log append calls = 0, want committed prepared output")
	}
	if err := d.Checkpoint(); err != nil {
		_ = leafLog.Close()
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		_ = leafLog.Close()
		t.Fatalf("Close db: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	for _, key := range [][]byte{[]byte("key-000000"), []byte("key-001029-007"), []byte("key-003079-047")} {
		got, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("reopen Get(%q): %v", key, err)
		}
		if len(got) == 0 {
			t.Fatalf("reopen Get(%q) returned empty value", key)
		}
	}
}

func TestFlushApplySpanNativeRootMismatchTracksAbandonedLeafLogOutput(t *testing.T) {
	d := openFlushApplyLeafLogTestDBWithSpanNative(t, 4, true)
	putBatch(t, d, 0, 9000, "base")

	var fired atomic.Bool
	d.testAfterOptimisticApplyHook = func() {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		other := d.NewBatch()
		if err := other.Set([]byte("key-concurrent"), []byte("concurrent")); err != nil {
			t.Fatalf("concurrent Set: %v", err)
		}
		if err := other.Write(); err != nil {
			t.Fatalf("concurrent Write: %v", err)
		}
	}
	defer func() { d.testAfterOptimisticApplyHook = nil }()

	b := d.NewBatch()
	for i := 0; i < 9000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := b.Set(key, []byte(fmt.Sprintf("span-retry-%06d", i))); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write with retry: %v", err)
	}
	if got, err := d.Get([]byte("key-concurrent")); err != nil || string(got) != "concurrent" {
		t.Fatalf("concurrent value got=%q err=%v", got, err)
	}
	if got, err := d.Get([]byte("key-000123")); err != nil || string(got) != "span-retry-000123" {
		t.Fatalf("retried value got=%q err=%v", got, err)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.used_ops_total"); got == 0 {
		t.Fatalf("span-native used ops = 0, want first optimistic attempt to run span-native")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.mismatch_total"); got == 0 {
		t.Fatalf("mismatch stat=0 want >0")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.fallback.reason.root_mismatch.ops_total"); got == 0 {
		t.Fatalf("span-native root_mismatch fallback ops=0 want >0")
	}
	if got := requireDBStatUint64(t, d, "treedb.raw.span_native.route.point_put.fallback.reason.root_mismatch.ops_total"); got == 0 {
		t.Fatalf("raw point_put root_mismatch fallback ops=0 want >0")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.retry_total"); got == 0 {
		t.Fatalf("retry stat=0 want >0")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.prepared_output.leaf_log_pages_abandoned_total"); got == 0 {
		t.Fatalf("abandoned leaf-log output = 0, want span-native retry output counted")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.prepared_output.leaf_log_pages_installed_total"); got == 0 {
		t.Fatalf("installed leaf-log output = 0, want final retry output counted")
	}
}

func TestFlushApplyFinalizeFailureTracksAbandonedLeafLogOutput(t *testing.T) {
	d := openFlushApplyLeafLogTestDB(t, 4)
	d.testFailFinalizeCommit.Store(true)
	err := func() error {
		b := d.NewBatch()
		defer b.Close()
		for i := 0; i < 7000; i++ {
			key := []byte(fmt.Sprintf("key-%06d", i))
			if err := b.Set(key, []byte(fmt.Sprintf("fail-%06d", i))); err != nil {
				return err
			}
		}
		return b.Write()
	}()
	d.testFailFinalizeCommit.Store(false)
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("Write failpoint err=%v, want %v", err, errTestFinalizeCommitFailpoint)
	}
	if got, err := d.Get([]byte("key-000123")); err != nil || got != nil {
		t.Fatalf("failed publish exposed key: got=%q err=%v", got, err)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.prepared_output.leaf_log_pages_abandoned_total"); got == 0 {
		t.Fatalf("abandoned leaf-log output = 0, want finalize-failure output counted")
	}
	putBatch(t, d, 0, 256, "ok")
	if got, err := d.Get([]byte("key-000123")); err != nil || string(got) != "ok-000123" {
		t.Fatalf("post-failure write got=%q err=%v", got, err)
	}
}

func TestFlushApplySpanNativeFinalizeFailureTracksOutputOwnershipFallback(t *testing.T) {
	d := openFlushApplyLeafLogTestDBWithSpanNative(t, 4, true)
	d.testFailFinalizeCommit.Store(true)
	err := func() error {
		b := d.NewBatch()
		defer b.Close()
		for i := 0; i < 7000; i++ {
			key := []byte(fmt.Sprintf("key-%06d", i))
			if err := b.Set(key, []byte(fmt.Sprintf("span-fail-%06d", i))); err != nil {
				return err
			}
		}
		return b.Write()
	}()
	d.testFailFinalizeCommit.Store(false)
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("Write failpoint err=%v, want %v", err, errTestFinalizeCommitFailpoint)
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.span_native.fallback.reason.output_ownership_failure.ops_total"); got == 0 {
		t.Fatalf("span-native output_ownership_failure fallback ops=0 want >0")
	}
	if got := requireDBStatUint64(t, d, "treedb.raw.span_native.route.point_put.fallback.reason.output_ownership_failure.ops_total"); got == 0 {
		t.Fatalf("raw point_put output_ownership_failure fallback ops=0 want >0")
	}
	if got := requireDBStatUint64(t, d, "treedb.flush_apply.prepared_output.leaf_log_pages_abandoned_total"); got == 0 {
		t.Fatalf("abandoned leaf-log output = 0, want span-native finalize-failure output counted")
	}
}

func TestFlushApplyCloseAndCheckpointDrainInProgressApply(t *testing.T) {
	d := openFlushApplyTestDB(t, 4)
	putBatch(t, d, 0, 9000, "base")

	block := make(chan struct{})
	entered := make(chan struct{})
	var fired atomic.Bool
	d.testAfterOptimisticApplyHook = func() {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		close(entered)
		<-block
	}

	writeDone := make(chan error, 1)
	go func() {
		b := d.NewBatch()
		for i := 0; i < 7000; i++ {
			key := []byte(fmt.Sprintf("key-%06d", i))
			if err := b.Set(key, []byte(fmt.Sprintf("drain-%06d", i))); err != nil {
				writeDone <- err
				return
			}
		}
		writeDone <- b.Write()
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("apply hook did not run")
	}

	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- d.Checkpoint() }()
	select {
	case err := <-checkpointDone:
		t.Fatalf("Checkpoint returned before in-progress apply drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(block)
	if err := <-writeDone; err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := <-checkpointDone; err != nil {
		t.Fatalf("checkpoint after drain: %v", err)
	}

	// Close should also drain an in-progress apply before closing the worker pool
	// and index resources.
	blockClose := make(chan struct{})
	enteredClose := make(chan struct{})
	fired.Store(false)
	d.testAfterOptimisticApplyHook = func() {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		close(enteredClose)
		<-blockClose
	}
	writeDone = make(chan error, 1)
	go func() {
		b := d.NewBatch()
		for i := 0; i < 7000; i++ {
			key := []byte(fmt.Sprintf("key-%06d", i))
			if err := b.Set(key, []byte(fmt.Sprintf("close-%06d", i))); err != nil {
				writeDone <- err
				return
			}
		}
		writeDone <- b.Write()
	}()
	select {
	case <-enteredClose:
	case <-time.After(5 * time.Second):
		t.Fatal("close apply hook did not run")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before in-progress apply drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(blockClose)
	if err := <-writeDone; err != nil {
		t.Fatalf("close-drained write: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close after drain: %v", err)
	}
}
