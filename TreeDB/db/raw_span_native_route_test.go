package db

import (
	"bytes"
	"errors"
	"runtime"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/zipper"
)

func TestRawSpanNativeRouteStatsPointWritesUseDefaultAuto(t *testing.T) {
	prev := runtime.GOMAXPROCS(8)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Set([]byte("seed"), []byte("one")); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	if err := d.Set([]byte("point"), []byte("two")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := d.SetSync([]byte("sync"), []byte("three")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	stats := d.Stats()
	if got := stats["treedb.flush_admission.policy"]; got != "auto" {
		t.Fatalf("admission policy=%q want auto", got)
	}
	if got := stats["treedb.flush_admission.admitted"]; got != "true" {
		t.Fatalf("admission admitted=%q want true", got)
	}
	if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.point_put.used_ops_total"); got == 0 {
		t.Fatalf("point_put used_ops_total=0, want default auto point puts to use span-native apply")
	}
	if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.point_put.fallback.reason.unknown.count_total"); got != 0 {
		t.Fatalf("point_put unknown fallbacks=%d want 0", got)
	}
}

func TestRawSpanNativeRouteStatsPointShapeMatrix(t *testing.T) {
	d, err := Open(Options{
		Dir:                   t.TempDir(),
		FlushAdmissionPolicy:  FlushAdmissionPolicyExplicit,
		FlushApplySpanNative:  true,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Set([]byte("put"), []byte("one")); err != nil {
		t.Fatalf("Set put: %v", err)
	}
	if err := d.Set([]byte("delete"), []byte("old")); err != nil {
		t.Fatalf("Set delete seed: %v", err)
	}
	if err := d.Delete([]byte("delete")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	b := d.NewBatch()
	if err := b.Set([]byte("mixed-put"), []byte("two")); err != nil {
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Delete([]byte("put")); err != nil {
		t.Fatalf("batch Delete: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}

	stats := d.Stats()
	for _, route := range []string{"point_put", "point_delete", "mixed_point"} {
		key := "treedb.raw.span_native.route." + route + ".observations_total"
		if got := rawSpanNativeRouteStatUint(t, stats, key); got == 0 {
			t.Fatalf("%s=0, want observed route", key)
		}
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route."+route+".fallback.reason.unknown.count_total"); got != 0 {
			t.Fatalf("%s unknown fallbacks=%d want 0", route, got)
		}
	}
}

func TestRawSpanNativeRouteStatsMixedPointDeleteMaintenanceFallback(t *testing.T) {
	d := openExplicitRawSpanNativeRouteTestDB(t)
	if err := d.Set([]byte("delete-me"), []byte("old")); err != nil {
		t.Fatalf("Set delete-me seed: %v", err)
	}

	b := d.NewBatch()
	if err := b.Set([]byte("new"), []byte("value")); err != nil {
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Delete([]byte("delete-me")); err != nil {
		t.Fatalf("batch Delete: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}

	stats := d.Stats()
	prefix := "treedb.raw.span_native.route.mixed_point."
	if got := rawSpanNativeRouteStatUint(t, stats, prefix+"observations_total"); got == 0 {
		t.Fatalf("mixed_point observations=0, want observed route")
	}
	if got := rawSpanNativeRouteStatUint(t, stats, prefix+"candidate_ops_total"); got == 0 {
		t.Fatalf("mixed_point candidate_ops_total=0, want delete-maintenance batch to remain a visible candidate")
	}
	if got := rawSpanNativeRouteStatUint(t, stats, prefix+"used_ops_total"); got != 0 {
		t.Fatalf("mixed_point used_ops_total=%d, want raw delete-maintenance fallback", got)
	}
	if got := rawSpanNativeRouteStatUint(t, stats, prefix+"fallback.reason."+FlushSpanRunFallbackMaintenance.String()+".count_total"); got == 0 {
		t.Fatalf("mixed_point maintenance fallback count=0, want raw delete-maintenance fallback")
	}
	if got := rawSpanNativeRouteStatUint(t, stats, prefix+"fallback.reason.unknown.count_total"); got != 0 {
		t.Fatalf("mixed_point unknown fallback count=%d, want classified maintenance fallback", got)
	}
	if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.fallback.reason."+FlushSpanRunFallbackMaintenance.String()+".ops_total"); got == 0 {
		t.Fatalf("raw maintenance fallback ops=0, want aggregate maintenance fallback")
	}
	if got, err := d.Get([]byte("new")); err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("Get new=%q err=%v, want value", got, err)
	}
	if got, err := d.Get([]byte("delete-me")); err != nil || got != nil {
		t.Fatalf("Get delete-me=%q err=%v, want deleted key", got, err)
	}
}

func TestRawSpanNativeRouteStatsUnsupportedRowsHaveNamedFallbacks(t *testing.T) {
	t.Run("range delete barrier", func(t *testing.T) {
		d := openExplicitRawSpanNativeRouteTestDB(t)
		for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
			if err := d.Set(key, []byte("v")); err != nil {
				t.Fatalf("seed Set(%q): %v", key, err)
			}
		}
		b := d.NewBatch()
		if err := b.DeleteRange([]byte("a"), []byte("z")); err != nil {
			t.Fatalf("DeleteRange: %v", err)
		}
		if err := b.Write(); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		stats := d.Stats()
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.range_delete.fallback.reason.range_delete_barrier.count_total"); got == 0 {
			t.Fatalf("range_delete range_delete_barrier count=0, want named fallback")
		}
	})

	t.Run("mixed range delete barrier", func(t *testing.T) {
		d := openExplicitRawSpanNativeRouteTestDB(t)
		for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
			if err := d.Set(key, []byte("v")); err != nil {
				t.Fatalf("seed Set(%q): %v", key, err)
			}
		}
		b := d.NewBatch()
		if err := b.Set([]byte("point"), []byte("v2")); err != nil {
			t.Fatalf("Set mixed point: %v", err)
		}
		if err := b.Delete([]byte("b")); err != nil {
			t.Fatalf("Delete mixed point: %v", err)
		}
		if err := b.DeleteRange([]byte("a"), []byte("z")); err != nil {
			t.Fatalf("DeleteRange: %v", err)
		}
		if err := b.Write(); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		stats := d.Stats()
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.mixed_range_delete.observations_total"); got == 0 {
			t.Fatalf("mixed_range_delete observations=0, want observed route")
		}
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.mixed_range_delete.fallback.reason.range_delete_barrier.count_total"); got == 0 {
			t.Fatalf("mixed_range_delete range_delete_barrier count=0, want named fallback")
		}
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.mixed_range_delete.fallback.reason.unknown.count_total"); got != 0 {
			t.Fatalf("mixed_range_delete unknown fallbacks=%d want 0", got)
		}
	})

	t.Run("empty batch below threshold", func(t *testing.T) {
		d := openExplicitRawSpanNativeRouteTestDB(t)
		b := d.NewBatch()
		if err := b.Write(); err != nil {
			t.Fatalf("empty Write: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		stats := d.Stats()
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.empty_batch.fallback.reason.below_threshold.count_total"); got == 0 {
			t.Fatalf("empty_batch below_threshold count=0, want named fallback")
		}
	})

	t.Run("empty writesync below threshold", func(t *testing.T) {
		d := openExplicitRawSpanNativeRouteTestDB(t)
		b := d.NewBatch()
		if err := b.WriteSync(); err != nil {
			t.Fatalf("empty WriteSync: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		stats := d.Stats()
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.empty_batch.fallback.reason.below_threshold.count_total"); got == 0 {
			t.Fatalf("empty_batch below_threshold count=0, want WriteSync shortcut named fallback")
		}
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.empty_batch.fallback.reason.disabled.count_total"); got != 0 {
			t.Fatalf("empty_batch disabled count=%d, want 0 for WriteSync shortcut", got)
		}
	})

	t.Run("policy off rollback", func(t *testing.T) {
		d, err := Open(Options{
			Dir:                   t.TempDir(),
			FlushAdmissionPolicy:  FlushAdmissionPolicyOff,
			FlushApplySpanNative:  true,
			FlushApplyConcurrency: 4,
			FlushApplyMinEntries:  1,
			FlushApplyMinSpans:    1,
			FlushApplyMinBytes:    1,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()
		if err := d.Set([]byte("off"), []byte("v")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		stats := d.Stats()
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.point_put.fallback.reason.disabled.count_total"); got == 0 {
			t.Fatalf("point_put disabled count=0, want policy-off rollback fallback")
		}
	})

	t.Run("auto declined", func(t *testing.T) {
		d, err := Open(Options{
			Dir:                   t.TempDir(),
			FlushAdmissionPolicy:  FlushAdmissionPolicyAuto,
			FlushApplySpanNative:  true,
			FlushApplyConcurrency: 1,
			FlushApplyMinEntries:  1,
			FlushApplyMinSpans:    1,
			FlushApplyMinBytes:    1,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()
		if err := d.Set([]byte("declined"), []byte("v")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		stats := d.Stats()
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.point_put.fallback.reason.admission_policy_decline.count_total"); got == 0 {
			t.Fatalf("point_put admission_policy_decline count=0, want auto-declined fallback")
		}
	})

	t.Run("parallel only disabled", func(t *testing.T) {
		d, err := Open(Options{
			Dir:                   t.TempDir(),
			FlushAdmissionPolicy:  FlushAdmissionPolicyExplicit,
			FlushApplySpanNative:  false,
			FlushApplyConcurrency: 4,
			FlushApplyMinEntries:  1,
			FlushApplyMinSpans:    1,
			FlushApplyMinBytes:    1,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()
		if err := d.Set([]byte("parallel-only"), []byte("v")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		stats := d.Stats()
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.point_put.fallback.reason.disabled.count_total"); got == 0 {
			t.Fatalf("point_put disabled count=0, want parallel-only disabled fallback")
		}
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.point_put.fallback.reason.span_native_not_implemented.count_total"); got != 0 {
			t.Fatalf("point_put span_native_not_implemented count=%d, want 0 for parallel-only disabled fallback", got)
		}
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.point_put.candidate_ops_total"); got != 0 {
			t.Fatalf("point_put candidate_ops_total=%d, want 0 when span-native apply is disabled", got)
		}
	})

	t.Run("parallel only range delete disabled", func(t *testing.T) {
		d, err := Open(Options{
			Dir:                   t.TempDir(),
			FlushAdmissionPolicy:  FlushAdmissionPolicyExplicit,
			FlushApplySpanNative:  false,
			FlushApplyConcurrency: 4,
			FlushApplyMinEntries:  1,
			FlushApplyMinSpans:    1,
			FlushApplyMinBytes:    1,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = d.Close() }()
		for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
			if err := d.Set(key, []byte("v")); err != nil {
				t.Fatalf("seed Set(%q): %v", key, err)
			}
		}
		b := d.NewBatch()
		if err := b.DeleteRange([]byte("a"), []byte("z")); err != nil {
			t.Fatalf("DeleteRange: %v", err)
		}
		if err := b.Write(); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		stats := d.Stats()
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.range_delete.fallback.reason.disabled.count_total"); got == 0 {
			t.Fatalf("range_delete disabled count=0, want parallel-only disabled fallback")
		}
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.range_delete.fallback.reason.range_delete_barrier.count_total"); got != 0 {
			t.Fatalf("range_delete barrier count=%d, want 0 for parallel-only disabled fallback", got)
		}
		if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.range_delete.candidate_ops_total"); got != 0 {
			t.Fatalf("range_delete candidate_ops_total=%d, want 0 when span-native apply is disabled", got)
		}
	})
}

func TestRawSpanNativeRouteStatsCommandWALCheckpointPiggybackUsesSpanNativeApply(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                   dir,
		FlushAdmissionPolicy:  FlushAdmissionPolicyExplicit,
		FlushApplySpanNative:  true,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	key := []byte("checkpoint-command-wal-piggyback")
	value := []byte("v")
	batch := d.NewPhysicalBatch()
	b, ok := batch.(*Batch)
	if !ok {
		t.Fatalf("NewPhysicalBatch type=%T, want *Batch", batch)
	}
	if err := b.Set(key, value); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.SetCommandWALPublish(1, []CommandWALLSNRange{{First: 1, Last: 1}}); err != nil {
		t.Fatalf("SetCommandWALPublish: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	stats := d.Stats()
	if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.point_put.used_ops_total"); got == 0 {
		t.Fatalf("point_put used_ops_total=0, want command WAL publish piggyback to use span-native apply")
	}
	if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.point_put.fallback.reason.command_wal_barrier.count_total"); got != 0 {
		t.Fatalf("point_put command_wal_barrier count=%d, want 0 for span-native command WAL publish", got)
	}
	if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.route.point_put.fallback.reason.span_native_not_implemented.count_total"); got != 0 {
		t.Fatalf("point_put span_native_not_implemented count=%d, want 0 for explicit command WAL barrier", got)
	}
	if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.fallback.reason.command_wal_barrier.count_total"); got != 0 {
		t.Fatalf("raw command_wal_barrier count=%d, want 0", got)
	}
	if got := rawSpanNativeRouteStatUint(t, stats, "treedb.raw.span_native.fallback.reason.span_native_not_implemented.count_total"); got != 0 {
		t.Fatalf("raw span_native_not_implemented count=%d, want 0", got)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if got := reopened.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("reopened AppliedCommandLSN=%d, want 1", got)
	}
	got, err := reopened.Get(key)
	if err != nil {
		t.Fatalf("reopened Get: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("reopened value=%q want %q", got, value)
	}
}

func TestRawSpanNativePublishFallbackUsesPreReleaseSummarySnapshot(t *testing.T) {
	result := zipper.ApplyResult{
		ReadOnlyPrepareRequested: true,
		ReadOnlyPrepare: zipper.ReadOnlyPrepareResult{
			Ops:            3,
			PointOps:       3,
			ExactLeafSpans: true,
			LeafSpans: []zipper.ReadOnlyLeafSpan{
				{OpCount: 1, PointOpCount: 1, ByteCount: 10},
				{OpCount: 2, PointOpCount: 2, ByteCount: 20},
			},
		},
		SpanNativeEligible: true,
	}
	snapshot := newFlushApplySpanNativePublishSnapshot(result)

	for i := range result.ReadOnlyPrepare.LeafSpans {
		result.ReadOnlyPrepare.LeafSpans[i] = zipper.ReadOnlyLeafSpan{OpCount: 99, PointOpCount: 99, ByteCount: 99}
	}
	if got, want := snapshot.summary.SpanOps, 3; got != want {
		t.Fatalf("snapshot span ops=%d want %d", got, want)
	}
	if got, want := snapshot.summary.SpanBytes, 30; got != want {
		t.Fatalf("snapshot span bytes=%d want %d", got, want)
	}

	d := &DB{}
	reason := FlushSpanRunFallbackRootMismatch
	d.observeRawBatchSpanNativePublishFallback(rawSpanNativeBatchPlan{
		route: RawSpanNativeRoutePointPut,
		ops:   3,
	}, snapshot, reason)

	if got := d.flushApplySpanNativeFallbackOps[reason].Load(); got != 3 {
		t.Fatalf("flush fallback ops=%d want 3", got)
	}
	if got := d.flushApplySpanNativeFallbackSpans[reason].Load(); got != 2 {
		t.Fatalf("flush fallback spans=%d want 2", got)
	}
	if got := d.rawSpanNativeFallbackOps[reason].Load(); got != 3 {
		t.Fatalf("raw fallback ops=%d want 3", got)
	}
	if got := d.rawSpanNativeFallbackSpans[reason].Load(); got != 2 {
		t.Fatalf("raw fallback spans=%d want 2", got)
	}
	routeCounters := d.rawSpanNativeRouteCountersFor(RawSpanNativeRoutePointPut)
	if routeCounters == nil {
		t.Fatalf("missing point_put route counters")
	}
	if got := routeCounters.fallbackOps[reason].Load(); got != 3 {
		t.Fatalf("point_put fallback ops=%d want 3", got)
	}
	if got := routeCounters.fallbackSpans[reason].Load(); got != 2 {
		t.Fatalf("point_put fallback spans=%d want 2", got)
	}
}

func TestRawSpanNativePrepareErrorRequiresPrepareFailureFlag(t *testing.T) {
	d := openExplicitRawSpanNativeRouteTestDB(t)
	req := rawSpanNativeEligibilityRequest{
		route:                    RawSpanNativeRoutePointPut,
		deltaOps:                 1,
		readOnlyPrepareRequested: true,
		err:                      errors.New("later apply failure"),
		applyOptionsUsed:         true,
		spanNativeRequested:      true,
	}
	if got := d.rawSpanNativeEligibility(req).fallbackReason; got == FlushSpanRunFallbackPrepareError {
		t.Fatalf("fallbackReason=%s, want later apply failure to avoid prepare_error without ReadOnlyPrepareFailed", got)
	}

	req.readOnlyPrepareFailed = true
	if got := d.rawSpanNativeEligibility(req).fallbackReason; got != FlushSpanRunFallbackPrepareError {
		t.Fatalf("fallbackReason=%s, want prepare_error when ReadOnlyPrepareFailed is set", got)
	}
}

func TestRawSpanNativeCloseDrainDisabledPrecedesCloseFallback(t *testing.T) {
	d, err := Open(Options{
		Dir:                   t.TempDir(),
		FlushAdmissionPolicy:  FlushAdmissionPolicyExplicit,
		FlushApplySpanNative:  false,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	req := rawSpanNativeEligibilityRequest{
		route:                    RawSpanNativeRouteCloseOrCheckpointDrain,
		deltaOps:                 1,
		readOnlyPrepareRequested: true,
		applyOptionsUsed:         true,
		spanNativeRequested:      false,
	}
	if got := d.rawSpanNativeEligibility(req).fallbackReason; got != FlushSpanRunFallbackDisabled {
		t.Fatalf("fallbackReason=%s, want disabled for parallel-only close/checkpoint drains", got)
	}

	req.spanNativeRequested = true
	if got := d.rawSpanNativeEligibility(req).fallbackReason; got != FlushSpanRunFallbackCloseOrCheckpoint {
		t.Fatalf("fallbackReason=%s, want close_or_checkpoint when span-native is requested", got)
	}
}

func openExplicitRawSpanNativeRouteTestDB(t *testing.T) *DB {
	t.Helper()
	d, err := Open(Options{
		Dir:                   t.TempDir(),
		FlushAdmissionPolicy:  FlushAdmissionPolicyExplicit,
		FlushApplySpanNative:  true,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func rawSpanNativeRouteStatUint(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %s", key)
	}
	got, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %s=%q: %v", key, raw, err)
	}
	return got
}
