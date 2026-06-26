package db

import (
	"runtime"
	"strconv"
	"testing"
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
