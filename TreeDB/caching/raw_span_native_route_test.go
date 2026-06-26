package caching

import (
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCachingRawSpanNativeCheckpointAndCloseDrainRoutes(t *testing.T) {
	t.Run("checkpoint point flush", func(t *testing.T) {
		cached, backend := openCachingRawSpanNativeRouteTestDB(t)
		if err := backend.Set([]byte("seed"), []byte("backend")); err != nil {
			t.Fatalf("backend seed Set: %v", err)
		}
		if err := cached.Set([]byte("checkpoint-1"), []byte("one")); err != nil {
			t.Fatalf("cached Set checkpoint-1: %v", err)
		}
		if err := cached.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint 1: %v", err)
		}
		if err := cached.Set([]byte("checkpoint-2"), []byte("two")); err != nil {
			t.Fatalf("cached Set checkpoint-2: %v", err)
		}
		if err := cached.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint 2: %v", err)
		}
		stats := backend.Stats()
		if got := cachingRawSpanNativeStatUint(t, stats, "treedb.raw.span_native.route.point_put.observations_total"); got == 0 {
			t.Fatalf("point_put observations_total=0, want checkpoint flush route")
		}
		if got := cachingRawSpanNativeStatUint(t, stats, "treedb.raw.span_native.route.point_put.fallback.reason.unknown.count_total"); got != 0 {
			t.Fatalf("point_put unknown fallbacks=%d want 0", got)
		}
	})

	t.Run("close drain fallback", func(t *testing.T) {
		cached, backend := openCachingRawSpanNativeRouteTestDB(t)
		if err := backend.Set([]byte("seed"), []byte("backend")); err != nil {
			t.Fatalf("backend seed Set: %v", err)
		}
		enqueuePointMemtables(t, cached, 1, "close")
		cached.closing.Store(true)
		if !cached.flushLaneOnceWithCollectionMode(false, 0, nil, flushCollectionBackground) {
			t.Fatalf("flushLaneOnceWithCollectionMode returned false")
		}
		cached.closing.Store(false)
		stats := backend.Stats()
		if got := cachingRawSpanNativeStatUint(t, stats, "treedb.raw.span_native.route.close_or_checkpoint_drain.fallback.reason.close_or_checkpoint.count_total"); got == 0 {
			t.Fatalf("close_or_checkpoint_drain close_or_checkpoint count=0, want close drain fallback")
		}
	})
}

func openCachingRawSpanNativeRouteTestDB(t *testing.T) (*DB, *backenddb.DB) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                   dir,
		FlushAdmissionPolicy:  backenddb.FlushAdmissionPolicyExplicit,
		FlushApplySpanNative:  true,
		FlushApplyConcurrency: 4,
		FlushApplyMinEntries:  1,
		FlushApplyMinSpans:    1,
		FlushApplyMinBytes:    1,
	})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cached Open: %v", err)
	}
	t.Cleanup(func() {
		_ = cached.Close()
		_ = backend.Close()
	})
	return cached, backend
}

func cachingRawSpanNativeStatUint(t *testing.T, stats map[string]string, key string) uint64 {
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
