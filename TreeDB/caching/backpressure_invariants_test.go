package caching

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

type bpScenario struct {
	name string
	opts Options
	run  func(t *testing.T, db *DB)
}

type bpInvariant struct {
	name string
	fn   func(t *testing.T, stats map[string]string)
}

func TestBackpressureInvariantsAcrossWorkloads(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	baseOpts := Options{
		FlushThreshold:          1 << 20,
		MemtableShards:          1,
		MaxBacklogBytes:         1 << 20,
		SlowdownBacklogSeconds:  0.25,
		StopBacklogSeconds:      0.5,
		WriterFlushMaxMemtables: 2,
		DisableWAL:              true,
		AllowUnsafe:             true,
	}

	scenarios := []bpScenario{
		{
			name: "sequential_sets",
			opts: baseOpts,
			run: func(t *testing.T, db *DB) {
				val := bytes.Repeat([]byte("a"), 512)
				for i := 0; i < 2000; i++ {
					key := []byte(fmt.Sprintf("k%04d", i))
					if err := db.Set(key, val); err != nil {
						t.Fatalf("Set: %v", err)
					}
				}
				db.flushAll(false)
			},
		},
		{
			name: "batch_writes",
			opts: baseOpts,
			run: func(t *testing.T, db *DB) {
				val := bytes.Repeat([]byte("b"), 256)
				for i := 0; i < 4; i++ {
					b := db.NewBatch()
					for j := 0; j < 500; j++ {
						key := []byte(fmt.Sprintf("b%02d-%04d", i, j))
						if err := b.Set(key, val); err != nil {
							_ = b.Close()
							t.Fatalf("Batch.Set: %v", err)
						}
					}
					if err := b.Write(); err != nil {
						_ = b.Close()
						t.Fatalf("Batch.Write: %v", err)
					}
					if err := b.Close(); err != nil {
						t.Fatalf("Batch.Close: %v", err)
					}
				}
				db.flushAll(false)
			},
		},
		{
			name: "delete_overwrite_mix",
			opts: baseOpts,
			run: func(t *testing.T, db *DB) {
				if err := db.Set([]byte("keep"), []byte("v1")); err != nil {
					t.Fatalf("Set keep: %v", err)
				}
				if err := db.Set([]byte("del"), []byte("gone")); err != nil {
					t.Fatalf("Set del: %v", err)
				}
				if err := db.Delete([]byte("del")); err != nil {
					t.Fatalf("Delete: %v", err)
				}
				if err := db.Set([]byte("keep"), []byte("v2")); err != nil {
					t.Fatalf("Set overwrite: %v", err)
				}
				db.flushAll(false)
			},
		},
		{
			name: "manual_rotation",
			opts: baseOpts,
			run: func(t *testing.T, db *DB) {
				val := bytes.Repeat([]byte("r"), 128)
				for i := 0; i < 10; i++ {
					if err := db.Set([]byte(fmt.Sprintf("r%02d", i)), val); err != nil {
						t.Fatalf("Set: %v", err)
					}
					if i%3 == 2 {
						db.mu.Lock()
						if err := db.rotateMemtableLocked(false); err != nil {
							db.mu.Unlock()
							t.Fatalf("rotateMemtableLocked: %v", err)
						}
						db.mu.Unlock()
					}
				}
				db.flushAll(false)
			},
		},
		{
			name: "stop_backpressure_wait",
			opts: baseOpts,
			run: func(t *testing.T, db *DB) {
				val := bytes.Repeat([]byte("s"), 2048)
				db.mu.Lock()
				for i := 0; i < 3; i++ {
					setMutable(db, []byte{byte('a' + i)}, val)
					if err := db.rotateMemtableLocked(false); err != nil {
						db.mu.Unlock()
						t.Fatalf("rotateMemtableLocked: %v", err)
					}
				}
				db.mu.Unlock()
				done := make(chan struct{})
				go func() {
					db.waitForStop()
					close(done)
				}()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatalf("waitForStop timeout")
				}
				db.flushAll(false)
			},
		},
		{
			name: "concurrent_flush_trigger",
			opts: baseOpts,
			run: func(t *testing.T, db *DB) {
				val := bytes.Repeat([]byte("t"), 1024)
				for i := 0; i < 10; i++ {
					if err := db.Set([]byte(fmt.Sprintf("t%02d", i)), val); err != nil {
						t.Fatalf("Set: %v", err)
					}
				}
				db.TriggerFlush()
				db.flushAll(false)
			},
		},
	}

	invariants := []bpInvariant{
		{
			name: "laneid_misses_zero",
			fn: func(t *testing.T, stats map[string]string) {
				if got := mustStatInt64(t, stats, "treedb.cache.queue_laneid_misses"); got != 0 {
					t.Fatalf("queue_laneid_misses=%d want 0", got)
				}
			},
		},
		{
			name: "queue_len_nonnegative",
			fn: func(t *testing.T, stats map[string]string) {
				if got := mustStatInt64(t, stats, "treedb.cache.queue_len"); got < 0 {
					t.Fatalf("queue_len=%d want >=0", got)
				}
			},
		},
		{
			name: "backlog_nonnegative",
			fn: func(t *testing.T, stats map[string]string) {
				if got := mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes"); got < 0 {
					t.Fatalf("queue_backlog_bytes=%d want >=0", got)
				}
			},
		},
		{
			name: "backlog_zero_when_queue_empty",
			fn: func(t *testing.T, stats map[string]string) {
				queueLen := mustStatInt64(t, stats, "treedb.cache.queue_len")
				backlog := mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes")
				if queueLen == 0 && backlog != 0 {
					t.Fatalf("queue_len=0 but backlog=%d", backlog)
				}
			},
		},
		{
			name: "flush_bps_ewma_nonnegative",
			fn: func(t *testing.T, stats map[string]string) {
				if got := mustStatInt64(t, stats, "treedb.cache.flush_bps_ewma"); got < 0 {
					t.Fatalf("flush_bps_ewma=%d want >=0", got)
				}
			},
		},
		{
			name: "flush_bps_ewma_when_backlog_remains",
			fn: func(t *testing.T, stats map[string]string) {
				backlog := mustStatInt64(t, stats, "treedb.cache.queue_backlog_bytes")
				ewma := mustStatInt64(t, stats, "treedb.cache.flush_bps_ewma")
				if backlog > 0 && ewma == 0 {
					t.Fatalf("backlog=%d but flush_bps_ewma=0", backlog)
				}
			},
		},
		{
			name: "wal_estimates_nonnegative",
			fn: func(t *testing.T, stats map[string]string) {
				if got := mustStatInt64(t, stats, "treedb.cache.wal_bytes_estimate"); got < 0 {
					t.Fatalf("wal_bytes_estimate=%d want >=0", got)
				}
				if got := mustStatInt64(t, stats, "treedb.cache.wal_closed_bytes_estimate"); got < 0 {
					t.Fatalf("wal_closed_bytes_estimate=%d want >=0", got)
				}
				if got := mustStatInt64(t, stats, "treedb.cache.wal_current_bytes_estimate"); got < 0 {
					t.Fatalf("wal_current_bytes_estimate=%d want >=0", got)
				}
			},
		},
		{
			name: "vlog_estimates_nonnegative",
			fn: func(t *testing.T, stats map[string]string) {
				if got := mustStatInt64(t, stats, "treedb.cache.vlog_retained_segments"); got < 0 {
					t.Fatalf("vlog_retained_segments=%d want >=0", got)
				}
				if got := mustStatInt64(t, stats, "treedb.cache.vlog_retained_bytes_estimate"); got < 0 {
					t.Fatalf("vlog_retained_bytes_estimate=%d want >=0", got)
				}
			},
		},
		{
			name: "backpressure_mode_matches_config",
			fn: func(t *testing.T, stats map[string]string) {
				if got := mustStatString(t, stats, "treedb.cache.backpressure_mode"); got != "adaptive" {
					t.Fatalf("backpressure_mode=%q want adaptive", got)
				}
			},
		},
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			db, err := Open(dir, backend, sc.opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })

			sc.run(t, db)
			stats := db.Stats()
			for _, inv := range invariants {
				inv.fn(t, stats)
			}
		})
	}
}
