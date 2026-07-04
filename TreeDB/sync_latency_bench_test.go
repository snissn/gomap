package treedb

import (
	"encoding/binary"
	"testing"
)

func TestSyncDurabilityBoundaryDoesNotCheckpoint(t *testing.T) {
	tests := []struct {
		name       string
		durability DurabilityMode
		commandWAL bool
	}{
		{name: "wal_on_sync", durability: DurabilityDurable},
		{name: "wal_on_sync_command_wal", durability: DurabilityDurable, commandWAL: true},
		{name: "wal_on_relaxed_sync", durability: DurabilityWALOnRelaxed},
		{name: "wal_on_relaxed_sync_command_wal", durability: DurabilityWALOnRelaxed, commandWAL: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := Open(Options{
				Dir:        t.TempDir(),
				Durability: tt.durability,
				CommandWAL: tt.commandWAL,
			})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = d.Close() }()

			val := make([]byte, 128)
			var key [8]byte
			for i := 0; i < 16; i++ {
				binary.BigEndian.PutUint64(key[:], uint64(i))
				if err := d.SetSync(key[:], val); err != nil {
					t.Fatalf("SetSync(%d): %v", i, err)
				}
			}

			stats := d.Stats()
			if got := mustStatUint64(t, stats, "treedb.cache.checkpoint.runs"); got != 0 {
				t.Fatalf("checkpoint.runs=%d, want 0 before explicit Checkpoint/Close", got)
			}
		})
	}
}

func BenchmarkSyncLatencyCached(b *testing.B) {
	tests := []struct {
		name       string
		durability DurabilityMode
		commandWAL bool
		batchSize  int
		byteHint   bool
	}{
		{name: "SetSync/wal_on_sync", durability: DurabilityDurable},
		{name: "SetSync/wal_on_sync_command_wal", durability: DurabilityDurable, commandWAL: true},
		{name: "SetSync/wal_on_relaxed_sync", durability: DurabilityWALOnRelaxed},
		{name: "SetSync/wal_on_relaxed_sync_command_wal", durability: DurabilityWALOnRelaxed, commandWAL: true},
		{name: "BatchWriteSync/wal_on_sync/32/entry_hint", durability: DurabilityDurable, batchSize: 32},
		{name: "BatchWriteSync/wal_on_sync_command_wal/32/entry_hint", durability: DurabilityDurable, commandWAL: true, batchSize: 32},
		{name: "BatchWriteSync/wal_on_relaxed_sync/32/entry_hint", durability: DurabilityWALOnRelaxed, batchSize: 32},
		{name: "BatchWriteSync/wal_on_relaxed_sync_command_wal/32/entry_hint", durability: DurabilityWALOnRelaxed, commandWAL: true, batchSize: 32},
		{name: "BatchWriteSync/wal_on_relaxed_sync_command_wal/32/byte_hint", durability: DurabilityWALOnRelaxed, commandWAL: true, batchSize: 32, byteHint: true},
		{name: "BatchWriteSync/wal_on_sync_command_wal/128/entry_hint", durability: DurabilityDurable, commandWAL: true, batchSize: 128},
		{name: "BatchWriteSync/wal_on_relaxed_sync_command_wal/128/entry_hint", durability: DurabilityWALOnRelaxed, commandWAL: true, batchSize: 128},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			d, err := Open(Options{
				Dir:        b.TempDir(),
				Durability: tt.durability,
				CommandWAL: tt.commandWAL,
			})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer func() { _ = d.Close() }()

			val := make([]byte, 128)

			batchSize := tt.batchSize
			if batchSize <= 0 {
				batchSize = 1
			}
			batchHint := batchSize
			if tt.byteHint {
				batchHint = batchSize * (8 + len(val))
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if batchSize == 1 {
					var key [8]byte
					binary.BigEndian.PutUint64(key[:], uint64(i))
					if err := d.SetSync(key[:], val); err != nil {
						b.Fatalf("SetSync: %v", err)
					}
					continue
				}

				batch := d.NewBatchWithSize(batchHint)
				if batch == nil {
					b.Fatal("NewBatchWithSize returned nil")
				}
				base := uint64(i * batchSize)
				for j := 0; j < batchSize; j++ {
					var key [8]byte
					binary.BigEndian.PutUint64(key[:], base+uint64(j))
					if err := batch.Set(key[:], val); err != nil {
						_ = batch.Close()
						b.Fatalf("batch Set(%d): %v", j, err)
					}
				}
				if err := batch.WriteSync(); err != nil {
					_ = batch.Close()
					b.Fatalf("batch WriteSync: %v", err)
				}
				if err := batch.Close(); err != nil {
					b.Fatalf("batch Close: %v", err)
				}
			}
			b.StopTimer()

			totalOps := b.N * batchSize
			if elapsed := b.Elapsed(); elapsed > 0 {
				b.ReportMetric(float64(totalOps)/elapsed.Seconds(), "writes/s")
			}
			b.ReportMetric(float64(batchHint), "batch_hint")
			b.ReportMetric(float64(batchSize), "writes/op")
			reportSyncLatencyStats(b, d)
		})
	}
}

func reportSyncLatencyStats(b *testing.B, d *DB) {
	b.Helper()
	if b.N <= 1 {
		return
	}
	stats := d.Stats()
	for _, key := range []string{
		"treedb.durability_mode",
		"treedb.write_path.mode",
		"treedb.write_path.redo_log",
		"treedb.cache.memtable_mode",
		"treedb.cache.queue_len",
		"treedb.cache.mutable_bytes",
		"treedb.cache.checkpoint.runs",
		"treedb.cache.checkpoint.total_ms",
		"treedb.cache.checkpoint.max_ms",
		"treedb.cache.checkpoint.stage.flush_all.samples",
		"treedb.cache.checkpoint.stage.flush_all.total_ns",
		"treedb.cache.checkpoint.stage.value_log_flush.samples",
		"treedb.cache.checkpoint.stage.value_log_flush.total_ns",
		"treedb.cache.checkpoint.stage.command_wal_publish.samples",
		"treedb.cache.checkpoint.stage.command_wal_publish.total_ns",
		"treedb.cache.checkpoint.stage.backend_boundary.samples",
		"treedb.cache.checkpoint.stage.backend_boundary.total_ns",
		"treedb.cache.append_only.entry_pool_gets_total",
		"treedb.cache.append_only.entry_pool_puts_total",
		"treedb.cache.append_only.entry_pool_drops_total",
		"treedb.cache.append_only.entry_pool_drop_bytes_total",
		"treedb.cache.append_only.entry_pool_admission_drops_total",
		"treedb.cache.append_only.entry_pool_retained_bytes_estimate",
		"treedb.cache.append_only.mutable_from_pool_total",
		"treedb.cache.append_only.mutable_from_lease_total",
		"treedb.cache.append_only.mutable_new_alloc_total",
		"treedb.cache.append_only.mutable_pool_entry_backing_dropped_bytes_total",
		"treedb.cache.append_only.reserve.calls_total",
		"treedb.cache.append_only.reserve.grow_calls_total",
		"treedb.cache.append_only.reserve.grow_bytes_total",
		"treedb.cache.append_only.value_arena_pool_gets_total",
		"treedb.cache.append_only.value_arena_pool_puts_total",
		"treedb.cache.append_only.value_arena_pool_drop_bytes_total",
		"treedb.cache.vlog_shape.segments_total",
		"treedb.cache.vlog_shape.bytes_total",
		"treedb.cache.vlog_queue.depth_max",
		"treedb.cache.vlog_queue.lag_max_ms",
		"treedb.process.memory.rss_peak_bytes",
		"treedb.process.memory.heap_alloc_peak_bytes",
	} {
		if value, ok := stats[key]; ok {
			b.Logf("%s=%s", key, value)
		}
	}
	b.Logf("sync_latency_summary=stats_keys=%d", len(stats))
}
