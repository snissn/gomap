package treedb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
)

// BenchmarkExternalRecoveryCheckpoint measures one existing-key mutation and
// its certified checkpoint, including ordinary TreeDB checkpoint work, resource
// retention, certificate serialization, all syncs and old-generation retirement.
// Fixture loading, the initial certificate, final recovery verification and Close
// are excluded. Run fixed counts (for example -benchtime=8x). This is a bounded
// component diagnostic; it does not certify sustained RSS/disk equilibrium or
// Dgraph RPC/transaction latency. Pointer4096 uses default compression and a
// repeated value, so N measures index/cardinality scale, not unique value bytes.
func BenchmarkExternalRecoveryCheckpoint(b *testing.B) {
	for _, layout := range []string{"Inline64", "Pointer4096"} {
		for _, records := range []int{1024, 16384, 131072} {
			b.Run(fmt.Sprintf("%s/N=%d/OneUpdateAndCheckpoint", layout, records), func(b *testing.B) {
				b.StopTimer()
				opts := OptionsFor(ProfileNoWALFast, b.TempDir())
				opts.MemtableMode, opts.MemtableShards = "cow_ordered", 2
				opts.BackgroundCheckpointInterval = -1
				opts.BackgroundCheckpointIdleDuration = -1
				opts.BackgroundIndexVacuumInterval = -1
				opts.DisableBackgroundPrune = true
				size := 64
				opts.ValueLog.PointerThreshold = 1 << 20
				if layout == "Pointer4096" {
					size = 4096
					opts.ValueLog.PointerThreshold = 1
					opts.ValueLog.ForcePointers = true
				}
				owner, err := OpenExternalRecovery(opts, ExternalRecoveryConfig{Identity: "checkpoint-benchmark"})
				if err != nil {
					b.Fatal(err)
				}
				defer func() {
					if owner == nil {
						return
					}
					if err := owner.Close(); err != nil {
						b.Error(err)
					}
				}()
				stats := owner.DB().Stats()
				if owner.DB().ResolvedProfile() != ProfileNoWALFast || stats["treedb.cache.memtable_mode"] != "cow_ordered" || owner.DB().commandWALCached {
					b.Fatal("external COW/no-WAL path not active")
				}
				seedValue := bytes.Repeat([]byte{0x5a}, size)
				if err := owner.Apply(func() error {
					batch := owner.DB().NewBatchWithSize(256)
					defer batch.Close()
					for i := 0; i < records; i++ {
						key := []byte(fmt.Sprintf("checkpoint/%08d", i))
						if err := batch.Set(key, seedValue); err != nil {
							return err
						}
						if (i+1)%256 == 0 {
							if err := batch.Write(); err != nil {
								return err
							}
						}
					}
					return batch.Write()
				}); err != nil {
					b.Fatal(err)
				}
				ctx := context.Background()
				initial, err := owner.Checkpoint(ctx, nil)
				if err != nil {
					b.Fatal(err)
				}
				key := []byte("checkpoint/00000000")
				value := bytes.Repeat([]byte{0x6b}, size)
				var metadata [8]byte
				var checkpointFileBytes int64
				var last ExternalRecoveryCheckpoint
				b.ReportAllocs()
				b.ResetTimer()
				b.StartTimer()
				for i := 0; i < b.N; i++ {
					if err := owner.Apply(func() error { return owner.DB().Set(key, value) }); err != nil {
						b.Fatal(err)
					}
					binary.LittleEndian.PutUint64(metadata[:], uint64(i+1))
					last, err = owner.Checkpoint(ctx, metadata[:])
					if err != nil {
						b.Fatal(err)
					}
					for _, file := range last.Files {
						checkpointFileBytes += file.Bytes
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(checkpointFileBytes)/float64(b.N), "checkpoint_files_B/op")
				if last.Generation != initial.Generation+uint64(b.N) || !bytes.Equal(last.Metadata, metadata[:]) {
					b.Fatal("checkpoint progress/metadata mismatch")
				}
				if err := owner.Close(); err != nil {
					b.Fatal(err)
				}
				owner, err = OpenExternalRecovery(opts, ExternalRecoveryConfig{Identity: "checkpoint-benchmark"})
				if err != nil {
					b.Fatal(err)
				}
				got, err := owner.DB().Get(key)
				if err != nil || !bytes.Equal(got, value) {
					b.Fatalf("certified reopen value=%d bytes err=%v", len(got), err)
				}
				recovered, ok := owner.RecoveredCheckpoint()
				if !ok || recovered.Digest != last.Digest {
					b.Fatal("reopened certificate mismatch")
				}
			})
		}
	}
}
