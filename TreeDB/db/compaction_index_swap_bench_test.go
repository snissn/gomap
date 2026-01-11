package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/slab"
)

func BenchmarkCompactionIndexSwapPointerValues(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip("CompactSlabsIndexSwap unsupported on windows")
	}

	oldMax := slab.MaxSlabSize
	slab.MaxSlabSize = 256 << 10
	b.Cleanup(func() { slab.MaxSlabSize = oldMax })

	value := bytes.Repeat([]byte("v"), 1024)
	updated := bytes.Repeat([]byte("u"), 1024)

	var lastStats IndexSwapCompactionStats

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		dir := b.TempDir()
		opts := Options{
			Dir:                dir,
			ForceValuePointers: true,
			OmitSlabKeys:       true,
			SlabCompression: slab.CompressionOptions{
				Kind:            slab.CompressionZSTD,
				MinBytes:        1024,
				MinSavingsBytes: 0,
			},
			SlabCompressionAdaptiveTrainBytes:     1 << 20,
			SlabCompressionAdaptiveTrainDictBytes: 32 << 10,
		}
		d, err := Open(opts)
		if err != nil {
			b.Fatalf("Open: %v", err)
		}

		for k := 0; k < 2000; k++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(k))
			if err := d.Set(key, value); err != nil {
				_ = d.Close()
				b.Fatalf("Set: %v", err)
			}
		}
		for k := 0; k < 200; k++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(k))
			if err := d.Set(key, updated); err != nil {
				_ = d.Close()
				b.Fatalf("Set update: %v", err)
			}
		}

		if _, err := d.SlabManager().Rotate(); err != nil {
			_ = d.Close()
			b.Fatalf("Rotate: %v", err)
		}

		stats := IndexSwapCompactionStats{}
		b.StartTimer()
		if err := d.CompactSlabsIndexSwap(context.Background(), []uint32{0}, IndexSwapCompactionOptions{
			Stats:                     &stats,
			SampleCompressionDict:     true,
			ApplyCompressionShiftPlan: true,
			ShiftWindowDivisor:        1,
			ShiftMinWindowBytes:       32 << 10,
			ShiftRatioToleranceSet:    true,
			ShiftRatioTolerance:       -0.1,
			ShiftMaxPoints:            64,
		}); err != nil {
			b.StopTimer()
			_ = d.Close()
			b.Fatalf("CompactSlabsIndexSwap: %v", err)
		}
		b.StopTimer()

		lastStats = stats
		if err := d.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}

	if lastStats.RemapCount > 0 {
		b.ReportMetric(float64(lastStats.RemapCount), "remap_ops")
	}
	if lastStats.RemapBytes > 0 {
		b.ReportMetric(float64(lastStats.RemapBytes), "remap_bytes")
	}
	if lastStats.SlabWriteBytes > 0 {
		b.ReportMetric(float64(lastStats.SlabWriteBytes), "slab_write_bytes")
	}
	if lastStats.SlabDeadBytes > 0 {
		b.ReportMetric(float64(lastStats.SlabDeadBytes), "slab_dead_bytes")
	}
	if lastStats.SampleDictBytes > 0 {
		b.ReportMetric(float64(lastStats.SampleDictBytes), "sample_dict_bytes")
	}
	if lastStats.SampleDictRatio > 0 {
		b.ReportMetric(lastStats.SampleDictRatio, "sample_dict_ratio")
	}
	if lastStats.SampleBaseRatio > 0 {
		b.ReportMetric(lastStats.SampleBaseRatio, "sample_base_ratio")
	}
	if lastStats.SampleBaseBytes > 0 {
		b.ReportMetric(float64(lastStats.SampleBaseBytes), "sample_base_bytes")
	}
	if lastStats.SampleBaseStored > 0 {
		b.ReportMetric(float64(lastStats.SampleBaseStored), "sample_base_stored")
	}
	if lastStats.SampleBaseRecords > 0 {
		b.ReportMetric(float64(lastStats.SampleBaseRecords), "sample_base_records")
	}
	if lastStats.SampleCandidates > 0 {
		b.ReportMetric(float64(lastStats.SampleCandidates), "sample_candidates")
	}
	if lastStats.SampleRecords > 0 {
		b.ReportMetric(float64(lastStats.SampleRecords), "sample_records")
	}
	if lastStats.SampleShiftPoints > 0 {
		b.ReportMetric(float64(lastStats.SampleShiftPoints), "sample_shift_points")
	}
	if lastStats.SampleShiftWorstRatio > 0 {
		b.ReportMetric(lastStats.SampleShiftWorstRatio, "sample_shift_worst_ratio")
	}
	if lastStats.SampleShiftAvgRatio > 0 {
		b.ReportMetric(lastStats.SampleShiftAvgRatio, "sample_shift_avg_ratio")
	}
	if lastStats.SampleShiftBytes > 0 {
		b.ReportMetric(float64(lastStats.SampleShiftBytes), "sample_shift_bytes")
	}
	if lastStats.SampleShiftRecords > 0 {
		b.ReportMetric(float64(lastStats.SampleShiftRecords), "sample_shift_records")
	}
	if lastStats.ShiftOverrideRecords > 0 {
		b.ReportMetric(float64(lastStats.ShiftOverrideRecords), "shift_override_records")
	}
	if lastStats.ShiftOverrideBytes > 0 {
		b.ReportMetric(float64(lastStats.ShiftOverrideBytes), "shift_override_bytes")
	}
}
