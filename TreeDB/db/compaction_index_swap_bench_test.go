package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/slab"
)

func sumSlabBytes(dir string) (int64, int) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0
	}
	var total int64
	count := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "data-") || !strings.HasSuffix(name, ".slab") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		total += info.Size()
		count++
	}
	return total, count
}

func BenchmarkCompactionIndexSwapPointerValues(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip("CompactSlabsIndexSwap unsupported on windows")
	}

	oldMax := slab.MaxSlabSize
	slab.MaxSlabSize = slab.SlabV2DataStart + (256 << 10)
	b.Cleanup(func() { slab.MaxSlabSize = oldMax })

	value := bytes.Repeat([]byte("v"), 1024)
	updated := bytes.Repeat([]byte("u"), 1024)

	var lastStats IndexSwapCompactionStats
	var lastSlabBytes int64
	var lastSlabCount int

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
		lastSlabBytes, lastSlabCount = sumSlabBytes(dir)
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
	if lastStats.SampleShiftDictSampleBytes > 0 {
		b.ReportMetric(float64(lastStats.SampleShiftDictSampleBytes), "sample_shift_dict_sample_bytes")
	}
	if lastStats.SampleShiftDictSampleRecords > 0 {
		b.ReportMetric(float64(lastStats.SampleShiftDictSampleRecords), "sample_shift_dict_sample_records")
	}
	if lastStats.SampleShiftDictBytes > 0 {
		b.ReportMetric(float64(lastStats.SampleShiftDictBytes), "sample_shift_dict_bytes")
	}
	if lastStats.SampleShiftDictRatio > 0 {
		b.ReportMetric(lastStats.SampleShiftDictRatio, "sample_shift_dict_ratio")
	}
	if lastStats.ShiftOverrideRecords > 0 {
		b.ReportMetric(float64(lastStats.ShiftOverrideRecords), "shift_override_records")
	}
	if lastStats.ShiftOverrideBytes > 0 {
		b.ReportMetric(float64(lastStats.ShiftOverrideBytes), "shift_override_bytes")
	}
	if lastStats.DisableAllSlabs > 0 {
		b.ReportMetric(float64(lastStats.DisableAllSlabs), "disable_all_slabs")
	}
	if lastSlabBytes > 0 {
		b.ReportMetric(float64(lastSlabBytes), "slab_bytes_total")
	}
	if lastSlabCount > 0 {
		b.ReportMetric(float64(lastSlabCount), "slab_file_count")
	}
}
