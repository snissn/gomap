package valuelog

import (
	"runtime"
	"testing"

	"github.com/snissn/compress/zstd"
)

func BenchmarkFramePreparerDictIronbirdLikeHistoryReuse(b *testing.B) {
	const (
		dictID      = uint64(3553)
		valueSize   = 4096
		recordCount = 8
		valueCount  = 128
	)

	values := make([][]byte, valueCount)
	for i := range values {
		values[i] = makeSyntheticDictSampleForBench(i, valueSize)
	}
	dict, err := buildBenchDictWithHistory(uint32(dictID), values, 32<<10)
	if err != nil {
		b.Fatalf("build dict: %v", err)
	}

	records := make([]Record, recordCount)
	fillRecords := func(base int) {
		for i := range records {
			records[i] = Record{
				RID:   uint64(base + i + 1),
				Value: values[(base+i)%len(values)],
			}
		}
	}

	prep := NewFramePreparer()
	prep.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)
	fillRecords(0)
	body, stats, err := prep.PrepareFrameInto(nil, dictID, dict, records)
	if err != nil {
		b.Fatalf("warm PrepareFrameInto: %v", err)
	}
	if !stats.Attempted || !stats.Kept {
		b.Fatalf("warm frame stats attempted=%v kept=%v raw=%d stored=%d", stats.Attempted, stats.Kept, stats.RawPayloadBytes, stats.StoredPayloadBytes)
	}

	b.ReportAllocs()
	b.SetBytes(int64(valueSize * recordCount))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Two GC cycles clear sync.Pool victims, matching the Ironbird profile shape
		// where pooled zstd encoders were repeatedly recreated between batches.
		runtime.GC()
		runtime.GC()
		fillRecords(i * recordCount)
		var prepErr error
		body, stats, prepErr = prep.PrepareFrameInto(body[:0], dictID, dict, records)
		if prepErr != nil {
			b.Fatalf("PrepareFrameInto: %v", prepErr)
		}
		if !stats.Attempted || !stats.Kept {
			b.Fatalf("frame stats attempted=%v kept=%v raw=%d stored=%d", stats.Attempted, stats.Kept, stats.RawPayloadBytes, stats.StoredPayloadBytes)
		}
	}
	b.StopTimer()
	runtime.KeepAlive(prep)
	runtime.KeepAlive(body)
}
