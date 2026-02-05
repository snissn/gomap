package caching

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func BenchmarkValueLogDictFramePipelineThroughput_NoIO(b *testing.B) {
	const (
		dictID    = 1
		fileID    = 123
		valueSize = 4 << 10
		valueCnt  = 2048
		k         = 32
	)

	values := make([][]byte, valueCnt)
	pattern := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	for i := 0; i < valueCnt; i++ {
		v := make([]byte, valueSize)
		copy(v, bytes.Repeat(pattern, (valueSize/len(pattern))+1))
		binary.LittleEndian.PutUint32(v[valueSize-4:], uint32(i))
		values[i] = v
	}

	const historyBytes = 32 << 10
	history := make([]byte, 0, historyBytes)
	for _, v := range values {
		if len(history) >= historyBytes {
			break
		}
		need := historyBytes - len(history)
		if len(v) > need {
			history = append(history, v[:need]...)
		} else {
			history = append(history, v...)
		}
	}
	if len(history) < 8 {
		b.Fatalf("history too small: %d", len(history))
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       dictID,
		Contents: values[:128],
		History:  history,
		Level:    zstd.SpeedFastest,
	})
	if err != nil || len(dict) == 0 {
		b.Fatalf("BuildDict failed: %v", err)
	}

	records := make([]valuelog.Record, valueCnt)
	for i := range records {
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: values[i]}
	}

	rawBytesPerOp := int64(valueCnt * valueSize)
	rawPayloadBytes := int(rawBytesPerOp)
	b.SetBytes(rawBytesPerOp)

	b.Run("sequential", func(b *testing.B) {
		writer := valuelog.NewWriterWithSink(io.Discard, fileID)
		writer.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)

		ptrs := make([]page.ValuePtr, len(records))
		for off := 0; off < len(records); off += k {
			end := off + k
			if end > len(records) {
				end = len(records)
			}
			if _, _, err := writer.AppendFrameWithStatsInto(dictID, dict, records[off:end], ptrs[off:end]); err != nil {
				b.Fatalf("warmup AppendFrameWithStatsInto: %v", err)
			}
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for off := 0; off < len(records); off += k {
				end := off + k
				if end > len(records) {
					end = len(records)
				}
				if _, _, err := writer.AppendFrameWithStatsInto(dictID, dict, records[off:end], ptrs[off:end]); err != nil {
					b.Fatalf("AppendFrameWithStatsInto: %v", err)
				}
			}
		}
	})

	benchPipeline := func(b *testing.B, workers int) {
		writer := valuelog.NewWriterWithSink(io.Discard, fileID)
		writer.SetDictFrameEncoderOptions(zstd.SpeedFastest, false)

		db := &DB{
			closeCh:                          make(chan struct{}),
			valueLogDictFramePipelineWorkers: workers,
			valueLogDictFramePipelineMaxInFlightBytes: int64(workers) * (16 << 20),
			valueLogDictFrameEncodeLevel:              zstd.SpeedFastest,
			valueLogDictFrameEnableEntropy:            false,
		}

		ptrs := make([]page.ValuePtr, len(records))
		if _, _, _, _, _, _, _, _, err := db.appendValueLogDictFramesPipeline(writer, dictID, dict, records, rawPayloadBytes, k, ptrs); err != nil {
			b.Fatalf("warmup appendValueLogDictFramesPipeline: %v", err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, _, _, _, _, _, _, err := db.appendValueLogDictFramesPipeline(writer, dictID, dict, records, rawPayloadBytes, k, ptrs); err != nil {
				b.Fatalf("appendValueLogDictFramesPipeline: %v", err)
			}
		}
		b.StopTimer()
		close(db.closeCh)
		db.wg.Wait()
	}

	b.Run("pipeline/w=4", func(b *testing.B) { benchPipeline(b, 4) })
	b.Run("pipeline/w=8", func(b *testing.B) { benchPipeline(b, 8) })
}
