package db

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

var commandWALV2BenchSink struct {
	classification commandWALV2Classification
	stages         []string
}

func BenchmarkClassifyCommandWALV2Frames(b *testing.B) {
	for _, frameCount := range []int{32, 256, 2048} {
		b.Run(fmt.Sprintf("frames_%d", frameCount), func(b *testing.B) {
			frames := commandWALV2BenchFrames(frameCount)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				classification, err := classifyCommandWALV2Frames(frames, 0, func(uint64) bool { return true })
				if err != nil {
					b.Fatal(err)
				}
				commandWALV2BenchSink.classification = classification
			}
		})
	}
}

func BenchmarkPlanCommandWALV2SuffixRepair(b *testing.B) {
	for _, frameCount := range []int{32, 256, 2048} {
		b.Run(fmt.Sprintf("frames_%d_segments_8", frameCount), func(b *testing.B) {
			frames := commandWALV2BenchFrames(frameCount)
			classification := commandWALV2Classification{
				CompletePrefix: frames[:1],
				DiscardSuffix:  frames[1:],
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stages, _ := planCommandWALV2RepairStages(classification)
				commandWALV2BenchSink.stages = stages
			}
		})
	}
}

func commandWALV2BenchFrames(frameCount int) []commandWALV2PhysicalFrame {
	frames := make([]commandWALV2PhysicalFrame, frameCount)
	for i := range frames {
		class := commitlog.CommandDurabilityRelaxed
		if i == 0 {
			class = commitlog.CommandDurabilityDurable
		}
		lane := i % 2
		segment := (i % 8) + 1
		path := filepath.Join("/bench/wal", commitlog.CommandSegmentName(lane, uint64(segment)))
		frames[i] = commandWALV2PhysicalFrame{
			Envelope: commitlog.CommandEnvelope{
				Version:         commitlog.CommandFrameVersionV2,
				DurabilityClass: class,
				LSN:             uint64(i + 1),
				Kind:            commitlog.CommandKindRawKVBatch,
				Scope:           commitlog.CommandScopeRawKV,
				PayloadFormat:   commitlog.PayloadFormatRawKVBatchV1,
			},
			Coordinate: commandWALV2Coordinate{
				Lane:            lane,
				SegmentSequence: uint64(segment),
				StartOffset:     int64(i * 128),
				EndOffset:       int64((i + 1) * 128),
				SourceSegment:   path,
			},
		}
	}
	return frames
}
