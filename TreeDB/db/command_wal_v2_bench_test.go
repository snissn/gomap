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
		for _, benchCase := range []struct {
			name    string
			frames  []commandWALV2PhysicalFrame
			hasRID  func(uint64) bool
			discard bool
		}{
			{name: "found_rid", frames: commandWALV2BenchFrames(frameCount), hasRID: func(uint64) bool { return true }},
			{name: "missing_rid_discard", frames: commandWALV2BenchMissingRIDFrames(frameCount), hasRID: func(uint64) bool { return false }, discard: true},
		} {
			b.Run(fmt.Sprintf("%s/frames_%d", benchCase.name, frameCount), func(b *testing.B) {
				classification, err := classifyCommandWALV2Frames(benchCase.frames, 0, benchCase.hasRID)
				if err != nil {
					b.Fatal(err)
				}
				if benchCase.discard && (len(classification.DiscardSuffix) == 0 || classification.Diagnostic.MissingRIDCount == 0) {
					b.Fatalf("benchmark setup missed discard diagnostics: %+v", classification.Diagnostic)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					classification, err := classifyCommandWALV2Frames(benchCase.frames, 0, benchCase.hasRID)
					if err != nil {
						b.Fatal(err)
					}
					commandWALV2BenchSink.classification = classification
				}
			})
		}
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

func commandWALV2BenchMissingRIDFrames(frameCount int) []commandWALV2PhysicalFrame {
	frames := commandWALV2BenchFrames(frameCount)
	for i := frameCount / 2; i < len(frames); i++ {
		frames[i].RequiredRIDs = []uint64{uint64(i + 1)}
	}
	return frames
}
