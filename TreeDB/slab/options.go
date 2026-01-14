package slab

import "github.com/snissn/gomap/TreeDB/internal/compression"

type CompressionKind = compression.Kind

const (
	CompressionNone = compression.KindNone
	CompressionZSTD = compression.KindZSTD
)

type CompressionOptions = compression.Options

type CompressionTrainConfig = compression.TrainConfig

type Options struct {
	Compression                            CompressionOptions
	CompressionDisableFullRecord           bool
	OmitSlabKeys                           bool
	CompressionMetrics                     bool
	CompressionMetricsWindowBytes          int
	CompressionAdaptiveRatio               float64
	CompressionAdaptivePauseBytes          int
	CompressionAdaptiveMinRecords          int
	CompressionAdaptiveTrainBytes          int
	CompressionAdaptiveTrainDictBytes      int
	CompressionAdaptiveTrainMinRecords     int
	CompressionAdaptiveTrainMaxRecordBytes int
	CompressionAdaptiveTrainSampleStride   int
	CompressionAdaptiveTrainDedupWindow    int
	CompressionAdaptiveProbeBytes          int
	CompressionAdaptivePauseSampleStride   int
}

func (o Options) ToTrainConfig() compression.TrainConfig {
	return compression.TrainConfig{
		TrainBytes:     o.CompressionAdaptiveTrainBytes,
		DictBytes:      o.CompressionAdaptiveTrainDictBytes,
		MinRecords:     o.CompressionAdaptiveTrainMinRecords,
		MaxRecordBytes: o.CompressionAdaptiveTrainMaxRecordBytes,
		SampleStride:   o.CompressionAdaptiveTrainSampleStride,
		DedupWindow:    o.CompressionAdaptiveTrainDedupWindow,
		Level:          o.Compression.Level,
	}
}

func (o Options) ToMetricsOptions() compression.MetricsOptions {
	return compression.MetricsOptions{
		MetricsEnabled: o.CompressionMetrics,
		AdaptiveRatio:  o.CompressionAdaptiveRatio,
		WindowBytes:    o.CompressionMetricsWindowBytes,
		MinRecords:     o.CompressionAdaptiveMinRecords,
		PauseBytes:     o.CompressionAdaptivePauseBytes,
	}
}
