package treedb

import (
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/compression"
)

func TestApplyEnvMaintenanceOverrides_VlogCompressionModes(t *testing.T) {
	t.Run("block", func(t *testing.T) {
		opts := Options{}
		t.Setenv(envVlogCompression, "block")
		applyEnvMaintenanceOverrides(&opts)
		if got := opts.ValueLog.Compression; got != ValueLogCompressionBlock {
			t.Fatalf("expected compression=block, got %v", got)
		}
	})

	t.Run("dict", func(t *testing.T) {
		opts := Options{}
		t.Setenv(envVlogCompression, "dict")
		applyEnvMaintenanceOverrides(&opts)
		if got := opts.ValueLog.Compression; got != ValueLogCompressionDict {
			t.Fatalf("expected compression=dict, got %v", got)
		}
	})

	t.Run("auto", func(t *testing.T) {
		opts := Options{}
		t.Setenv(envVlogCompression, "auto")
		applyEnvMaintenanceOverrides(&opts)
		if got := opts.ValueLog.Compression; got != ValueLogCompressionAuto {
			t.Fatalf("expected compression=auto, got %v", got)
		}
	})

	t.Run("off", func(t *testing.T) {
		opts := Options{}
		t.Setenv(envVlogCompression, "off")
		applyEnvMaintenanceOverrides(&opts)
		if got := opts.ValueLog.Compression; got != ValueLogCompressionOff {
			t.Fatalf("expected compression=off, got %v", got)
		}
	})
}

func TestApplyEnvMaintenanceOverrides_VlogAutoPolicy(t *testing.T) {
	opts := Options{}
	t.Setenv(envVlogAutoPolicy, "size")
	applyEnvMaintenanceOverrides(&opts)
	if got := opts.ValueLog.AutoPolicy; got != ValueLogAutoSize {
		t.Fatalf("expected auto policy=size, got %v", got)
	}
}

func TestApplyEnvMaintenanceOverrides_VlogBlockCodec(t *testing.T) {
	cases := []struct {
		env  string
		want ValueLogBlockCodec
	}{
		{env: "snappy", want: ValueLogBlockSnappy},
		{env: "lz4", want: ValueLogBlockLZ4},
		{env: "zstd", want: ValueLogBlockZSTD},
	}
	for _, tc := range cases {
		t.Run(tc.env, func(t *testing.T) {
			opts := Options{}
			t.Setenv(envVlogBlockCodec, tc.env)
			applyEnvMaintenanceOverrides(&opts)
			if got := opts.ValueLog.BlockCodec; got != tc.want {
				t.Fatalf("expected block codec=%s, got %v", tc.env, got)
			}
		})
	}
}

func TestApplyEnvMaintenanceOverrides_VlogDictEnableDisable(t *testing.T) {
	t.Run("enable", func(t *testing.T) {
		opts := Options{DisableSideStores: true}
		t.Setenv(envVlogDictEnable, "")
		applyEnvMaintenanceOverrides(&opts)
		if opts.DisableSideStores {
			t.Fatalf("expected side stores enabled when dict enabled")
		}
		if got := opts.ValueLog.DictTrain.TrainBytes; got != compression.DefaultTrainBytes {
			t.Fatalf("expected train bytes=%d, got %d", compression.DefaultTrainBytes, got)
		}
	})

	t.Run("disable", func(t *testing.T) {
		opts := Options{}
		opts.ValueLog.DictTrain.TrainBytes = compression.DefaultTrainBytes
		t.Setenv(envVlogDictEnable, "false")
		applyEnvMaintenanceOverrides(&opts)
		if got := opts.ValueLog.DictTrain.TrainBytes; got != -1 {
			t.Fatalf("expected train bytes=-1, got %d", got)
		}
	})
}

func TestApplyEnvMaintenanceOverrides_VlogDictTrainOverrides(t *testing.T) {
	opts := Options{}
	t.Setenv(envVlogDictTrainBytes, "1234")
	t.Setenv(envVlogDictBytes, "777")
	t.Setenv(envVlogDictMinRecords, "222")
	t.Setenv(envVlogDictMaxRecordBytes, "333")
	t.Setenv(envVlogDictSampleStride, "7")
	t.Setenv(envVlogDictDedupWindow, "9")
	t.Setenv(envVlogDictTrainLevel, "5")
	applyEnvMaintenanceOverrides(&opts)

	train := opts.ValueLog.DictTrain
	if train.TrainBytes != 1234 {
		t.Fatalf("expected train bytes=1234, got %d", train.TrainBytes)
	}
	if train.DictBytes != 777 {
		t.Fatalf("expected dict bytes=777, got %d", train.DictBytes)
	}
	if train.MinRecords != 222 {
		t.Fatalf("expected min records=222, got %d", train.MinRecords)
	}
	if train.MaxRecordBytes != 333 {
		t.Fatalf("expected max record bytes=333, got %d", train.MaxRecordBytes)
	}
	if train.SampleStride != 7 {
		t.Fatalf("expected sample stride=7, got %d", train.SampleStride)
	}
	if train.DedupWindow != 9 {
		t.Fatalf("expected dedup window=9, got %d", train.DedupWindow)
	}
	if train.Level != 5 {
		t.Fatalf("expected train level=5, got %d", train.Level)
	}
}

func TestApplyEnvMaintenanceOverrides_VlogDictFrameOptions(t *testing.T) {
	opts := Options{}
	t.Setenv(envVlogDictMaxK, "16")
	t.Setenv(envVlogDictClassMode, "split_outer_leaf")
	t.Setenv(envVlogDictZstdLevel, "best")
	t.Setenv(envVlogDictEntropy, "true")
	applyEnvMaintenanceOverrides(&opts)

	if got := opts.ValueLog.DictMaxK; got != 16 {
		t.Fatalf("expected max k=16, got %d", got)
	}
	if got := opts.ValueLog.DictClassMode; got != ValueLogDictClassSplitOuterLeaf {
		t.Fatalf("expected dict class mode split_outer_leaf, got %v", got)
	}
	if got := opts.ValueLog.DictFrameEncodeLevel; got != zstd.SpeedBestCompression {
		t.Fatalf("expected zstd level=best, got %v", got)
	}
	if got := opts.ValueLog.DictFrameEnableEntropy; !got {
		t.Fatalf("expected entropy=true, got false")
	}
}

func TestApplyEnvMaintenanceOverrides_VlogDictClassModeDefaultAlias(t *testing.T) {
	opts := Options{}
	opts.ValueLog.DictClassMode = ValueLogDictClassSplitOuterLeaf
	t.Setenv(envVlogDictClassMode, "default")
	applyEnvMaintenanceOverrides(&opts)
	if got := opts.ValueLog.DictClassMode; got != ValueLogDictClassSingle {
		t.Fatalf("expected dict class mode single for default alias, got %v", got)
	}
}

func TestApplyEnvMaintenanceOverrides_VlogRetainedCaps(t *testing.T) {
	opts := Options{}
	t.Setenv(envVlogMaxRetainedBytes, "123456")
	t.Setenv(envVlogMaxRetainedBytesHard, "654321")
	applyEnvMaintenanceOverrides(&opts)
	if got := opts.ValueLog.MaxRetainedBytes; got != 123456 {
		t.Fatalf("expected max retained bytes=123456, got %d", got)
	}
	if got := opts.ValueLog.MaxRetainedBytesHard; got != 654321 {
		t.Fatalf("expected max retained bytes hard=654321, got %d", got)
	}
}

func TestApplyEnvMaintenanceOverrides_VlogRewriteControls(t *testing.T) {
	opts := Options{}
	t.Setenv(envVlogRewriteBudgetBytesPerSec, "123456789")
	t.Setenv(envVlogRewriteBudgetRecordsPerSec, "4321")
	t.Setenv(envVlogRewriteTriggerTotalBytes, "987654321")
	t.Setenv(envVlogRewriteTriggerStaleRatioPPM, "345678")
	t.Setenv(envVlogRewriteTriggerChurnPerSec, "13579")
	applyEnvMaintenanceOverrides(&opts)

	gen := opts.ValueLog.Generational
	if got := gen.RewriteBudgetBytesPerSec; got != 123456789 {
		t.Fatalf("expected rewrite budget bytes/sec=123456789, got %d", got)
	}
	if got := gen.RewriteBudgetRecordsPerSec; got != 4321 {
		t.Fatalf("expected rewrite budget records/sec=4321, got %d", got)
	}
	if got := gen.RewriteTriggerTotalBytes; got != 987654321 {
		t.Fatalf("expected rewrite trigger total bytes=987654321, got %d", got)
	}
	if got := gen.RewriteTriggerStaleRatioPPM; got != 345678 {
		t.Fatalf("expected rewrite trigger stale ratio ppm=345678, got %d", got)
	}
	if got := gen.RewriteTriggerChurnPerSec; got != 13579 {
		t.Fatalf("expected rewrite trigger churn/sec=13579, got %d", got)
	}
}
