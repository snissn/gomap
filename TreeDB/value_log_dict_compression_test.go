package treedb

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/compression"
)

func TestEnableValueLogDictCompression_SetsDefaults(t *testing.T) {
	opts := Options{
		DisableSideStores: true,
		ValueLog: ValueLogOptions{
			DictTrain: TrainConfig{TrainBytes: -1},
		},
	}
	EnableValueLogDictCompression(&opts)
	if opts.DisableSideStores {
		t.Fatalf("expected DisableSideStores=false after enable")
	}
	if got := opts.ValueLog.DictTrain.TrainBytes; got != compression.DefaultTrainBytes {
		t.Fatalf("unexpected TrainBytes after enable: got=%d want=%d", got, compression.DefaultTrainBytes)
	}
}

func TestEnableValueLogDictCompression_DoesNotClobberExplicitTrainBytes(t *testing.T) {
	opts := Options{
		ValueLog: ValueLogOptions{
			DictTrain: TrainConfig{TrainBytes: 1234},
		},
	}
	EnableValueLogDictCompression(&opts)
	if got := opts.ValueLog.DictTrain.TrainBytes; got != 1234 {
		t.Fatalf("expected TrainBytes to remain unchanged, got=%d", got)
	}
}

func TestDisableValueLogDictCompression_PreservesOtherFields(t *testing.T) {
	opts := Options{
		ValueLog: ValueLogOptions{
			DictTrain: TrainConfig{
				TrainBytes: 64 << 10,
				DictBytes:  777,
			},
		},
	}
	DisableValueLogDictCompression(&opts)
	if got := opts.ValueLog.DictTrain.TrainBytes; got != -1 {
		t.Fatalf("expected TrainBytes=-1 after disable, got=%d", got)
	}
	if got := opts.ValueLog.DictTrain.DictBytes; got != 777 {
		t.Fatalf("expected DictBytes preserved after disable, got=%d", got)
	}
}
