package main

import (
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestBuildTreeDBOptions_VlogCompressionBlockFlags(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	oldCompression := *treedbVlogCompression
	oldCodec := *treedbVlogBlockCodec
	oldTarget := *treedbVlogBlockTargetBytes
	oldHold := *treedbVlogIncompressibleHoldBytes
	oldProbe := *treedbVlogIncompressibleProbeBytes
	oldTrainBytes := *treedbVlogDictTrainBytes
	defer func() {
		*treedbVlogCompression = oldCompression
		*treedbVlogBlockCodec = oldCodec
		*treedbVlogBlockTargetBytes = oldTarget
		*treedbVlogIncompressibleHoldBytes = oldHold
		*treedbVlogIncompressibleProbeBytes = oldProbe
		*treedbVlogDictTrainBytes = oldTrainBytes
	}()

	*treedbVlogCompression = "block"
	*treedbVlogBlockCodec = "lz4"
	*treedbVlogBlockTargetBytes = 8192
	*treedbVlogIncompressibleHoldBytes = 2 << 20
	*treedbVlogIncompressibleProbeBytes = 512 << 10
	*treedbVlogDictTrainBytes = 4 << 20

	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if opts.ValueLog.Compression != treedb.ValueLogCompressionBlock {
		t.Fatalf("unexpected compression mode: %v", opts.ValueLog.Compression)
	}
	if opts.ValueLog.BlockCodec != treedb.ValueLogBlockLZ4 {
		t.Fatalf("unexpected block codec: %v", opts.ValueLog.BlockCodec)
	}
	if opts.ValueLog.BlockTargetCompressedBytes != 8192 {
		t.Fatalf("unexpected block target: %d", opts.ValueLog.BlockTargetCompressedBytes)
	}
	if opts.ValueLog.IncompressibleHoldBytes != 2<<20 {
		t.Fatalf("unexpected hold bytes: %d", opts.ValueLog.IncompressibleHoldBytes)
	}
	if opts.ValueLog.IncompressibleProbeIntervalBytes != 512<<10 {
		t.Fatalf("unexpected probe bytes: %d", opts.ValueLog.IncompressibleProbeIntervalBytes)
	}
	if opts.ValueLog.DictTrain.TrainBytes > 0 {
		t.Fatalf("expected dict train to be disabled in block mode, got %d", opts.ValueLog.DictTrain.TrainBytes)
	}
}

func TestBuildTreeDBOptions_InvalidVlogBlockCodec(t *testing.T) {
	oldCodec := *treedbVlogBlockCodec
	defer func() { *treedbVlogBlockCodec = oldCodec }()
	*treedbVlogBlockCodec = "nope"
	if _, _, err := buildTreeDBOptions(""); err == nil {
		t.Fatalf("expected error for invalid block codec")
	}
}
