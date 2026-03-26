package main

import (
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestParseTreeDBVlogCompressionMode_DefaultIsAuto(t *testing.T) {
	cases := []string{"", "default", "unset"}
	for _, in := range cases {
		mode, explicit, err := parseTreeDBVlogCompressionMode(in)
		if err != nil {
			t.Fatalf("parseTreeDBVlogCompressionMode(%q): %v", in, err)
		}
		if explicit {
			t.Fatalf("parseTreeDBVlogCompressionMode(%q): expected explicit=false", in)
		}
		if mode != treedb.ValueLogCompressionAuto {
			t.Fatalf("parseTreeDBVlogCompressionMode(%q): mode=%v want=%v", in, mode, treedb.ValueLogCompressionAuto)
		}
	}
}

func TestBuildTreeDBOptions_DefaultVlogCompressionAuto(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	*treedbVlogCompression = "default"

	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if opts.ValueLog.Compression != treedb.ValueLogCompressionAuto {
		t.Fatalf("unexpected default compression mode: %v", opts.ValueLog.Compression)
	}
	if opts.ValueLog.AutoPolicy != treedb.ValueLogAutoBalanced {
		t.Fatalf("unexpected default auto policy: %v", opts.ValueLog.AutoPolicy)
	}
}

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

func TestBuildTreeDBOptions_VlogAutoPolicyFlag(t *testing.T) {
	oldPolicy := *treedbVlogAutoPolicy
	defer func() { *treedbVlogAutoPolicy = oldPolicy }()
	*treedbVlogAutoPolicy = "throughput"

	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if opts.ValueLog.AutoPolicy != treedb.ValueLogAutoThroughput {
		t.Fatalf("unexpected auto policy: %v", opts.ValueLog.AutoPolicy)
	}
}

func TestBuildTreeDBOptions_InvalidVlogAutoPolicy(t *testing.T) {
	oldPolicy := *treedbVlogAutoPolicy
	defer func() { *treedbVlogAutoPolicy = oldPolicy }()
	*treedbVlogAutoPolicy = "invalid"
	if _, _, err := buildTreeDBOptions(""); err == nil {
		t.Fatalf("expected error for invalid auto policy")
	}
}

func TestResolvedTreeDBVlogTrainDefaults(t *testing.T) {
	train, dict := resolvedTreeDBVlogTrainDefaults(0, 0)
	if train != 1<<20 {
		t.Fatalf("expected default train bytes, got %d", train)
	}
	if dict != 32<<10 {
		t.Fatalf("expected default dict bytes, got %d", dict)
	}

	train, dict = resolvedTreeDBVlogTrainDefaults(-1, 0)
	if train != -1 {
		t.Fatalf("expected explicit train disable to be preserved, got %d", train)
	}
	if dict != 0 {
		t.Fatalf("expected dict bytes preserved with disabled train, got %d", dict)
	}
}

func TestBuildTreeDBOptions_DefaultChunkSize(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbVlogCompression = "default"

	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if opts.ChunkSize != defaultTreeDBChunkSizeBytes {
		t.Fatalf("unexpected default chunk size: got=%d want=%d", opts.ChunkSize, defaultTreeDBChunkSizeBytes)
	}
}

func TestBuildTreeDBOptions_ChunkSizeFlagOverride(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbChunkSize = 2 << 20
	*treedbVlogCompression = "default"

	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if opts.ChunkSize != (2 << 20) {
		t.Fatalf("unexpected chunk size: got=%d want=%d", opts.ChunkSize, 2<<20)
	}
}
