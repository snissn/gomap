package main

import (
	"testing"

	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestLevelDBBenchOptions_BlockSizeFlag(t *testing.T) {
	prev := *leveldbBlockSize
	defer func() { *leveldbBlockSize = prev }()

	*leveldbBlockSize = 8 << 10
	opts := leveldbBenchOptions(opt.NoCompression)
	if got := opts.GetBlockSize(); got != 8<<10 {
		t.Fatalf("GetBlockSize()=%d, want %d", got, 8<<10)
	}

	*leveldbBlockSize = -1
	opts = leveldbBenchOptions(opt.NoCompression)
	if got := opts.GetBlockSize(); got != 4096 {
		t.Fatalf("GetBlockSize()=%d, want default %d", got, 4096)
	}
}
