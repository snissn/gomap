package db

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestLeafGenerationLogicalRebuildOffline_UsesCurrentOuterLeafDictForSplitLeafLog(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		ValueLog: ValueLogOptions{
			Compression:      ValueLogCompressionBlock,
			BlockCodec:       ValueLogBlockLZ4,
			PointerThreshold: 4096,
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	leafLog := &rewriteTestLeafPageLog{db: db, dir: dir}
	db.SetLeafPageLog(leafLog)
	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("leaf-logical-dict-%04d", i))
		val := bytes.Repeat([]byte(fmt.Sprintf("leaf-logical-%02d|", i%32)), 2)
		if err := db.Set(key, val); err != nil {
			_ = leafLog.Close()
			_ = db.Close()
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if err := leafLog.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("close leaf log: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	leafA, _, err := valuelog.MaybeCompactLeafLogPayload(buildRewriteLeafPageFixture(t, "logical-a"))
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload(a): %v", err)
	}
	leafB, _, err := valuelog.MaybeCompactLeafLogPayload(buildRewriteLeafPageFixture(t, "logical-b"))
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload(b): %v", err)
	}
	leafC, _, err := valuelog.MaybeCompactLeafLogPayload(buildRewriteLeafPageFixture(t, "logical-c"))
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload(c): %v", err)
	}
	dictID := uint64(902311)
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: [][]byte{leafA, leafB, leafC},
		History:  append([]byte(nil), leafA...),
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}

	rebuildOpts := opts
	rebuildOpts.ValueLog.DictLookup = func(id uint64) ([]byte, error) {
		if id != dictID {
			return nil, valuelog.ErrMissingDict
		}
		return dict, nil
	}
	rebuildOpts.ValueLog.DictCurrentForClass = func(_ context.Context, class string) (uint64, error) {
		if class == "outer_leaf" {
			return dictID, nil
		}
		return 0, nil
	}
	stats, err := LeafGenerationLogicalRebuildOffline(rebuildOpts)
	if err != nil {
		t.Fatalf("LeafGenerationLogicalRebuildOffline: %v", err)
	}
	if stats.LeafDictID != dictID {
		t.Fatalf("logical rebuild leaf dict id=%d want=%d", stats.LeafDictID, dictID)
	}

	leafDir := filepath.Join(dir, "leaf_vlog")
	frameHeader := readFirstRewriteFrameHeaderForLane(t, leafDir, rewriteLeafLogLaneID)
	if frameHeader.DictID != dictID {
		t.Fatalf("logical rebuild leaf dict id=%d want=%d", frameHeader.DictID, dictID)
	}
}
