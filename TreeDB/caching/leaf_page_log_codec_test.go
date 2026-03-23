package caching

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func readValueLogFrameHeaderAtPtr(t *testing.T, dir string, ptr page.ValuePtr) valuelog.FrameHeader {
	t.Helper()
	lane, seq := valuelog.DecodeFileID(ptr.FileID)
	path := filepath.Join(dir, "wal", fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	start := int64(ptr.Offset - 4)
	var header [valuelog.HeaderSize + valuelog.FrameHeaderSize]byte
	if _, err := f.ReadAt(header[:], start); err != nil {
		t.Fatalf("ReadAt(%s,%d): %v", path, start, err)
	}
	if header[4] != valuelog.Version {
		t.Fatalf("unexpected record version %d", header[4])
	}
	if header[5]&1 == 0 {
		t.Fatalf("expected grouped leaf-page record")
	}
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if valueLen < valuelog.FrameHeaderSize {
		t.Fatalf("grouped record too short: %d", valueLen)
	}
	return valuelog.FrameHeader{
		Version:  header[valuelog.HeaderSize+0],
		Flags:    header[valuelog.HeaderSize+1],
		K:        header[valuelog.HeaderSize+2],
		Reserved: header[valuelog.HeaderSize+3],
		DictID:   binary.LittleEndian.Uint64(header[valuelog.HeaderSize+4 : valuelog.HeaderSize+12]),
	}
}

func TestCachingLeafPageLog_AppendLeafPages_DefaultAutoUsesGroupedSnappyFrames(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	db, err := Open(dir, backend, Options{
		DisableWAL:                 true,
		AllowUnsafe:                true,
		RelaxedSync:                true,
		FlushThreshold:             32 << 10,
		IndexOuterLeavesInValueLog: true,
		ValueLogCompression:        uint8(vlogCompressionAuto),
		ValueLogBlockCodec:         1, // lz4 global preference; leaf path should override this.
	})
	if err != nil {
		t.Fatalf("cache open: %v", err)
	}
	defer db.Close()

	log := newCachingLeafPageLog(db, 0)
	ptrs, err := log.(*cachingLeafPageLog).AppendLeafPages([][]byte{
		make([]byte, page.PageSize),
		make([]byte, page.PageSize),
		make([]byte, page.PageSize),
		make([]byte, page.PageSize),
	})
	if err != nil {
		t.Fatalf("AppendLeafPages: %v", err)
	}
	if err := db.flushValueLog(0); err != nil {
		t.Fatalf("flushValueLog: %v", err)
	}
	if len(ptrs) != 4 {
		t.Fatalf("ptr count=%d want 4", len(ptrs))
	}

	sawCompressed := false
	sawBatched := false
	for _, ptr := range ptrs {
		frame := readValueLogFrameHeaderAtPtr(t, dir, ptr)
		if frame.Version != valuelog.FrameVersion {
			t.Fatalf("unexpected frame version %d", frame.Version)
		}
		if frame.K > 1 || page.ValuePtrSubIndex(ptr) > 0 {
			sawBatched = true
		}
		if frame.DictID != 0 {
			t.Fatalf("unexpected dict-compressed leaf frame dictID=%d", frame.DictID)
		}
		if frame.Flags&valuelog.FrameFlagCompressed == 0 {
			continue
		}
		sawCompressed = true
		if valuelog.BlockCodec(frame.Reserved) == valuelog.BlockCodecLZ4 {
			t.Fatalf("leaf page frame used lz4 block codec")
		}
	}
	if !sawCompressed {
		t.Fatalf("expected at least one compressed leaf page frame")
	}
	if !sawBatched {
		t.Fatalf("expected at least one batched leaf page frame")
	}
}
