package caching

import (
	"bytes"
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
	closedDB := false
	defer func() {
		if !closedDB {
			_ = db.Close()
		}
	}()

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

func TestCachedBatchWrite_OuterLeafFramesIncludeBatchedSnappyPages(t *testing.T) {
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
	closedDB := false
	defer func() {
		if !closedDB {
			_ = db.Close()
		}
	}()

	b := db.NewBatch()
	for i := 0; i < 12000; i++ {
		key := []byte(fmt.Sprintf("k%06d", i))
		val := bytes.Repeat([]byte(fmt.Sprintf("value-%06d-", i)), 8)
		if err := b.Set(key, val); err != nil {
			_ = b.Close()
			t.Fatalf("Set(%q): %v", key, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("WriteSync: %v", err)
	}
	_ = b.Close()
	for _, idx := range []int{42, 777, 11042} {
		key := []byte(fmt.Sprintf("k%06d", idx))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) before checkpoint: %v", key, err)
		}
		want := bytes.Repeat([]byte(fmt.Sprintf("value-%06d-", idx)), 8)
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) mismatch before checkpoint", key)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	for _, idx := range []int{42, 777, 11042} {
		key := []byte(fmt.Sprintf("k%06d", idx))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) after checkpoint: %v", key, err)
		}
		want := bytes.Repeat([]byte(fmt.Sprintf("value-%06d-", idx)), 8)
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) mismatch after checkpoint", key)
		}
	}

	proj, ok := db.backend.(pointerProjectionBackend)
	if !ok {
		t.Fatalf("backend does not expose pager/state")
	}
	state := proj.State()
	if state == nil {
		t.Fatalf("missing backend state")
	}
	leafRefs := collectLeafRefs(t, proj.Pager(), state.RootPageID)
	if len(leafRefs) < 4 {
		t.Fatalf("expected many leaf refs, got %d", len(leafRefs))
	}

	batched := 0
	compressed := 0
	lz4 := 0
	for _, leafID := range leafRefs {
		ptr, ok := page.DecodeLeafRef(leafID)
		if !ok {
			t.Fatalf("DecodeLeafRef(%d): false", leafID)
		}
		frame := readValueLogFrameHeaderAtPtr(t, dir, ptr)
		if frame.K > 1 || page.ValuePtrSubIndex(ptr) > 0 {
			batched++
		}
		if frame.Flags&valuelog.FrameFlagCompressed != 0 {
			compressed++
			if valuelog.BlockCodec(frame.Reserved) == valuelog.BlockCodecLZ4 {
				lz4++
			}
		}
	}
	if batched == 0 {
		t.Fatalf("expected at least one batched outer-leaf frame across %d leaf refs", len(leafRefs))
	}
	if compressed == 0 {
		t.Fatalf("expected compressed outer-leaf frames")
	}
	if lz4 != 0 {
		t.Fatalf("unexpected lz4 outer-leaf frames=%d", lz4)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	closedDB = true
	reopen, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	defer reopen.Close()
	for _, idx := range []int{42, 777, 11042} {
		key := []byte(fmt.Sprintf("k%06d", idx))
		got, err := reopen.Get(key)
		if err != nil {
			t.Fatalf("reopen.Get(%q): %v", key, err)
		}
		want := bytes.Repeat([]byte(fmt.Sprintf("value-%06d-", idx)), 8)
		if !bytes.Equal(got, want) {
			t.Fatalf("reopen.Get(%q) mismatch", key)
		}
	}
}
