package db

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/leafblock"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func leafBlockCodecHeaderID(codec ValueLogBlockCodec) int {
	switch codec {
	case ValueLogBlockSnappy:
		return 1
	case ValueLogBlockLZ4:
		return 2
	default:
		return 0
	}
}

func TestNewReplayInlineAppender_PackedValuePtrCapsSegmentSize(t *testing.T) {
	db := &DB{
		dir:                 t.TempDir(),
		indexPackedValuePtr: true,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	defer func() { _ = app.close() }()

	if app.writer == nil {
		t.Fatalf("expected replay inline appender writer")
	}
	want := int64(^uint32(0)) - 4
	if got := app.writer.maxSize; got != want {
		t.Fatalf("unexpected replay appender maxSize: got %d want %d", got, want)
	}
}

func TestNewReplayInlineAppender_UnpackedNoSegmentCap(t *testing.T) {
	db := &DB{
		dir:                 t.TempDir(),
		indexPackedValuePtr: false,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	defer func() { _ = app.close() }()

	if app.writer == nil {
		t.Fatalf("expected replay inline appender writer")
	}
	if got := app.writer.maxSize; got != 0 {
		t.Fatalf("unexpected replay appender maxSize: got %d want 0", got)
	}
}

func assertReplayInlineAppenderLeafBlockEncoding(t *testing.T, codec ValueLogBlockCodec, restart int) {
	t.Helper()
	db := &DB{
		dir:              t.TempDir(),
		leafBlockCodec:   codec,
		leafBlockRestart: restart,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(db.dir, "wal"), 0o755); err != nil {
		t.Fatalf("mkdir wal dir: %v", err)
	}
	key := []byte("k")
	value := bytes.Repeat([]byte("a"), 4096)
	ptr, err := app.append(db, key, value)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := app.syncIfDirty(); err != nil {
		t.Fatalf("syncIfDirty: %v", err)
	}
	if err := app.close(); err != nil {
		t.Fatalf("close appender: %v", err)
	}

	vm, err := valuelog.NewManager(filepath.Join(db.dir, "wal"))
	if err != nil {
		t.Fatalf("open value log manager: %v", err)
	}
	defer vm.Close()
	raw, err := vm.ReadUnsafe(ptr)
	if err != nil {
		t.Fatalf("read appended payload: %v", err)
	}
	if !leafblock.HasMagic(raw) {
		t.Fatalf("expected leaf-block encoded payload")
	}
	if len(raw) < 8 {
		t.Fatalf("leaf-block payload too short: %d", len(raw))
	}
	wantCodec := leafBlockCodecHeaderID(codec)
	if wantCodec == 0 {
		t.Fatalf("unsupported test codec %d", codec)
	}
	if got := int(raw[5]); got != wantCodec {
		t.Fatalf("leaf-block codec id = %d, want %d", got, wantCodec)
	}
	if got, want := int(binary.LittleEndian.Uint16(raw[6:8])), leafblock.NormalizeRestartInterval(db.leafBlockRestart); got != want {
		t.Fatalf("leaf-block restart interval = %d, want %d", got, want)
	}
	decoded, ok, found, _, err := leafblock.DecodeValueForKey(raw, key, nil)
	if err != nil {
		t.Fatalf("decode appended payload: %v", err)
	}
	if !ok || !found {
		t.Fatalf("decode result ok=%v found=%v", ok, found)
	}
	if !bytes.Equal(decoded, value) {
		t.Fatalf("decoded value mismatch")
	}
}

func TestReplayInlineAppender_UsesConfiguredLeafBlockEncoding(t *testing.T) {
	assertReplayInlineAppenderLeafBlockEncoding(t, ValueLogBlockLZ4, 7)
}
