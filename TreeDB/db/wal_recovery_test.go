package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

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

func TestNewReplayInlineAppender_UsesConfiguredValueLogBlockCompression(t *testing.T) {
	db := &DB{
		dir:                 t.TempDir(),
		valueLogCompression: ValueLogCompressionBlock,
		valueLogBlockCodec:  ValueLogBlockLZ4,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	defer func() { _ = app.close() }()

	if !app.writer.blockCompression {
		t.Fatalf("expected replay writer block compression enabled")
	}
	if got, want := app.writer.blockCodec, valuelog.BlockCodecLZ4; got != want {
		t.Fatalf("unexpected replay writer block codec: got=%v want=%v", got, want)
	}
}

func TestNewReplayInlineAppender_ValueLogCompressionOff_DisablesBlockCompression(t *testing.T) {
	db := &DB{
		dir:                 t.TempDir(),
		valueLogCompression: ValueLogCompressionOff,
		valueLogBlockCodec:  ValueLogBlockSnappy,
	}
	app, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("newReplayInlineAppender: %v", err)
	}
	defer func() { _ = app.close() }()

	if app.writer.blockCompression {
		t.Fatalf("expected replay writer block compression disabled")
	}
}

func TestReplayWALIntoBackend_IgnoresEmptyCommitSegments(t *testing.T) {
	segments := []logSegment{
		{path: "/tmp/empty-commit.log", size: 0, valueLog: false},
		{path: "/tmp/value.log", size: 128, valueLog: true, fileID: 1},
	}
	if err := replayWALIntoBackend(nil, segments, 0, nil); err != nil {
		t.Fatalf("replayWALIntoBackend: %v", err)
	}
}

func TestScanValueLogSegments_IgnoresSegmentRemovedAfterScan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	if err := os.WriteFile(path, []byte{0x01}, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	segments := []logSegment{
		{path: path, fileID: fileID, valueLog: true},
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	ridMap, err := scanValueLogSegments(segments, nil)
	if err != nil {
		t.Fatalf("scanValueLogSegments: %v", err)
	}
	if len(ridMap) != 0 {
		t.Fatalf("ridMap len=%d want 0", len(ridMap))
	}
}
