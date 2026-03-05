package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

func (db *DB) batchSetWithPointerFallback(b *Batch, key, value []byte, view bool) (bool, error) {
	var err error
	if view {
		err = b.SetView(key, value)
	} else {
		err = b.Set(key, value)
	}
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, batchpkg.ErrValueTooLarge) {
		return false, err
	}
	ptr, err := db.appendValueLogPointer(value)
	if err != nil {
		return false, err
	}
	if view {
		return true, b.SetPointerView(key, ptr)
	}
	return true, b.SetPointer(key, ptr)
}

func (db *DB) appendValueLogPointer(value []byte) (ptr page.ValuePtr, err error) {
	if db == nil {
		return page.ValuePtr{}, fmt.Errorf("missing db")
	}
	db.inlineAppendMu.Lock()
	defer db.inlineAppendMu.Unlock()

	app, err := db.inlineAppenderLocked()
	if err != nil {
		return page.ValuePtr{}, err
	}
	return app.append(value)
}

func (db *DB) flushInlineAppender(sync bool) error {
	if db == nil {
		return fmt.Errorf("missing db")
	}
	db.inlineAppendMu.Lock()
	defer db.inlineAppendMu.Unlock()

	if db.inlineAppender == nil {
		return nil
	}
	if sync {
		return db.inlineAppender.Sync()
	}
	return db.inlineAppender.Flush()
}

func (db *DB) inlineAppenderLocked() (*replayInlineAppender, error) {
	if db.inlineAppender != nil {
		return db.inlineAppender, nil
	}
	segments, err := listWALSegments(db.dir)
	if err != nil {
		return nil, err
	}
	nextRID, err := nextRewriteRIDStart(segments)
	if err != nil {
		return nil, err
	}
	var maxLane0Seq uint32
	for _, seg := range segments {
		if !seg.valueLog || seg.lane != 0 {
			continue
		}
		if seg.seq > uint64(maxLane0Seq) {
			if seg.seq > uint64(^uint32(0)) {
				return nil, fmt.Errorf("valuelog: lane 0 sequence overflow %d", seg.seq)
			}
			maxLane0Seq = uint32(seg.seq)
		}
	}
	maxSegmentBytes := int64(0)
	if db.indexPackedValuePtr || db.leafPageLog != nil {
		maxSegmentBytes = int64(^uint32(0)) - 4
	}
	walDir := filepath.Join(db.dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return nil, err
	}
	writer := newRewriteWriter(walDir, 0, maxLane0Seq, maxSegmentBytes)
	writer.blockCompression = db.valueLogCompression != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(db.valueLogBlockCodec)
	db.inlineAppender = &replayInlineAppender{
		writer:  writer,
		nextRID: nextRID,
	}
	return db.inlineAppender, nil
}
