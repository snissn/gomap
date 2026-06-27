package caching

import (
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCachingValueLogExternalRefFlusherSyncsRotatedSegments(t *testing.T) {
	dir := t.TempDir()
	oldFileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID old: %v", err)
	}
	currentFileID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("EncodeFileID current: %v", err)
	}

	db := &DB{
		valueLogDir: dir,
		lanes:       make([]lane, 1),
	}
	db.lanes[0].id = 0
	db.lanes[0].vlogSeq = 2
	db.lanes[0].vlogPath = valuelog.SegmentPath(dir, currentFileID)
	appender := &cachingValueLogAppender{db: db, lane: &db.lanes[0]}

	if err := appender.FlushValueLogExternalRefs([]uint32{currentFileID}, true); err != nil {
		t.Fatalf("FlushValueLogExternalRefs current segment: %v", err)
	}
	if err := appender.FlushValueLogExternalRefs([]uint32{oldFileID}, true); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("FlushValueLogExternalRefs missing rotated segment error=%v, want os.ErrNotExist", err)
	}
	if err := os.WriteFile(valuelog.SegmentPath(dir, oldFileID), []byte("old segment"), 0o644); err != nil {
		t.Fatalf("write old segment: %v", err)
	}
	if err := appender.FlushValueLogExternalRefs([]uint32{oldFileID, currentFileID, oldFileID}, true); err != nil {
		t.Fatalf("FlushValueLogExternalRefs rotated segment: %v", err)
	}
}
