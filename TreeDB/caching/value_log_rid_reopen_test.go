package caching

import (
	"bytes"
	"io"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestDisableWAL_ReopenDoesNotReuseValueLogRIDs(t *testing.T) {
	dir := t.TempDir()

	writeSession := func(prefix byte) {
		backend, err := backenddb.Open(backenddb.Options{Dir: dir})
		if err != nil {
			t.Fatalf("backend open: %v", err)
		}
		db, err := Open(dir, backend, Options{
			FlushThreshold:           1 << 20,
			DisableWAL:               true,
			RelaxedSync:              true,
			AllowUnsafe:              true,
			MemtableShards:           1,
			JournalLanes:             1,
			ValueLogPointerThreshold: 1,
			ForceValueLogPointers:    true,
			ValueLogMaxSegmentBytes:  8 << 10,
		})
		if err != nil {
			_ = backend.Close()
			t.Fatalf("Open: %v", err)
		}
		value := bytes.Repeat([]byte{prefix}, 4096)
		for i := 0; i < 8; i++ {
			key := []byte{prefix, byte(i)}
			if err := db.Set(key, value); err != nil {
				_ = db.Close()
				t.Fatalf("Set(%d): %v", i, err)
			}
		}
		if err := db.Checkpoint(); err != nil {
			_ = db.Close()
			t.Fatalf("Checkpoint: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	writeSession('a')
	writeSession('b')

	segments, _ := listNonEmptyLogSegments(dir + "/wal")
	dups := duplicateValueLogRIDCount(t, segments)
	if dups != 0 {
		t.Fatalf("expected no duplicate RIDs after reopen, got %d", dups)
	}
}

func duplicateValueLogRIDCount(t *testing.T, segments []logSegmentInfo) int {
	t.Helper()

	seen := make(map[uint64]struct{})
	dups := 0
	for _, seg := range segments {
		if !seg.valueLog || seg.size <= 0 || seg.lane < 0 || seg.seq < 0 {
			continue
		}
		fileID, err := valuelog.EncodeFileID(uint32(seg.lane), uint32(seg.seq))
		if err != nil {
			t.Fatalf("EncodeFileID(%d,%d): %v", seg.lane, seg.seq, err)
		}
		reader, err := valuelog.NewReader(seg.path, fileID)
		if err != nil {
			t.Fatalf("NewReader(%s): %v", seg.path, err)
		}
		reader.DisableValueDecode()
		for {
			rid, _, _, err := reader.ReadNext()
			if err == nil {
				if _, ok := seen[rid]; ok {
					dups++
				} else {
					seen[rid] = struct{}{}
				}
				continue
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			_ = reader.Close()
			t.Fatalf("ReadNext(%s): %v", seg.path, err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("Close(%s): %v", seg.path, err)
		}
	}
	return dups
}
