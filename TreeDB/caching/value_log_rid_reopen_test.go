package caching

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
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
		defer func() { _ = backend.Close() }()
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
		closed := false
		defer func() {
			if !closed {
				_ = db.Close()
			}
		}()
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
		closed = true
	}

	writeSession('a')
	writeSession('b')

	segments, _ := listNonEmptyLogSegments(filepath.Join(dir, "value_vlog"))
	dups := duplicateValueLogRIDCount(t, segments)
	if dups != 0 {
		t.Fatalf("expected no duplicate RIDs after reopen, got %d", dups)
	}
}

func TestDisableWAL_OnlineRewriteDoesNotReuseValueLogRIDs(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer func() { _ = backend.Close() }()
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
	defer func() { _ = db.Close() }()

	value := bytes.Repeat([]byte("seed-"), 1024)
	for i := 0; i < 8; i++ {
		key := []byte{'k', byte(i)}
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint seed: %v", err)
	}

	segments, _ := listNonEmptyLogSegments(filepath.Join(dir, "value_vlog"))
	var sourceID uint32
	for _, seg := range segments {
		if seg.valueLog {
			fileID, err := valuelog.EncodeFileID(uint32(seg.lane), uint32(seg.seq))
			if err != nil {
				t.Fatalf("EncodeFileID(%d,%d): %v", seg.lane, seg.seq, err)
			}
			sourceID = fileID
			break
		}
	}
	if sourceID == 0 {
		t.Fatalf("expected value-log segment after seed writes")
	}

	rewriter, ok := db.backend.(interface {
		ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error)
	})
	if !ok {
		t.Fatalf("backend missing ValueLogRewriteOnline")
	}
	stats, err := rewriter.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{sourceID},
		ReserveRIDs: func(count int) (uint64, error) {
			if count <= 0 {
				return 0, nil
			}
			end := db.nextRID.Add(uint64(count))
			start := end - uint64(count) + 1
			if start == 0 || end < start {
				return 0, errWALUnavailable
			}
			return start, nil
		},
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected rewrite to copy records")
	}

	if err := db.Set([]byte("after"), bytes.Repeat([]byte("after-"), 1024)); err != nil {
		t.Fatalf("Set(after): %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after rewrite: %v", err)
	}

	segments, _ = listNonEmptyLogSegments(filepath.Join(dir, "value_vlog"))
	dups := duplicateValueLogRIDCount(t, segments)
	if dups != 0 {
		t.Fatalf("expected no duplicate RIDs after online rewrite, got %d", dups)
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
