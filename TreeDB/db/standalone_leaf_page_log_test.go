package db

import (
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestStandaloneLeafPageLogEnablesDirectOuterLeafWrites(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	dbClosed := false
	t.Cleanup(func() {
		if !dbClosed {
			_ = d.Close()
		}
	})
	var leafLog LeafPageLogCloser
	t.Cleanup(func() {
		if leafLog != nil {
			_ = leafLog.Close()
		}
	})
	leafLog, err = NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{
		Compression: ValueLogCompressionOff,
	})
	if err != nil {
		t.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	d.SetLeafPageLog(leafLog)

	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		value := []byte(fmt.Sprintf("v%04d", i))
		if err := d.Set(key, value); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close db: %v", err)
	}
	dbClosed = true
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}
	leafLog = nil

	reopened, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.Get([]byte("k0255"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(got) != "v0255" {
		t.Fatalf("value after reopen=%q, want v0255", got)
	}
}

func TestStandaloneLeafPageLogReopenAdvancesLeafLaneSequence(t *testing.T) {
	dir := t.TempDir()
	first, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionOff})
	if err != nil {
		t.Fatalf("NewStandaloneLeafPageLog first: %v", err)
	}
	if _, err := first.AppendLeafPage([]byte("leaf-1")); err != nil {
		_ = first.Close()
		t.Fatalf("AppendLeafPage first: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	second, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionOff})
	if err != nil {
		t.Fatalf("NewStandaloneLeafPageLog second: %v", err)
	}
	ptr, err := second.AppendLeafPage([]byte("leaf-2"))
	if err != nil {
		t.Fatalf("AppendLeafPage second: %v", err)
	}
	lane, seq := valuelog.DecodeFileID(ptr.ValuePtr().FileID)
	if lane != rewriteLeafLogLaneID || seq != 2 {
		t.Fatalf("second leaf ptr lane=%d seq=%d, want lane=%d seq=2", lane, seq, rewriteLeafLogLaneID)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close second: %v", err)
	}
	segments, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("listValueLogSegments: %v", err)
	}
	ridMap, err := scanValueLogSegments(segments, nil)
	if err != nil {
		t.Fatalf("scanValueLogSegments: %v", err)
	}
	if len(ridMap) != 2 {
		t.Fatalf("RID map size=%d, want 2", len(ridMap))
	}
}

func TestStandaloneLeafPageLogRejectsInvalidMaxSegmentBytes(t *testing.T) {
	for _, maxBytes := range []int64{-1, standaloneLeafPageLogMaxSegmentBytes + 1} {
		if log, err := NewStandaloneLeafPageLog(t.TempDir(), StandaloneLeafPageLogOptions{MaxSegmentBytes: maxBytes}); err == nil {
			_ = log.Close()
			t.Fatalf("NewStandaloneLeafPageLog MaxSegmentBytes=%d unexpectedly succeeded", maxBytes)
		} else if !strings.Contains(err.Error(), fmt.Sprintf("%d", maxBytes)) {
			t.Fatalf("NewStandaloneLeafPageLog MaxSegmentBytes=%d error=%q, want invalid value in message", maxBytes, err)
		}
	}
}
